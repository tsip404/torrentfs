//! DownloadService — orchestrates download operations through DownloadManager.
//!
//! Wraps `Arc<Mutex<DownloadManager>>` and exposes a clean API
//! for the FUSE layer, hiding the lock management.

use std::path::Path;
use std::sync::{Arc, Mutex};

use hex;

use crate::domain::pieces_manager::PieceStatus;
use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::config::TorrentfsConfig;
use crate::infrastructure::download::{
    DownloadManager, SessionStats, TorrentHandle, TorrentStatus,
};
use crate::infrastructure::metadata::TorrentInfo;
use crate::seeding::SeedingManager;
use crate::infrastructure::alert::SharedSessionStats;

pub struct DownloadService {
    download_manager: Arc<Mutex<DownloadManager>>,
    /// Cached reference to the CacheManager — avoids locking
    /// download_manager for fast disk-cache checks.
    cache_manager: Arc<Mutex<CacheManager>>,
    cached_stats: SharedSessionStats,
}

impl DownloadService {

    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        let dm = DownloadManager::new(cache_dir, config)?;
        let cm = dm.get_cache_manager();
        Ok(Self {
            download_manager: Arc::new(Mutex::new(dm)),
            cache_manager: cm,
            cached_stats: SharedSessionStats::new(),
        })
    }
    /// Register a SeedingManager to receive eviction callbacks from the CacheManager.
    pub fn register_seeding_callback(&self, seeding: Arc<SeedingManager>) {
        let mut dm = self
            .download_manager
            .lock()
            .expect("DownloadManager lock poisoned");
        dm.register_seeding_callback(seeding);
    }

    /// Get the raw libtorrent session pointer for background threads
    /// (e.g., the AlertConsumer). The pointer remains valid for the
    /// lifetime of the Session.
    pub fn session_ptr(&self) -> Option<libtorrent_sys::lt_session_t> {
        self.download_manager
            .lock()
            .ok()
            .and_then(|dm| dm.session_ptr())
    }
    /// Get the shared pending-reads table for the AlertConsumer to
    /// deliver `read_piece_alert` data.
    pub fn pending_reads(&self) -> Option<crate::infrastructure::download::PendingReads> {
        self.download_manager
            .lock()
            .ok()
            .map(|dm| dm.pending_reads())
    }

    /// Ensure a lightweight handle exists for the given torrent info.
    /// Uses upload_mode so it never requests pieces automatically.
    pub fn ensure_handle_lightweight(
        &self,
        info: &TorrentInfo,
    ) -> TorrentResult<Arc<Mutex<TorrentHandle>>> {
        let mut dm = self
            .download_manager
            .lock()
            .map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "DownloadManager lock poisoned".to_string(),
            })?;
        dm.ensure_handle_lightweight(info)
    }

    /// Read a range of bytes from a specific file within a torrent.
    pub fn read_file_range(
        &self,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        let mut dm = self
            .download_manager
            .lock()
            .map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "DownloadManager lock poisoned".to_string(),
            })?;
        dm.read_file_range(info, file_index, offset, size)
    }

    /// Get session-level stats.
    pub fn get_session_stats(&self) -> TorrentResult<SessionStats> {
        let dm = self
            .download_manager
            .lock()
            .map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "DownloadManager lock poisoned".to_string(),
            })?;
        dm.get_session_stats()
    }

    /// Snapshot cached session stats — safe to call from FUSE handlers
    /// without blocking on the libtorrent session FFI.
    pub fn snapshot_stats(&self) -> SessionStats {
        self.cached_stats.snapshot()
    }

    /// Get a clone of the shared cached stats for the alert consumer.
    pub fn cached_stats(&self) -> SharedSessionStats {
        self.cached_stats.clone()
    }


    /// Get all torrent handles and their info hashes.
    pub fn get_all_handles(&self) -> Vec<(String, Arc<Mutex<TorrentHandle>>)> {
        self.download_manager
            .lock()
            .map(|dm| dm.get_all_handles())
            .unwrap_or_default()
    }

    /// Get the CacheManager shared with the download session.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        Some(self.cache_manager.clone())
    }


    /// Get piece status vector `PieceStatus` for all pieces of a torrent.
    /// Used by `.stats` to render the piece markers.
    ///
    /// Clones the PiecesManager Arc under a brief DM lock, then releases the DM lock
    /// before calling PiecesManager to avoid ABBA deadlock with read_file_range.
    pub fn get_pieces_status(
        &self,
        info_hash: &str,
        num_pieces: i32,
    ) -> TorrentResult<Vec<PieceStatus>> {
        let pm = {
            let dm = self
                .download_manager
                .lock()
                .map_err(|_| TorrentError::Unknown {
                    code: -1,
                    message: "DownloadManager lock poisoned".to_string(),
                })?;
            dm.pieces_manager_arc()
        };
        let pm_guard = pm
            .lock()
            .map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "PiecesManager lock poisoned".to_string(),
            })?;
        pm_guard.get_pieces_status(info_hash, num_pieces)
    }


    /// Check whether all piece files needed for a file range exist on disk.
    ///
    /// Uses the same criteria as `DownloadManager::is_piece_complete_in_cache`
    /// (TSI-2048: verified + size check), so the pre-check is consistent with
    /// the synchronous fast path in `read_file_range`.
    ///
    /// Uses only the CacheManager (no DownloadManager lock), so it's safe
    /// to call from the FUSE thread while a background download is running.
    pub fn pieces_on_disk(
        &self,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<bool> {
        let info_hash = hex::encode(info.info_hash()?);
        let piece_length = info.piece_length() as u64;
        let num_pieces = info.num_pieces() as i32;

        if num_pieces <= 0 || piece_length == 0 {
            return Ok(false);
        }

        let files = info.files()?;
        let file_start_offset: u64 = files
            .iter()
            .take(file_index as usize)
            .map(|f| f.size)
            .sum();
        let file_size = files
            .get(file_index as usize)
            .map(|f| f.size)
            .unwrap_or(0);

        let absolute_offset = file_start_offset + offset;
        let file_end = file_start_offset + file_size;
        if absolute_offset >= file_end || size == 0 {
            return Ok(true); // empty range, nothing to check
        }

        let size = std::cmp::min(size as u64, file_end - absolute_offset) as u32;
        let start_piece = (absolute_offset / piece_length) as i32;
        let end_offset = absolute_offset + size as u64;
        let end_piece = std::cmp::min(
            ((end_offset - 1) / piece_length) as i32,
            num_pieces - 1,
        );

        if start_piece > end_piece || start_piece >= num_pieces {
            return Ok(true);
        }

        let total_size = info.total_size();
        let cache = self.cache_manager.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Cache lock poisoned".to_string(),
        })?;

        for piece_idx in start_piece..=end_piece {
            let piece_key = format!("{}:piece:{}", info_hash, piece_idx);
            if !DownloadManager::is_piece_complete_in_cache(
                &cache,
                &piece_key,
                piece_idx,
                piece_length,
                num_pieces,
                total_size,
            ) {
                return Ok(false);
            }
        }

        Ok(true)
    }
    /// Spawn a background thread to download pieces for a file range.
    ///
    /// The DownloadManager lock is held only briefly (to get/create the
    /// torrent handle).  The piece-wait loop runs without the DM lock,
    /// using only the handle and CacheManager locks, so other FUSE
    /// operations (stats, cached reads) remain responsive.
    ///
    /// Returns immediately.  Data is written to the disk cache by
    /// libtorrent's custom storage.  FUSE reads should return EAGAIN
    /// and retry later.
    pub fn request_download_async(
        &self,
        torrent_data: Vec<u8>,
        file_index: i32,
        offset: u64,
        size: u32,
    ) {
        let dm = self.download_manager.clone();
        let cm = self.cache_manager.clone();

        std::thread::spawn(move || {
            // Parse torrent metadata
            let info = match TorrentInfo::from_bytes(torrent_data) {
                Ok(info) => info,
                Err(e) => {
                    tracing::warn!("request_download_async: failed to parse torrent: {:?}", e);
                    return;
                }
            };

            let info_hash = match info.info_hash() {
                Ok(h) => hex::encode(h),
                Err(e) => {
                    tracing::warn!("request_download_async: info_hash failed: {:?}", e);
                    return;
                }
            };
            let piece_length = info.piece_length() as u64;
            let num_pieces = info.num_pieces() as i32;
            let total_size = info.total_size();

            // Compute piece range
            let (start_piece, end_piece) = match Self::compute_piece_range(
                &info, file_index, offset, size,
            ) {
                Some(range) => range,
                None => return,
            };

            // Phase 1: get handle + apply selective priority (brief DM lock)
            // Phase 1: get handle + pieces_manager + apply selective priority
            let (handle, read_timeout, pm) = {
                let mut mgr = match dm.lock() {
                    Ok(mgr) => mgr,
                    Err(_) => {
                        tracing::error!("request_download_async: DM lock poisoned");
                        return;
                    }
                };
                let handle = match mgr.ensure_handle_lightweight(&info) {
                    Ok(h) => h,
                    Err(e) => {
                        tracing::warn!("request_download_async: ensure_handle_lightweight failed: {:?}", e);
                        return;
                    }
                };
                // Clone PiecesManager Arc for background thread use.
                let pm = mgr.pieces_manager_arc();
                // Apply selective piece priority while we hold the DM lock.
                if let Ok(h) = handle.lock() {
                    if let Err(e) = mgr.apply_read_priority(
                        &h, &info, file_index, offset, size,
                    ) {
                        tracing::warn!(
                            "request_download_async: apply_read_priority failed: {:?}",
                            e
                        );
                    }
                }
                (handle, mgr.read_timeout_secs, pm)
            }; // DM lock RELEASED here

            // Phase 2: wait for pieces (NO DM lock — only handle + cache + pieces_manager)
            DownloadManager::background_wait_for_pieces(
                &handle,
                &cm,
                &pm,
                &info_hash,
                start_piece,
                end_piece,
                piece_length,
                num_pieces,
                total_size,
                std::time::Duration::from_secs(read_timeout),
            );
        });
    }

    /// Compute the piece range covering a file read request.
    /// Returns `None` on invalid inputs (out-of-range file_index, etc.).
    fn compute_piece_range(
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> Option<(i32, i32)> {
        let piece_length = info.piece_length() as u64;
        let num_pieces = info.num_pieces() as i32;
        if num_pieces <= 0 || piece_length == 0 {
            return None;
        }

        let files = info.files().ok()?;
        let file_start_offset: u64 = files.iter().take(file_index as usize).map(|f| f.size).sum();
        let file_size = files.get(file_index as usize)?.size;

        let absolute_offset = file_start_offset + offset;
        let file_end = file_start_offset + file_size;
        if absolute_offset >= file_end || size == 0 {
            return None;
        }

        let size = std::cmp::min(size as u64, file_end - absolute_offset) as u32;
        let start_piece = (absolute_offset / piece_length) as i32;
        let end_offset = absolute_offset + size as u64;
        let end_piece =
            std::cmp::min(((end_offset - 1) / piece_length) as i32, num_pieces - 1);

        if start_piece > end_piece || start_piece >= num_pieces {
            return None;
        }

        Some((start_piece, end_piece))
    }

    /// Query torrent status for a given info_hash without triggering downloads.
    pub fn query_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        self.download_manager
            .lock()
            .ok()
            .and_then(|dm| dm.query_torrent_status(info_hash))
    }
}
