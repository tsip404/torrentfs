//! DownloadService — orchestrates download operations through DownloadManager.
//!
//! Wraps `Arc<Mutex<DownloadManager>>` and exposes a clean API
//! for the FUSE layer, hiding the lock management.

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hex;

use crate::domain::pieces_manager::PieceStatus;
use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::alert::SharedSessionStats;
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::config::TorrentfsConfig;
use crate::infrastructure::download::{
    DownloadManager, SessionStats, TorrentHandle, TorrentStatus,
};
use crate::infrastructure::metadata::TorrentInfo;
use crate::infrastructure::metrics::Metrics;
use crate::seeding::SeedingManager;

pub struct DownloadService {
    download_manager: Arc<Mutex<DownloadManager>>,
    /// Cached reference to the CacheManager — avoids locking
    /// download_manager for fast disk-cache checks.
    cache_manager: Arc<Mutex<CacheManager>>,
    cached_stats: SharedSessionStats,
    metrics: Arc<Metrics>,
}
impl DownloadService {

    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        Self::new_with_metrics(cache_dir, config, Arc::new(Metrics::new()))
    }

    pub fn new_with_metrics(
        cache_dir: &Path,
        config: &TorrentfsConfig,
        metrics: Arc<Metrics>,
    ) -> TorrentResult<Self> {
        let dm = DownloadManager::new_with_metrics(cache_dir, config, metrics.clone())?;
        let cm = dm.get_cache_manager();
        Ok(Self {
            download_manager: Arc::new(Mutex::new(dm)),
            cache_manager: cm,
            cached_stats: SharedSessionStats::new(),
            metrics,
        })
    }

    /// Shared observability counters (TSI-2139).
    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

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
        let lock_start = Instant::now();
        let mut dm = self
            .download_manager
            .lock()
            .map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "DownloadManager lock poisoned".to_string(),
            })?;
        self.metrics.record_lock_wait(lock_start.elapsed());
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
    /// The read timeout (seconds) used to bound piece-download waits.
    pub fn read_timeout_secs(&self) -> u64 {
        self.download_manager
            .lock()
            .map(|dm| dm.read_timeout_secs)
            .unwrap_or(30)
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

    /// Non-blocking piece status for `.stats`: returns `(piece_length, statuses)`,
    /// or `None` when the DownloadManager / PiecesManager / cache locks are
    /// contended (an in-flight read is downloading).  `.stats` must never block
    /// on the download path (TSI-2119).
    pub fn try_get_pieces_status(&self, info_hash: &str) -> Option<(u64, Vec<PieceStatus>)> {
        let pm = {
            let dm = self.download_manager.try_lock().ok()?;
            dm.pieces_manager_arc()
        };
        let pm_guard = pm.try_lock().ok()?;
        let num_pieces = pm_guard.num_pieces(info_hash)?;
        let piece_length = pm_guard.piece_length(info_hash).unwrap_or(0);
        let statuses = pm_guard.get_pieces_status(info_hash, num_pieces).ok()?;
        Some((piece_length, statuses))
    }

    /// Non-blocking torrent status: returns `None` when the DownloadManager or
    /// handle lock is contended (download in progress), so `.stats` degrades to
    /// zeroes instead of blocking (TSI-2119).
    pub fn try_query_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        let handle = {
            let dm = self.download_manager.try_lock().ok()?;
            dm.handles.get(info_hash)?.clone()
        };
        let guard = handle.try_lock().ok()?;
        guard.status().ok()
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
    /// Blocking read of a file range, driving the piece download if needed.
    ///
    /// Runs WITHOUT holding the DownloadManager lock during the piece-wait
    /// (only handle + cache + PiecesManager locks), so `.stats` and other
    /// FUSE operations stay responsive while the read is blocked waiting for
    /// its pieces (TSI-2114 / TSI-2119 / TSI-2133).
    ///
    /// Blocks until the requested pieces are downloaded and the data is
    /// returned — no premature EIO on slow peers or transient disconnects.
    /// Call this from a dedicated worker thread, NOT the FUSE dispatch loop.
    pub fn read_file_range_blocking(
        &self,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
        stopping: &AtomicBool,
    ) -> TorrentResult<Vec<u8>> {
        let dm = self.download_manager.clone();
        let cm = self.cache_manager.clone();

        let info_hash = hex::encode(info.info_hash()?);
        let piece_length = info.piece_length() as u64;
        let num_pieces = info.num_pieces() as i32;
        let total_size = info.total_size();

        let (start_piece, end_piece) = Self::compute_piece_range(info, file_index, offset, size)
            .ok_or_else(|| TorrentError::InvalidFile(
                "read_file_range_blocking: invalid read range".to_string(),
            ))?;

        // Phase 1: ensure handle + elevate selective priority (brief DM lock).
        let (handle, pm) = {
            let lock_start = Instant::now();
            let mut mgr = dm.lock().map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "DownloadManager lock poisoned".to_string(),
            })?;
            self.metrics.record_lock_wait(lock_start.elapsed());
            let handle = mgr.ensure_handle_lightweight(info)?;
            let pm = mgr.pieces_manager_arc();
            {
                let h = handle.lock().map_err(|_| TorrentError::Unknown {
                    code: -1,
                    message: "Handle lock poisoned".to_string(),
                })?;
                if let Err(e) = mgr.apply_read_priority(&h, info, file_index, offset, size) {
                    tracing::warn!(
                        "read_file_range_blocking: apply_read_priority failed: {:?}",
                        e
                    );
                }
            }
            (handle, pm)
        }; // DM lock released here.
        // Phase 2: block until all needed pieces are on disk (no DM lock).
        // Returns false when shutdown interrupts the piece-wait; abort then so
        // the worker pool's `shutdown` join is not blocked by an in-flight read.
        let completed = DownloadManager::background_wait_for_pieces(
            &handle,
            &cm,
            &pm,
            &self.metrics,
            &info_hash,
            start_piece,
            end_piece,
            piece_length,
            num_pieces,
            total_size,
            stopping,
        );
        if !completed {
            return Err(TorrentError::Timeout(
                "shutdown requested, read aborted".to_string(),
            ));
        }
        // Phase 3: read the range (fast path — pieces are now on disk).
        let lock_start = Instant::now();
        let mut mgr = dm.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "DownloadManager lock poisoned".to_string(),
        })?;
        self.metrics.record_lock_wait(lock_start.elapsed());
        mgr.read_file_range(info, file_index, offset, size)
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
