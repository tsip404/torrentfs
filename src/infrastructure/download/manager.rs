use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::error::{TorrentError, TorrentResult};
use crate::seeding::SeedingManager;

use super::session::{Session, TorrentHandle};
use super::types::{SessionStats, TorrentState, TorrentStatus};

pub struct DownloadManager {
    pub(crate) session: Arc<Mutex<Session>>,
    pub(crate) handles: HashMap<String, Arc<Mutex<TorrentHandle>>>,
    pub(crate) cache_dir: String,
    pub(crate) cache_manager: Arc<Mutex<CacheManager>>,
    pub(crate) custom_storage_active: bool,
    pub(crate) read_timeout_secs: u64,
    pub(crate) seeding_manager: Option<Arc<SeedingManager>>,
}

impl DownloadManager {
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        let session = Session::new(config)?;
        let cache_dir_str = cache_dir.to_string_lossy().into_owned();

        let cache_manager = CacheManager::new(cache_dir, 1024 * 1024 * 1024)?;

        let read_timeout_secs = config
            .timeouts
            .read_timeout_secs
            .map(|v| if v > 0 { v as u64 } else { 30 })
            .unwrap_or(30);

        Ok(DownloadManager {
            session: Arc::new(Mutex::new(session)),
            handles: HashMap::new(),
            cache_dir: cache_dir_str,
            cache_manager: Arc::new(Mutex::new(cache_manager)),
            custom_storage_active: false,
            read_timeout_secs,
            seeding_manager: None,
        })
    }

    pub fn get_cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache_manager.clone()
    }

    /// Register a SeedingManager to receive eviction callbacks from the CacheManager.
    pub fn register_seeding_callback(&mut self, seeding: Arc<SeedingManager>) {
        let mut cache = self
            .cache_manager
            .lock()
            .expect("CacheManager lock poisoned");
        seeding.register_eviction_callback(&mut cache);
        self.seeding_manager = Some(seeding);
    }

    /// Get session-level stats.
    pub fn get_session_stats(&self) -> TorrentResult<SessionStats> {
        let session = self.session.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Session lock poisoned".to_string(),
        })?;
        session.get_stats()
    }

    /// Get all torrent handles and their info hashes.
    pub fn get_all_handles(&self) -> Vec<(String, Arc<Mutex<TorrentHandle>>)> {
        self.handles
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Ensure a lightweight handle exists for the given torrent info.
    pub fn ensure_handle_lightweight(
        &mut self,
        info: &crate::TorrentInfo,
    ) -> TorrentResult<Arc<Mutex<TorrentHandle>>> {
        let info_hash = hex::encode(info.info_hash()?);

        // If handle already exists, return it
        if let Some(handle) = self.handles.get(&info_hash) {
            return Ok(handle.clone());
        }

        // Create handle using upload_mode: connects to trackers/peers
        // but never requests pieces. Uses custom storage for the first
        // torrent to set up PieceStorageDiskIO.
        let cache_base = Path::new(&self.cache_dir);
        let pieces_dir = cache_base.join("pieces");
        std::fs::create_dir_all(&pieces_dir).map_err(|e| TorrentError::IoError(e.to_string()))?;

        let mut session = self.session.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Session lock poisoned".to_string(),
        })?;

        let handle = if !self.custom_storage_active {
            let h = session.add_torrent_with_custom_storage_upload_mode(info, cache_base)?;
            self.custom_storage_active = true;
            h
        } else {
            session.add_torrent_upload_mode(info, &pieces_dir)?
        };

        // Set all pieces to priority 0 to prevent automatic downloading.
        let (_piece_length, num_pieces) = handle.get_torrent_info()?;
        if num_pieces > 0 {
            unsafe {
                libtorrent_sys::lt_torrent_handle_set_all_piece_priorities(handle.inner, 0);
            }
        }
        let handle = Arc::new(Mutex::new(handle));
        self.handles.insert(info_hash.clone(), handle.clone());

        Ok(handle)
    }

    /// Query torrent status for a given info_hash without triggering downloads.
    pub fn query_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        self.handles
            .get(info_hash)
            .and_then(|h| h.lock().ok())
            .and_then(|guard| guard.status().ok())
    }

    pub fn get_or_create_handle(
        &mut self,
        info: &crate::TorrentInfo,
    ) -> TorrentResult<Arc<Mutex<TorrentHandle>>> {
        let info_hash = hex::encode(info.info_hash()?);

        if let Some(handle) = self.handles.get(&info_hash) {
            return Ok(handle.clone());
        }

        // Use cache/pieces/ as the piece storage directory.
        let cache_base = Path::new(&self.cache_dir);
        let pieces_dir = cache_base.join("pieces");
        std::fs::create_dir_all(&pieces_dir).map_err(|e| TorrentError::IoError(e.to_string()))?;

        let mut session = self.session.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Session lock poisoned".to_string(),
        })?;

        let handle = if !self.custom_storage_active {
            // First torrent: replace session with custom-storage session
            let h = session.add_torrent_with_custom_storage(info, cache_base)?;
            self.custom_storage_active = true;
            h
        } else {
            // Custom storage already active: use regular add_torrent with a
            // per-torrent save path to prevent cross-torrent interference.
            let torrent_save_dir = pieces_dir.join(&info_hash);
            std::fs::create_dir_all(&torrent_save_dir)
                .map_err(|e| TorrentError::IoError(e.to_string()))?;
            session.add_torrent(info, &torrent_save_dir)?
        };

        let handle = Arc::new(Mutex::new(handle));
        self.handles.insert(info_hash.clone(), handle.clone());

        Ok(handle)
    }

    fn make_piece_key(info_hash: &str, piece_idx: i32) -> String {
        format!("{}:piece:{}", info_hash, piece_idx)
    }

    pub fn read_file_range(
        &mut self,
        info: &crate::TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        let handle = self.get_or_create_handle(info)?;
        let handle_guard = handle.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Handle lock poisoned".to_string(),
        })?;

        if !handle_guard.is_valid() {
            return Err(TorrentError::InvalidFile(
                "Torrent handle is invalid".to_string(),
            ));
        }

        let mut status = handle_guard.status()?;
        tracing::debug!(
            "read_file_range: initial torrent state = {:?}, progress = {:.2}%",
            status.state,
            status.progress * 100.0
        );

        // Wait up to read_timeout_secs for CheckingFiles/re-verification to complete.
        let max_wait_secs = self.read_timeout_secs;
        let start = std::time::Instant::now();
        while matches!(
            status.state,
            TorrentState::QueuedForChecking
                | TorrentState::CheckingFiles
                | TorrentState::Allocating
                | TorrentState::CheckingResumeData
        ) {
            if start.elapsed().as_secs() > max_wait_secs {
                return Err(TorrentError::InvalidFile(format!(
                    "Torrent stuck in state {:?} for {} seconds",
                    status.state, max_wait_secs
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            status = handle_guard.status()?;
            tracing::debug!(
                "read_file_range: waiting for torrent state = {:?}, progress = {:.2}%",
                status.state,
                status.progress * 100.0
            );
        }

        tracing::debug!(
            "read_file_range: final torrent state = {:?}, progress = {:.2}%, peers = {}, seeds = {}",
            status.state,
            status.progress * 100.0,
            status.num_peers,
            status.num_seeds
        );

        std::thread::sleep(std::time::Duration::from_millis(100));

        let info_hash = handle_guard.info_hash().to_string();
        let piece_info = handle_guard.get_file_piece_info(file_index)?;
        let (handle_piece_length, handle_num_pieces) = handle_guard.get_torrent_info()?;
        let piece_length = handle_piece_length as u64;
        let num_pieces = handle_num_pieces as i32;

        let file_start_offset = piece_info.file_offset as u64;
        let absolute_offset = file_start_offset + offset;

        if num_pieces <= 0 {
            return Err(TorrentError::InvalidFile(format!(
                "Invalid torrent: num_pieces = {}",
                num_pieces
            )));
        }

        let start_piece = (absolute_offset / piece_length) as i32;
        let end_offset = absolute_offset + size as u64;
        let end_piece = if size > 0 {
            std::cmp::min(((end_offset - 1) / piece_length) as i32, num_pieces - 1)
        } else {
            start_piece
        };

        if start_piece >= num_pieces {
            return Err(TorrentError::InvalidFile(format!(
                "start_piece {} exceeds num_pieces {} (absolute_offset={}, piece_length={})",
                start_piece, num_pieces, absolute_offset, piece_length
            )));
        }

        if start_piece > end_piece {
            return Ok(Vec::new());
        }

        tracing::debug!(
            "read_file_range: file_index={}, offset={}, size={}, start_piece={}, end_piece={}, num_pieces={}, piece_length={}",
            file_index, offset, size, start_piece, end_piece, num_pieces, piece_length
        );

        // Peer discovery wait
        if status.num_peers == 0 && status.num_seeds == 0 {
            let peer_wait_start = std::time::Instant::now();
            let peer_wait_timeout =
                std::time::Duration::from_secs(std::cmp::min(self.read_timeout_secs, 10));
            let mut poll_count: u32 = 0;
            loop {
                if peer_wait_start.elapsed() >= peer_wait_timeout {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
                poll_count += 1;
                match handle_guard.status() {
                    Ok(s) => {
                        status = s;
                        if status.num_peers > 1 || status.num_seeds > 0 {
                            tracing::debug!(
                                "read_file_range: peers discovered after {:.1}s (peers={}, seeds={})",
                                peer_wait_start.elapsed().as_secs_f64(),
                                status.num_peers,
                                status.num_seeds
                            );
                            break;
                        }
                        if poll_count >= 3 {
                            let all_cached = {
                                let cache = self.cache_manager.lock().map_err(|_| {
                                    TorrentError::Unknown {
                                        code: -1,
                                        message: "Cache lock poisoned".to_string(),
                                    }
                                })?;
                                let mut all_found = true;
                                for piece_idx in start_piece..=end_piece {
                                    let piece_key = Self::make_piece_key(&info_hash, piece_idx);
                                    if !cache.has_piece(&piece_key)
                                        && !cache.has_piece_on_disk(&piece_key)
                                    {
                                        all_found = false;
                                        break;
                                    }
                                }
                                all_found
                            };
                            if !all_cached {
                                tracing::debug!(
                                    "read_file_range: still 0 peers after {} polls ({:.1}s), pieces not cached, failing fast",
                                    poll_count,
                                    peer_wait_start.elapsed().as_secs_f64()
                                );
                                break;
                            }
                            tracing::debug!(
                                "read_file_range: still 0 peers after {} polls ({:.1}s), but pieces are cached, continuing wait",
                                poll_count,
                                peer_wait_start.elapsed().as_secs_f64()
                            );
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        // Refresh status after peer_wait loop
        match handle_guard.status() {
            Ok(s) => status = s,
            Err(_) => {}
        }

        // Health check: if still no real peers
        if status.num_peers <= 1 && status.num_seeds == 0 {
            let all_cached = {
                let cache = self
                    .cache_manager
                    .lock()
                    .map_err(|_| TorrentError::Unknown {
                        code: -1,
                        message: "Cache lock poisoned".to_string(),
                    })?;
                let mut all_found = true;
                for piece_idx in start_piece..=end_piece {
                    let piece_key = Self::make_piece_key(&info_hash, piece_idx);
                    if !cache.has_piece(&piece_key) && !cache.has_piece_on_disk(&piece_key) {
                        all_found = false;
                        break;
                    }
                }
                all_found
            };
            if !all_cached {
                return Err(TorrentError::NoPeers(format!(
                    "Torrent has {} peers and {} seeds (progress: {:.2}%, state: {:?}). \
                     The tracker may be unreachable or the torrent has no active peers.",
                    status.num_peers,
                    status.num_seeds,
                    status.progress * 100.0,
                    status.state
                )));
            }
            tracing::debug!(
                "read_file_range: no peers but all needed pieces are cached on disk, proceeding"
            );
        }

        // Read-triggered piece prioritization
        for piece_idx in start_piece..=end_piece {
            if !handle_guard.have_piece(piece_idx) {
                tracing::debug!(
                    "read_file_range: setting piece deadline for piece {} (read-triggered prioritization)",
                    piece_idx
                );
                handle_guard.set_piece_deadline(piece_idx, 0);
            }
        }

        let piece_wait_timeout = std::time::Duration::from_secs(self.read_timeout_secs);
        for piece_idx in start_piece..=end_piece {
            if !handle_guard.have_piece(piece_idx) {
                let piece_key = Self::make_piece_key(&info_hash, piece_idx);

                // Fast path: check local disk cache BEFORE entering the download wait loop
                {
                    let cache = self
                        .cache_manager
                        .lock()
                        .map_err(|_| TorrentError::Unknown {
                            code: -1,
                            message: "Cache lock poisoned".to_string(),
                        })?;
                    if cache.has_piece(&piece_key) || cache.has_piece_on_disk(&piece_key) {
                        tracing::debug!(
                            "read_file_range: piece {} found in local disk cache (metadata={}, on_disk={}), skipping download wait",
                            piece_idx,
                            cache.has_piece(&piece_key),
                            cache.has_piece_on_disk(&piece_key)
                        );
                        continue;
                    }
                }

                // Check peers availability
                if let Ok(s) = handle_guard.status() {
                    status = s;
                }
                if status.num_peers <= 1 && status.num_seeds == 0 {
                    return Err(TorrentError::NoPeers(format!(
                        "No peers available and piece {} is not in cache. \
                         Torrent progress: {:.2}%, state: {:?}",
                        piece_idx,
                        status.progress * 100.0,
                        status.state
                    )));
                }

                tracing::debug!(
                    "read_file_range: piece {} not available, waiting for download...",
                    piece_idx
                );
                let piece_wait_start = std::time::Instant::now();
                let mut last_status_check = piece_wait_start;
                loop {
                    if piece_wait_start.elapsed() >= piece_wait_timeout {
                        status = handle_guard.status()?;
                        return Err(TorrentError::InvalidFile(format!(
                            "Timed out waiting for piece {} after {:.0}s. \
                             Torrent progress: {:.2}%, peers: {}, seeds: {}",
                            piece_idx,
                            piece_wait_timeout.as_secs(),
                            status.progress * 100.0,
                            status.num_peers,
                            status.num_seeds
                        )));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(200));
                    if handle_guard.have_piece(piece_idx) {
                        tracing::debug!(
                            "read_file_range: piece {} is now available after {:.1}s",
                            piece_idx,
                            piece_wait_start.elapsed().as_secs_f64()
                        );
                        break;
                    }
                    // Periodically refresh torrent status
                    if last_status_check.elapsed() >= std::time::Duration::from_secs(2) {
                        match handle_guard.status() {
                            Ok(s) => {
                                status = s;
                                last_status_check = std::time::Instant::now();
                                if status.num_peers == 0 && status.num_seeds == 0 {
                                    let grace_period = std::time::Duration::from_secs(
                                        std::cmp::min(self.read_timeout_secs, 10),
                                    );
                                    if piece_wait_start.elapsed() >= grace_period {
                                        return Err(TorrentError::NoPeers(format!(
                                            "Peers dropped to 0 while waiting for piece {}. \
                                             Torrent progress: {:.2}%, state: {:?}",
                                            piece_idx,
                                            status.progress * 100.0,
                                            status.state
                                        )));
                                    }
                                    tracing::debug!(
                                        "read_file_range: piece {} peers=0 but within grace period ({:.1}s elapsed of {:.1}s), continuing wait",
                                        piece_idx,
                                        piece_wait_start.elapsed().as_secs_f64(),
                                        grace_period.as_secs_f64()
                                    );
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    // Periodic cache re-check inside the wait loop
                    {
                        let cache =
                            self.cache_manager
                                .lock()
                                .map_err(|_| TorrentError::Unknown {
                                    code: -1,
                                    message: "Cache lock poisoned".to_string(),
                                })?;
                        if cache.has_piece(&piece_key) || cache.has_piece_on_disk(&piece_key) {
                            tracing::debug!(
                                "read_file_range: piece {} found in cache during wait (metadata={}, on_disk={}) after {:.1}s",
                                piece_idx,
                                cache.has_piece(&piece_key),
                                cache.has_piece_on_disk(&piece_key),
                                piece_wait_start.elapsed().as_secs_f64()
                            );
                            break;
                        }
                    }
                }
            }
        }

        let session = self.session.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Session lock poisoned".to_string(),
        })?;

        let mut result = Vec::with_capacity(size as usize);
        let mut bytes_read = 0usize;

        for piece_idx in start_piece..=end_piece {
            let piece_key = Self::make_piece_key(&info_hash, piece_idx);
            let piece_data = {
                let mut cache = self
                    .cache_manager
                    .lock()
                    .map_err(|_| TorrentError::Unknown {
                        code: -1,
                        message: "Cache lock poisoned".to_string(),
                    })?;

                if cache.has_piece(&piece_key) {
                    let piece_path = cache.piece_path(&piece_key);
                    if let Ok(data) = std::fs::read(&piece_path) {
                        if let Err(e) = cache.record_access(&piece_key) {
                            tracing::warn!(
                                "Failed to record cache access for {}: {:?}",
                                piece_key,
                                e
                            );
                        }
                        data
                    } else {
                        drop(cache);
                        let data = handle_guard.read_piece(&session, piece_idx)?;
                        let mut cache =
                            self.cache_manager
                                .lock()
                                .map_err(|_| TorrentError::Unknown {
                                    code: -1,
                                    message: "Cache lock poisoned".to_string(),
                                })?;
                        let piece_path = cache.ensure_piece_dir(&piece_key)?;
                        if let Err(e) = std::fs::write(&piece_path, &data) {
                            tracing::warn!("Failed to write cache piece {}: {:?}", piece_key, e);
                        }
                        if let Err(e) = cache.add_piece(&piece_key, data.len() as u64) {
                            tracing::warn!(
                                "Failed to add piece {} to cache metadata: {:?}",
                                piece_key,
                                e
                            );
                        }
                        data
                    }
                } else if cache.has_piece_on_disk(&piece_key) {
                    let piece_path = cache.piece_path(&piece_key);
                    let data = std::fs::read(&piece_path).map_err(|e| {
                        TorrentError::IoError(format!(
                            "Failed to read cached piece {} from disk: {}",
                            piece_key, e
                        ))
                    })?;
                    tracing::debug!(
                        "read_file_range: piece {} read from disk (not in metadata), size={}",
                        piece_idx,
                        data.len()
                    );
                    if let Err(e) = cache.add_piece(&piece_key, data.len() as u64) {
                        tracing::warn!(
                            "Failed to register on-disk piece {} in cache metadata: {:?}",
                            piece_key,
                            e
                        );
                    }
                    data
                } else {
                    drop(cache);
                    let data = handle_guard.read_piece(&session, piece_idx)?;
                    let mut cache =
                        self.cache_manager
                            .lock()
                            .map_err(|_| TorrentError::Unknown {
                                code: -1,
                                message: "Cache lock poisoned".to_string(),
                            })?;
                    let piece_path = cache.ensure_piece_dir(&piece_key)?;
                    if let Err(e) = std::fs::write(&piece_path, &data) {
                        tracing::warn!("Failed to write cache piece {}: {:?}", piece_key, e);
                    }
                    if let Err(e) = cache.add_piece(&piece_key, data.len() as u64) {
                        tracing::warn!(
                            "Failed to add piece {} to cache metadata: {:?}",
                            piece_key,
                            e
                        );
                    }
                    data
                }
            };

            // Notify SeedingManager that this piece is now available for seeding
            if let Some(ref seeding) = self.seeding_manager {
                if let Err(e) = seeding.mark_piece_available(&info_hash, piece_idx) {
                    tracing::warn!(
                        "Failed to mark piece {} available for info_hash={}: {:?}",
                        piece_idx,
                        info_hash,
                        e
                    );
                }
            }

            let piece_start = (piece_idx as u64) * piece_length;
            let piece_end = piece_start + piece_data.len() as u64;

            let read_start = std::cmp::max(absolute_offset, piece_start);
            let read_end = std::cmp::min(end_offset, piece_end);

            if read_start < read_end {
                let local_start = (read_start - piece_start) as usize;
                let local_end = (read_end - piece_start) as usize;

                let chunk = &piece_data[local_start..local_end];
                result.extend_from_slice(chunk);
                bytes_read += chunk.len();

                if bytes_read >= size as usize {
                    break;
                }
            }
        }

        Ok(result)
    }
}

unsafe impl Send for DownloadManager {}
