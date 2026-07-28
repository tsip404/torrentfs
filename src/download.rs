use std::collections::HashMap;
use std::ffi::CString;
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex};

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::error::{error_from_c, TorrentError, TorrentResult};
use crate::seeding::SeedingManager;

pub struct Session {
    inner: libtorrent_sys::lt_session_t,
}

pub struct TorrentHandle {
    inner: libtorrent_sys::lt_torrent_handle_t,
    info_hash: String,
    #[allow(dead_code)]
    session: libtorrent_sys::lt_session_t,
}

pub struct DownloadManager {
    session: Arc<Mutex<Session>>,
    handles: HashMap<String, Arc<Mutex<TorrentHandle>>>,
    cache_dir: String,
    cache_manager: Arc<Mutex<CacheManager>>,
    custom_storage_active: bool,
    read_timeout_secs: u64,
    seeding_manager: Option<Arc<SeedingManager>>,
}

impl Session {
    pub fn new(config: &TorrentfsConfig) -> TorrentResult<Self> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        // Create session with default settings (alert_mask only, no listen_interface)
        let inner = unsafe { libtorrent_sys::lt_session_create(ptr::null(), &mut error) };

        if inner.is_null() {
            return Err(unsafe { error_from_c(&error) });
        }

        let session = Session { inner };

        // Apply user configuration via JSON
        let settings_json = config.to_settings_json();
        if settings_json != "{}" {
            let json_c = CString::new(settings_json).unwrap_or_default();
            unsafe {
                libtorrent_sys::lt_session_apply_settings(session.inner, json_c.as_ptr());
            }
        }

        Ok(session)
    }

    pub fn add_torrent(
        &mut self,
        info: &crate::TorrentInfo,
        save_path: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let save_path_c = CString::new(save_path.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Save path contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent(
                self.inner,
                info.inner,
                save_path_c.as_ptr(),
                &mut error,
            )
        };

        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle {
                inner: handle,
                info_hash,
                session: self.inner,
            })
        }
    }

    pub fn add_torrent_with_custom_storage(
        &mut self,
        info: &crate::TorrentInfo,
        piece_cache_dir: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let piece_cache_dir_c = CString::new(piece_cache_dir.to_string_lossy().into_owned())
            .map_err(|_| {
                TorrentError::InvalidFile("Piece cache dir contains null byte".to_string())
            })?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent_with_custom_storage(
                self.inner,
                info.inner,
                piece_cache_dir_c.as_ptr(),
                &mut error,
            )
        };

        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle {
                inner: handle,
                info_hash,
                session: self.inner,
            })
        }
    }

    /// Add a torrent in upload_mode: connects to trackers and peers
    /// but never requests pieces. Use for lightweight status-only handles.
    pub fn add_torrent_upload_mode(
        &mut self,
        info: &crate::TorrentInfo,
        save_path: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let save_path_c = CString::new(save_path.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Save path contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent_upload_mode(
                self.inner,
                info.inner,
                save_path_c.as_ptr(),
                &mut error,
            )
        };

        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle {
                inner: handle,
                info_hash,
                session: self.inner,
            })
        }
    }

    /// Add a torrent with custom storage in upload_mode: replaces the session
    /// with custom PieceStorageDiskIO and adds the torrent without downloading.
    pub fn add_torrent_with_custom_storage_upload_mode(
        &mut self,
        info: &crate::TorrentInfo,
        piece_cache_dir: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let piece_cache_dir_c = CString::new(piece_cache_dir.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Piece cache dir contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent_with_custom_storage_upload_mode(
                self.inner,
                info.inner,
                piece_cache_dir_c.as_ptr(),
                &mut error,
            )
        };

        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle {
                inner: handle,
                info_hash,
                session: self.inner,
            })
        }
    }

    #[allow(dead_code)]
    pub fn remove_torrent(&mut self, handle: TorrentHandle, remove_files: bool) {
        unsafe {
            libtorrent_sys::lt_session_remove_torrent(
                self.inner,
                handle.inner,
                if remove_files { 1 } else { 0 },
            );
        }
    }

    fn inner(&self) -> libtorrent_sys::lt_session_t {
        self.inner
    }

    /// Get session-level statistics (rates, connections, DHT nodes).
    pub fn get_stats(&self) -> TorrentResult<SessionStats> {
        let mut stats = libtorrent_sys::lt_session_stats_t {
            download_rate: 0,
            upload_rate: 0,
            total_downloaded: 0,
            total_uploaded: 0,
            dht_nodes: 0,
            peers_connected: 0,
            half_open_connections: 0,
        };
        let mut status: i32 = -1;

        let result =
            unsafe { libtorrent_sys::lt_session_get_stats(self.inner, &mut stats, &mut status) };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get session stats".to_string(),
            })
        } else {
            Ok(SessionStats {
                download_rate: stats.download_rate,
                upload_rate: stats.upload_rate,
                total_downloaded: stats.total_downloaded,
                total_uploaded: stats.total_uploaded,
                dht_nodes: stats.dht_nodes,
                peers_connected: stats.peers_connected,
                half_open_connections: stats.half_open_connections,
            })
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        unsafe {
            libtorrent_sys::lt_session_destroy(self.inner);
        }
    }
}

unsafe impl Send for Session {}

impl TorrentHandle {
    pub fn is_valid(&self) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_is_valid(self.inner) != 0 }
    }

    pub fn status(&self) -> TorrentResult<TorrentStatus> {
        let mut state: i32 = 0;
        let mut progress: f32 = 0.0;
        let mut total_done: u64 = 0;
        let mut total: u64 = 0;
        let mut download_rate: i64 = 0;
        let mut upload_rate: i64 = 0;
        let mut total_download: i64 = 0;
        let mut total_upload: i64 = 0;
        let mut num_peers: i32 = 0;
        let mut num_seeds: i32 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_status(
                self.inner,
                &mut state,
                &mut progress,
                &mut total_done,
                &mut total,
                &mut download_rate,
                &mut upload_rate,
                &mut total_download,
                &mut total_upload,
                &mut num_peers,
                &mut num_seeds,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get torrent status".to_string(),
            })
        } else {
            Ok(TorrentStatus {
                state: TorrentState::from(state),
                progress,
                total_done,
                total,
                download_rate,
                upload_rate,
                total_download,
                total_upload,
                num_peers,
                num_seeds,
            })
        }
    }

    pub fn read_piece(&self, session: &Session, piece_index: i32) -> TorrentResult<Vec<u8>> {
        let mut data_out: *mut u8 = ptr::null_mut();
        let mut size_out: usize = 0;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_read_piece(
                session.inner(),
                self.inner,
                piece_index,
                &mut data_out,
                &mut size_out,
                &mut error,
            )
        };

        if result != 0 {
            Err(unsafe { error_from_c(&error) })
        } else if data_out.is_null() || size_out == 0 {
            Ok(Vec::new())
        } else {
            let slice = unsafe { std::slice::from_raw_parts(data_out, size_out) };
            let data = slice.to_vec();
            unsafe { libtorrent_sys::lt_piece_data_free(data_out) };
            Ok(data)
        }
    }

    pub fn get_file_piece_info(&self, file_index: i32) -> TorrentResult<FilePieceInfo> {
        let mut first_piece: i64 = 0;
        let mut num_pieces: i64 = 0;
        let mut file_offset: i64 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_get_piece_info(
                self.inner,
                file_index,
                &mut first_piece,
                &mut num_pieces,
                &mut file_offset,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get file piece info".to_string(),
            })
        } else {
            Ok(FilePieceInfo {
                first_piece,
                num_pieces,
                file_offset,
            })
        }
    }

    pub fn get_torrent_info(&self) -> TorrentResult<(i64, i64)> {
        let mut piece_length: i64 = 0;
        let mut num_pieces: i64 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_get_torrent_info(
                self.inner,
                &mut piece_length,
                &mut num_pieces,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get torrent info from handle".to_string(),
            })
        } else {
            Ok((piece_length, num_pieces))
        }
    }

    pub fn have_piece(&self, piece_index: i32) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_have_piece(self.inner, piece_index) != 0 }
    }

    /// Set a piece deadline to prioritize downloading this piece.
    /// deadline_ms is the number of milliseconds until the piece is needed.
    /// Higher priority pieces are requested before lower priority ones.
    pub fn set_piece_deadline(&self, piece_index: i32, deadline_ms: i32) -> bool {
        unsafe {
            libtorrent_sys::lt_torrent_handle_set_piece_deadline(
                self.inner,
                piece_index,
                deadline_ms,
            ) == 0
        }
    }

    /// Set piece priority for seeding/availability announcement.
    /// priority=0 means the piece will not be announced to peers.
    /// priority=7 is the default (highest) priority.
    pub fn set_piece_priority(&self, piece_index: i32, priority: i32) -> bool {
        unsafe {
            libtorrent_sys::lt_torrent_handle_set_piece_priority(self.inner, piece_index, priority)
                == 0
        }
    }

    pub fn info_hash(&self) -> &str {
        &self.info_hash
    }
}

impl Drop for TorrentHandle {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe {
                libtorrent_sys::lt_torrent_handle_destroy(self.inner);
            }
        }
    }
}

unsafe impl Send for TorrentHandle {}

#[derive(Debug, Clone)]
pub struct SessionStats {
    pub download_rate: i64,
    pub upload_rate: i64,
    pub total_downloaded: i64,
    pub total_uploaded: i64,
    pub dht_nodes: i32,
    pub peers_connected: i32,
    pub half_open_connections: i32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentStatus {
    pub state: TorrentState,
    pub progress: f32,
    pub total_done: u64,
    pub total: u64,
    pub download_rate: i64,
    pub upload_rate: i64,
    pub total_download: i64,
    pub total_upload: i64,
    pub num_peers: i32,
    pub num_seeds: i32,
}

#[derive(Debug, Clone, Copy)]
pub enum TorrentState {
    QueuedForChecking,
    CheckingFiles,
    DownloadingMetadata,
    Downloading,
    Finished,
    Seeding,
    Allocating,
    CheckingResumeData,
    Unknown,
}

impl From<i32> for TorrentState {
    fn from(value: i32) -> Self {
        match value {
            0 => TorrentState::QueuedForChecking,
            1 => TorrentState::CheckingFiles,
            2 => TorrentState::DownloadingMetadata,
            3 => TorrentState::Downloading,
            4 => TorrentState::Finished,
            5 => TorrentState::Seeding,
            6 => TorrentState::Allocating,
            7 => TorrentState::CheckingResumeData,
            _ => TorrentState::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FilePieceInfo {
    pub first_piece: i64,
    pub num_pieces: i64,
    pub file_offset: i64,
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
    /// When CacheManager evicts cached pieces, the affected infohash and piece_index will be sent to
    /// the SeedingManager so it can mark the piece as unavailable for seeding.
    /// Also stores the seeding_manager so DownloadManager can call mark_piece_available
    /// when a previously-evicted piece is re-downloaded.
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
    /// Creates a handle with `upload_mode` flag: the torrent connects to
    /// trackers and peers (so peer/seed info is available via status())
    /// but libtorrent will NEVER request pieces from peers.
    /// When `read_file_range()` later needs pieces, it calls
    /// `get_or_create_handle()` which returns the existing handle,
    /// and `set_piece_deadline()` will prioritize the needed pieces.
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
        // The torrent still connects to trackers and discovers peers,
        // making peer/seed info available via status().
        // Pieces needed by read_file_range() will be prioritized via
        // set_piece_deadline(), which overrides the 0-priority.
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
    /// Returns None if no handle exists for this info_hash.
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
        // Note: the C++ PieceStorageDiskIO creates a "pieces/" subdirectory
        // under the given path, so we pass the base cache_dir (not cache/pieces/).
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
            // Using a shared `pieces_dir` would cause libtorrent to scan ALL
            // cached pieces from ALL torrents, triggering unwanted download/seeding
            // of unrelated torrents (TSI-1969).  Each torrent gets its own
            // subdirectory: cache/pieces/<info_hash>/
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
        // Configurable via [timeouts] read_timeout_secs in config.toml.
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

        // Peer discovery wait: when there are 0 peers and 0 seeds after the
        // initial state check, the tracker may not have responded yet.
        // Wait a short period for peers to be discovered before giving up.
        // This is critical for acceptance testing with local trackers where
        // the seeder may have announced just before the downloader connects.
        //
        // TSI-1972: During the wait, also check whether the needed pieces are
        // already cached.  If they are, we can break early and serve from cache.
        // If they are NOT cached and we still have 0 peers after a few polls,
        // break early instead of waiting the full 10s — a tracker that hasn't
        // responded after 2-3 polls is unlikely to produce working peers in
        // the remaining time.
        if status.num_peers == 0 && status.num_seeds == 0 {
            let peer_wait_start = std::time::Instant::now();
            let peer_wait_timeout = std::time::Duration::from_secs(
                std::cmp::min(self.read_timeout_secs, 10)
            );
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
                        // TSI-1970: A single peer with 0 seeds is often a libtorrent
                        // self-reference (transient).  Require > 1 peers or
                        // at least 1 seed before breaking, so the fail-fast
                        // cache check below can run and the health check after
                        // the loop sees an accurate status.
                        if status.num_peers > 1 || status.num_seeds > 0 {
                            tracing::debug!(
                                "read_file_range: peers discovered after {:.1}s (peers={}, seeds={})",
                                peer_wait_start.elapsed().as_secs_f64(),
                                status.num_peers,
                                status.num_seeds
                            );
                            break;
                        }
                        // TSI-1972: Still 0 peers — check cache after the
                        // first few polls.  If pieces are cached we can serve
                        // from disk; if not, fail fast instead of waiting 10s.
                        if poll_count >= 3 {
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

        // TSI-1970: Refresh status after peer_wait loop.
        // The loop may have set status to a transient value (e.g. peers=1
        // from a libtorrent self-reference that disappeared right after the
        // break).  The health check below (and the piece-wait zero-peers
        // guard) need the current state, not a snapshot from a now-stale poll.
        match handle_guard.status() {
            Ok(s) => status = s,
            Err(_) => {} // keep whatever we have on error
        }

        // Health check: if still no real peers (0 or 1 peer with 0 seeds),
        // check whether all needed pieces are already cached on disk before
        // giving up.  A single peer with 0 seeds is often a libtorrent
        // self-reference (transient) — treat it as effectively no peers.
        // If pieces are cached, we can serve the read without any peers.
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

        // Read-triggered piece prioritization: set deadlines on the pieces
        // needed for this read so they are prioritized over rarest-first selection.
        // deadline_ms=0 means "as soon as possible" (highest priority).
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

                // ── Fast path: check local disk cache BEFORE entering the
                // download wait loop.  If the piece already exists on disk
                // (e.g. from a previous download, a previous run, or from
                // libtorrent custom storage), skip the network wait entirely
                // and proceed directly to reading from cache.  This is the
                // primary fix for TSI-1969: cached pieces should be served
                // from local disk without any network dependency.
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
                        continue; // skip wait, go to read section
                    }
                }
                // If we have no real peers (≤1 peer with 0 seeds) and the
                // piece is not cached, fail fast instead of waiting for
                // the full timeout.  A single peer with 0 seeds is often a
                // libtorrent self-reference — treat it as effectively no peers.
                // This prevents 30s hangs on uncached pieces with no peers.
                //
                // TSI-1971: Refresh status here since the peer_wait loop may
                // have set it to a transient non-zero value (libtorrent
                // self-reference peers=1).  A fresh status check catches peer
                // drops before entering the download wait.
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
                    // TSI-1971: Periodically refresh torrent status and
                    // re-check whether peers are still available.  A transient
                    // peer (peers=1 from libtorrent self-reference) can make
                    // the initial `status` snapshot show peers > 0 even when
                    // no real peers exist.  By the time we reach this loop the
                    // transient peer may have vanished.  Poll every ~2s to
                    // catch peer drops without adding too much overhead.
                    //
                    // TSI-1974: During the initial grace period (first 10s), do
                    // NOT abort on peers=0.  Seeders may not have announced yet
                    // during early connection establishment.  Only apply the
                    // fail-fast check after the grace period has elapsed.
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
                            Err(_) => {} // keep waiting, will be caught by timeout
                        }
                    }
                    // Periodic cache re-check inside the wait loop.
                    // A piece may be written to disk by libtorrent custom
                    // storage during the wait but not yet reflected in
                    // have_piece().  Check both in-memory metadata AND the
                    // filesystem.
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
                    // Piece exists on disk (e.g. from libtorrent custom storage
                    // or from a previous run) but is not registered in the
                    // in-memory metadata. Read it directly from the filesystem
                    // and register it so future reads hit the fast path.
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
                    // Register the piece in metadata so future reads hit the fast path
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

            // Notify SeedingManager that this piece is now available for seeding.
            // This restores the piece's priority to default (7) so libtorrent
            // will announce it to peers. Harmless if the piece was never evicted.
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
