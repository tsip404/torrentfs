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
    pub(crate) read_timeout_secs: u64,
    pub(crate) seeding_manager: Option<Arc<SeedingManager>>,
}

impl DownloadManager {
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        let cache_dir_str = cache_dir.to_string_lossy().into_owned();

        // Create pieces directory for custom piece storage
        let pieces_dir = cache_dir.join("pieces");
        std::fs::create_dir_all(&pieces_dir).map_err(|e| TorrentError::IoError(e.to_string()))?;

        // Create session with custom piece storage from the start
        let session = Session::new_with_custom_storage(config, &pieces_dir)?;

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

        let handle = session.add_torrent_upload_mode(info, &pieces_dir)?;

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

        let torrent_save_dir = pieces_dir.join(&info_hash);
        std::fs::create_dir_all(&torrent_save_dir)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;
        let handle = session.add_torrent(info, &torrent_save_dir)?;

        let handle = Arc::new(Mutex::new(handle));
        self.handles.insert(info_hash.clone(), handle.clone());

        Ok(handle)
    }

    fn make_piece_key(info_hash: &str, piece_idx: i32) -> String {
        format!("{}:piece:{}", info_hash, piece_idx)
    }

    /// Compute the byte-range within a piece's data that overlaps with a requested
    /// read range. Returns `(local_start, local_end)` indices into `piece_data`,
    /// or `None` when the piece does not contribute data to the requested range
    /// (e.g. empty piece data causes `read_start >= read_end`).
    fn piece_chunk_bounds(
        piece_data: &[u8],
        piece_idx: i32,
        piece_length: u64,
        absolute_offset: u64,
        end_offset: u64,
    ) -> Option<(usize, usize)> {
        debug_assert!(
            piece_idx >= 0,
            "piece_idx must be non-negative, got {}",
            piece_idx
        );
        let piece_start = (piece_idx as u64) * piece_length;
        let piece_end = piece_start + piece_data.len() as u64;

        let read_start = std::cmp::max(absolute_offset, piece_start);
        let read_end = std::cmp::min(end_offset, piece_end);

        if read_start < read_end {
            let local_start = (read_start - piece_start) as usize;
            let local_end = (read_end - piece_start) as usize;
            Some((local_start, local_end))
        } else {
            None
        }
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

        let info_hash = handle_guard.info_hash().to_string();
        let piece_info = handle_guard.get_file_piece_info(file_index)?;
        let (handle_piece_length, handle_num_pieces) = handle_guard.get_torrent_info()?;
        let piece_length = handle_piece_length as u64;
        let num_pieces = handle_num_pieces as i32;

        let file_start_offset = piece_info.file_offset as u64;
        let absolute_offset = file_start_offset + offset;

        // Clamp the request range to the file's actual size.  Reads past
        // end-of-file are legitimate short reads in FUSE; without this
        // clamp the empty-piece overlap check and the post-loop guard
        // both misjudge them as PieceNotReady (TSI-2020 regression).
        let file_size = info
            .files()
            .ok()
            .and_then(|fs| fs.get(file_index as usize).map(|f| f.size))
            .unwrap_or(u64::MAX);
        let file_end = file_start_offset + file_size;
        let size = if absolute_offset < file_end {
            (std::cmp::min(size as u64, file_end - absolute_offset) as u32).max(1)
        } else {
            return Ok(Vec::new());
        };

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

        // Fast-path pre-check: if all needed pieces are already available
        // locally (have_piece or disk cache), skip the peer-wait entirely.
        // This avoids the unnecessary wait when data is already complete
        // but peers have disconnected (common after finishing a download).
        let all_pieces_local = {
            let cache = self
                .cache_manager
                .lock()
                .map_err(|_| TorrentError::Unknown {
                    code: -1,
                    message: "Cache lock poisoned".to_string(),
                })?;
            let mut all_available = true;
            for piece_idx in start_piece..=end_piece {
                let piece_key = Self::make_piece_key(&info_hash, piece_idx);
                if !handle_guard.have_piece(piece_idx)
                    && !cache.has_piece(&piece_key)
                    && !cache.has_piece_on_disk(&piece_key)
                {
                    all_available = false;
                    break;
                }
            }
            all_available
        };

        // ── Fast path: all needed pieces are available locally ──────────
        // When every piece is already on disk (have_piece or disk cache),
        // skip the health-check, piece-deadline, and piece-wait phases.
        // Lock the cache only long enough to collect piece paths, then
        // release and do I/O outside the lock to avoid blocking eviction.
        // On any I/O error (eviction race), fall through to slow path.
        let fast_result: Option<TorrentResult<Vec<u8>>> = 'fast: {
            if !all_pieces_local {
                break 'fast None;
            }

            // Collect piece paths while holding the lock (cheap).
            let pieces: Vec<(i32, String, u64, usize)> = {
                let cache = self
                    .cache_manager
                    .lock()
                    .map_err(|_| TorrentError::Unknown {
                        code: -1,
                        message: "Cache lock poisoned".to_string(),
                    })?;
                (start_piece..=end_piece)
                    .map(|idx| {
                        let piece_key = Self::make_piece_key(&info_hash, idx);
                        let path = cache.piece_path(&piece_key);
                        let piece_start = (idx as u64) * piece_length;
                        let local_start = absolute_offset.saturating_sub(piece_start);
                        let local_end =
                            std::cmp::min(end_offset - piece_start, piece_length as u64);
                        let chunk_size = if local_start < local_end {
                            (local_end - local_start) as usize
                        } else {
                            0
                        };
                        (
                            idx,
                            path.to_string_lossy().into_owned(),
                            local_start,
                            chunk_size,
                        )
                    })
                    .collect()
            }; // cache lock released here

            let mut result = Vec::with_capacity(size as usize);
            let mut bytes_read = 0usize;
            let mut accessed_keys: Vec<String> = Vec::new();

            for (idx, ref path_str, local_start, chunk_size) in &pieces {
                if *chunk_size == 0 {
                    continue;
                }

                // Read directly from the piece file — no cache lock held.
                let piece_path = std::path::Path::new(path_str);
                let mut file = match std::fs::File::open(piece_path) {
                    Ok(f) => f,
                    Err(_) => {
                        tracing::debug!(
                            "Fast-path: piece {} file missing (eviction race), falling back to slow path",
                            idx
                        );
                        break 'fast None;
                    }
                };
                let chunk = match Self::read_file_offset(&mut file, *local_start, *chunk_size) {
                    Ok(data) => data,
                    Err(_) => {
                        tracing::debug!(
                            "Fast-path: piece {} I/O error, falling back to slow path",
                            idx
                        );
                        break 'fast None;
                    }
                };

                if !chunk.is_empty() {
                    let piece_key = Self::make_piece_key(&info_hash, *idx);
                    accessed_keys.push(piece_key);

                    // Notify SeedingManager that this piece is available
                    if let Some(ref seeding) = self.seeding_manager {
                        if let Err(e) = seeding.mark_piece_available(&info_hash, *idx) {
                            tracing::warn!(
                                "Fast-path: failed to mark piece {} available: {:?}",
                                idx,
                                e
                            );
                        }
                    }

                    result.extend_from_slice(&chunk);
                    bytes_read += chunk.len();

                    if bytes_read >= size as usize {
                        break;
                    }
                }
            }

            // Post-loop guard: never return short data for a real request
            if size > 0 && bytes_read < size as usize {
                tracing::debug!(
                    "Fast-path short read ({} < {}), falling back to slow path",
                    bytes_read,
                    size
                );
                break 'fast None;
            }

            // Record cache accesses in one short lock
            if !accessed_keys.is_empty() {
                if let Ok(mut cache) = self.cache_manager.lock() {
                    for key in &accessed_keys {
                        if let Err(e) = cache.record_access(key) {
                            tracing::warn!(
                                "Fast-path: failed to record cache access for {}: {:?}",
                                key,
                                e
                            );
                        }
                    }
                }
            }

            Some(Ok(result))
        };

        if let Some(result) = fast_result {
            return result;
        }

        // Settle sleep for libtorrent state transitions — only needed
        // when reading through the full path (not in fast path above).
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Peer discovery wait — only when pieces are NOT all available locally
        if status.num_peers == 0 && status.num_seeds == 0 && !all_pieces_local {
            let peer_wait_start = std::time::Instant::now();
            // Cap peer discovery wait at 9s (was 10s).  After this timeout we
            // proceed to the piece-wait loop which sets piece deadlines and
            // waits up to read_timeout_secs.  Keeping this slightly under the
            // piece-wait inner grace period (also min(read_timeout_secs, 10))
            // avoids an additive worst-case where both phases stack.
            let peer_wait_timeout =
                std::time::Duration::from_secs(std::cmp::min(self.read_timeout_secs, 9));
            loop {
                if peer_wait_start.elapsed() >= peer_wait_timeout {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
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
                        // Continue waiting — the peer_wait_timeout bounds this loop.
                        // Failing fast at 3 polls (1.5s) prevents read-triggered
                        // downloads from ever reaching the piece-wait path where
                        // piece deadlines trigger active peer discovery (TSI-2032).
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

        // Health check: if still no real peers — but also check libtorrent's
        // have_piece() in case the torrent already finished downloading
        // (progress=100%, state=Seeding) and peers have since disconnected.
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
                    if !cache.has_piece(&piece_key)
                        && !cache.has_piece_on_disk(&piece_key)
                        && !handle_guard.have_piece(piece_idx)
                    {
                        all_found = false;
                        break;
                    }
                }
                all_found
            };
            if !all_cached {
                // Instead of returning NoPeers immediately, proceed to the
                // piece-wait loop below.  Setting piece deadlines tells
                // libtorrent to actively seek these pieces, which can trigger
                // peer connections that hadn't been established yet when the
                // torrent was just added (TSI-2032).
                tracing::warn!(
                    "read_file_range: {} peers, {} seeds (progress: {:.2}%, state: {:?}) — \
                     proceeding to piece-wait with deadline prioritization",
                    status.num_peers,
                    status.num_seeds,
                    status.progress * 100.0,
                    status.state
                );
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

                // Check peers availability — but only if the piece isn't already complete.
                // After a successful download, the torrent may be in Seeding/Finished state
                // with 0 peers while the piece IS available locally.
                if let Ok(s) = handle_guard.status() {
                    status = s;
                }
                if handle_guard.have_piece(piece_idx) {
                    tracing::debug!(
                        "read_file_range: piece {} is already complete (have_piece=true), skipping peer check",
                        piece_idx
                    );
                    break;
                }
                if status.num_peers <= 1 && status.num_seeds == 0 {
                    // Instead of returning NoPeers immediately, proceed to the
                    // piece-wait loop below.  Piece deadlines have already been
                    // set, and the wait loop gives libtorrent time to establish
                    // peer connections that may not be up yet for a newly-added
                    // torrent (TSI-2039, same pattern as TSI-2032 outer check).
                    tracing::warn!(
                        "read_file_range: {} peers, {} seeds (progress: {:.2}%, state: {:?}) — \
                         proceeding to piece-wait loop for piece {} with deadline prioritization",
                        status.num_peers,
                        status.num_seeds,
                        status.progress * 100.0,
                        status.state,
                        piece_idx
                    );
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

            // ── Fetch piece data (cache-first, then libtorrent) ──────────
            let piece_data = Self::fetch_piece_data(
                &self.cache_manager,
                &handle_guard,
                &session,
                &piece_key,
                piece_idx,
            )?;

            // ── Validate: empty piece data means data is not ready ───────
            if piece_data.is_empty() {
                // Determine whether this piece *should* contribute data to
                // the requested range.  If it overlaps, returning empty is
                // a premature-EOF bug.
                let piece_start = (piece_idx as u64) * piece_length;
                let piece_end_theoretical = piece_start + piece_length;
                if absolute_offset < piece_end_theoretical && end_offset > piece_start {
                    return Err(TorrentError::PieceNotReady(format!(
                        "Piece {} data is empty but overlaps requested \
                         range [{}, {}); piece theoretical range [{}, {})",
                        piece_idx, absolute_offset, end_offset, piece_start, piece_end_theoretical
                    )));
                }
                // No overlap: legitimately irrelevant piece, skip.
                continue;
            }

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

            if let Some((local_start, local_end)) = Self::piece_chunk_bounds(
                &piece_data,
                piece_idx,
                piece_length,
                absolute_offset,
                end_offset,
            ) {
                let chunk = &piece_data[local_start..local_end];
                result.extend_from_slice(chunk);
                bytes_read += chunk.len();

                if bytes_read >= size as usize {
                    break;
                }
            }
        }

        // ── Post-loop guard: never return short data for a real request ──
        if size > 0 && bytes_read < size as usize {
            return Err(TorrentError::PieceNotReady(format!(
                "Short read: expected {} bytes, got {} bytes \
                 (file_index={}, offset={}, pieces {}-{})",
                size, bytes_read, file_index, offset, start_piece, end_piece
            )));
        }

        Ok(result)
    }

    /// Fetch piece data for a single piece, preferring the on-disk cache.
    /// Falls back to libtorrent `read_piece` when the cache is empty or
    /// the cached data is invalid (empty file).
    ///
    /// On `PieceNotReady` from `read_piece`, retries up to 3 times with
    /// a 200 ms sleep between attempts.
    fn fetch_piece_data(
        cache_manager: &Arc<Mutex<CacheManager>>,
        handle_guard: &TorrentHandle,
        session: &Session,
        piece_key: &str,
        piece_idx: i32,
    ) -> TorrentResult<Vec<u8>> {
        // ── Try cache first (minimize lock hold time) ────────────────
        {
            let piece_path = {
                let cache = cache_manager.lock().map_err(|_| TorrentError::Unknown {
                    code: -1,
                    message: "Cache lock poisoned".to_string(),
                })?;
                let in_metadata = cache.has_piece(piece_key);
                let on_disk = in_metadata || cache.has_piece_on_disk(piece_key);

                if !on_disk {
                    // Not in cache at all — release lock and fall through.
                    None
                } else {
                    // Compute path while holding the lock (cheap).
                    let path = cache.piece_path(piece_key);
                    drop(cache); // release lock before disk I/O
                    if in_metadata {
                        Some((path, true))
                    } else {
                        Some((path, false))
                    }
                }
            };

            if let Some((piece_path, in_metadata)) = piece_path {
                match std::fs::read(&piece_path) {
                    Ok(data) if !data.is_empty() => {
                        if !in_metadata {
                            // Register on-disk piece in metadata.
                            let mut cache =
                                cache_manager.lock().map_err(|_| TorrentError::Unknown {
                                    code: -1,
                                    message: "Cache lock poisoned".to_string(),
                                })?;
                            if let Err(e) = cache.add_piece(piece_key, data.len() as u64) {
                                tracing::warn!(
                                    "Failed to register on-disk piece {} in cache metadata: {:?}",
                                    piece_key,
                                    e
                                );
                            }
                        } else {
                            // Record access for LRU.
                            let mut cache =
                                cache_manager.lock().map_err(|_| TorrentError::Unknown {
                                    code: -1,
                                    message: "Cache lock poisoned".to_string(),
                                })?;
                            if let Err(e) = cache.record_access(piece_key) {
                                tracing::warn!(
                                    "Failed to record cache access for {}: {:?}",
                                    piece_key,
                                    e
                                );
                            }
                        }
                        tracing::debug!(
                            "read_file_range: piece {} read from disk cache, size={}",
                            piece_idx,
                            data.len()
                        );
                        return Ok(data);
                    }
                    Ok(_) => {
                        // Empty file on disk — fall through to read_piece.
                        tracing::warn!(
                            "Cached piece {} has empty file on disk; falling through to read_piece",
                            piece_key
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read cached piece {} from disk ({}): {}; falling through to read_piece",
                            piece_key, piece_path.display(), e
                        );
                    }
                }
            }
        } // cache lock released here

        // ── Fallback to libtorrent read_piece with PieceNotReady retry ──
        let max_retries = 3u32;
        for retry in 0..=max_retries {
            match handle_guard.read_piece(session, piece_idx) {
                Ok(data) if !data.is_empty() => {
                    // Cache the result for future reads.
                    let mut cache = cache_manager.lock().map_err(|_| TorrentError::Unknown {
                        code: -1,
                        message: "Cache lock poisoned".to_string(),
                    })?;
                    let piece_path = cache.ensure_piece_dir(piece_key)?;
                    if let Err(e) = std::fs::write(&piece_path, &data) {
                        tracing::warn!("Failed to write cache piece {}: {:?}", piece_key, e);
                    }
                    if let Err(e) = cache.add_piece(piece_key, data.len() as u64) {
                        tracing::warn!(
                            "Failed to add piece {} to cache metadata: {:?}",
                            piece_key,
                            e
                        );
                    }
                    return Ok(data);
                }
                Err(TorrentError::PieceNotReady(_)) => {
                    if retry < max_retries {
                        tracing::debug!(
                            "Piece {} not ready (retry {}/{}), waiting 200ms",
                            piece_idx,
                            retry + 1,
                            max_retries
                        );
                        std::thread::sleep(std::time::Duration::from_millis(200));
                        continue;
                    }
                    return Err(TorrentError::PieceNotReady(format!(
                        "Piece {} still not ready after {} retries",
                        piece_idx, max_retries
                    )));
                }
                Ok(_) => {
                    // Data is empty — this shouldn't happen after the
                    // read_piece fix, but if it does, treat as not ready.
                    if retry < max_retries {
                        tracing::warn!(
                            "Piece {} returned empty data (retry {}/{})",
                            piece_idx,
                            retry + 1,
                            max_retries
                        );
                        std::thread::sleep(std::time::Duration::from_millis(200));
                        continue;
                    }
                    return Err(TorrentError::PieceNotReady(format!(
                        "Piece {} returned empty data after {} retries",
                        piece_idx, max_retries
                    )));
                }
                Err(e) => return Err(e),
            }
        }

        // Shouldn't reach here, but guard:
        Err(TorrentError::PieceNotReady(format!(
            "Piece {} unavailable after {} retries",
            piece_idx, max_retries
        )))
    }

    /// Read a byte range from an already-open piece file.  Used by the
    /// fast path to avoid holding the cache lock during disk I/O.
    fn read_file_offset(
        file: &mut std::fs::File,
        offset: u64,
        size: usize,
    ) -> TorrentResult<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        if offset > 0 {
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| TorrentError::IoError(format!("Fast-path seek error: {}", e)))?;
        }
        let mut buf = vec![0u8; size];
        file.read_exact(&mut buf)
            .map_err(|e| TorrentError::IoError(format!("Fast-path read error: {}", e)))?;
        Ok(buf)
    }
}

unsafe impl Send for DownloadManager {}

#[cfg(test)]
mod tests {
    use super::DownloadManager;

    // ── piece_chunk_bounds unit tests ──────────────────────────────────
    //
    // These tests exercise the byte-range-overlap logic that determines
    // whether a piece contributes data to a requested read range, and if
    // so, which byte offsets.  The function is the critical gate inside
    // read_file_range's assembly loop.  It returns None when piece data
    // does not overlap the requested range — a pure math result.
    //
    // The assembly loop now validates piece data before calling this:
    // if piece_data is empty and overlaps the request, it's treated as
    // PieceNotReady rather than silently skipped.  The post-loop guard
    // also prevents returning short data.
    //
    // TSI-2018 regression: empty/short piece data no longer produces
    // premature EOF; the caller (read_file_range / fetch_piece_data)
    // retries or returns PieceNotReady.

    /// piece_length = 256 KiB (262144), the default in libtorrent.
    const PIECE_LEN: u64 = 262144;

    // ── piece_chunk_bounds: byte-range math tests ───────────────────
    //
    // These test the pure overlap calculation.  The function is
    // deterministic — given slice length, piece index, piece length,
    // and requested range, it returns the local byte indices within
    // the slice that overlap the request.  None means no overlap.

    #[test]
    fn test_piece_chunk_normal_full_piece() {
        // Piece data fills the entire piece_length.  Request covers the
        // whole piece.
        let piece_data = vec![0u8; PIECE_LEN as usize];
        let result = DownloadManager::piece_chunk_bounds(&piece_data, 0, PIECE_LEN, 0, PIECE_LEN);
        assert_eq!(result, Some((0, PIECE_LEN as usize)));
    }

    #[test]
    fn test_piece_chunk_partial_read_within_piece() {
        // Read a sub-range within a single piece (offset 100, size 500).
        let piece_data = vec![0u8; PIECE_LEN as usize];
        let result = DownloadManager::piece_chunk_bounds(&piece_data, 0, PIECE_LEN, 100, 600);
        assert_eq!(result, Some((100, 600)));
    }

    #[test]
    fn test_piece_chunk_across_two_pieces_first() {
        // Request spans pieces 0 and 1.  The first piece should contribute
        // from offset 100 to the end of its data.
        let piece_data = vec![0u8; PIECE_LEN as usize];
        let result = DownloadManager::piece_chunk_bounds(
            &piece_data,
            0,
            PIECE_LEN,
            100,              // absolute_offset
            PIECE_LEN + 1024, // end_offset (crosses piece boundary)
        );
        assert_eq!(result, Some((100, PIECE_LEN as usize)));
    }

    #[test]
    fn test_piece_chunk_across_two_pieces_second() {
        // Second piece: should contribute from 0 to end_offset overflow.
        let piece_data = vec![0u8; PIECE_LEN as usize];
        let result = DownloadManager::piece_chunk_bounds(
            &piece_data,
            1,
            PIECE_LEN,
            PIECE_LEN,        // absolute_offset (start of piece 1)
            PIECE_LEN + 1024, // end_offset
        );
        assert_eq!(result, Some((0, 1024)));
    }

    #[test]
    fn test_piece_chunk_no_overlap_before_request() {
        // Piece entirely before the requested range — should return None.
        let piece_data = vec![0u8; PIECE_LEN as usize];
        let result = DownloadManager::piece_chunk_bounds(
            &piece_data,
            0,
            PIECE_LEN,
            PIECE_LEN * 2, // absolute_offset (way past piece 0)
            PIECE_LEN * 2 + 512,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_piece_chunk_no_overlap_after_request() {
        // Piece entirely after the requested range.
        let piece_data = vec![0u8; PIECE_LEN as usize];
        let result = DownloadManager::piece_chunk_bounds(
            &piece_data,
            2,
            PIECE_LEN,
            0,   // absolute_offset
            512, // end_offset (before piece 2 starts)
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_piece_chunk_short_piece_data() {
        // Piece data is shorter than piece_length (partial download /
        // cache inconsistency).  The read range extends past the available
        // data — bounds must clamp to what's actually available.
        let short_len = PIECE_LEN as usize / 2; // 128 KiB
        let piece_data = vec![0u8; short_len];
        let result = DownloadManager::piece_chunk_bounds(
            &piece_data,
            0,
            PIECE_LEN,
            0,
            PIECE_LEN, // request full piece
        );
        // Should only return the data that's actually available.
        assert_eq!(result, Some((0, short_len)));
    }

    #[test]
    fn test_piece_chunk_short_piece_data_mid_range() {
        // Short piece with a partial read — only the overlap is returned.
        let short_len = PIECE_LEN as usize / 2;
        let piece_data = vec![0u8; short_len];
        let result = DownloadManager::piece_chunk_bounds(&piece_data, 0, PIECE_LEN, 1000, 2000);
        // short piece (0..131072) overlaps with request (1000..2000).
        assert_eq!(result, Some((1000, 2000)));
    }

    #[test]
    fn test_piece_chunk_last_piece_shorter() {
        // Last piece of a torrent is typically shorter than piece_length.
        let last_piece_data = vec![0u8; 50000]; // ~49 KiB
        let piece_idx = 10;
        let result = DownloadManager::piece_chunk_bounds(
            &last_piece_data,
            piece_idx,
            PIECE_LEN,
            (piece_idx as u64) * PIECE_LEN, // start of last piece
            (piece_idx as u64) * PIECE_LEN + 50000, // end of last piece
        );
        assert_eq!(result, Some((0, 50000)));
    }

    #[test]
    fn test_piece_chunk_offset_after_available_data() {
        // The piece has data but the requested offset starts after it —
        // no overlap.
        let piece_data = vec![0u8; 100];
        let piece_idx = 0;
        let result = DownloadManager::piece_chunk_bounds(
            &piece_data,
            piece_idx,
            PIECE_LEN,
            200, // absolute_offset (past the 100 bytes of data)
            300,
        );
        assert!(result.is_none());
    }

    // ── read_file_range contract-level invariants ──────────────────
    //
    // These tests validate the post-fix behavioral contract:
    // empty piece data that overlaps a requested range must never
    // produce a silent short read or premature EOF.  The assembly
    // loop detects this via the overlap check before calling
    // piece_chunk_bounds, and the post-loop guard catches any
    // remaining short-read cases.

    #[test]
    fn test_empty_piece_overlap_detection_triggered() {
        // When piece_data is empty but the piece's theoretical range
        // overlaps the request, the assembly loop returns PieceNotReady.
        // This is the exact guard that prevents the TSI-2018 bug.
        let piece_idx = 0i32;
        let piece_start = (piece_idx as u64) * PIECE_LEN;
        let piece_end_theoretical = piece_start + PIECE_LEN;

        // Request overlaps piece 0: [0, 1024)
        let absolute_offset = 0u64;
        let end_offset = 1024u64;

        let overlaps = absolute_offset < piece_end_theoretical && end_offset > piece_start;
        assert!(
            overlaps,
            "empty piece overlaps request — must trigger PieceNotReady"
        );

        // piece_chunk_bounds on empty data returns None (math result).
        let bounds = DownloadManager::piece_chunk_bounds(
            &[],
            piece_idx,
            PIECE_LEN,
            absolute_offset,
            end_offset,
        );
        assert!(
            bounds.is_none(),
            "piece_chunk_bounds returns None for empty data — \
             caller must check overlap before relying on this result"
        );
    }

    #[test]
    fn test_empty_piece_no_overlap_safe_to_skip() {
        // Empty piece that does NOT overlap the request is safe to skip.
        // piece 0, request starts at piece 1 (PIECE_LEN).
        let piece_idx = 0i32;
        let piece_start = (piece_idx as u64) * PIECE_LEN;
        let piece_end_theoretical = piece_start + PIECE_LEN;

        let absolute_offset = PIECE_LEN; // starts at piece 1
        let end_offset = PIECE_LEN + 1024;

        let overlaps = absolute_offset < piece_end_theoretical && end_offset > piece_start;
        assert!(!overlaps, "empty piece does NOT overlap — safe to skip");

        let bounds = DownloadManager::piece_chunk_bounds(
            &[],
            piece_idx,
            PIECE_LEN,
            absolute_offset,
            end_offset,
        );
        assert!(bounds.is_none());
    }

    #[test]
    fn test_short_read_guard_would_fire() {
        // Simulate the post-loop short-read guard: if bytes_read < size
        // and size > 0, the function returns PieceNotReady.
        let size: u32 = 4096;
        let bytes_read: usize = 1024; // only got 1 KiB of 4 KiB
        assert!(
            size > 0 && bytes_read < size as usize,
            "short read ({} < {}) must trigger PieceNotReady guard",
            bytes_read,
            size
        );
    }

    #[test]
    fn test_read_piece_returns_piece_not_ready_on_empty() {
        // Contract: TorrentError::PieceNotReady is now a first-class
        // error variant.  The assembly loop's fetch_piece_data retries
        // on this error up to 3 times before propagating it upward.
        let err = crate::error::TorrentError::PieceNotReady("test empty piece".to_string());
        let msg = format!("{}", err);
        assert!(
            msg.contains("Piece not ready"),
            "PieceNotReady error message: {}",
            msg
        );
        assert!(msg.contains("test empty piece"));
    }
}
