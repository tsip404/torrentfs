//! PiecesManager — unified piece lifecycle management.
//!
//! Manages piece priority elevation/deprioritization and coordinates
//! with the disk cache.  All torrents join in upload_mode (priority=0)
//! so peer/seed information is visible without downloading.  Piece
//! priority is elevated selectively when a file range is read, using
//! a gradient algorithm that prioritises the current read range highest
//! and decays toward the file tail.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::TorrentResult;
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::download::TorrentHandle;
use crate::infrastructure::metadata::TorrentInfo;

/// Priority gradient configuration for selective piece download.
#[derive(Debug, Clone)]
pub struct PiecePriorityConfig {
    /// Prefetch window in MiB beyond the current read range (default 4).
    pub prefetch_window_mb: u32,
    /// Priority for pieces inside the current read range (default 7).
    pub current_priority: i32,
    /// Step priorities for pieces at distance 1, 2, 3 from current range end.
    pub step_priorities: [i32; 4],
    /// Priority for pieces beyond the prefetch window but still in the file.
    pub rest_priority: i32,
    /// Priority for pieces before the current read offset but still in the file.
    /// These may be re-accessed during random reads (default 1, lowest non-zero).
    pub backward_priority: i32,
}

impl Default for PiecePriorityConfig {
    fn default() -> Self {
        Self {
            prefetch_window_mb: 4,
            current_priority: 7,
            step_priorities: [6, 5, 4, 3],
            rest_priority: 2,
            backward_priority: 1,
        }
    }
}

// ── PiecesManager ─────────────────────────────────────────────────────────

/// Manages piece priority lifecycle across all active torrents.
///
/// Tracks elevated priorities per info_hash and coordinates with
/// CacheManager so that cached pieces are always deprioritized.
pub struct PiecesManager {
    /// Per-info_hash priority vector: `elevated[info_hash][piece_idx]` = current priority.
    pub(crate) elevated: HashMap<String, Vec<i32>>,
    /// Priority configuration.
    pub(crate) config: PiecePriorityConfig,
    /// Reference to the disk cache for piece-availability queries.
    pub(crate) cache_manager: Arc<Mutex<CacheManager>>,
}

impl PiecesManager {
    /// Create a new PiecesManager backed by the given CacheManager.
    pub fn new(
        cache_manager: Arc<Mutex<CacheManager>>,
        config: PiecePriorityConfig,
    ) -> Self {
        Self {
            elevated: HashMap::new(),
            config,
            cache_manager,
        }
    }

    // ── Initialization ───────────────────────────────────────────────

    /// Initialize a torrent handle in upload_mode: all pieces → priority 0.
    ///
    /// The handle must already have been created with
    /// `Session::add_torrent_upload_mode`.  This records an all-zero
    /// priority vector so subsequent calls know the base state.
    /// Record an all-zero priority vector for this info_hash.
    /// The caller (`ensure_handle_lightweight`) has already batched
    /// `lt_torrent_handle_set_all_piece_priorities(handle.inner, 0)`,
    /// so we only persist the tracking state here.
    pub fn init_upload_mode(
        &mut self,
        _handle: &TorrentHandle,
        info_hash: &str,
        num_pieces: i32,
    ) -> TorrentResult<()> {
        if num_pieces <= 0 {
            return Ok(());
        }
        let priorities = vec![0i32; num_pieces as usize];
        self.elevated.insert(info_hash.to_string(), priorities);
        Ok(())
    }

    // ── Priority elevation ───────────────────────────────────────────

    /// Apply the priority gradient for a read on `(file_index, offset, size)`.
    ///
    /// Only pieces belonging to the accessed file are elevated; all other
    /// files stay at priority 0.  Cached pieces are skipped (priority=0).
    ///
    /// The gradient decays from the current read range (priority 7)
    /// through step priorities toward the file tail (priority 2).
    pub fn apply_read_priority(
        &mut self,
        handle: &TorrentHandle,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<()> {
        let info_hash = hex::encode(info.info_hash()?);
        let piece_length = info.piece_length() as u64;
        let num_pieces = info.num_pieces() as i32;

        if num_pieces <= 0 || piece_length == 0 {
            return Ok(());
        }

        let piece_info = handle.get_file_piece_info(file_index)?;
        let file_offset = piece_info.file_offset as u64;
        let p_file_start = piece_info.first_piece as i32;
        let p_file_end = p_file_start + piece_info.num_pieces as i32 - 1;

        // Clamp file piece range.
        if p_file_start >= num_pieces || p_file_end < 0 {
            return Ok(());
        }

        // Current read range in absolute piece indices.
        let absolute_offset = file_offset + offset;
        let files = info.files()?;
        let file_size = files
            .get(file_index as usize)
            .map(|f| f.size)
            .unwrap_or(0);
        let file_abs_end = file_offset + file_size;
        let clamped_size = if absolute_offset < file_abs_end {
            std::cmp::min(size as u64, file_abs_end - absolute_offset) as u32
        } else {
            return Ok(()); // past EOF
        };

        let p_cur_start = (absolute_offset / piece_length) as i32;
        let p_cur_end = if clamped_size > 0 {
            ((absolute_offset + clamped_size as u64 - 1) / piece_length) as i32
        } else {
            p_cur_start
        };

        // Prefetch window in pieces.
        let prefetch_bytes = self.config.prefetch_window_mb as u64 * 1024 * 1024;
        let prefetch_pieces = if prefetch_bytes > 0 && piece_length > 0 {
            ((prefetch_bytes + piece_length - 1) / piece_length) as i32
        } else {
            0i32
        };
        let prefetch_end = std::cmp::min(p_file_end, p_cur_end.saturating_add(prefetch_pieces));

        // Ensure we have a priority vector for this info_hash.
        let cache = self
            .cache_manager
            .lock()
            .map_err(|_| crate::error::TorrentError::Unknown {
                code: -1,
                message: "Cache lock poisoned".to_string(),
            })?;

        let priorities = self
            .elevated
            .entry(info_hash.clone())
            .or_insert_with(|| vec![0i32; num_pieces as usize]);

        // Scan the accessed file's piece range and assign priorities.
        let p_start = std::cmp::max(0, p_file_start);
        let p_end = std::cmp::min(num_pieces - 1, p_file_end);

        for p in p_start..=p_end {
            // Check if piece is already cached — skip.
            let piece_key = format!("{}:piece:{}", info_hash, p);
            if cache.has_piece(&piece_key) {
                priorities[p as usize] = 0;
                handle.set_piece_priority(p, 0);
                continue;
            }

            // Before current read? → lowest non-zero (backward_priority).
            // These may be re-accessed during random reads.
            if p < p_cur_start {
                priorities[p as usize] = self.config.backward_priority;
                handle.set_piece_priority(p, self.config.backward_priority);
                continue;
            }

            // Inside current read range? → highest priority.
            if p <= p_cur_end {
                priorities[p as usize] = self.config.current_priority;
                handle.set_piece_priority(p, self.config.current_priority);
                continue;
            }

            // After current read — gradient.
            let dist = p - p_cur_end; // 1-based distance after current read end.
            let idx = (dist - 1) as usize;
            let prio = if idx < self.config.step_priorities.len() {
                self.config.step_priorities[idx]
            } else if p <= prefetch_end {
                3 // far prefetch — step_priorities[3] but may have been exhausted
            } else {
                self.config.rest_priority
            };

            priorities[p as usize] = prio;
            handle.set_piece_priority(p, prio);
        }

        drop(cache);
        Ok(())
    }

    /// Deprioritize all cached pieces for a given info_hash (set priority → 0).
    ///
    /// TODO: call when a torrent is fully cached / when resuming from disk.
    pub fn deprioritize_cached(
        &mut self,
        handle: &TorrentHandle,
        info_hash: &str,
    ) -> TorrentResult<()> {
        let cache = self
            .cache_manager
            .lock()
            .map_err(|_| crate::error::TorrentError::Unknown {
                code: -1,
                message: "Cache lock poisoned".to_string(),
            })?;

        if let Some(priorities) = self.elevated.get_mut(info_hash) {
            for (p, prio) in priorities.iter_mut().enumerate() {
                if *prio != 0 {
                    let piece_key = format!("{}:piece:{}", info_hash, p);
                    if cache.has_piece(&piece_key) {
                        *prio = 0;
                        handle.set_piece_priority(p as i32, 0);
                    }
                }
            }
        }

        Ok(())
    }

    // ── Reset ────────────────────────────────────────────────────────

    /// Reset all pieces for the given info_hash to priority 0.
    /// TODO: call when a torrent is removed from the session.
    pub fn reset_all(
        &mut self,
        handle: &TorrentHandle,
        info_hash: &str,
    ) {
        if let Some(priorities) = self.elevated.get_mut(info_hash) {
            for (p, prio) in priorities.iter_mut().enumerate() {
                if *prio != 0 {
                    *prio = 0;
                    handle.set_piece_priority(p as i32, 0);
                }
            }
        }
    }

    // ── Cache delegation ─────────────────────────────────────────────

    /// Check whether a piece exists in the disk cache.
    pub fn has_piece(&self, info_hash: &str, piece_index: i32) -> bool {
        let piece_key = format!("{}:piece:{}", info_hash, piece_index);
        self.cache_manager
            .lock()
            .map(|c| c.has_piece(&piece_key))
            .unwrap_or(false)
    }

    /// Read a byte range from a cached piece file.
    pub fn read_piece_range(
        &self,
        piece_key: &str,
        offset: u64,
        size: usize,
    ) -> TorrentResult<Vec<u8>> {
        let cache = self
            .cache_manager
            .lock()
            .map_err(|_| crate::error::TorrentError::Unknown {
                code: -1,
                message: "Cache lock poisoned".to_string(),
            })?;
        cache.read_piece_range(piece_key, offset, size)
    }

    /// Register a newly downloaded piece in the cache, then deprioritize it.
    pub fn add_piece(
        &mut self,
        handle: &TorrentHandle,
        info_hash: &str,
        piece_index: i32,
        size: u64,
    ) -> TorrentResult<()> {
        let piece_key = format!("{}:piece:{}", info_hash, piece_index);
        {
            let mut cache = self
                .cache_manager
                .lock()
                .map_err(|_| crate::error::TorrentError::Unknown {
                    code: -1,
                    message: "Cache lock poisoned".to_string(),
                })?;
            cache.add_piece(&piece_key, size)?;
        }
        // Deprioritize this piece now that it's cached.
        if let Some(priorities) = self.elevated.get_mut(info_hash) {
            if piece_index >= 0 && (piece_index as usize) < priorities.len() {
                priorities[piece_index as usize] = 0;
                handle.set_piece_priority(piece_index, 0);
            }
        }
        Ok(())
    }

    // ── Status queries ───────────────────────────────────────────────

    /// Get the current priority level for a single piece (0..7, 0 = not wanted).
    pub fn get_piece_priority(&self, info_hash: &str, piece_index: i32) -> i32 {
        self.elevated
            .get(info_hash)
            .and_then(|v| {
                if piece_index >= 0 && (piece_index as usize) < v.len() {
                    Some(v[piece_index as usize])
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    /// Get the full status vector `(priority, is_cached)` for all pieces.
    ///
    /// Used by `.stats` to render the `-- Pieces --` visualisation block.
    pub fn get_pieces_status(
        &self,
        info_hash: &str,
        num_pieces: i32,
    ) -> TorrentResult<Vec<(i32, bool)>> {
        let cache = self
            .cache_manager
            .lock()
            .map_err(|_| crate::error::TorrentError::Unknown {
                code: -1,
                message: "Cache lock poisoned".to_string(),
            })?;

        let priorities = self.elevated.get(info_hash);
        let mut result = Vec::with_capacity(num_pieces as usize);

        for p in 0..num_pieces {
            let piece_key = format!("{}:piece:{}", info_hash, p);
            let is_cached = cache.has_piece(&piece_key);
            let priority = priorities
                .and_then(|v| {
                    if (p as usize) < v.len() {
                        Some(v[p as usize])
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            result.push((priority, is_cached));
        }

        Ok(result)
    }

    /// Return a clone of the CacheManager Arc for direct access.
    pub fn get_cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache_manager.clone()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = PiecePriorityConfig::default();
        assert_eq!(cfg.prefetch_window_mb, 4);
        assert_eq!(cfg.current_priority, 7);
        assert_eq!(cfg.step_priorities, [6, 5, 4, 3]);
        assert_eq!(cfg.rest_priority, 2);
        assert_eq!(cfg.backward_priority, 1);
    }

    #[test]
    fn test_pieces_manager_new() {
        let tmp = std::env::temp_dir().join("pieces_mgr_test_new");
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CacheManager::new(&tmp, 1024 * 1024).unwrap();
        let cm = Arc::new(Mutex::new(cm));
        let pm = PiecesManager::new(cm, PiecePriorityConfig::default());
        assert!(pm.elevated.is_empty());
    }

    #[test]
    fn test_get_piece_priority_unknown_hash() {
        let tmp = std::env::temp_dir().join("pieces_mgr_test_prio");
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CacheManager::new(&tmp, 1024 * 1024).unwrap();
        let cm = Arc::new(Mutex::new(cm));
        let pm = PiecesManager::new(cm, PiecePriorityConfig::default());
        assert_eq!(pm.get_piece_priority("deadbeef", 0), 0);
        assert_eq!(pm.get_piece_priority("deadbeef", 42), 0);
    }

    #[test]
    fn test_get_pieces_status_empty() {
        let tmp = std::env::temp_dir().join("pieces_mgr_test_status");
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CacheManager::new(&tmp, 1024 * 1024).unwrap();
        let cm = Arc::new(Mutex::new(cm));
        let pm = PiecesManager::new(cm, PiecePriorityConfig::default());
        let status = pm.get_pieces_status("deadbeef", 4).unwrap();
        assert_eq!(status.len(), 4);
        for s in &status {
            assert_eq!(s.0, 0);
            assert!(!s.1);
        }
    }

    #[test]
    fn test_init_upload_mode_populates_elevated() {
        let tmp = std::env::temp_dir().join("pieces_mgr_test_init");
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CacheManager::new(&tmp, 1024 * 1024).unwrap();
        let cm = Arc::new(Mutex::new(cm));
        let mut pm = PiecesManager::new(cm, PiecePriorityConfig::default());

        // init_upload_mode needs a real TorrentHandle, but we can test
        // the elevated table directly by inserting manually.
        let info_hash = "abc123";
        pm.elevated.insert(
            info_hash.to_string(),
            vec![0i32; 8],
        );

        let status = pm.get_pieces_status(info_hash, 8).unwrap();
        assert_eq!(status.len(), 8);
        for s in &status {
            assert_eq!(s.0, 0);
        }
    }

    #[test]
    fn test_get_pieces_status_with_priorities() {
        let tmp = std::env::temp_dir().join("pieces_mgr_test_prios");
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CacheManager::new(&tmp, 1024 * 1024).unwrap();
        let cm = Arc::new(Mutex::new(cm));
        let mut pm = PiecesManager::new(cm, PiecePriorityConfig::default());

        let info_hash = "gradient_test";
        // Simulate a gradient: piece 0=7, piece 1=6, piece 2=5, piece 3=0
        pm.elevated.insert(
            info_hash.to_string(),
            vec![7, 6, 5, 0],
        );

        let status = pm.get_pieces_status(info_hash, 4).unwrap();
        assert_eq!(status[0], (7, false));
        assert_eq!(status[1], (6, false));
        assert_eq!(status[2], (5, false));
        assert_eq!(status[3], (0, false));
    }

    #[test]
    fn test_gradient_step_priorities_match_config() {
        let cfg = PiecePriorityConfig::default();
        // Step priorities should form a descending sequence.
        assert!(cfg.step_priorities[0] > cfg.step_priorities[1]);
        assert!(cfg.step_priorities[1] > cfg.step_priorities[2]);
        assert!(cfg.step_priorities[2] > cfg.step_priorities[3]);

        // current > first step
        assert!(cfg.current_priority > cfg.step_priorities[0]);

        // last step > rest
        assert!(cfg.step_priorities[3] > cfg.rest_priority);

        // rest > backward
        assert!(cfg.rest_priority > cfg.backward_priority);

        // backward > 0
        assert!(cfg.backward_priority > 0);
    }

    #[test]
    fn test_get_piece_priority_with_elevated() {
        let tmp = std::env::temp_dir().join("pieces_mgr_test_elev");
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CacheManager::new(&tmp, 1024 * 1024).unwrap();
        let cm = Arc::new(Mutex::new(cm));
        let mut pm = PiecesManager::new(cm, PiecePriorityConfig::default());

        let info_hash = "test_elev";
        pm.elevated.insert(
            info_hash.to_string(),
            vec![0, 7, 0, 3],
        );

        assert_eq!(pm.get_piece_priority(info_hash, 0), 0);
        assert_eq!(pm.get_piece_priority(info_hash, 1), 7);
        assert_eq!(pm.get_piece_priority(info_hash, 2), 0);
        assert_eq!(pm.get_piece_priority(info_hash, 3), 3);
        // Out of bounds returns 0.
        assert_eq!(pm.get_piece_priority(info_hash, 4), 0);
        assert_eq!(pm.get_piece_priority(info_hash, -1), 0);
    }

    #[test]
    fn test_prefetch_window_to_pieces() {
        let cfg = PiecePriorityConfig::default();
        // 4 MiB window with 256 KiB pieces = 16 pieces.
        // We test the math: ceil(4 * 1024 * 1024 / piece_length).
        let prefetch_bytes = cfg.prefetch_window_mb as u64 * 1024 * 1024;
        let piece_length: u64 = 262144; // 256 KiB
        let prefetch_pieces = (prefetch_bytes + piece_length - 1) / piece_length;
        assert_eq!(prefetch_pieces, 16);
    }
}
