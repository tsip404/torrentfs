//! `PieceScheduler` — the piece control plane.
//!
//! Owns the per-torrent piece priority state and recomputes it from *events*:
//! `ReaderAdded` / `ReaderReleased` (reference-counted readers), `PieceReady`
//! (a piece finished downloading) and `PieceEvicted` (a cached piece was
//! evicted).  The download engine actor thread is the **single writer** of
//! this state, so it holds no lock and needs no `Send`.
//!
//! It deliberately holds no [`CacheManager`]; cached-piece presence is queried
//! through [`super::piece_store::PieceStore`] (read-only) when a gradient is
//! applied.

use std::collections::HashMap;

use crate::error::TorrentResult;

use crate::infrastructure::download::TorrentHandle;
use crate::infrastructure::metadata::TorrentInfo;

use super::piece_store::PieceStore;
use super::types::FilePieceInfo;

/// libtorrent `download_priority_t::default_priority` (4).  This is the
/// baseline priority a torrent uses while idle in upload_mode: pieces are
/// "wanted" (so the torrent stays connected and is not treated as Finished)
/// but upload_mode suppresses any piece requests.
const DEFAULT_PRIORITY: i32 = 4;

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

/// Status of a single piece for `.stats` rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceStatus {
    /// Current piece priority (0 = not wanted).
    pub priority: i32,
    /// Whether the piece is present in the disk cache (downloaded).
    pub is_cached: bool,
    /// Number of times the cached piece has been accessed.
    pub hit_count: u64,
}

/// An active reader: its precomputed priority gradient.  Reference-counted by
/// [`PieceScheduler::readers`].
#[derive(Debug, Clone)]
struct ReadRange {
    gradient: Vec<i32>,
}
/// Manages piece priority lifecycle across all active torrents.
pub struct PieceScheduler {
    /// Per-info_hash priority vector: `elevated[info_hash][piece_idx]` = priority.
    elevated: HashMap<String, Vec<i32>>,
    /// Per-info_hash piece length (bytes) for `.stats` rendering without the handle.
    piece_lengths: HashMap<String, i64>,
    /// Priority configuration.
    config: PiecePriorityConfig,
    /// Active readers per info_hash (reference counting).
    readers: HashMap<String, Vec<ReadRange>>,
}

impl PieceScheduler {
    pub fn new(config: PiecePriorityConfig) -> Self {
        Self {
            elevated: HashMap::new(),
            piece_lengths: HashMap::new(),
            config,
            readers: HashMap::new(),
        }
    }

    /// Initialize a torrent: records an all-zero priority vector and its
    /// piece length.
    pub fn init_torrent(
        &mut self,
        info_hash: &str,
        num_pieces: i32,
        piece_length: i64,
    ) -> TorrentResult<()> {
        if num_pieces <= 0 {
            return Ok(());
        }
        self.elevated.insert(
            info_hash.to_string(),
            vec![DEFAULT_PRIORITY; num_pieces as usize],
        );
        self.piece_lengths.insert(info_hash.to_string(), piece_length);
        Ok(())
    }

    // ── Priority events ───────────────────────────────────────────────

    /// `ReaderAdded` event: a reader started.  Records the read range and
    /// recomputes the priority gradient (union over all active readers).
    pub fn reader_added(
        &mut self,
        handle: &TorrentHandle,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
        store: &PieceStore,
    ) -> TorrentResult<()> {
        let info_hash = hex::encode(info.info_hash()?);
        let gradient = match self.gradient_for(handle, info, file_index, offset, size) {
            Some(g) => g,
            None => return Ok(()),
        };
        self.readers
            .entry(info_hash.clone())
            .or_default()
            .push(ReadRange { gradient });
        self.recompute(handle, &info_hash, store)
    }

    /// `ReaderReleased` event: a reader finished.  Decrements the reference
    /// count and recomputes (or resets to zero when no readers remain).
    pub fn reader_released(
        &mut self,
        handle: &TorrentHandle,
        info_hash: &str,
        store: &PieceStore,
    ) -> TorrentResult<()> {
        let empty = if let Some(ranges) = self.readers.get_mut(info_hash) {
            ranges.pop();
            ranges.is_empty()
        } else {
            true
        };
        if empty {
            self.reset_all(handle, info_hash);
            Ok(())
        } else {
            self.recompute(handle, info_hash, store)
        }
    }

    /// `PieceReady` event: a piece finished downloading.  Deprioritize it.
    pub fn piece_ready(&mut self, handle: &TorrentHandle, info_hash: &str, piece_index: i32) {
        if let Some(priorities) = self.elevated.get_mut(info_hash) {
            if piece_index >= 0 && (piece_index as usize) < priorities.len() {
                if priorities[piece_index as usize] != 0 {
                    priorities[piece_index as usize] = 0;
                    handle.set_piece_priority(piece_index, 0);
                }
            }
        }
    }

    /// `PieceEvicted` event: a cached piece was evicted.  Recompute the
    /// gradient so the piece is re-elevated if an active reader still wants it.
    pub fn piece_evicted(
        &mut self,
        handle: &TorrentHandle,
        info_hash: &str,
        store: &PieceStore,
    ) -> TorrentResult<()> {
        self.recompute(handle, info_hash, store)
    }
    // ── Status queries ────────────────────────────────────────────────

    /// The current priority vector for an info_hash.
    pub fn priorities(&self, info_hash: &str) -> Option<&[i32]> {
        self.elevated.get(info_hash).map(|v| v.as_slice())
    }

    /// Current priority for a single piece (0..7, 0 = not wanted).
    pub fn get_piece_priority(&self, info_hash: &str, piece_index: i32) -> i32 {
        self.priorities(info_hash)
            .and_then(|v| {
                if piece_index >= 0 && (piece_index as usize) < v.len() {
                    Some(v[piece_index as usize])
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    /// Number of pieces tracked for this info_hash.
    pub fn num_pieces(&self, info_hash: &str) -> Option<i32> {
        self.elevated.get(info_hash).map(|v| v.len() as i32)
    }

    /// Piece length (bytes) recorded at torrent initialization.
    pub fn piece_length(&self, info_hash: &str) -> Option<u64> {
        self.piece_lengths.get(info_hash).map(|&v| v as u64)
    }

    /// Indices of pieces currently elevated (priority > 0).
    pub fn elevated_pieces(&self, info_hash: &str) -> Vec<i32> {
        self.priorities(info_hash)
            .map(|v| {
                v.iter()
                    .enumerate()
                    .filter(|(_, &prio)| prio > 0)
                    .map(|(i, _)| i as i32)
                    .collect()
            })
            .unwrap_or_default()
    }

    // ── Internals ─────────────────────────────────────────────────────

    /// Reset every piece of a torrent back to the idle baseline priority.
    fn reset_all(&mut self, handle: &TorrentHandle, info_hash: &str) {
        if let Some(priorities) = self.elevated.get_mut(info_hash) {
            for (p, prio) in priorities.iter_mut().enumerate() {
                if *prio != DEFAULT_PRIORITY {
                    *prio = DEFAULT_PRIORITY;
                    handle.set_piece_priority(p as i32, DEFAULT_PRIORITY);
                }
            }
        }
    }

    /// Recompute the priority gradient as the element-wise maximum over all
    /// active readers' gradients, then apply it to the handle (skipping
    /// already-cached pieces).
    fn recompute(
        &mut self,
        handle: &TorrentHandle,
        info_hash: &str,
        store: &PieceStore,
    ) -> TorrentResult<()> {
        let num_pieces = self
            .elevated
            .get(info_hash)
            .map(|v| v.len() as i32)
            .unwrap_or(0);

        if num_pieces <= 0 {
            return Ok(());
        }

        let priorities = self
            .elevated
            .entry(info_hash.to_string())
            .or_insert_with(|| vec![0i32; num_pieces as usize]);

        // Union over active readers (element-wise max).
        let mut target = vec![0i32; num_pieces as usize];
        let mut any = false;
        if let Some(ranges) = self.readers.get(info_hash) {
            for range in ranges {
                any = true;
                for (i, &p) in range.gradient.iter().enumerate() {
                    if p > target[i] {
                        target[i] = p;
                    }
                }
            }
        }

        if !any {
            for (p, prio) in priorities.iter_mut().enumerate() {
                if *prio != DEFAULT_PRIORITY {
                    *prio = DEFAULT_PRIORITY;
                    handle.set_piece_priority(p as i32, DEFAULT_PRIORITY);
                }
            }
            return Ok(());
        }

        for (p, &prio) in target.iter().enumerate() {
            let piece_key = PieceStore::piece_key(info_hash, p as i32);
            let cached = store.has_piece(info_hash, p as i32) || store.has_piece_on_disk(&piece_key);
            let new_prio = if cached { 0 } else { prio };
            if priorities[p] != new_prio {
                priorities[p] = new_prio;
                handle.set_piece_priority(p as i32, new_prio);
            }
        }

        Ok(())
    }

    /// Compute the priority gradient for a single read range, as a full
    /// `Vec<i32>` of length `num_pieces` (zero outside the accessed file's
    /// piece range).  Returns `None` for an invalid / out-of-range read.
    fn gradient_for(
        &self,
        handle: &TorrentHandle,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> Option<Vec<i32>> {
        let piece_length = info.piece_length() as u64;
        let num_pieces = info.num_pieces() as i32;
        if num_pieces <= 0 || piece_length == 0 {
            return None;
        }

        let piece_info: FilePieceInfo = handle.get_file_piece_info(file_index).ok()?;
        let file_offset = piece_info.file_offset as u64;
        let p_file_start = piece_info.first_piece as i32;
        let p_file_end = p_file_start + piece_info.num_pieces as i32 - 1;

        if p_file_start >= num_pieces || p_file_end < 0 {
            return None;
        }

        let absolute_offset = file_offset + offset;
        let files = info.files().ok()?;
        let file_size = files.get(file_index as usize).map(|f| f.size).unwrap_or(0);
        let file_abs_end = file_offset + file_size;
        let clamped_size = if absolute_offset < file_abs_end {
            std::cmp::min(size as u64, file_abs_end - absolute_offset) as u32
        } else {
            return None; // past EOF
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

        let mut gradient = vec![0i32; num_pieces as usize];
        let p_start = std::cmp::max(0, p_file_start);
        let p_end = std::cmp::min(num_pieces - 1, p_file_end);

        for p in p_start..=p_end {
            let prio = if p < p_cur_start {
                self.config.backward_priority
            } else if p <= p_cur_end {
                self.config.current_priority
            } else {
                let dist = p - p_cur_end; // 1-based distance after current read end.
                let idx = (dist - 1) as usize;
                if idx < self.config.step_priorities.len() {
                    self.config.step_priorities[idx]
                } else if p <= prefetch_end {
                    3
                } else {
                    self.config.rest_priority
                }
            };
            gradient[p as usize] = prio;
        }

        Some(gradient)
    }
}
