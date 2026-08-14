//! `PieceStore` — the piece data plane.
//!
//! Owns all access to the on-disk piece cache ([`CacheManager`]) and exposes
//! the data operations the download path needs: piece presence checks,
//! byte-range reads, and piece registration (metadata + verification).  It is
//! deliberately free of any *control* logic (priority, scheduling); that lives
//! in [`super::piece_scheduler::PieceScheduler`].

use std::sync::{Arc, Mutex};

use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::metadata::TorrentInfo;

#[derive(Clone)]
pub struct PieceStore {
    cache: Arc<Mutex<CacheManager>>,
}

impl PieceStore {
    pub fn new(cache: Arc<Mutex<CacheManager>>) -> Self {
        Self { cache }
    }

    /// The shared cache handle, for non-blocking `.stats` / on-disk checks.
    pub fn cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache.clone()
    }

    /// Canonical piece key: `"{info_hash}:piece:{index}"`.
    pub fn piece_key(info_hash: &str, piece_index: i32) -> String {
        format!("{}:piece:{}", info_hash, piece_index)
    }

    /// Whether the piece is registered in the cache metadata.
    pub fn has_piece(&self, info_hash: &str, piece_index: i32) -> bool {
        let key = Self::piece_key(info_hash, piece_index);
        self.cache
            .lock()
            .map(|c| c.has_piece(&key))
            .unwrap_or(false)
    }

    /// Whether a piece file physically exists on disk (independent of metadata).
    pub fn has_piece_on_disk(&self, piece_key: &str) -> bool {
        self.cache
            .lock()
            .map(|c| c.has_piece_on_disk(piece_key))
            .unwrap_or(false)
    }

    /// Read a whole piece from the on-disk cache, recording an access.
    pub fn read_piece(&self, piece_key: &str) -> TorrentResult<Vec<u8>> {
        let path = {
            let cache = self.cache.lock().map_err(|_| Self::poisoned())?;
            cache.piece_path(piece_key)
        };
        let data = std::fs::read(&path).map_err(|e| {
            TorrentError::IoError(format!(
                "Failed to read cached piece {} from {}: {}",
                piece_key,
                path.display(),
                e
            ))
        })?;
        if let Ok(mut cache) = self.cache.lock() {
            let _ = cache.record_access(piece_key);
        }
        Ok(data)
    }

    /// Read a byte range from a piece file without loading the whole piece.
    pub fn read_piece_range(
        &self,
        piece_key: &str,
        offset: u64,
        size: usize,
    ) -> TorrentResult<Vec<u8>> {
        let cache = self.cache.lock().map_err(|_| Self::poisoned())?;
        cache.read_piece_range(piece_key, offset, size)
    }

    /// Register a newly-downloaded piece in the cache: records its size in
    /// metadata and marks it verified.
    pub fn register_piece(&self, info_hash: &str, piece_index: i32, size: u64) -> TorrentResult<()> {
        let key = Self::piece_key(info_hash, piece_index);
        let mut cache = self.cache.lock().map_err(|_| Self::poisoned())?;
        cache.add_piece(&key, size)
    }

    fn poisoned() -> TorrentError {
        TorrentError::Unknown {
            code: -1,
            message: "Cache lock poisoned".to_string(),
        }
    }

    /// TSI-2048: whether a piece is genuinely complete in cache.  Both
    /// conditions must hold:
    /// 1. the piece was registered via `register_piece` (a successful
    ///    libtorrent download), not merely discovered by a startup scan;
    /// 2. the registered size meets the expected piece length.
    pub fn is_piece_complete_in_cache(
        cache: &CacheManager,
        piece_key: &str,
        piece_idx: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) -> bool {
        if !cache.is_piece_verified(piece_key) {
            return false;
        }
        if let Some(size) = cache.piece_metadata_size(piece_key) {
            let expected = if piece_idx == num_pieces - 1 {
                let remainder = total_size.saturating_sub(((num_pieces - 1) as u64) * piece_length);
                if remainder > 0 {
                    remainder
                } else {
                    piece_length
                }
            } else {
                piece_length
            };
            size >= expected
        } else {
            false
        }
    }

    /// Non-blocking pre-check used by the FUSE read path: are all pieces
    /// needed for a file range already on disk (verified + complete)?
    pub fn pieces_on_disk(
        cache: &CacheManager,
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
        let file_size = files.get(file_index as usize).map(|f| f.size).unwrap_or(0);

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
        for piece_idx in start_piece..=end_piece {
            let piece_key = Self::piece_key(&info_hash, piece_idx);
            if !Self::is_piece_complete_in_cache(
                cache,
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
}
