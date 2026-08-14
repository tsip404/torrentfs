//! DownloadService — orchestrates download operations through the DownloadEngine.
//!
//! Wraps the [`DownloadEngine`] actor and exposes a clean API for the FUSE
//! layer.  There is no big `Arc<Mutex<DownloadManager>>` lock and no
//! `try_lock` hack: blocking reads go through the engine's command channel,
//! and non-blocking `.stats` reads use the engine's shared snapshot.

use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::config::TorrentfsConfig;
use crate::infrastructure::download::{
    DownloadEngine, PieceStatus, PieceStore, SessionStats, TorrentStatus,
};
use crate::infrastructure::metadata::TorrentInfo;
use crate::infrastructure::metrics::Metrics;
use crate::seeding::SeedingManager;

pub struct DownloadService {
    engine: Arc<DownloadEngine>,
    cache_manager: Arc<Mutex<CacheManager>>,
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
        let engine = DownloadEngine::new_with_metrics(cache_dir, config, metrics.clone())?;
        let cache_manager = engine.cache_manager();
        Ok(Self {
            engine: Arc::new(engine),
            cache_manager,
            metrics,
        })
    }

    /// Shared observability counters (TSI-2139).
    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn register_seeding_callback(&self, seeding: Arc<SeedingManager>) {
        self.engine.register_seeding(seeding);
    }

    /// Ensure a lightweight handle exists for the given torrent info.
    /// Uses upload_mode so it never requests pieces automatically.
    pub fn ensure_handle_lightweight(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        self.engine.ensure_handle(info)
    }

    /// Read a range of bytes from a specific file within a torrent.
    pub fn read_file_range(
        &self,
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        self.engine.read_file_range(info, file_index, offset, size)
    }

    /// Get session-level stats.
    pub fn get_session_stats(&self) -> TorrentResult<SessionStats> {
        self.engine.get_session_stats()
    }

    /// Snapshot cached session stats — safe to call from FUSE handlers
    /// without blocking on the libtorrent session FFI.
    pub fn snapshot_stats(&self) -> SessionStats {
        self.engine.snapshot_stats()
    }

    /// Get the CacheManager shared with the download session.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        Some(self.cache_manager.clone())
    }

    /// Non-blocking torrent status for `.stats`.
    pub fn try_query_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        self.engine.try_torrent_status(info_hash)
    }

    /// Non-blocking piece status for `.stats`: returns `(piece_length, statuses)`.
    pub fn try_get_pieces_status(&self, info_hash: &str) -> Option<(u64, Vec<PieceStatus>)> {
        self.engine.try_pieces_status(info_hash)
    }

    /// Check whether all piece files needed for a file range exist on disk.
    ///
    /// Uses only the CacheManager (no engine round-trip), so it's safe to call
    /// from the FUSE thread while a background download is running.
    pub fn pieces_on_disk(
        &self,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<bool> {
        let cache = self.cache_manager.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Cache lock poisoned".to_string(),
        })?;
        PieceStore::pieces_on_disk(&cache, info, file_index, offset, size)
    }

    /// Blocking read of a file range, driving the piece download if needed.
    ///
    /// The download happens on the engine thread (which owns the session and
    /// handles); this call blocks until the pieces are downloaded and the data
    /// is returned.  Call this from a dedicated worker thread, NOT the FUSE
    /// dispatch loop.
    pub fn read_file_range_blocking(
        &self,
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        self.engine.read_file_range(info, file_index, offset, size)
    }

    /// Stop the download engine (aborts in-flight reads and joins the thread).
    pub fn shutdown(&self) {
        self.engine.shutdown();
    }
}