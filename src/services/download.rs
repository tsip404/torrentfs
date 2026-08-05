//! DownloadService — orchestrates download operations through DownloadManager.
//!
//! Wraps `Arc<Mutex<DownloadManager>>` and exposes a clean API
//! for the FUSE layer, hiding the lock management.

use std::path::Path;
use std::sync::{Arc, Mutex};

use tracing::warn;

use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::config::TorrentfsConfig;
use crate::infrastructure::download::{
    DownloadManager, SessionStats, TorrentHandle, TorrentStatus,
};
use crate::infrastructure::metadata::TorrentInfo;
use crate::seeding::SeedingManager;

/// DownloadService wraps DownloadManager behind a shared lock,
/// providing thread-safe access for the FUSE presentation layer.
pub struct DownloadService {
    download_manager: Arc<Mutex<DownloadManager>>,
}

impl DownloadService {
    /// Create a new DownloadService with the given cache directory and config.
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        let dm = DownloadManager::new(cache_dir, config)?;
        Ok(Self {
            download_manager: Arc::new(Mutex::new(dm)),
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

    /// Get all torrent handles and their info hashes.
    pub fn get_all_handles(&self) -> Vec<(String, Arc<Mutex<TorrentHandle>>)> {
        self.download_manager
            .lock()
            .map(|dm| dm.get_all_handles())
            .unwrap_or_else(|_| {
                warn!("DownloadManager lock poisoned in get_all_handles; returning empty list");
                Vec::new()
            })
    }

    /// Get the CacheManager shared with the download session.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        self.download_manager
            .lock()
            .ok()
            .map(|dm| dm.get_cache_manager())
    }

    /// Query torrent status for a given info_hash without triggering downloads.
    pub fn query_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        self.download_manager
            .lock()
            .ok()
            .and_then(|dm| dm.query_torrent_status(info_hash))
    }
}
