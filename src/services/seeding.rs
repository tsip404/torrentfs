//! SeedingService — orchestrates seeding operations through SeedingManager.
//!
//! Wraps `Arc<SeedingManager>` and exposes a clean API
//! for the FUSE layer, hiding the internal command/thread management.

use std::path::Path;
use std::sync::Arc;

use crate::error::TorrentResult;
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::config::TorrentfsConfig;
use crate::infrastructure::metadata::TorrentInfo;
use crate::seeding::{SeedingInfo, SeedingManager};

/// SeedingService wraps SeedingManager behind an Arc,
/// providing thread-safe access for the FUSE presentation layer.
pub struct SeedingService {
    seeding_manager: Arc<SeedingManager>,
}

impl SeedingService {
    /// Create a new SeedingService with the given cache directory and config.
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        let sm = SeedingManager::new(cache_dir, config)?;
        Ok(Self {
            seeding_manager: Arc::new(sm),
        })
    }

    /// Access the underlying Arc<SeedingManager> for callback registration.
    pub fn get_seeding_manager(&self) -> Arc<SeedingManager> {
        self.seeding_manager.clone()
    }

    /// Register this service's SeedingManager as an eviction callback on
    /// the given CacheManager.
    pub fn register_eviction_callback(&self, cache: &mut CacheManager) {
        self.seeding_manager.register_eviction_callback(cache);
    }

    /// Add a torrent for seeding.
    pub fn add_seed(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        self.seeding_manager.add_seed(info)
    }

    /// Remove a torrent from seeding by info_hash.
    pub fn remove_seed(&self, info_hash: &str) -> TorrentResult<()> {
        self.seeding_manager.remove_seed(info_hash)
    }

    /// Get seeding info for a specific info_hash.
    pub fn get_seeding_info(&self, info_hash: &str) -> Option<SeedingInfo> {
        self.seeding_manager.get_seeding_info(info_hash)
    }

    /// Get all active seeding infos.
    pub fn get_all_seeds(&self) -> Vec<SeedingInfo> {
        self.seeding_manager.get_all_seeds()
    }
}
