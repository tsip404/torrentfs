use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::download::{Session, TorrentHandle, TorrentState};
use crate::error::{TorrentError, TorrentResult};

pub struct SeedingManager {
    session: Arc<Mutex<Session>>,
    handles: Arc<Mutex<HashMap<String, TorrentHandle>>>,
    seeding_info: Arc<Mutex<HashMap<String, SeedingInfo>>>,
    cache_dir: PathBuf,
    custom_storage_active: Arc<Mutex<bool>>,
}

#[derive(Debug, Clone)]
pub struct SeedingInfo {
    pub info_hash: String,
    pub name: String,
    pub total_size: u64,
    pub uploaded: u64,
    pub state: SeedingState,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SeedingState {
    Checking,
    Seeding,
    Queued,
    Paused,
    Error,
}

impl SeedingManager {
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        let session = Session::new(config)?;

        Ok(Self {
            session: Arc::new(Mutex::new(session)),
            handles: Arc::new(Mutex::new(HashMap::new())),
            seeding_info: Arc::new(Mutex::new(HashMap::new())),
            cache_dir: cache_dir.to_path_buf(),
            custom_storage_active: Arc::new(Mutex::new(false)),
        })
    }

    pub fn add_seed(&self, info: &crate::TorrentInfo) -> TorrentResult<()> {
        let info_hash = hex::encode(info.info_hash()?);

        {
            let handles = self.handles.lock().map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "Handles lock poisoned".to_string(),
            })?;

            if handles.contains_key(&info_hash) {
                return Ok(());
            }
        }

        // Use cache/pieces/ as the piece storage directory.
        // Note: the C++ PieceStorageDiskIO creates a "pieces/" subdirectory
        // under the given path, so we pass the base cache_dir (not cache/pieces/).
        let pieces_dir = self.cache_dir.join("pieces");
        if !pieces_dir.exists() {
            std::fs::create_dir_all(&pieces_dir)
                .map_err(|e| TorrentError::IoError(e.to_string()))?;
        }

        let handle = {
            let mut session = self.session.lock().map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "Session lock poisoned".to_string(),
            })?;

            let custom_active = {
                let flag = self.custom_storage_active.lock().map_err(|_| {
                    TorrentError::Unknown {
                        code: -1,
                        message: "Custom storage flag lock poisoned".to_string(),
                    }
                })?;
                *flag
            };

            if !custom_active {
                // First seed: replace session with custom-storage session
                let h = session.add_torrent_with_custom_storage(info, &self.cache_dir)?;
                let mut flag = self.custom_storage_active.lock().map_err(|_| {
                    TorrentError::Unknown {
                        code: -1,
                        message: "Custom storage flag lock poisoned".to_string(),
                    }
                })?;
                *flag = true;
                h
            } else {
                // Custom storage already active: use regular add_torrent
                session.add_torrent(info, &pieces_dir)?
            }
        };

        let name = info.name();
        let total_size = info.total_size();

        {
            let mut handles = self.handles.lock().map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "Handles lock poisoned".to_string(),
            })?;

            handles.insert(info_hash.clone(), handle);
        }

        {
            let mut info_map = self
                .seeding_info
                .lock()
                .map_err(|_| TorrentError::Unknown {
                    code: -1,
                    message: "Seeding info lock poisoned".to_string(),
                })?;

            info_map.insert(
                info_hash.clone(),
                SeedingInfo {
                    info_hash: info_hash.clone(),
                    name,
                    total_size,
                    uploaded: 0,
                    state: SeedingState::Checking,
                },
            );
        }

        Ok(())
    }

    pub fn remove_seed(&self, info_hash: &str) -> TorrentResult<()> {
        let handle = {
            let mut handles = self.handles.lock().map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "Handles lock poisoned".to_string(),
            })?;
            handles.remove(info_hash)
        };

        if let Some(handle) = handle {
            let mut session = self.session.lock().map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "Session lock poisoned".to_string(),
            })?;
            session.remove_torrent(handle, false);
        }

        {
            let mut info_map = self
                .seeding_info
                .lock()
                .map_err(|_| TorrentError::Unknown {
                    code: -1,
                    message: "Seeding info lock poisoned".to_string(),
                })?;
            info_map.remove(info_hash);
        }

        Ok(())
    }

    /// Handle piece eviction notification from CacheManager.
    /// Marks the evicted piece as unavailable for seeding (priority 0)
    /// instead of removing the entire torrent from seeding.
    pub fn handle_eviction(&self, info_hash: &str, piece_index: i32) {
        tracing::info!(
            "Eviction-triggered: marking piece {} unavailable for seeding info_hash={}",
            piece_index,
            info_hash
        );
        match self.mark_piece_unavailable(info_hash, piece_index) {
            Ok(()) => {
                tracing::info!(
                    "Successfully marked piece {} unavailable for info_hash={}",
                    piece_index,
                    info_hash
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to mark piece {} unavailable for info_hash={}: {:?}",
                    piece_index,
                    info_hash,
                    e
                );
            }
        }
    }

    /// Mark a piece as unavailable for seeding by setting its priority to 0.
    /// When priority is 0, libtorrent will not announce this piece to peers.
    pub fn mark_piece_unavailable(&self, info_hash: &str, piece_index: i32) -> TorrentResult<()> {
        let handles = self.handles.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Handles lock poisoned".to_string(),
        })?;

        if let Some(handle) = handles.get(info_hash) {
            if !handle.set_piece_priority(piece_index, 0) {
                tracing::warn!(
                    "set_piece_priority(0) returned false for info_hash={}, piece_index={}",
                    info_hash,
                    piece_index
                );
            }
        } else {
            tracing::debug!(
                "No seeding handle found for info_hash={}, skipping mark_piece_unavailable",
                info_hash
            );
        }

        Ok(())
    }

    /// Mark a piece as available for seeding by restoring its priority to default (7).
    /// Called after a previously-evicted piece has been re-downloaded by DownloadManager.
    pub fn mark_piece_available(&self, info_hash: &str, piece_index: i32) -> TorrentResult<()> {
        let handles = self.handles.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Handles lock poisoned".to_string(),
        })?;

        if let Some(handle) = handles.get(info_hash) {
            if !handle.set_piece_priority(piece_index, 7) {
                tracing::warn!(
                    "set_piece_priority(7) returned false for info_hash={}, piece_index={}",
                    info_hash,
                    piece_index
                );
            }
        } else {
            tracing::debug!(
                "No seeding handle found for info_hash={}, skipping mark_piece_available",
                info_hash
            );
        }

        Ok(())
    }

    /// Register this SeedingManager as an eviction callback on the given CacheManager.
    /// When CacheManager evicts pieces, handle_eviction will be called with info_hash and piece_index.
    pub fn register_eviction_callback(self: &Arc<Self>, cache: &mut CacheManager) {
        let this = Arc::clone(self);
        cache.on_evict(Box::new(move |info_hash: String, piece_index: i32| {
            this.handle_eviction(&info_hash, piece_index);
        }));
    }

    pub fn update_seeding_status(&self) -> TorrentResult<Vec<SeedingInfo>> {
        let handles = self.handles.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Handles lock poisoned".to_string(),
        })?;

        let mut info_map = self
            .seeding_info
            .lock()
            .map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "Seeding info lock poisoned".to_string(),
            })?;

        for (info_hash, handle) in handles.iter() {
            if let Ok(status) = handle.status() {
                if let Some(info) = info_map.get_mut(info_hash) {
                    info.uploaded = status.total_done;

                    info.state = match status.state {
                        TorrentState::Seeding => SeedingState::Seeding,
                        TorrentState::Finished => SeedingState::Seeding,
                        TorrentState::CheckingFiles
                        | TorrentState::CheckingResumeData
                        | TorrentState::QueuedForChecking => SeedingState::Checking,
                        TorrentState::Downloading | TorrentState::DownloadingMetadata => {
                            SeedingState::Checking
                        }
                        TorrentState::Allocating => SeedingState::Checking,
                        TorrentState::Unknown => SeedingState::Error,
                    };
                }
            }
        }

        Ok(info_map.values().cloned().collect())
    }

    pub fn get_seeding_info(&self, info_hash: &str) -> Option<SeedingInfo> {
        if let Err(e) = self.update_seeding_status() {
            eprintln!("[WARN] Failed to update seeding status: {}", e);
        }
        let info_map = self.seeding_info.lock().ok()?;
        info_map.get(info_hash).cloned()
    }

    pub fn has_handle(&self, info_hash: &str) -> bool {
        self.handles
            .lock()
            .ok()
            .map(|h| h.contains_key(info_hash))
            .unwrap_or(false)
    }

    pub fn is_seeding(&self, info_hash: &str) -> bool {
        if let Err(e) = self.update_seeding_status() {
            eprintln!("[WARN] Failed to update seeding status: {}", e);
        }
        let info_map = match self.seeding_info.lock().ok() {
            Some(m) => m,
            None => return false,
        };
        match info_map.get(info_hash) {
            Some(info) => info.state == SeedingState::Seeding,
            None => false,
        }
    }

    pub fn get_all_seeds(&self) -> Vec<SeedingInfo> {
        if let Err(e) = self.update_seeding_status() {
            eprintln!("[WARN] Failed to update seeding status: {}", e);
        }
        let info_map = self.seeding_info.lock().ok();
        info_map
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_total_uploaded(&self) -> u64 {
        if let Err(e) = self.update_seeding_status() {
            eprintln!("[WARN] Failed to update seeding status: {}", e);
        }
        let info_map = self.seeding_info.lock().ok();
        info_map
            .map(|m| m.values().map(|s| s.uploaded).sum())
            .unwrap_or(0)
    }
}

unsafe impl Send for SeedingManager {}
unsafe impl Sync for SeedingManager {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn default_config() -> TorrentfsConfig {
        TorrentfsConfig::default_config()
    }

    #[test]
    fn test_seeding_manager_new() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path(), &default_config());
        assert!(manager.is_ok());
    }

    #[test]
    fn test_has_handle() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path(), &default_config()).unwrap();

        assert!(!manager.has_handle("nonexistent"));
    }

    #[test]
    fn test_is_seeding() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path(), &default_config()).unwrap();

        assert!(!manager.is_seeding("nonexistent"));
    }

    #[test]
    fn test_get_all_seeds() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path(), &default_config()).unwrap();

        let seeds = manager.get_all_seeds();
        assert!(seeds.is_empty());
    }
}
