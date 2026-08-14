//! SeedingManager — peer seeding state + cache eviction callbacks.
//!
//! Owns its libtorrent `Session` + torrent handles on a dedicated thread (the
//! same single-owner pattern as the download engine), so the raw pointers never
//! cross a thread boundary and the old `unsafe impl Send/Sync` are gone.  All
//! public methods send a command to that thread and wait for the reply.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Mutex;
use std::thread::JoinHandle;

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::download::{Session, TorrentHandle, TorrentState};
use crate::error::{TorrentError, TorrentResult};
use crate::metadata::TorrentInfo;
use std::sync::Arc;

pub struct SeedingManager {
    tx: mpsc::Sender<SeedingCommand>,
    join: Mutex<Option<JoinHandle<()>>>,
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

enum SeedingCommand {
    AddSeed {
        info: Arc<TorrentInfo>,
        reply: SyncSender<TorrentResult<()>>,
    },
    RemoveSeed {
        info_hash: String,
        reply: SyncSender<TorrentResult<()>>,
    },
    MarkPieceAvailable {
        info_hash: String,
        piece_index: i32,
        reply: SyncSender<TorrentResult<()>>,
    },
    MarkPieceUnavailable {
        info_hash: String,
        piece_index: i32,
        reply: SyncSender<TorrentResult<()>>,
    },
    HandleEviction {
        info_hash: String,
        piece_index: i32,
    },
    UpdateStatus {
        reply: SyncSender<TorrentResult<Vec<SeedingInfo>>>,
    },
    GetSeedingInfo {
        info_hash: String,
        reply: SyncSender<Option<SeedingInfo>>,
    },
    HasHandle {
        info_hash: String,
        reply: SyncSender<bool>,
    },
    IsSeeding {
        info_hash: String,
        reply: SyncSender<bool>,
    },
    GetAllSeeds {
        reply: SyncSender<Vec<SeedingInfo>>,
    },
    GetTotalUploaded {
        reply: SyncSender<u64>,
    },
    Shutdown,
}

impl SeedingManager {
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        let pieces_dir = cache_dir.join("pieces");
        std::fs::create_dir_all(&pieces_dir)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;

        let (init_tx, init_rx) = mpsc::sync_channel::<TorrentResult<()>>(1);
        let (tx, rx) = mpsc::channel::<SeedingCommand>();
        let cache_dir = cache_dir.to_path_buf();
        let config = config.clone();

        let handle = std::thread::Builder::new()
            .name("seeding-manager".into())
            .spawn(move || {
                // The session must be created on this thread: `Session` owns a
                // raw libtorrent pointer and is no longer `Send`.
                match Session::new_with_custom_storage(&config, &pieces_dir) {
                    Ok(session) => {
                        let _ = init_tx.send(Ok(()));
                        seeding_loop(session, rx, cache_dir);
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(e));
                    }
                }
            })
            .map_err(|e| TorrentError::Unknown {
                code: -1,
                message: format!("Failed to spawn seeding thread: {}", e),
            })?;

        init_rx
            .recv()
            .map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "Seeding thread disconnected before init".to_string(),
            })??;

        Ok(Self {
            tx,
            join: Mutex::new(Some(handle)),
        })
    }

    pub fn add_seed(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        let (reply, rx) = mpsc::sync_channel(1);
        self.send(SeedingCommand::AddSeed { info, reply })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    pub fn remove_seed(&self, info_hash: &str) -> TorrentResult<()> {
        let (reply, rx) = mpsc::sync_channel(1);
        self.send(SeedingCommand::RemoveSeed {
            info_hash: info_hash.to_string(),
            reply,
        })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Mark a piece as available for seeding (priority 7).
    pub fn mark_piece_available(&self, info_hash: &str, piece_index: i32) -> TorrentResult<()> {
        let (reply, rx) = mpsc::sync_channel(1);
        self.send(SeedingCommand::MarkPieceAvailable {
            info_hash: info_hash.to_string(),
            piece_index,
            reply,
        })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Mark a piece as unavailable for seeding (priority 0).
    pub fn mark_piece_unavailable(&self, info_hash: &str, piece_index: i32) -> TorrentResult<()> {
        let (reply, rx) = mpsc::sync_channel(1);
        self.send(SeedingCommand::MarkPieceUnavailable {
            info_hash: info_hash.to_string(),
            piece_index,
            reply,
        })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Register this manager as an eviction callback on the given CacheManager.
    pub fn register_eviction_callback(&self, cache: &mut CacheManager) {
        let tx = self.tx.clone();
        cache.on_evict(Box::new(move |info_hash: String, piece_index: i32| {
            let _ = tx.send(SeedingCommand::HandleEviction {
                info_hash,
                piece_index,
            });
        }));
    }

    pub fn update_seeding_status(&self) -> TorrentResult<Vec<SeedingInfo>> {
        let (reply, rx) = mpsc::sync_channel(1);
        self.send(SeedingCommand::UpdateStatus { reply })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    pub fn get_seeding_info(&self, info_hash: &str) -> Option<SeedingInfo> {
        let (reply, rx) = mpsc::sync_channel(1);
        if self
            .send(SeedingCommand::GetSeedingInfo {
                info_hash: info_hash.to_string(),
                reply,
            })
            .is_err()
        {
            return None;
        }
        rx.recv().ok().flatten()
    }

    pub fn has_handle(&self, info_hash: &str) -> bool {
        let (reply, rx) = mpsc::sync_channel(1);
        if self
            .send(SeedingCommand::HasHandle {
                info_hash: info_hash.to_string(),
                reply,
            })
            .is_err()
        {
            return false;
        }
        rx.recv().unwrap_or(false)
    }

    pub fn is_seeding(&self, info_hash: &str) -> bool {
        let (reply, rx) = mpsc::sync_channel(1);
        if self
            .send(SeedingCommand::IsSeeding {
                info_hash: info_hash.to_string(),
                reply,
            })
            .is_err()
        {
            return false;
        }
        rx.recv().unwrap_or(false)
    }

    pub fn get_all_seeds(&self) -> Vec<SeedingInfo> {
        let (reply, rx) = mpsc::sync_channel(1);
        if self.send(SeedingCommand::GetAllSeeds { reply }).is_err() {
            return vec![];
        }
        rx.recv().unwrap_or_default()
    }

    pub fn get_total_uploaded(&self) -> u64 {
        let (reply, rx) = mpsc::sync_channel(1);
        if self.send(SeedingCommand::GetTotalUploaded { reply }).is_err() {
            return 0;
        }
        rx.recv().unwrap_or(0)
    }


    fn send(&self, cmd: SeedingCommand) -> TorrentResult<()> {
        self.tx.send(cmd).map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Seeding manager has shut down".to_string(),
        })
    }

    fn disconnected() -> TorrentError {
        TorrentError::Unknown {
            code: -1,
            message: "Seeding manager thread disconnected".to_string(),
        }
    }
}

impl Drop for SeedingManager {
    fn drop(&mut self) {
        let _ = self.tx.send(SeedingCommand::Shutdown);
        if let Some(handle) = self.join.lock().ok().and_then(|mut g| g.take()) {
            let _ = handle.join();
        }
    }
}

// ── Thread body ─────────────────────────────────────────────────────────────

fn seeding_loop(
    mut session: Session,
    rx: Receiver<SeedingCommand>,
    cache_dir: PathBuf,
) {
    let mut handles: HashMap<String, TorrentHandle> = HashMap::new();
    let mut seeding_info: HashMap<String, SeedingInfo> = HashMap::new();

    while let Ok(cmd) = rx.recv() {
        match cmd {
            SeedingCommand::AddSeed { info, reply } => {
                let _ = reply.send(add_seed_impl(
                    &mut session,
                    &mut handles,
                    &mut seeding_info,
                    &cache_dir,
                    &info,
                ));
            }
            SeedingCommand::RemoveSeed { info_hash, reply } => {
                let _ = reply.send(remove_seed_impl(
                    &mut session,
                    &mut handles,
                    &mut seeding_info,
                    &info_hash,
                ));
            }
            SeedingCommand::MarkPieceAvailable {
                info_hash,
                piece_index,
                reply,
            } => {
                let _ = reply.send(mark_piece(&handles, &info_hash, piece_index, 7));
            }
            SeedingCommand::MarkPieceUnavailable {
                info_hash,
                piece_index,
                reply,
            } => {
                let _ = reply.send(mark_piece(&handles, &info_hash, piece_index, 0));
            }
            SeedingCommand::HandleEviction {
                info_hash,
                piece_index,
            } => {
                tracing::info!(
                    "Eviction-triggered: marking piece {} unavailable for seeding info_hash={}",
                    piece_index,
                    info_hash
                );
                let _ = mark_piece(&handles, &info_hash, piece_index, 0);
            }
            SeedingCommand::UpdateStatus { reply } => {
                let _ = reply.send(update_status_impl(&handles, &mut seeding_info));
            }
            SeedingCommand::GetSeedingInfo { info_hash, reply } => {
                let _ = update_status_impl(&handles, &mut seeding_info);
                let _ = reply.send(seeding_info.get(&info_hash).cloned());
            }
            SeedingCommand::HasHandle { info_hash, reply } => {
                let _ = reply.send(handles.contains_key(&info_hash));
            }
            SeedingCommand::IsSeeding { info_hash, reply } => {
                let _ = update_status_impl(&handles, &mut seeding_info);
                let _ = reply.send(
                    seeding_info
                        .get(&info_hash)
                        .map(|i| i.state == SeedingState::Seeding)
                        .unwrap_or(false),
                );
            }
            SeedingCommand::GetAllSeeds { reply } => {
                let _ = update_status_impl(&handles, &mut seeding_info);
                let _ = reply.send(seeding_info.values().cloned().collect());
            }
            SeedingCommand::GetTotalUploaded { reply } => {
                let _ = update_status_impl(&handles, &mut seeding_info);
                let _ = reply.send(seeding_info.values().map(|s| s.uploaded).sum());
            }
            SeedingCommand::Shutdown => break,
        }
    }
}

fn add_seed_impl(
    session: &mut Session,
    handles: &mut HashMap<String, TorrentHandle>,
    seeding_info: &mut HashMap<String, SeedingInfo>,
    cache_dir: &Path,
    info: &TorrentInfo,
) -> TorrentResult<()> {
    let info_hash = hex::encode(info.info_hash()?);
    if handles.contains_key(&info_hash) {
        return Ok(());
    }

    let pieces_dir = cache_dir.join("pieces");
    if !pieces_dir.exists() {
        std::fs::create_dir_all(&pieces_dir)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;
    }

    let handle = session.add_torrent(info, &pieces_dir)?;
    let name = info.name();
    let total_size = info.total_size();

    handles.insert(info_hash.clone(), handle);
    seeding_info.insert(
        info_hash.clone(),
        SeedingInfo {
            info_hash,
            name,
            total_size,
            uploaded: 0,
            state: SeedingState::Checking,
        },
    );
    Ok(())
}

fn remove_seed_impl(
    session: &mut Session,
    handles: &mut HashMap<String, TorrentHandle>,
    seeding_info: &mut HashMap<String, SeedingInfo>,
    info_hash: &str,
) -> TorrentResult<()> {
    if let Some(handle) = handles.remove(info_hash) {
        session.remove_torrent(handle, false);
    }
    seeding_info.remove(info_hash);
    Ok(())
}

fn mark_piece(
    handles: &HashMap<String, TorrentHandle>,
    info_hash: &str,
    piece_index: i32,
    priority: i32,
) -> TorrentResult<()> {
    match handles.get(info_hash) {
        Some(handle) => {
            if !handle.set_piece_priority(piece_index, priority) {
                tracing::warn!(
                    "set_piece_priority({}) returned false for info_hash={}, piece_index={}",
                    priority,
                    info_hash,
                    piece_index
                );
            }
        }
        None => {
            tracing::debug!(
                "No seeding handle found for info_hash={}, skipping mark_piece priority={}",
                info_hash,
                priority
            );
        }
    }
    Ok(())
}

fn update_status_impl(
    handles: &HashMap<String, TorrentHandle>,
    seeding_info: &mut HashMap<String, SeedingInfo>,
) -> TorrentResult<Vec<SeedingInfo>> {
    for (info_hash, handle) in handles.iter() {
        if let Ok(status) = handle.status() {
            if let Some(info) = seeding_info.get_mut(info_hash) {
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
    Ok(seeding_info.values().cloned().collect())
}

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
        let manager = SeedingManager::new(temp_dir.path(), &default_config()).unwrap();
        assert!(manager.get_all_seeds().is_empty());
    }

    #[test]
    fn test_get_all_seeds() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path(), &default_config()).unwrap();
        let seeds = manager.get_all_seeds();
        assert!(seeds.is_empty());
    }

    #[test]
    fn test_has_handle() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path(), &default_config()).unwrap();
        assert!(!manager.has_handle("deadbeef"));
    }

    #[test]
    fn test_is_seeding() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path(), &default_config()).unwrap();
        assert!(!manager.is_seeding("deadbeef"));
    }
}
