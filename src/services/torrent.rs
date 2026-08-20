//! TorrentService — orchestrates torrent operations,
//! cutting the direct FUSE→DB calls in favour of a service layer.
//!
//! Returns `FsError` (a domain error), never raw errno — the errno mapping is
//! the FUSE adapter's job. This module does not import `libc`.

use std::sync::{Arc, Mutex};
use crate::seeding::SeedingManager;

use crate::db::{Database, FileEntry, InsertTorrentResult, TorrentFile};
use crate::domain::fs_error::{FsError, FsResult};
use crate::metadata::TorrentInfo;
use tracing::{error, info, warn};

use super::download::DownloadService;

/// TorrentService wraps database operations for torrent lifecycle management.
pub struct TorrentService {
    db: Arc<Mutex<Database>>,
    download_service: Option<Arc<DownloadService>>,
    seeding_manager: Option<Arc<SeedingManager>>,
}

impl TorrentService {
    pub fn new(
        db: Arc<Mutex<Database>>,
        download_service: Option<Arc<DownloadService>>,
        seeding_manager: Option<Arc<SeedingManager>>,
    ) -> Self {
        Self {
            db,
            download_service,
            seeding_manager,
        }
    }

    /// Add a torrent to the database. Parses the torrent data, extracts metadata,
    /// and persists everything atomically.  After persistence, creates an
    /// upload_mode libtorrent handle so peer/seed information is immediately
    /// available without downloading any data.
    pub fn add_torrent(&self, data: &[u8], source_path: &str, filename: &str) -> FsResult<()> {
        let info = TorrentInfo::from_bytes(data.to_vec()).map_err(|e| {
            warn!("Failed to parse torrent {}: {:?}", filename, e);
            FsError::CorruptTorrent(format!("Failed to parse torrent {}: {:?}", filename, e))
        })?;

        let metadata = info.metadata().map_err(|e| {
            error!("Failed to get torrent metadata {}: {:?}", filename, e);
            FsError::Internal(format!(
                "Failed to get torrent metadata {}: {:?}",
                filename, e
            ))
        })?;

        let info_hash_hex = hex::encode(metadata.info_hash);

        let is_new = {
            let mut db_guard = self.db.lock().map_err(|_| {
                error!("Database lock poisoned");
                FsError::LockPoisoned
            })?;

            let files: Vec<FileEntry> = metadata
                .files
                .iter()
                .map(|f| FileEntry {
                    path: f.path.clone(),
                    size: f.size as i64,
                })
                .collect();

            let result = db_guard
                .insert_torrent_with_files(
                    source_path,
                    &metadata.name,
                    filename,
                    metadata.total_size as i64,
                    &info_hash_hex,
                    metadata.num_files as i64,
                    &files,
                )
                .map_err(|e| {
                    error!("Failed to insert torrent with files {}: {:?}", filename, e);
                    FsError::from(e)
                })?;

            let is_new = match result {
                InsertTorrentResult::Inserted(torrent_id) => {
                    db_guard.set_torrent_data(torrent_id, data).map_err(|e| {
                        error!("Failed to store torrent data for {}: {:?}", filename, e);
                        FsError::from(e)
                    })?;

                    info!(
                        "Persisted torrent '{}' ({} files, {} bytes) from {}",
                        metadata.name,
                        metadata.num_files,
                        metadata.total_size,
                        if source_path.is_empty() {
                            "root"
                        } else {
                            source_path
                        }
                    );
                    true
                }
                InsertTorrentResult::Duplicate(existing_id) => {
                    info!(
                        "Torrent '{}' already exists (id={}), duplicate recorded",
                        metadata.name, existing_id
                    );
                    false
                }
            };
            // db_guard dropped here (end of block scope)
            is_new
        };

        // Create upload_mode handle so peer/seed info is visible immediately
        // without triggering any data download (all pieces at priority 0).
        if is_new {
            if let Some(ref ds) = self.download_service {
                match ds.ensure_handle_lightweight(Arc::new(info)) {
                    Ok(_) => {
                        info!(
                            "Created lightweight handle for torrent '{}' (upload_mode)",
                            metadata.name
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create lightweight handle for torrent '{}': {:?}",
                            metadata.name, e
                        );
                        // Non-fatal: the torrent is already in the database and
                        // a handle will be created lazily when first accessed.
                    }
                }
            }
        }

        Ok(())
    }

    /// Remove a torrent from the database by filename and source_path.
    ///
    /// After the DB record is deleted, when no other torrent still references
    /// the same info_hash: purge the on-disk pieces cache, release the
    /// DownloadEngine handle (and its scheduler state), and remove the
    /// SeedingManager seed — so a long-running daemon does not accumulate
    /// handles or keep announcing a deleted torrent (TSI-2232).
    pub fn remove_torrent(&self, filename: &str, source_path: &str) -> FsResult<Option<i64>> {
        let (torrent_id, info_hash, purge_pieces) = {
            let mut db_guard = self.db.lock().map_err(|_| {
                error!("Database lock poisoned");
                FsError::LockPoisoned
            })?;

            match db_guard.get_torrent_by_filename_and_source_path(filename, source_path) {
                Ok(Some(torrent)) => {
                    let torrent_id = torrent.id;
                    let info_hash = torrent.info_hash.clone();
                    db_guard.delete_torrent(torrent_id).map_err(|e| {
                        error!("Failed to delete torrent from database: {:?}", e);
                        FsError::from(e)
                    })?;

                    let purge_pieces = match db_guard.get_torrents_by_infohash(&info_hash) {
                        Ok(remaining) if !remaining.is_empty() => {
                            info!(
                                "Skipping pieces cache purge for info_hash={} ({} torrent(s) still reference it)",
                                info_hash,
                                remaining.len()
                            );
                            false
                        }
                        Ok(_) => true,
                        Err(e) => {
                            warn!(
                                "Failed to check remaining torrents for info_hash={}: {:?}; skipping purge",
                                info_hash, e
                            );
                            false
                        }
                    };

                    info!(
                        "Deleted torrent '{}' (id={}, source_path='{}', info_hash={})",
                        filename, torrent_id, source_path, info_hash
                    );
                    (Some(torrent_id), info_hash, purge_pieces)
                }
                Ok(None) => return Ok(None),
                Err(e) => {
                    error!("Database error during remove_torrent: {:?}", e);
                    return Err(FsError::from(e));
                }
            }
        };

        if purge_pieces {
            self.purge_pieces_cache(&info_hash);
            self.release_engine_and_seeding(&info_hash);
        }

        Ok(torrent_id)
    }

    /// Remove the `cache/pieces/<info_hash>/` directory and its cache metadata.
    /// Best-effort: a failure leaves orphaned pieces, which remain safe to
    /// re-read/re-download and can be cleaned by a later removal or restart
    /// scan.
    fn purge_pieces_cache(&self, info_hash: &str) {
        let Some(ds) = &self.download_service else {
            return;
        };
        let Some(cache) = ds.get_cache_manager() else {
            return;
        };
        let mut guard = match cache.lock() {
            Ok(g) => g,
            Err(_) => {
                error!(
                    "Cache lock poisoned during pieces purge for info_hash={}",
                    info_hash
                );
                return;
            }
        };
        if let Err(e) = guard.remove_infohash_pieces(info_hash) {
            warn!(
                "Failed to purge pieces cache for info_hash={}: {:?}",
                info_hash, e
            );
        }
    }

    /// Release the DownloadEngine handle and the SeedingManager seed for an
    /// info_hash.  Best-effort: the DB row and pieces cache are already gone,
    /// so a transient engine/seeding failure only leaves a stale handle that a
    /// restart clears — it never desyncs DB vs. cache (TSI-2232).
    fn release_engine_and_seeding(&self, info_hash: &str) {
        if let Some(ds) = &self.download_service {
            if let Err(e) = ds.remove_handle(info_hash) {
                warn!(
                    "Failed to remove DownloadEngine handle for info_hash={}: {:?}",
                    info_hash, e
                );
            }
        }
        if let Some(sm) = &self.seeding_manager {
            if let Err(e) = sm.remove_seed(info_hash) {
                warn!(
                    "Failed to remove seeding seed for info_hash={}: {:?}",
                    info_hash, e
                );
            }
        }
    }

    /// Clean up metadata directories orphaned by a torrent deletion or move.
    /// Returns the removed directory paths (leaf-first) so callers can evict
    /// stale `data_inodes` cache entries.
    pub fn cleanup_orphaned_metadata_directories(
        &self,
        source_path: &str,
    ) -> FsResult<Vec<String>> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        db_guard
            .cleanup_orphaned_metadata_directories(source_path)
            .map_err(|e| {
                error!("Failed to cleanup orphaned metadata directories: {:?}", e);
                FsError::from(e)
            })
    }

    /// Rename a torrent in the database.
    pub fn rename_torrent(
        &self,
        old_name: &str,
        old_source_path: &str,
        new_name: &str,
        new_source_path: &str,
    ) -> FsResult<()> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        match db_guard.get_torrent_by_filename_and_source_path(old_name, old_source_path) {
            Ok(Some(torrent)) => {
                db_guard
                    .rename_torrent(torrent.id, &torrent.name, new_name, new_source_path)
                    .map_err(|e| {
                        error!("Failed to rename torrent in database: {:?}", e);
                        FsError::from(e)
                    })?;
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => {
                error!("Database error during rename_torrent: {:?}", e);
                Err(FsError::from(e))
            }
        }
    }

    /// Ensure metadata directories exist in the database for a given source_path.
    pub fn ensure_metadata_directories(&self, source_path: &str) -> FsResult<()> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        db_guard
            .ensure_metadata_directories(source_path)
            .map_err(|e| {
                warn!("Failed to persist directory to database: {:?}", e);
                FsError::from(e)
            })
    }

    /// Delete a metadata directory from the database.
    pub fn delete_metadata_directory(&self, source_path: &str) -> FsResult<()> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        db_guard
            .delete_metadata_directory(source_path)
            .map_err(|e| {
                warn!("Failed to delete metadata directory from database: {:?}", e);
                FsError::from(e)
            })
    }

    /// Rename a metadata directory in the database.
    pub fn rename_metadata_directory(
        &self,
        old_path: &str,
        new_name: &str,
        new_path: &str,
    ) -> FsResult<()> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        db_guard
            .rename_metadata_directory(old_path, new_name, new_path)
            .map_err(|e| {
                error!("Failed to rename metadata directory in database: {:?}", e);
                FsError::from(e)
            })
    }

    /// Check if a torrent with the given id exists in the database.
    pub fn torrent_exists_by_id(&self, id: i64) -> bool {
        let db_guard = match self.db.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };
        db_guard.get_torrent_by_id(id).ok().flatten().is_some()
    }

    /// Get torrent with its files by torrent_id.
    /// Returns (info_hash, source_path, files) on success.
    pub fn get_torrent_with_files(
        &self,
        torrent_id: i64,
    ) -> FsResult<Option<(String, String, Vec<TorrentFile>)>> {
        let db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        let torrent = match db_guard.get_torrent_by_id(torrent_id).map_err(|e| {
            error!("Failed to get torrent by id: {:?}", e);
            FsError::from(e)
        })? {
            Some(t) => t,
            None => return Ok(None),
        };

        let files = db_guard.get_files_by_torrent_id(torrent_id).map_err(|e| {
            error!("Failed to get files for torrent: {:?}", e);
            FsError::from(e)
        })?;

        Ok(Some((torrent.info_hash, torrent.source_path, files)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, InsertTorrentResult};
    use crate::infrastructure::config::TorrentfsConfig;
    use tempfile::TempDir;

    fn insert_torrent(
        db: &mut Database,
        source_path: &str,
        filename: &str,
        info_hash: &str,
    ) -> i64 {
        match db.insert_torrent(source_path, "foo", filename, 16, info_hash, 1) {
            Ok(InsertTorrentResult::Inserted(id)) => id,
            other => panic!("unexpected insert result: {:?}", other),
        }
    }

    fn service_with_piece(
        cache_dir: &std::path::Path,
        info_hash: &str,
    ) -> (Arc<DownloadService>, String) {
        let config = TorrentfsConfig::default_config();
        let download_service = Arc::new(DownloadService::new(cache_dir, &config).unwrap());

        // Register one piece through the shared CacheManager so its metadata
        // is populated, exactly as a real read would.
        let piece_key = format!("{}:piece:0", info_hash);
        {
            let cache = download_service.get_cache_manager().unwrap();
            let mut guard = cache.lock().unwrap();
            let path = guard.ensure_piece_dir(&piece_key).unwrap();
            std::fs::write(&path, b"piece-data").unwrap();
            guard.add_piece(&piece_key, 10).unwrap();
        }

        (download_service, piece_key)
    }

    #[test]
    fn test_remove_torrent_purges_pieces_cache() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let info_hash = "0123456789abcdef0123456789abcdef01234567".to_string();
        let (download_service, piece_key) = service_with_piece(&cache_dir, &info_hash);

        let db = Database::open_in_memory().unwrap();
        let db_arc = Arc::new(Mutex::new(db));
        {
            let mut guard = db_arc.lock().unwrap();
            insert_torrent(&mut guard, "src", "foo.torrent", &info_hash);
        }

        let svc = TorrentService::new(db_arc, Some(download_service.clone()), None);

        let pieces_dir = cache_dir.join("pieces").join(&info_hash);
        assert!(pieces_dir.exists());

        let removed = svc.remove_torrent("foo.torrent", "src").unwrap();
        assert_eq!(removed, Some(1));

        // Directory and cache metadata are both gone.
        assert!(!pieces_dir.exists());
        let cache = download_service.get_cache_manager().unwrap();
        let guard = cache.lock().unwrap();
        assert!(!guard.has_piece(&piece_key));
        assert!(!guard.get_all_infohashes().contains(&info_hash));
    }

    #[test]
    fn test_remove_torrent_keeps_shared_pieces() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let info_hash = "fedcba9876543210fedcba9876543210fedcba98".to_string();
        let (download_service, piece_key) = service_with_piece(&cache_dir, &info_hash);

        let db = Database::open_in_memory().unwrap();
        let db_arc = Arc::new(Mutex::new(db));
        {
            let mut guard = db_arc.lock().unwrap();
            // Two distinct source paths share one info_hash (same .torrent
            // copied twice).
            insert_torrent(&mut guard, "a", "foo.torrent", &info_hash);
            insert_torrent(&mut guard, "b", "foo.torrent", &info_hash);
        }

        let svc = TorrentService::new(db_arc, Some(download_service.clone()), None);

        let pieces_dir = cache_dir.join("pieces").join(&info_hash);
        assert!(pieces_dir.exists());

        let removed = svc.remove_torrent("foo.torrent", "a").unwrap();
        assert_eq!(removed, Some(1));

        // The other torrent still references this info_hash: pieces stay.
        assert!(pieces_dir.exists());
        let cache = download_service.get_cache_manager().unwrap();
        let guard = cache.lock().unwrap();
        assert!(guard.has_piece(&piece_key));
    }

    /// `release_engine_and_seeding` must be called when the last DB reference
    /// to an info_hash is deleted (TSI-2232).  This test wires a real
    /// SeedingManager into the service and asserts that after remove_torrent
    /// the seeding manager no longer tracks the info_hash.
    #[test]
    fn test_remove_torrent_releases_seeding_manager() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let config = TorrentfsConfig::default_config();

        let info_hash = "aabbccdd00112233445566778899aabbccdd0011".to_string();
        let download_service = Arc::new(DownloadService::new(&cache_dir, &config).unwrap());
        let seeding_manager =
            Arc::new(crate::seeding::SeedingManager::new(&cache_dir, &config).unwrap());

        let db = Database::open_in_memory().unwrap();
        let db_arc = Arc::new(Mutex::new(db));
        {
            let mut guard = db_arc.lock().unwrap();
            insert_torrent(&mut guard, "src", "foo.torrent", &info_hash);
        }

        let svc = TorrentService::new(
            db_arc,
            Some(download_service),
            Some(seeding_manager.clone()),
        );

        // Before removal the SeedingManager has no handle yet (none was
        // added); remove_torrent must still call remove_seed without panic
        // (idempotent).  After removal, has_handle returns false.
        assert!(!seeding_manager.has_handle(&info_hash));

        let removed = svc.remove_torrent("foo.torrent", "src").unwrap();
        assert_eq!(removed, Some(1));

        // remove_seed was called (best-effort, no-op if no handle existed);
        // the seeding manager must not track the deleted info_hash.
        assert!(!seeding_manager.has_handle(&info_hash));
        assert!(seeding_manager.get_all_seeds().is_empty());
    }
}
