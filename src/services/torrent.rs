//! TorrentService — orchestrates torrent operations,
//! cutting the direct FUSE→DB calls in favour of a service layer.
//!
//! Returns `FsError` (a domain error), never raw errno — the errno mapping is
//! the FUSE adapter's job. This module does not import `libc`.

use std::sync::{Arc, Mutex};

use crate::db::{Database, FileEntry, InsertTorrentResult, TorrentFile};
use crate::domain::fs_error::{FsError, FsResult};
use crate::metadata::TorrentInfo;
use tracing::{error, info, warn};

use super::download::DownloadService;

/// TorrentService wraps database operations for torrent lifecycle management.
pub struct TorrentService {
    db: Arc<Mutex<Database>>,
    download_service: Option<Arc<DownloadService>>,
}

impl TorrentService {
    pub fn new(db: Arc<Mutex<Database>>, download_service: Option<Arc<DownloadService>>) -> Self {
        Self {
            db,
            download_service,
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
    pub fn remove_torrent(&self, filename: &str, source_path: &str) -> FsResult<Option<i64>> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        match db_guard.get_torrent_by_filename_and_source_path(filename, source_path) {
            Ok(Some(torrent)) => {
                let torrent_id = torrent.id;
                db_guard.delete_torrent(torrent_id).map_err(|e| {
                    error!("Failed to delete torrent from database: {:?}", e);
                    FsError::from(e)
                })?;
                info!(
                    "Deleted torrent '{}' (id={}, source_path='{}')",
                    filename, torrent_id, source_path
                );
                Ok(Some(torrent_id))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                error!("Database error during remove_torrent: {:?}", e);
                Err(FsError::from(e))
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
