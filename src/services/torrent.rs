//! TorrentService — orchestrates torrent operations,
//! cutting the direct FUSE→DB calls in favour of a service layer.

use std::sync::{Arc, Mutex};

use crate::db::{Database, FileEntry, InsertTorrentResult, TorrentFile};
use crate::metadata::TorrentInfo;
use tracing::{error, info, warn};

/// TorrentService wraps database operations for torrent lifecycle management.
pub struct TorrentService {
    db: Arc<Mutex<Database>>,
}

impl TorrentService {
    pub fn new(db: Arc<Mutex<Database>>) -> Self {
        Self { db }
    }

    /// Add a torrent to the database. Parses the torrent data, extracts metadata,
    /// and persists everything atomically.
    pub fn add_torrent(&self, data: &[u8], source_path: &str, filename: &str) -> Result<(), i32> {
        let info = TorrentInfo::from_bytes(data.to_vec()).map_err(|e| {
            warn!("Failed to parse torrent {}: {:?}", filename, e);
            libc::EINVAL
        })?;

        let metadata = info.metadata().map_err(|e| {
            error!("Failed to get torrent metadata {}: {:?}", filename, e);
            libc::EIO
        })?;

        let info_hash_hex = hex::encode(metadata.info_hash);

        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            libc::EIO
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
                libc::EIO
            })?;

        match result {
            InsertTorrentResult::Inserted(torrent_id) => {
                db_guard.set_torrent_data(torrent_id, data).map_err(|e| {
                    error!("Failed to store torrent data for {}: {:?}", filename, e);
                    libc::EIO
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
            }
            InsertTorrentResult::Duplicate(existing_id) => {
                info!(
                    "Torrent '{}' already exists (id={}), duplicate recorded",
                    metadata.name, existing_id
                );
            }
        }

        Ok(())
    }

    /// Remove a torrent from the database by filename and source_path.
    pub fn remove_torrent(&self, filename: &str, source_path: &str) -> Result<Option<i64>, i32> {
        let torrent_id = {
            let mut db_guard = self.db.lock().map_err(|_| {
                error!("Database lock poisoned");
                libc::EIO
            })?;

            match db_guard.get_torrent_by_filename_and_source_path(filename, source_path) {
                Ok(Some(torrent)) => {
                    let torrent_id = torrent.id;
                    db_guard.delete_torrent(torrent_id).map_err(|e| {
                        error!("Failed to delete torrent from database: {:?}", e);
                        libc::EIO
                    })?;
                    info!(
                        "Deleted torrent '{}' (id={}, source_path='{}')",
                        filename, torrent_id, source_path
                    );
                    Some(torrent_id)
                }
                Ok(None) => None,
                Err(e) => {
                    error!("Database error during remove_torrent: {:?}", e);
                    return Err(libc::EIO);
                }
            }
        }; // Drop db lock before cleanup (cleanup acquires its own lock)

        if torrent_id.is_some() {
            self.cleanup_orphaned_metadata_directories(source_path);
        }

        Ok(torrent_id)
    }
    /// Rename a torrent in the database.
    pub fn rename_torrent(
        &self,
        old_name: &str,
        old_source_path: &str,
        new_name: &str,
        new_source_path: &str,
    ) -> Result<(), i32> {
        let path_changed = old_source_path != new_source_path;

        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            libc::EIO
        })?;

        match db_guard.get_torrent_by_filename_and_source_path(old_name, old_source_path) {
            Ok(Some(torrent)) => {
                db_guard
                    .rename_torrent(torrent.id, &torrent.name, new_name, new_source_path)
                    .map_err(|e| {
                        error!("Failed to rename torrent in database: {:?}", e);
                        libc::EIO
                    })?;
            }
            Ok(None) => return Ok(()),
            Err(e) => {
                error!("Database error during rename_torrent: {:?}", e);
                return Err(libc::EIO);
            }
        }
        drop(db_guard);

        if path_changed {
            self.cleanup_orphaned_metadata_directories(old_source_path);
        }

        Ok(())
    }

    /// Ensure metadata directories exist in the database for a given source_path.
    pub fn ensure_metadata_directories(&self, source_path: &str) -> Result<(), i32> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            libc::EIO
        })?;

        db_guard
            .ensure_metadata_directories(source_path)
            .map_err(|e| {
                warn!("Failed to persist directory to database: {:?}", e);
                libc::EIO
            })
    }

    /// Delete a metadata directory from the database.
    pub fn delete_metadata_directory(&self, source_path: &str) -> Result<(), i32> {
        let parent_path: Option<String> = {
            let mut db_guard = self.db.lock().map_err(|_| {
                error!("Database lock poisoned");
                libc::EIO
            })?;

            let parent: Option<i64> = db_guard
                .conn
                .query_row(
                    "SELECT parent_id FROM metadata_directories WHERE path = ?",
                    rusqlite::params![source_path],
                    |row| row.get(0),
                )
                .ok()
                .flatten()
                .flatten();

            let parent_path = parent.and_then(|pid| {
                db_guard
                    .conn
                    .query_row(
                        "SELECT path FROM metadata_directories WHERE id = ?",
                        rusqlite::params![pid],
                        |row| row.get(0),
                    )
                    .ok()
                    .flatten()
            });

            db_guard.delete_metadata_directory(source_path).map_err(|e| {
                warn!("Failed to delete metadata directory from database: {:?}", e);
                libc::EIO
            })?;

            parent_path
        }; // Drop db lock before cleanup

        if let Some(parent) = parent_path {
            self.cleanup_orphaned_metadata_directories(&parent);
        }

        Ok(())
    }

    /// Rename a metadata directory in the database.
    pub fn rename_metadata_directory(
        &self,
        old_path: &str,
        new_name: &str,
        new_path: &str,
    ) -> Result<(), i32> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            libc::EIO
        })?;

        db_guard
            .rename_metadata_directory(old_path, new_name, new_path)
            .map_err(|e| {
                error!("Failed to rename metadata directory in database: {:?}", e);
                libc::EIO
            })
    }

    /// Clean up orphaned metadata directories starting from `source_path`.
    /// Removes metadata_directories entries that have no torrents and no child directories.
    fn cleanup_orphaned_metadata_directories(&self, source_path: &str) {
        let mut db_guard = match self.db.lock() {
            Ok(guard) => guard,
            Err(_) => {
                error!("Database lock poisoned during orphan cleanup");
                return;
            }
        };
        if let Err(e) = db_guard.cleanup_orphaned_metadata_directories(source_path) {
            warn!("Failed to cleanup orphaned metadata directories: {:?}", e);
        }
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
    ) -> Result<Option<(String, String, Vec<TorrentFile>)>, i32> {
        let db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            libc::EIO
        })?;

        let torrent = match db_guard.get_torrent_by_id(torrent_id).map_err(|e| {
            error!("Failed to get torrent by id: {:?}", e);
            libc::EIO
        })? {
            Some(t) => t,
            None => return Ok(None),
        };

        let files = db_guard.get_files_by_torrent_id(torrent_id).map_err(|e| {
            error!("Failed to get files for torrent: {:?}", e);
            libc::EIO
        })?;

        Ok(Some((torrent.info_hash, torrent.source_path, files)))
    }
}
