//! DataResolver — resolves FUSE lookup operations for the data/ subtree.
//! Extracted from TorrentFs to separate data tree resolution from the rest of FUSE handling.

use std::sync::{Arc, Mutex};

use crate::db::Database;
use crate::fuse::inodes::{DataInode, InodeManager};
use tracing::error;

pub struct DataResolver;

impl DataResolver {
    /// Resolve a child lookup within the data/ subtree.
    /// Returns (ino, DataInode) if found.
    pub fn resolve_data_lookup(
        inode_mgr: &InodeManager,
        db: &Arc<Mutex<Database>>,
        parent: u64,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        if parent == super::inodes::DATA_INO {
            return Self::resolve_data_root_lookup(db, name);
        }

        let data_inode = inode_mgr.data_inodes.get(&parent)?;
        match data_inode {
            DataInode::SourcePathDir { path } => {
                Self::resolve_source_path_dir_lookup(db, path, name)
            }
            DataInode::TorrentRoot { torrent_id, .. } => {
                Self::resolve_torrent_root_lookup(db, *torrent_id, name)
            }
            DataInode::TorrentDir {
                torrent_id, dir_id, ..
            } => Self::resolve_torrent_dir_lookup(db, *torrent_id, Some(*dir_id), name),
            DataInode::TorrentFile { .. } => None,
        }
    }

    fn resolve_data_root_lookup(db: &Arc<Mutex<Database>>, name: &str) -> Option<(u64, DataInode)> {
        let db_guard = db.lock().ok()?;

        let prefixes = db_guard.get_source_path_prefixes("").ok()?;
        if prefixes.contains(&name.to_string()) {
            let full_path = name.to_string();
            let ino = InodeManager::make_source_path_dir_ino(&full_path);
            return Some((ino, DataInode::SourcePathDir { path: full_path }));
        }

        let root_torrents = db_guard.get_torrents_by_source_path("").ok()?;
        for torrent in root_torrents {
            if torrent.filename == name {
                let ino = InodeManager::make_torrent_root_ino(torrent.id);
                return Some((
                    ino,
                    DataInode::TorrentRoot {
                        torrent_id: torrent.id,
                        source_path: torrent.source_path.clone(),
                        name: torrent.name.clone(),
                        filename: torrent.filename.clone(),
                    },
                ));
            }
        }

        None
    }

    fn resolve_source_path_dir_lookup(
        db: &Arc<Mutex<Database>>,
        prefix: &str,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        let new_path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", prefix, name)
        };

        let db_guard = db.lock().ok()?;

        let prefixes = db_guard.get_source_path_prefixes(prefix).ok()?;
        if prefixes.contains(&name.to_string()) {
            let ino = InodeManager::make_source_path_dir_ino(&new_path);
            return Some((ino, DataInode::SourcePathDir { path: new_path }));
        }

        let torrents = db_guard.get_torrents_by_source_path(prefix).ok()?;
        for torrent in torrents {
            if torrent.filename == name {
                let ino = InodeManager::make_torrent_root_ino(torrent.id);
                return Some((
                    ino,
                    DataInode::TorrentRoot {
                        torrent_id: torrent.id,
                        source_path: torrent.source_path.clone(),
                        name: torrent.name.clone(),
                        filename: torrent.filename.clone(),
                    },
                ));
            }
        }

        None
    }

    pub fn resolve_torrent_root_lookup(
        db: &Arc<Mutex<Database>>,
        torrent_id: i64,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        Self::resolve_torrent_dir_lookup(db, torrent_id, None, name)
    }

    pub fn resolve_torrent_dir_lookup(
        db: &Arc<Mutex<Database>>,
        torrent_id: i64,
        parent_dir_id: Option<i64>,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        let db_guard = db.lock().ok()?;

        if let Some(dir) = db_guard
            .get_torrent_directory(torrent_id, parent_dir_id, name)
            .ok()?
        {
            let ino = InodeManager::make_torrent_dir_ino(dir.id);
            return Some((
                ino,
                DataInode::TorrentDir {
                    torrent_id,
                    dir_id: dir.id,
                    name: dir.name,
                },
            ));
        }

        let files = if let Some(pid) = parent_dir_id {
            db_guard.get_files_in_directory(pid).ok()?
        } else {
            db_guard.get_root_files(torrent_id).ok()?
        };

        for file in files {
            if file.name == name {
                let ino = InodeManager::make_torrent_file_ino(file.id);
                return Some((
                    ino,
                    DataInode::TorrentFile {
                        torrent_id,
                        file_id: file.id,
                        name: file.name,
                        size: file.size,
                    },
                ));
            }
        }

        None
    }

    /// Lookup a data inode and cache it. Returns (ino, FileType, size).
    pub fn lookup_data_inode(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        parent: u64,
        name: &str,
    ) -> Option<(u64, fuser::FileType, u64)> {
        let (ino, data_inode) = Self::resolve_data_lookup(inode_mgr, db, parent, name)?;

        inode_mgr.data_inodes.insert(ino, data_inode.clone());

        match &data_inode {
            DataInode::SourcePathDir { .. }
            | DataInode::TorrentRoot { .. }
            | DataInode::TorrentDir { .. } => Some((ino, fuser::FileType::Directory, 0)),
            DataInode::TorrentFile { size, .. } => {
                Some((ino, fuser::FileType::RegularFile, *size as u64))
            }
        }
    }

    /// Generate readdir entries for a data/ inode.
    pub fn readdir_data(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        ino: u64,
        offset: i64,
    ) -> Option<Vec<(u64, i64, fuser::FileType, String)>> {
        use super::inodes::{DATA_INO, ROOT_INO};

        let mut entries: Vec<(u64, i64, fuser::FileType, String)> = Vec::new();
        let mut cache_entries: Vec<(u64, DataInode)> = Vec::new();

        if ino == DATA_INO {
            entries.push((DATA_INO, 1, fuser::FileType::Directory, ".".to_string()));
            entries.push((ROOT_INO, 2, fuser::FileType::Directory, "..".to_string()));

            {
                let db_guard = db.lock().ok()?;

                let mut offset_counter = 3i64;

                let root_torrents = db_guard.get_torrents_by_source_path("").ok()?;
                for torrent in root_torrents {
                    let torrent_ino = InodeManager::make_torrent_root_ino(torrent.id);
                    let name = torrent.filename.clone();
                    cache_entries.push((
                        torrent_ino,
                        DataInode::TorrentRoot {
                            torrent_id: torrent.id,
                            source_path: torrent.source_path.clone(),
                            name: torrent.name.clone(),
                            filename: torrent.filename.clone(),
                        },
                    ));
                    entries.push((
                        torrent_ino,
                        offset_counter,
                        fuser::FileType::Directory,
                        name,
                    ));
                    offset_counter += 1;
                }

                let prefixes = db_guard.get_source_path_prefixes("").ok()?;

                for prefix in prefixes {
                    let child_ino = InodeManager::make_source_path_dir_ino(&prefix);
                    cache_entries.push((
                        child_ino,
                        DataInode::SourcePathDir {
                            path: prefix.clone(),
                        },
                    ));
                    entries.push((
                        child_ino,
                        offset_counter,
                        fuser::FileType::Directory,
                        prefix,
                    ));
                    offset_counter += 1;
                }
            }

            for (cache_ino, cache_inode) in cache_entries {
                inode_mgr.data_inodes.insert(cache_ino, cache_inode);
            }

            return Some(
                entries
                    .into_iter()
                    .filter(|(_, o, _, _)| *o > offset)
                    .collect(),
            );
        }

        let data_inode = inode_mgr.data_inodes.get(&ino)?.clone();

        match data_inode {
            DataInode::SourcePathDir { path } => {
                entries.push((ino, 1, fuser::FileType::Directory, ".".to_string()));

                let parent_ino = if path.is_empty() {
                    DATA_INO
                } else {
                    let path_parts: Vec<&str> = path.split('/').collect();
                    if path_parts.len() == 1 {
                        DATA_INO
                    } else {
                        let parent_path = path_parts[..path_parts.len() - 1].join("/");
                        InodeManager::make_source_path_dir_ino(&parent_path)
                    }
                };
                entries.push((parent_ino, 2, fuser::FileType::Directory, "..".to_string()));

                {
                    let db_guard = db.lock().ok()?;

                    let mut offset_counter = 3i64;

                    let sub_prefixes = db_guard.get_source_path_prefixes(&path).ok()?;
                    for sub in sub_prefixes {
                        let new_path = if path.is_empty() {
                            sub.clone()
                        } else {
                            format!("{}/{}", path, sub)
                        };
                        let child_ino = InodeManager::make_source_path_dir_ino(&new_path);
                        cache_entries.push((
                            child_ino,
                            DataInode::SourcePathDir {
                                path: new_path.clone(),
                            },
                        ));
                        entries.push((child_ino, offset_counter, fuser::FileType::Directory, sub));
                        offset_counter += 1;
                    }

                    let direct_torrents = db_guard.get_torrents_by_source_path(&path).ok()?;
                    for torrent in direct_torrents {
                        let torrent_ino = InodeManager::make_torrent_root_ino(torrent.id);
                        let name = torrent.filename.clone();
                        cache_entries.push((
                            torrent_ino,
                            DataInode::TorrentRoot {
                                torrent_id: torrent.id,
                                source_path: torrent.source_path.clone(),
                                name: torrent.name.clone(),
                                filename: torrent.filename.clone(),
                            },
                        ));
                        entries.push((
                            torrent_ino,
                            offset_counter,
                            fuser::FileType::Directory,
                            name,
                        ));
                        offset_counter += 1;
                    }
                }

                for (cache_ino, cache_inode) in cache_entries {
                    inode_mgr.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentRoot {
                torrent_id,
                source_path,
                ..
            } => {
                entries.push((ino, 1, fuser::FileType::Directory, ".".to_string()));

                let parent_ino = if source_path.is_empty() {
                    DATA_INO
                } else {
                    let path_parts: Vec<&str> = source_path.split('/').collect();
                    if path_parts.len() == 1 {
                        DATA_INO
                    } else {
                        let parent_path = path_parts[..path_parts.len() - 1].join("/");
                        let db_guard = db.lock().ok()?;

                        let torrents = db_guard
                            .get_torrents_by_source_path(&parent_path)
                            .ok()
                            .unwrap_or_default();
                        if !torrents.is_empty() {
                            InodeManager::make_torrent_root_ino(torrents[0].id)
                        } else {
                            DATA_INO
                        }
                    }
                };
                entries.push((parent_ino, 2, fuser::FileType::Directory, "..".to_string()));

                {
                    let db_guard = db.lock().ok()?;

                    let mut offset_counter = 3i64;

                    let root_dirs = db_guard
                        .get_torrent_directories_by_parent(None, torrent_id)
                        .ok()?;
                    for dir in root_dirs {
                        let dir_ino = InodeManager::make_torrent_dir_ino(dir.id);
                        cache_entries.push((
                            dir_ino,
                            DataInode::TorrentDir {
                                torrent_id,
                                dir_id: dir.id,
                                name: dir.name.clone(),
                            },
                        ));
                        entries.push((
                            dir_ino,
                            offset_counter,
                            fuser::FileType::Directory,
                            dir.name,
                        ));
                        offset_counter += 1;
                    }

                    let root_files = db_guard.get_root_files(torrent_id).ok()?;
                    for file in root_files {
                        let file_ino = InodeManager::make_torrent_file_ino(file.id);
                        cache_entries.push((
                            file_ino,
                            DataInode::TorrentFile {
                                torrent_id,
                                file_id: file.id,
                                name: file.name.clone(),
                                size: file.size,
                            },
                        ));
                        entries.push((
                            file_ino,
                            offset_counter,
                            fuser::FileType::RegularFile,
                            file.name,
                        ));
                        offset_counter += 1;
                    }
                }

                for (cache_ino, cache_inode) in cache_entries {
                    inode_mgr.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentDir {
                torrent_id, dir_id, ..
            } => {
                entries.push((ino, 1, fuser::FileType::Directory, ".".to_string()));

                {
                    let db_guard = db.lock().ok()?;

                    let parent_ino = db_guard
                        .get_torrent_directory_by_id(dir_id)
                        .ok()
                        .flatten()
                        .and_then(|d| d.parent_id)
                        .map(InodeManager::make_torrent_dir_ino)
                        .unwrap_or_else(|| InodeManager::make_torrent_root_ino(torrent_id));
                    entries.push((parent_ino, 2, fuser::FileType::Directory, "..".to_string()));

                    let mut offset_counter = 3i64;

                    let sub_dirs = db_guard
                        .get_torrent_directories_by_parent(Some(dir_id), torrent_id)
                        .ok()?;
                    for dir in sub_dirs {
                        let sub_dir_ino = InodeManager::make_torrent_dir_ino(dir.id);
                        cache_entries.push((
                            sub_dir_ino,
                            DataInode::TorrentDir {
                                torrent_id,
                                dir_id: dir.id,
                                name: dir.name.clone(),
                            },
                        ));
                        entries.push((
                            sub_dir_ino,
                            offset_counter,
                            fuser::FileType::Directory,
                            dir.name,
                        ));
                        offset_counter += 1;
                    }

                    let dir_files = db_guard.get_files_in_directory(dir_id).ok()?;
                    for file in dir_files {
                        let file_ino = InodeManager::make_torrent_file_ino(file.id);
                        cache_entries.push((
                            file_ino,
                            DataInode::TorrentFile {
                                torrent_id,
                                file_id: file.id,
                                name: file.name.clone(),
                                size: file.size,
                            },
                        ));
                        entries.push((
                            file_ino,
                            offset_counter,
                            fuser::FileType::RegularFile,
                            file.name,
                        ));
                        offset_counter += 1;
                    }
                }

                for (cache_ino, cache_inode) in cache_entries {
                    inode_mgr.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentFile { .. } => {
                return None;
            }
        }

        Some(
            entries
                .into_iter()
                .filter(|(_, o, _, _)| *o > offset)
                .collect(),
        )
    }

    /// Get the DB reference, returning an error code on failure.
    pub fn get_db(db: &Option<Arc<Mutex<Database>>>) -> Result<&Arc<Mutex<Database>>, i32> {
        db.as_ref().ok_or_else(|| {
            error!("Database not available");
            libc::EIO
        })
    }
}
