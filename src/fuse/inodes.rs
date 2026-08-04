//! InodeManager — manages the in-memory inode tree for metadata/ and data/ subtrees.
//! Extracted from TorrentFs to separate inode management from FUSE protocol handling.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use crate::db;
use tracing::info;

// ── Inode constants ──
pub const ROOT_INO: u64 = 1;
pub const METADATA_INO: u64 = 2;
pub const DATA_INO: u64 = 3;
pub const STATS_INO: u64 = 4;
pub const MAX_TORRENT_SIZE: usize = 10 * 1024 * 1024;

// ── Data inode base addresses ──
pub const DATA_TORRENT_INO_BASE: u64 = 1_000_000;
pub const DATA_DIR_INO_BASE: u64 = 2_000_000;
pub const DATA_FILE_INO_BASE: u64 = 3_000_000;
pub const SOURCE_PATH_DIR_INO_BASE: u64 = 4_000_000;
pub const STATS_INO_OFFSET: u64 = 10_000_000;

pub static NEXT_INO: AtomicU64 = AtomicU64::new(5);
pub static NEXT_FH: AtomicU64 = AtomicU64::new(1);

// ── Inode data types ──

#[derive(Clone, Debug)]
pub enum InodeData {
    Directory {
        parent: u64,
        name: String,
    },
    File {
        parent: u64,
        name: String,
        data: Vec<u8>,
    },
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum DataInode {
    SourcePathDir {
        path: String,
    },
    TorrentRoot {
        torrent_id: i64,
        source_path: String,
        name: String,
        filename: String,
    },
    TorrentDir {
        torrent_id: i64,
        dir_id: i64,
        name: String,
    },
    TorrentFile {
        torrent_id: i64,
        file_id: i64,
        name: String,
        size: i64,
    },
}

// ── InodeManager ──

pub struct InodeManager {
    pub inodes: HashMap<u64, InodeData>,
    pub data_inodes: HashMap<u64, DataInode>,
    pub open_files: HashMap<u64, u64>,
    pub creation_time: Duration,
}

impl InodeManager {
    pub fn new(creation_time: Duration) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(
            ROOT_INO,
            InodeData::Directory {
                parent: 0,
                name: String::new(),
            },
        );
        inodes.insert(
            METADATA_INO,
            InodeData::Directory {
                parent: ROOT_INO,
                name: "metadata".to_string(),
            },
        );
        inodes.insert(
            DATA_INO,
            InodeData::Directory {
                parent: ROOT_INO,
                name: "data".to_string(),
            },
        );
        inodes.insert(
            STATS_INO,
            InodeData::File {
                parent: ROOT_INO,
                name: ".stats".to_string(),
                data: Vec::new(),
            },
        );

        Self {
            inodes,
            data_inodes: HashMap::new(),
            open_files: HashMap::new(),
            creation_time,
        }
    }

    // ── Inode ID generators ──

    pub fn make_torrent_root_ino(torrent_id: i64) -> u64 {
        DATA_TORRENT_INO_BASE + (torrent_id as u64)
    }

    pub fn make_torrent_dir_ino(dir_id: i64) -> u64 {
        DATA_DIR_INO_BASE + (dir_id as u64)
    }

    pub fn make_torrent_file_ino(file_id: i64) -> u64 {
        DATA_FILE_INO_BASE + (file_id as u64)
    }

    pub fn make_source_path_dir_ino(path: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        SOURCE_PATH_DIR_INO_BASE + (hasher.finish() % 1_000_000)
    }

    pub fn is_data_ino(ino: u64) -> bool {
        ino >= DATA_TORRENT_INO_BASE
    }

    /// Derive a stats inode from a directory inode.
    pub fn make_stats_ino(dir_ino: u64) -> u64 {
        dir_ino + STATS_INO_OFFSET
    }

    /// Check if an inode is a stats inode (derived from a directory inode).
    pub fn is_stats_ino(ino: u64) -> bool {
        ino >= STATS_INO_OFFSET && ino < STATS_INO_OFFSET + 10_000_000
    }

    /// Get the parent directory inode from a stats inode.
    pub fn stats_ino_to_dir_ino(ino: u64) -> Option<u64> {
        if Self::is_stats_ino(ino) {
            Some(ino - STATS_INO_OFFSET)
        } else {
            None
        }
    }

    // ── Path resolution ──

    pub fn get_full_path(&self, ino: u64) -> String {
        let mut path_parts = Vec::new();
        let mut current_ino = ino;

        while current_ino != ROOT_INO && current_ino != 0 {
            if let Some(data) = self.inodes.get(&current_ino) {
                match data {
                    InodeData::Directory { parent, name } => {
                        if !name.is_empty() {
                            path_parts.push(name.clone());
                        }
                        current_ino = *parent;
                    }
                    InodeData::File { parent, name, .. } => {
                        path_parts.push(name.clone());
                        current_ino = *parent;
                    }
                }
            } else {
                break;
            }
        }

        path_parts.reverse();
        path_parts.join("/")
    }

    pub fn is_metadata_child(&self, ino: u64) -> bool {
        if ino == METADATA_INO {
            return true;
        }

        let mut current_ino = ino;
        while current_ino != ROOT_INO && current_ino != 0 {
            if let Some(data) = self.inodes.get(&current_ino) {
                match data {
                    InodeData::Directory { parent, .. } => {
                        if *parent == METADATA_INO || current_ino == METADATA_INO {
                            return true;
                        }
                        current_ino = *parent;
                    }
                    InodeData::File { parent, .. } => {
                        current_ino = *parent;
                    }
                }
            } else {
                break;
            }
        }
        false
    }

    pub fn find_child_by_name(&self, parent: u64, name: &str) -> Option<u64> {
        for (ino, data) in &self.inodes {
            match data {
                InodeData::Directory { parent: p, name: n } if *p == parent && n == name => {
                    return Some(*ino);
                }
                InodeData::File {
                    parent: p, name: n, ..
                } if *p == parent && n == name => {
                    return Some(*ino);
                }
                _ => {}
            }
        }
        None
    }

    pub fn find_ino_by_full_path(&self, target_path: &str) -> Option<u64> {
        for (ino, data) in &self.inodes {
            let full_path = match data {
                InodeData::Directory { .. } | InodeData::File { .. } => self.get_full_path(*ino),
            };
            if full_path == target_path {
                return Some(*ino);
            }
        }
        None
    }

    pub fn extract_source_path(&self, parent: u64) -> String {
        if parent == METADATA_INO {
            return String::new();
        }

        let full_path = self.get_full_path(parent);
        if let Some(stripped) = full_path.strip_prefix("metadata/") {
            stripped.to_string()
        } else {
            full_path
        }
    }

    // ── Inode restoration from DB ──

    /// Restore metadata/ subdirectory inodes from the database on startup.
    pub fn restore_metadata_inodes(
        &mut self,
        dirs: Vec<(i64, Option<i64>, String, String)>,
        torrents: Vec<db::Torrent>,
    ) {
        let mut sorted_dirs = dirs;
        sorted_dirs.sort_by(|a, b| {
            let depth_a = a.3.matches('/').count();
            let depth_b = b.3.matches('/').count();
            depth_a.cmp(&depth_b)
        });

        let mut dir_id_to_ino: HashMap<i64, u64> = HashMap::new();

        for (db_id, parent_db_id, name, path) in &sorted_dirs {
            let parent_ino = if let Some(pid) = parent_db_id {
                *dir_id_to_ino.get(pid).unwrap_or(&METADATA_INO)
            } else {
                METADATA_INO
            };

            let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
            dir_id_to_ino.insert(*db_id, new_ino);

            self.inodes.insert(
                new_ino,
                InodeData::Directory {
                    parent: parent_ino,
                    name: name.clone(),
                },
            );

            info!(
                "Restored metadata inode {} for path '{}' (db_id={}, parent_ino={})",
                new_ino, path, db_id, parent_ino
            );
        }

        for torrent in &torrents {
            let parent_ino = if torrent.source_path.is_empty() {
                METADATA_INO
            } else {
                let full_source = format!("metadata/{}", torrent.source_path);
                self.find_ino_by_full_path(&full_source)
                    .unwrap_or(METADATA_INO)
            };

            let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
            let filename = if !torrent.filename.is_empty() {
                if torrent.filename.ends_with(".torrent") {
                    torrent.filename.clone()
                } else {
                    format!("{}.torrent", torrent.filename)
                }
            } else {
                if torrent.name.ends_with(".torrent") {
                    torrent.name.clone()
                } else {
                    format!("{}.torrent", torrent.name)
                }
            };
            self.inodes.insert(
                new_ino,
                InodeData::File {
                    parent: parent_ino,
                    name: filename,
                    data: torrent.torrent_data.clone().unwrap_or_default(),
                },
            );
        }
    }

    // ── FUSE attribute helpers ──

    pub fn attr_for_dir(&self, ino: u64, writable: bool) -> fuser::FileAttr {
        fuser::FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH + self.creation_time,
            mtime: UNIX_EPOCH + self.creation_time,
            ctime: UNIX_EPOCH + self.creation_time,
            crtime: UNIX_EPOCH + self.creation_time,
            kind: fuser::FileType::Directory,
            perm: if writable { 0o755 } else { 0o555 },
            nlink: 2,
            // SAFETY: libc::getuid() / libc::getgid() are always safe to call
            // in POSIX environments — they simply return the current process's
            // real user/group IDs and have no preconditions.
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    pub fn attr_for_file(&self, ino: u64, size: u64) -> fuser::FileAttr {
        fuser::FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: UNIX_EPOCH + self.creation_time,
            mtime: UNIX_EPOCH + self.creation_time,
            ctime: UNIX_EPOCH + self.creation_time,
            crtime: UNIX_EPOCH + self.creation_time,
            kind: fuser::FileType::RegularFile,
            perm: 0o444,
            nlink: 1,
            // SAFETY: libc::getuid() / libc::getgid() are always safe to call
            // in POSIX environments — they simply return the current process's
            // real user/group IDs and have no preconditions.
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── stats ino derivation round-trip ──

    #[test]
    fn test_make_stats_ino_derives_from_dir_ino() {
        assert_eq!(
            InodeManager::make_stats_ino(DATA_INO),
            DATA_INO + STATS_INO_OFFSET
        );
        assert_eq!(
            InodeManager::make_stats_ino(ROOT_INO),
            ROOT_INO + STATS_INO_OFFSET
        );
        assert_eq!(
            InodeManager::make_stats_ino(METADATA_INO),
            METADATA_INO + STATS_INO_OFFSET
        );
    }

    #[test]
    fn test_is_stats_ino_true_for_derived_inos() {
        assert!(InodeManager::is_stats_ino(InodeManager::make_stats_ino(
            DATA_INO
        )));
        assert!(InodeManager::is_stats_ino(InodeManager::make_stats_ino(
            ROOT_INO
        )));
    }

    #[test]
    fn test_is_stats_ino_false_for_regular_inos() {
        assert!(!InodeManager::is_stats_ino(DATA_INO));
        assert!(!InodeManager::is_stats_ino(ROOT_INO));
        assert!(!InodeManager::is_stats_ino(METADATA_INO));
        assert!(!InodeManager::is_stats_ino(0));
    }

    #[test]
    fn test_is_stats_ino_boundaries() {
        // STATS_INO_OFFSET (10_000_000) is included, STATS_INO_OFFSET + 10_000_000 is excluded
        assert!(InodeManager::is_stats_ino(STATS_INO_OFFSET));
        assert!(InodeManager::is_stats_ino(
            STATS_INO_OFFSET + 10_000_000 - 1
        ));
        assert!(!InodeManager::is_stats_ino(STATS_INO_OFFSET + 10_000_000));
        assert!(!InodeManager::is_stats_ino(STATS_INO_OFFSET - 1));
    }

    #[test]
    fn test_stats_ino_to_dir_ino_round_trip() {
        let dir_ino = DATA_INO;
        let stats_ino = InodeManager::make_stats_ino(dir_ino);
        assert_eq!(InodeManager::stats_ino_to_dir_ino(stats_ino), Some(dir_ino));
    }

    #[test]
    fn test_stats_ino_to_dir_ino_none_for_regular_ino() {
        assert_eq!(InodeManager::stats_ino_to_dir_ino(DATA_INO), None);
        assert_eq!(InodeManager::stats_ino_to_dir_ino(ROOT_INO), None);
        assert_eq!(InodeManager::stats_ino_to_dir_ino(0), None);
    }
}
