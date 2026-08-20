//! `FsService` — the filesystem application service layer.
//!
//! This is where the domain logic lives: inode resolution, `.torrent`
//! lifecycle persistence, data reads (local vs. remote), and `.stats`
//! rendering. `FsService` imports **neither** `fuser` **nor** `libc`; it
//! returns domain results (`FsError`, `Attr`, `ReadOutcome`, …) and leaves all
//! protocol/errno adaptation to `TorrentFs` (the thin adapter in `super::mod`).
//!
//! See the architecture design doc §5.2 for the namespace × operation table.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::{error, info, warn};

use sha1_smol::Sha1;

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::db::Database;
use crate::domain::fs_error::{FsError, FsResult};
use crate::infrastructure::metrics::Metrics;
use crate::metadata::TorrentInfo;
use crate::seeding::SeedingManager;
use crate::services::download::DownloadService;
use crate::services::seeding::SeedingService;
use crate::services::torrent::TorrentService;

use super::fs_types::{
    Attr, Created, DirEntry, Entry, FileKind, OpenOutcome, ReadOutcome, StatsKind,
};
use super::inodes::{
    DataInode, InodeData, InodeManager, DATA_DIR_INO_BASE, DATA_INO, DATA_TORRENT_INO_BASE,
    MAX_TORRENT_SIZE, METADATA_INO, NEXT_FH, NEXT_INO, ROOT_INO, STATS_INO,
};
#[cfg(test)]
use super::inodes::{DATA_FILE_INO_BASE, SOURCE_PATH_DIR_INO_BASE};
use super::lookup::DataResolver;
use super::stats::{generate_directory_stats, generate_global_stats, generate_torrent_stats};

pub struct FsService {
    pub inode_mgr: InodeManager,
    pub db: Option<Arc<Mutex<Database>>>,
    pub torrent_service: Option<TorrentService>,
    pub processing_torrents: Arc<Mutex<HashMap<(String, String), ()>>>,
    pub download_service: Option<Arc<DownloadService>>,
    pub seeding_manager: Option<Arc<SeedingManager>>,
    pub torrent_data_cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    pub torrent_info_cache: Arc<Mutex<HashMap<String, Arc<TorrentInfo>>>>,
    pub listen_addr: String,
    pub metrics: Arc<Metrics>,
}

impl FsService {
    // ── Construction ────────────────────────────────────────────────────────

    pub fn new_with_cache_path(cache_path: PathBuf, config: &TorrentfsConfig) -> Self {
        if !cache_path.exists() {
            if let Err(e) = std::fs::create_dir_all(&cache_path) {
                warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
            }
        }
        let metrics = Arc::new(Metrics::new());
        let download_service =
            DownloadService::new_with_metrics(cache_path.as_path(), config, metrics.clone())
                .ok()
                .map(Arc::new);

        // Create the SeedingManager and register it as the CacheManager
        // eviction callback.  The Arc is kept on FsService so it can be
        // shared with TorrentService for seed removal on unlink (TSI-2232).
        let seeding_manager = match &download_service {
            Some(ds) => match SeedingService::new(&cache_path, config) {
                Ok(seeding_svc) => {
                    let sm = seeding_svc.get_seeding_manager();
                    ds.register_seeding_callback(sm.clone());
                    info!("SeedingManager registered as CacheManager eviction callback");
                    Some(sm)
                }
                Err(e) => {
                    warn!(
                        "SeedingService initialization failed; seeding disabled: {:?}",
                        e
                    );
                    None
                }
            },
            None => None,
        };

        let creation_time = Duration::from_secs(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        let listen_addr = config
            .connections
            .listen_interfaces
            .clone()
            .unwrap_or_else(|| "0.0.0.0:6881".to_string());
        Self {
            inode_mgr: InodeManager::new(creation_time),
            db: None,
            torrent_service: None,
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            download_service,
            seeding_manager,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr,
            metrics,
        }
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::new_with_cache_path(
            PathBuf::from("/tmp/torrentfs-cache"),
            &TorrentfsConfig::default_config(),
        )
    }

    pub fn new_with_db_and_cache(
        db: Database,
        cache_path: PathBuf,
        config: &TorrentfsConfig,
    ) -> Self {
        let mut svc = Self::new_with_cache_path(cache_path, config);

        // Collect data from database before moving it.
        let (dirs, torrents, torrent_datas) = {
            let dirs = db.get_all_metadata_directories().unwrap_or_default();
            let torrents = db.get_all_torrents().unwrap_or_default();
            let torrent_datas: Vec<Vec<u8>> = torrents
                .iter()
                .filter_map(|t| t.torrent_data.clone())
                .collect();
            (dirs, torrents, torrent_datas)
        };

        let db_arc = Arc::new(Mutex::new(db));
        svc.torrent_service = Some(TorrentService::new(
            db_arc.clone(),
            svc.download_service.clone(),
            svc.seeding_manager.clone(),
        ));
        svc.db = Some(db_arc);
        svc.inode_mgr.restore_metadata_inodes(dirs, torrents);

        // Recreate lightweight libtorrent handles for all persisted torrents so
        // peer/seed information and `.stats` piece status are visible without a
        // read (TSI-2112 / TSI-2133).
        let mut infos_by_hash: HashMap<String, Arc<TorrentInfo>> = HashMap::new();
        if let Some(ds) = &svc.download_service {
            for data in torrent_datas {
                match TorrentInfo::from_bytes(data) {
                    Ok(info) => {
                        let name = info.name();
                        let info = Arc::new(info);
                        if let Ok(ih) = info.info_hash() {
                            infos_by_hash.insert(hex::encode(ih), info.clone());
                        }
                        if let Err(e) = ds.ensure_handle_lightweight(info) {
                            warn!(
                                "Failed to restore lightweight handle for torrent '{}': {:?}",
                                name, e
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse stored torrent data on startup: {:?}", e);
                    }
                }
            }
        }

        // TSI-2199: background SHA-1 verification of on-disk cached pieces that
        // the startup scan re-registered as unverified. Runs asynchronously so
        // FUSE mount readiness is never blocked.
        if !infos_by_hash.is_empty() {
            if let Some(cache) = svc.get_cache_manager() {
                spawn_cache_verification(cache, infos_by_hash);
            }
        }

        svc
    }

    /// Get the CacheManager shared with DownloadService.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        self.download_service
            .as_ref()
            .and_then(|ds| ds.get_cache_manager())
    }

    // ── Namespace-agnostic queries ──────────────────────────────────────────

    pub fn lookup(&mut self, parent: u64, name: &str) -> FsResult<Option<Entry>> {
        if parent == ROOT_INO {
            return Ok(match name {
                "metadata" => Some(Entry {
                    ino: METADATA_INO,
                    attr: self.inode_mgr.attr_for_dir(METADATA_INO, true),
                }),
                "data" => Some(Entry {
                    ino: DATA_INO,
                    attr: self.inode_mgr.attr_for_dir(DATA_INO, false),
                }),
                ".stats" => {
                    let stats_size = self.read_stats(StatsKind::Global).len() as u64;
                    Some(Entry {
                        ino: STATS_INO,
                        attr: self.inode_mgr.attr_for_file(STATS_INO, stats_size),
                    })
                }
                _ => None,
            });
        }

        // Handle .stats virtual file for data/ subtree directories.
        if name == ".stats" {
            if parent == DATA_INO {
                let stats_ino = InodeManager::make_stats_ino(parent);
                let content = self.generate_dir_stats_content("");
                return Ok(Some(Entry {
                    ino: stats_ino,
                    attr: self
                        .inode_mgr
                        .attr_for_file(stats_ino, content.len() as u64),
                }));
            }

            if let Some(data_inode) = self.inode_mgr.data_inodes.get(&parent) {
                match data_inode {
                    DataInode::SourcePathDir { path } => {
                        let stats_ino = InodeManager::make_stats_ino(parent);
                        let content = self.generate_dir_stats_content(path);
                        return Ok(Some(Entry {
                            ino: stats_ino,
                            attr: self
                                .inode_mgr
                                .attr_for_file(stats_ino, content.len() as u64),
                        }));
                    }
                    DataInode::TorrentRoot { torrent_id, .. } => {
                        let stats_ino = InodeManager::make_stats_ino(parent);
                        let content = self.generate_torrent_stats_for_id(*torrent_id);
                        return Ok(Some(Entry {
                            ino: stats_ino,
                            attr: self
                                .inode_mgr
                                .attr_for_file(stats_ino, content.len() as u64),
                        }));
                    }
                    _ => return Ok(None),
                }
            }
            // Parent not yet cached in data_inodes — fall through to the normal
            // data/ lookup below.
        }

        if parent == DATA_INO || InodeManager::is_data_ino(parent) {
            if let Some(db) = &self.db {
                if let Some((ino, kind, size)) =
                    DataResolver::lookup_data_inode(&mut self.inode_mgr, db, parent, name)
                {
                    let attr = match kind {
                        FileKind::Directory => self.inode_mgr.attr_for_dir(ino, false),
                        FileKind::RegularFile => self.inode_mgr.attr_for_file(ino, size),
                    };
                    return Ok(Some(Entry { ino, attr }));
                }
            }
            return Ok(None);
        }

        if let Some(child_ino) = self.inode_mgr.find_child_by_name(parent, name) {
            if let Some(data) = self.inode_mgr.inodes.get(&child_ino) {
                let attr = match data {
                    InodeData::Directory { .. } => self.inode_mgr.attr_for_dir(child_ino, true),
                    InodeData::File {
                        data: file_data, ..
                    } => self
                        .inode_mgr
                        .attr_for_file(child_ino, file_data.len() as u64),
                };
                return Ok(Some(Entry {
                    ino: child_ino,
                    attr,
                }));
            }
        }

        Ok(None)
    }

    pub fn getattr(&mut self, ino: u64) -> FsResult<Attr> {
        match ino {
            ROOT_INO => Ok(self.inode_mgr.attr_for_dir(ino, false)),
            METADATA_INO => Ok(self.inode_mgr.attr_for_dir(ino, true)),
            DATA_INO => Ok(self.inode_mgr.attr_for_dir(ino, false)),
            STATS_INO => {
                let stats_size = self.generate_global_stats_content().len() as u64;
                Ok(self.inode_mgr.attr_for_file(ino, stats_size))
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let content = self.generate_data_stats_for_ino(ino);
                    return Ok(self.inode_mgr.attr_for_file(ino, content.len() as u64));
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(data_inode) = self.inode_mgr.data_inodes.get(&ino) {
                        return Ok(match data_inode {
                            DataInode::SourcePathDir { .. }
                            | DataInode::TorrentRoot { .. }
                            | DataInode::TorrentDir { .. } => {
                                self.inode_mgr.attr_for_dir(ino, false)
                            }
                            DataInode::TorrentFile { size, .. } => {
                                self.inode_mgr.attr_for_file(ino, *size as u64)
                            }
                        });
                    }

                    let torrent_id = (ino - DATA_TORRENT_INO_BASE) as i64;
                    if (DATA_TORRENT_INO_BASE..DATA_DIR_INO_BASE).contains(&ino) {
                        if self
                            .torrent_service
                            .as_ref()
                            .map(|ts| ts.torrent_exists_by_id(torrent_id))
                            .unwrap_or(false)
                        {
                            return Ok(self.inode_mgr.attr_for_dir(ino, false));
                        } else {
                            return Err(FsError::NotFound);
                        }
                    }

                    return Err(FsError::NotFound);
                }

                if let Some(data) = &self.inode_mgr.inodes.get(&ino) {
                    match data {
                        InodeData::Directory { .. } => Ok(self
                            .inode_mgr
                            .attr_for_dir(ino, self.inode_mgr.is_metadata_child(ino))),
                        InodeData::File {
                            data: file_data, ..
                        } => Ok(self.inode_mgr.attr_for_file(ino, file_data.len() as u64)),
                    }
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }

    pub fn readdir(&mut self, ino: u64, offset: i64) -> FsResult<Vec<DirEntry>> {
        if ino == DATA_INO || InodeManager::is_data_ino(ino) {
            if let Some(db) = &self.db {
                if let Some(entries) =
                    DataResolver::readdir_data(&mut self.inode_mgr, db, ino, offset)
                {
                    return Ok(entries
                        .into_iter()
                        .map(|(entry_ino, entry_offset, kind, name)| DirEntry {
                            ino: entry_ino,
                            offset: entry_offset,
                            kind,
                            name,
                        })
                        .collect());
                }
            }
            return Err(FsError::NotFound);
        }

        let mut entries: Vec<DirEntry> = vec![DirEntry {
            ino,
            offset: 1,
            kind: FileKind::Directory,
            name: ".".to_string(),
        }];

        if ino == ROOT_INO {
            entries.push(DirEntry {
                ino: ROOT_INO,
                offset: 2,
                kind: FileKind::Directory,
                name: "..".to_string(),
            });
            entries.push(DirEntry {
                ino: METADATA_INO,
                offset: 3,
                kind: FileKind::Directory,
                name: "metadata".to_string(),
            });
            entries.push(DirEntry {
                ino: DATA_INO,
                offset: 4,
                kind: FileKind::Directory,
                name: "data".to_string(),
            });
            entries.push(DirEntry {
                ino: STATS_INO,
                offset: 5,
                kind: FileKind::RegularFile,
                name: ".stats".to_string(),
            });
        } else if let Some(InodeData::Directory { parent, .. }) = self.inode_mgr.inodes.get(&ino) {
            entries.push(DirEntry {
                ino: *parent,
                offset: 2,
                kind: FileKind::Directory,
                name: "..".to_string(),
            });

            let mut offset_counter = entries.len() as i64 + 1;
            // Collect entries first to avoid borrowing issues.
            let children: Vec<(u64, FileKind, String)> = self
                .inode_mgr
                .inodes
                .iter()
                .filter_map(|(child_ino, data)| match data {
                    InodeData::Directory {
                        parent: p, name, ..
                    } if *p == ino && !name.is_empty() => {
                        Some((*child_ino, FileKind::Directory, name.clone()))
                    }
                    InodeData::File {
                        parent: p, name, ..
                    } if *p == ino => Some((*child_ino, FileKind::RegularFile, name.clone())),
                    _ => None,
                })
                .collect();

            for (child_ino, kind, name) in children {
                entries.push(DirEntry {
                    ino: child_ino,
                    offset: offset_counter,
                    kind,
                    name,
                });
                offset_counter += 1;
            }
        } else {
            return Err(FsError::NotDirectory);
        }

        Ok(entries.into_iter().filter(|e| e.offset > offset).collect())
    }

    /// Open a file, returning the file handle and open-mode hints.
    ///
    /// Data torrent files set `direct_io: true` so the adapter applies
    /// `FOPEN_DIRECT_IO`, bypassing the kernel page cache — this lets the
    /// daemon's errno (e.g. ENODATA for "no seeder") reach userspace instead
    /// of being converted to EIO by `filemap_read_folio` (TSI-2246).
    pub fn open(&mut self, ino: u64) -> FsResult<OpenOutcome> {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Ok(0.into()),
            STATS_INO => {
                let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                self.inode_mgr.open_files.insert(fh, ino);
                Ok(fh.into())
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.inode_mgr.open_files.insert(fh, ino);
                    return Ok(fh.into());
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile { .. }) =
                        self.inode_mgr.data_inodes.get(&ino)
                    {
                        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                        self.inode_mgr.open_files.insert(fh, ino);
                        // Bypass page cache so read errors propagate directly.
                        Ok(OpenOutcome {
                            fh,
                            direct_io: true,
                        })
                    } else {
                        Ok(0.into())
                    }
                } else if self.inode_mgr.inodes.contains_key(&ino) {
                    let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.inode_mgr.open_files.insert(fh, ino);
                    Ok(fh.into())
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }
    pub fn opendir(&mut self, ino: u64) -> FsResult<()> {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Ok(()),
            _ => {
                if InodeManager::is_data_ino(ino) || self.inode_mgr.inodes.contains_key(&ino) {
                    Ok(())
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }

    // ── Metadata namespace (.torrent lifecycle) ─────────────────────────────

    pub fn mknod(&mut self, parent: u64, name: &str) -> FsResult<Entry> {
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::PermissionDenied);
        }

        if self.inode_mgr.find_child_by_name(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name.to_string(),
                data: Vec::new(),
            },
        );

        info!(
            "Created file {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(Entry {
            ino: new_ino,
            attr: self.inode_mgr.attr_for_file(new_ino, 0),
        })
    }

    pub fn create(&mut self, parent: u64, name: &str) -> FsResult<Created> {
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::PermissionDenied);
        }

        if self.inode_mgr.find_child_by_name(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name.to_string(),
                data: Vec::new(),
            },
        );

        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.open_files.insert(fh, new_ino);

        info!(
            "Created file {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(Created {
            attr: self.inode_mgr.attr_for_file(new_ino, 0),
            fh,
        })
    }

    pub fn write(&mut self, ino: u64, offset: i64, data: &[u8]) -> FsResult<u32> {
        if ino == STATS_INO || InodeManager::is_stats_ino(ino) {
            return Err(FsError::ReadOnlyFileSystem);
        }

        // TSI-2228: data/ namespace is read-only. Data inodes live in
        // `data_inodes`, not `inodes`, so without this guard `write` would
        // fall through to the `None` arm and return `ENOENT` — the inode
        // exists, it is just not writable. Return `EROFS` instead.
        if InodeManager::is_data_namespace(ino) {
            return Err(FsError::ReadOnlyFileSystem);
        }

        match self.inode_mgr.inodes.get_mut(&ino) {
            Some(InodeData::File {
                data: file_data,
                name,
                ..
            }) => {
                let offset = offset as usize;

                if offset > file_data.len() {
                    file_data.resize(offset, 0);
                }

                if offset + data.len() > file_data.len() {
                    file_data.resize(offset + data.len(), 0);
                }

                file_data[offset..offset + data.len()].copy_from_slice(data);

                info!("Wrote {} bytes to file {}", data.len(), name);
                Ok(data.len() as u32)
            }
            Some(InodeData::Directory { .. }) => Err(FsError::IsDirectory),
            None => Err(FsError::NotFound),
        }
    }

    /// Validate a buffered `.torrent` on close (flush).
    pub fn flush(&mut self, ino: u64) -> FsResult<()> {
        if let Some(InodeData::File { data, name, .. }) = self.inode_mgr.inodes.get(&ino) {
            if name.ends_with(".torrent") {
                if data.is_empty() {
                    warn!("Zero-byte torrent file {} rejected", name);
                    return Err(FsError::InvalidArgument);
                }

                if data.len() > MAX_TORRENT_SIZE {
                    warn!(
                        "Torrent file {} exceeds size limit ({} bytes)",
                        name,
                        data.len()
                    );
                    return Err(FsError::FileTooLarge(format!(
                        "{} exceeds {} bytes",
                        name, MAX_TORRENT_SIZE
                    )));
                }

                match TorrentInfo::from_bytes(data.clone()) {
                    Ok(_) => {
                        info!("Torrent {} validated successfully", name);
                    }
                    Err(e) => {
                        warn!("Invalid torrent file {}: {:?}", name, e);
                        return Err(FsError::InvalidArgument);
                    }
                }
            }
        }
        Ok(())
    }

    /// Persist a closed `.torrent` into the database (release).
    pub fn release(&mut self, fh: u64) -> FsResult<()> {
        if let Some(ino) = self.inode_mgr.open_files.remove(&fh) {
            if let Some(InodeData::File { data, name, parent }) =
                self.inode_mgr.inodes.get(&ino).cloned()
            {
                if name.ends_with(".torrent") {
                    if data.is_empty() {
                        warn!("Zero-byte torrent file {} removed", name);
                        self.inode_mgr.inodes.remove(&ino);
                        return Ok(());
                    }

                    if data.len() > MAX_TORRENT_SIZE {
                        self.inode_mgr.inodes.remove(&ino);
                        return Ok(());
                    }

                    if TorrentInfo::from_bytes(data.clone()).is_err() {
                        warn!("Torrent {} invalid, removing inode", name);
                        self.inode_mgr.inodes.remove(&ino);
                        return Ok(());
                    }

                    let source_path = self.inode_mgr.extract_source_path(parent);
                    let dedup_key = (source_path.clone(), name.clone());

                    // TSI-2247: Dedup guard — check + insert atomically, then
                    // DROP the lock before spawning background work.  The key
                    // is `(source_path, filename)` so that torrents in the
                    // same directory (especially the root, where
                    // `source_path` is `""`) don't collide.  The previous
                    // code held `processing_torrents` during the entire
                    // `add_torrent` call (DB insert + handle creation),
                    // blocking the single-threaded FUSE dispatch loop.  Now
                    // `add_torrent` runs on a detached thread so `release`
                    // never blocks the dispatcher.
                    {
                        let mut processing = self.processing_torrents.lock().map_err(|e| {
                            error!("Mutex poisoned in release(): {}", e);
                            FsError::LockPoisoned
                        })?;

                        if processing.contains_key(&dedup_key) {
                            warn!(
                                "Torrent at '{}{}' already being processed, skipping",
                                source_path, name
                            );
                            return Ok(());
                        }
                        processing.insert(dedup_key.clone(), ());
                    }
                    // Lock released here.

                    // The inode is NOT removed here: if the background
                    // `add_torrent` fails, the file must remain visible so
                    // the user can retry or delete it.  On success the
                    // DB-backed entry supersedes this inode (lookup queries
                    // the DB), so keeping it is harmless — this mirrors the
                    // pre-TSI-2247 behavior.
                    if let Some(ts) = &self.torrent_service {
                        let ts = ts.clone();
                        let processing = self.processing_torrents.clone();
                        let key = dedup_key.clone();
                        let fname = name.clone();
                        std::thread::spawn(move || {
                            match ts.add_torrent(&data, &source_path, &name) {
                                Ok(()) => {
                                    info!("Successfully processed torrent: {}", fname);
                                }
                                Err(e) => {
                                    error!("Failed to process torrent {}: {}", fname, e);
                                }
                            }
                            if let Ok(mut guard) = processing.lock() {
                                guard.remove(&key);
                            }
                        });
                    } else {
                        info!(
                            "Torrent {} received (no DB configured, skipping insert)",
                            name
                        );
                        if let Ok(mut guard) = self.processing_torrents.lock() {
                            guard.remove(&dedup_key);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn mkdir(&mut self, parent: u64, name: &str) -> FsResult<Attr> {
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if self.inode_mgr.find_child_by_name(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::Directory {
                parent,
                name: name.to_string(),
            },
        );

        // Persist the directory to the database.
        let source_path = if parent == METADATA_INO {
            name.to_string()
        } else {
            let parent_source_path = self.inode_mgr.extract_source_path(parent);
            if parent_source_path.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", parent_source_path, name)
            }
        };

        if let Some(ref ts) = self.torrent_service {
            let _ = ts.ensure_metadata_directories(&source_path);
        }

        info!(
            "Created directory {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(self.inode_mgr.attr_for_dir(new_ino, true))
    }

    pub fn rmdir(&mut self, parent: u64, name: &str) -> FsResult<()> {
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        let ino = match self.inode_mgr.find_child_by_name(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        match self.inode_mgr.inodes.get(&ino) {
            Some(InodeData::Directory { .. }) => {
                let has_children = self.inode_mgr.inodes.iter().any(|(_, data)| match data {
                    InodeData::Directory { parent: p, .. } if *p == ino => true,
                    InodeData::File { parent: p, .. } if *p == ino => true,
                    _ => false,
                });

                if has_children {
                    return Err(FsError::DirectoryNotEmpty);
                }

                let source_path = self.inode_mgr.extract_source_path(ino);
                self.inode_mgr.inodes.remove(&ino);

                // Clean up data_inodes cache.
                self.inode_mgr
                    .data_inodes
                    .retain(|_, data_inode| match data_inode {
                        DataInode::SourcePathDir { path } => {
                            if path == &source_path {
                                false
                            } else if source_path.is_empty() {
                                true
                            } else {
                                !path.starts_with(&format!("{}/", source_path))
                            }
                        }
                        _ => true,
                    });

                if let Some(ref ts) = self.torrent_service {
                    let _ = ts.delete_metadata_directory(&source_path);
                }

                info!(
                    "Deleted directory '{}' (source_path='{}')",
                    name, source_path
                );
                Ok(())
            }
            Some(InodeData::File { .. }) => Err(FsError::NotDirectory),
            None => Err(FsError::NotFound),
        }
    }

    pub fn unlink(&mut self, parent: u64, name: &str) -> FsResult<Option<i64>> {
        let mut removed_id = None;
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::PermissionDenied);
        }

        let ino = match self.inode_mgr.find_child_by_name(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        match self.inode_mgr.inodes.get(&ino) {
            Some(InodeData::File {
                name,
                parent: file_parent,
                ..
            }) => {
                let filename = name.clone();
                let source_path = self.inode_mgr.extract_source_path(*file_parent);

                if let Some(ref ts) = self.torrent_service {
                    match ts.remove_torrent(&filename, &source_path) {
                        Ok(Some(torrent_id)) => {
                            removed_id = Some(torrent_id);
                            self.inode_mgr.inodes.remove(&ino);
                            self.inode_mgr
                                .open_files
                                .retain(|_, &mut open_ino| open_ino != ino);

                            // Clean up metadata directories left empty by this
                            // deletion so the data/ mirror no longer exposes
                            // orphaned directories. A cleanup failure is
                            // non-fatal: the dirs stay in the DB, so the
                            // cached SourcePathDir entries remain valid.
                            let cleaned = ts
                                .cleanup_orphaned_metadata_directories(&source_path)
                                .unwrap_or_default();

                            self.inode_mgr
                                .data_inodes
                                .retain(|_, data_inode| match data_inode {
                                    DataInode::TorrentRoot {
                                        torrent_id: tid, ..
                                    } => *tid != torrent_id,
                                    DataInode::TorrentDir {
                                        torrent_id: tid, ..
                                    } => *tid != torrent_id,
                                    DataInode::TorrentFile {
                                        torrent_id: tid, ..
                                    } => *tid != torrent_id,
                                    DataInode::SourcePathDir { path } => !cleaned.contains(path),
                                });

                            let mut processing = self.processing_torrents.lock().map_err(|e| {
                                error!("Mutex poisoned in unlink() processing_torrents: {}", e);
                                FsError::LockPoisoned
                            })?;
                            processing.remove(&(source_path.clone(), filename.clone()));
                            drop(processing);

                            let mut cache = self.torrent_data_cache.lock().map_err(|e| {
                                error!("Mutex poisoned in unlink() torrent_data_cache: {}", e);
                                FsError::LockPoisoned
                            })?;
                            cache.remove(&source_path);
                            drop(cache);

                            info!(
                                "Deleted torrent '{}' (id={}, source_path='{}')",
                                filename, torrent_id, source_path
                            );
                        }
                        Ok(None) => {
                            self.inode_mgr.inodes.remove(&ino);
                            self.inode_mgr
                                .open_files
                                .retain(|_, &mut open_ino| open_ino != ino);
                            info!("Deleted file '{}' (not yet in database)", filename);
                        }
                        Err(e) => {
                            error!("Failed to delete torrent: {:?}", e);
                            return Err(e);
                        }
                    }
                } else {
                    self.inode_mgr.inodes.remove(&ino);
                    self.inode_mgr
                        .open_files
                        .retain(|_, &mut open_ino| open_ino != ino);
                    info!("Deleted file '{}' (no database)", filename);
                }

                Ok(removed_id)
            }
            Some(InodeData::Directory { .. }) => Err(FsError::IsDirectory),
            None => Err(FsError::NotFound),
        }
    }

    pub fn rename(
        &mut self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
    ) -> FsResult<()> {
        // TSI-2228: data/ is a read-only namespace — renames into or out
        // of it must return `EROFS`, not `ENOENT` or `EPERM`. Check this
        // before parent existence: an inode number in the data range is
        // inherently read-only, regardless of whether it is currently
        // materialised.
        if InodeManager::is_data_namespace(parent) || InodeManager::is_data_namespace(newparent) {
            return Err(FsError::ReadOnlyFileSystem);
        }

        // Check parent existence in both inodes and data_inodes tables.
        let parent_exists = self.inode_mgr.inodes.contains_key(&parent)
            || self.inode_mgr.data_inodes.contains_key(&parent);
        if !parent_exists {
            return Err(FsError::NotFound);
        }

        let newparent_exists = self.inode_mgr.inodes.contains_key(&newparent)
            || self.inode_mgr.data_inodes.contains_key(&newparent);
        if !newparent_exists {
            return Err(FsError::NotFound);
        }

        if !self.inode_mgr.is_metadata_child(parent) || !self.inode_mgr.is_metadata_child(newparent)
        {
            return Err(FsError::NotPermitted);
        }

        let source_ino = match self.inode_mgr.find_child_by_name(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        if let Some(target_ino) = self.inode_mgr.find_child_by_name(newparent, newname) {
            if target_ino == source_ino {
                return Ok(());
            }
            return Err(FsError::AlreadyExists);
        }

        let is_directory = matches!(
            self.inode_mgr.inodes.get(&source_ino),
            Some(InodeData::Directory { .. })
        );

        if is_directory {
            // --- Directory rename ---
            let old_source_path = self.inode_mgr.extract_source_path(source_ino);
            let new_source_path = if newparent == METADATA_INO {
                newname.to_string()
            } else {
                let parent_path = self.inode_mgr.extract_source_path(newparent);
                if parent_path.is_empty() {
                    newname.to_string()
                } else {
                    format!("{}/{}", parent_path, newname)
                }
            };

            self.inode_mgr.inodes.insert(
                source_ino,
                InodeData::Directory {
                    parent: newparent,
                    name: newname.to_string(),
                },
            );

            // Update data_inodes cache.
            let old_prefix = format!("{}/", old_source_path);
            let new_prefix = format!("{}/", new_source_path);
            for data_inode in self.inode_mgr.data_inodes.values_mut() {
                match data_inode {
                    DataInode::SourcePathDir { path } => {
                        if path == &old_source_path {
                            *path = new_source_path.clone();
                        } else if path.starts_with(&old_prefix) {
                            *path = format!("{}{}", new_prefix, &path[old_prefix.len()..]);
                        }
                    }
                    DataInode::TorrentRoot { source_path, .. } => {
                        if source_path == &old_source_path {
                            *source_path = new_source_path.clone();
                        } else if source_path.starts_with(&old_prefix) {
                            *source_path =
                                format!("{}{}", new_prefix, &source_path[old_prefix.len()..]);
                        }
                    }
                    _ => {}
                }
            }

            // Persist to database.
            if let Some(ref ts) = self.torrent_service {
                ts.rename_metadata_directory(&old_source_path, newname, &new_source_path)?;
                info!(
                    "Renamed metadata directory '{}' to '{}' (source_path: '{}' -> '{}')",
                    name, newname, old_source_path, new_source_path
                );
            } else {
                info!(
                    "Renamed directory '{}' to '{}' (no database)",
                    name, newname
                );
            }

            Ok(())
        } else {
            // --- File rename ---
            if !name.ends_with(".torrent") || !newname.ends_with(".torrent") {
                return Err(FsError::PermissionDenied);
            }

            let (file_data, old_name) = match self.inode_mgr.inodes.get(&source_ino) {
                Some(InodeData::File { data, name, .. }) => (data.clone(), name.clone()),
                None => {
                    return Err(FsError::NotFound);
                }
                _ => unreachable!(),
            };

            self.inode_mgr.inodes.insert(
                source_ino,
                InodeData::File {
                    parent: newparent,
                    name: newname.to_string(),
                    data: file_data,
                },
            );

            if let Some(ref ts) = self.torrent_service {
                let old_source_path = self.inode_mgr.extract_source_path(parent);
                let new_source_path = self.inode_mgr.extract_source_path(newparent);

                ts.rename_torrent(&old_name, &old_source_path, newname, &new_source_path)?;
                info!(
                    "Renamed torrent '{}' to '{}' (source_path: '{}' -> '{}')",
                    old_name, newname, old_source_path, new_source_path
                );
            } else {
                info!("Renamed file '{}' to '{}' (no database)", old_name, newname);
            }

            Ok(())
        }
    }

    // ── Data namespace (read-only: local / remote) ──────────────────────────

    /// Serve a read request across all namespaces (EISDIR, stats, data, metadata).
    pub fn read(&mut self, ino: u64, offset: i64, size: u32) -> FsResult<ReadOutcome> {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Err(FsError::IsDirectory),
            STATS_INO => {
                let offset = offset as usize;
                let stats = self.generate_global_stats_content();
                if offset >= stats.len() {
                    Ok(ReadOutcome::Ready(Vec::new()))
                } else {
                    let end = std::cmp::min(offset + size as usize, stats.len());
                    Ok(ReadOutcome::Ready(stats[offset..end].to_vec()))
                }
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let stats = self.generate_data_stats_for_ino(ino);
                    let offset = offset as usize;
                    if offset >= stats.len() {
                        return Ok(ReadOutcome::Ready(Vec::new()));
                    }
                    let end = std::cmp::min(offset + size as usize, stats.len());
                    return Ok(ReadOutcome::Ready(stats[offset..end].to_vec()));
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile {
                        torrent_id,
                        file_id,
                        name,
                        size: file_size,
                    }) = self.inode_mgr.data_inodes.get(&ino)
                    {
                        let offset = offset as usize;
                        let read_size = size as usize;

                        info!(
                            "Read request for torrent file: {} (torrent_id={}, file_id={}, offset={}, size={})",
                            name, torrent_id, file_id, offset, read_size
                        );

                        let actual_size = *file_size as usize;
                        if offset >= actual_size {
                            return Ok(ReadOutcome::Ready(Vec::new()));
                        }

                        let end = std::cmp::min(offset + read_size, actual_size);
                        let result_size = end - offset;

                        return self.read_data(
                            *torrent_id,
                            *file_id,
                            offset as u64,
                            result_size as u32,
                        );
                    }

                    return Err(FsError::NotFound);
                }

                if let Some(InodeData::File { data, .. }) = &self.inode_mgr.inodes.get(&ino) {
                    let offset = offset as usize;
                    let end = std::cmp::min(offset + size as usize, data.len());
                    if offset < data.len() {
                        Ok(ReadOutcome::Ready(data[offset..end].to_vec()))
                    } else {
                        Ok(ReadOutcome::Ready(Vec::new()))
                    }
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }

    /// Read torrent file data from local cache or the BitTorrent network.
    pub fn read_data(
        &mut self,
        torrent_id: i64,
        file_id: i64,
        offset: u64,
        size: u32,
    ) -> FsResult<ReadOutcome> {
        let ts = self.torrent_service.as_ref().ok_or_else(|| {
            error!("Torrent service not available");
            FsError::Internal("torrent service not available".to_string())
        })?;

        let (info_hash, _source_path, files) =
            ts.get_torrent_with_files(torrent_id)?.ok_or_else(|| {
                error!("Torrent not found: {}", torrent_id);
                FsError::NotFound
            })?;

        let _file = files.iter().find(|f| f.id == file_id).ok_or_else(|| {
            error!("File not found: {}", file_id);
            FsError::NotFound
        })?;

        let file_index = files.iter().position(|f| f.id == file_id).ok_or_else(|| {
            error!("File index not found for file_id: {}", file_id);
            FsError::Internal(format!("file index not found for file_id: {}", file_id))
        })? as i32;

        let cache_key = format!("{}:{}", info_hash, file_id);
        {
            let cache = self
                .torrent_data_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            if let Some(cached) = cache.get(&cache_key) {
                self.metrics.l1_hit();
                let offset = offset as usize;
                let end = std::cmp::min(offset + size as usize, cached.len());
                if offset < cached.len() {
                    return Ok(ReadOutcome::Ready(cached[offset..end].to_vec()));
                } else {
                    return Ok(ReadOutcome::Ready(Vec::new()));
                }
            }
        }
        self.metrics.l1_miss();

        if let Some(ds) = &self.download_service {
            // L3 (metadata) cache: parsed `TorrentInfo` keyed by info_hash,
            // avoiding a DB re-read + bencode re-parse on every read.
            let info = self.get_or_parse_torrent_info(&info_hash, torrent_id)?;

            // Fast pre-check: are all needed pieces already on disk?
            match ds.pieces_on_disk(&info, file_index, offset, size) {
                Ok(true) => {
                    self.metrics.l2_hit();
                    match ds.read_file_range(info.clone(), file_index, offset, size) {
                        Ok(data) => {
                            info!(
                                "Successfully read {} bytes from torrent file \
                                 (torrent_id={}, file_id={})",
                                data.len(),
                                torrent_id,
                                file_id
                            );
                            Ok(ReadOutcome::Ready(data))
                        }
                        Err(e) => {
                            warn!(
                                "Failed to read from torrent file (torrent_id={}, file_id={}): {}. \
                                 The torrent may have no active peers/seeds. \
                                 Check tracker health with `cat .stats`.",
                                torrent_id, file_id, e
                            );
                            Err(e.into())
                        }
                    }
                }
                Ok(false) => {
                    self.metrics.l2_miss();
                    self.metrics.deferred_read();
                    info!(
                        "Deferring read for torrent file (torrent_id={}, file_id={}): \
                         pieces not on disk, blocking in worker thread",
                        torrent_id, file_id
                    );
                    Ok(ReadOutcome::Pending {
                        info,
                        file_index,
                        offset,
                        size,
                        info_hash: info_hash.clone(),
                        torrent_id,
                    })
                }
                Err(e) => {
                    error!(
                        "Failed to check pieces on disk (torrent_id={}, file_id={}): {:?}",
                        torrent_id, file_id, e
                    );
                    Err(e.into())
                }
            }
        } else {
            error!("Download manager not available");
            Err(FsError::Internal(
                "download manager not available".to_string(),
            ))
        }
    }

    /// Return the parsed `TorrentInfo` for `info_hash`, hitting the L3
    /// (metadata) cache when possible.
    ///
    /// On a miss this reads the raw `.torrent` bytes from the DB once and
    /// parses them once; subsequent reads for the same torrent reuse the
    /// cached `Arc<TorrentInfo>` and skip both the DB re-read and the
    /// bencode re-parse.
    fn get_or_parse_torrent_info(
        &self,
        info_hash: &str,
        torrent_id: i64,
    ) -> FsResult<Arc<TorrentInfo>> {
        {
            let cache = self
                .torrent_info_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            if let Some(info) = cache.get(info_hash) {
                self.metrics.l3_hit();
                return Ok(info.clone());
            }
        }

        self.metrics.l3_miss();
        let info = Arc::new(
            TorrentInfo::from_bytes(self.get_torrent_raw_data(torrent_id)?).map_err(|e| {
                error!("Failed to parse torrent info for download: {:?}", e);
                FsError::Internal(format!(
                    "failed to parse torrent info for download: {:?}",
                    e
                ))
            })?,
        );

        {
            let mut cache = self
                .torrent_info_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            cache.insert(info_hash.to_string(), info.clone());
        }

        Ok(info)
    }

    fn get_torrent_raw_data(&self, torrent_id: i64) -> FsResult<Vec<u8>> {
        let db = DataResolver::get_db(&self.db)?;
        let db_guard = db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        let torrent = db_guard
            .get_torrent_by_id(torrent_id)
            .map_err(|e| {
                error!("Failed to get torrent: {:?}", e);
                FsError::from(e)
            })?
            .ok_or_else(|| {
                error!("Torrent not found for id: {}", torrent_id);
                FsError::NotFound
            })?;

        if let Some(ref data) = torrent.torrent_data {
            if !data.is_empty() {
                return Ok(data.clone());
            }
        }

        for data in self.inode_mgr.inodes.values() {
            if let InodeData::File {
                name,
                data: file_data,
                ..
            } = data
            {
                if name.ends_with(".torrent") && !file_data.is_empty() {
                    if let Ok(info) = TorrentInfo::from_bytes(file_data.clone()) {
                        if let Ok(metadata) = info.metadata() {
                            if hex::encode(metadata.info_hash) == torrent.info_hash {
                                return Ok(file_data.clone());
                            }
                        }
                    }
                }
            }
        }

        Err(FsError::NotFound)
    }

    // ── Stats namespace ─────────────────────────────────────────────────────

    /// Render the requested `.stats` content.
    pub fn read_stats(&self, kind: StatsKind) -> Vec<u8> {
        match kind {
            StatsKind::Global => self.generate_global_stats_content(),
            StatsKind::StatsInode { ino } => self.generate_data_stats_for_ino(ino),
        }
    }

    fn generate_global_stats_content(&self) -> Vec<u8> {
        let get_cm = || self.get_cache_manager();
        let session_stats = self.download_service.as_ref().map(|ds| ds.snapshot_stats());
        generate_global_stats(
            self.inode_mgr.creation_time,
            &self.db,
            session_stats,
            get_cm,
            &self.listen_addr,
            Some(self.metrics.snapshot()),
        )
    }

    fn generate_dir_stats_content(&self, source_path: &str) -> Vec<u8> {
        let get_cm = || self.get_cache_manager();
        generate_directory_stats(source_path, &self.db, &self.download_service, get_cm)
    }

    fn generate_torrent_stats_for_id(&self, torrent_id: i64) -> Vec<u8> {
        // Extract info_hash inside a short-lived lock scope, then release before
        // calling generate_torrent_stats() which acquires its own lock.
        let info_hash = {
            let db_guard = match self.db.as_ref().and_then(|db| db.lock().ok()) {
                Some(g) => g,
                None => return b"Database not available\n".to_vec(),
            };
            match db_guard.get_torrent_by_id(torrent_id).ok().flatten() {
                Some(t) => t.info_hash,
                None => return format!("Torrent not found (id={})\n", torrent_id).into_bytes(),
            }
        };
        let get_cm = || self.get_cache_manager();
        generate_torrent_stats(
            torrent_id,
            &info_hash,
            &self.db,
            &self.download_service,
            get_cm,
        )
    }

    fn generate_data_stats_for_ino(&self, ino: u64) -> Vec<u8> {
        let dir_ino = match InodeManager::stats_ino_to_dir_ino(ino) {
            Some(d) => d,
            None => return b"Invalid stats inode\n".to_vec(),
        };

        if dir_ino == DATA_INO {
            return self.generate_dir_stats_content("");
        }

        match self.inode_mgr.data_inodes.get(&dir_ino) {
            Some(DataInode::SourcePathDir { path }) => self.generate_dir_stats_content(path),
            Some(DataInode::TorrentRoot { torrent_id, .. }) => {
                self.generate_torrent_stats_for_id(*torrent_id)
            }
            _ => b"Stats not available for this inode\n".to_vec(),
        }
    }
}

/// TSI-2199: background SHA-1 verification of on-disk cached pieces.
///
/// After a restart, [`CacheManager::scan_pieces_subdirectory`] re-registers
/// every on-disk piece but leaves it unverified — it may be a complete piece
/// or an incomplete/corrupt file left by a crash. This worker recomputes each
/// candidate's SHA-1 and compares it against the torrent's expected piece
/// hash: matches are marked verified (so subsequent reads serve from local
/// cache), mismatches are purged so they can be re-downloaded on demand.
///
/// Runs on a detached background thread so it never blocks FUSE mount
/// readiness.
fn spawn_cache_verification(
    cache: Arc<Mutex<CacheManager>>,
    infos_by_hash: HashMap<String, Arc<TorrentInfo>>,
) {
    let spawned = std::thread::Builder::new()
        .name("cache-verify".to_string())
        .spawn(move || {
            let unverified = match cache.lock() {
                Ok(c) => c.unverified_pieces(),
                Err(_) => {
                    warn!("Cache lock poisoned during piece verification");
                    return;
                }
            };

            let mut verified = 0usize;
            let mut purged = 0usize;
            let mut skipped = 0usize;

            for piece_key in unverified {
                let (info_hash, piece_index) = match split_piece_key(&piece_key) {
                    Some(v) => v,
                    None => {
                        warn!("Skipping malformed piece key: {}", piece_key);
                        skipped += 1;
                        continue;
                    }
                };

                let info = match infos_by_hash.get(info_hash) {
                    Some(info) => info,
                    None => {
                        // Torrent no longer present in the DB: leave untouched.
                        skipped += 1;
                        continue;
                    }
                };

                // Resolve the on-disk path inside a brief lock, then do all
                // file I/O lock-free so active reads (PieceStore /
                // DownloadService / .stats — all take the same cache mutex)
                // are never blocked by the verification thread.
                let path = match cache.lock() {
                    Ok(c) => c.piece_path(&piece_key),
                    Err(_) => {
                        warn!("Cache lock poisoned during piece verification");
                        return;
                    }
                };

                let outcome = verify_single_piece(&path, piece_index, info, &piece_key);

                match outcome {
                    VerifyOutcome::Verified => {
                        if let Ok(mut c) = cache.lock() {
                            c.mark_verified(&piece_key);
                        }
                        verified += 1;
                    }
                    VerifyOutcome::Purged => {
                        if let Ok(mut c) = cache.lock() {
                            let _ = c.delete_piece(&piece_key);
                        }
                        purged += 1;
                    }
                    VerifyOutcome::Skipped => {
                        skipped += 1;
                    }
                }
            }

            info!(
                "Cache piece verification complete: {} verified, {} purged, {} skipped",
                verified, purged, skipped
            );
        });

    if let Err(e) = spawned {
        warn!("Failed to spawn cache verification thread: {}", e);
    }
}

/// Outcome of verifying a single cached piece.
enum VerifyOutcome {
    Verified,
    Purged,
    Skipped,
}

/// Verify a single on-disk cached piece against its expected SHA-1 hash.
///
/// Pure file I/O — no cache lock is held.  The caller resolves the on-disk
/// `path` (and `piece_index`) from the piece key inside a brief lock, then
/// calls this function lock-free.  The caller is responsible for applying
/// the side effect (mark verified / delete piece).
fn verify_single_piece(
    path: &std::path::Path,
    piece_index: i32,
    info: &TorrentInfo,
    piece_key: &str,
) -> VerifyOutcome {
    let expected = match info.hash_for_piece(piece_index) {
        Some(h) => h,
        None => return VerifyOutcome::Skipped,
    };
    let expected_size = match info.piece_size(piece_index) {
        Some(s) => s,
        None => return VerifyOutcome::Skipped,
    };

    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => {
            warn!("Cached piece file missing, purging: {}", piece_key);
            return VerifyOutcome::Purged;
        }
    };

    let file_size = meta.len();
    if file_size != expected_size {
        warn!(
            "Cached piece has wrong size ({} != {}), leaving unverified: {}",
            file_size, expected_size, piece_key
        );
        return VerifyOutcome::Skipped;
    }

    // TSI-2229: a piece file may have the correct *logical* size but still be
    // incomplete — `write_piece` writes blocks at arbitrary offsets, and a
    // crash between block writes leaves a sparse file whose zero-filled gaps
    // make st_size match piece_length while the physical allocation is smaller.
    // Treat it like a wrong-size piece (leave unverified) instead of purging.
    if is_sparse_file(&meta, file_size) {
        warn!(
            "Cached piece is sparse (partial write), leaving unverified: {}",
            piece_key
        );
        return VerifyOutcome::Skipped;
    }

    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => {
            warn!("Cached piece file disappeared, purging: {}", piece_key);
            return VerifyOutcome::Purged;
        }
    };
    let actual = Sha1::from(&data[..]).digest().bytes();
    if actual == expected {
        VerifyOutcome::Verified
    } else {
        warn!(
            "Cached piece failed SHA-1 verification, purging: {}",
            piece_key
        );
        VerifyOutcome::Purged
    }
}

/// Split a piece key of the form `{info_hash}:piece:{index}` into its parts.
fn split_piece_key(key: &str) -> Option<(&str, i32)> {
    let mut parts = key.split(':');
    let info_hash = parts.next()?;
    if parts.next()? != "piece" {
        return None;
    }
    let index = parts.next()?.parse::<i32>().ok()?;
    Some((info_hash, index))
}

/// Whether a piece file is *sparse* — its physical disk allocation is smaller
/// than its logical size.
///
/// `write_piece` writes blocks at arbitrary offsets via `seekp`; a crash
/// between block writes leaves a file whose `st_size` matches the expected
/// piece length but whose interior has zero-filled gaps (the filesystem does
/// not allocate blocks for the unwritten regions). Such a file is not a
/// complete piece even though its logical size is correct.
///
/// Comparing `st_blocks * 512` (physical) against `st_size` (logical) detects
/// this condition. A fully-written all-zero piece also appears sparse, but
/// leaving it unverified is harmless — the next read re-downloads it,
/// `register_piece` marks it verified, and no data is lost.
///
/// Takes the already-fetched `Metadata` so the caller avoids a duplicate
/// `stat` syscall (one per piece — significant at 987+ pieces).
fn is_sparse_file(meta: &std::fs::Metadata, logical_size: u64) -> bool {
    if logical_size == 0 {
        return false;
    }
    use std::os::unix::fs::MetadataExt;
    let physical = meta.blocks() * 512;
    physical < logical_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::db::{Database, InsertTorrentResult};
    use crate::infrastructure::metrics::Metrics;
    use crate::metadata::TorrentInfo;
    use crate::services::torrent::TorrentService;

    use crate::fuse::inodes::InodeManager;

    /// Minimal single-file bencode so `TorrentInfo::from_bytes` parses.
    fn minimal_torrent_bytes() -> Vec<u8> {
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi16e");
        t.extend_from_slice(b"4:name3:foo");
        t.extend_from_slice(b"12:piece lengthi16384e");
        t.extend_from_slice(b"6:pieces20:");
        t.extend_from_slice(&[0u8; 20]);
        t.extend_from_slice(b"ee");
        t
    }

    fn service_with_torrent(torrent_bytes: &[u8]) -> (FsService, String, i64, Arc<Metrics>) {
        let info = TorrentInfo::from_bytes(torrent_bytes.to_vec()).expect("parse torrent");
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        drop(info);

        let mut db = Database::open_in_memory().expect("in-memory db");
        let torrent_id = match db
            .insert_torrent("src", "foo", "foo.torrent", 16, &info_hash, 1)
            .expect("insert torrent")
        {
            InsertTorrentResult::Inserted(id) => id,
            other => panic!("unexpected insert result: {:?}", other),
        };
        db.set_torrent_data(torrent_id, torrent_bytes)
            .expect("set torrent data");

        let db_arc = Arc::new(Mutex::new(db));
        let metrics = Arc::new(Metrics::new());
        let svc = FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(TorrentService::new(db_arc, None, None)),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            download_service: None,
            seeding_manager: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics: metrics.clone(),
        };
        (svc, info_hash, torrent_id, metrics)
    }

    #[test]
    fn torrent_info_cached_by_info_hash() {
        let torrent_bytes = minimal_torrent_bytes();
        let (svc, info_hash, torrent_id, metrics) = service_with_torrent(&torrent_bytes);

        // First lookup: miss → parse + insert.
        let first = svc
            .get_or_parse_torrent_info(&info_hash, torrent_id)
            .unwrap();
        assert_eq!(metrics.snapshot().l3_misses, 1);
        assert_eq!(metrics.snapshot().l3_hits, 0);

        // Second lookup: hit → same Arc, no re-parse.
        let second = svc
            .get_or_parse_torrent_info(&info_hash, torrent_id)
            .unwrap();
        assert_eq!(metrics.snapshot().l3_misses, 1);
        assert_eq!(metrics.snapshot().l3_hits, 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn split_piece_key_parses_info_hash_and_index() {
        assert_eq!(
            split_piece_key("abcdef0123456789:piece:42"),
            Some(("abcdef0123456789", 42))
        );
        // Non-`piece` segment or missing index are rejected.
        assert_eq!(split_piece_key("abc:data:0"), None);
        assert_eq!(split_piece_key("abc:piece:x"), None);
        assert_eq!(split_piece_key("abc"), None);
        assert_eq!(split_piece_key(""), None);
    }

    /// TSI-2228: Bare service without any torrents — sufficient for testing
    /// that mutating operations on the read-only `data/` namespace return
    /// `EROFS` (`ReadOnlyFileSystem`), not `ENOENT` or `EACCES`.
    fn bare_service() -> FsService {
        let metrics = Arc::new(Metrics::new());
        FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: None,
            torrent_service: None,
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            download_service: None,
            seeding_manager: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
        }
    }

    #[test]
    fn data_namespace_write_returns_erofs() {
        let mut svc = bare_service();
        // DATA_INO (the `data/` root) is a directory → would be EISDIR
        // without the read-only guard, but the guard fires first.
        let err = svc.write(DATA_INO, 0, b"x").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // A data file inode (not in `inodes`, only in `data_inodes`) would
        // previously fall through to the `None` arm → `NotFound` → ENOENT.
        let data_file_ino = DATA_FILE_INO_BASE + 1;
        let err = svc.write(data_file_ino, 0, b"x").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    #[test]
    fn data_namespace_create_mknod_mkdir_return_erofs() {
        let mut svc = bare_service();

        let err = svc.create(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.mknod(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.mkdir(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // A nested data inode as parent.
        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc.mkdir(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    #[test]
    fn data_namespace_unlink_rmdir_return_erofs() {
        let mut svc = bare_service();

        let err = svc.unlink(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.rmdir(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc.unlink(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
        let err = svc.rmdir(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    #[test]
    fn data_namespace_rename_returns_erofs() {
        let mut svc = bare_service();

        // Rename *into* data/ as the new parent.
        let err = svc
            .rename(METADATA_INO, "foo", DATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // Rename *out of* data/ as the source parent.
        let err = svc
            .rename(DATA_INO, "foo", METADATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // Both parents in data/ namespace.
        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc
            .rename(data_dir_ino, "foo", DATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    #[test]
    fn data_namespace_inodes_correctly_identified() {
        // Unit-test the helper itself.
        assert!(InodeManager::is_data_namespace(DATA_INO));
        assert!(InodeManager::is_data_namespace(DATA_TORRENT_INO_BASE));
        assert!(InodeManager::is_data_namespace(DATA_TORRENT_INO_BASE + 42));
        assert!(InodeManager::is_data_namespace(DATA_DIR_INO_BASE + 1));
        assert!(InodeManager::is_data_namespace(DATA_FILE_INO_BASE + 1));
        assert!(InodeManager::is_data_namespace(SOURCE_PATH_DIR_INO_BASE));

        // Non-data inodes are not in the data namespace.
        assert!(!InodeManager::is_data_namespace(ROOT_INO));
        assert!(!InodeManager::is_data_namespace(METADATA_INO));
        assert!(!InodeManager::is_data_namespace(STATS_INO));
        assert!(!InodeManager::is_data_namespace(
            NEXT_INO.load(Ordering::SeqCst),
        ));
    }

    /// TSI-2246: opening a `data/` torrent file must set `direct_io: true`
    /// so the kernel bypasses its page cache and the daemon's errno
    /// (e.g. ENODATA) reaches userspace instead of being converted to EIO.
    #[test]
    fn open_data_torrent_file_sets_direct_io() {
        let mut svc = bare_service();
        let ino = DATA_FILE_INO_BASE + 1;
        svc.inode_mgr.data_inodes.insert(
            ino,
            DataInode::TorrentFile {
                torrent_id: 1,
                file_id: 1,
                name: "foo".to_string(),
                size: 16,
            },
        );
        let outcome = svc.open(ino).expect("open data file");
        assert!(
            outcome.direct_io,
            "data torrent file must request direct_io"
        );
        assert_ne!(outcome.fh, 0, "data torrent file must get a real fh");
    }

    /// TSI-2246: non-data files (e.g. metadata) must NOT set direct_io —
    /// page cache is fine for static in-memory content.
    #[test]
    fn open_metadata_file_does_not_set_direct_io() {
        let mut svc = bare_service();
        let ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            ino,
            InodeData::File {
                parent: ROOT_INO,
                name: "x".to_string(),
                data: vec![b'x'],
            },
        );
        let outcome = svc.open(ino).expect("open metadata file");
        assert!(
            !outcome.direct_io,
            "metadata file must not request direct_io"
        );
    }

    /// TSI-2246: stats inodes must NOT set direct_io.
    #[test]
    fn open_stats_file_does_not_set_direct_io() {
        let mut svc = bare_service();
        // STATS_INO is the global `.stats` file.
        let outcome = svc.open(STATS_INO).expect("open stats");
        assert!(!outcome.direct_io, "stats file must not request direct_io");
    }

    #[test]
    fn is_sparse_file_detects_sparse_partial_write() {
        // Simulate a partial multi-block piece write: create a file with the
        // right logical size but a zero-filled gap (sparse hole) by seeking
        // past the start before writing the trailing block.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse_piece");
        use std::io::{Seek, Write};
        let mut file = std::fs::File::create(&path).unwrap();
        // Write 16 KiB at offset 256 KiB — the file's logical size becomes
        // 256 KiB + 16 KiB, but bytes 0..256 KiB are a sparse hole.
        file.seek(std::io::SeekFrom::Start(256 * 1024)).unwrap();
        file.write_all(&vec![0xABu8; 16 * 1024]).unwrap();
        drop(file);

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 256 * 1024 + 16 * 1024);
        assert!(
            is_sparse_file(&meta, meta.len()),
            "file with a sparse hole must be detected as sparse"
        );
    }

    #[test]
    fn is_sparse_file_false_for_complete_file() {
        // A fully-written (non-sparse) file must not be flagged as sparse.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("complete_piece");
        std::fs::write(&path, vec![0xCDu8; 262_144]).unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 262_144);
        assert!(
            !is_sparse_file(&meta, meta.len()),
            "fully-written file must not be detected as sparse"
        );
    }

    #[test]
    fn is_sparse_file_false_for_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_piece");
        std::fs::write(&path, b"").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert!(!is_sparse_file(&meta, 0));
    }

    /// Build a single-file bencoded torrent whose single piece has a known
    /// SHA-1 hash.  Returns `(torrent_bytes, piece_content, piece_length)`.
    fn build_single_piece_torrent() -> (Vec<u8>, Vec<u8>, u64) {
        let piece_length: usize = 16_384;
        let content = (0..piece_length)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>();
        let hash = {
            use sha1_smol::Sha1;
            Sha1::from(&content).digest().bytes()
        };
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(content.len().to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:name4:test");
        t.extend_from_slice(b"12:piece lengthi");
        t.extend_from_slice(piece_length.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"6:pieces20:");
        t.extend_from_slice(&hash);
        t.extend_from_slice(b"ee");
        (t, content, piece_length as u64)
    }

    #[test]
    fn verify_single_piece_sparse_file_is_skipped_not_purged() {
        let (torrent_bytes, content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();

        // Create the piece file on disk *before* constructing the
        // CacheManager so the startup scan registers it as unverified
        // (exactly like a real restart).
        let pieces_dir = temp_dir.path().join("pieces").join(&info_hash);
        std::fs::create_dir_all(&pieces_dir).unwrap();
        let piece_path = pieces_dir.join(&piece_key);

        // Sparse file: correct logical size but a zero-filled hole at the
        // start (simulates an interrupted multi-block write where only the
        // trailing block was flushed to disk).
        use std::io::{Seek, Write};
        let mut file = std::fs::File::create(&piece_path).unwrap();
        let tail_start = piece_length as u64 - 4096;
        file.seek(std::io::SeekFrom::Start(tail_start)).unwrap();
        file.write_all(&content[tail_start as usize..]).unwrap();
        drop(file);

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        // Scan registered the piece but left it unverified.
        assert!(cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));

        // The sparse file must be Skipped, not Purged.
        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Skipped),
            "sparse piece file should be skipped, not purged"
        );
        // The piece file must still exist on disk (not deleted).
        assert!(
            piece_path.exists(),
            "skipped piece file must not be deleted"
        );
    }

    #[test]
    fn verify_single_piece_complete_correct_hash_is_verified() {
        let (torrent_bytes, content, _piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        std::fs::write(&piece_path, &content).unwrap();
        cache.add_piece(&piece_key, content.len() as u64).unwrap();

        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Verified),
            "complete piece with correct hash should be verified"
        );
    }

    #[test]
    fn verify_single_piece_complete_wrong_hash_is_purged() {
        let (torrent_bytes, _content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        // Write a complete, non-sparse file with wrong content.
        std::fs::write(&piece_path, vec![0xFFu8; piece_length as usize]).unwrap();
        cache.add_piece(&piece_key, piece_length).unwrap();

        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Purged),
            "complete non-sparse piece with wrong hash should be purged"
        );
    }

    #[test]
    fn verify_single_piece_wrong_size_is_skipped() {
        let (torrent_bytes, _content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        // Write a short file (incomplete piece).
        std::fs::write(&piece_path, vec![0u8; piece_length as usize / 2]).unwrap();
        cache.add_piece(&piece_key, piece_length / 2).unwrap();

        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Skipped),
            "wrong-size piece should be skipped"
        );
    }

    // ── TSI-2247: release must not block the FUSE dispatch thread ─────────

    /// Build a service backed by an in-memory DB + TorrentService so
    /// `release` can exercise the full add_torrent path.
    fn service_with_db() -> FsService {
        let db = Database::open_in_memory().expect("in-memory db");
        let db_arc = Arc::new(Mutex::new(db));
        let metrics = Arc::new(Metrics::new());
        FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(TorrentService::new(db_arc, None, None)),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            download_service: None,
            seeding_manager: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
        }
    }

    /// Create a writable `.torrent` inode via the public `create()` path and
    /// return its (ino, fh).
    fn create_torrent_file(svc: &mut FsService, name: &str) -> (u64, u64) {
        let created = svc.create(METADATA_INO, name).expect("create file");
        (created.attr.ino, created.fh)
    }

    /// TSI-2247: Closing an empty `.torrent` file must hit the fast path —
    /// `release` returns `Ok(())` immediately without touching
    /// `processing_torrents` or the DB.
    #[test]
    fn release_empty_torrent_is_fast_path() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "empty.torrent");

        // release must succeed instantly — no DB insert, no background spawn.
        svc.release(fh).expect("release ok");

        // Inode is removed.
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        // processing_torrents is untouched (empty).
        assert!(svc.processing_torrents.lock().unwrap().is_empty());
    }

    /// TSI-2247: Closing a `.torrent` with invalid (non-parseable) data must
    /// also hit the fast path — no DB insert, no background spawn.
    #[test]
    fn release_invalid_torrent_is_fast_path() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "bad.torrent");
        // Write garbage that is non-empty but not a valid torrent.
        svc.write(ino, 0, b"not a torrent").expect("write ok");

        svc.release(fh).expect("release ok");

        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        assert!(svc.processing_torrents.lock().unwrap().is_empty());
    }

    /// TSI-2247: Closing a valid `.torrent` file must NOT block the caller.
    /// `add_torrent` runs on a background thread; `release` returns `Ok(())`
    /// immediately.  The `processing_torrents` entry is cleaned up by the
    /// background thread after the DB insert completes, and the torrent is
    /// persisted in the DB.
    #[test]
    fn release_valid_torrent_returns_immediately() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "valid.torrent");
        let data = minimal_torrent_bytes();
        svc.write(ino, 0, &data).expect("write ok");

        // release should return instantly — the DB insert is deferred to a
        // background thread.
        let start = std::time::Instant::now();
        svc.release(fh).expect("release ok");
        let elapsed = start.elapsed();

        // Even an in-memory DB insert takes < 1s; the point is that
        // release itself does not wait for it.  Allow generous slack
        // for CI scheduling jitter.
        assert!(
            elapsed.as_secs() < 5,
            "release took {:?} — should be near-instant",
            elapsed
        );

        // Inode is NOT removed (data integrity: if add_torrent fails, the
        // file must remain visible to the user).
        assert!(svc.inode_mgr.inodes.contains_key(&ino));

        // Wait for the background thread to finish and clean up
        // processing_torrents.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("processing_torrents not cleaned up after 10s");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // Verify the torrent was persisted to the DB by the background thread.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let torrent = db_guard
            .get_torrent_by_filename_and_source_path("valid.torrent", "")
            .expect("db query");
        assert!(torrent.is_some(), "torrent should be in DB after release");
        assert_eq!(torrent.unwrap().filename, "valid.torrent");
    }

    /// TSI-2247: `processing_torrents` is NOT held during `add_torrent` —
    /// the lock is released before the background thread is spawned.  Two
    /// `.torrent` files in the root directory (both `source_path == ""`)
    /// must both be processed and persisted — the dedup key is
    /// `(source_path, filename)`, so they don't collide.
    #[test]
    fn release_does_not_hold_processing_lock_during_add() {
        let mut svc = service_with_db();
        let (ino1, fh1) = create_torrent_file(&mut svc, "a.torrent");
        let data = minimal_torrent_bytes();
        svc.write(ino1, 0, &data).expect("write ok");
        svc.release(fh1).expect("release ok");

        // At this point the background thread for a.torrent may or may not
        // have finished.  Either way, processing_torrents should be lockable
        // without blocking (it would be held only briefly by the background
        // thread's cleanup).  The second release for a *different* file
        // must succeed immediately — even though both share source_path "".
        let (ino2, fh2) = create_torrent_file(&mut svc, "b.torrent");
        svc.write(ino2, 0, &data).expect("write ok");

        let start = std::time::Instant::now();
        svc.release(fh2).expect("release ok");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 5,
            "second release took {:?} — should not wait for first",
            elapsed
        );

        // Wait for both background threads to finish.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("processing_torrents not cleaned up after 10s");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // Both torrents must be in the DB — the tuple dedup key prevents
        // the second from being skipped as a duplicate of the first.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let t1 = db_guard
            .get_torrent_by_filename_and_source_path("a.torrent", "")
            .expect("db query");
        assert!(t1.is_some(), "a.torrent should be in DB");
        let t2 = db_guard
            .get_torrent_by_filename_and_source_path("b.torrent", "")
            .expect("db query");
        assert!(t2.is_some(), "b.torrent should be in DB");
    }

    /// TSI-2247: `flush` for an empty `.torrent` returns `EINVAL` (fast
    /// path — no FFI call to libtorrent).
    #[test]
    fn flush_empty_torrent_returns_einval() {
        let mut svc = service_with_db();
        let (ino, _fh) = create_torrent_file(&mut svc, "empty.torrent");

        let err = svc.flush(ino).unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);
    }
}
