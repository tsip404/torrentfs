//! FUSE module — TorrentFs struct and Filesystem trait implementation.
//! Delegates inode management to InodeManager, data lookups to DataResolver,
//! stats generation to StatsGenerator, and torrent operations to TorrentService.

pub mod inodes;
pub mod lookup;
pub mod stats;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use libc::{EACCES, EEXIST, EFBIG, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, ENOTEMPTY, EROFS};
use tracing::{error, info, warn};

use self::inodes::{
    DataInode, InodeData, InodeManager, DATA_INO, METADATA_INO, ROOT_INO, STATS_INO,
};
use self::lookup::DataResolver;
use self::stats::{generate_directory_stats, generate_global_stats, generate_torrent_stats};

use crate::cache::CacheManager;
use crate::db::Database;
use crate::metadata::TorrentInfo;
use crate::services::download::DownloadService;
use crate::services::seeding::SeedingService;
use crate::services::torrent::TorrentService;

pub struct TorrentFs {
    pub inode_mgr: InodeManager,
    pub db: Option<Arc<Mutex<Database>>>,
    pub torrent_service: Option<TorrentService>,
    pub processing_torrents: Arc<Mutex<HashMap<String, ()>>>,
    pub download_service: Option<DownloadService>,
    pub torrent_data_cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    pub listen_addr: String,
}

impl TorrentFs {
    pub fn new_with_cache_path(
        cache_path: PathBuf,
        config: &crate::config::TorrentfsConfig,
    ) -> Self {
        if !cache_path.exists() {
            if let Err(e) = std::fs::create_dir_all(&cache_path) {
                warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
            }
        }

        let download_service = DownloadService::new(cache_path.as_path(), config).ok();

        // Register SeedingManager as eviction callback on the DownloadService's CacheManager
        if let Some(ref ds) = download_service {
            if let Ok(seeding_svc) = SeedingService::new(&cache_path, config) {
                ds.register_seeding_callback(seeding_svc.get_seeding_manager());
                info!("SeedingManager registered as CacheManager eviction callback");
            }
        }

        let creation_time = Duration::from_secs(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
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
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr,
        }
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::new_with_cache_path(
            PathBuf::from("/tmp/torrentfs-cache"),
            &crate::config::TorrentfsConfig::default_config(),
        )
    }

    #[allow(dead_code)]
    pub fn new_with_db(_db: Database) -> Self {
        Self::new()
    }

    pub fn new_with_db_and_cache(
        db: Database,
        cache_path: PathBuf,
        config: &crate::config::TorrentfsConfig,
    ) -> Self {
        let mut fs = Self::new_with_cache_path(cache_path, config);

        // Collect data from database before moving it
        let (dirs, torrents) = {
            let dirs = db.get_all_metadata_directories().unwrap_or_default();
            let torrents = db.get_all_torrents().unwrap_or_default();
            (dirs, torrents)
        };

        let db_arc = Arc::new(Mutex::new(db));
        fs.torrent_service = Some(TorrentService::new(db_arc.clone()));
        fs.db = Some(db_arc);
        fs.inode_mgr.restore_metadata_inodes(dirs, torrents);
        fs
    }

    /// Get the CacheManager shared with DownloadService.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        self.download_service
            .as_ref()
            .and_then(|ds| ds.get_cache_manager())
    }

    /// Read torrent file data from the BitTorrent network.
    fn read_torrent_file_data(
        &self,
        torrent_id: i64,
        file_id: i64,
        offset: usize,
        size: usize,
    ) -> Result<Vec<u8>, i32> {
        let ts = self.torrent_service.as_ref().ok_or_else(|| {
            error!("Torrent service not available");
            EIO
        })?;

        let (info_hash, _source_path, files) =
            ts.get_torrent_with_files(torrent_id)?.ok_or_else(|| {
                error!("Torrent not found: {}", torrent_id);
                ENOENT
            })?;

        let _file = files.iter().find(|f| f.id == file_id).ok_or_else(|| {
            error!("File not found: {}", file_id);
            ENOENT
        })?;

        let file_index = files.iter().position(|f| f.id == file_id).ok_or_else(|| {
            error!("File index not found for file_id: {}", file_id);
            EIO
        })? as i32;

        let cache_key = format!("{}:{}", info_hash, file_id);
        {
            let cache = self.torrent_data_cache.lock().map_err(|_| EIO)?;
            if let Some(cached) = cache.get(&cache_key) {
                let end = std::cmp::min(offset + size, cached.len());
                if offset < cached.len() {
                    return Ok(cached[offset..end].to_vec());
                } else {
                    return Ok(Vec::new());
                }
            }
        }

        if let Some(ds) = &self.download_service {
            let torrent_data = self.get_torrent_raw_data(torrent_id)?;
            let info = TorrentInfo::from_bytes(torrent_data).map_err(|e| {
                error!("Failed to parse torrent info for download: {:?}", e);
                EIO
            })?;

            // Retry loop: on cold reads, pieces may not be ready on the first
            // attempt.  Retry with increasing backoff to give libtorrent time
            // to download needed pieces — avoids returning EIO prematurely
            // (TSI-2045).
            // Only retry on transient errors (PieceNotReady, NoPeers, timeouts,
            // IoError); permanent errors (InvalidFile, ParseError, NullPointer)
            // fail immediately.
            const MAX_READ_RETRIES: u32 = 3;
            let mut last_err = String::new();
            for attempt in 0..=MAX_READ_RETRIES {
                match ds.read_file_range(&info, file_index, offset as u64, size as u32) {
                    Ok(data) => {
                        if attempt > 0 {
                            info!(
                                "Successfully read {} bytes from torrent file on retry {} \
                                 (torrent_id={}, file_id={})",
                                data.len(),
                                attempt,
                                torrent_id,
                                file_id
                            );
                        } else {
                            info!(
                                "Successfully read {} bytes from torrent file \
                                 (torrent_id={}, file_id={})",
                                data.len(),
                                torrent_id,
                                file_id
                            );
                        }
                        return Ok(data);
                    }
                    Err(e) => {
                        last_err = e.to_string();
                        let is_transient = crate::domain::error::is_transient_read_error(&e);
                        if !is_transient {
                            warn!("Permanent read error, not retrying: {}", last_err);
                            return Err(EIO);
                        }
                        if attempt < MAX_READ_RETRIES {
                            let delay = std::time::Duration::from_secs(1u64 << attempt);
                            warn!(
                                "Retry {}/{} after {:?}: {}",
                                attempt + 1,
                                MAX_READ_RETRIES,
                                delay,
                                last_err
                            );
                            std::thread::sleep(delay);
                        }
                    }
                }
            }
            warn!(
                "Failed to read from BitTorrent network after {} retries: {}. \
                 The torrent may have no active peers/seeds. \
                 Check tracker health with `cat .stats`.",
                MAX_READ_RETRIES, last_err
            );
            Err(EIO)
        } else {
            error!("Download manager not available");
            Err(EIO)
        }
    }

    fn get_torrent_raw_data(&self, torrent_id: i64) -> Result<Vec<u8>, i32> {
        let db = DataResolver::get_db(&self.db)?;
        let db_guard = db.lock().map_err(|_| {
            error!("Database lock poisoned");
            EIO
        })?;

        let torrent = db_guard
            .get_torrent_by_id(torrent_id)
            .map_err(|e| {
                error!("Failed to get torrent: {:?}", e);
                EIO
            })?
            .ok_or_else(|| {
                error!("Torrent not found for id: {}", torrent_id);
                ENOENT
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

        Err(ENOENT)
    }
}

impl Filesystem for TorrentFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();

        if parent == ROOT_INO {
            match name_str.as_ref() {
                "metadata" => {
                    reply.entry(
                        &Duration::from_secs(1),
                        &self.inode_mgr.attr_for_dir(METADATA_INO, true),
                        0,
                    );
                }
                "data" => {
                    reply.entry(
                        &Duration::from_secs(1),
                        &self.inode_mgr.attr_for_dir(DATA_INO, false),
                        0,
                    );
                }
                ".stats" => {
                    let stats_size = self.generate_stats().len() as u64;
                    reply.entry(
                        &Duration::from_secs(1),
                        &self.inode_mgr.attr_for_file(STATS_INO, stats_size),
                        0,
                    );
                }
                _ => reply.error(ENOENT),
            }
            return;
        }

        // Handle .stats virtual file for data/ subtree directories.
        // Must be intercepted before the normal data/ lookup so that
        // .stats is not resolved as a torrent name or source path.
        if name_str == ".stats" {
            if parent == DATA_INO {
                let stats_ino = InodeManager::make_stats_ino(parent);
                let content = self.generate_dir_stats_content("");
                reply.entry(
                    &Duration::from_secs(1),
                    &self
                        .inode_mgr
                        .attr_for_file(stats_ino, content.len() as u64),
                    0,
                );
                return;
            }

            if let Some(data_inode) = self.inode_mgr.data_inodes.get(&parent) {
                match data_inode {
                    DataInode::SourcePathDir { path } => {
                        let stats_ino = InodeManager::make_stats_ino(parent);
                        let content = self.generate_dir_stats_content(path);
                        reply.entry(
                            &Duration::from_secs(1),
                            &self
                                .inode_mgr
                                .attr_for_file(stats_ino, content.len() as u64),
                            0,
                        );
                        return;
                    }
                    DataInode::TorrentRoot { torrent_id, .. } => {
                        let stats_ino = InodeManager::make_stats_ino(parent);
                        let content = self.generate_torrent_stats_for_id(*torrent_id);
                        reply.entry(
                            &Duration::from_secs(1),
                            &self
                                .inode_mgr
                                .attr_for_file(stats_ino, content.len() as u64),
                            0,
                        );
                        return;
                    }
                    _ => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            }
            // Parent not yet cached in data_inodes — fall through to
            // normal data/ lookup below.  If the parent doesn't exist,
            // the lookup will return ENOENT as expected.
        }

        if parent == DATA_INO || InodeManager::is_data_ino(parent) {
            if let Some(db) = &self.db {
                if let Some((ino, kind, size)) =
                    DataResolver::lookup_data_inode(&mut self.inode_mgr, db, parent, &name_str)
                {
                    match kind {
                        fuser::FileType::Directory => {
                            reply.entry(
                                &Duration::from_secs(1),
                                &self.inode_mgr.attr_for_dir(ino, false),
                                0,
                            );
                        }
                        fuser::FileType::RegularFile => {
                            reply.entry(
                                &Duration::from_secs(1),
                                &self.inode_mgr.attr_for_file(ino, size),
                                0,
                            );
                        }
                        _ => reply.error(ENOENT),
                    }
                    return;
                }
            }
            reply.error(ENOENT);
            return;
        }

        if let Some(child_ino) = self.inode_mgr.find_child_by_name(parent, &name_str) {
            if let Some(data) = self.inode_mgr.inodes.get(&child_ino) {
                match data {
                    InodeData::Directory { .. } => {
                        reply.entry(
                            &Duration::from_secs(1),
                            &self.inode_mgr.attr_for_dir(child_ino, true),
                            0,
                        );
                    }
                    InodeData::File {
                        data: file_data, ..
                    } => {
                        reply.entry(
                            &Duration::from_secs(1),
                            &self
                                .inode_mgr
                                .attr_for_file(child_ino, file_data.len() as u64),
                            0,
                        );
                    }
                }
                return;
            }
        }

        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            ROOT_INO => reply.attr(
                &Duration::from_secs(1),
                &self.inode_mgr.attr_for_dir(ino, false),
            ),
            METADATA_INO => reply.attr(
                &Duration::from_secs(1),
                &self.inode_mgr.attr_for_dir(ino, true),
            ),
            DATA_INO => reply.attr(
                &Duration::from_secs(1),
                &self.inode_mgr.attr_for_dir(ino, false),
            ),
            STATS_INO => {
                let stats_size = self.generate_global_stats_content().len() as u64;
                reply.attr(
                    &Duration::from_secs(1),
                    &self.inode_mgr.attr_for_file(ino, stats_size),
                );
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let content = self.generate_data_stats_for_ino(ino);
                    reply.attr(
                        &Duration::from_secs(1),
                        &self.inode_mgr.attr_for_file(ino, content.len() as u64),
                    );
                    return;
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(data_inode) = self.inode_mgr.data_inodes.get(&ino) {
                        match data_inode {
                            DataInode::SourcePathDir { .. }
                            | DataInode::TorrentRoot { .. }
                            | DataInode::TorrentDir { .. } => {
                                reply.attr(
                                    &Duration::from_secs(1),
                                    &self.inode_mgr.attr_for_dir(ino, false),
                                );
                            }
                            DataInode::TorrentFile { size, .. } => {
                                reply.attr(
                                    &Duration::from_secs(1),
                                    &self.inode_mgr.attr_for_file(ino, *size as u64),
                                );
                            }
                        }
                        return;
                    }

                    let torrent_id = (ino - inodes::DATA_TORRENT_INO_BASE) as i64;
                    if (inodes::DATA_TORRENT_INO_BASE..inodes::DATA_DIR_INO_BASE).contains(&ino) {
                        if self
                            .torrent_service
                            .as_ref()
                            .map(|ts| ts.torrent_exists_by_id(torrent_id))
                            .unwrap_or(false)
                        {
                            reply.attr(
                                &Duration::from_secs(1),
                                &self.inode_mgr.attr_for_dir(ino, false),
                            );
                        } else {
                            reply.error(ENOENT);
                        }
                        return;
                    }

                    reply.error(ENOENT);
                    return;
                }

                if let Some(data) = &self.inode_mgr.inodes.get(&ino) {
                    match data {
                        InodeData::Directory { .. } => {
                            reply.attr(
                                &Duration::from_secs(1),
                                &self
                                    .inode_mgr
                                    .attr_for_dir(ino, self.inode_mgr.is_metadata_child(ino)),
                            );
                        }
                        InodeData::File {
                            data: file_data, ..
                        } => {
                            reply.attr(
                                &Duration::from_secs(1),
                                &self.inode_mgr.attr_for_file(ino, file_data.len() as u64),
                            );
                        }
                    }
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino == DATA_INO || InodeManager::is_data_ino(ino) {
            if let Some(db) = &self.db {
                if let Some(entries) =
                    DataResolver::readdir_data(&mut self.inode_mgr, db, ino, offset)
                {
                    for (entry_ino, entry_offset, kind, name) in entries {
                        if reply.add(entry_ino, entry_offset, kind, &name) {
                            break;
                        }
                    }
                    reply.ok();
                    return;
                }
            }
            reply.error(ENOENT);
            return;
        }

        let mut entries: Vec<(u64, i64, fuser::FileType, String)> =
            vec![(ino, 1, fuser::FileType::Directory, ".".to_string())];

        if ino == ROOT_INO {
            entries.push((ROOT_INO, 2, fuser::FileType::Directory, "..".to_string()));
            entries.push((
                METADATA_INO,
                3,
                fuser::FileType::Directory,
                "metadata".to_string(),
            ));
            entries.push((DATA_INO, 4, fuser::FileType::Directory, "data".to_string()));
            entries.push((
                STATS_INO,
                5,
                fuser::FileType::RegularFile,
                ".stats".to_string(),
            ));
        } else if let Some(InodeData::Directory { parent, .. }) = self.inode_mgr.inodes.get(&ino) {
            entries.push((*parent, 2, fuser::FileType::Directory, "..".to_string()));

            let mut offset_counter = entries.len() as i64 + 1;
            // Collect entries first to avoid borrowing issues
            let children: Vec<(u64, fuser::FileType, String)> = self
                .inode_mgr
                .inodes
                .iter()
                .filter_map(|(child_ino, data)| match data {
                    InodeData::Directory {
                        parent: p, name, ..
                    } if *p == ino && !name.is_empty() => {
                        Some((*child_ino, fuser::FileType::Directory, name.clone()))
                    }
                    InodeData::File {
                        parent: p, name, ..
                    } if *p == ino => {
                        Some((*child_ino, fuser::FileType::RegularFile, name.clone()))
                    }
                    _ => None,
                })
                .collect();

            for (child_ino, kind, name) in children {
                entries.push((child_ino, offset_counter, kind, name));
                offset_counter += 1;
            }
        } else {
            reply.error(ENOTDIR);
            return;
        }

        for (ino_child, offset_child, kind, name) in entries.iter() {
            if *offset_child <= offset {
                continue;
            }
            if reply.add(*ino_child, *offset_child, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.opened(0, 0),
            STATS_INO => {
                let fh = inodes::NEXT_FH.fetch_add(1, Ordering::SeqCst);
                self.inode_mgr.open_files.insert(fh, ino);
                reply.opened(fh, 0);
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let fh = inodes::NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.inode_mgr.open_files.insert(fh, ino);
                    reply.opened(fh, 0);
                    return;
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile { .. }) =
                        self.inode_mgr.data_inodes.get(&ino)
                    {
                        let fh = inodes::NEXT_FH.fetch_add(1, Ordering::SeqCst);
                        self.inode_mgr.open_files.insert(fh, ino);
                        reply.opened(fh, 0);
                    } else {
                        reply.opened(0, 0);
                    }
                    return;
                }

                if self.inode_mgr.inodes.contains_key(&ino) {
                    let fh = inodes::NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.inode_mgr.open_files.insert(fh, ino);
                    reply.opened(fh, 0);
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn flush(&mut self, _req: &Request, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        if let Some(InodeData::File { data, name, .. }) = self.inode_mgr.inodes.get(&ino) {
            if name.ends_with(".torrent") {
                if data.is_empty() {
                    warn!("Zero-byte torrent file {} rejected", name);
                    reply.error(EINVAL);
                    return;
                }

                if data.len() > inodes::MAX_TORRENT_SIZE {
                    warn!(
                        "Torrent file {} exceeds size limit ({} bytes)",
                        name,
                        data.len()
                    );
                    reply.error(EFBIG);
                    return;
                }

                match TorrentInfo::from_bytes(data.clone()) {
                    Ok(_) => {
                        info!("Torrent {} validated successfully", name);
                    }
                    Err(e) => {
                        warn!("Invalid torrent file {}: {:?}", name, e);
                        reply.error(EINVAL);
                        return;
                    }
                }
            }
        }
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if let Some(ino) = self.inode_mgr.open_files.remove(&fh) {
            if let Some(InodeData::File { data, name, parent }) =
                self.inode_mgr.inodes.get(&ino).cloned()
            {
                if name.ends_with(".torrent") {
                    if data.is_empty() {
                        warn!("Zero-byte torrent file {} removed", name);
                        self.inode_mgr.inodes.remove(&ino);
                        reply.ok();
                        return;
                    }

                    if data.len() > inodes::MAX_TORRENT_SIZE {
                        self.inode_mgr.inodes.remove(&ino);
                        reply.ok();
                        return;
                    }

                    if TorrentInfo::from_bytes(data.clone()).is_err() {
                        warn!("Torrent {} invalid, removing inode", name);
                        self.inode_mgr.inodes.remove(&ino);
                        reply.ok();
                        return;
                    }

                    let source_path = self.inode_mgr.extract_source_path(parent);

                    {
                        let mut processing = match self.processing_torrents.lock() {
                            Ok(guard) => guard,
                            Err(e) => {
                                error!("Mutex poisoned in release(): {}", e);
                                reply.error(EIO);
                                return;
                            }
                        };
                        if processing.contains_key(&source_path) {
                            warn!("Torrent {} already being processed, skipping", source_path);
                            reply.ok();
                            return;
                        }
                        processing.insert(source_path.clone(), ());
                    }

                    if let Some(ref ts) = self.torrent_service {
                        match ts.add_torrent(&data, &source_path, &name) {
                            Ok(()) => {
                                info!("Successfully processed torrent: {}", name);
                            }
                            Err(e) => {
                                error!("Failed to process torrent {}: {}", name, e);
                                let mut processing = match self.processing_torrents.lock() {
                                    Ok(guard) => guard,
                                    Err(poison) => {
                                        error!(
                                            "Mutex poisoned in release() error path: {}",
                                            poison
                                        );
                                        reply.error(EIO);
                                        return;
                                    }
                                };
                                processing.remove(&source_path);
                            }
                        }
                    } else {
                        // Fallback: no DB configured
                        info!(
                            "Torrent {} received (no DB configured, skipping insert)",
                            name
                        );
                    }

                    let mut processing = match self.processing_torrents.lock() {
                        Ok(guard) => guard,
                        Err(e) => {
                            error!("Mutex poisoned in release() cleanup: {}", e);
                            reply.error(EIO);
                            return;
                        }
                    };
                    processing.remove(&source_path);
                }
            }
        }

        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.error(EISDIR),
            STATS_INO => {
                let offset = offset as usize;
                let stats = self.generate_global_stats_content();
                if offset >= stats.len() {
                    reply.data(&[]);
                } else {
                    let end = std::cmp::min(offset + size as usize, stats.len());
                    reply.data(&stats[offset..end]);
                }
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let stats = self.generate_data_stats_for_ino(ino);
                    let offset = offset as usize;
                    if offset >= stats.len() {
                        reply.data(&[]);
                    } else {
                        let end = std::cmp::min(offset + size as usize, stats.len());
                        reply.data(&stats[offset..end]);
                    }
                    return;
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

                        info!("Read request for torrent file: {} (torrent_id={}, file_id={}, offset={}, size={})",
                              name, torrent_id, file_id, offset, read_size);

                        let actual_size = *file_size as usize;
                        if offset >= actual_size {
                            reply.data(&[]);
                            return;
                        }

                        let end = std::cmp::min(offset + read_size, actual_size);
                        let result_size = end - offset;

                        match self.read_torrent_file_data(
                            *torrent_id,
                            *file_id,
                            offset,
                            result_size,
                        ) {
                            Ok(data) => {
                                reply.data(&data);
                            }
                            Err(e) => {
                                warn!("Failed to read torrent file data: {:?}", e);
                                reply.error(EIO);
                            }
                        }
                    } else {
                        reply.error(ENOENT);
                    }
                    return;
                }

                if let Some(InodeData::File { data, .. }) = &self.inode_mgr.inodes.get(&ino) {
                    let offset = offset as usize;
                    let end = std::cmp::min(offset + size as usize, data.len());
                    if offset < data.len() {
                        reply.data(&data[offset..end]);
                    } else {
                        reply.data(&[]);
                    }
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.opened(0, 0),
            _ => {
                if InodeManager::is_data_ino(ino) {
                    reply.opened(0, 0);
                    return;
                }

                if self.inode_mgr.inodes.contains_key(&ino) {
                    reply.opened(0, 0);
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        if !self.inode_mgr.is_metadata_child(parent) {
            reply.error(EACCES);
            return;
        }

        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".torrent") {
            reply.error(EACCES);
            return;
        }

        if self
            .inode_mgr
            .find_child_by_name(parent, &name_str)
            .is_some()
        {
            reply.error(EEXIST);
            return;
        }

        let new_ino = inodes::NEXT_INO.fetch_add(1, Ordering::SeqCst);

        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name_str.to_string(),
                data: Vec::new(),
            },
        );

        info!(
            "Created file {} with inode {} in {}",
            name_str,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        reply.entry(
            &Duration::from_secs(1),
            &self.inode_mgr.attr_for_file(new_ino, 0),
            0,
        );
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if !self.inode_mgr.is_metadata_child(parent) {
            reply.error(EACCES);
            return;
        }

        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".torrent") {
            reply.error(EACCES);
            return;
        }

        if self
            .inode_mgr
            .find_child_by_name(parent, &name_str)
            .is_some()
        {
            reply.error(EEXIST);
            return;
        }

        let new_ino = inodes::NEXT_INO.fetch_add(1, Ordering::SeqCst);

        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name_str.to_string(),
                data: Vec::new(),
            },
        );

        let fh = inodes::NEXT_FH.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.open_files.insert(fh, new_ino);

        info!(
            "Created file {} with inode {} in {}",
            name_str,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        reply.created(
            &Duration::from_secs(1),
            &self.inode_mgr.attr_for_file(new_ino, 0),
            0,
            fh,
            0,
        );
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if ino == STATS_INO || InodeManager::is_stats_ino(ino) {
            reply.error(EROFS);
            return;
        }

        if let Some(inode_data) = self.inode_mgr.inodes.get_mut(&ino) {
            if let InodeData::File {
                data: ref mut file_data,
                name,
                ..
            } = inode_data
            {
                let offset = offset as usize;

                if offset > file_data.len() {
                    file_data.resize(offset, 0);
                }

                if offset + data.len() > file_data.len() {
                    file_data.resize(offset + data.len(), 0);
                }

                file_data[offset..offset + data.len()].copy_from_slice(data);

                info!("Wrote {} bytes to file {}", data.len(), name);
                reply.written(data.len() as u32);
            } else {
                reply.error(EISDIR);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        if !self.inode_mgr.is_metadata_child(parent) {
            reply.error(EACCES);
            return;
        }

        let name_str = name.to_string_lossy();

        if self
            .inode_mgr
            .find_child_by_name(parent, &name_str)
            .is_some()
        {
            reply.error(EEXIST);
            return;
        }

        let new_ino = inodes::NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::Directory {
                parent,
                name: name_str.to_string(),
            },
        );

        // Persist the directory to the database
        let source_path = if parent == METADATA_INO {
            name_str.to_string()
        } else {
            let parent_source_path = self.inode_mgr.extract_source_path(parent);
            if parent_source_path.is_empty() {
                name_str.to_string()
            } else {
                format!("{}/{}", parent_source_path, name_str)
            }
        };

        if let Some(ref ts) = self.torrent_service {
            let _ = ts.ensure_metadata_directories(&source_path);
        }

        info!(
            "Created directory {} with inode {} in {}",
            name_str,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        reply.entry(
            &Duration::from_secs(1),
            &self.inode_mgr.attr_for_dir(new_ino, true),
            0,
        );
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if let Some(data) = &self.inode_mgr.inodes.get(&ino) {
            match data {
                InodeData::Directory { .. } => {
                    reply.attr(
                        &Duration::from_secs(1),
                        &self
                            .inode_mgr
                            .attr_for_dir(ino, self.inode_mgr.is_metadata_child(ino)),
                    );
                }
                InodeData::File {
                    data: file_data, ..
                } => {
                    reply.attr(
                        &Duration::from_secs(1),
                        &self.inode_mgr.attr_for_file(ino, file_data.len() as u64),
                    );
                }
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name_str = name.to_string_lossy();
        let newname_str = newname.to_string_lossy();

        // Check parent existence — if the source parent inode doesn't exist in the
        // inode table, the directory it references has been removed.  Return ENOENT
        // rather than a misleading EACCES from the metadata-child check below.
        if !self.inode_mgr.inodes.contains_key(&parent) {
            warn!(
                "Rename source parent inode {} not found in inode table",
                parent
            );
            reply.error(ENOENT);
            return;
        }

        // Same check for the target parent.
        if !self.inode_mgr.inodes.contains_key(&newparent) {
            warn!(
                "Rename target parent inode {} not found in inode table (directory does not exist)",
                newparent
            );
            reply.error(ENOENT);
            return;
        }

        if !self.inode_mgr.is_metadata_child(parent) || !self.inode_mgr.is_metadata_child(newparent)
        {
            error!("Rename only allowed within metadata/ directory");
            reply.error(EACCES);
            return;
        }

        let source_ino = match self.inode_mgr.find_child_by_name(parent, &name_str) {
            Some(ino) => ino,
            None => {
                error!("Source file not found: {}", name_str);
                reply.error(ENOENT);
                return;
            }
        };

        if let Some(target_ino) = self.inode_mgr.find_child_by_name(newparent, &newname_str) {
            if target_ino == source_ino {
                info!(
                    "Self-rename detected for '{}' (ino={}), treating as no-op",
                    name_str, source_ino
                );
                reply.ok();
                return;
            }
            error!("Target file already exists: {}", newname_str);
            reply.error(EEXIST);
            return;
        }

        let is_directory = matches!(
            self.inode_mgr.inodes.get(&source_ino),
            Some(InodeData::Directory { .. })
        );

        if is_directory {
            // --- Directory rename ---
            let old_source_path = self.inode_mgr.extract_source_path(source_ino);
            let new_source_path = if newparent == METADATA_INO {
                newname_str.to_string()
            } else {
                let parent_path = self.inode_mgr.extract_source_path(newparent);
                if parent_path.is_empty() {
                    newname_str.to_string()
                } else {
                    format!("{}/{}", parent_path, newname_str)
                }
            };

            self.inode_mgr.inodes.insert(
                source_ino,
                InodeData::Directory {
                    parent: newparent,
                    name: newname_str.to_string(),
                },
            );

            // Update data_inodes cache
            let old_prefix = format!("{}/", old_source_path);
            let new_prefix = format!("{}/", new_source_path);
            for (_, data_inode) in self.inode_mgr.data_inodes.iter_mut() {
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

            // Persist to database
            if let Some(ref ts) = self.torrent_service {
                if let Err(e) =
                    ts.rename_metadata_directory(&old_source_path, &newname_str, &new_source_path)
                {
                    error!("Failed to rename metadata directory in database: {:?}", e);
                    reply.error(EIO);
                    return;
                }
                info!(
                    "Renamed metadata directory '{}' to '{}' (source_path: '{}' -> '{}')",
                    name_str, newname_str, old_source_path, new_source_path
                );
            } else {
                info!(
                    "Renamed directory '{}' to '{}' (no database)",
                    name_str, newname_str
                );
            }

            reply.ok();
        } else {
            // --- File rename ---
            if !name_str.ends_with(".torrent") || !newname_str.ends_with(".torrent") {
                error!("Rename only allowed for .torrent files or directories");
                reply.error(EACCES);
                return;
            }

            let (file_data, old_name) = match self.inode_mgr.inodes.get(&source_ino) {
                Some(InodeData::File { data, name, .. }) => (data.clone(), name.clone()),
                None => {
                    error!("Source inode not found: {}", source_ino);
                    reply.error(ENOENT);
                    return;
                }
                _ => unreachable!(),
            };

            self.inode_mgr.inodes.insert(
                source_ino,
                InodeData::File {
                    parent: newparent,
                    name: newname_str.to_string(),
                    data: file_data,
                },
            );

            if let Some(ref ts) = self.torrent_service {
                let old_source_path = self.inode_mgr.extract_source_path(parent);
                let new_source_path = self.inode_mgr.extract_source_path(newparent);

                if let Err(e) =
                    ts.rename_torrent(&old_name, &old_source_path, &newname_str, &new_source_path)
                {
                    error!("Failed to rename torrent in database: {:?}", e);
                    reply.error(EIO);
                    return;
                }
                info!(
                    "Renamed torrent '{}' to '{}' (source_path: '{}' -> '{}')",
                    old_name, newname_str, old_source_path, new_source_path
                );
            } else {
                info!(
                    "Renamed file '{}' to '{}' (no database)",
                    old_name, newname_str
                );
            }

            reply.ok();
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_string_lossy();

        if !self.inode_mgr.is_metadata_child(parent) {
            error!("Unlink only allowed within metadata/ directory");
            reply.error(EACCES);
            return;
        }

        if !name_str.ends_with(".torrent") {
            error!("Unlink only allowed for .torrent files");
            reply.error(EACCES);
            return;
        }

        let ino = match self.inode_mgr.find_child_by_name(parent, &name_str) {
            Some(ino) => ino,
            None => {
                error!("File not found: {}", name_str);
                reply.error(ENOENT);
                return;
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
                            // DB delete succeeded, clean up in-memory state
                            self.inode_mgr.inodes.remove(&ino);
                            self.inode_mgr
                                .open_files
                                .retain(|_, &mut open_ino| open_ino != ino);
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
                                    _ => true,
                                });

                            let mut processing = match self.processing_torrents.lock() {
                                Ok(guard) => guard,
                                Err(e) => {
                                    error!("Mutex poisoned in unlink() processing_torrents: {}", e);
                                    reply.error(EIO);
                                    return;
                                }
                            };
                            processing.remove(&source_path);
                            drop(processing);

                            let mut cache = match self.torrent_data_cache.lock() {
                                Ok(guard) => guard,
                                Err(e) => {
                                    error!("Mutex poisoned in unlink() torrent_data_cache: {}", e);
                                    reply.error(EIO);
                                    return;
                                }
                            };
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
                            reply.error(EIO);
                            return;
                        }
                    }
                } else {
                    self.inode_mgr.inodes.remove(&ino);
                    self.inode_mgr
                        .open_files
                        .retain(|_, &mut open_ino| open_ino != ino);
                    info!("Deleted file '{}' (no database)", filename);
                }

                reply.ok();
            }
            Some(InodeData::Directory { .. }) => {
                error!("Cannot unlink directory: {}", name_str);
                reply.error(EISDIR);
            }
            None => {
                error!("Inode not found: {}", ino);
                reply.error(ENOENT);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_string_lossy();

        if !self.inode_mgr.is_metadata_child(parent) {
            error!("Rmdir only allowed within metadata/ directory");
            reply.error(EACCES);
            return;
        }

        let ino = match self.inode_mgr.find_child_by_name(parent, &name_str) {
            Some(ino) => ino,
            None => {
                error!("Directory not found: {}", name_str);
                reply.error(ENOENT);
                return;
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
                    error!("Directory not empty: {}", name_str);
                    reply.error(ENOTEMPTY);
                    return;
                }

                let source_path = self.inode_mgr.extract_source_path(ino);

                self.inode_mgr.inodes.remove(&ino);

                // Clean up data_inodes cache
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
                    name_str, source_path
                );

                reply.ok();
            }
            Some(InodeData::File { .. }) => {
                error!("Cannot rmdir file: {}", name_str);
                reply.error(ENOTDIR);
            }
            None => {
                error!("Inode not found: {}", ino);
                reply.error(ENOENT);
            }
        }
    }
}

impl TorrentFs {
    /// Generate global stats (delegates to StatsGenerator).
    pub fn generate_stats(&self) -> Vec<u8> {
        self.generate_global_stats_content()
    }

    /// Generate global stats content for root .stats file.
    fn generate_global_stats_content(&self) -> Vec<u8> {
        let get_cm = || self.get_cache_manager();
        generate_global_stats(
            self.inode_mgr.creation_time,
            &self.db,
            &self.download_service,
            get_cm,
            &self.listen_addr,
        )
    }

    /// Generate directory-aggregated stats for a source_path.
    fn generate_dir_stats_content(&self, source_path: &str) -> Vec<u8> {
        let get_cm = || self.get_cache_manager();
        generate_directory_stats(source_path, &self.db, &self.download_service, get_cm)
    }

    /// Generate single-torrent stats for a torrent_id.
    fn generate_torrent_stats_for_id(&self, torrent_id: i64) -> Vec<u8> {
        // Extract info_hash inside a short-lived lock scope, then release before
        // calling generate_torrent_stats() which acquires its own lock (avoids Mutex
        // deadlock — std::sync::Mutex is not reentrant).
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

    /// Dispatch stats generation for a stats inode based on its parent directory inode.
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
