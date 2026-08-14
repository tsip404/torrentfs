//! FUSE module — the thin protocol adapter (`TorrentFs`) over `FsService`.
//!
//! `TorrentFs` is now pure glue: every `Filesystem` method converts the FUSE
//! request parameters into a domain call, delegates to `FsService`, and
//! converts the domain result back into a `fuser` reply. All domain logic,
//! inode management, data resolution and stats rendering live in `FsService`
//! (and its helpers), which is unit-testable without a FUSE session.
//!
//! The errno mapping (`impl From<FsError> for libc::c_int`) lives in `errno`
//! — the single exit point from domain errors to kernel error codes.

pub mod errno;
pub mod fs_service;
pub mod fs_types;
pub mod inodes;
pub mod lookup;
pub mod stats;

use std::ffi::OsStr;
use std::os::raw::c_int;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use tracing::warn;

pub use self::fs_service::FsService;
use self::fs_types::{Attr, FileKind, ReadOutcome, StatsKind};

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::db::Database;
use crate::domain::fs_error::FsError;
use crate::services::download::DownloadService;

/// FUSE entry TTL (seconds).
const TTL: Duration = Duration::from_secs(1);

pub struct TorrentFs {
    service: FsService,
}

impl TorrentFs {
    pub fn new_with_cache_path(cache_path: PathBuf, config: &TorrentfsConfig) -> Self {
        Self {
            service: FsService::new_with_cache_path(cache_path, config),
        }
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            service: FsService::new(),
        }
    }

    #[allow(dead_code)]
    pub fn new_with_db(_db: Database) -> Self {
        Self::new()
    }

    pub fn new_with_db_and_cache(
        db: Database,
        cache_path: PathBuf,
        config: &TorrentfsConfig,
    ) -> Self {
        Self {
            service: FsService::new_with_db_and_cache(db, cache_path, config),
        }
    }

    /// The download service, for the background alert consumer.
    pub fn download_service(&self) -> Option<&Arc<DownloadService>> {
        self.service.download_service.as_ref()
    }

    /// Get the CacheManager shared with DownloadService.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        self.service.get_cache_manager()
    }

    /// Generate global stats (delegates to FsService).
    pub fn generate_stats(&self) -> Vec<u8> {
        self.service.read_stats(StatsKind::Global)
    }

    /// Convert a domain `Attr` into a `fuser::FileAttr`, filling the fields the
    /// adapter owns: timestamps (from inode creation time) and uid/gid (process).
    fn to_fuse_attr(&self, attr: &Attr) -> fuser::FileAttr {
        let t = self.service.inode_mgr.creation_time;
        fuser::FileAttr {
            ino: attr.ino,
            size: attr.size,
            blocks: attr.size.div_ceil(512),
            atime: UNIX_EPOCH + t,
            mtime: UNIX_EPOCH + t,
            ctime: UNIX_EPOCH + t,
            crtime: UNIX_EPOCH + t,
            kind: kind_to_fuse(attr.kind),
            perm: attr.perm,
            nlink: attr.nlink,
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

fn kind_to_fuse(kind: FileKind) -> fuser::FileType {
    match kind {
        FileKind::Directory => fuser::FileType::Directory,
        FileKind::RegularFile => fuser::FileType::RegularFile,
    }
}

// NOTE: fuser 0.16 requires `&mut self` on all Filesystem trait methods.
// Upgrading to a fuser version that accepts `&self` would enable true
// multi-threaded FUSE dispatch without serializing on a global lock.

impl Filesystem for TorrentFs {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), c_int> {
        if let Err(e) = config.add_capabilities(fuser::consts::FUSE_ASYNC_READ) {
            tracing::warn!("Failed to set FUSE_CAP_ASYNC_READ: {:?}", e);
        } else {
            tracing::info!("FUSE_CAP_ASYNC_READ enabled");
        }
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.service.lookup(parent, &name.to_string_lossy()) {
            Ok(Some(entry)) => reply.entry(&TTL, &self.to_fuse_attr(&entry.attr), 0),
            Ok(None) => reply.error(FsError::NotFound.into()),
            Err(e) => reply.error(e.into()),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.service.getattr(ino) {
            Ok(attr) => reply.attr(&TTL, &self.to_fuse_attr(&attr)),
            Err(e) => reply.error(e.into()),
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
        match self.service.readdir(ino, offset) {
            Ok(entries) => {
                for entry in entries {
                    if reply.add(
                        entry.ino,
                        entry.offset,
                        kind_to_fuse(entry.kind),
                        &entry.name,
                    ) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.service.open(ino) {
            Ok(fh) => reply.opened(fh, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn flush(&mut self, _req: &Request, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        match self.service.flush(ino) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
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
        match self.service.release(fh) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
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
        match self.service.read(ino, offset, size) {
            Ok(ReadOutcome::Ready(data)) => reply.data(&data),
            Ok(ReadOutcome::Pending {
                info,
                file_index,
                offset,
                size,
            }) => {
                // Complete the read on a worker thread and reply asynchronously.
                // The worker blocks until the requested pieces are downloaded,
                // keeping the FUSE dispatch loop free for readdir/.stats
                // (TSI-2114 / TSI-2133).
                match self.service.download_service.clone() {
                    Some(ds) => {
                        let metrics = self.service.metrics.clone();
                        std::thread::spawn(move || {
                            let _worker = metrics.worker_guard();
                            match ds.read_file_range_blocking(&info, file_index, offset, size) {
                                Ok(data) => reply.data(&data),
                                Err(e) => {
                                    warn!("Failed to read torrent file data (async): {:?}", e);
                                    reply.error(FsError::from(e).into());
                                }
                            }
                        });
                    }
                    None => reply.error(
                        FsError::Internal("download manager not available".to_string()).into(),
                    ),
                }
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.service.opendir(ino) {
            Ok(()) => reply.opened(0, 0),
            Err(e) => reply.error(e.into()),
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
        match self.service.mknod(parent, &name.to_string_lossy()) {
            Ok(entry) => reply.entry(&TTL, &self.to_fuse_attr(&entry.attr), 0),
            Err(e) => reply.error(e.into()),
        }
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
        match self.service.create(parent, &name.to_string_lossy()) {
            Ok(created) => reply.created(&TTL, &self.to_fuse_attr(&created.attr), 0, created.fh, 0),
            Err(e) => reply.error(e.into()),
        }
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
        match self.service.write(ino, offset, data) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(e.into()),
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
        match self.service.mkdir(parent, &name.to_string_lossy()) {
            Ok(attr) => reply.entry(&TTL, &self.to_fuse_attr(&attr), 0),
            Err(e) => reply.error(e.into()),
        }
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
        // Attributes are virtual and immutable; return the current attributes.
        match self.service.getattr(ino) {
            Ok(attr) => reply.attr(&TTL, &self.to_fuse_attr(&attr)),
            Err(e) => reply.error(e.into()),
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
        match self.service.rename(
            parent,
            &name.to_string_lossy(),
            newparent,
            &newname.to_string_lossy(),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.service.unlink(parent, &name.to_string_lossy()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.service.rmdir(parent, &name.to_string_lossy()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
}
