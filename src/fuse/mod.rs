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
pub mod worker_pool;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::raw::c_int;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use tracing::warn;

pub use self::fs_service::FsService;
use self::fs_types::{Attr, FileKind, ReadOutcome, StatsKind};
pub use self::worker_pool::WorkerPool;

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::db::Database;
use crate::domain::fs_error::FsError;
use crate::infrastructure::metrics::Metrics;
use crate::services::download::DownloadService;

// ── Pending reply table ────────────────────────────────────────────────────

/// Maximum number of concurrent pending (deferred) read replies.
const MAX_PENDING: usize = 256;

/// A reply that can be resolved exactly once — either with data or an errno.
///
/// Implemented for `fuser::ReplyData` in production and for a counting mock in
/// tests, which lets the pending-table invariants (bounded capacity, deadline,
/// cancellation, exactly-once consumption) be unit-tested without a FUSE
/// session.
trait PendingReply {
    fn resolve_data(self, data: &[u8]);
    fn resolve_error(self, errno: libc::c_int);
}

impl PendingReply for ReplyData {
    fn resolve_data(self, data: &[u8]) {
        self.data(data);
    }
    fn resolve_error(self, errno: libc::c_int) {
        self.error(errno);
    }
}

/// A single entry in the pending reply table.
struct PendingEntry<R: PendingReply> {
    reply: R,
    torrent_id: i64,
    deadline: Instant,
}

/// Bounded table of FUSE read replies that are waiting for pieces to download.
///
/// * Capacity is bounded at `MAX_PENDING`; overflow returns `EBUSY` backpressure
///   to the kernel (not EIO, not truncated data).
/// * Each entry carries a deadline; a background thread expires overdue entries
///   with EIO.
/// * `unlink` / `remove_torrent` cancels in-flight reads for the removed
///   torrent and resolves their tickets with EIO.
/// * Every reply is consumed exactly once (ok or error), zero leak.
struct PendingTable<R: PendingReply = ReplyData> {
    entries: HashMap<u64, PendingEntry<R>>,
    next_id: u64,
}

impl<R: PendingReply> PendingTable<R> {
    fn new() -> Self {
        Self {
            entries: HashMap::with_capacity(MAX_PENDING),
            next_id: 0,
        }
    }

    /// Insert a pending reply.  Returns `Err(reply)` when the table is full
    /// (backpressure) so the caller can consume the reply with EBUSY.
    fn insert(&mut self, reply: R, torrent_id: i64, deadline: Instant) -> Result<u64, R> {
        if self.entries.len() >= MAX_PENDING {
            return Err(reply);
        }
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.entries.insert(
            id,
            PendingEntry {
                reply,
                torrent_id,
                deadline,
            },
        );
        Ok(id)
    }

    /// Resolve a pending entry with data.
    fn resolve(&mut self, id: u64, data: &[u8]) {
        if let Some(entry) = self.entries.remove(&id) {
            entry.reply.resolve_data(data);
        }
    }

    /// Resolve a pending entry with an errno.
    fn resolve_error(&mut self, id: u64, errno: libc::c_int) {
        if let Some(entry) = self.entries.remove(&id) {
            entry.reply.resolve_error(errno);
        }
    }

    /// Cancel all pending entries for a given `torrent_id` (unlink / remove_torrent).
    /// Returns the number of entries cancelled.
    fn cancel_by_torrent_id(&mut self, torrent_id: i64) -> usize {
        let ids: Vec<u64> = self
            .entries
            .iter()
            .filter(|(_, e)| e.torrent_id == torrent_id)
            .map(|(id, _)| *id)
            .collect();
        let count = ids.len();
        for id in &ids {
            if let Some(entry) = self.entries.remove(id) {
                entry.reply.resolve_error(libc::EIO);
            }
        }
        count
    }

    /// Expire all entries whose deadline has passed.  Resolves them with EIO.
    /// Returns the number of entries expired.
    fn expire(&mut self) -> usize {
        let now = Instant::now();
        let ids: Vec<u64> = self
            .entries
            .iter()
            .filter(|(_, e)| e.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        let count = ids.len();
        for id in &ids {
            if let Some(entry) = self.entries.remove(id) {
                entry.reply.resolve_error(libc::EIO);
            }
        }
        count
    }
}

/// FUSE entry TTL (seconds).
const TTL: Duration = Duration::from_secs(1);

pub struct TorrentFs {
    service: FsService,
    pending_table: Arc<Mutex<PendingTable>>,
    read_timeout_secs: u64,
    worker_pool: Arc<WorkerPool>,
}
impl TorrentFs {
    fn read_timeout(config: &TorrentfsConfig) -> u64 {
        config
            .timeouts
            .read_timeout_secs
            .map(|v| if v > 0 { v as u64 } else { 30 })
            .unwrap_or(30)
    }

    /// Number of download worker threads (bounded pool). Defaults to the
    /// number of logical CPUs when the config leaves it unset.
    fn download_workers(config: &TorrentfsConfig) -> usize {
        config
            .concurrency
            .download_workers
            .filter(|&v| v > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
            })
    }

    /// Capacity of the bounded download submission queue. Defaults to 256.
    fn download_queue_depth(config: &TorrentfsConfig) -> usize {
        config
            .concurrency
            .download_queue_depth
            .filter(|&v| v > 0)
            .unwrap_or(256)
    }

    /// Spawn a background thread that expires overdue pending replies every
    /// second, resolving each with EIO and decrementing the pending-reads
    /// metric once per expired entry.
    fn spawn_deadline_checker(pending_table: Arc<Mutex<PendingTable>>, metrics: Arc<Metrics>) {
        std::thread::spawn(move || {
            let tick = Duration::from_secs(1);
            loop {
                std::thread::sleep(tick);
                if let Ok(mut table) = pending_table.lock() {
                    let expired = table.expire();
                    if expired > 0 {
                        for _ in 0..expired {
                            metrics.pending_reads_dec();
                        }
                        warn!(
                            "Pending table: {} entries expired (deadline exceeded)",
                            expired
                        );
                    }
                }
            }
        });
    }

    pub fn new_with_cache_path(cache_path: PathBuf, config: &TorrentfsConfig) -> Self {
        let timeout = Self::read_timeout(config);
        let service = FsService::new_with_cache_path(cache_path, config);
        let pending_table = Arc::new(Mutex::new(PendingTable::new()));
        let metrics = service.metrics.clone();
        Self::spawn_deadline_checker(pending_table.clone(), metrics);
        let worker_pool = WorkerPool::new(
            Self::download_workers(config),
            Self::download_queue_depth(config),
        );
        Self {
            service,
            pending_table,
            read_timeout_secs: timeout,
            worker_pool,
        }
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        let service = FsService::new();
        let pending_table = Arc::new(Mutex::new(PendingTable::new()));
        let metrics = service.metrics.clone();
        Self::spawn_deadline_checker(pending_table.clone(), metrics);
        let worker_pool = WorkerPool::new(
            Self::download_workers(&TorrentfsConfig::default_config()),
            Self::download_queue_depth(&TorrentfsConfig::default_config()),
        );
        Self {
            service,
            pending_table,
            read_timeout_secs: 30,
            worker_pool,
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
        let timeout = Self::read_timeout(config);
        let service = FsService::new_with_db_and_cache(db, cache_path, config);
        let pending_table = Arc::new(Mutex::new(PendingTable::new()));
        let metrics = service.metrics.clone();
        Self::spawn_deadline_checker(pending_table.clone(), metrics);
        let worker_pool = WorkerPool::new(
            Self::download_workers(config),
            Self::download_queue_depth(config),
        );
        Self {
            service,
            pending_table,
            read_timeout_secs: timeout,
            worker_pool,
        }
    }

    /// The download service, for the background alert consumer.
    pub fn download_service(&self) -> Option<&Arc<DownloadService>> {
        self.service.download_service.as_ref()
    }

    /// The bounded download worker pool, for graceful shutdown from `main`.
    pub fn worker_pool(&self) -> Arc<WorkerPool> {
        self.worker_pool.clone()
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
                info_hash,
                torrent_id,
            }) => {
                // Insert into the bounded pending table.
                // Backpressure: if the table is full, reply EBUSY so the
                // kernel retries instead of getting EIO or truncated data.
                let deadline = Instant::now() + Duration::from_secs(self.read_timeout_secs + 5);
                let id = match self.pending_table.lock() {
                    Ok(mut table) => match table.insert(reply, torrent_id, deadline) {
                        Ok(id) => {
                            self.service.metrics.pending_reads_inc();
                            warn!(
                                "Deferred read queued (ticket={}, info_hash={}, torrent_id={})",
                                id, info_hash, torrent_id
                            );
                            id
                        }
                        Err(reply) => {
                            // Table full — backpressure.
                            warn!(
                                "Pending table full ({} entries), returning EBUSY",
                                MAX_PENDING
                            );
                            return reply.error(
                                FsError::ResourceBusy("too many pending reads, retry".to_string())
                                    .into(),
                            );
                        }
                    },
                    Err(_) => {
                        return reply.error(FsError::LockPoisoned.into());
                    }
                };
                // Dispatch the read to the bounded worker pool and reply
                // asynchronously.
                match self.service.download_service.clone() {
                    Some(ds) => {
                        let metrics = self.service.metrics.clone();
                        let pt = self.pending_table.clone();
                        let stopping = self.worker_pool.stopping_flag();
                        let job = Box::new(move || {
                            let _worker = metrics.worker_guard();
                            match ds.read_file_range_blocking(
                                &info, file_index, offset, size, &stopping,
                            ) {
                                Ok(data) => {
                                    if let Ok(mut table) = pt.lock() {
                                        table.resolve(id, &data);
                                        metrics.pending_reads_dec();
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to read torrent file data (async): {:?}", e);
                                    if let Ok(mut table) = pt.lock() {
                                        table.resolve_error(id, FsError::from(e).into());
                                        metrics.pending_reads_dec();
                                    }
                                }
                            }
                        });
                        if let Err(_job) = self.worker_pool.try_submit(job) {
                            // Worker queue full (or shutting down) — backpressure.
                            // Resolve the pending ticket with EBUSY so the kernel
                            // retries instead of getting EIO or truncated data.
                            warn!("Download worker queue full, returning EBUSY");
                            if let Ok(mut table) = self.pending_table.lock() {
                                table.resolve_error(
                                    id,
                                    FsError::ResourceBusy(
                                        "download worker queue full, retry".to_string(),
                                    )
                                    .into(),
                                );
                                self.service.metrics.pending_reads_dec();
                            }
                        }
                    }
                    None => {
                        if let Ok(mut table) = self.pending_table.lock() {
                            table.resolve_error(
                                id,
                                FsError::Internal("download manager not available".to_string())
                                    .into(),
                            );
                            self.service.metrics.pending_reads_dec();
                        }
                    }
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

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.service.unlink(parent, &name.to_string_lossy()) {
            Ok(Some(torrent_id)) => {
                // Cancel any in-flight reads for the removed torrent.
                if let Ok(mut table) = self.pending_table.lock() {
                    let cancelled = table.cancel_by_torrent_id(torrent_id);
                    if cancelled > 0 {
                        warn!(
                            "Cancelled {} pending read(s) for removed torrent_id={}",
                            cancelled, torrent_id
                        );
                        for _ in 0..cancelled {
                            self.service.metrics.pending_reads_dec();
                        }
                    }
                }
                reply.ok();
            }
            Ok(None) => reply.ok(),
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

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.service.rmdir(parent, &name.to_string_lossy()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
}

#[cfg(test)]
mod pending_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts how many times the reply is resolved (with data or error).  This
    /// is the test double for `fuser::ReplyData` and verifies the "consumed
    /// exactly once" invariant.
    #[derive(Debug)]
    struct MockReply {
        resolves: Arc<AtomicUsize>,
    }

    impl PendingReply for MockReply {
        fn resolve_data(self, _data: &[u8]) {
            self.resolves.fetch_add(1, Ordering::Relaxed);
        }
        fn resolve_error(self, _errno: libc::c_int) {
            self.resolves.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn deadline_in(d: std::time::Duration) -> Instant {
        Instant::now() + d
    }

    #[test]
    fn capacity_is_bounded_with_backpressure() {
        let mut table: PendingTable<MockReply> = PendingTable::new();
        let resolves = Arc::new(AtomicUsize::new(0));

        for i in 0..MAX_PENDING {
            let r = table.insert(
                MockReply {
                    resolves: resolves.clone(),
                },
                i as i64,
                deadline_in(std::time::Duration::from_secs(60)),
            );
            assert!(r.is_ok(), "insert {i} should succeed");
        }

        // The (MAX_PENDING + 1)-th insert must be rejected: backpressure.
        let r = table.insert(
            MockReply {
                resolves: resolves.clone(),
            },
            MAX_PENDING as i64,
            deadline_in(std::time::Duration::from_secs(60)),
        );
        assert!(r.is_err(), "insert at capacity must fail");
        // The returned reply was not consumed.
        assert_eq!(resolves.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn cancel_by_torrent_id_resolves_only_matching_tickets() {
        let mut table: PendingTable<MockReply> = PendingTable::new();
        let resolves = Arc::new(AtomicUsize::new(0));

        let a1 = table
            .insert(
                MockReply {
                    resolves: resolves.clone(),
                },
                1,
                deadline_in(std::time::Duration::from_secs(60)),
            )
            .unwrap();
        let a2 = table
            .insert(
                MockReply {
                    resolves: resolves.clone(),
                },
                1,
                deadline_in(std::time::Duration::from_secs(60)),
            )
            .unwrap();
        let b1 = table
            .insert(
                MockReply {
                    resolves: resolves.clone(),
                },
                2,
                deadline_in(std::time::Duration::from_secs(60)),
            )
            .unwrap();

        let cancelled = table.cancel_by_torrent_id(1);
        assert_eq!(cancelled, 2);
        assert_eq!(resolves.load(Ordering::Relaxed), 2);

        // The remaining entry (torrent_id=2) resolves exactly once.
        table.resolve(b1, b"ok");
        assert_eq!(resolves.load(Ordering::Relaxed), 3);

        // Cancelled tickets are gone: resolving them again is a no-op.
        table.resolve(a1, b"dup");
        table.resolve(a2, b"dup");
        assert_eq!(resolves.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn expire_resolves_overdue_tickets_only() {
        let mut table: PendingTable<MockReply> = PendingTable::new();
        let resolves = Arc::new(AtomicUsize::new(0));

        let overdue = table
            .insert(
                MockReply {
                    resolves: resolves.clone(),
                },
                7,
                deadline_in(std::time::Duration::from_secs(0)),
            )
            .unwrap();
        let future = table
            .insert(
                MockReply {
                    resolves: resolves.clone(),
                },
                7,
                deadline_in(std::time::Duration::from_secs(60)),
            )
            .unwrap();

        let expired = table.expire();
        assert_eq!(expired, 1);
        assert_eq!(resolves.load(Ordering::Relaxed), 1);

        // The future entry is unaffected.
        table.resolve(future, b"ok");
        assert_eq!(resolves.load(Ordering::Relaxed), 2);

        // The overdue entry is gone.
        table.resolve(overdue, b"dup");
        assert_eq!(resolves.load(Ordering::Relaxed), 2);
    }
}
