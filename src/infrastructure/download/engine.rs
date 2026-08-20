//! `DownloadEngine` — single-owner-thread actor for the download subsystem.
//!
//! The engine thread exclusively owns the libtorrent `Session`, all torrent
//! `TorrentHandle`s, the [`PieceStore`] (data plane) and the [`PieceScheduler`]
//! (control plane / priority).  External callers send [`Command`]s over an
//! `mpsc` channel and receive results on a `sync_channel`; the raw libtorrent
//! pointers therefore never cross a thread boundary, which lets `Session` and
//! `TorrentHandle` drop their `unsafe impl Send`.
//!
//! Non-blocking `.stats` reads go through a shared [`DownloadSnapshot`] that
//! the engine refreshes each tick, replacing the old `try_lock` on the
//! `DownloadManager` big lock (TSI-2119).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::alert::{AlertConsumer, SharedSessionStats};
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::config::TorrentfsConfig;
use crate::infrastructure::metadata::TorrentInfo;
use crate::infrastructure::metrics::Metrics;
use crate::seeding::SeedingManager;

use super::piece_scheduler::{PiecePriorityConfig, PieceScheduler, PieceStatus};
use super::piece_store::PieceStore;
use super::session::{Session, TorrentHandle};
use super::types::{SessionStats, TorrentState, TorrentStatus};

/// A command sent to the download engine actor.
pub enum Command {
    /// Ensure a lightweight handle exists for a torrent (no download).
    EnsureHandle {
        info: Arc<TorrentInfo>,
        reply: SyncSender<TorrentResult<()>>,
    },
    /// Fire-and-forget variant of [`Command::EnsureHandle`]: the engine
    /// creates the handle eventually but the sender does not wait for it.
    /// Used by the FUSE write path (torrent release), which must never block
    /// the single-threaded FUSE dispatch loop on a busy download.
    EnsureHandleAsync { info: Arc<TorrentInfo> },
    /// Read a byte range from a file, driving the piece download if needed.
    /// Blocks on the engine thread until the pieces are available.
    ReadFileRange {
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
        reply: SyncSender<TorrentResult<Vec<u8>>>,
    },
    /// Session-level statistics.
    GetSessionStats {
        reply: SyncSender<TorrentResult<SessionStats>>,
    },
    /// Piece status for a torrent (used by `.stats`).
    GetPiecesStatus {
        info_hash: String,
        num_pieces: i32,
        reply: SyncSender<TorrentResult<Vec<PieceStatus>>>,
    },
    /// Register a seeding manager (receives eviction + piece-ready callbacks).
    RegisterSeeding {
        seeding: Arc<SeedingManager>,
        reply: SyncSender<()>,
    },
    /// Stop the engine thread.
    Shutdown,
}

/// Shared, non-blocking snapshot of the download subsystem state.
#[derive(Default)]
pub struct DownloadSnapshot {
    /// Per-info_hash torrent status.
    pub statuses: HashMap<String, TorrentStatus>,
    /// Per-info_hash `(piece_length, piece statuses)`.
    pub pieces: HashMap<String, (u64, Vec<PieceStatus>)>,
}

/// Handle to a running download engine.  Cheap to clone (`Send + Sync`).
pub struct DownloadEngine {
    tx: mpsc::Sender<Command>,
    stopping: Arc<AtomicBool>,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
    cache_manager: Arc<Mutex<CacheManager>>,
    shared_stats: SharedSessionStats,
    snapshot: Arc<Mutex<DownloadSnapshot>>,
    metrics: Arc<Metrics>,
    read_timeout_secs: u64,
}

/// State owned exclusively by the engine thread.
struct EngineState {
    session: Session,
    handles: HashMap<String, TorrentHandle>,
    store: PieceStore,
    scheduler: PieceScheduler,
    cache_dir: String,
    read_timeout_secs: u64,
    seeding: Option<Arc<SeedingManager>>,
    metrics: Arc<Metrics>,
    snapshot: Arc<Mutex<DownloadSnapshot>>,
    stopping: Arc<AtomicBool>,
    alert_consumer: Option<AlertConsumer>,
}

impl DownloadEngine {
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        Self::new_with_metrics(cache_dir, config, Arc::new(Metrics::new()))
    }

    pub fn new_with_metrics(
        cache_dir: &Path,
        config: &TorrentfsConfig,
        metrics: Arc<Metrics>,
    ) -> TorrentResult<Self> {
        let cache_dir_str = cache_dir.to_string_lossy().into_owned();
        let pieces_dir = cache_dir.join("pieces");
        std::fs::create_dir_all(&pieces_dir)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;

        // Send-safe shared state, created on the caller's thread.
        let cache_manager = Arc::new(Mutex::new(CacheManager::new(
            cache_dir,
            1024 * 1024 * 1024,
        )?));
        let store = PieceStore::new(cache_manager.clone());
        let scheduler = PieceScheduler::new(PiecePriorityConfig::from_toml(&config.piece_priority));

        let read_timeout_secs = config
            .timeouts
            .read_timeout_secs
            .map(|v| if v > 0 { v as u64 } else { 30 })
            .unwrap_or(30);

        let (tx, rx) = mpsc::channel::<Command>();
        let stopping = Arc::new(AtomicBool::new(false));
        let shared_stats = SharedSessionStats::new();
        let snapshot = Arc::new(Mutex::new(DownloadSnapshot::default()));

        // The libtorrent `Session` owns a raw pointer and is not `Send`, so it
        // must be created on the engine thread itself.  Everything else moved
        // into the closure is `Send`.
        let (init_tx, init_rx) = mpsc::sync_channel::<TorrentResult<()>>(1);
        let config = config.clone();

        // Clones moved into the engine thread; the originals remain for the
        // returned handle.
        let thread_shared_stats = shared_stats.clone();
        let thread_snapshot = snapshot.clone();
        let thread_stopping = stopping.clone();
        let thread_metrics = metrics.clone();

        let handle = std::thread::Builder::new()
            .name("download-engine".into())
            .spawn(move || {
                let session = match Session::new_with_custom_storage(&config, &pieces_dir) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = init_tx.send(Err(e));
                        return;
                    }
                };
                // Spawn the dedicated alert consumer (design §4.2): it drains
                // libtorrent alerts event-driven via `set_alert_notify`, so the
                // engine loop no longer polls. `pop_alerts` is mutex-serialized
                // and thread-safe on the C++ side, so only the raw session
                // pointer crosses this thread boundary.
                // SAFETY: `session` outlives the consumer — `engine_loop` calls
                // `consumer.stop()` (unregistering the notify hook) before the
                // session is dropped.
                let alert_consumer = unsafe {
                    AlertConsumer::spawn(
                        session.inner(),
                        thread_shared_stats.clone(),
                        thread_metrics.clone(),
                    )
                };
                let state = EngineState {
                    session,
                    handles: HashMap::new(),
                    store,
                    scheduler,
                    cache_dir: cache_dir_str,
                    read_timeout_secs,
                    seeding: None,
                    metrics: thread_metrics,
                    snapshot: thread_snapshot,
                    stopping: thread_stopping,
                    alert_consumer: Some(alert_consumer),
                };
                let _ = init_tx.send(Ok(()));
                engine_loop(state, rx);
            })
            .map_err(|e| TorrentError::Unknown {
                code: -1,
                message: format!("Failed to spawn download engine thread: {}", e),
            })?;

        init_rx
            .recv()
            .map_err(|_| TorrentError::Unknown {
                code: -1,
                message: "Download engine thread disconnected before init".to_string(),
            })??;

        Ok(DownloadEngine {
            tx,
            stopping,
            join: Arc::new(Mutex::new(Some(handle))),
            cache_manager,
            shared_stats,
            snapshot,
            metrics,
            read_timeout_secs,
        })
    }

    // ── Non-blocking snapshots (used by `.stats`) ────────────────────

    /// Shared cache handle (non-blocking `.stats` / on-disk checks).
    pub fn cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache_manager.clone()
    }

    /// Cached session stats snapshot.
    pub fn snapshot_stats(&self) -> SessionStats {
        self.shared_stats.snapshot()
    }

    /// Non-blocking torrent status from the last engine snapshot.
    pub fn try_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        self.snapshot
            .try_lock()
            .ok()?
            .statuses
            .get(info_hash)
            .cloned()
    }

    /// Non-blocking piece status from the last engine snapshot.
    pub fn try_pieces_status(&self, info_hash: &str) -> Option<(u64, Vec<PieceStatus>)> {
        self.snapshot.try_lock().ok()?.pieces.get(info_hash).cloned()
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn read_timeout_secs(&self) -> u64 {
        self.read_timeout_secs
    }

    // ── Command senders ──────────────────────────────────────────────

    fn send(&self, cmd: Command) -> TorrentResult<()> {
        self.tx.send(cmd).map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Download engine has shut down".to_string(),
        })
    }

    /// Ensure a lightweight handle exists for a torrent.
    ///
    /// Blocks the caller until the engine thread has created (or found) the
    /// handle.  Only call this from contexts that may block (tests, the
    /// engine's own read path); never from the FUSE dispatch loop, where a
    /// busy download would stall every other filesystem operation.
    pub fn ensure_handle(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::EnsureHandle { info, reply: tx })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Ensure a lightweight handle exists for a torrent without waiting for
    /// the engine thread to finish.
    ///
    /// The command is queued and executed eventually (the `Arc<TorrentInfo>`
    /// keeps the metadata alive).  Used by the FUSE release path so a torrent
    /// write never blocks the single-threaded FUSE dispatch loop on a busy
    /// download; the handle is created lazily on first read either way.
    pub fn ensure_handle_async(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        self.send(Command::EnsureHandleAsync { info })
    }

    /// Read a file range, driving piece download on the engine thread.
    pub fn read_file_range(
        &self,
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::ReadFileRange {
            info,
            file_index,
            offset,
            size,
            reply: tx,
        })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Session-level statistics (synchronous FFI on the engine thread).
    pub fn get_session_stats(&self) -> TorrentResult<SessionStats> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::GetSessionStats { reply: tx })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Piece status for a torrent (synchronous).
    pub fn get_pieces_status(
        &self,
        info_hash: &str,
        num_pieces: i32,
    ) -> TorrentResult<Vec<PieceStatus>> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::GetPiecesStatus {
            info_hash: info_hash.to_string(),
            num_pieces,
            reply: tx,
        })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Register a seeding manager for eviction + piece-ready callbacks.
    pub fn register_seeding(&self, seeding: Arc<SeedingManager>) {
        let (tx, rx) = mpsc::sync_channel(1);
        if self.send(Command::RegisterSeeding { seeding, reply: tx }).is_ok() {
            let _ = rx.recv();
        }
    }

    /// Stop the engine: abort in-flight reads and join the thread.
    /// Idempotent.
    pub fn shutdown(&self) {
        self.stopping.store(true, Ordering::Relaxed);
        let _ = self.tx.send(Command::Shutdown);
        if let Some(handle) = self.join.lock().ok().and_then(|mut g| g.take()) {
            let _ = handle.join();
        }
    }

    fn disconnected() -> TorrentError {
        TorrentError::Unknown {
            code: -1,
            message: "Download engine thread disconnected".to_string(),
        }
    }
}

impl Drop for DownloadEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ── Engine loop ────────────────────────────────────────────────────────────

/// libtorrent `torrent_flags::upload_mode` numeric value (`1 << 1`).
const UPLOAD_MODE_FLAG: u64 = 1 << 1;

/// Snapshot refresh interval. Alerts are no longer drained here — a dedicated
/// consumer thread handles them event-driven via `set_alert_notify` — so this
/// interval only bounds `.stats` staleness for per-torrent status/pieces.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);


fn engine_loop(mut state: EngineState, rx: Receiver<Command>) {
    tracing::info!("Download engine started");
    loop {
        // Block on commands; wake every `SNAPSHOT_INTERVAL` to refresh the
        // non-blocking `.stats` snapshot (design §4.2: no alert polling here).
        match rx.recv_timeout(SNAPSHOT_INTERVAL) {
            Ok(cmd) => {
                let stop = state.handle_command(cmd);
                state.publish_snapshot();
                if stop {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                state.publish_snapshot();
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    // Unregister the alert-notify hook before the session is dropped.
    if let Some(consumer) = state.alert_consumer.take() {
        consumer.stop();
    }
    tracing::info!("Download engine stopped");
}

impl EngineState {
    /// Handle one command; returns `true` when the engine should stop.
    fn handle_command(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::EnsureHandle { info, reply } => {
                let _ = reply.send(self.ensure_handle(&info));
            }
            Command::EnsureHandleAsync { info } => {
                let _ = self.ensure_handle(&info);
            }
            Command::ReadFileRange {
                info,
                file_index,
                offset,
                size,
                reply,
            } => {
                let _ = reply.send(self.read_file_range(&info, file_index, offset, size));
            }
            Command::GetSessionStats { reply } => {
                let _ = reply.send(self.session.get_stats());
            }
            Command::GetPiecesStatus {
                info_hash,
                num_pieces,
                reply,
            } => {
                let _ = reply.send(self.build_pieces_status(&info_hash, num_pieces));
            }
            Command::RegisterSeeding { seeding, reply } => {
                self.seeding = Some(seeding);
                let _ = reply.send(());
            }
            Command::Shutdown => return true,
        }
        false
    }

    /// Ensure a lightweight handle exists for the torrent.
    ///
    /// The handle is added in upload_mode: it connects to trackers and peers
    /// (so peer/seed info is visible immediately) but never requests pieces,
    /// so nothing is downloaded until the first read that needs data.  On that
    /// read, [`Self::read_file_range`] clears the upload_mode flag to switch
    /// the torrent into download mode.
    fn ensure_handle(&mut self, info: &TorrentInfo) -> TorrentResult<()> {
        let info_hash = hex::encode(info.info_hash()?);
        if self.handles.contains_key(&info_hash) {
            return Ok(());
        }

        let pieces_dir = Path::new(&self.cache_dir).join("pieces");
        std::fs::create_dir_all(&pieces_dir)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;
        let torrent_save_dir = pieces_dir.join(&info_hash);
        std::fs::create_dir_all(&torrent_save_dir)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;

        let handle = self.session.add_torrent_upload_mode(info, &torrent_save_dir)?;

        let (piece_length, num_pieces) = handle.get_torrent_info()?;
        self.scheduler
            .init_torrent(&info_hash, num_pieces as i32, piece_length)?;
        self.handles.insert(info_hash, handle);
        Ok(())
    }

    /// Read a file range, driving piece download if needed.  Runs entirely on
    /// the engine thread; alerts are drained during the piece wait.
    fn read_file_range(
        &mut self,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        self.ensure_handle(info)?;
        let info_hash = hex::encode(info.info_hash()?);

        // ── Collect handle metadata (scoped borrow) ────────────────────
        let (piece_length, num_pieces, total_size, file_start_offset, file_size, status) = {
            let handle = self
                .handles
                .get(&info_hash)
                .ok_or_else(|| Self::missing())?;
            if !handle.is_valid() {
                return Err(TorrentError::InvalidFile(
                    "Torrent handle is invalid".to_string(),
                ));
            }
            let status = handle.status()?;
            let piece_info = handle.get_file_piece_info(file_index)?;
            let (piece_length, num_pieces) = handle.get_torrent_info()?;
            let file_start_offset = piece_info.file_offset as u64;
            let file_size = info
                .files()
                .ok()
                .and_then(|fs| fs.get(file_index as usize).map(|f| f.size))
                .unwrap_or(u64::MAX);
            (
                piece_length as u64,
                num_pieces as i32,
                info.total_size(),
                file_start_offset,
                file_size,
                status,
            )
        };

        if num_pieces <= 0 || piece_length == 0 {
            return Err(TorrentError::InvalidFile(format!(
                "Invalid torrent: num_pieces = {}, piece_length = {}",
                num_pieces, piece_length
            )));
        }

        let absolute_offset = file_start_offset + offset;
        let file_end = file_start_offset + file_size;
        let size = if absolute_offset < file_end {
            (std::cmp::min(size as u64, file_end - absolute_offset) as u32).max(1)
        } else {
            return Ok(Vec::new());
        };

        let start_piece = (absolute_offset / piece_length) as i32;
        let end_offset = absolute_offset + size as u64;
        let end_piece = if size > 0 {
            std::cmp::min(((end_offset - 1) / piece_length) as i32, num_pieces - 1)
        } else {
            start_piece
        };
        if start_piece >= num_pieces {
            return Err(TorrentError::InvalidFile(format!(
                "start_piece {} exceeds num_pieces {}",
                start_piece, num_pieces
            )));
        }
        if start_piece > end_piece {
            return Ok(Vec::new());
        }

        // ── Wait for initial state transitions ─────────────────────────
        let max_wait_secs = self.read_timeout_secs;
        let start = Instant::now();
        let mut status = status;
        while matches!(
            status.state,
            TorrentState::QueuedForChecking
                | TorrentState::CheckingFiles
                | TorrentState::Allocating
                | TorrentState::CheckingResumeData
        ) {
            if start.elapsed().as_secs() > max_wait_secs {
                return Err(TorrentError::Timeout(format!(
                    "Torrent stuck in state {:?} for {} seconds",
                    status.state, max_wait_secs
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
            match self.handles.get(&info_hash).map(|h| h.status()) {
                Some(Ok(s)) => status = s,
                _ => {}
            }
        }

        // ── ReaderAdded: elevate priority for this read ────────────────
        {
            let handle = self.handles.get(&info_hash).ok_or_else(|| Self::missing())?;
            if let Err(e) = self.scheduler.reader_added(
                handle,
                info,
                file_index,
                offset,
                size,
                &self.store,
            ) {
                tracing::warn!("read_file_range: reader_added failed: {:?}", e);
            }
        }
        // Publish the snapshot immediately so `.stats` reflects the elevated
        // piece priorities while this read is in progress (TSI-2224).  Without
        // this, `publish_snapshot` only runs in the engine loop between
        // commands — but this handler blocks the engine thread until the read
        // completes, by which point `release_reader` has already reset all
        // priorities to 0, so `.stats` always saw an all-`[]` Pieces grid.
        self.publish_snapshot();

        // ── Fast path: all pieces available locally ────────────────────
        if self.all_pieces_local(&info_hash, start_piece, end_piece, piece_length, num_pieces, total_size) {
            let result = self.read_from_disk(
                &info_hash,
                start_piece,
                end_piece,
                piece_length,
                num_pieces,
                total_size,
                absolute_offset,
                end_offset,
                size,
            );
            self.release_reader(&info_hash);
            return result;
        }

        // ── Switch to download mode ────────────────────────────────────
        // The handle was created in upload_mode (connect, never request). The
        // reader_added call above has already applied the piece priority
        // gradient; clearing upload_mode now lets libtorrent start requesting
        // those pieces from the peers it is already connected to.
        {
            let handle = self.handles.get(&info_hash).ok_or_else(|| Self::missing())?;
            if !handle.unset_flags(UPLOAD_MODE_FLAG) {
                tracing::warn!(
                    "read_file_range: failed to clear upload_mode for {}",
                    info_hash
                );
            }
        }

        // Settle sleep for libtorrent state transitions.
        std::thread::sleep(Duration::from_millis(100));

        // ── Slow path: peer discovery + piece-wait ─────────────────────
        // TSI-2246: when the swarm has no available seeder we fail fast with
        // `NoPeers` instead of falling through to the full `piece_wait_timeout`
        // (which would surface as a `Timeout` → EIO, a misleading "I/O error"
        // for what is really "no seeder").  We already passed the
        // all-pieces-local fast path above, so reaching here means at least one
        // piece is missing and only the network could supply it — which it
        // cannot with zero peers/seeds.
        let peer_wait_timeout =
            Duration::from_secs(std::cmp::min(self.read_timeout_secs, 9));
        {
            let handle = self.handles.get(&info_hash).ok_or_else(|| Self::missing())?;
            if status.num_peers == 0 && status.num_seeds == 0 {
                let peer_wait_start = Instant::now();
                loop {
                    if peer_wait_start.elapsed() >= peer_wait_timeout {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(500));
                    match handle.status() {
                        Ok(s) => {
                            status = s;
                            if status.num_peers > 1 || status.num_seeds > 0 {
                                break;
                            }
                        }
                        // TSI-2246 review: `handle.status()` failed — return
                        // the actual error instead of falling through to the
                        // `NoPeers` check below, which would mask the real
                        // cause behind a misleading "no seeder" message.
                        Err(e) => {
                            self.release_reader(&info_hash);
                            return Err(e);
                        }
                    }
                }
            }
            if status.num_peers == 0 && status.num_seeds == 0 {
                self.release_reader(&info_hash);
                return Err(TorrentError::NoPeers(format!(
                    "No peers or seeders connected for info_hash {} after {}s; \
                     the swarm has no available seeder. Check tracker health or \
                     try again later.",
                    info_hash,
                    peer_wait_timeout.as_secs()
                )));
            }
            let _ = handle.status();
        }

        // ── Set piece deadlines ────────────────────────────────────────
        {
            let handle = self.handles.get(&info_hash).ok_or_else(|| Self::missing())?;
            for piece_idx in start_piece..=end_piece {
                if !handle.have_piece(piece_idx) {
                    handle.set_piece_deadline(piece_idx, 0);
                }
            }
        }

        // ── Wait for each missing piece ────────────────────────────────
        let piece_wait_timeout = Duration::from_secs(self.read_timeout_secs);
        for piece_idx in start_piece..=end_piece {
            let piece_start = Instant::now();
            loop {
                if self.stopping.load(Ordering::Relaxed) {
                    self.release_reader(&info_hash);
                    return Err(TorrentError::Timeout(
                        "shutdown requested, read aborted".to_string(),
                    ));
                }

                let have = self
                    .handles
                    .get(&info_hash)
                    .map(|h| h.have_piece(piece_idx))
                    .unwrap_or(false);
                let cached = {
                    let piece_key = PieceStore::piece_key(&info_hash, piece_idx);
                    let cache = self.store.cache_manager();
                    cache
                        .lock()
                        .map(|c| {
                            PieceStore::is_piece_complete_in_cache(
                                &c,
                                &piece_key,
                                piece_idx,
                                piece_length,
                                num_pieces,
                                total_size,
                            )
                        })
                        .unwrap_or(false)
                };
                self.metrics.record_poll(have || cached);
                if have || cached {
                    if have {
                        self.register_piece(
                            &info_hash,
                            piece_idx,
                            piece_length,
                            num_pieces,
                            total_size,
                        );
                    }
                    break;
                }

                if piece_start.elapsed() >= piece_wait_timeout {
                    self.release_reader(&info_hash);
                    let progress = self
                        .handles
                        .get(&info_hash)
                        .and_then(|h| h.status().ok())
                        .map(|s| s.progress * 100.0)
                        .unwrap_or(0.0);
                    return Err(TorrentError::Timeout(format!(
                        "Timed out waiting for piece {} after {:.0}s. Torrent progress: {:.2}%",
                        piece_idx,
                        piece_wait_timeout.as_secs(),
                        progress
                    )));
                }

                // Refresh the snapshot so `.stats` shows pieces becoming
                // cached and priority changes during long reads (TSI-2224).
                self.publish_snapshot();

                std::thread::sleep(Duration::from_millis(200));
            }
        }

        let result = self.read_from_disk(
            &info_hash,
            start_piece,
            end_piece,
            piece_length,
            num_pieces,
            total_size,
            absolute_offset,
            end_offset,
            size,
        );
        self.release_reader(&info_hash);
        result
    }

    fn all_pieces_local(
        &self,
        info_hash: &str,
        start_piece: i32,
        end_piece: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) -> bool {
        let handle = match self.handles.get(info_hash) {
            Some(h) => h,
            None => return false,
        };
        for piece_idx in start_piece..=end_piece {
            let piece_key = PieceStore::piece_key(info_hash, piece_idx);
            let complete = self
                .store
                .cache_manager()
                .lock()
                .map(|c| {
                    PieceStore::is_piece_complete_in_cache(
                        &c,
                        &piece_key,
                        piece_idx,
                        piece_length,
                        num_pieces,
                        total_size,
                    )
                })
                .unwrap_or(false);
            if !handle.have_piece(piece_idx) && !complete {
                return false;
            }
        }
        true
    }

    fn read_from_disk(
        &mut self,
        info_hash: &str,
        start_piece: i32,
        end_piece: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
        absolute_offset: u64,
        end_offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        let mut result = Vec::with_capacity(size as usize);
        let mut bytes_read = 0usize;
        for piece_idx in start_piece..=end_piece {
            let piece_key = PieceStore::piece_key(info_hash, piece_idx);
            let piece_data = match self.store.read_piece(&piece_key) {
                Ok(d) => d,
                Err(_) => {
                    tracing::debug!("read_from_disk: piece {} not on disk", piece_idx);
                    continue;
                }
            };
            if piece_data.is_empty() {
                let piece_start = (piece_idx as u64) * piece_length;
                let piece_end_theoretical = piece_start + piece_length;
                if absolute_offset < piece_end_theoretical && end_offset > piece_start {
                    return Err(TorrentError::PieceNotReady(format!(
                        "Piece {} data is empty but overlaps requested range",
                        piece_idx
                    )));
                }
                continue;
            }
            // TSI-2225: the piece is being served from the local disk. If it is
            // not yet registered in the cache metadata (e.g. it was downloaded
            // eagerly by the access-window prefetch rather than through this
            // read's piece-wait loop), register it now so `pieces_on_disk` and
            // restart scans treat it as a complete, verified piece instead of
            // forcing a re-download that can time out with EIO.
            if !self.store.has_piece(info_hash, piece_idx) {
                self.register_piece(info_hash, piece_idx, piece_length, num_pieces, total_size);
            }
            if let Some(seeding) = &self.seeding {
                if let Err(e) = seeding.mark_piece_available(info_hash, piece_idx) {
                    tracing::warn!(
                        "Failed to mark piece {} available for info_hash={}: {:?}",
                        piece_idx,
                        info_hash,
                        e
                    );
                }
            }
            if let Some((local_start, local_end)) = Self::piece_chunk_bounds(
                &piece_data,
                piece_idx,
                piece_length,
                absolute_offset,
                end_offset,
            ) {
                let chunk = &piece_data[local_start..local_end];
                result.extend_from_slice(chunk);
                bytes_read += chunk.len();
                if bytes_read >= size as usize {
                    break;
                }
            }
        }
        if size > 0 && bytes_read < size as usize {
            return Err(TorrentError::PieceNotReady(format!(
                "Short read: expected {} bytes, got {} bytes",
                size, bytes_read
            )));
        }
        Ok(result)
    }

    fn register_piece(
        &mut self,
        info_hash: &str,
        piece_idx: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) {
        let expected = if piece_idx == num_pieces - 1 {
            let remainder = total_size.saturating_sub(((num_pieces - 1) as u64) * piece_length);
            if remainder > 0 {
                remainder
            } else {
                piece_length
            }
        } else {
            piece_length
        };
        if let Err(e) = self.store.register_piece(info_hash, piece_idx, expected) {
            tracing::warn!(
                "register_piece: failed for {}:piece:{}: {:?}",
                info_hash,
                piece_idx,
                e
            );
        }
        if let Some(handle) = self.handles.get(info_hash) {
            self.scheduler.piece_ready(handle, info_hash, piece_idx);
        }
    }

    fn release_reader(&mut self, info_hash: &str) {
        if let Some(handle) = self.handles.get(info_hash) {
            // Return to idle upload_mode first so no eager piece requests leak
            // out before the priority gradient is reset below.
            if !handle.set_flags(UPLOAD_MODE_FLAG) {
                tracing::warn!(
                    "release_reader: failed to re-enable upload_mode for {}",
                    info_hash
                );
            }
            if let Err(e) = self
                .scheduler
                .reader_released(handle, info_hash, &self.store)
            {
                tracing::warn!("release_reader: reader_released failed: {:?}", e);
            }
        }
    }

    fn build_pieces_status(
        &self,
        info_hash: &str,
        num_pieces: i32,
    ) -> TorrentResult<Vec<PieceStatus>> {
        let cache = self.store.cache_manager();
        let cache = cache.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Cache lock poisoned".to_string(),
        })?;
        let priorities = self.scheduler.priorities(info_hash);
        let mut result = Vec::with_capacity(num_pieces as usize);
        for p in 0..num_pieces {
            let piece_key = PieceStore::piece_key(info_hash, p);
            let is_cached = cache.has_piece(&piece_key);
            let hit_count = if is_cached {
                cache.piece_hit_count(&piece_key)
            } else {
                0
            };
            let priority = priorities
                .and_then(|v| {
                    if (p as usize) < v.len() {
                        Some(v[p as usize])
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            result.push(PieceStatus {
                priority,
                is_cached,
                hit_count,
            });
        }
        Ok(result)
    }

    /// Publish the current engine state into the shared snapshot.
    fn publish_snapshot(&mut self) {
        let mut statuses = HashMap::new();
        let mut pieces = HashMap::new();
        for (info_hash, handle) in &self.handles {
            if let Ok(status) = handle.status() {
                statuses.insert(info_hash.clone(), status);
            }
            if let Some(num_pieces) = self.scheduler.num_pieces(info_hash) {
                if let Ok(status) = self.build_pieces_status(info_hash, num_pieces) {
                    let piece_length = self.scheduler.piece_length(info_hash).unwrap_or(0);
                    pieces.insert(info_hash.clone(), (piece_length, status));
                }
            }
        }
        if let Ok(mut snap) = self.snapshot.lock() {
            *snap = DownloadSnapshot { statuses, pieces };
        }
    }

    fn missing() -> TorrentError {
        TorrentError::Unknown {
            code: -1,
            message: "Torrent handle missing".to_string(),
        }
    }

    /// Compute the byte-range within a piece that overlaps a requested read.
    fn piece_chunk_bounds(
        piece_data: &[u8],
        piece_idx: i32,
        piece_length: u64,
        absolute_offset: u64,
        end_offset: u64,
    ) -> Option<(usize, usize)> {
        let piece_start = (piece_idx as u64) * piece_length;
        let piece_end = piece_start + piece_data.len() as u64;
        let read_start = std::cmp::max(absolute_offset, piece_start);
        let read_end = std::cmp::min(end_offset, piece_end);
        if read_start < read_end {
            Some((
                (read_start - piece_start) as usize,
                (read_end - piece_start) as usize,
            ))
        } else {
            None
        }
    }
}
