//! Background alert consumer thread for libtorrent.
//!
//! Continuously pops alerts from the libtorrent session and dispatches them:
//! - `read_piece_alert`   → delivers data via pending-reads sync_channel
//! - `session_stats_alert` → logged at trace level
//! - `torrent_finished_alert` / `torrent_removed_alert` → logged
//! - other alerts          → `tracing::debug!`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;


use crate::infrastructure::download::SessionStats;

// ── Shared session stats ────────────────────────────────────

/// Shared session statistics updated by the alert consumer thread
/// and read by `.stats` generation without blocking on libtorrent FFI.
#[derive(Debug, Clone)]
pub struct SharedSessionStats {
    pub inner: Arc<Mutex<SessionStats>>,
}

impl SharedSessionStats {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SessionStats {
                download_rate: 0,
                upload_rate: 0,
                total_downloaded: 0,
                total_uploaded: 0,
                dht_nodes: 0,
                peers_connected: 0,
                half_open_connections: 0,
            })),
        }
    }

    /// Snapshot the current stats.
    pub fn snapshot(&self) -> SessionStats {
        self.inner.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

// ── Alert types matching the C FFI enum ─────────────────────

#[derive(Debug)]
#[allow(dead_code)]
enum AlertType {
    ReadPiece,
    SessionStats,
    TorrentFinished,
    TorrentRemoved,
    Other(i32),
}

impl From<i32> for AlertType {
    fn from(v: i32) -> Self {
        match v {
            1 => AlertType::ReadPiece,
            2 => AlertType::SessionStats,
            3 => AlertType::TorrentFinished,
            4 => AlertType::TorrentRemoved,
            n => AlertType::Other(n),
        }
    }
}

// ── AlertConsumer ───────────────────────────────────────────

/// Background thread that continuously pops and dispatches libtorrent alerts.
pub struct AlertConsumer {
    handle: Option<JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
}

impl AlertConsumer {
    /// Spawn a background alert consumer thread.
    ///
    /// - `session`: raw libtorrent session pointer
    /// - `pending_reads`: table of pending read requests keyed by
    ///   `"{info_hash}:piece:{idx}"` → oneshot sender
    /// - `poll_interval_ms`: sleep between pop cycles (0 = disable)
    pub fn spawn(
        session: libtorrent_sys::lt_session_t,
        stats: SharedSessionStats,
        pending_reads: Option<Arc<Mutex<HashMap<String, SyncSender<Vec<u8>>>>>>,
        poll_interval_ms: u64,
    ) -> Self {
        if poll_interval_ms == 0 {
            tracing::info!("Alert consumer disabled (poll_interval_ms=0)");
            return Self {
                handle: None,
                stop_flag: Arc::new(AtomicBool::new(false)),
            };
        }

        let interval = Duration::from_millis(poll_interval_ms);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop = stop_flag.clone();
        // Cast to usize to work around `*mut c_void: !Send`.  The pointer
        // is valid for the lifetime of the `BackgroundSession` and the
        let session_ptr = session as usize;
        let stats_clone = stats.clone();

        let handle = thread::Builder::new()
            .name("alert-consumer".into())
            .spawn(move || {
                tracing::info!(
                    "Alert consumer started (poll_interval={}ms)",
                    poll_interval_ms
                );
                let session = session_ptr as libtorrent_sys::lt_session_t;
                loop {
                    if stop.load(Ordering::Relaxed) {
                        tracing::info!("Alert consumer stopping");
                        break;
                    }

                    let list = unsafe { libtorrent_sys::lt_session_pop_alerts(session) };
                    if list.is_null() {
                        thread::sleep(interval);
                        continue;
                    }

                    let count = unsafe { (*list).count };
                    let alerts = unsafe { (*list).alerts };

                    if !alerts.is_null() && count > 0 {
                        for i in 0..count {
                            let alert = unsafe { &*alerts.add(i as usize) };
                            let alert_type = AlertType::from(alert.type_);
                            Self::dispatch(alert_type, alert, &stats_clone, &pending_reads);
                        }
                    }

                    unsafe { libtorrent_sys::lt_alert_list_destroy(list) };
                    thread::sleep(interval);
                }
            })
            .expect("Failed to spawn alert-consumer thread");

        Self {
            handle: Some(handle),
            stop_flag,
        }
    }

    /// Signal the alert consumer to stop. Does not join — call
    /// `Drop` or join the handle for that.
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
    }

    fn dispatch(
        alert_type: AlertType,
        alert: &libtorrent_sys::lt_alert_data_t,
        stats: &SharedSessionStats,
        pending_reads: &Option<Arc<Mutex<HashMap<String, SyncSender<Vec<u8>>>>>>,
    ) {
        match alert_type {
            AlertType::ReadPiece => {
                let info_hash = Self::cstr(&alert.info_hash);
                let piece_key = format!("{}:piece:{}", info_hash, alert.piece_index);
                let error_code = alert.error_code;

                // Try to deliver data to a waiting reader via the pending-reads table.
                let delivered = if let Some(ref pending) = pending_reads {
                    if let Ok(mut guard) = pending.lock() {
                        if let Some(tx) = guard.remove(&piece_key) {
                            if error_code == 0
                                && alert.piece_data_size > 0
                                && !alert.piece_data.is_null()
                            {
                                let slice = unsafe {
                                    std::slice::from_raw_parts(
                                        alert.piece_data,
                                        alert.piece_data_size,
                                    )
                                };
                                let _ = tx.send(slice.to_vec());
                            } else {
                                // Send empty vec on error — the reader
                                // will receive empty data and can retry or error.
                                let _ = tx.send(Vec::new());
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                if delivered {
                    tracing::debug!(
                        "alert: read_piece delivered (info_hash={}, piece={}, size={})",
                        info_hash,
                        alert.piece_index,
                        alert.piece_data_size
                    );
                } else if error_code != 0 {
                    let msg = Self::cstr_ptr(alert.message);
                    tracing::debug!(
                        "alert: read_piece error (info_hash={}, piece={}, error={}): {}",
                        info_hash,
                        alert.piece_index,
                        error_code,
                        msg
                    );
                } else {
                    tracing::debug!(
                        "alert: read_piece ok (info_hash={}, piece={}, size={}) — no waiter",
                        info_hash,
                        alert.piece_index,
                        alert.piece_data_size
                    );
                }
            }
            AlertType::SessionStats => {
                if let Ok(mut guard) = stats.inner.lock() {
                    guard.download_rate = alert.download_rate;
                    guard.upload_rate = alert.upload_rate;
                    guard.total_downloaded = alert.total_downloaded;
                    guard.total_uploaded = alert.total_uploaded;
                    guard.dht_nodes = alert.dht_nodes;
                    guard.peers_connected = alert.peers_connected;
                    guard.half_open_connections = alert.half_open_connections;
                }
                tracing::trace!(
                    "alert: session_stats (dl={}, ul={}, peers={}, dht={})",
                    alert.download_rate,
                    alert.upload_rate,
                    alert.peers_connected,
                    alert.dht_nodes
                );
            }
            AlertType::TorrentFinished => {
                let info_hash = Self::cstr(&alert.info_hash);
                tracing::info!("alert: torrent_finished (info_hash={})", info_hash);
            }
            AlertType::TorrentRemoved => {
                let info_hash = Self::cstr(&alert.info_hash);
                tracing::info!("alert: torrent_removed (info_hash={})", info_hash);
            }
            AlertType::Other(category) => {
                let msg = Self::cstr_ptr(alert.message);
                tracing::debug!("alert: category={}, message={}", category, msg);
            }
        }
    }

    /// Read a null-terminated fixed-size char array as a &str.
    fn cstr(buf: &[i8; 41]) -> &str {
        let bytes: &[u8] = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, 41) };
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(41);
        std::str::from_utf8(&bytes[..end]).unwrap_or("")
    }

    /// Read a nullable C string pointer, returning an owned `String`.
    fn cstr_ptr(ptr: *const std::os::raw::c_char) -> String {
        if ptr.is_null() {
            return String::new();
        }
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_str()
            .unwrap_or("")
            .to_string()
    }
}

impl Drop for AlertConsumer {
    fn drop(&mut self) {
        self.stop();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
