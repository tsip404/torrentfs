//! Background alert consumer thread for libtorrent.
//!
//! Continuously pops alerts from the libtorrent session and dispatches them:
//! - `read_piece_alert`   → delivers data via pending-reads sync_channel
//! - `session_stats_alert` → logged at trace level
//! - `torrent_finished_alert` / `torrent_removed_alert` → logged
//! - other alerts          → `tracing::debug!`

use std::collections::HashMap;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Condvar, Mutex};
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

// ── Alert notify (wakeup) machinery ─────────────────────────

/// Safety-net timeout for the condvar wait. The notify callback is the
/// primary wakeup; this timeout only guards against a missed wakeup in the
/// narrow window between an empty `pop_alerts` and entering the wait. It is
/// deliberately generous (not a poll interval) so an idle consumer sleeps.
const SAFETY_NET_TIMEOUT: Duration = Duration::from_secs(1);

/// Shared state signaled by the C alert-notify callback and waited on by the
/// consumer thread. `flag` closes the missed-wakeup race: the callback sets
/// it before `notify_one`, and the consumer clears it under the mutex before
/// deciding whether to block.
struct NotifyState {
    flag: AtomicBool,
    cv: Condvar,
    mutex: Mutex<()>,
}

impl NotifyState {
    /// Block until a notify arrives or `timeout` elapses.
    ///
    /// Returns `true` if a notify was observed (the flag was set by the
    /// callback, either before this call or via `notify_one` while blocked),
    /// `false` if the wait timed out.
    fn wait(&self, timeout: Duration) -> bool {
        let guard = self
            .mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.flag.swap(false, Ordering::SeqCst) {
            // A callback already fired before we blocked — don't block.
            return true;
        }
        let (_guard, timed_out) = self
            .cv
            .wait_timeout(guard, timeout)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.flag.store(false, Ordering::SeqCst);
        !timed_out.timed_out()
    }
}

/// libtorrent alert-notify callback. Invoked on libtorrent's internal
/// thread(s) whenever the alert queue goes 0→1. Must be non-blocking and must
/// not touch the session — it only flags and notifies the consumer.
unsafe extern "C" fn notify_cb(user_data: *mut c_void) {
    // SAFETY: `user_data` is `Arc::as_ptr(&notify)` and remains valid for as
    // long as the session is alive (the callback is unregistered before the
    // shared state is freed; see `AlertConsumer::stop`).
    let state = &*(user_data as *const NotifyState);
    state.flag.store(true, Ordering::SeqCst);
    state.cv.notify_one();
}

// ── AlertConsumer ───────────────────────────────────────────

/// Background thread that continuously pops and dispatches libtorrent alerts.
/// Woken by the libtorrent `set_alert_notify` callback instead of a fixed
/// poll interval.
pub struct AlertConsumer {
    handle: Option<JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
    /// Raw session pointer — kept only to unregister the notify callback in
    /// [`AlertConsumer::stop`], which is always called while the session is
    /// still alive.
    session: libtorrent_sys::lt_session_t,
    notify: Arc<NotifyState>,
}

impl AlertConsumer {
    /// Spawn a background alert consumer thread.
    ///
    /// - `session`: raw libtorrent session pointer (valid for the session's
    ///   lifetime; the notify callback is registered on it and unregistered
    ///   in [`AlertConsumer::stop`]).
    /// - `stats`: shared session-stats snapshot updated on `session_stats_alert`.
    /// - `pending_reads`: table of pending read requests keyed by
    ///   `"{info_hash}:piece:{idx}"` → oneshot sender.
    pub fn spawn(
        session: libtorrent_sys::lt_session_t,
        stats: SharedSessionStats,
        pending_reads: Option<Arc<Mutex<HashMap<String, SyncSender<Vec<u8>>>>>>,
    ) -> Self {
        let notify = Arc::new(NotifyState {
            flag: AtomicBool::new(false),
            cv: Condvar::new(),
            mutex: Mutex::new(()),
        });

        // Register the non-blocking notify hook. It fires on libtorrent's
        // thread(s) whenever the alert queue goes 0→1 and only signals this
        // consumer — all alert draining stays on the consumer thread.
        unsafe {
            libtorrent_sys::lt_session_set_alert_notify(
                session,
                Some(notify_cb),
                Arc::as_ptr(&notify) as *mut c_void,
            );
        }

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop = stop_flag.clone();
        // Cast to usize to work around `*mut c_void: !Send`. The pointer is
        // valid for the session's lifetime, and `stop()` (called before the
        // session is dropped) unregisters the notify hook.
        let session_ptr = session as usize;
        let stats_clone = stats.clone();
        let notify_thread = notify.clone();

        let handle = thread::Builder::new()
            .name("alert-consumer".into())
            .spawn(move || {
                tracing::info!("Alert consumer started (event-driven via set_alert_notify)");
                let session = session_ptr as libtorrent_sys::lt_session_t;
                loop {
                    if stop.load(Ordering::Relaxed) {
                        tracing::info!("Alert consumer stopping");
                        break;
                    }

                    let list = unsafe { libtorrent_sys::lt_session_pop_alerts(session) };
                    if list.is_null() {
                        // Queue empty — block until the notify callback fires,
                        // or the safety-net timeout elapses.
                        let _ = notify_thread.wait(SAFETY_NET_TIMEOUT);
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
                }
            })
            .expect("Failed to spawn alert-consumer thread");

        Self {
            handle: Some(handle),
            stop_flag,
            session,
            notify,
        }
    }

    /// Signal the alert consumer to stop and unregister the notify callback.
    /// Must be called while the session is still alive. Does not join — call
    /// `Drop` (or join the handle) for that.
    pub fn stop(&self) {
        // Unregister the hook first so libtorrent stops invoking the callback
        // before the shared `notify` state is freed.
        unsafe {
            libtorrent_sys::lt_session_set_alert_notify(self.session, None, std::ptr::null_mut());
        }
        self.stop_flag.store(true, Ordering::Relaxed);
        self.notify.cv.notify_all();
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
    /// Generic over T to support both `[i8; 41]` (amd64) and `[u8; 41]` (arm64)
    /// due to platform-specific C `char` signedness.
    fn cstr<T>(buf: &[T; 41]) -> &str {
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
        // Do not unregister the notify hook here: the session may already be
        // destroyed by the time this runs — `stop()` unregisters it while the
        // session is still alive (the caller must invoke `stop()` first). Just
        // wake the consumer thread and join it.
        self.stop_flag.store(true, Ordering::Relaxed);
        self.notify.cv.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn new_state() -> Arc<NotifyState> {
        Arc::new(NotifyState {
            flag: AtomicBool::new(false),
            cv: Condvar::new(),
            mutex: Mutex::new(()),
        })
    }

    #[test]
    fn notify_cb_sets_flag() {
        let state = new_state();
        unsafe { notify_cb(Arc::as_ptr(&state) as *mut c_void) };
        assert!(state.flag.load(Ordering::SeqCst));
    }

    #[test]
    fn wait_returns_immediately_when_already_flagged() {
        // Simulate a callback firing before the consumer blocks (the
        // missed-wakeup race). The flag must make `wait` return at once
        // instead of blocking for the full timeout.
        let state = new_state();
        state.flag.store(true, Ordering::SeqCst);
        let start = Instant::now();
        assert!(state.wait(Duration::from_secs(60)));
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "wait should return immediately when the flag is already set"
        );
    }
}
