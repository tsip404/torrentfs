//! libtorrent alert dispatching for the download engine.
//!
//! The download engine actor thread owns the libtorrent session and drains its
//! alert queue inline (no background thread), so the raw session pointer never
//! crosses a thread boundary.  Alert processing is:
//! - `session_stats_alert` → updates the shared session-stats snapshot
//! - `torrent_finished_alert` / `torrent_removed_alert` → logged
//! - `read_piece_alert` and others → logged (piece data is read from disk)
//!
//! The old `set_alert_notify` callback + `session as usize` cast were removed:
//! the engine polls alerts on each loop tick instead of blocking on a condvar.

use std::sync::{Arc, Mutex};

use crate::infrastructure::download::SessionStats;
use crate::infrastructure::metrics::Metrics;

// ── Shared session stats ────────────────────────────────────

/// Shared session statistics updated by the engine's alert drain and read by
/// `.stats` generation without blocking on libtorrent FFI.
#[derive(Debug, Clone)]
pub struct SharedSessionStats {
    pub inner: Arc<Mutex<SessionStats>>,
}

impl SharedSessionStats {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SessionStats::default())),
        }
    }

    pub fn snapshot(&self) -> SessionStats {
        self.inner
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }
}

impl Default for SharedSessionStats {
    fn default() -> Self {
        Self::new()
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
    fn from(value: i32) -> Self {
        match value {
            0 => AlertType::ReadPiece,
            1 => AlertType::SessionStats,
            2 => AlertType::TorrentFinished,
            3 => AlertType::TorrentRemoved,
            other => AlertType::Other(other),
        }
    }
}

/// Drain all pending libtorrent alerts and dispatch them.
///
/// `session` is the raw session pointer, valid for the session's lifetime.
/// This function must be called from the thread that owns the session (the
/// engine actor thread).
pub fn drain_alerts(session: libtorrent_sys::lt_session_t, stats: &SharedSessionStats, metrics: &Metrics) {
    let list = unsafe { libtorrent_sys::lt_session_pop_alerts(session) };
    if list.is_null() {
        return;
    }

    let count = unsafe { (*list).count };
    let alerts = unsafe { (*list).alerts };

    if !alerts.is_null() && count > 0 {
        for i in 0..count {
            let alert = unsafe { &*alerts.add(i as usize) };
            let alert_type = AlertType::from(alert.type_);
            dispatch(alert_type, alert, stats, metrics);
        }
    }

    unsafe { libtorrent_sys::lt_alert_list_destroy(list) };
}

fn dispatch(
    alert_type: AlertType,
    alert: &libtorrent_sys::lt_alert_data_t,
    stats: &SharedSessionStats,
    _metrics: &Metrics,
) {
    match alert_type {
        AlertType::ReadPiece => {
            let info_hash = cstr(&alert.info_hash);
            tracing::debug!(
                "alert: read_piece (info_hash={}, piece={}, size={}, error={})",
                info_hash,
                alert.piece_index,
                alert.piece_data_size,
                alert.error_code
            );
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
            let info_hash = cstr(&alert.info_hash);
            tracing::info!("alert: torrent_finished (info_hash={})", info_hash);
        }
        AlertType::TorrentRemoved => {
            let info_hash = cstr(&alert.info_hash);
            tracing::info!("alert: torrent_removed (info_hash={})", info_hash);
        }
        AlertType::Other(category) => {
            let msg = cstr_ptr(alert.message);
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
