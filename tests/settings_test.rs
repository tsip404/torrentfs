//! Integration tests for settings readback API (TSI-2013).
//!
//! Custom-storage tests validate that settings are correctly applied
//! when creating a session with PieceStorageDiskIO from the start
//! (new_with_custom_storage). CI runs on Debian Sid (libtorrent 2.1.x).

mod common;

use std::thread;
use torrentfs::{Session, TorrentfsConfig};

fn with_large_stack<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap()
}

fn non_default_config() -> TorrentfsConfig {
    let mut c = TorrentfsConfig::default_config();
    c.connections.allow_multiple_connections_per_ip = Some(true);
    c.dht.enabled = Some(false);
    c
}

fn assert_setting(session: &Session, key: &str, expected: bool) {
    let actual = session
        .get_bool_setting(key)
        .unwrap_or_else(|e| panic!("get_bool_setting({key}) failed: {e:?}"));
    assert_eq!(
        actual, expected,
        "setting '{key}' expected {expected}, got {actual}"
    );
}

#[test]
fn session_new_works() {
    let config = TorrentfsConfig::default_config();
    let _session = Session::new(&config).unwrap();
}

#[test]
fn get_bool_setting_with_explicit_config() {
    let config = non_default_config();
    let session = Session::new(&config).unwrap();
    assert_setting(&session, "allow_multiple_connections_per_ip", true);
    assert_setting(&session, "enable_dht", false);
    assert!(session.get_bool_setting("nonexistent_key").is_err());
}

#[test]
fn settings_work_with_custom_storage_session() {
    let dir = tempfile::TempDir::new().unwrap();
    with_large_stack(move || {
        let config = non_default_config();
        let session = Session::new_with_custom_storage(&config, dir.path()).unwrap();
        assert_setting(&session, "allow_multiple_connections_per_ip", true);
        assert_setting(&session, "enable_dht", false);
    });
}
