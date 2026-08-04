//! Integration tests for settings readback API (TSI-2013).
//!
//! Custom-storage tests use `skip_if_ci()` because libtorrent 2.0.x
//! (CI / Ubuntu 24.04) has known heap corruption bugs in session
//! management (fixed in 2.1.x). Following `peer_download_test.rs`.

mod common;

use std::thread;
use torrentfs::{Session, TorrentInfo, TorrentfsConfig};

fn skip_if_ci() -> bool {
    if std::env::var("CI").is_ok() {
        eprintln!("Skipping custom_storage test in CI (libtorrent 2.0.x compatibility)");
        return true;
    }
    false
}

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

/// Build a config with explicit non-default bool settings so that
/// settings persistence is verifiable via `get_bool_setting` after
/// session rebuild — the test fails if `build_settings_pack` is not
/// called during `add_torrent_with_custom_storage`.
fn non_default_config() -> TorrentfsConfig {
    let mut c = TorrentfsConfig::default_config();
    c.connections.allow_multiple_connections_per_ip = Some(true);
    c.dht.enabled = Some(false);
    c
}

/// Assert that the given bool setting matches the expected value.
fn assert_setting(session: &Session, key: &str, expected: bool) {
    let actual = session
        .get_bool_setting(key)
        .unwrap_or_else(|e| panic!("get_bool_setting({key}) failed: {e:?}"));
    assert_eq!(
        actual, expected,
        "setting '{key}' expected {expected}, got {actual}"
    );
}

// ── non-custom-storage tests (always run) ──────────────────────────

#[test]
fn session_new_works() {
    let config = TorrentfsConfig::default_config();
    let _session = Session::new(&config).unwrap();
}

#[test]
fn get_bool_setting_with_explicit_config() {
    let config = non_default_config();
    let session = Session::new(&config).unwrap();

    // Explicit non-default values must be reflected in the session.
    assert_setting(&session, "allow_multiple_connections_per_ip", true);
    assert_setting(&session, "enable_dht", false);
    assert!(session.get_bool_setting("nonexistent_key").is_err());
}

// ── custom-storage tests (skip on CI) ──────────────────────────────

/// Settings persist after `add_torrent_with_custom_storage` rebuilds
/// the session. If `build_settings_pack` is not called on the
/// C++ side during rebuild, the session reverts to libtorrent
/// defaults and this test fails.
#[test]
fn settings_persist_after_custom_storage_rebuild() {
    if skip_if_ci() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    with_large_stack(move || {
        let config = non_default_config();
        let mut session = Session::new(&config).unwrap();

        // Pre-rebuild: non-default settings are active.
        assert_setting(&session, "allow_multiple_connections_per_ip", true);
        assert_setting(&session, "enable_dht", false);

        let (torrent_bytes, _) =
            common::create_test_torrent_with_tracker("http://127.0.0.1:0/announce");
        let info = TorrentInfo::from_bytes(torrent_bytes).unwrap();
        let handle = session
            .add_torrent_with_custom_storage(&info, dir.path())
            .unwrap();
        assert!(handle.is_valid());

        // Post-rebuild: settings must still hold.
        assert_setting(&session, "allow_multiple_connections_per_ip", true);
        assert_setting(&session, "enable_dht", false);
    });
}

/// Same as above but for the upload_mode variant.
#[test]
fn settings_persist_after_custom_storage_upload_mode_rebuild() {
    if skip_if_ci() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    with_large_stack(move || {
        let config = non_default_config();
        let mut session = Session::new(&config).unwrap();

        assert_setting(&session, "allow_multiple_connections_per_ip", true);
        assert_setting(&session, "enable_dht", false);

        let (torrent_bytes, _) =
            common::create_test_torrent_with_tracker("http://127.0.0.1:0/announce");
        let info = TorrentInfo::from_bytes(torrent_bytes).unwrap();
        let handle = session
            .add_torrent_with_custom_storage_upload_mode(&info, dir.path())
            .unwrap();
        assert!(handle.is_valid());

        assert_setting(&session, "allow_multiple_connections_per_ip", true);
        assert_setting(&session, "enable_dht", false);
    });
}
