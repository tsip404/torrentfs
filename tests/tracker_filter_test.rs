//! TSI-2171: libtorrent 2.1.x crashes (SIGSEGV in the DNS resolver) when
//! announcing to a hostname tracker.  The wrapper must drop hostname trackers
//! and keep only IP-literal ones, so a lazy file read can never trigger a
//! hostname DNS lookup.  These tests assert the filtering at the add_torrent /
//! add_torrent_upload_mode boundary via `TorrentHandle::trackers()`.

mod common;

use common::{acquire_session_lock, create_test_torrent_with_tracker, local_test_config};

/// A hostname tracker must be dropped when the torrent is added, leaving an
/// empty tracker list (peer discovery falls back to DHT/LSD/PEX).
#[test]
fn test_hostname_tracker_filtered_on_add() {
    let _guard = acquire_session_lock();

    let (torrent_bytes, _content) =
        create_test_torrent_with_tracker("http://torrentfs-filter-test.invalid:6969/announce");
    let info = torrentfs::TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let config = local_test_config();
    let mut session = torrentfs::Session::new(&config).expect("create session");

    let handle = session
        .add_torrent_upload_mode(&info, cache_dir.path())
        .expect("add torrent");

    let trackers = handle.trackers().expect("query trackers");
    assert!(
        trackers.is_empty(),
        "hostname tracker should be filtered, got: {:?}",
        trackers
    );
}

/// An IP-literal tracker must be preserved, so local IP-based trackers (the
/// peer-to-peer read path used by the integration tests) keep working.
#[test]
fn test_ip_literal_tracker_kept_on_add() {
    let _guard = acquire_session_lock();

    let (torrent_bytes, _content) =
        create_test_torrent_with_tracker("http://127.0.0.1:42424/announce");
    let info = torrentfs::TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let config = local_test_config();
    let mut session = torrentfs::Session::new(&config).expect("create session");

    let handle = session
        .add_torrent_upload_mode(&info, cache_dir.path())
        .expect("add torrent");

    let trackers = handle.trackers().expect("query trackers");
    assert_eq!(
        trackers,
        vec!["http://127.0.0.1:42424/announce".to_string()],
        "IP-literal tracker should be preserved"
    );
}
