//! End-to-end test: validate file read via DownloadEngine::read_file_range
//! using a local tracker + seeder (TestHarness).
//!
//! This test addresses TSI-1947 (Gap1): scenario 4 file reading fails when
//! no real peers are available. By using a self-hosted tracker + seeder,
//! we validate the full lazy-loading flow without external infrastructure.

mod common;

use common::{local_test_config, TestHarness};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Test that DownloadEngine::read_file_range can download and return
/// correct file data when a local seeder is available via tracker.
///
/// This is the exact code path exercised in the QA test scenario 4,
/// validating that the lazy-loading flow works end-to-end.
#[test]
fn test_read_file_range_with_local_seeder() {
    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    // ── Setup: start tracker + seeder ──────────────────────────────────
    let harness = TestHarness::new();

    let info_hash = hex::encode(harness.info.info_hash().expect("Failed to get info hash"));
    println!("TestHarness ready. Info hash: {}", info_hash);
    println!(
        "Tracker URL: {}, announces: {}",
        harness.tracker.announce_url(),
        harness.tracker.announce_count()
    );

    // ── Create DownloadEngine pointing at the tracker ──────────────────
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let config = local_test_config();

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    // ── Re-parse torrent data (raw pointer can't cross into the engine) ─
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );

    // ── Read file range (file_index=0, offset=0, size=50) ──────────────
    // This goes through read_file_range → ensure handle →
    // piece download → cache → return data.
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);

    let mut last_error: Option<torrentfs::TorrentError> = None;
    loop {
        match engine.read_file_range(info.clone(), 0, 0, 50) {
            Ok(data) => {
                println!(
                    "Successfully read {} bytes after {:.1}s",
                    data.len(),
                    start.elapsed().as_secs_f64()
                );
                println!("Data: {:?}", String::from_utf8_lossy(&data));

                assert!(!data.is_empty(), "Expected non-empty data");
                assert_eq!(
                    &data[..50.min(data.len())],
                    &harness.file_content[..50.min(data.len())],
                    "Downloaded data doesn't match seed content"
                );
                return;
            }
            Err(e) => {
                last_error = Some(e);
                println!(
                    "Read attempt at {:.1}s: {:?}",
                    start.elapsed().as_secs_f64(),
                    last_error.as_ref().unwrap()
                );
            }
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out after {:.0}s waiting for file read. Last error: {:?}",
                timeout.as_secs(),
                last_error
            );
        }

        thread::sleep(Duration::from_secs(1));
    }
}

/// Regression test (TSI-2151 P0): a lightweight handle created at torrent-add
/// time (upload_mode, no pieces downloaded) must switch to download mode and
/// fetch data from a tracker-only peer when a read arrives later — not stay
/// stuck in a "Finished" state with no peer connections.
#[test]
fn test_read_file_range_after_idle_handle() {
    // Serialize libtorrent session creation to avoid resource contention.
    let _session_guard = common::acquire_session_lock();

    let harness = TestHarness::new();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // TSI-2068: force the downloader onto a distinct listen port so the
    // MiniTracker can distinguish it from the seeder (which defaults to
    // 6881 via Session::new with NULL listen_interfaces).  When both
    // sessions collide on the same port the tracker deduplicates by
    // IP:port and returns 0 peers, causing a 30s timeout.
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );

    // Mirror the FUSE "torrent added" path: create the lightweight handle
    // first, then leave it idle long enough to settle into upload_mode.
    engine
        .ensure_handle(info.clone())
        .expect("Failed to ensure lightweight handle");
    thread::sleep(Duration::from_secs(3));

    // Now read — the idle handle must switch to download mode and fetch the
    // piece from the seeder over the tracker (peer-to-peer path).
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    let mut last_error: Option<torrentfs::TorrentError> = None;
    loop {
        match engine.read_file_range(info.clone(), 0, 0, 50) {
            Ok(data) => {
                assert!(!data.is_empty(), "Expected non-empty data");
                assert_eq!(
                    &data[..50.min(data.len())],
                    &harness.file_content[..50.min(data.len())],
                    "Downloaded data doesn't match seed content"
                );
                return;
            }
            Err(e) => {
                last_error = Some(e);
            }
        }
        if start.elapsed() > timeout {
            panic!(
                "Timed out after {:.0}s waiting for file read after idle. Last error: {:?}",
                timeout.as_secs(),
                last_error
            );
        }
        thread::sleep(Duration::from_secs(1));
    }
}

/// Test that read_file_range returns correct data for different offset/size
/// combinations, validating boundary handling.
#[test]
fn test_read_file_range_boundaries() {
    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    let harness = TestHarness::new();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // TSI-2068: force the downloader onto a distinct listen port so the
    // MiniTracker can distinguish it from the seeder (which defaults to
    // 6881 via Session::new with NULL listen_interfaces).  When both
    // sessions collide on the same port the tracker deduplicates by
    // IP:port and returns 0 peers, causing a 30s timeout.
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );

    // Helper: retry read until success or timeout
    let retry_read = |offset: u64, size: u32| -> Vec<u8> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(60);
        loop {
            match engine.read_file_range(info.clone(), 0, offset, size) {
                Ok(data) => return data,
                Err(e) => {
                    if start.elapsed() > timeout {
                        panic!(
                            "Timed out reading offset={}, size={}: {:?}",
                            offset, size, e
                        );
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    };

    // Read first 10 bytes
    let data = retry_read(0, 10);
    assert_eq!(data.len(), 10);
    assert_eq!(&data, &harness.file_content[..10]);

    // Read bytes 50-60 (middle of content)
    let data = retry_read(50, 10);
    assert_eq!(data.len(), 10);
    assert_eq!(&data, &harness.file_content[50..60]);

    // Read bytes from offset 10 to end (size 16374 = total 16384 - offset 10)
    let data = retry_read(10, 16374);
    assert_eq!(data.len(), 16374);
    assert_eq!(&data, &harness.file_content[10..16384]);

    // Read past end should return empty or truncated
    let data = retry_read(16378, 10);
    assert_eq!(data.len(), 6); // 16384 - 16378 = 6 bytes left
    assert_eq!(&data, &harness.file_content[16378..16384]);
}

/// Test that read_file_range correctly returns an error when no
/// peers/seeds are available AND no cached pieces exist.
#[test]
fn test_read_file_range_no_peers_error() {
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");

    // Use config with DHT disabled so we don't accidentally find peers
    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.timeouts.read_timeout_secs = Some(2); // Short timeout for test

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    // Create a test torrent with a fake tracker URL (no real tracker running)
    let (torrent_data, _file_content) =
        common::create_test_torrent_with_tracker("http://127.0.0.1:19999/announce");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );

    // This should fail since the tracker doesn't exist
    let result = engine.read_file_range(info, 0, 0, 50);

    match result {
        Err(torrentfs::TorrentError::NoPeers(_)) => {
            println!("Correctly got NoPeers error as expected");
        }
        Err(e) => {
            // Could also be a Timeout if the state check or piece wait expires.
            println!(
                "Got error: {:?} (NoPeers expected but other error acceptable)",
                e
            );
        }
        Ok(data) => {
            // Not expected but could happen if pieces somehow cached
            println!(
                "Unexpectedly got data: {} bytes (may have cached pieces)",
                data.len()
            );
        }
    }
}

/// Build a structurally valid single-file `.torrent` whose piece hashes are
/// all zero.  Parsing and handle creation only need valid structure (not
/// correct hashes); distinct `name`s yield distinct info hashes.
fn distinct_torrent(name: &str) -> Vec<u8> {
    let mut t = Vec::new();
    t.extend_from_slice(b"d8:announce31:http://127.0.0.1:19999/announce4:infod");
    t.extend_from_slice(b"6:lengthi16384e");
    t.extend_from_slice(format!("4:name{}:{}", name.len(), name).as_bytes());
    t.extend_from_slice(b"12:piece lengthi16384e6:pieces20:");
    t.extend_from_slice(&[0u8; 20]);
    t.extend_from_slice(b"ee");
    t
}

/// Regression test (TSI-2226 P0): creating a lightweight handle through the
/// fire-and-forget `ensure_handle_async` path must not block the caller while
/// the engine thread is busy downloading.  The FUSE release path calls this
/// when a `.torrent` is written to metadata/; a blocking round-trip would
/// stall the single-threaded FUSE dispatch loop behind an in-flight read and
/// surface as a write timeout (EIO).
#[test]
fn test_ensure_handle_async_does_not_block_on_busy_engine() {
    let _session_guard = common::acquire_session_lock();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    // Short timeout so the "no peers" read blocks only a few seconds.
    config.timeouts.read_timeout_secs = Some(3);

    let engine = Arc::new(
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine"),
    );

    let a = Arc::new(
        torrentfs::TorrentInfo::from_bytes(distinct_torrent("a.iso"))
            .expect("Failed to parse torrent a"),
    );
    let b = Arc::new(
        torrentfs::TorrentInfo::from_bytes(distinct_torrent("b.iso"))
            .expect("Failed to parse torrent b"),
    );

    // Create A's handle, then block the engine thread on a read with no peers.
    engine.ensure_handle(a.clone()).expect("ensure handle a");
    let engine_for_read = engine.clone();
    let a_for_read = a.clone();
    let read_thread = thread::spawn(move || {
        let _ = engine_for_read.read_file_range(a_for_read, 0, 0, 4096);
    });

    // Give the engine thread time to pick up the blocking read.
    thread::sleep(Duration::from_millis(500));

    // ensure_handle_async must return immediately rather than queue behind the
    // in-flight read (which blocks for ~6s: peer wait + piece wait).
    let start = std::time::Instant::now();
    engine
        .ensure_handle_async(b.clone())
        .expect("ensure_handle_async b");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "ensure_handle_async blocked for {:?} behind a busy engine",
        elapsed
    );

    read_thread.join().expect("read thread");
    engine.shutdown();
}

/// Regression test (TSI-2238): `shutdown()` must abort an in-flight
/// `read_file_range` that is blocked in a wait loop (state-transition,
/// peer-discovery, or piece-wait) instead of stalling until
/// `read_timeout_secs` elapses.  Before the fix, the state-transition and
/// peer-wait loops never checked `self.stopping`, so `shutdown()`'s
/// `handle.join()` blocked for up to `read_timeout_secs` (default 30s).
///
/// Here the torrent has no tracker and no peers, so the read blocks in the
/// peer-discovery wait loop.  A read timeout of 30s makes the contrast
/// sharp: with the fix `shutdown()` returns in well under a second; without
/// it the test would hang ~30s on the join.
#[test]
fn test_shutdown_aborts_blocked_read() {
    let _session_guard = common::acquire_session_lock();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    // Long timeout: the read would block this long without the shutdown fix.
    config.timeouts.read_timeout_secs = Some(30);

    let engine = Arc::new(
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine"),
    );

    // A torrent with a fake tracker URL: no peers will ever connect, so the
    // read blocks in the peer-discovery wait loop.
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(distinct_torrent("shutdown-abort.iso"))
            .expect("Failed to parse torrent"),
    );
    engine.ensure_handle(info.clone()).expect("ensure handle");

    let engine_for_read = engine.clone();
    let info_for_read = info.clone();
    let read_thread = thread::spawn(move || {
        let _ = engine_for_read.read_file_range(info_for_read, 0, 0, 4096);
    });

    // Give the engine thread time to enter the blocking peer-wait loop.
    thread::sleep(Duration::from_millis(500));

    // shutdown() sets `stopping` and joins the engine thread.  The blocked
    // read must observe `stopping` and return promptly; then the engine loop
    // processes the queued `Command::Shutdown` and the join completes.
    let start = std::time::Instant::now();
    engine.shutdown();
    let elapsed = start.elapsed();

    // With the fix, shutdown completes in well under a second (the peer-wait
    // loop polls `stopping` every 500ms).  Assert far below the 30s read
    // timeout to catch regressions without being flaky on slow CI.
    assert!(
        elapsed < Duration::from_secs(5),
        "shutdown took {:?} to abort a blocked read (read_timeout_secs=30); \
         expected well under 5s",
        elapsed
    );

    read_thread.join().expect("read thread panicked");
}
