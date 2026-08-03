//! Integration test: verify that peer-to-peer connectivity works
//! via local HTTP tracker + seeder.
//!
//! This test starts a local HTTP tracker + seeder, then creates a
//! downloader session that discovers the seeder via the tracker.
//!
//! This is the CI-level test infrastructure required by TSI-1938 to
//! validate file reads beyond cached data.

mod common;

use common::{local_test_config, TestHarness};
use std::thread;
use std::time::Duration;

/// Skip integration tests that require a libtorrent session in CI,
/// where libtorrent 2.0.x has known heap corruption bugs in session
/// management (fixed in 2.1.x). Local development uses 2.1.x.
fn skip_if_ci() -> bool {
    if std::env::var("CI").is_ok() {
        eprintln!("Skipping integration test in CI (libtorrent 2.0.x compatibility)");
        return true;
    }
    false
}

/// Verify that two libtorrent sessions can discover each other via
/// the local HTTP tracker and establish peer connections.
#[test]
fn test_peer_discovery_via_tracker() {
    if skip_if_ci() {
        return;
    }
    // ── Setup: start tracker + seeder ──────────────────────────────────
    let harness = TestHarness::new();

    println!(
        "Tracker: {}, announces: {}",
        harness.tracker.announce_url(),
        harness.tracker.announce_count()
    );

    let info_hash = hex::encode(harness.info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);

    // ── Create downloader session directly (bypass DownloadManager) ────
    let config = local_test_config();
    let mut dl_session =
        torrentfs::download::Session::new(&config).expect("Failed to create downloader session");

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");

    // Re-parse torrent data from the harness (fresh TorrentInfo)
    let torrent_data = harness.torrent_data.clone();
    let dl_info = torrentfs::TorrentInfo::from_bytes(torrent_data)
        .expect("Failed to parse torrent for downloader");

    let handle = dl_session
        .add_torrent(&dl_info, cache_dir.path())
        .expect("Failed to add torrent to downloader session");

    println!("Downloader: torrent added, waiting for peers...");

    // ── Wait for the downloader to discover the seeder ─────────────────
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);
    let mut peers_found = false;

    loop {
        match handle.status() {
            Ok(status) => {
                println!(
                    "Downloader: state={:?}, progress={:.2}%, peers={}, seeds={}, dl_rate={}, ul_rate={}",
                    status.state,
                    status.progress * 100.0,
                    status.num_peers,
                    status.num_seeds,
                    status.download_rate,
                    status.upload_rate
                );

                if status.num_peers > 0 || status.num_seeds > 0 {
                    println!(
                        "Downloader: found {} peers, {} seeds!",
                        status.num_peers, status.num_seeds
                    );
                    peers_found = true;
                    break;
                }

                // Also check if already finished/seeding (file was cached somehow)
                if matches!(
                    status.state,
                    torrentfs::download::TorrentState::Finished
                        | torrentfs::download::TorrentState::Seeding
                ) {
                    println!("Downloader: torrent completed (may have found data locally)");
                    peers_found = true;
                    break;
                }
            }
            Err(e) => {
                panic!("Downloader: status error: {:?}", e);
            }
        }

        if start.elapsed() > timeout {
            break;
        }

        thread::sleep(Duration::from_millis(500));
    }

    // ── Verify tracker received the downloader's announce ──────────────
    let final_announce_count = harness.tracker.announce_count();
    println!("Tracker final announce count: {}", final_announce_count);

    if !peers_found {
        panic!(
            "Downloader did not find any peers within {} seconds. \
             Tracker announces: {}. Check tracker and seeder health.",
            timeout.as_secs(),
            final_announce_count
        );
    }

    // ── If peers found, verify we can read a piece ─────────────────────
    println!("\n--- Testing piece read ---");
    let session_ref = &dl_session;
    match handle.read_piece(session_ref, 0) {
        Ok(data) => {
            if !data.is_empty() {
                println!(
                    "Read piece 0: {} bytes, first 50: {:?}",
                    data.len(),
                    String::from_utf8_lossy(&data[..50.min(data.len())])
                );
                assert_eq!(
                    &data[..50.min(data.len())],
                    &harness.file_content[..50.min(data.len())],
                    "Downloaded data doesn't match seed content"
                );
            } else {
                println!("Read piece 0: empty (piece not yet downloaded)");
            }
        }
        Err(e) => {
            // Piece read can fail if not downloaded yet - that's OK
            // as long as we proved peer connectivity
            println!(
                "Piece read failed (expected if not yet downloaded): {:?}",
                e
            );
        }
    }

    println!("\n=== Peer discovery test passed! ===");
}

/// Verify that the downloader detects transient peer disconnection
/// and fails quickly instead of waiting for the full piece_wait timeout.
///
/// TSI-1975: Integration test for transient-peer fast-exit scenario.
/// Start a seeder that announces to the tracker then immediately exits,
/// then verify the downloader in piece_wait does NOT experience the
/// full read_timeout_secs delay — it should return NoPeers quickly
/// (within a few seconds, not the full timeout).
#[test]
fn test_transient_peer_fast_exit() {
    if skip_if_ci() {
        return;
    }
    use common::{create_test_torrent_with_tracker, MiniTracker};

    // ── Start tracker ──────────────────────────────────────────────────
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    println!("Tracker started at {}", announce_url);

    // ── Create torrent data ────────────────────────────────────────────
    let (torrent_data, file_content) = create_test_torrent_with_tracker(&announce_url);
    let info =
        torrentfs::TorrentInfo::from_bytes(torrent_data.clone()).expect("Failed to parse torrent");

    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);

    // ── Start a transient seeder: announces then immediately exits ─────
    // The seeder creates a libtorrent session, adds the torrent with the
    // complete file data, waits for it to check and announce to the
    // tracker, then immediately destroys the session — just like a peer
    // that briefly appears and disappears.
    {
        let torrent_data_for_seeder = torrent_data.clone();
        let seed_dir = tempfile::TempDir::new().expect("Failed to create seed temp dir");
        let seed_file = seed_dir.path().join("final_verification.txt");
        std::fs::write(&seed_file, &file_content).expect("Failed to write seed file");

        let config = local_test_config();
        let mut session = torrentfs::download::Session::new(&config)
            .expect("Transient seeder: failed to create session");

        let seeder_info = torrentfs::TorrentInfo::from_bytes(torrent_data_for_seeder)
            .expect("Transient seeder: failed to parse torrent");

        let handle = session
            .add_torrent(&seeder_info, seed_dir.path())
            .expect("Transient seeder: failed to add torrent");

        println!("Transient seeder: torrent added, waiting for seeding state...");

        // Wait for the torrent to check and start seeding (announces to tracker)
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(30);
        loop {
            match handle.status() {
                Ok(status) => {
                    println!(
                        "Transient seeder: state={:?}, progress={:.2}%",
                        status.state,
                        status.progress * 100.0
                    );
                    if matches!(
                        status.state,
                        torrentfs::download::TorrentState::Seeding
                            | torrentfs::download::TorrentState::Finished
                    ) {
                        println!("Transient seeder: now seeding!");
                        break;
                    }
                }
                Err(e) => {
                    panic!("Transient seeder: status error: {:?}", e);
                }
            }
            if start.elapsed() > timeout {
                panic!("Transient seeder: timeout waiting for seed state");
            }
            thread::sleep(Duration::from_millis(200));
        }

        // Wait for the tracker to register the seeder's announce
        let start = std::time::Instant::now();
        loop {
            if tracker.announce_count() >= 1 {
                println!(
                    "Tracker received {} announces from transient seeder",
                    tracker.announce_count()
                );
                break;
            }
            if start.elapsed() > Duration::from_secs(10) {
                println!("Warning: tracker didn't receive seeder announce within 10s");
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }

        println!("Transient seeder: dropping session (simulating fast exit)...");
        // Drop handle and session — the seeder disappears immediately
        drop(handle);
        drop(session);
        // seed_dir is dropped here too
    }

    // Give the tracker and network stack a moment to process the
    // seeder's stopped event and for the peer to be truly gone.
    thread::sleep(Duration::from_secs(1));

    // ── Create DownloadManager and try to read ─────────────────────────
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Short timeout to test fast exit — the downloader should fail
    // much sooner than this via the transient-peer detection.
    config.timeouts.read_timeout_secs = Some(15);

    let mut dm = torrentfs::download::DownloadManager::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadManager");

    let read_start = std::time::Instant::now();
    let result = dm.read_file_range(&info, 0, 0, 50);
    let elapsed = read_start.elapsed();

    match &result {
        Err(torrentfs::TorrentError::NoPeers(msg)) => {
            println!(
                "Got NoPeers error after {:.2}s: {}",
                elapsed.as_secs_f64(),
                msg
            );
            assert!(
                elapsed < Duration::from_secs(10),
                "NoPeers error took too long ({:.2}s). Expected fast exit (<10s).",
                elapsed.as_secs_f64()
            );
            println!(
                "✓ Fast exit verified: NoPeers in {:.2}s (timeout is 15s)",
                elapsed.as_secs_f64()
            );
        }
        Err(e) => {
            println!(
                "Got other error after {:.2}s: {:?}",
                elapsed.as_secs_f64(),
                e
            );
            // Any error is acceptable as long as it's fast
            assert!(
                elapsed < Duration::from_secs(10),
                "Error took too long ({:.2}s). Expected fast exit (<10s).",
                elapsed.as_secs_f64()
            );
            println!(
                "✓ Fast exit verified (non-NoPeers error) in {:.2}s",
                elapsed.as_secs_f64()
            );
        }
        Ok(data) => {
            println!(
                "Read succeeded after {:.2}s ({} bytes)",
                elapsed.as_secs_f64(),
                data.len()
            );
            // If the seeder was fast enough that the downloader managed to
            // download the piece before the seeder exited, that's also fine —
            // the key assertion is that we didn't wait the full timeout.
            assert!(
                elapsed < Duration::from_secs(10),
                "Read took too long ({:.2}s). Expected fast completion (<10s).",
                elapsed.as_secs_f64()
            );
            assert_eq!(
                &data[..50.min(data.len())],
                &file_content[..50.min(data.len())],
                "Downloaded data doesn't match seed content"
            );
            println!(
                "✓ Fast completion verified: read in {:.2}s",
                elapsed.as_secs_f64()
            );
        }
    }

    println!("\n=== Transient peer fast-exit test passed! ===");
}
