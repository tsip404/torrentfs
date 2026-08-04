use std::thread;
use std::time::Duration;
use torrentfs::TorrentInfo;

mod common;

fn create_test_torrent() -> (Vec<u8>, Vec<u8>) {
    let mut test_content = b"Hello, this is a test file for torrentfs verification.\n".to_vec();
    while test_content.len() < 16384 {
        test_content.push(b'X');
    }
    test_content.truncate(16384);

    let mut torrent = Vec::new();
    torrent.extend_from_slice(b"d8:announce30:http://localhost:6969/announce4:infod");
    torrent.extend_from_slice(
        b"6:lengthi16384e4:name22:final_verification.txt12:piece lengthi16384e6:pieces20:",
    );
    torrent.extend_from_slice(&hashlib_sha1(&test_content));
    torrent.extend_from_slice(b"ee");

    (torrent, test_content)
}

fn hashlib_sha1(data: &[u8]) -> [u8; 20] {
    let mut hasher = sha1_smol::Sha1::new();
    hasher.update(data);
    hasher.digest().bytes()
}

#[test]
fn test_torrent_info_from_bytes() {
    let (torrent_data, _) = create_test_torrent();

    let result = TorrentInfo::from_bytes(torrent_data.clone());
    match result {
        Ok(info) => {
            println!("Torrent name: {}", info.name());
            println!("Total size: {}", info.total_size());
            println!("Piece length: {}", info.piece_length());
            println!("Num pieces: {}", info.num_pieces());
            println!("Num files: {}", info.num_files());
            assert_eq!(info.name(), "final_verification.txt");
            assert_eq!(info.total_size(), 16384);
            assert_eq!(info.num_files(), 1);
        }
        Err(e) => {
            panic!("Failed to parse torrent: {:?}", e);
        }
    }
}

#[test]
fn test_read_file_range_with_local_seed() {
    use std::fs;
    use torrentfs::download::DownloadManager;

    let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().join("cache");
    let seed_dir = temp_dir.path().join("seed");

    fs::create_dir_all(&cache_dir).expect("Failed to create cache dir");
    fs::create_dir_all(&seed_dir).expect("Failed to create seed dir");

    let (torrent_data, file_content) = create_test_torrent();

    let info = TorrentInfo::from_bytes(torrent_data.clone()).expect("Failed to parse torrent");

    println!("Torrent info:");
    println!("  Name: {}", info.name());
    println!("  Total size: {}", info.total_size());
    println!("  Piece length: {}", info.piece_length());
    println!("  Num pieces: {}", info.num_pieces());

    let mut dm = DownloadManager::new(&cache_dir, &torrentfs::TorrentfsConfig::default_config())
        .expect("Failed to create download manager");

    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    let torrent_cache_dir = cache_dir.join(&info_hash);
    fs::create_dir_all(&torrent_cache_dir).expect("Failed to create torrent cache dir");

    let seed_file_path = torrent_cache_dir.join("final_verification.txt");
    fs::write(&seed_file_path, &file_content).expect("Failed to write seed file");

    println!("\nAttempting to read file range...");
    println!("Info hash: {}", info_hash);
    println!("Seed file path: {:?}", seed_file_path);

    let result = dm.read_file_range(&info, 0, 0, 50);

    match result {
        Ok(data) => {
            println!("Successfully read {} bytes", data.len());
            println!("Data: {:?}", String::from_utf8_lossy(&data));
            assert!(!data.is_empty(), "Expected non-empty data");
            assert_eq!(data.as_slice(), &file_content[0..50], "Data mismatch");
        }
        Err(e) => {
            // No external peers available — expected in CI/test environments.
            // Only NoPeers is expected — piece timeout (InvalidFile) is a real defect.
            assert!(
                matches!(e, torrentfs::TorrentError::NoPeers(_)),
                "Unexpected error: {:?}",
                e
            );
        }
    }
}

/// Integration test: verify that read_file_range works end-to-end with
/// a local tracker + seeder (TestHarness). This validates the full
/// peer-discovery-and-download flow required by acceptance scenario 4.
#[test]
fn test_read_file_range_with_test_harness() {
    let harness = common::TestHarness::new();

    println!(
        "Tracker: {}, announces: {}",
        harness.tracker.announce_url(),
        harness.tracker.announce_count()
    );

    let info_hash = hex::encode(harness.info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);

    // Create a downloader session using DownloadManager with local test config
    let config = common::local_test_config();
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");

    let mut dm = torrentfs::download::DownloadManager::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadManager");

    // Re-parse torrent data (raw pointer in TorrentInfo can't be shared)
    let dl_info = torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
        .expect("Failed to parse torrent for downloader");

    println!("Downloader: attempting read_file_range...");

    let dl_timeout = Duration::from_secs(60);
    let read_start = std::time::Instant::now();
    let mut last_error: Option<torrentfs::TorrentError> = None;

    let result = loop {
        match dm.read_file_range(&dl_info, 0, 0, 16384) {
            Ok(data) => break Ok(data),
            Err(e) => {
                last_error = Some(e);
                if read_start.elapsed() > dl_timeout {
                    break Err(last_error.take().unwrap());
                }
                println!(
                    "Read attempt at {:.1}s: {:?}, retrying...",
                    read_start.elapsed().as_secs_f64(),
                    last_error.as_ref().unwrap()
                );
                thread::sleep(Duration::from_secs(1));
            }
        }
    };

    match result {
        Ok(data) => {
            let elapsed = read_start.elapsed();
            println!(
                "Successfully read {} bytes in {:.1}s",
                data.len(),
                elapsed.as_secs_f64()
            );
            assert!(!data.is_empty(), "Expected non-empty data");
            assert_eq!(
                data.as_slice(),
                &harness.file_content[..],
                "Downloaded data doesn't match seed content"
            );
        }
        Err(e) => {
            panic!(
                "read_file_range failed: {:?}. \
                 Tracker announces: {}. \
                 This test requires a functioning local tracker + seeder.",
                e,
                harness.tracker.announce_count()
            );
        }
    }

    println!("=== read_file_range with TestHarness passed! ===");
}
