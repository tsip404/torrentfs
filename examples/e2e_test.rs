use std::fs;
use std::path::Path;
use torrentfs::{download::DownloadManager, TorrentInfo, TorrentfsConfig};

fn main() {
    tracing_subscriber::fmt::init();

    let test_dir = Path::new("/tmp/torrentfs-e2e-test");
    let cache_dir = test_dir.join("cache");
    let seed_dir = test_dir.join("seed");
    let torrent_path = test_dir.join("test_e2e.torrent");

    fs::create_dir_all(&cache_dir).expect("Failed to create cache dir");

    let torrent_data = fs::read(&torrent_path).expect("Failed to read torrent file");
    let info = TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent");

    println!("=== Torrent Info ===");
    println!("Name: {}", info.name());
    println!("Total size: {} bytes", info.total_size());
    println!("Piece length: {} bytes", info.piece_length());
    println!("Num pieces: {}", info.num_pieces());

    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);

    let mut dm = DownloadManager::new(&cache_dir, &TorrentfsConfig::default_config())
        .expect("Failed to create download manager");

    let torrent_cache_dir = cache_dir.join(&info_hash);
    fs::create_dir_all(&torrent_cache_dir).expect("Failed to create torrent cache dir");

    let seed_file = seed_dir.join("test_e2e.txt");
    let target_file = torrent_cache_dir.join("test_e2e.txt");
    fs::copy(&seed_file, &target_file).expect("Failed to copy seed file");

    println!("\n=== Seed file setup ===");
    println!("Seed file: {:?}", seed_file);
    println!("Target file: {:?}", target_file);

    let seed_content = fs::read(&seed_file).expect("Failed to read seed file");
    println!("Seed content length: {} bytes", seed_content.len());

    println!("\n=== Testing read_file_range ===");

    let test_cases = [
        (0u64, 50u32, "First 50 bytes"),
        (0u64, 162u32, "Full file"),
        (50u64, 50u32, "Bytes 50-100"),
        (100u64, 62u32, "Bytes 100-162"),
    ];

    for (offset, size, desc) in test_cases {
        print!("Testing {} (offset={}, size={})...", desc, offset, size);

        match dm.read_file_range(&info, 0, offset, size) {
            Ok(data) => {
                let expected_end =
                    std::cmp::min(offset as usize + size as usize, seed_content.len());
                let expected = &seed_content[offset as usize..expected_end];

                if data.as_slice() == expected {
                    println!(" OK ({} bytes)", data.len());
                    if size <= 100 {
                        println!("  Content: {:?}", String::from_utf8_lossy(&data));
                    }
                } else {
                    println!(" MISMATCH!");
                    println!("  Expected: {:?}", String::from_utf8_lossy(expected));
                    println!("  Got: {:?}", String::from_utf8_lossy(&data));
                    std::process::exit(1);
                }
            }
            Err(e) => {
                println!(" ERROR: {:?}", e);
                std::process::exit(1);
            }
        }
    }

    println!("\n=== All tests passed! ===");
}
