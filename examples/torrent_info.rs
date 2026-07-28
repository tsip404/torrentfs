use torrentfs::{TorrentError, TorrentInfo};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <torrent-file>", args[0]);
        std::process::exit(1);
    }

    let torrent_path = &args[1];

    match TorrentInfo::from_file(torrent_path) {
        Ok(info) => {
            println!("Torrent: {}", info.name());
            println!("Total Size: {} bytes", info.total_size());
            println!("Piece Length: {} bytes", info.piece_length());
            println!("Number of Pieces: {}", info.num_pieces());
            println!("Number of Files: {}", info.num_files());

            match info.info_hash() {
                Ok(hash) => {
                    print!("Info Hash: ");
                    for byte in &hash {
                        print!("{:02x}", byte);
                    }
                    println!();
                }
                Err(e) => eprintln!("Error getting info hash: {}", e),
            }

            match info.files() {
                Ok(files) => {
                    if files.len() > 0 {
                        println!("\nFiles:");
                        for (i, file) in files.iter().enumerate() {
                            println!("  [{}] {} ({} bytes)", i + 1, file.path, file.size);
                        }
                    }
                }
                Err(e) => eprintln!("Error getting file list: {}", e),
            }
        }
        Err(TorrentError::InvalidFile(msg)) => {
            eprintln!("Invalid torrent file: {}", msg);
            std::process::exit(2);
        }
        Err(TorrentError::ParseError(msg)) => {
            eprintln!("Failed to parse torrent: {}", msg);
            std::process::exit(3);
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(4);
        }
    }
}
