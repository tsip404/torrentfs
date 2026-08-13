//! torrentfs — A FUSE filesystem for BitTorrent management.
//! Thin binary entry point. All logic lives in the library crate.

use clap::Parser;
use fuser::MountOption;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use torrentfs::config::TorrentfsConfig;
use torrentfs::db::Database;
use torrentfs::fuse::TorrentFs;
use torrentfs::infrastructure::alert::AlertConsumer;

#[derive(Parser, Debug)]
#[command(name = "torrentfs")]
#[command(about = "A FUSE filesystem for torrent management")]
struct Args {
    #[arg(help = "Mount point path")]
    mountpoint: PathBuf,
    #[arg(long, help = "Database path")]
    db: Option<PathBuf>,
    #[arg(long, help = "Cache directory for downloaded pieces")]
    cache: Option<PathBuf>,
    #[arg(long, help = "Configuration file path (TOML)")]
    config: Option<PathBuf>,
}

fn fuse_allow_other_enabled() -> io::Result<bool> {
    let file = File::open("/etc/fuse.conf")?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim() == "user_allow_other" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn user_in_fuse_group() -> bool {
    use std::fs;
    if let Ok(group_file) = fs::read_to_string("/etc/group") {
        for line in group_file.lines() {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 4 && parts[0] == "fuse" {
                let members = parts[3];
                if let Ok(current_user) = std::env::var("USER") {
                    if members.split(',').any(|m| m.trim() == current_user) {
                        return true;
                    }
                }
            }
        }
    }

    if let Ok(output) = std::process::Command::new("groups").output() {
        let groups = String::from_utf8_lossy(&output.stdout);
        if groups.split_whitespace().any(|g| g == "fuse") {
            return true;
        }
    }

    false
}

/// Spawn the background alert consumer thread if a session is available.
fn spawn_alert_consumer(fs: &TorrentFs) -> Option<AlertConsumer> {
    let ds = fs.download_service.as_ref()?;
    let session_ptr = ds.session_ptr()?;
    let stats = ds.cached_stats();
    let pending_reads = ds.pending_reads();
    Some(AlertConsumer::spawn(session_ptr, stats, pending_reads))
}

fn main() {
    let log_level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "trace" => Some(Level::TRACE),
            "debug" => Some(Level::DEBUG),
            "info" => Some(Level::INFO),
            "warn" => Some(Level::WARN),
            "error" => Some(Level::ERROR),
            _ => None,
        })
        .unwrap_or(Level::INFO);

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let args = Args::parse();

    // Load configuration from TOML file if provided
    let config = match &args.config {
        Some(config_path) => match TorrentfsConfig::from_file(config_path) {
            Ok(cfg) => {
                info!("Loaded configuration from {:?}", config_path);
                cfg
            }
            Err(e) => {
                error!("Failed to load config from {:?}: {}", config_path, e);
                std::process::exit(1);
            }
        },
        None => TorrentfsConfig::default_config(),
    };

    // Early check: /dev/fuse must exist for FUSE mounts to work.
    // On rootless containers this is the most common failure point.
    if !std::path::Path::new("/dev/fuse").exists() {
        error!(
            "/dev/fuse not found. torrentfs requires the FUSE kernel module.\n\
             Container users: pass --device /dev/fuse --cap-add SYS_ADMIN to podman/docker.\n\
             Host users: ensure the fuse kernel module is loaded (modprobe fuse)."
        );
        std::process::exit(3);
    }

    if !args.mountpoint.exists() {
        std::fs::create_dir_all(&args.mountpoint).expect("Failed to create mountpoint");
    }

    let cache_path = args.cache.clone().unwrap_or_else(|| {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("torrentfs/cache")
    });

    if !cache_path.exists() {
        if let Err(e) = std::fs::create_dir_all(&cache_path) {
            warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
        }
    }

    let db_path = if let Some(db_path) = &args.db {
        db_path.clone()
    } else {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("torrentfs/db/metadata.db")
    };

    if let Some(parent) = db_path.parent() {
        if !parent.exists() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("Failed to create database directory: {:?}", e);
            }
        }
    }

    let allow_other_enabled = fuse_allow_other_enabled().unwrap_or(false);

    if allow_other_enabled {
        let options = vec![
            MountOption::FSName("torrentfs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
        ];

        let db = match Database::open(&db_path) {
            Ok(db) => {
                info!("Database opened at {:?}", db_path);
                Some(db)
            }
            Err(e) => {
                if args.db.is_some() {
                    error!("Failed to open database: {:?}", e);
                    std::process::exit(1);
                }
                warn!(
                    "Failed to open database at {:?}: {:?}, running without persistence",
                    db_path, e
                );
                None
            }
        };

        let fs = match db {
            Some(d) => TorrentFs::new_with_db_and_cache(d, cache_path.clone(), &config),
            None => TorrentFs::new_with_cache_path(cache_path.clone(), &config),
        };

        let alert_consumer = spawn_alert_consumer(&fs);

        match fuser::spawn_mount2(fs, &args.mountpoint, &options) {
            Ok(bg) => {
                info!("torrentfs mounted");
                // Park to keep the process running until interrupted.
                std::thread::park();
                alert_consumer.as_ref().map(|a| a.stop());
                drop(bg);
                return;
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                warn!("Mount with AllowOther failed, falling back to owner-only mode");
            }
            Err(e) => {
                error!("Failed to mount filesystem: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        warn!("user_allow_other not set in /etc/fuse.conf, mount will only be accessible by owner");
    }

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];

    let db = match Database::open(&db_path) {
        Ok(db) => {
            info!("Database opened at {:?}", db_path);
            Some(db)
        }
        Err(e) => {
            if args.db.is_some() {
                error!("Failed to open database: {:?}", e);
                std::process::exit(1);
            }
            warn!(
                "Failed to open database at {:?}: {:?}, running without persistence",
                db_path, e
            );
            None
        }
    };

    let fs = match db {
        Some(d) => TorrentFs::new_with_db_and_cache(d, cache_path.clone(), &config),
        None => TorrentFs::new_with_cache_path(cache_path.clone(), &config),
    };

    let alert_consumer = spawn_alert_consumer(&fs);

    match fuser::spawn_mount2(fs, &args.mountpoint, &options) {
        Ok(bg) => {
            info!("torrentfs mounted");
            std::thread::park();
            alert_consumer.as_ref().map(|a| a.stop());
            drop(bg);
            info!("torrentfs unmounted successfully");
        }
        Err(e) => {
            let error_msg = e.to_string();
            if e.kind() == io::ErrorKind::PermissionDenied {
                let mut hints = Vec::new();
                if !allow_other_enabled {
                    hints.push("'user_allow_other' is not set in /etc/fuse.conf");
                }
                if !user_in_fuse_group() {
                    hints.push("user may not be in the 'fuse' group (some systems require this)");
                }
                hints.push("running in a container or restricted environment");
                hints.push("SELinux/AppArmor restrictions");
                hints.push("/dev/fuse device permissions");
                error!(
                    "Mount failed: Operation not permitted. Possible causes:\n  - {}",
                    hints.join("\n  - ")
                );
                std::process::exit(2);
            }
            error!("Failed to mount filesystem: {}", error_msg);
            std::process::exit(1);
        }
    }
}
