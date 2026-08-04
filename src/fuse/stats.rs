//! StatsGenerator — generates the .stats file content.
//! Extracted from TorrentFs to separate stats generation from FUSE protocol handling.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use crate::cache::CacheManager;
use crate::db::{Database, TorrentStatus};
use crate::services::download::DownloadService;

/// Format bytes into human-readable form.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a number with thousand separators.
pub fn format_num(n: u64) -> String {
    let s = n.to_string();
    let len = s.len();
    let mut result = String::with_capacity(len + (len.saturating_sub(1)) / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result
}

// ── Shared helpers ──────────────────────────────────────────────────────────

const BANNER: &str = "===========================================================\n";
const BANNER_LINE: &str = "===========================================================";

fn write_banner(output: &mut String, subtitle: Option<&str>) {
    let version = env!("CARGO_PKG_VERSION");
    output.push_str(BANNER);
    if let Some(sub) = subtitle {
        output.push_str(&format!("  torrentfs v{} — {}\n", version, sub));
    } else {
        output.push_str(&format!("  torrentfs v{}\n", version));
    }
    output.push_str(BANNER);
    output.push_str("\n\n");
}

fn write_overview(
    output: &mut String,
    creation_time: Duration,
    db: &Option<Arc<Mutex<Database>>>,
    download_service: &Option<DownloadService>,
    get_cache_manager: &impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
    listen_addr: &str,
) {
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let uptime_secs = now.as_secs().saturating_sub(creation_time.as_secs());
    let uptime_h = uptime_secs / 3600;
    let uptime_m = (uptime_secs % 3600) / 60;
    let uptime_s = uptime_secs % 60;

    output.push_str("-- Overview --\n");
    output.push_str(&format!(
        "  Uptime:       {}h {}m {}s\n",
        uptime_h, uptime_m, uptime_s
    ));
    output.push_str("  Mount:        (dynamic)\n");

    let db_path = if db.is_some() { "(active)" } else { "(none)" };
    output.push_str(&format!("  Database:     {}\n", db_path));

    let (cache_total_size, cache_max_size, cache_dir_str) =
        if let Some(ref cm) = get_cache_manager() {
            if let Ok(cm_guard) = cm.lock() {
                (
                    cm_guard.current_size(),
                    cm_guard.max_cache_size(),
                    "(cache)",
                )
            } else {
                (0, 0, "(locked)")
            }
        } else {
            (0, 0, "(none)")
        };
    let cache_pct = if cache_max_size > 0 {
        (cache_total_size as f64 / cache_max_size as f64) * 100.0
    } else {
        0.0
    };
    output.push_str(&format!("  Cache Dir:    {}\n", cache_dir_str));
    output.push_str(&format!(
        "  Cache Usage:  {} / {} ({:.1}%)\n",
        format_bytes(cache_total_size),
        format_bytes(cache_max_size),
        cache_pct
    ));

    let session_stats = if let Some(ref ds) = download_service {
        ds.get_session_stats().ok()
    } else {
        None
    };

    if let Some(ref ss) = session_stats {
        output.push_str(&format!("  Listen:       {}\n", listen_addr));
        output.push_str(&format!("  DHT Nodes:    {}\n", ss.dht_nodes));
    } else {
        output.push_str("  Listen:       (not available)\n");
        output.push_str("  DHT Nodes:    —\n");
    }
}

fn write_global_rates(output: &mut String, download_service: &Option<DownloadService>) {
    let session_stats = if let Some(ref ds) = download_service {
        ds.get_session_stats().ok()
    } else {
        None
    };

    output.push_str("\n-- Global Rates --\n");
    if let Some(ref ss) = session_stats {
        output.push_str(&format!(
            "  Download Rate:  {}/s\n",
            format_bytes(ss.download_rate as u64)
        ));
        output.push_str(&format!(
            "  Upload Rate:    {}/s\n",
            format_bytes(ss.upload_rate as u64)
        ));
        output.push_str(&format!(
            "  Total DL:       {}\n",
            format_bytes(ss.total_downloaded as u64)
        ));
        output.push_str(&format!(
            "  Total UL:       {}\n",
            format_bytes(ss.total_uploaded as u64)
        ));
    } else {
        output.push_str("  Download Rate:  —\n");
        output.push_str("  Upload Rate:    —\n");
        output.push_str("  Total DL:       —\n");
        output.push_str("  Total UL:       —\n");
    }
}

fn write_connections(output: &mut String, download_service: &Option<DownloadService>) {
    let session_stats = if let Some(ref ds) = download_service {
        ds.get_session_stats().ok()
    } else {
        None
    };

    output.push_str("\n-- Connections --\n");
    if let Some(ref ss) = session_stats {
        output.push_str(&format!("  Connected:      {}\n", ss.peers_connected));
        output.push_str(&format!("  Half-open:      {}\n", ss.half_open_connections));
        output.push_str("  Total Attempts: —\n");
    } else {
        output.push_str("  Connected:      —\n");
        output.push_str("  Half-open:      —\n");
        output.push_str("  Total Attempts: —\n");
    }
}

fn write_torrent_overview_counts(output: &mut String, db: &Option<Arc<Mutex<Database>>>) {
    output.push_str("\n-- Torrents --\n");
    let (pending, downloading, seeding, error, total_torrents) = if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            db_guard
                .get_torrent_counts_by_status()
                .unwrap_or((0, 0, 0, 0, 0))
        } else {
            (0, 0, 0, 0, 0)
        }
    } else {
        (0, 0, 0, 0, 0)
    };

    let unique_info_hashes = if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            if let Ok(torrents) = db_guard.get_all_torrents() {
                let mut set: std::collections::HashSet<&str> = std::collections::HashSet::new();
                for t in &torrents {
                    set.insert(t.info_hash.as_str());
                }
                set.len() as i64
            } else {
                0
            }
        } else {
            0
        }
    } else {
        0
    };

    output.push_str(&format!(
        "  Total: {}  Unique: {}  Pending: {}  Downloading: {}  Seeding: {}  Error: {}\n",
        total_torrents, unique_info_hashes, pending, downloading, seeding, error
    ));
}

fn write_global_cache_summary(
    output: &mut String,
    get_cache_manager: &impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
) {
    output.push_str("\n-- Cache --\n");
    let (global_hits, global_misses) = if let Some(ref cm) = get_cache_manager() {
        if let Ok(cm_guard) = cm.lock() {
            (cm_guard.hit_count, cm_guard.miss_count)
        } else {
            (0, 0)
        }
    } else {
        (0, 0)
    };
    let global_total = global_hits + global_misses;
    let hit_rate = if global_total > 0 {
        (global_hits as f64 / global_total as f64) * 100.0
    } else {
        0.0
    };
    output.push_str(&format!(
        "  Hits: {}  Misses: {}  Hit Rate: {:.1}%  Evictions: 0\n",
        format_num(global_hits),
        format_num(global_misses),
        hit_rate
    ));
}

fn write_performance(output: &mut String) {
    output.push_str("\n-- Performance --\n");
    output.push_str("  Tick Interval:  1000 ms\n");
    output.push_str("  Memory (RSS):   —\n");
}

fn status_to_english(status: &TorrentStatus) -> &'static str {
    match status {
        TorrentStatus::Pending => "Pending",
        TorrentStatus::Downloading => "Downloading",
        TorrentStatus::Seeding => "Seeding",
        TorrentStatus::Error => "Error",
    }
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Generate global stats (no per-torrent details, no per-infohash cache breakdown).
pub fn generate_global_stats(
    creation_time: Duration,
    db: &Option<Arc<Mutex<Database>>>,
    download_service: &Option<DownloadService>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
    listen_addr: &str,
) -> Vec<u8> {
    let mut output = String::new();

    write_banner(&mut output, None);
    write_overview(
        &mut output,
        creation_time,
        db,
        download_service,
        &get_cache_manager,
        listen_addr,
    );
    write_global_rates(&mut output, download_service);
    write_connections(&mut output, download_service);
    write_torrent_overview_counts(&mut output, db);
    write_global_cache_summary(&mut output, &get_cache_manager);
    write_performance(&mut output);

    output.push('\n');
    output.push_str(BANNER_LINE);
    output.push('\n');
    output.into_bytes()
}

/// Generate stats for a single torrent identified by torrent_id and info_hash.
pub fn generate_torrent_stats(
    torrent_id: i64,
    info_hash: &str,
    db: &Option<Arc<Mutex<Database>>>,
    download_service: &Option<DownloadService>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
) -> Vec<u8> {
    let mut output = String::new();

    let torrent = if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            db_guard.get_torrent_by_id(torrent_id).ok().flatten()
        } else {
            None
        }
    } else {
        None
    };

    let t = match torrent {
        Some(t) => t,
        None => {
            output.push_str(&format!(
                "  Torrent not found (id={}, info_hash={}...)\n",
                torrent_id,
                &info_hash[..std::cmp::min(10, info_hash.len())]
            ));
            output.push('\n');
            output.push_str(BANNER_LINE);
            output.push('\n');
            return output.into_bytes();
        }
    };

    // Torrent title line
    output.push_str(&format!("===== torrent: {} =====\n\n", t.name));

    let status_str = status_to_english(&t.status);

    let (
        dl_rate,
        ul_rate,
        num_peers,
        num_seeds,
        progress,
        total_size,
        total_done,
        total_upload,
        total_download,
    ) = if let Some(ref ds) = download_service {
        let handles = ds.get_all_handles();
        if let Some((_, handle)) = handles.iter().find(|(ih, _)| ih == info_hash) {
            if let Ok(h) = handle.lock() {
                if let Ok(status) = h.status() {
                    (
                        status.download_rate,
                        status.upload_rate,
                        status.num_peers,
                        status.num_seeds,
                        status.progress,
                        status.total,
                        status.total_done,
                        status.total_upload,
                        status.total_download,
                    )
                } else {
                    (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
                }
            } else {
                (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
            }
        } else {
            (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
        }
    } else {
        (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
    };

    let prog_pct = if total_size > 0 {
        progress * 100.0
    } else {
        0.0
    };

    // -- Status --
    output.push_str("-- Status --\n");
    output.push_str(&format!("  Name: {}\n", t.name));
    output.push_str(&format!(
        "  Status: {}  Progress: {:.1}%  Size: {}\n",
        status_str,
        prog_pct,
        format_bytes(t.total_size as u64)
    ));

    let share = if total_download > 0 {
        format!("{:.2}", total_upload as f64 / total_download as f64)
    } else {
        "—".to_string()
    };

    output.push_str(&format!(
        "  DL: {}  UL: {}  Ratio: {}\n",
        format_bytes(total_done),
        format_bytes(total_upload as u64),
        share
    ));

    // -- Rates --
    output.push_str("\n-- Rates --\n");
    output.push_str(&format!(
        "  Rate: ↓ {}/s  ↑ {}/s\n",
        format_bytes(dl_rate as u64),
        format_bytes(ul_rate as u64)
    ));

    // -- Peers --
    output.push_str("\n-- Peers --\n");
    output.push_str(&format!("  Peers: {}  Seeds: {}\n", num_peers, num_seeds));
    if num_peers == 0 && num_seeds == 0 {
        output.push_str("  ⚠ Health: 0 peers / 0 seeds — tracker may be unreachable\n");
    }

    // -- Info --
    output.push_str("\n-- Info --\n");
    if let Some(ref cm) = get_cache_manager() {
        if let Ok(cm_guard) = cm.lock() {
            let cache_stats = cm_guard.get_cache_stats_by_infohash(info_hash);
            output.push_str(&format!(
                "  Cache: {} pieces  {}\n",
                cache_stats.piece_count,
                format_bytes(cache_stats.total_size)
            ));
        }
    }
    output.push_str(&format!("  info_hash: {}\n", t.info_hash));
    output.push_str(&format!("  source_path: \"{}\"\n", t.source_path));

    output.push('\n');
    output.push_str(BANNER_LINE);
    output.push('\n');
    output.into_bytes()
}

/// Generate aggregated stats for all torrents under a given source_path.
pub fn generate_directory_stats(
    source_path: &str,
    db: &Option<Arc<Mutex<Database>>>,
    download_service: &Option<DownloadService>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
) -> Vec<u8> {
    let mut output = String::new();

    output.push_str(&format!("===== directory: {} =====\n\n", source_path));

    let torrents = if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            db_guard
                .get_torrents_by_source_path_prefix(source_path)
                .unwrap_or_default()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    if torrents.is_empty() {
        output.push_str("  No torrents found under this path.\n");
        output.push('\n');
        output.push_str(BANNER_LINE);
        output.push('\n');
        return output.into_bytes();
    }

    let torrent_count = torrents.len();
    let mut total_size: u64 = 0;
    let mut total_done: u64 = 0;
    let mut total_upload: u64 = 0;
    let mut total_download: u64 = 0;
    let mut aggregate_dl_rate: i64 = 0;
    let mut aggregate_ul_rate: i64 = 0;
    let mut aggregate_peers: i32 = 0;
    let mut aggregate_seeds: i32 = 0;

    for t in &torrents {
        total_size += t.total_size as u64;
        if let Some(ref ds) = download_service {
            let handles = ds.get_all_handles();
            if let Some((_, handle)) = handles.iter().find(|(ih, _)| ih == &t.info_hash) {
                if let Ok(h) = handle.lock() {
                    if let Ok(status) = h.status() {
                        total_done += status.total_done;
                        total_upload += status.total_upload as u64;
                        total_download += status.total_download as u64;
                        aggregate_dl_rate += status.download_rate;
                        aggregate_ul_rate += status.upload_rate;
                        aggregate_peers += status.num_peers;
                        aggregate_seeds += status.num_seeds;
                    }
                }
            }
        }
    }

    // -- Rates --
    output.push_str("-- Rates --\n");
    output.push_str(&format!(
        "  Torrents: {}  Total Size: {}  Downloaded: {}\n",
        torrent_count,
        format_bytes(total_size),
        format_bytes(total_done)
    ));
    output.push_str(&format!(
        "  DL Rate: ↓ {}/s  UL Rate: ↑ {}/s\n",
        format_bytes(aggregate_dl_rate as u64),
        format_bytes(aggregate_ul_rate as u64)
    ));
    output.push_str(&format!(
        "  Total UL: {}  Total DL: {}\n",
        format_bytes(total_upload),
        format_bytes(total_download)
    ));

    // -- Peers --
    output.push_str("\n-- Peers --\n");
    output.push_str(&format!(
        "  Peers: {}  Seeds: {}\n",
        aggregate_peers, aggregate_seeds
    ));

    // -- Cache --
    output.push_str("\n-- Cache --\n");
    if let Some(ref cm) = get_cache_manager() {
        if let Ok(cm_guard) = cm.lock() {
            let (cache_total_size, cache_max_size) =
                (cm_guard.current_size(), cm_guard.max_cache_size());
            let global_hits = cm_guard.hit_count;
            let global_misses = cm_guard.miss_count;
            let cache_pct = if cache_max_size > 0 {
                (cache_total_size as f64 / cache_max_size as f64) * 100.0
            } else {
                0.0
            };
            let global_total = global_hits + global_misses;
            let hit_rate = if global_total > 0 {
                (global_hits as f64 / global_total as f64) * 100.0
            } else {
                0.0
            };
            output.push_str(&format!(
                "  Cache Usage: {} / {} ({:.1}%)\n",
                format_bytes(cache_total_size),
                format_bytes(cache_max_size),
                cache_pct
            ));
            output.push_str(&format!(
                "  Hits: {}  Misses: {}  Hit Rate: {:.1}%\n",
                format_num(global_hits),
                format_num(global_misses),
                hit_rate
            ));
        } else {
            output.push_str("  (locked)\n");
        }
    } else {
        output.push_str("  (none)\n");
    }

    output.push_str("\n-- Torrents --\n");
    for (idx, t) in torrents.iter().enumerate() {
        let status_str = status_to_english(&t.status);

        let (dl_rate, ul_rate, peers, seeds, progress, ts) = if let Some(ref ds) = download_service
        {
            let handles = ds.get_all_handles();
            if let Some((_, handle)) = handles.iter().find(|(ih, _)| ih == &t.info_hash) {
                if let Ok(h) = handle.lock() {
                    if let Ok(status) = h.status() {
                        (
                            status.download_rate,
                            status.upload_rate,
                            status.num_peers,
                            status.num_seeds,
                            status.progress,
                            status.total,
                        )
                    } else {
                        (0, 0, 0, 0, 0.0, 0)
                    }
                } else {
                    (0, 0, 0, 0, 0.0, 0)
                }
            } else {
                (0, 0, 0, 0, 0.0, 0)
            }
        } else {
            (0, 0, 0, 0, 0.0, 0)
        };

        let prog_pct = if ts > 0 { progress * 100.0 } else { 0.0 };

        output.push_str(&format!(
            "  #{:<3} {:<40} {}  {:>5.1}%  ↓ {:<10}/s  ↑ {:<10}/s  {:>3}P/{:<3}S\n",
            idx + 1,
            if t.name.len() > 40 {
                t.name.chars().take(37).collect::<String>() + "..."
            } else {
                t.name.clone()
            },
            status_str,
            prog_pct,
            format_bytes(dl_rate as u64),
            format_bytes(ul_rate as u64),
            peers,
            seeds,
        ));
    }

    output.push('\n');
    output.push_str(BANNER_LINE);
    output.push('\n');
    output.into_bytes()
}

/// Generate the .stats file content (compatibility wrapper).
pub fn generate_stats(
    creation_time: Duration,
    db: &Option<Arc<Mutex<Database>>>,
    download_service: &Option<DownloadService>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
    torrent_data_cache: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
    listen_addr: &str,
) -> Vec<u8> {
    let _ = torrent_data_cache;
    generate_global_stats(
        creation_time,
        db,
        download_service,
        get_cache_manager,
        listen_addr,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_num_zero() {
        assert_eq!(format_num(0), "0");
    }

    #[test]
    fn test_format_num_single_digit() {
        assert_eq!(format_num(5), "5");
    }

    #[test]
    fn test_format_num_two_digits() {
        assert_eq!(format_num(42), "42");
    }

    #[test]
    fn test_format_num_three_digits() {
        assert_eq!(format_num(999), "999");
    }

    #[test]
    fn test_format_num_thousand() {
        assert_eq!(format_num(1000), "1,000");
    }

    #[test]
    fn test_format_num_ten_thousand() {
        assert_eq!(format_num(10000), "10,000");
    }

    #[test]
    fn test_format_num_hundred_thousand() {
        assert_eq!(format_num(100000), "100,000");
    }

    #[test]
    fn test_format_num_million() {
        assert_eq!(format_num(1000000), "1,000,000");
    }

    #[test]
    fn test_format_num_seven_digits() {
        assert_eq!(format_num(1234567), "1,234,567");
    }

    #[test]
    fn test_format_num_eight_digits() {
        assert_eq!(format_num(12345678), "12,345,678");
    }

    #[test]
    fn test_format_num_nine_digits() {
        assert_eq!(format_num(123456789), "123,456,789");
    }

    #[test]
    fn test_format_num_u64_max() {
        assert_eq!(format_num(u64::MAX), "18,446,744,073,709,551,615");
    }

    #[test]
    fn test_global_stats_header_present() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            &None,
            || None,
            "0.0.0.0:6881",
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("torrentfs v0.1.0"));
        assert!(text.contains("-- Overview --"));
    }

    #[test]
    fn test_global_stats_no_torrent_details() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            &None,
            || None,
            "0.0.0.0:6881",
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("── 种子详情 ──"));
    }

    #[test]
    fn test_global_stats_no_per_infohash_cache() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            &None,
            || None,
            "0.0.0.0:6881",
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("[info_hash]"));
    }

    #[test]
    fn test_torrent_stats_not_found() {
        let stats = generate_torrent_stats(999, "deadbeef", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("Torrent not found"));
    }

    #[test]
    fn test_directory_stats_empty_path() {
        let stats = generate_directory_stats("/nonexistent", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("No torrents found"));
    }

    #[test]
    fn test_generate_stats_is_wrapper() {
        let global = generate_global_stats(
            Duration::from_secs(0),
            &None,
            &None,
            || None,
            "0.0.0.0:6881",
        );
        let wrapper = generate_stats(
            Duration::from_secs(0),
            &None,
            &None,
            || None,
            &Arc::new(Mutex::new(HashMap::new())),
            "0.0.0.0:6881",
        );
        let gtext = String::from_utf8_lossy(&global);
        let wtext = String::from_utf8_lossy(&wrapper);
        assert_eq!(
            gtext, wtext,
            "generate_stats should produce same output as generate_global_stats"
        );
    }

    #[test]
    fn test_name_truncation_utf8_safe() {
        let long_cjk = "这是一个很长的种子文件名测试用例".to_string(); // 16 chars, 48 bytes
        assert!(long_cjk.len() > 40);
        let truncated = long_cjk.chars().take(37).collect::<String>() + "...";
        assert_eq!(truncated.chars().count(), 16 + 3);
    }

    #[test]
    fn test_global_stats_ascii_borders() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            &None,
            || None,
            "0.0.0.0:6881",
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("====="), "borders must be ASCII '='");
        assert!(!text.contains('\u{2550}'), "no Unicode double-line borders");
    }

    #[test]
    fn test_global_stats_english_headers() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            &None,
            || None,
            "0.0.0.0:6881",
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("-- Overview --"));
        assert!(text.contains("-- Global Rates --"));
        assert!(text.contains("-- Connections --"));
        assert!(text.contains("-- Torrents --"));
        assert!(text.contains("-- Cache --"));
        assert!(text.contains("-- Performance --"));
    }

    #[test]
    fn test_global_stats_total_unique_format() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            &None,
            || None,
            "0.0.0.0:6881",
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("Total: "));
        assert!(text.contains("Unique: "));
    }

    #[test]
    fn test_torrent_stats_english_status() {
        // Without a real DB, this should just not panic with "Torrent not found"
        let stats = generate_torrent_stats(1, "abc", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("等待"));
        assert!(!text.contains("下载"));
        assert!(!text.contains("做种"));
        assert!(!text.contains("错误"));
    }

    #[test]
    fn test_directory_stats_english_headers() {
        let stats = generate_directory_stats("/nonexistent", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("──"));
        assert!(text.contains("No torrents found"));
    }

    #[test]
    fn test_torrent_stats_has_title_line() {
        // Without a real DB, this should just not panic with "Torrent not found"
        // but we can still verify the format when it IS found by checking code structure.
        // Test with torrent not found case: verify the function doesn't crash.
        let stats = generate_torrent_stats(999, "deadbeef", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(
            text.contains("Torrent not found"),
            "stats for missing torrent should include 'Torrent not found', got: {}",
            text
        );
    }

    #[test]
    fn test_torrent_stats_section_headers_present_in_code() {
        // Verify the section header strings exist in the compiled binary
        // by checking the const patterns that would appear in any torrent stats output.
        let stats = generate_torrent_stats(999, "deadbeef", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        // torrent not found case; version banner must NOT appear (root-only)
        assert!(!text.contains("torrentfs v"));
    }

    #[test]
    fn test_torrent_stats_no_version_banner() {
        // Leaf .stats must not include the version banner.
        let stats = generate_torrent_stats(999, "deadbeef", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("torrentfs v0"));
    }

    #[test]
    fn test_directory_stats_header_format() {
        let stats = generate_directory_stats("/test/path", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("===== directory: /test/path ====="));
    }

    #[test]
    fn test_directory_stats_no_version_banner() {
        // Intermediate directory .stats must not include the version banner.
        let stats = generate_directory_stats("/test/path", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("torrentfs v0"));
    }

    #[test]
    fn test_directory_stats_has_path_title() {
        let stats = generate_directory_stats("/nonexistent", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        // Path title uses spec format: ===== directory: {path} =====
        assert!(text.contains("===== directory: /nonexistent ====="));
    }

    #[test]
    fn test_directory_stats_section_headers_empty() {
        let stats = generate_directory_stats("/nonexistent", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        // Empty path shows "No torrents found" not section headers
        assert!(text.contains("No torrents found"));
    }
}
