//! StatsGenerator — generates the .stats file content.
//! Extracted from TorrentFs to separate stats generation from FUSE protocol handling.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use crate::cache::CacheManager;
use crate::db::{Database, TorrentStatus};
use crate::download::DownloadManager;
use crate::metadata::TorrentInfo;
use tracing::warn;

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
    if n == 0 {
        return "0".to_string();
    }
    let mut s = String::new();
    let mut remaining = n;
    let mut count = 0;
    while remaining > 0 {
        if count > 0 && count % 3 == 0 {
            s.push(',');
        }
        s.push(((remaining % 10) as u8 + b'0') as char);
        remaining /= 10;
        count += 1;
    }
    s.chars().rev().collect()
}

/// Generate the .stats file content.
pub fn generate_stats(
    creation_time: Duration,
    db: &Option<Arc<Mutex<Database>>>,
    download_manager: &Option<Arc<Mutex<DownloadManager>>>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
    torrent_data_cache: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
) -> Vec<u8> {
    let _ = torrent_data_cache; // kept for future use
    let mut output = String::new();

    // Header
    output.push_str("═══════════════════════════════════════════════════════════════\n");
    output.push_str("  torrentfs v0.1.0\n");
    output.push_str("═══════════════════════════════════════════════════════════════\n\n");

    // --- 概况 ---
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let uptime_secs = now.as_secs().saturating_sub(creation_time.as_secs());
    let uptime_h = uptime_secs / 3600;
    let uptime_m = (uptime_secs % 3600) / 60;
    let uptime_s = uptime_secs % 60;

    output.push_str("── 概况 ──\n");
    output.push_str(&format!(
        "  运行时间:    {}h {}m {}s\n",
        uptime_h, uptime_m, uptime_s
    ));
    output.push_str("  挂载点:      (dynamic)\n");

    // DB path
    let db_path = if db.is_some() {
        "(active)"
    } else {
        "(none)"
    };
    output.push_str(&format!("  数据库:      {}\n", db_path));

    // Cache info
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
    output.push_str(&format!("  缓存目录:    {}\n", cache_dir_str));
    output.push_str(&format!(
        "  缓存总用量:  {} / {} ({:.1}%)\n",
        format_bytes(cache_total_size),
        format_bytes(cache_max_size),
        cache_pct
    ));

    // Session stats
    let session_stats = if let Some(ref dm) = download_manager {
        if let Ok(dm_guard) = dm.lock() {
            dm_guard.get_session_stats().ok()
        } else {
            None
        }
    } else {
        None
    };

    if let Some(ref ss) = session_stats {
        output.push_str("  监听地址:    0.0.0.0:6881\n");
        output.push_str(&format!("  DHT 节点:    {}\n", ss.dht_nodes));
    } else {
        output.push_str("  监听地址:    (not available)\n");
        output.push_str("  DHT 节点:    —\n");
    }

    // --- 全局速率 ---
    output.push_str("\n── 全局速率 ──\n");
    if let Some(ref ss) = session_stats {
        output.push_str(&format!(
            "  下载速率:    {}/s\n",
            format_bytes(ss.download_rate as u64)
        ));
        output.push_str(&format!(
            "  上传速率:    {}/s\n",
            format_bytes(ss.upload_rate as u64)
        ));
        output.push_str(&format!(
            "  累计下载:    {}\n",
            format_bytes(ss.total_downloaded as u64)
        ));
        output.push_str(&format!(
            "  累计上传:    {}\n",
            format_bytes(ss.total_uploaded as u64)
        ));
    } else {
        output.push_str("  下载速率:    —\n");
        output.push_str("  上传速率:    —\n");
        output.push_str("  累计下载:    —\n");
        output.push_str("  累计上传:    —\n");
    }

    // --- 连接 ---
    output.push_str("\n── 连接 ──\n");
    if let Some(ref ss) = session_stats {
        output.push_str(&format!("  已连接 peers:   {}\n", ss.peers_connected));
        output.push_str(&format!(
            "  半开连接:        {}\n",
            ss.half_open_connections
        ));
        output.push_str("  累计尝试:      —\n");
    } else {
        output.push_str("  已连接 peers:   —\n");
        output.push_str("  半开连接:       —\n");
        output.push_str("  累计尝试:      —\n");
    }

    // --- 种子总览 ---
    output.push_str("\n── 种子总览 ──\n");
    let (pending, downloading, seeding, error, total_torrents) =
        if let Some(db) = db.as_ref() {
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

    // Count unique info_hashes
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
        "  种子实例: {}    info_hash 去重: {}    等待: {}    下载: {}    做种: {}    错误: {}\n",
        total_torrents, unique_info_hashes, pending, downloading, seeding, error
    ));

    // --- 种子详情 ---
    output.push_str("\n── 种子详情 ──\n");

    // Pre-pass: ensure lightweight handles exist for all torrents
    if let Some(ref dm) = download_manager {
        if let Some(db) = db.as_ref() {
            if let Ok(db_guard) = db.lock() {
                if let Ok(torrents) = db_guard.get_all_torrents() {
                    if let Ok(mut dm_guard) = dm.lock() {
                        for t in &torrents {
                            if dm_guard.query_torrent_status(&t.info_hash).is_none() {
                                if let Some(ref torrent_data) = t.torrent_data {
                                    if let Ok(info) =
                                        TorrentInfo::from_bytes(torrent_data.clone())
                                    {
                                        if let Err(e) =
                                            dm_guard.ensure_handle_lightweight(&info)
                                        {
                                            warn!(
                                                "Failed to create lightweight handle for {} ({}): {:?}",
                                                t.name, &t.info_hash[..std::cmp::min(10, t.info_hash.len())], e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            if let Ok(torrents) = db_guard.get_all_torrents() {
                let mut torrent_idx = 0usize;
                for t in &torrents {
                    torrent_idx += 1;
                    output.push_str(&format!("  #{}  {}\n", torrent_idx, t.name));

                    let status_str = match t.status {
                        TorrentStatus::Pending => "等待",
                        TorrentStatus::Downloading => "下载",
                        TorrentStatus::Seeding => "做种",
                        TorrentStatus::Error => "错误",
                    };

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
                    ) = if let Some(ref dm) = download_manager {
                        if let Ok(dm_guard) = dm.lock() {
                            let handles = dm_guard.get_all_handles();
                            if let Some((_, handle)) =
                                handles.iter().find(|(ih, _)| ih == &t.info_hash)
                            {
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
                        }
                    } else {
                        (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
                    };

                    let prog_pct = if total_size > 0 {
                        progress * 100.0
                    } else {
                        0.0
                    };

                    output.push_str(&format!(
                        "      状态: {}     进度: {:.1}%    大小: {}\n",
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
                        "      下载: {}   上传: {}   分享率: {}\n",
                        format_bytes(total_done),
                        format_bytes(total_upload as u64),
                        share
                    ));

                    output.push_str(&format!(
                        "      速度: ↓ {}/s   ↑ {}/s  peers: {}   seeds: {}\n",
                        format_bytes(dl_rate as u64),
                        format_bytes(ul_rate as u64),
                        num_peers,
                        num_seeds
                    ));

                    if num_peers == 0 && num_seeds == 0 {
                        output.push_str(
                            "      ⚠ 健康警告: 0 peers / 0 seeds — tracker 可能无响应或种子无活跃节点\n",
                        );
                    }

                    output.push_str(&format!(
                        "      source_path: \"{}\"                       info_hash: {}...\n",
                        if t.source_path.is_empty() {
                            ""
                        } else {
                            &t.source_path
                        },
                        &t.info_hash[..std::cmp::min(10, t.info_hash.len())]
                    ));

                    output.push('\n');
                }
            }
        }
    }

    // --- 缓存详情 ---
    output.push_str("── 缓存详情 ──\n");
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
        "  全局命中: {}    全局未命中: {}    全局命中率: {:.1}%    淘汰: 0\n\n",
        format_num(global_hits),
        format_num(global_misses),
        hit_rate
    ));

    // Per-info_hash cache stats
    if let Some(ref cm) = get_cache_manager() {
        if let Ok(cm_guard) = cm.lock() {
            let infohashes = cm_guard.get_all_infohashes();
            for ih in &infohashes {
                let stats = cm_guard.get_cache_stats_by_infohash(ih);
                output.push_str(&format!(
                    "  [info_hash] {}...\n",
                    &ih[..std::cmp::min(10, ih.len())]
                ));
                output.push_str(&format!(
                    "    缓存:  {} pieces    {}    命中: {}    淘汰: 0\n",
                    stats.piece_count,
                    format_bytes(stats.total_size),
                    format_num(stats.hit_count)
                ));
                output.push_str("    种子:\n");

                // Query DB for associated torrents
                if let Some(db) = db.as_ref() {
                    if let Ok(db_guard) = db.lock() {
                        if let Ok(associated) = db_guard.get_torrents_by_infohash(ih) {
                            for (torrent_id, name, _filename, source_path) in &associated {
                                let idx_str = if let Ok(all) = db_guard.get_all_torrents() {
                                    all.iter()
                                        .position(|t| t.id == *torrent_id)
                                        .map(|p| format!("#{}", p + 1))
                                        .unwrap_or_else(|| "#?".to_string())
                                } else {
                                    "#?".to_string()
                                };
                                let sp = if source_path.is_empty() {
                                    ""
                                } else {
                                    source_path
                                };
                                output.push_str(&format!(
                                    "      {}  {:<40}  source_path: \"{}\"\n",
                                    idx_str, name, sp
                                ));
                            }
                        }
                    }
                }
                output.push('\n');
            }
        }
    }

    // --- 性能 ---
    output.push_str("── 性能 ──\n");
    output.push_str("  tick 间隔:      1000 ms\n");
    output.push_str("  内存 (RSS):     —\n");

    output.push_str("\n═══════════════════════════════════════════════════════════════\n");

    output.into_bytes()
}
