use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::{TorrentError, TorrentResult};

/// Top-level TOML configuration for torrentfs.
/// All fields are optional — missing values use libtorrent defaults.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TorrentfsConfig {
    #[serde(default)]
    pub connections: ConnectionsConfig,

    #[serde(default)]
    pub dht: DhtConfig,

    #[serde(default)]
    pub local_discovery: LocalDiscoveryConfig,

    #[serde(default)]
    pub rate_limits: RateLimitsConfig,

    #[serde(default)]
    pub disk_io: DiskIoConfig,

    #[serde(default)]
    pub cache: CacheConfig,

    #[serde(default)]
    pub pieces: PiecesConfig,

    #[serde(default)]
    pub timeouts: TimeoutsConfig,

    #[serde(default)]
    pub tracker: TrackerConfig,

    #[serde(default)]
    pub algorithms: AlgorithmsConfig,

    #[serde(default)]
    pub active_limits: ActiveLimitsConfig,

    #[serde(default)]
    pub auto_manage: AutoManageConfig,

    #[serde(default)]
    pub encryption: EncryptionConfig,

    #[serde(default)]
    pub proxy: ProxyConfig,

    #[serde(default)]
    pub user_agent: UserAgentConfig,

    #[serde(default)]
    pub alert: AlertConfig,

    #[serde(default)]
    pub performance: PerformanceConfig,

    #[serde(default)]
    pub misc: MiscConfig,
}

impl TorrentfsConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &Path) -> TorrentResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            TorrentError::ParseError(format!("Failed to read config file {:?}: {}", path, e))
        })?;
        let config: TorrentfsConfig = toml::from_str(&content).map_err(|e| {
            TorrentError::ParseError(format!("Invalid config TOML in {:?}: {}", path, e))
        })?;
        Ok(config)
    }

    /// Default configuration (all libtorrent defaults).
    pub fn default_config() -> Self {
        Self::default()
    }

    /// Serialize non-default settings to a flat JSON string for the C FFI layer.
    /// The JSON keys must match libtorrent settings_pack names exactly.
    pub fn to_settings_json(&self) -> String {
        let mut map = serde_json::Map::new();

        // --- connections ---
        self.connections.write_json(&mut map);
        // --- dht ---
        self.dht.write_json(&mut map);
        // --- local_discovery ---
        self.local_discovery.write_json(&mut map);
        // --- rate_limits ---
        self.rate_limits.write_json(&mut map);
        // --- disk_io ---
        self.disk_io.write_json(&mut map);
        // --- cache ---
        self.cache.write_json(&mut map);
        // --- pieces ---
        self.pieces.write_json(&mut map);
        // --- timeouts ---
        self.timeouts.write_json(&mut map);
        // --- tracker ---
        self.tracker.write_json(&mut map);
        // --- algorithms ---
        self.algorithms.write_json(&mut map);
        // --- active_limits ---
        self.active_limits.write_json(&mut map);
        // --- auto_manage ---
        self.auto_manage.write_json(&mut map);
        // --- encryption ---
        self.encryption.write_json(&mut map);
        // --- proxy ---
        self.proxy.write_json(&mut map);
        // --- user_agent ---
        self.user_agent.write_json(&mut map);
        // --- alert ---
        self.alert.write_json(&mut map);
        // --- performance ---
        self.performance.write_json(&mut map);
        // --- misc ---
        self.misc.write_json(&mut map);

        serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
    }
}

impl Default for TorrentfsConfig {
    fn default() -> Self {
        TorrentfsConfig {
            connections: ConnectionsConfig::default(),
            dht: DhtConfig::default(),
            local_discovery: LocalDiscoveryConfig::default(),
            rate_limits: RateLimitsConfig::default(),
            disk_io: DiskIoConfig::default(),
            cache: CacheConfig::default(),
            pieces: PiecesConfig::default(),
            timeouts: TimeoutsConfig::default(),
            tracker: TrackerConfig::default(),
            algorithms: AlgorithmsConfig::default(),
            active_limits: ActiveLimitsConfig::default(),
            auto_manage: AutoManageConfig::default(),
            encryption: EncryptionConfig::default(),
            proxy: ProxyConfig::default(),
            user_agent: UserAgentConfig::default(),
            alert: AlertConfig::default(),
            performance: PerformanceConfig::default(),
            misc: MiscConfig::default(),
        }
    }
}

// Helper trait to write config sections to JSON
trait WriteJson {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>);
}

macro_rules! json_field_str {
    ($map:expr, $self:expr, $field:ident) => {
        if let Some(ref val) = $self.$field {
            if !val.is_empty() {
                $map.insert(
                    stringify!($field).to_string(),
                    serde_json::Value::String(val.clone()),
                );
            }
        }
    };
}

macro_rules! json_field_int {
    ($map:expr, $self:expr, $field:ident) => {
        if let Some(val) = $self.$field {
            $map.insert(
                stringify!($field).to_string(),
                serde_json::Value::Number(serde_json::Number::from(val)),
            );
        }
    };
}

macro_rules! json_field_bool {
    ($map:expr, $self:expr, $field:ident) => {
        if let Some(val) = $self.$field {
            $map.insert(stringify!($field).to_string(), serde_json::Value::Bool(val));
        }
    };
}

// ============================================================
// Connections
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ConnectionsConfig {
    pub listen_interfaces: Option<String>,
    pub outgoing_interfaces: Option<String>,
    pub max_connections: Option<i64>,
    pub max_uploads: Option<i64>,
    pub listen_queue_size: Option<i64>,
    pub connection_speed: Option<i64>,
    pub smooth_connects: Option<bool>,
    pub allow_multiple_connections_per_ip: Option<bool>,
    pub max_peerlist_size: Option<i64>,
    pub max_paused_peerlist_size: Option<i64>,
    pub min_reconnect_time: Option<i64>,
    pub peer_connect_timeout: Option<i64>,
}

impl WriteJson for ConnectionsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_str!(map, self, listen_interfaces);
        json_field_str!(map, self, outgoing_interfaces);
        json_field_int!(map, self, max_connections);
        json_field_int!(map, self, max_uploads);
        json_field_int!(map, self, listen_queue_size);
        json_field_int!(map, self, connection_speed);
        json_field_bool!(map, self, smooth_connects);
        json_field_bool!(map, self, allow_multiple_connections_per_ip);
        json_field_int!(map, self, max_peerlist_size);
        json_field_int!(map, self, max_paused_peerlist_size);
        json_field_int!(map, self, min_reconnect_time);
        json_field_int!(map, self, peer_connect_timeout);
    }
}

// ============================================================
// DHT
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DhtConfig {
    pub enabled: Option<bool>,
    pub max_dht_items: Option<i64>,
    pub dht_announce_interval: Option<i64>,
    pub max_active_dht_limit: Option<i64>,
}

impl WriteJson for DhtConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        if let Some(val) = self.enabled {
            map.insert("enable_dht".to_string(), serde_json::Value::Bool(val));
        }
        json_field_int!(map, self, max_dht_items);
        json_field_int!(map, self, dht_announce_interval);
        json_field_int!(map, self, max_active_dht_limit);
    }
}

// ============================================================
// Local Discovery
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct LocalDiscoveryConfig {
    pub lsd_enabled: Option<bool>,
    pub upnp_enabled: Option<bool>,
    pub natpmp_enabled: Option<bool>,
}

impl WriteJson for LocalDiscoveryConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        if let Some(val) = self.lsd_enabled {
            map.insert("enable_lsd".to_string(), serde_json::Value::Bool(val));
        }
        if let Some(val) = self.upnp_enabled {
            map.insert("enable_upnp".to_string(), serde_json::Value::Bool(val));
        }
        if let Some(val) = self.natpmp_enabled {
            map.insert("enable_natpmp".to_string(), serde_json::Value::Bool(val));
        }
    }
}

// ============================================================
// Rate Limits
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RateLimitsConfig {
    pub download_rate_limit: Option<i64>,
    pub upload_rate_limit: Option<i64>,
    pub rate_limit_utp: Option<bool>,
    pub rate_limit_ip_overhead: Option<bool>,
}

impl WriteJson for RateLimitsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, download_rate_limit);
        json_field_int!(map, self, upload_rate_limit);
        json_field_bool!(map, self, rate_limit_utp);
        json_field_bool!(map, self, rate_limit_ip_overhead);
    }
}

// ============================================================
// Disk I/O
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiskIoConfig {
    pub disk_io_write_mode: Option<i64>,
    pub disk_io_read_mode: Option<i64>,
    pub file_pool_size: Option<i64>,
    pub max_queued_disk_bytes: Option<i64>,
    pub max_queued_disk_bytes_low_watermark: Option<i64>,
    pub use_disk_read_ahead: Option<bool>,
    pub lock_disk_cache: Option<bool>,
    pub no_atime_storage: Option<bool>,
    pub low_prio_disk: Option<bool>,
}

impl WriteJson for DiskIoConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, disk_io_write_mode);
        json_field_int!(map, self, disk_io_read_mode);
        json_field_int!(map, self, file_pool_size);
        json_field_int!(map, self, max_queued_disk_bytes);
        json_field_int!(map, self, max_queued_disk_bytes_low_watermark);
        json_field_bool!(map, self, use_disk_read_ahead);
        json_field_bool!(map, self, lock_disk_cache);
        json_field_bool!(map, self, no_atime_storage);
        json_field_bool!(map, self, low_prio_disk);
    }
}

// ============================================================
// Cache
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct CacheConfig {
    pub cache_size: Option<i64>,
    pub cache_expiry: Option<i64>,
    pub use_read_cache: Option<bool>,
    pub use_disk_cache_pool: Option<bool>,
    pub volatile_read_cache: Option<bool>,
    pub guided_read_cache: Option<bool>,
    pub default_cache_min_age: Option<i64>,
}

impl WriteJson for CacheConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, cache_size);
        json_field_int!(map, self, cache_expiry);
        json_field_bool!(map, self, use_read_cache);
        json_field_bool!(map, self, use_disk_cache_pool);
        json_field_bool!(map, self, volatile_read_cache);
        json_field_bool!(map, self, guided_read_cache);
        json_field_int!(map, self, default_cache_min_age);
    }
}

// ============================================================
// Pieces
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PiecesConfig {
    pub whole_pieces_threshold: Option<i64>,
    pub prioritize_partial_pieces: Option<bool>,
    pub max_out_request_queue: Option<i64>,
    pub max_allowed_in_request_queue: Option<i64>,
    pub piece_timeout: Option<i64>,
    pub request_timeout: Option<i64>,
    pub predictive_piece_announce: Option<i64>,
    pub max_suggest_pieces: Option<i64>,
    pub drop_skipped_requests: Option<bool>,
    pub seeding_piece_quota: Option<i64>,
    pub max_sparse_regions: Option<i64>,
}

impl WriteJson for PiecesConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, whole_pieces_threshold);
        json_field_bool!(map, self, prioritize_partial_pieces);
        json_field_int!(map, self, max_out_request_queue);
        json_field_int!(map, self, max_allowed_in_request_queue);
        json_field_int!(map, self, piece_timeout);
        json_field_int!(map, self, request_timeout);
        json_field_int!(map, self, predictive_piece_announce);
        json_field_int!(map, self, max_suggest_pieces);
        json_field_bool!(map, self, drop_skipped_requests);
        json_field_int!(map, self, seeding_piece_quota);
        json_field_int!(map, self, max_sparse_regions);
    }
}

// ============================================================
// Timeouts
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TimeoutsConfig {
    pub peer_timeout: Option<i64>,
    pub urlseed_timeout: Option<i64>,
    pub urlseed_pipeline_size: Option<i64>,
    pub stop_tracker_timeout: Option<i64>,
    pub tracker_completion_timeout: Option<i64>,
    pub tracker_receive_timeout: Option<i64>,
    pub inactivity_timeout: Option<i64>,
    /// Timeout in seconds for waiting on torrent state transitions and piece downloads
    /// during FUSE read operations. Defaults to 30s if not set.
    /// This is a torrentfs-level timeout, not passed to libtorrent.
    pub read_timeout_secs: Option<i64>,
}

impl WriteJson for TimeoutsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, peer_timeout);
        json_field_int!(map, self, urlseed_timeout);
        json_field_int!(map, self, urlseed_pipeline_size);
        json_field_int!(map, self, stop_tracker_timeout);
        json_field_int!(map, self, tracker_completion_timeout);
        json_field_int!(map, self, tracker_receive_timeout);
        json_field_int!(map, self, inactivity_timeout);
    }
}

// ============================================================
// Tracker
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TrackerConfig {
    pub announce_to_all_trackers: Option<bool>,
    pub announce_to_all_tiers: Option<bool>,
    pub prefer_udp_trackers: Option<bool>,
    pub tracker_backoff: Option<i64>,
    pub tracker_maximum_response_length: Option<i64>,
    pub min_announce_interval: Option<i64>,
    pub udp_tracker_token_expiry: Option<i64>,
}

impl WriteJson for TrackerConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_bool!(map, self, announce_to_all_trackers);
        json_field_bool!(map, self, announce_to_all_tiers);
        json_field_bool!(map, self, prefer_udp_trackers);
        json_field_int!(map, self, tracker_backoff);
        json_field_int!(map, self, tracker_maximum_response_length);
        json_field_int!(map, self, min_announce_interval);
        json_field_int!(map, self, udp_tracker_token_expiry);
    }
}

// ============================================================
// Algorithms
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AlgorithmsConfig {
    pub choking_algorithm: Option<i64>,
    pub seed_choking_algorithm: Option<i64>,
    pub mixed_mode_algorithm: Option<i64>,
    pub suggest_mode: Option<i64>,
}

impl WriteJson for AlgorithmsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, choking_algorithm);
        json_field_int!(map, self, seed_choking_algorithm);
        json_field_int!(map, self, mixed_mode_algorithm);
        json_field_int!(map, self, suggest_mode);
    }
}

// ============================================================
// Active Limits
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ActiveLimitsConfig {
    pub active_downloads: Option<i64>,
    pub active_seeds: Option<i64>,
    pub active_checking: Option<i64>,
    pub active_limit: Option<i64>,
    pub active_tracker_limit: Option<i64>,
    pub active_lsd_limit: Option<i64>,
    pub active_dht_limit: Option<i64>,
}

impl WriteJson for ActiveLimitsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, active_downloads);
        json_field_int!(map, self, active_seeds);
        json_field_int!(map, self, active_checking);
        json_field_int!(map, self, active_limit);
        json_field_int!(map, self, active_tracker_limit);
        json_field_int!(map, self, active_lsd_limit);
        json_field_int!(map, self, active_dht_limit);
    }
}

// ============================================================
// Auto Manage
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AutoManageConfig {
    pub auto_manage_interval: Option<i64>,
    pub auto_manage_startup: Option<i64>,
    pub auto_manage_prefer_seeds: Option<bool>,
    pub dont_count_slow_torrents: Option<bool>,
    pub share_ratio_limit: Option<f64>,
    pub seed_time_ratio_limit: Option<f64>,
    pub seed_time_limit: Option<i64>,
}

impl WriteJson for AutoManageConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, auto_manage_interval);
        json_field_int!(map, self, auto_manage_startup);
        json_field_bool!(map, self, auto_manage_prefer_seeds);
        json_field_bool!(map, self, dont_count_slow_torrents);
        if let Some(val) = self.share_ratio_limit {
            map.insert(
                "share_ratio_limit".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }
        if let Some(val) = self.seed_time_ratio_limit {
            map.insert(
                "seed_time_ratio_limit".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }
        json_field_int!(map, self, seed_time_limit);
    }
}

// ============================================================
// Encryption
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct EncryptionConfig {
    pub encryption_policy: Option<i64>,
    pub allowed_encryption_level: Option<i64>,
    pub ssl_listen: Option<i64>,
}

impl WriteJson for EncryptionConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, encryption_policy);
        json_field_int!(map, self, allowed_encryption_level);
        json_field_int!(map, self, ssl_listen);
    }
}

// ============================================================
// Proxy
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProxyConfig {
    pub host: Option<String>,
    pub port: Option<i64>,
    #[serde(rename = "type")]
    pub proxy_type: Option<String>,
    pub proxy_hostnames: Option<bool>,
    pub proxy_peer_connections: Option<bool>,
    pub proxy_tracker_connections: Option<bool>,
    pub anonymous_mode: Option<bool>,
    pub force_proxy: Option<bool>,
}

impl WriteJson for ProxyConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_str!(map, self, host);
        json_field_int!(map, self, port);
        if let Some(ref val) = self.proxy_type {
            if !val.is_empty() {
                map.insert(
                    "proxy_type".to_string(),
                    serde_json::Value::String(val.clone()),
                );
            }
        }
        json_field_bool!(map, self, proxy_hostnames);
        json_field_bool!(map, self, proxy_peer_connections);
        json_field_bool!(map, self, proxy_tracker_connections);
        json_field_bool!(map, self, anonymous_mode);
        json_field_bool!(map, self, force_proxy);
    }
}

// ============================================================
// User Agent
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UserAgentConfig {
    pub user_agent: Option<String>,
    pub peer_fingerprint: Option<String>,
    pub always_send_user_agent: Option<bool>,
}

impl WriteJson for UserAgentConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_str!(map, self, user_agent);
        json_field_str!(map, self, peer_fingerprint);
        json_field_bool!(map, self, always_send_user_agent);
    }
}

// ============================================================
// Alert
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AlertConfig {
    pub alert_mask: Option<i64>,
    pub alert_queue_size: Option<i64>,
}

impl WriteJson for AlertConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, alert_mask);
        json_field_int!(map, self, alert_queue_size);
    }
}

// ============================================================
// Performance
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PerformanceConfig {
    pub aio_threads: Option<i64>,
    pub network_threads: Option<i64>,
    pub checking_mem_usage: Option<i64>,
    pub tick_interval: Option<i64>,
    pub send_buffer_watermark: Option<i64>,
    pub send_buffer_watermark_factor: Option<i64>,
    pub send_buffer_low_watermark: Option<i64>,
    pub recv_socket_buffer_size: Option<i64>,
    pub send_socket_buffer_size: Option<i64>,
    pub optimistic_disk_retry: Option<i64>,
}

impl WriteJson for PerformanceConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, aio_threads);
        json_field_int!(map, self, network_threads);
        json_field_int!(map, self, checking_mem_usage);
        json_field_int!(map, self, tick_interval);
        json_field_int!(map, self, send_buffer_watermark);
        json_field_int!(map, self, send_buffer_watermark_factor);
        json_field_int!(map, self, send_buffer_low_watermark);
        json_field_int!(map, self, recv_socket_buffer_size);
        json_field_int!(map, self, send_socket_buffer_size);
        json_field_int!(map, self, optimistic_disk_retry);
    }
}

// ============================================================
// Misc
// ============================================================
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MiscConfig {
    pub ignore_resume_timestamps: Option<bool>,
    pub no_recheck_incomplete_resume: Option<bool>,
    pub disable_hash_checks: Option<bool>,
    pub allow_i2p_mixed: Option<bool>,
    pub incoming_starts_queued: Option<bool>,
    pub ban_web_seeds: Option<bool>,
    pub report_web_seed_downloads: Option<bool>,
    pub num_optimistic_unchoke_slots: Option<i64>,
    pub max_failcount: Option<i64>,
    pub max_rejects: Option<i64>,
    pub share_mode_target: Option<i64>,
    pub apply_ip_filter_to_trackers: Option<bool>,
    pub announce_double_nat: Option<bool>,
    pub lock_files: Option<bool>,
    pub local_service_announce_interval: Option<i64>,
    pub read_job_every: Option<i64>,
    pub strict_super_seeding: Option<bool>,
    pub enable_os_cache: Option<bool>,
}

impl WriteJson for MiscConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_bool!(map, self, ignore_resume_timestamps);
        json_field_bool!(map, self, no_recheck_incomplete_resume);
        json_field_bool!(map, self, disable_hash_checks);
        json_field_bool!(map, self, allow_i2p_mixed);
        json_field_bool!(map, self, incoming_starts_queued);
        json_field_bool!(map, self, ban_web_seeds);
        json_field_bool!(map, self, report_web_seed_downloads);
        json_field_int!(map, self, num_optimistic_unchoke_slots);
        json_field_int!(map, self, max_failcount);
        json_field_int!(map, self, max_rejects);
        json_field_int!(map, self, share_mode_target);
        json_field_bool!(map, self, apply_ip_filter_to_trackers);
        json_field_bool!(map, self, announce_double_nat);
        json_field_bool!(map, self, lock_files);
        json_field_int!(map, self, local_service_announce_interval);
        json_field_int!(map, self, read_job_every);
        json_field_bool!(map, self, strict_super_seeding);
        json_field_bool!(map, self, enable_os_cache);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn test_default_config_is_empty_json() {
        let config = TorrentfsConfig::default();
        let json = config.to_settings_json();
        assert_eq!(json, "{}");
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml_str = r#"
[connections]
listen_interfaces = "0.0.0.0:6881"

[dht]
enabled = true
"#;
        let config: TorrentfsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.connections.listen_interfaces,
            Some("0.0.0.0:6881".to_string())
        );
        assert_eq!(config.dht.enabled, Some(true));
        // Unspecified fields should be None
        assert_eq!(config.connections.max_connections, None);
    }

    #[test]
    fn test_settings_json_with_values() {
        let toml_str = r#"
[connections]
listen_interfaces = "0.0.0.0:6881"
max_connections = 200

[dht]
enabled = true
"#;
        let config: TorrentfsConfig = toml::from_str(toml_str).unwrap();
        let json = config.to_settings_json();
        assert!(json.contains("listen_interfaces"));
        assert!(json.contains("0.0.0.0:6881"));
        assert!(json.contains("max_connections"));
        assert!(json.contains("200"));
        assert!(json.contains("enable_dht"));
        assert!(json.contains("true"));
    }

    #[test]
    fn test_config_from_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"
[connections]
listen_interfaces = "0.0.0.0:6881"
max_connections = 200

[timeouts]
read_timeout_secs = 60

[local_discovery]
lsd_enabled = false
"#,
        )
        .unwrap();

        let config = TorrentfsConfig::from_file(&config_path).unwrap();
        assert_eq!(
            config.connections.listen_interfaces,
            Some("0.0.0.0:6881".to_string())
        );
        assert_eq!(config.connections.max_connections, Some(200));
        assert_eq!(config.timeouts.read_timeout_secs, Some(60));
        assert_eq!(config.local_discovery.lsd_enabled, Some(false));

        // Verify JSON output includes the settings
        let json = config.to_settings_json();
        assert!(json.contains("listen_interfaces"));
        assert!(json.contains("max_connections"));
        assert!(json.contains("enable_lsd")); // false → still written as key
    }

    #[test]
    fn test_config_from_file_nonexistent() {
        let result = TorrentfsConfig::from_file(std::path::Path::new("/nonexistent/config.toml"));
        assert!(result.is_err());
        match result {
            Err(TorrentError::ParseError(_)) => {} // ParseError wraps IO errors for config
            Err(e) => panic!("Expected ParseError, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[test]
    fn test_config_from_file_invalid_toml() {
        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("invalid.toml");
        std::fs::write(&config_path, "this is not valid toml {{{").unwrap();

        let result = TorrentfsConfig::from_file(&config_path);
        assert!(result.is_err());
        match result {
            Err(TorrentError::ParseError(_)) => {}
            Err(e) => panic!("Expected ParseError, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[test]
    fn test_read_timeout_config() {
        // Default: read_timeout_secs not set → defaults to 30
        let default_config = TorrentfsConfig::default_config();
        let timeout = default_config
            .timeouts
            .read_timeout_secs
            .map(|v| if v > 0 { v as u64 } else { 30 })
            .unwrap_or(30);
        assert_eq!(timeout, 30);

        // Custom timeout
        let toml_str = r#"
[timeouts]
read_timeout_secs = 10
"#;
        let config: TorrentfsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.timeouts.read_timeout_secs, Some(10));
    }
}
