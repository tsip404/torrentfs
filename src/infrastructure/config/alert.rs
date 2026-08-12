use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Alert
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertConfig {
    pub alert_mask: Option<i64>,
    pub alert_queue_size: Option<i64>,
    /// Polling interval in milliseconds for the background alert consumer thread.
    /// Defaults to 50ms. Setting to 0 disables the background consumer.
    #[serde(default = "default_poll_interval_ms")]
    pub alert_poll_interval_ms: u64,
}

fn default_poll_interval_ms() -> u64 {
    50
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            alert_mask: None,
            alert_queue_size: None,
            alert_poll_interval_ms: 50,
        }
    }
}

impl WriteJson for AlertConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, alert_mask);
        json_field_int!(map, self, alert_queue_size);
    }
}
