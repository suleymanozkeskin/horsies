use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

/// Worker state snapshot from the database
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WorkerState {
    pub worker_id: String,
    pub snapshot_at: DateTime<Utc>,
    pub hostname: String,
    pub pid: i32,

    // Configuration
    pub processes: i32,
    pub max_claim_batch: i32,
    pub max_claim_per_worker: i32,
    pub cluster_wide_cap: Option<i32>,
    pub queues: Vec<String>,
    pub queue_priorities: Option<JsonValue>,
    pub queue_max_concurrency: Option<JsonValue>,
    pub recovery_config: Option<JsonValue>,

    // Current load
    pub tasks_running: i32,
    pub tasks_claimed: i32,

    // System metrics
    pub memory_usage_mb: Option<f64>,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,

    // Lifecycle
    pub worker_started_at: DateTime<Utc>,
}

impl WorkerState {
    /// Calculate worker utilization percentage
    pub fn utilization(&self) -> f64 {
        if self.processes == 0 {
            return 0.0;
        }
        (self.tasks_running as f64 / self.processes as f64) * 100.0
    }

    /// Check if worker is considered alive (seen in last 2 minutes)
    pub fn is_alive(&self) -> bool {
        let now = Utc::now();
        let age = now.signed_duration_since(self.snapshot_at);
        age.num_seconds() < 120 // 2 minutes
    }

    /// Get uptime duration
    pub fn uptime(&self) -> chrono::Duration {
        self.snapshot_at
            .signed_duration_since(self.worker_started_at)
    }

    /// Format uptime as human-readable string
    pub fn uptime_string(&self) -> String {
        let duration = self.uptime();
        let hours = duration.num_hours();
        let minutes = duration.num_minutes() % 60;
        let seconds = duration.num_seconds() % 60;

        if hours > 0 {
            format!("{}h {}m {}s", hours, minutes, seconds)
        } else if minutes > 0 {
            format!("{}m {}s", minutes, seconds)
        } else {
            format!("{}s", seconds)
        }
    }
}
