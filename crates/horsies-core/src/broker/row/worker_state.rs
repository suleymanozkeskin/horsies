use chrono::{DateTime, Utc};
use sqlx::FromRow;

/// Row from the `horsies_worker_states` table.
///
/// Tracks worker snapshots as a timeseries for monitoring and cluster management.
/// Each row represents a point-in-time snapshot (inserted every ~5 seconds).
///
/// Python equivalent: `WorkerStateModel` in `horsies/core/models/task_pg.py`
#[derive(Debug, Clone, FromRow)]
pub struct WorkerStateRow {
    /// BIGINT since migration 0020 (`horsies_worker_states.id`).
    pub id: i64,
    pub worker_id: String,
    pub snapshot_at: DateTime<Utc>,
    pub hostname: String,
    pub pid: i32,
    pub processes: i32,
    pub max_claim_batch: i32,
    pub max_claim_per_worker: i32,
    pub cluster_wide_cap: Option<i32>,
    pub queues: Vec<String>,
    pub queue_priorities: Option<serde_json::Value>,
    pub queue_max_concurrency: Option<serde_json::Value>,
    pub recovery_config: Option<serde_json::Value>,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub memory_usage_mb: Option<f64>,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,
    pub worker_started_at: DateTime<Utc>,
}
