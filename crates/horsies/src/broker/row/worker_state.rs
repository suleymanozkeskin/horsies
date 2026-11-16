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

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the struct can be constructed with all fields.
    #[test]
    fn worker_state_row_field_types() {
        let now = Utc::now();
        let row = WorkerStateRow {
            id: 1,
            worker_id: "worker-abc-123".to_owned(),
            snapshot_at: now,
            hostname: "host1.local".to_owned(),
            pid: 42,
            processes: 4,
            max_claim_batch: 2,
            max_claim_per_worker: 8,
            cluster_wide_cap: Some(100),
            queues: vec!["default".to_owned(), "high".to_owned()],
            queue_priorities: Some(serde_json::json!({"default": 1, "high": 10})),
            queue_max_concurrency: Some(serde_json::json!({"default": 5})),
            recovery_config: Some(serde_json::json!({})),
            tasks_running: 2,
            tasks_claimed: 3,
            memory_usage_mb: Some(128.5),
            memory_percent: Some(1.2),
            cpu_percent: Some(15.3),
            worker_started_at: now,
        };
        assert_eq!(row.worker_id, "worker-abc-123");
        assert_eq!(row.processes, 4);
        assert_eq!(row.tasks_running, 2);
        assert_eq!(row.tasks_claimed, 3);
    }

    /// Verify queues field holds a Vec<String>.
    #[test]
    fn worker_state_row_queues_vec() {
        let now = Utc::now();
        let row = WorkerStateRow {
            id: 1,
            worker_id: "w1".to_owned(),
            snapshot_at: now,
            hostname: "h1".to_owned(),
            pid: 1,
            processes: 1,
            max_claim_batch: 2,
            max_claim_per_worker: 4,
            cluster_wide_cap: None,
            queues: vec![
                "default".to_owned(),
                "priority".to_owned(),
                "bulk".to_owned(),
            ],
            queue_priorities: None,
            queue_max_concurrency: None,
            recovery_config: None,
            tasks_running: 0,
            tasks_claimed: 0,
            memory_usage_mb: None,
            memory_percent: None,
            cpu_percent: None,
            worker_started_at: now,
        };
        assert_eq!(row.queues.len(), 3);
        assert_eq!(row.queues[0], "default");
    }

    /// WorkerStateRow must implement Debug and Clone.
    #[test]
    fn worker_state_row_traits() {
        fn assert_debug_clone<T: std::fmt::Debug + Clone>() {}
        assert_debug_clone::<WorkerStateRow>();
    }
}
