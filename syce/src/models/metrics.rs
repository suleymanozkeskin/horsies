use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::postgres::types::PgInterval;
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ClusterCapacitySummary {
    pub active_workers: i64,
    pub total_capacity: Option<i64>,
    pub total_running: Option<i64>,
    pub total_claimed: Option<i64>,
    pub cluster_utilization_pct: Option<f64>,
    pub avg_memory_pct: Option<f64>,
    pub avg_cpu_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TaskStatusRow {
    pub status: String,
    pub count: i64,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ClusterUtilizationPoint {
    pub minute: DateTime<Utc>,
    pub avg_utilization_pct: Option<f64>,
    pub total_running: Option<i64>,
    pub total_capacity: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OverloadedWorkerAlert {
    pub worker_id: String,
    pub hostname: String,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,
    pub tasks_running: i32,
    pub processes: i32,
    pub snapshot_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct StaleClaimsAlert {
    pub worker_id: String,
    pub hostname: String,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub claim_ratio: Option<f64>,
    pub snapshot_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ActiveWorkerRow {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub processes: i32,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,
    pub snapshot_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct WorkerUptimeRow {
    pub worker_id: String,
    pub hostname: String,
    pub worker_started_at: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub uptime: PgInterval,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WorkerQueuesRow {
    pub worker_id: String,
    pub hostname: String,
    pub queues: Vec<String>,
    pub queue_priorities: Option<JsonValue>,
    pub queue_max_concurrency: Option<JsonValue>,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WorkerLoadPoint {
    pub worker_id: String,
    pub snapshot_at: DateTime<Utc>,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,
}

#[derive(Debug, Clone, FromRow)]
pub struct DeadWorkerRow {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub last_seen: DateTime<Utc>,
    pub offline_duration: PgInterval,
    pub tasks_at_death: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AggregatedBreakdownRow {
    pub worker_id: String,
    pub total_count: i64,
    pub pending_count: i64,
    pub claimed_count: i64,
    pub running_count: i64,
    pub completed_count: i64,
    pub failed_count: i64,
    pub claimed_task_ids: Option<Vec<String>>,
    pub running_task_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct SnapshotAgeBucket {
    pub age_bucket: String,
    pub snapshot_count: i64,
    pub unique_workers: i64,
}
