//! Typed result models for the worker/database health API.
//!
//! Mirrors Python's `horsies/core/models/health.py`. These carry no behaviour,
//! only observed data returned to callers from the broker's health methods.

use chrono::{DateTime, Utc};
use sqlx::FromRow;

/// Result of a `SELECT 1` round-trip through the live broker pool.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DatabasePing {
    /// Measured round-trip latency in milliseconds.
    pub latency_ms: f64,
}

/// One `horsies_worker_states` row — a worker's state at a point in time.
///
/// Mirrors the timeseries `WorkerStateRow` minus its internal `id`. Returned as
/// the latest snapshot per worker (cluster-wide listing) or as a timeseries
/// (single-worker history).
#[derive(Debug, Clone, FromRow)]
pub struct WorkerStateSnapshot {
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

    #[test]
    fn database_ping_holds_latency() {
        let ping = DatabasePing { latency_ms: 1.5 };
        assert_eq!(ping.latency_ms, 1.5);
    }
}
