//! Typed result models for the worker/database health API.
//!
//! Mirrors Python's `horsies/core/models/health.py`. These carry no behaviour,
//! only observed data returned to callers from the broker's health methods.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Shared inbound channel every worker subscribes to. A ping NOTIFY carries a
/// [`WorkerPingRequest`]; each worker replies on the request's `reply_channel`.
pub const WORKER_PING_CHANNEL: &str = "horsies_worker_ping";

/// Result of a `SELECT 1` round-trip through the live broker pool.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DatabasePing {
    /// Measured round-trip latency in milliseconds.
    pub latency_ms: f64,
}

/// Request payload broadcast on [`WORKER_PING_CHANNEL`].
///
/// `target_worker_id` of `None` is a broadcast (every worker replies);
/// otherwise only the worker whose instance id matches replies. `deny_unknown_fields`
/// mirrors Python's `extra='forbid'` so the wire contract is validated on decode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerPingRequest {
    pub correlation_id: String,
    pub reply_channel: String,
    #[serde(default)]
    pub target_worker_id: Option<String>,
}

/// Reply payload a worker sends on the request's `reply_channel`.
///
/// Carries only identity. A pong proves the worker's event loop is responsive
/// and that it can reach Postgres (the reply travels through the worker's pool).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerPongPayload {
    pub correlation_id: String,
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
}

/// A single worker's reply to a ping, with measured round-trip latency.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerPong {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub round_trip_ms: f64,
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

    #[test]
    fn ping_request_round_trip_with_optional_target() {
        let req = WorkerPingRequest {
            correlation_id: "abc".to_owned(),
            reply_channel: "horsies_worker_pong_abc".to_owned(),
            target_worker_id: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: WorkerPingRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.correlation_id, "abc");
        assert!(back.target_worker_id.is_none());
    }

    #[test]
    fn ping_request_target_defaults_when_absent() {
        // A broadcast ping omits target_worker_id on the wire.
        let req: WorkerPingRequest = serde_json::from_str(
            r#"{"correlation_id":"c","reply_channel":"r"}"#,
        )
        .unwrap();
        assert!(req.target_worker_id.is_none());
    }

    #[test]
    fn pong_payload_rejects_unknown_fields() {
        let result: Result<WorkerPongPayload, _> = serde_json::from_str(
            r#"{"correlation_id":"c","worker_id":"w","hostname":"h","pid":1,"extra":true}"#,
        );
        assert!(result.is_err(), "unknown fields must be rejected");
    }
}
