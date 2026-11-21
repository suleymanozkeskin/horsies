//! Typed result models for the worker/database health API.
//!
//! Mirrors Python's `clients/python/horsies/core/models/health.py`. These carry no behaviour,
//! only observed data returned to callers from the broker's health methods,
//! and the reading of a stored worker state row into a snapshot.

use std::collections::BTreeMap;
use std::ops::Range;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::broker::class_budgets::{parse_class_capacity, ClassBudgets};
use crate::broker::result_types::{BrokerOperationError, BrokerOperationSource, BrokerResult};
use crate::broker::slot_health::{parse_slot_health, SlotHealth};

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
/// Mirrors Python's `WorkerStateSnapshot` (`clients/python/horsies/core/models/health.py:399-433`).
/// Returned as the latest snapshot per worker (cluster-wide listing) or as a
/// timeseries (single-worker history). The worker state reads build it from
/// the stored row; a stored value of another shape fails the read.
#[derive(Debug, Clone, PartialEq)]
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
    /// Queue name to priority. Empty when the worker had none set.
    pub queue_priorities: BTreeMap<String, i64>,
    /// Queue name to the most rows of that queue in flight. Empty when the
    /// worker had none set.
    pub queue_max_concurrency: BTreeMap<String, i64>,
    /// The recovery settings and the health of the worker's maintenance
    /// passes, as the worker wrote them. Empty when the worker had none.
    pub recovery_config: Map<String, Value>,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    /// Resident memory of the worker process only, in MB.
    pub memory_usage_mb: Option<f64>,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,
    /// Summed resident memory of the worker's executor children, in MB.
    pub children_memory_mb: Option<f64>,
    pub worker_started_at: DateTime<Utc>,
    /// Whether any slot was down, and why.
    pub slot_health: SlotHealth,
    /// The claim budget of each execution class that the worker runs
    /// children for: its capacity in service and the classes it claims.
    pub class_capacity: ClassBudgets,
}

/// A `horsies_worker_states` row as stored, before its JSON columns are read.
/// [`StoredWorkerState::into_snapshot`] reads it.
#[derive(Debug, Clone)]
pub(crate) struct StoredWorkerState {
    pub(crate) worker_id: String,
    pub(crate) snapshot_at: DateTime<Utc>,
    pub(crate) hostname: String,
    pub(crate) pid: i32,
    pub(crate) processes: i32,
    pub(crate) max_claim_batch: i32,
    pub(crate) max_claim_per_worker: i32,
    pub(crate) cluster_wide_cap: Option<i32>,
    pub(crate) queues: Vec<String>,
    pub(crate) queue_priorities: Value,
    pub(crate) queue_max_concurrency: Value,
    pub(crate) recovery_config: Value,
    pub(crate) tasks_running: i32,
    pub(crate) tasks_claimed: i32,
    pub(crate) memory_usage_mb: Option<f64>,
    pub(crate) memory_percent: Option<f64>,
    pub(crate) cpu_percent: Option<f64>,
    pub(crate) children_memory_mb: Option<f64>,
    pub(crate) worker_started_at: DateTime<Utc>,
    pub(crate) slots_down: Value,
    pub(crate) class_capacity: Value,
}

/// A row of the worker list statement: a stored worker state and
/// `read_at`, the `statement_timestamp()` of that statement. Every row of
/// one list read has the same `read_at`.
#[derive(Debug, Clone)]
pub(crate) struct ListedWorkerStateRow {
    pub(crate) stored: StoredWorkerState,
    pub(crate) read_at: DateTime<Utc>,
}

/// A worker's latest snapshot and the database clock of the list statement
/// that read it. The web worker list measures the snapshot age, the
/// staleness and the uptime from `read_at`, so the web host clock does not
/// enter them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListedWorkerState {
    pub(crate) snapshot: WorkerStateSnapshot,
    pub(crate) read_at: DateTime<Utc>,
}

/// A JSON column of a worker state row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerStateColumn {
    SlotsDown,
    ClassCapacity,
    QueuePriorities,
    QueueMaxConcurrency,
    RecoveryConfig,
}

impl WorkerStateColumn {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SlotsDown => "slots_down",
            Self::ClassCapacity => "class_capacity",
            Self::QueuePriorities => "queue_priorities",
            Self::QueueMaxConcurrency => "queue_max_concurrency",
            Self::RecoveryConfig => "recovery_config",
        }
    }
}

/// A stored worker state row whose JSON column does not have the shape that
/// the worker writes. The message names the worker, the snapshot, the column
/// and the cause, as Python's (`clients/python/horsies/core/brokers/postgres.py:2971-2986`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "worker {worker_id} snapshot at {snapshot_at}: {} is unreadable: {reason}",
    column.as_str()
)]
pub struct WorkerStateUnreadable {
    pub(crate) worker_id: String,
    pub(crate) snapshot_at: DateTime<Utc>,
    pub(crate) column: WorkerStateColumn,
    pub(crate) reason: String,
}

impl StoredWorkerState {
    /// The snapshot of this row (Python `_row_to_worker_snapshot`,
    /// `clients/python/horsies/core/brokers/postgres.py:2961-3009`).
    ///
    /// `slots_down` and `class_capacity` are read first, in Python's order.
    /// The three map columns must be JSON objects; the queue maps hold whole
    /// numbers. Any other value is [`WorkerStateUnreadable`]. Reads only.
    pub(crate) fn into_snapshot(self) -> Result<WorkerStateSnapshot, WorkerStateUnreadable> {
        let slot_health = parse_slot_health(&self.slots_down, self.processes)
            .map_err(|cause| self.unreadable(WorkerStateColumn::SlotsDown, cause.reason))?;
        let class_capacity = parse_class_capacity(&self.class_capacity)
            .map_err(|cause| self.unreadable(WorkerStateColumn::ClassCapacity, cause.reason))?;
        let queue_priorities = queue_numbers(&self.queue_priorities)
            .map_err(|reason| self.unreadable(WorkerStateColumn::QueuePriorities, reason))?;
        let queue_max_concurrency = queue_numbers(&self.queue_max_concurrency)
            .map_err(|reason| self.unreadable(WorkerStateColumn::QueueMaxConcurrency, reason))?;
        let recovery_config = json_object(&self.recovery_config)
            .map_err(|reason| self.unreadable(WorkerStateColumn::RecoveryConfig, reason))?;
        Ok(WorkerStateSnapshot {
            worker_id: self.worker_id,
            snapshot_at: self.snapshot_at,
            hostname: self.hostname,
            pid: self.pid,
            processes: self.processes,
            max_claim_batch: self.max_claim_batch,
            max_claim_per_worker: self.max_claim_per_worker,
            cluster_wide_cap: self.cluster_wide_cap,
            queues: self.queues,
            queue_priorities,
            queue_max_concurrency,
            recovery_config,
            tasks_running: self.tasks_running,
            tasks_claimed: self.tasks_claimed,
            memory_usage_mb: self.memory_usage_mb,
            memory_percent: self.memory_percent,
            cpu_percent: self.cpu_percent,
            children_memory_mb: self.children_memory_mb,
            worker_started_at: self.worker_started_at,
            slot_health,
            class_capacity,
        })
    }

    fn unreadable(&self, column: WorkerStateColumn, reason: String) -> WorkerStateUnreadable {
        WorkerStateUnreadable {
            worker_id: self.worker_id.clone(),
            snapshot_at: self.snapshot_at,
            column,
            reason,
        }
    }
}

fn queue_numbers(value: &Value) -> Result<BTreeMap<String, i64>, String> {
    let entries = json_object(value)?;
    entries
        .iter()
        .map(|(queue, number)| match number.as_i64() {
            Some(whole) => Ok((queue.clone(), whole)),
            None => Err(format!(
                "queue '{queue}' holds {}, not a whole number",
                stored_value_text(Some(number))
            )),
        })
        .collect()
}

fn json_object(value: &Value) -> Result<Map<String, Value>, String> {
    match value {
        Value::Object(entries) => Ok(entries.clone()),
        other => Err(format!(
            "{} is not a JSON object",
            stored_value_text(Some(other))
        )),
    }
}

/// The refusal of a worker ping on a handle with no listener (Python's
/// `PostgresBroker(assume_initialized=True)`, the core's `AssumeInitialized`
/// schema mode): `LISTENER_DISABLED`, not retryable, with Python's text
/// (`ping_workers_async`). Nothing ran.
pub fn ping_listener_disabled() -> BrokerOperationError {
    BrokerOperationError::ListenerDisabled {
        message: "ping_workers listener unavailable: this broker was built with \
                  assume_initialized=True and has no listener"
            .to_owned(),
    }
}

/// The refusal of a worker ping window of 0 seconds or less:
/// `INVALID_INPUT`, not retryable, with Python's text
/// (`ping_workers_async`). `timeout_seconds` is the given value as the
/// caller wrote it. Nothing ran.
pub fn ping_timeout_refused(timeout_seconds: &dyn std::fmt::Display) -> BrokerOperationError {
    BrokerOperationError::invalid_input(format!(
        "timeout_seconds must be positive, got {timeout_seconds}"
    ))
}

/// The refusal of a worker ping `min_responses` below 1: `INVALID_INPUT`,
/// not retryable, with Python's text (`ping_workers_async`).
/// `min_responses` is the given value as the caller wrote it. Nothing ran.
pub fn ping_min_responses_refused(min_responses: &dyn std::fmt::Display) -> BrokerOperationError {
    BrokerOperationError::invalid_input(format!(
        "min_responses must be >= 1 when set, got {min_responses}"
    ))
}

/// The worker state history limit, by Python's rule
/// (`get_worker_state_history_async`): `None` reads every retained row; a
/// limit of 0 or less is refused ([`worker_history_limit_refused`]). Pure.
/// The bridge checks the limit with this rule before any connect.
pub fn checked_history_limit(limit: Option<i64>) -> BrokerResult<Option<i64>> {
    match limit {
        Some(refused) if refused <= 0 => Err(worker_history_limit_refused(&refused)),
        other => Ok(other),
    }
}

/// The refusal of a worker state history `limit` of 0 or less:
/// `INVALID_INPUT`, not retryable, with Python's text
/// (`get_worker_state_history_async`). `limit` is the given value as the
/// caller wrote it. Nothing ran.
pub fn worker_history_limit_refused(limit: &dyn std::fmt::Display) -> BrokerOperationError {
    BrokerOperationError::invalid_input(format!("limit must be positive when set, got {limit}"))
}

/// The snapshots of stored rows, for the read `operation`. The first row that
/// does not read fails the read: `MONITORING_QUERY_FAILED`, not retryable,
/// since the same rows give the same answer (Python: the `ValueError` of
/// `_row_to_worker_snapshot` in the read's `try`,
/// `clients/python/horsies/core/brokers/postgres.py:3011-3100`). Nothing changed.
pub(crate) fn read_worker_snapshots(
    operation: &str,
    rows: Vec<StoredWorkerState>,
) -> BrokerResult<Vec<WorkerStateSnapshot>> {
    rows.into_iter()
        .map(|row| read_worker_snapshot(operation, row))
        .collect()
}

/// The snapshot of one stored row, for the read `operation`; as
/// [`read_worker_snapshots`].
pub(crate) fn read_worker_snapshot(
    operation: &str,
    row: StoredWorkerState,
) -> BrokerResult<WorkerStateSnapshot> {
    row.into_snapshot().map_err(|unreadable| {
        BrokerOperationError::monitoring_query_failed(
            operation,
            BrokerOperationSource::Snapshot(unreadable),
        )
    })
}

/// The listed states of the worker list rows, for the read `operation`.
/// Each keeps the `read_at` of its row. The first row that does not read
/// fails the read, as in [`read_worker_snapshots`].
pub(crate) fn read_listed_worker_states(
    operation: &str,
    rows: Vec<ListedWorkerStateRow>,
) -> BrokerResult<Vec<ListedWorkerState>> {
    rows.into_iter()
        .map(|row| {
            let read_at = row.read_at;
            read_worker_snapshot(operation, row.stored)
                .map(|snapshot| ListedWorkerState { snapshot, read_at })
        })
        .collect()
}

/// The exclusive bound of a stored count: it stays in the range of a
/// PostgreSQL bigint (Python `_COUNT_LIMIT = 2**63`,
/// `clients/python/horsies/core/models/health.py:134`).
pub(crate) const STORED_COUNT_LIMIT: u64 = 1 << 63;

/// A stored JSON whole number inside `valid` (Python `_whole_in`,
/// `clients/python/horsies/core/models/health.py:214-226`). A bool is not a whole number;
/// any other value outside `valid` is out of range. The error is the reason,
/// naming `name` and the stored value.
pub(crate) fn stored_whole_number(
    value: Option<&Value>,
    name: &str,
    valid: Range<u64>,
) -> Result<u64, String> {
    match value {
        Some(Value::Bool(_)) => Err(format!(
            "{name} {} is not a whole number",
            stored_value_text(value)
        )),
        Some(Value::Number(number)) => match number.as_u64() {
            Some(whole) if valid.contains(&whole) => Ok(whole),
            _ => Err(format!(
                "{name} {} is out of range",
                stored_value_text(value)
            )),
        },
        _ => Err(format!(
            "{name} {} is out of range",
            stored_value_text(value)
        )),
    }
}

/// A stored JSON value as text for a refusal reason, in the form of Python's
/// `repr` of the decoded value: `None` for a missing key or `null`, `True` or
/// `False`, a string in single quotes, any other value as JSON text.
pub(crate) fn stored_value_text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "None".to_owned(),
        Some(Value::Bool(true)) => "True".to_owned(),
        Some(Value::Bool(false)) => "False".to_owned(),
        Some(Value::String(text)) => format!("'{text}'"),
        Some(other) => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::result_types::BrokerErrorCode;

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
        let req: WorkerPingRequest =
            serde_json::from_str(r#"{"correlation_id":"c","reply_channel":"r"}"#).unwrap();
        assert!(req.target_worker_id.is_none());
    }

    #[test]
    fn pong_payload_rejects_unknown_fields() {
        let result: Result<WorkerPongPayload, _> = serde_json::from_str(
            r#"{"correlation_id":"c","worker_id":"w","hostname":"h","pid":1,"extra":true}"#,
        );
        assert!(result.is_err(), "unknown fields must be rejected");
    }

    /// Python `test_health_models.py::TestWorkerPingRequest::test_roundtrip_through_json_io`:
    /// a request with a target keeps each field through its wire text.
    #[test]
    fn ping_request_round_trip_keeps_each_field() {
        let req = WorkerPingRequest {
            correlation_id: "abc123".to_owned(),
            reply_channel: "horsies_worker_pong_abc123".to_owned(),
            target_worker_id: Some("worker-7".to_owned()),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: WorkerPingRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.correlation_id, "abc123");
        assert_eq!(back.reply_channel, "horsies_worker_pong_abc123");
        assert_eq!(back.target_worker_id.as_deref(), Some("worker-7"));
    }

    /// Python `test_health_models.py::TestWorkerPingRequest::test_rejects_extra_fields`.
    #[test]
    fn ping_request_rejects_unknown_fields() {
        let result: Result<WorkerPingRequest, _> = serde_json::from_str(
            r#"{"correlation_id":"c","reply_channel":"r","target_worker_id":null,"rogue":1}"#,
        );
        assert!(result.is_err(), "unknown fields must be rejected");
    }

    /// Python `test_health_models.py::TestWorkerPingRequest::test_rejects_missing_required`.
    #[test]
    fn ping_request_rejects_a_missing_reply_channel() {
        let result: Result<WorkerPingRequest, _> =
            serde_json::from_str(r#"{"correlation_id":"c"}"#);
        assert!(result.is_err(), "a request needs its reply channel");
    }

    /// Python `test_health_models.py::TestWorkerPongPayload::test_roundtrip_through_json_io`.
    #[test]
    fn pong_payload_round_trip_keeps_each_field() {
        let pong = WorkerPongPayload {
            correlation_id: "abc".to_owned(),
            worker_id: "w1".to_owned(),
            hostname: "host".to_owned(),
            pid: 999,
        };
        let json = serde_json::to_string(&pong).unwrap();
        let back: WorkerPongPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back.correlation_id, "abc");
        assert_eq!(back.worker_id, "w1");
        assert_eq!(back.hostname, "host");
        assert_eq!(back.pid, 999);
    }

    /// Python `test_health_models.py::TestWorkerPongPayload::test_rejects_non_integer_pid`.
    #[test]
    fn pong_payload_rejects_a_pid_that_is_not_an_integer() {
        let result: Result<WorkerPongPayload, _> = serde_json::from_str(
            r#"{"correlation_id":"c","worker_id":"w","hostname":"h","pid":"not-a-pid"}"#,
        );
        assert!(result.is_err(), "a pid must be an integer");
    }

    /// Python `test_health_models.py::TestResultModels::test_channel_constant`:
    /// the shared ping channel name is stable.
    #[test]
    fn the_ping_channel_name_is_stable() {
        assert_eq!(WORKER_PING_CHANNEL, "horsies_worker_ping");
    }

    fn stored_row() -> StoredWorkerState {
        let at = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 11, 16, 12, 0, 0).unwrap();
        StoredWorkerState {
            worker_id: "w1".to_owned(),
            snapshot_at: at,
            hostname: "host".to_owned(),
            pid: 4242,
            processes: 2,
            max_claim_batch: 2,
            max_claim_per_worker: 2,
            cluster_wide_cap: None,
            queues: vec!["default".to_owned()],
            queue_priorities: serde_json::json!({"default": 1}),
            queue_max_concurrency: serde_json::json!({}),
            recovery_config: serde_json::json!({"phase2_recovery": {"state": "ok"}}),
            tasks_running: 0,
            tasks_claimed: 0,
            memory_usage_mb: Some(10.5),
            memory_percent: None,
            cpu_percent: None,
            children_memory_mb: Some(64.0),
            worker_started_at: at,
            slots_down: serde_json::json!([]),
            class_capacity: serde_json::json!({
                "ISOLATED": {"capacity": 2, "serves": ["ISOLATED"]},
            }),
        }
    }

    /// Python `test_worker_health_api.py::test_worker_state_reads_the_slot_health`
    /// and `::test_worker_state_reads_the_class_capacity`, without the database.
    #[test]
    fn a_stored_row_reads_into_a_snapshot() {
        let snapshot = stored_row().into_snapshot().expect("readable row");

        assert_eq!(snapshot.slot_health, SlotHealth::AllSlotsServing);
        let budgets = snapshot.class_capacity.as_slice();
        assert_eq!(budgets.len(), 1);
        assert_eq!(budgets[0].capacity, 2);
        assert_eq!(snapshot.children_memory_mb, Some(64.0));
        assert_eq!(snapshot.queue_priorities.get("default"), Some(&1));
        assert!(snapshot.queue_max_concurrency.is_empty());
        assert!(snapshot.recovery_config.contains_key("phase2_recovery"));
    }

    /// Python `test_worker_health_api.py::test_an_unreadable_slot_health_fails_the_read`:
    /// the failure names the worker and the column.
    #[test]
    fn an_unreadable_slots_down_names_the_worker_and_the_column() {
        let mut row = stored_row();
        row.slots_down = serde_json::json!({"slot": 1});

        let unreadable = row.into_snapshot().expect_err("unreadable slots_down");

        assert_eq!(unreadable.column, WorkerStateColumn::SlotsDown);
        let message = unreadable.to_string();
        assert!(message.starts_with("worker w1 snapshot at "), "{message}");
        assert!(
            message.ends_with("slots_down is unreadable: slots_down is not a JSON array"),
            "{message}"
        );
    }

    /// Python `test_worker_health_api.py::test_an_unreadable_class_capacity_fails_the_read`.
    #[test]
    fn an_unreadable_class_capacity_names_the_budget() {
        let mut row = stored_row();
        row.class_capacity =
            serde_json::json!({"EXCLUSIVE": {"capacity": 1, "serves": ["EXCLUSIVE"]}});

        let unreadable = row.into_snapshot().expect_err("unreadable class_capacity");

        assert_eq!(unreadable.column, WorkerStateColumn::ClassCapacity);
        assert!(unreadable.to_string().contains("'EXCLUSIVE'"));
    }

    /// The maps are NOT NULL objects; a stored value of another shape fails
    /// the read instead of reading as empty.
    #[test]
    fn a_map_column_of_another_shape_is_unreadable() {
        let mut null_map = stored_row();
        null_map.queue_priorities = serde_json::Value::Null;
        let mut text_number = stored_row();
        text_number.queue_max_concurrency = serde_json::json!({"default": "5"});
        let mut list_config = stored_row();
        list_config.recovery_config = serde_json::json!([]);

        for (row, column) in [
            (null_map, WorkerStateColumn::QueuePriorities),
            (text_number, WorkerStateColumn::QueueMaxConcurrency),
            (list_config, WorkerStateColumn::RecoveryConfig),
        ] {
            assert_eq!(row.into_snapshot().expect_err("unreadable").column, column);
        }
    }

    /// An unreadable row fails the read with `MONITORING_QUERY_FAILED`, not
    /// retryable (Python: a `ValueError` is not a connection error).
    #[test]
    fn an_unreadable_row_fails_the_read_and_is_not_retryable() {
        let mut row = stored_row();
        row.slots_down = serde_json::json!("down");

        let failed = read_worker_snapshots("list_worker_states", vec![stored_row(), row])
            .expect_err("the second row is unreadable");

        assert_eq!(failed.code(), BrokerErrorCode::MonitoringQueryFailed);
        assert!(!failed.retryable());
        assert!(failed
            .message()
            .starts_with("list_worker_states failed: worker w1 snapshot at "));
    }

    /// Each listed state keeps the `read_at` of its row; an unreadable row
    /// fails the list read as it fails the other worker state reads.
    #[test]
    fn listed_states_keep_the_read_time_of_their_rows() {
        let read_at = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 11, 16, 12, 0, 30).unwrap();

        let listed = read_listed_worker_states(
            "list_worker_states",
            vec![ListedWorkerStateRow {
                stored: stored_row(),
                read_at,
            }],
        )
        .expect("a readable row");

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].read_at, read_at);
        assert_eq!(listed[0].snapshot.worker_id, "w1");

        let mut unreadable = stored_row();
        unreadable.slots_down = serde_json::json!("down");
        let failed = read_listed_worker_states(
            "list_worker_states",
            vec![ListedWorkerStateRow {
                stored: unreadable,
                read_at,
            }],
        )
        .expect_err("an unreadable row");

        assert_eq!(failed.code(), BrokerErrorCode::MonitoringQueryFailed);
        assert!(!failed.retryable());
    }

    #[test]
    fn the_ping_refusals_carry_python_text() {
        let timeout = ping_timeout_refused(&"-1.5");
        assert_eq!(timeout.code(), BrokerErrorCode::InvalidInput);
        assert_eq!(
            timeout.message(),
            "timeout_seconds must be positive, got -1.5"
        );
        assert!(!timeout.retryable());

        let minimum = ping_min_responses_refused(&0);
        assert_eq!(minimum.code(), BrokerErrorCode::InvalidInput);
        assert_eq!(
            minimum.message(),
            "min_responses must be >= 1 when set, got 0"
        );
        assert!(!minimum.retryable());
    }

    #[test]
    fn a_history_limit_above_zero_or_none_passes_and_zero_or_less_is_refused() {
        assert_eq!(checked_history_limit(None).ok(), Some(None));
        assert_eq!(checked_history_limit(Some(1)).ok(), Some(Some(1)));
        for refused in [0, -1, i64::MIN] {
            let error = checked_history_limit(Some(refused)).expect_err("0 or less is refused");
            assert_eq!(error.code(), BrokerErrorCode::InvalidInput);
            assert_eq!(
                error.message(),
                format!("limit must be positive when set, got {refused}")
            );
        }
    }

    #[test]
    fn a_history_limit_of_zero_or_less_is_refused_with_python_text() {
        let refused = worker_history_limit_refused(&-(1_i128 << 70));

        assert_eq!(refused.code(), BrokerErrorCode::InvalidInput);
        assert_eq!(
            refused.message(),
            format!("limit must be positive when set, got {}", -(1_i128 << 70))
        );
        assert!(!refused.retryable());
    }
}
