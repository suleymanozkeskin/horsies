use std::time::Duration;

use chrono::{DateTime, Timelike, Utc};
use tokio::time::Instant;
use uuid::Uuid;

use crate::core::history::archive::results::decode_result_envelope_text;
use crate::core::history::enqueue::{
    parse_optional_object, prepare_enqueue_facts, EnqueuePreparationError, PreparedEnqueueFacts,
    SendPart,
};
use crate::core::history::identity::keys::{
    reservation_window_micros, IDEMPOTENCY_WINDOW_DEFAULT_HOURS, IDEMPOTENCY_WINDOW_DEFAULT_MICROS,
};
use crate::core::history::reads::detail::{HistoryTaskDetail, TaskAbsenceReason, TaskDetailResult};
use crate::core::history::reads::result::{
    HistoryResultPayload, HistoryTaskResult, TaskResultRead,
};
use crate::core::history::rerun::operations::{
    rerun_first_statement, rerun_task_in_transaction, RegisteredTaskCodes, RerunCommand,
    RerunEnqueuePolicy, RerunError, RerunOutcome,
};
use crate::core::{EnqueueRetention, PostgresConfig, ResultSlot, TaskInfo};

use crate::broker::core_rows::{
    column, expired_list_row, listed_worker_state_row, one_row, read_rows, scalar, stale_list_row,
    stored_worker_state, task_attempt_row, task_info_row, RowDecodeError, StatementLog,
};
use crate::broker::enqueue_task::{
    decode_enqueue_outcome, enqueue_result, EnqueueKey, EnqueueStart, EnqueueTaskOutcome, SendKind,
    ENQUEUE_TASK,
};
use crate::broker::error::{BrokerError, EnqueueRefusal};
use crate::broker::health::{
    checked_history_limit, checked_history_rows, checked_ping_wait, history_fetch_limit,
    ping_min_responses_refused, read_listed_worker_states, read_worker_snapshot,
    read_worker_snapshots, DatabasePing, ListedWorkerState, WorkerPingRequest, WorkerPong,
    WorkerPongPayload, WorkerStateSnapshot, WORKER_PING_CHANNEL,
};
use crate::broker::history_reads::{read_history_detail, read_task_result, ResultReaderSeen};
use crate::broker::result_types::{
    BrokerCheckCause, BrokerOperationSource, DatabaseOperationError, DatabaseResult, RawResultRead,
    RawResultRecord, StoredEnvelopeText, TaskInfoRead, WorkerPingStep,
};
use crate::broker::row::task::{ExpiredTaskRow, StaleTaskRow, TaskAttemptRow};
use crate::broker::workflow_claim_settle::{settle_claimed_workflow_tasks, ClaimDisposition};

use crate::broker::client_family::{
    admit_family, recorded_family_from, ClientFamily, FamilyCheckError, FamilyCheckFailure,
};
use crate::broker::partition_maintenance::default_install_coverage;
use crate::broker::process_listener::{
    default_payload_capacity, fixed_channel, ListenError, ProcessListener, SessionCheckFailure,
    TASK_DONE_CHANNEL, WORKER_PONG_CHANNEL_PREFIX,
};
use crate::broker::task_lists::{
    bounded_list_from_rows, BoundedTaskList, ListShapeError, StaleMinutes,
};
use crate::broker::wait_limit::WaitLimit;
use crate::core::history::maintenance::coverage::CoverageRequest;
#[cfg(test)]
use crate::db::DEFAULT_SOCKET_SETTINGS;
use crate::db::{PayloadEvent, PayloadFilter, WakeEvent, WakeSubscription};

use crate::broker::pools::{
    CloseOutcome, CoreHandle, OpenPools, PoolProfile, SessionPool, WriteInFlight,
};
use crate::db::{
    BoundStatement, CheckoutClass, IsolationLevel, NotSentCause, ParameterType, Row,
    StatementBound, StatementFailure, TypedStatement,
};
use crate::telemetry::trace_carrier::{SendTrace, StoredTrace};

/// Refuse a sent task input that breaks the rules of a user value (Python `.send()` refuses it as
/// `VALIDATION_FAILED`, `core/task_decorator.py:2378-2387`). An object input is Python's keyword input: no
/// member name is an engine key, and each member value has no engine key at any depth and at most
/// `MAX_VALUE_JSON_DEPTH` levels. Any other input except `null` is one value under the same rules. A text that
/// is not strict JSON is refused. The members are checked from their texts, so an integer of any width that
/// the strict rule takes is checked as it is sent.
fn check_sent_input(input: &str) -> Result<(), BrokerError> {
    crate::core::codec::check_json_text(input).map_err(|refused| {
        let error = crate::core::codec::JsonReadError::Refused(refused);
        BrokerError::EnqueueContract(EnqueueRefusal::unreadable_json(
            SendPart::Input,
            format!("task input refused: {error}"),
            &error,
        ))
    })?;
    let no_engine_members: std::collections::HashSet<String> = std::collections::HashSet::new();
    let checked = crate::core::codec::user_value::check_input_text(input, &no_engine_members);
    checked.map_err(|refusal| {
        BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
            SendPart::Input,
            format!("task input refused: {refusal}"),
        ))
    })
}

#[cfg(test)]
mod sent_input_tests {
    use super::check_sent_input;
    use crate::broker::BrokerError;

    /// Python `.send()` refuses an engine key in the input as `VALIDATION_FAILED`
    /// (`core/task_decorator.py:2378-2387`).
    #[test]
    fn a_sent_input_with_an_engine_key_is_refused() {
        for input in [
            r#"{"x": {"__h_task_result__": true}}"#,
            r#"{"__h_workflow_ctx__": {}}"#,
            r#"[{"a": [{"__builtin_task_code__": "X"}]}]"#,
            r#"{"x": NaN}"#,
        ] {
            assert!(
                matches!(
                    check_sent_input(input),
                    Err(BrokerError::EnqueueContract(_))
                ),
                "{input}"
            );
        }
        assert!(check_sent_input(r#"{"x": {"y": [1, 2]}}"#).is_ok());
        assert!(check_sent_input("[1]").is_ok());
        assert!(check_sent_input("null").is_ok());
        let wide = format!(
            r#"{{"max": 340282366920938463463374607431768211455, "below": -9223372036854775809, "long": {}}}"#,
            "9".repeat(400)
        );
        assert!(
            check_sent_input(&wide).is_ok(),
            "an integer of any width the strict rule takes"
        );
    }
}

#[cfg(test)]
mod checked_send_tests {
    use super::*;
    use crate::broker::error::EnqueueRefusalCause;

    const TASK_ID: Uuid = Uuid::from_u128(7);

    fn checked(idempotency_key: Option<&str>) -> Result<CheckedSend<'static>, BrokerError> {
        CheckedSend::check(
            "send.task",
            "{}",
            "default",
            100,
            Some(Utc::now()),
            None,
            None,
            None,
            Some(TASK_ID),
            None,
            idempotency_key,
            EnqueueRetention::Forever,
            false,
        )
    }

    fn checked_with_priority(priority: i32) -> Result<CheckedSend<'static>, BrokerError> {
        CheckedSend::check(
            "send.task",
            "{}",
            "default",
            priority,
            Some(Utc::now()),
            None,
            None,
            None,
            Some(TASK_ID),
            None,
            None,
            EnqueueRetention::Forever,
            false,
        )
    }

    /// An empty key is refused before any I/O: the send answers `VALIDATION_FAILED`. The
    /// check needs no connection, so a caller runs it before its connect.
    #[test]
    fn an_empty_idempotency_key_is_refused_by_the_check() {
        match checked(Some("")) {
            Err(BrokerError::EnqueueContract(refusal)) => {
                assert_eq!(refusal.part, SendPart::Input);
                assert!(
                    refusal.message.contains("must be non-empty"),
                    "{}",
                    refusal.message
                );
            }
            Err(other) => panic!("an empty key gave {other}"),
            Ok(send) => panic!("an empty key passed the check: {send:?}"),
        }
    }

    /// A rule of the scoped key and a rule of the version-1 command keep the
    /// text of the rule as the cause: Python's `ScopedIdempotencyKey` and
    /// `EnqueueCommandV1` raise that text as a `ValueError`.
    #[test]
    fn a_key_rule_and_a_command_rule_keep_the_rule_text() {
        let over = "k".repeat(256);
        for (case, outcome, expected) in [
            (
                "empty key",
                checked(Some("")).map(|_send| ()),
                EnqueueRefusalCause::KeyRuleRefused("idempotency key must be non-empty".to_owned()),
            ),
            (
                "key over the bound",
                checked(Some(&over)).map(|_send| ()),
                EnqueueRefusalCause::KeyRuleRefused(
                    "idempotency key must be at most 255 UTF-8 bytes".to_owned(),
                ),
            ),
            (
                "priority 0",
                checked_with_priority(0).map(|_send| ()),
                EnqueueRefusalCause::CommandRuleRefused(
                    "priority must be between 1 and 100".to_owned(),
                ),
            ),
        ] {
            match outcome {
                Err(BrokerError::EnqueueContract(refusal)) => {
                    assert_eq!(refusal.part, SendPart::Input, "{case}");
                    assert_eq!(refusal.cause, expected, "{case}");
                }
                Err(other) => panic!("{case} gave {other}"),
                Ok(()) => panic!("{case} passed the check"),
            }
        }
    }

    /// An empty retention class key is the command's rule, with the
    /// command's text (Python `EnqueueCommandV1`).
    #[test]
    fn an_empty_retention_class_key_is_the_command_rule() {
        let refusal = EnqueueRefusal::empty_retention_class_key();
        assert_eq!(refusal.part, SendPart::Input);
        assert_eq!(
            refusal.cause,
            EnqueueRefusalCause::CommandRuleRefused(
                "retention_class_key must be non-empty".to_owned()
            )
        );
    }

    #[test]
    fn a_key_of_at_most_255_bytes_passes_and_a_longer_one_is_refused() {
        let over = "k".repeat(256);
        assert!(matches!(
            checked(Some(&over)),
            Err(BrokerError::EnqueueContract(_))
        ));
        let at_bound = "k".repeat(255);
        let send = checked(Some(&at_bound)).expect("a key of 255 bytes passes");
        assert_eq!(send.task_id(), TASK_ID);
        assert_eq!(
            checked(None).expect("an unkeyed send passes").task_id(),
            TASK_ID
        );
    }
}

#[cfg(test)]
mod raw_result_probe_tests {
    //! The probe records of the staged result reader's rows. Pure.
    use super::*;
    use crate::broker::result_types::DatabaseErrorCode;
    use crate::core::history::archive::versions::{
        archive_digest, JSON_CONTENT_TYPE, JSON_UTF8_CODEC,
    };

    const TASK_ID: Uuid = Uuid::from_u128(9);

    fn history(status: &str, payload: HistoryResultPayload) -> HistoryTaskResult {
        HistoryTaskResult {
            task_name: "result.task".to_owned(),
            status: status.to_owned(),
            failed_reason: None,
            envelope_version: 1,
            codec: JSON_UTF8_CODEC.to_owned(),
            content_type: JSON_CONTENT_TYPE.to_owned(),
            payload,
        }
    }

    fn stored(text: &str) -> HistoryResultPayload {
        HistoryResultPayload::Stored {
            payload: text.as_bytes().to_vec(),
            digest: archive_digest(text.as_bytes()).to_vec(),
        }
    }

    #[test]
    fn a_history_row_is_a_record_with_its_stored_envelope() {
        let envelope = r#"{"ok":{"value":7}}"#;
        let Ok(RawResultProbe::Record(record)) =
            raw_result_from_history_result(TASK_ID, history("COMPLETED", stored(envelope)))
        else {
            panic!("a completed history row is a record");
        };
        assert_eq!(record.task_id, TASK_ID);
        assert_eq!(record.task_name, "result.task");
        assert_eq!(record.status, crate::core::TaskStatus::Completed);
        assert_eq!(
            record.raw_result.as_ref().map(StoredEnvelopeText::as_str),
            Some(envelope)
        );
        let Ok(RawResultProbe::Record(cancelled)) = raw_result_from_history_result(
            TASK_ID,
            history("CANCELLED", HistoryResultPayload::Absent),
        ) else {
            panic!("a cancelled history row without a result is a record");
        };
        assert_eq!(cancelled.raw_result, None);
    }

    #[test]
    fn a_history_row_whose_digest_does_not_match_is_invalid_json_payload() {
        let tampered = HistoryResultPayload::Stored {
            payload: br#"{"ok":1}"#.to_vec(),
            digest: vec![0; 32],
        };
        let Err(refused) = raw_result_from_history_result(TASK_ID, history("COMPLETED", tampered))
        else {
            panic!("a digest mismatch is refused");
        };
        assert_eq!(refused.code(), DatabaseErrorCode::InvalidJsonPayload);
        assert!(!refused.retryable());
    }

    #[test]
    fn a_history_row_with_a_live_status_is_invalid_json_payload() {
        let Err(refused) = raw_result_from_history_result(
            TASK_ID,
            history("RUNNING", HistoryResultPayload::Absent),
        ) else {
            panic!("a live status in history is refused");
        };
        assert_eq!(refused.code(), DatabaseErrorCode::InvalidJsonPayload);
    }

    /// Python `test_broker_postgres.py::TestPythonRawResultRead::test_live_row_with_a_terminal_status_returns_record_with_none_payload`:
    /// a live row holds no result, so each terminal status read from it is a
    /// record with no result.
    #[test]
    fn a_live_row_with_a_terminal_status_is_a_record_with_no_result() {
        for status in [
            crate::core::TaskStatus::Completed,
            crate::core::TaskStatus::Failed,
            crate::core::TaskStatus::Expired,
            crate::core::TaskStatus::Cancelled,
        ] {
            let Ok(RawResultProbe::Record(record)) =
                raw_result_from_live(TASK_ID, "result.task".to_owned(), &status.to_string())
            else {
                panic!("a live {status} row is a record");
            };
            assert_eq!(record.task_id, TASK_ID);
            assert_eq!(record.task_name, "result.task");
            assert_eq!(record.status, status);
            assert_eq!(record.raw_result, None, "{status}");
        }
    }

    /// Python `test_broker_postgres.py::TestPythonRawResultRead::test_an_empty_stored_result_text_is_invalid_json_payload`:
    /// an empty stored result is not JSON and is not "no result".
    #[test]
    fn an_empty_stored_result_is_invalid_json_payload() {
        let Err(refused) =
            raw_result_from_history_result(TASK_ID, history("COMPLETED", stored("")))
        else {
            panic!("an empty stored result is refused");
        };
        assert_eq!(refused.code(), DatabaseErrorCode::InvalidJsonPayload);
        assert!(!refused.retryable());
    }

    #[test]
    fn a_live_row_waits_until_its_status_is_terminal() {
        assert!(matches!(
            raw_result_from_live(TASK_ID, "result.task".to_owned(), "RUNNING"),
            Ok(RawResultProbe::Waiting(_))
        ));
        assert!(matches!(
            raw_result_from_live(TASK_ID, "result.task".to_owned(), "CANCELLED"),
            Ok(RawResultProbe::Record(RawResultRecord {
                raw_result: None,
                ..
            }))
        ));
        assert!(matches!(
            raw_result_from_live(TASK_ID, "result.task".to_owned(), "NOT_A_STATUS"),
            Err(DatabaseOperationError::InvalidJsonPayload { .. })
        ));
    }
}

#[cfg(test)]
fn test_uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("test identity must be UUID")
}

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

/// What the pre-start expiry of a claimed task found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrestartExpiry {
    /// The task is EXPIRED: this call moved it, or an earlier call did.
    Expired,
    /// The deadline has not passed, or the row is not this worker's claim;
    /// nothing is written.
    NotExpired,
}

// Terminal transitions (COMPLETED/FAILED/CANCELLED/EXPIRED) run through the
// terminalization operations — broker/terminalization.rs and the functions
// installed by migration 0032.

// retry_count is derived from the row being CAS-updated (the same idiom as
// horsies_complete_task_fused's ctx CTE), not passed by the caller: the
// ownership CAS makes the claim-time snapshot provably equal to the row, so
// self-incrementing removes the snapshot from the written value entirely.
//
// The written row is the retry row of the retry writes
// (`horsies_finish_failed_attempt`, `core/migrations/1001_lifecycle_record.sql`):
// PENDING, retry_count + 1, enqueued_at = next_retry_at, every claim and run
// field cleared, and the claim count of the next attempt at 0 (attempt_claims
// 0, first_claimed_at NULL). The retry time is `$2`, or the database clock when `$2`
// is NULL (no delay): `enqueued_at` is NOT NULL in the baseline
// (`0001_tables.sql:79`). A retry time at or after `good_until` is refused.
const REQUEUE_SQL: &str = "\
WITH retry AS (
    SELECT COALESCE($2::timestamptz, now()) AS due_at
)
UPDATE horsies_tasks t
SET status = 'PENDING',
    retry_count = t.retry_count + 1,
    next_retry_at = retry.due_at,
    enqueued_at = retry.due_at,
    claimed_at = NULL,
    claimed_by_worker_id = NULL,
    claim_expires_at = NULL,
    started_at = NULL,
    worker_pid = NULL,
    worker_hostname = NULL,
    worker_process_name = NULL,
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    attempt_claims = 0,
    first_claimed_at = NULL,
    updated_at = NOW()
FROM retry
WHERE t.id = $1
  AND t.status = 'RUNNING'
  AND t.claimed_by_worker_id = $3
  AND (t.good_until IS NULL OR retry.due_at < t.good_until)
RETURNING t.id";

/// [`REQUEUE_SQL`] on the statement layer: the task id, the retry time
/// (`NULL`: the database clock), the worker id.
static REQUEUE: TypedStatement = TypedStatement::new(
    "requeue",
    REQUEUE_SQL,
    &[
        ParameterType::Uuid,
        ParameterType::Timestamptz,
        ParameterType::Text,
    ],
);

// A live row holds no result and no failed reason: the baseline keeps them
// with the terminal record in history (`horsies_tasks` in
// `core/baseline/0001_tables.sql`). The live reads below select
// `NULL` for them, as Python's live reads do (`GET_TASK_STATUS_NAME_SQL`,
// `clients/python/horsies/core/brokers/postgres.py:942-948`; `get_task_info_async`,
// `:3659-3672`).

const GET_RAW_RESULT_PROBE_SQL: &str = "\
SELECT id, task_name, status
FROM horsies_tasks
WHERE id = $1";

static GET_RAW_RESULT_PROBE: TypedStatement = TypedStatement::new(
    "get_raw_result_probe",
    GET_RAW_RESULT_PROBE_SQL,
    &[ParameterType::Uuid],
);

/// The live row of a task info read. The terminal times, the error code and
/// the failed reason are `NULL` whatever the include flags say: the flags
/// select fields of a history row only. The live table stores no result, so
/// the row has no result column.
const GET_TASK_INFO_SQL: &str = "\
SELECT id, task_name, status, queue_name, priority, retry_count, max_retries,
       next_retry_at, sent_at, enqueued_at, claimed_at, started_at,
       NULL::timestamptz AS completed_at, NULL::timestamptz AS failed_at,
       worker_hostname, worker_pid, worker_process_name, NULL::text AS error_code,
       NULL::text AS failed_reason, execution
FROM horsies_tasks
WHERE id = $1";

// ---------------------------------------------------------------------------
// Task attempt SQL
// ---------------------------------------------------------------------------

/// Query task attempts for a specific task (most recent first).
const SELECT_TASK_ATTEMPTS_SQL: &str = "\
SELECT task_id, attempt, outcome, will_retry,
       started_at, finished_at,
       error_code, error_message, failed_reason,
       worker_id, worker_hostname, worker_pid, worker_process_name
FROM horsies_task_attempts
WHERE task_id = $1
ORDER BY attempt DESC";

static SELECT_TASK_ATTEMPTS: TypedStatement = TypedStatement::new(
    "select_task_attempts",
    SELECT_TASK_ATTEMPTS_SQL,
    &[ParameterType::Uuid],
);

/// Whether the live table holds the task id `$1`.
static LIVE_TASK_EXISTS: TypedStatement = TypedStatement::new(
    "live_task_exists",
    "SELECT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = $1)",
    &[ParameterType::Uuid],
);

static GET_TASK_INFO: TypedStatement =
    TypedStatement::new("get_task_info", GET_TASK_INFO_SQL, &[ParameterType::Uuid]);

// ---------------------------------------------------------------------------
// Monitoring / observability SQL
// ---------------------------------------------------------------------------

/// Aggregate RUNNING task counts per worker for load visibility.
///
/// Groups by `(worker_hostname, worker_pid, worker_process_name)` and
/// includes the oldest task start time and most recent heartbeat.
/// Latest snapshot per worker (cluster-wide), including idle workers.
///
/// Recursive skip-scan: one `(worker_id, snapshot_at DESC)` index probe per
/// distinct worker (`idx_horsies_worker_states_worker_snapshot`, migration
/// 0018) — the seed term takes the first worker's newest snapshot, each
/// recursive step probes the next `worker_id` boundary. Postgres has no loose
/// index scan, so the previous `DISTINCT ON (worker_id)` form read every
/// retained snapshot in the timeseries to return one row per worker.
/// Parity with horsies PR #170.
///
/// Each row also has `read_at`, the `statement_timestamp()` of this
/// statement: the database clock, one value for every row. The web worker
/// list measures ages from it.
const LIST_WORKER_STATES_SQL: &str = "\
WITH RECURSIVE latest AS (
    (
        SELECT
            worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
            max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
            queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
            memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
            worker_started_at, slots_down, class_capacity, profile_name
        FROM horsies_worker_states
        ORDER BY worker_id, snapshot_at DESC
        LIMIT 1
    )
    UNION ALL
    SELECT nxt.* FROM latest l
    CROSS JOIN LATERAL (
        SELECT
            w.worker_id, w.snapshot_at, w.hostname, w.pid, w.processes, w.max_claim_batch,
            w.max_claim_per_worker, w.cluster_wide_cap, w.queues, w.queue_priorities,
            w.queue_max_concurrency, w.recovery_config, w.tasks_running, w.tasks_claimed,
            w.memory_usage_mb, w.memory_percent, w.cpu_percent, w.children_memory_mb,
            w.worker_started_at, w.slots_down, w.class_capacity, w.profile_name
        FROM horsies_worker_states w
        WHERE w.worker_id > l.worker_id
        ORDER BY w.worker_id, w.snapshot_at DESC
        LIMIT 1
    ) nxt
)
SELECT latest.*, statement_timestamp() AS read_at FROM latest";

/// Latest snapshot for a single worker.
const GET_WORKER_STATE_LATEST_SQL: &str = "\
SELECT
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
    max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
    queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
    memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
    worker_started_at, slots_down, class_capacity, profile_name
FROM horsies_worker_states
WHERE worker_id = $1
ORDER BY snapshot_at DESC
LIMIT 1";

/// History for a single worker, newest first. A `NULL` limit (`$2`) returns all
/// retained rows; callers pass an explicit cap to bound the fetch.
const GET_WORKER_STATE_HISTORY_SQL: &str = "\
SELECT
    worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
    max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
    queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
    memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
    worker_started_at, slots_down, class_capacity, profile_name
FROM horsies_worker_states
WHERE worker_id = $1
ORDER BY snapshot_at DESC
LIMIT $2";

/// The stale list: `horsies_list_stale_tasks` (baseline `0007`), task rows
/// in list order, then its `LIST_END` row. `$1` is the threshold in minutes.
static LIST_STALE_TASKS: TypedStatement = TypedStatement::new(
    "list_stale_tasks",
    "SELECT row_kind, task_id, task_name, worker_hostname, worker_pid, worker_process_name,
            started_at, last_heartbeat, list_state, row_bound
     FROM horsies_list_stale_tasks($1)
     ORDER BY row_position NULLS LAST",
    &[ParameterType::Int4],
);

static LIST_WORKER_STATES: TypedStatement =
    TypedStatement::new("list_worker_states", LIST_WORKER_STATES_SQL, &[]);

static GET_WORKER_STATE_LATEST: TypedStatement = TypedStatement::new(
    "get_worker_state_latest",
    GET_WORKER_STATE_LATEST_SQL,
    &[ParameterType::Text],
);

static GET_WORKER_STATE_HISTORY: TypedStatement = TypedStatement::new(
    "get_worker_state_history",
    GET_WORKER_STATE_HISTORY_SQL,
    &[ParameterType::Text, ParameterType::Int8],
);

/// The expired list: `horsies_list_expired_tasks` (baseline `0007`), task
/// rows in list order, then its `LIST_END` row. `expired_for` is read as
/// whole microseconds, in the column `expired_for_us`.
static LIST_EXPIRED_TASKS: TypedStatement = TypedStatement::new(
    "list_expired_tasks",
    "SELECT row_kind, task_id, task_name, queue_name, priority, sent_at, enqueued_at, good_until,
            (EXTRACT(EPOCH FROM expired_for) * 1000000)::bigint AS expired_for_us,
            list_state, row_bound
     FROM horsies_list_expired_tasks()
     ORDER BY row_position NULLS LAST",
    &[],
);

/// The worker ping broadcast: `$1` the ping channel, `$2` the request JSON.
static NOTIFY_WORKER_PING: TypedStatement = TypedStatement::new(
    "notify_worker_ping",
    "SELECT pg_notify($1, $2)",
    &[ParameterType::Text, ParameterType::Text],
);

/// The family row of the database, read before the first write of a handle.
static READ_CLIENT_FAMILY: TypedStatement = TypedStatement::new(
    "read_client_family",
    crate::broker::client_family::READ_FAMILY_SQL,
    &[],
);

/// Health check: `SELECT 1` to verify broker connectivity.
const HEALTH_CHECK_SQL: &str = "SELECT 1";

static HEALTH_CHECK: TypedStatement = TypedStatement::new("health_check", HEALTH_CHECK_SQL, &[]);

/// `SELECT 1` on the session pool in the worker boot check.
static SESSION_CHECK: TypedStatement = TypedStatement::new("session_check", HEALTH_CHECK_SQL, &[]);

/// The NOTIFY of the worker boot's session check, on the main core pool:
/// `$1` the check channel.
static SESSION_CHECK_NOTIFY: TypedStatement = TypedStatement::new(
    "session_check_notify",
    "SELECT pg_notify($1, 'ok')",
    &[ParameterType::Text],
);

// ---------------------------------------------------------------------------
// PostgresBroker
// ---------------------------------------------------------------------------

/// Schema behavior selected when a broker is constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaInitializationMode {
    /// Apply migrations and require the validated task-history fleet gate.
    MigrateAndValidate,
    /// Connect for monitoring reads without executing schema DDL or fleet
    /// gates. The broker keeps its listener, as Python's web tool, which runs
    /// with `SchemaMode.OBSERVE_ONLY`.
    ObserveOnly,
}

/// The stored values of one send command, checked: only the database can
/// refuse it now. Built by [`SendCommand::prepare`]
/// and [`PostgresBroker::prepare_send_command`].
///
/// The version-1 fingerprint leaves the task id and `sent_at` out, so one
/// value serves every row of the command: each slot of a schedule fire.
/// `stored_input` is the `input` text the row stores; `execution` and
/// `max_retries` are read from `task_options`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendCommand {
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub stored_input: String,
    pub task_options: Option<String>,
    pub execution: crate::core::task::execution::Execution,
    pub max_retries: i32,
    pub retention_class_key: String,
    pub retain_rerun_input: bool,
    pub fingerprint_version: i16,
    pub fingerprint: Vec<u8>,
}

impl SendCommand {
    /// The stored values of an unkeyed send with no deadline and no delay,
    /// with the rerun-input posture `retain_rerun_input`: the values that
    /// [`PostgresBroker::enqueue`] writes for the same arguments. Touches no
    /// database.
    ///
    /// Errors: `EnqueueContract`, the refusals of `enqueue` before its
    /// write: an input that the rules refuse, options that do not parse or
    /// whose retry policy does not fit, a value the fingerprint refuses.
    /// It takes a shared class with no `timeout_ms`, which only a new send
    /// refuses (`check_new_send_timeout`): a schedule keeps such options,
    /// and the worker refuses the run.
    pub fn prepare(
        task_name: &str,
        queue_name: &str,
        priority: i32,
        input: &str,
        task_options: Option<&str>,
        retention: EnqueueRetention,
        retain_rerun_input: bool,
    ) -> Result<Self, BrokerError> {
        check_sent_input(input)?;
        // The options are read with the strict rule before the retry limit,
        // as `CheckedSend::check` reads them, so a text that does not read
        // names its reader's error.
        let _options = parse_optional_object(task_options)
            .map_err(|error| BrokerError::EnqueueContract(EnqueueRefusal::from(error)))?;
        let max_retries = enqueue_max_retries(task_name, task_options)?;
        let retention_class_key = retention.class_key();
        let facts = prepare_enqueue_facts(
            task_name,
            queue_name,
            priority,
            input,
            None,
            None,
            task_options,
            Some(&retention_class_key),
            retain_rerun_input,
            None,
        )
        .map_err(|error| BrokerError::EnqueueContract(EnqueueRefusal::from(error)))?;
        Ok(Self {
            task_name: task_name.to_owned(),
            queue_name: queue_name.to_owned(),
            priority,
            stored_input: input.to_owned(),
            task_options: task_options.map(str::to_owned),
            execution: facts.execution,
            max_retries,
            retention_class_key: facts.retention_class_key,
            retain_rerun_input: facts.retain_rerun_input,
            fingerprint_version: facts.command_fingerprint_version,
            fingerprint: facts.command_fingerprint.to_vec(),
        })
    }
}

/// One send after every refusal that needs no connection: the values that
/// [`PostgresBroker::enqueue_checked`] binds to `horsies_enqueue_task`,
/// except the key's reservation window, which is the broker's. It borrows
/// the texts of the send. Only [`CheckedSend::check`] builds it, with no
/// trace carrier; [`CheckedSend::with_trace`] sets one.
#[derive(Debug, Clone)]
pub struct CheckedSend<'a> {
    task_id: Uuid,
    task_name: &'a str,
    input: &'a str,
    queue_name: &'a str,
    priority: i32,
    good_until: Option<DateTime<Utc>>,
    task_options: Option<&'a str>,
    start: EnqueueStart,
    sent_at: DateTime<Utc>,
    max_retries: i32,
    facts: PreparedEnqueueFacts,
    trace: StoredTrace,
}

impl<'a> CheckedSend<'a> {
    /// The refusals of one send that need no connection, in the order of
    /// `_prepare_enqueue` (`core/brokers/postgres.py` of the `v0.7.0` package),
    /// which ran before the schema install:
    /// both `enqueued_at` and `enqueue_delay_seconds`; a `sent_at` in the
    /// future of a send that starts now; an input that the rules of a user
    /// value refuse (`check_sent_input`); options that do not read by the
    /// strict rule, that hold an integer outside `i64::MIN` to `u64::MAX`,
    /// or whose retry limit does not fit; a `sent_at` with no
    /// whole-microsecond form; and the facts of the row
    /// (`prepare_enqueue_facts`): the strict JSON rule, the execution
    /// class, the command fingerprint and the scope of the idempotency key;
    /// then a shared class with no `timeout_ms` (`check_new_send_timeout`).
    /// A send with no id mints it here. `retain_rerun_input` is resolved:
    /// the send's choice, or the default of the handle. Touches no
    /// database; reads the clock.
    ///
    /// Errors: `EnqueueContract`, which the send maps to
    /// `VALIDATION_FAILED`. Nothing is written.
    #[allow(clippy::too_many_arguments)]
    pub fn check(
        task_name: &'a str,
        input: &'a str,
        queue_name: &'a str,
        priority: i32,
        sent_at: Option<DateTime<Utc>>,
        enqueued_at: Option<DateTime<Utc>>,
        good_until: Option<DateTime<Utc>>,
        task_options: Option<&'a str>,
        predetermined_task_id: Option<Uuid>,
        enqueue_delay_seconds: Option<i64>,
        idempotency_key: Option<&str>,
        retention: EnqueueRetention,
        retain_rerun_input: bool,
    ) -> Result<Self, BrokerError> {
        let start = EnqueueStart::of(enqueued_at, enqueue_delay_seconds)?;
        if sent_at.is_some_and(|value| value > Utc::now() + chrono::Duration::seconds(5))
            && matches!(start, EnqueueStart::Now)
        {
            return Err(BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
                SendPart::Input,
                "sent_at is in the future without enqueued_at or delay_seconds; sent_at is a call-site timestamp".to_owned(),
            )));
        }
        check_sent_input(input)?;
        let task_id = match predetermined_task_id {
            Some(task_id) => task_id,
            // The task id is a fact of the send's input; a send with no id
            // mints it here.
            None => crate::core::history::identity::uuid7::mint_task_id().map_err(|error| {
                BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
                    SendPart::Input,
                    format!("task identity mint failed: {error}"),
                ))
            })?,
        };
        // The options are read with the strict rule before the retry limit,
        // so a text that does not read names its reader's error.
        let options = parse_optional_object(task_options)
            .map_err(|error| BrokerError::EnqueueContract(EnqueueRefusal::from(error)))?;
        let max_retries = enqueue_max_retries(task_name, task_options)?;
        let sent_at = sent_at_at_stored_precision(sent_at.unwrap_or_else(Utc::now))?;
        let retention_class_key = retention.class_key();
        let facts = prepare_enqueue_facts(
            task_name,
            queue_name,
            priority,
            input,
            good_until,
            enqueue_delay_seconds,
            task_options,
            Some(&retention_class_key),
            retain_rerun_input,
            idempotency_key,
        )
        .map_err(|error| BrokerError::EnqueueContract(EnqueueRefusal::from(error)))?;
        check_new_send_timeout(facts.execution, options.as_ref())?;
        Ok(Self {
            task_id,
            task_name,
            input,
            queue_name,
            priority,
            good_until,
            task_options,
            start,
            sent_at,
            max_retries,
            facts,
            trace: StoredTrace::NoCarrier,
        })
    }

    /// The send with the trace carrier `trace` (the send of
    /// [`CheckedSend::check`] has none). The carrier is checked against the
    /// bounds of `telemetry::trace_carrier`: a carrier that breaks a bound
    /// is not stored, the send keeps no carrier, and the process logs one
    /// warning. Never refuses the send. Touches no database.
    pub fn with_trace(self, trace: SendTrace) -> CheckedSend<'a> {
        CheckedSend {
            trace: StoredTrace::of_send(trace),
            ..self
        }
    }

    /// The task id of the send: the given one, or the one minted by the
    /// check.
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }
}

/// PostgreSQL-backed task broker.
///
/// Every statement runs on the statement layer's pools (`core`:
/// [`crate::db::CorePool`], one per role). No pool connects when the broker
/// is built: each opens its first connection at its first checkout.
///
/// Every LISTEN of the broker goes through the process listener of its
/// session URL: one connection, shared with the other brokers of the process
/// on the same URL.
pub struct PostgresBroker {
    core: CoreHandle,
    statement_log: StatementLog,
    pgbouncer_transaction_mode: bool,
    retain_rerun_input_default: bool,
    idempotency_reservation_window_micros: i64,
    listener: ProcessListener,
    schema_initialized: tokio::sync::OnceCell<()>,
    schema_initialization_mode: SchemaInitializationMode,
    /// The coverage pass that the schema install runs after the migrations
    /// ([`PostgresBroker::ensure_schema_initialized`]): the request of the
    /// default retention config, or the one that the client set with
    /// [`PostgresBroker::with_install_coverage`].
    install_coverage: CoverageRequest,
    /// The client family of this handle: the family that its install records
    /// and that its writes check. Every constructor takes it from its
    /// caller; it does not change after construction.
    client_family: ClientFamily,
    /// Set once the family check before the first write passed.
    client_family_admitted: tokio::sync::OnceCell<()>,
    /// Whether this broker saw the staged result function published. The
    /// result read keeps it ([`read_task_result`]).
    result_reader: ResultReaderSeen,
}

impl PostgresBroker {
    /// Construct a test broker of `family` on the test database of `url`:
    /// the core pools of [`crate::broker::pools::handle_for_test_url`].
    ///
    /// The broker has no session URL, so it has no listener: result waits
    /// poll, and an operation that needs a listener is refused with
    /// [`ListenError::NoSessionUrl`]. Use
    /// [`from_test_url_and_session_url`](Self::from_test_url_and_session_url)
    /// for a broker with a listener.
    #[cfg(test)]
    pub fn from_test_url(url: &str, family: ClientFamily) -> Self {
        Self::from_test_url_with_schema_initialization_mode(
            url,
            SchemaInitializationMode::MigrateAndValidate,
            ProcessListener::without_session_url(),
            family,
        )
    }

    /// Construct a test broker of `family` on the test database of `url`,
    /// with its listener on `session_url`: the process listener of that URL,
    /// which opens its connection at the first subscription with the default
    /// socket settings (`DEFAULT_SOCKET_SETTINGS`).
    #[cfg(test)]
    pub fn from_test_url_and_session_url(
        url: &str,
        session_url: &str,
        family: ClientFamily,
    ) -> Self {
        Self::from_test_url_with_schema_initialization_mode(
            url,
            SchemaInitializationMode::MigrateAndValidate,
            ProcessListener::on_session_url(session_url, DEFAULT_SOCKET_SETTINGS),
            family,
        )
    }

    /// Construct a test monitoring broker of `family` on the test database
    /// of `url` that never applies migrations or checks the fleet cutover
    /// gate. It has no session URL, so it has no listener.
    #[cfg(test)]
    pub fn from_test_url_observe_only(url: &str, family: ClientFamily) -> Self {
        Self::from_test_url_with_schema_initialization_mode(
            url,
            SchemaInitializationMode::ObserveOnly,
            ProcessListener::without_session_url(),
            family,
        )
    }

    /// The test broker on `url`.
    #[cfg(test)]
    fn from_test_url_with_schema_initialization_mode(
        url: &str,
        schema_initialization_mode: SchemaInitializationMode,
        listener: ProcessListener,
        family: ClientFamily,
    ) -> Self {
        Self {
            core: crate::broker::pools::handle_for_test_url(url),
            statement_log: StatementLog::Off,
            pgbouncer_transaction_mode: false,
            retain_rerun_input_default: false,
            idempotency_reservation_window_micros: IDEMPOTENCY_WINDOW_DEFAULT_MICROS,
            listener,
            schema_initialized: tokio::sync::OnceCell::new(),
            schema_initialization_mode,
            install_coverage: default_install_coverage(),
            client_family: family,
            client_family_admitted: tokio::sync::OnceCell::new(),
            result_reader: ResultReaderSeen::default(),
        }
    }

    /// Construct a test broker of `family` on the test database of `url`
    /// with an explicit keyed-enqueue reservation window. `None` selects the
    /// 24-hour contract default.
    #[cfg(test)]
    pub fn from_test_url_with_idempotency_reservation_window(
        url: &str,
        window: Option<chrono::Duration>,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        Self::from_test_url_with_enqueue_policy(url, false, window, family)
    }

    /// Construct a test broker of `family` on the test database of `url`
    /// with both enqueue-time broker policies.
    #[cfg(test)]
    pub fn from_test_url_with_enqueue_policy(
        url: &str,
        retain_rerun_input_default: bool,
        window: Option<chrono::Duration>,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        let micros = idempotency_reservation_window_micros(window)?;
        let mut broker = Self::from_test_url(url, family);
        broker.retain_rerun_input_default = retain_rerun_input_default;
        broker.idempotency_reservation_window_micros = micros;
        Ok(broker)
    }

    /// Connect using a raw database URL, for a client of `family`.
    ///
    /// A URL whose scheme is not `postgresql://` or `postgres://` is refused
    /// with [`BrokerError::InvalidConnectionSettings`] before any connection.
    pub async fn connect(database_url: &str, family: ClientFamily) -> Result<Self, BrokerError> {
        Self::connect_with(&PostgresConfig::from_database_url(database_url), family).await
    }

    /// Connect using a `PostgresConfig` from horsies-core, for a client of
    /// `family`.
    pub async fn connect_with(
        config: &PostgresConfig,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        Self::connect_with_schema_initialization_mode(
            config,
            SchemaInitializationMode::MigrateAndValidate,
            family,
        )
        .await
    }

    /// Connect a monitoring broker of `family` without applying migrations
    /// or checking the task-history fleet gate.
    pub async fn connect_observe_only(
        config: &PostgresConfig,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        Self::connect_with_schema_initialization_mode(
            config,
            SchemaInitializationMode::ObserveOnly,
            family,
        )
        .await
    }

    /// Connect the broker of a worker's child of `family`: no migration and
    /// no fleet gate check, as [`Self::connect_observe_only`]; the main URL
    /// pool keeps `kept_open` connections open and, with `check`, pings
    /// every pooled connection at checkout ([`PoolProfile::Child`]).
    ///
    /// Must run inside a tokio runtime when `kept_open` keeps connections
    /// open: the pool starts the task that opens them.
    pub async fn connect_child(
        config: &PostgresConfig,
        kept_open: crate::db::KeptOpen,
        check: bool,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        Self::connect_with_profile(
            config,
            SchemaInitializationMode::ObserveOnly,
            PoolProfile::Child { kept_open, check },
            family,
        )
        .await
    }

    /// Connect a worker's broker of `family`: the main URL pool is the worker
    /// profile pool, which keeps [`crate::db::WORKER_RESERVED_CONNECTIONS`]
    /// free from batch checkouts for the reaper and the per-task writes.
    pub async fn connect_worker_with(
        config: &PostgresConfig,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        Self::connect_with_profile(
            config,
            SchemaInitializationMode::MigrateAndValidate,
            PoolProfile::Worker,
            family,
        )
        .await
    }

    async fn connect_with_schema_initialization_mode(
        config: &PostgresConfig,
        schema_initialization_mode: SchemaInitializationMode,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        Self::connect_with_profile(
            config,
            schema_initialization_mode,
            PoolProfile::Producer,
            family,
        )
        .await
    }

    /// Checks the config and builds the core pools of a broker of `family`.
    /// Opens no connection: each pool connects at its first checkout.
    async fn connect_with_profile(
        config: &PostgresConfig,
        schema_initialization_mode: SchemaInitializationMode,
        profile: PoolProfile,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        config.validate()?;
        let core = CoreHandle::open(config, profile)?;
        Ok(Self {
            core,
            statement_log: StatementLog::from_echo(config.echo),
            pgbouncer_transaction_mode: config.pgbouncer_transaction_mode,
            retain_rerun_input_default: config.retain_rerun_input_default,
            idempotency_reservation_window_micros: IDEMPOTENCY_WINDOW_DEFAULT_MICROS,
            listener: ProcessListener::on_session_url(
                config.effective_session_database_url(),
                config.socket_settings(),
            ),
            schema_initialized: tokio::sync::OnceCell::new(),
            schema_initialization_mode,
            install_coverage: default_install_coverage(),
            client_family: family,
            client_family_admitted: tokio::sync::OnceCell::new(),
            result_reader: ResultReaderSeen::default(),
        })
    }

    /// Connect a broker of `family` with an explicit keyed-enqueue
    /// reservation window. Validation happens before any database connection
    /// is attempted.
    pub async fn connect_with_idempotency_reservation_window(
        config: &PostgresConfig,
        window: Option<chrono::Duration>,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        let micros = idempotency_reservation_window_micros(window)?;
        let mut broker = Self::connect_with(config, family).await?;
        broker.idempotency_reservation_window_micros = micros;
        Ok(broker)
    }

    /// Connect a broker of `family` with the schema behaviour and the
    /// keyed-enqueue reservation window of a client manifest, with the pools
    /// of `profile`: a producer, a worker process (the main URL pool keeps
    /// [`crate::db::WORKER_RESERVED_CONNECTIONS`] free from batch checkouts), or a
    /// worker's child. The window is checked before any database connection
    /// is attempted.
    pub async fn connect_with_policies(
        config: &PostgresConfig,
        schema_initialization_mode: SchemaInitializationMode,
        window: chrono::Duration,
        profile: PoolProfile,
        family: ClientFamily,
    ) -> Result<Self, BrokerError> {
        let micros = idempotency_reservation_window_micros(Some(window))?;
        let mut broker =
            Self::connect_with_profile(config, schema_initialization_mode, profile, family).await?;
        broker.idempotency_reservation_window_micros = micros;
        Ok(broker)
    }

    /// Install the schema: run the embedded migrations that the database
    /// lacks, and record or check this handle's client family
    /// ([`crate::broker::migrations`]). A broker built with
    /// [`SchemaInitializationMode::ObserveOnly`] runs nothing.
    ///
    /// Runs on the session-work core pool (the session pool, or the main
    /// pool when the URLs are equal). Bookkeeps in the horsies-owned
    /// `horsies_migrations` table, so it never collides with an
    /// application's own migration runner. A close waits for it as for a
    /// write.
    ///
    /// Errors: [`BrokerError::SchemaRefused`] and
    /// [`BrokerError::ClientFamilyMismatch`] (nothing was changed; not
    /// retryable); [`BrokerError::InstallLock`]; [`BrokerError::Statement`];
    /// [`BrokerError::RowDecode`]; [`BrokerError::PoolSettings`] when a
    /// closed handle cannot open its pools again.
    pub async fn migrate(&self) -> Result<(), BrokerError> {
        match self.schema_initialization_mode {
            SchemaInitializationMode::ObserveOnly => return Ok(()),
            SchemaInitializationMode::MigrateAndValidate => {}
        }
        let (open, _write) = self.core.pools_for_write()?;
        crate::broker::migrations::install_schema_on(
            open.pools().session_work(),
            self.client_family,
            crate::broker::migrations::InstallLockPlan::fleet(),
            self.statement_log,
        )
        .await
        .map_err(BrokerError::from)
    }

    /// The client family of this handle: the family that its install records
    /// and that its writes check.
    pub fn client_family(&self) -> ClientFamily {
        self.client_family
    }

    /// The same handle, whose schema install runs the coverage pass of
    /// `request`: the request of the client's retention config
    /// ([`CoverageRequest::of_retention`]), so the install covers every class
    /// that the client's sends can stamp.
    pub fn with_install_coverage(mut self, request: CoverageRequest) -> Self {
        self.install_coverage = request;
        self
    }

    /// The same handle with no listener (the schema mode
    /// `ASSUME_INITIALIZED`): a result wait reads again
    /// every 200 ms (`TASK_RESULT_REPOLL_POLLING`), and an operation that
    /// needs a listener answers `LISTENER_DISABLED`. Opens and closes
    /// nothing: the listener of a connected broker opens its connection at
    /// the first subscription.
    pub fn without_listener(mut self) -> Self {
        self.listener = ProcessListener::without_session_url();
        self
    }

    /// Check once, before the first write of this handle, that the database
    /// records this handle's client family. Reads never call it. A handle
    /// whose install ran in this process has passed it already. Nothing is
    /// written when the check does not pass:
    ///
    /// - a database of another family, or with no family row, is
    ///   `BrokerError::ClientFamilyMismatch`, not retryable;
    /// - a family row whose value is not a client family (corrupt stored
    ///   data) is `BrokerError::UnknownStoredFamily`, not retryable;
    /// - a failed read ([`FamilyCheckFailure`]) is `BrokerError::Statement`,
    ///   `BrokerError::PoolSettings` or `BrokerError::RowDecode`.
    ///
    /// Only a pass is kept, so the next write checks again after a refusal.
    /// One statement, once.
    pub async fn ensure_client_family_for_write(&self) -> Result<(), BrokerError> {
        match self.check_client_family_for_write().await {
            Ok(()) => Ok(()),
            Err(FamilyCheckError::Mismatch(mismatch)) => Err(mismatch.into()),
            Err(FamilyCheckError::UnknownStoredFamily(unknown)) => Err(unknown.into()),
            Err(FamilyCheckError::ReadFailed(failure)) => Err(failure.into()),
        }
    }

    /// The check of [`PostgresBroker::ensure_client_family_for_write`], with
    /// its closed error. A pass is kept for the life of the handle; a
    /// mismatch, an unknown stored family or a failed read is not.
    async fn check_client_family_for_write(&self) -> Result<(), FamilyCheckError> {
        self.client_family_admitted
            .get_or_try_init(|| async {
                let stored = self
                    .read_client_family()
                    .await
                    .map_err(FamilyCheckError::ReadFailed)?;
                let recorded =
                    recorded_family_from(stored).map_err(FamilyCheckError::UnknownStoredFamily)?;
                admit_family(recorded, self.client_family).map_err(FamilyCheckError::Mismatch)
            })
            .await?;
        Ok(())
    }

    /// Reads the stored text of the family row on the main core pool (`None`:
    /// no row). Checkout and bound as [`PostgresBroker::query_main`]. Writes
    /// nothing.
    async fn read_client_family(&self) -> Result<Option<String>, FamilyCheckFailure> {
        let open = self
            .core
            .pools()
            .map_err(FamilyCheckFailure::PoolsNotReopened)?;
        let statement = READ_CLIENT_FAMILY.bind().finish();
        self.log_statement(statement.statement());
        let rows = open
            .main()
            .query(
                CheckoutClass::Batch,
                &statement,
                StatementBound::ConnectionTimeouts,
            )
            .await
            .map_err(FamilyCheckFailure::Statement)?;
        match rows.first() {
            Some(row) => Ok(Some(
                column(row, &READ_CLIENT_FAMILY, "family")
                    .map_err(FamilyCheckFailure::FamilyColumnUndecodable)?,
            )),
            None => Ok(None),
        }
    }

    /// Install the schema once for this broker: the migrations
    /// ([`PostgresBroker::migrate`]), then the gated coverage pass of the
    /// install's coverage request ([`PostgresBroker::install_coverage`]), the
    /// pass that the worker start runs. The history leaves of the current
    /// UTC day and of the lookahead then exist for every class of the
    /// request before the first terminal write of any process, with or
    /// without a worker. A contained refusal of the pass is logged; a
    /// terminal write whose leaf is still missing fails with SQLSTATE
    /// `55H01`.
    ///
    /// The first successful call installs or checks the schema and runs the
    /// pass; later calls are a no-op. A failure is not kept, so a later call
    /// runs the install again. The install ends only at this build's whole
    /// chain (a newer or unknown schema is refused), and it records or checks
    /// this handle's family, so writes of this handle need no second family
    /// read. A handle built with [`SchemaInitializationMode::ObserveOnly`]
    /// runs nothing.
    ///
    /// Errors: as [`PostgresBroker::migrate`], and
    /// [`BrokerError::InstallCoverage`] when the pass had no outcome (the
    /// migrations stay committed).
    pub async fn ensure_schema_initialized(&self) -> Result<(), BrokerError> {
        match self.schema_initialization_mode {
            SchemaInitializationMode::ObserveOnly => return Ok(()),
            SchemaInitializationMode::MigrateAndValidate => {}
        }
        self.schema_initialized
            .get_or_try_init(|| async {
                self.migrate().await?;
                let _already_admitted = self.client_family_admitted.set(());
                self.cover_after_install().await
            })
            .await?;
        Ok(())
    }

    /// The coverage request of the schema install of this handle.
    pub fn install_coverage(&self) -> &CoverageRequest {
        &self.install_coverage
    }

    /// The schema behavior selected when this broker was constructed.
    pub const fn schema_initialization_mode(&self) -> SchemaInitializationMode {
        self.schema_initialization_mode
    }

    /// The open core pools, for a lifecycle writer that runs on the main
    /// core pool ([`crate::broker::attempt_writes`]) or a workflow step
    /// (`workflow_engine::steps`), and the token of its write:
    /// [`PostgresBroker::close`] waits for the write while the token lives.
    /// In a worker the main core pool is the worker profile pool. An `Idle`
    /// handle opens its pools first.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when the pools cannot be built.
    pub(crate) fn core_pools_for_write(
        &self,
    ) -> Result<(std::sync::Arc<OpenPools>, WriteInFlight), BrokerError> {
        Ok(self.core.pools_for_write()?)
    }

    /// The open core pools, for a read of a client handle that runs on the
    /// main core pool ([`crate::broker::lifecycle_operations`]). A close does
    /// not wait for it. An `Idle` handle opens its pools first.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when the pools cannot be built.
    pub(crate) fn core_pools_for_read(&self) -> Result<std::sync::Arc<OpenPools>, BrokerError> {
        Ok(self.core.pools()?)
    }

    /// Whether this broker logs each core statement it runs, for the
    /// lifecycle writers that run on the main core pool.
    pub(crate) fn statement_log(&self) -> StatementLog {
        self.statement_log
    }

    /// Runs one statement on the main core pool and reads every row.
    ///
    /// The checkout passes the reserve gate ([`CheckoutClass::Batch`]): the
    /// moved call sites are producer and monitoring work, never the reaper or
    /// a per-task write. Bound: the connection's own timeouts.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when a closed handle cannot open
    /// its pools again; [`BrokerError::Statement`].
    pub(crate) async fn query_main(
        &self,
        statement: &BoundStatement<'_>,
    ) -> Result<Vec<Row>, BrokerError> {
        let open = self.core.pools()?;
        self.log_statement(statement.statement());
        Ok(open
            .main()
            .query(
                CheckoutClass::Batch,
                statement,
                StatementBound::ConnectionTimeouts,
            )
            .await?)
    }

    /// Runs one statement on the main core pool and reads the count of
    /// affected rows. Checkout, bound and errors as
    /// [`PostgresBroker::query_main`].
    async fn execute_main(&self, statement: &BoundStatement<'_>) -> Result<u64, BrokerError> {
        let open = self.core.pools()?;
        self.log_statement(statement.statement());
        Ok(open
            .main()
            .execute(
                CheckoutClass::Batch,
                statement,
                StatementBound::ConnectionTimeouts,
            )
            .await?)
    }

    /// Runs one read on the main core pool and reads each row with `read`.
    ///
    /// Errors: [`BrokerError::Statement`] when the statement failed;
    /// [`BrokerError::RowDecode`] for the first row that does not read.
    async fn read_main<T>(
        &self,
        statement: &BoundStatement<'_>,
        read: fn(&Row, &TypedStatement) -> Result<T, RowDecodeError>,
    ) -> Result<Vec<T>, BrokerError> {
        let rows = self.query_main(statement).await?;
        Ok(read_rows(&rows, statement.statement(), read)?)
    }

    fn log_statement(&self, statement: &TypedStatement) {
        self.statement_log.log(statement);
    }

    /// Whether this broker's runtime pool is configured for PgBouncer
    /// transaction pooling.
    pub fn pgbouncer_transaction_mode(&self) -> bool {
        self.pgbouncer_transaction_mode
    }

    /// Rerun one retained terminal task in a broker-owned transaction on the
    /// main core pool ([`CheckoutClass::Batch`]): the source read, then one
    /// `horsies_enqueue_task` with the lineage `RERUN`, which claims the key
    /// of a keyed rerun (`core::history::rerun::operations`). A close waits
    /// for it as for a write.
    ///
    /// The handle checks its client family before the transaction: a
    /// database of another family, or with no family row, is
    /// `RerunError::ClientFamilyMismatch`; a family row whose value is not a
    /// client family is `RerunError::UnknownStoredFamily`; a family row that
    /// could not be read is `RerunError::FamilyCheckFailed`. In each case
    /// nothing is written. `codes` are the declared custom codes of the
    /// calling app's tasks: the stored retry list of the source is checked
    /// against them before the write.
    ///
    /// Errors: as the rerun in its transaction, and
    /// [`RerunError::PoolSettings`] when a closed handle cannot open its
    /// pools again. After an error nothing committed, except after
    /// `OutcomeUnknown` at `COMMIT`.
    pub async fn rerun_task(
        &self,
        command: RerunCommand,
        policy: RerunEnqueuePolicy,
        codes: &RegisteredTaskCodes,
    ) -> Result<RerunOutcome, RerunError> {
        self.check_client_family_for_write()
            .await
            .map_err(rerun_family_error)?;
        let (open, _write) = self.core.pools_for_write()?;
        let mut connection = open
            .main()
            .checkout(CheckoutClass::Batch)
            .await
            .map_err(|cause| StatementFailure::NotSent {
                statement: rerun_first_statement().name(),
                cause: NotSentCause::Checkout(cause),
            })?;
        let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
        let outcome = rerun_task_in_transaction(
            &mut transaction,
            self.statement_log,
            &command,
            &policy,
            codes,
            self.client_family,
        )
        .await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    /// The broker's listener: the process listener of the session URL, one
    /// LISTEN connection shared by every subscriber of every broker of the
    /// process on that URL.
    pub(crate) fn listener(&self) -> &ProcessListener {
        &self.listener
    }

    // -----------------------------------------------------------------------
    // Task lifecycle operations
    // -----------------------------------------------------------------------

    /// Enqueue a new task: the refusals that need no connection
    /// ([`CheckedSend::check`], with the broker's rerun-input default when
    /// `retain_rerun_input` is `None`), then one `horsies_enqueue_task`
    /// statement in autocommit on the main core pool, checked out as batch
    /// work, after the client family check ([`Self::enqueue_checked`];
    /// Python `PostgresBroker.enqueue_async`). A caller that connects or
    /// installs the schema first runs the check before those steps. Returns the id of
    /// the task that holds the send: the sent id (minted here when
    /// `predetermined_task_id` is `None`), or the task that holds the
    /// idempotency key with the same command.
    ///
    /// `input` is the encoded task input as one JSON value; it is stored as
    /// written. The input follows the rules of a user value
    /// (`check_sent_input`); a refusal is `EnqueueContract`, which the send
    /// maps to `VALIDATION_FAILED`. A task id that is taken answers by the
    /// stored version-1 command fingerprint and `sent_at` (the exact-id
    /// resend check). `send_kind` is [`SendKind::Repeat`] for a send retry:
    /// a task that finished then answers from history and does not run
    /// again; [`SendKind::First`] does not read history.
    ///
    /// Errors: `EnqueueContract` (refused before any statement: both
    /// `enqueued_at` and `enqueue_delay_seconds`, a future `sent_at` with
    /// neither, an input, options or an idempotency key that the rules
    /// refuse);
    /// `IdempotencyKeyConflict`, `PayloadMismatch` and
    /// `EnqueueConflictUnverifiable` (nothing written, no reservation left);
    /// the client family check's errors; `PoolSettings`; `Statement` (nothing
    /// written, or `OutcomeUnknown`: a resend of the same send with
    /// [`SendKind::Repeat`] answers with the task id); `RowDecode`.
    ///
    /// Panics: when the function returns other than one row, or an outcome
    /// it does not define (Python raises `AssertionError`).
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        task_name: &str,
        input: &str,
        queue_name: &str,
        priority: i32,
        sent_at: Option<DateTime<Utc>>,
        enqueued_at: Option<DateTime<Utc>>,
        good_until: Option<DateTime<Utc>>,
        task_options: Option<&str>,
        predetermined_task_id: Option<Uuid>,
        enqueue_delay_seconds: Option<i64>,
        idempotency_key: Option<&str>,
        retention: EnqueueRetention,
        retain_rerun_input: Option<bool>,
        send_kind: SendKind,
    ) -> Result<Uuid, BrokerError> {
        let send = CheckedSend::check(
            task_name,
            input,
            queue_name,
            priority,
            sent_at,
            enqueued_at,
            good_until,
            task_options,
            predetermined_task_id,
            enqueue_delay_seconds,
            idempotency_key,
            retention,
            retain_rerun_input.unwrap_or(self.retain_rerun_input_default),
        )?;
        self.enqueue_checked(&send, send_kind).await
    }

    /// Write one checked send ([`CheckedSend::check`]): the client family
    /// check, then one `horsies_enqueue_task` statement in autocommit on the
    /// main core pool, checked out as batch work, with the broker's key
    /// reservation window. Returns the id of the task that holds the send,
    /// as [`PostgresBroker::enqueue`].
    ///
    /// Errors: those of [`PostgresBroker::enqueue`] after its check:
    /// `IdempotencyKeyConflict`, `PayloadMismatch`,
    /// `EnqueueConflictUnverifiable`, the client family check's errors,
    /// `PoolSettings`, `Statement`, `RowDecode`.
    ///
    /// Panics: as [`PostgresBroker::enqueue`].
    pub async fn enqueue_checked(
        &self,
        send: &CheckedSend<'_>,
        send_kind: SendKind,
    ) -> Result<Uuid, BrokerError> {
        let key = EnqueueKey::of(
            send.facts.idempotency_key_digest,
            self.idempotency_reservation_window_micros,
        );
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core.pools_for_write()?;

        let task_id = send.task_id;
        let task_name = send.task_name;
        let queue_name = send.queue_name;
        let execution = send.facts.execution.as_str();
        let fingerprint: &[u8] = send.facts.command_fingerprint.as_slice();
        let (start_kind, start_at, start_delay_seconds) = send.start.columns();
        let (key_kind, key_scope_version, key_window_micros) = key.columns();
        let key_digest = key.digest();
        let send_kind_text = send_kind.as_sql();
        let (trace_kind, trace_carrier, trace_id) = send.trace.columns();
        let statement = &ENQUEUE_TASK;
        let bound = statement
            .bind()
            .value(&task_id)
            .value(&task_name)
            .value(&queue_name)
            .value(&send.priority)
            .value(&send.input)
            .value(&send.task_options)
            .value(&execution)
            .value(&send.facts.retention_class_key)
            .value(&send.facts.retain_rerun_input)
            .value(&send.max_retries)
            .value(&send.good_until)
            .value(&send.sent_at)
            .value(&start_kind)
            .value(&start_at)
            .value(&start_delay_seconds)
            .value(&send.facts.command_fingerprint_version)
            .value(&fingerprint)
            .value(&key_kind)
            .value(&key_digest)
            .value(&key_scope_version)
            .value(&key_window_micros)
            .value(&send_kind_text)
            .value(&trace_kind)
            .value(&trace_carrier)
            .value(&trace_id)
            .finish();
        self.log_statement(statement);
        let rows = open
            .main()
            .query(
                CheckoutClass::Batch,
                &bound,
                StatementBound::ConnectionTimeouts,
            )
            .await?;
        let [row] = rows.as_slice() else {
            panic!(
                "horsies_enqueue_task returned {} rows for task {task_id}; it returns one",
                rows.len()
            );
        };
        let outcome_text: String = column(row, statement, "outcome")?;
        let answering_task_id: Uuid = column(row, statement, "task_id")?;
        let outcome = match decode_enqueue_outcome(&outcome_text, answering_task_id) {
            Ok(outcome) => outcome,
            Err(contract) => panic!("{contract} for task {task_id}"),
        };
        match outcome {
            EnqueueTaskOutcome::Inserted => {
                tracing::debug!(task_id = %task_id, task_name, queue = queue_name, "task enqueued");
            }
            EnqueueTaskOutcome::ConflictUnverifiable => {
                tracing::warn!(
                    task_id = %task_id,
                    task_name,
                    "enqueue conflict but row disappeared before verification — cannot verify payload identity",
                );
            }
            EnqueueTaskOutcome::SameCommand
            | EnqueueTaskOutcome::KeyReplay { .. }
            | EnqueueTaskOutcome::KeyConflict { .. }
            | EnqueueTaskOutcome::PayloadMismatch => {}
        }
        enqueue_result(outcome, task_id, task_name)
    }

    /// The stored values of an unkeyed send with no deadline and no delay,
    /// without a write: the
    /// values [`PostgresBroker::enqueue`] writes for the same arguments, with
    /// the broker's default rerun-input posture ([`SendCommand::prepare`]).
    /// One command serves every row of the send, for example each slot of a
    /// schedule fire: the version-1 fingerprint leaves the task id and
    /// `sent_at` out. Touches no database.
    ///
    /// Errors: those of [`SendCommand::prepare`].
    pub fn prepare_send_command(
        &self,
        task_name: &str,
        queue_name: &str,
        priority: i32,
        input: &str,
        task_options: Option<&str>,
        retention: EnqueueRetention,
    ) -> Result<SendCommand, BrokerError> {
        SendCommand::prepare(
            task_name,
            queue_name,
            priority,
            input,
            task_options,
            retention,
            self.retain_rerun_input_default,
        )
    }

    /// Expire a CLAIMED task whose `good_until` passed before user code started.
    ///
    /// Runs through `horsies_expire_owned_claim`
    /// ([`crate::broker::expiry_and_orphans::expire_owned_claim`]), which
    /// fences on worker ownership but deliberately not on the claim
    /// generation: once the deadline has passed, expiry is the correct
    /// outcome for whichever generation holds the row. One statement on the
    /// main core pool, checked out as reserved work, within `deadline`, the
    /// deadline of the start attempt whose miss path it is
    /// ([`crate::broker::child_writes`]). An end and a replay of the expiry
    /// both mean the task is expired, as Python's
    /// `_expire_claimed_task_before_start` (`core/worker/child_runner.py`)
    /// reads them; any other outcome is logged with its evidence at the
    /// adapter boundary. The stored error is Python's: `TASK_EXPIRED` with
    /// its message and the task and worker ids as `data`.
    ///
    /// Errors: [`BrokerError::Statement`] (nothing sent when `deadline`
    /// passed first; nothing written, a cancel at the bound included; or
    /// `OutcomeUnknown`), or [`BrokerError::TerminalizationContract`] for a
    /// row outside the wire contract.
    pub async fn expire_claimed_task_before_start(
        &self,
        task_id: Uuid,
        worker_id: &str,
        deadline: crate::broker::child_writes::WriteDeadline,
    ) -> Result<PrestartExpiry, BrokerError> {
        let (open, _write) = self.core_pools_for_write()?;
        let end = crate::broker::expiry_and_orphans::expire_owned_claim(
            open.main(),
            self.statement_log,
            task_id,
            worker_id,
            deadline,
        )
        .await?;
        Ok(match end {
            crate::broker::expiry_and_orphans::TaskRowEnd::Ended { .. }
            | crate::broker::expiry_and_orphans::TaskRowEnd::Refused(
                crate::core::lifecycle::AttemptWriteRefusal::AlreadyApplied { .. },
            ) => PrestartExpiry::Expired,
            crate::broker::expiry_and_orphans::TaskRowEnd::Refused(
                crate::core::lifecycle::AttemptWriteRefusal::LostClaim { .. }
                | crate::core::lifecycle::AttemptWriteRefusal::SourceStateConflict { .. }
                | crate::core::lifecycle::AttemptWriteRefusal::TaskAbsent { .. },
            ) => PrestartExpiry::NotExpired,
        })
    }

    /// Requeue a task for retry with an optional delay.
    ///
    /// `retry_count` is incremented from the CAS-locked row itself.
    /// Returns `true` if the update was applied (task was RUNNING).
    /// Returns `false` if the task was no longer RUNNING (e.g., already
    /// marked FAILED by the stale-task reaper).
    ///
    /// One statement in autocommit on the main core pool, as a per-task
    /// write ([`CheckoutClass::Reserved`]); a close waits for it. The fence
    /// is the owner (`worker_id`), with no claim generation.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when a closed handle cannot open
    /// its pools again; [`BrokerError::Statement`]. After `OutcomeUnknown`
    /// the row may be PENDING; the same requeue sent again answers `false`.
    pub async fn requeue(
        &self,
        task_id: Uuid,
        next_retry_at: Option<DateTime<Utc>>,
        worker_id: &str,
    ) -> Result<bool, BrokerError> {
        let (open, _write) = self.core_pools_for_write()?;
        let statement = &REQUEUE;
        self.log_statement(statement);
        let rows = open
            .main()
            .query(
                CheckoutClass::Reserved,
                &statement
                    .bind()
                    .value(&task_id)
                    .value(&next_retry_at)
                    .value(&worker_id)
                    .finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await?;
        let requeued = !rows.is_empty();

        match requeued {
            true => tracing::debug!(task_id = %task_id, "task requeued"),
            false => tracing::warn!(
                task_id = %task_id,
                "task requeue skipped: task no longer RUNNING or not owned by this worker"
            ),
        }
        Ok(requeued)
    }

    /// Get task attempts for a specific task (most recent first).
    pub async fn get_task_attempts(
        &self,
        task_id: Uuid,
    ) -> Result<Vec<TaskAttemptRow>, BrokerError> {
        let rows = self
            .read_main(
                &SELECT_TASK_ATTEMPTS.bind().value(&task_id).finish(),
                task_attempt_row,
            )
            .await?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        let live_rows = self
            .query_main(&LIVE_TASK_EXISTS.bind().value(&task_id).finish())
            .await?;
        let live: bool = scalar(&one_row(live_rows, &LIVE_TASK_EXISTS)?, &LIVE_TASK_EXISTS)?;
        if live {
            return Ok(rows);
        }

        let TaskDetailResult::History(detail) = self.fetch_history_detail(task_id).await? else {
            return Ok(Vec::new());
        };
        Ok(detail
            .attempts
            .into_iter()
            .rev()
            .map(|record| TaskAttemptRow {
                task_id,
                attempt: record.attempt(),
                outcome: record.outcome().to_owned(),
                will_retry: record.will_retry(),
                started_at: record.started_at(),
                finished_at: record.finished_at(),
                error_code: record.error_code().map(str::to_owned),
                error_message: record.error_message().map(str::to_owned),
                failed_reason: record.failed_reason().map(str::to_owned),
                worker_id: record.worker_id().map(str::to_owned),
                worker_hostname: record.worker_hostname().map(str::to_owned),
                worker_pid: record.worker_pid(),
                worker_process_name: record.worker_process_name().map(str::to_owned),
            })
            .collect())
    }

    // -----------------------------------------------------------------------
    // Result retrieval
    // -----------------------------------------------------------------------

    /// Fetch the verified outer result envelope without task-specific decoding.
    ///
    /// A terminal history row is resolved through the staged detail reader.
    /// When `timeout` expires for a live non-terminal task, the latest status is
    /// returned with no result. A task absent from both live and retained
    /// history returns [`RawResultRead::Absent`] with the reason of the
    /// history read. A limit above
    /// [`MAX_WAIT_LIMIT_MS`](crate::broker::wait_limit::MAX_WAIT_LIMIT_MS) is
    /// `INVALID_INPUT` before any read. A listener that cannot start makes the
    /// wait poll.
    pub async fn get_raw_result_record(
        &self,
        task_id: Uuid,
        timeout: Option<Duration>,
    ) -> DatabaseResult<RawResultRead> {
        let start = Instant::now();
        let limit =
            WaitLimit::parse(timeout).map_err(|refused| invalid_input(refused.to_string()))?;
        match Box::pin(self.raw_result_probe(task_id)).await? {
            RawResultProbe::Record(record) => return Ok(RawResultRead::Found(record)),
            RawResultProbe::Absent(reason) => return Ok(RawResultRead::Absent { reason }),
            RawResultProbe::Waiting(_) | RawResultProbe::Retry => {}
        }

        let mut wait_mode = self.result_wait_mode(task_id).await;

        match Box::pin(self.raw_result_probe(task_id)).await? {
            RawResultProbe::Record(record) => return Ok(RawResultRead::Found(record)),
            RawResultProbe::Absent(reason) => return Ok(RawResultRead::Absent { reason }),
            RawResultProbe::Waiting(_) | RawResultProbe::Retry => {}
        }

        let deadline = limit.deadline_from(start);
        loop {
            if deadline.is_some_and(|at| Instant::now() >= at) {
                return match Box::pin(self.raw_result_probe(task_id)).await? {
                    RawResultProbe::Record(record) | RawResultProbe::Waiting(record) => {
                        Ok(RawResultRead::Found(record))
                    }
                    RawResultProbe::Absent(reason) => Ok(RawResultRead::Absent { reason }),
                    RawResultProbe::Retry => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                };
            }
            let interval = wait_mode.repoll_interval();
            let wait = deadline
                .map(|at| at.saturating_duration_since(Instant::now()).min(interval))
                .unwrap_or(interval);
            wait_mode.pause(wait).await;
            match Box::pin(self.raw_result_probe(task_id)).await? {
                RawResultProbe::Record(record) => return Ok(RawResultRead::Found(record)),
                RawResultProbe::Absent(reason) => return Ok(RawResultRead::Absent { reason }),
                RawResultProbe::Waiting(_) | RawResultProbe::Retry => {}
            }
        }
    }

    /// Subscribe a result wait on the broker's listener: a wake for this task
    /// id on `task_done`. A broker with no listener, or a refused
    /// subscription, makes the wait poll instead, as Python
    /// (`clients/python/horsies/core/brokers/postgres.py:3276-3281`).
    async fn result_wait_mode(&self, task_id: Uuid) -> ResultWaitMode {
        let filter = PayloadFilter::Payload(task_id.to_string());
        match self
            .listener
            .subscribe_wake(fixed_channel(TASK_DONE_CHANNEL), filter)
            .await
        {
            Ok(subscription) => ResultWaitMode::Subscribed(subscription),
            Err(error @ ListenError::NoSessionUrl) => {
                tracing::debug!(%task_id, error = %error, "the result wait polls: {error}");
                ResultWaitMode::Polling
            }
            Err(error @ (ListenError::PlanRefused(_) | ListenError::Subscribe(_))) => {
                tracing::warn!(
                    %task_id,
                    error = %error,
                    "LISTEN unavailable; falling back to polling for task_done. Original error: \
                     {error}",
                );
                ResultWaitMode::Polling
            }
        }
    }

    /// Fetch task metadata.
    ///
    /// By default, the `result` and `failed_reason` fields are excluded:
    /// `result` is `ResultSlot::NotRequested` and `failed_reason` is `None`,
    /// matching Python's `info(include_result=False,
    /// include_failed_reason=False)` default. Pass `true` for either flag to
    /// include the field of a terminal task's history row. A live task has no
    /// result, failed reason, error code or terminal time: with the result
    /// flag `result` is `ResultSlot::NotStored`, and the other fields are
    /// `None`, whatever the flags say.
    pub async fn get_task_info(
        &self,
        task_id: Uuid,
        include_result: bool,
        include_failed_reason: bool,
    ) -> DatabaseResult<TaskInfoRead> {
        self.get_task_info_with_attempts(task_id, include_result, include_failed_reason, false)
            .await
    }

    /// Fetch task metadata and optionally compose execution attempts from the
    /// live attempt table or the immutable history snapshot.
    ///
    /// A task that neither the live table nor the task history holds is
    /// [`TaskInfoRead::Absent`] with the reason of the history read. When
    /// the live read finds no row and the history read then finds the task
    /// live (a send of that id committed between the two statements), the
    /// read runs again, at most [`TASK_INFO_READ_PASSES`] times in all; a
    /// task that moves on every pass is `TASK_INFO_QUERY_FAILED`, retryable.
    pub async fn get_task_info_with_attempts(
        &self,
        task_id: Uuid,
        include_result: bool,
        include_failed_reason: bool,
        include_attempts: bool,
    ) -> DatabaseResult<TaskInfoRead> {
        for _pass in 0..TASK_INFO_READ_PASSES {
            match Box::pin(self.task_info_pass(
                task_id,
                include_result,
                include_failed_reason,
                include_attempts,
            ))
            .await?
            {
                TaskInfoPass::Read(read) => return Ok(read),
                TaskInfoPass::BecameLive => {}
            }
        }
        Err(DatabaseOperationError::task_info_query_failed(
            task_id,
            BrokerOperationSource::TaskInfoMoved {
                task_id,
                passes: TASK_INFO_READ_PASSES,
            },
        ))
    }

    /// One pass of [`Self::get_task_info_with_attempts`]: the live row, else
    /// the history record or the absence reason.
    async fn task_info_pass(
        &self,
        task_id: Uuid,
        include_result: bool,
        include_failed_reason: bool,
        include_attempts: bool,
    ) -> DatabaseResult<TaskInfoPass> {
        let row = self
            .read_main(
                &GET_TASK_INFO.bind().value(&task_id).finish(),
                task_info_row,
            )
            .await
            .map_err(|error| {
                DatabaseOperationError::task_info_query_failed(
                    task_id,
                    BrokerOperationSource::Broker(error),
                )
            })?
            .into_iter()
            .next();

        match row {
            Some(r) => {
                let mut info = r
                    .into_task_info(include_result)
                    .map_err(|error| task_info_history_error(task_id, error))?;
                if include_attempts {
                    info.attempts = Some(
                        self.get_task_attempts(task_id)
                            .await
                            .map_err(|error| task_info_history_error(task_id, error))?
                            .into_iter()
                            .map(task_attempt_info_from_row)
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|error| task_info_history_error(task_id, error))?,
                    );
                }
                Ok(TaskInfoPass::Read(TaskInfoRead::Found(Box::new(info))))
            }
            None => {
                let detail = Box::pin(self.fetch_history_detail(task_id))
                    .await
                    .map_err(|error| task_info_history_error(task_id, error))?;
                match detail {
                    TaskDetailResult::History(detail) => history_task_info(
                        *detail,
                        include_result,
                        include_failed_reason,
                        include_attempts,
                    )
                    .map(|info| TaskInfoPass::Read(TaskInfoRead::Found(Box::new(info)))),
                    TaskDetailResult::Live { .. } => Ok(TaskInfoPass::BecameLive),
                    TaskDetailResult::Absent { reason, .. } => {
                        Ok(TaskInfoPass::Read(TaskInfoRead::Absent { reason }))
                    }
                }
            }
        }
    }

    /// The claimed tasks of a claim pass that must not be dispatched because
    /// their workflow is PAUSED or CANCELLED, after the settle wrote what
    /// each of them needs, through
    /// `horsies_workflow_claimed_tasks_settle`
    /// ([`crate::broker::workflow_claim_settle`]):
    /// - PAUSED: this worker's claim at its claim generation is abandoned,
    ///   and the node of an abandoned task goes back to READY with no task;
    /// - CANCELLED: the claim is cancelled, and the node of a cancelled task
    ///   is skipped.
    ///
    /// The workflow status is read under the workflow row lock, so the
    /// tasks of a workflow that a resume made RUNNING first are dispatched.
    /// A blocked task is never dispatched, whether its move applied or found
    /// the row moved on: that row belongs to its current owner. The moves are
    /// fenced to `worker_id` and each claim's generation, so one worker never
    /// ends another worker's claim, or a later claim of its own.
    ///
    /// Returns the blocked task ids, in input order. One statement per
    /// [`crate::broker::workflow_claim_settle::MAX_SETTLED_CLAIMS`] claims,
    /// each its own transaction, on the main core pool. No claim sends no
    /// statement.
    ///
    /// Errors: [`BrokerError::PoolSettings`] when a closed handle cannot open
    /// its pools again; [`BrokerError::Statement`];
    /// [`BrokerError::RowDecode`]; [`BrokerError::ClaimContract`] for rows
    /// that do not answer each claim once, in order. The statements before a
    /// failed one stay committed.
    pub async fn filter_non_runnable_workflow_tasks(
        &self,
        claims: &[(Uuid, DateTime<Utc>)],
        worker_id: &str,
    ) -> Result<Vec<Uuid>, BrokerError> {
        if claims.is_empty() {
            return Ok(Vec::new());
        }
        let (open, _write) = self.core_pools_for_write()?;
        let settled =
            settle_claimed_workflow_tasks(open.main(), self.statement_log, worker_id, claims)
                .await?;
        Ok(settled
            .into_iter()
            .filter_map(|(task_id, disposition)| match disposition {
                ClaimDisposition::Blocked => Some(task_id),
                ClaimDisposition::Dispatch => None,
            })
            .collect())
    }

    /// Close the broker's pools.
    ///
    /// - Core pools: no new operation takes them. Writes in flight run to
    ///   their end within [`crate::broker::pools::CLOSE_WRITE_BOUND`]; then
    ///   the pools close and the handle is `Idle`. The next operation opens
    ///   new core pools. A read in flight that checks out again fails with
    ///   `PoolClosed`.
    ///
    /// Writes that did not end within the bound are logged with their count;
    /// their outcome is unknown.
    pub async fn close(&self) {
        match self.core.close().await {
            CloseOutcome::AlreadyIdle | CloseOutcome::Closed => {}
            CloseOutcome::ClosedWithWritesUnfinished { unfinished } => {
                tracing::warn!(
                    unfinished,
                    bound_ms = crate::broker::pools::CLOSE_WRITE_BOUND.as_millis() as u64,
                    "broker close: writes in flight did not end within the bound; their \
                     outcome is unknown",
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Monitoring / observability queries
    // -----------------------------------------------------------------------

    /// Verify runtime-pool connectivity with a cheap `SELECT 1` query.
    ///
    /// Returns `Ok(())` if the database is reachable, `Err(BrokerError)` otherwise.
    /// Mirrors Python's `HEALTH_CHECK_SQL` used by `horsies check --live`.
    ///
    /// This does not check LISTEN/NOTIFY. Workers run
    /// [`check_worker_session`](Self::check_worker_session) at every boot.
    pub async fn health_check(&self) -> Result<(), BrokerError> {
        let rows = self.query_main(&HEALTH_CHECK.bind().finish()).await?;
        let _row = one_row(rows, &HEALTH_CHECK)?;
        Ok(())
    }

    /// Active database liveness probe: a timed `SELECT 1` round-trip.
    ///
    /// Returns a [`DatabasePing`] carrying the measured latency, or a
    /// `DATABASE_PING_FAILED` broker error. Unlike [`health_check`](Self::health_check)
    /// (which callers use as a plain reachability gate), this surfaces latency
    /// for monitoring. Mirrors Python's `ping_database_async`.
    pub async fn ping_database(&self) -> DatabaseResult<DatabasePing> {
        let start = std::time::Instant::now();
        let probe = self
            .query_main(&HEALTH_CHECK.bind().finish())
            .await
            .and_then(|rows| one_row(rows, &HEALTH_CHECK).map_err(BrokerError::RowDecode));
        match probe {
            Ok(_row) => Ok(DatabasePing {
                latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            }),
            Err(error) => Err(DatabaseOperationError::database_ping_failed(error)),
        }
    }

    /// Active worker liveness probe: broadcast a ping and collect pongs.
    ///
    /// Subscribes to a unique reply channel, broadcasts a [`WorkerPingRequest`]
    /// on [`WORKER_PING_CHANNEL`], then collects distinct [`WorkerPong`] replies
    /// (de-duplicated by `worker_id`) until a stop condition:
    ///
    /// - `target_worker_id` set: returns as soon as that worker replies.
    /// - `min_responses` set: returns as soon as that many distinct workers
    ///   reply (fast fail-open liveness, e.g. `Some(1)` for a `/health` gate — a
    ///   healthy fleet answers in milliseconds; only a degraded fleet pays the
    ///   full `wait`).
    /// - neither set: waits the full window and enumerates every responder.
    ///
    /// A pong proves the replying worker's event loop is responsive *and* that
    /// it can reach Postgres. Workers present in
    /// [`list_worker_states`](Self::list_worker_states) but absent here are
    /// non-responsive. Mirrors Python's `ping_workers_async`.
    ///
    /// The replies arrive on a payload subscription of the reply channel on
    /// the broker's listener; the drop of the subscription UNLISTENs it. A
    /// broker with no listener is `LISTENER_DISABLED`, not retryable. A
    /// refused subscription is `WORKER_PING_FAILED` with the listener's
    /// retryable flag. Replies lost to a listener gap or a full reply queue
    /// are not waited for: the window goes on. The window starts when the
    /// ping NOTIFY has committed, as Python's. A reply that does not decode
    /// is logged and left out, as Python's (`_decode_pong`).
    pub async fn ping_workers(
        &self,
        target_worker_id: Option<&str>,
        wait: std::time::Duration,
        min_responses: Option<usize>,
    ) -> DatabaseResult<Vec<WorkerPong>> {
        let wait = checked_ping_wait(wait)?;
        if min_responses == Some(0) {
            return Err(ping_min_responses_refused(&0));
        }

        let correlation_id = Uuid::new_v4().simple().to_string();
        let reply_channel = fixed_channel(&format!("{WORKER_PONG_CHANNEL_PREFIX}{correlation_id}"));

        let replies = self
            .listener
            .subscribe_payloads(reply_channel.clone(), default_payload_capacity())
            .await
            .map_err(worker_ping_subscribe_error)?;

        let request = WorkerPingRequest {
            correlation_id: correlation_id.clone(),
            reply_channel: reply_channel.as_str().to_owned(),
            target_worker_id: target_worker_id.map(str::to_owned),
        };
        let payload = serde_json::to_string(&request).map_err(|error| {
            DatabaseOperationError::worker_ping_failed(
                WorkerPingStep::PayloadEncode,
                BrokerOperationSource::Broker(BrokerError::Serialization(error)),
            )
        })?;

        let _notified = self
            .execute_main(
                &NOTIFY_WORKER_PING
                    .bind()
                    .value(&WORKER_PING_CHANNEL)
                    .value(&payload)
                    .finish(),
            )
            .await
            .map_err(worker_ping_statement_error)?;
        // The window and each round trip start when the NOTIFY has committed
        // (Python's `sent_at`); a reply that arrived before waits in the
        // reply queue.
        let start = std::time::Instant::now();

        let mut pongs: Vec<WorkerPong> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        while let Some(remaining) = wait.checked_sub(start.elapsed()) {
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, replies.recv()).await {
                Err(_elapsed) => break, // window closed
                // Replies sent while the listener reconnected, or dropped by
                // a full reply queue, are lost; the window goes on.
                Ok(PayloadEvent::Gap | PayloadEvent::Overflowed) => continue,
                Ok(PayloadEvent::Stopped) => {
                    return Err(DatabaseOperationError::worker_ping_failed(
                        WorkerPingStep::ReplyWait,
                        BrokerOperationSource::Broker(BrokerError::ListenerClosed),
                    ));
                }
                Ok(PayloadEvent::Payload(reply)) => {
                    let Some(pong) = decode_pong(&reply, &correlation_id, start.elapsed()) else {
                        continue; // malformed or correlation-id mismatch
                    };
                    if !seen.insert(pong.worker_id.clone()) {
                        continue; // duplicate worker
                    }
                    let is_target = target_worker_id == Some(pong.worker_id.as_str());
                    pongs.push(pong);
                    if is_target {
                        break;
                    }
                    if min_responses.is_some_and(|n| pongs.len() >= n) {
                        break;
                    }
                }
            }
        }

        Ok(pongs)
    }

    /// The session check of a worker boot, on the statement layer: both core
    /// pools keep a transaction, and the session URL keeps session state (a
    /// LISTEN on it receives a NOTIFY). Runs at every worker boot, with or
    /// without PgBouncer mode (Python `validate_worker_database_paths`,
    /// `clients/python/horsies/core/brokers/postgres.py:1383-1412`).
    ///
    /// Steps:
    /// 1. `SELECT 1` on the session core pool when the session URL differs
    ///    from the main URL. Its first checkout runs the statement-pooling
    ///    check of that pool.
    /// 2. Subscribe a wake on `horsies_check_<32 hex>` on the broker's
    ///    listener, the connection that carries the claim wakes and the pings.
    /// 3. One autocommit `SELECT pg_notify($1, 'ok')` on the main core pool.
    ///    Its first checkout runs the statement-pooling check of the main
    ///    pool.
    /// 4. Wait at most `SESSION_CAPABILITY_TIMEOUT` (2 s) for the NOTIFY.
    ///
    /// Steps 2 to 4 are the LISTEN probe that the live check also runs
    /// ([`PostgresBroker::check_connectivity`]).
    ///
    /// The subscription is dropped at the end, and the listener UNLISTENs the
    /// channel. Nothing is written.
    ///
    /// Outcomes: `Ok` when the NOTIFY arrives. Not retryable: `NotCapable`
    /// (no NOTIFY in time), `PoolSettings`, and `SessionPool` or `Notify`
    /// with a permanent failure (a statement-pooled URL). `Subscribe`,
    /// `SessionPool`, `Notify` and `Interrupted` otherwise carry their retry
    /// rule (a database that cannot be reached is retryable).
    pub async fn check_worker_session(&self) -> Result<(), SessionCheckFailure> {
        let open = self
            .core
            .pools()
            .map_err(SessionCheckFailure::PoolSettings)?;
        self.check_session_pool(open.pools().session()).await?;
        self.probe_listen_delivery(&self.listener, &open)
            .await
            .map_err(SessionCheckFailure::from)
    }

    /// Step 1 of [`PostgresBroker::check_worker_session`]: `SELECT 1` on the
    /// session core pool when the session URL differs from the main URL;
    /// nothing when it does not (the main pool's check runs at the NOTIFY).
    async fn check_session_pool(&self, session: &SessionPool) -> Result<(), SessionCheckFailure> {
        match session {
            SessionPool::SameAsMain => Ok(()),
            SessionPool::Separate { pool, .. } => {
                self.log_statement(&SESSION_CHECK);
                let _selected = pool
                    .execute(
                        CheckoutClass::Batch,
                        &SESSION_CHECK.bind().finish(),
                        StatementBound::ConnectionTimeouts,
                    )
                    .await
                    .map_err(SessionCheckFailure::SessionPool)?;
                Ok(())
            }
        }
    }

    /// RUNNING tasks whose latest runner beat, or their start when they have
    /// no beat, is older than `threshold_minutes`. Read-only: the reaper
    /// recovers stale tasks.
    ///
    /// A threshold outside 0 to one year is `INVALID_INPUT` with no database
    /// call. At most [`TASK_LIST_BOUND`](crate::broker::task_lists::TASK_LIST_BOUND)
    /// rows come back; more is
    /// [`BoundedTaskList::Truncated`]. A database failure is
    /// `MONITORING_QUERY_FAILED` with the driver classification.
    /// Mirrors `PostgresBroker.get_stale_tasks()` in the Python library.
    pub async fn get_stale_tasks(
        &self,
        threshold_minutes: i32,
    ) -> DatabaseResult<BoundedTaskList<StaleTaskRow>> {
        let threshold =
            StaleMinutes::parse(i64::from(threshold_minutes)).map_err(|refused| refused.error())?;
        let minutes = threshold.minutes();
        let rows = self
            .read_main(
                &LIST_STALE_TASKS.bind().value(&minutes).finish(),
                stale_list_row,
            )
            .await
            .map_err(|error| monitoring_query_error("get_stale_tasks", error))?;
        bounded_list_from_rows(rows).map_err(|shape| {
            monitoring_query_error(
                "get_stale_tasks",
                list_shape_error(&LIST_STALE_TASKS, shape),
            )
        })
    }

    /// Latest state snapshot per worker (cluster-wide), including idle workers.
    ///
    /// Reads the `horsies_worker_states` timeseries, so every worker that has
    /// reported a snapshot appears regardless of current load — unlike the
    /// retired `get_worker_stats` (which counted RUNNING tasks only and missed
    /// idle workers). Mirrors Python's `list_worker_states_async`.
    ///
    /// A stored `slots_down`, `class_capacity` or map column of another shape
    /// fails the read: `MONITORING_QUERY_FAILED`, not retryable, naming the
    /// worker and the column (Python `postgres.py:2961-3034`).
    pub async fn list_worker_states(&self) -> DatabaseResult<Vec<WorkerStateSnapshot>> {
        let listed = self.list_worker_states_with_read_time().await?;
        Ok(listed.into_iter().map(|state| state.snapshot).collect())
    }

    /// [`list_worker_states`](Self::list_worker_states), with the database
    /// clock of the list statement on each entry (`read_at`,
    /// `statement_timestamp()`). The web worker list measures the snapshot
    /// age, the staleness and the uptime from it. Same statement, same
    /// errors; reads only.
    pub(crate) async fn list_worker_states_with_read_time(
        &self,
    ) -> DatabaseResult<Vec<ListedWorkerState>> {
        let rows = self
            .read_main(&LIST_WORKER_STATES.bind().finish(), listed_worker_state_row)
            .await
            .map_err(|error| monitoring_query_error("list_worker_states", error))?;
        read_listed_worker_states("list_worker_states", rows)
    }

    /// Latest state snapshot for one worker, or `None` if it has never reported.
    ///
    /// An unreadable stored row fails the read as in
    /// [`list_worker_states`](Self::list_worker_states). Mirrors Python's
    /// `get_worker_state_async`.
    pub async fn get_worker_state(
        &self,
        worker_id: &str,
    ) -> DatabaseResult<Option<WorkerStateSnapshot>> {
        let rows = self
            .read_main(
                &GET_WORKER_STATE_LATEST.bind().value(&worker_id).finish(),
                stored_worker_state,
            )
            .await
            .map_err(|error| monitoring_query_error("get_worker_state", error))?;
        rows.into_iter()
            .next()
            .map(|stored| read_worker_snapshot("get_worker_state", stored))
            .transpose()
    }

    /// State-snapshot history for one worker, newest first.
    ///
    /// `limit` of `None` returns all retained rows, at most
    /// [`WORKER_HISTORY_ROW_BOUND`](crate::broker::WORKER_HISTORY_ROW_BOUND);
    /// pass `Some(n)` to bound the fetch. A limit of 0 or less, or above the
    /// bound, is `INVALID_INPUT`, with no database call. A worker that holds
    /// more retained rows than the bound refuses the read of `None` with
    /// `INVALID_INPUT`: no rows are returned in part. An unreadable stored row fails the read as in
    /// [`list_worker_states`](Self::list_worker_states). Mirrors Python's
    /// `get_worker_state_history_async`.
    pub async fn get_worker_state_history(
        &self,
        worker_id: &str,
        limit: Option<i64>,
    ) -> DatabaseResult<Vec<WorkerStateSnapshot>> {
        let limit = checked_history_limit(limit)?;
        let fetch_limit = history_fetch_limit(limit);
        let rows = self
            .read_main(
                &GET_WORKER_STATE_HISTORY
                    .bind()
                    .value(&worker_id)
                    .value(&fetch_limit)
                    .finish(),
                stored_worker_state,
            )
            .await
            .map_err(|error| monitoring_query_error("get_worker_state_history", error))?;
        let rows = checked_history_rows(limit, rows)?;
        read_worker_snapshots("get_worker_state_history", rows)
    }

    /// Find PENDING tasks that have expired (past their `good_until` deadline).
    ///
    /// These tasks will never be picked up by workers because the claim SQL
    /// filters on `good_until > now()`. Surfacing them helps operators detect
    /// under-provisioned queues or misconfigured deadlines.
    ///
    /// At most [`TASK_LIST_BOUND`](crate::broker::task_lists::TASK_LIST_BOUND)
    /// rows come back, oldest deadline first;
    /// more is [`BoundedTaskList::Truncated`]. A database failure is
    /// `MONITORING_QUERY_FAILED` with the driver classification.
    /// Mirrors `PostgresBroker.get_expired_tasks()` in the Python library.
    pub async fn get_expired_tasks(&self) -> DatabaseResult<BoundedTaskList<ExpiredTaskRow>> {
        let rows = self
            .read_main(&LIST_EXPIRED_TASKS.bind().finish(), expired_list_row)
            .await
            .map_err(|error| monitoring_query_error("get_expired_tasks", error))?;
        bounded_list_from_rows(rows).map_err(|shape| {
            monitoring_query_error(
                "get_expired_tasks",
                list_shape_error(&LIST_EXPIRED_TASKS, shape),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// One probe of the raw result wait: one statement through the staged result function
    /// ([`read_task_result`], with the broker's [`ResultReaderSeen`]). While
    /// that function is not published, the live probe and the staged detail
    /// read run instead ([`Self::raw_result_probe_unstaged`]). An absence
    /// from the function is confirmed by the detail reader: a reader
    /// published before a leaf existed does not probe that leaf. A function
    /// dropped out of band after this broker saw it fails one probe with the
    /// `42883` refusal that names it (`TASK_INFO_QUERY_FAILED`); the next
    /// probe checks the publication again and falls back.
    async fn raw_result_probe(&self, task_id: Uuid) -> DatabaseResult<RawResultProbe> {
        let open = self
            .core
            .pools()
            .map_err(|error| raw_result_operation_error(task_id, BrokerError::from(error)))?;
        let read = read_task_result(
            open.main(),
            self.statement_log,
            &self.result_reader,
            task_id,
        )
        .await
        .map_err(|error| raw_result_operation_error(task_id, error))?;
        match read {
            TaskResultRead::Live(live) => {
                raw_result_from_live(task_id, live.task_name, &live.status)
            }
            TaskResultRead::History(history) => raw_result_from_history_result(task_id, history),
            TaskResultRead::Absent => self.history_raw_result_probe(task_id).await,
            TaskResultRead::NotPublished => self.raw_result_probe_unstaged(task_id).await,
        }
    }

    /// The probe of a schema whose staged result function is not published.
    /// A live row holds no result, so the live read takes the id, name and
    /// status only, as Python's `_probe_live_result_row`; a row that is not
    /// live is read from history.
    async fn raw_result_probe_unstaged(&self, task_id: Uuid) -> DatabaseResult<RawResultProbe> {
        let live = self
            .read_main(
                &GET_RAW_RESULT_PROBE.bind().value(&task_id).finish(),
                raw_result_probe_row,
            )
            .await
            .map_err(|error| raw_result_operation_error(task_id, error))?;
        let Some(row) = live.into_iter().next() else {
            return self.history_raw_result_probe(task_id).await;
        };
        raw_result_from_live(row.id, row.task_name, &row.status)
    }

    async fn history_raw_result_probe(&self, task_id: Uuid) -> DatabaseResult<RawResultProbe> {
        let detail = self
            .fetch_history_detail(task_id)
            .await
            .map_err(|error| raw_result_operation_error(task_id, error))?;
        raw_result_from_history_detail(task_id, detail)
    }

    async fn fetch_history_detail(&self, task_id: Uuid) -> Result<TaskDetailResult, BrokerError> {
        let open = self.core.pools()?;
        read_history_detail(open.main(), self.statement_log, task_id).await
    }
}

/// The keyed-send reservation window in microseconds, the unit of the
/// `interval` that the key claim stores. `None` selects the 24-hour default.
///
/// Errors: [`BrokerError::InvalidIdempotencyReservationWindow`] for a window
/// that is not positive, is over 30 days, or is not a whole number of
/// microseconds.
fn idempotency_reservation_window_micros(
    window: Option<chrono::Duration>,
) -> Result<i64, BrokerError> {
    let window =
        window.unwrap_or_else(|| chrono::Duration::hours(IDEMPOTENCY_WINDOW_DEFAULT_HOURS));
    reservation_window_micros(window)
        .map_err(|error| BrokerError::InvalidIdempotencyReservationWindow(error.to_string()))
}

/// The rerun error of a client-family check that did not pass.
fn rerun_family_error(error: FamilyCheckError) -> RerunError {
    match error {
        FamilyCheckError::Mismatch(mismatch) => RerunError::ClientFamilyMismatch {
            stored: mismatch.stored,
            own: mismatch.own,
        },
        FamilyCheckError::UnknownStoredFamily(unknown) => RerunError::UnknownStoredFamily(unknown),
        FamilyCheckError::ReadFailed(failure) => RerunError::FamilyCheckFailed(failure),
    }
}

#[cfg(test)]
mod rerun_family_error_tests {
    use super::rerun_family_error;
    use crate::broker::client_family::{
        ClientFamily, FamilyCheckError, FamilyCheckFailure, FamilyMismatch, RecordedFamily,
        UnknownStoredFamily,
    };
    use crate::core::history::rerun::RerunError;
    use crate::db::{NotSentCause, StatementFailure, Transience};

    /// A family row whose value is not a client family is the rerun's own
    /// named error, with the stored value.
    #[test]
    fn an_unknown_stored_family_is_the_rerun_unknown_stored_family() {
        let unknown = UnknownStoredFamily {
            stored: "GO".to_owned(),
        };
        let error = rerun_family_error(FamilyCheckError::UnknownStoredFamily(unknown.clone()));
        assert!(
            matches!(&error, RerunError::UnknownStoredFamily(inner) if *inner == unknown),
            "{error:?}"
        );
    }

    #[test]
    fn a_family_mismatch_is_the_rerun_mismatch() {
        let error = rerun_family_error(FamilyCheckError::Mismatch(FamilyMismatch {
            stored: RecordedFamily::NotRecorded,
            own: ClientFamily::Rust,
        }));
        assert!(matches!(
            error,
            RerunError::ClientFamilyMismatch {
                stored: RecordedFamily::NotRecorded,
                own: ClientFamily::Rust,
            }
        ));
    }

    /// A failed family read is a rerun error, never a panic, and keeps the
    /// retry class of its statement.
    #[test]
    fn a_failed_family_read_is_a_rerun_error_with_its_retry_class() {
        let error = rerun_family_error(FamilyCheckError::ReadFailed(
            FamilyCheckFailure::Statement(StatementFailure::NotSent {
                statement: "read_client_family",
                cause: NotSentCause::ConnectionClosed,
            }),
        ));
        match error {
            RerunError::FamilyCheckFailed(failure) => {
                assert!(matches!(failure, FamilyCheckFailure::Statement(_)));
                assert_eq!(failure.transience(), Transience::Transient);
            }
            other => panic!("expected FamilyCheckFailed, got {other:?}"),
        }
    }
}

/// The task info code of a read error, by Python's rule
/// (`clients/python/horsies/core/brokers/postgres.py:4889-4904`, `:4973-4977`): only a
/// stored result that fails its check is `INVALID_JSON_PAYLOAD`; a status or
/// an attempt snapshot that does not decode, and a database failure, are
/// `TASK_INFO_QUERY_FAILED`, retryable by the driver classification. A
/// history result is checked in [`history_task_info`]; the live row holds no
/// result.
fn task_info_history_error(task_id: Uuid, error: BrokerError) -> DatabaseOperationError {
    match error {
        unreadable @ BrokerError::Serialization(_) => {
            let detail = unreadable.to_string();
            DatabaseOperationError::task_info_invalid_json(
                task_id,
                &detail,
                BrokerCheckCause::Error(unreadable),
            )
        }
        other => DatabaseOperationError::task_info_query_failed(
            task_id,
            BrokerOperationSource::Broker(other),
        ),
    }
}

#[derive(Debug)]
struct RawResultProbeRow {
    id: Uuid,
    task_name: String,
    status: String,
}

fn raw_result_probe_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<RawResultProbeRow, RowDecodeError> {
    Ok(RawResultProbeRow {
        id: column(row, statement, "id")?,
        task_name: column(row, statement, "task_name")?,
        status: column(row, statement, "status")?,
    })
}

enum RawResultProbe {
    Record(RawResultRecord),
    Waiting(RawResultRecord),
    Retry,
    Absent(TaskAbsenceReason),
}

/// The passes of a task info read whose live read and history read race a
/// send of the same id.
const TASK_INFO_READ_PASSES: u32 = 3;

/// The end of one task info pass.
enum TaskInfoPass {
    Read(TaskInfoRead),
    /// The live read found no row, and the history read found the task
    /// live: a send of the id committed between the two statements.
    BecameLive,
}

/// The probe of a live row: a terminal status is a record with no result (a
/// live row holds none), a live status waits. A status that is not a task
/// status is `INVALID_JSON_PAYLOAD`. Pure.
fn raw_result_from_live(
    task_id: Uuid,
    task_name: String,
    status: &str,
) -> DatabaseResult<RawResultProbe> {
    let status: crate::core::TaskStatus = status
        .parse()
        .map_err(|error| raw_result_operation_error(task_id, BrokerError::InvalidStatus(error)))?;
    let record = RawResultRecord {
        task_id,
        task_name,
        status,
        raw_result: None,
    };
    Ok(match status {
        crate::core::TaskStatus::Completed
        | crate::core::TaskStatus::Failed
        | crate::core::TaskStatus::Cancelled
        | crate::core::TaskStatus::Expired => RawResultProbe::Record(record),
        crate::core::TaskStatus::Pending
        | crate::core::TaskStatus::Claimed
        | crate::core::TaskStatus::Running => RawResultProbe::Waiting(record),
    })
}

/// The record of a history row read through the staged result function,
/// with the rules of [`raw_result_from_history_detail`]: a terminal status,
/// and a stored result that passes its version, digest and UTF-8 checks and
/// is a JSON object or `null`. A failed check is `INVALID_JSON_PAYLOAD`.
/// Pure.
fn raw_result_from_history_result(
    task_id: Uuid,
    history: HistoryTaskResult,
) -> DatabaseResult<RawResultProbe> {
    let status: crate::core::TaskStatus = history
        .status
        .parse()
        .map_err(|error| raw_result_operation_error(task_id, BrokerError::InvalidStatus(error)))?;
    if !status.is_terminal() {
        return Err(raw_result_operation_error(
            task_id,
            BrokerError::InvalidStatus(format!(
                "history task {task_id} has non-terminal status {status}"
            )),
        ));
    }
    let text = match &history.payload {
        HistoryResultPayload::Absent => None,
        HistoryResultPayload::Stored { payload, digest } => Some(
            decode_result_envelope_text(
                history.envelope_version,
                &history.codec,
                &history.content_type,
                payload,
                digest,
            )
            .map_err(|error| {
                raw_result_operation_error(
                    task_id,
                    BrokerError::HistoryReadContract(error.to_string()),
                )
            })?,
        ),
    };
    let raw_result = text.map(stored_envelope_text).transpose()?.flatten();
    Ok(RawResultProbe::Record(RawResultRecord {
        task_id,
        task_name: history.task_name,
        status,
        raw_result,
    }))
}

fn raw_result_from_history_detail(
    task_id: Uuid,
    detail: TaskDetailResult,
) -> DatabaseResult<RawResultProbe> {
    match detail {
        TaskDetailResult::Absent { reason, .. } => Ok(RawResultProbe::Absent(reason)),
        TaskDetailResult::Live { .. } => Ok(RawResultProbe::Retry),
        TaskDetailResult::History(detail) => {
            // Parsed at the history read (`HistoryTaskDetail.status`).
            let status: crate::core::TaskStatus = detail.status;
            if !status.is_terminal() {
                return Err(raw_result_operation_error(
                    task_id,
                    BrokerError::InvalidStatus(format!(
                        "history task {task_id} has non-terminal status {status}"
                    )),
                ));
            }
            let raw_result = history_result_text(&detail)
                .map_err(|error| raw_result_operation_error(task_id, error))?
                .map(stored_envelope_text)
                .transpose()?
                .flatten();
            Ok(RawResultProbe::Record(RawResultRecord {
                task_id: detail.task_id,
                task_name: detail.task_name,
                status,
                raw_result,
            }))
        }
    }
}

/// The envelope text of a stored result: `None` for the JSON `null`; any
/// other value than an object is `INVALID_JSON_PAYLOAD`.
fn stored_envelope_text(text: String) -> DatabaseResult<Option<StoredEnvelopeText>> {
    StoredEnvelopeText::parse(text).map_err(|refused| invalid_raw_result(refused.to_string()))
}

/// `INVALID_JSON_PAYLOAD` of a raw result read: a check of the core refused
/// the stored result; `message` names the refusal.
fn invalid_raw_result(message: String) -> DatabaseOperationError {
    DatabaseOperationError::InvalidJsonPayload {
        message,
        cause: Box::new(BrokerCheckCause::Check),
    }
}

/// The error of a raw result read of `task_id`, by Python's rule: stored
/// data that does not read is `INVALID_JSON_PAYLOAD` with the error's text;
/// every other failure is `TASK_INFO_QUERY_FAILED` with Python's message
/// (`Broker error while retrieving task {task_id}: {error}`), retryable by
/// the error's rule.
fn raw_result_operation_error(task_id: Uuid, error: BrokerError) -> DatabaseOperationError {
    match error {
        unreadable @ (BrokerError::Serialization(_)
        | BrokerError::HistoryReadContract(_)
        | BrokerError::InvalidStatus(_)) => DatabaseOperationError::InvalidJsonPayload {
            message: unreadable.to_string(),
            cause: Box::new(BrokerCheckCause::Error(unreadable)),
        },
        other => DatabaseOperationError::raw_result_query_failed(task_id, other),
    }
}

/// The stored result of a history row for a task info read: `None` when the
/// row stores no result or the JSON `null`, else the envelope text as
/// stored. Pure.
///
/// Errors: `INVALID_JSON_PAYLOAD`, not retryable, for an absent digest, an
/// envelope that fails its version, digest or strict JSON check, and a value
/// other than an object or `null`.
fn history_result_envelope(
    detail: &HistoryTaskDetail,
) -> DatabaseResult<Option<StoredEnvelopeText>> {
    let checked = match history_result_text(detail) {
        Ok(None) => Ok(None),
        Ok(Some(text)) => StoredEnvelopeText::parse(text).map_err(|refused| refused.to_string()),
        Err(error) => Err(error.to_string()),
    };
    checked.map_err(|cause| {
        let detail_text = format!(
            "History result envelope failed validation for task {}: {cause}",
            detail.task_id
        );
        DatabaseOperationError::task_info_invalid_json(
            detail.task_id,
            &detail_text,
            BrokerCheckCause::Check,
        )
    })
}

/// The text of the stored result of a history row, as stored: an integer
/// keeps every digit (`decode_result_envelope_text`).
fn history_result_text(detail: &HistoryTaskDetail) -> Result<Option<String>, BrokerError> {
    let Some(payload) = detail.result_payload.as_deref() else {
        return Ok(None);
    };
    let digest = detail.result_digest.as_deref().ok_or_else(|| {
        BrokerError::HistoryReadContract(format!(
            "history result digest is absent for task {}",
            detail.task_id
        ))
    })?;
    decode_result_envelope_text(
        detail.result_envelope_version,
        &detail.result_codec,
        &detail.result_content_type,
        payload,
        digest,
    )
    .map(Some)
    .map_err(|error| BrokerError::HistoryReadContract(error.to_string()))
}

/// The task info of a history row (Python `PostgresBroker._history_task_info`).
/// The stored result is checked as a result envelope (version, digest, strict
/// JSON, an object or `null`) and returned as its stored text, with no typed
/// decode: a client reads it with the task's types. A stored result that
/// fails the check is `INVALID_JSON_PAYLOAD`; a status or an attempt snapshot
/// that does not decode is `TASK_INFO_QUERY_FAILED`, not retryable (Python
/// `clients/python/horsies/core/brokers/postgres.py:4889-4904`, `:4973-4977`).
fn history_task_info(
    detail: HistoryTaskDetail,
    include_result: bool,
    include_failed_reason: bool,
    include_attempts: bool,
) -> DatabaseResult<TaskInfo<StoredEnvelopeText>> {
    let task_id = detail.task_id;
    let status = detail.status;
    let result = match include_result {
        false => ResultSlot::NotRequested,
        true => match history_result_envelope(&detail)? {
            None => ResultSlot::NotStored,
            Some(envelope) => ResultSlot::Stored(envelope),
        },
    };
    let terminal_at = detail.terminal_at;
    // The snapshot holds the attempts oldest first; a task info lists them
    // newest first, as the live read (`ORDER BY attempt DESC`) and Python's
    // history read (`reversed(detail.attempts)`).
    let attempts = include_attempts
        .then(|| {
            detail
                .attempts
                .iter()
                .rev()
                .map(|record| task_attempt_info_from_record(detail.task_id, record))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
        .map_err(|error| task_info_history_error(task_id, error))?;
    Ok(TaskInfo {
        task_id: detail.task_id,
        task_name: detail.task_name,
        status,
        // History column queue_name.
        queue: detail.queue_name,
        priority: detail.priority,
        retry_count: detail.retry_count as u32,
        max_retries: detail.max_retries as u32,
        next_retry_at: None,
        sent_at: detail.sent_at,
        // History column enqueued_at: the time the row became due.
        due_at: detail.enqueued_at,
        claimed_at: detail.claimed_at,
        started_at: detail.started_at,
        completed_at: (status == crate::core::TaskStatus::Completed).then_some(terminal_at),
        failed_at: (status != crate::core::TaskStatus::Completed).then_some(terminal_at),
        worker_hostname: detail.last_worker_hostname,
        worker_pid: detail
            .last_worker_pid
            .and_then(|pid| u32::try_from(pid).ok()),
        worker_process_name: None,
        error_code: detail.error_code,
        failed_reason: include_failed_reason
            .then_some(detail.final_failed_reason)
            .flatten(),
        result,
        attempts,
        // The history table has no `execution` column (Python reports `None`).
        execution: crate::core::ExecutionClass::NotKeptInHistory,
    })
}

fn task_attempt_info_from_row(
    row: TaskAttemptRow,
) -> Result<crate::core::TaskAttemptInfo, BrokerError> {
    Ok(crate::core::TaskAttemptInfo {
        task_id: row.task_id,
        attempt: row.attempt,
        outcome: parse_task_attempt_outcome(&row.outcome)?,
        will_retry: row.will_retry,
        started_at: row.started_at,
        finished_at: row.finished_at,
        error_code: row.error_code,
        error_message: row.error_message,
        failed_reason: row.failed_reason,
        worker_id: row.worker_id,
        worker_hostname: row.worker_hostname,
        worker_pid: row.worker_pid,
        worker_process_name: row.worker_process_name,
    })
}

fn task_attempt_info_from_record(
    task_id: Uuid,
    record: &crate::core::history::archive::attempts::AttemptRecord,
) -> Result<crate::core::TaskAttemptInfo, BrokerError> {
    Ok(crate::core::TaskAttemptInfo {
        task_id,
        attempt: record.attempt(),
        outcome: parse_task_attempt_outcome(record.outcome())?,
        will_retry: record.will_retry(),
        started_at: record.started_at(),
        finished_at: record.finished_at(),
        error_code: record.error_code().map(str::to_owned),
        error_message: record.error_message().map(str::to_owned),
        failed_reason: record.failed_reason().map(str::to_owned),
        worker_id: record.worker_id().map(str::to_owned),
        worker_hostname: record.worker_hostname().map(str::to_owned),
        worker_pid: record.worker_pid(),
        worker_process_name: record.worker_process_name().map(str::to_owned),
    })
}

fn parse_task_attempt_outcome(value: &str) -> Result<crate::core::TaskAttemptOutcome, BrokerError> {
    match value {
        "COMPLETED" => Ok(crate::core::TaskAttemptOutcome::Completed),
        "FAILED" => Ok(crate::core::TaskAttemptOutcome::Failed),
        "WORKER_FAILURE" => Ok(crate::core::TaskAttemptOutcome::WorkerFailure),
        "CANCELLED" => Ok(crate::core::TaskAttemptOutcome::Cancelled),
        other => Err(BrokerError::HistoryReadContract(format!(
            "unknown task-attempt outcome {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// A list function's rows that break its contract, as a row decode error.
fn list_shape_error(statement: &TypedStatement, shape: ListShapeError) -> BrokerError {
    BrokerError::RowDecode(RowDecodeError::Shape {
        statement: statement.name(),
        reason: shape.to_string(),
    })
}

/// Build a `MONITORING_QUERY_FAILED` broker error for a failed read query.
/// `retryable` is the classification of the cause, as Python's
/// `_operation_failed`.
fn monitoring_query_error(operation: &str, error: BrokerError) -> DatabaseOperationError {
    DatabaseOperationError::monitoring_query_failed(operation, BrokerOperationSource::Broker(error))
}

/// The error of a refused reply subscription of `ping_workers`, as Python
/// (`clients/python/horsies/core/brokers/postgres.py:2821-2865`): a broker with no listener
/// is `LISTENER_DISABLED`, not retryable; any other refusal is
/// `WORKER_PING_FAILED` with the listener's retryable flag.
fn worker_ping_subscribe_error(error: ListenError) -> DatabaseOperationError {
    match error {
        ListenError::NoSessionUrl => DatabaseOperationError::ListenerDisabled {
            message: format!("ping_workers needs a listener: {error}"),
        },
        ListenError::PlanRefused(_) | ListenError::Subscribe(_) => {
            DatabaseOperationError::worker_ping_failed(
                WorkerPingStep::ReplySubscribe,
                BrokerOperationSource::Listen(error),
            )
        }
    }
}

/// Build a `WORKER_PING_FAILED` broker error for the failed NOTIFY of a
/// ping, with Python's message (`ping_workers failed: {error}`). `retryable`
/// is the classification of the cause.
fn worker_ping_statement_error(error: BrokerError) -> DatabaseOperationError {
    DatabaseOperationError::worker_ping_failed(
        WorkerPingStep::Notify,
        BrokerOperationSource::Broker(error),
    )
}

/// Build an `INVALID_INPUT` broker error: an argument was refused before
/// anything ran, and the same argument is refused again.
fn invalid_input(message: String) -> DatabaseOperationError {
    DatabaseOperationError::invalid_input(message)
}

/// Decode a pong notification, discarding malformed or mismatched replies.
///
/// Returns `None` when the payload is unparseable or its `correlation_id` does
/// not match this ping round, so stray replies never pollute the result.
fn decode_pong(
    raw_payload: &str,
    correlation_id: &str,
    elapsed: std::time::Duration,
) -> Option<WorkerPong> {
    // Python's `_decode_pong`: text that is not JSON and an object that is
    // not a pong are logged; a value that is not an object and a reply of
    // another round are left out without a record.
    let body: serde_json::Value = match serde_json::from_str(raw_payload) {
        Ok(body) => body,
        Err(error) => {
            tracing::warn!("Discarding unparseable pong payload: {error}");
            return None;
        }
    };
    if !body.is_object() {
        return None;
    }
    let payload: WorkerPongPayload = match serde_json::from_value(body) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::warn!("Discarding invalid pong payload: {error}");
            return None;
        }
    };
    if payload.correlation_id != correlation_id {
        return None;
    }
    Some(WorkerPong {
        worker_id: payload.worker_id,
        hostname: payload.hostname,
        pid: payload.pid,
        round_trip_ms: elapsed.as_secs_f64() * 1000.0,
    })
}

use crate::worker::retry::stored_max_retries;

/// The `max_retries` that enqueue stores, by Python's rule
/// (`clients/python/horsies/core/codec/task_options.py`, `stored_max_retries`): no
/// options (`None`) and no `retry_policy` store 0; a policy stores its
/// `max_retries`. The options are read with the reader of the dispatch
/// ([`stored_max_retries`]), so a text that the worker would refuse is refused
/// here, before any statement: text that does not parse (an empty text
/// included), that is not a JSON object, or whose `retry_policy` does not fit
/// Python's `RetryPolicy` (a limit outside 1 to 20, a wrong field, no code) is
/// `EnqueueContract`, which a send reports as `VALIDATION_FAILED`. Other keys
/// are not judged here.
fn enqueue_max_retries(task_name: &str, task_options: Option<&str>) -> Result<i32, BrokerError> {
    stored_max_retries(task_options).map_err(|refusal| {
        BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
            SendPart::TaskOptions,
            format!("task_options refused for {task_name}: {refusal}"),
        ))
    })
}

/// The `timeout_ms` rule of a shared class (`check_shared_timeout`) for the
/// decoded options `options` of a new send of the class `execution`, as the
/// task options refusal (`InvalidExecution`). Only a new send applies it: a
/// schedule (`SendCommand::prepare`) and a rerun write such options, and
/// the worker refuses the run, as before this rule. Pure.
fn check_new_send_timeout(
    execution: crate::core::task::execution::Execution,
    options: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<(), BrokerError> {
    crate::core::task::execution::check_shared_timeout(
        execution,
        options.and_then(|object| object.get("timeout_ms")),
    )
    .map_err(|invalid| {
        BrokerError::EnqueueContract(EnqueueRefusal::from(
            EnqueuePreparationError::InvalidExecution(invalid),
        ))
    })
}

// ---------------------------------------------------------------------------
// Result wait
// ---------------------------------------------------------------------------

/// Repoll interval of a task result wait whose listener works: a lost
/// notification costs at most this long (Python's 5 s,
/// `clients/python/horsies/core/brokers/postgres.py:3285`).
pub(crate) const TASK_RESULT_REPOLL_SUBSCRIBED: Duration = Duration::from_secs(5);

/// Repoll interval of a task result wait with no listener (Python's 0.2 s,
/// `clients/python/horsies/core/brokers/postgres.py:3285`).
pub(crate) const TASK_RESULT_REPOLL_POLLING: Duration = Duration::from_millis(200);

/// How a task result wait learns that the task may have ended.
enum ResultWaitMode {
    /// A wake for the task id on `task_done`. The listener also sends a gap to
    /// every subscription after it reconnects, because notifications sent
    /// while it was down are lost; the wait then reads at once.
    Subscribed(WakeSubscription),
    /// No listener: the wait reads again at [`TASK_RESULT_REPOLL_POLLING`].
    Polling,
}

impl ResultWaitMode {
    /// The longest pause between two reads in this mode.
    fn repoll_interval(&self) -> Duration {
        match self {
            Self::Subscribed(_) => TASK_RESULT_REPOLL_SUBSCRIBED,
            Self::Polling => TASK_RESULT_REPOLL_POLLING,
        }
    }

    /// Pause until a wake for the task, a listener gap, or until `wait`
    /// passes. A listener that has stopped turns the wait into polling, so a
    /// stopped subscription never makes the loop spin.
    async fn pause(&mut self, wait: Duration) {
        match self {
            Self::Subscribed(subscription) => {
                match tokio::time::timeout(wait, subscription.recv()).await {
                    Ok(WakeEvent::Notified | WakeEvent::Gap) | Err(_) => {}
                    Ok(WakeEvent::Stopped) => {
                        tracing::warn!("the listener stopped; the result wait polls instead");
                        *self = Self::Polling;
                    }
                }
            }
            Self::Polling => tokio::time::sleep(wait).await,
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use crate::test_support::sql::TestPoolOptions;

    #[tokio::test]
    async fn a_broker_without_a_listener_waits_for_a_result_by_polling() {
        // No server listens on port 1; the session URL would give a
        // listener, and `without_listener` takes it away.
        let pool = TestPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies");
        let broker = PostgresBroker::from_test_url_and_session_url(
            pool.url(),
            "postgresql://horsies@127.0.0.1:1/horsies",
            crate::ClientFamily::Rust,
        )
        .without_listener();
        let mode = broker.result_wait_mode(Uuid::from_u128(7)).await;
        assert!(matches!(mode, ResultWaitMode::Polling));
        assert_eq!(mode.repoll_interval(), TASK_RESULT_REPOLL_POLLING);
    }

    #[test]
    fn idempotency_reservation_window_defaults_and_bounds_are_exact() {
        assert_eq!(
            idempotency_reservation_window_micros(None).unwrap(),
            86_400_000_000
        );
        assert_eq!(
            idempotency_reservation_window_micros(Some(chrono::Duration::days(30))).unwrap(),
            2_592_000_000_000,
        );
        assert_eq!(
            idempotency_reservation_window_micros(Some(chrono::Duration::milliseconds(1_500)))
                .unwrap(),
            1_500_000,
        );
        assert_eq!(
            idempotency_reservation_window_micros(Some(chrono::Duration::milliseconds(500)))
                .unwrap(),
            500_000,
        );
        for invalid in [
            chrono::Duration::zero(),
            chrono::Duration::seconds(-1),
            chrono::Duration::nanoseconds(1_500),
        ] {
            assert!(matches!(
                idempotency_reservation_window_micros(Some(invalid)),
                Err(BrokerError::InvalidIdempotencyReservationWindow(_)),
            ));
        }
        assert!(matches!(
            idempotency_reservation_window_micros(Some(
                chrono::Duration::days(30) + chrono::Duration::microseconds(1),
            )),
            Err(BrokerError::InvalidIdempotencyReservationWindow(_)),
        ));
    }

    /// The list rules (equal deadline is expired; the stale verdict) live in
    /// the baseline functions; the broker calls them.
    #[test]
    fn the_task_lists_call_the_baseline_list_functions() {
        assert!(LIST_EXPIRED_TASKS
            .sql()
            .contains("FROM horsies_list_expired_tasks()"));
        assert!(LIST_STALE_TASKS
            .sql()
            .contains("FROM horsies_list_stale_tasks($1)"));
    }

    /// Verify that get_task_info returns DatabaseResult<TaskInfoRead>
    /// and accepts include_result / include_failed_reason filter params.
    #[test]
    fn get_task_info_return_type_contract() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &PostgresBroker) {
            let _result: DatabaseResult<TaskInfoRead> =
                broker.get_task_info(Uuid::nil(), false, false).await;
            let _result: DatabaseResult<TaskInfoRead> =
                broker.get_task_info(Uuid::nil(), true, false).await;
            let _result: DatabaseResult<TaskInfoRead> =
                broker.get_task_info(Uuid::nil(), true, true).await;
        }
    }

    /// Verify that ping_database returns DatabaseResult<DatabasePing>.
    #[test]
    fn ping_database_return_type_contract() {
        #[allow(unused, unreachable_code)]
        async fn _check(broker: &PostgresBroker) {
            let _result: DatabaseResult<DatabasePing> = broker.ping_database().await;
        }
    }

    // -----------------------------------------------------------------------
    // PostgresBroker trait bounds
    // -----------------------------------------------------------------------

    /// PostgresBroker must be Send + Sync (used behind Arc in TaskHandle
    /// and shared across async tasks).
    #[test]
    fn postgres_broker_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<PostgresBroker>();
        assert_sync::<PostgresBroker>();
    }

    /// Arc<PostgresBroker> must be Send + Sync (used in TaskHandle).
    #[test]
    fn arc_postgres_broker_is_send_sync() {
        use std::sync::Arc;
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Arc<PostgresBroker>>();
        assert_sync::<Arc<PostgresBroker>>();
    }

    #[test]
    fn sent_at_is_cut_to_whole_microseconds() {
        let sent_at = DateTime::parse_from_rfc3339("2026-11-16T12:00:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        let stored = sent_at_at_stored_precision(sent_at).unwrap();
        assert_eq!(stored.nanosecond(), 123_456_000);
        assert_eq!(sent_at_at_stored_precision(stored).unwrap(), stored);
    }
}

/// `sent_at` at the precision that PostgreSQL stores (whole microseconds), so
/// the stored value equals the value that a resend of the same call carries.
fn sent_at_at_stored_precision(sent_at: DateTime<Utc>) -> Result<DateTime<Utc>, BrokerError> {
    let nanos = sent_at.nanosecond();
    sent_at
        .with_nanosecond(nanos - nanos % 1_000)
        .ok_or_else(|| {
            BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
                SendPart::Input,
                format!("sent_at {sent_at} has no whole-microsecond form"),
            ))
        })
}

#[cfg(test)]
mod fused_finalize_tests {
    //! Behavior pins for the fused ok-path finalize (parity with horsies PR
    //! #134, re-anchored on `horsies_complete_task_fused`): one statement
    //! locks the RUNNING row, writes the COMPLETED attempt from the locked
    //! row, transitions, and fires the capacity notify.
    use super::*;
    use crate::broker::terminalization::on_test_pool::terminalize;
    use crate::core::lifecycle::{OwnedClaim, TerminalizationCommand, TerminalizationOutcome};
    use crate::test_support::sql::{self, TestPool};
    use uuid::Uuid;

    async fn fused(
        pool: &TestPool,
        task_id: &str,
        worker: &str,
        claimed_at: DateTime<Utc>,
        result_json: &str,
    ) -> Vec<TerminalizationOutcome> {
        terminalize(
            pool,
            &TerminalizationCommand::CompleteTaskFused {
                task_id: Uuid::parse_str(task_id).expect("test identity must be UUID"),
                fence: OwnedClaim {
                    worker_id: worker.to_owned(),
                    claimed_at,
                },
                result_json: result_json.to_owned(),
                placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
            },
        )
        .await
        .expect("terminalize")
    }

    fn applied(outcomes: &[TerminalizationOutcome]) -> bool {
        matches!(outcomes, [TerminalizationOutcome::Applied { .. }])
    }

    /// The claim generation the seeded row carries.
    async fn generation(pool: &TestPool, id: &str) -> DateTime<Utc> {
        sql::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .fetch_one(pool)
            .await
            .expect("seeded claim generation")
    }

    /// Seed a task in `status`, optionally claimed by `worker`, with `retry_count`.
    async fn seed(pool: &TestPool, id: &str, status: &str, worker: Option<&str>, retry: i32) {
        sql::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'fused_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                1, decode(repeat('00', 32), 'hex'), NULL,
                $2, NOW(), CASE WHEN $3 IS NOT NULL THEN NOW() END, $3, CASE WHEN $2 = 'CLAIMED' THEN NOW() + interval '1 minute' END,
                NOW(), NULL, NULL,
                123, 'host1', 'worker-123',
                $4, 3, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(test_uuid(id))
        .bind(status)
        .bind(worker)
        .bind(retry)
        .execute(pool)
        .await
        .expect("seed task");
    }

    async fn cleanup(pool: &TestPool, id: &str) {
        sql::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .expect("cleanup");
        sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn fused_finalize_completes_running_task() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 1).await;

        let generation = generation(&pool, &id).await;
        let outcomes = fused(&pool, &id, "w1", generation, "{\"Ok\":7}").await;
        assert!(applied(&outcomes), "RUNNING owned task must finalize");

        let (status, result, attempt_snapshot): (String, Option<String>, Vec<u8>) = sql::query_as(
            "SELECT status, convert_from(result_payload, 'UTF8'), attempt_snapshot
                 FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(test_uuid(&id))
        .fetch_one(&pool)
        .await
        .expect("read archived task");
        assert_eq!(status, "COMPLETED");
        assert_eq!(result.as_deref(), Some("{\"Ok\":7}"));

        // Archive version 2: `[attempts, open]`; the attempt ended with its
        // row, so no attempt is open.
        let archive: serde_json::Value = serde_json::from_slice(&attempt_snapshot).unwrap();
        let attempt = &archive[0];
        assert_eq!(attempt[0][0], 2, "attempt = retry_count(1) + 1");
        assert_eq!(attempt[0][1], "COMPLETED");
        assert_eq!(attempt[0][2], false);
        assert_eq!(archive[1], serde_json::Value::Null);

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    async fn fused_finalize_noop_when_not_running() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "PENDING", Some("w1"), 0).await;

        let generation = generation(&pool, &id).await;
        let outcomes = fused(&pool, &id, "w1", generation, "{\"Ok\":1}").await;
        assert!(!applied(&outcomes), "non-RUNNING row must not be touched");

        let attempts: i64 =
            sql::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
                .bind(test_uuid(&id))
                .fetch_one(&pool)
                .await
                .expect("count attempts");
        assert_eq!(attempts, 0, "no attempt row written on no-op");

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    async fn fused_finalize_noop_on_wrong_worker() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 0).await;

        let generation = generation(&pool, &id).await;
        let outcomes = fused(&pool, &id, "w2", generation, "{\"Ok\":1}").await;
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::LostClaim { .. }]
            ),
            "ownership mismatch must not finalize"
        );

        let status: String = sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(&id))
            .fetch_one(&pool)
            .await
            .expect("status");
        assert_eq!(status, "RUNNING", "row untouched");

        cleanup(&pool, &id).await;
    }

    /// C10: the fused finalize must be fenced to a claim generation. A stale
    /// finalize carrying an earlier attempt's `claimed_at` must not complete
    /// a task the same worker re-claimed (new `claimed_at`) after a reaper
    /// requeue.
    #[tokio::test]
    async fn fused_finalize_fenced_by_claimed_at() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), 0).await;

        // The current claim generation is the row's actual claimed_at.
        let current: DateTime<Utc> =
            sql::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
                .bind(test_uuid(&id))
                .fetch_one(&pool)
                .await
                .expect("claimed_at");
        let stale = current - chrono::Duration::minutes(1);

        // Stale generation: fenced out, row stays RUNNING.
        let outcomes = fused(&pool, &id, "w1", stale, "{\"Ok\":1}").await;
        assert!(
            matches!(
                outcomes.as_slice(),
                [TerminalizationOutcome::LostClaim { .. }]
            ),
            "stale claimed_at must be fenced out"
        );
        let status: String = sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(&id))
            .fetch_one(&pool)
            .await
            .expect("status");
        assert_eq!(
            status, "RUNNING",
            "row must stay RUNNING under a stale fence"
        );

        // Current generation: finalizes.
        let outcomes = fused(&pool, &id, "w1", current, "{\"Ok\":2}").await;
        assert!(applied(&outcomes), "matching claimed_at must finalize");
        let status: String =
            sql::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
                .bind(test_uuid(&id))
                .fetch_one(&pool)
                .await
                .expect("status");
        assert_eq!(status, "COMPLETED");

        cleanup(&pool, &id).await;
    }
}

#[cfg(test)]
mod prestart_expiry_tests {
    //! The expiry of a claimed row whose deadline passed before its start
    //! (`expire_claimed_task_before_start`, `horsies_expire_owned_claim`).
    use super::*;
    use crate::test_support::sql::{self, TestPool};
    use uuid::Uuid;

    async fn seed_claimed(pool: &TestPool, id: &str, worker: &str) {
        sql::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'hb_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                1, decode(repeat('00', 32), 'hex'), NULL,
                'CLAIMED', NOW(), NOW(), $2, NOW() + interval '1 minute',
                NULL, NULL, NULL,
                NULL, NULL, NULL,
                0, 3, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(test_uuid(id))
        .bind(worker)
        .execute(pool)
        .await
        .expect("seed claimed");
    }

    async fn cleanup(pool: &TestPool, id: &str) {
        sql::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .expect("cleanup");
    }

    /// The expiry of a claimed row before its start carries no claim
    /// generation: once the deadline has passed, expiry is the correct outcome
    /// for whichever generation of this worker holds the row, so a stale
    /// dispatch and a live one commit the same event. Before the deadline, the
    /// deadline guard (not a generation fence) refuses. The start of a stale
    /// generation is
    /// `lifecycle_tests::claim_start_release::start::a_start_that_does_not_hold_writes_nothing_and_names_its_reason`.
    #[tokio::test]
    async fn prestart_expiry_waits_for_the_deadline_and_then_expires_any_generation() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        let id = Uuid::new_v4().to_string();
        seed_claimed(&pool, &id, "w1").await;

        let not_due = broker
            .expire_claimed_task_before_start(
                Uuid::parse_str(&id).unwrap(),
                "w1",
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("expire");
        assert_eq!(
            not_due,
            PrestartExpiry::NotExpired,
            "unexpired deadline must refuse"
        );
        sql::query(
            "UPDATE horsies_tasks SET good_until = NOW() - INTERVAL '1 second' WHERE id = $1",
        )
        .bind(test_uuid(&id))
        .execute(&pool)
        .await
        .expect("age good_until");
        let expired = broker
            .expire_claimed_task_before_start(
                Uuid::parse_str(&id).unwrap(),
                "w1",
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("expire");
        assert_eq!(
            expired,
            PrestartExpiry::Expired,
            "expired deadline must expire for any generation of this worker's claim"
        );
        // Python counts a replay as expired too.
        let replayed = broker
            .expire_claimed_task_before_start(
                Uuid::parse_str(&id).unwrap(),
                "w1",
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("expire replay");
        assert_eq!(replayed, PrestartExpiry::Expired);

        cleanup(&pool, &id).await;
    }
}

#[cfg(test)]
mod absent_result_wait_tests {
    //! C3: a result wait must never block forever. A wait with no limit on a
    //! task that does not exist (pruned by retention, or a bogus id) answers
    //! its absence from the first probe instead of hanging.
    use super::*;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn staged_detail_live_race_retries_instead_of_reporting_absence() {
        let task_id = Uuid::new_v4();
        let probe =
            raw_result_from_history_detail(task_id, TaskDetailResult::Live { task_id }).unwrap();
        assert!(matches!(probe, RawResultProbe::Retry));
    }

    /// The absence reason of the history read reaches the raw result read:
    /// a task that retention can have removed differs from an id that no
    /// retained history held.
    #[test]
    fn an_absent_history_detail_keeps_its_reason() {
        let task_id = Uuid::new_v4();
        for reason in [
            TaskAbsenceReason::PredatesRetainedHistory,
            TaskAbsenceReason::WithinRetainedHistory,
            TaskAbsenceReason::NotClassified,
        ] {
            let probe = raw_result_from_history_detail(
                task_id,
                TaskDetailResult::Absent { task_id, reason },
            )
            .unwrap();
            assert!(
                matches!(probe, RawResultProbe::Absent(kept) if kept == reason),
                "{reason:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_result_wait_with_no_limit_ends_for_a_missing_task() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        let missing = Uuid::new_v4();

        // An outer timeout: a wait with no limit on a missing task that
        // hangs fails here at once.
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            broker.get_raw_result_record(missing, None),
        )
        .await
        .expect("a wait with no limit must not hang for a missing task")
        .expect("no broker error");
        assert!(matches!(read, RawResultRead::Absent { .. }), "{read:?}");
    }
}

#[cfg(test)]
mod filter_non_runnable_tests {
    //! C15: `filter_non_runnable_workflow_tasks` must, for a PAUSED workflow,
    //! both cancel the worker's claimed task and reset its node to READY — in one
    //! transaction, so a crash cannot leave a terminal task linked to a live node.
    //! The atomicity itself is not crash-testable in a unit test; this pins the
    //! functional outcome (both mutations applied, id returned as filtered).
    use super::*;
    use crate::test_support::sql;
    use uuid::Uuid;

    /// The bounded lookup of the interim function.
    const FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL: &str =
        "SELECT id, status FROM horsies_find_non_runnable_workflow_tasks($1)";

    #[tokio::test]
    async fn bounded_lookup_preserves_rows_for_duplicates_missing_links_and_status_changes() {
        let pool = crate::test_support::migrated_pool().await;
        let mut tx = pool.begin().await.unwrap();
        sql::query("SET LOCAL plan_cache_mode = force_custom_plan")
            .execute(&mut *tx)
            .await
            .unwrap();
        let mut ids = Vec::new();
        for status in ["PAUSED", "CANCELLED", "RUNNING"] {
            let workflow_id = Uuid::new_v4();
            sql::query(
                "INSERT INTO horsies_workflows(id,name,status,on_error,depth,root_workflow_id,sent_at,created_at,updated_at) VALUES($1,'lookup_test',$2,'fail',0,$1,NOW(),NOW(),NOW())",
            )
            .bind(workflow_id)
            .bind(status)
            .execute(&mut *tx)
            .await
            .unwrap();
            for index in 0..2 {
                let task_id = Uuid::new_v4();
                ids.push(task_id);
                sql::query("INSERT INTO horsies_workflow_tasks(downstream_subworkflow, id,workflow_id,task_index,node_id,task_name,input,queue_name,priority,dependencies,allow_failed_deps,join_type,status,is_subworkflow,max_retries,execution,retention_class_key,task_id,created_at) VALUES('NONE', $1,$2,$3,$4,'lookup_test','null','default',100,'{}',FALSE,'all','ENQUEUED',FALSE,0,'ISOLATED','forever',$5,NOW())")
                    .bind(Uuid::new_v4()).bind(workflow_id).bind(index)
                    .bind(format!("node_{index}")).bind(task_id)
                    .execute(&mut *tx).await.unwrap();
                if index == 0 {
                    sql::query("INSERT INTO horsies_tasks (
                            id, task_name, queue_name, priority, input, task_options, execution,
                            retention_class_key, sent_at, created_at,
                            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                            started_at, finalizing_at, finalizing_by_worker_id,
                            worker_pid, worker_hostname, worker_process_name,
                            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
                        ) VALUES (
                            $1, 'lookup_test', 'default', 100, 'null', NULL, 'ISOLATED',
                            'forever', NOW(), NOW(),
                            1, decode(repeat('00', 32), 'hex'), NULL,
                            'PENDING', NOW(), NULL, NULL, NULL,
                            NULL, NULL, NULL,
                            NULL, NULL, NULL,
                            0, 3, NULL, NULL, FALSE,
                            NOW(), FALSE, NULL, NULL
                        )")
                        .bind(task_id).execute(&mut *tx).await.unwrap();
                }
            }
        }
        let original = "SELECT t.id,w.status FROM horsies_tasks t JOIN horsies_workflow_tasks wt ON wt.task_id=t.id JOIN horsies_workflows w ON w.id=wt.workflow_id WHERE t.id=ANY($1) AND w.status IN ('PAUSED','CANCELLED')";
        for input in [
            Vec::<Option<Uuid>>::new(),
            vec![None],
            vec![Some(ids[0]), Some(ids[0]), None, Some(Uuid::new_v4())],
            ids.iter().copied().map(Some).collect(),
        ] {
            let mut expected: Vec<(Uuid, String)> = sql::query_as(original)
                .bind(&input)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            let mut actual: Vec<(Uuid, String)> =
                sql::query_as(FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL)
                    .bind(&input)
                    .fetch_all(&mut *tx)
                    .await
                    .unwrap();
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected);
        }
        let absent: Vec<(Uuid, String)> = sql::query_as(FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL)
            .bind(Option::<Vec<Uuid>>::None)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert!(absent.is_empty());
        sql::query("UPDATE horsies_workflows SET status='RUNNING' WHERE name='lookup_test'")
            .execute(&mut *tx)
            .await
            .unwrap();
        let changed: Vec<(Uuid, String)> = sql::query_as(FIND_NON_RUNNABLE_WORKFLOW_TASKS_SQL)
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert!(changed.is_empty());
        let mode: String = sql::query_scalar("SHOW plan_cache_mode")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(mode, "force_custom_plan");
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn paused_workflow_cancels_task_and_resets_node() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        let wf_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();

        sql::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'c15_wf', 'PAUSED', 'fail', NULL, 'test.c15.v1', 0, $1,
                      NOW(), NOW(), NOW(), NOW())",
        )
        .bind(wf_id)
        .execute(&pool)
        .await
        .expect("insert workflow");

        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, max_retries, execution, retention_class_key,
                task_id, created_at
            ) VALUES ('NONE', $1, $2, 0, 'node_0', 'c15_task', 'null',
                      'default', 100, '{}', FALSE, 'all',
                      'ENQUEUED', FALSE, 3, 'ISOLATED', 'standard_30d',
                      $3, NOW())",
        )
        .bind(Uuid::new_v4())
        .bind(wf_id)
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("insert workflow_task");

        sql::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'c15_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                NULL, NULL, NULL,
                'CLAIMED', NOW(), NOW(), 'w1', NOW() + interval '1 minute',
                NULL, NULL, NULL,
                NULL, NULL, NULL,
                0, 3, NULL, NULL, TRUE,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("insert task");
        let generation: DateTime<Utc> =
            sql::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("seeded claim generation");

        let filtered = broker
            .filter_non_runnable_workflow_tasks(&[(task_id, generation)], "w1")
            .await
            .expect("filter");
        assert_eq!(filtered, vec![task_id], "paused task must be filtered");

        let task_status: String =
            sql::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(task_status, "CANCELLED", "task must be cancelled");

        let (node_status, linked): (String, Option<Uuid>) = sql::query_as(
            "SELECT status, task_id FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(node_status, "READY", "node must be reset to READY");
        assert!(linked.is_none(), "node's task_id must be cleared on reset");

        sql::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(wf_id)
            .execute(&pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(wf_id)
            .execute(&pool)
            .await
            .ok();
    }
}

#[cfg(test)]
mod terminal_at_stamp_tests {
    //! Every terminal transition moves the row out of `horsies_tasks` into
    //! `horsies_task_history` and stamps `terminal_at` from the database
    //! clock in the same statement; a live transition keeps the row live and
    //! writes no history row. The baseline live table has no `terminal_at`
    //! column, and the history column is NOT NULL
    //! (`core/baseline/0001_tables.sql`). One pin per status
    //! family; the writer-inventory scan (tests/writer_inventory.rs) covers
    //! the rest structurally.
    use super::*;
    use crate::test_support::sql::{self, TestPool};
    use uuid::Uuid;

    async fn connect() -> (PostgresBroker, TestPool) {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker.ensure_schema_initialized().await.expect("schema");
        (broker, pool)
    }

    /// Seed a task in `status`, optionally claimed by `worker`, with
    /// `good_until` offset seconds from now (negative = already passed).
    async fn seed(
        pool: &TestPool,
        id: &str,
        status: &str,
        worker: Option<&str>,
        good_until_offset_s: Option<i64>,
    ) {
        sql::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'terminal_at_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                1, decode(repeat('00', 32), 'hex'), NULL,
                $2, NOW(), CASE WHEN $3 IS NOT NULL THEN NOW() END, $3, CASE WHEN $2 = 'CLAIMED' THEN NOW() + interval '1 minute' END,
                NOW(), NULL, NULL,
                CASE WHEN $2 = 'RUNNING' THEN 4242 END, CASE WHEN $2 = 'RUNNING' THEN 'test-host' END, CASE WHEN $2 = 'RUNNING' THEN 'test-process' END,
                0, 3, NULL, NOW() + $4 * INTERVAL '1 second', FALSE,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(test_uuid(id))
        .bind(status)
        .bind(worker)
        .bind(good_until_offset_s)
        .execute(pool)
        .await
        .expect("seed task");
    }

    /// The database clock, read in a statement of its own.
    async fn database_now(pool: &TestPool) -> DateTime<Utc> {
        sql::query_scalar("SELECT now()")
            .fetch_one(pool)
            .await
            .expect("read the database clock")
    }

    /// The status of the live row, or `None` when `horsies_tasks` does not
    /// hold the task.
    async fn live_status(pool: &TestPool, id: &str) -> Option<String> {
        sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .fetch_optional(pool)
            .await
            .expect("read the live row")
    }

    /// The status and `terminal_at` of the history row, or `None` when
    /// history does not hold the task.
    async fn history_row(pool: &TestPool, id: &str) -> Option<(String, DateTime<Utc>)> {
        sql::query_as("SELECT status, terminal_at FROM horsies_task_history WHERE task_id = $1")
            .bind(test_uuid(id))
            .fetch_optional(pool)
            .await
            .expect("read the history row")
    }

    /// The task left the live table, and its history row has `status` and a
    /// `terminal_at` of the transition: at or after `before` (the database
    /// clock before the transition) and at or before the database clock
    /// after it.
    async fn assert_moved(pool: &TestPool, id: &str, status: &str, before: DateTime<Utc>) {
        assert_eq!(
            live_status(pool, id).await,
            None,
            "the {status} transition must remove the live row"
        );
        let after = database_now(pool).await;
        let Some((history_status, terminal_at)) = history_row(pool, id).await else {
            panic!("the {status} transition must write a history row");
        };
        assert_eq!(history_status, status);
        assert!(
            before <= terminal_at && terminal_at <= after,
            "terminal_at {terminal_at} must be the transition time, from {before} to {after}"
        );
    }

    async fn cleanup(pool: &TestPool, id: &str) {
        sql::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .expect("cleanup");
        sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(test_uuid(id))
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn completed_row_gets_terminal_at() {
        let (_broker, pool) = connect().await;
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;
        // The fused completion fences on the claim generation: stamp one.
        let generation: DateTime<Utc> = sql::query_scalar(
            "UPDATE horsies_tasks SET claimed_at = NOW() WHERE id = $1 RETURNING claimed_at",
        )
        .bind(test_uuid(&id))
        .fetch_one(&pool)
        .await
        .expect("stamp claim generation");
        let before = database_now(&pool).await;

        let outcomes = crate::broker::terminalization::on_test_pool::terminalize(
            &pool,
            &crate::core::lifecycle::TerminalizationCommand::CompleteTaskFused {
                task_id: Uuid::parse_str(&id).expect("test identity must be UUID"),
                fence: crate::core::lifecycle::OwnedClaim {
                    worker_id: "w1".to_owned(),
                    claimed_at: generation,
                },
                result_json: "{\"Ok\":1}".to_owned(),
                placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
            },
        )
        .await
        .expect("finalize");
        assert!(matches!(
            outcomes.as_slice(),
            [crate::core::lifecycle::TerminalizationOutcome::Applied { .. }]
        ));

        assert_moved(&pool, &id, "COMPLETED", before).await;

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    async fn failed_row_gets_terminal_at() {
        let (_broker, pool) = connect().await;
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;
        let before = database_now(&pool).await;

        let outcomes = crate::test_support::terminal_writes::fail_running_task(
            &pool,
            Uuid::parse_str(&id).expect("test identity must be UUID"),
            "w1",
            &crate::test_support::terminal_writes::TerminalFailure::of_task(
                "{\"Err\":{}}",
                Some("TASK_EXCEPTION"),
                None,
            ),
        )
        .await
        .expect("fail");
        assert!(matches!(
            outcomes.as_slice(),
            [crate::core::lifecycle::TerminalizationOutcome::Applied { .. }]
        ));

        assert_moved(&pool, &id, "FAILED", before).await;

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    async fn cancelled_row_gets_terminal_at() {
        let (broker, pool) = connect().await;
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "PENDING", None, None).await;
        let before = database_now(&pool).await;

        let cancelled = crate::monitoring::cancel_task(
            &broker,
            Uuid::parse_str(&id).unwrap(),
            crate::monitoring::CancelPolicy::PendingOrClaimedOnly,
        )
        .await
        .expect("cancel");
        assert_eq!(
            cancelled.was_status,
            crate::core::types::status::TaskStatus::Pending
        );

        assert_moved(&pool, &id, "CANCELLED", before).await;

        cleanup(&pool, &id).await;
    }

    #[tokio::test]
    async fn expired_row_gets_terminal_at() {
        let (broker, pool) = connect().await;
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "CLAIMED", Some("w1"), Some(-60)).await;
        let before = database_now(&pool).await;

        let expired = broker
            .expire_claimed_task_before_start(
                Uuid::parse_str(&id).unwrap(),
                "w1",
                crate::broker::child_writes::WriteDeadline::start_attempt(),
            )
            .await
            .expect("expire");
        assert_eq!(expired, PrestartExpiry::Expired);

        assert_moved(&pool, &id, "EXPIRED", before).await;

        cleanup(&pool, &id).await;
    }

    /// A requeue is a live transition: the row stays live as PENDING, writes
    /// no history row, counts the retry, and clears every claim and run
    /// field, as the retry write of `horsies_finish_failed_attempt`.
    #[tokio::test]
    async fn requeue_keeps_the_row_live_and_writes_no_history_row() {
        let (broker, pool) = connect().await;
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;

        let applied = broker
            .requeue(Uuid::parse_str(&id).unwrap(), Some(Utc::now()), "w1")
            .await
            .expect("requeue");
        assert!(applied);

        assert_eq!(live_status(&pool, &id).await.as_deref(), Some("PENDING"));
        assert_eq!(
            history_row(&pool, &id).await,
            None,
            "a live transition must write no history row"
        );
        let (retry_count, cleared): (i32, serde_json::Value) = sql::query_as(
            "SELECT retry_count,
                    jsonb_build_object(
                        'claimed_at', claimed_at,
                        'claimed_by_worker_id', claimed_by_worker_id,
                        'claim_expires_at', claim_expires_at,
                        'started_at', started_at,
                        'worker_pid', worker_pid,
                        'worker_hostname', worker_hostname,
                        'worker_process_name', worker_process_name,
                        'finalizing_at', finalizing_at,
                        'finalizing_by_worker_id', finalizing_by_worker_id
                    )
             FROM horsies_tasks WHERE id = $1",
        )
        .bind(test_uuid(&id))
        .fetch_one(&pool)
        .await
        .expect("requeued row");
        assert_eq!(retry_count, 1);
        let fields = cleared.as_object().expect("field object");
        for (field, value) in fields {
            assert!(value.is_null(), "requeue must clear {field}, found {value}");
        }
        assert_eq!(fields.len(), 9);

        cleanup(&pool, &id).await;
    }

    /// A requeue with no retry time is due at once: `next_retry_at` and the
    /// NOT NULL `enqueued_at` take the database clock of the requeue.
    #[tokio::test]
    async fn requeue_with_no_retry_time_is_due_at_the_database_clock() {
        let (broker, pool) = connect().await;
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;
        let before = database_now(&pool).await;

        let applied = broker
            .requeue(Uuid::parse_str(&id).unwrap(), None, "w1")
            .await
            .expect("requeue");
        assert!(applied);

        let after = database_now(&pool).await;
        let (enqueued_at, next_retry_at): (DateTime<Utc>, Option<DateTime<Utc>>) =
            sql::query_as("SELECT enqueued_at, next_retry_at FROM horsies_tasks WHERE id = $1")
                .bind(test_uuid(&id))
                .fetch_one(&pool)
                .await
                .expect("requeued row");
        assert_eq!(next_retry_at, Some(enqueued_at));
        assert!(
            before <= enqueued_at && enqueued_at <= after,
            "enqueued_at {enqueued_at} must be the requeue time, from {before} to {after}"
        );

        cleanup(&pool, &id).await;
    }

    /// A requeue begins the next attempt with no claim: it sets
    /// `attempt_claims` to 0 and `first_claimed_at` to NULL, from a counted
    /// claim of the ended attempt.
    #[tokio::test]
    async fn requeue_resets_the_claim_count_of_the_next_attempt() {
        let (broker, pool) = connect().await;
        let id = Uuid::new_v4().to_string();
        seed(&pool, &id, "RUNNING", Some("w1"), None).await;
        sql::query(
            "UPDATE horsies_tasks SET attempt_claims = 2, first_claimed_at = claimed_at
             WHERE id = $1",
        )
        .bind(test_uuid(&id))
        .execute(&pool)
        .await
        .expect("count two claims of the attempt");

        let applied = broker
            .requeue(Uuid::parse_str(&id).unwrap(), Some(Utc::now()), "w1")
            .await
            .expect("requeue");
        assert!(applied);

        let (attempt_claims, first_claimed_at): (Option<i32>, Option<DateTime<Utc>>) =
            sql::query_as(
                "SELECT attempt_claims, first_claimed_at FROM horsies_tasks WHERE id = $1",
            )
            .bind(test_uuid(&id))
            .fetch_one(&pool)
            .await
            .expect("requeued row");
        assert_eq!(attempt_claims, Some(0));
        assert_eq!(first_claimed_at, None);

        cleanup(&pool, &id).await;
    }
}

#[cfg(test)]
mod broker_operation_error_tests {
    //! Python's catalog for broker operations: a refused argument is
    //! `INVALID_INPUT` with no database call, refused connection settings are
    //! not retryable, and a database failure carries the driver
    //! classification of its cause (`core/brokers/postgres.py`,
    //! `_operation_failed`).
    use super::*;
    use crate::broker::result_types::DatabaseErrorCode;
    use crate::db::StatementFailure;

    /// No server listens on port 1, so a call that reached a pool would fail
    /// with a connect error instead of the refusal under test.
    const UNREACHABLE_URL: &str = "postgresql://horsies@127.0.0.1:1/horsies";

    fn unreachable_broker() -> PostgresBroker {
        PostgresBroker::from_test_url(UNREACHABLE_URL, crate::ClientFamily::Rust)
    }

    #[tokio::test]
    async fn connect_refuses_a_driver_suffix_scheme_before_any_connection() {
        let refused = PostgresBroker::connect(
            "postgresql+psycopg://horsies@127.0.0.1:1/horsies",
            crate::ClientFamily::Rust,
        )
        .await
        .err()
        .expect("the scheme is refused");

        assert!(matches!(
            &refused,
            BrokerError::InvalidConnectionSettings(
                crate::core::config::PostgresConfigError::InvalidUrlScheme(scheme)
            ) if scheme == "postgresql+psycopg"
        ));
        assert!(!refused.is_retryable());
    }

    #[tokio::test]
    async fn connect_with_refuses_a_config_rule_as_not_retryable() {
        let config = PostgresConfig::from_database_url("mysql://horsies@127.0.0.1:1/horsies");

        let refused = PostgresBroker::connect_with(&config, crate::ClientFamily::Rust)
            .await
            .err()
            .expect("the scheme is refused");

        assert!(matches!(
            refused,
            BrokerError::InvalidConnectionSettings(
                crate::core::config::PostgresConfigError::InvalidUrlScheme(_)
            )
        ));
        assert!(!refused.is_retryable());
    }

    /// The statement layer parses the URL when the broker is built: a port
    /// that is not a number is refused before any connection.
    #[tokio::test]
    async fn an_unparseable_url_is_refused_as_not_retryable() {
        let refused = PostgresBroker::connect(
            "postgresql://horsies@127.0.0.1:notaport/horsies",
            crate::ClientFamily::Rust,
        )
        .await
        .err()
        .expect("the port does not parse");

        assert!(
            matches!(
                &refused,
                BrokerError::PoolSettings(crate::broker::pools::PoolSettingsError::Url {
                    cause: crate::db::UrlError::InvalidPort { value },
                    ..
                }) if value == "notaport"
            ),
            "{refused:?}"
        );
        assert!(!refused.is_retryable());
    }

    #[tokio::test]
    async fn ping_workers_refuses_a_zero_wait_as_invalid_input() {
        let broker = unreachable_broker();

        let Err(refused) = broker.ping_workers(None, Duration::ZERO, None).await else {
            panic!("a zero wait must be refused");
        };

        assert_eq!(refused.code(), DatabaseErrorCode::InvalidInput);
        assert!(!refused.retryable());
    }

    #[tokio::test]
    async fn ping_workers_refuses_a_wait_above_the_bound_as_invalid_input() {
        let broker = unreachable_broker();

        let over = crate::broker::WORKER_PING_WAIT_BOUND + Duration::from_millis(1);
        let Err(refused) = broker.ping_workers(None, over, None).await else {
            panic!("a wait above the bound must be refused");
        };

        assert_eq!(refused.code(), DatabaseErrorCode::InvalidInput);
        assert!(!refused.retryable());
    }

    #[tokio::test]
    async fn ping_workers_refuses_zero_min_responses_as_invalid_input() {
        let broker = unreachable_broker();

        let Err(refused) = broker
            .ping_workers(None, Duration::from_secs(1), Some(0))
            .await
        else {
            panic!("min_responses of 0 must be refused");
        };

        assert_eq!(refused.code(), DatabaseErrorCode::InvalidInput);
        assert!(!refused.retryable());
    }

    /// A broker with no listener (built from a pool with no session URL):
    /// `LISTENER_DISABLED`, not retryable, before any database call, as
    /// Python's broker with no listener.
    #[tokio::test]
    async fn ping_workers_on_a_broker_without_a_listener_is_listener_disabled() {
        let observe_only =
            PostgresBroker::from_test_url_observe_only(UNREACHABLE_URL, crate::ClientFamily::Rust);
        for broker in [unreachable_broker(), observe_only] {
            let Err(refused) = broker
                .ping_workers(None, Duration::from_secs(1), None)
                .await
            else {
                panic!("a broker without a listener cannot ping");
            };

            assert_eq!(refused.code(), DatabaseErrorCode::ListenerDisabled);
            assert!(!refused.retryable());
        }
    }

    /// Python `test_broker_postgres.py::test_history_rejects_nonpositive_limit`.
    #[tokio::test]
    async fn worker_state_history_refuses_a_limit_of_zero_or_less() {
        let broker = unreachable_broker();

        for limit in [0, -1, i64::MIN] {
            let Err(refused) = broker.get_worker_state_history("w1", Some(limit)).await else {
                panic!("limit {limit} must be refused");
            };
            assert_eq!(refused.code(), DatabaseErrorCode::InvalidInput, "{limit}");
            assert!(!refused.retryable(), "{limit}");
        }
    }

    #[tokio::test]
    async fn worker_state_history_refuses_a_limit_above_the_bound() {
        let broker = unreachable_broker();

        let over = crate::broker::WORKER_HISTORY_ROW_BOUND + 1;
        let Err(refused) = broker.get_worker_state_history("w1", Some(over)).await else {
            panic!("limit {over} must be refused");
        };
        assert_eq!(refused.code(), DatabaseErrorCode::InvalidInput);
        assert!(!refused.retryable());
    }

    /// `close` returns the core pools to `Idle`, and the next operation opens
    /// new pools (`HandleState::Idle`, `broker/pools.rs`), as Python's broker
    /// stays usable after `close_async`
    /// (`clients/python/horsies/core/brokers/postgres.py:2403-2411`). A ping after a close
    /// meets the server: here a refused connection, which is retryable. A
    /// checkout on a closed pool (`PoolClosed`) stays not retryable
    /// (`a_monitoring_read_failure_carries_the_driver_classification`).
    #[tokio::test]
    async fn a_database_ping_after_close_opens_new_pools_and_a_refused_connection_is_retryable() {
        let broker = unreachable_broker();
        broker.close().await;

        let Err(failed) = broker.ping_database().await else {
            panic!("no server listens on the unreachable URL");
        };

        assert_eq!(failed.code(), DatabaseErrorCode::DatabasePingFailed);
        assert!(
            !failed.message().contains("pool is closed"),
            "the ping must run on new pools: {}",
            failed.message()
        );
        assert!(failed.retryable(), "{}", failed.message());
    }

    fn checkout_failed(cause: crate::db::CheckoutFailure) -> BrokerError {
        BrokerError::Statement(StatementFailure::NotSent {
            statement: "read",
            cause: crate::db::NotSentCause::Checkout(cause),
        })
    }

    fn checkout_timed_out() -> BrokerError {
        checkout_failed(crate::db::CheckoutFailure::TimedOut {
            role: crate::db::PoolRole::Main,
            waited: Duration::from_secs(30),
        })
    }

    fn pool_closed() -> BrokerError {
        checkout_failed(crate::db::CheckoutFailure::PoolClosed {
            role: crate::db::PoolRole::Main,
        })
    }

    #[test]
    fn a_monitoring_read_failure_carries_the_driver_classification() {
        assert!(monitoring_query_error("read", checkout_timed_out()).retryable());
        assert!(!monitoring_query_error("read", pool_closed()).retryable());
        assert!(!monitoring_query_error(
            "read",
            BrokerError::RowDecode(RowDecodeError::NoRow { statement: "read" })
        )
        .retryable());
        assert!(worker_ping_statement_error(checkout_timed_out()).retryable());
        assert!(!worker_ping_statement_error(pool_closed()).retryable());
    }
}

#[cfg(test)]
mod connect_tests {
    //! A connect checks the config and opens no connection.
    use super::*;

    /// No server listens on port 1.
    const UNREACHABLE_URL: &str = "postgresql://horsies@127.0.0.1:1/horsies";

    #[tokio::test]
    async fn a_connect_opens_no_connection() {
        let broker = PostgresBroker::connect(UNREACHABLE_URL, crate::ClientFamily::Rust)
            .await
            .expect("a connect opens no connection, so it needs no server");

        let open = broker
            .core_pools_for_read()
            .expect("the connect built the pools");
        assert_eq!(open.main().status().open, 0);
        assert_eq!(open.pools().session_work().status().open, 0);
    }

    #[tokio::test]
    async fn a_worker_connect_with_a_split_session_url_opens_no_connection() {
        let mut config = PostgresConfig::from_database_url(UNREACHABLE_URL);
        config.session_database = crate::core::config::SessionDatabase::Separate {
            url: "postgresql://horsies@127.0.0.1:2/horsies".to_owned(),
        };

        let broker = PostgresBroker::connect_worker_with(&config, crate::ClientFamily::Rust)
            .await
            .expect("a connect opens no connection, so it needs no server");

        let open = broker
            .core_pools_for_read()
            .expect("the connect built the pools");
        assert_eq!(open.main().status().open, 0);
        assert_eq!(open.pools().session_work().status().open, 0);
    }
}

#[cfg(test)]
mod result_wait_tests {
    //! The result wait: Python's limit bound (`clients/python/horsies/core/utils/wait_timeout.py`)
    //! before any read, Python's repoll intervals, a read at once after a
    //! listener gap, and polling with no listener or when the listener stops
    //! (`clients/python/horsies/core/brokers/postgres.py:3265-3285`).
    use super::*;
    use crate::broker::result_types::DatabaseErrorCode;
    use crate::broker::wait_limit::MAX_WAIT_LIMIT_MS;
    use crate::test_support::sql::{self, TestPoolOptions};
    use serial_test::serial;

    /// No server listens on port 1, so a call that reached the pool would
    /// fail with a pool timeout instead of the refusal under test.
    fn unreachable_broker() -> PostgresBroker {
        let pool = TestPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies");
        PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
    }

    fn refused_limits() -> [Duration; 2] {
        [Duration::from_millis(MAX_WAIT_LIMIT_MS + 1), Duration::MAX]
    }

    #[tokio::test]
    async fn get_raw_result_record_refuses_a_limit_above_the_bound_before_any_read() {
        let broker = unreachable_broker();

        for limit in refused_limits() {
            let Err(refused) = broker
                .get_raw_result_record(Uuid::new_v4(), Some(limit))
                .await
            else {
                panic!("limit {limit:?} must be refused");
            };
            assert_eq!(refused.code(), DatabaseErrorCode::InvalidInput, "{limit:?}");
            assert!(!refused.retryable());
        }
    }

    #[test]
    fn the_repoll_intervals_are_python_s() {
        assert_eq!(TASK_RESULT_REPOLL_SUBSCRIBED, Duration::from_secs(5));
        assert_eq!(TASK_RESULT_REPOLL_POLLING, Duration::from_millis(200));
        assert_eq!(
            ResultWaitMode::Polling.repoll_interval(),
            TASK_RESULT_REPOLL_POLLING
        );
    }

    /// A broker with no listener (built from a pool with no session URL)
    /// polls, as Python's broker with no listener. Nothing connects.
    #[tokio::test]
    async fn a_broker_without_a_listener_makes_the_wait_poll() {
        let observe_only = PostgresBroker::from_test_url_observe_only(
            TestPoolOptions::new()
                .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
                .url(),
            crate::ClientFamily::Rust,
        );
        for broker in [unreachable_broker(), observe_only] {
            let mode = broker.result_wait_mode(Uuid::new_v4()).await;

            assert!(matches!(mode, ResultWaitMode::Polling));
        }
    }

    /// The longest a wake, a gap or a stop may take to end a pause in these
    /// tests; well below the subscribed repoll interval.
    const WAKE_BOUND: Duration = Duration::from_secs(3);

    /// A broker on the test database whose listener connection carries
    /// `application_name`, so a test can find its backend.
    async fn listener_broker(application_name: &str) -> PostgresBroker {
        let url = crate::test_support::migrated_database_url().await;
        let separator = if url.contains('?') { '&' } else { '?' };
        PostgresBroker::from_test_url_and_session_url(
            crate::test_support::migrated_pool().await.url(),
            &format!("{url}{separator}application_name={application_name}"),
            crate::ClientFamily::Rust,
        )
    }

    #[tokio::test]
    async fn a_pause_without_a_wake_keeps_the_subscription() {
        let broker = listener_broker("result_wait_keep").await;
        let mut mode = broker.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));
        assert_eq!(mode.repoll_interval(), TASK_RESULT_REPOLL_SUBSCRIBED);

        mode.pause(Duration::from_millis(10)).await;

        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));
    }

    #[tokio::test]
    async fn a_task_done_notification_for_the_task_ends_the_pause() {
        let broker = listener_broker("result_wait_notify").await;
        let task_id = Uuid::new_v4();
        let mut mode = broker.result_wait_mode(task_id).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));

        sql::query("SELECT pg_notify($1, $2)")
            .bind(TASK_DONE_CHANNEL)
            .bind(task_id.to_string())
            .execute(&crate::test_support::migrated_pool().await)
            .await
            .expect("notify task_done");
        let started = Instant::now();
        mode.pause(TASK_RESULT_REPOLL_SUBSCRIBED).await;

        assert!(started.elapsed() < WAKE_BOUND, "{:?}", started.elapsed());
    }

    /// The listener loses its connection: it connects again, LISTENs again
    /// and sends a gap, and the wait reads at once instead of at its repoll.
    #[tokio::test]
    #[serial(timing)]
    async fn a_listener_gap_ends_the_pause_at_once() {
        let application_name = format!("result_wait_gap_{}", Uuid::new_v4().simple());
        let broker = listener_broker(&application_name).await;
        let mut mode = broker.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));

        let terminated: i64 = sql::query_scalar(
            "SELECT count(*) FROM (
                 SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                 WHERE application_name = $1
             ) AS killed",
        )
        .bind(&application_name)
        .fetch_one(&crate::test_support::migrated_pool().await)
        .await
        .expect("terminate the listener backend");
        assert_eq!(terminated, 1, "exactly the listener backend is terminated");

        let started = Instant::now();
        mode.pause(TASK_RESULT_REPOLL_SUBSCRIBED).await;

        assert!(
            started.elapsed() < WAKE_BOUND,
            "the gap must end the pause before the repoll interval: {:?}",
            started.elapsed()
        );
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));
    }

    /// An observe-only broker keeps its listener, as Python's web tool
    /// (`SchemaMode.OBSERVE_ONLY`): its waits subscribe and it can ping.
    #[tokio::test]
    async fn an_observe_only_broker_keeps_its_listener() {
        let config = PostgresConfig {
            database_url: crate::test_support::migrated_database_url().await,
            session_database: crate::core::config::SessionDatabase::SameAsDatabaseUrl,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            worker_pool_pre_ping: false,
            pool_size: 2,
            max_overflow: 0,
            retain_rerun_input_default: false,
            pool_timeout_seconds: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        };
        let broker = PostgresBroker::connect_observe_only(&config, crate::ClientFamily::Rust)
            .await
            .expect("connect an observe-only broker");

        let mode = broker.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));
        let pinged = broker
            .ping_workers(None, Duration::from_millis(200), None)
            .await;
        assert!(pinged.is_ok(), "{:?}", pinged.err());
    }

    /// Two brokers on one session URL in one process share one listener
    /// connection.
    #[tokio::test]
    async fn two_brokers_on_one_session_url_open_one_listener_connection() {
        let application_name = format!("shared_listener_{}", Uuid::new_v4().simple());
        let first = listener_broker(&application_name).await;
        let second = listener_broker(&application_name).await;

        let first_wait = first.result_wait_mode(Uuid::new_v4()).await;
        let second_wait = second.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(first_wait, ResultWaitMode::Subscribed(_)));
        assert!(matches!(second_wait, ResultWaitMode::Subscribed(_)));

        let connections: i64 =
            sql::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1")
                .bind(&application_name)
                .fetch_one(&crate::test_support::migrated_pool().await)
                .await
                .expect("count the listener connections");
        assert_eq!(connections, 1);
    }

    /// Dropping the broker stops its listener. The wait turns into polling,
    /// so the loop does not spin on a stopped subscription.
    #[tokio::test]
    async fn a_stopped_listener_turns_the_wait_into_polling() {
        let broker = listener_broker("result_wait_stop").await;
        let mut mode = broker.result_wait_mode(Uuid::new_v4()).await;
        assert!(matches!(mode, ResultWaitMode::Subscribed(_)));

        drop(broker);
        mode.pause(TASK_RESULT_REPOLL_SUBSCRIBED).await;

        assert!(matches!(mode, ResultWaitMode::Polling));
    }
}

#[cfg(test)]
mod stale_and_expired_list_tests {
    //! The stale and expired task lists: a threshold of whole minutes from 0
    //! to one year, a bound of `TASK_LIST_BOUND` rows, Python's verdict and
    //! order, and the expired record's `enqueued_at` and `expired_for`.
    use super::*;
    use crate::broker::result_types::DatabaseErrorCode;
    use crate::test_support::history::{covered_beat_instant, SeedInstant};
    use crate::test_support::sql::{self, TestPool, TestPoolOptions};

    #[tokio::test]
    async fn a_threshold_outside_one_year_is_refused_before_any_read() {
        let pool = TestPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies");
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

        for minutes in [
            -1,
            crate::broker::task_lists::MAX_STALE_THRESHOLD_MINUTES + 1,
        ] {
            let Err(refused) = broker.get_stale_tasks(minutes).await else {
                panic!("threshold {minutes} must be refused");
            };
            assert_eq!(refused.code(), DatabaseErrorCode::InvalidInput, "{minutes}");
        }
    }

    /// A row of `status`, enqueued 20 minutes ago. A RUNNING row was claimed
    /// and started 10 minutes ago: it holds its claim generation (CHECK
    /// `horsies_tasks_claim_generation_by_status`).
    async fn seed(
        pool: &TestPool,
        task_name: &str,
        status: &str,
        good_until_ago_min: Option<i32>,
    ) -> Uuid {
        let task_id = Uuid::new_v4();
        sql::query(
            "INSERT INTO horsies_tasks (
                 id, task_name, queue_name, priority, input, task_options, execution,
                 retention_class_key, sent_at, created_at, command_fingerprint_version,
                 command_fingerprint, status, enqueued_at, claimed_at, started_at, good_until,
                 retry_count, max_retries, is_workflow_task, updated_at, retain_rerun_input
             ) VALUES (
                 $1, $2, 'default', 100, '{}', NULL, 'ISOLATED',
                 'forever', NOW(), NOW(), 1,
                 decode(repeat('0d', 32), 'hex'), $3, NOW() - INTERVAL '20 minutes',
                 CASE WHEN $3 = 'RUNNING' THEN NOW() - INTERVAL '10 minutes' END,
                 CASE WHEN $3 = 'RUNNING' THEN NOW() - INTERVAL '10 minutes' END,
                 NOW() - $4 * INTERVAL '1 minute',
                 0, 0, FALSE, NOW(), FALSE
             )",
        )
        .bind(task_id)
        .bind(task_name)
        .bind(status)
        .bind(good_until_ago_min)
        .execute(pool)
        .await
        .expect("seed a baseline task");
        task_id
    }

    /// A runner beat of `task_id` sent `minutes_ago` minutes before the
    /// database clock, in the hourly leaf that the guard creates for it.
    async fn runner_beat(pool: &TestPool, task_id: Uuid, minutes_ago: i64) {
        let sent_at = covered_beat_instant(
            pool,
            SeedInstant::BeforeNow(chrono::Duration::minutes(minutes_ago)),
        )
        .await;
        sql::query(
            "INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
             VALUES ($1, 'list-worker', 'runner', $2, 'h', 1)",
        )
        .bind(task_id)
        .bind(sent_at.at())
        .execute(pool)
        .await
        .expect("seed a runner beat");
    }

    async fn clean(pool: &TestPool, task_name: &str) {
        sql::query(
            "DELETE FROM horsies_heartbeats WHERE task_id IN (
                 SELECT id FROM horsies_tasks WHERE task_name = $1)",
        )
        .bind(task_name)
        .execute(pool)
        .await
        .expect("clean beats");
        sql::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(pool)
            .await
            .expect("clean tasks");
    }

    /// A row with no beat and a row whose latest beat is older than the
    /// threshold are stale, beatless first; a fresh beat keeps a row out.
    #[tokio::test]
    async fn the_stale_list_follows_the_latest_beat() {
        let pool = crate::test_support::migrated_pool().await;
        let task_name = format!("stale_list_{}", Uuid::new_v4().simple());
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        let beatless = seed(&pool, &task_name, "RUNNING", None).await;
        let old_beat = seed(&pool, &task_name, "RUNNING", None).await;
        let fresh_beat = seed(&pool, &task_name, "RUNNING", None).await;
        runner_beat(&pool, old_beat, 5).await;
        runner_beat(&pool, fresh_beat, 0).await;

        let list = broker.get_stale_tasks(1).await.expect("stale list");

        let ours: Vec<&StaleTaskRow> = list
            .rows()
            .iter()
            .filter(|row| row.task_name == task_name)
            .collect();
        assert_eq!(
            ours.iter().map(|row| row.task_id).collect::<Vec<_>>(),
            vec![beatless, old_beat]
        );
        assert_eq!(ours[0].last_heartbeat_at, None);
        assert!(ours[1].last_heartbeat_at.is_some());
        clean(&pool, &task_name).await;
    }

    /// Python's expired record: `enqueued_at` and how long ago the deadline
    /// passed, on the database clock.
    #[tokio::test]
    async fn the_expired_list_carries_enqueued_at_and_expired_for() {
        let pool = crate::test_support::migrated_pool().await;
        let task_name = format!("expired_list_{}", Uuid::new_v4().simple());
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        let expired = seed(&pool, &task_name, "PENDING", Some(2)).await;
        let _not_yet = seed(&pool, &task_name, "PENDING", Some(-5)).await;

        let list = broker.get_expired_tasks().await.expect("expired list");

        let ours: Vec<&ExpiredTaskRow> = list
            .rows()
            .iter()
            .filter(|row| row.task_name == task_name)
            .collect();
        assert_eq!(ours.len(), 1);
        assert_eq!(ours[0].task_id, expired);
        assert!(
            ours[0].expired_for >= std::time::Duration::from_secs(120),
            "{:?}",
            ours[0].expired_for
        );
        assert!(ours[0].enqueued_at < ours[0].good_until);
        clean(&pool, &task_name).await;
    }
}

#[cfg(test)]
mod enqueue_task_options_tests {
    //! Python `test_enqueue_retry_limit.py` and `test_broker_postgres.py`
    //! (`test_malformed_task_options_raises`,
    //! `test_task_options_with_non_dict_retry_policy_is_refused`): enqueue
    //! stores a policy's limit, no policy stores 0, and options the worker
    //! would refuse are refused before any statement.
    use super::*;

    fn policy(max_retries: u32, extra: &str) -> String {
        let intervals = vec!["60"; max_retries as usize].join(", ");
        format!(
            r#"{{"retry_policy": {{"max_retries": {max_retries}, "intervals_seconds": [{intervals}],
                "backoff_strategy": "fixed", "jitter": false,
                "retry_for": ["STORAGE_ERROR"]{extra}}},
              "good_until": null, "timeout_ms": null, "execution": "ISOLATED",
              "cancel_grace_ms": null}}"#
        )
    }

    #[test]
    fn no_options_and_no_policy_store_zero_retries() {
        for options in [
            None,
            Some(r#"{"retry_policy": null, "execution": "ISOLATED"}"#),
            Some("{}"),
        ] {
            assert_eq!(
                enqueue_max_retries("t", options).expect("accepted"),
                0,
                "{options:?}"
            );
        }
    }

    /// An empty text is not "no options": the retry limit refuses it with
    /// the reason of the dispatch reader (`read_stored_options`).
    #[test]
    fn an_empty_text_is_refused_with_the_dispatch_reason() {
        let reason = match crate::worker::retry::read_stored_options(Some("")) {
            crate::worker::retry::StoredOptions::Unreadable { reason } => reason,
            readable => panic!("the dispatch reads an empty text as {readable:?}"),
        };

        match enqueue_max_retries("t", Some("")) {
            Err(BrokerError::EnqueueContract(refusal)) => assert_eq!(
                refusal,
                EnqueueRefusal::by_rule(
                    SendPart::TaskOptions,
                    format!("task_options refused for t: {reason}"),
                )
            ),
            other => panic!("an empty text gave {other:?}"),
        }
    }

    /// A send command holds the values that a send of the same texts binds:
    /// the input, the options, the execution class, the retry limit, the
    /// retention class, the rerun-input posture and the version-1
    /// fingerprint, for the default and the `forever` retention and for both
    /// postures.
    #[test]
    fn a_send_command_holds_the_values_that_a_send_of_its_texts_binds() {
        let options = policy(4, "")
            .replace("\"ISOLATED\"", "\"SHARED_ASYNC\"")
            .replace("\"cancel_grace_ms\": null", "\"cancel_grace_ms\": 5000")
            .replace("\"timeout_ms\": null", "\"timeout_ms\": 30000");
        assert!(options.contains("\"cancel_grace_ms\": 5000"), "{options}");
        assert!(options.contains("\"timeout_ms\": 30000"), "{options}");
        for (retention, retain_rerun_input) in [
            (EnqueueRetention::Default, false),
            (EnqueueRetention::Forever, true),
        ] {
            let command = SendCommand::prepare(
                "tasks.report",
                "default",
                100,
                r#"{"value":1}"#,
                Some(options.as_str()),
                retention,
                retain_rerun_input,
            )
            .expect("the send command builds");
            let send = CheckedSend::check(
                "tasks.report",
                r#"{"value":1}"#,
                "default",
                100,
                None,
                None,
                None,
                Some(options.as_str()),
                Some(Uuid::from_u128(7)),
                None,
                None,
                retention,
                retain_rerun_input,
            )
            .expect("the send is checked");

            assert_eq!(
                command,
                SendCommand {
                    task_name: send.task_name.to_owned(),
                    queue_name: send.queue_name.to_owned(),
                    priority: send.priority,
                    stored_input: send.input.to_owned(),
                    task_options: send.task_options.map(str::to_owned),
                    execution: send.facts.execution,
                    max_retries: send.max_retries,
                    retention_class_key: send.facts.retention_class_key.clone(),
                    retain_rerun_input: send.facts.retain_rerun_input,
                    fingerprint_version: send.facts.command_fingerprint_version,
                    fingerprint: send.facts.command_fingerprint.to_vec(),
                },
                "{retention:?}"
            );
            assert_eq!(
                (command.execution, command.max_retries),
                (crate::core::task::execution::Execution::SharedAsync, 4)
            );
        }
    }

    /// A send command and a send read the options with the strict rule
    /// before the retry limit, so both refuse an empty text with the strict
    /// read's error, not with the retry limit's.
    #[test]
    fn both_send_paths_refuse_an_empty_text_with_the_strict_read_error() {
        let expected = EnqueueRefusal {
            part: SendPart::TaskOptions,
            message: "task_options JSON is refused: JSON text is empty".to_owned(),
            cause: crate::broker::error::EnqueueRefusalCause::JsonUnreadable(
                "JSON text is empty".to_owned(),
            ),
        };
        let command = SendCommand::prepare(
            "t",
            "default",
            100,
            "{}",
            Some(""),
            EnqueueRetention::Forever,
            false,
        )
        .map(|_command| ());
        let send = CheckedSend::check(
            "t",
            "{}",
            "default",
            100,
            None,
            None,
            None,
            Some(""),
            Some(Uuid::from_u128(7)),
            None,
            None,
            EnqueueRetention::Forever,
            false,
        )
        .map(|_send| ());

        for (path, outcome) in [
            ("SendCommand::prepare", command),
            ("CheckedSend::check", send),
        ] {
            match outcome {
                Err(BrokerError::EnqueueContract(refusal)) => {
                    assert_eq!(refusal, expected, "{path}");
                }
                other => panic!("{path}: an empty text gave {other:?}"),
            }
        }
    }

    /// Only a new send refuses a shared class with no `timeout_ms`. A send
    /// command (the command of a schedule) keeps such options, and the
    /// worker refuses the run.
    #[test]
    fn only_a_new_send_refuses_a_shared_class_with_no_timeout() {
        let options = r#"{"execution": "SHARED_THREAD", "timeout_ms": null}"#;
        let command = SendCommand::prepare(
            "tasks.report",
            "default",
            100,
            "{}",
            Some(options),
            EnqueueRetention::Forever,
            false,
        )
        .expect("a send command takes a shared class with no timeout");
        assert_eq!(
            command.execution,
            crate::core::task::execution::Execution::SharedThread
        );
        assert_eq!(command.task_options.as_deref(), Some(options));

        let send = CheckedSend::check(
            "tasks.report",
            "{}",
            "default",
            100,
            None,
            None,
            None,
            Some(options),
            Some(Uuid::from_u128(7)),
            None,
            None,
            EnqueueRetention::Forever,
            false,
        )
        .map(|_send| ());
        let expected = EnqueueRefusal {
            part: SendPart::TaskOptions,
            message: "task_options refused: timeout_ms is absent for execution SHARED_THREAD; \
                      a shared class requires timeout_ms"
                .to_owned(),
            cause: crate::broker::error::EnqueueRefusalCause::RuleRefused,
        };
        match send {
            Err(BrokerError::EnqueueContract(refusal)) => assert_eq!(refusal, expected),
            other => panic!("a new send of a shared class with no timeout gave {other:?}"),
        }
    }

    #[test]
    fn a_valid_policy_stores_its_limit() {
        for limit in [1, 3, 20] {
            assert_eq!(
                enqueue_max_retries("t", Some(&policy(limit, ""))).expect("accepted"),
                limit as i32
            );
        }
    }

    #[test]
    fn options_the_worker_would_refuse_are_refused() {
        let over_limit = policy(21, "");
        let unknown_field = policy(2, r#", "extra": 1"#);
        for options in [
            "not json",
            "[]",
            r#"{"retry_policy": 5}"#,
            r#"{"retry_policy": {"max_retries": 2}}"#,
            over_limit.as_str(),
            unknown_field.as_str(),
        ] {
            assert!(
                matches!(
                    enqueue_max_retries("t", Some(options)),
                    Err(BrokerError::EnqueueContract(_))
                ),
                "{options}"
            );
        }
    }

    /// What the core writes for a task is what its enqueue accepts.
    #[test]
    fn the_core_written_options_are_accepted() {
        let options = crate::core::TaskOptions {
            task_name: "t".to_owned(),
            queue: None,
            good_until: None,
            retry_for: Some(vec![crate::core::TaskErrorCode::Custom(
                crate::core::CustomCode::parse("X").expect("X is a custom code"),
            )]),
            retry_policy: Some(crate::core::RetryPolicy::exponential(30, 4, true).unwrap()),
            timeout_ms: Some(1_000),
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };
        let text = options.stored_options_text();

        assert_eq!(enqueue_max_retries("t", Some(&text)).expect("accepted"), 4);
    }
}

#[cfg(test)]
mod checked_send_trace_tests {
    //! The trace carrier of a checked send: none by default, the checked
    //! carrier when it holds every bound, none when it breaks one. The send
    //! itself is never refused for its carrier.
    use std::collections::BTreeMap;

    use super::*;
    use crate::telemetry::trace_carrier::{PropagatedTrace, MAX_TRACE_CARRIER_KEYS};

    fn checked_send() -> CheckedSend<'static> {
        CheckedSend::check(
            "tasks.traced",
            "{}",
            "default",
            100,
            None,
            None,
            None,
            None,
            Some(Uuid::from_u128(7)),
            None,
            None,
            EnqueueRetention::Forever,
            false,
        )
        .expect("the send is checked")
    }

    fn propagated(carrier: BTreeMap<String, String>) -> SendTrace {
        SendTrace::Propagated(PropagatedTrace {
            carrier,
            trace_id: (1..=16).collect(),
        })
    }

    #[test]
    fn a_checked_send_has_no_carrier() {
        assert_eq!(checked_send().trace, StoredTrace::NoCarrier);
    }

    #[test]
    fn a_send_with_a_carrier_stores_its_checked_text() {
        let send = checked_send().with_trace(propagated(BTreeMap::from([(
            "traceparent".to_owned(),
            "00-0102-03-01".to_owned(),
        )])));
        assert_eq!(
            send.trace.columns(),
            (
                "CARRIER",
                Some("{\"traceparent\":\"00-0102-03-01\"}"),
                Some((1..=16).collect::<Vec<u8>>().as_slice())
            )
        );
        assert_eq!(send.task_id(), Uuid::from_u128(7));
    }

    #[test]
    fn a_send_with_a_carrier_over_a_bound_keeps_the_send_and_stores_no_carrier() {
        let over: BTreeMap<String, String> = (0..=MAX_TRACE_CARRIER_KEYS)
            .map(|index| (format!("k{index}"), "v".to_owned()))
            .collect();
        let send = checked_send().with_trace(propagated(over));
        assert_eq!(send.trace, StoredTrace::NoCarrier);
        assert_eq!(send.task_id(), Uuid::from_u128(7));
        assert_eq!(send.task_name, "tasks.traced");
    }
}

#[cfg(test)]
mod client_family_tests {
    //! The client family at install and before the first write: a handle
    //! writes only into a database of its own family, and reads are not
    //! checked.
    use super::*;
    use crate::test_support::sql::{self, TestPool};

    async fn family_row(pool: &TestPool) -> Option<String> {
        sql::query_scalar("SELECT family FROM horsies_client_family")
            .fetch_optional(pool)
            .await
            .expect("read the family row")
    }

    /// The test database is installed for the Rust client.
    #[tokio::test]
    async fn the_install_records_the_family_and_a_rust_handle_may_write() {
        let pool = crate::test_support::migrated_pool().await;
        assert_eq!(family_row(&pool).await.as_deref(), Some("RUST"));

        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        broker
            .ensure_client_family_for_write()
            .await
            .expect("a Rust handle writes into a Rust database");
    }

    /// A handle of the other family is refused before it writes, and the
    /// refusal is not retryable. The check reads only.
    #[tokio::test]
    async fn a_handle_of_the_other_family_is_refused_before_its_first_write() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), ClientFamily::Python);

        let refused = broker.ensure_client_family_for_write().await.unwrap_err();

        assert!(matches!(
            refused,
            BrokerError::ClientFamilyMismatch {
                stored: crate::broker::client_family::RecordedFamily::Recorded(ClientFamily::Rust),
                own: ClientFamily::Python,
            }
        ));
        assert!(!refused.is_retryable());
        assert_eq!(family_row(&pool).await.as_deref(), Some("RUST"));
    }

    /// A handle with migrations off (observe only) runs no install, so its
    /// first send checks the family: on a database of the other family the
    /// send is refused before the insert, not retryable, and writes no row.
    #[tokio::test]
    async fn a_send_of_a_handle_with_migrations_off_into_the_other_family_is_refused() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url_observe_only(pool.url(), ClientFamily::Python);
        let task_id = Uuid::new_v4();

        let refused = broker
            .enqueue(
                "family.send",
                "{}",
                "default",
                50,
                Some(Utc::now()),
                None,
                None,
                None,
                Some(task_id),
                None,
                None,
                EnqueueRetention::Default,
                None,
                SendKind::First,
            )
            .await
            .expect_err("a Python handle does not send into a Rust database");

        assert!(matches!(
            refused,
            BrokerError::ClientFamilyMismatch {
                own: ClientFamily::Python,
                ..
            }
        ));
        assert!(!refused.is_retryable());
        let rows: i64 = sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("count the rows of the refused send");
        assert_eq!(rows, 0);
    }

    /// Writes `value` into the family row past its CHECK
    /// (`horsies_client_family_family_known`), which it drops first: stored
    /// data that no horsies path writes.
    async fn corrupt_family_row(pool: &TestPool, value: &str) {
        sql::query(
            "ALTER TABLE horsies_client_family \
             DROP CONSTRAINT horsies_client_family_family_known",
        )
        .execute(pool)
        .await
        .expect("drop the family CHECK");
        sql::query("UPDATE horsies_client_family SET family = $1")
            .bind(value)
            .execute(pool)
            .await
            .expect("write the corrupt family");
    }

    /// A family row that holds no client family is corrupt stored data. For
    /// a handle of either family, the install, the check before the first
    /// write and the first send are refused with the stored value, not
    /// retryable, and the send writes no row.
    #[tokio::test]
    async fn a_family_row_that_holds_no_client_family_is_refused_before_any_write() {
        let pool = crate::test_support::migrated_pool().await;
        corrupt_family_row(&pool, "GO").await;

        for family in [ClientFamily::Python, ClientFamily::Rust] {
            let installing = PostgresBroker::from_test_url(pool.url(), family);
            let refused = installing
                .migrate()
                .await
                .expect_err("no install into a database whose family row is corrupt");
            assert!(
                matches!(&refused, BrokerError::UnknownStoredFamily(unknown) if unknown.stored == "GO"),
                "{refused:?}"
            );
            assert!(!refused.is_retryable());

            let broker = PostgresBroker::from_test_url_observe_only(pool.url(), family);
            let refused = broker
                .ensure_client_family_for_write()
                .await
                .expect_err("no write into a database whose family row is corrupt");
            assert!(
                matches!(&refused, BrokerError::UnknownStoredFamily(unknown) if unknown.stored == "GO"),
                "{refused:?}"
            );
            assert!(!refused.is_retryable());

            let task_id = Uuid::new_v4();
            let refused = broker
                .enqueue(
                    "family.send",
                    "{}",
                    "default",
                    50,
                    Some(Utc::now()),
                    None,
                    None,
                    None,
                    Some(task_id),
                    None,
                    None,
                    EnqueueRetention::Default,
                    None,
                    SendKind::First,
                )
                .await
                .expect_err("no send into a database whose family row is corrupt");
            assert!(
                matches!(&refused, BrokerError::UnknownStoredFamily(unknown) if unknown.stored == "GO"),
                "{refused:?}"
            );
            let rows: i64 = sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("count the rows of the refused send");
            assert_eq!(rows, 0);
        }
        assert_eq!(family_row(&pool).await.as_deref(), Some("GO"));
    }

    /// A database whose family row holds `TYPESCRIPT` (the CHECK admits it
    /// from migration 1006 on) is of the TypeScript client. A Python and a
    /// Rust handle are refused before the first write, at the install and
    /// at the check, with the stored family named, not retryable; a
    /// TypeScript handle passes the check. The family row does not change.
    #[tokio::test]
    async fn a_typescript_database_refuses_a_python_and_a_rust_handle_before_any_write() {
        let pool = crate::test_support::migrated_pool().await;
        sql::query("UPDATE horsies_client_family SET family = 'TYPESCRIPT'")
            .execute(&pool)
            .await
            .expect("the family CHECK admits TYPESCRIPT");
        let typescript =
            crate::broker::client_family::RecordedFamily::Recorded(ClientFamily::TypeScript);

        for family in [ClientFamily::Python, ClientFamily::Rust] {
            let installing = PostgresBroker::from_test_url(pool.url(), family);
            let refused = installing
                .migrate()
                .await
                .expect_err("no install into a database of another family");
            assert!(
                matches!(
                    &refused,
                    BrokerError::ClientFamilyMismatch { stored, own }
                        if *stored == typescript && *own == family
                ),
                "{refused:?}"
            );
            assert!(!refused.is_retryable());
            assert!(
                refused.to_string().contains("the TYPESCRIPT client"),
                "{refused}"
            );

            let broker = PostgresBroker::from_test_url_observe_only(pool.url(), family);
            let refused = broker
                .ensure_client_family_for_write()
                .await
                .expect_err("no write into a database of another family");
            assert!(
                matches!(
                    &refused,
                    BrokerError::ClientFamilyMismatch { stored, own }
                        if *stored == typescript && *own == family
                ),
                "{refused:?}"
            );
            assert!(!refused.is_retryable());
        }
        let broker =
            PostgresBroker::from_test_url_observe_only(pool.url(), ClientFamily::TypeScript);
        broker
            .ensure_client_family_for_write()
            .await
            .expect("a TypeScript handle writes into a TypeScript database");
        assert_eq!(family_row(&pool).await.as_deref(), Some("TYPESCRIPT"));
    }
}

#[cfg(test)]
mod task_info_code_tests {
    //! Python's task info codes (`clients/python/horsies/core/brokers/postgres.py:4889-4904`,
    //! `:4973-4977`): only a stored result that fails its check is
    //! `INVALID_JSON_PAYLOAD`.
    use super::*;
    use crate::broker::result_types::DatabaseErrorCode;
    use crate::db::StatementFailure;

    /// The task of the reads in these tests.
    const TASK_INFO_TEST_ID: Uuid = Uuid::from_u128(0x0192_b3a4_0000_7000_8000_0000_0000_0002);

    #[test]
    fn a_result_that_does_not_parse_is_invalid_json_payload() {
        let parse_error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let detail =
            BrokerError::Serialization(serde_json::from_str::<serde_json::Value>("{").unwrap_err())
                .to_string();

        let error =
            task_info_history_error(TASK_INFO_TEST_ID, BrokerError::Serialization(parse_error));

        assert_eq!(error.code(), DatabaseErrorCode::InvalidJsonPayload);
        assert!(!error.retryable());
        // Python's message of a task info read names the task.
        assert_eq!(
            error.message(),
            format!("get_task_info failed for {TASK_INFO_TEST_ID}: {detail}")
        );
    }

    /// A task that moved between the two reads on every pass is
    /// `TASK_INFO_QUERY_FAILED`, retryable, with Python's message.
    #[test]
    fn a_task_that_moved_on_every_pass_is_retryable_with_python_s_message() {
        let error = DatabaseOperationError::task_info_query_failed(
            TASK_INFO_TEST_ID,
            BrokerOperationSource::TaskInfoMoved {
                task_id: TASK_INFO_TEST_ID,
                passes: TASK_INFO_READ_PASSES,
            },
        );

        assert_eq!(error.code(), DatabaseErrorCode::TaskInfoQueryFailed);
        assert!(error.retryable());
        assert!(error.message().starts_with(&format!(
            "get_task_info failed for {TASK_INFO_TEST_ID}: task {TASK_INFO_TEST_ID} was absent"
        )));
    }

    #[test]
    fn a_status_or_attempt_that_does_not_decode_is_task_info_query_failed() {
        for error in [
            BrokerError::InvalidStatus("BOGUS".to_owned()),
            BrokerError::HistoryReadContract("unknown task-attempt outcome".to_owned()),
        ] {
            let mapped = task_info_history_error(TASK_INFO_TEST_ID, error);
            assert_eq!(mapped.code(), DatabaseErrorCode::TaskInfoQueryFailed);
            assert!(!mapped.retryable());
        }
    }

    #[test]
    fn a_core_statement_failure_is_task_info_query_failed_with_its_transience() {
        let timed_out = task_info_history_error(
            TASK_INFO_TEST_ID,
            BrokerError::Statement(StatementFailure::NotSent {
                statement: "select_task_attempts",
                cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Main,
                    waited: Duration::from_secs(30),
                }),
            }),
        );
        assert_eq!(timed_out.code(), DatabaseErrorCode::TaskInfoQueryFailed);
        assert!(timed_out.retryable());

        let undecodable = task_info_history_error(
            TASK_INFO_TEST_ID,
            BrokerError::RowDecode(RowDecodeError::NoRow {
                statement: "live_task_exists",
            }),
        );
        assert_eq!(undecodable.code(), DatabaseErrorCode::TaskInfoQueryFailed);
        assert!(!undecodable.retryable());
    }

    /// A FAILED history row whose stored result is `payload`, with a
    /// matching digest and envelope version 1.
    fn history_detail_with_result(payload: &[u8]) -> HistoryTaskDetail {
        let at = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 7, 0, 0, 0).unwrap();
        HistoryTaskDetail {
            task_id: test_uuid("0192b3a4-0000-7000-8000-000000000001"),
            task_name: "stored_result".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            status: crate::core::TaskStatus::Failed,
            terminalization_kind: "FAIL_RUNNING".to_owned(),
            terminal_at: at,
            retention_class_key: "forever".to_owned(),
            enqueued_at: at,
            created_at: at,
            sent_at: Some(at),
            claimed_at: Some(at),
            started_at: Some(at),
            good_until: None,
            retry_count: 0,
            max_retries: 0,
            last_claimed_worker_id: Some("w-1".to_owned()),
            last_worker_hostname: Some("host-1".to_owned()),
            last_worker_pid: Some(100),
            result_envelope_version: crate::core::history::archive::versions::ARCHIVE_VERSION_1,
            result_codec: crate::core::history::archive::versions::JSON_UTF8_CODEC.to_owned(),
            result_content_type: crate::core::history::archive::versions::JSON_CONTENT_TYPE
                .to_owned(),
            result_payload: Some(payload.to_vec()),
            result_digest: Some(
                crate::core::history::archive::versions::archive_digest(payload).to_vec(),
            ),
            error_code: Some("BOOM".to_owned()),
            final_failed_reason: None,
            rerun_of_task_id: None,
            rerun_root_task_id: None,
            workflow_id: None,
            is_workflow_task: false,
            rerun_input_disposition: "NOT_REQUESTED".to_owned(),
            rerun_input_version: None,
            rerun_input_codec: None,
            rerun_input_content_type: None,
            rerun_input_digest: None,
            rerun_input_inline: None,
            attempts: Vec::new(),
        }
    }

    /// Asserts that the task info of a history row with `payload` as its
    /// stored result is `INVALID_JSON_PAYLOAD`, not retryable.
    fn assert_refused_as_invalid_json_payload(payload: &[u8]) {
        let shown = String::from_utf8_lossy(payload);
        let Err(refused) =
            history_task_info(history_detail_with_result(payload), true, false, false)
        else {
            panic!("a stored result of {shown:.40} must not read");
        };
        assert_eq!(
            refused.code(),
            DatabaseErrorCode::InvalidJsonPayload,
            "{shown:.40}"
        );
        assert!(!refused.retryable(), "{shown:.40}");
    }

    fn history_attempt(
        attempt: i32,
        outcome: &str,
        will_retry: bool,
    ) -> crate::core::history::archive::attempts::AttemptRecord {
        let at = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 7, 0, 0, 0).unwrap();
        crate::core::history::archive::attempts::AttemptRecord::new(
            attempt,
            outcome,
            will_retry,
            at,
            at,
            None,
            None,
            None,
            Some("w-1".to_owned()),
            Some("host-1".to_owned()),
            Some(100),
            None,
        )
        .expect("a valid attempt record")
    }

    /// Python `test_broker_postgres.py::TestHistoryTaskInfoResultDecode::test_attempts_are_listed_newest_first`:
    /// the snapshot holds the attempts oldest first, and the task info lists
    /// them newest first, as the live read does.
    #[test]
    fn history_attempts_are_listed_newest_first() {
        let mut detail = history_detail_with_result(b"{}");
        detail.attempts = vec![
            history_attempt(1, "FAILED", true),
            history_attempt(2, "FAILED", false),
        ];

        let info = history_task_info(detail, false, false, true)
            .expect("a history row with two attempts reads");

        let attempts = info.attempts.expect("the read asked for the attempts");
        assert_eq!(
            attempts
                .iter()
                .map(|attempt| attempt.attempt)
                .collect::<Vec<i32>>(),
            vec![2, 1],
        );
    }

    /// Python `test_broker_postgres.py::TestHistoryTaskInfoResultDecode::test_stored_result_is_refused_as_invalid_json_payload`,
    /// the payloads that every build refuses: `NaN` and an empty payload.
    #[test]
    fn a_stored_result_outside_strict_json_is_invalid_json_payload() {
        for payload in [&b"{\"x\":NaN}"[..], &b""[..]] {
            assert_refused_as_invalid_json_payload(payload);
        }
    }

    /// Python `test_broker_postgres.py::TestHistoryTaskInfoResultDecode::test_stored_result_is_refused_as_invalid_json_payload`,
    /// the payloads past the strict read rule (`core::codec::check_json_bytes`):
    /// a float out of range, an integer of 5000 digits and a repeated key,
    /// which the strict check of the stored text refuses in every build.
    #[test]
    fn a_stored_result_past_the_strict_rule_is_invalid_json_payload_in_every_build() {
        let long_integer = format!("{{\"x\":{}}}", "1".repeat(5000));
        for payload in [
            &b"{\"x\":1e9999}"[..],
            long_integer.as_bytes(),
            &b"{\"x\":1,\"x\":2}"[..],
        ] {
            assert_refused_as_invalid_json_payload(payload);
        }
    }

    /// Python `PostgresBroker._history_task_info`: the stored result is
    /// returned as stored, with no typed decode. An error with a plain
    /// built-in name as its code, an error without its null keys, a value
    /// that is not an envelope and an integer above `u64` all come back
    /// byte for byte; the client's typed read decides on them.
    #[test]
    fn a_history_result_is_returned_as_stored() {
        for payload in [
            r#"{"__h_task_result__":true,"ok":null,"err":{"error_code":"TASK_EXPIRED"}}"#,
            r#"{"__h_task_result__":true,"ok":null,"err":{"error_code":"E2"}}"#,
            r#"{"not":"an envelope"}"#,
            r#"{"__h_task_result__":true,"ok":18446744073709551616,"err":null}"#,
        ] {
            let info = history_task_info(
                history_detail_with_result(payload.as_bytes()),
                true,
                false,
                false,
            )
            .unwrap_or_else(|refused| panic!("{payload}: {refused}"));
            match &info.result {
                ResultSlot::Stored(envelope) => assert_eq!(envelope.as_str(), payload),
                other => panic!("{payload}: expected a stored result, got {other:?}"),
            }
        }
    }

    /// Python: a stored JSON `null` is no result.
    #[test]
    fn a_history_result_of_null_is_not_stored() {
        let info = history_task_info(history_detail_with_result(b"null"), true, false, false)
            .expect("a stored null reads");
        assert_eq!(info.result, ResultSlot::NotStored);
    }

    /// A history row with no result payload stores no result.
    #[test]
    fn a_history_row_without_a_payload_is_not_stored() {
        let mut detail = history_detail_with_result(b"{}");
        detail.result_payload = None;
        let info =
            history_task_info(detail, true, false, false).expect("a row without a payload reads");
        assert_eq!(info.result, ResultSlot::NotStored);
    }

    /// Python: a stored result that is neither an object nor `null` is
    /// `INVALID_JSON_PAYLOAD`.
    #[test]
    fn a_history_result_that_is_not_an_object_is_invalid_json_payload() {
        for payload in [&b"[]"[..], &b"\"text\""[..], &b"1"[..], &b"true"[..]] {
            assert_refused_as_invalid_json_payload(payload);
        }
    }

    /// The result is read only when the caller asks for it.
    #[test]
    fn a_history_result_is_not_read_unless_asked() {
        let info = history_task_info(history_detail_with_result(b"[]"), false, false, false)
            .expect("a result that is not read is not checked");
        assert_eq!(info.result, ResultSlot::NotRequested);
    }
}

mod connectivity;
pub use connectivity::{ConnectivityRefusal, ListenProbeFailure};

#[cfg(test)]
mod ping_and_wait_tests;
