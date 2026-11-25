//! Seeds and reads that the lifecycle ports share.
//!
//! Every seed names every column of its baseline table
//! (`core/baseline/0001_tables.sql`). The baseline has no column
//! defaults, so a seed that leaves a column out does not insert.
//!
//! Times that a guard compares with the database clock (`started_at`,
//! `finalizing_at`, `good_until`, heartbeat `sent_at`) are set relative to
//! `NOW()` in SQL, as the Python tests do, so the worker clock never enters a
//! comparison.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, TimeZone, Timelike, Utc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_postgres::error::DbError;
use tokio_postgres::Row;
use uuid::Uuid;

use crate::broker::attempt_writes::{
    finish_failed_attempt, finish_unreported_attempt, finish_workflow_success, FinishFailedAttempt,
    FinishWorkflowSuccess,
};
use crate::broker::core_rows::StatementLog;
use crate::broker::error::BrokerError;
use crate::broker::pools::core_pool_for_test_url;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::history::archive::attempts::{
    decode_attempt_snapshot, encode_attempt_snapshot, AttemptRecord,
};
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::lifecycle::{
    BatchSize, FailedAttemptOutcome, OwnedClaim, TerminalizationCommand, TerminalizationOutcome,
    UnreportedAttemptOutcome, WorkflowSuccessOutcome,
};
use crate::db::{CheckoutClass, CorePool, StatementBound, StatementFailure, TypedStatement};
use crate::test_support::history::{covered_beat_instant, SeedInstant};
use crate::test_support::sql::{self, TestConnection, TestListener, TestPool, TestPoolOptions};

/// The worker that holds the seeded claims.
pub(super) const WORKER: &str = "lifecycle-test-worker";

/// A worker that is not this test's worker.
pub(super) const OTHER_WORKER: &str = "someone-else";

/// Bound of the passes that drain a sweep before a test seeds its rows.
const DRAIN_PASS_LIMIT: u32 = 10_000;

/// Rows per pass of a drained sweep.
const DRAIN_BATCH_SIZE: i32 = 500;

/// Bound of the wait for a lock: 100 polls of 10 ms.
const LOCK_WAIT_POLLS: u32 = 100;
const LOCK_WAIT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// The claim generation of a seeded claim (Python's `GENERATION`).
pub(super) fn generation() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 4, 9, 0, 0)
        .single()
        .expect("the test generation is a valid instant")
}

/// The fence of a claim that `WORKER` holds at `generation()`.
pub(super) fn owned_claim() -> OwnedClaim {
    OwnedClaim {
        worker_id: WORKER.to_owned(),
        claimed_at: generation(),
    }
}

/// The migrated database of the running test (`migrated_pool`).
pub(super) async fn pool() -> TestPool {
    crate::test_support::migrated_pool().await
}

/// The live status of a seeded task row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LiveStatus {
    Pending,
    Claimed,
    Running,
}

impl LiveStatus {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Claimed => "CLAIMED",
            Self::Running => "RUNNING",
        }
    }
}

/// Who holds a seeded task row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Holder {
    /// No claim: the claim fields are NULL.
    Unclaimed,
    /// A claim of `worker_id` at generation `claimed_at`.
    Claim {
        worker_id: String,
        claimed_at: DateTime<Utc>,
    },
}

impl Holder {
    /// The claim of `WORKER` at `generation()`.
    pub(super) fn this_worker() -> Self {
        Self::Claim {
            worker_id: WORKER.to_owned(),
            claimed_at: generation(),
        }
    }

    /// The claim of `OTHER_WORKER` at `generation()`.
    pub(super) fn other_worker() -> Self {
        Self::Claim {
            worker_id: OTHER_WORKER.to_owned(),
            claimed_at: generation(),
        }
    }

    fn worker_id(&self) -> Option<&str> {
        match self {
            Self::Unclaimed => None,
            Self::Claim { worker_id, .. } => Some(worker_id.as_str()),
        }
    }

    fn claimed_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Unclaimed => None,
            Self::Claim { claimed_at, .. } => Some(*claimed_at),
        }
    }
}

/// Where a seeded task comes from: a send, or a workflow node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Origin {
    /// A sent task: it carries a command fingerprint.
    Send,
    /// A workflow task: no command fingerprint.
    WorkflowNode,
}

/// One live task row. `started_at` is `NOW()` for a RUNNING row and NULL
/// otherwise; `claim_expires_at` is five minutes ahead for a CLAIMED row and
/// NULL otherwise; the worker fields are set for a RUNNING row only.
#[derive(Debug, Clone)]
pub(super) struct TaskSeed {
    pub(super) id: Uuid,
    pub(super) status: LiveStatus,
    pub(super) holder: Holder,
    pub(super) origin: Origin,
    pub(super) queue_name: &'static str,
    pub(super) execution: &'static str,
    pub(super) input: &'static str,
    pub(super) retention_class_key: &'static str,
    pub(super) task_options: Option<String>,
    pub(super) retry_count: i32,
    pub(super) max_retries: i32,
    pub(super) retain_rerun_input: bool,
}

impl TaskSeed {
    /// A RUNNING row that `WORKER` holds at `generation()`.
    pub(super) fn running() -> Self {
        Self {
            id: Uuid::new_v4(),
            status: LiveStatus::Running,
            holder: Holder::this_worker(),
            origin: Origin::Send,
            queue_name: "default",
            execution: "ISOLATED",
            input: "{}",
            retention_class_key: "forever",
            task_options: None,
            retry_count: 0,
            max_retries: 0,
            retain_rerun_input: false,
        }
    }

    /// A CLAIMED row that `WORKER` holds at `generation()`.
    pub(super) fn claimed() -> Self {
        Self {
            status: LiveStatus::Claimed,
            ..Self::running()
        }
    }

    /// A PENDING row with no claim.
    pub(super) fn pending() -> Self {
        Self {
            status: LiveStatus::Pending,
            holder: Holder::Unclaimed,
            ..Self::running()
        }
    }
}

/// Insert one live task row with every baseline column named.
pub(super) async fn seed_task(pool: &TestPool, seed: &TaskSeed) -> Uuid {
    let (fingerprint_version, fingerprint): (Option<i16>, Option<Vec<u8>>) = match seed.origin {
        Origin::Send => (Some(1), Some(vec![7_u8; 32])),
        Origin::WorkflowNode => (None, None),
    };
    let is_workflow_task = match seed.origin {
        Origin::Send => false,
        Origin::WorkflowNode => true,
    };
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
            $1, 'lifecycle.test', $2, 100, $14, $3, $13,
            $15, NOW(), NOW(),
            $4, $5, NULL,
            $6, NOW(), $7, $8,
            CASE WHEN $6 = 'CLAIMED' THEN NOW() + interval '5 minutes' END,
            CASE WHEN $6 = 'RUNNING' THEN NOW() END, NULL, NULL,
            CASE WHEN $6 = 'RUNNING' THEN 4242 END,
            CASE WHEN $6 = 'RUNNING' THEN 'lifecycle-host' END,
            CASE WHEN $6 = 'RUNNING' THEN 'lifecycle-process' END,
            $9, $10, NULL, NULL, $11,
            NOW(), $12, NULL, NULL
        )",
    )
    .bind(seed.id)
    .bind(seed.queue_name)
    .bind(seed.task_options.as_deref())
    .bind(fingerprint_version)
    .bind(fingerprint)
    .bind(seed.status.as_sql())
    .bind(seed.holder.claimed_at())
    .bind(seed.holder.worker_id())
    .bind(seed.retry_count)
    .bind(seed.max_retries)
    .bind(is_workflow_task)
    .bind(seed.retain_rerun_input)
    .bind(seed.execution)
    .bind(seed.input)
    .bind(seed.retention_class_key)
    .execute(pool)
    .await
    .expect("seed live task row");
    seed.id
}

/// A point in time relative to the database clock.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Since {
    /// The column is NULL.
    Never,
    /// `NOW()` minus this many seconds.
    SecondsAgo(f64),
}

/// Set `started_at` (Python's `_age`, first half).
pub(super) async fn set_started(pool: &TestPool, task_id: Uuid, since: Since) {
    let seconds_ago: Option<f64> = match since {
        Since::Never => None,
        Since::SecondsAgo(seconds) => Some(seconds),
    };
    sql::query(
        "UPDATE horsies_tasks
         SET started_at = CASE WHEN $2::float8 IS NULL THEN NULL
                               ELSE NOW() - make_interval(secs => $2::float8) END
         WHERE id = $1",
    )
    .bind(task_id)
    .bind(seconds_ago)
    .execute(pool)
    .await
    .expect("set started_at");
}

/// Set the finalizing marker of `WORKER` (Python's `_age`, second half).
pub(super) async fn set_finalizing(pool: &TestPool, task_id: Uuid, since: Since) {
    let seconds_ago: Option<f64> = match since {
        Since::Never => None,
        Since::SecondsAgo(seconds) => Some(seconds),
    };
    sql::query(
        "UPDATE horsies_tasks
         SET finalizing_at = CASE WHEN $2::float8 IS NULL THEN NULL
                                  ELSE NOW() - make_interval(secs => $2::float8) END,
             finalizing_by_worker_id = CASE WHEN $2::float8 IS NULL THEN NULL ELSE $3 END
         WHERE id = $1",
    )
    .bind(task_id)
    .bind(seconds_ago)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("set finalizing marker");
}

/// The deadline of a seeded row (Python's `_deadline`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Deadline {
    /// `good_until` is NULL.
    Absent,
    /// `good_until` is this many seconds before `NOW()`.
    PassedSecondsAgo(f64),
    /// `good_until` is this many seconds after `NOW()`.
    AheadSeconds(f64),
}

/// Set `good_until` relative to the database clock.
pub(super) async fn set_deadline(pool: &TestPool, task_id: Uuid, deadline: Deadline) {
    let offset_seconds: Option<f64> = match deadline {
        Deadline::Absent => None,
        Deadline::PassedSecondsAgo(seconds) => Some(-seconds),
        Deadline::AheadSeconds(seconds) => Some(seconds),
    };
    sql::query(
        "UPDATE horsies_tasks
         SET good_until = CASE WHEN $2::float8 IS NULL THEN NULL
                               ELSE NOW() + make_interval(secs => $2::float8) END
         WHERE id = $1",
    )
    .bind(task_id)
    .bind(offset_seconds)
    .execute(pool)
    .await
    .expect("set good_until");
}

/// Leave the row where an automatic retry leaves it (Python's
/// `_requeued_after_failed_attempt`): attempt 1 FAILED with `will_retry` and
/// its reason, and `retry_count` 1. The live row stores no failed reason.
pub(super) async fn record_failed_first_attempt(
    pool: &TestPool,
    task_id: Uuid,
    failed_reason: &str,
) {
    sql::query(
        "INSERT INTO horsies_task_attempts (
            task_id, attempt, outcome, will_retry, started_at, finished_at,
            error_code, error_message, failed_reason, worker_id,
            worker_hostname, worker_pid, worker_process_name
        ) VALUES (
            $1, 1, 'FAILED', TRUE, NOW() - interval '2 minutes', NOW() - interval '1 minute',
            'TASK_EXCEPTION', 'attempt one raised', $2, $3,
            'lifecycle-host', 4242, 'lifecycle-process'
        )",
    )
    .bind(task_id)
    .bind(failed_reason)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("record attempt one");
    sql::query("UPDATE horsies_tasks SET retry_count = 1 WHERE id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("advance retry_count");
}

/// The `terminal_at` that a copied history row gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminalStamp {
    /// `NOW()` of the copying transaction.
    Now,
    /// NULL: the row the baseline refuses.
    Missing,
}

/// Copy the live row into a history row with `status` and `kind`, and an
/// empty version-1 attempt snapshot. The live row stays. Returns the
/// database's answer, so a test can assert a refusal.
pub(super) async fn copy_live_row_to_history(
    connection: &mut TestConnection,
    task_id: Uuid,
    status: &str,
    kind: Option<&str>,
    stamp: TerminalStamp,
) -> Result<u64, sql::Error> {
    let snapshot = encode_attempt_snapshot(&[]).expect("encode the empty attempt snapshot");
    let stamp_sql = match stamp {
        TerminalStamp::Now => "NOW",
        TerminalStamp::Missing => "MISSING",
    };
    let inserted = sql::query(
        "INSERT INTO horsies_task_history (
            task_id, task_name, queue_name, priority,
            command_fingerprint_version, command_fingerprint,
            retention_class_key, is_workflow_task, workflow_id,
            rerun_of_task_id, rerun_root_task_id,
            sent_at, created_at, enqueued_at, claimed_at, started_at, good_until,
            terminal_at, status, terminalization_kind, retry_count, max_retries,
            last_claimed_worker_id, last_worker_hostname, last_worker_pid,
            last_worker_process_name, error_code, final_failed_reason,
            result_envelope_version, result_payload, result_digest,
            history_schema_version, attempt_archive_version, attempt_snapshot,
            attempt_snapshot_digest, rerun_input_disposition, rerun_input_version,
            rerun_input_digest, rerun_input_inline
        )
        SELECT id, task_name, queue_name, priority,
               command_fingerprint_version, command_fingerprint,
               retention_class_key, is_workflow_task, NULL,
               rerun_of_task_id, rerun_root_task_id,
               sent_at, created_at, enqueued_at, claimed_at, started_at, good_until,
               CASE WHEN $7 = 'NOW' THEN NOW() END, $2, $3, retry_count, max_retries,
               claimed_by_worker_id, worker_hostname, worker_pid,
               worker_process_name, NULL, NULL,
               1, NULL, NULL,
               1, $4, $5,
               $6, 'NEVER_ELIGIBLE', NULL,
               NULL, NULL
        FROM horsies_tasks WHERE id = $1",
    )
    .bind(task_id)
    .bind(status)
    .bind(kind)
    .bind(snapshot.version)
    .bind(snapshot.payload)
    .bind(snapshot.digest.to_vec())
    .bind(stamp_sql)
    .execute(connection)
    .await?;
    Ok(inserted.rows_affected())
}

/// Put the task where another operation would have left it (Python's
/// `_force_terminal`): a history row with `kind`, no live row and no live
/// attempt rows.
pub(super) async fn force_terminal(pool: &TestPool, task_id: Uuid, status: &str, kind: &str) {
    let mut transaction = pool.begin().await.expect("begin force terminal");
    let inserted = copy_live_row_to_history(
        &mut transaction,
        task_id,
        status,
        Some(kind),
        TerminalStamp::Now,
    )
    .await
    .expect("insert forced history row");
    assert_eq!(inserted, 1, "force_terminal needs a live row");
    sql::query("DELETE FROM horsies_task_attempts WHERE task_id = $1")
        .bind(task_id)
        .execute(&mut *transaction)
        .await
        .expect("delete live attempts");
    sql::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&mut *transaction)
        .await
        .expect("delete live row");
    transaction.commit().await.expect("commit force terminal");
}

/// The live row as JSON, or `None` when the row has left the live table.
pub(super) async fn live_row(pool: &TestPool, task_id: Uuid) -> Option<serde_json::Value> {
    sql::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read live row")
}

/// The terminal facts of a history row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HistoryImage {
    pub(super) status: String,
    pub(super) terminalization_kind: String,
    pub(super) error_code: Option<String>,
    pub(super) final_failed_reason: Option<String>,
    pub(super) workflow_id: Option<Uuid>,
    pub(super) claimed_at: Option<DateTime<Utc>>,
    pub(super) last_claimed_worker_id: Option<String>,
    pub(super) result_payload: Option<Vec<u8>>,
    pub(super) rerun_input_disposition: String,
}

impl sql::FromTestRow for HistoryImage {
    fn from_row(row: &Row) -> Result<Self, sql::Error> {
        Ok(Self {
            status: sql::named(row, "status")?,
            terminalization_kind: sql::named(row, "terminalization_kind")?,
            error_code: sql::named(row, "error_code")?,
            final_failed_reason: sql::named(row, "final_failed_reason")?,
            workflow_id: sql::named(row, "workflow_id")?,
            claimed_at: sql::named(row, "claimed_at")?,
            last_claimed_worker_id: sql::named(row, "last_claimed_worker_id")?,
            result_payload: sql::named(row, "result_payload")?,
            rerun_input_disposition: sql::named(row, "rerun_input_disposition")?,
        })
    }
}

/// The history row of a task, or `None` when the task has not moved.
pub(super) async fn history_row(pool: &TestPool, task_id: Uuid) -> Option<HistoryImage> {
    sql::query_as(
        "SELECT status::text AS status, terminalization_kind::text AS terminalization_kind,
                error_code, final_failed_reason, workflow_id, claimed_at,
                last_claimed_worker_id::text AS last_claimed_worker_id,
                result_payload, rerun_input_disposition::text AS rerun_input_disposition
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read history row")
}

/// One archived attempt: number, outcome and failed reason (Python's
/// `read_attempt_history`).
pub(super) type AttemptSummary = (i32, String, Option<String>);

/// The attempts that the history row archived, in attempt order.
pub(super) async fn archived_attempts(pool: &TestPool, task_id: Uuid) -> Vec<AttemptSummary> {
    archived_attempt_records(pool, task_id)
        .await
        .iter()
        .map(|record| {
            (
                record.attempt(),
                record.outcome().to_owned(),
                record.failed_reason().map(str::to_owned),
            )
        })
        .collect()
}

/// The attempts that the history row archived, with every field. History
/// stores no codec or content type: each attempt archive version (1, and 2
/// from migration 1001) implies the json-utf8 codec and the
/// application/json content type, as in the production read
/// (`core/history/reads/detail.rs`).
pub(super) async fn archived_attempt_records(pool: &TestPool, task_id: Uuid) -> Vec<AttemptRecord> {
    let (version, payload, digest): (i16, Vec<u8>, Vec<u8>) = sql::query_as(
        "SELECT attempt_archive_version, attempt_snapshot, attempt_snapshot_digest
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the archived attempt snapshot");
    decode_attempt_snapshot(
        version,
        JSON_UTF8_CODEC,
        JSON_CONTENT_TYPE,
        &payload,
        &digest,
    )
    .expect("decode the archived attempt snapshot")
}

/// The attempt rows still in the live attempt table.
pub(super) async fn live_attempts(pool: &TestPool, task_id: Uuid) -> Vec<AttemptSummary> {
    sql::query_as(
        "SELECT attempt, outcome::text, failed_reason FROM horsies_task_attempts
         WHERE task_id = $1 ORDER BY attempt",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await
    .expect("read live attempts")
}

/// One workflow row with every baseline column named. A terminal status
/// carries `completed_at`.
pub(super) async fn seed_workflow(pool: &TestPool, workflow_id: Uuid, status: &str) {
    sql::query(
        "INSERT INTO horsies_workflows (
            id, name, definition_key, status, on_error, output_task_index,
            success_policy, result, error, parent_workflow_id, parent_task_index,
            root_workflow_id, depth, sent_at, created_at, started_at, completed_at,
            updated_at
        ) VALUES (
            $1, 'lifecycle_workflow', 'lifecycle.workflow.v1', $2, 'fail', NULL,
            NULL, NULL, NULL, NULL, NULL,
            $1, 0, NOW(), NOW(), NOW(),
            CASE WHEN $2 IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN NOW() END,
            NOW()
        )",
    )
    .bind(workflow_id)
    .bind(status)
    .execute(pool)
    .await
    .expect("seed workflow row");
}

/// One task node of `workflow_id` with every baseline column named. The node
/// takes the next free `task_index`. Returns the node row id.
pub(super) async fn seed_node(
    pool: &TestPool,
    workflow_id: Uuid,
    task_id: Option<Uuid>,
    node_status: &str,
) -> Uuid {
    try_seed_node(pool, workflow_id, task_id, node_status)
        .await
        .expect("seed workflow node")
}

/// `seed_node`, with the database's answer, so a test can assert a refusal.
pub(super) async fn try_seed_node(
    pool: &TestPool,
    workflow_id: Uuid,
    task_id: Option<Uuid>,
    node_status: &str,
) -> Result<Uuid, sql::Error> {
    let node_row_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
            downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
            priority, dependencies, args_from, workflow_ctx_from, allow_failed_deps,
            join_type, min_success, task_options, max_retries, execution, good_until,
            retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
            sub_workflow_name, sub_definition_key, sub_workflow_summary, result, error,
            created_at, started_at, completed_at
        ) VALUES (
            'NONE', $1, $2,
            COALESCE((SELECT MAX(task_index) + 1 FROM horsies_workflow_tasks
                      WHERE workflow_id = $2), 0),
            'node_' || $1::text, 'lifecycle.test', '{}', 'default',
            100, '{}', NULL, NULL, FALSE,
            'all', NULL, NULL, 0, 'ISOLATED', NULL,
            'forever', $3, $4, FALSE, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NOW(), NULL, NULL
        )",
    )
    .bind(node_row_id)
    .bind(workflow_id)
    .bind(node_status)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(node_row_id)
}

/// A workflow with `workflow_status` and one task node linked to
/// `task_id`. Returns the workflow id.
pub(super) async fn link_workflow_task(
    pool: &TestPool,
    task_id: Uuid,
    node_status: &str,
    workflow_status: &str,
) -> Uuid {
    let workflow_id = Uuid::new_v4();
    seed_workflow(pool, workflow_id, workflow_status).await;
    seed_node(pool, workflow_id, Some(task_id), node_status).await;
    workflow_id
}

/// Whether a phase-2 record waits for the task.
pub(super) async fn phase2_pending(pool: &TestPool, task_id: Uuid) -> bool {
    sql::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read phase-2 presence")
}

/// The role of a heartbeat sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BeatRole {
    Runner,
    Claimer,
}

impl BeatRole {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Runner => "runner",
            Self::Claimer => "claimer",
        }
    }
}

/// Create the hourly heartbeat leaves of the previous, current and next hour.
/// A beat whose hour has no leaf fails, and a beat a little in the past can
/// fall in the previous hour.
pub(super) async fn ensure_recent_heartbeat_leaves(pool: &TestPool) {
    let mut transaction = pool.begin().await.expect("begin heartbeat leaves");
    let now: DateTime<Utc> = sql::query_scalar("SELECT NOW()")
        .fetch_one(&mut *transaction)
        .await
        .expect("database clock");
    let hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate the database clock to its hour");
    for offset in [-1_i64, 0, 1] {
        let outcome: String =
            sql::query_scalar("SELECT outcome FROM horsies_create_heartbeat_leaf($1)")
                .bind(hour + Duration::hours(offset))
                .fetch_one(&mut *transaction)
                .await
                .expect("create hourly heartbeat leaf");
        assert!(
            matches!(
                outcome.as_str(),
                "CREATED" | "ALREADY_CONFORMANT" | "INDEX_REPAIRED"
            ),
            "heartbeat leaf not usable: {outcome}"
        );
    }
    transaction.commit().await.expect("commit heartbeat leaves");
}

/// Insert one heartbeat sent `ago` before the connection's `NOW()`, in the
/// hourly leaf that the seed guard creates for it. In a transaction the
/// guard and the insert read one `NOW()`.
pub(super) async fn insert_beat(
    connection: &mut TestConnection,
    task_id: Uuid,
    role: BeatRole,
    ago: Duration,
) {
    let sent_at = covered_beat_instant(&mut *connection, SeedInstant::BeforeNow(ago)).await;
    sql::query(
        "INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid)
         VALUES ($1, $2, $3, $4, 'lifecycle-host', 4242)",
    )
    .bind(task_id)
    .bind(role.as_sql())
    .bind(sent_at.at())
    .bind(WORKER)
    .execute(connection)
    .await
    .expect("insert heartbeat");
}

/// Insert one heartbeat in its own transaction.
pub(super) async fn beat(pool: &TestPool, task_id: Uuid, role: BeatRole, ago: Duration) {
    let mut connection = pool.acquire().await.expect("acquire for heartbeat");
    insert_beat(&mut connection, task_id, role, ago).await;
}

/// The constraint that a refusal must name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Constraint {
    /// The refusal names no constraint (a NOT NULL refusal, a RAISE).
    Unnamed,
    /// The refusal names this constraint.
    Named(&'static str),
}

/// Assert that the database refused a statement with `sqlstate`.
pub(super) fn assert_refused<T: std::fmt::Debug>(
    result: Result<T, sql::Error>,
    sqlstate: &str,
    constraint: Constraint,
) {
    match result {
        Err(sql::Error::Database(error)) => {
            assert_eq!(Some(error.code().code()), Some(sqlstate), "{error}");
            match constraint {
                Constraint::Unnamed => {}
                Constraint::Named(name) => {
                    assert_eq!(error.constraint(), Some(name), "{error}");
                }
            }
        }
        other => panic!("expected SQLSTATE {sqlstate}, got {other:?}"),
    }
}

/// The server error of a core statement that the database refused.
pub(super) fn server_refusal<T: std::fmt::Debug>(result: Result<T, BrokerError>) -> DbError {
    match result {
        Err(BrokerError::Statement(StatementFailure::StatementRefused { cause, .. })) => {
            cause.server_error().clone()
        }
        other => panic!("expected a statement that the database refused, got {other:?}"),
    }
}

/// Assert that a refusal has `sqlstate` and a message that contains
/// `fragment`.
pub(super) fn assert_server_refusal(error: &DbError, sqlstate: &str, fragment: &str) {
    assert_eq!(error.code().code(), sqlstate, "{error}");
    assert!(error.message().contains(fragment), "{error}");
}

/// The backend pid of the connection that runs a statement.
static BACKEND_PID: TypedStatement =
    TypedStatement::new("backend_pid", "SELECT pg_backend_pid() AS pid", &[]);

/// Open (or reuse) the one connection of a core pool and return its backend
/// pid. The connection is back in the pool when this returns.
pub(super) async fn core_pool_backend_pid(pool: &CorePool) -> i32 {
    let rows = pool
        .query(
            CheckoutClass::Reserved,
            &BACKEND_PID.bind().finish(),
            StatementBound::ConnectionTimeouts,
        )
        .await
        .expect("read the pooled backend pid");
    let [row] = rows.as_slice() else {
        panic!("expected one row, got {}", rows.len());
    };
    row.try_get("pid").expect("the pid column")
}

/// The backend pid of a connection.
pub(super) async fn backend_pid(connection: &mut TestConnection) -> i32 {
    sql::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(connection)
        .await
        .expect("read backend pid")
}

/// Whether `waiter_pid` waits on a lock that `holder_pid` holds, polled up to
/// `LOCK_WAIT_POLLS` times.
pub(super) async fn waits_on(pool: &TestPool, waiter_pid: i32, holder_pid: i32) -> bool {
    for _ in 0..LOCK_WAIT_POLLS {
        let blocked: bool = sql::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
            .bind(holder_pid)
            .bind(waiter_pid)
            .fetch_one(pool)
            .await
            .expect("read blocking pids");
        if blocked {
            return true;
        }
        tokio::time::sleep(LOCK_WAIT_POLL_INTERVAL).await;
    }
    false
}

/// Expire every PENDING row that is already due, so the rows a test seeds
/// next are the only due rows of the sweep.
pub(super) async fn drain_pending_expiry(pool: &TestPool) {
    for _ in 0..DRAIN_PASS_LIMIT {
        let drained = terminalize(
            pool,
            &TerminalizationCommand::ExpirePendingTasks {
                batch_size: BatchSize::new(DRAIN_BATCH_SIZE).expect("positive batch size"),
                result_json: r#"{"err": 4}"#.to_owned(),
                error_code: "TASK_EXPIRED".to_owned(),
            },
        )
        .await
        .expect("drain pending expiry");
        if drained.len() < DRAIN_BATCH_SIZE as usize {
            return;
        }
    }
    panic!("pending expiry did not drain in {DRAIN_PASS_LIMIT} passes");
}

/// The cycle count and the rows of the last pass of the orphan audit cursor.
async fn orphan_audit_progress(pool: &TestPool) -> (i64, i32) {
    sql::query_as(
        "SELECT completed_cycles, last_scan_rows
         FROM horsies_recovery_scan_cursors
         WHERE scan_name = 'orphan_workflow_tasks'",
    )
    .fetch_one(pool)
    .await
    .expect("read orphan audit progress")
}

/// Run orphan sweep passes until the audit cycle that is open at the call
/// ends, and return every outcome of those passes.
async fn sweep_orphans_to_cycle_end(pool: &TestPool) -> Vec<TerminalizationOutcome> {
    let (initial_cycles, _) = orphan_audit_progress(pool).await;
    let mut outcomes = Vec::new();
    for _ in 0..DRAIN_PASS_LIMIT {
        let pass = terminalize(
            pool,
            &TerminalizationCommand::CancelOrphanedTasks {
                batch_size: BatchSize::new(DRAIN_BATCH_SIZE).expect("positive batch size"),
            },
        )
        .await
        .expect("orphan sweep pass");
        outcomes.extend(pass);
        let (completed_cycles, scanned) = orphan_audit_progress(pool).await;
        if scanned == 0 || completed_cycles > initial_cycles {
            return outcomes;
        }
    }
    panic!("the orphan audit did not end its cycle in {DRAIN_PASS_LIMIT} passes");
}

/// End the open orphan audit cycle, so the rows a test seeds next are inside
/// the next cycle.
pub(super) async fn drain_orphan_sweep(pool: &TestPool) {
    let _drained: Vec<TerminalizationOutcome> = sweep_orphans_to_cycle_end(pool).await;
}

/// One full orphan audit cycle over the rows that exist now: the outcomes
/// of every pass of that cycle. Call `drain_orphan_sweep` before seeding.
pub(super) async fn sweep_orphans_one_cycle(pool: &TestPool) -> Vec<TerminalizationOutcome> {
    sweep_orphans_to_cycle_end(pool).await
}

/// Delete every row that a test wrote for these tasks.
pub(super) async fn cleanup_tasks(pool: &TestPool, task_ids: &[Uuid]) {
    for statement in [
        "DELETE FROM horsies_workflow_phase2_pending WHERE task_id = ANY($1)",
        "DELETE FROM horsies_task_attempts WHERE task_id = ANY($1)",
        "DELETE FROM horsies_heartbeats WHERE task_id = ANY($1)",
        "DELETE FROM horsies_tasks WHERE id = ANY($1)",
        "DELETE FROM horsies_task_history WHERE task_id = ANY($1)",
        "DELETE FROM horsies_task_history_clock_exceptions WHERE task_id = ANY($1)",
        "DELETE FROM horsies_key_reservations WHERE task_id = ANY($1)",
    ] {
        sql::query(statement)
            .bind(task_ids)
            .execute(pool)
            .await
            .expect("clean up task rows");
    }
}

/// Delete workflows; their nodes and phase-2 records go with them.
pub(super) async fn cleanup_workflows(pool: &TestPool, workflow_ids: &[Uuid]) {
    sql::query("DELETE FROM horsies_workflows WHERE id = ANY($1)")
        .bind(workflow_ids)
        .execute(pool)
        .await
        .expect("clean up workflow rows");
}

/// The status and the task link of the one node of a workflow.
pub(super) async fn node_state(pool: &TestPool, workflow_id: Uuid) -> (String, Option<Uuid>) {
    sql::query_as("SELECT status::text, task_id FROM horsies_workflow_tasks WHERE workflow_id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("read node state")
}

/// Bound of the wait for a terminated backend to exit: 50 polls of 100 ms.
const BACKEND_EXIT_POLLS: u32 = 50;
const BACKEND_EXIT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// The failed-attempt write of a task whose child reported a result
/// (claimed-row rule `REFUSE`), on a core pool on the test database.
pub(super) async fn write_failed_attempt(
    pool: &TestPool,
    command: &FinishFailedAttempt,
) -> Result<FailedAttemptOutcome, BrokerError> {
    finish_failed_attempt(
        &core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        command,
    )
    .await
}

/// The failed-attempt write of a task whose child ended without a report
/// (claimed-row rule `RELEASE`), on a core pool on the test database.
pub(super) async fn write_unreported_attempt(
    pool: &TestPool,
    command: &FinishFailedAttempt,
) -> Result<UnreportedAttemptOutcome, BrokerError> {
    finish_unreported_attempt(
        &core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        command,
    )
    .await
}

/// The success write of a workflow task, on a core pool on the test
/// database.
pub(super) async fn write_workflow_success(
    pool: &TestPool,
    command: &FinishWorkflowSuccess,
) -> Result<WorkflowSuccessOutcome, BrokerError> {
    finish_workflow_success(
        &core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        command,
    )
    .await
}

/// A pool of one session on `url`. A test pool never pings a session at
/// checkout, so a dead pooled session shows at its first statement
/// (Python's worker pre-ping off).
pub(crate) async fn single_session_pool(url: &str) -> TestPool {
    TestPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("open a single-session pool")
}

/// Terminate the server side of a connection and wait until it is gone. The
/// client side stays in its pool.
pub(super) async fn terminate_backend(observer: &TestPool, pid: i32) {
    let terminated: bool = sql::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(observer)
        .await
        .expect("terminate backend");
    assert!(terminated, "backend {pid} was not terminated");
    for _ in 0..BACKEND_EXIT_POLLS {
        let alive: i64 = sql::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid = $1")
            .bind(pid)
            .fetch_one(observer)
            .await
            .expect("read backend activity");
        if alive == 0 {
            return;
        }
        tokio::time::sleep(BACKEND_EXIT_POLL_INTERVAL).await;
    }
    panic!("backend {pid} still runs after pg_terminate_backend");
}

/// Connections that one proxy relays before it stops accepting.
const PROXY_CONNECTION_LIMIT: u32 = 16;
/// Reads that one relay direction makes before it ends its connection.
const PROXY_READ_LIMIT: u32 = 100_000;
const PROXY_BUFFER_BYTES: usize = 16 * 1024;
/// Time that the server gets to finish the statement whose reply is
/// dropped, before the client's connection ends.
const REPLY_DROP_SETTLE: std::time::Duration = std::time::Duration::from_millis(500);
/// The longest marker that the request filter matches
/// ([`WireProxy::drop_reply_to_request_carrying`]).
const PROXY_MARKER_MAX_BYTES: usize = 64;
/// Bytes of the open request that a relay keeps from its earlier reads, so a
/// marker that one read ends and the next read continues still matches.
const PROXY_REQUEST_TAIL_BYTES: usize = PROXY_MARKER_MAX_BYTES - 1;

/// The client request whose reply the proxy drops next.
#[derive(Debug)]
enum DropTarget {
    /// No reply is dropped.
    Disarmed,
    /// The reply to the next request of any connection.
    NextRequest,
    /// The reply to the next request, of any connection, whose bytes carry
    /// this marker.
    RequestCarrying(Vec<u8>),
}

/// A TCP proxy to the test database (Python's `RoundTripProxy`). It counts
/// round trips, and it can drop the reply to one request: the next request,
/// or the next request that carries a marker. The server receives and runs
/// the request; the client gets no reply and then a closed connection.
pub(crate) struct WireProxy {
    port: u16,
    target: Arc<Mutex<DropTarget>>,
    dropped: Arc<AtomicBool>,
    round_trips: Arc<AtomicU32>,
    accept_loop: tokio::task::JoinHandle<()>,
}

impl WireProxy {
    /// Listen on a free local port and relay to the server of the test
    /// database URL `upstream_url`.
    pub(crate) async fn start(upstream_url: &str) -> Self {
        let (upstream_host, upstream_port) = sql::endpoint_of(upstream_url);
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind the proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let target = Arc::new(Mutex::new(DropTarget::Disarmed));
        let dropped = Arc::new(AtomicBool::new(false));
        let round_trips = Arc::new(AtomicU32::new(0));
        let accept_loop = tokio::spawn(accept_connections(
            listener,
            upstream_host,
            upstream_port,
            RelayState {
                target: Arc::clone(&target),
                dropped: Arc::clone(&dropped),
                round_trips: Arc::clone(&round_trips),
            },
        ));
        Self {
            port,
            target,
            dropped,
            round_trips,
            accept_loop,
        }
    }

    /// The URL that reaches the database of `upstream_url` through this
    /// proxy.
    pub(crate) fn url(&self, upstream_url: &str) -> String {
        sql::with_endpoint(upstream_url, "127.0.0.1", self.port)
    }

    /// Drop the reply to the next request that a client sends.
    pub(crate) fn drop_next_reply(&self) {
        self.arm(DropTarget::NextRequest);
    }

    /// Drop the reply to the next request, of any connection, whose bytes
    /// carry `marker`, for example a bound parameter in its binary form. The
    /// requests before it are relayed.
    ///
    /// Panics when `marker` is empty or longer than
    /// [`PROXY_MARKER_MAX_BYTES`].
    pub(crate) fn drop_reply_to_request_carrying(&self, marker: &[u8]) {
        assert!(
            (1..=PROXY_MARKER_MAX_BYTES).contains(&marker.len()),
            "a request marker has 1 to {PROXY_MARKER_MAX_BYTES} bytes, not {}",
            marker.len()
        );
        self.arm(DropTarget::RequestCarrying(marker.to_vec()));
    }

    fn arm(&self, target: DropTarget) {
        *self.target.lock().expect("the proxy's drop target") = target;
    }

    /// Whether a reply was dropped.
    pub(crate) fn reply_was_dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }

    /// Start a new count of round trips.
    pub(crate) fn reset_round_trips(&self) {
        self.round_trips.store(0, Ordering::SeqCst);
    }

    /// Round trips since the last reset: requests of a client that the
    /// server answered or still has to answer.
    pub(crate) fn round_trips(&self) -> u32 {
        self.round_trips.load(Ordering::SeqCst)
    }
}

/// The state that every relay of one proxy shares.
#[derive(Debug, Clone)]
struct RelayState {
    target: Arc<Mutex<DropTarget>>,
    dropped: Arc<AtomicBool>,
    round_trips: Arc<AtomicU32>,
}

impl Drop for WireProxy {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

async fn accept_connections(
    listener: TcpListener,
    upstream_host: String,
    upstream_port: u16,
    state: RelayState,
) {
    for _ in 0..PROXY_CONNECTION_LIMIT {
        let Ok((client, _)) = listener.accept().await else {
            return;
        };
        let Ok(upstream) = TcpStream::connect((upstream_host.as_str(), upstream_port)).await else {
            return;
        };
        let _relay = tokio::spawn(relay(client, upstream, state.clone()));
    }
}

/// Relay one client connection. A client request that follows a server
/// reply starts a round trip. A read of a request that hits the proxy's
/// [`DropTarget`] marks the connection; the reply to it is not relayed, and
/// the client's connection ends after `REPLY_DROP_SETTLE`.
async fn relay(client: TcpStream, upstream: TcpStream, state: RelayState) {
    let (mut client_read, mut client_write) = client.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let cut = Arc::new(AtomicBool::new(false));
    let request_open = Arc::new(AtomicBool::new(false));
    let cut_by_request = Arc::clone(&cut);
    let opened_by_request = Arc::clone(&request_open);
    let RelayState {
        target,
        dropped,
        round_trips,
    } = state;
    let requests = tokio::spawn(async move {
        let mut buffer = vec![0_u8; PROXY_BUFFER_BYTES];
        let mut request_tail: Vec<u8> = Vec::with_capacity(2 * PROXY_REQUEST_TAIL_BYTES);
        for _ in 0..PROXY_READ_LIMIT {
            let read = match client_read.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(read) => read,
            };
            let bytes = &buffer[..read];
            if !opened_by_request.swap(true, Ordering::SeqCst) {
                round_trips.fetch_add(1, Ordering::SeqCst);
                request_tail.clear();
            }
            if hits_drop_target(&target, &request_tail, bytes) {
                cut_by_request.store(true, Ordering::SeqCst);
            }
            keep_request_tail(&mut request_tail, bytes);
            if upstream_write.write_all(bytes).await.is_err() {
                return;
            }
        }
    });
    let mut buffer = vec![0_u8; PROXY_BUFFER_BYTES];
    for _ in 0..PROXY_READ_LIMIT {
        let read = match upstream_read.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        request_open.store(false, Ordering::SeqCst);
        if cut.load(Ordering::SeqCst) {
            dropped.store(true, Ordering::SeqCst);
            tokio::time::sleep(REPLY_DROP_SETTLE).await;
            break;
        }
        if client_write.write_all(&buffer[..read]).await.is_err() {
            break;
        }
    }
    requests.abort();
}

/// Whether one read of a client request hits `target`. `request_tail` holds
/// the last bytes of the earlier reads of the same request. A hit disarms
/// the target, so it hits once.
fn hits_drop_target(target: &Mutex<DropTarget>, request_tail: &[u8], read: &[u8]) -> bool {
    let mut current = target.lock().expect("the proxy's drop target");
    let hit = match &*current {
        DropTarget::Disarmed => false,
        DropTarget::NextRequest => true,
        DropTarget::RequestCarrying(marker) => [request_tail, read]
            .concat()
            .windows(marker.len())
            .any(|window| window == marker.as_slice()),
    };
    if hit {
        *current = DropTarget::Disarmed;
    }
    hit
}

/// Keeps the last [`PROXY_REQUEST_TAIL_BYTES`] bytes of the open request in
/// `request_tail`, after `read`.
fn keep_request_tail(request_tail: &mut Vec<u8>, read: &[u8]) {
    let read_tail = &read[read.len().saturating_sub(PROXY_REQUEST_TAIL_BYTES)..];
    request_tail.extend_from_slice(read_tail);
    let start = request_tail.len().saturating_sub(PROXY_REQUEST_TAIL_BYTES);
    *request_tail = request_tail.split_off(start);
}

/// The status of a task: of its live row, else of its history row.
pub(super) async fn status_of(pool: &TestPool, task_id: Uuid) -> String {
    sql::query_scalar(
        "SELECT status::text FROM horsies_tasks WHERE id = $1
         UNION ALL
         SELECT status::text FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read task status")
}

/// A workflow task that `WORKER` claimed at `claimed_at`.
pub(super) fn workflow_task_claimed_at(claimed_at: DateTime<Utc>) -> TaskSeed {
    TaskSeed {
        origin: Origin::WorkflowNode,
        holder: Holder::Claim {
            worker_id: WORKER.to_owned(),
            claimed_at,
        },
        ..TaskSeed::claimed()
    }
}

/// The request of one 0004 claim pass of `WORKER` over `queue`, with no
/// cap: one `ISOLATED` budget of 10 that every class routes to.
pub(super) fn claim_request(queue: &str) -> crate::broker::claims::ClaimPassRequest {
    use crate::core::task::Execution;
    crate::broker::claims::ClaimPassRequest {
        worker_id: WORKER.to_owned(),
        queues: vec![queue.to_owned()],
        queue_priority: std::iter::once((queue.to_owned(), 100)).collect(),
        queue_max_concurrency: std::collections::HashMap::new(),
        hard_cap_mode: true,
        classes: crate::broker::claims::ClaimClasses::new(
            std::collections::HashMap::from([(Execution::Isolated, 10)]),
            std::collections::HashMap::from([
                (Execution::Isolated, Execution::Isolated),
                (Execution::SharedAsync, Execution::Isolated),
                (Execution::SharedThread, Execution::Isolated),
            ]),
        )
        .expect("every route names the ISOLATED budget"),
        prefetch_buffer: 0,
        max_claim_per_worker: 0,
        max_claim_batch: 0,
        cluster_wide_cap: None,
        claim_lease_ms: std::num::NonZeroU32::new(60_000).expect("positive lease"),
        lock_keys: Vec::new(),
    }
}

/// Bound of the wait for the sentinel wake.
const WAKE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
/// Bound of the wakes read before the sentinel.
const WAKE_LIMIT: u32 = 100;

/// A listener on one wake channel (Python's `wakes` fixture).
pub(super) struct Wakes {
    channel: String,
    listener: TestListener,
}

impl Wakes {
    /// Listen on `channel`.
    pub(super) async fn listen(pool: &TestPool, channel: &str) -> Self {
        let mut listener = TestListener::connect_with(pool)
            .await
            .expect("connect the wake listener");
        listener.listen(channel).await.expect("listen");
        Self {
            channel: channel.to_owned(),
            listener,
        }
    }

    /// Every payload on the channel committed before a sentinel wake that
    /// this call commits. Postgres delivers notifications in commit order.
    pub(super) async fn payloads(&mut self, pool: &TestPool) -> Vec<String> {
        let sentinel = format!("sentinel:{}", Uuid::new_v4());
        sql::query("SELECT pg_notify($1, $2)")
            .bind(&self.channel)
            .bind(&sentinel)
            .execute(pool)
            .await
            .expect("send the sentinel wake");
        let mut payloads = Vec::new();
        for _ in 0..WAKE_LIMIT {
            let notification = tokio::time::timeout(WAKE_WAIT, self.listener.recv())
                .await
                .expect("a wake within the wait")
                .expect("receive a wake");
            if notification.payload() == sentinel {
                return payloads;
            }
            payloads.push(notification.payload().to_owned());
        }
        panic!("sentinel wake not seen; payloads {payloads:?}");
    }
}

/// The rerun-input columns of a history row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RerunInputImage {
    pub(super) rerun_input_disposition: String,
    pub(super) rerun_input_version: Option<i16>,
    pub(super) rerun_input_digest: Option<Vec<u8>>,
    pub(super) rerun_input_inline: Option<Vec<u8>>,
}

impl sql::FromTestRow for RerunInputImage {
    fn from_row(row: &Row) -> Result<Self, sql::Error> {
        Ok(Self {
            rerun_input_disposition: sql::named(row, "rerun_input_disposition")?,
            rerun_input_version: sql::named(row, "rerun_input_version")?,
            rerun_input_digest: sql::named(row, "rerun_input_digest")?,
            rerun_input_inline: sql::named(row, "rerun_input_inline")?,
        })
    }
}

/// The rerun-input columns of the history row of a task.
pub(super) async fn rerun_input_of(pool: &TestPool, task_id: Uuid) -> RerunInputImage {
    sql::query_as(
        "SELECT rerun_input_disposition::text AS rerun_input_disposition,
                rerun_input_version, rerun_input_digest, rerun_input_inline
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the rerun input columns")
}

/// The phase-2 record that a deferred terminalization wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Phase2Record {
    pub(super) workflow_id: Uuid,
    pub(super) workflow_node_row_id: Uuid,
    pub(super) recovery_source: String,
    pub(super) history_class: Option<String>,
    pub(super) history_anchor: Option<DateTime<Utc>>,
    pub(super) result_digest: Vec<u8>,
    pub(super) terminalization_kind: String,
}

impl sql::FromTestRow for Phase2Record {
    fn from_row(row: &Row) -> Result<Self, sql::Error> {
        Ok(Self {
            workflow_id: sql::named(row, "workflow_id")?,
            workflow_node_row_id: sql::named(row, "workflow_node_row_id")?,
            recovery_source: sql::named(row, "recovery_source")?,
            history_class: sql::named(row, "history_class")?,
            history_anchor: sql::named(row, "history_anchor")?,
            result_digest: sql::named(row, "result_digest")?,
            terminalization_kind: sql::named(row, "terminalization_kind")?,
        })
    }
}

/// The phase-2 record of a task.
pub(super) async fn phase2_record_of(pool: &TestPool, task_id: Uuid) -> Phase2Record {
    sql::query_as(
        "SELECT workflow_id, workflow_node_row_id, recovery_source::text AS recovery_source,
                history_class::text AS history_class, history_anchor, result_digest,
                terminalization_kind::text AS terminalization_kind
         FROM horsies_workflow_phase2_pending WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the phase-2 record")
}

/// The `terminal_at` of the history row of a task.
pub(super) async fn terminal_at_of(pool: &TestPool, task_id: Uuid) -> DateTime<Utc> {
    sql::query_scalar("SELECT terminal_at FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read terminal_at")
}

/// The SHA-256 digest of `bytes`.
pub(super) fn sha256_of(bytes: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    sha2::Sha256::digest(bytes).to_vec()
}

/// Call the internal move of a live task directly, as the deferred guard
/// of every deferring family sees it. The move takes the live row that its
/// caller locked (`horsies_move_task_to_history(horsies_tasks, ...)`), read
/// here FOR UPDATE as `sql_tests/cases/baseline_function_calls.sql` reads it.
pub(super) async fn move_to_history(
    pool: &TestPool,
    task_id: Uuid,
    status: &str,
    kind: &str,
    result: Option<&str>,
) -> Result<u64, sql::Error> {
    sql::query(
        "SELECT horsies_move_task_to_history(
             (SELECT t FROM horsies_tasks AS t WHERE t.id = $1 FOR UPDATE),
             $2, $3, NOW(), $4, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(status)
    .bind(kind)
    .bind(result)
    .execute(pool)
    .await
    .map(|done| done.rows_affected())
}

/// Assert that the database refused a statement with `sqlstate` and a
/// message that contains `fragment`.
pub(super) fn assert_refused_with_message<T: std::fmt::Debug>(
    result: Result<T, sql::Error>,
    sqlstate: &str,
    fragment: &str,
) {
    match result {
        Err(sql::Error::Database(error)) => {
            assert_eq!(Some(error.code().code()), Some(sqlstate), "{error}");
            assert!(error.message().contains(fragment), "{error}");
        }
        other => panic!("expected SQLSTATE {sqlstate}, got {other:?}"),
    }
}
