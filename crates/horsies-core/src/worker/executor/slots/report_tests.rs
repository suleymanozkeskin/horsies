//! The finalize decision of a child report: what the worker writes for each
//! end that a child reported (Python `python/horsies/core/lifecycle/classify.py`,
//! `classify` and `terminalization_for_refused_retry`, and
//! `python/horsies/core/worker/finalize.py`, `_attempt_failure_of`).
//!
//! Python decides on typed reports first and writes after. The core builds
//! the write of each failure with a pure function of the report and the
//! claim (`outcome::worker_failure_write`, `task_error_write`,
//! `killed_child_write`, `timeout_write`); those are tested here without a
//! database. The ends whose decision is a statement choice (fused or locked
//! success, the replay of phase 2, the serialization
//! failure) are tested through [`OutcomeWriter::settle`] on a migrated
//! database, by the rows they leave.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::outcome::{
    killed_child_write, task_error_write, test_successes, timeout_write, worker_failure_write,
};
use super::scripted::{ok_envelope, task};
use super::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, RetryPlacement, RetryRequest,
};
use crate::broker::terminalization_matrix::migrated_pool;
use crate::broker::{PostgresBroker, PrestartExpiry};
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::archive::attempts::{decode_attempt_snapshot, AttemptRecord};
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::registry::BuilderHookName;
use crate::core::task::error::{OperationalErrorCode, OutcomeCode, TaskError};
use crate::core::task::result::{read_task_result_text, TaskResult, TaskResultText};
use crate::worker::executor::protocol::{ResultEnvelopeText, RunEnd, StopReason, TaskTimeoutMs};
use crate::worker::retry::read_stored_options;

const WORKER: &str = "report-worker";
const TASK_NAME: &str = "app.tasks.report";

/// The bound of the wait for one notification.
const WAKE_BOUND: Duration = Duration::from_secs(10);
/// The most notifications that one read takes before its sentinel.
const WAKE_READ_LIMIT: usize = 32;

// ---------------------------------------------------------------------------
// Pure: the write of each failure
// ---------------------------------------------------------------------------

/// A claimed plain task whose stored options name no retry policy, so every
/// failed write asks for no retry.
fn claimed_plain() -> ClaimedTask {
    task(0xc1, ExecutionClass::Isolated)
}

/// The result envelope of a task that returned `error`.
fn error_envelope(error: &TaskError) -> ResultEnvelopeText {
    ResultEnvelopeText::new(
        serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error.clone()))
            .expect("a task error holds only JSON values, so its envelope encodes"),
    )
}

/// Python `test_lifecycle_classification.py::TestFailures::test_worker_failure_manufactures_its_own_payload`:
/// the task produced nothing, so the worker writes its own account as the
/// result: a `BROKER_ERROR` error whose message is the detail and whose
/// data names the task. The detail is the reason of the task and of its
/// attempt, and the write never retries.
#[test]
fn a_worker_failure_records_its_detail_as_the_result_and_the_reason() {
    let claimed = claimed_plain();

    let write = worker_failure_write(&claimed, WORKER, "pool died");

    assert_eq!(write.error_code.as_deref(), Some("BROKER_ERROR"));
    assert_eq!(write.failed_reason.as_deref(), Some("pool died"));
    assert_eq!(write.retry, RetryRequest::NoRetry);
    assert_eq!(
        write.attempt,
        AttemptFailure {
            kind: AttemptFailureKind::WorkerFailure,
            error_message: Some("pool died".to_owned()),
            failed_reason: Some("pool died".to_owned()),
        }
    );
    match read_task_result_text(&write.result_json) {
        Ok(TaskResultText::Err(error)) => {
            assert_eq!(error.error_code.to_string(), "BROKER_ERROR");
            assert_eq!(error.message.as_deref(), Some("pool died"));
            assert_eq!(
                error.data,
                Some(serde_json::json!({ "task_id": claimed.task_id.to_string() }))
            );
        }
        other => panic!("the result is the worker's error envelope, got {other:?}"),
    }
}

/// Python `TestFailures::test_worker_failure_without_detail_still_records_a_reason`
/// and `test_failed_attempt.py::TestAttemptOfAReport::test_a_worker_failure`:
/// a worker failure with no detail records `Worker failure` as the reason of
/// the task, and as the message and the reason of its attempt.
#[test]
fn a_worker_failure_without_detail_records_the_fallback_text() {
    let write = worker_failure_write(&claimed_plain(), WORKER, "");

    assert_eq!(write.failed_reason.as_deref(), Some("Worker failure"));
    assert_eq!(
        write.attempt,
        AttemptFailure {
            kind: AttemptFailureKind::WorkerFailure,
            error_message: Some("Worker failure".to_owned()),
            failed_reason: Some("Worker failure".to_owned()),
        }
    );
}

/// Python `TestFailures::test_string_error_codes_survive_unchanged` and
/// `test_failed_attempt.py::TestAttemptOfAReport::test_a_task_error`: a task
/// error with a code of its own keeps that code, the task's own envelope is
/// the stored result, and the attempt carries the task's message and no
/// reason.
#[test]
fn a_task_error_keeps_its_own_code_payload_and_message() {
    let error = TaskError::new("MY_OWN_CODE", "boom").expect("a code of the task's own");
    let envelope = error_envelope(&error);

    let write = task_error_write(&claimed_plain(), WORKER, &error, &envelope);

    assert_eq!(write.error_code.as_deref(), Some("MY_OWN_CODE"));
    assert_eq!(write.result_json, envelope.as_str());
    assert_eq!(write.failed_reason, None);
    assert_eq!(
        write.attempt,
        AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some("boom".to_owned()),
            failed_reason: None,
        }
    );
}

/// Python `test_failed_attempt.py::TestErrorCodeText::test_the_stored_spelling`:
/// the stored code of an outcome code, of an operational code and of a code
/// of the task's own is its spelling.
#[test]
fn each_failure_stores_the_spelling_of_its_code() {
    let claimed = claimed_plain();
    let own = TaskError::new("MY_CODE", "m").expect("a code of the task's own");

    let timed_out = killed_child_write(
        &claimed,
        WORKER,
        OutcomeCode::TaskTimeout.into(),
        "m".to_owned(),
        RetryPlacement::SameClass,
    );
    let crashed = killed_child_write(
        &claimed,
        WORKER,
        OperationalErrorCode::WorkerCrashed.into(),
        "m".to_owned(),
        RetryPlacement::SameClass,
    );
    let returned = task_error_write(&claimed, WORKER, &own, &error_envelope(&own));

    assert_eq!(timed_out.error_code.as_deref(), Some("TASK_TIMEOUT"));
    assert_eq!(crashed.error_code.as_deref(), Some("WORKER_CRASHED"));
    assert_eq!(returned.error_code.as_deref(), Some("MY_CODE"));
}

/// Python `TestDecisionsAreDataOnly::test_classification_is_repeatable`: the
/// write of each failure is built from the report and the claim alone, with
/// no clock, counter or read, so the same input gives the same write. The
/// task has no retry policy, so no retry delay enters the write.
#[test]
fn the_write_of_a_report_is_the_same_for_the_same_report() {
    let claimed = claimed_plain();
    let error = TaskError::new("MY_OWN_CODE", "x").expect("a code of the task's own");
    let envelope = error_envelope(&error);
    let timeout = TaskTimeoutMs::new(1_000).expect("1 s is a valid task timeout");
    let crash = || {
        killed_child_write(
            &claimed,
            WORKER,
            OperationalErrorCode::WorkerCrashed.into(),
            "lost".to_owned(),
            RetryPlacement::SameClass,
        )
    };

    assert_eq!(
        worker_failure_write(&claimed, WORKER, "x"),
        worker_failure_write(&claimed, WORKER, "x")
    );
    assert_eq!(
        task_error_write(&claimed, WORKER, &error, &envelope),
        task_error_write(&claimed, WORKER, &error, &envelope)
    );
    assert_eq!(crash(), crash());
    assert_eq!(
        timeout_write(
            &claimed,
            WORKER,
            timeout,
            RetryPlacement::Isolated,
            TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace)
        ),
        timeout_write(
            &claimed,
            WORKER,
            timeout,
            RetryPlacement::Isolated,
            TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace)
        )
    );
}

// ---------------------------------------------------------------------------
// Database: the statement that each end takes
// ---------------------------------------------------------------------------

/// How the seeded row is held by [`WORKER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    Claimed,
    Running,
}

impl Held {
    fn as_sql(self) -> &'static str {
        match self {
            Held::Claimed => "CLAIMED",
            Held::Running => "RUNNING",
        }
    }
}

/// The `good_until` of a seeded row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Deadline {
    /// No deadline.
    NotSet,
    /// The deadline passed one second ago.
    Passed,
}

/// The shape of one seeded row.
#[derive(Debug, Clone, Copy)]
struct Seed {
    held: Held,
    kind: TaskKind,
    deadline: Deadline,
}

const RUNNING_PLAIN: Seed = Seed {
    held: Held::Running,
    kind: TaskKind::Plain,
    deadline: Deadline::NotSet,
};

const RUNNING_WORKFLOW_TASK: Seed = Seed {
    held: Held::Running,
    kind: TaskKind::WorkflowTask,
    deadline: Deadline::NotSet,
};

const CLAIMED_WORKFLOW_TASK_PAST_ITS_DEADLINE: Seed = Seed {
    held: Held::Claimed,
    kind: TaskKind::WorkflowTask,
    deadline: Deadline::Passed,
};

/// A row on `queue` held by [`WORKER`], with every baseline column named. A
/// workflow task has no command fingerprint (baseline
/// `horsies_tasks_command_fingerprint_by_origin`). Returns the id and the
/// claim generation.
async fn seed_task(pool: &PgPool, queue: &str, seed: Seed) -> (Uuid, DateTime<Utc>) {
    let task_id = Uuid::new_v4();
    let generation = sqlx::query_scalar(
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
            $1, $2, $3, 100, '{}', NULL, 'ISOLATED',
            'forever', NOW() - interval '31 minutes', NOW() - interval '31 minutes',
            CASE WHEN $5 THEN NULL ELSE 1 END,
            CASE WHEN $5 THEN NULL ELSE decode(repeat('0c', 32), 'hex') END, NULL,
            $4, NOW() - interval '31 minutes',
            date_trunc('milliseconds', NOW()) - interval '30 minutes', $6,
            CASE WHEN $4 = 'CLAIMED' THEN NOW() + interval '1 minute' END,
            CASE WHEN $4 = 'RUNNING' THEN NOW() - interval '30 minutes' END, NULL, NULL,
            CASE WHEN $4 = 'RUNNING' THEN 4242 END,
            CASE WHEN $4 = 'RUNNING' THEN 'report-host' END,
            CASE WHEN $4 = 'RUNNING' THEN 'report-process' END,
            0, 0, NULL, CASE WHEN $7 THEN NOW() - interval '1 second' END, $5,
            NOW(), FALSE, NULL, NULL
         ) RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(TASK_NAME)
    .bind(queue)
    .bind(seed.held.as_sql())
    .bind(seed.kind.is_workflow_task())
    .bind(WORKER)
    .bind(seed.deadline == Deadline::Passed)
    .fetch_one(pool)
    .await
    .expect("seed a task row");
    (task_id, generation)
}

/// A RUNNING workflow with one task node in `node_status`, linked to
/// `task_id`. Returns the workflow id.
async fn seed_workflow(pool: &PgPool, task_id: Uuid, queue: &str, node_status: &str) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'report_workflow', NULL, 'RUNNING', 'fail', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("seed the workflow");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, max_retries,
             execution, retention_class_key, status, task_id, is_subworkflow, created_at,
             started_at
         ) VALUES ('NONE', $1, $2, 0, 'only', $3, '{}', $4, 100,
                   '{}', FALSE, 'all', 0, 'ISOLATED',
                   'forever', $5, $6, FALSE, NOW(),
                   CASE WHEN $5 = 'RUNNING' THEN NOW() END)",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(TASK_NAME)
    .bind(queue)
    .bind(node_status)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed the workflow node");
    workflow_id
}

/// The claimed task of a seeded row, as the claim returns it.
fn claimed(task_id: Uuid, queue: &str, generation: DateTime<Utc>, kind: TaskKind) -> ClaimedTask {
    ClaimedTask {
        task_id,
        task_name: TASK_NAME.to_owned(),
        queue_name: queue.to_owned(),
        claimed_at: generation,
        execution: ExecutionClass::Isolated,
        kind,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(None),
    }
}

/// The outcome writer of [`WORKER`] on `pool`.
fn writer(pool: &PgPool) -> OutcomeWriter {
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let successes = test_successes(&broker, WORKER);
    OutcomeWriter::new(
        broker,
        crate::workflow_engine::child_spec::ChildSpecs::Registry(Arc::new(
            WorkflowSpecRegistry::new(BuilderHookName::SPEC_BUILDER),
        )),
        WORKER.to_owned(),
        PayloadPolicy::default(),
        RetentionConfig::default(),
        successes,
    )
}

fn stopped(reason: StopReason) -> AttemptEnd {
    AttemptEnd::Reported(RunEnd::StoppedBeforeRunning(reason))
}

/// A queue of this test: `task_queue_<name>` stays within the 63-byte
/// channel limit.
fn test_queue() -> String {
    format!("report_{}", &Uuid::new_v4().simple().to_string()[..8])
}

/// The terminal facts of a history row: status, terminalization kind, error
/// code and final failed reason.
type HistoryFacts = (String, String, Option<String>, Option<String>);

async fn history_facts(pool: &PgPool, task_id: Uuid) -> Option<HistoryFacts> {
    sqlx::query_as(
        "SELECT status::text, terminalization_kind::text, error_code, final_failed_reason
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read the history row")
}

/// The attempts that the history row of `task_id` archived.
async fn archived_attempts(pool: &PgPool, task_id: Uuid) -> Vec<AttemptRecord> {
    let (version, payload, digest): (i16, Vec<u8>, Vec<u8>) = sqlx::query_as(
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

/// The status of the one node of `workflow_id`, and whether a phase-2
/// record waits for `task_id`.
async fn node_and_phase_two_record(
    pool: &PgPool,
    workflow_id: Uuid,
    task_id: Uuid,
) -> (String, bool) {
    sqlx::query_as(
        "SELECT
             (SELECT status::text FROM horsies_workflow_tasks WHERE workflow_id = $1),
             EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $2)",
    )
    .bind(workflow_id)
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the node and the phase-2 record")
}

async fn listen_on_queue(pool: &PgPool, queue: &str) -> sqlx::postgres::PgListener {
    let mut listener = sqlx::postgres::PgListener::connect_with(pool)
        .await
        .expect("open a listener");
    listener
        .listen(&format!("task_queue_{queue}"))
        .await
        .expect("listen on the queue channel");
    listener
}

/// Every payload on the channel of `queue` committed before a sentinel that
/// this call commits. PostgreSQL delivers notifications in commit order.
async fn wakes_before_sentinel(
    listener: &mut sqlx::postgres::PgListener,
    pool: &PgPool,
    queue: &str,
) -> Vec<String> {
    let sentinel = format!("sentinel:{}", Uuid::new_v4());
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(format!("task_queue_{queue}"))
        .bind(&sentinel)
        .execute(pool)
        .await
        .expect("send the sentinel");
    let mut payloads = Vec::new();
    for _ in 0..WAKE_READ_LIMIT {
        let notification = tokio::time::timeout(WAKE_BOUND, listener.recv())
            .await
            .expect("a notification within the bound")
            .expect("receive a notification");
        if notification.payload() == sentinel {
            return payloads;
        }
        payloads.push(notification.payload().to_owned());
    }
    panic!("the sentinel did not come; payloads {payloads:?}");
}

async fn remove_task(pool: &PgPool, task_id: Uuid) {
    for statement in [
        "DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        "DELETE FROM horsies_task_attempts WHERE task_id = $1",
        "DELETE FROM horsies_heartbeats WHERE task_id = $1",
        "DELETE FROM horsies_tasks WHERE id = $1",
        "DELETE FROM horsies_task_history WHERE task_id = $1",
    ] {
        sqlx::query(statement)
            .bind(task_id)
            .execute(pool)
            .await
            .expect("remove the test task");
    }
}

async fn remove_workflow(pool: &PgPool, workflow_id: Uuid) {
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("remove the test workflow");
}

/// Python `TestFailures::test_undecodable_payload_fails_as_a_serialization_error`,
/// `::test_undecodable_payload_leaves_the_reason_column_alone` and
/// `test_failed_attempt.py::TestAttemptOfAReport::test_an_undecodable_result`:
/// a result that does not read as an envelope fails the task with
/// `WORKER_SERIALIZATION_ERROR` and no failed reason; its attempt is FAILED,
/// with the decode diagnosis as its message and no reason.
#[tokio::test]
#[serial]
async fn an_undecodable_result_fails_the_task_as_a_serialization_error_with_no_reason() {
    let pool = migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed_task(&pool, &queue, RUNNING_PLAIN).await;
    let envelope = ResultEnvelopeText::new("{not an envelope".to_owned());

    let settled = writer(&pool)
        .settle(
            &claimed(task_id, &queue, generation, TaskKind::Plain),
            AttemptEnd::Reported(RunEnd::Result(envelope)),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let (status, _kind, error_code, final_failed_reason) = history_facts(&pool, task_id)
        .await
        .expect("the task moved to history");
    assert_eq!(status, "FAILED");
    assert_eq!(error_code.as_deref(), Some("WORKER_SERIALIZATION_ERROR"));
    assert_eq!(final_failed_reason, None);
    let attempts = archived_attempts(&pool, task_id).await;
    let [attempt] = attempts.as_slice() else {
        panic!("one attempt, got {}", attempts.len());
    };
    assert_eq!(attempt.outcome(), "FAILED");
    let message = attempt.error_message().unwrap_or_default();
    assert!(
        message.starts_with("failed to parse task result JSON: "),
        "{message:?}"
    );
    assert_eq!(attempt.failed_reason(), None);
    remove_task(&pool, task_id).await;
}

/// Python `TestExpiryRoutesByTaskKind::test_workflow_node_still_needs_phase_two`:
/// the child expired a claimed workflow task before it started, so the
/// terminal row exists; the worker writes no task row and replays phase 2,
/// which fails the node and consumes its phase-2 record. Otherwise the node
/// waits ENQUEUED against a terminal task until recovery.
#[tokio::test]
#[serial]
async fn an_expired_workflow_task_replays_its_phase_two() {
    let pool = migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) =
        seed_task(&pool, &queue, CLAIMED_WORKFLOW_TASK_PAST_ITS_DEADLINE).await;
    let workflow_id = seed_workflow(&pool, task_id, &queue, "ENQUEUED").await;
    let expired = PostgresBroker::from_pool(pool.clone())
        .expire_claimed_task_before_start(
            task_id,
            WORKER,
            crate::broker::child_writes::WriteDeadline::start_attempt(),
        )
        .await
        .expect("the expiry that the child writes");
    assert_eq!(expired, PrestartExpiry::Expired);
    assert_eq!(
        node_and_phase_two_record(&pool, workflow_id, task_id).await,
        ("ENQUEUED".to_owned(), true)
    );

    let settled = writer(&pool)
        .settle(
            &claimed(task_id, &queue, generation, TaskKind::WorkflowTask),
            stopped(StopReason::TaskExpired),
        )
        .await;

    assert_eq!(settled, Settlement::NothingOwed);
    assert_eq!(
        node_and_phase_two_record(&pool, workflow_id, task_id).await,
        ("FAILED".to_owned(), false)
    );
    remove_task(&pool, task_id).await;
    remove_workflow(&pool, workflow_id).await;
}

/// Python `TestSuccess::test_plain_task_completion_fuses_and_wakes_capacity`:
/// the success of a plain task owes nothing after its write, so it ends in
/// the fused write (`COMPLETE_FUSED`), which sends one capacity wake on the
/// task's queue in the same transaction.
#[tokio::test]
#[serial]
async fn a_plain_success_completes_in_the_fused_write_and_wakes_its_queue() {
    let pool = migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed_task(&pool, &queue, RUNNING_PLAIN).await;
    let mut listener = listen_on_queue(&pool, &queue).await;

    let settled = writer(&pool)
        .settle(
            &claimed(task_id, &queue, generation, TaskKind::Plain),
            AttemptEnd::Reported(RunEnd::Result(ok_envelope(42))),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let (status, kind, error_code, final_failed_reason) = history_facts(&pool, task_id)
        .await
        .expect("the task moved to history");
    assert_eq!(status, "COMPLETED");
    assert_eq!(kind, "COMPLETE_FUSED");
    assert_eq!(error_code, None);
    assert_eq!(final_failed_reason, None);
    assert_eq!(
        wakes_before_sentinel(&mut listener, &pool, &queue).await,
        vec!["capacity".to_owned()]
    );
    remove_task(&pool, task_id).await;
}

/// Python `TestSuccess::test_workflow_node_completion_cannot_fuse`: phase 2
/// still owes work after the success of a workflow task, so the task ends
/// in the locked write (`COMPLETE_LOCKED`), which records the phase-2 work,
/// and phase 2 then completes the node and consumes the record.
#[tokio::test]
#[serial]
async fn a_workflow_success_completes_in_the_locked_write_and_finishes_its_node() {
    let pool = migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed_task(&pool, &queue, RUNNING_WORKFLOW_TASK).await;
    let workflow_id = seed_workflow(&pool, task_id, &queue, "RUNNING").await;

    let settled = writer(&pool)
        .settle(
            &claimed(task_id, &queue, generation, TaskKind::WorkflowTask),
            AttemptEnd::Reported(RunEnd::Result(ok_envelope(7))),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let (status, kind, _error_code, _final_failed_reason) = history_facts(&pool, task_id)
        .await
        .expect("the task moved to history");
    assert_eq!(status, "COMPLETED");
    assert_eq!(kind, "COMPLETE_LOCKED");
    assert_eq!(
        node_and_phase_two_record(&pool, workflow_id, task_id).await,
        ("COMPLETED".to_owned(), false)
    );
    remove_task(&pool, task_id).await;
    remove_workflow(&pool, workflow_id).await;
}
