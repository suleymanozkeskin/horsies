//! The finalize decision of a child report: what the worker writes for each
//! end that a child reported.
//!
//! The core builds
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
use uuid::Uuid;

use super::outcome::{
    killed_child_write, task_error_write, test_successes, timeout_write, worker_failure_write,
};
use super::scripted::{ok_envelope, task};
use super::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, RetryPlacement, RetryRequest,
};
use crate::broker::{PostgresBroker, PrestartExpiry};
use crate::core::config::payload::PayloadPolicy;
use crate::core::history::archive::attempts::{decode_attempt_snapshot, AttemptRecord};
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::lifecycle::OwnedClaim;
use crate::core::task::error::{OperationalErrorCode, OutcomeCode, TaskError};
use crate::core::task::result::{read_task_result_text, TaskResult, TaskResultText};
use crate::test_support::migrated_pool;
use crate::test_support::sql::{self, TestListener, TestPool};
use crate::worker::executor::protocol::{
    FailureDetail, ProcessId, ResultEnvelopeText, RunEnd, StopReason, TaskTimeoutMs,
};
use crate::worker::executor::supervisor::{
    ChildExit, ChildLost, LossCause, PartialFrame, ReapedExit,
};
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

/// The task produced nothing, so the worker writes its own account as the
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
                error.data_value(),
                Some(serde_json::json!({ "task_id": claimed.task_id.to_string() }))
            );
        }
        other => panic!("the result is the worker's error envelope, got {other:?}"),
    }
}

/// A worker failure with no detail records `Worker failure` as the reason of
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

/// A child that died by itself with exit code 9, its exit seen.
fn child_died_with_exit_code_9() -> ChildLost {
    ChildLost {
        cause: LossCause::ChildDied,
        exit: ChildExit::Reaped(ReapedExit::ExitedWith(9)),
        partial: PartialFrame::NoneAtHand,
        pid: ProcessId::new(4242).expect("4242 is a process id"),
    }
}

/// Python `_finalize_child_loss`: the crash reason of a lost child is
/// `Worker child lost: ` and the loss description, which names the cause,
/// the exit and a discarded partial result.
#[test]
fn a_lost_child_s_crash_reason_carries_python_s_prefix() {
    let killed_mid_frame = ChildLost {
        cause: LossCause::KilledByParent,
        exit: ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL)),
        partial: PartialFrame::Discarded,
        ..child_died_with_exit_code_9()
    };

    assert_eq!(
        AttemptEnd::of_lost_child(&child_died_with_exit_code_9()),
        AttemptEnd::Crashed {
            reason: FailureDetail::cut("Worker child lost: child died: exit code 9"),
        }
    );
    assert_eq!(
        AttemptEnd::of_lost_child(&killed_mid_frame),
        AttemptEnd::Crashed {
            reason: FailureDetail::cut(
                "Worker child lost: killed by the worker: signal 9 (SIGKILL), \
                 partial result discarded"
            ),
        }
    );
}

/// Python `_write_ended_attempt`: the write of a child that the worker
/// killed or lost records its message as the attempt's message and as the
/// attempt's failed reason, for each code it writes. The task's failed
/// reason is not written.
#[test]
fn a_killed_or_lost_child_records_its_message_as_the_attempt_reason() {
    let claimed = claimed_plain();
    let text = "Worker child lost: child died: exit code 9";
    for code in [
        OperationalErrorCode::WorkerCrashed,
        LoopKillCode::TaskBlockedLoop.code(),
        LoopKillCode::NeighbourBlockedLoop.code(),
    ] {
        let code_text = code.to_string();
        let write = killed_child_write(
            &claimed,
            WORKER,
            code.into(),
            text.to_owned(),
            RetryPlacement::SameClass,
        );

        assert_eq!(write.failed_reason, None, "{code_text}");
        assert_eq!(
            write.attempt,
            AttemptFailure {
                kind: AttemptFailureKind::Failed,
                error_message: Some(text.to_owned()),
                failed_reason: Some(text.to_owned()),
            },
            "{code_text}"
        );
    }
}

/// Python `_settle_timed_out_row` through `_write_ended_attempt`: a
/// `TASK_TIMEOUT` write records `Task exceeded timeout_ms=<n>` as the
/// attempt's message and as the attempt's failed reason, for every cancel
/// end. The task's failed reason is not written.
#[test]
fn a_timeout_records_its_text_as_the_attempt_reason() {
    let timeout = TaskTimeoutMs::new(1_000).expect("1 s is a valid task timeout");
    for cancel in [
        TimeoutCancel::NoSignal,
        TimeoutCancel::Ended(CancelEnd::ReturnedAfterSignal),
        TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace),
        TimeoutCancel::Ended(CancelEnd::KilledWithChild),
    ] {
        let write = timeout_write(
            &claimed_plain(),
            WORKER,
            timeout,
            RetryPlacement::SameClass,
            cancel,
        );

        assert_eq!(write.failed_reason, None, "{cancel:?}");
        assert_eq!(
            write.attempt,
            AttemptFailure {
                kind: AttemptFailureKind::Failed,
                error_message: Some("Task exceeded timeout_ms=1000".to_owned()),
                failed_reason: Some("Task exceeded timeout_ms=1000".to_owned()),
            },
            "{cancel:?}"
        );
    }
}

/// A task error with a code of its own keeps that code, the task's own envelope is
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

/// The stored code of an outcome code, of an operational code and of a code
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

/// The failure writes: the write of a worker failure and of a task error
/// carries the worker and the claim generation of this dispatch, to the
/// microsecond. Its statement refuses a row that a later claim holds
/// (`a_reported_end_on_a_later_claim_of_the_row_writes_nothing`).
#[test]
fn each_failure_write_carries_the_claim_of_its_dispatch() {
    let first = claimed_plain();
    let claimed = ClaimedTask {
        claimed_at: first.claimed_at + chrono::Duration::microseconds(1),
        ..first
    };
    let dispatched = OwnedClaim {
        worker_id: WORKER.to_owned(),
        claimed_at: claimed.claimed_at,
    };
    let error = TaskError::new("MY_OWN_CODE", "x").expect("a code of the task's own");

    assert_eq!(
        worker_failure_write(&claimed, WORKER, "pool died").fence,
        dispatched
    );
    assert_eq!(
        task_error_write(&claimed, WORKER, &error, &error_envelope(&error)).fence,
        dispatched
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

const CLAIMED_WORKFLOW_TASK: Seed = Seed {
    held: Held::Claimed,
    kind: TaskKind::WorkflowTask,
    deadline: Deadline::NotSet,
};

/// A row on `queue` held by [`WORKER`], with every baseline column named. A
/// workflow task has no command fingerprint (baseline
/// `horsies_tasks_command_fingerprint_by_origin`). Returns the id and the
/// claim generation.
async fn seed_task(pool: &TestPool, queue: &str, seed: Seed) -> (Uuid, DateTime<Utc>) {
    let task_id = Uuid::new_v4();
    let generation = sql::query_scalar(
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
async fn seed_workflow(pool: &TestPool, task_id: Uuid, queue: &str, node_status: &str) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sql::query(
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
    sql::query(
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
        trace: crate::events::RunTrace::NoCarrier,
    }
}

/// The outcome writer of [`WORKER`] on `pool`.
fn writer(pool: &TestPool) -> OutcomeWriter {
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));
    let successes = test_successes(&broker, WORKER);
    OutcomeWriter::new(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        WORKER.to_owned(),
        PayloadPolicy::default(),
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

async fn history_facts(pool: &TestPool, task_id: Uuid) -> Option<HistoryFacts> {
    sql::query_as(
        "SELECT status::text, terminalization_kind::text, error_code, final_failed_reason
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read the history row")
}

/// The attempts that the history row of `task_id` archived.
async fn archived_attempts(pool: &TestPool, task_id: Uuid) -> Vec<AttemptRecord> {
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

/// The status of the one node of `workflow_id`, and whether a phase-2
/// record waits for `task_id`.
async fn node_and_phase_two_record(
    pool: &TestPool,
    workflow_id: Uuid,
    task_id: Uuid,
) -> (String, bool) {
    sql::query_as(
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

async fn listen_on_queue(pool: &TestPool, queue: &str) -> TestListener {
    let mut listener = TestListener::connect_with(pool)
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
    listener: &mut TestListener,
    pool: &TestPool,
    queue: &str,
) -> Vec<String> {
    let sentinel = format!("sentinel:{}", Uuid::new_v4());
    sql::query("SELECT pg_notify($1, $2)")
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

async fn remove_task(pool: &TestPool, task_id: Uuid) {
    for statement in [
        "DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        "DELETE FROM horsies_task_attempts WHERE task_id = $1",
        "DELETE FROM horsies_heartbeats WHERE task_id = $1",
        "DELETE FROM horsies_tasks WHERE id = $1",
        "DELETE FROM horsies_task_history WHERE task_id = $1",
    ] {
        sql::query(statement)
            .bind(task_id)
            .execute(pool)
            .await
            .expect("remove the test task");
    }
}

async fn remove_workflow(pool: &TestPool, workflow_id: Uuid) {
    sql::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("remove the test workflow");
}

/// A result that does not read as an envelope fails the task with
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
    let expired = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
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

/// The child expired the claimed task of node 0 before it started; node 1
/// waits for node 0. The worker writes no task row and replays phase 2
/// alone, with no recovery: node 0 fails with the `TASK_EXPIRED` result,
/// node 1 is skipped, and the workflow (`on_error` `fail`) fails.
#[tokio::test]
#[serial]
async fn an_expired_workflow_task_fails_its_node_skips_its_dependent_and_fails_the_workflow() {
    let pool = migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) =
        seed_task(&pool, &queue, CLAIMED_WORKFLOW_TASK_PAST_ITS_DEADLINE).await;
    let workflow_id = seed_workflow(&pool, task_id, &queue, "ENQUEUED").await;
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, max_retries,
             execution, retention_class_key, status, task_id, is_subworkflow, created_at,
             started_at
         ) VALUES ('NONE', $1, $2, 1, 'dependent', $3, '{}', $4, 100,
                   '{0}', FALSE, 'all', 0, 'ISOLATED',
                   'forever', 'PENDING', NULL, FALSE, NOW(), NULL)",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(TASK_NAME)
    .bind(&queue)
    .execute(&pool)
    .await
    .expect("seed the dependent node");
    let expired = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
        .expire_claimed_task_before_start(
            task_id,
            WORKER,
            crate::broker::child_writes::WriteDeadline::start_attempt(),
        )
        .await
        .expect("the expiry that the child writes");
    assert_eq!(expired, PrestartExpiry::Expired);

    let settled = writer(&pool)
        .settle(
            &claimed(task_id, &queue, generation, TaskKind::WorkflowTask),
            stopped(StopReason::TaskExpired),
        )
        .await;

    assert_eq!(settled, Settlement::NothingOwed);
    let nodes: Vec<(i32, String, Option<String>)> = sql::query_as(
        "SELECT task_index, status::text, result FROM horsies_workflow_tasks
         WHERE workflow_id = $1 ORDER BY task_index",
    )
    .bind(workflow_id)
    .fetch_all(&pool)
    .await
    .expect("read the nodes");
    let [(0, first_status, first_result), (1, second_status, _)] = nodes.as_slice() else {
        panic!("expected nodes 0 and 1, got {nodes:?}");
    };
    assert_eq!(first_status, "FAILED");
    assert!(
        first_result
            .as_deref()
            .is_some_and(|result| result.contains("TASK_EXPIRED")),
        "{first_result:?}"
    );
    assert_eq!(second_status, "SKIPPED");
    let workflow_status: String =
        sql::query_scalar("SELECT status::text FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .expect("read the workflow");
    assert_eq!(workflow_status, "FAILED");
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

/// The live row of `task_id` as JSON; `None` when the row left the live
/// table.
async fn live_snapshot(pool: &TestPool, task_id: Uuid) -> Option<serde_json::Value> {
    sql::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live row")
}

/// The attempt rows of `task_id` in the live attempt table.
async fn attempt_rows(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the attempt rows")
}

/// The reported ends of Python `TestEveryRowEndingCommandCarriesTheDispatchedClaim`,
/// each with the seed of the row it ends: a plain and a workflow success, a
/// plain and a workflow worker failure, a result that is not an envelope,
/// and a task error that no retry policy admits (Python
/// `terminalization_for_refused_retry`).
fn row_ending_reports() -> Vec<(&'static str, Seed, AttemptEnd)> {
    let error = TaskError::new("MY_OWN_CODE", "x").expect("a code of the task's own");
    vec![
        (
            "plain-success",
            RUNNING_PLAIN,
            AttemptEnd::Reported(RunEnd::Result(ok_envelope(42))),
        ),
        (
            "workflow-success",
            RUNNING_WORKFLOW_TASK,
            AttemptEnd::Reported(RunEnd::Result(ok_envelope(42))),
        ),
        (
            "plain-worker-failure",
            RUNNING_PLAIN,
            AttemptEnd::Reported(RunEnd::WorkerFailure(FailureDetail::cut("pool died"))),
        ),
        (
            "workflow-worker-failure",
            RUNNING_WORKFLOW_TASK,
            AttemptEnd::Reported(RunEnd::WorkerFailure(FailureDetail::cut("pool died"))),
        ),
        (
            "undecodable",
            RUNNING_PLAIN,
            AttemptEnd::Reported(RunEnd::Result(ResultEnvelopeText::new(
                "{not an envelope".to_owned(),
            ))),
        ),
        (
            "refused-retry",
            RUNNING_PLAIN,
            AttemptEnd::Reported(RunEnd::Result(error_envelope(&error))),
        ),
    ]
}

/// Every write that ends the row is fenced
/// on the worker and the claim generation of this dispatch. The row is
/// RUNNING under a claim of the same worker one microsecond after the
/// dispatch, as after a lapsed lease and a new claim. Each reported end
/// writes nothing and settles as not held: the live row is unchanged, and
/// no attempt row and no history row exist. A workflow task has its node,
/// so only the fence refuses the write.
#[tokio::test]
#[serial]
async fn a_reported_end_on_a_later_claim_of_the_row_writes_nothing() {
    let pool = migrated_pool().await;
    let mut workflows = Vec::new();
    for (case, seed, end) in row_ending_reports() {
        let queue = test_queue();
        let (task_id, generation) = seed_task(&pool, &queue, seed).await;
        match seed.kind {
            TaskKind::WorkflowTask => {
                workflows.push(seed_workflow(&pool, task_id, &queue, "RUNNING").await);
            }
            TaskKind::Plain => {}
        }
        let before = live_snapshot(&pool, task_id).await;
        let dispatched = generation - chrono::Duration::microseconds(1);

        let settled = writer(&pool)
            .settle(&claimed(task_id, &queue, dispatched, seed.kind), end)
            .await;

        assert_eq!(settled, Settlement::NotHeld, "{case}");
        assert_eq!(live_snapshot(&pool, task_id).await, before, "{case}");
        assert_eq!(attempt_rows(&pool, task_id).await, 0, "{case}");
        assert_eq!(history_facts(&pool, task_id).await, None, "{case}");
        remove_task(&pool, task_id).await;
    }
    for workflow_id in workflows {
        remove_workflow(&pool, workflow_id).await;
    }
}

/// The stops `ORPHAN_CANCELLED`, `NODE_NOT_LINKED` (version 1 spelled it
/// `WORKFLOW_CHECK_FAILED`) and the aborts of a workflow task: the child's
/// stop before start wrote what each of these stops needs, the orphan cancel
/// included, so the worker writes nothing. Each
/// stop settles as nothing owed; the live row is unchanged, and no attempt
/// row and no history row exist.
#[tokio::test]
#[serial]
async fn the_stops_that_the_child_wrote_owe_nothing_for_a_workflow_task() {
    let pool = migrated_pool().await;
    for reason in [
        StopReason::ClaimLost,
        StopReason::WorkflowStopped,
        StopReason::OrphanCancelled,
        StopReason::NodeNotLinked,
    ] {
        let queue = test_queue();
        let (task_id, generation) = seed_task(&pool, &queue, CLAIMED_WORKFLOW_TASK).await;
        let before = live_snapshot(&pool, task_id).await;

        let settled = writer(&pool)
            .settle(
                &claimed(task_id, &queue, generation, TaskKind::WorkflowTask),
                stopped(reason),
            )
            .await;

        assert_eq!(settled, Settlement::NothingOwed, "{reason:?}");
        assert_eq!(live_snapshot(&pool, task_id).await, before, "{reason:?}");
        assert_eq!(attempt_rows(&pool, task_id).await, 0, "{reason:?}");
        assert_eq!(history_facts(&pool, task_id).await, None, "{reason:?}");
        remove_task(&pool, task_id).await;
    }
}

/// Settle `end` on a RUNNING plain row with no retry policy, and read what
/// history holds for it: the terminal facts and the archived attempts.
async fn settle_on_a_running_row(end: AttemptEnd) -> (HistoryFacts, Vec<AttemptRecord>) {
    let pool = migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed_task(&pool, &queue, RUNNING_PLAIN).await;

    let settled = writer(&pool)
        .settle(&claimed(task_id, &queue, generation, TaskKind::Plain), end)
        .await;

    let facts = history_facts(&pool, task_id).await;
    let attempts = archived_attempts(&pool, task_id).await;
    remove_task(&pool, task_id).await;
    assert_eq!(settled, Settlement::Written);
    (facts.expect("the task moved to history"), attempts)
}

/// The row failed with `code` and no task failed reason; its one attempt
/// is FAILED with `code`, and with `text` as its message and its failed
/// reason (Python `_write_ended_attempt`).
fn assert_attempt_failed_with_reason(
    facts: &HistoryFacts,
    attempts: &[AttemptRecord],
    code: &str,
    text: &str,
) {
    let (status, _kind, error_code, final_failed_reason) = facts;
    assert_eq!(status, "FAILED");
    assert_eq!(error_code.as_deref(), Some(code));
    assert_eq!(final_failed_reason, &None);
    let [attempt] = attempts else {
        panic!("one attempt, got {}", attempts.len());
    };
    assert_eq!(attempt.outcome(), "FAILED");
    assert_eq!(attempt.error_code(), Some(code));
    assert_eq!(attempt.error_message(), Some(text));
    assert_eq!(attempt.failed_reason(), Some(text));
}

/// Python `_finalize_child_loss`: a child lost with its exit seen fails the
/// running task with `WORKER_CRASHED`; the attempt stores
/// `Worker child lost: child died: exit code 9` as its message and its
/// failed reason.
#[tokio::test]
#[serial]
async fn a_lost_child_stores_its_reason_on_the_attempt() {
    let end = AttemptEnd::of_lost_child(&child_died_with_exit_code_9());

    let (facts, attempts) = settle_on_a_running_row(end).await;

    assert_attempt_failed_with_reason(
        &facts,
        &attempts,
        "WORKER_CRASHED",
        "Worker child lost: child died: exit code 9",
    );
}

/// Python `_kill_at_deadline` and `_finalize_child_loss`: the drain
/// deadline kills the child with the worker's kill, so the loss is
/// `KILLED_BY_PARENT` with the exit `signal 9 (SIGKILL)`; the running task
/// fails with `WORKER_CRASHED`, and the attempt stores
/// `Worker child lost: killed by the worker: signal 9 (SIGKILL)` as its
/// message and its failed reason. The isolated slot gives this end at the
/// drain deadline
/// (`stop_and_loss_tests::the_drain_deadline_kills_a_busy_child_and_its_task_takes_the_crash_path`).
#[tokio::test]
#[serial]
async fn a_child_killed_at_the_drain_deadline_stores_its_reason_on_the_attempt() {
    let killed_at_drain_deadline = ChildLost {
        cause: LossCause::KilledByParent,
        exit: ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL)),
        ..child_died_with_exit_code_9()
    };
    let end = AttemptEnd::of_lost_child(&killed_at_drain_deadline);

    let (facts, attempts) = settle_on_a_running_row(end).await;

    assert_attempt_failed_with_reason(
        &facts,
        &attempts,
        "WORKER_CRASHED",
        "Worker child lost: killed by the worker: signal 9 (SIGKILL)",
    );
}

/// The objects of [`refuse_first_attempt_insert`], named for one test.
struct AttemptInsertRefusal {
    counter: String,
    function: String,
    trigger: String,
}

impl AttemptInsertRefusal {
    async fn remove(self, pool: &TestPool) {
        for statement in [
            format!(
                "DROP TRIGGER IF EXISTS {} ON horsies_task_attempts",
                self.trigger
            ),
            format!("DROP FUNCTION IF EXISTS {}()", self.function),
            format!("DROP SEQUENCE IF EXISTS {}", self.counter),
        ] {
            sql::query(&statement)
                .execute(pool)
                .await
                .expect("remove the attempt insert refusal");
        }
    }
}

/// Refuse the first insert of an attempt row of `task_id` with SQLSTATE
/// 40001 (`serialization_failure`, a class that may clear): the first send
/// of the failed-attempt write fails, and the outcome writer sends the
/// write again. A sequence counts the inserts: `nextval` is not rolled back
/// with the refused statement.
async fn refuse_first_attempt_insert(pool: &TestPool, task_id: Uuid) -> AttemptInsertRefusal {
    let suffix = Uuid::new_v4().simple().to_string();
    let refusal = AttemptInsertRefusal {
        counter: format!("report_attempt_inserts_{suffix}"),
        function: format!("report_refuse_attempt_{suffix}"),
        trigger: format!("report_refuse_attempt_{suffix}"),
    };
    for statement in [
        format!("CREATE SEQUENCE {}", refusal.counter),
        format!(
            "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $body$
             BEGIN
                 IF nextval('{counter}') <= 1 THEN
                     RAISE EXCEPTION 'a serialization failure for the test'
                         USING ERRCODE = 'serialization_failure';
                 END IF;
                 RETURN NEW;
             END
             $body$",
            function = refusal.function,
            counter = refusal.counter,
        ),
        format!(
            "CREATE TRIGGER {trigger} BEFORE INSERT ON horsies_task_attempts
             FOR EACH ROW WHEN (NEW.task_id = '{task_id}'::uuid)
             EXECUTE FUNCTION {function}()",
            trigger = refusal.trigger,
            function = refusal.function,
        ),
    ] {
        sql::query(&statement)
            .execute(pool)
            .await
            .expect("create the attempt insert refusal");
    }
    refusal
}

/// The `err.message` of the result that the history row of `task_id`
/// stores.
async fn stored_error_message(pool: &TestPool, task_id: Uuid) -> Option<String> {
    sql::query_scalar(
        "SELECT convert_from(result_payload, 'UTF8')::jsonb -> 'err' ->> 'message'
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the stored error message")
}

/// The crash-recovery write of a lost child fails once with a database
/// error that may clear; the write sent again stores
/// `Retry after future-stage finalize error: ` and the first message as the
/// task's error message, and as the attempt's message and failed reason.
#[tokio::test]
#[serial]
async fn a_crash_write_sent_again_stores_python_s_retry_text() {
    let pool = migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed_task(&pool, &queue, RUNNING_PLAIN).await;
    let refusal = refuse_first_attempt_insert(&pool, task_id).await;
    let end = AttemptEnd::of_lost_child(&child_died_with_exit_code_9());

    let settled = writer(&pool)
        .settle(&claimed(task_id, &queue, generation, TaskKind::Plain), end)
        .await;

    let facts = history_facts(&pool, task_id).await;
    let attempts = archived_attempts(&pool, task_id).await;
    let error_message = stored_error_message(&pool, task_id).await;
    refusal.remove(&pool).await;
    remove_task(&pool, task_id).await;
    let text =
        "Retry after future-stage finalize error: Worker child lost: child died: exit code 9";
    assert_eq!(settled, Settlement::Written);
    assert_attempt_failed_with_reason(
        &facts.expect("the task moved to history"),
        &attempts,
        "WORKER_CRASHED",
        text,
    );
    assert_eq!(error_message.as_deref(), Some(text));
}

/// Python `_settle_timed_out_row`: a run past its deadline fails the
/// running task with `TASK_TIMEOUT`; the attempt stores
/// `Task exceeded timeout_ms=1000` as its message and its failed reason.
#[tokio::test]
#[serial]
async fn a_timed_out_attempt_stores_its_reason() {
    let end = AttemptEnd::TimedOut {
        timeout: TaskTimeoutMs::new(1_000).expect("1 s is a valid task timeout"),
        retry: RetryPlacement::SameClass,
        cancel: TimeoutCancel::NoSignal,
    };

    let (facts, attempts) = settle_on_a_running_row(end).await;

    assert_attempt_failed_with_reason(
        &facts,
        &attempts,
        "TASK_TIMEOUT",
        "Task exceeded timeout_ms=1000",
    );
}

/// Python `_finalize_killed_run`, `BlockedTheLoop`: the run that held the
/// event loop fails with `TASK_BLOCKED_LOOP`; the attempt stores the block
/// message as its message and its failed reason.
#[tokio::test]
#[serial]
async fn a_run_that_blocked_the_loop_stores_its_reason_on_the_attempt() {
    let text = "Task held the event loop of its shared async child for 5200 ms, past the \
                block limit, at app/tasks.py:12; the worker killed the child \
                (pid=4242, signal 9 (SIGKILL))";
    let end = AttemptEnd::KilledForLoop {
        code: LoopKillCode::TaskBlockedLoop,
        retry: RetryPlacement::Isolated,
        reason: FailureDetail::cut(text),
    };

    let (facts, attempts) = settle_on_a_running_row(end).await;

    assert_attempt_failed_with_reason(&facts, &attempts, "TASK_BLOCKED_LOOP", text);
}

/// Python `_finalize_killed_run`, `BesideBlockedLoop`: a run beside the
/// run that held the event loop fails with `NEIGHBOUR_BLOCKED_LOOP`; the
/// attempt stores the block message as its message and its failed reason.
#[tokio::test]
#[serial]
async fn a_run_beside_a_blocked_loop_stores_its_reason_on_the_attempt() {
    let text = "Task 00000000-0000-0000-0000-000000000001 held the event loop of the shared \
                async child of this task for 5200 ms, past the block limit; the worker \
                killed the child (pid=4242, signal 9 (SIGKILL))";
    let end = AttemptEnd::KilledForLoop {
        code: LoopKillCode::NeighbourBlockedLoop,
        retry: RetryPlacement::SameClass,
        reason: FailureDetail::cut(text),
    };

    let (facts, attempts) = settle_on_a_running_row(end).await;

    assert_attempt_failed_with_reason(&facts, &attempts, "NEIGHBOUR_BLOCKED_LOOP", text);
}
