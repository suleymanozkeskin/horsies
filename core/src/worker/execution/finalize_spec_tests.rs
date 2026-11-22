//! The parent finalize rows: retry scheduling, the terminal state, the
//! result of a replay, the wakes of phase 2, and `sent_at` across retries
//! (Python `clients/python/tests/e2e/test_layer1_tasks.py`).
//!
//! Each test enters at [`persist_terminal_state`], the worker's phase 1 for a
//! result of an attempt, and reads the rows it left. The dispatch facts (the
//! outcome writer's [`ClaimedTask`]) are read from the seeded row, as the
//! claim returns them. The wake rows then enter phase 2 at
//! [`finalize_workflow_phase`].

use std::time::Duration;

use chrono::{DateTime, Utc};
use serial_test::serial;
use uuid::Uuid;

use super::{
    finalize_workflow_phase, persist_terminal_state, FinalizeError, FinalizeOutcome, FinalizeStage,
    PhaseTwoForm,
};
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::PostgresConfig;
use crate::core::task::error::{BuiltInTaskCode, TaskError};
use crate::core::task::result::TaskResult;
use crate::core::task::Execution;
use crate::test_support::sql::{self, TestListener, TestPool};
use crate::worker::executor::slots::{AttemptLog, ClaimedTask, RetryFacts, TaskKind};
use crate::worker::recovery::on_test_pool::mark_stale_running_as_failed;
use crate::worker::retry::read_stored_options;

const WORKER: &str = "finalize-spec-worker";
const TASK_NAME: &str = "finalize_spec_task";

/// How long after the retry write the test shows that no wake came yet. The
/// retry delay of these tests is 1 s.
const EARLY_WAKE_WINDOW: Duration = Duration::from_millis(300);

/// The bound of the wait for the delayed retry wake.
const WAKE_BOUND: Duration = Duration::from_secs(10);

/// The stale threshold of the reaper call: the seeded rows started 30
/// minutes ago and have no runner beat.
const STALE_THRESHOLD_SECONDS: f64 = 60.0;
const FINALIZING_THRESHOLD_SECONDS: f64 = 300.0;
const STALE_SCAN_LIMIT: i64 = 1_000;

/// Whether the seeded task belongs to a workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Plain,
    Workflow,
}

/// One RUNNING task on the baseline schema, held by [`WORKER`].
struct TaskSeed<'a> {
    queue: &'a str,
    retry_count: i32,
    max_retries: i32,
    task_options: Option<&'a str>,
    good_until: Option<DateTime<Utc>>,
    origin: Origin,
}

impl<'a> TaskSeed<'a> {
    fn plain(queue: &'a str) -> TaskSeed<'a> {
        TaskSeed {
            queue,
            retry_count: 0,
            max_retries: 0,
            task_options: None,
            good_until: None,
            origin: Origin::Plain,
        }
    }
}

/// The ids of a seeded task and, for a workflow task, its workflow.
struct Seeded {
    task_id: Uuid,
    workflow_id: Option<Uuid>,
}

/// Stored options with a retry policy (Python's stored layout).
fn retry_options(
    max_retries: u32,
    backoff: &str,
    intervals: &[u32],
    auto_retry_for: &[&str],
) -> String {
    serde_json::json!({
        "retry_policy": {
            "max_retries": max_retries,
            "intervals": intervals,
            "backoff_strategy": backoff,
            "jitter": false,
            "max_delay_seconds": null,
            "auto_retry_for": auto_retry_for,
        }
    })
    .to_string()
}

/// A RUNNING task row, started 30 minutes ago with no heartbeat. A workflow
/// task also gets a RUNNING workflow and a RUNNING node linked to it.
async fn seed_task(pool: &TestPool, seed: &TaskSeed<'_>) -> Seeded {
    let task_id = Uuid::new_v4();
    let (fingerprint_version, fingerprint): (Option<i16>, Option<Vec<u8>>) = match seed.origin {
        Origin::Plain => (Some(1), Some(vec![5_u8; 32])),
        Origin::Workflow => (None, None),
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
            $1, $2, $3, 100, '{}', $4, 'ISOLATED',
            'forever', date_trunc('milliseconds', NOW()) - interval '31 minutes',
            NOW() - interval '31 minutes',
            $5, $6, NULL,
            'RUNNING', NOW() - interval '31 minutes',
            date_trunc('milliseconds', NOW()) - interval '30 minutes', $7, NULL,
            NOW() - interval '30 minutes', NULL, NULL,
            4242, 'finalize-host', 'finalize-process',
            $8, $9, NULL, $10, $11,
            NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(TASK_NAME)
    .bind(seed.queue)
    .bind(seed.task_options)
    .bind(fingerprint_version)
    .bind(fingerprint)
    .bind(WORKER)
    .bind(seed.retry_count)
    .bind(seed.max_retries)
    .bind(seed.good_until)
    .bind(seed.origin == Origin::Workflow)
    .execute(pool)
    .await
    .expect("seed RUNNING task");
    let workflow_id = match seed.origin {
        Origin::Plain => None,
        Origin::Workflow => Some(seed_workflow_for(pool, task_id, seed.queue).await),
    };
    Seeded {
        task_id,
        workflow_id,
    }
}

/// A RUNNING workflow with one RUNNING task node linked to `task_id`.
async fn seed_workflow_for(pool: &TestPool, task_id: Uuid, queue: &str) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'finalize_spec_workflow', NULL, 'RUNNING', 'fail', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("seed workflow");
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, allow_failed_deps, join_type, max_retries, execution,
             retention_class_key, status, task_id, is_subworkflow, created_at, started_at
         ) VALUES ('NONE', $1, $2, 0, 'only', $3, '{}', $4, 100,
                   '{}', FALSE, 'all', 0, 'ISOLATED',
                   'forever', 'RUNNING', $5, FALSE, NOW(), NOW())",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(TASK_NAME)
    .bind(queue)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed workflow node");
    workflow_id
}

/// The claimed task as the claim returns it: the claim generation, the class,
/// the kind, the retry facts and the stored options of the row.
async fn claimed_task(pool: &TestPool, task_id: Uuid) -> ClaimedTask {
    let (
        task_name,
        queue_name,
        claimed_at,
        execution,
        is_workflow_task,
        retry_count,
        max_retries,
        task_options,
    ): (
        String,
        String,
        DateTime<Utc>,
        String,
        bool,
        i32,
        i32,
        Option<String>,
    ) = sql::query_as(
        "SELECT task_name, queue_name, claimed_at, execution, is_workflow_task,
                retry_count, max_retries, task_options
         FROM horsies_tasks WHERE id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("claimed row");
    ClaimedTask {
        task_id,
        task_name,
        queue_name,
        claimed_at,
        execution: Execution::from_stored(&execution).expect("a stored execution class"),
        kind: match is_workflow_task {
            true => TaskKind::WorkflowTask,
            false => TaskKind::Plain,
        },
        retry: RetryFacts {
            retry_count,
            max_retries,
        },
        options: read_stored_options(task_options.as_deref()),
        trace: crate::events::RunTrace::NoCarrier,
        trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
    }
}

/// Run phase 1 with the dispatch facts read now.
async fn finalize(
    pool: &TestPool,
    task_id: Uuid,
    result: TaskResult<Vec<u8>>,
) -> Result<FinalizeOutcome, FinalizeError> {
    let claimed = claimed_task(pool, task_id).await;
    finalize_with(pool, result, &claimed).await
}

/// Run phase 1 with dispatch facts that the caller read earlier.
async fn finalize_with(
    pool: &TestPool,
    result: TaskResult<Vec<u8>>,
    claimed: &ClaimedTask,
) -> Result<FinalizeOutcome, FinalizeError> {
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    persist_terminal_state(&broker, claimed, result, WORKER, &PayloadPolicy::default()).await
}

fn error_result(code: &str) -> TaskResult<Vec<u8>> {
    TaskResult::Err(
        TaskError::new(code, format!("{code} happened")).expect("the test codes are user codes"),
    )
}

fn ok_result(text: &str) -> TaskResult<Vec<u8>> {
    TaskResult::Ok(serde_json::to_vec(text).expect("encode a JSON string"))
}

/// A name for each outcome, for assertion messages.
fn describe(outcome: &Result<FinalizeOutcome, FinalizeError>) -> String {
    match outcome {
        Ok(FinalizeOutcome::Terminal {
            capacity_notified,
            phase_two,
        }) => format!(
            "Terminal {{ capacity_notified: {capacity_notified}, phase_two: {phase_two:?} }}"
        ),
        Ok(FinalizeOutcome::Retried { .. }) => "Retried".to_owned(),
        Ok(FinalizeOutcome::Finalized) => "Finalized".to_owned(),
        Ok(FinalizeOutcome::Refused) => "Refused".to_owned(),
        Err(error) => format!("Err({error})"),
    }
}

async fn live_row(pool: &TestPool, task_id: Uuid) -> Option<serde_json::Value> {
    sql::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read live row")
}

async fn history_row(pool: &TestPool, task_id: Uuid) -> Option<serde_json::Value> {
    sql::query_scalar("SELECT to_jsonb(h) FROM horsies_task_history h WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read history row")
}

/// The live attempt rows: attempt, will_retry, error code.
async fn live_attempts(pool: &TestPool, task_id: Uuid) -> Vec<(i32, bool, Option<String>)> {
    sql::query_as(
        "SELECT attempt, will_retry, error_code FROM horsies_task_attempts
         WHERE task_id = $1 ORDER BY attempt",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await
    .expect("read attempts")
}

async fn cleanup(pool: &TestPool, seeded: &Seeded) {
    for statement in [
        "DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        "DELETE FROM horsies_task_attempts WHERE task_id = $1",
        "DELETE FROM horsies_heartbeats WHERE task_id = $1",
        "DELETE FROM horsies_tasks WHERE id = $1",
        "DELETE FROM horsies_task_history WHERE task_id = $1",
    ] {
        sql::query(statement)
            .bind(seeded.task_id)
            .execute(pool)
            .await
            .expect("cleanup");
    }
    if let Some(workflow_id) = seeded.workflow_id {
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await
            .expect("cleanup workflow");
    }
}

/// A queue name of this test run: `task_queue_<name>` stays within the
/// 63-byte channel limit.
fn test_queue() -> String {
    format!(
        "finalize_spec_{}",
        &Uuid::new_v4().simple().to_string()[..8]
    )
}

/// A listed code schedules the retry:
/// PENDING, the retry count up, the owner cleared, `enqueued_at` equal to
/// `next_retry_at`, one attempt that will retry, and a wake of the queue
/// after the retry delay, not before.
#[tokio::test]
#[serial(timing)]
async fn a_listed_code_schedules_the_retry_and_wakes_its_queue_after_the_delay() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let options = retry_options(3, "fixed", &[1, 1, 1], &["ERR"]);
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            retry_count: 1,
            max_retries: 3,
            task_options: Some(&options),
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let mut listener = TestListener::connect_with(&pool).await.expect("listener");
    listener
        .listen(&format!("task_queue_{queue}"))
        .await
        .expect("listen on the queue channel");

    let outcome = finalize(&pool, seeded.task_id, error_result("ERR")).await;

    assert!(
        matches!(outcome, Ok(FinalizeOutcome::Retried { .. })),
        "{}",
        describe(&outcome)
    );
    let row = live_row(&pool, seeded.task_id).await.expect("live row");
    assert_eq!(row["status"], "PENDING");
    assert_eq!(row["retry_count"], 2);
    assert!(row["claimed_by_worker_id"].is_null());
    assert!(!row["next_retry_at"].is_null());
    assert_eq!(row["enqueued_at"], row["next_retry_at"]);
    assert_eq!(
        live_attempts(&pool, seeded.task_id).await,
        vec![(2, true, Some("ERR".to_owned()))]
    );
    let early = tokio::time::timeout(EARLY_WAKE_WINDOW, listener.try_recv()).await;
    assert!(early.is_err(), "the queue was woken before the retry delay");
    let wake = tokio::time::timeout(WAKE_BOUND, listener.try_recv())
        .await
        .expect("the delayed wake arrives within the bound")
        .expect("the listener reads")
        .expect("the listener stays connected");
    assert_eq!(wake.payload(), seeded.task_id.to_string());
    cleanup(&pool, &seeded).await;
}

/// The dispatch read `good_until` an hour ahead at the start; it moves into
/// the past before the write. The write judges the value it locks, so the
/// task fails instead of retrying.
#[tokio::test]
async fn good_until_is_judged_under_the_lock() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let options = retry_options(1, "fixed", &[1], &["ERR"]);
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            max_retries: 1,
            task_options: Some(&options),
            good_until: Some(Utc::now() + chrono::Duration::hours(1)),
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let claimed = claimed_task(&pool, seeded.task_id).await;
    sql::query("UPDATE horsies_tasks SET good_until = NOW() - interval '5 minutes' WHERE id = $1")
        .bind(seeded.task_id)
        .execute(&pool)
        .await
        .expect("move good_until into the past");

    let outcome = finalize_with(&pool, error_result("ERR"), &claimed).await;

    assert!(
        matches!(outcome, Ok(FinalizeOutcome::Finalized)),
        "{}",
        describe(&outcome)
    );
    assert_eq!(live_row(&pool, seeded.task_id).await, None);
    let history = history_row(&pool, seeded.task_id)
        .await
        .expect("history row");
    assert_eq!(history["status"], "FAILED");
    cleanup(&pool, &seeded).await;
}

/// The failure path: the reaper failed the row first; the worker's
/// failure write is refused, owes nothing and changes nothing.
#[tokio::test]
async fn a_failure_for_a_row_the_reaper_already_failed_changes_nothing() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let seeded = seed_task(&pool, &TaskSeed::plain(&queue)).await;
    let claimed = claimed_task(&pool, seeded.task_id).await;
    let _processed = mark_stale_running_as_failed(
        &pool,
        STALE_THRESHOLD_SECONDS,
        FINALIZING_THRESHOLD_SECONDS,
        STALE_SCAN_LIMIT,
    )
    .await
    .expect("stale recovery");
    let before = history_row(&pool, seeded.task_id)
        .await
        .expect("the reaper moved the row to history");
    assert_eq!(before["status"], "FAILED");

    let outcome = finalize_with(&pool, error_result("ERR"), &claimed).await;

    assert!(
        matches!(outcome, Ok(FinalizeOutcome::Refused)),
        "{}",
        describe(&outcome)
    );
    assert_eq!(history_row(&pool, seeded.task_id).await, Some(before));
    assert_eq!(live_row(&pool, seeded.task_id).await, None);
    cleanup(&pool, &seeded).await;
}

/// A plain task: a second phase 1 of a committed failure writes nothing,
/// and a plain task owes no phase 2 either time.
#[tokio::test]
async fn a_plain_failure_replay_after_a_lost_reply_writes_nothing() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let seeded = seed_task(&pool, &TaskSeed::plain(&queue)).await;
    let claimed = claimed_task(&pool, seeded.task_id).await;

    let first = finalize_with(&pool, error_result("TRANSIENT"), &claimed).await;
    let committed = history_row(&pool, seeded.task_id)
        .await
        .expect("history row");
    let replay = finalize_with(&pool, error_result("TRANSIENT"), &claimed).await;

    assert!(
        matches!(first, Ok(FinalizeOutcome::Finalized)),
        "{}",
        describe(&first)
    );
    assert!(
        matches!(replay, Ok(FinalizeOutcome::Finalized)),
        "{}",
        describe(&replay)
    );
    assert_eq!(history_row(&pool, seeded.task_id).await, Some(committed));
    assert_eq!(live_attempts(&pool, seeded.task_id).await, Vec::new());
    cleanup(&pool, &seeded).await;
}

/// A workflow task: the replay writes nothing, and the workflow task still
/// owes its phase 2 for the committed failure.
#[tokio::test]
async fn a_workflow_failure_replay_after_a_lost_reply_owes_phase_two() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::Workflow,
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let claimed = claimed_task(&pool, seeded.task_id).await;

    let first = finalize_with(&pool, error_result("TRANSIENT"), &claimed).await;
    let committed = history_row(&pool, seeded.task_id)
        .await
        .expect("history row");
    let replay = finalize_with(&pool, error_result("TRANSIENT"), &claimed).await;

    for outcome in [&first, &replay] {
        assert!(
            matches!(outcome, Ok(FinalizeOutcome::Terminal { .. })),
            "{}",
            describe(outcome)
        );
    }
    assert_eq!(committed["status"], "FAILED");
    assert_eq!(history_row(&pool, seeded.task_id).await, Some(committed));
    cleanup(&pool, &seeded).await;
}

/// A committed workflow success replays phase 2, not phase 1; the replay
/// adds no attempt and leaves the committed record as it was.
#[tokio::test]
async fn a_workflow_success_replay_owes_phase_two() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::Workflow,
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let claimed = claimed_task(&pool, seeded.task_id).await;

    let first = finalize_with(&pool, ok_result("persisted"), &claimed).await;
    let committed = history_row(&pool, seeded.task_id)
        .await
        .expect("history row");
    let replay = finalize_with(&pool, ok_result("persisted"), &claimed).await;

    for outcome in [&first, &replay] {
        assert!(
            matches!(outcome, Ok(FinalizeOutcome::Terminal { .. })),
            "{}",
            describe(outcome)
        );
    }
    assert_eq!(committed["status"], "COMPLETED");
    assert_eq!(history_row(&pool, seeded.task_id).await, Some(committed));
    cleanup(&pool, &seeded).await;
}

/// Entered at the finalize: a workflow success with a stale claim
/// generation is refused, owes nothing, and leaves the row of the newer
/// claim RUNNING with no attempt.
#[tokio::test]
async fn a_stale_generation_success_leaves_the_newer_claim_unchanged() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::Workflow,
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let mut claimed = claimed_task(&pool, seeded.task_id).await;
    claimed.claimed_at -= chrono::Duration::minutes(1);
    let before = live_row(&pool, seeded.task_id).await;

    let outcome = finalize_with(&pool, ok_result("stale"), &claimed).await;

    assert!(
        matches!(outcome, Ok(FinalizeOutcome::Refused)),
        "{}",
        describe(&outcome)
    );
    assert_eq!(live_row(&pool, seeded.task_id).await, before);
    assert_eq!(live_attempts(&pool, seeded.task_id).await, Vec::new());
    assert_eq!(history_row(&pool, seeded.task_id).await, None);
    cleanup(&pool, &seeded).await;
}

/// Python `test_worker_load_persisted_task_result.py::test_a_plain_task_owes_no_phase_two`
/// (horsies #500): the terminal write of a plain task, success or failure,
/// sends its capacity wake in the same statement and owes no phase 2.
#[tokio::test]
async fn a_plain_task_owes_no_phase_two() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let succeeded = seed_task(&pool, &TaskSeed::plain(&queue)).await;
    let failed = seed_task(&pool, &TaskSeed::plain(&queue)).await;

    let success = finalize(&pool, succeeded.task_id, ok_result("done")).await;
    let failure = finalize(&pool, failed.task_id, error_result("FATAL")).await;

    for outcome in [&success, &failure] {
        assert!(
            matches!(outcome, Ok(FinalizeOutcome::Finalized)),
            "{}",
            describe(outcome)
        );
    }
    let completed = history_row(&pool, succeeded.task_id)
        .await
        .expect("history row of the success");
    assert_eq!(completed["status"], "COMPLETED");
    let failed_row = history_row(&pool, failed.task_id)
        .await
        .expect("history row of the failure");
    assert_eq!(failed_row["status"], "FAILED");
    let pending_phase_two: i64 = sql::query_scalar(
        "SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = ANY($1)",
    )
    .bind(vec![succeeded.task_id, failed.task_id])
    .fetch_one(&pool)
    .await
    .expect("read phase-2 records");
    assert_eq!(pending_phase_two, 0);
    cleanup(&pool, &succeeded).await;
    cleanup(&pool, &failed).await;
}

/// `WORKFLOW_STOPPED` is a reserved code and the branch
/// that left the row RUNNING is removed (decisions, "Task returns: one rule
/// in the core"). No task result can carry the code: it is not a built-in
/// code and `TaskError::new` refuses it, so no result that reaches
/// `persist_terminal_state` holds it.
#[test]
fn a_task_result_cannot_carry_the_workflow_stopped_code() {
    assert_eq!(BuiltInTaskCode::parse("WORKFLOW_STOPPED"), None);
    assert!(TaskError::new("WORKFLOW_STOPPED", "workflow was stopped").is_err());
}

/// Python `test_layer1_tasks.py::test_exponential_backoff_intervals`: two
/// retries keep the `sent_at` of the send, and the second retry waits longer
/// than the first.
#[tokio::test]
async fn retries_keep_the_sent_at_and_the_second_delay_is_longer() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let options = retry_options(3, "exponential", &[1], &["ERR"]);
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            max_retries: 3,
            task_options: Some(&options),
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let sent_at = live_row(&pool, seeded.task_id).await.expect("live row")["sent_at"].clone();

    let mut delays = Vec::new();
    for retry in 1..=2_i32 {
        let outcome = finalize(&pool, seeded.task_id, error_result("ERR")).await;
        assert!(
            matches!(outcome, Ok(FinalizeOutcome::Retried { .. })),
            "retry {retry}: {}",
            describe(&outcome)
        );
        let (retry_count, row_sent_at, delay_ms): (i32, serde_json::Value, f64) = sql::query_as(
            "SELECT t.retry_count, to_jsonb(t.sent_at),
                    (EXTRACT(EPOCH FROM (t.next_retry_at - a.finished_at)) * 1000)::float8
             FROM horsies_tasks t
             JOIN horsies_task_attempts a ON a.task_id = t.id AND a.attempt = t.retry_count
             WHERE t.id = $1",
        )
        .bind(seeded.task_id)
        .fetch_one(&pool)
        .await
        .expect("read the retried row");
        assert_eq!(retry_count, retry);
        assert_eq!(row_sent_at, sent_at, "retry {retry} changed sent_at");
        delays.push(delay_ms);
        // The next attempt: claimed again and started by this worker.
        sql::query(
            "UPDATE horsies_tasks
             SET status = 'RUNNING', attempt_claims = 1, first_claimed_at = date_trunc('milliseconds', NOW()), claimed_at = date_trunc('milliseconds', NOW()),
                 claimed_by_worker_id = $2, claim_expires_at = NULL, started_at = NOW(),
                 worker_pid = 4242, worker_hostname = 'finalize-host',
                 worker_process_name = 'finalize-process', next_retry_at = NULL
             WHERE id = $1",
        )
        .bind(seeded.task_id)
        .bind(WORKER)
        .execute(&pool)
        .await
        .expect("start the next attempt");
    }

    assert!(
        delays[1] > delays[0],
        "the second delay ({} ms) is not longer than the first ({} ms)",
        delays[1],
        delays[0]
    );
    cleanup(&pool, &seeded).await;
}

/// Bound of the wakes read before the sentinel wake.
const WAKE_READ_LIMIT: u32 = 100;

/// A queue name whose channel `task_queue_<name>` is over PostgreSQL's
/// 63-byte limit, so the server refuses its wake (Python `_TOO_LONG_QUEUE`).
fn too_long_queue() -> String {
    "q".repeat(60)
}

/// A listener on the wake channel of `queue`.
async fn listen_on_queue(pool: &TestPool, queue: &str) -> TestListener {
    let mut listener = TestListener::connect_with(pool).await.expect("listener");
    listener
        .listen(&format!("task_queue_{queue}"))
        .await
        .expect("listen on the queue channel");
    listener
}

/// Every payload on the channel of `queue` committed before a sentinel wake
/// that this call commits. PostgreSQL delivers notifications in commit
/// order.
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
        .expect("send the sentinel wake");
    let mut payloads = Vec::new();
    for _ in 0..WAKE_READ_LIMIT {
        let notification = tokio::time::timeout(WAKE_BOUND, listener.recv())
            .await
            .expect("a wake within the bound")
            .expect("receive a wake");
        if notification.payload() == sentinel {
            return payloads;
        }
        payloads.push(notification.payload().to_owned());
    }
    panic!("sentinel wake not seen; payloads {payloads:?}");
}

/// Run phase 2 on a broker of `pool`, as [`phase_two_on`].
async fn phase_two(
    pool: &TestPool,
    task_id: Uuid,
    queue: &str,
    is_workflow_task: bool,
) -> Result<(), FinalizeError> {
    phase_two_on(
        &PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust),
        task_id,
        queue,
        is_workflow_task,
    )
    .await
}

/// Run phase 2 on `broker` with no wake sent by phase 1, in the form a
/// phase-1 replay gives (`PhaseTwoForm::NotKnown`: the transaction form of
/// the node finish).
async fn phase_two_on(
    broker: &PostgresBroker,
    task_id: Uuid,
    queue: &str,
    is_workflow_task: bool,
) -> Result<(), FinalizeError> {
    finalize_workflow_phase(
        broker,
        &crate::test_support::child_specs::TestChildSpecs::empty(),
        task_id,
        AttemptLog::FIRST_UNTRACED,
        queue,
        is_workflow_task,
        false,
        PhaseTwoForm::NotKnown,
    )
    .await
    .map(|_parent_advances| ())
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

/// Phase 2 of a workflow task sends one capacity wake on its queue, in the
/// transaction that completes the node (#722). The payload is the constant
/// `capacity` of the baseline function (`0005_workflow_functions.sql:1972`).
#[tokio::test]
async fn phase_two_of_a_workflow_task_wakes_its_queue_once() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::Workflow,
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let workflow_id = seeded.workflow_id.expect("a workflow task has a workflow");
    let phase_one = finalize(&pool, seeded.task_id, ok_result("done")).await;
    assert!(
        matches!(
            phase_one,
            Ok(FinalizeOutcome::Terminal {
                capacity_notified: false,
                ..
            })
        ),
        "{}",
        describe(&phase_one)
    );
    let mut listener = listen_on_queue(&pool, &queue).await;

    let result = phase_two(&pool, seeded.task_id, &queue, true).await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        wakes_before_sentinel(&mut listener, &pool, &queue).await,
        vec!["capacity".to_owned()]
    );
    assert_eq!(
        node_and_phase_two_record(&pool, workflow_id, seeded.task_id).await,
        ("COMPLETED".to_owned(), false)
    );
    cleanup(&pool, &seeded).await;
}

/// A plain task: its phase 2 is the capacity wake alone, one `capacity`
/// wake on its queue (#722).
#[tokio::test]
async fn phase_two_of_a_plain_task_wakes_its_queue_once() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let mut listener = listen_on_queue(&pool, &queue).await;

    let result = phase_two(&pool, Uuid::new_v4(), &queue, false).await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        wakes_before_sentinel(&mut listener, &pool, &queue).await,
        vec!["capacity".to_owned()]
    );
}

/// A workflow task with no phase-2 record is a replay: the node finish
/// writes nothing and sends no wake (`horsies_workflow_node_apply`,
/// `0005_workflow_functions.sql:1914-1915`). The wake is the baseline's
/// single source. Python's phase 2
/// (`test_phase_two_wakes_the_queue_of_the_task_once[no_node]`) sends the
/// wake from its own statement before the node lookup, so it wakes here.
#[tokio::test]
async fn phase_two_of_a_workflow_task_with_no_record_sends_no_wake() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let mut listener = listen_on_queue(&pool, &queue).await;

    let result = phase_two(&pool, Uuid::new_v4(), &queue, true).await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        wakes_before_sentinel(&mut listener, &pool, &queue).await,
        Vec::<String>::new()
    );
}

/// The longest queue name that the schema admits, in bytes
/// (`horsies_tasks_queue_name_length`,
/// `horsies_workflow_tasks_queue_name_length`): its channel
/// `task_queue_<name>` is 63 bytes, PostgreSQL's limit.
const LONGEST_QUEUE_BYTES: usize = 52;

/// A queue name of this test run of [`LONGEST_QUEUE_BYTES`] bytes.
fn longest_queue() -> String {
    let queue = test_queue();
    let padding = "q".repeat(LONGEST_QUEUE_BYTES - queue.len());
    format!("{queue}{padding}")
}

/// The node finish sends its wake on the channel of the node row's queue,
/// not on a queue that the caller names (`horsies_workflow_node_apply`,
/// `0005_workflow_functions.sql:1919`, `:1972`). The schema bounds a node's
/// queue at [`LONGEST_QUEUE_BYTES`], so `task_queue_<queue>` always fits
/// PostgreSQL's channel limit and the server cannot refuse the wake for its
/// channel: the longest admitted queue gets its wake, and a node row with a
/// longer queue is refused at the insert. Python's
/// `test_a_notify_that_fails_on_the_server_is_a_phase2_error` takes the
/// channel from the caller's queue. A refused wake of a plain task stays a
/// phase-2 error (`a_refused_wake_of_a_plain_task_is_a_phase_two_error_not_retried`).
#[tokio::test]
async fn the_node_finish_wakes_the_queue_of_the_node_row() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = longest_queue();
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::Workflow,
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let workflow_id = seeded.workflow_id.expect("a workflow task has a workflow");
    let refused = sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, status,
             is_subworkflow, created_at, max_retries, execution, retention_class_key
         ) VALUES ('NONE', $1, $2, 1, 'node_1', $3, '{}', $4, 100, '{0}',
                   FALSE, 'all', 'PENDING', FALSE, NOW(), 0, 'ISOLATED', 'forever')",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(TASK_NAME)
    .bind(format!("{queue}q"))
    .execute(&pool)
    .await;
    match refused {
        Err(sql::Error::Database(error)) => assert_eq!(
            error.constraint(),
            Some("horsies_workflow_tasks_queue_name_length"),
            "{error}"
        ),
        other => panic!("a node queue over the bound must be refused: {other:?}"),
    }
    let phase_one = finalize(&pool, seeded.task_id, ok_result("done")).await;
    assert!(
        matches!(phase_one, Ok(FinalizeOutcome::Terminal { .. })),
        "{}",
        describe(&phase_one)
    );
    let mut listener = listen_on_queue(&pool, &queue).await;

    let result = phase_two(&pool, seeded.task_id, &too_long_queue(), true).await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        wakes_before_sentinel(&mut listener, &pool, &queue).await,
        vec!["capacity".to_owned()]
    );
    assert_eq!(
        node_and_phase_two_record(&pool, workflow_id, seeded.task_id).await,
        ("COMPLETED".to_owned(), false)
    );
    cleanup(&pool, &seeded).await;
}

/// A plain task's phase 2 is the wake alone: a wake that the server refuses
/// is a phase-2 error that is not retried, because the refusal repeats
/// (#722).
#[tokio::test]
async fn a_refused_wake_of_a_plain_task_is_a_phase_two_error_not_retried() {
    let pool = crate::test_support::migrated_pool().await;

    let result = phase_two(&pool, Uuid::new_v4(), &too_long_queue(), false).await;

    match result {
        Err(error) => {
            assert_eq!(error.stage, FinalizeStage::Phase2Workflow);
            assert!(!error.retryable, "{error}");
            assert!(error.message.contains("channel name too long"), "{error}");
        }
        Ok(()) => panic!("phase 2 returned Ok for a refused wake"),
    }
}

/// The lock timeout of the phase-2 connections in
/// `a_transient_error_at_the_consume_is_a_retryable_phase_two_error`, as the
/// URL parameter `options=-c lock_timeout=100ms`: the bound of their wait for
/// the workflow row that the test holds. It goes into the URL that the
/// broker connects with: phase 2 runs on the core pools, which the broker
/// opens from that URL.
const CONSUME_LOCK_TIMEOUT_PARAMETER: &str = "options=-c%20lock_timeout%3D100ms";

/// The bound of the test's wait for that phase 2: a lock timeout that does
/// not apply fails the test instead of blocking the run.
const CONSUME_WAIT_BOUND: Duration = Duration::from_secs(10);

/// `url` with `parameter` added to its query.
fn with_query_parameter(url: &str, parameter: &str) -> String {
    let separator = match url.contains('?') {
        true => '&',
        false => '?',
    };
    format!("{url}{separator}{parameter}")
}

/// At the consume statement, a transient database error after the wake (a
/// lock timeout, SQLSTATE 55P03) is a retryable phase-2 error, the node and
/// its phase-2 record stay, and the retry completes the node (#865).
#[tokio::test]
async fn a_transient_error_at_the_consume_is_a_retryable_phase_two_error() {
    let pool = crate::test_support::migrated_pool().await;
    let queue = test_queue();
    let seeded = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::Workflow,
            ..TaskSeed::plain(&queue)
        },
    )
    .await;
    let workflow_id = seeded.workflow_id.expect("a workflow task has a workflow");
    let phase_one = finalize(&pool, seeded.task_id, ok_result("done")).await;
    assert!(
        matches!(phase_one, Ok(FinalizeOutcome::Terminal { .. })),
        "{}",
        describe(&phase_one)
    );
    let database_url = crate::test_support::migrated_database_url().await;
    let timeout_broker = PostgresBroker::connect_with(
        &PostgresConfig::from_url(with_query_parameter(
            &database_url,
            CONSUME_LOCK_TIMEOUT_PARAMETER,
        )),
        crate::ClientFamily::Rust,
    )
    .await
    .expect("connect a broker with a lock timeout");
    let mut holder = pool.begin().await.expect("begin the lock holder");
    sql::query("SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE")
        .bind(workflow_id)
        .execute(&mut *holder)
        .await
        .expect("lock the workflow row");

    let bounded = tokio::time::timeout(
        CONSUME_WAIT_BOUND,
        phase_two_on(&timeout_broker, seeded.task_id, &queue, true),
    )
    .await;

    holder.rollback().await.expect("release the workflow row");
    timeout_broker.close().await;
    let result = bounded.expect("phase 2 ends within the bound: its lock timeout applies");
    match result {
        Err(error) => {
            assert_eq!(error.stage, FinalizeStage::Phase2Workflow);
            assert!(error.retryable, "{error}");
        }
        Ok(()) => panic!("phase 2 returned Ok while the workflow row was locked"),
    }
    assert_eq!(
        node_and_phase_two_record(&pool, workflow_id, seeded.task_id).await,
        ("RUNNING".to_owned(), true)
    );
    let retry = phase_two(&pool, seeded.task_id, &queue, true).await;
    assert!(retry.is_ok(), "{retry:?}");
    assert_eq!(
        node_and_phase_two_record(&pool, workflow_id, seeded.task_id).await,
        ("COMPLETED".to_owned(), false)
    );
    cleanup(&pool, &seeded).await;
}
