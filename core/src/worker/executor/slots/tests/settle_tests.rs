//! The write of each attempt end on the test database, and on a database
//! that cannot be reached (Python `clients/python/tests/unit/test_worker_helpers.py`,
//! `TestFinalizeAfterSlotOutcome`, `TestRequeueDbErrorContainment`,
//! `TestHandleFinalizeError`, `TestRetryFinalizeFuture`).
//!
//! - A task that never reached a child goes back to the queue.
//! - A reported success and a lost child each end the row.
//! - A crash write on a row that this claim no longer holds writes nothing.
//! - A write that cannot reach the database is sent again at most three
//!   times (phase 2: five), then the end is left to recovery; it never reads
//!   as written. An error that cannot clear is not sent again.
//! - After the write for a lost child, a phase-2 error that can clear is
//!   tried again until the node finishes.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use super::*;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::test_support::sql::{self, TestPool, TestPoolOptions};
use crate::worker::execution::{phase_retry_delay, run_phase2, Phase2Work, PhaseTwoForm};
use crate::worker::executor::protocol::FailureDetail;

const WORKER: &str = "worker-1";
const OTHER_WORKER: &str = "worker-2";

fn writer(broker: Arc<PostgresBroker>) -> OutcomeWriter {
    let successes = crate::worker::executor::slots::outcome::test_successes(&broker, WORKER);
    OutcomeWriter::new(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        WORKER.to_owned(),
        PayloadPolicy::default(),
        successes,
    )
}

/// A broker on a port where no server listens: each statement fails before
/// it is sent, with a failure that may clear.
fn unreachable_broker() -> Arc<PostgresBroker> {
    Arc::new(PostgresBroker::from_test_url(
        "postgresql://horsies@127.0.0.1:1/horsies",
        crate::ClientFamily::Rust,
    ))
}

/// A claimed task with its own id and claim generation, no retry policy.
fn claimed_now() -> ClaimedTask {
    ClaimedTask {
        task_id: Uuid::new_v4(),
        claimed_at: Utc::now(),
        ..task(ExecutionClass::Isolated, "{}")
    }
}

/// A CLAIMED row of `owner` at the claim generation `claimed_at`.
async fn seed_claimed(pool: &TestPool, task_id: Uuid, owner: &str, claimed_at: DateTime<Utc>) {
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at, command_fingerprint_version,
            command_fingerprint, idempotency_key_digest, status, enqueued_at,
            claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
            finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
            worker_process_name, retry_count, max_retries, next_retry_at, good_until,
            is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
            rerun_root_task_id
        ) VALUES (
            $1, 'settle_test', 'default', 100, '{}', NULL, 'ISOLATED',
            'standard_30d', NOW(), NOW(), 1,
            decode(repeat('00', 32), 'hex'), NULL, 'CLAIMED', NOW(),
            $2, $3, NOW() + interval '1 minute', NULL,
            NULL, NULL, NULL, NULL,
            NULL, 0, 2, NULL, NULL,
            FALSE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(claimed_at)
    .bind(owner)
    .execute(pool)
    .await
    .unwrap();
}

/// A RUNNING row of `owner` at the claim generation `claimed_at`.
async fn seed_running(pool: &TestPool, task_id: Uuid, owner: &str, claimed_at: DateTime<Utc>) {
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at, command_fingerprint_version,
            command_fingerprint, idempotency_key_digest, status, enqueued_at,
            claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
            finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
            worker_process_name, retry_count, max_retries, next_retry_at, good_until,
            is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
            rerun_root_task_id
        ) VALUES (
            $1, 'settle_test', 'default', 100, '{}', NULL, 'ISOLATED',
            'standard_30d', NOW(), NOW(), 1,
            decode(repeat('00', 32), 'hex'), NULL, 'RUNNING', NOW(),
            $2, $3, NULL, NOW(),
            NULL, NULL, 1, 'host',
            'worker', 0, 2, NULL, NULL,
            FALSE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(claimed_at)
    .bind(owner)
    .execute(pool)
    .await
    .unwrap();
}

/// A RUNNING workflow task of `owner` at the claim generation `claimed_at`,
/// the one node of a RUNNING workflow whose failure policy is `fail`. A
/// workflow task has no command fingerprint. Returns the workflow id.
async fn seed_running_workflow_task(
    pool: &TestPool,
    task_id: Uuid,
    owner: &str,
    claimed_at: DateTime<Utc>,
) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflows (
            id, name, status, on_error, output_task_index,
            definition_key, depth, root_workflow_id,
            sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'settle_phase2', 'RUNNING', 'fail', NULL,
                   'test.settle.phase2.v1', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .unwrap();
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at, command_fingerprint_version,
            command_fingerprint, idempotency_key_digest, status, enqueued_at,
            claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
            finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
            worker_process_name, retry_count, max_retries, next_retry_at, good_until,
            is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
            rerun_root_task_id
        ) VALUES (
            $1, 'settle_test', 'default', 100, '{}', NULL, 'ISOLATED',
            'standard_30d', NOW(), NOW(), NULL,
            NULL, NULL, 'RUNNING', NOW(),
            $2, $3, NULL, NOW(),
            NULL, NULL, 1, 'host',
            'worker', 0, 0, NULL, NULL,
            TRUE, NOW(), FALSE, NULL,
            NULL
        )",
    )
    .bind(task_id)
    .bind(claimed_at)
    .bind(owner)
    .execute(pool)
    .await
    .unwrap();
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
            downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
            queue_name, priority, dependencies, allow_failed_deps, join_type, status,
            is_subworkflow, task_id, created_at, max_retries, execution, retention_class_key
        ) VALUES (
            'NONE', $1, $2, 0, 'root', 'settle_test', 'null',
            'default', 100, '{}', FALSE, 'all', 'RUNNING',
            FALSE, $3, NOW(), 0, 'ISOLATED', 'standard_30d'
        )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_id)
    .execute(pool)
    .await
    .unwrap();
    workflow_id
}

/// The objects of [`refuse_node_updates`], named for one test.
struct NodeUpdateRefusal {
    counter: String,
    function: String,
    trigger: String,
}

impl NodeUpdateRefusal {
    /// The node updates of the task that reached the trigger: the refused
    /// ones and the ones after them.
    async fn updates_seen(&self, pool: &TestPool) -> i64 {
        sql::query_scalar(&format!(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM {}",
            self.counter
        ))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn remove(self, pool: &TestPool) {
        for statement in [
            format!(
                "DROP TRIGGER IF EXISTS {} ON horsies_workflow_tasks",
                self.trigger
            ),
            format!("DROP FUNCTION IF EXISTS {}()", self.function),
            format!("DROP SEQUENCE IF EXISTS {}", self.counter),
        ] {
            sql::query(&statement).execute(pool).await.unwrap();
        }
    }
}

/// Refuse the first `refused` updates of the node of `task_id` with SQLSTATE
/// 40001 (`serialization_failure`, class 40: may clear). Phase 1 of a
/// workflow task does not update its node; phase 2 does. A sequence counts
/// the updates: `nextval` is not rolled back with the refused statement.
async fn refuse_node_updates(pool: &TestPool, task_id: Uuid, refused: i64) -> NodeUpdateRefusal {
    let suffix = Uuid::new_v4().simple().to_string();
    let refusal = NodeUpdateRefusal {
        counter: format!("settle_node_updates_{suffix}"),
        function: format!("settle_refuse_node_update_{suffix}"),
        trigger: format!("settle_refuse_node_update_{suffix}"),
    };
    for statement in [
        format!("CREATE SEQUENCE {}", refusal.counter),
        format!(
            "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $body$
             BEGIN
                 IF nextval('{counter}') <= {refused} THEN
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
            "CREATE TRIGGER {trigger} BEFORE UPDATE ON horsies_workflow_tasks
             FOR EACH ROW WHEN (OLD.task_id = '{task_id}'::uuid)
             EXECUTE FUNCTION {function}()",
            trigger = refusal.trigger,
            function = refusal.function,
        ),
    ] {
        sql::query(&statement).execute(pool).await.unwrap();
    }
    refusal
}

/// The status of the node of `task_id`, and of its workflow.
async fn node_and_workflow(pool: &TestPool, task_id: Uuid) -> (String, String) {
    sql::query_as(
        "SELECT n.status, w.status
         FROM horsies_workflow_tasks n
         JOIN horsies_workflows w ON w.id = n.workflow_id
         WHERE n.task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// The phase-2 records of `task_id` that are still owed.
async fn phase_two_records(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The status and owner of a live row.
async fn live_row(pool: &TestPool, task_id: Uuid) -> (String, Option<String>) {
    sql::query_as("SELECT status, claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The status and error code of the history row of a task that ended.
async fn history_row(pool: &TestPool, task_id: Uuid) -> (String, Option<String>) {
    sql::query_as("SELECT status, error_code FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn live_row_count(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

fn crashed() -> AttemptEnd {
    AttemptEnd::Crashed {
        reason: FailureDetail::cut("child died: signal 9 (SIGKILL)"),
    }
}

/// Python `TestFinalizeAfterSlotOutcome::test_task_that_never_reached_a_child_is_requeued`
/// and `TestRequeueDbErrorContainment::test_requeue_success_logs_no_critical`:
/// the claim is released at its generation and the row is PENDING again.
#[tokio::test]
#[serial_test::serial]
async fn a_task_that_never_reached_a_child_goes_back_to_the_queue() {
    let pool = crate::test_support::migrated_pool().await;
    let claimed = claimed_now();
    seed_claimed(&pool, claimed.task_id, WORKER, claimed.claimed_at).await;

    let settled = writer(Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    )))
    .settle(
        &claimed,
        AttemptEnd::NotDispatched(NotRunCause::PoolStopping),
    )
    .await;

    assert!(matches!(settled, Settlement::Written(_)), "{:?}", settled);
    assert_eq!(
        live_row(&pool, claimed.task_id).await,
        ("PENDING".to_owned(), None)
    );
}

/// Python `TestFinalizeAfterSlotOutcome::test_a_failed_requeue_is_retried`
/// and `TestRequeueDbErrorContainment::test_requeue_db_error_logs_critical`:
/// a release that cannot reach the database is tried again, never panics,
/// and does not read as written: the row is still CLAIMED, and its lease
/// returns it.
#[tokio::test]
async fn a_release_that_cannot_reach_the_database_is_not_written() {
    let settled = writer(unreachable_broker())
        .settle(
            &claimed_now(),
            AttemptEnd::NotDispatched(NotRunCause::QueueFull),
        )
        .await;
    assert_eq!(settled, Settlement::RetriesExhausted);
}

/// Python `TestFinalizeAfterSlotOutcome::test_finished_task_goes_to_persist`:
/// a reported value completes the task.
#[tokio::test]
#[serial_test::serial]
async fn a_reported_value_completes_the_task() {
    let pool = crate::test_support::migrated_pool().await;
    let claimed = claimed_now();
    seed_running(&pool, claimed.task_id, WORKER, claimed.claimed_at).await;

    let settled = writer(Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    )))
    .settle(
        &claimed,
        AttemptEnd::Reported(RunEnd::Result(ResultEnvelopeText::new(
            r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned(),
        ))),
    )
    .await;

    assert!(matches!(settled, Settlement::Written(_)), "{:?}", settled);
    assert_eq!(live_row_count(&pool, claimed.task_id).await, 0);
    assert_eq!(
        history_row(&pool, claimed.task_id).await,
        ("COMPLETED".to_owned(), None)
    );
}

/// Python `TestFinalizeAfterSlotOutcome::test_lost_child_is_recovered_and_retried_only_on_a_db_error`,
/// the write that applies: a lost child fails its RUNNING task with
/// `WORKER_CRASHED`, with no retry when the task has no policy.
#[tokio::test]
#[serial_test::serial]
async fn a_lost_child_fails_its_running_task_as_crashed() {
    let pool = crate::test_support::migrated_pool().await;
    let claimed = claimed_now();
    seed_running(&pool, claimed.task_id, WORKER, claimed.claimed_at).await;

    let settled = writer(Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    )))
    .settle(&claimed, crashed())
    .await;

    assert!(matches!(settled, Settlement::Written(_)), "{:?}", settled);
    assert_eq!(
        history_row(&pool, claimed.task_id).await,
        ("FAILED".to_owned(), Some("WORKER_CRASHED".to_owned()))
    );
}

/// Python `clients/python/tests/integration/test_lifecycle_regressions.py::test_future_failure_terminal_phase2_error_schedules_finalize_retry`:
/// the write for a lost child commits phase 1 (the task is FAILED with
/// `WORKER_CRASHED`), and a phase-2 error that can clear is not dropped:
/// phase 2 is tried again, and the node and its workflow finish. The first
/// two node updates are refused with SQLSTATE 40001; the third try passes
/// after waits of 0.5 s and 1 s.
#[tokio::test]
#[serial_test::serial]
async fn a_phase_two_error_after_a_lost_child_write_is_tried_again() {
    let pool = crate::test_support::migrated_pool().await;
    let claimed = ClaimedTask {
        kind: TaskKind::WorkflowTask,
        ..claimed_now()
    };
    seed_running_workflow_task(&pool, claimed.task_id, WORKER, claimed.claimed_at).await;
    let refusal = refuse_node_updates(&pool, claimed.task_id, 2).await;

    let settled = writer(Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    )))
    .settle(&claimed, crashed())
    .await;
    let updates_seen = refusal.updates_seen(&pool).await;
    refusal.remove(&pool).await;

    assert!(matches!(settled, Settlement::Written(_)), "{:?}", settled);
    assert_eq!(
        history_row(&pool, claimed.task_id).await,
        ("FAILED".to_owned(), Some("WORKER_CRASHED".to_owned()))
    );
    assert!(
        updates_seen > 2,
        "phase 2 reached the node {updates_seen} time(s): two refused tries, then a try that passed"
    );
    assert_eq!(
        node_and_workflow(&pool, claimed.task_id).await,
        ("FAILED".to_owned(), "FAILED".to_owned())
    );
    assert_eq!(phase_two_records(&pool, claimed.task_id).await, 0);
}

/// Python `TestFinalizeAfterSlotOutcome::test_lost_child_is_recovered_and_retried_only_on_a_db_error`
/// (`NOT_OWNER_OR_NOT_CLAIMED`) and `TestRetryFinalizeFuture::test_requeue_not_owner_clears_future_attempts`:
/// a row that another worker holds is not this claim's; the write changes
/// nothing and is not tried again.
#[tokio::test]
#[serial_test::serial]
async fn a_crash_write_on_a_row_this_claim_does_not_hold_writes_nothing() {
    let pool = crate::test_support::migrated_pool().await;
    let claimed = claimed_now();
    seed_running(&pool, claimed.task_id, OTHER_WORKER, claimed.claimed_at).await;

    let settled = writer(Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    )))
    .settle(&claimed, crashed())
    .await;

    assert_eq!(settled, Settlement::NotHeld);
    assert_eq!(
        live_row(&pool, claimed.task_id).await,
        ("RUNNING".to_owned(), Some(OTHER_WORKER.to_owned()))
    );
}

/// Python `TestFinalizeAfterSlotOutcome::test_lost_child_is_recovered_and_retried_only_on_a_db_error`
/// (`DB_ERROR`), `TestHandleFinalizeError::test_retryable_under_max_increments_and_spawns`,
/// `::test_retryable_at_max_attempts_logs_critical_no_spawn`,
/// `::test_future_at_max_attempts_uses_future_limit`,
/// `::test_phase1_retryable_spawns_phase1_retry`,
/// `::test_future_retryable_spawns_future_retry` and
/// `TestRetryFinalizeFuture::test_requeue_db_error_delegates_to_handle_finalize_error`:
/// the write of a reported value and the write of a lost child, when they
/// cannot reach the database, are each sent again three times, after waits
/// of 0.5 s, 1 s and 2 s; then the end is left to recovery.
#[tokio::test]
async fn a_write_that_cannot_reach_the_database_is_sent_three_more_times() {
    let ends = [
        crashed(),
        AttemptEnd::Reported(RunEnd::Result(ResultEnvelopeText::new(
            r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned(),
        ))),
    ];
    let three_waits: f64 = (1..=3).map(phase_retry_delay).sum();
    let four_waits = three_waits + phase_retry_delay(4);
    for end in ends {
        let started = std::time::Instant::now();
        let settled = writer(unreachable_broker())
            .settle(&claimed_now(), end.clone())
            .await;
        let elapsed = started.elapsed();
        assert_eq!(settled, Settlement::RetriesExhausted, "{end:?}");
        assert!(
            elapsed >= Duration::from_secs_f64(three_waits),
            "{end:?}: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs_f64(four_waits),
            "{end:?}: {elapsed:?}"
        );
    }
}

/// Python `TestHandleFinalizeError::test_non_retryable_clears_attempts_no_spawn`:
/// a write error that cannot clear (the database does not exist) is not
/// sent again, and the end does not read as written.
#[tokio::test]
#[serial_test::serial]
async fn a_write_error_that_cannot_clear_is_not_sent_again() {
    let url = crate::test_support::migrated_database_url().await;
    let missing = crate::test_support::with_database(
        &url,
        &format!("horsies_missing_{}", Uuid::new_v4().simple()),
    );
    let pool = TestPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_lazy(&missing);
    let started = std::time::Instant::now();
    let settled = writer(Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    )))
    .settle(&claimed_now(), crashed())
    .await;
    assert!(!matches!(settled, Settlement::Written(_)), "{:?}", settled);
    assert!(
        started.elapsed() < Duration::from_secs_f64(phase_retry_delay(1)),
        "a retry wait passed: {:?}",
        started.elapsed()
    );
}

/// Python `TestHandleFinalizeError::test_phase2_at_max_attempts_uses_phase2_limit`
/// and `::test_phase2_retryable_spawns_phase2_retry`: phase 2 of a workflow
/// task that cannot reach the database is tried again five times, after
/// waits of 0.5 s to 8 s.
#[tokio::test]
async fn a_phase_two_that_cannot_reach_the_database_is_tried_five_more_times() {
    let broker = unreachable_broker();
    let started = std::time::Instant::now();
    run_phase2(
        &broker,
        &crate::test_support::child_specs::TestChildSpecs::empty(),
        Phase2Work {
            task_id: Uuid::new_v4(),
            log: crate::worker::executor::slots::AttemptLog::FIRST_UNTRACED,
            queue_name: "default".to_owned(),
            is_workflow_task: true,
            capacity_notified: true,
            phase_two: PhaseTwoForm::NotKnown,
        },
    )
    .await;
    let elapsed = started.elapsed();
    let five_waits: f64 = (1..=5).map(phase_retry_delay).sum();
    let six_waits = five_waits + phase_retry_delay(6);
    assert!(
        elapsed >= Duration::from_secs_f64(five_waits),
        "{elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs_f64(six_waits), "{elapsed:?}");
}

/// Python `TestHandleFinalizeError::test_backoff_delay_doubles_per_attempt`:
/// the wait before retry `n` is 0.5 s doubled per retry, at most 15 s.
#[test]
fn the_retry_wait_doubles_up_to_its_cap() {
    let waits: Vec<f64> = (1..=7).map(phase_retry_delay).collect();
    assert_eq!(waits, vec![0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 15.0]);
    assert_eq!(phase_retry_delay(1_000), 15.0);
}
