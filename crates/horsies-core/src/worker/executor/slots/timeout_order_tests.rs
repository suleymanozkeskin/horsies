//! Ports of Python `tests/unit/test_task_timeout_order.py` and
//! `test_isolated_left_work.py` rows: the timeout write of a row in each
//! state, the bound of the wait for a killed child, and the end of an
//! isolated task whose body returned.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::isolated::ISOLATED_LEFTOVER_BACKSTOP;
use super::scripted::{
    answer_of, boot, isolated_config, isolated_init, ok_envelope, read_frame, reported, send,
    shell, start_isolated, submit_isolated, task, timeout_ms, SPAWNER_STOP_WAIT, WORKER_ID,
};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::worker::executor::protocol::{
    BodyEnd, BodyReturnedFrame, ChildFrame, LeftoverName, LoopHoldsTasks,
};
use crate::worker::executor::supervisor::{Spawner, CHILD_KILL_WAIT};
use crate::worker::retry::read_stored_options;

// ---------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------

/// Python `test_task_timeout_order.py::TestTheKillComesFirst::test_the_wait_for_the_exit_has_the_kill_bound`:
/// after the kill at a deadline, the slot waits at most 30 s for the exit
/// (`slots/isolated.rs`, `end_after_kill`).
#[test]
fn the_wait_for_the_exit_of_a_killed_child_has_the_kill_bound() {
    assert_eq!(CHILD_KILL_WAIT, Duration::from_secs(30));
}

/// Python `test_isolated_left_work.py::TestMarkerBounds::test_the_parent_waits_past_the_child_bound_after_a_return`:
/// a child whose loop turns reports the end of the left work before the
/// parent kills it for reporting nothing. The child's bound (Python
/// `child_runner.py`, `task_loop.py`): 1 s of grace, three cancel rounds
/// of 0.1 s, the 5 s connection wait and the 5 s statements of the marker,
/// and the 5 s cancel request.
#[test]
fn the_parent_waits_past_the_child_bound_after_a_return() {
    let child_bound = Duration::from_millis(1_000 + 3 * 100 + 5_000 + 5_000 + 5_000);
    assert!(ISOLATED_LEFTOVER_BACKSTOP > child_bound);
}

// ---------------------------------------------------------------------------
// A second return
// ---------------------------------------------------------------------------

/// Python `test_isolated_left_work.py::TestHandle::test_a_second_return_is_a_defect`:
/// a second return report of one task is out of turn; the child is
/// killed, and the task ends with what its body returned first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_return_report_kills_the_child_and_keeps_the_first_return() {
    let first = task(1, ExecutionClass::Isolated);
    let returned = |value| {
        ChildFrame::BodyReturned(BodyReturnedFrame {
            task_id: first.task_id,
            returned: BodyEnd::Result(ok_envelope(value)),
            left: LoopHoldsTasks::new(
                std::num::NonZeroU64::MIN,
                vec![LeftoverName::cut("_write_after")],
            )
            .unwrap(),
        })
    };
    let script = format!(
        "{}{}{}exec sleep 60",
        boot(&isolated_init()),
        read_frame(),
        send(&[returned(1), returned(2)]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(answer, reported(1));
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// The timeout write of a row in each state
// ---------------------------------------------------------------------------

/// The state of a seeded row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowState {
    /// RUNNING, held by `worker`.
    Running,
    /// CLAIMED, held by `worker`, not started.
    Claimed,
    /// PENDING, held by no one.
    Pending,
}

const SEED_COLUMNS: &str = "id, task_name, queue_name, priority, input, task_options, execution,
    retention_class_key, sent_at, created_at,
    command_fingerprint_version, command_fingerprint, idempotency_key_digest,
    status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
    started_at, finalizing_at, finalizing_by_worker_id,
    worker_pid, worker_hostname, worker_process_name,
    retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
    updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id";

/// A plain ISOLATED row in `state`, held by `worker` when it is held.
/// Returns the id and the claim generation (now for a PENDING row).
async fn seed(pool: &PgPool, queue: &str, state: RowState, worker: &str) -> (Uuid, DateTime<Utc>) {
    let id = Uuid::new_v4();
    let values = match state {
        RowState::Running => {
            "$1, 'app.tasks.scripted', $2, 100, '{}', NULL, 'ISOLATED',
             'forever', date_trunc('milliseconds', NOW()) - interval '31 minutes',
             NOW() - interval '31 minutes',
             1, decode(repeat('05', 32), 'hex'), NULL,
             'RUNNING', NOW() - interval '31 minutes',
             date_trunc('milliseconds', NOW()) - interval '30 minutes', $3, NULL,
             NOW() - interval '30 minutes', NULL, NULL,
             4242, 'slots-host', 'slots-process',
             0, 0, NULL, NULL, FALSE,
             NOW(), FALSE, NULL, NULL"
        }
        RowState::Claimed => {
            "$1, 'app.tasks.scripted', $2, 100, '{}', NULL, 'ISOLATED',
             'forever', NOW(), NOW(),
             1, decode(repeat('05', 32), 'hex'), NULL,
             'CLAIMED', NOW(), date_trunc('milliseconds', NOW()), $3,
             NOW() + interval '1 minute',
             NULL, NULL, NULL,
             NULL, NULL, NULL,
             0, 0, NULL, NULL, FALSE,
             NOW(), FALSE, NULL, NULL"
        }
        RowState::Pending => {
            "$1, 'app.tasks.scripted', $2, 100, '{}', NULL, 'ISOLATED',
             'forever', NOW(), NOW(),
             1, decode(repeat('05', 32), 'hex'), NULL,
             'PENDING', NOW(), NULL, NULL, NULL,
             NULL, NULL, NULL,
             NULL, NULL, NULL,
             0, 0, NULL, NULL, FALSE,
             NOW(), FALSE, NULL, NULL"
        }
    };
    let statement = format!(
        "INSERT INTO horsies_tasks ({SEED_COLUMNS}) VALUES ({values})
         RETURNING COALESCE(claimed_at, date_trunc('milliseconds', NOW()))"
    );
    let query = sqlx::query_scalar(&statement).bind(id).bind(queue);
    let query = match state {
        RowState::Running | RowState::Claimed => query.bind(worker),
        RowState::Pending => query,
    };
    let generation = query.fetch_one(pool).await.expect("seed a task row");
    (id, generation)
}

async fn live_row(pool: &PgPool, task_id: Uuid) -> Option<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live row")
}

async fn remove_task(pool: &PgPool, task_id: Uuid) {
    for statement in [
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

fn test_queue() -> String {
    format!("timeout_{}", &Uuid::new_v4().simple().to_string()[..8])
}

fn writer(pool: PgPool) -> OutcomeWriter {
    let broker = Arc::new(PostgresBroker::from_pool(pool));
    let successes = super::outcome::test_successes(&broker, WORKER_ID);
    OutcomeWriter::new(
        broker,
        crate::workflow_engine::child_spec::ChildSpecs::Registry(Arc::new(
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
        )),
        WORKER_ID.to_owned(),
        PayloadPolicy::default(),
        RetentionConfig::default(),
        successes,
    )
}

fn claimed(task_id: Uuid, queue: &str, generation: DateTime<Utc>) -> ClaimedTask {
    ClaimedTask {
        task_id,
        queue_name: queue.to_owned(),
        claimed_at: generation,
        options: read_stored_options(None),
        ..task(1, ExecutionClass::Isolated)
    }
}

fn timed_out() -> AttemptEnd {
    AttemptEnd::TimedOut {
        timeout: timeout_ms(1_000),
        retry: RetryPlacement::SameClass,
        cancel: TimeoutCancel::NoSignal,
    }
}

/// Python `test_task_timeout_order.py::TestTheKillComesFirst::test_a_plain_task_failure_runs_no_phase_2`:
/// the timeout of a plain task fails its row and owes no phase 2.
#[tokio::test]
#[serial]
async fn a_timeout_of_a_plain_task_owes_no_phase_two() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed(&pool, &queue, RowState::Running, WORKER_ID).await;

    let settled = writer(pool.clone())
        .settle(&claimed(task_id, &queue, generation), timed_out())
        .await;

    assert_eq!(settled, Settlement::Written);
    assert_eq!(live_row(&pool, task_id).await, None);
    let pending_phase_two: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("read the phase-2 records");
    assert_eq!(pending_phase_two, 0);
    remove_task(&pool, task_id).await;
}

/// Python `test_task_timeout_order.py::TestTheKillComesFirst::test_a_claimed_row_is_requeued_only_after_the_exit`
/// (the write; the order after the exit is the slot's): a row that its
/// child never set RUNNING goes back to the queue.
#[tokio::test]
#[serial]
async fn a_timeout_of_a_row_still_claimed_puts_it_back_in_the_queue() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed(&pool, &queue, RowState::Claimed, WORKER_ID).await;

    let settled = writer(pool.clone())
        .settle(&claimed(task_id, &queue, generation), timed_out())
        .await;

    assert_eq!(settled, Settlement::Written);
    let row = live_row(&pool, task_id).await.expect("the row stays live");
    assert_eq!(row["status"], "PENDING");
    assert_eq!(row["claimed_by_worker_id"], serde_json::Value::Null);
    remove_task(&pool, task_id).await;
}

/// Python `test_task_timeout_order.py::TestTimeoutWriteOutcomes::test_a_row_another_actor_holds_is_left_alone`
/// and `::test_a_status_outside_the_locked_read_is_a_failed_write`: the
/// timeout write of a row that this claim does not hold changes nothing.
/// (Python reports a row in another status as a failed write; the core
/// reads both as a row it does not hold.)
#[tokio::test]
#[serial]
async fn a_timeout_of_a_row_this_claim_does_not_hold_changes_nothing() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    for (state, holder) in [
        (RowState::Running, "another-worker"),
        (RowState::Pending, WORKER_ID),
    ] {
        let queue = test_queue();
        let (task_id, generation) = seed(&pool, &queue, state, holder).await;
        let before = live_row(&pool, task_id).await;

        let settled = writer(pool.clone())
            .settle(&claimed(task_id, &queue, generation), timed_out())
            .await;

        assert_eq!(settled, Settlement::NotHeld, "{state:?}");
        assert_eq!(live_row(&pool, task_id).await, before, "{state:?}");
        remove_task(&pool, task_id).await;
    }
}

/// A RUNNING ISOLATED workflow task on `queue`, held by [`WORKER_ID`],
/// linked to the RUNNING node of a RUNNING one-node workflow whose
/// `on_error` is `fail`. Returns the task id, its claim generation and the
/// workflow id.
async fn seed_workflow_task(pool: &PgPool, queue: &str) -> (Uuid, DateTime<Utc>, Uuid) {
    let task_id = Uuid::new_v4();
    let statement = format!(
        "INSERT INTO horsies_tasks ({SEED_COLUMNS}) VALUES (
            $1, 'app.tasks.scripted', $2, 100, '{{}}', NULL, 'ISOLATED',
            'forever', date_trunc('milliseconds', NOW()) - interval '31 minutes',
            NOW() - interval '31 minutes',
            NULL, NULL, NULL,
            'RUNNING', NOW() - interval '31 minutes',
            date_trunc('milliseconds', NOW()) - interval '30 minutes', $3, NULL,
            NOW() - interval '30 minutes', NULL, NULL,
            4242, 'slots-host', 'slots-process',
            0, 0, NULL, NULL, TRUE,
            NOW(), FALSE, NULL, NULL
         ) RETURNING claimed_at"
    );
    let generation = sqlx::query_scalar(&statement)
        .bind(task_id)
        .bind(queue)
        .bind(WORKER_ID)
        .fetch_one(pool)
        .await
        .expect("seed a workflow task row");
    let workflow_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'timeout_order_workflow', NULL, 'RUNNING', 'fail', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("seed a workflow");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow,
             id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, allow_failed_deps, join_type, max_retries, execution,
             retention_class_key, status, task_id, is_subworkflow, created_at, started_at
         ) VALUES ('NONE', $1, $2, 0, 'only', 'app.tasks.scripted', '{}', $3, 100,
                   '{}', FALSE, 'all', 0, 'ISOLATED',
                   'forever', 'RUNNING', $4, FALSE, NOW(), NOW())",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(queue)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed a workflow node");
    (task_id, generation, workflow_id)
}

async fn text_of(pool: &PgPool, statement: &str, id: Uuid) -> String {
    sqlx::query_scalar(statement)
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read one text")
}

/// Python `test_task_timeout_order.py::TestTheKillComesFirst::test_a_workflow_task_fails_then_phase_2_runs_after_the_exit`
/// (the write, on the database): the timeout of a workflow task fails its
/// row with `TASK_TIMEOUT` and no retry, then phase 2 runs: it consumes the
/// phase-2 record, fails the node, and fails the one-node workflow.
#[tokio::test]
#[serial]
async fn a_timeout_of_a_workflow_task_fails_it_then_phase_two_runs() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation, workflow_id) = seed_workflow_task(&pool, &queue).await;
    let task = ClaimedTask {
        kind: TaskKind::WorkflowTask,
        ..claimed(task_id, &queue, generation)
    };

    let settled = writer(pool.clone()).settle(&task, timed_out()).await;

    assert_eq!(settled, Settlement::Written);
    assert_eq!(live_row(&pool, task_id).await, None);
    let history: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(h) FROM horsies_task_history h WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the history row");
    assert_eq!(history["status"], "FAILED");
    assert_eq!(history["error_code"], "TASK_TIMEOUT");
    assert_eq!(history["retry_count"], 0);
    let pending_phase_two: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("read the phase-2 records");
    assert_eq!(pending_phase_two, 0, "phase 2 consumed its record");
    assert_eq!(
        text_of(
            &pool,
            "SELECT status::text FROM horsies_workflow_tasks WHERE task_id = $1",
            task_id
        )
        .await,
        "FAILED"
    );
    assert_eq!(
        text_of(
            &pool,
            "SELECT status::text FROM horsies_workflows WHERE id = $1",
            workflow_id
        )
        .await,
        "FAILED"
    );
    sqlx::query("DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("remove the phase-2 record");
    remove_task(&pool, task_id).await;
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("remove the workflow");
}
