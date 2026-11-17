//! Ports of Python `tests/unit/test_shared_dispatch.py` and
//! `test_thread_dispatch.py` rows of the worker side of shared tasks: the
//! route of a shared row, the capacity of a worker with shared children,
//! and the write of each end of a run (`OutcomeWriter::settle`) on the
//! database.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use super::scripted::{ok_envelope, task, timeout_ms, WORKER_ID};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::worker::executor::protocol::{
    CancelGraceMs, FailureDetail, IsolatedWork, RunEnd, SharedWork, StopReason,
    StoredOptionsRefusal,
};
use crate::worker::retry::read_stored_options;

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// A `SHARED_ASYNC` row that the worker cannot run, and the refusal it gets.
struct Unusable {
    options: &'static str,
    refusal: fn(&StoredOptionsRefusal) -> bool,
}

fn unusable_shared_async_rows() -> Vec<Unusable> {
    vec![
        Unusable {
            options: r#"{"cancel_grace_ms": 700}"#,
            refusal: |refusal: &StoredOptionsRefusal| {
                matches!(refusal, StoredOptionsRefusal::TimeoutValueInvalid(reason)
                    if reason.as_str() == "stored timeout_ms is absent for execution SHARED_ASYNC")
            },
        },
        Unusable {
            options: "{not json",
            refusal: |refusal: &StoredOptionsRefusal| {
                matches!(refusal, StoredOptionsRefusal::OptionsUnreadable(_))
            },
        },
        Unusable {
            options: r#"{"timeout_ms": 5, "cancel_grace_ms": 700}"#,
            refusal: |refusal: &StoredOptionsRefusal| {
                matches!(refusal, StoredOptionsRefusal::TimeoutValueInvalid(reason)
                    if reason.as_str() == "stored timeout_ms is below 1000 ms")
            },
        },
        Unusable {
            options: r#"{"timeout_ms": 2000, "cancel_grace_ms": 1}"#,
            refusal: |refusal: &StoredOptionsRefusal| {
                matches!(refusal, StoredOptionsRefusal::CancelGraceInvalid(reason)
                    if reason.as_str() == "stored cancel_grace_ms is below 100 ms")
            },
        },
        Unusable {
            options: r#"{"timeout_ms": 2000, "cancel_grace_ms": "5"}"#,
            refusal: |refusal: &StoredOptionsRefusal| {
                matches!(refusal, StoredOptionsRefusal::CancelGraceInvalid(reason)
                    if reason.as_str() == "stored cancel_grace_ms is a JSON str, not an int")
            },
        },
        Unusable {
            options: r#"{"timeout_ms": 2000}"#,
            refusal: |refusal: &StoredOptionsRefusal| {
                matches!(refusal, StoredOptionsRefusal::CancelGraceInvalid(reason)
                    if reason.as_str() == "stored cancel_grace_ms is absent for execution SHARED_ASYNC")
            },
        },
    ]
}

/// Python `test_shared_dispatch.py::TestRoute::test_a_shared_row_with_no_usable_timeout_or_grace_is_refused_on_a_seat`
/// (both plans) and `TestDispatch::test_a_refused_shared_row_goes_to_a_seat_on_either_shape`
/// (the route half): the refusal runs on a seat, never on an isolated
/// child and never back to the queue.
#[test]
fn every_unusable_shared_async_row_is_refused_on_a_seat_on_either_plan() {
    for plan in [
        ClaimPlan::for_children(1, 1, 0),
        ClaimPlan::for_children(0, 1, 0),
    ] {
        for row in unusable_shared_async_rows() {
            match dispatch_route(
                ExecutionClass::SharedAsync,
                plan,
                &read_stored_options(Some(row.options)),
            ) {
                Ok(DispatchRoute::SharedAsync(SharedWork::Refusal(refusal))) => {
                    assert!((row.refusal)(&refusal), "{}: {refusal:?}", row.options);
                }
                other => panic!(
                    "{}: expected a refusal on a seat, got {other:?}",
                    row.options
                ),
            }
        }
    }
}

/// Python `test_shared_dispatch.py::TestRoute::test_a_worker_with_no_shared_children_refuses_it_in_an_isolated_child`
/// and `TestDispatch::test_a_worker_with_no_shared_children_sends_the_refusal_isolated`
/// (the route half).
#[test]
fn a_worker_with_only_isolated_children_refuses_an_unusable_shared_row_in_an_isolated_child() {
    let plan = ClaimPlan::for_children(1, 0, 0);
    for row in unusable_shared_async_rows() {
        match dispatch_route(
            ExecutionClass::SharedAsync,
            plan,
            &read_stored_options(Some(row.options)),
        ) {
            Ok(DispatchRoute::Isolated {
                work: IsolatedWork::Refusal(refusal),
                deadline: IsolatedDeadline::NoDeadline,
            }) => assert!((row.refusal)(&refusal), "{}: {refusal:?}", row.options),
            other => panic!(
                "{}: expected a refusal in an isolated child, got {other:?}",
                row.options
            ),
        }
    }
}

/// Python `test_shared_dispatch.py::TestCapacityAndConfig::test_a_worker_with_only_shared_children_is_valid`
/// and `test_thread_dispatch.py::TestCapacityAndConfig::test_a_worker_with_only_thread_children_is_valid`:
/// a worker with only shared children has one budget per kind, and its
/// seats are its capacity.
#[test]
fn a_worker_with_only_shared_children_has_their_seats_as_its_capacity() {
    let shared = |service, seats| SharedSlotService { service, seats };
    for (shared_async, shared_thread, budget) in [
        (2, 0, ExecutionClass::SharedAsync),
        (0, 2, ExecutionClass::SharedThread),
    ] {
        let slots = vec![shared(SlotService::InService, 5); 2];
        let pool = PoolService {
            isolated: Vec::new(),
            shared_async: match shared_async {
                0 => Vec::new(),
                _ => slots.clone(),
            },
            shared_thread: match shared_thread {
                0 => Vec::new(),
                _ => slots,
            },
        };
        assert_eq!(pool.plan().budgets(), vec![budget]);
        assert_eq!(
            pool.capacities(),
            vec![BudgetCapacity {
                budget,
                in_service: 10
            }]
        );
        assert_eq!(pool.total_capacity(), 10);
        assert_eq!(
            ClaimPlan::for_children(0, shared_async, shared_thread).route(ExecutionClass::Isolated),
            ClaimRoute::NotServed
        );
    }
}

// ---------------------------------------------------------------------------
// The writes of each end
// ---------------------------------------------------------------------------

/// How the seeded row is held by [`WORKER_ID`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    Claimed,
    Running,
}

/// A plain task row of `execution` on `queue`, held by [`WORKER_ID`].
/// Returns the id and the claim generation.
async fn seed(
    pool: &PgPool,
    queue: &str,
    execution: ExecutionClass,
    held: Held,
    task_options: Option<&str>,
    max_retries: i32,
) -> (Uuid, DateTime<Utc>) {
    let id = Uuid::new_v4();
    let statement = match held {
        Held::Running => {
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
                $1, 'app.tasks.scripted', $2, 100, '{}', $3, $4,
                'forever', date_trunc('milliseconds', NOW()) - interval '31 minutes',
                NOW() - interval '31 minutes',
                1, decode(repeat('05', 32), 'hex'), NULL,
                'RUNNING', NOW() - interval '31 minutes',
                date_trunc('milliseconds', NOW()) - interval '30 minutes', $5, NULL,
                NOW() - interval '30 minutes', NULL, NULL,
                4242, 'slots-host', 'slots-process',
                0, $6, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
             ) RETURNING claimed_at"
        }
        Held::Claimed => {
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
                $1, 'app.tasks.scripted', $2, 100, '{}', $3, $4,
                'forever', NOW(), NOW(),
                1, decode(repeat('05', 32), 'hex'), NULL,
                'CLAIMED', NOW(), date_trunc('milliseconds', NOW()), $5,
                NOW() + interval '1 minute',
                NULL, NULL, NULL,
                NULL, NULL, NULL,
                0, $6, NULL, NULL, FALSE,
                NOW(), FALSE, NULL, NULL
             ) RETURNING claimed_at"
        }
    };
    let generation = sqlx::query_scalar(statement)
        .bind(id)
        .bind(queue)
        .bind(task_options)
        .bind(execution.as_str())
        .bind(WORKER_ID)
        .bind(max_retries)
        .fetch_one(pool)
        .await
        .expect("seed a task row");
    (id, generation)
}

async fn live_row(pool: &PgPool, task_id: Uuid) -> Option<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live row")
}

async fn history_row(pool: &PgPool, task_id: Uuid) -> serde_json::Value {
    sqlx::query_scalar("SELECT to_jsonb(h) FROM horsies_task_history h WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the history row")
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
    format!("settle_{}", &Uuid::new_v4().simple().to_string()[..8])
}

fn writer(pool: PgPool) -> OutcomeWriter {
    writer_with_self_heal(pool, OrphanSelfHeal::On)
}

fn writer_with_self_heal(pool: PgPool, orphan_self_heal: OrphanSelfHeal) -> OutcomeWriter {
    let broker = Arc::new(PostgresBroker::from_pool(pool));
    let successes = super::outcome::test_successes(&broker, WORKER_ID);
    OutcomeWriter::new(
        broker,
        Arc::new(WorkflowSpecRegistry::new(
            crate::core::registry::BuilderHookName::SPEC_BUILDER,
        )),
        WORKER_ID.to_owned(),
        PayloadPolicy::default(),
        RetentionConfig::default(),
        successes,
        orphan_self_heal,
    )
}

/// The claimed task of a seeded row.
fn claimed_row(
    task_id: Uuid,
    queue: &str,
    generation: DateTime<Utc>,
    execution: ExecutionClass,
    options: Option<&str>,
    max_retries: i32,
) -> ClaimedTask {
    ClaimedTask {
        task_id,
        queue_name: queue.to_owned(),
        claimed_at: generation,
        execution,
        retry: RetryFacts {
            retry_count: 0,
            max_retries,
        },
        options: read_stored_options(options),
        ..task(1, execution)
    }
}

const RETRY_ON_TIMEOUT: &str = r#"{"timeout_ms": 1000, "cancel_grace_ms": 500, "retry_policy": {
    "max_retries": 2, "intervals": [5, 5], "backoff_strategy": "fixed", "jitter": false,
    "max_delay_seconds": null, "auto_retry_for": ["TASK_TIMEOUT"]}}"#;

/// Python `test_shared_dispatch.py::TestFinalizeShared::test_a_result_takes_the_path_of_an_isolated_result`,
/// `::test_the_result_of_a_refusal_takes_the_path_of_an_isolated_result` and
/// `TestFinalizeKilledRun::test_a_run_that_returned_before_the_kill_writes_its_result`:
/// a reported result of a shared run completes its row.
#[tokio::test]
#[serial]
async fn a_reported_result_of_a_shared_run_completes_the_row() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed(
        &pool,
        &queue,
        ExecutionClass::SharedAsync,
        Held::Running,
        None,
        0,
    )
    .await;
    let claimed = claimed_row(
        task_id,
        &queue,
        generation,
        ExecutionClass::SharedAsync,
        None,
        0,
    );

    let settled = writer(pool.clone())
        .settle(
            &claimed,
            AttemptEnd::Reported(RunEnd::Result(ok_envelope(7))),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    assert_eq!(history_row(&pool, task_id).await["status"], "COMPLETED");
    remove_task(&pool, task_id).await;
}

/// Python `test_shared_dispatch.py::TestFinalizeShared::test_a_reported_deadline_writes_the_timeout`
/// and `TestFinalizeKilledRun::test_a_run_past_its_deadline_writes_the_timeout`,
/// and `test_thread_dispatch.py::TestFinalizeThread::test_a_task_past_its_deadline_writes_the_timeout`:
/// the timeout retries by the policy at the placement of the end; an
/// isolated placement moves the row to the `ISOLATED` class.
#[tokio::test]
#[serial]
async fn a_timeout_retries_the_row_at_the_placement_of_its_end() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    for (execution, placement, cancel, class_after) in [
        (
            ExecutionClass::SharedAsync,
            RetryPlacement::Isolated,
            TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace),
            "ISOLATED",
        ),
        (
            ExecutionClass::SharedAsync,
            RetryPlacement::SameClass,
            TimeoutCancel::Ended(CancelEnd::ReturnedAfterSignal),
            "SHARED_ASYNC",
        ),
        (
            ExecutionClass::SharedThread,
            RetryPlacement::Isolated,
            TimeoutCancel::NoSignal,
            "ISOLATED",
        ),
    ] {
        let queue = test_queue();
        let (task_id, generation) = seed(
            &pool,
            &queue,
            execution,
            Held::Running,
            Some(RETRY_ON_TIMEOUT),
            2,
        )
        .await;
        let claimed = claimed_row(
            task_id,
            &queue,
            generation,
            execution,
            Some(RETRY_ON_TIMEOUT),
            2,
        );

        let settled = writer(pool.clone())
            .settle(
                &claimed,
                AttemptEnd::TimedOut {
                    timeout: timeout_ms(1_000),
                    retry: placement,
                    cancel,
                },
            )
            .await;

        assert_eq!(settled, Settlement::Written, "{execution} {placement:?}");
        let row = live_row(&pool, task_id)
            .await
            .expect("a retried row stays live");
        assert_eq!(row["status"], "PENDING", "{execution} {placement:?}");
        assert_eq!(row["execution"], class_after, "{execution} {placement:?}");
        assert_eq!(row["retry_count"], 1, "{execution} {placement:?}");
        remove_task(&pool, task_id).await;
    }
}

/// A timeout with no retry fails the row with `TASK_TIMEOUT`.
#[tokio::test]
#[serial]
async fn a_timeout_with_no_retry_fails_the_row_with_task_timeout() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed(
        &pool,
        &queue,
        ExecutionClass::SharedThread,
        Held::Running,
        None,
        0,
    )
    .await;
    let claimed = claimed_row(
        task_id,
        &queue,
        generation,
        ExecutionClass::SharedThread,
        None,
        0,
    );

    let settled = writer(pool.clone())
        .settle(
            &claimed,
            AttemptEnd::TimedOut {
                timeout: timeout_ms(1_000),
                retry: RetryPlacement::SameClass,
                cancel: TimeoutCancel::NoSignal,
            },
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let history = history_row(&pool, task_id).await;
    assert_eq!(history["status"], "FAILED");
    assert_eq!(history["error_code"], "TASK_TIMEOUT");
    remove_task(&pool, task_id).await;
}

/// Python `test_shared_dispatch.py::TestFinalizeKilledRun::test_the_run_that_blocked_the_loop_retries_isolated`
/// and `::test_a_neighbour_of_a_blocked_loop_gets_its_own_code`: each run
/// of a child killed for a blocked loop fails with its own code.
#[tokio::test]
#[serial]
async fn a_loop_kill_fails_the_row_with_its_code() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    for (code, retry, name) in [
        (
            LoopKillCode::TaskBlockedLoop,
            RetryPlacement::Isolated,
            "TASK_BLOCKED_LOOP",
        ),
        (
            LoopKillCode::NeighbourBlockedLoop,
            RetryPlacement::SameClass,
            "NEIGHBOUR_BLOCKED_LOOP",
        ),
    ] {
        let queue = test_queue();
        let (task_id, generation) = seed(
            &pool,
            &queue,
            ExecutionClass::SharedAsync,
            Held::Running,
            None,
            0,
        )
        .await;
        let claimed = claimed_row(
            task_id,
            &queue,
            generation,
            ExecutionClass::SharedAsync,
            None,
            0,
        );

        let settled = writer(pool.clone())
            .settle(
                &claimed,
                AttemptEnd::KilledForLoop {
                    code,
                    retry,
                    reason: FailureDetail::cut("app.py:7 in fetch"),
                },
            )
            .await;

        assert_eq!(settled, Settlement::Written, "{name}");
        let history = history_row(&pool, task_id).await;
        assert_eq!(history["status"], "FAILED", "{name}");
        assert_eq!(history["error_code"], name);
        remove_task(&pool, task_id).await;
    }
}

/// Python `test_shared_dispatch.py::TestFinalizeShared::test_a_task_that_reached_no_child_is_requeued`.
#[tokio::test]
#[serial]
async fn a_task_that_reached_no_child_goes_back_to_the_queue() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed(
        &pool,
        &queue,
        ExecutionClass::SharedAsync,
        Held::Claimed,
        None,
        0,
    )
    .await;
    let claimed = claimed_row(
        task_id,
        &queue,
        generation,
        ExecutionClass::SharedAsync,
        None,
        0,
    );

    let settled = writer(pool.clone())
        .settle(
            &claimed,
            AttemptEnd::NotDispatched(NotRunCause::PoolStopping),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let row = live_row(&pool, task_id).await.expect("the row stays live");
    assert_eq!(row["status"], "PENDING");
    assert_eq!(row["claimed_by_worker_id"], serde_json::Value::Null);
    remove_task(&pool, task_id).await;
}

/// Python `tests/integration/test_soft_cap_start_check.py::test_stop_returns_the_held_rows_to_their_queue`:
/// at the stop, a row that waited for its queue cap goes back to the queue
/// with the claim fence (`worker::worker::return_held_rows` settles each
/// held row so).
#[tokio::test]
#[serial]
async fn a_row_held_for_its_queue_cap_goes_back_to_the_queue_at_the_stop() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed(
        &pool,
        &queue,
        ExecutionClass::Isolated,
        Held::Claimed,
        None,
        0,
    )
    .await;
    let held = claimed_row(
        task_id,
        &queue,
        generation,
        ExecutionClass::Isolated,
        None,
        0,
    );

    let settled = writer(pool.clone())
        .settle(
            &held,
            AttemptEnd::NotDispatched(NotRunCause::HeldForQueueCap),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let row = live_row(&pool, task_id).await.expect("the row stays live");
    assert_eq!(row["status"], "PENDING");
    assert_eq!(row["claimed_by_worker_id"], serde_json::Value::Null);
    assert_eq!(row["execution"], "ISOLATED");
    remove_task(&pool, task_id).await;
}

/// The number of attempt rows of a task.
async fn attempt_count(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the attempt rows")
}

/// A child killed before it set its row RUNNING (each unreported end:
/// a crash, a loop kill, a timeout): the failed-attempt statement under the
/// claimed-row rule `RELEASE` puts the CLAIMED row back in the queue in its
/// class, with no attempt row (Python `_write_ended_attempt`, which sends
/// the release as its own statement).
#[tokio::test]
#[serial]
async fn an_unreported_end_of_a_row_still_claimed_puts_it_back_with_no_attempt_row() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let ends = [
        AttemptEnd::Crashed {
            reason: FailureDetail::cut("the child was lost"),
        },
        AttemptEnd::KilledForLoop {
            code: LoopKillCode::TaskBlockedLoop,
            retry: RetryPlacement::Isolated,
            reason: FailureDetail::cut("app.py:7 in fetch"),
        },
        AttemptEnd::TimedOut {
            timeout: timeout_ms(1_000),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::Ended(CancelEnd::KilledWithChild),
        },
    ];
    for end in ends {
        let queue = test_queue();
        let (task_id, generation) = seed(
            &pool,
            &queue,
            ExecutionClass::SharedAsync,
            Held::Claimed,
            Some(RETRY_ON_TIMEOUT),
            2,
        )
        .await;
        let claimed = claimed_row(
            task_id,
            &queue,
            generation,
            ExecutionClass::SharedAsync,
            Some(RETRY_ON_TIMEOUT),
            2,
        );
        let label = format!("{end:?}");

        let settled = writer(pool.clone()).settle(&claimed, end).await;

        assert_eq!(settled, Settlement::Written, "{label}");
        let row = live_row(&pool, task_id).await.expect("the row stays live");
        assert_eq!(row["status"], "PENDING", "{label}");
        assert_eq!(row["execution"], "SHARED_ASYNC", "{label}");
        assert_eq!(row["retry_count"], 0, "{label}");
        assert_eq!(
            row["claimed_by_worker_id"],
            serde_json::Value::Null,
            "{label}"
        );
        assert_eq!(attempt_count(&pool, task_id).await, 0, "{label}");
        remove_task(&pool, task_id).await;
    }
}

/// A task that reached no child, on a row that another claim generation
/// holds now: the release leaves the row as it is.
#[tokio::test]
#[serial]
async fn a_task_that_reached_no_child_leaves_a_row_of_another_claim_alone() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let (task_id, generation) = seed(
        &pool,
        &queue,
        ExecutionClass::Isolated,
        Held::Claimed,
        None,
        0,
    )
    .await;
    let earlier_claim = claimed_row(
        task_id,
        &queue,
        generation - chrono::Duration::seconds(5),
        ExecutionClass::Isolated,
        None,
        0,
    );
    let before = live_row(&pool, task_id).await;

    let settled = writer(pool.clone())
        .settle(
            &earlier_claim,
            AttemptEnd::NotDispatched(NotRunCause::QueueFull),
        )
        .await;

    assert_eq!(settled, Settlement::NotHeld);
    assert_eq!(live_row(&pool, task_id).await, before);
    remove_task(&pool, task_id).await;
}

// ---------------------------------------------------------------------------
// The retries of a write
// ---------------------------------------------------------------------------

/// A CLAIMED workflow task held by [`WORKER_ID`] with no workflow node
/// linked to it. Returns the claimed task. A workflow task row has no
/// command fingerprint (baseline `horsies_tasks_command_fingerprint_by_origin`).
async fn seed_unlinked_workflow_task(pool: &PgPool, queue: &str) -> ClaimedTask {
    let (task_id, generation) = seed(
        pool,
        queue,
        ExecutionClass::Isolated,
        Held::Claimed,
        None,
        0,
    )
    .await;
    sqlx::query(
        "UPDATE horsies_tasks
         SET is_workflow_task = TRUE,
             command_fingerprint = NULL,
             command_fingerprint_version = NULL
         WHERE id = $1",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .expect("mark the row a workflow task");
    ClaimedTask {
        kind: TaskKind::WorkflowTask,
        ..claimed_row(
            task_id,
            queue,
            generation,
            ExecutionClass::Isolated,
            None,
            0,
        )
    }
}

/// Python `test_lifecycle_classification.py::TestOrphanedWorkflowTask::test_self_heal_enabled_cancels_the_orphan`
/// and `::test_orphan_cancel_fences_on_the_dispatched_generation`: a start
/// that found no linked node cancels the owned orphan of this claim.
#[tokio::test]
#[serial]
async fn a_task_whose_node_is_not_linked_is_cancelled_when_the_self_heal_is_on() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let claimed = seed_unlinked_workflow_task(&pool, &queue).await;

    let settled = writer_with_self_heal(pool.clone(), OrphanSelfHeal::On)
        .settle(
            &claimed,
            AttemptEnd::Reported(RunEnd::StoppedBeforeRunning(StopReason::NodeNotLinked)),
        )
        .await;

    assert_eq!(settled, Settlement::Written);
    let history = history_row(&pool, claimed.task_id).await;
    assert_eq!(history["status"], "CANCELLED");
    assert_eq!(history["error_code"], "WORKFLOW_CHECK_FAILED");
    remove_task(&pool, claimed.task_id).await;
}

/// Python `test_lifecycle_classification.py::TestOrphanedWorkflowTask::test_self_heal_disabled_leaves_it_claimed`:
/// with the self-heal off the row stays CLAIMED by this claim.
#[tokio::test]
#[serial]
async fn a_task_whose_node_is_not_linked_stays_claimed_when_the_self_heal_is_off() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let queue = test_queue();
    let claimed = seed_unlinked_workflow_task(&pool, &queue).await;

    let settled = writer_with_self_heal(pool.clone(), OrphanSelfHeal::Off)
        .settle(
            &claimed,
            AttemptEnd::Reported(RunEnd::StoppedBeforeRunning(StopReason::NodeNotLinked)),
        )
        .await;

    assert_eq!(settled, Settlement::NothingOwed);
    let row = live_row(&pool, claimed.task_id)
        .await
        .expect("the row stays live");
    assert_eq!(row["status"], "CLAIMED");
    assert_eq!(row["claimed_by_worker_id"], WORKER_ID);
    remove_task(&pool, claimed.task_id).await;
}

/// No server listens on port 1, so every write fails with a retryable pool
/// timeout after 100 ms.
fn unreachable_writer() -> OutcomeWriter {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
        .expect("a lazy pool opens no connection");
    writer(pool)
}

/// The retry waits of one write stage: 0.5 s, 1 s and 2 s.
const ONE_STAGE_OF_WAITS: Duration = Duration::from_millis(3_500);

/// Python `test_shared_dispatch.py::TestFinalizeShared::test_a_failed_timeout_write_is_retried_as_the_timeout_write`,
/// `TestFinalizeKilledRun::test_a_database_error_of_the_write_is_retryable`,
/// `::test_the_retry_of_a_failed_kill_write_writes_the_same_verdict`,
/// `::test_a_crash_error_retries_as_a_crash`, `TestTimeoutWriteRetry::*`,
/// and `test_thread_dispatch.py::TestFinalizeThread::test_a_failed_timeout_write_after_the_kill_is_retried_as_that_write`:
/// a write that keeps failing is sent again up to its bound; a timeout
/// runs its crash recovery stage after the last retry of its own write, a
/// loop kill or a crash has one stage only. Each settles as spent retries.
#[tokio::test]
async fn a_failing_timeout_write_runs_crash_recovery_after_its_own_retries() {
    let claimed = task(1, ExecutionClass::SharedAsync);
    let writer = unreachable_writer();

    let started = Instant::now();
    let loop_kill = writer
        .settle(
            &claimed,
            AttemptEnd::KilledForLoop {
                code: LoopKillCode::TaskBlockedLoop,
                retry: RetryPlacement::Isolated,
                reason: FailureDetail::cut("app.py:7 in fetch"),
            },
        )
        .await;
    let one_stage = started.elapsed();
    let started = Instant::now();
    let timed_out = writer
        .settle(
            &claimed,
            AttemptEnd::TimedOut {
                timeout: timeout_ms(1_000),
                retry: RetryPlacement::Isolated,
                cancel: TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace),
            },
        )
        .await;
    let two_stages = started.elapsed();

    assert_eq!(loop_kill, Settlement::RetriesExhausted);
    assert_eq!(timed_out, Settlement::RetriesExhausted);
    assert!(one_stage >= ONE_STAGE_OF_WAITS, "{one_stage:?}");
    assert!(one_stage < 2 * ONE_STAGE_OF_WAITS, "{one_stage:?}");
    assert!(two_stages >= 2 * ONE_STAGE_OF_WAITS, "{two_stages:?}");
}

/// The work of a shared async run with its stored timeout and grace, as
/// the route reads it (Python `TestRoute::test_a_row_of_the_shared_async_budget_runs_on_a_seat`
/// with the grace of `SHARED_OPTIONS`).
#[test]
fn a_shared_async_row_runs_on_a_seat_with_its_stored_grace() {
    assert_eq!(
        dispatch_route(
            ExecutionClass::SharedAsync,
            ClaimPlan::for_children(1, 1, 0),
            &read_stored_options(Some(r#"{"timeout_ms": 2000, "cancel_grace_ms": 700}"#)),
        ),
        Ok(DispatchRoute::SharedAsync(SharedWork::AsyncTask {
            timeout_ms: timeout_ms(2_000),
            cancel_grace_ms: CancelGraceMs::new(700).unwrap(),
        }))
    );
}

/// Python `test_thread_dispatch.py::TestRoute::test_a_row_of_the_thread_budget_runs_on_a_thread_seat`,
/// `::test_a_thread_row_with_no_usable_timeout_is_refused_on_a_seat`,
/// `::test_a_worker_with_no_thread_children_runs_it_isolated` and
/// `::test_an_isolated_row_is_not_served_by_thread_children_only`.
#[test]
fn a_shared_thread_row_runs_on_a_thread_seat_or_is_refused_there() {
    let with_threads = ClaimPlan::for_children(1, 0, 1);
    let timed = read_stored_options(Some(r#"{"timeout_ms": 2000}"#));
    assert_eq!(
        dispatch_route(ExecutionClass::SharedThread, with_threads, &timed),
        Ok(DispatchRoute::SharedThread(SharedWork::ThreadTask {
            timeout_ms: timeout_ms(2_000),
        }))
    );
    for plan in [with_threads, ClaimPlan::for_children(0, 0, 1)] {
        match dispatch_route(
            ExecutionClass::SharedThread,
            plan,
            &read_stored_options(Some("{}")),
        ) {
            Ok(DispatchRoute::SharedThread(SharedWork::Refusal(
                StoredOptionsRefusal::TimeoutValueInvalid(reason),
            ))) => assert_eq!(
                reason.as_str(),
                "stored timeout_ms is absent for execution SHARED_THREAD"
            ),
            other => panic!("expected a refusal on a thread seat, got {other:?}"),
        }
    }
    assert_eq!(
        dispatch_route(
            ExecutionClass::SharedThread,
            ClaimPlan::for_children(1, 0, 0),
            &timed
        ),
        Ok(DispatchRoute::Isolated {
            work: IsolatedWork::Run,
            deadline: IsolatedDeadline::After(timeout_ms(2_000)),
        })
    );
    assert_eq!(
        dispatch_route(
            ExecutionClass::Isolated,
            ClaimPlan::for_children(0, 0, 1),
            &timed
        ),
        Err(UnservedClass {
            class: ExecutionClass::Isolated
        })
    );
}

/// Python `test_thread_dispatch.py::TestRequest::test_a_timeout_out_of_bounds_is_a_defect`:
/// the bound is in the type of the timeout, so a run cannot carry one out
/// of bounds.
#[test]
fn a_seat_timeout_out_of_its_bounds_cannot_be_built() {
    assert!(crate::worker::executor::protocol::TaskTimeoutMs::new(999).is_none());
    assert!(crate::worker::executor::protocol::TaskTimeoutMs::new(1_000).is_some());
    assert!(
        crate::worker::executor::protocol::TaskTimeoutMs::new(u64::from(u32::MAX) + 1).is_none()
    );
}
