//! The task advisory lock goes before the row lock, on every path.
//!
//! A transaction that takes the row lock first and the advisory lock later
//! can deadlock against one that took the advisory lock first (SQLSTATE
//! 40P01). Python pauses one side between its two locks with a gate in its
//! client code. The core sides are single statements, so a holder
//! transaction plays the paused side: it takes the advisory lock, the other
//! side starts and waits, and the holder then runs its move and commits.

use std::time::Duration as StdDuration;

use chrono::Duration;
use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, FinishWorkflowSuccess, RetryRequest,
};
use crate::broker::claims::{start_attempt, StartAttempt, StartMiss, StartOutcome};
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_url;
use crate::broker::postgres::PostgresBroker;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::lifecycle::{
    AttemptWriteRefusal, FailedAttemptOutcome, GuardEvidence, OwnedClaim, TerminalizationCommand,
    TerminalizationKind, TerminalizationOutcome, WorkerOwned, WorkflowSuccessOutcome,
};
use crate::monitoring::cancel_task;
use crate::test_support::sql::{self, TestConnection, TestPool};
use crate::test_support::terminal_writes::{self, StaleFailure};

const TASK_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1::text, 731))";
const RESULT: &str = r#"{"err": "boom"}"#;
/// Bound of a side that must end once the holder commits.
const SIDE_WAIT: StdDuration = StdDuration::from_secs(10);
/// Bound of the wait for a backend to block on the holder: 100 polls of 10 ms.
const BLOCK_POLLS: u32 = 100;
const BLOCK_POLL_INTERVAL: StdDuration = StdDuration::from_millis(10);
/// Rounds of the free race.
const RACE_ROUNDS: u32 = 10;

/// Whether some backend waits on a lock that `holder_pid` holds.
async fn a_backend_waits_on(pool: &TestPool, holder_pid: i32) -> bool {
    for _ in 0..BLOCK_POLLS {
        let waiting: bool = sql::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))
             )",
        )
        .bind(holder_pid)
        .fetch_one(pool)
        .await
        .expect("read blocking pids");
        if waiting {
            return true;
        }
        tokio::time::sleep(BLOCK_POLL_INTERVAL).await;
    }
    false
}

/// A silent RUNNING task of `WORKER`, stale for the stale move.
async fn stale_task(pool: &TestPool, origin: Origin) -> (Uuid, Option<Uuid>) {
    let task_id = seed_task(
        pool,
        &TaskSeed {
            origin,
            ..TaskSeed::running()
        },
    )
    .await;
    set_started(pool, task_id, Since::SecondsAgo(120.0)).await;
    let workflow_id = match origin {
        Origin::WorkflowNode => Some(link_workflow_task(pool, task_id, "RUNNING", "RUNNING").await),
        Origin::Send => None,
    };
    (task_id, workflow_id)
}

fn failure(task_id: Uuid) -> FinishFailedAttempt {
    FinishFailedAttempt {
        task_id,
        fence: owned_claim(),
        retry: RetryRequest::NoRetry,
        result_json: RESULT.to_owned(),
        error_code: Some("BOOM".to_owned()),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some("boom".to_owned()),
            failed_reason: None,
        },
    }
}

fn success(task_id: Uuid) -> FinishWorkflowSuccess {
    FinishWorkflowSuccess {
        task_id,
        fence: owned_claim(),
        result_json: r#"{"ok": 1}"#.to_owned(),
    }
}

/// The failure of the stale recovery (`horsies_finish_stale_attempt`, no
/// retry), as Python's `_STALE_FINISH_SQL`.
fn stale_failure() -> StaleFailure {
    StaleFailure {
        stale_after_ms: 60_000,
        finalizing_stale_after_ms: 30_000,
        result_json: RESULT.to_owned(),
        error_code: "WORKER_CRASHED".to_owned(),
        failed_reason: "went silent".to_owned(),
    }
}

/// The stale move of the holder, in the holder's transaction.
async fn stale_move_in(
    connection: &mut TestConnection,
    task_id: Uuid,
) -> Vec<TerminalizationOutcome> {
    terminal_writes::fail_stale_task(connection, task_id, &stale_failure())
        .await
        .expect("stale move")
}

async fn history_kinds(pool: &TestPool, task_id: Uuid) -> Vec<(String, String)> {
    sql::query_as(
        "SELECT status::text, terminalization_kind::text FROM horsies_task_history
         WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await
    .expect("read history kinds")
}

fn foreign_kind_of_failure(outcome: &FailedAttemptOutcome) -> Option<TerminalizationKind> {
    match outcome {
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(foreign),
            ..
        }) => Some(foreign.committed_kind),
        FailedAttemptOutcome::FailureApplied { .. }
        | FailedAttemptOutcome::RetryScheduled { .. }
        | FailedAttemptOutcome::Refused(_) => None,
    }
}

async fn cleanup(pool: &TestPool, task_id: Uuid, workflow_id: Option<Uuid>) {
    cleanup_tasks(pool, &[task_id]).await;
    if let Some(workflow_id) = workflow_id {
        cleanup_workflows(pool, &[workflow_id]).await;
    }
}

/// Python `test_a_failure_finalize_waits_for_a_stale_recovery_that_holds_its_locks`.
#[tokio::test]
#[serial]
async fn a_failure_finalize_waits_for_a_stale_recovery_that_holds_its_locks() {
    let pool = pool().await;
    let (task_id, workflow_id) = stale_task(&pool, Origin::Send).await;
    let mut recovery = pool.begin().await.expect("begin the recovery");
    let recovery_pid = backend_pid(&mut *recovery).await;
    sql::query(TASK_LOCK_SQL)
        .bind(task_id)
        .execute(&mut *recovery)
        .await
        .expect("take the task lock");

    let finalize_pool = pool.clone();
    let finalize =
        tokio::spawn(async move { write_failed_attempt(&finalize_pool, &failure(task_id)).await });
    let waited = a_backend_waits_on(&pool, recovery_pid).await;
    let moved = stale_move_in(&mut *recovery, task_id).await;
    recovery.commit().await.expect("commit the recovery");
    let finalized = tokio::time::timeout(SIDE_WAIT, finalize)
        .await
        .expect("the finalize ends")
        .expect("join the finalize")
        .expect("failure write");

    assert!(waited, "the finalize never waited on the recovery");
    assert!(
        matches!(moved.as_slice(), [TerminalizationOutcome::Applied { .. }]),
        "{moved:?}"
    );
    assert_eq!(
        foreign_kind_of_failure(&finalized),
        Some(TerminalizationKind::FailStale),
        "{finalized:?}"
    );
    assert_eq!(
        history_kinds(&pool, task_id).await,
        vec![("FAILED".to_owned(), "FAIL_STALE".to_owned())]
    );
    cleanup(&pool, task_id, workflow_id).await;
}

/// Python `test_a_workflow_success_waits_for_a_stale_recovery_that_holds_its_locks`.
#[tokio::test]
#[serial]
async fn a_workflow_success_waits_for_a_stale_recovery_that_holds_its_locks() {
    let pool = pool().await;
    let (task_id, workflow_id) = stale_task(&pool, Origin::WorkflowNode).await;
    let mut recovery = pool.begin().await.expect("begin the recovery");
    let recovery_pid = backend_pid(&mut *recovery).await;
    sql::query(TASK_LOCK_SQL)
        .bind(task_id)
        .execute(&mut *recovery)
        .await
        .expect("take the task lock");

    let finalize_pool = pool.clone();
    let finalize =
        tokio::spawn(
            async move { write_workflow_success(&finalize_pool, &success(task_id)).await },
        );
    let waited = a_backend_waits_on(&pool, recovery_pid).await;
    let moved = stale_move_in(&mut *recovery, task_id).await;
    recovery.commit().await.expect("commit the recovery");
    let finalized = tokio::time::timeout(SIDE_WAIT, finalize)
        .await
        .expect("the success ends")
        .expect("join the success")
        .expect("workflow success");

    assert!(waited, "the success never waited on the recovery");
    assert!(
        matches!(moved.as_slice(), [TerminalizationOutcome::Applied { .. }]),
        "{moved:?}"
    );
    assert!(
        matches!(
            &finalized,
            WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
                evidence: GuardEvidence::ForeignTerminalization(foreign),
                ..
            }) if foreign.committed_kind == TerminalizationKind::FailStale
        ),
        "{finalized:?}"
    );
    assert_eq!(
        history_kinds(&pool, task_id).await,
        vec![("FAILED".to_owned(), "FAIL_STALE".to_owned())]
    );
    cleanup(&pool, task_id, workflow_id).await;
}

/// Python `test_the_stale_move_waits_for_a_workflow_success_that_holds_its_locks`:
/// the success runs in a transaction that stays open and holds both locks;
/// the stale move waits, and finds the row moved after the commit.
#[tokio::test]
#[serial]
async fn the_stale_move_waits_for_a_workflow_success_that_holds_its_locks() {
    let pool = pool().await;
    let (task_id, workflow_id) = stale_task(&pool, Origin::WorkflowNode).await;
    let mut holder = pool.begin().await.expect("begin the success");
    let holder_pid = backend_pid(&mut *holder).await;
    let applied: String =
        sql::query_scalar("SELECT outcome FROM horsies_finish_workflow_success($1, $2, $3, $4)")
            .bind(task_id)
            .bind(WORKER)
            .bind(generation())
            .bind(r#"{"ok": 1}"#)
            .fetch_one(&mut *holder)
            .await
            .expect("workflow success in an open transaction");

    let stale_pool = pool.clone();
    let stale_move = tokio::spawn(async move {
        terminal_writes::fail_stale_task(&stale_pool, task_id, &stale_failure()).await
    });
    let waited = a_backend_waits_on(&pool, holder_pid).await;
    holder.commit().await.expect("commit the success");
    let moved = tokio::time::timeout(SIDE_WAIT, stale_move)
        .await
        .expect("the stale move ends")
        .expect("join the stale move")
        .expect("stale move");

    assert_eq!(applied, "APPLIED");
    assert!(waited, "the stale move never waited on the success");
    assert!(
        matches!(
            moved.as_slice(),
            [TerminalizationOutcome::SourceStateConflict {
                evidence: GuardEvidence::ForeignTerminalization(foreign),
                ..
            }] if foreign.committed_kind == TerminalizationKind::CompleteLocked
        ),
        "{moved:?}"
    );
    assert_eq!(
        history_kinds(&pool, task_id).await,
        vec![("COMPLETED".to_owned(), "COMPLETE_LOCKED".to_owned())]
    );
    cleanup(&pool, task_id, workflow_id).await;
}

/// Python `test_a_failure_finalize_waits_for_a_monitoring_cancel_that_holds_its_locks`,
/// with the roles turned: the core's monitoring cancel is one call, so the
/// holder plays the failure write. The cancel waits on the task lock while it
/// holds no row lock, and no deadlock occurs.
#[tokio::test]
#[serial]
async fn a_failure_finalize_waits_for_a_monitoring_cancel_that_holds_its_locks() {
    let pool = pool().await;
    let (task_id, workflow_id) = stale_task(&pool, Origin::Send).await;
    let mut finalizer = pool.begin().await.expect("begin the failure write");
    let finalizer_pid = backend_pid(&mut *finalizer).await;
    sql::query(TASK_LOCK_SQL)
        .bind(task_id)
        .execute(&mut *finalizer)
        .await
        .expect("take the task lock");

    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let cancel = tokio::spawn(async move { cancel_task(&broker, task_id, true).await });
    let waited = a_backend_waits_on(&pool, finalizer_pid).await;
    let mut probe = pool.begin().await.expect("begin the probe");
    let row_free = sql::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE NOWAIT")
        .bind(task_id)
        .execute(&mut *probe)
        .await
        .is_ok();
    probe.rollback().await.expect("release the probe");
    // The baseline argument list: no retry, the REFUSE rule (the child
    // reported a result) and the capacity wake of a plain task.
    let applied: String = sql::query_scalar(
        "SELECT outcome FROM horsies_finish_failed_attempt($1, $2, $3, 'NONE', NULL, NULL, NULL,
                $4, 'BOOM', NULL, 'FAILED', 'boom', NULL, 'REFUSE', TRUE)",
    )
    .bind(task_id)
    .bind(WORKER)
    .bind(generation())
    .bind(RESULT)
    .fetch_one(&mut *finalizer)
    .await
    .expect("failure write in the holder");
    finalizer.commit().await.expect("commit the failure write");
    let cancelled = tokio::time::timeout(SIDE_WAIT, cancel)
        .await
        .expect("the cancel ends")
        .expect("join the cancel");

    assert!(waited, "the cancel never waited on the task lock");
    assert!(row_free, "the waiting cancel held the task row");
    assert_eq!(applied, "APPLIED");
    assert!(
        cancelled.is_err(),
        "the cancel found a FAILED task: {cancelled:?}"
    );
    assert_eq!(
        history_kinds(&pool, task_id).await,
        vec![("FAILED".to_owned(), "FAIL_RUNNING".to_owned())]
    );
    cleanup(&pool, task_id, workflow_id).await;
}

/// Python `test_the_row_lock_of_an_isolated_start_ends_before_its_miss_path`
/// and `test_the_row_lock_of_a_shared_async_start_ends_before_its_miss_path`
/// (the core has one start for every class, `horsies_start_attempt`): a
/// start of an older generation misses with `NOT_HELD`, takes no task lock
/// and holds no row lock after it returns, so the holder of the task lock can
/// lock the row, and the miss path then waits for the task lock.
#[tokio::test]
#[serial]
async fn the_row_lock_of_a_start_ends_before_its_miss_path() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            holder: Holder::Claim {
                worker_id: WORKER.to_owned(),
                claimed_at: generation() + Duration::minutes(1),
            },
            ..TaskSeed::claimed()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "RUNNING").await;
    let mut holder = pool.begin().await.expect("begin the holder");
    let holder_pid = backend_pid(&mut *holder).await;
    sql::query(TASK_LOCK_SQL)
        .bind(task_id)
        .execute(&mut *holder)
        .await
        .expect("take the task lock");

    let started = tokio::time::timeout(
        SIDE_WAIT,
        start_attempt(
            &core_pool_for_test_url(pool.url()),
            StatementLog::Off,
            &StartAttempt::of_process(
                task_id,
                OwnedClaim {
                    worker_id: WORKER.to_owned(),
                    claimed_at: generation(),
                },
                4242,
                "lifecycle-host".to_owned(),
            ),
            crate::broker::child_writes::WriteDeadline::start_attempt(),
        ),
    )
    .await
    .expect("the start does not wait")
    .expect("start");
    sql::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE NOWAIT")
        .bind(task_id)
        .execute(&mut *holder)
        .await
        .expect("the start left the row unlocked");
    let miss_pool = pool.clone();
    let miss_path = tokio::spawn(async move {
        terminalize(
            &miss_pool,
            &TerminalizationCommand::ExpireOwnedClaim {
                task_id,
                fence: WorkerOwned {
                    worker_id: WORKER.to_owned(),
                },
                result_json: RESULT.to_owned(),
                error_code: "TASK_EXPIRED".to_owned(),
            },
        )
        .await
    });
    let waited = a_backend_waits_on(&pool, holder_pid).await;
    holder.commit().await.expect("commit the holder");
    let missed = tokio::time::timeout(SIDE_WAIT, miss_path)
        .await
        .expect("the miss path ends")
        .expect("join the miss path")
        .expect("miss path");

    assert!(
        matches!(started, StartOutcome::Missed(StartMiss::NotHeld)),
        "a start of an older generation ran: {started:?}"
    );
    assert!(waited, "the miss path never waited for the task lock");
    assert_eq!(missed.len(), 1, "{missed:?}");
    cleanup(&pool, task_id, Some(workflow_id)).await;
}

/// Python `test_an_isolated_abandon_waits_for_the_workflow_without_task_locks`
/// and `test_a_shared_async_abandon_waits_for_the_workflow_without_task_locks`:
/// the abandon of a task of a PAUSED workflow waits for the workflow row
/// first, and holds neither the task lock nor the task row while it waits.
#[tokio::test]
#[serial]
async fn an_abandon_waits_for_the_workflow_without_task_locks() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            ..TaskSeed::claimed()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "ENQUEUED", "PAUSED").await;
    let mut holder = pool.begin().await.expect("begin the workflow holder");
    let holder_pid = backend_pid(&mut *holder).await;
    sql::query("SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE")
        .bind(workflow_id)
        .execute(&mut *holder)
        .await
        .expect("lock the workflow row");

    let abandon_pool = pool.clone();
    let abandon = tokio::spawn(async move {
        terminalize(
            &abandon_pool,
            &TerminalizationCommand::AbandonOwnedNode {
                task_id,
                fence: owned_claim(),
            },
        )
        .await
    });
    let waited = a_backend_waits_on(&pool, holder_pid).await;
    let mut probe = pool.begin().await.expect("begin the probe");
    let task_lock_free: bool =
        sql::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 731))")
            .bind(task_id)
            .fetch_one(&mut *probe)
            .await
            .expect("try the task lock");
    let row_free = sql::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE NOWAIT")
        .bind(task_id)
        .execute(&mut *probe)
        .await
        .is_ok();
    probe.rollback().await.expect("release the probe");
    holder.commit().await.expect("release the workflow row");
    let abandoned = tokio::time::timeout(SIDE_WAIT, abandon)
        .await
        .expect("the abandon ends")
        .expect("join the abandon")
        .expect("abandon");

    assert!(waited, "the abandon never waited for the workflow row");
    assert!(task_lock_free, "the waiting abandon held the task lock");
    assert!(row_free, "the waiting abandon held the task row");
    assert!(
        matches!(
            abandoned.as_slice(),
            [TerminalizationOutcome::Applied {
                kind: TerminalizationKind::PauseAbandonClaim,
                ..
            }]
        ),
        "{abandoned:?}"
    );
    cleanup(&pool, task_id, Some(workflow_id)).await;
}

/// Python `test_a_free_race_of_finalize_and_stale_recovery_never_deadlocks`,
/// cases `failure` and `workflow completion`: a finalize and the stale move
/// of one task race, round after round. Neither side fails, and exactly one
/// of them moves the task.
#[tokio::test]
#[serial]
async fn a_free_race_of_finalize_and_stale_recovery_never_deadlocks() {
    let pool = pool().await;
    for origin in [Origin::Send, Origin::WorkflowNode] {
        for round in 0..RACE_ROUNDS {
            let (task_id, workflow_id) = stale_task(&pool, origin).await;
            let finalize_pool = pool.clone();
            let finalize = tokio::spawn(async move {
                match origin {
                    Origin::Send => write_failed_attempt(&finalize_pool, &failure(task_id))
                        .await
                        .map(|outcome| {
                            matches!(outcome, FailedAttemptOutcome::FailureApplied { .. })
                        }),
                    Origin::WorkflowNode => {
                        write_workflow_success(&finalize_pool, &success(task_id))
                            .await
                            .map(|outcome| {
                                matches!(outcome, WorkflowSuccessOutcome::SuccessApplied { .. })
                            })
                    }
                }
            });
            let stale_pool = pool.clone();
            let stale_move = tokio::spawn(async move {
                terminal_writes::fail_stale_task(&stale_pool, task_id, &stale_failure())
                    .await
                    .map(|outcomes| {
                        matches!(
                            outcomes.as_slice(),
                            [TerminalizationOutcome::Applied { .. }]
                        )
                    })
            });
            let finalized = finalize
                .await
                .expect("join the finalize")
                .unwrap_or_else(|error| panic!("{origin:?} round {round}: finalize: {error}"));
            let moved = stale_move
                .await
                .expect("join the stale move")
                .unwrap_or_else(|error| panic!("{origin:?} round {round}: stale move: {error}"));

            assert!(
                finalized ^ moved,
                "{origin:?} round {round}: exactly one side moves the task"
            );
            assert_eq!(history_kinds(&pool, task_id).await.len(), 1);
            cleanup(&pool, task_id, workflow_id).await;
        }
    }
}
