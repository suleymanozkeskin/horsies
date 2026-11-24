//! The monitoring task cancel: a claimed row, stored data that does not
//! parse, a finalize after the cancel, a claim that meets the cancel, and the
//! cancel of a RUNNING row against a normal end, a retry and stale recovery,
//! in both orders.
//!
//! Python specification: `clients/python/tests/integration/test_monitoring_task_actions.py`
//! (baseline version on `program/0.8.0`). Each test runs on a database of
//! its own ([`SpecDatabase`]), so a claim pass sees only its rows.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::spec_support::{
    seed_live, seed_terminal, set_history_status, Ending, LiveTask, SpecDatabase,
};
use crate::broker::claims::{claim_pass, ClaimClasses, ClaimPassRequest};
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_url;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::lifecycle::{OwnedClaim, TerminalizationCommand, TerminalizationOutcome};
use crate::core::task::Execution;
use crate::monitoring::{cancel_task, TaskActionError, TaskActionErrorCode};
use crate::test_support::sql::{self, TestPool};
use crate::TaskStatus;

const WORKER: &str = "cancel-spec-worker";
const OTHER_WORKER: &str = "cancel-spec-other-worker";

/// The host of a RUNNING row: the start write records it with the pid and
/// the process name, and every attempt writer copies it.
const WORKER_HOST: &str = "cancel-spec-host";

/// A failed attempt of a RUNNING row with a retry, as the worker writes it
/// for a task error (no observed placement). Parameters: task id, worker id,
/// claim generation. Returns the outcome text.
const RETRY_FAILED_ATTEMPT_SQL: &str = "SELECT outcome FROM horsies_finish_failed_attempt(
    $1, $2, $3, 'RETRY', 0, 0::double precision, 'SAME_CLASS', '{\"err\": 1}', 'TASK_EXCEPTION', NULL,
    'FAILED', NULL, NULL, 'REFUSE', TRUE, 'NOT_OBSERVED', NULL)";

/// The stale RUNNING recovery of a row with no runner beat for 1 s, with no
/// retry. Parameter: task id. Returns the outcome text.
const STALE_FAILURE_SQL: &str = "SELECT outcome FROM horsies_finish_stale_attempt(
    $1, 'BEAT_AGE', 1000, 1000, NULL, NULL, 'NONE', NULL, NULL, '{\"err\": 2}', 'WORKER_CRASHED',
    'Worker crashed', 'Worker crashed', 'Worker crashed')";

/// A RUNNING row of [`WORKER`] with the process facts that its start write
/// records, started 30 s ago.
fn running_task(task_name: &str) -> LiveTask {
    LiveTask {
        worker_hostname: Some(WORKER_HOST.to_owned()),
        ..LiveTask::running(task_name, WORKER)
    }
}

/// The attempt archive of the history row of `task_id`, as its JSON text.
async fn archived_attempts(pool: &TestPool, task_id: Uuid) -> String {
    sql::query_scalar(
        "SELECT convert_from(attempt_snapshot, 'UTF8') FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the attempt archive")
}

/// The live status of `task_id`, or `None` when the row left the live table.
async fn live_status(pool: &TestPool, task_id: Uuid) -> Option<String> {
    sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live row")
}

/// The history status of `task_id`, or `None` when it has no history row.
async fn history_status(pool: &TestPool, task_id: Uuid) -> Option<String> {
    sql::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the history row")
}

async fn attempt_rows(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the attempt rows")
}

async fn claim_generation(pool: &TestPool, task_id: Uuid) -> DateTime<Utc> {
    sql::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the claim generation")
}

/// One claim pass of `worker_id` on `default` (Python `claim_once`), through
/// the claim function of the worker (`horsies_claim` with an ISOLATED budget
/// of 10): the ids it claimed.
async fn claim_once(pool: &TestPool, worker_id: &str) -> Vec<Uuid> {
    let queues = vec!["default".to_owned()];
    let request = ClaimPassRequest {
        worker_id: worker_id.to_owned(),
        queue_priority: queues.iter().map(|queue| (queue.clone(), 100)).collect(),
        queues,
        queue_max_concurrency: std::collections::HashMap::new(),
        hard_cap_mode: true,
        classes: ClaimClasses::new(
            std::collections::HashMap::from([(Execution::Isolated, 10)]),
            std::collections::HashMap::from([(Execution::Isolated, Execution::Isolated)]),
        )
        .expect("the route names the ISOLATED budget"),
        prefetch_buffer: 0,
        max_claim_per_worker: 0,
        max_claim_batch: 0,
        cluster_wide_cap: None,
        claim_lease_ms: std::num::NonZeroU32::new(60_000).expect("a positive lease"),
        lock_keys: Vec::new(),
    };
    claim_pass(
        &core_pool_for_test_url(pool.url()),
        StatementLog::Off,
        &request,
    )
    .await
    .expect("run one claim pass")
    .claimed
    .into_iter()
    .map(|row| row.id)
    .collect()
}

/// Python `TestCancelTaskSucceeds::test_claimed_task_is_cancelled_and_claim_released`:
/// the cancel of a CLAIMED row moves it to history, so no live row is left
/// to claim; the history row keeps the claim time as provenance.
#[tokio::test]
async fn a_cancelled_claim_leaves_no_live_row_and_keeps_its_claim_time() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::claimed("alpha_task", WORKER);
    seed_live(&database.pool, &task).await;

    let cancelled = cancel_task(&database.broker(), task.id, false)
        .await
        .expect("a claimed task is cancelled");

    assert_eq!(cancelled.was_status, TaskStatus::Claimed);
    assert_eq!(live_status(&database.pool, task.id).await, None);
    assert_eq!(
        history_status(&database.pool, task.id).await.as_deref(),
        Some("CANCELLED")
    );
    let claimed_at: Option<DateTime<Utc>> =
        sql::query_scalar("SELECT claimed_at FROM horsies_task_history WHERE task_id = $1")
            .bind(task.id)
            .fetch_one(&database.pool)
            .await
            .expect("read the history claim time");
    assert!(claimed_at.is_some(), "the claim time is not kept");
    database.drop().await;
}

/// Python `TestCancelTaskRefuses::test_history_row_with_an_unknown_status_is_stored_data_invalid`:
/// a history status that is not a task status is `STORED_DATA_INVALID`,
/// not retryable, naming the value; the row does not change.
#[tokio::test]
async fn an_unknown_stored_history_status_is_stored_data_invalid_and_nothing_changes() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    seed_terminal(&database.pool, &task, Ending::failed("TASK_EXCEPTION")).await;
    set_history_status(&database.pool, task.id, "NOT_KNOWN").await;

    let refused = cancel_task(&database.broker(), task.id, false)
        .await
        .expect_err("stored data that does not parse is refused");

    assert_eq!(refused.code(), TaskActionErrorCode::StoredDataInvalid);
    assert!(!refused.retryable());
    assert_eq!(refused.task_id(), task.id);
    assert!(
        refused.message().contains("NOT_KNOWN"),
        "{}",
        refused.message()
    );
    assert_eq!(
        history_status(&database.pool, task.id).await.as_deref(),
        Some("NOT_KNOWN")
    );
    database.drop().await;
}

/// Python `TestCancelRunningVersusFinalize::test_finalize_after_cancel_is_a_no_op`:
/// the worker's success finalize after the cancel of its RUNNING row is a
/// source-state conflict; the row stays CANCELLED with no result, no live
/// attempt row is left, and the history row holds the attempt that the
/// cancel recorded as CANCELLED.
#[tokio::test]
async fn a_success_finalize_after_the_cancel_of_a_running_task_changes_nothing() {
    let database = SpecDatabase::create().await;
    let task = running_task("alpha_task");
    seed_live(&database.pool, &task).await;
    let generation = claim_generation(&database.pool, task.id).await;

    let cancelled = cancel_task(&database.broker(), task.id, true)
        .await
        .expect("a running task is cancelled with the opt-in");
    let finalized = terminalize(
        &database.pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: task.id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: generation,
            },
            result_json: r#"{"ok": 99}"#.to_owned(),
            placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
        },
    )
    .await
    .expect("run the finalize");

    assert_eq!(cancelled.was_status, TaskStatus::Running);
    assert!(
        matches!(
            finalized.as_slice(),
            [TerminalizationOutcome::SourceStateConflict { .. }]
        ),
        "{finalized:?}"
    );
    assert_eq!(
        history_status(&database.pool, task.id).await.as_deref(),
        Some("CANCELLED")
    );
    let result: Option<Vec<u8>> =
        sql::query_scalar("SELECT result_payload FROM horsies_task_history WHERE task_id = $1")
            .bind(task.id)
            .fetch_one(&database.pool)
            .await
            .expect("read the history result");
    assert_eq!(result, None);
    assert_eq!(attempt_rows(&database.pool, task.id).await, 0);
    let archive = archived_attempts(&database.pool, task.id).await;
    assert!(
        archive.starts_with(r#"[[[1,"CANCELLED",false,"#)
            && archive.ends_with(r#""CANCEL",null]],null]"#),
        "{archive}"
    );
    database.drop().await;
}

/// The normal end of a RUNNING row commits before the cancel: the cancel is
/// refused with the terminal status, writes nothing, and the history row
/// keeps the one COMPLETED attempt of the worker.
#[tokio::test]
async fn a_cancel_after_the_success_of_a_running_task_is_not_cancellable() {
    let database = SpecDatabase::create().await;
    let task = running_task("alpha_task");
    seed_live(&database.pool, &task).await;
    let generation = claim_generation(&database.pool, task.id).await;
    let finalized = terminalize(
        &database.pool,
        &TerminalizationCommand::CompleteTaskFused {
            task_id: task.id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at: generation,
            },
            result_json: r#"{"ok": 1}"#.to_owned(),
            placement: crate::core::lifecycle::AttemptPlacement::NotObserved,
        },
    )
    .await
    .expect("run the finalize");
    assert!(
        matches!(
            finalized.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{finalized:?}"
    );

    let refused = cancel_task(&database.broker(), task.id, true)
        .await
        .expect_err("a completed task is not cancelled");

    let TaskActionError::NotCancellable { current_status, .. } = &refused else {
        panic!("expected TASK_NOT_CANCELLABLE, got {refused:?}");
    };
    assert_eq!(*current_status, TaskStatus::Completed);
    let archive = archived_attempts(&database.pool, task.id).await;
    assert!(
        archive.starts_with(r#"[[[1,"COMPLETED",false,"#),
        "{archive}"
    );
    assert!(!archive.contains(r#""CANCELLED""#), "{archive}");
    database.drop().await;
}

/// The failed attempt with a retry commits before the cancel: the row is
/// PENDING for its next attempt, the cancel moves it from PENDING with no new
/// attempt, and the history row keeps the FAILED attempt with `will_retry`.
#[tokio::test]
async fn a_cancel_after_a_retry_moves_the_pending_row_with_the_failed_attempt() {
    let database = SpecDatabase::create().await;
    let task = running_task("alpha_task");
    seed_live(&database.pool, &task).await;
    let generation = claim_generation(&database.pool, task.id).await;
    let retried: String = sql::query_scalar(RETRY_FAILED_ATTEMPT_SQL)
        .bind(task.id)
        .bind(WORKER)
        .bind(generation)
        .fetch_one(&database.pool)
        .await
        .expect("write the failed attempt with a retry");
    assert_eq!(retried, "RETRY_SCHEDULED");

    let cancelled = cancel_task(&database.broker(), task.id, true)
        .await
        .expect("a pending task is cancelled");

    assert_eq!(cancelled.was_status, TaskStatus::Pending);
    let archive = archived_attempts(&database.pool, task.id).await;
    assert!(archive.starts_with(r#"[[[1,"FAILED",true,"#), "{archive}");
    assert!(!archive.contains(r#""CANCELLED""#), "{archive}");
    database.drop().await;
}

/// The cancel commits before the failed attempt of the worker: the attempt
/// write finds the row in history and writes nothing; the history row keeps
/// the one CANCELLED attempt.
#[tokio::test]
async fn a_failed_attempt_after_the_cancel_of_a_running_task_writes_nothing() {
    let database = SpecDatabase::create().await;
    let task = running_task("alpha_task");
    seed_live(&database.pool, &task).await;
    let generation = claim_generation(&database.pool, task.id).await;
    let cancelled = cancel_task(&database.broker(), task.id, true)
        .await
        .expect("a running task is cancelled with the opt-in");

    let refused: String = sql::query_scalar(RETRY_FAILED_ATTEMPT_SQL)
        .bind(task.id)
        .bind(WORKER)
        .bind(generation)
        .fetch_one(&database.pool)
        .await
        .expect("run the failed attempt write");

    assert_eq!(cancelled.was_status, TaskStatus::Running);
    assert_eq!(refused, "SOURCE_STATE_CONFLICT");
    assert_eq!(
        history_status(&database.pool, task.id).await.as_deref(),
        Some("CANCELLED")
    );
    let archive = archived_attempts(&database.pool, task.id).await;
    assert!(
        archive.starts_with(r#"[[[1,"CANCELLED",false,"#),
        "{archive}"
    );
    assert!(!archive.contains(r#""FAILED""#), "{archive}");
    database.drop().await;
}

/// Stale RUNNING recovery commits before the cancel: the task is FAILED
/// (`FAIL_STALE`), and the cancel is refused with that status.
#[tokio::test]
async fn a_cancel_after_stale_recovery_is_not_cancellable() {
    let database = SpecDatabase::create().await;
    let task = running_task("alpha_task");
    seed_live(&database.pool, &task).await;
    let recovered: String = sql::query_scalar(STALE_FAILURE_SQL)
        .bind(task.id)
        .fetch_one(&database.pool)
        .await
        .expect("run the stale recovery");
    assert_eq!(recovered, "APPLIED");

    let refused = cancel_task(&database.broker(), task.id, true)
        .await
        .expect_err("a failed task is not cancelled");

    let TaskActionError::NotCancellable { current_status, .. } = &refused else {
        panic!("expected TASK_NOT_CANCELLABLE, got {refused:?}");
    };
    assert_eq!(*current_status, TaskStatus::Failed);
    let archive = archived_attempts(&database.pool, task.id).await;
    assert!(!archive.contains(r#""CANCELLED""#), "{archive}");
    database.drop().await;
}

/// The cancel commits before stale RUNNING recovery: the recovery finds the
/// row in history and writes nothing; the task stays CANCELLED with its
/// CANCELLED attempt.
#[tokio::test]
async fn stale_recovery_after_the_cancel_of_a_running_task_writes_nothing() {
    let database = SpecDatabase::create().await;
    let task = running_task("alpha_task");
    seed_live(&database.pool, &task).await;
    let _cancelled = cancel_task(&database.broker(), task.id, true)
        .await
        .expect("a running task is cancelled with the opt-in");

    let refused: String = sql::query_scalar(STALE_FAILURE_SQL)
        .bind(task.id)
        .fetch_one(&database.pool)
        .await
        .expect("run the stale recovery");

    assert_eq!(refused, "SOURCE_STATE_CONFLICT");
    assert_eq!(
        history_status(&database.pool, task.id).await.as_deref(),
        Some("CANCELLED")
    );
    let archive = archived_attempts(&database.pool, task.id).await;
    assert!(
        archive.starts_with(r#"[[[1,"CANCELLED",false,"#),
        "{archive}"
    );
    database.drop().await;
}

/// Python `TestClaimCancelRace::test_cancel_of_a_freshly_claimed_row_still_wins`:
/// a claim through the claim function lands first, then the cancel takes
/// the row.
#[tokio::test]
async fn the_cancel_of_a_row_the_claim_function_just_claimed_wins() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    seed_live(&database.pool, &task).await;
    let broker = database.broker();
    assert_eq!(claim_once(&database.pool, WORKER).await, [task.id]);

    let cancelled = cancel_task(&broker, task.id, false)
        .await
        .expect("the claimed row is cancelled");

    assert_eq!(cancelled.was_status, TaskStatus::Claimed);
    assert_eq!(live_status(&database.pool, task.id).await, None);
    assert_eq!(
        history_status(&database.pool, task.id).await.as_deref(),
        Some("CANCELLED")
    );
    database.drop().await;
}

/// Python `TestClaimCancelRace::test_concurrent_claim_and_cancel_leave_one_consistent_outcome`:
/// a claim and a cancel that run at once leave one outcome: the cancel
/// wins from PENDING or CLAIMED and the row is CANCELLED, or the cancel
/// loses to a later status and the row is not CANCELLED. No attempt row is
/// written either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_concurrent_claim_and_cancel_leave_one_consistent_outcome() {
    let database = SpecDatabase::create().await;
    let task = LiveTask::pending("alpha_task");
    seed_live(&database.pool, &task).await;
    let broker = database.broker();

    let (cancelled, claimed) = tokio::join!(
        cancel_task(&broker, task.id, false),
        claim_once(&database.pool, OTHER_WORKER),
    );

    let history = history_status(&database.pool, task.id).await;
    match cancelled {
        Ok(cancel) => {
            assert!(
                matches!(cancel.was_status, TaskStatus::Pending | TaskStatus::Claimed),
                "{cancel:?}"
            );
            assert_eq!(history.as_deref(), Some("CANCELLED"));
            assert_eq!(live_status(&database.pool, task.id).await, None);
        }
        Err(refused) => {
            assert_eq!(refused.code(), TaskActionErrorCode::TaskNotCancellable);
            let TaskActionError::NotCancellable { current_status, .. } = &refused else {
                panic!("expected TASK_NOT_CANCELLABLE, got {refused:?}");
            };
            assert!(
                !matches!(current_status, TaskStatus::Pending | TaskStatus::Claimed),
                "{refused:?}"
            );
            assert_ne!(history.as_deref(), Some("CANCELLED"));
        }
    }
    assert!(claimed.is_empty() || claimed == [task.id], "{claimed:?}");
    assert_eq!(attempt_rows(&database.pool, task.id).await, 0);
    database.drop().await;
}
