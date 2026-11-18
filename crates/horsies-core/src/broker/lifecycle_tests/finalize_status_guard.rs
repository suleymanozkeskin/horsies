//! Port of Python `tests/integration/test_finalize_status_guard.py`.
//!
//! The rows that a core test already checks are not repeated here:
//! `test_workflow_success_rejects_stale_claim_generation` is
//! `attempt_writes::tests::a_stale_claim_generation_writes_no_workflow_success`.
//! `test_in_flight_read_rejects_stale_claim_generation` has no subject in the
//! core: the core reads no row before the failed-attempt statement
//! (differences ledger, row 18), and that statement's generation fence is
//! `attempt_writes::tests::a_stale_claim_generation_writes_no_failure`.

use serial_test::serial;
use uuid::Uuid;

use super::support::*;
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, FinishWorkflowSuccess, RetryPlacement,
    RetryRequest,
};
use crate::core::lifecycle::{
    AttemptWriteRefusal, FailedAttemptOutcome, GuardEvidence, OwnedClaim, TerminalizationKind,
    TerminalizationOutcome, WorkflowSuccessOutcome,
};
use crate::test_support::sql::{self, TestPool};
use crate::test_support::terminal_writes::{self, StaleFailure};

const RETRY_FACTS_QUEUE: &str = "lifecycle_retry_facts";

fn failed_attempt(
    task_id: Uuid,
    retry: RetryRequest,
    kind: AttemptFailureKind,
) -> FinishFailedAttempt {
    FinishFailedAttempt {
        task_id,
        fence: owned_claim(),
        retry,
        result_json: r#"{"err": 1}"#.to_owned(),
        error_code: Some("TASK_EXCEPTION".to_owned()),
        failed_reason: None,
        attempt: AttemptFailure {
            kind,
            error_message: Some("attempt raised".to_owned()),
            failed_reason: None,
        },
    }
}

fn retry_after(expected_retry_count: i32) -> RetryRequest {
    RetryRequest::RetryAfter {
        expected_retry_count,
        delay_seconds: 0.0,
        placement: RetryPlacement::SameClass,
    }
}

/// The reaper fails a silent runner (`horsies_finish_stale_attempt`, no
/// retry): the row moves to history as FAIL_STALE.
async fn reaper_fails(pool: &TestPool, task_id: Uuid) {
    set_started(pool, task_id, Since::SecondsAgo(120.0)).await;
    let outcomes = terminal_writes::fail_stale_task(
        pool,
        task_id,
        &StaleFailure {
            stale_after_ms: 60_000,
            finalizing_stale_after_ms: 30_000,
            result_json: r#"{"err": 2}"#.to_owned(),
            error_code: "WORKER_CRASHED".to_owned(),
            failed_reason: "Worker crashed".to_owned(),
        },
    )
    .await
    .expect("reaper failure");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied {
                kind: TerminalizationKind::FailStale,
                ..
            }]
        ),
        "{outcomes:?}"
    );
}

/// The refusal of a failure write on a row that the reaper failed first.
fn assert_refused_by_the_reaper(outcome: &FailedAttemptOutcome) {
    assert!(
        matches!(
            outcome,
            FailedAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
                evidence: GuardEvidence::ForeignTerminalization(foreign),
                ..
            }) if foreign.committed_kind == TerminalizationKind::FailStale
        ),
        "{outcome:?}"
    );
}

/// Python `test_fail_guard_blocks_when_not_running`: the reaper put the row
/// back to PENDING; the failure write refuses it and writes nothing.
#[tokio::test]
#[serial]
async fn fail_guard_blocks_when_not_running() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    sql::query("UPDATE horsies_tasks SET status = 'PENDING' WHERE id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("requeue the row");

    let outcome = write_failed_attempt(
        &pool,
        &failed_attempt(task_id, RetryRequest::NoRetry, AttemptFailureKind::Failed),
    )
    .await
    .expect("failure write");

    assert!(
        matches!(
            outcome,
            FailedAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict { .. })
        ),
        "{outcome:?}"
    );
    assert_eq!(status_of(&pool, task_id).await, "PENDING");
    assert!(live_attempts(&pool, task_id).await.is_empty());
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_fail_worker_guard_blocks_when_not_running`: a worker-level
/// failure refuses a row that the reaper already failed.
#[tokio::test]
#[serial]
async fn fail_worker_guard_blocks_when_not_running() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    reaper_fails(&pool, task_id).await;

    let mut command = failed_attempt(
        task_id,
        RetryRequest::NoRetry,
        AttemptFailureKind::WorkerFailure,
    );
    command.failed_reason = Some("late worker failure".to_owned());
    command.attempt.failed_reason = Some("late worker failure".to_owned());
    let outcome = write_failed_attempt(&pool, &command)
        .await
        .expect("failure write");

    assert_refused_by_the_reaper(&outcome);
    let image = history_row(&pool, task_id).await.expect("history row");
    assert_eq!(image.terminalization_kind, "FAIL_STALE");
    assert_eq!(image.final_failed_reason.as_deref(), Some("Worker crashed"));
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_retry_guard_blocks_when_not_running`: the retry write
/// refuses a row that is no longer RUNNING.
#[tokio::test]
#[serial]
async fn retry_guard_blocks_when_not_running() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    reaper_fails(&pool, task_id).await;

    let outcome = write_failed_attempt(
        &pool,
        &failed_attempt(task_id, retry_after(0), AttemptFailureKind::Failed),
    )
    .await
    .expect("retry write");

    assert_refused_by_the_reaper(&outcome);
    assert_eq!(status_of(&pool, task_id).await, "FAILED");
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_reaper_then_retry_race_sequence`: the reaper fails the row,
/// then a late worker asks for a retry. The row stays FAILED and does not
/// come back as PENDING.
#[tokio::test]
#[serial]
async fn reaper_then_retry_race_sequence() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    reaper_fails(&pool, task_id).await;

    let late_retry = write_failed_attempt(
        &pool,
        &failed_attempt(task_id, retry_after(0), AttemptFailureKind::Failed),
    )
    .await
    .expect("late retry write");

    assert_refused_by_the_reaper(&late_retry);
    assert_eq!(live_row(&pool, task_id).await, None);
    assert_eq!(status_of(&pool, task_id).await, "FAILED");
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_in_flight_read_returns_the_retry_facts`, with the core's
/// design: the claim returns `retry_count` and `max_retries`, and no read
/// runs before the failure write (differences ledger, row 18).
#[tokio::test]
#[serial]
async fn the_claim_returns_the_retry_facts() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            queue_name: RETRY_FACTS_QUEUE,
            retry_count: 1,
            max_retries: 3,
            ..TaskSeed::pending()
        },
    )
    .await;
    let pass = crate::broker::claims::claim_pass(
        &crate::broker::pools::core_pool_for_test_url(pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        &claim_request(RETRY_FACTS_QUEUE),
    )
    .await
    .expect("claim pass");

    let row = pass
        .claimed
        .iter()
        .find(|row| row.id == task_id)
        .expect("the claim returned the seeded task");
    assert_eq!((row.retry_count, row.max_retries), (1, 3));
    cleanup_tasks(&pool, &[task_id]).await;
}

/// Python `test_workflow_success_blocks_on_ownership_mismatch`: the success
/// of a workflow task refuses a RUNNING row of another worker and writes
/// nothing; the owner's success applies.
#[tokio::test]
#[serial]
async fn workflow_success_blocks_on_ownership_mismatch() {
    let pool = pool().await;
    let task_id = seed_task(
        &pool,
        &TaskSeed {
            origin: Origin::WorkflowNode,
            holder: Holder::other_worker(),
            ..TaskSeed::running()
        },
    )
    .await;
    let workflow_id = link_workflow_task(&pool, task_id, "RUNNING", "RUNNING").await;
    let success = |worker_id: &str| FinishWorkflowSuccess {
        task_id,
        fence: OwnedClaim {
            worker_id: worker_id.to_owned(),
            claimed_at: generation(),
        },
        result_json: r#"{"ok": 1}"#.to_owned(),
    };

    let refused = write_workflow_success(&pool, &success(WORKER))
        .await
        .expect("workflow success");
    assert!(
        matches!(
            refused,
            WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::LostClaim { .. })
        ),
        "{refused:?}"
    );
    assert_eq!(status_of(&pool, task_id).await, "RUNNING");
    assert!(live_attempts(&pool, task_id).await.is_empty());

    let owner = write_workflow_success(&pool, &success(OTHER_WORKER))
        .await
        .expect("workflow success");
    assert!(
        matches!(owner, WorkflowSuccessOutcome::SuccessApplied { .. }),
        "{owner:?}"
    );
    cleanup_tasks(&pool, &[task_id]).await;
    cleanup_workflows(&pool, &[workflow_id]).await;
}

/// Python `test_fused_finalize_null_fence_matches_any_generation`, with the
/// core's rule: the stored fused completion refuses a NULL generation before
/// any lock or write (differences ledger, row 3).
#[tokio::test]
#[serial]
async fn fused_finalize_null_fence_is_refused() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::running()).await;
    let before = live_row(&pool, task_id).await;

    let refused = sql::query("SELECT * FROM horsies_complete_task_fused($1, $2, NULL, $3, TRUE)")
        .bind(task_id)
        .bind(WORKER)
        .bind(r#"{"ok": 2}"#)
        .fetch_all(&pool)
        .await
        .map(|rows| rows.len());

    assert_refused(refused, "22023", Constraint::Unnamed);
    assert_eq!(live_row(&pool, task_id).await, before);
    cleanup_tasks(&pool, &[task_id]).await;
}
