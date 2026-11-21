//! The writer of COMPLETE_LOCKED applies (Python `TestAppliedTransitions`).
//! The tests of the fused completion stay in Python: they call it through
//! `apply_async`.

use super::*;
use crate::core::lifecycle::TerminalizationKind;
use crate::core::types::status::TaskStatus;

/// The decoder refuses an applied row with no `terminal_at`, so the decode
/// asserts that the transition stamped its terminal instant.
#[tokio::test]
async fn locked_completion_applies_and_stamps_its_kind() {
    let session = covered().await;
    let task_id = workflow_task(&session).await;

    match locked(&session, task_id).await {
        TerminalizationOutcome::Applied { kind, .. } => {
            assert_eq!(kind, TerminalizationKind::CompleteLocked);
        }
        other => panic!("expected Applied: {other:?}"),
    }
}

/// The pre-transition image is what the guard saw.
#[tokio::test]
async fn applied_reports_the_image_it_matched() {
    let session = covered().await;
    let task_id = workflow_task(&session).await;

    match locked(&session, task_id).await {
        TerminalizationOutcome::Applied { observed, .. } => {
            assert_eq!(observed.status, Some(TaskStatus::Running));
            assert_eq!(observed.worker_id.as_deref(), Some(WORKER));
            assert_eq!(observed.claimed_at, Some(generation()));
        }
        other => panic!("expected Applied: {other:?}"),
    }
}

#[tokio::test]
async fn transition_leaves_the_row_terminal_and_attributed() {
    let session = covered().await;
    let task_id = workflow_task(&session).await;
    let _outcome = locked(&session, task_id).await;

    let row = task_row(&session, task_id).await;
    assert_eq!(row.get::<_, String>("status"), "COMPLETED");
    assert!(row.get::<_, Option<DateTime<Utc>>>("terminal_at").is_some());
    assert_eq!(
        row.get::<_, Option<String>>("terminalization_kind")
            .as_deref(),
        Some("COMPLETE_LOCKED")
    );
    assert_eq!(
        row.get::<_, Option<String>>("result").as_deref(),
        Some(LOCKED_RESULT)
    );
}

/// Attempt one failed and requeued; attempt two completes through the
/// writer of COMPLETE_LOCKED. The terminal row holds only the final
/// disposition: the failure summary of attempt one is cleared beside
/// COMPLETED. The archived attempt one keeps its reason, and the completion
/// is attempt two.
#[tokio::test]
async fn completion_clears_an_earlier_attempts_failure_summary_locked() {
    let session = covered().await;
    let task_id = workflow_task(&session).await;
    requeued_after_failed_attempt(&session, task_id, ATTEMPT_ONE_REASON).await;

    match locked(&session, task_id).await {
        TerminalizationOutcome::Applied { kind, .. } => {
            assert_eq!(kind, TerminalizationKind::CompleteLocked);
        }
        other => panic!("expected Applied: {other:?}"),
    }

    let row = task_row(&session, task_id).await;
    assert_eq!(row.get::<_, String>("status"), "COMPLETED");
    assert_eq!(row.get::<_, Option<String>>("error_code"), None);
    assert_eq!(row.get::<_, Option<String>>("failed_reason"), None);
    assert_eq!(
        attempt_history(&session, task_id).await,
        vec![
            (1, "FAILED".to_owned(), Some(ATTEMPT_ONE_REASON.to_owned())),
            (2, "COMPLETED".to_owned(), None),
        ]
    );
}
