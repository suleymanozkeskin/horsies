//! The writer of FAIL_RUNNING, `horsies_finish_failed_attempt` (Python
//! `TestFailLocked`).

use super::*;
use crate::core::lifecycle::outcomes::ObservedClaim;
use crate::core::lifecycle::{GuardEvidence, TerminalizationKind};
use crate::core::types::status::TaskStatus;

#[tokio::test]
async fn applies_and_stamps_its_kind() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;

    match fail_locked(&session, task_id, Some("worker failure")).await {
        TerminalizationOutcome::Applied { kind, .. } => {
            assert_eq!(kind, TerminalizationKind::FailRunning);
        }
        other => panic!("expected Applied: {other:?}"),
    }

    let row = task_row(&session, task_id).await;
    assert_eq!(row.get::<_, String>("status"), "FAILED");
    assert!(row.get::<_, Option<DateTime<Utc>>>("failed_at").is_some());
    assert_eq!(
        row.get::<_, Option<String>>("failed_reason").as_deref(),
        Some("worker failure")
    );
    assert_eq!(
        row.get::<_, Option<String>>("error_code").as_deref(),
        Some(FAIL_RUNNING_ERROR_CODE)
    );
    assert_eq!(
        row.get::<_, Option<String>>("result").as_deref(),
        Some(FAIL_RUNNING_RESULT)
    );
}

/// The terminal writer owns the complete summary of the final attempt.
/// Attempt one failed with a reason and requeued; the final attempt fails
/// with none. A NULL reason means that this final attempt has no failure
/// reason: the reason of the requeued attempt is cleared on the terminal
/// row, and the archived attempt one keeps its reason.
#[tokio::test]
async fn a_null_reason_clears_an_earlier_attempts_reason() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    requeued_after_failed_attempt(&session, task_id, ATTEMPT_ONE_REASON).await;

    let outcome = fail_locked(&session, task_id, None).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::Applied { .. }),
        "{outcome:?}"
    );

    let row = task_row(&session, task_id).await;
    assert_eq!(row.get::<_, Option<String>>("failed_reason"), None);
    assert_eq!(
        attempt_history(&session, task_id).await,
        vec![
            (1, "FAILED".to_owned(), Some(ATTEMPT_ONE_REASON.to_owned())),
            (2, "FAILED".to_owned(), None),
        ]
    );
}

#[tokio::test]
async fn a_supplied_reason_overwrites() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;

    let _outcome = fail_locked(&session, task_id, Some("attempt two crashed")).await;

    let row = task_row(&session, task_id).await;
    assert_eq!(
        row.get::<_, Option<String>>("failed_reason").as_deref(),
        Some("attempt two crashed")
    );
}

#[tokio::test]
async fn replay_is_already_applied() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let _first = fail_locked(&session, task_id, None).await;

    match fail_locked(&session, task_id, None).await {
        TerminalizationOutcome::AlreadyApplied { kind, .. } => {
            assert_eq!(kind, TerminalizationKind::FailRunning);
        }
        other => panic!("expected AlreadyApplied: {other:?}"),
    }
}

/// Both write FAILED; they are not one class on purpose. Distinctness is
/// the conservative direction: a conflict makes the caller look, an
/// already-applied lets it walk away.
#[tokio::test]
async fn a_stale_failure_is_a_conflict_not_a_replay() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    force_terminal(&session, task_id, "FAILED", "FAIL_STALE").await;

    match fail_locked(&session, task_id, None).await {
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(evidence),
            ..
        } => {
            assert_eq!(evidence.committed_kind, TerminalizationKind::FailStale);
        }
        other => panic!("expected a conflict with a foreign terminalization: {other:?}"),
    }
}

#[tokio::test]
async fn another_worker_holds_the_claim() {
    let session = covered().await;
    let task_id = seed(
        &session,
        &Seed {
            worker_id: Some(OTHER_WORKER),
            ..Seed::claimed_by_this_worker("RUNNING", false)
        },
    )
    .await;

    match fail_locked(&session, task_id, None).await {
        TerminalizationOutcome::LostClaim { observed, .. } => {
            assert_eq!(observed.worker_id.as_deref(), Some(OTHER_WORKER));
        }
        other => panic!("expected LostClaim: {other:?}"),
    }
}

#[tokio::test]
async fn the_source_status_is_wrong() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;

    match fail_locked(&session, task_id, None).await {
        TerminalizationOutcome::SourceStateConflict {
            observed, evidence, ..
        } => {
            assert_eq!(observed.status, Some(TaskStatus::Claimed));
            assert_eq!(
                evidence,
                GuardEvidence::Claim(ObservedClaim {
                    worker_id: Some(WORKER.to_owned()),
                    claimed_at: Some(generation()),
                })
            );
        }
        other => panic!("expected SourceStateConflict: {other:?}"),
    }
}

#[tokio::test]
async fn a_task_that_does_not_exist() {
    let session = covered().await;
    let outcome = fail_locked(&session, Uuid::new_v4(), None).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::TaskAbsent { .. }),
        "{outcome:?}"
    );
}
