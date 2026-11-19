//! The writer of COMPLETE_LOCKED refuses a row that is not its to end
//! (Python `TestRefusals`). The test of a claim generation that moved on
//! stays in Python: it calls the fused completion through `apply_async`.

use super::*;
use crate::core::lifecycle::outcomes::ObservedClaim;
use crate::core::lifecycle::GuardEvidence;
use crate::core::types::status::TaskStatus;

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

    match locked(&session, task_id).await {
        TerminalizationOutcome::LostClaim { observed, .. } => {
            assert_eq!(observed.worker_id.as_deref(), Some(OTHER_WORKER));
        }
        other => panic!("expected LostClaim: {other:?}"),
    }
}

/// PENDING with the claim cleared is the requeue case. A fenced command
/// classifies it as a lost claim, not as a state conflict: the generation
/// that held it is gone, which is what the caller must act on.
#[tokio::test]
async fn a_requeued_row_reports_a_lost_claim() {
    let session = covered().await;
    let task_id = seed(
        &session,
        &Seed {
            status: "PENDING",
            worker_id: Some(OTHER_WORKER),
            claimed_at: None,
            is_workflow_task: false,
        },
    )
    .await;
    let _cleared = run(
        &session,
        "clear the claim",
        "UPDATE horsies_tasks SET claimed_by_worker_id = NULL WHERE id = $1",
        &[&task_id],
    )
    .await;

    match locked(&session, task_id).await {
        TerminalizationOutcome::LostClaim { observed, .. } => {
            assert_eq!(observed.status, Some(TaskStatus::Pending));
        }
        other => panic!("expected LostClaim: {other:?}"),
    }
}

/// Owned by this worker at this generation, but not yet running.
#[tokio::test]
async fn the_source_status_is_wrong() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;

    match locked(&session, task_id).await {
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

/// Absence is an outcome, not an empty result.
#[tokio::test]
async fn a_task_that_does_not_exist() {
    let session = covered().await;
    let outcome = locked(&session, Uuid::new_v4()).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::TaskAbsent { .. }),
        "{outcome:?}"
    );
}
