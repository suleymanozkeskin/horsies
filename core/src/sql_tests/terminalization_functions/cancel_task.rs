//! The writer of CANCEL_ADMIN, `horsies_cancel_task` (Python
//! `TestCancelTask`).

use super::*;
use crate::core::lifecycle::{GuardEvidence, TerminalizationKind};
use crate::core::types::status::TaskStatus;

#[tokio::test]
async fn applies_with_the_operator_owned_literals_and_pre_image() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;

    let terminal_at = match cancel_admin(&session, task_id, false).await {
        TerminalizationOutcome::Applied {
            kind,
            terminal_at,
            observed,
            ..
        } => {
            assert_eq!(kind, TerminalizationKind::CancelAdmin);
            assert_eq!(observed.status, Some(TaskStatus::Claimed));
            assert_eq!(observed.worker_id.as_deref(), Some(WORKER));
            assert_eq!(observed.claimed_at, Some(generation()));
            terminal_at
        }
        other => panic!("expected Applied: {other:?}"),
    };

    let row = task_row(&session, task_id).await;
    assert_eq!(row.get::<_, String>("status"), "CANCELLED");
    assert_eq!(
        row.get::<_, Option<String>>("error_code").as_deref(),
        Some(CANCEL_ADMIN_ERROR_CODE)
    );
    assert_eq!(
        row.get::<_, Option<String>>("failed_reason").as_deref(),
        Some(CANCEL_ADMIN_REASON)
    );
    assert!(row.get::<_, Option<DateTime<Utc>>>("failed_at").is_some());
    assert_eq!(
        row.get::<_, Option<DateTime<Utc>>>("terminal_at"),
        Some(terminal_at)
    );
    // No claim state survives the move; the history row keeps the claim
    // instant as provenance.
    assert!(!row.get::<_, bool>("claimed"));
    assert_eq!(row.get::<_, Option<String>>("claimed_by_worker_id"), None);
    assert_eq!(
        row.get::<_, Option<DateTime<Utc>>>("claimed_at"),
        Some(generation())
    );
    assert_eq!(row.get::<_, Option<DateTime<Utc>>>("finalizing_at"), None);
    assert_eq!(
        row.get::<_, Option<String>>("finalizing_by_worker_id"),
        None
    );
}

#[tokio::test]
async fn running_requires_explicit_permission() {
    let session = covered().await;

    let refused_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    match cancel_admin(&session, refused_id, false).await {
        TerminalizationOutcome::SourceStateConflict { observed, .. } => {
            assert_eq!(observed.status, Some(TaskStatus::Running));
        }
        other => panic!("expected SourceStateConflict: {other:?}"),
    }

    let allowed_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    match cancel_admin(&session, allowed_id, true).await {
        TerminalizationOutcome::Applied { observed, .. } => {
            assert_eq!(observed.status, Some(TaskStatus::Running));
        }
        other => panic!("expected Applied: {other:?}"),
    }
}

#[tokio::test]
async fn a_workflow_backing_task_is_never_administratively_cancelled() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", true)).await;

    let outcome = cancel_admin(&session, task_id, false).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::SourceStateConflict { .. }),
        "{outcome:?}"
    );
    assert_eq!(status_of(&session, task_id).await, "CLAIMED");
}

#[tokio::test]
async fn replay_is_already_applied() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;
    let _first = cancel_admin(&session, task_id, false).await;

    match cancel_admin(&session, task_id, false).await {
        TerminalizationOutcome::AlreadyApplied { kind, .. } => {
            assert_eq!(kind, TerminalizationKind::CancelAdmin);
        }
        other => panic!("expected AlreadyApplied: {other:?}"),
    }
}

#[tokio::test]
async fn a_finished_task_of_another_kind_is_never_overwritten() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;
    force_terminal(&session, task_id, "CANCELLED", "CANCEL_ORPHAN").await;

    match cancel_admin(&session, task_id, true).await {
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(evidence),
            ..
        } => {
            assert_eq!(evidence.committed_kind, TerminalizationKind::CancelOrphan);
        }
        other => panic!("expected a conflict with a foreign terminalization: {other:?}"),
    }
}

#[tokio::test]
async fn absence_is_an_outcome() {
    let session = covered().await;
    let outcome = cancel_admin(&session, Uuid::new_v4(), false).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::TaskAbsent { .. }),
        "{outcome:?}"
    );
}
