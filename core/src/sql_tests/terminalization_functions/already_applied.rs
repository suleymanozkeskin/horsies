//! A replay of the writer of COMPLETE_LOCKED, and a row that another
//! operation ended (Python `TestAlreadyApplied`). The test of the replay
//! after the fused completion stays in Python: it calls the fused
//! completion through `apply_async`.

use super::*;
use crate::core::lifecycle::{GuardEvidence, TerminalizationKind};
use crate::core::types::status::TaskStatus;

#[tokio::test]
async fn replay_of_the_same_operation() {
    let session = covered().await;
    let task_id = workflow_task(&session).await;
    let _first = locked(&session, task_id).await;

    match locked(&session, task_id).await {
        TerminalizationOutcome::AlreadyApplied { kind, .. } => {
            assert_eq!(kind, TerminalizationKind::CompleteLocked);
        }
        other => panic!("expected AlreadyApplied: {other:?}"),
    }
}

/// Someone else ended this task, and the caller has to hear that. Reported
/// as already applied, the row of an administrative cancellation would let
/// a completion replay assume work it never did.
#[tokio::test]
async fn a_foreign_kind_is_a_conflict_not_a_replay() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    force_terminal(&session, task_id, "CANCELLED", "CANCEL_ADMIN").await;

    match locked(&session, task_id).await {
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(evidence),
            ..
        } => {
            assert_eq!(evidence.observed_status, TaskStatus::Cancelled);
            assert_eq!(evidence.committed_kind, TerminalizationKind::CancelAdmin);
            assert!(evidence.terminal_at.is_some());
        }
        other => panic!("expected a conflict with a foreign terminalization: {other:?}"),
    }
}
