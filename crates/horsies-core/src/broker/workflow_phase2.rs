//! Phase 2 of a finished workflow task, the parent advance after it, and the
//! phase-2 recovery pass of the reaper, on a
//! client handle whose children come from the client's
//! [`ChildSpecSource`]: the entry points of the Python worker's
//! `FinalizeMixin._finalize_workflow_phase` and `_drive_parent_propagations`
//! (`python/horsies/core/worker/finalize.py`) and of its reaper's phase-2
//! recovery (`python/horsies/core/worker/reaper.py`), which call them through
//! `horsies._core` in place of their own statements.
//!
//! Each runs on the main core pool with the reserve
//! ([`CheckoutClass::Reserved`]): per-task writes of a worker and the
//! reaper's pass.

use uuid::Uuid;

use crate::broker::postgres::PostgresBroker;
use crate::db::CheckoutClass;
use crate::workflow_engine::child_spec::ChildSpecSource;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::phase2_recovery::{drive_phase2_recovery_with, Phase2RecoverySummary};
use crate::workflow_engine::steps::{
    advance_parents, node_finish_alone, node_finish_in_transaction, ChildBuild, Dependents,
    NodeFinishDisposition, OwedParentAdvances, ParentAdvance, ParentAdvanceFailure, SourceFailure,
};

/// The form of a phase 2, from the node facts that phase 1 read after its
/// move (Python `PhaseTwoStep` and `FactsUnknown`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeFinishForm {
    /// No node depends on this node: one statement in autocommit, the
    /// function reads no dependents.
    NoDependents,
    /// No sub-workflow node is below this node: one statement in
    /// autocommit, the function reads the dependents.
    DependentsWithoutSubworkflow,
    /// A sub-workflow node is reachable below this node: a transaction with
    /// the child starts that the step owes.
    SubworkflowReachable,
    /// A replay or a retry, without the phase-1 facts: the transaction form.
    FactsUnknown,
}

/// What a phase 2 did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeFinished {
    /// The wire text of the disposition: `APPLIED`, `APPLIED_WORKFLOW_ENDED`,
    /// `ALREADY_APPLIED`, `PENDING_ABSENT`, or a `SOURCE_*` text when the
    /// step kept the phase-2 record and wrote nothing.
    pub disposition: &'static str,
    /// The status of the node's workflow that the function read; `None`
    /// when it read none.
    pub workflow_status: Option<String>,
    /// The child workflows whose parent nodes advance after the commit, in
    /// order. The caller drives them ([`PostgresBroker::advance_workflow_parents`]).
    pub parent_advances: Vec<Uuid>,
}

/// A parent advance that stopped at the level of `failed_child_workflow_id`.
/// The levels before it committed; that level wrote nothing, or its commit
/// outcome is unknown.
#[derive(Debug)]
pub struct ParentAdvanceStopped {
    pub failed_child_workflow_id: Uuid,
    /// Every advance still owed, the failed level first: what a retry runs.
    pub still_owed: Vec<Uuid>,
    pub error: WorkflowError,
}

/// The wire text of a node finish disposition. Pure.
fn disposition_text(disposition: NodeFinishDisposition) -> &'static str {
    match disposition {
        NodeFinishDisposition::Applied => "APPLIED",
        NodeFinishDisposition::AppliedWorkflowEnded => "APPLIED_WORKFLOW_ENDED",
        NodeFinishDisposition::AlreadyApplied => "ALREADY_APPLIED",
        NodeFinishDisposition::PendingAbsent => "PENDING_ABSENT",
        NodeFinishDisposition::Retained(SourceFailure::Absent) => "SOURCE_ABSENT",
        NodeFinishDisposition::Retained(SourceFailure::StateConflict) => "SOURCE_STATE_CONFLICT",
        NodeFinishDisposition::Retained(SourceFailure::VersionConflict) => {
            "SOURCE_VERSION_CONFLICT"
        }
        NodeFinishDisposition::Retained(SourceFailure::DigestMismatch) => "SOURCE_DIGEST_MISMATCH",
    }
}

impl PostgresBroker {
    /// Phase 2 of the finished workflow task `task_id`:
    /// `horsies_workflow_node_finish` in the form `form`, with the capacity
    /// wake of the node's queue when `wake` (delivered at the commit). The
    /// child starts that the transaction form owes build their children
    /// through `source`. Does not run the parent advances.
    ///
    /// The client family is checked once per handle before the first write.
    ///
    /// Errors: [`WorkflowError::Broker`] for a failed check, checkout or
    /// statement (nothing committed, or the outcome is unknown);
    /// [`WorkflowError::ChildSpecSource`] for a child build that `source`
    /// did not run (nothing committed); [`WorkflowError::StepContract`] for
    /// a row outside the contract (in the autocommit form the step
    /// committed); the refusals of a child tree (nothing committed).
    pub async fn finish_workflow_node(
        &self,
        task_id: Uuid,
        form: NodeFinishForm,
        wake: bool,
        source: &dyn ChildSpecSource,
    ) -> Result<NodeFinished, WorkflowError> {
        self.ensure_client_family_for_write()
            .await
            .map_err(WorkflowError::Broker)?;
        let (disposition, detail, parent_advances) = match form {
            NodeFinishForm::NoDependents => {
                let outcome =
                    node_finish_alone(self, task_id, wake, Dependents::NoneToRead).await?;
                (
                    outcome.disposition,
                    outcome.detail,
                    OwedParentAdvances::of(outcome.parent_advance),
                )
            }
            NodeFinishForm::DependentsWithoutSubworkflow => {
                let outcome = node_finish_alone(self, task_id, wake, Dependents::Read).await?;
                (
                    outcome.disposition,
                    outcome.detail,
                    OwedParentAdvances::of(outcome.parent_advance),
                )
            }
            NodeFinishForm::SubworkflowReachable | NodeFinishForm::FactsUnknown => {
                let committed =
                    node_finish_in_transaction(self, task_id, wake, ChildBuild::Client(source))
                        .await?;
                (
                    committed.disposition,
                    committed.detail,
                    committed.parent_advances,
                )
            }
        };
        Ok(NodeFinished {
            disposition: disposition_text(disposition),
            workflow_status: detail,
            parent_advances: parent_advances.child_workflow_ids().to_vec(),
        })
    }

    /// Advance the parent nodes above `child_workflow_ids`:
    /// `horsies_workflow_child_finish` with the child starts that each level
    /// owes (children from `source`), one transaction per level, while the
    /// levels name further ended children; at most one level per depth of a
    /// tree (`MAX_PARENT_ADVANCES`).
    ///
    /// The client family is checked once per handle before the first write.
    ///
    /// Errors: [`ParentAdvanceStopped`] at the level that failed (the levels
    /// before it committed), with every advance still owed. A failed client
    /// family check stops at the first level, with every advance owed.
    pub async fn advance_workflow_parents(
        &self,
        child_workflow_ids: &[Uuid],
        source: &dyn ChildSpecSource,
    ) -> Result<(), ParentAdvanceStopped> {
        let mut owed = OwedParentAdvances::default();
        for child_workflow_id in child_workflow_ids {
            owed.add(ParentAdvance::Owed {
                child_workflow_id: *child_workflow_id,
            });
        }
        let Some(first) = owed.child_workflow_ids().first().copied() else {
            return Ok(());
        };
        if let Err(error) = self.ensure_client_family_for_write().await {
            return Err(ParentAdvanceStopped {
                failed_child_workflow_id: first,
                still_owed: owed.child_workflow_ids().to_vec(),
                error: WorkflowError::Broker(error),
            });
        }
        advance_parents(
            self,
            CheckoutClass::Reserved,
            owed,
            ChildBuild::Client(source),
        )
        .await
        .map_err(|failure: ParentAdvanceFailure| ParentAdvanceStopped {
            failed_child_workflow_id: failure.failed_child_workflow_id,
            still_owed: failure.still_owed().child_workflow_ids().to_vec(),
            error: failure.error,
        })
    }
}

impl PostgresBroker {
    /// One phase-2 recovery pass of the reaper (Python
    /// `drive_phase2_recovery`, `python/horsies/core/workflows/phase2_recovery.py`):
    /// the phase-2 records older than `grace_ms` and under
    /// `quarantine_after_attempts`, oldest first, at most `max_rows`, each in
    /// its own transaction (`horsies_workflow_phase2_recover` with the child
    /// starts it owes, children from `source`), then the parent advances of
    /// each. A persistent failure of a record is counted toward the bound; a
    /// child build that did not run, or an interrupt, is not.
    ///
    /// The client family is checked once per handle before the first write.
    ///
    /// Errors: a failed check, discovery or count (nothing recovered), and a
    /// grace or bound above the integer range (`WorkflowError::Validation`).
    pub async fn recover_workflow_phase2(
        &self,
        grace_ms: u64,
        max_rows: i64,
        quarantine_after_attempts: u32,
        source: &dyn ChildSpecSource,
    ) -> Result<Phase2RecoverySummary, WorkflowError> {
        self.ensure_client_family_for_write()
            .await
            .map_err(WorkflowError::Broker)?;
        drive_phase2_recovery_with(
            self,
            ChildBuild::Client(source),
            grace_ms,
            max_rows,
            quarantine_after_attempts,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_disposition_has_its_wire_text() {
        assert_eq!(disposition_text(NodeFinishDisposition::Applied), "APPLIED");
        assert_eq!(
            disposition_text(NodeFinishDisposition::AppliedWorkflowEnded),
            "APPLIED_WORKFLOW_ENDED"
        );
        assert_eq!(
            disposition_text(NodeFinishDisposition::Retained(
                SourceFailure::DigestMismatch
            )),
            "SOURCE_DIGEST_MISMATCH"
        );
        for text in [
            "APPLIED",
            "APPLIED_WORKFLOW_ENDED",
            "ALREADY_APPLIED",
            "PENDING_ABSENT",
            "SOURCE_ABSENT",
            "SOURCE_STATE_CONFLICT",
            "SOURCE_VERSION_CONFLICT",
            "SOURCE_DIGEST_MISMATCH",
        ] {
            let disposition = <NodeFinishDisposition as crate::workflow_engine::steps::StepDisposition>::from_text(text)
                .expect("a disposition of the function");
            assert_eq!(disposition_text(disposition), text);
        }
    }
}
