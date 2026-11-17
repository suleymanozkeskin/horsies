//! Phase 2 of a finished workflow task, for a caller that does not hold the
//! phase-1 node facts: the node finish step of the baseline
//! (`horsies_workflow_node_finish`), its owed child starts, and the parent
//! advances after the commit (`workflow_engine::steps`).

use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::config::retention::RetentionConfig;
use crate::core::WorkflowSpecRegistry;
use crate::db::CheckoutClass;
use crate::workflow_engine::error::WorkflowError;
pub use crate::workflow_engine::steps::SourceFailure;
use crate::workflow_engine::steps::{
    advance_parents, node_finish_in_transaction, ChildBuild, NodeFinishDisposition,
};

/// What [`on_workflow_task_complete`] did with the phase-2 record of a finished workflow task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowTaskCompletion {
    /// The node took the task's terminal status and result, and the record is deleted. The workflow is
    /// live: its failure policy, the promotion of the node's dependents, the owed child starts and the
    /// completion check ran.
    Applied,
    /// The node took the task's terminal status and result, and the record is deleted. The workflow had
    /// ended (COMPLETED, FAILED, CANCELLED or EXPIRED): nothing was promoted, and the workflow and the
    /// other nodes keep their statuses.
    AppliedWorkflowEnded,
    /// The node was terminal already (a replay): nothing written.
    AlreadyApplied,
    /// No phase-2 record, and the node is not terminal: nothing written.
    PendingAbsent,
}

/// Why [`on_workflow_task_complete`] did not finish.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowTaskCompletionError {
    /// The node finish did not commit: a failed checkout, `BEGIN`, step, child start or `COMMIT`, a row
    /// outside the contract of the step, or a refused host build of an owed child. Nothing committed,
    /// except after a `COMMIT` whose reply was lost (`OutcomeUnknown` in the broker error). The phase-2
    /// record stays for a retry or for phase-2 recovery. Retry only when `error` may clear (a retryable
    /// broker or database error).
    #[error("node finish of workflow task {task_id} failed: {error}")]
    NodeFinish { task_id: Uuid, error: WorkflowError },
    /// The stored result of the task did not pass its check (`check`): nothing was written, and the
    /// phase-2 record is kept. Phase-2 recovery counts it and quarantines it at its bound. Not retryable.
    #[error(
        "phase-2 record of workflow task {task_id} kept: the stored result did not pass its check \
         ({check:?})"
    )]
    RecordKept { task_id: Uuid, check: SourceFailure },
    /// The node finish committed. The advance of the parent node above `child_workflow_id`, a child
    /// workflow that the step ended, failed; the levels before it committed, and that level wrote nothing
    /// or its outcome is unknown. Workflow recovery (case 1.6: a terminal child under a live parent node)
    /// advances the rest.
    #[error(
        "workflow task {task_id} finished; the advance of the parent of child workflow \
         {child_workflow_id} failed: {error}"
    )]
    ParentAdvance {
        task_id: Uuid,
        child_workflow_id: Uuid,
        error: WorkflowError,
    },
}

/// Phase 2 of a finished workflow task, in the form the worker runs when the phase-1 node facts are not
/// at hand: `BEGIN`; `horsies_workflow_node_finish(task_id, FALSE, 'READ')`; one
/// `horsies_workflow_child_start` per owed child start; `COMMIT`. Then the parent advances that the step
/// owes (`advance_parents`, one transaction per level). Every statement runs on the main core pool of
/// `broker` with the worker reserve ([`CheckoutClass::Reserved`]), as the worker's phase 2.
///
/// Phase 1 (the terminalization) stored the task's result and its phase-2 record. The node takes its
/// status from the record kind (COMPLETED for a completed task, FAILED for the other kinds) and its result
/// from the stored history row, whatever the status of its workflow. An ended workflow promotes nothing
/// (Python `COMPLETE_WORKFLOW_TASK_SQL`, `core/workflows/sql.py:418-465`, run by
/// `on_workflow_task_complete`, `core/workflows/engine.py:1746-1843`). The step sends no capacity wake
/// (Python's default `NoQueueWake`). Does not retry.
///
/// Returns: [`WorkflowTaskCompletion`]. Errors: [`WorkflowTaskCompletionError`], with the state that
/// stays after each.
pub async fn on_workflow_task_complete(
    broker: &PostgresBroker,
    task_id: Uuid,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<WorkflowTaskCompletion, WorkflowTaskCompletionError> {
    on_workflow_task_complete_with(
        broker,
        task_id,
        ChildBuild::Registry {
            registry,
            retention,
        },
    )
    .await
}

/// [`on_workflow_task_complete`] with the children of the owed child starts
/// built by `build`. Outcomes and errors as [`on_workflow_task_complete`].
pub(crate) async fn on_workflow_task_complete_with(
    broker: &PostgresBroker,
    task_id: Uuid,
    build: ChildBuild<'_>,
) -> Result<WorkflowTaskCompletion, WorkflowTaskCompletionError> {
    let committed = node_finish_in_transaction(broker, task_id, false, build)
        .await
        .map_err(|error| WorkflowTaskCompletionError::NodeFinish { task_id, error })?;
    let completion = match committed.disposition {
        NodeFinishDisposition::Applied => WorkflowTaskCompletion::Applied,
        NodeFinishDisposition::AppliedWorkflowEnded => WorkflowTaskCompletion::AppliedWorkflowEnded,
        NodeFinishDisposition::AlreadyApplied => WorkflowTaskCompletion::AlreadyApplied,
        NodeFinishDisposition::PendingAbsent => WorkflowTaskCompletion::PendingAbsent,
        NodeFinishDisposition::Retained(check) => {
            return Err(WorkflowTaskCompletionError::RecordKept { task_id, check });
        }
    };
    advance_parents(
        broker,
        CheckoutClass::Reserved,
        committed.parent_advances,
        build,
    )
    .await
    .map_err(|failure| WorkflowTaskCompletionError::ParentAdvance {
        task_id,
        child_workflow_id: failure.failed_child_workflow_id,
        error: failure.error,
    })?;
    Ok(completion)
}

#[cfg(test)]
mod task_complete_tests;
