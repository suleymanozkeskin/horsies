//! The terminal transitions that run through `broker::terminalization`, as
//! eleven commands. The success of a workflow task, a failed attempt, a stale
//! RUNNING recovery and the administrative cancel have writers of their own
//! (`broker::attempt_writes`, `broker::stale_recovery`,
//! `broker::lifecycle_operations`).
//!
//! What this vocabulary buys is that states which cannot occur cannot be
//! written down: a batch fence cannot be attached to a single-task command, a
//! pause disposition cannot be attached to a cancellation, and a
//! workflow-scoped command cannot name the workflow status it requires — the
//! variant means it. Each variant holds exactly the data its guard needs and
//! nothing that could contradict it.
//!
//! Payload obeys the same rule and is per-variant. A variant carries a
//! result, an error code, or a failure reason only where its statement takes
//! that value from the caller; the operations that own those values as
//! literals — a workflow pause always writes the same reason — carry none,
//! and cannot be handed a payload the database would silently ignore.
//!
//! One enum, not one per cardinality. Cardinality is variant identity here —
//! a separate batch enum would encode shape twice and let the two encodings
//! drift.

use crate::core::types::status::TaskStatus;
use uuid::Uuid;

use super::fences::{OwnedClaim, OwnedClaimBatch, TerminalFence, WorkerOwned};
use super::placement::AttemptPlacement;
use super::LifecycleValidationError;

/// A discovery batch's bound, validated at construction.
///
/// The database function raises on the same precondition; validating here
/// reports the mistake at the call site that made it, before a connection is
/// ever involved. The bound exists to keep one pass from committing an
/// unbounded notification burst.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchSize(i32);

impl BatchSize {
    pub fn new(batch_size: i32) -> Result<Self, LifecycleValidationError> {
        if batch_size <= 0 {
            return Err(LifecycleValidationError::NonPositiveBatchSize { got: batch_size });
        }
        Ok(Self(batch_size))
    }

    pub fn get(self) -> i32 {
        self.0
    }
}

/// The typed vocabulary of terminal transitions. One variant per operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalizationCommand {
    /// Success as one statement: locks the row, writes the attempt from the
    /// locked row's own context, transitions, and wakes queue capacity — one
    /// round trip. The fence lives in the locking read ahead of the update,
    /// not in the update's predicate. The function builds the capacity wake
    /// from the locked row: channel `task_queue_<queue>`, payload
    /// `capacity`. The attempt row carries the phase record, with the
    /// placement that the worker observed.
    CompleteTaskFused {
        task_id: Uuid,
        fence: OwnedClaim,
        result_json: String,
        placement: AttemptPlacement,
    },

    /// A claimed task whose deadline passed before user code started.
    ///
    /// Fences on worker ownership but deliberately not on claim generation:
    /// once the deadline has passed, expiry is the correct outcome for
    /// whichever generation holds the row.
    ExpireOwnedClaim {
        task_id: Uuid,
        fence: WorkerOwned,
        result_json: String,
        error_code: String,
    },

    /// Unclaimed tasks whose deadline passed, in bounded batches.
    ///
    /// Batched rather than done in one statement because a mass expiry
    /// commits two notifications per row at once and overflows listener
    /// queues. Rows a concurrent claim holds are skipped; the claim re-checks
    /// the deadline itself, so the race resolves the same way either way.
    ExpirePendingTasks {
        batch_size: BatchSize,
        result_json: String,
        error_code: String,
    },

    /// One workflow task this worker holds that can never progress.
    ///
    /// An orphan has no workflow-task row in a runnable state, so its
    /// transition to running can never succeed. Left alone it holds a claim
    /// forever.
    CancelOwnedOrphan { task_id: Uuid, fence: OwnedClaim },

    /// The same condition, swept in bounded batches across all workers.
    CancelOrphanedTasks { batch_size: BatchSize },

    /// One claimed node abandoned because its workflow paused.
    ///
    /// The backing row is abandoned so resume can enqueue a fresh one; the
    /// node is returned to ready and detached from this task. Pause is
    /// resumable, which is why the node is readied rather than skipped — the
    /// disposition follows from which command this is, and cannot be set to
    /// disagree with it.
    AbandonOwnedNode { task_id: Uuid, fence: OwnedClaim },

    /// The same, for a batch this worker just claimed.
    AbandonOwnedNodes { fence: OwnedClaimBatch },

    /// Every claimed node under workflows that are paused right now.
    ///
    /// Reaches claims other workers hold, which is the point: a claim taken
    /// after the pause is exactly what must be abandoned.
    AbandonNodesOfPausedWorkflows { workflow_ids: Vec<Uuid> },

    /// One node cancelled because its workflow was cancelled.
    ///
    /// Also accepts a row already requeued to pending. That row carries no
    /// claim, so there is no generation to fence — and refusing it would
    /// leave a task of a cancelled workflow live. The workflow's cancellation
    /// is the guard there, and it is final, so it cannot go stale the way a
    /// pause can.
    CancelOwnedNode {
        task_id: Uuid,
        fence: OwnedClaim,
        accepts_requeued_pending: bool,
    },

    /// The same, for a batch this worker just claimed.
    CancelOwnedNodes { fence: OwnedClaimBatch },

    /// Nodes enqueued but not yet started under a cancelled workflow.
    ///
    /// Reaches a backing row that is briefly running: user code starts only
    /// after the node's own transition to running, so a node still enqueued
    /// has not begun executing whatever its task row says.
    CancelNodesOfCancelledWorkflow { workflow_ids: Vec<Uuid> },
}

impl TerminalizationCommand {
    /// The status the command writes. Exhaustive over the enum.
    pub fn target_status(&self) -> TaskStatus {
        match self {
            Self::CompleteTaskFused { .. } => TaskStatus::Completed,
            Self::ExpireOwnedClaim { .. } | Self::ExpirePendingTasks { .. } => TaskStatus::Expired,
            Self::CancelOwnedOrphan { .. }
            | Self::CancelOrphanedTasks { .. }
            | Self::AbandonOwnedNode { .. }
            | Self::AbandonOwnedNodes { .. }
            | Self::AbandonNodesOfPausedWorkflows { .. }
            | Self::CancelOwnedNode { .. }
            | Self::CancelOwnedNodes { .. }
            | Self::CancelNodesOfCancelledWorkflow { .. } => TaskStatus::Cancelled,
        }
    }

    /// The command's claim-ownership guard, for outcome reporting.
    ///
    /// `None` means the statement carries no ownership predicate. Three kinds
    /// of command have none, for three different reasons: batch expiry acts
    /// across workers because the owner is irrelevant; orphan sweeping acts on
    /// rows whose linkage, not their claim, is the defect; and the
    /// workflow-scoped commands must reach claims other workers hold, so an
    /// ownership predicate would skip their whole purpose.
    pub fn fence(&self) -> Option<TerminalFence<'_>> {
        match self {
            Self::CompleteTaskFused { fence, .. } => Some(TerminalFence::OwnedClaim(fence)),
            Self::ExpireOwnedClaim { fence, .. } => Some(TerminalFence::WorkerOwned(fence)),
            Self::CancelOwnedOrphan { fence, .. }
            | Self::AbandonOwnedNode { fence, .. }
            | Self::CancelOwnedNode { fence, .. } => Some(TerminalFence::OwnedClaim(fence)),
            Self::AbandonOwnedNodes { fence } | Self::CancelOwnedNodes { fence } => {
                Some(TerminalFence::OwnedClaimBatch(fence))
            }
            Self::ExpirePendingTasks { .. }
            | Self::CancelOrphanedTasks { .. }
            | Self::AbandonNodesOfPausedWorkflows { .. }
            | Self::CancelNodesOfCancelledWorkflow { .. } => None,
        }
    }
}
