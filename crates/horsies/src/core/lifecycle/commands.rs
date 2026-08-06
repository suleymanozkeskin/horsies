//! Every way a task can be moved to a terminal status, as fifteen commands.
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

use super::fences::{
    CallerHoldsRowLock, OwnedClaim, OwnedClaimBatch, PriorLockedRead, TerminalFence, WorkerOwned,
};
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
    /// Success for a task whose row the caller already locked and read.
    ///
    /// The attempt row is written separately by the caller in the same
    /// transaction, from the context that locked read produced.
    CompleteLockedTask {
        task_id: String,
        fence: PriorLockedRead,
        result_json: String,
    },

    /// Success as one statement: locks the row, writes the attempt from the
    /// locked row's own context, transitions, and wakes queue capacity — one
    /// round trip. The fence lives in the locking read ahead of the update,
    /// not in the update's predicate.
    CompleteTaskFused {
        task_id: String,
        fence: OwnedClaim,
        result_json: String,
        notify_channel: String,
        notify_payload: String,
    },

    /// Failure for a task whose row the caller already locked and read.
    ///
    /// Covers both application failure and worker-level failure; they differ
    /// only in whether a failure reason is carried. `failed_reason` is
    /// assigned unconditionally: the terminal writer owns the complete
    /// final-attempt summary, and `None` clears any value a requeued earlier
    /// attempt left on the row. Per-attempt history lives in
    /// `horsies_task_attempts`.
    FailLockedTask {
        task_id: String,
        fence: PriorLockedRead,
        result_json: String,
        error_code: Option<String>,
        failed_reason: Option<String>,
    },

    /// Failure for a task whose runner stopped reporting.
    ///
    /// Cross-worker by design: the guard is staleness, not ownership, because
    /// the worker that held this task is by hypothesis not answering. Retry
    /// policy is evaluated before this command is built — a task that will be
    /// retried is never terminalized.
    FailStaleTask {
        task_id: String,
        stale_after_ms: i32,
        finalizing_stale_after_ms: i32,
        result_json: String,
        error_code: String,
        failed_reason: String,
    },

    /// A claimed task whose deadline passed before user code started.
    ///
    /// Fences on worker ownership but deliberately not on claim generation:
    /// once the deadline has passed, expiry is the correct outcome for
    /// whichever generation holds the row.
    ExpireOwnedClaim {
        task_id: String,
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

    /// Operator cancellation of a plain task, under the caller's row lock.
    ///
    /// The permitted source statuses are the operator's choice — whether a
    /// task already running may be cancelled is a decision the caller makes,
    /// and the command carries it explicitly rather than leaving it implicit
    /// in a caller-supplied predicate.
    CancelLockedTask {
        task_id: String,
        fence: CallerHoldsRowLock,
        permitted_source_statuses: Vec<TaskStatus>,
    },

    /// One workflow task this worker holds that can never progress.
    ///
    /// An orphan has no workflow-task row in a runnable state, so its
    /// transition to running can never succeed. Left alone it holds a claim
    /// forever.
    CancelOwnedOrphan { task_id: String, fence: OwnedClaim },

    /// The same condition, swept in bounded batches across all workers.
    CancelOrphanedTasks { batch_size: BatchSize },

    /// One claimed node abandoned because its workflow paused.
    ///
    /// The backing row is abandoned so resume can enqueue a fresh one; the
    /// node is returned to ready and detached from this task. Pause is
    /// resumable, which is why the node is readied rather than skipped — the
    /// disposition follows from which command this is, and cannot be set to
    /// disagree with it.
    AbandonOwnedNode { task_id: String, fence: OwnedClaim },

    /// The same, for a batch this worker just claimed.
    AbandonOwnedNodes { fence: OwnedClaimBatch },

    /// Every claimed node under workflows that are paused right now.
    ///
    /// Reaches claims other workers hold, which is the point: a claim taken
    /// after the pause is exactly what must be abandoned.
    AbandonNodesOfPausedWorkflows { workflow_ids: Vec<String> },

    /// One node cancelled because its workflow was cancelled.
    ///
    /// Also accepts a row already requeued to pending. That row carries no
    /// claim, so there is no generation to fence — and refusing it would
    /// leave a task of a cancelled workflow live. The workflow's cancellation
    /// is the guard there, and it is final, so it cannot go stale the way a
    /// pause can.
    CancelOwnedNode {
        task_id: String,
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
    CancelNodesOfCancelledWorkflow { workflow_ids: Vec<String> },
}

impl TerminalizationCommand {
    /// The status the command writes. Exhaustive over the enum.
    pub fn target_status(&self) -> TaskStatus {
        match self {
            Self::CompleteLockedTask { .. } | Self::CompleteTaskFused { .. } => {
                TaskStatus::Completed
            }
            Self::FailLockedTask { .. } | Self::FailStaleTask { .. } => TaskStatus::Failed,
            Self::ExpireOwnedClaim { .. } | Self::ExpirePendingTasks { .. } => TaskStatus::Expired,
            Self::CancelLockedTask { .. }
            | Self::CancelOwnedOrphan { .. }
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
    /// of command have none, for three different reasons: stale recovery and
    /// batch expiry act across workers because the owner is by hypothesis
    /// absent or irrelevant; orphan sweeping acts on rows whose linkage, not
    /// their claim, is the defect; and the workflow-scoped commands must
    /// reach claims other workers hold, so an ownership predicate would skip
    /// their whole purpose.
    pub fn fence(&self) -> Option<TerminalFence<'_>> {
        match self {
            Self::CompleteLockedTask { fence, .. } | Self::FailLockedTask { fence, .. } => {
                Some(TerminalFence::PriorLockedRead(fence))
            }
            Self::CompleteTaskFused { fence, .. } => Some(TerminalFence::OwnedClaim(fence)),
            Self::ExpireOwnedClaim { fence, .. } => Some(TerminalFence::WorkerOwned(fence)),
            Self::CancelLockedTask { fence, .. } => Some(TerminalFence::CallerHoldsRowLock(fence)),
            Self::CancelOwnedOrphan { fence, .. }
            | Self::AbandonOwnedNode { fence, .. }
            | Self::CancelOwnedNode { fence, .. } => Some(TerminalFence::OwnedClaim(fence)),
            Self::AbandonOwnedNodes { fence } | Self::CancelOwnedNodes { fence } => {
                Some(TerminalFence::OwnedClaimBatch(fence))
            }
            Self::FailStaleTask { .. }
            | Self::ExpirePendingTasks { .. }
            | Self::CancelOrphanedTasks { .. }
            | Self::AbandonNodesOfPausedWorkflows { .. }
            | Self::CancelNodesOfCancelledWorkflow { .. } => None,
        }
    }
}
