//! What the executor gets for each claimed task, and the one closed type
//! for how an attempt ended (Python executor design, sections 5.5 and 8.7).

use std::fmt;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::attempt_writes::RetryPlacement;
use crate::core::task::error::OperationalErrorCode;
use crate::worker::executor::protocol::{
    FailureDetail, ProcessId, RunEnd, TaskInputText, TaskTimeoutMs,
};
use crate::worker::retry::StoredOptions;

/// The execution class of a task row: the class that the claim reads, the
/// budget that claims it and the kind of child that runs it. It is the
/// stored class of the core, so a claimed row needs no conversion.
pub use crate::core::task::Execution as ExecutionClass;

/// Whether a task is a node of a workflow: one type with the frames. A
/// workflow task owes phase 2 after its terminal write.
pub use crate::worker::executor::protocol::TaskKind;

/// The retry facts of a claimed row. They cannot change inside one claim
/// generation: only the two retry writers change `retry_count`, and both end
/// the generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryFacts {
    pub retry_count: i32,
    pub max_retries: i32,
}

/// One claimed row, as the executor needs it. The claim returns every
/// field; the stored options are read once, here, into typed values. The
/// task input travels apart ([`Claimed`]), so the executor can drop it once
/// the frame is sent while the outcome writer keeps this.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedTask {
    pub task_id: Uuid,
    pub task_name: String,
    pub queue_name: String,
    /// The claim generation: the `claimed_at` the claim set. Every write of
    /// the attempt fences on it.
    pub claimed_at: DateTime<Utc>,
    /// The class of the row. The budget that claimed it decides the child.
    pub execution: ExecutionClass,
    pub kind: TaskKind,
    pub retry: RetryFacts,
    pub options: StoredOptions,
}

/// A claimed row and its input.
#[derive(Debug, Clone, PartialEq)]
pub struct Claimed {
    pub task: ClaimedTask,
    pub input: TaskInputText,
}

/// Why a task never reached a child. Its row is still CLAIMED.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotRunCause {
    /// The worker is stopping.
    PoolStopping,
    /// No child could start.
    PoolFailed,
    /// Too many tasks wait for a slot (the pending limit).
    QueueFull,
    /// The task's route names a child kind this worker does not run.
    NoChildOfItsKind,
    /// The runner heartbeats failed closed: no beat would cover the task.
    RunnerBeatsFailedClosed,
    /// The worker stopped while the row waited for its queue cap (soft-cap
    /// mode; `worker::queue_cap_hold`).
    HeldForQueueCap,
}

impl fmt::Display for NotRunCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::PoolStopping => "the worker is stopping",
            Self::PoolFailed => "no child could start",
            Self::QueueFull => "too many tasks wait for a slot",
            Self::NoChildOfItsKind => "this worker runs no child of the task's kind",
            Self::RunnerBeatsFailedClosed => "the runner heartbeats of the worker failed closed",
            Self::HeldForQueueCap => "the worker stopped while the row waited for its queue cap",
        };
        f.write_str(text)
    }
}

/// The code of a run that a blocked event loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopKillCode {
    /// The run held the blocked loop, before its deadline.
    TaskBlockedLoop,
    /// Another run held the loop; this run was beside it.
    NeighbourBlockedLoop,
}

impl LoopKillCode {
    pub fn code(self) -> OperationalErrorCode {
        match self {
            Self::TaskBlockedLoop => OperationalErrorCode::TaskBlockedLoop,
            Self::NeighbourBlockedLoop => OperationalErrorCode::NeighbourBlockedLoop,
        }
    }
}

/// How a shared async run that passed its deadline ended in its child, read
/// from the child's frames. Stored as `cancel_end` in the `data` of its
/// `TASK_TIMEOUT` error, for Python and Rust children alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelEnd {
    /// The task and its work ended within the cancel grace after the cancel
    /// at the deadline: the child sent no `second_cancel`.
    ReturnedAfterSignal,
    /// Something of the run was alive after the cancel grace: the child sent
    /// `second_cancel` and stopped it (a Rust child drops the future).
    DroppedAfterGrace,
}

impl CancelEnd {
    /// The stored value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReturnedAfterSignal => "RETURNED_AFTER_SIGNAL",
            Self::DroppedAfterGrace => "DROPPED_AFTER_GRACE",
        }
    }
}

/// What the attempt of a run that passed its deadline records about the
/// cancel at that deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutCancel {
    /// An isolated or a shared thread run: the deadline sends no cancel.
    NoSignal,
    /// A shared async run whose child reported its end.
    Ended(CancelEnd),
    /// A shared async run that ended when the worker killed its child. Its
    /// `TASK_TIMEOUT` data has no `cancel_end`.
    EndedByKill,
}

/// How one attempt ended in the executor. Each submitted task gets exactly
/// one. The outcome writer maps it to one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptEnd {
    /// The task never reached a child; the row goes back to the queue.
    NotDispatched(NotRunCause),
    /// The child reported how the run ended: a result envelope, a stop
    /// before RUNNING, or a failure of its worker code. A body that returned
    /// before its child was lost also ends here, with what it returned.
    Reported(RunEnd),
    /// The deadline decided. A result that came later was dropped.
    TimedOut {
        timeout: TaskTimeoutMs,
        retry: RetryPlacement,
        cancel: TimeoutCancel,
    },
    /// A blocked event loop ended the run (a shared async child).
    KilledForLoop {
        code: LoopKillCode,
        retry: RetryPlacement,
        reason: FailureDetail,
    },
    /// The child was lost with its exit seen, or killed at the drain
    /// deadline: the crash path (`WORKER_CRASHED`).
    Crashed { reason: FailureDetail },
    /// The child was killed and its exit was not seen. Nothing is written:
    /// stale recovery or the claim lease decides the row.
    EndUnknown { pid: ProcessId },
}

/// How the write of one attempt end settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    /// The statement applied (or a replay found its own write).
    Written,
    /// The child wrote what the case needs; nothing was owed.
    NothingOwed,
    /// The statement found the row not held by this claim and wrote nothing.
    NotHeld,
    /// Nothing was written: stale recovery or the lease decides the row.
    LeftToRecovery,
    /// Every retry of the write failed with an error a retry can clear; the
    /// row waits for recovery.
    RetriesExhausted,
    /// The write failed with an error that no retry can clear (the database
    /// refused the statement, or the value did not encode); the row waits
    /// for recovery.
    WriteFailed,
}
