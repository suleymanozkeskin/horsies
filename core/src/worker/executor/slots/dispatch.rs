//! What the executor gets for each claimed task, and the one closed type
//! for how an attempt ended (Python executor design, sections 5.5 and 8.7).

use std::fmt;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::attempt_writes::RetryPlacement;
use crate::core::task::error::OperationalErrorCode;
use crate::events::{AttemptNumber, AttemptStart, RunTrace};
use crate::telemetry::trace_carrier::StoredTraceId;
use crate::worker::executor::protocol::{
    BodyEnd, FailureDetail, ProcessId, ResultEnvelopeText, RunEnd, StopReason, TaskInputText,
    TaskTimeoutMs,
};
use crate::worker::executor::supervisor::ChildLost;
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

impl RetryFacts {
    /// The attempt that this claim runs ([`attempt_of`]).
    pub fn attempt(&self) -> AttemptNumber {
        attempt_of(self.retry_count)
    }
}

/// The attempt that a claim of a row with `retry_count` runs: the count
/// plus 1. The claim refuses a row with a count below 0; such a count here
/// reads as 0. Pure and total: a count of 0 to `i32::MAX` gives 1 to
/// `MAX_ATTEMPT`.
pub fn attempt_of(retry_count: i32) -> AttemptNumber {
    let from_zero = u32::try_from(retry_count).unwrap_or(0);
    match AttemptNumber::try_from(from_zero + 1) {
        Ok(attempt) => attempt,
        Err(outside) => unreachable!(
            "a retry count of 0 to i32::MAX gives an attempt of 1 to MAX_ATTEMPT: {outside}"
        ),
    }
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
    /// The trace carrier that the send stored on the row. The run frame
    /// carries it to the child.
    pub trace: RunTrace,
    /// The trace id that the send stored on the row, for the log records of
    /// the attempt.
    pub trace_id: StoredTraceId,
}

impl ClaimedTask {
    /// The log fields of this attempt.
    pub fn log(&self) -> AttemptLog {
        AttemptLog {
            attempt: self.retry.attempt(),
            trace_id: self.trace_id,
        }
    }
}

/// The log fields of one attempt besides its task id: the attempt number
/// and the trace id of the send. Each log record of the worker parent that
/// names the task of an attempt carries both (`attempt = log.attempt.get()`,
/// `trace_id = log.trace_id.log_field()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptLog {
    pub attempt: AttemptNumber,
    pub trace_id: StoredTraceId,
}

#[cfg(test)]
impl AttemptLog {
    /// The log fields of a first attempt whose row stores no trace id.
    pub(crate) const FIRST_UNTRACED: Self = Self {
        attempt: AttemptNumber::SMALLEST,
        trace_id: StoredTraceId::NoTraceId,
    };
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

/// How a shared async run that passed its deadline ended: in its child, read
/// from the child's frames, or at a kill of its child by the worker. Stored
/// as `cancel_end` in the `data` of its `TASK_TIMEOUT` error, for Python and
/// Rust children alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelEnd {
    /// The task and its work ended within the cancel grace after the cancel
    /// at the deadline: the child sent no `second_cancel`.
    ReturnedAfterSignal,
    /// Something of the run was alive after the cancel grace: the child sent
    /// `second_cancel` and stopped it (a Rust child drops the future).
    DroppedAfterGrace,
    /// The run ended when the worker killed its child, before the child
    /// reported its end.
    KilledWithChild,
}

impl CancelEnd {
    /// The stored value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReturnedAfterSignal => "RETURNED_AFTER_SIGNAL",
            Self::DroppedAfterGrace => "DROPPED_AFTER_GRACE",
            Self::KilledWithChild => "KILLED_WITH_CHILD",
        }
    }
}

/// What the attempt of a run that passed its deadline records about the
/// cancel at that deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutCancel {
    /// An isolated or a shared thread run: the deadline sends no cancel.
    NoSignal,
    /// A shared async run: how it ended after the cancel at its deadline.
    Ended(CancelEnd),
}

/// What the body of a run returned, and the start of its attempt: the time
/// that the start write returned to the child. A body that ran had its
/// start written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyReturn {
    pub end: BodyEnd,
    pub started_at: DateTime<Utc>,
}

/// How one attempt ended in the executor. Each submitted task gets exactly
/// one. The outcome writer maps it to one statement.
///
/// The three ends that a child reports (`StoppedBeforeRunning`, `Result`,
/// `WorkerFailure`) come from its result frame
/// ([`AttemptEnd::reported`]). A body that returned before its child was
/// lost also ends as `Result` or `WorkerFailure`, with what it returned
/// ([`AttemptEnd::returned`]). `start` is the start that the child reported
/// for the attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptEnd {
    /// The task never reached a child; the row goes back to the queue.
    NotDispatched(NotRunCause),
    /// The child stopped before it set the row RUNNING: the attempt did not
    /// start.
    StoppedBeforeRunning(StopReason),
    /// The task produced a result envelope: a value or a task error.
    Result {
        envelope: ResultEnvelopeText,
        start: AttemptStart,
    },
    /// The worker code of the child failed, not the task. `start` is
    /// `NotReported` when the failure came before the start write returned.
    WorkerFailure {
        detail: FailureDetail,
        start: AttemptStart,
    },
    /// The deadline decided. A result that came later was dropped. `start`
    /// is the start that a shared child reported when it ended the run at
    /// its deadline; a timeout at a kill of the child has none
    /// (`NotReported`).
    TimedOut {
        timeout: TaskTimeoutMs,
        retry: RetryPlacement,
        cancel: TimeoutCancel,
        start: AttemptStart,
    },
    /// A blocked event loop ended the run (a shared async child).
    KilledForLoop {
        code: LoopKillCode,
        retry: RetryPlacement,
        reason: FailureDetail,
    },
    /// The child was lost with its exit seen, a kill at the drain deadline
    /// included: the crash path (`WORKER_CRASHED`). The reason is built by
    /// [`AttemptEnd::of_lost_child`].
    Crashed { reason: FailureDetail },
    /// The child was killed and its exit was not seen. Nothing is written:
    /// stale recovery or the claim lease decides the row.
    EndUnknown { pid: ProcessId },
}

/// The start of the crash reason of a child lost with no result.
pub const LOST_CHILD_REASON_PREFIX: &str = "Worker child lost: ";

impl AttemptEnd {
    /// The end that a result frame reports: `end`, with the start `start`
    /// of the frame. The frame rules give a stop before running no start
    /// (`NotReported`), so its end holds none. Pure.
    pub fn reported(end: RunEnd, start: AttemptStart) -> Self {
        match end {
            RunEnd::StoppedBeforeRunning(reason) => Self::StoppedBeforeRunning(reason),
            RunEnd::Result(envelope) => Self::Result { envelope, start },
            RunEnd::WorkerFailure(detail) => Self::WorkerFailure { detail, start },
        }
    }

    /// The end of a run whose body returned `returned` before its child
    /// ended, with the start of the attempt that the child reported. Pure.
    pub fn returned(returned: BodyReturn) -> Self {
        let start = AttemptStart::Reported {
            started_at: returned.started_at,
        };
        match returned.end {
            BodyEnd::Result(envelope) => Self::Result { envelope, start },
            BodyEnd::WorkerFailure(detail) => Self::WorkerFailure { detail, start },
        }
    }

    /// The crash end of a run whose child `lost` ended with no result, once
    /// its exit was seen. The reason is Python's
    /// `f'Worker child lost: {lost.describe()}'`; the outcome writer stores
    /// it as the error message and as the attempt's failed reason. Pure.
    pub fn of_lost_child(lost: &ChildLost) -> Self {
        Self::Crashed {
            reason: FailureDetail::cut(&format!("{LOST_CHILD_REASON_PREFIX}{lost}")),
        }
    }
}

/// The terminal state that an applied terminal write set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalState {
    /// A success write: the task is `COMPLETED`.
    Completed,
    /// A failed-attempt write with no retry: the task is `FAILED`.
    Failed,
}

/// What an applied write of an attempt end left the row in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrittenEnd {
    /// The task is terminal. A replay that found its own committed write
    /// reads as the state of that write: a failed-attempt replay finds a
    /// committed retry as a lost claim, not as its own write.
    Terminal(TerminalState),
    /// The failed attempt scheduled a retry: the task is `PENDING` with
    /// `retry_count`, due at `next_retry_at`.
    RetryScheduled {
        retry_count: i32,
        next_retry_at: DateTime<Utc>,
    },
    /// The claim went back to the queue: the task is `PENDING` with the same
    /// retry count.
    ClaimReleased,
}

/// How the write of one attempt end settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    /// The statement applied (or a replay found its own write), and left
    /// the row as its [`WrittenEnd`] says.
    Written(WrittenEnd),
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
