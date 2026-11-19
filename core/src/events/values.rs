//! The bounded values and the closed types that event fields hold.
//!
//! Each closed type derives `EventSchema`. The core's own closed types that
//! events hold as they are derive it where they are defined: `Execution`,
//! `StopReason`, `ShortageErrno`, and the metric label sets that an event
//! field shares (`ReleaseReason`, `ReplaceReason`, `RecoveryAction` of
//! `telemetry::labels`). Pure: no I/O.

use chrono::{DateTime, Utc};

use super::schema::{
    wire_text, CodeUnion, DecodeRefusal, EncodeRefusal, EventValue, ValueForm, WireValue,
};
use crate::broker::ClearableBootCause;
use crate::catalog::CodeEnum;
use crate::core::boot_failure::ShortageErrno;
use crate::core::config::bounded::BoundedU32;
use crate::core::config::worker_profile::{
    MAX_ISOLATED_CHILDREN, MAX_SHARED_ASYNC_CHILDREN, MAX_SHARED_THREAD_CHILDREN,
};
use crate::core::{
    BuiltInTaskCode, ContractCode, OperationalErrorCode, OutcomeCode, RetrievalCode, TaskErrorCode,
};
use crate::telemetry::labels::ReleaseReason;
use crate::worker::executor::protocol::{BoundedText, ProcessId};
use crate::worker::executor::slots::dispatch::NotRunCause;
use crate::worker_manifest::MAX_QUEUE_PRIORITY;

/// The most characters of a task name: the `task_name` column is
/// `varchar(255)`.
pub const MAX_EVENT_TASK_NAME_CHARS: usize = 255;

/// The most characters of a queue name. A queue name has at most 52 bytes
/// (the check `horsies_tasks_queue_name_length`), so at most 52 characters.
pub const MAX_EVENT_QUEUE_NAME_CHARS: usize = 52;

/// The most characters of a user error code that an event carries. A longer
/// code is [`AttemptErrorCode::UserOverBound`].
pub const MAX_EVENT_USER_CODE_CHARS: usize = 128;

/// The most characters of a stored trace carrier that an event carries. The
/// bound that the send puts on a stored carrier must not pass this one, so
/// that every stored carrier fits in an event.
pub const MAX_EVENT_TRACE_CARRIER_CHARS: usize = 2048;

/// The largest attempt number. The retry count of a row is a Postgres
/// `integer` from 0, and the attempt is the retry count plus 1.
pub const MAX_ATTEMPT: u32 = i32::MAX as u32 + 1;

/// The largest retry count: the largest Postgres `integer`.
pub const MAX_RETRY_COUNT: u32 = i32::MAX as u32;

/// The largest slot index. Slots are numbered from 0 across kinds: the
/// isolated slots, then the shared async slots, then the shared thread
/// slots.
pub const MAX_SLOT_INDEX: u32 =
    MAX_ISOLATED_CHILDREN + MAX_SHARED_ASYNC_CHILDREN + MAX_SHARED_THREAD_CHILDREN - 1;

/// The name of a task.
pub type TaskName = BoundedText<MAX_EVENT_TASK_NAME_CHARS>;

/// The name of a queue.
pub type QueueName = BoundedText<MAX_EVENT_QUEUE_NAME_CHARS>;

/// The text of a user error code, at most [`MAX_EVENT_USER_CODE_CHARS`]
/// characters.
pub type UserCodeText = BoundedText<MAX_EVENT_USER_CODE_CHARS>;

/// The text of a stored trace carrier, at most
/// [`MAX_EVENT_TRACE_CARRIER_CHARS`] characters.
pub type TraceCarrierText = BoundedText<MAX_EVENT_TRACE_CARRIER_CHARS>;

/// The number of an attempt of a task, from 1: its retry count plus 1.
pub type AttemptNumber = BoundedU32<1, MAX_ATTEMPT>;

/// The retry count of a task after a retry was scheduled, from 1.
pub type RetryCount = BoundedU32<1, MAX_RETRY_COUNT>;

/// The priority of a queue: 1 is claimed first, 100 last.
pub type Priority = BoundedU32<1, MAX_QUEUE_PRIORITY>;

/// The index of a slot of a worker, from 0.
pub type SlotIndex = BoundedU32<0, MAX_SLOT_INDEX>;

/// The four built-in task code catalogs. Their wire values are unique
/// across them.
pub const BUILT_IN_TASK_CODES: CodeUnion = CodeUnion {
    python_name: "BuiltInTaskCode",
    doc: "A built-in task error code: a member of one of the four built-in code catalogs.",
    catalogs: &[
        <OperationalErrorCode as CodeEnum>::CATALOG,
        <ContractCode as CodeEnum>::CATALOG,
        <RetrievalCode as CodeEnum>::CATALOG,
        <OutcomeCode as CodeEnum>::CATALOG,
    ],
};

impl EventValue for BuiltInTaskCode {
    const FORM: ValueForm = ValueForm::Codes(&BUILT_IN_TASK_CODES);

    fn to_wire(&self) -> Result<WireValue, EncodeRefusal> {
        Ok(WireValue::String(self.value()))
    }

    fn from_wire(wire: &WireValue) -> Result<Self, DecodeRefusal> {
        let text = wire_text(wire)?;
        BuiltInTaskCode::parse(text)
            .ok_or_else(|| DecodeRefusal::unknown_name(BUILT_IN_TASK_CODES.python_name, text))
    }
}

/// The release reason of a claim that never reached a child.
impl From<NotRunCause> for ReleaseReason {
    fn from(cause: NotRunCause) -> Self {
        match cause {
            NotRunCause::PoolStopping => Self::PoolStopping,
            NotRunCause::PoolFailed => Self::PoolFailed,
            NotRunCause::QueueFull => Self::QueueFull,
            NotRunCause::NoChildOfItsKind => Self::NoChildOfItsKind,
            NotRunCause::RunnerBeatsFailedClosed => Self::RunnerBeatsFailedClosed,
            NotRunCause::HeldForQueueCap => Self::HeldForQueueCap,
        }
    }
}

/// The error code of an attempt that ended with a task error.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub enum AttemptErrorCode {
    /// A built-in code of horsies.
    BuiltIn { code: BuiltInTaskCode },
    /// A code that the task chose, of at most 128 characters.
    User { code: UserCodeText },
    /// A code that the task chose, of more than 128 characters. The event
    /// does not carry it; the attempt row and the history row keep it.
    UserOverBound,
}

impl From<&TaskErrorCode> for AttemptErrorCode {
    fn from(code: &TaskErrorCode) -> Self {
        match code {
            TaskErrorCode::BuiltIn(built_in) => Self::BuiltIn {
                code: built_in.clone(),
            },
            TaskErrorCode::User(user) => match UserCodeText::new(user.as_str().to_owned()) {
                Ok(text) => Self::User { code: text },
                Err(_over_bound) => Self::UserOverBound,
            },
        }
    }
}

/// How the task ended a run that its child reported.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub enum ChildResult {
    /// The task returned a value. The attempt outcome is `COMPLETED`.
    Completed,
    /// The task returned an error, or a result that the worker could not
    /// read. The attempt outcome is `FAILED`.
    Failed { error_code: AttemptErrorCode },
    /// The worker code of the child failed, not the task. The attempt
    /// outcome is `WORKER_FAILURE`, with the code `BROKER_ERROR`.
    WorkerFailure,
}

/// When the attempt started: the time of the start write that set the task
/// RUNNING.
#[derive(Debug, Clone, Copy, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub enum AttemptStart {
    /// The time that the database returned to the child for its start
    /// write.
    Reported { started_at: DateTime<Utc> },
    /// The child reported no start time.
    NotReported,
}

/// Why the worker ended an attempt itself. The attempt outcome is `FAILED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, horsies_core_derive::EventSchema)]
pub enum WorkerDecision {
    /// The task reached its timeout. The code is `TASK_TIMEOUT`.
    TimedOut,
    /// The task held the event loop of its shared async child past the block
    /// limit; the worker killed the child. The code is `TASK_BLOCKED_LOOP`.
    TaskBlockedLoop,
    /// Another task held the event loop of this task's shared async child
    /// past the block limit; the worker killed the child. The code is
    /// `NEIGHBOUR_BLOCKED_LOOP`.
    NeighbourBlockedLoop,
    /// The child was lost before it reported the end, and its exit was
    /// seen. The code is `WORKER_CRASHED`.
    ChildLost,
}

impl WorkerDecision {
    /// The error code that the attempt row records for this end.
    pub fn error_code(self) -> BuiltInTaskCode {
        match self {
            Self::TimedOut => OutcomeCode::TaskTimeout.into(),
            Self::TaskBlockedLoop => OperationalErrorCode::TaskBlockedLoop.into(),
            Self::NeighbourBlockedLoop => OperationalErrorCode::NeighbourBlockedLoop.into(),
            Self::ChildLost => OperationalErrorCode::WorkerCrashed.into(),
        }
    }
}

/// The stored trace carrier of a task.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub enum RunTrace {
    /// The carrier text that the send stored on the task row.
    Carrier { text: TraceCarrierText },
    /// The task row has no carrier.
    NoCarrier,
}

/// How an attempt ended.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub enum AttemptEnd {
    /// The child reported how the run ended.
    ReportedByChild {
        result: ChildResult,
        start: AttemptStart,
    },
    /// The worker ended the attempt: the outcome is `FAILED`, with the code
    /// of `reason`.
    DecidedByWorker {
        reason: WorkerDecision,
        /// Internal: the trace carrier of the task, for the span of this
        /// attempt that the handler child makes.
        #[event_schema(internal)]
        trace: RunTrace,
        /// Internal: when the task reached its child (wall clock of the
        /// worker), the start of the span that the handler child makes.
        #[event_schema(internal)]
        placed_at: DateTime<Utc>,
    },
    /// The worker killed the child and did not see its exit. Nothing was
    /// written: stale recovery or the claim lease decides the task.
    NotKnown { child_pid: ProcessId },
}

/// What the worker recorded for the end of an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub enum AttemptRecord {
    /// The task is `COMPLETED`.
    Completed,
    /// The task is `FAILED`: no retry is left or admitted.
    Failed,
    /// The task is `PENDING` for its next attempt.
    RetryScheduled {
        /// The retry count of the task after this attempt.
        retry_count: RetryCount,
        /// When the next attempt is due.
        next_retry_at: DateTime<Utc>,
    },
    /// The claim went back to the queue: the task is `PENDING` with the same
    /// retry count.
    ClaimReleased,
    /// The child wrote what the end needs; the worker owed no write.
    NothingOwed,
    /// The task is no longer held by this claim; nothing was written.
    NotHeld,
    /// Nothing was written: stale recovery or the claim lease decides the
    /// task.
    LeftToRecovery,
    /// Every retry of the write failed with an error that a retry can clear;
    /// the task waits for recovery.
    RetriesExhausted,
    /// The write failed with an error that no retry can clear; the task
    /// waits for recovery.
    WriteFailed,
}

/// Why a slot is down: a boot failure that can clear. The worker boots the
/// slot again later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub enum SlotDownCause {
    /// A boot step failed with an errno of a resource limit.
    ResourceShortage { errno: ShortageErrno },
    /// The OS refused a thread that the child starts at boot.
    ThreadStartRefused,
}

impl From<ClearableBootCause> for SlotDownCause {
    fn from(cause: ClearableBootCause) -> Self {
        match cause {
            ClearableBootCause::ResourceShortage(errno) => Self::ResourceShortage { errno },
            ClearableBootCause::ThreadStartRefused => Self::ThreadStartRefused,
        }
    }
}
