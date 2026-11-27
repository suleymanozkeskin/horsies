//! The event types. Each is a frozen record: the core worker builds it at
//! one lifecycle step and never changes it. The first field of each is
//! `at`, the wall-clock time of the step. Pure: no I/O.

use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::values::{
    AttemptEnd, AttemptNumber, AttemptRecord, Priority, QueueName, SlotDownCause, SlotIndex,
    TaskName,
};
use crate::core::task::Execution;
use crate::telemetry::labels::{RecoveryAction, ReleaseReason, ReplaceReason};
use crate::worker::executor::protocol::{ProcessId, StopReason};

/// The worker claimed a task.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct TaskClaimed {
    /// When the worker read the claim (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub task_id: Uuid,
    pub task_name: TaskName,
    pub queue: QueueName,
    /// The execution class of the task.
    pub execution: Execution,
    /// The attempt that the claim starts: the retry count plus 1.
    pub attempt: AttemptNumber,
    /// The priority of the task's queue: 1 is claimed first, 100 last.
    pub priority: Priority,
    /// From the time the task was due (`enqueued_at`) to the claim
    /// (`claimed_at`). The database wrote both times.
    pub queue_wait: Duration,
}

/// A claimed task went back to the queue before it reached a child.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct ClaimReleased {
    /// When the worker released the claim (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub task_id: Uuid,
    pub task_name: TaskName,
    pub queue: QueueName,
    pub reason: ReleaseReason,
}

/// A task reached a child.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct AttemptPlaced {
    /// When the child took the task (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub task_id: Uuid,
    pub task_name: TaskName,
    pub queue: QueueName,
    /// The execution class of the task.
    pub execution: Execution,
    pub attempt: AttemptNumber,
    /// The execution class of the child that took the task. A worker that
    /// runs no child of a shared class serves that class on its isolated
    /// children, so a task of a shared class can run on an isolated child.
    pub child_execution: Execution,
    /// The process id of the child.
    pub child_pid: ProcessId,
    /// From the claim to the time the child took the task (monotonic clock
    /// of the worker).
    pub slot_wait: Duration,
}

/// The child stopped before it set the task RUNNING. No task code ran.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct AttemptNotStarted {
    /// When the worker read the stop (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub task_id: Uuid,
    pub task_name: TaskName,
    pub queue: QueueName,
    pub attempt: AttemptNumber,
    pub reason: StopReason,
}

/// An attempt ended: the child reported its end, the worker ended it, or the
/// worker killed the child and did not see its exit.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct AttemptEnded {
    /// When the end reached the worker (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub task_id: Uuid,
    pub task_name: TaskName,
    pub queue: QueueName,
    pub execution: Execution,
    pub attempt: AttemptNumber,
    pub end: AttemptEnd,
    /// From the time the child took the task to the time the end reached
    /// the worker (monotonic clock of the worker). It includes the transit of
    /// the frames and the start write of the child.
    pub run_duration: Duration,
}

/// The worker recorded the end of an attempt.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct AttemptRecorded {
    /// When the write of the end returned (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub task_id: Uuid,
    pub task_name: TaskName,
    pub queue: QueueName,
    pub attempt: AttemptNumber,
    pub record: AttemptRecord,
    /// From the time the end reached the worker to the time its write
    /// returned (monotonic clock of the worker).
    pub record_duration: Duration,
}

/// A recovery pass of the worker changed a task.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct TaskRecovered {
    /// When the pass read the change (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub task_id: Uuid,
    pub task_name: TaskName,
    pub queue: QueueName,
    pub action: RecoveryAction,
}

/// A child of a slot sent its ready frame.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct ChildStarted {
    /// When the ready frame reached the worker (wall clock of the worker).
    pub at: DateTime<Utc>,
    /// The execution class of the child: the class of its slot.
    pub child_execution: Execution,
    pub slot: SlotIndex,
    pub child_pid: ProcessId,
    /// From the start of the boot to the ready frame (monotonic clock of the
    /// worker).
    pub boot_duration: Duration,
}

/// A slot replaced its child.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct ChildReplaced {
    /// When the worker ended the old child (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub child_execution: Execution,
    pub slot: SlotIndex,
    /// The process id of the old child.
    pub child_pid: ProcessId,
    pub reason: ReplaceReason,
}

/// A slot is down after a boot failure that can clear.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct SlotDown {
    /// When the boot failed (wall clock of the worker).
    pub at: DateTime<Utc>,
    pub child_execution: Execution,
    pub slot: SlotIndex,
    pub cause: SlotDownCause,
}

/// A slot that was down serves again.
#[derive(Debug, Clone, PartialEq, Eq, horsies_core_derive::EventSchema)]
pub struct SlotRecovered {
    /// When the slot's new child sent its ready frame (wall clock of the
    /// worker).
    pub at: DateTime<Utc>,
    pub child_execution: Execution,
    pub slot: SlotIndex,
}
