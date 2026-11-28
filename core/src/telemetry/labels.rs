//! The labels of the metrics: their names, and the closed set of values of
//! each label.
//!
//! A label value is never free text. Each label takes its values from one
//! closed set ([`LabelValues`]):
//! - a set fixed in the core: one value per variant of a closed enum, or one
//!   value per member of a code catalog;
//! - the names that the worker declares at start, and one fallback value
//!   that stands for every name outside them;
//! - fixed values, then declared names (the `error_code` label);
//! - the one value of the process (the labels of `horsies.worker.info`).
//!
//! A task id, a host name and an error message are never label values.
//!
//! A type that implements [`LabelValue`] is the value of one label set. A
//! metric handle takes its label values as these types, so a value outside
//! the set of its label does not type-check.

use crate::catalog::{CodeCatalog, CodeEnum};
use crate::telemetry::catalog::MetricId;
use crate::worker::executor::protocol::StopReason;
use crate::worker::executor::slots::outcome::WriteStage;
use crate::{
    ContractCode, Execution, OperationalCode, OutcomeCode, RetrievalCode, TaskAttemptOutcome,
    TaskSendErrorCode, UNDECLARED_LABEL,
};

/// The name of a label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LabelName {
    Task,
    Queue,
    Execution,
    ChildExecution,
    Outcome,
    ErrorCode,
    Reason,
    Stage,
    Action,
    Step,
    Result,
    Process,
    Cause,
    Metric,
    Handler,
    Version,
    Profile,
    WorkerId,
    Status,
    State,
    Part,
    Schedule,
    Query,
}

impl LabelName {
    /// The Prometheus label name.
    pub fn prometheus_name(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Queue => "queue",
            Self::Execution => "execution",
            Self::ChildExecution => "execution",
            Self::Outcome => "outcome",
            Self::ErrorCode => "error_code",
            Self::Reason => "reason",
            Self::Stage => "stage",
            Self::Action => "action",
            Self::Step => "step",
            Self::Result => "result",
            Self::Process => "process",
            Self::Cause => "cause",
            Self::Metric => "metric",
            Self::Handler => "handler",
            Self::Version => "version",
            Self::Profile => "profile",
            Self::WorkerId => "worker_id",
            Self::Status => "status",
            Self::State => "state",
            Self::Part => "part",
            Self::Schedule => "schedule",
            Self::Query => "query",
        }
    }

    /// The OpenTelemetry attribute key. The keys that a span also carries
    /// are the span's keys (`horsies.task.name`, `horsies.queue`,
    /// `horsies.task.execution`, `horsies.worker.id`, `error.type`).
    pub fn attribute_key(self) -> &'static str {
        match self {
            Self::Task => "horsies.task.name",
            Self::Queue => "horsies.queue",
            Self::Execution => "horsies.task.execution",
            Self::ChildExecution => "horsies.child.execution",
            Self::Outcome => "horsies.task.outcome",
            Self::ErrorCode => "error.type",
            Self::Reason => "horsies.reason",
            Self::Stage => "horsies.finalize.stage",
            Self::Action => "horsies.recovery.action",
            Self::Step => "horsies.recovery.step",
            Self::Result => "horsies.result",
            Self::Process => "horsies.process",
            Self::Cause => "horsies.drop.cause",
            Self::Metric => "horsies.metric",
            Self::Handler => "horsies.handler",
            Self::Version => "horsies.version",
            Self::Profile => "horsies.worker.profile",
            Self::WorkerId => "horsies.worker.id",
            Self::Status => "horsies.status",
            Self::State => "horsies.worker.state",
            Self::Part => "horsies.telemetry.part",
            Self::Schedule => "horsies.schedule",
            Self::Query => "horsies.collector.query",
        }
    }
}

/// One group of fixed label values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueGroup {
    /// The listed values, in order.
    Listed(&'static [&'static str]),
    /// The wire values of the members of one code catalog, in member order.
    Codes(CodeCatalog),
    /// The OpenTelemetry names of the metrics of the catalog, in the order
    /// of [`MetricId::ALL`].
    MetricNames,
}

impl ValueGroup {
    /// The values of the group, in order.
    pub fn texts(self) -> Vec<&'static str> {
        match self {
            Self::Listed(values) => values.to_vec(),
            Self::Codes(catalog) => catalog.members.iter().map(|member| member.value).collect(),
            Self::MetricNames => MetricId::ALL.iter().map(|id| id.spec().name).collect(),
        }
    }
}

/// The values of the groups, in group order.
pub fn group_texts(groups: &[ValueGroup]) -> Vec<&'static str> {
    groups.iter().flat_map(|group| group.texts()).collect()
}

/// A label whose values are names that the worker declares at start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeclaredSource {
    /// The names of the tasks of the worker manifest.
    TaskNames,
    /// The names of the queues of the config.
    QueueNames,
    /// The names of the registered event handlers.
    HandlerNames,
    /// The custom error codes that the tasks declare
    /// (`DeclaredErrorCodes` of each task); the fallback is
    /// [`UNDECLARED_LABEL`].
    CustomErrorCodes,
    /// The names of the schedules of the config.
    ScheduleNames,
}

impl DeclaredSource {
    /// The value that stands for every name outside the declared names.
    pub fn fallback(self) -> &'static str {
        match self {
            Self::TaskNames => "(unregistered)",
            Self::QueueNames | Self::HandlerNames | Self::ScheduleNames => "(unknown)",
            Self::CustomErrorCodes => UNDECLARED_LABEL,
        }
    }

    /// What the names are, for the catalog text.
    pub fn description(self) -> &'static str {
        match self {
            Self::TaskNames => "task names",
            Self::QueueNames => "queue names",
            Self::HandlerNames => "handler names",
            Self::CustomErrorCodes => "custom error codes",
            Self::ScheduleNames => "schedule names",
        }
    }
}

/// A label whose one value is a fact of the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessFact {
    /// The horsies version of the process.
    Version,
    /// The worker profile that the process runs.
    Profile,
    /// The worker id of the process.
    WorkerId,
}

impl ProcessFact {
    /// What the fact is, for the catalog text.
    pub fn description(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::Profile => "profile",
            Self::WorkerId => "worker id",
        }
    }
}

/// The closed set of values of one label.
///
/// The position of a value is its index in the value list of the label:
/// - [`LabelValues::Fixed`]: the values of the groups, in group order;
/// - [`LabelValues::Declared`]: the declared names in their order, then the
///   fallback;
/// - [`LabelValues::FixedThenDeclared`]: the fixed values, then the declared
///   names, then the fallback;
/// - [`LabelValues::OneProcessValue`]: position 0 only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelValues {
    /// Values fixed in the core.
    Fixed(&'static [ValueGroup]),
    /// Names declared at start, and the fallback of the source.
    Declared(DeclaredSource),
    /// Fixed values, then names declared at start, then the fallback of the
    /// source.
    FixedThenDeclared(&'static [ValueGroup], DeclaredSource),
    /// The one value of the process.
    OneProcessValue(ProcessFact),
}

/// The value of the `error_code` label for an attempt with no error code
/// (a `COMPLETED` attempt). An empty label value is the same as no label in
/// Prometheus.
pub const NO_ERROR_CODE: &str = "";

/// The fixed values of the `error_code` label: no code, then every built-in
/// task code.
pub const ERROR_CODE_FIXED: &[ValueGroup] = &[
    ValueGroup::Listed(&[NO_ERROR_CODE]),
    ValueGroup::Codes(<OperationalCode as CodeEnum>::CATALOG),
    ValueGroup::Codes(<ContractCode as CodeEnum>::CATALOG),
    ValueGroup::Codes(<RetrievalCode as CodeEnum>::CATALOG),
    ValueGroup::Codes(<OutcomeCode as CodeEnum>::CATALOG),
];

/// The values of the `error_code` label: no code, every built-in task code,
/// the custom codes that the tasks declare, and `undeclared`.
pub const ERROR_CODE_VALUES: LabelValues =
    LabelValues::FixedThenDeclared(ERROR_CODE_FIXED, DeclaredSource::CustomErrorCodes);

/// The value of the `result` label of a send that succeeded.
pub const SEND_OK_RESULT: &str = "ok";

/// The values of the `result` label of a send: `ok`, or the send error code.
pub const SEND_RESULT_VALUES: LabelValues = LabelValues::Fixed(&[
    ValueGroup::Listed(&[SEND_OK_RESULT]),
    ValueGroup::Codes(<TaskSendErrorCode as CodeEnum>::CATALOG),
]);

/// The value of one label set.
pub trait LabelValue: Copy {
    /// The value set of the label, as the catalog states it.
    const VALUES: LabelValues;

    /// The position of this value in the value list of its label (see
    /// [`LabelValues`]).
    fn position(self) -> u32;
}

/// The `execution` and the `kind` labels: the execution class of a task, and
/// the kind of a child (one set of values).
impl LabelValue for Execution {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&[
        "ISOLATED",
        "SHARED_ASYNC",
        "SHARED_THREAD",
    ])]);

    fn position(self) -> u32 {
        match self {
            Self::Isolated => 0,
            Self::SharedAsync => 1,
            Self::SharedThread => 2,
        }
    }
}

/// The `outcome` label.
impl LabelValue for TaskAttemptOutcome {
    const VALUES: LabelValues =
        LabelValues::Fixed(&[ValueGroup::Codes(<TaskAttemptOutcome as CodeEnum>::CATALOG)]);

    fn position(self) -> u32 {
        match self {
            Self::Completed => 0,
            Self::Failed => 1,
            Self::WorkerFailure => 2,
            Self::Cancelled => 3,
        }
    }
}

/// The `reason` label of `horsies.task.starts.refused`.
impl LabelValue for StopReason {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&[
        "claim_lost",
        "ownership_unconfirmed",
        "workflow_stopped",
        "task_expired",
        "node_not_linked",
        "orphan_cancelled",
    ])]);

    fn position(self) -> u32 {
        match self {
            Self::ClaimLost => 0,
            Self::OwnershipUnconfirmed => 1,
            Self::WorkflowStopped => 2,
            Self::TaskExpired => 3,
            Self::NodeNotLinked => 4,
            Self::OrphanCancelled => 5,
        }
    }
}

/// The `stage` label of `horsies.finalize.retries`: the stage names of the
/// finalize log lines.
impl LabelValue for WriteStage {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&[
        "phase1_persist",
        "timeout_write",
        "future",
    ])]);

    fn position(self) -> u32 {
        match self {
            Self::PhaseOne => 0,
            Self::TimeoutWrite => 1,
            Self::CrashRecovery => 2,
        }
    }
}

/// The `metric` label of `horsies.telemetry.series.dropped`.
impl LabelValue for MetricId {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::MetricNames]);

    fn position(self) -> u32 {
        self as u32
    }
}

/// Why the worker released a claimed task before it ran (the `reason` label
/// of the claims-released metric, and the `reason` of a `ClaimReleased`
/// event).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, horsies_core_derive::EventSchema)]
pub enum ReleaseReason {
    /// The worker is stopping.
    PoolStopping,
    /// No child could start.
    PoolFailed,
    /// Too many tasks wait for a slot.
    QueueFull,
    /// The task's route names a child kind that this worker does not run.
    NoChildOfItsKind,
    /// The runner heartbeats failed closed.
    RunnerBeatsFailedClosed,
    /// The worker stopped while the row waited for its queue cap.
    HeldForQueueCap,
    /// The claim pass found the workflow task not runnable.
    WorkflowNotRunnable,
    /// The claim pass returned a row of another class as `ISOLATED`.
    ReturnedAsIsolated,
}

impl ReleaseReason {
    pub const ALL: [Self; 8] = [
        Self::PoolStopping,
        Self::PoolFailed,
        Self::QueueFull,
        Self::NoChildOfItsKind,
        Self::RunnerBeatsFailedClosed,
        Self::HeldForQueueCap,
        Self::WorkflowNotRunnable,
        Self::ReturnedAsIsolated,
    ];
}

impl LabelValue for ReleaseReason {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&[
        "pool_stopping",
        "pool_failed",
        "queue_full",
        "no_child_of_its_kind",
        "runner_beats_failed_closed",
        "held_for_queue_cap",
        "workflow_not_runnable",
        "returned_as_isolated",
    ])]);

    fn position(self) -> u32 {
        match self {
            Self::PoolStopping => 0,
            Self::PoolFailed => 1,
            Self::QueueFull => 2,
            Self::NoChildOfItsKind => 3,
            Self::RunnerBeatsFailedClosed => 4,
            Self::HeldForQueueCap => 5,
            Self::WorkflowNotRunnable => 6,
            Self::ReturnedAsIsolated => 7,
        }
    }
}

/// Why a child ended and was replaced (the `reason` label of
/// the children-replaced metric): the loss causes of a child, and the
/// reasons why a child retires after a task. The `reason` of a
/// `ChildReplaced` event too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, horsies_core_derive::EventSchema)]
pub enum ReplaceReason {
    /// The child died by itself.
    ChildDied,
    /// The socket closed while the child lived.
    ChannelLost,
    /// The child sent a bad frame.
    ProtocolViolation,
    /// The ready frame named another protocol version.
    ProtocolMismatch,
    /// The worker killed the child.
    KilledByParent,
    /// The slot of the child failed.
    SlotDefect,
    /// The spawner thread ended, so no exit can be seen.
    SpawnerStopped,
    /// The child reached its task limit.
    TaskCount,
    /// The child reached its memory limit.
    MemoryLimit,
    /// A task on the child needed a second cancel.
    SecondCancel,
    /// A task blocked the event loop of the child.
    LoopBlocked,
    /// Work that a task left after its return needed a cancel.
    LeftoverWork,
    /// A run on the child passed its deadline.
    AtDeadline,
    /// The core of the child stopped, and the child ended itself (exit
    /// code 80).
    CoreStopped,
}

impl ReplaceReason {
    pub const ALL: [Self; 14] = [
        Self::ChildDied,
        Self::ChannelLost,
        Self::ProtocolViolation,
        Self::ProtocolMismatch,
        Self::KilledByParent,
        Self::SlotDefect,
        Self::SpawnerStopped,
        Self::TaskCount,
        Self::MemoryLimit,
        Self::SecondCancel,
        Self::LoopBlocked,
        Self::LeftoverWork,
        Self::AtDeadline,
        Self::CoreStopped,
    ];
}

impl LabelValue for ReplaceReason {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&[
        "child_died",
        "channel_lost",
        "protocol_violation",
        "protocol_mismatch",
        "killed_by_parent",
        "slot_defect",
        "spawner_stopped",
        "task_count",
        "memory_limit",
        "second_cancel",
        "loop_blocked",
        "leftover_work",
        "at_deadline",
        "core_stopped",
    ])]);

    fn position(self) -> u32 {
        match self {
            Self::ChildDied => 0,
            Self::ChannelLost => 1,
            Self::ProtocolViolation => 2,
            Self::ProtocolMismatch => 3,
            Self::KilledByParent => 4,
            Self::SlotDefect => 5,
            Self::SpawnerStopped => 6,
            Self::TaskCount => 7,
            Self::MemoryLimit => 8,
            Self::SecondCancel => 9,
            Self::LoopBlocked => 10,
            Self::LeftoverWork => 11,
            Self::AtDeadline => 12,
            Self::CoreStopped => 13,
        }
    }
}

/// What the reaper did to a task row (the `action` label of
/// the recovered-tasks metric, and the `action` of a `TaskRecovered` event).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, horsies_core_derive::EventSchema)]
pub enum RecoveryAction {
    /// A stale RUNNING row failed.
    RunningFailed,
    /// A stale RUNNING row got a retry.
    RunningRetried,
    /// A stale CLAIMED row went back to PENDING.
    ClaimRequeued,
    /// An orphaned workflow task was cancelled.
    OrphanCancelled,
    /// A PENDING row passed its `good_until` and expired.
    PendingExpired,
}

impl RecoveryAction {
    pub const ALL: [Self; 5] = [
        Self::RunningFailed,
        Self::RunningRetried,
        Self::ClaimRequeued,
        Self::OrphanCancelled,
        Self::PendingExpired,
    ];
}

impl LabelValue for RecoveryAction {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&[
        "running_failed",
        "running_retried",
        "claim_requeued",
        "orphan_cancelled",
        "pending_expired",
    ])]);

    fn position(self) -> u32 {
        match self {
            Self::RunningFailed => 0,
            Self::RunningRetried => 1,
            Self::ClaimRequeued => 2,
            Self::OrphanCancelled => 3,
            Self::PendingExpired => 4,
        }
    }
}

/// One step of a reaper pass (the `step` label of
/// `horsies.recovery.pass.failures`), in the order of the pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryStep {
    /// The tasks of workers whose liveness lock stays free.
    OwnerGone,
    /// The requeue of stale CLAIMED rows.
    StaleClaimedRequeue,
    /// The audit of orphaned workflow tasks.
    OrphanTaskAudit,
    /// The recovery of stale RUNNING rows.
    StaleRunningRecovery,
    /// The expiry of PENDING rows past `good_until`.
    PendingExpiry,
    /// The recovery of stuck workflows.
    WorkflowRecovery,
    /// The recovery of pending phase-2 workflow work.
    Phase2Recovery,
    /// The delete of old worker-state and workflow rows.
    RetentionCleanup,
    /// The delete of the liveness rows of ended workers.
    EndedWorkerRows,
    /// The cancel of paused workflows past their age policy.
    PausedWorkflowExpiry,
    /// The creation of the history and heartbeat partitions ahead.
    PartitionCoverage,
    /// The delete of expired idempotency key reservations.
    KeyReservationCleanup,
    /// The removal of expired partitions.
    PartitionPruning,
}

impl RecoveryStep {
    pub const ALL: [Self; 13] = [
        Self::OwnerGone,
        Self::StaleClaimedRequeue,
        Self::OrphanTaskAudit,
        Self::StaleRunningRecovery,
        Self::PendingExpiry,
        Self::WorkflowRecovery,
        Self::Phase2Recovery,
        Self::RetentionCleanup,
        Self::EndedWorkerRows,
        Self::PausedWorkflowExpiry,
        Self::PartitionCoverage,
        Self::KeyReservationCleanup,
        Self::PartitionPruning,
    ];

    /// The label value, also the step name of the log lines.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OwnerGone => "owner_gone",
            Self::StaleClaimedRequeue => "stale_claimed_requeue",
            Self::OrphanTaskAudit => "orphan_task_audit",
            Self::StaleRunningRecovery => "stale_running_recovery",
            Self::PendingExpiry => "pending_expiry",
            Self::WorkflowRecovery => "workflow_recovery",
            Self::Phase2Recovery => "phase2_recovery",
            Self::RetentionCleanup => "retention_cleanup",
            Self::EndedWorkerRows => "ended_worker_rows",
            Self::PausedWorkflowExpiry => "paused_workflow_expiry",
            Self::PartitionCoverage => "partition_coverage",
            Self::KeyReservationCleanup => "key_reservation_cleanup",
            Self::PartitionPruning => "partition_pruning",
        }
    }
}

impl LabelValue for RecoveryStep {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&[
        "owner_gone",
        "stale_claimed_requeue",
        "orphan_task_audit",
        "stale_running_recovery",
        "pending_expiry",
        "workflow_recovery",
        "phase2_recovery",
        "retention_cleanup",
        "ended_worker_rows",
        "paused_workflow_expiry",
        "partition_coverage",
        "key_reservation_cleanup",
        "partition_pruning",
    ])]);

    fn position(self) -> u32 {
        match self {
            Self::OwnerGone => 0,
            Self::StaleClaimedRequeue => 1,
            Self::OrphanTaskAudit => 2,
            Self::StaleRunningRecovery => 3,
            Self::PendingExpiry => 4,
            Self::WorkflowRecovery => 5,
            Self::Phase2Recovery => 6,
            Self::RetentionCleanup => 7,
            Self::EndedWorkerRows => 8,
            Self::PausedWorkflowExpiry => 9,
            Self::PartitionCoverage => 10,
            Self::KeyReservationCleanup => 11,
            Self::PartitionPruning => 12,
        }
    }
}

/// How one pass of the claim loop ended (the `result` label of
/// `horsies.worker.claim.passes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClaimPassResult {
    /// The pass claimed one or more rows.
    Claimed,
    /// The pass claimed no row.
    Empty,
    /// The claim statement or a step after it failed.
    Failed,
}

impl ClaimPassResult {
    pub const ALL: [Self; 3] = [Self::Claimed, Self::Empty, Self::Failed];
}

impl LabelValue for ClaimPassResult {
    const VALUES: LabelValues =
        LabelValues::Fixed(&[ValueGroup::Listed(&["claimed", "empty", "failed"])]);

    fn position(self) -> u32 {
        match self {
            Self::Claimed => 0,
            Self::Empty => 1,
            Self::Failed => 2,
        }
    }
}

/// The process of a memory reading (the `process` label of
/// `horsies.worker.memory.rss`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryProcess {
    /// The worker parent.
    Parent,
    /// The task children, summed.
    Children,
    /// The handler child.
    HandlerChild,
}

impl MemoryProcess {
    pub const ALL: [Self; 3] = [Self::Parent, Self::Children, Self::HandlerChild];
}

impl LabelValue for MemoryProcess {
    const VALUES: LabelValues =
        LabelValues::Fixed(&[ValueGroup::Listed(&["parent", "children", "handler_child"])]);

    fn position(self) -> u32 {
        match self {
            Self::Parent => 0,
            Self::Children => 1,
            Self::HandlerChild => 2,
        }
    }
}

/// Why a lifecycle event did not reach the handler child (the `cause` label
/// of `horsies.telemetry.events.dropped`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventDropCause {
    /// The event queue of the worker was full.
    QueueFull,
    /// The handler child was down.
    ChildDown,
}

impl EventDropCause {
    pub const ALL: [Self; 2] = [Self::QueueFull, Self::ChildDown];
}

impl LabelValue for EventDropCause {
    const VALUES: LabelValues =
        LabelValues::Fixed(&[ValueGroup::Listed(&["queue_full", "child_down"])]);

    fn position(self) -> u32 {
        match self {
            Self::QueueFull => 0,
            Self::ChildDown => 1,
        }
    }
}

/// Why the handler child has no workflow node for the span of an attempt
/// of a workflow task that the worker ended (the `reason` label of
/// `horsies.spans.worker_ended.read_failures`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpanReadFailure {
    /// The node read ran and failed.
    ReadFailed,
    /// The node read did not run: the queue of the reader was full, or the
    /// handler child stopped first.
    NotRead,
}

impl SpanReadFailure {
    pub const ALL: [Self; 2] = [Self::ReadFailed, Self::NotRead];
}

impl LabelValue for SpanReadFailure {
    const VALUES: LabelValues =
        LabelValues::Fixed(&[ValueGroup::Listed(&["read_failed", "not_read"])]);

    fn position(self) -> u32 {
        match self {
            Self::ReadFailed => 0,
            Self::NotRead => 1,
        }
    }
}

/// The status of a live task row (the `status` label of
/// `horsies.queue.tasks`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiveTaskStatus {
    Pending,
    Claimed,
    Running,
}

impl LiveTaskStatus {
    pub const ALL: [Self; 3] = [Self::Pending, Self::Claimed, Self::Running];
}

impl LabelValue for LiveTaskStatus {
    const VALUES: LabelValues =
        LabelValues::Fixed(&[ValueGroup::Listed(&["PENDING", "CLAIMED", "RUNNING"])]);

    fn position(self) -> u32 {
        match self {
            Self::Pending => 0,
            Self::Claimed => 1,
            Self::Running => 2,
        }
    }
}

/// The status of a workflow that is not terminal (the `status` label of
/// `horsies.workflows`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiveWorkflowStatus {
    Running,
    Paused,
}

impl LiveWorkflowStatus {
    pub const ALL: [Self; 2] = [Self::Running, Self::Paused];
}

impl LabelValue for LiveWorkflowStatus {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&["RUNNING", "PAUSED"])]);

    fn position(self) -> u32 {
        match self {
            Self::Running => 0,
            Self::Paused => 1,
        }
    }
}

/// The state of a live worker (the `state` label of
/// `horsies.fleet.workers`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FleetWorkerState {
    /// Every slot of the worker serves.
    Serving,
    /// One or more slots of the worker are down.
    Degraded,
}

impl FleetWorkerState {
    pub const ALL: [Self; 2] = [Self::Serving, Self::Degraded];
}

impl LabelValue for FleetWorkerState {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&["serving", "degraded"])]);

    fn position(self) -> u32 {
        match self {
            Self::Serving => 0,
            Self::Degraded => 1,
        }
    }
}

/// A telemetry part of a worker (the `part` label of
/// `horsies.fleet.workers.telemetry_down`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TelemetryPart {
    /// The metrics endpoint.
    Endpoint,
    /// The handler child.
    HandlerChild,
}

impl TelemetryPart {
    pub const ALL: [Self; 2] = [Self::Endpoint, Self::HandlerChild];
}

impl LabelValue for TelemetryPart {
    const VALUES: LabelValues =
        LabelValues::Fixed(&[ValueGroup::Listed(&["endpoint", "handler_child"])]);

    fn position(self) -> u32 {
        match self {
            Self::Endpoint => 0,
            Self::HandlerChild => 1,
        }
    }
}

/// One query of the fleet collector (the `query` label of
/// `horsies.exporter.collect.duration`, `horsies.exporter.collect.errors` and
/// `horsies.exporter.collect.failed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FleetQuery {
    /// The live task rows by queue, class and status.
    QueueTasks,
    /// The oldest due PENDING row by queue and class.
    OldestPending,
    /// The latest snapshot of each live worker.
    Workers,
    /// The class coverage of the queues.
    ClassCoverage,
    /// The workflows that are not terminal.
    Workflows,
    /// The schedule states.
    Schedules,
    /// The schema version.
    SchemaVersion,
}

impl FleetQuery {
    pub const ALL: [Self; 7] = [
        Self::QueueTasks,
        Self::OldestPending,
        Self::Workers,
        Self::ClassCoverage,
        Self::Workflows,
        Self::Schedules,
        Self::SchemaVersion,
    ];
}

impl LabelValue for FleetQuery {
    const VALUES: LabelValues = LabelValues::Fixed(&[ValueGroup::Listed(&[
        "queue_tasks",
        "oldest_pending",
        "workers",
        "class_coverage",
        "workflows",
        "schedules",
        "schema_version",
    ])]);

    fn position(self) -> u32 {
        match self {
            Self::QueueTasks => 0,
            Self::OldestPending => 1,
            Self::Workers => 2,
            Self::ClassCoverage => 3,
            Self::Workflows => 4,
            Self::Schedules => 5,
            Self::SchemaVersion => 6,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The positions of `all` are 0, 1, 2, ... in order, and the label has
    /// one fixed value per variant.
    fn assert_positions<L: LabelValue + std::fmt::Debug>(all: &[L]) {
        for (index, value) in all.iter().enumerate() {
            assert_eq!(value.position() as usize, index, "{value:?}");
        }
        match L::VALUES {
            LabelValues::Fixed(groups) => assert_eq!(group_texts(groups).len(), all.len()),
            other => panic!("a label enum has fixed values, not {other:?}"),
        }
    }

    #[test]
    fn each_label_enum_has_one_value_per_variant_at_its_position() {
        assert_positions(&Execution::ALL);
        assert_positions(&[
            TaskAttemptOutcome::Completed,
            TaskAttemptOutcome::Failed,
            TaskAttemptOutcome::WorkerFailure,
            TaskAttemptOutcome::Cancelled,
        ]);
        assert_positions(&[
            StopReason::ClaimLost,
            StopReason::OwnershipUnconfirmed,
            StopReason::WorkflowStopped,
            StopReason::TaskExpired,
            StopReason::NodeNotLinked,
            StopReason::OrphanCancelled,
        ]);
        assert_positions(&[
            WriteStage::PhaseOne,
            WriteStage::TimeoutWrite,
            WriteStage::CrashRecovery,
        ]);
        assert_positions(&MetricId::ALL);
        assert_positions(&ReleaseReason::ALL);
        assert_positions(&ReplaceReason::ALL);
        assert_positions(&RecoveryAction::ALL);
        assert_positions(&RecoveryStep::ALL);
        assert_positions(&ClaimPassResult::ALL);
        assert_positions(&MemoryProcess::ALL);
        assert_positions(&EventDropCause::ALL);
        assert_positions(&SpanReadFailure::ALL);
        assert_positions(&LiveTaskStatus::ALL);
        assert_positions(&LiveWorkflowStatus::ALL);
        assert_positions(&FleetWorkerState::ALL);
        assert_positions(&TelemetryPart::ALL);
        assert_positions(&FleetQuery::ALL);
    }

    /// The fixed text at the position of `value`.
    fn text_of<L: LabelValue>(value: L) -> &'static str {
        match L::VALUES {
            LabelValues::Fixed(groups) => group_texts(groups)[value.position() as usize],
            other => panic!("a label enum has fixed values, not {other:?}"),
        }
    }

    #[test]
    fn label_texts_of_core_enums_are_their_own_text_forms() {
        for execution in Execution::ALL {
            assert_eq!(text_of(execution), execution.as_str());
        }
        for outcome in [
            TaskAttemptOutcome::Completed,
            TaskAttemptOutcome::Failed,
            TaskAttemptOutcome::WorkerFailure,
            TaskAttemptOutcome::Cancelled,
        ] {
            assert_eq!(text_of(outcome), outcome.to_string());
        }
        for stage in [
            WriteStage::PhaseOne,
            WriteStage::TimeoutWrite,
            WriteStage::CrashRecovery,
        ] {
            assert_eq!(text_of(stage), stage.log_name());
        }
        for step in RecoveryStep::ALL {
            assert_eq!(text_of(step), step.as_str());
        }
        for id in MetricId::ALL {
            assert_eq!(text_of(id), id.spec().name);
        }
    }

    #[test]
    fn error_code_values_hold_no_code_then_every_built_in_code_once() {
        let texts = group_texts(ERROR_CODE_FIXED);
        assert_eq!(texts.first(), Some(&NO_ERROR_CODE));
        let built_in = OperationalCode::ALL.len()
            + ContractCode::ALL.len()
            + RetrievalCode::ALL.len()
            + OutcomeCode::ALL.len();
        assert_eq!(texts.len(), 1 + built_in);
        let unique: std::collections::HashSet<&str> = texts.iter().copied().collect();
        assert_eq!(unique.len(), texts.len());
        assert!(!texts.contains(&DeclaredSource::CustomErrorCodes.fallback()));
    }

    #[test]
    fn a_fallback_is_no_fixed_value_of_its_label() {
        let send_results = match SEND_RESULT_VALUES {
            LabelValues::Fixed(groups) => group_texts(groups),
            other => panic!("the send result label has fixed values, not {other:?}"),
        };
        assert_eq!(send_results.first(), Some(&SEND_OK_RESULT));
        assert_eq!(send_results.len(), 1 + TaskSendErrorCode::VARIANTS.len());
        for source in [
            DeclaredSource::TaskNames,
            DeclaredSource::QueueNames,
            DeclaredSource::HandlerNames,
            DeclaredSource::CustomErrorCodes,
            DeclaredSource::ScheduleNames,
        ] {
            assert!(!source.fallback().is_empty(), "{source:?}");
            assert!(!group_texts(ERROR_CODE_FIXED).contains(&source.fallback()));
        }
    }
}
