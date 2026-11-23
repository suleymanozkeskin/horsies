//! Response models of the lifecycle record: the phase record of one attempt
//! ([`AttemptPhasesInfo`]), the phase timing of one workflow node
//! ([`NodeTimingInfo`]) and of one run ([`RunTimingInfo`]).
//!
//! Every union is tagged by `kind`, as the other tagged models of the
//! monitoring API (Python `clients/python/horsies/monitoring/models.py`). Times
//! are UTC text as the other fields (`time_text`). No model holds a duration:
//! a client subtracts the two times of a segment.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::time_text;

/// The phase record of one attempt row, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttemptPhasesInfo {
    /// The attempt row was written before the lifecycle record (migration
    /// 1001), or the history row holds a version-1 attempt archive.
    NotCaptured,
    /// The phase times of the attempt. `enqueued_at` is the due time of the
    /// attempt; `claimed_at` is the claim that ran it.
    Captured {
        #[serde(serialize_with = "time_text::utc")]
        enqueued_at: DateTime<Utc>,
        #[serde(serialize_with = "time_text::utc")]
        claimed_at: DateTime<Utc>,
        claims: AttemptClaimsInfo,
        finalizing: AttemptFinalizingInfo,
        recorded_by: AttemptRecordedByInfo,
    },
}

/// The claims of one attempt, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttemptClaimsInfo {
    /// `count` claims (1 or more) were taken for the attempt; the first was
    /// at `first_claimed_at`. A count above 1 holds lost leases and released
    /// claims.
    Counted {
        count: u32,
        #[serde(serialize_with = "time_text::utc")]
        first_claimed_at: DateTime<Utc>,
    },
    /// The attempt began before the lifecycle record: its claims were not
    /// counted.
    NotCounted,
}

/// When a child of the worker took the task of the attempt, tagged by
/// `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttemptPlacementInfo {
    /// A child took the task at `placed_at`: the claim time plus the slot
    /// wait that the worker measured.
    Placed {
        #[serde(serialize_with = "time_text::utc")]
        placed_at: DateTime<Utc>,
    },
    /// The writer of the attempt row observed no placement.
    NotObserved,
}

/// The finalizing marker of the claim that ran the attempt, tagged by
/// `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttemptFinalizingInfo {
    /// The child marked the end of the task body at `finalizing_at`.
    Marked {
        #[serde(serialize_with = "time_text::utc")]
        finalizing_at: DateTime<Utc>,
    },
    /// No marker was written.
    NotMarked,
}

/// Who wrote the attempt row, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttemptRecordedByInfo {
    /// The worker wrote the end of the attempt, with the placement it
    /// observed.
    Worker { placement: AttemptPlacementInfo },
    /// Stale RUNNING recovery wrote the attempt row. `last_seen_at` is the
    /// last time the attempt was known alive. Recovery observes no placement.
    Recovery {
        #[serde(serialize_with = "time_text::utc")]
        last_seen_at: DateTime<Utc>,
    },
}

/// The phase timing of one workflow node, tagged by `kind`: the coverage
/// label of the node (`exact`, `partial`, `unavailable`, `inconsistent`)
/// with what each label holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeTimingInfo {
    /// Every boundary of every segment is a recorded time, and no segment is
    /// `unknown`.
    Exact {
        segments: Vec<TimingSegmentInfo>,
        endpoint: NodeEndpointInfo,
    },
    /// At least one segment is `unknown`. `reasons` holds the reason of each
    /// `unknown` segment, each once, in name order.
    Partial {
        reasons: Vec<UnknownReason>,
        segments: Vec<TimingSegmentInfo>,
        endpoint: NodeEndpointInfo,
    },
    /// The record cannot show the timing of the run: no segment.
    Unavailable { reason: UnavailableReason },
    /// The record of the node contradicts itself: no segment. `conflict`
    /// names the contradiction.
    Inconsistent { conflict: TimingConflictInfo },
}

/// One phase interval of a node, from `started_at` to `end`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TimingSegmentInfo {
    pub phase: SegmentPhaseInfo,
    #[serde(serialize_with = "time_text::utc")]
    pub started_at: DateTime<Utc>,
    pub end: SegmentEndInfo,
    pub source: SegmentSourceInfo,
}

/// The phase of one segment, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SegmentPhaseInfo {
    /// The node waits for its dependencies.
    Pending,
    /// The dependencies of the node are met and nothing is dispatched.
    Ready,
    /// The attempt is due and waits for a claim; a sub-workflow node waits
    /// for its child start.
    Enqueued,
    /// The retry delay of the attempt has not passed.
    RetryWait,
    /// A worker holds a claim and the task code did not start.
    Claimed { claim: SegmentClaimInfo },
    /// The task code runs; for a sub-workflow node, the child run runs.
    Running,
    /// The task body ended and the attempt is not recorded yet.
    Finalizing,
    /// The task is in history, or the child run ended, and the workflow has
    /// not applied the node yet.
    Advancing,
    /// A known interval whose phase the record cannot show.
    Unknown { reason: UnknownReason },
}

/// Which claim a `claimed` segment shows, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SegmentClaimInfo {
    /// `count` claims before the claim that holds the attempt: lost leases
    /// and released claims, with any queue wait between them. For an
    /// attempt that waits in the queue again, every claim it had.
    Earlier { count: u32 },
    /// The claim that ran the attempt, from its attempt row, with the
    /// placement that the row holds.
    Recorded { placement: AttemptPlacementInfo },
    /// The claim of an attempt that has no attempt row: it still runs, or a
    /// cancel, an expiry or a pause abandon ended it.
    NotRecorded,
}

/// The end of one segment, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SegmentEndInfo {
    /// The segment ended at `at`.
    Ended {
        #[serde(serialize_with = "time_text::utc")]
        at: DateTime<Utc>,
    },
    /// The segment had not ended at the read (`as_of` of the run timing).
    Open,
}

/// What one segment describes, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SegmentSourceInfo {
    /// The node itself: before, between and after its tasks, and the child
    /// run of a sub-workflow node.
    Node,
    /// The task `task_id`, the `generation`-th task of the node (1-based),
    /// when the record holds no attempt of it (its history row was removed).
    Task { generation: i32, task_id: Uuid },
    /// Attempt `attempt` of the task `task_id`, the `generation`-th task of
    /// the node (1-based).
    Attempt {
        generation: i32,
        task_id: Uuid,
        attempt: i32,
    },
}

/// The terminal state of a node, tagged by `kind`. A marker, not a segment:
/// it can come before the end of the segments of a task that ran on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeEndpointInfo {
    /// The node reached `status` at `at` (its `completed_at`).
    Terminal {
        status: TerminalNodeStatus,
        #[serde(serialize_with = "time_text::utc")]
        at: DateTime<Utc>,
    },
    /// The node is not terminal.
    NotTerminal,
}

/// A terminal status of a workflow node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TerminalNodeStatus {
    Completed,
    Failed,
    Skipped,
}

/// Why a segment is `unknown`. The members are declared in name order, so
/// the order of the type is the name order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UnknownReason {
    /// The attempt row has no phase record.
    AttemptNotCaptured,
    /// The claims of the attempt were not counted: earlier claims can hide
    /// in its queue wait.
    ClaimsNotCounted,
    /// Retention removed the history row of the task: the segment runs to
    /// the time the task moved to history.
    HistoryPruned,
    /// Recovery recorded the attempt: the worker was lost after the last
    /// time the attempt was known alive.
    WorkerLost,
}

/// Why the record cannot show the timing of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UnavailableReason {
    /// The run began before the lifecycle record (`captured_since`).
    RunBeforeCapture,
    /// The run has more events than one read takes (100,000).
    TimingBoundExceeded,
}

/// A contradiction in the record of a node or a run, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimingConflictInfo {
    /// Two boundaries are in the wrong order: `earlier` comes first in the
    /// record, and its time `earlier_at` is after the time `later_at` of
    /// `later`. A step of the database clock can cause it.
    ClockOrder {
        earlier: TimingBoundary,
        #[serde(serialize_with = "time_text::utc")]
        earlier_at: DateTime<Utc>,
        later: TimingBoundary,
        #[serde(serialize_with = "time_text::utc")]
        later_at: DateTime<Utc>,
    },
    /// The state of the node needs a record that is not there.
    MissingRecord { record: MissingRecord },
    /// An event that the state of the node or the run at that point does not
    /// allow.
    UnexpectedEvent {
        event_id: i64,
        event: WorkflowEventKind,
    },
}

/// One boundary of the phase timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimingBoundary {
    /// The node was created (`created_at` of the node).
    NodeCreated,
    /// A `NODE_READY` event.
    NodeReady,
    /// A `NODE_ENQUEUED` event.
    NodeEnqueued,
    /// The node reached its terminal status (`completed_at` of the node).
    NodeCompleted,
    /// The task of a node was created (`created_at` of the task).
    TaskCreated,
    /// The due time of an attempt (`enqueued_at`).
    AttemptEnqueued,
    /// The first claim of an attempt.
    FirstClaim,
    /// The claim that holds or held an attempt.
    Claim,
    /// The placement of an attempt on a child.
    Placement,
    /// The start of the task code.
    Start,
    /// The finalizing marker.
    Finalizing,
    /// The last time a recovered attempt was known alive.
    LastSeen,
    /// The time an attempt was recorded (`finished_at`).
    AttemptRecorded,
    /// The time a task moved to history (`terminal_at`).
    TaskMoved,
    /// The child run of a sub-workflow node was created.
    ChildCreated,
    /// The child run of a sub-workflow node ended.
    ChildCompleted,
    /// A `WORKFLOW_PAUSED` event.
    RunPaused,
    /// A `WORKFLOW_RESUMED` event.
    RunResumed,
    /// The run ended (`completed_at` of the run).
    RunCompleted,
    /// The read (`as_of`), the end of an open segment.
    ReadTime,
}

/// A record that the state of a node needs and that is not there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MissingRecord {
    /// The node links a task that no live row and no event of the run holds.
    LinkedTask,
    /// The node is `ENQUEUED` or `RUNNING` and links no task, or is a
    /// `RUNNING` sub-workflow node with no child run.
    LinkedWork,
    /// The node names a child run that no row holds.
    ChildRun,
    /// The node has a terminal status and no completion time.
    CompletionTime,
    /// The task row is `CLAIMED` or `RUNNING` and has no claim time, no
    /// start time or no counted claim that the status needs.
    TaskTimes,
    /// The attempt archive says that the last attempt ended with its attempt
    /// row, and it holds no attempt row.
    AttemptRow,
    /// The live task has no attempt without a row: its current attempt ended
    /// and the task did not retry or move.
    OpenAttempt,
}

/// The kind of one event of a run (`horsies_workflow_events.event_kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowEventKind {
    WorkflowPaused,
    WorkflowResumed,
    NodeReady,
    NodeEnqueued,
    TaskMoved,
}

/// The phase timing of one run, tagged by `kind`. `as_of` is the database
/// time of the read: an open segment ends there for drawing.
/// `captured_since` is the start of the lifecycle record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunTimingInfo {
    /// The record cannot show the timing of the run; every node is
    /// `unavailable` with the same reason.
    Unavailable {
        #[serde(serialize_with = "time_text::utc")]
        as_of: DateTime<Utc>,
        #[serde(serialize_with = "time_text::utc")]
        captured_since: DateTime<Utc>,
        reason: UnavailableReason,
    },
    /// The timing of each node is composed. `range_started_at` is the
    /// creation of the run; `range_ended_at` is the latest of every segment
    /// end (an open one at `as_of`), every endpoint, every pause end and the
    /// end of the run. `open_segments` is true when a segment or a pause is
    /// open. `pauses` holds the pauses of the run in order, without a pause
    /// that a conflict of `coverage` names.
    Composed {
        #[serde(serialize_with = "time_text::utc")]
        as_of: DateTime<Utc>,
        #[serde(serialize_with = "time_text::utc")]
        captured_since: DateTime<Utc>,
        coverage: RunCoverageInfo,
        #[serde(serialize_with = "time_text::utc")]
        range_started_at: DateTime<Utc>,
        #[serde(serialize_with = "time_text::utc")]
        range_ended_at: DateTime<Utc>,
        open_segments: bool,
        pauses: Vec<PauseIntervalInfo>,
    },
}

/// The coverage of a composed run, tagged by `kind`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunCoverageInfo {
    /// Every node is `exact` and every pause is in order.
    Exact,
    /// No conflict, and at least one node is `partial`. `reasons` holds the
    /// reasons of every node, each once, in name order.
    Partial { reasons: Vec<UnknownReason> },
    /// At least one node is `inconsistent`, or a pause record contradicts
    /// itself. `conflicts` names each, nodes in task index order first.
    Inconsistent { conflicts: Vec<RunConflictInfo> },
}

/// One contradiction of a run, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunConflictInfo {
    /// The node at `task_index` is `inconsistent`; its timing names the
    /// conflict.
    Node { task_index: i32 },
    /// A pause record of the run contradicts itself.
    Pause { conflict: TimingConflictInfo },
}

/// One pause of a run: from a `WORKFLOW_PAUSED` event to `end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct PauseIntervalInfo {
    #[serde(serialize_with = "time_text::utc")]
    pub started_at: DateTime<Utc>,
    pub end: PauseEndInfo,
}

/// The end of one pause, tagged by `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PauseEndInfo {
    /// A `WORKFLOW_RESUMED` event at `at` ended the pause.
    Resumed {
        #[serde(serialize_with = "time_text::utc")]
        at: DateTime<Utc>,
    },
    /// A cancel or a paused expiry ended the run at `at` (`completed_at` of
    /// the run) while it was paused.
    RunEnded {
        #[serde(serialize_with = "time_text::utc")]
        at: DateTime<Utc>,
    },
    /// The run is still paused.
    Open,
}
