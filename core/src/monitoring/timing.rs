//! The composer of the phase timing of a workflow run and of each of its
//! nodes, from the rows of `horsies_monitoring_workflow_run`.
//!
//! Pure: no I/O and no clock read. The read time is `as_of` of the run row.
//! It takes the facts that the row decoder built ([`RunFacts`],
//! [`NodeFacts`]) and gives one [`RunTimingInfo`] and one [`NodeTimingInfo`]
//! per node ([`compose_run`]).
//!
//! The timeline of a node is a chain of boundaries. Each segment runs from
//! one boundary to the next, so the segments of a node do not overlap and
//! leave no gap, but where the record stops (a task that a cancel ended).
//! Each boundary is a recorded time; the composer adds no duration. A
//! boundary before the boundary that comes before it in the record makes the
//! node `inconsistent` (`clock_order`): the composer does not reorder or
//! guess. A segment of zero length is dropped.
//!
//! - Task node: `pending` (or `ready` for a node with no dependency, which the
//!   start inserts `READY`) from its creation; `ready` from a `NODE_READY`
//!   event; then each task of the node, in event order (its `TASK_MOVED`
//!   events, then the live task that the node links). Each task gives the
//!   segments of its attempts ([`compose_task`]). Between two tasks: `ready`.
//!   After the last task that moved: `ready` when a `NODE_READY` event
//!   follows it, `advancing` when the move wrote a phase-2 record, else
//!   nothing.
//! - Sub-workflow node: `pending` or `ready`, then `ready` and `enqueued` from
//!   the `NODE_READY` and `NODE_ENQUEUED` events, `running` from the creation
//!   of the child run, `advancing` from its end.
//! - A terminal node ends a waiting segment (`pending`, `ready`, `enqueued`,
//!   `advancing`) at its completion time. The segments of a task or of a
//!   child run that went on after the node became terminal are kept; the
//!   endpoint then comes before their end.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::core::history::archive::attempts::{
    AttemptRecord, ClaimRecord, Finalizing, OpenAttempt, OpenAttemptFacts, OpenClaims, PhaseRecord,
    Placement, RecordedBy,
};
use crate::core::lifecycle::TerminalizationKind;

use super::timing_models::{
    AttemptClaimsInfo, AttemptFinalizingInfo, AttemptPhasesInfo, AttemptPlacementInfo,
    AttemptRecordedByInfo, MissingRecord, NodeEndpointInfo, NodeTimingInfo, PauseEndInfo,
    PauseIntervalInfo, RunConflictInfo, RunCoverageInfo, RunTimingInfo, SegmentClaimInfo,
    SegmentEndInfo, SegmentPhaseInfo, SegmentSourceInfo, TerminalNodeStatus, TimingBoundary,
    TimingConflictInfo, TimingSegmentInfo, UnavailableReason, UnknownReason, WorkflowEventKind,
};

/// The most nodes of one run read: `c_max_nodes` of
/// `horsies_monitoring_workflow_run`, the bound of the workflow start.
pub(super) const MAX_RUN_NODES: usize = 10_000;

/// The most events of one run read: `c_max_events` of
/// `horsies_monitoring_workflow_run`. A run with more is `unavailable`.
pub(super) const MAX_RUN_EVENTS: usize = 100_000;

/// The most attempts of one task (`horsies_task_attempts_attempt_range`).
pub(super) const MAX_TASK_ATTEMPTS: usize = 21;

/// The run facts that the composer reads: the read time, the capture start,
/// the run row times and the records of the run.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RunFacts {
    pub(super) as_of: DateTime<Utc>,
    pub(super) captured_since: DateTime<Utc>,
    pub(super) created_at: DateTime<Utc>,
    pub(super) completed_at: Option<DateTime<Utc>>,
    pub(super) records: TimingRecords,
}

/// What the run read returned for the timing of the run.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum TimingRecords {
    /// The run began before the lifecycle record: no event and no task.
    BeforeCapture,
    /// The run has more than [`MAX_RUN_EVENTS`] events: no event and no task.
    BoundExceeded,
    /// The events of the run in `event_id` order, and the live tasks that its
    /// nodes link, by task id. The task of a `TASK_MOVED` event is in its
    /// event.
    Included {
        events: Vec<RunEvent>,
        live_tasks: HashMap<Uuid, LiveTask>,
    },
}

/// One event of the run (`horsies_workflow_events`).
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RunEvent {
    pub(super) event_id: i64,
    pub(super) occurred_at: DateTime<Utc>,
    pub(super) kind: RunEventKind,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum RunEventKind {
    WorkflowPaused,
    WorkflowResumed,
    /// An event of the node at `task_index`.
    Node {
        task_index: i32,
        event: NodeEventKind,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum NodeEventKind {
    Ready,
    Enqueued,
    TaskMoved(Box<TaskMove>),
}

/// A task of a node that moved to history, with the kind of the move and
/// the history row at the locator of the event.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct TaskMove {
    pub(super) task_id: Uuid,
    pub(super) move_kind: TerminalizationKind,
    pub(super) record: MovedRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum MovedRecord {
    /// The history row of the task.
    History(MovedTask),
    /// Retention removed the history row.
    Pruned,
}

/// The times of a task row (live or history) that the composer reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TaskTimes {
    pub(super) created_at: DateTime<Utc>,
    pub(super) enqueued_at: DateTime<Utc>,
    pub(super) claimed_at: Option<DateTime<Utc>>,
    pub(super) started_at: Option<DateTime<Utc>>,
}

/// The decoded attempt archive of a task: its attempt rows in attempt order
/// (contiguous from 1, at most [`MAX_TASK_ATTEMPTS`]) and its open attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskAttempts {
    pub(super) records: Vec<AttemptRecord>,
    pub(super) open: OpenAttempt,
}

/// A live task row that a node links.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LiveTask {
    pub(super) times: TaskTimes,
    pub(super) status: LiveStatus,
    pub(super) attempts: TaskAttempts,
}

/// The status of a live task row (`horsies_tasks_status` admits these).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LiveStatus {
    Pending,
    Claimed,
    Running,
}

/// A history row of a task of a node. Its terminal time is the time of its
/// `TASK_MOVED` event (the locator of the row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MovedTask {
    pub(super) times: TaskTimes,
    pub(super) attempts: TaskAttempts,
}

/// The node facts that the composer reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NodeFacts {
    pub(super) task_index: i32,
    pub(super) status: NodeStatus,
    pub(super) created_at: DateTime<Utc>,
    pub(super) completed_at: Option<DateTime<Utc>>,
    pub(super) entry: NodeEntry,
    pub(super) work: NodeWork,
}

/// The status of a node (`horsies_workflow_tasks_status_known`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeStatus {
    Pending,
    Ready,
    Enqueued,
    Running,
    Completed,
    Failed,
    Skipped,
}

impl NodeStatus {
    /// The status of the stored text; `None` for a text that is not a node
    /// status.
    pub(super) fn parse(stored: &str) -> Option<Self> {
        match stored {
            "PENDING" => Some(Self::Pending),
            "READY" => Some(Self::Ready),
            "ENQUEUED" => Some(Self::Enqueued),
            "RUNNING" => Some(Self::Running),
            "COMPLETED" => Some(Self::Completed),
            "FAILED" => Some(Self::Failed),
            "SKIPPED" => Some(Self::Skipped),
            _ => None,
        }
    }

    fn terminal(self) -> Option<TerminalNodeStatus> {
        match self {
            Self::Pending | Self::Ready | Self::Enqueued | Self::Running => None,
            Self::Completed => Some(TerminalNodeStatus::Completed),
            Self::Failed => Some(TerminalNodeStatus::Failed),
            Self::Skipped => Some(TerminalNodeStatus::Skipped),
        }
    }
}

/// The status a node had at its insert: the start inserts a node with no
/// dependency as `READY` and every other node as `PENDING`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeEntry {
    Pending,
    Ready,
}

/// What a node runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeWork {
    /// A task node, with the task it links now, if any.
    Task { linked: Option<Uuid> },
    /// A sub-workflow node, with its child run.
    SubWorkflow { child: ChildRun },
}

/// The child run of a sub-workflow node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChildRun {
    /// The node names no child run.
    NotLinked,
    /// The child run row, with its times.
    Found {
        created_at: DateTime<Utc>,
        completed_at: Option<DateTime<Utc>>,
    },
    /// The node names a child run that no row holds.
    NotFound,
}

/// The timing of a run and of each of its nodes, in the order of the nodes
/// given to [`compose_run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ComposedRun {
    pub(super) run: RunTimingInfo,
    pub(super) nodes: Vec<NodeTimingInfo>,
}

/// The phase record of an attempt row as the API shows it.
pub(super) fn attempt_phases_info(phases: PhaseRecord) -> AttemptPhasesInfo {
    match phases {
        PhaseRecord::NotCaptured => AttemptPhasesInfo::NotCaptured,
        PhaseRecord::Captured(phases) => AttemptPhasesInfo::Captured {
            enqueued_at: phases.enqueued_at(),
            claimed_at: phases.claimed_at(),
            claims: match phases.claims() {
                ClaimRecord::NotCounted => AttemptClaimsInfo::NotCounted,
                ClaimRecord::Counted {
                    count,
                    first_claimed_at,
                } => AttemptClaimsInfo::Counted {
                    count: count.get(),
                    first_claimed_at,
                },
            },
            finalizing: match phases.finalizing() {
                Finalizing::Marked { at } => AttemptFinalizingInfo::Marked { finalizing_at: at },
                Finalizing::NotMarked => AttemptFinalizingInfo::NotMarked,
            },
            recorded_by: match phases.recorded_by() {
                RecordedBy::Worker { placement } => AttemptRecordedByInfo::Worker {
                    placement: placement_info(placement),
                },
                RecordedBy::Recovery { last_seen_at } => {
                    AttemptRecordedByInfo::Recovery { last_seen_at }
                }
                RecordedBy::Cancel => AttemptRecordedByInfo::Cancel,
            },
        },
    }
}

fn placement_info(placement: Placement) -> AttemptPlacementInfo {
    match placement {
        Placement::Placed { at } => AttemptPlacementInfo::Placed { placed_at: at },
        Placement::NotObserved => AttemptPlacementInfo::NotObserved,
    }
}

/// The timing of the run and of each node of `nodes` (in their order).
/// Total: every input gives a timing; a record that contradicts itself is
/// `inconsistent`, never an error.
pub(super) fn compose_run(run: &RunFacts, nodes: &[NodeFacts]) -> ComposedRun {
    let (events, live_tasks) = match &run.records {
        TimingRecords::BeforeCapture => {
            return unavailable_run(run, nodes, UnavailableReason::RunBeforeCapture)
        }
        TimingRecords::BoundExceeded => {
            return unavailable_run(run, nodes, UnavailableReason::TimingBoundExceeded)
        }
        TimingRecords::Included { events, live_tasks } => (events, live_tasks),
    };
    let by_node = node_events(events);
    let timings: Vec<NodeTimingInfo> = nodes
        .iter()
        .map(|node| {
            let own = by_node
                .get(&node.task_index)
                .map(Vec::as_slice)
                .unwrap_or_default();
            compose_node(node, own, live_tasks, run.as_of)
        })
        .collect();
    let pauses = compose_pauses(events, run);
    let coverage = run_coverage(nodes, &timings, &pauses.conflicts);
    let range = run_range(run, &timings, &pauses.intervals);
    ComposedRun {
        run: RunTimingInfo::Composed {
            as_of: run.as_of,
            captured_since: run.captured_since,
            coverage,
            range_started_at: run.created_at,
            range_ended_at: range.ended_at,
            open_segments: range.open,
            pauses: pauses.intervals,
        },
        nodes: timings,
    }
}

fn unavailable_run(run: &RunFacts, nodes: &[NodeFacts], reason: UnavailableReason) -> ComposedRun {
    ComposedRun {
        run: RunTimingInfo::Unavailable {
            as_of: run.as_of,
            captured_since: run.captured_since,
            reason,
        },
        nodes: nodes
            .iter()
            .map(|_| NodeTimingInfo::Unavailable { reason })
            .collect(),
    }
}

/// One event of a node, as the node composer reads it.
#[derive(Debug, Clone, Copy)]
struct NodeEvent<'a> {
    event_id: i64,
    occurred_at: DateTime<Utc>,
    kind: &'a NodeEventKind,
}

impl NodeEvent<'_> {
    fn name(&self) -> WorkflowEventKind {
        match self.kind {
            NodeEventKind::Ready => WorkflowEventKind::NodeReady,
            NodeEventKind::Enqueued => WorkflowEventKind::NodeEnqueued,
            NodeEventKind::TaskMoved(_) => WorkflowEventKind::TaskMoved,
        }
    }
}

/// The node events of the run by node, each list in event order.
fn node_events(events: &[RunEvent]) -> HashMap<i32, Vec<NodeEvent<'_>>> {
    let mut by_node: HashMap<i32, Vec<NodeEvent<'_>>> = HashMap::new();
    for event in events {
        match &event.kind {
            RunEventKind::Node {
                task_index,
                event: kind,
            } => {
                by_node.entry(*task_index).or_default().push(NodeEvent {
                    event_id: event.event_id,
                    occurred_at: event.occurred_at,
                    kind,
                });
            }
            RunEventKind::WorkflowPaused | RunEventKind::WorkflowResumed => {}
        }
    }
    by_node
}

/// One time of the record with the boundary that it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Bound {
    at: DateTime<Utc>,
    boundary: TimingBoundary,
}

impl Bound {
    fn new(at: DateTime<Utc>, boundary: TimingBoundary) -> Self {
        Self { at, boundary }
    }
}

/// `Ok` when `earlier` is not after `later`; else the `clock_order`
/// conflict of the pair.
fn check_order(earlier: Bound, later: Bound) -> Result<(), TimingConflictInfo> {
    match earlier.at <= later.at {
        true => Ok(()),
        false => Err(TimingConflictInfo::ClockOrder {
            earlier: earlier.boundary,
            earlier_at: earlier.at,
            later: later.boundary,
            later_at: later.at,
        }),
    }
}

fn missing(record: MissingRecord) -> TimingConflictInfo {
    TimingConflictInfo::MissingRecord { record }
}

fn unknown(reason: UnknownReason) -> SegmentPhaseInfo {
    SegmentPhaseInfo::Unknown { reason }
}

/// The phase that runs from its start until the next boundary.
#[derive(Debug, Clone, Copy)]
struct OpenPhase {
    start: Bound,
    phase: SegmentPhaseInfo,
    source: SegmentSourceInfo,
}

/// The segments of one node, built boundary by boundary. `last` is the
/// latest boundary given; every boundary is checked against it.
#[derive(Debug)]
struct Timeline {
    segments: Vec<TimingSegmentInfo>,
    current: Option<OpenPhase>,
    last: Bound,
}

impl Timeline {
    fn start(bound: Bound, phase: SegmentPhaseInfo, source: SegmentSourceInfo) -> Self {
        Self {
            segments: Vec::new(),
            current: Some(OpenPhase {
                start: bound,
                phase,
                source,
            }),
            last: bound,
        }
    }

    /// Ends the current phase at `bound` and starts `phase` there. The same
    /// phase of the same source goes on as one segment.
    fn begin(
        &mut self,
        bound: Bound,
        phase: SegmentPhaseInfo,
        source: SegmentSourceInfo,
    ) -> Result<(), TimingConflictInfo> {
        check_order(self.last, bound)?;
        self.last = bound;
        match self.current.take() {
            Some(open) if open.phase == phase && open.source == source => {
                self.current = Some(open);
            }
            Some(open) => {
                self.push_ended(open, bound);
                self.current = Some(OpenPhase {
                    start: bound,
                    phase,
                    source,
                });
            }
            None => {
                self.current = Some(OpenPhase {
                    start: bound,
                    phase,
                    source,
                });
            }
        }
        Ok(())
    }

    /// Passes a boundary inside the current phase: checks its order and
    /// makes it the latest boundary, with no new segment.
    fn pass(&mut self, bound: Bound) -> Result<(), TimingConflictInfo> {
        check_order(self.last, bound)?;
        self.last = bound;
        Ok(())
    }

    /// Ends the current phase at `bound`; no phase follows.
    fn close(&mut self, bound: Bound) -> Result<(), TimingConflictInfo> {
        check_order(self.last, bound)?;
        self.last = bound;
        if let Some(open) = self.current.take() {
            self.push_ended(open, bound);
        }
        Ok(())
    }

    /// Ends the current phase at the latest boundary and starts `phase`
    /// there: the record holds no boundary between them.
    fn continue_as(&mut self, phase: SegmentPhaseInfo, source: SegmentSourceInfo) {
        let bound = self.last;
        if let Some(open) = self.current.take() {
            self.push_ended(open, bound);
        }
        self.current = Some(OpenPhase {
            start: bound,
            phase,
            source,
        });
    }

    /// Drops the current phase: it began after the endpoint of the node.
    fn drop_current(&mut self) {
        self.current = None;
    }

    /// Whether the current phase is a wait of the node (`pending`, `ready`,
    /// `enqueued`, `advancing`), which the terminal status of the node ends.
    fn waits_on_node(&self) -> bool {
        match self.current {
            Some(OpenPhase {
                source: SegmentSourceInfo::Node,
                phase:
                    SegmentPhaseInfo::Pending
                    | SegmentPhaseInfo::Ready
                    | SegmentPhaseInfo::Enqueued
                    | SegmentPhaseInfo::Advancing,
                ..
            }) => true,
            Some(_) | None => false,
        }
    }

    fn push_ended(&mut self, open: OpenPhase, end: Bound) {
        if end.at > open.start.at {
            self.segments.push(TimingSegmentInfo {
                phase: open.phase,
                started_at: open.start.at,
                end: SegmentEndInfo::Ended { at: end.at },
                source: open.source,
            });
        }
    }

    /// The segments; a current phase is open at the read time `as_of`.
    fn finish(
        mut self,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<TimingSegmentInfo>, TimingConflictInfo> {
        if let Some(open) = self.current.take() {
            check_order(self.last, Bound::new(as_of, TimingBoundary::ReadTime))?;
            self.segments.push(TimingSegmentInfo {
                phase: open.phase,
                started_at: open.start.at,
                end: SegmentEndInfo::Open,
                source: open.source,
            });
        }
        Ok(self.segments)
    }
}

/// Whether a node had work (a task or a child run) in the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkSeen {
    NoWork,
    Worked,
}

fn compose_node(
    node: &NodeFacts,
    events: &[NodeEvent<'_>],
    live_tasks: &HashMap<Uuid, LiveTask>,
    as_of: DateTime<Utc>,
) -> NodeTimingInfo {
    match node_segments(node, events, live_tasks, as_of) {
        Ok((segments, endpoint)) => {
            let reasons: BTreeSet<UnknownReason> = segments
                .iter()
                .filter_map(|segment| match segment.phase {
                    SegmentPhaseInfo::Unknown { reason } => Some(reason),
                    _ => None,
                })
                .collect();
            match reasons.is_empty() {
                true => NodeTimingInfo::Exact { segments, endpoint },
                false => NodeTimingInfo::Partial {
                    reasons: reasons.into_iter().collect(),
                    segments,
                    endpoint,
                },
            }
        }
        Err(conflict) => NodeTimingInfo::Inconsistent { conflict },
    }
}

fn node_segments(
    node: &NodeFacts,
    events: &[NodeEvent<'_>],
    live_tasks: &HashMap<Uuid, LiveTask>,
    as_of: DateTime<Utc>,
) -> Result<(Vec<TimingSegmentInfo>, NodeEndpointInfo), TimingConflictInfo> {
    let endpoint = match (node.status.terminal(), node.completed_at) {
        (Some(status), Some(at)) => NodeEndpointInfo::Terminal { status, at },
        (Some(_), None) => return Err(missing(MissingRecord::CompletionTime)),
        (None, _) => NodeEndpointInfo::NotTerminal,
    };
    let entry = match node.entry {
        NodeEntry::Pending => SegmentPhaseInfo::Pending,
        NodeEntry::Ready => SegmentPhaseInfo::Ready,
    };
    let mut timeline = Timeline::start(
        Bound::new(node.created_at, TimingBoundary::NodeCreated),
        entry,
        SegmentSourceInfo::Node,
    );
    let worked = match node.work {
        NodeWork::Task { linked } => task_node(
            &mut timeline,
            node.status,
            linked,
            events,
            live_tasks,
            as_of,
        )?,
        NodeWork::SubWorkflow { child } => {
            sub_workflow_node(&mut timeline, node.status, child, events)?
        }
    };
    end_at_endpoint(&mut timeline, endpoint, worked)?;
    Ok((timeline.finish(as_of)?, endpoint))
}

/// Ends a wait of the node at the completion time of a terminal node. A wait
/// that began after that time, after the work of the node, is dropped: the
/// node became terminal while its task or child went on.
fn end_at_endpoint(
    timeline: &mut Timeline,
    endpoint: NodeEndpointInfo,
    worked: WorkSeen,
) -> Result<(), TimingConflictInfo> {
    let completed = match endpoint {
        NodeEndpointInfo::Terminal { at, .. } => Bound::new(at, TimingBoundary::NodeCompleted),
        NodeEndpointInfo::NotTerminal => return Ok(()),
    };
    match (
        timeline.waits_on_node(),
        worked,
        timeline.last.at > completed.at,
    ) {
        (false, _, _) => Ok(()),
        (true, WorkSeen::Worked, true) => {
            timeline.drop_current();
            Ok(())
        }
        (true, WorkSeen::Worked, false) | (true, WorkSeen::NoWork, _) => timeline.close(completed),
    }
}

/// A sub-workflow node: its events, then its child run.
fn sub_workflow_node(
    timeline: &mut Timeline,
    status: NodeStatus,
    child: ChildRun,
    events: &[NodeEvent<'_>],
) -> Result<WorkSeen, TimingConflictInfo> {
    for event in events {
        let (boundary, phase) = match event.kind {
            NodeEventKind::Ready => (TimingBoundary::NodeReady, SegmentPhaseInfo::Ready),
            NodeEventKind::Enqueued => (TimingBoundary::NodeEnqueued, SegmentPhaseInfo::Enqueued),
            NodeEventKind::TaskMoved(_) => return Err(unexpected(event)),
        };
        timeline.begin(
            Bound::new(event.occurred_at, boundary),
            phase,
            SegmentSourceInfo::Node,
        )?;
    }
    match (child, status) {
        (ChildRun::NotLinked, NodeStatus::Running) => Err(missing(MissingRecord::LinkedWork)),
        (ChildRun::NotLinked, _) => Ok(WorkSeen::NoWork),
        (ChildRun::NotFound, _) => Err(missing(MissingRecord::ChildRun)),
        (
            ChildRun::Found {
                created_at,
                completed_at,
            },
            _,
        ) => {
            timeline.begin(
                Bound::new(created_at, TimingBoundary::ChildCreated),
                SegmentPhaseInfo::Running,
                SegmentSourceInfo::Node,
            )?;
            if let Some(ended_at) = completed_at {
                timeline.begin(
                    Bound::new(ended_at, TimingBoundary::ChildCompleted),
                    SegmentPhaseInfo::Advancing,
                    SegmentSourceInfo::Node,
                )?;
            }
            Ok(WorkSeen::Worked)
        }
    }
}

fn unexpected(event: &NodeEvent<'_>) -> TimingConflictInfo {
    TimingConflictInfo::UnexpectedEvent {
        event_id: event.event_id,
        event: event.name(),
    }
}

/// One task of a node with the `NODE_READY` events that follow its move.
#[derive(Debug)]
struct Generation<'a> {
    task_id: Uuid,
    record: GenerationRecord<'a>,
    ready_after: Vec<NodeEvent<'a>>,
}

#[derive(Debug, Clone, Copy)]
enum GenerationRecord<'a> {
    Live(&'a LiveTask),
    Moved {
        task: &'a MovedTask,
        moved: Bound,
        kind: TerminalizationKind,
    },
    Pruned {
        moved: Bound,
        kind: TerminalizationKind,
    },
}

/// The tasks of a task node in order, and the `NODE_READY` events before the
/// first of them.
#[derive(Debug)]
struct GenerationPlan<'a> {
    ready_before: Vec<NodeEvent<'a>>,
    generations: Vec<Generation<'a>>,
}

fn generation_plan<'a>(
    linked: Option<Uuid>,
    events: &[NodeEvent<'a>],
    live_tasks: &'a HashMap<Uuid, LiveTask>,
) -> Result<GenerationPlan<'a>, TimingConflictInfo> {
    let mut plan = GenerationPlan {
        ready_before: Vec::new(),
        generations: Vec::new(),
    };
    for event in events {
        match event.kind {
            NodeEventKind::Ready => match plan.generations.last_mut() {
                Some(generation) => generation.ready_after.push(*event),
                None => plan.ready_before.push(*event),
            },
            NodeEventKind::Enqueued => return Err(unexpected(event)),
            NodeEventKind::TaskMoved(task_move) => {
                let moved = Bound::new(event.occurred_at, TimingBoundary::TaskMoved);
                let kind = task_move.move_kind;
                plan.generations.push(Generation {
                    task_id: task_move.task_id,
                    record: match &task_move.record {
                        MovedRecord::History(task) => GenerationRecord::Moved { task, moved, kind },
                        MovedRecord::Pruned => GenerationRecord::Pruned { moved, kind },
                    },
                    ready_after: Vec::new(),
                });
            }
        }
    }
    let Some(task_id) = linked else {
        return Ok(plan);
    };
    if plan
        .generations
        .iter()
        .any(|generation| generation.task_id == task_id)
    {
        return Ok(plan);
    }
    match live_tasks.get(&task_id) {
        Some(task) => {
            plan.generations.push(Generation {
                task_id,
                record: GenerationRecord::Live(task),
                ready_after: Vec::new(),
            });
            Ok(plan)
        }
        None => Err(missing(MissingRecord::LinkedTask)),
    }
}

/// A task node: the `NODE_READY` events before its first task, then each
/// task and what follows it.
fn task_node(
    timeline: &mut Timeline,
    status: NodeStatus,
    linked: Option<Uuid>,
    events: &[NodeEvent<'_>],
    live_tasks: &HashMap<Uuid, LiveTask>,
    as_of: DateTime<Utc>,
) -> Result<WorkSeen, TimingConflictInfo> {
    let plan = generation_plan(linked, events, live_tasks)?;
    for event in &plan.ready_before {
        timeline.begin(
            Bound::new(event.occurred_at, TimingBoundary::NodeReady),
            SegmentPhaseInfo::Ready,
            SegmentSourceInfo::Node,
        )?;
    }
    let count = plan.generations.len();
    for (number, (index, generation)) in (1_i32..).zip(plan.generations.iter().enumerate()) {
        let ids = TaskIds {
            generation: number,
            task_id: generation.task_id,
        };
        let end = compose_generation(timeline, generation.record, ids, as_of)?;
        let next = match index + 1 < count {
            true => Following::AnotherTask,
            false => Following::Nothing,
        };
        after_generation(timeline, generation, end, next)?;
    }
    match (count, status) {
        (0, NodeStatus::Enqueued | NodeStatus::Running) => Err(missing(MissingRecord::LinkedWork)),
        (0, _) => Ok(WorkSeen::NoWork),
        (_, _) => Ok(WorkSeen::Worked),
    }
}

/// The generation number and the task id of the segments of one task.
#[derive(Debug, Clone, Copy)]
struct TaskIds {
    generation: i32,
    task_id: Uuid,
}

impl TaskIds {
    fn task(self) -> SegmentSourceInfo {
        SegmentSourceInfo::Task {
            generation: self.generation,
            task_id: self.task_id,
        }
    }

    fn attempt(self, attempt: i32) -> SegmentSourceInfo {
        SegmentSourceInfo::Attempt {
            generation: self.generation,
            task_id: self.task_id,
            attempt,
        }
    }
}

/// Where the segments of one task end.
#[derive(Debug, Clone, Copy)]
enum GenerationEnd {
    /// The task is live: its last segment is open.
    Open,
    /// The task ended at this boundary.
    At(Bound),
}

/// What follows one task of a node in the record.
#[derive(Debug, Clone, Copy)]
enum Following {
    AnotherTask,
    Nothing,
}

fn compose_generation(
    timeline: &mut Timeline,
    record: GenerationRecord<'_>,
    ids: TaskIds,
    as_of: DateTime<Utc>,
) -> Result<GenerationEnd, TimingConflictInfo> {
    match record {
        GenerationRecord::Live(task) => compose_task(
            timeline,
            &task.times,
            &task.attempts,
            TaskClose::Live(task.status),
            ids,
            as_of,
        ),
        GenerationRecord::Moved { task, moved, .. } => compose_task(
            timeline,
            &task.times,
            &task.attempts,
            TaskClose::Moved(moved),
            ids,
            as_of,
        ),
        GenerationRecord::Pruned { moved, .. } => {
            timeline.continue_as(unknown(UnknownReason::HistoryPruned), ids.task());
            Ok(GenerationEnd::At(moved))
        }
    }
}

/// What follows the move of a task: `ready` when a `NODE_READY` event
/// follows it or another task follows; `advancing` when the move wrote a
/// phase-2 record; else the record stops.
fn after_generation(
    timeline: &mut Timeline,
    generation: &Generation<'_>,
    end: GenerationEnd,
    next: Following,
) -> Result<(), TimingConflictInfo> {
    let (end, kind) = match (end, generation.record) {
        (GenerationEnd::Open, _) | (GenerationEnd::At(_), GenerationRecord::Live(_)) => {
            return Ok(())
        }
        (GenerationEnd::At(end), GenerationRecord::Moved { kind, .. })
        | (GenerationEnd::At(end), GenerationRecord::Pruned { kind, .. }) => (end, kind),
    };
    let ready_follows = !generation.ready_after.is_empty();
    match (ready_follows, next, writes_phase2_record(kind)) {
        (true, _, _) | (false, Following::AnotherTask, _) => {
            timeline.begin(end, SegmentPhaseInfo::Ready, SegmentSourceInfo::Node)?;
            for event in &generation.ready_after {
                timeline.pass(Bound::new(event.occurred_at, TimingBoundary::NodeReady))?;
            }
            Ok(())
        }
        (false, Following::Nothing, true) => {
            timeline.begin(end, SegmentPhaseInfo::Advancing, SegmentSourceInfo::Node)
        }
        (false, Following::Nothing, false) => timeline.close(end),
    }
}

/// Whether the move of a workflow task with `kind` writes a phase-2 record,
/// which the workflow applies to the node later.
fn writes_phase2_record(kind: TerminalizationKind) -> bool {
    match kind {
        TerminalizationKind::CompleteLocked
        | TerminalizationKind::FailRunning
        | TerminalizationKind::FailStale
        | TerminalizationKind::ExpireClaimed
        | TerminalizationKind::ExpirePending => true,
        TerminalizationKind::CompleteFused
        | TerminalizationKind::CancelAdmin
        | TerminalizationKind::CancelOrphan
        | TerminalizationKind::CancelOrphanSweep
        | TerminalizationKind::PauseAbandonClaim
        | TerminalizationKind::PauseAbandonClaimBatch
        | TerminalizationKind::PauseAbandonWorkflow
        | TerminalizationKind::WorkflowCancelClaim
        | TerminalizationKind::WorkflowCancelClaimBatch
        | TerminalizationKind::WorkflowCancelWorkflow => false,
    }
}

/// How a task ends: live, with its status, or moved to history at a
/// boundary.
#[derive(Debug, Clone, Copy)]
enum TaskClose {
    Live(LiveStatus),
    Moved(Bound),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptOrder {
    /// The first attempt of the task: it waits from the creation of the
    /// task.
    First,
    /// A retry: it waits from the end of the attempt before it.
    Later,
}

/// The segments of the attempts of one task, from its creation.
fn compose_task(
    timeline: &mut Timeline,
    times: &TaskTimes,
    attempts: &TaskAttempts,
    close: TaskClose,
    ids: TaskIds,
    as_of: DateTime<Utc>,
) -> Result<GenerationEnd, TimingConflictInfo> {
    let mut previous = Bound::new(times.created_at, TimingBoundary::TaskCreated);
    let mut order = AttemptOrder::First;
    for record in &attempts.records {
        previous = recorded_attempt(timeline, record, previous, order, ids)?;
        order = AttemptOrder::Later;
    }
    let end = match close {
        TaskClose::Live(_) => GenerationEnd::Open,
        TaskClose::Moved(moved) => GenerationEnd::At(moved),
    };
    match (attempts.open, close) {
        (OpenAttempt::Ended, TaskClose::Moved(_)) => match attempts.records.is_empty() {
            true => Err(missing(MissingRecord::AttemptRow)),
            false => Ok(GenerationEnd::At(previous)),
        },
        (OpenAttempt::Ended, TaskClose::Live(_)) => Err(missing(MissingRecord::OpenAttempt)),
        (OpenAttempt::NotStored, _) => {
            timeline.begin(
                previous,
                unknown(UnknownReason::AttemptNotCaptured),
                ids.task(),
            )?;
            Ok(end)
        }
        (OpenAttempt::Open(facts), _) => {
            let attempt = attempts.records.last().map_or(1, |last| last.attempt() + 1);
            let view = OpenView {
                times,
                facts,
                close,
                order,
                horizon: match close {
                    TaskClose::Live(_) => as_of,
                    TaskClose::Moved(moved) => moved.at,
                },
            };
            open_attempt(timeline, &view, previous, ids.attempt(attempt))?;
            Ok(end)
        }
    }
}

/// The claims before the claim that holds an attempt.
#[derive(Debug, Clone, Copy)]
enum EarlierClaims {
    /// The claims were not counted.
    NotCounted,
    /// The claim that holds the attempt is its only claim.
    OnlyClaim { first: Bound },
    /// `count` claims came before it, the first at `first`.
    Before { count: u32, first: Bound },
}

fn earlier_claims(count: u32, first_claimed_at: DateTime<Utc>) -> EarlierClaims {
    let first = Bound::new(first_claimed_at, TimingBoundary::FirstClaim);
    match count.saturating_sub(1) {
        0 => EarlierClaims::OnlyClaim { first },
        earlier => EarlierClaims::Before {
            count: earlier,
            first,
        },
    }
}

/// The queue wait of an attempt: from the creation of the task for the
/// first attempt; a retry wait, then the queue wait from its due time, for
/// a retry.
fn queue_wait(
    timeline: &mut Timeline,
    previous: Bound,
    enqueued: Bound,
    queue_phase: SegmentPhaseInfo,
    order: AttemptOrder,
    source: SegmentSourceInfo,
) -> Result<(), TimingConflictInfo> {
    match order {
        AttemptOrder::First => {
            timeline.begin(previous, queue_phase, source)?;
            timeline.pass(enqueued)
        }
        AttemptOrder::Later => {
            timeline.begin(previous, SegmentPhaseInfo::RetryWait, source)?;
            timeline.begin(enqueued, queue_phase, source)
        }
    }
}

/// The claims before `claim`, then the claimed phase from `claim`.
fn claim_segments(
    timeline: &mut Timeline,
    earlier: EarlierClaims,
    claim: Bound,
    claimed: SegmentClaimInfo,
    source: SegmentSourceInfo,
) -> Result<(), TimingConflictInfo> {
    match earlier {
        EarlierClaims::NotCounted => {}
        EarlierClaims::OnlyClaim { first } => timeline.pass(first)?,
        EarlierClaims::Before { count, first } => timeline.begin(
            first,
            SegmentPhaseInfo::Claimed {
                claim: SegmentClaimInfo::Earlier { count },
            },
            source,
        )?,
    }
    timeline.begin(claim, SegmentPhaseInfo::Claimed { claim: claimed }, source)
}

fn finalizing_segment(
    timeline: &mut Timeline,
    finalizing: Finalizing,
    source: SegmentSourceInfo,
) -> Result<(), TimingConflictInfo> {
    match finalizing {
        Finalizing::Marked { at } => timeline.begin(
            Bound::new(at, TimingBoundary::Finalizing),
            SegmentPhaseInfo::Finalizing,
            source,
        ),
        Finalizing::NotMarked => Ok(()),
    }
}

/// The segments of one attempt that has its row; returns the time it was
/// recorded, where the next segment begins.
fn recorded_attempt(
    timeline: &mut Timeline,
    record: &AttemptRecord,
    previous: Bound,
    order: AttemptOrder,
    ids: TaskIds,
) -> Result<Bound, TimingConflictInfo> {
    let source = ids.attempt(record.attempt());
    let recorded = Bound::new(record.finished_at(), TimingBoundary::AttemptRecorded);
    let phases = match record.phases() {
        PhaseRecord::NotCaptured => {
            timeline.begin(previous, unknown(UnknownReason::AttemptNotCaptured), source)?;
            return Ok(recorded);
        }
        PhaseRecord::Captured(phases) => phases,
    };
    let (queue_phase, earlier) = match phases.claims() {
        ClaimRecord::NotCounted => (
            unknown(UnknownReason::ClaimsNotCounted),
            EarlierClaims::NotCounted,
        ),
        ClaimRecord::Counted {
            count,
            first_claimed_at,
        } => (
            SegmentPhaseInfo::Enqueued,
            earlier_claims(count.get(), first_claimed_at),
        ),
    };
    let enqueued = Bound::new(phases.enqueued_at(), TimingBoundary::AttemptEnqueued);
    queue_wait(timeline, previous, enqueued, queue_phase, order, source)?;
    let placement = match phases.recorded_by() {
        RecordedBy::Worker { placement } => placement_info(placement),
        RecordedBy::Recovery { .. } | RecordedBy::Cancel => AttemptPlacementInfo::NotObserved,
    };
    let claim = Bound::new(phases.claimed_at(), TimingBoundary::Claim);
    claim_segments(
        timeline,
        earlier,
        claim,
        SegmentClaimInfo::Recorded { placement },
        source,
    )?;
    if let AttemptPlacementInfo::Placed { placed_at } = placement {
        timeline.pass(Bound::new(placed_at, TimingBoundary::Placement))?;
    }
    timeline.begin(
        Bound::new(record.started_at(), TimingBoundary::Start),
        SegmentPhaseInfo::Running,
        source,
    )?;
    finalizing_segment(timeline, phases.finalizing(), source)?;
    if let RecordedBy::Recovery { last_seen_at } = phases.recorded_by() {
        timeline.begin(
            Bound::new(last_seen_at, TimingBoundary::LastSeen),
            unknown(UnknownReason::WorkerLost),
            source,
        )?;
    }
    Ok(recorded)
}

/// The attempt of a task that has no attempt row: it is live, or a cancel,
/// an expiry or a pause abandon moved its task. `horizon` is the read time
/// for a live task and the move time for a moved task.
#[derive(Debug, Clone, Copy)]
struct OpenView<'a> {
    times: &'a TaskTimes,
    facts: OpenAttemptFacts,
    close: TaskClose,
    order: AttemptOrder,
    horizon: DateTime<Utc>,
}

/// The state of an open attempt at its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenState {
    Pending,
    Claimed,
    Running,
}

impl OpenView<'_> {
    /// The status of a live task; for a moved task, the state that its last
    /// claim and start show (a retry and a release clear both).
    fn state(&self) -> OpenState {
        match (self.close, self.times.claimed_at, self.times.started_at) {
            (TaskClose::Live(LiveStatus::Pending), _, _) => OpenState::Pending,
            (TaskClose::Live(LiveStatus::Claimed), _, _) => OpenState::Claimed,
            (TaskClose::Live(LiveStatus::Running), _, _) => OpenState::Running,
            (TaskClose::Moved(_), _, Some(_)) => OpenState::Running,
            (TaskClose::Moved(_), Some(_), None) => OpenState::Claimed,
            (TaskClose::Moved(_), None, None) => OpenState::Pending,
        }
    }
}

fn open_attempt(
    timeline: &mut Timeline,
    view: &OpenView<'_>,
    previous: Bound,
    source: SegmentSourceInfo,
) -> Result<(), TimingConflictInfo> {
    let state = view.state();
    let claims = view.facts.claims();
    let queue_phase = match claims {
        OpenClaims::NotCounted => unknown(UnknownReason::ClaimsNotCounted),
        OpenClaims::NotClaimed | OpenClaims::Counted { .. } => SegmentPhaseInfo::Enqueued,
    };
    let not_due = view.times.enqueued_at > view.horizon;
    match (view.order, state, not_due) {
        (AttemptOrder::Later, OpenState::Pending, true) => {
            return timeline.begin(previous, SegmentPhaseInfo::RetryWait, source);
        }
        (AttemptOrder::First, _, _)
        | (AttemptOrder::Later, OpenState::Pending, false)
        | (AttemptOrder::Later, OpenState::Claimed | OpenState::Running, _) => {
            let enqueued = Bound::new(view.times.enqueued_at, TimingBoundary::AttemptEnqueued);
            queue_wait(
                timeline,
                previous,
                enqueued,
                queue_phase,
                view.order,
                source,
            )?;
        }
    }
    match state {
        OpenState::Pending => match claims {
            OpenClaims::Counted {
                count,
                first_claimed_at,
            } => timeline.begin(
                Bound::new(first_claimed_at, TimingBoundary::FirstClaim),
                SegmentPhaseInfo::Claimed {
                    claim: SegmentClaimInfo::Earlier { count: count.get() },
                },
                source,
            ),
            OpenClaims::NotClaimed | OpenClaims::NotCounted => Ok(()),
        },
        OpenState::Claimed => open_claim(timeline, view, source),
        OpenState::Running => {
            open_claim(timeline, view, source)?;
            let started_at = view
                .times
                .started_at
                .ok_or_else(|| missing(MissingRecord::TaskTimes))?;
            timeline.begin(
                Bound::new(started_at, TimingBoundary::Start),
                SegmentPhaseInfo::Running,
                source,
            )?;
            finalizing_segment(timeline, view.facts.finalizing(), source)
        }
    }
}

/// The claims of a claimed or running open attempt, then its claim.
fn open_claim(
    timeline: &mut Timeline,
    view: &OpenView<'_>,
    source: SegmentSourceInfo,
) -> Result<(), TimingConflictInfo> {
    let claimed_at = view
        .times
        .claimed_at
        .ok_or_else(|| missing(MissingRecord::TaskTimes))?;
    let earlier = match view.facts.claims() {
        OpenClaims::NotCounted => EarlierClaims::NotCounted,
        OpenClaims::NotClaimed => return Err(missing(MissingRecord::TaskTimes)),
        OpenClaims::Counted {
            count,
            first_claimed_at,
        } => earlier_claims(count.get(), first_claimed_at),
    };
    claim_segments(
        timeline,
        earlier,
        Bound::new(claimed_at, TimingBoundary::Claim),
        SegmentClaimInfo::NotRecorded,
        source,
    )
}

/// The pauses of a run in order, and the pause records that contradict
/// themselves.
#[derive(Debug, Default)]
struct Pauses {
    intervals: Vec<PauseIntervalInfo>,
    conflicts: Vec<TimingConflictInfo>,
}

impl Pauses {
    fn ended(&mut self, start: Bound, end: Bound, info: PauseEndInfo) {
        match check_order(start, end) {
            Ok(()) => self.intervals.push(PauseIntervalInfo {
                started_at: start.at,
                end: info,
            }),
            Err(conflict) => self.conflicts.push(conflict),
        }
    }
}

fn run_event_conflict(event: &RunEvent, name: WorkflowEventKind) -> TimingConflictInfo {
    TimingConflictInfo::UnexpectedEvent {
        event_id: event.event_id,
        event: name,
    }
}

/// One pause per `WORKFLOW_PAUSED` event, ended by the next
/// `WORKFLOW_RESUMED` event, else by the end of the run, else open.
fn compose_pauses(events: &[RunEvent], run: &RunFacts) -> Pauses {
    let mut pauses = Pauses::default();
    let mut open: Option<Bound> = None;
    for event in events {
        match (&event.kind, open) {
            (RunEventKind::WorkflowPaused, None) => {
                open = Some(Bound::new(event.occurred_at, TimingBoundary::RunPaused));
            }
            (RunEventKind::WorkflowPaused, Some(_)) => pauses
                .conflicts
                .push(run_event_conflict(event, WorkflowEventKind::WorkflowPaused)),
            (RunEventKind::WorkflowResumed, Some(start)) => {
                open = None;
                pauses.ended(
                    start,
                    Bound::new(event.occurred_at, TimingBoundary::RunResumed),
                    PauseEndInfo::Resumed {
                        at: event.occurred_at,
                    },
                );
            }
            (RunEventKind::WorkflowResumed, None) => pauses.conflicts.push(run_event_conflict(
                event,
                WorkflowEventKind::WorkflowResumed,
            )),
            (RunEventKind::Node { .. }, _) => {}
        }
    }
    match (open, run.completed_at) {
        (None, _) => {}
        (Some(start), Some(ended_at)) => pauses.ended(
            start,
            Bound::new(ended_at, TimingBoundary::RunCompleted),
            PauseEndInfo::RunEnded { at: ended_at },
        ),
        (Some(start), None) => {
            match check_order(start, Bound::new(run.as_of, TimingBoundary::ReadTime)) {
                Ok(()) => pauses.intervals.push(PauseIntervalInfo {
                    started_at: start.at,
                    end: PauseEndInfo::Open,
                }),
                Err(conflict) => pauses.conflicts.push(conflict),
            }
        }
    }
    pauses
}

fn run_coverage(
    nodes: &[NodeFacts],
    timings: &[NodeTimingInfo],
    pause_conflicts: &[TimingConflictInfo],
) -> RunCoverageInfo {
    let mut conflicts: Vec<RunConflictInfo> = nodes
        .iter()
        .zip(timings)
        .filter_map(|(node, timing)| match timing {
            NodeTimingInfo::Inconsistent { .. } => Some(RunConflictInfo::Node {
                task_index: node.task_index,
            }),
            NodeTimingInfo::Exact { .. }
            | NodeTimingInfo::Partial { .. }
            | NodeTimingInfo::Unavailable { .. } => None,
        })
        .collect();
    conflicts.extend(
        pause_conflicts
            .iter()
            .map(|conflict| RunConflictInfo::Pause {
                conflict: *conflict,
            }),
    );
    if !conflicts.is_empty() {
        return RunCoverageInfo::Inconsistent { conflicts };
    }
    let reasons: BTreeSet<UnknownReason> = timings
        .iter()
        .filter_map(|timing| match timing {
            NodeTimingInfo::Partial { reasons, .. } => Some(reasons),
            NodeTimingInfo::Exact { .. }
            | NodeTimingInfo::Unavailable { .. }
            | NodeTimingInfo::Inconsistent { .. } => None,
        })
        .flatten()
        .copied()
        .collect();
    match reasons.is_empty() {
        true => RunCoverageInfo::Exact,
        false => RunCoverageInfo::Partial {
            reasons: reasons.into_iter().collect(),
        },
    }
}

/// The end of the range of a run and whether anything is open.
#[derive(Debug, Clone, Copy)]
struct RunRange {
    ended_at: DateTime<Utc>,
    open: bool,
}

impl RunRange {
    fn reach(&mut self, at: DateTime<Utc>) {
        self.ended_at = self.ended_at.max(at);
    }
}

fn run_range(run: &RunFacts, timings: &[NodeTimingInfo], pauses: &[PauseIntervalInfo]) -> RunRange {
    let mut range = RunRange {
        ended_at: run.created_at,
        open: false,
    };
    if let Some(ended_at) = run.completed_at {
        range.reach(ended_at);
    }
    for timing in timings {
        let (segments, endpoint) = match timing {
            NodeTimingInfo::Exact { segments, endpoint }
            | NodeTimingInfo::Partial {
                segments, endpoint, ..
            } => (segments, endpoint),
            NodeTimingInfo::Unavailable { .. } | NodeTimingInfo::Inconsistent { .. } => continue,
        };
        for segment in segments {
            match segment.end {
                SegmentEndInfo::Ended { at } => range.reach(at),
                SegmentEndInfo::Open => {
                    range.open = true;
                    range.reach(run.as_of);
                }
            }
        }
        if let NodeEndpointInfo::Terminal { at, .. } = endpoint {
            range.reach(*at);
        }
    }
    for pause in pauses {
        match pause.end {
            PauseEndInfo::Resumed { at } | PauseEndInfo::RunEnded { at } => range.reach(at),
            PauseEndInfo::Open => {
                range.open = true;
                range.reach(run.as_of);
            }
        }
    }
    range
}
