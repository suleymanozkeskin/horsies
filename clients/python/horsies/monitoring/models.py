"""Response models for the monitoring query API.

Field names here are the wire contract the monitoring UI consumes. Three
conventions hold across every model:

* Datetimes are timezone-aware UTC.
* Optional text is normalized before it reaches a model: horsies stores ``''``
  for cleared text, and empty strings arrive here as ``None``.
* A measured duration (``queue_s``, ``exec_s``, ``wall_s``,
  ``snapshot_age_s``, ``uptime_s``, ``wait_s``) is a float of seconds,
  exact to the microsecond.

These models are unstable pre-1.0.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from horsies._catalog.history import TaskAbsenceReason
from horsies.core.models.tasks import Execution


class ErrorCategory(str, enum.Enum):
    """Family an error code belongs to.

    An operator reads these differently: ``OPERATIONAL`` means infrastructure
    is failing, ``CONTRACT`` means a task's code or serialization is wrong,
    ``OUTCOME`` is a terminal lifecycle state (cancelled/expired/timeout),
    ``RETRIEVAL`` is a result-fetch state, and ``DOMAIN`` is a business error
    the task itself chose to return.
    """

    OPERATIONAL = 'OPERATIONAL'
    CONTRACT = 'CONTRACT'
    RETRIEVAL = 'RETRIEVAL'
    OUTCOME = 'OUTCOME'
    DOMAIN = 'DOMAIN'


# --------------------------------------------------------------------------- #
# Lifecycle record: the phases of an attempt and the timing of a run
# --------------------------------------------------------------------------- #
# Every union is told apart by ``kind``. A model holds times, not durations: a
# client subtracts the two times of a segment.


class UnknownReason(str, enum.Enum):
    """Why a segment is ``unknown``."""

    ATTEMPT_NOT_CAPTURED = 'ATTEMPT_NOT_CAPTURED'
    """The attempt row has no phase record."""
    CLAIMS_NOT_COUNTED = 'CLAIMS_NOT_COUNTED'
    """The claims of the attempt were not counted: earlier claims can hide in its queue wait."""
    HISTORY_PRUNED = 'HISTORY_PRUNED'
    """Retention removed the history row of the task."""
    WORKER_LOST = 'WORKER_LOST'
    """Recovery recorded the attempt after its last sign of life."""


class UnavailableReason(str, enum.Enum):
    """Why the record cannot show the timing of a run."""

    RUN_BEFORE_CAPTURE = 'RUN_BEFORE_CAPTURE'
    """The run began before the lifecycle record (``captured_since``)."""
    TIMING_BOUND_EXCEEDED = 'TIMING_BOUND_EXCEEDED'
    """The run has more events than one read takes (100,000)."""


class TimingBoundary(str, enum.Enum):
    """One boundary of the phase timing, as a ``clock_order`` conflict names it."""

    NODE_CREATED = 'NODE_CREATED'
    NODE_READY = 'NODE_READY'
    NODE_ENQUEUED = 'NODE_ENQUEUED'
    NODE_COMPLETED = 'NODE_COMPLETED'
    TASK_CREATED = 'TASK_CREATED'
    ATTEMPT_ENQUEUED = 'ATTEMPT_ENQUEUED'
    FIRST_CLAIM = 'FIRST_CLAIM'
    CLAIM = 'CLAIM'
    PLACEMENT = 'PLACEMENT'
    START = 'START'
    FINALIZING = 'FINALIZING'
    LAST_SEEN = 'LAST_SEEN'
    ATTEMPT_RECORDED = 'ATTEMPT_RECORDED'
    TASK_MOVED = 'TASK_MOVED'
    CHILD_CREATED = 'CHILD_CREATED'
    CHILD_COMPLETED = 'CHILD_COMPLETED'
    RUN_PAUSED = 'RUN_PAUSED'
    RUN_RESUMED = 'RUN_RESUMED'
    RUN_COMPLETED = 'RUN_COMPLETED'
    READ_TIME = 'READ_TIME'


class MissingRecord(str, enum.Enum):
    """A record that the state of a node needs and that is not there."""

    LINKED_TASK = 'LINKED_TASK'
    """The node links a task that no live row and no event of the run holds."""
    LINKED_WORK = 'LINKED_WORK'
    """The node is ENQUEUED or RUNNING with no task, or RUNNING with no child run."""
    CHILD_RUN = 'CHILD_RUN'
    """The node names a child run that no row holds."""
    COMPLETION_TIME = 'COMPLETION_TIME'
    """The node has a terminal status and no completion time."""
    TASK_TIMES = 'TASK_TIMES'
    """The task row lacks a claim time, a start time or a counted claim that its status needs."""
    ATTEMPT_ROW = 'ATTEMPT_ROW'
    """The archive says the last attempt ended with its row, and it holds no attempt row."""
    OPEN_ATTEMPT = 'OPEN_ATTEMPT'
    """The live task has no attempt without a row."""


class WorkflowEventKind(str, enum.Enum):
    """The kind of one event of a run."""

    WORKFLOW_PAUSED = 'WORKFLOW_PAUSED'
    WORKFLOW_RESUMED = 'WORKFLOW_RESUMED'
    NODE_READY = 'NODE_READY'
    NODE_ENQUEUED = 'NODE_ENQUEUED'
    TASK_MOVED = 'TASK_MOVED'


class ClaimsCountedInfo(BaseModel):
    """``count`` claims (1 or more) were taken for the attempt; the first at ``first_claimed_at``."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['counted'] = 'counted'
    count: int
    first_claimed_at: datetime


class ClaimsNotCountedInfo(BaseModel):
    """The attempt began before the lifecycle record: its claims were not counted."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['not_counted'] = 'not_counted'


AttemptClaimsInfo = Annotated[
    ClaimsCountedInfo | ClaimsNotCountedInfo,
    Field(discriminator='kind'),
]
"""The claims of one attempt."""


class PlacementPlacedInfo(BaseModel):
    """A child of the worker took the task at ``placed_at``: the claim time plus the measured slot wait."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['placed'] = 'placed'
    placed_at: datetime


class PlacementNotObservedInfo(BaseModel):
    """The writer of the attempt row observed no placement."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['not_observed'] = 'not_observed'


AttemptPlacementInfo = Annotated[
    PlacementPlacedInfo | PlacementNotObservedInfo,
    Field(discriminator='kind'),
]
"""When a child of the worker took the task of the attempt."""


class FinalizingMarkedInfo(BaseModel):
    """The child marked the end of the task body at ``finalizing_at``."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['marked'] = 'marked'
    finalizing_at: datetime


class FinalizingNotMarkedInfo(BaseModel):
    """No finalizing marker was written."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['not_marked'] = 'not_marked'


AttemptFinalizingInfo = Annotated[
    FinalizingMarkedInfo | FinalizingNotMarkedInfo,
    Field(discriminator='kind'),
]
"""The finalizing marker of the claim that ran the attempt."""


class RecordedByWorkerInfo(BaseModel):
    """The worker wrote the end of the attempt, with the placement it observed."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['worker'] = 'worker'
    placement: AttemptPlacementInfo


class RecordedByRecoveryInfo(BaseModel):
    """Stale RUNNING recovery wrote the attempt row. ``last_seen_at`` is the last sign of life."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['recovery'] = 'recovery'
    last_seen_at: datetime


class RecordedByCancelInfo(BaseModel):
    """The cancel statement wrote the attempt row: an operator cancelled the task while the attempt ran.

    The attempt ended at the cancel (``finished_at``). The cancel observes no placement.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['cancel'] = 'cancel'


AttemptRecordedByInfo = Annotated[
    RecordedByWorkerInfo | RecordedByRecoveryInfo | RecordedByCancelInfo,
    Field(discriminator='kind'),
]
"""Who wrote the attempt row."""


class PhasesCapturedInfo(BaseModel):
    """The phase times of the attempt.

    ``enqueued_at`` is the due time of the attempt; ``claimed_at`` is the claim
    that ran it.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['captured'] = 'captured'
    enqueued_at: datetime
    claimed_at: datetime
    claims: AttemptClaimsInfo
    finalizing: AttemptFinalizingInfo
    recorded_by: AttemptRecordedByInfo


class PhasesNotCapturedInfo(BaseModel):
    """The attempt row was written before the lifecycle record, or history holds a version-1 archive."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['not_captured'] = 'not_captured'


AttemptPhasesInfo = Annotated[
    PhasesCapturedInfo | PhasesNotCapturedInfo,
    Field(discriminator='kind'),
]
"""The phase record of one attempt row."""


class ClaimEarlierInfo(BaseModel):
    """``count`` claims before the claim that holds the attempt: lost leases and released claims."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['earlier'] = 'earlier'
    count: int


class ClaimRecordedInfo(BaseModel):
    """The claim that ran the attempt, from its attempt row, with the placement that the row holds."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['recorded'] = 'recorded'
    placement: AttemptPlacementInfo


class ClaimNotRecordedInfo(BaseModel):
    """The claim of an attempt that has no attempt row.

    The attempt still runs, or a cancel, an expiry or a pause abandon ended it.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['not_recorded'] = 'not_recorded'


SegmentClaimInfo = Annotated[
    ClaimEarlierInfo | ClaimRecordedInfo | ClaimNotRecordedInfo,
    Field(discriminator='kind'),
]
"""Which claim a ``claimed`` segment shows."""


class PhasePendingInfo(BaseModel):
    """The node waits for its dependencies."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['pending'] = 'pending'


class PhaseReadyInfo(BaseModel):
    """The dependencies of the node are met and nothing is dispatched."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['ready'] = 'ready'


class PhaseEnqueuedInfo(BaseModel):
    """The attempt is due and waits for a claim; a sub-workflow node waits for its child start."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['enqueued'] = 'enqueued'


class PhaseRetryWaitInfo(BaseModel):
    """The retry delay of the attempt has not passed."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['retry_wait'] = 'retry_wait'


class PhaseClaimedInfo(BaseModel):
    """A worker holds a claim and the task code did not start."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['claimed'] = 'claimed'
    claim: SegmentClaimInfo


class PhaseRunningInfo(BaseModel):
    """The task code runs; for a sub-workflow node, the child run runs."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['running'] = 'running'


class PhaseFinalizingInfo(BaseModel):
    """The task body ended and the attempt is not recorded yet."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['finalizing'] = 'finalizing'


class PhaseAdvancingInfo(BaseModel):
    """The task is in history (or the child run ended) and the workflow has not applied the node yet."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['advancing'] = 'advancing'


class PhaseUnknownInfo(BaseModel):
    """A known interval whose phase the record cannot show."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['unknown'] = 'unknown'
    reason: UnknownReason


SegmentPhaseInfo = Annotated[
    PhasePendingInfo
    | PhaseReadyInfo
    | PhaseEnqueuedInfo
    | PhaseRetryWaitInfo
    | PhaseClaimedInfo
    | PhaseRunningInfo
    | PhaseFinalizingInfo
    | PhaseAdvancingInfo
    | PhaseUnknownInfo,
    Field(discriminator='kind'),
]
"""The phase of one segment."""


class SegmentEndedInfo(BaseModel):
    """The segment ended at ``at``."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['ended'] = 'ended'
    at: datetime


class SegmentOpenInfo(BaseModel):
    """The segment had not ended at the read (``as_of`` of the run timing)."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['open'] = 'open'


SegmentEndInfo = Annotated[
    SegmentEndedInfo | SegmentOpenInfo,
    Field(discriminator='kind'),
]
"""The end of one segment."""


class SourceNodeInfo(BaseModel):
    """The node itself: before, between and after its tasks, and the child run of a sub-workflow node."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['node'] = 'node'


class SourceTaskInfo(BaseModel):
    """The ``generation``-th task of the node (1-based), when the record holds no attempt of it."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['task'] = 'task'
    generation: int
    task_id: UUID


class SourceAttemptInfo(BaseModel):
    """Attempt ``attempt`` of the ``generation``-th task of the node (1-based)."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['attempt'] = 'attempt'
    generation: int
    task_id: UUID
    attempt: int


SegmentSourceInfo = Annotated[
    SourceNodeInfo | SourceTaskInfo | SourceAttemptInfo,
    Field(discriminator='kind'),
]
"""What one segment describes."""


class TimingSegmentInfo(BaseModel):
    """One phase interval of a node, from ``started_at`` to ``end``."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    phase: SegmentPhaseInfo
    started_at: datetime
    end: SegmentEndInfo
    source: SegmentSourceInfo


class EndpointTerminalInfo(BaseModel):
    """The node reached ``status`` at ``at``. It can come before the end of a task that ran on."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['terminal'] = 'terminal'
    status: Literal['COMPLETED', 'FAILED', 'SKIPPED']
    at: datetime


class EndpointNotTerminalInfo(BaseModel):
    """The node is not terminal."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['not_terminal'] = 'not_terminal'


NodeEndpointInfo = Annotated[
    EndpointTerminalInfo | EndpointNotTerminalInfo,
    Field(discriminator='kind'),
]
"""The terminal state of a node: a marker, not a segment."""


class ConflictClockOrderInfo(BaseModel):
    """Two boundaries in the wrong order.

    ``earlier`` comes first in the record, and its time ``earlier_at`` is after
    the time ``later_at`` of ``later``. A step of the database clock can cause it.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['clock_order'] = 'clock_order'
    earlier: TimingBoundary
    earlier_at: datetime
    later: TimingBoundary
    later_at: datetime


class ConflictMissingRecordInfo(BaseModel):
    """The state of the node needs a record that is not there."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['missing_record'] = 'missing_record'
    record: MissingRecord


class ConflictUnexpectedEventInfo(BaseModel):
    """An event that the state of the node or the run at that point does not allow."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['unexpected_event'] = 'unexpected_event'
    event_id: int
    event: WorkflowEventKind


TimingConflictInfo = Annotated[
    ConflictClockOrderInfo | ConflictMissingRecordInfo | ConflictUnexpectedEventInfo,
    Field(discriminator='kind'),
]
"""A contradiction in the record of a node or a run."""


class NodeTimingExactInfo(BaseModel):
    """Every boundary is a recorded time and no segment is ``unknown``."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['exact'] = 'exact'
    segments: list[TimingSegmentInfo]
    endpoint: NodeEndpointInfo


class NodeTimingPartialInfo(BaseModel):
    """At least one segment is ``unknown``; ``reasons`` holds each reason once, in name order."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['partial'] = 'partial'
    reasons: list[UnknownReason]
    segments: list[TimingSegmentInfo]
    endpoint: NodeEndpointInfo


class NodeTimingUnavailableInfo(BaseModel):
    """The record cannot show the timing of the run: no segment."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['unavailable'] = 'unavailable'
    reason: UnavailableReason


class NodeTimingInconsistentInfo(BaseModel):
    """The record of the node contradicts itself: no segment. ``conflict`` names it."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['inconsistent'] = 'inconsistent'
    conflict: TimingConflictInfo


NodeTimingInfo = Annotated[
    NodeTimingExactInfo
    | NodeTimingPartialInfo
    | NodeTimingUnavailableInfo
    | NodeTimingInconsistentInfo,
    Field(discriminator='kind'),
]
"""The phase timing of one node, tagged by its coverage label."""


class PauseResumedInfo(BaseModel):
    """A ``WORKFLOW_RESUMED`` event at ``at`` ended the pause."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['resumed'] = 'resumed'
    at: datetime


class PauseRunEndedInfo(BaseModel):
    """A cancel or a paused expiry ended the run at ``at`` while it was paused."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['run_ended'] = 'run_ended'
    at: datetime


class PauseOpenInfo(BaseModel):
    """The run is still paused."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['open'] = 'open'


PauseEndInfo = Annotated[
    PauseResumedInfo | PauseRunEndedInfo | PauseOpenInfo,
    Field(discriminator='kind'),
]
"""The end of one pause."""


class PauseIntervalInfo(BaseModel):
    """One pause of a run: from a ``WORKFLOW_PAUSED`` event to ``end``."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    started_at: datetime
    end: PauseEndInfo


class RunConflictNodeInfo(BaseModel):
    """The node at ``task_index`` is ``inconsistent``; its timing names the conflict."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['node'] = 'node'
    task_index: int


class RunConflictPauseInfo(BaseModel):
    """A pause record of the run contradicts itself."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['pause'] = 'pause'
    conflict: TimingConflictInfo


RunConflictInfo = Annotated[
    RunConflictNodeInfo | RunConflictPauseInfo,
    Field(discriminator='kind'),
]
"""One contradiction of a run."""


class RunCoverageExactInfo(BaseModel):
    """Every node is ``exact`` and every pause is in order."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['exact'] = 'exact'


class RunCoveragePartialInfo(BaseModel):
    """No conflict, and at least one node is ``partial``.

    ``reasons`` holds the reasons of every node, each once, in name order.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['partial'] = 'partial'
    reasons: list[UnknownReason]


class RunCoverageInconsistentInfo(BaseModel):
    """A node is ``inconsistent`` or a pause record contradicts itself; ``conflicts`` names each."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['inconsistent'] = 'inconsistent'
    conflicts: list[RunConflictInfo]


RunCoverageInfo = Annotated[
    RunCoverageExactInfo | RunCoveragePartialInfo | RunCoverageInconsistentInfo,
    Field(discriminator='kind'),
]
"""The coverage of a composed run."""


class RunTimingUnavailableInfo(BaseModel):
    """The record cannot show the timing of the run; every node is ``unavailable`` with the same reason."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['unavailable'] = 'unavailable'
    as_of: datetime
    captured_since: datetime
    reason: UnavailableReason


class RunTimingComposedInfo(BaseModel):
    """The timing of each node is composed.

    ``as_of`` is the database time of the read: an open segment ends there for
    drawing. ``captured_since`` is the start of the lifecycle record.
    ``range_started_at`` is the creation of the run; ``range_ended_at`` the
    latest segment end, endpoint, pause end or run end. ``open_segments`` is
    true when a segment or a pause is open. ``pauses`` holds the pauses in
    order, without a pause that a conflict of ``coverage`` names.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['composed'] = 'composed'
    as_of: datetime
    captured_since: datetime
    coverage: RunCoverageInfo
    range_started_at: datetime
    range_ended_at: datetime
    open_segments: bool
    pauses: list[PauseIntervalInfo]


RunTimingInfo = Annotated[
    RunTimingUnavailableInfo | RunTimingComposedInfo,
    Field(discriminator='kind'),
]
"""The phase timing of one run."""


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
class TaskAttemptInfo(BaseModel):
    """One immutable execution attempt of a task (from horsies_task_attempts).

    ``phases`` is the phase record of the attempt row.
    """

    attempt: int
    outcome: str
    will_retry: bool
    error_code: str | None
    error_message: str | None
    failed_reason: str | None
    worker_hostname: str | None
    started_at: datetime | None
    finished_at: datetime | None
    phases: AttemptPhasesInfo


class LeafTaskInfo(BaseModel):
    """The task row backing a list entry or a workflow node.

    ``queue_s`` is dispatch-to-start wait; ``exec_s`` is start-to-finish run
    time. Both are split so queue wait is never conflated with run time, and
    both are ``None`` when the span cannot be determined. Both are seconds,
    exact to the microsecond.

    ``execution`` is the stored ``Execution`` of a live task, and ``None``
    for a task read from history, which does not keep the class.

    ``trace_id`` is the trace id of the span that sent the task, as 32
    lowercase hex digits, for a live task and a task in history; ``None``
    for a task sent with no trace.
    """

    task_id: UUID
    status: str
    error_code: str | None
    failed_reason: str | None
    retry_count: int
    max_retries: int
    enqueued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    queue_s: float | None
    exec_s: float | None
    worker_hostname: str | None
    good_until: datetime | None
    execution: Execution | None
    trace_id: str | None


class StatusCount(BaseModel):
    """Count of tasks in one status, for the overview cards."""

    status: str
    count: int


class FacetValue(BaseModel):
    """A distinct filter value and how many tasks carry it under the scope."""

    value: str
    count: int


class ErrorFacet(FacetValue):
    """An error-code facet, tagged with its taxonomy category."""

    category: str


class Facets(BaseModel):
    """Scoped distinct values (with counts) that drive the filter comboboxes.

    ``error_codes`` is capped for the dropdown; ``error_category_totals`` is
    the uncapped per-category rollup, so the taxonomy summary never
    undercounts. Both count tasks carrying an error code, which includes
    COMPLETED tasks.
    """

    workers: list[FacetValue]
    task_names: list[FacetValue]
    queues: list[FacetValue]
    error_codes: list[ErrorFacet]
    error_category_totals: dict[str, int]


class BreakdownValueGroup(BaseModel):
    """The tasks whose group column holds ``value``: a worker id, a task name or a queue name."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['value'] = 'value'
    value: str


class BreakdownUnclaimedGroup(BaseModel):
    """The tasks that no worker claimed. Only the ``worker`` grouping has this group.

    A worker whose id is the text ``'unknown'`` is a ``BreakdownValueGroup``
    of its own, never this group.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['unclaimed'] = 'unclaimed'


BreakdownGroup = Annotated[
    BreakdownValueGroup | BreakdownUnclaimedGroup,
    Field(discriminator='kind'),
]
"""What one breakdown group holds: one stored value, or the unclaimed tasks."""


class BreakdownCounts(BaseModel):
    """Task counts per status, the task total, and the tasks retried at least once."""

    total: int
    pending: int
    claimed: int
    running: int
    completed: int
    failed: int
    cancelled: int
    expired: int
    retried: int


class GroupRow(BreakdownCounts):
    """One group's per-status task counts."""

    group: BreakdownGroup


class Breakdown(BaseModel):
    """Per-group status pivot plus the counts of every group.

    ``groups`` is capped to the top ``limit`` by task count; ``group_count``
    is the true number of distinct groups, so the UI can flag truncation.
    ``total`` always covers every matching task, not just shown groups.
    Groups come by total, highest first, then by the group text in
    code-point order; the unclaimed group sorts as the text ``'unknown'``,
    after a value group ``'unknown'`` with the same total.
    """

    group_by: str
    groups: list[GroupRow]
    total: BreakdownCounts
    group_count: int


class TaskSummary(BaseModel):
    """One task row, shaped for the list view.

    ``error_category`` names the error family; ``None`` when there is no
    error. ``worker_id`` is the claiming worker. ``queue_s`` and ``exec_s``
    are the spans of ``LeafTaskInfo``: seconds, exact to the microsecond.
    """

    id: UUID
    task_name: str
    queue_name: str
    status: str
    priority: int
    retry_count: int
    max_retries: int
    is_workflow_task: bool
    error_code: str | None
    error_category: str | None
    worker_hostname: str | None
    worker_id: str | None
    enqueued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    queue_s: float | None
    exec_s: float | None


class TaskListPage(BaseModel):
    """A paginated slice of tasks plus the total matching the active filters.

    ``total`` is exact when any filter is active and a planner estimate on
    the unfiltered view.
    """

    rows: list[TaskSummary]
    total: int


class TaskDetail(BaseModel):
    """A single task with its full attempt history, ordered by attempt.

    ``workflow_id`` and ``workflow_task_index`` locate the node a
    workflow-bound task belongs to, so a UI can link to the run rather than
    offering task actions it must refuse. Both are null for standalone tasks.
    """

    leaf: LeafTaskInfo
    task_name: str
    queue_name: str
    priority: int
    is_workflow_task: bool
    error_category: str | None
    attempts: list[TaskAttemptInfo]
    workflow_id: str | None
    workflow_task_index: int | None


@dataclass(frozen=True, slots=True)
class TaskDetailNotFound:
    """No live row and no history row has this task id."""

    task_id: UUID
    reason: TaskAbsenceReason


# --------------------------------------------------------------------------- #
# Workflows
# --------------------------------------------------------------------------- #
class WorkflowRunSummary(BaseModel):
    """One workflow run, root or subworkflow.

    ``wall_s`` counts up while ``completed_at`` is null, including for PAUSED
    runs. It is seconds, exact to the microsecond.
    """

    id: str
    name: str
    definition_key: str | None
    status: str
    created_at: datetime | None
    completed_at: datetime | None
    wall_s: float | None


class WorkflowNodeInfo(BaseModel):
    """A single node in a run's DAG. ``task_index`` is the stable key.

    ``exec_s`` is execution time only, in seconds, exact to the microsecond:
    null until the node starts running, so a node sitting in the queue does
    not accrue a misleading duration.
    ``child_total`` / ``child_failed`` roll up a subworkflow node's direct
    children and are both null for leaf nodes and for child runs with no task
    rows yet. ``timing`` is the phase timing of the node.
    """

    task_index: int
    node_id: str | None
    task_name: str
    node_status: str
    is_subworkflow: bool
    sub_workflow_id: str | None
    allow_failed_dependencies: bool
    started_at: datetime | None
    completed_at: datetime | None
    exec_s: float | None
    child_total: int | None
    child_failed: int | None
    timing: NodeTimingInfo


class WorkflowEdge(BaseModel):
    """A dependency edge: ``from_index`` must settle before ``to_index`` runs."""

    from_index: int
    to_index: int


class WorkflowRunDetail(BaseModel):
    """A run's metadata plus its full node/edge graph.

    ``failed_indices`` lists the ``task_index`` of every FAILED node in
    ascending order, so the UI can navigate failures without scanning the
    graph; ``failed_count`` is its length. ``trace_id`` is the trace id of
    the span that started the run, as 32 lowercase hex digits; ``None`` for
    a start with no trace. ``timing`` is the phase timing of the run.
    """

    run: WorkflowRunSummary
    nodes: list[WorkflowNodeInfo]
    edges: list[WorkflowEdge]
    failed_count: int
    failed_indices: list[int]
    trace_id: str | None
    timing: RunTimingInfo


class WorkflowTaskDetail(BaseModel):
    """Per-node detail: node-level error, backing task, and attempt history.

    Subworkflow nodes carry no backing task; their detail lives in the child
    run. ``leaf`` is also null when the backing task row no longer exists.
    """

    task_index: int
    node_id: str | None
    task_name: str
    node_status: str
    is_subworkflow: bool
    node_error: str | None
    leaf: LeafTaskInfo | None
    attempts: list[TaskAttemptInfo]


# --------------------------------------------------------------------------- #
# Workers and schedules
# --------------------------------------------------------------------------- #
class DownSlotInfo(BaseModel):
    """One slot that was down when the worker wrote its snapshot.

    ``cause`` describes the cause of the last failed child start, and
    ``detail`` is its error text.
    """

    slot: int
    cause: str
    detail: str
    down_since: datetime
    failed_boots: int


class ProfileNamedInfo(BaseModel):
    """The worker ran the profile ``name`` of ``AppConfig.worker_profiles``."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['named'] = 'named'
    name: str


class ProfileNotRecordedInfo(BaseModel):
    """The snapshot holds no profile name: a worker of a build before schema
    migration 1004 wrote it."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal['not_recorded'] = 'not_recorded'


SnapshotProfileInfo = Annotated[
    ProfileNamedInfo | ProfileNotRecordedInfo,
    Field(discriminator='kind'),
]
"""The worker profile that a snapshot names."""


class WorkerStateInfo(BaseModel):
    """Latest state snapshot for one worker (from horsies_worker_states).

    ``snapshot_age_s`` is how long ago the snapshot was written, and
    ``uptime_s`` how long the worker has run; both are seconds, exact to the
    microsecond. ``stale`` is True once that age exceeds the snapshot
    threshold, signalling a worker that stopped reporting. Liveness is proven
    separately by an active ping.

    ``slot_health`` is ``all_serving`` or ``slots_down`` (``down_slots`` lists
    them, in slot order). ``down_slots`` is empty unless ``slot_health`` is
    ``slots_down``.

    ``profile`` names the worker profile that the worker ran:
    ``ProfileNamedInfo`` or ``ProfileNotRecordedInfo``.
    """

    worker_id: str
    hostname: str
    pid: int
    snapshot_at: datetime
    snapshot_age_s: float | None
    stale: bool
    worker_started_at: datetime
    uptime_s: float | None
    processes: int
    queues: list[str]
    queue_max_concurrency: dict[str, int]
    tasks_running: int
    tasks_claimed: int
    cluster_wide_cap: int | None
    memory_usage_mb: float | None
    memory_percent: float | None
    cpu_percent: float | None
    slot_health: Literal['all_serving', 'slots_down']
    down_slots: list[DownSlotInfo]
    profile: SnapshotProfileInfo


class BudgetLoadInfo(BaseModel):
    """One claim budget of a live worker.

    ``capacity`` (children or seats in service) and ``serves`` come from the
    worker's latest snapshot. ``in_flight`` counts, at the read, its RUNNING
    rows and CLAIMED rows with a live lease of the classes in ``serves``.
    ``free`` is ``capacity`` minus ``in_flight``, at least 0.
    """

    budget: Execution
    capacity: int
    serves: list[Execution]
    in_flight: int
    free: int


class WorkerClassLoadInfo(BaseModel):
    """The claim budgets of one live worker, in execution class order."""

    worker_id: str
    snapshot_at: datetime
    budgets: list[BudgetLoadInfo]


class StreamServedInfo(BaseModel):
    """Live workers serve the stream with capacity in service.

    ``workers`` counts them; ``capacity`` sums the capacity in service of
    the budget that serves the class on each.
    """

    kind: Literal['served']
    workers: int
    capacity: int


class StreamNoCapacityInfo(BaseModel):
    """Live workers serve the stream, and none has capacity in service for it."""

    kind: Literal['no_capacity']
    workers: int


class StreamUnservedInfo(BaseModel):
    """No live worker serves the stream."""

    kind: Literal['unserved']


class StreamWaitingInfo(BaseModel):
    """At least one row is due. ``wait_s`` is the wait of the oldest due row,
    in seconds, exact to the microsecond."""

    kind: Literal['waiting']
    oldest_due_at: datetime
    wait_s: float


class StreamNoneDueInfo(BaseModel):
    """Every row is due later; ``next_due_at`` is the earliest due time."""

    kind: Literal['none_due']
    next_due_at: datetime


class PendingStreamInfo(BaseModel):
    """The PENDING rows of one queue and one execution class."""

    queue_name: str
    execution: Execution
    coverage: Annotated[
        StreamServedInfo | StreamNoCapacityInfo | StreamUnservedInfo,
        Field(discriminator='kind'),
    ]
    wait: Annotated[
        StreamWaitingInfo | StreamNoneDueInfo,
        Field(discriminator='kind'),
    ]


class ClassReportInfo(BaseModel):
    """Pending streams and the budget load of each live worker.

    ``checked_at`` is database time. A worker is live while its latest
    snapshot is at most ``live_window_s`` old. ``streams`` is ordered by
    queue name, then by execution class. ``workers`` holds the live workers,
    by worker id.
    """

    checked_at: datetime
    live_window_s: int
    streams: list[PendingStreamInfo]
    workers: list[WorkerClassLoadInfo]


class WorkerPingInfo(BaseModel):
    """One worker's reply to an active liveness ping."""

    worker_id: str
    hostname: str
    pid: int
    round_trip_ms: float


class LivenessReport(BaseModel):
    """Database reachability plus the workers that answered an active ping."""

    db_latency_ms: float | None
    db_reachable: bool
    workers: list[WorkerPingInfo]


class WorkerHistoryPoint(BaseModel):
    """One timeseries point for a worker's load/resource charts."""

    snapshot_at: datetime
    tasks_running: int
    tasks_claimed: int
    cpu_percent: float | None
    memory_usage_mb: float | None
    memory_percent: float | None


class ScheduleStateInfo(BaseModel):
    """One recurring schedule's execution state (from horsies_schedule_state)."""

    schedule_name: str
    last_run_at: datetime | None
    next_run_at: datetime | None
    last_task_id: UUID | None
    run_count: int
    updated_at: datetime
