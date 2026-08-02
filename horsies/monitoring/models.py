"""Response models for the monitoring query API.

Field names here are the wire contract the monitoring UI consumes. Two
conventions hold across every model:

* Datetimes are timezone-aware UTC.
* Optional text is normalized before it reaches a model: horsies stores ``''``
  for cleared text, and empty strings arrive here as ``None``.

These models are unstable pre-1.0.
"""

from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel


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
# Tasks
# --------------------------------------------------------------------------- #
class TaskAttemptInfo(BaseModel):
    """One immutable execution attempt of a task (from horsies_task_attempts)."""

    attempt: int
    outcome: str
    will_retry: bool
    error_code: str | None
    error_message: str | None
    failed_reason: str | None
    worker_hostname: str | None
    started_at: datetime | None
    finished_at: datetime | None


class LeafTaskInfo(BaseModel):
    """The task row backing a list entry or a workflow node.

    ``queue_s`` is dispatch-to-start wait; ``exec_s`` is start-to-finish run
    time. Both are split so queue wait is never conflated with run time, and
    both are ``None`` when the span cannot be determined.
    """

    task_id: str
    status: str
    error_code: str | None
    failed_reason: str | None
    retry_count: int
    max_retries: int
    enqueued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    queue_s: int | None
    exec_s: int | None
    worker_hostname: str | None
    good_until: datetime | None


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


class GroupRow(BaseModel):
    """One group's per-status task counts.

    ``group`` is the grouped value, ``'TOTAL'`` for the rollup row, or
    ``'unknown'`` for a null group key.
    """

    group: str
    total: int
    pending: int
    claimed: int
    running: int
    completed: int
    failed: int
    cancelled: int
    expired: int
    retried: int


class Breakdown(BaseModel):
    """Per-group status pivot plus the rollup TOTAL row.

    ``groups`` is capped to the top ``limit`` by task count; ``group_count``
    is the true number of distinct groups, so the UI can flag truncation. The
    ``total`` row always covers every matching task, not just shown groups.
    """

    group_by: str
    groups: list[GroupRow]
    total: GroupRow
    group_count: int


class TaskSummary(BaseModel):
    """One task row, shaped for the list view.

    ``error_category`` names the error family; ``None`` when there is no
    error. ``worker_id`` is the claiming worker.
    """

    id: str
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
    queue_s: int | None
    exec_s: int | None


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


# --------------------------------------------------------------------------- #
# Workflows
# --------------------------------------------------------------------------- #
class WorkflowRunSummary(BaseModel):
    """One workflow run, root or subworkflow.

    ``wall_s`` counts up while ``completed_at`` is null, including for PAUSED
    runs.
    """

    id: str
    name: str
    definition_key: str | None
    status: str
    created_at: datetime | None
    completed_at: datetime | None
    wall_s: int | None


class WorkflowNodeInfo(BaseModel):
    """A single node in a run's DAG. ``task_index`` is the stable key.

    ``exec_s`` is execution time only: null until the node starts running, so
    a node sitting in the queue does not accrue a misleading duration.
    ``child_total`` / ``child_failed`` roll up a subworkflow node's direct
    children and are both null for leaf nodes and for child runs with no task
    rows yet.
    """

    task_index: int
    node_id: str | None
    task_name: str
    node_status: str
    is_subworkflow: bool
    sub_workflow_id: str | None
    allow_failed_deps: bool
    started_at: datetime | None
    completed_at: datetime | None
    exec_s: int | None
    child_total: int | None
    child_failed: int | None


class WorkflowEdge(BaseModel):
    """A dependency edge: ``from_index`` must settle before ``to_index`` runs."""

    from_index: int
    to_index: int


class WorkflowRunDetail(BaseModel):
    """A run's metadata plus its full node/edge graph.

    ``failed_indices`` lists the ``task_index`` of every FAILED node in
    ascending order, so the UI can navigate failures without scanning the
    graph; ``failed_count`` is its length.
    """

    run: WorkflowRunSummary
    nodes: list[WorkflowNodeInfo]
    edges: list[WorkflowEdge]
    failed_count: int
    failed_indices: list[int]


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
class WorkerStateInfo(BaseModel):
    """Latest state snapshot for one worker (from horsies_worker_states).

    ``snapshot_age_s`` is how long ago the snapshot was written; ``stale`` is
    True once that age exceeds the snapshot threshold, signalling a worker
    that stopped reporting. Liveness is proven separately by an active ping.
    """

    worker_id: str
    hostname: str
    pid: int
    snapshot_at: datetime
    snapshot_age_s: int | None
    stale: bool
    worker_started_at: datetime
    uptime_s: int | None
    processes: int
    queues: list[str]
    queue_max_concurrency: dict[str, int] | None
    tasks_running: int
    tasks_claimed: int
    cluster_wide_cap: int | None
    memory_usage_mb: float | None
    memory_percent: float | None
    cpu_percent: float | None


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
    last_task_id: str | None
    run_count: int
    updated_at: datetime
