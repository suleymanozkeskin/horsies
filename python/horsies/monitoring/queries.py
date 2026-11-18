"""Read queries backing the monitoring UI.

Every query takes the broker and returns a ``MonitoringResult``. Each runs
in the core, on the broker's core handle (``horsies._core``): the core sends
the one statement of the read's 0007 function in autocommit, and the query
validates the JSON text of the core's model into the response model with
``validate_core_model_json`` of the codec. The error side has two states
that a read gives:

* ``MonitoringQueryDbOperationFailed``: a database failure that the core
  reports, or a core boundary error of the call (``CoreCallRefusal``).
* ``MonitoringQueryHistoryContractViolation``: stored history data that
  does not decode, in ``task_stats``, ``list_tasks``, ``get_task_detail``
  and ``get_workflow_node``.

``MonitoringQueryEventLoopLimit`` stays in the union and no read gives it:
the reads select no SQLAlchemy resource set. Every read raises
``ForkedAfterCoreStart`` and ``CoreStopped``: they are states of the
process, never a result (``call_refusal_or_raise``). Nothing else is caught. A row
that does not exist is ``Ok(None)``, not an error. ``get_task_detail`` names
it: ``Ok(TaskDetailNotFound)`` with the reason the reads know. Each read
sends one statement, so the live read and the history read see the same
rows.

Sort keys and group columns are ``Literal`` types; each passes as a bound
argument, and the monitoring function maps it to a fixed column text, so no
caller-supplied string reaches SQL text.

The live table holds no end time, error code, failed reason or result: a
task moves to history when it ends. The live side reads these facts as NULL,
a sort on them sorts every live row as NULL, and an error-code or category
filter selects no live row.

Load characteristics: ``task_stats``, ``task_facets`` and ``task_breakdown``
aggregate the live table plus the bounded history window. Each facet dimension
groups both lifecycle sides and applies its cap only after their counts are
summed. Retention and the explicit history window bound the visited partitions;
the UI's refresh cadences are deliberately spaced because each open dashboard
multiplies the load.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Final, Literal

from pydantic import TypeAdapter

from horsies._core import (
    CoreError,
    MonitoringJson,
    TaskBreakdownQuery as CoreTaskBreakdownQuery,
    TaskDetailAbsent,
    TaskFacetsQuery as CoreTaskFacetsQuery,
    TaskListQuery as CoreTaskListQuery,
    TaskStatsQuery as CoreTaskStatsQuery,
    WorkflowRunsQuery as CoreWorkflowRunsQuery,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import validate_core_model_json
from horsies.core.models.tasks import (
    ContractCode,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
)
from horsies.core.types.core_failure import (
    CoreCallRefusal,
    call_refusal_or_raise,
    core_error_retryable,
)
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring.history_window import MonitoringWindow
from horsies.monitoring.errors import (
    MonitoringQueryDbOperationFailed,
    MonitoringQueryHistoryContractViolation,
    MonitoringResult,
)
from horsies.monitoring.pages import BreakdownLimit, TaskPage, WorkflowRunsLimit
from horsies.monitoring.models import (
    Breakdown,
    ErrorCategory,
    Facets,
    ScheduleStateInfo,
    StatusCount,
    TaskAbsenceReason,
    TaskDetail,
    TaskDetailNotFound,
    TaskListPage,
    WorkflowRunDetail,
    WorkflowRunSummary,
    WorkflowTaskDetail,
)


# --------------------------------------------------------------------------- #
# Allowlists and vocabularies
# --------------------------------------------------------------------------- #
type TaskSortField = Literal[
    'enqueued_at',
    'started_at',
    'completed_at',
    'failed_at',
    'status',
    'task_name',
    'queue_name',
    'priority',
    'retry_count',
    'queue_s',
    'exec_s',
]

type TaskGroupBy = Literal['worker', 'task_name', 'queue']

type SortDirection = Literal['asc', 'desc']

# Built from horsies' own error-code enums so the mapping never drifts from
# the library. Any non-null code absent here is user-defined -> DOMAIN.
_CODE_TO_CATEGORY: dict[str, ErrorCategory] = {
    **{member.value: ErrorCategory.OPERATIONAL for member in OperationalErrorCode},
    **{member.value: ErrorCategory.CONTRACT for member in ContractCode},
    **{member.value: ErrorCategory.RETRIEVAL for member in RetrievalCode},
    **{member.value: ErrorCategory.OUTCOME for member in OutcomeCode},
}

# The integer range of a node index (the ``integer`` column of the node).
_INT4_MIN: Final = -(2**31)
_INT4_MAX: Final = 2**31 - 1

# The response type of each read, for ``validate_core_model_json``.
_STATUS_COUNTS: Final = TypeAdapter(list[StatusCount])
_FACETS: Final = TypeAdapter(Facets)
_BREAKDOWN: Final = TypeAdapter(Breakdown)
_TASK_LIST_PAGE: Final = TypeAdapter(TaskListPage)
_TASK_DETAIL: Final = TypeAdapter(TaskDetail)
_WORKFLOW_NAMES: Final = TypeAdapter(list[str])
_WORKFLOW_RUNS: Final = TypeAdapter(list[WorkflowRunSummary])
_WORKFLOW_RUN: Final[TypeAdapter[WorkflowRunDetail | None]] = TypeAdapter(
    WorkflowRunDetail | None,
)
_WORKFLOW_NODE: Final[TypeAdapter[WorkflowTaskDetail | None]] = TypeAdapter(
    WorkflowTaskDetail | None,
)
_SCHEDULES: Final = TypeAdapter(list[ScheduleStateInfo])


# --------------------------------------------------------------------------- #
# Value helpers
# --------------------------------------------------------------------------- #
def nz(value: str | None) -> str | None:
    """Normalize empty/whitespace text to None (horsies stores '' for cleared text)."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def as_utc(value: datetime) -> datetime:
    """Make a datetime timezone-aware, assuming UTC when naive.

    Horsies stores its timestamps as ``timestamptz``, so reads are normally
    aware. Coercing here keeps arithmetic against ``now`` from raising on a
    mixed-awareness subtraction.
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def elapsed_s(start: datetime | None, end: datetime | None) -> int | None:
    """Whole seconds between ``start`` and ``end``, using now when ``end`` is None."""
    if start is None:
        return None
    finish = as_utc(end) if end is not None else datetime.now(timezone.utc)
    return int((finish - as_utc(start)).total_seconds())


def span_s(start: datetime | None, end: datetime | None, *, live: bool) -> int | None:
    """Seconds between two timestamps.

    When ``end`` is missing, use now only if ``live`` says the span is still
    open; otherwise the span is unknowable and the result is None.
    """
    if start is None:
        return None
    if end is None:
        return elapsed_s(start, None) if live else None
    return elapsed_s(start, end)


def categorize_error_code(code: str | None) -> ErrorCategory | None:
    """Resolve an error code to its taxonomy family.

    Returns None when there is no error. A built-in code maps to its family;
    any other non-empty code is a user-defined domain error.
    """
    normalized = nz(code)
    if normalized is None:
        return None
    return _CODE_TO_CATEGORY.get(normalized, ErrorCategory.DOMAIN)


def _workflow_uuid(workflow_id: str) -> uuid.UUID | None:
    """The run id of a workflow id text, or ``None`` for a text that is not a
    UUID: such a text names no run. Pure."""
    try:
        return uuid.UUID(workflow_id)
    except ValueError:
        return None


def _core_call_failed(
    operation: str, exc: CoreCallRefusal,
) -> Err[MonitoringQueryDbOperationFailed]:
    """A core boundary error of a read call (the core did not start, or the
    loop has too many calls in flight): no statement ran, or the read was
    dropped. Nothing changed."""
    return Err(
        MonitoringQueryDbOperationFailed(
            message=f'{operation} failed: {exc}',
            retryable=core_error_retryable(exc),
            exception=exc,
        )
    )


# --------------------------------------------------------------------------- #
# Task queries
# --------------------------------------------------------------------------- #
async def task_stats(
    broker: PostgresBroker,
    *,
    window: MonitoringWindow,
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
) -> MonitoringResult[list[StatusCount]]:
    """Task counts by status, for the overview cards.

    Cross-lifecycle: live statuses count from the live table, terminal
    statuses from the history side over the window (terminalization
    moves finished rows there). Scoped by every filter except status,
    because the cards are the status selector. Always returns all
    seven statuses in a fixed order, including zeros, so cards never
    appear, disappear, or reorder between polls. A history row in the
    window whose status is not a task status answers
    ``MonitoringQueryHistoryContractViolation`` and no count.

    One statement (``horsies_monitoring_task_stats``), so both sides read
    one snapshot: a task that moves to history during the read counts once.

    The core runs the statement on the broker's core handle; the read
    validates the JSON text of the core's model.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it
    uses no SQLAlchemy resource set.
    """
    query = CoreTaskStatsQuery(
        window_lower=window.lower,
        window_upper=window.upper,
        task_names=list(task_names),
        queues=list(queues),
        workers=list(workers),
        error_codes=list(error_codes),
        error_categories=[category.value for category in error_categories],
        retried_only=retried_only,
    )
    operation = 'task stats query'
    try:
        outcome = await broker._core_handle.monitoring_task_stats_async(query)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_STATUS_COUNTS, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


async def task_facets(
    broker: PostgresBroker,
    *,
    window: MonitoringWindow,
    statuses: list[TaskStatus],
    error_categories: list[ErrorCategory],
    retried_only: bool,
) -> MonitoringResult[Facets]:
    """Distinct workers, task names, queues, and error codes, with counts.

    Scoped only by the coarse filters (status, retried) and deliberately not
    by worker/task_name/queue/error_code, so changing one of those selections
    never empties the others' option lists. ``error_category_totals`` is
    computed over the uncapped error-code list; the returned list is capped.

    ``error_categories`` narrows the returned code list to the selected
    families, which is how the code dropdown follows the taxonomy selection.
    It deliberately does not scope the totals: those totals are what the
    taxonomy strip offers as its own options, and a control that hid the
    families you have not picked could not express a second selection.

    One statement (``horsies_monitoring_task_facets``) that the core runs on
    the broker's core handle: the values and their counts, ranked after the
    two sides sum, with ties in the database collation.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it uses
    no SQLAlchemy resource set.
    """
    query = CoreTaskFacetsQuery(
        window_lower=window.lower,
        window_upper=window.upper,
        statuses=[status.value for status in statuses],
        error_categories=[category.value for category in error_categories],
        retried_only=retried_only,
    )
    operation = 'task facets query'
    try:
        outcome = await broker._core_handle.monitoring_task_facets_async(query)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_FACETS, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


async def task_breakdown(
    broker: PostgresBroker,
    *,
    window: MonitoringWindow,
    group_by: TaskGroupBy,
    statuses: list[TaskStatus],
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
    limit: BreakdownLimit,
) -> MonitoringResult[Breakdown]:
    """Per-group status pivot with the counts of every group.

    Each group row carries per-status counts; ``total`` sums every group,
    independent of ``limit``. Groups arrive count-descending and are capped
    to ``limit``, while ``group_count`` reports the true number of distinct
    groups. For ``worker``, the tasks that no worker claimed are their own
    group (``BreakdownUnclaimedGroup``), apart from a worker named
    ``'unknown'``.

    One statement (``horsies_monitoring_task_breakdown``), so both sides
    read one snapshot; at most ``limit`` groups leave the database.

    The core runs the statement on the broker's core handle; the read
    validates the JSON text of the core's model.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it
    uses no SQLAlchemy resource set.
    """
    query = CoreTaskBreakdownQuery(
        window_lower=window.lower,
        window_upper=window.upper,
        group_by=group_by,
        statuses=[status.value for status in statuses],
        task_names=list(task_names),
        queues=list(queues),
        workers=list(workers),
        error_codes=list(error_codes),
        error_categories=[category.value for category in error_categories],
        retried_only=retried_only,
        limit=limit.value,
    )
    operation = 'task breakdown query'
    try:
        outcome = await broker._core_handle.monitoring_task_breakdown_async(query)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_BREAKDOWN, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


async def list_tasks(
    broker: PostgresBroker,
    *,
    window: MonitoringWindow,
    statuses: list[TaskStatus],
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
    sort_by: TaskSortField,
    sort_dir: SortDirection,
    page: TaskPage,
) -> MonitoringResult[TaskListPage]:
    """A paginated, server-sorted, server-filtered slice of tasks.

    ``total`` is the count matching the filters, not the length of ``rows``,
    so the UI can paginate without a second request. With filters active it
    is exact; on the unfiltered view it is the planner's row estimate
    (an exact count would rescan the table on every poll for a number the
    UI only uses to size the pager).

    Order: the sort value in the database collation (NULL last in both
    directions), then a live row before a history row, then the task id
    ascending. The order is total, so a page does not reorder between
    polls.

    Outcomes:
        - ``Ok(TaskListPage)``: the page rows and the total.
        - ``Err(MonitoringQueryHistoryContractViolation)``: the EXPLAIN text
          of the unfiltered history estimate does not decode.
        - ``Err(MonitoringQueryDbOperationFailed)``: the statement failed, or
          the core could not run it.

    One statement (``horsies_monitoring_task_list``) that the core runs on
    the broker's core handle, so the page and the total read one snapshot: a
    task that moves to history during the read is on the page once or not at
    all. ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it
    uses no SQLAlchemy resource set.
    """
    query = CoreTaskListQuery(
        window_lower=window.lower,
        window_upper=window.upper,
        statuses=[status.value for status in statuses],
        task_names=list(task_names),
        queues=list(queues),
        workers=list(workers),
        error_codes=list(error_codes),
        error_categories=[category.value for category in error_categories],
        retried_only=retried_only,
        sort_by=sort_by,
        sort_dir=sort_dir,
        offset=page.offset,
        limit=page.limit,
    )
    operation = 'task list query'
    try:
        outcome = await broker._core_handle.monitoring_list_tasks_async(query)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_TASK_LIST_PAGE, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


async def get_task_detail(
    broker: PostgresBroker,
    task_id: uuid.UUID,
) -> MonitoringResult[TaskDetail | TaskDetailNotFound]:
    """A single task with its attempt history.

    The attempt history is where a task's cause lives: ``error_message``
    carries the unhandled exception per try, including retries.

    Outcomes:
        - ``Ok(TaskDetail)``: the live row, or the history row when the live
          table has no row. Attempts are oldest first.
        - ``Ok(TaskDetailNotFound)``: no table holds the id. ``reason`` says
          whether retention can explain it: an id that is not a UUIDv7 is
          not classified; a UUIDv7 born before the latest upper bound of a
          detached leaf of a period class (plus 5 s) predates retained
          history; any other UUIDv7 is within it.
        - ``Err(MonitoringQueryHistoryContractViolation)``: the history row
          or the attempts do not decode, or the history reader reports the
          task live in the snapshot where the live read found no row.
        - ``Err(MonitoringQueryDbOperationFailed)``: the statement failed, or
          the core could not run it.

    One statement (``horsies_monitoring_task_detail``) that the core runs on
    the broker's core handle, so every part comes from one snapshot.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it uses
    no SQLAlchemy resource set.
    """
    operation = 'task detail query'
    try:
        outcome = await broker._core_handle.monitoring_task_detail_async(task_id)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_TASK_DETAIL, text))
        case TaskDetailAbsent(task_id=absent_id, reason=reason):
            return Ok(TaskDetailNotFound(task_id=absent_id, reason=TaskAbsenceReason(reason)))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


# --------------------------------------------------------------------------- #
# Workflow queries
# --------------------------------------------------------------------------- #
async def list_workflow_names(
    broker: PostgresBroker,
) -> MonitoringResult[list[str]]:
    """Distinct names of root workflow runs, for the run-picker filter.

    At most ``LIST_ROW_BOUND`` names, in the database collation; a longer
    list is cut, and the cut is logged as a warning. One statement
    (``horsies_monitoring_workflow_names``).

    The core runs the statement on the broker's core handle; the read
    validates the JSON text of the core's model.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it
    uses no SQLAlchemy resource set.
    The cut is logged on ``horsies.monitoring`` by the core.
    """
    operation = 'workflow names query'
    try:
        outcome = await broker._core_handle.monitoring_workflow_names_async()  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_WORKFLOW_NAMES, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


async def list_workflow_runs(
    broker: PostgresBroker,
    *,
    name: str | None,
    status: str | None,
    limit: WorkflowRunsLimit,
) -> MonitoringResult[list[WorkflowRunSummary]]:
    """Recent root workflow runs, newest first.

    Runs created at one time come by id, highest first, so a page does not
    reorder between polls. ``name`` and ``status`` are independent
    exact-match filters combined with AND; a status no run carries yields an
    empty list. One statement (``horsies_monitoring_workflow_runs``).

    The core runs the statement on the broker's core handle; the read
    validates the JSON text of the core's model.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it
    uses no SQLAlchemy resource set.
    """
    query = CoreWorkflowRunsQuery(name=name, status=status, limit=limit.value)
    operation = 'workflow runs query'
    try:
        outcome = await broker._core_handle.monitoring_workflow_runs_async(query)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_WORKFLOW_RUNS, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


async def get_workflow_run(
    broker: PostgresBroker,
    workflow_id: str,
) -> MonitoringResult[WorkflowRunDetail | None]:
    """A run's DAG — metadata, nodes, and dependency edges — or None if absent.

    Works for any run id, root or subworkflow, so a subworkflow node is
    drilled into by calling this again with that node's ``sub_workflow_id``.
    Edges pointing at an index the run does not have are dropped. One
    statement (``horsies_monitoring_workflow_run``), so the run, its nodes and
    the child counts come from one snapshot.

    The core runs the statement on the broker's core handle; the read
    validates the JSON text of the core's model.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it
    uses no SQLAlchemy resource set.
    An id text that is not a UUID names no run: ``Ok(None)``, with no call.
    """
    match _workflow_uuid(workflow_id):
        case uuid.UUID() as run_id:
            pass
        case None:
            return Ok(None)
    operation = 'workflow run detail query'
    try:
        outcome = await broker._core_handle.monitoring_workflow_run_async(run_id)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_WORKFLOW_RUN, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


async def get_workflow_node(
    broker: PostgresBroker,
    workflow_id: str,
    task_index: int,
) -> MonitoringResult[WorkflowTaskDetail | None]:
    """Detail for one node: node error, backing task, and attempt history.

    ``node_error`` is the message of the error that the node's stored
    result holds; a stored result that does not decode returns
    ``MonitoringQueryHistoryContractViolation``.

    Returns None when the run has no node at that index. Subworkflow nodes
    have no backing task; drill into the child run instead. ``leaf`` is
    None, with no attempts, when no table holds the backing task (retention
    removed it). A history row of the backing task that does not decode
    returns ``MonitoringQueryHistoryContractViolation``.

    One statement (``horsies_monitoring_workflow_node``), so the node and
    its task come from one snapshot.

    The core runs the statement on the broker's core handle; the read
    validates the JSON text of the core's model.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it
    uses no SQLAlchemy resource set.
    An id text that is not a UUID, or an index outside the integer column
    of the node index, names no node: ``Ok(None)``, with no call.
    """
    operation = 'workflow node detail query'
    match _workflow_uuid(workflow_id):
        case uuid.UUID() as run_id:
            pass
        case None:
            return Ok(None)
    if not _INT4_MIN <= task_index <= _INT4_MAX:
        return Ok(None)
    try:
        outcome = await broker._core_handle.monitoring_workflow_node_async(  # pyright: ignore[reportPrivateUsage]
            run_id, task_index=task_index,
        )
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_WORKFLOW_NODE, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)


# --------------------------------------------------------------------------- #
# Schedules
# --------------------------------------------------------------------------- #
async def list_schedules(
    broker: PostgresBroker,
) -> MonitoringResult[list[ScheduleStateInfo]]:
    """Recurring schedule states, soonest next-run first, then by name.

    Schedules due at one time come by name, so the list does not reorder
    between polls. At most ``LIST_ROW_BOUND`` schedules; a longer list is
    cut, and the cut is logged as a warning. One statement
    (``horsies_monitoring_schedules``).

    The core runs the statement on the broker's core handle; the read
    validates the JSON text of the core's model.
    ``MonitoringQueryEventLoopLimit`` is not an answer of this read: it
    uses no SQLAlchemy resource set.
    The cut is logged on ``horsies.monitoring`` by the core.
    """
    operation = 'schedule state query'
    try:
        outcome = await broker._core_handle.monitoring_schedules_async()  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return _core_call_failed(operation, call_refusal_or_raise(exc))
    match outcome:
        case MonitoringJson(json=text):
            return Ok(validate_core_model_json(_SCHEDULES, text))
        case MonitoringQueryDbOperationFailed() | MonitoringQueryHistoryContractViolation():
            return Err(outcome)
