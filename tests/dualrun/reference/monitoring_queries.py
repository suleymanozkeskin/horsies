"""The monitoring reads before the monitoring switch: Python's bodies.

The dual-run reference of ``horsies.monitoring.queries``. Each read here is
the Python implementation that sent its own statement to its 0007 function
and decoded the rows; the production read of the same name calls the core.
The pairs of ``tests/dualrun/test_monitoring_*`` compare the two on one
database, and ``tests/dualrun/operations/monitoring_and_web.py`` puts these
functions in the place of the production reads for the Python run of the
integration tests. The public types (models, errors, pages, window) are the
production ones.

The text below is the module as it was before the switch, unchanged but for
this header.

---
Read queries backing the monitoring UI.

Every query takes the broker, selects its resources once, opens one
session, and returns a ``MonitoringResult``. The error side has three
states:

* ``MonitoringQueryEventLoopLimit``: the broker already holds resource sets
  for its limit of open event loops and the query runs on a new one.
  Nothing runs.
* ``MonitoringQueryDbOperationFailed``: a ``SQLAlchemyError`` at the
  session boundary.
* ``MonitoringQueryHistoryContractViolation``: a ``HistoryContractError``
  from the history reads of ``task_stats``, ``list_tasks``,
  ``get_task_detail`` and ``get_workflow_node``: stored history data that
  does not decode.

Nothing else is caught. A row that does not exist is ``Ok(None)``, not an
error. ``get_task_detail`` names it: ``Ok(TaskDetailNotFound)`` with the
reason the reads know. ``get_task_detail``, ``get_workflow_node`` and
``list_tasks`` each send one statement, so the live read and the history
read see the same rows.

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

import typing
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Final, Literal

import psycopg

from sqlalchemy import (
    TextClause,
    text,
)
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import BrokerResources, EventLoopLimit
from horsies.core.models.tasks import (
    Execution,
    ContractCode,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
)
from horsies.core.history.archive.attempts import (
    AttemptRecord as ArchiveAttemptRecord,
    decode_attempt_encoding,
    decode_attempt_snapshot,
)
from horsies.core.history.archive.versions import (
    ArchiveDecodeResult,
    DecodedArchiveValue,
)
from horsies.core.history.errors import HistoryContractError
from horsies.core.logging import get_logger
from horsies.core.history.reads.aggregates import (
    plan_rows_from_explain,
)
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import decode_task_error, validate_task_result_envelope
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import (
    autocommit_connection,
    is_retryable_connection_error,
    uuid_column_value,
)
from horsies.monitoring.history_window import MonitoringWindow
from horsies.monitoring.errors import (
    MonitoringQueryDbOperationFailed,
    MonitoringQueryEventLoopLimit,
    MonitoringQueryHistoryContractViolation,
    MonitoringResult,
)
from horsies.monitoring.pages import BreakdownLimit, TaskPage, WorkflowRunsLimit
from horsies.monitoring.models import (
    Breakdown,
    BreakdownCounts,
    BreakdownGroup,
    BreakdownUnclaimedGroup,
    BreakdownValueGroup,
    ErrorCategory,
    ErrorFacet,
    FacetValue,
    Facets,
    GroupRow,
    LeafTaskInfo,
    ScheduleStateInfo,
    StatusCount,
    TaskAbsenceReason,
    TaskAttemptInfo,
    TaskDetail,
    TaskDetailNotFound,
    TaskListPage,
    TaskSummary,
    WorkflowEdge,
    WorkflowNodeInfo,
    WorkflowRunDetail,
    WorkflowRunSummary,
    WorkflowTaskDetail,
)

logger = get_logger('monitoring')

# The most rows that ``list_workflow_names`` and ``list_schedules`` return. A
# longer list is cut, and the cut is logged as a warning.
LIST_ROW_BOUND: Final = 10_000

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

# Display order for the status overview cards. Stable rather than
# count-ordered, so cards never reorder as counts shift between polls.
_STATUS_ORDER: tuple[TaskStatus, ...] = (
    TaskStatus.PENDING,
    TaskStatus.CLAIMED,
    TaskStatus.RUNNING,
    TaskStatus.COMPLETED,
    TaskStatus.FAILED,
    TaskStatus.CANCELLED,
    TaskStatus.EXPIRED,
)

# Every stored status text that is a task status.
_STATUS_BY_VALUE: dict[str, TaskStatus] = {status.value: status for status in TaskStatus}

# Statuses in which a task is still waiting to start, so its queue wait is an
# open span that counts up.
_QUEUEING_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.PENDING, TaskStatus.CLAIMED}
)

# The error codes offered in the dropdown after the category selection. The
# facet function bounds each category to the same count. Not
# client-controllable.
_ERROR_FACET_CAP = 30

# Built from horsies' own error-code enums so the mapping never drifts from
# the library. Any non-null code absent here is user-defined -> DOMAIN.
_CODE_TO_CATEGORY: dict[str, ErrorCategory] = {
    **{member.value: ErrorCategory.OPERATIONAL for member in OperationalErrorCode},
    **{member.value: ErrorCategory.CONTRACT for member in ContractCode},
    **{member.value: ErrorCategory.RETRIEVAL for member in RetrievalCode},
    **{member.value: ErrorCategory.OUTCOME for member in OutcomeCode},
}


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


def _category_value(code: str | None) -> str | None:
    """Taxonomy family of an error code as a wire string."""
    category = categorize_error_code(code)
    return category.value if category is not None else None


def _is_uuid_text(value: str) -> bool:
    """Whether ``value`` parses as a uuid.

    Identity columns are uuid; text that does not parse as one names
    nothing and must answer as an absence, not reach a CAST or a typed
    bind that would error.
    """
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def _db_err(
    operation: str, exc: SQLAlchemyError,
) -> Err[MonitoringQueryDbOperationFailed]:
    """Wrap a session-boundary failure, classifying transient errors."""
    return Err(
        MonitoringQueryDbOperationFailed(
            message=f'{operation} failed: {exc}',
            retryable=is_retryable_connection_error(exc),
            exception=exc,
        )
    )


def _loop_limit_err(
    operation: str, loop_limit: EventLoopLimit,
) -> Err[MonitoringQueryEventLoopLimit]:
    """The broker already serves its limit of event loops. Nothing ran."""
    return Err(
        MonitoringQueryEventLoopLimit(
            message=f'{operation}: {loop_limit.describe()}',
            loop_limit=loop_limit,
        )
    )


def _contract_err(
    operation: str, exc: HistoryContractError,
) -> Err[MonitoringQueryHistoryContractViolation]:
    """Wrap stored history data that does not decode. Nothing changed."""
    return Err(
        MonitoringQueryHistoryContractViolation(
            message=f'{operation} failed: {exc}',
            exception=exc,
        )
    )


# --------------------------------------------------------------------------- #
# Row -> model mappers
# --------------------------------------------------------------------------- #
def _live_spans(
    status: TaskStatus,
    enqueued_at: datetime | None,
    started_at: datetime | None,
) -> tuple[int | None, int | None]:
    """``(queue_s, exec_s)`` of a live task.

    A live row has no end time. The queue span counts up only while the
    task is still waiting to start, and the execution span only while it is
    running.
    """
    queue_s = span_s(enqueued_at, started_at, live=status in _QUEUEING_STATUSES)
    exec_s = span_s(started_at, None, live=status is TaskStatus.RUNNING)
    return queue_s, exec_s


def _node_exec_s(
    node_status: str,
    started_at: datetime | None,
    completed_at: datetime | None,
) -> int | None:
    """Execution seconds for a workflow node, or None if it has not run.

    A queued node returns None: its ``started_at`` is the enqueue time, not an
    execution start, so reporting elapsed there would surface queue wait as if
    it were run time. A node that finished without a ``completed_at`` — the
    SKIPPED rows a workflow cancel writes — has no measurable span.
    """
    match node_status:
        case 'RUNNING':
            return elapsed_s(started_at, None)
        case 'COMPLETED' | 'FAILED' | 'SKIPPED' | 'CANCELLED' if (
            completed_at is not None
        ):
            return elapsed_s(started_at, completed_at)
        case _:
            return None


# --------------------------------------------------------------------------- #
# Monitoring functions
# --------------------------------------------------------------------------- #
# The built-in error-code taxonomy as the two parallel arrays the monitoring
# functions take: code i belongs to category i. A code outside it is DOMAIN.
_TAXONOMY_CODES: tuple[str, ...] = tuple(_CODE_TO_CATEGORY)
_TAXONOMY_CATEGORIES: tuple[str, ...] = tuple(
    category.value for category in _CODE_TO_CATEGORY.values()
)

# SQLSTATE data_corrupted: a monitoring function found a stored value that
# breaks the history contract, for example a status that is not a task status.
_DATA_CORRUPTED_SQLSTATE: Final = 'XX001'

_TASK_STATS_SQL = text("""
    SELECT row_position, status, task_count
    FROM horsies_monitoring_task_stats(
        CAST(:window_lower AS timestamptz), CAST(:window_upper AS timestamptz),
        CAST(:task_names AS text[]), CAST(:queue_names AS text[]),
        CAST(:worker_ids AS text[]), CAST(:error_codes AS text[]),
        CAST(:error_categories AS text[]), CAST(:retried_only AS boolean),
        CAST(:builtin_codes AS text[]), CAST(:builtin_categories AS text[])
    )
    ORDER BY row_position
""")

_TASK_BREAKDOWN_SQL = text("""
    SELECT row_kind, row_position, group_kind, group_value, group_count, total,
           pending, claimed, running, completed, failed, cancelled, expired, retried
    FROM horsies_monitoring_task_breakdown(
        CAST(:window_lower AS timestamptz), CAST(:window_upper AS timestamptz),
        CAST(:group_by AS text), CAST(:statuses AS text[]),
        CAST(:task_names AS text[]), CAST(:queue_names AS text[]),
        CAST(:worker_ids AS text[]), CAST(:error_codes AS text[]),
        CAST(:error_categories AS text[]), CAST(:retried_only AS boolean),
        CAST(:builtin_codes AS text[]), CAST(:builtin_categories AS text[]),
        CAST(:group_limit AS integer)
    )
    ORDER BY row_position
""")

# Rows come ordered by facet, then rank (the function's order).
_TASK_FACETS_SQL = text("""
    SELECT facet, facet_value, facet_count, category, category_total
    FROM horsies_monitoring_task_facets(
        CAST(:window_lower AS timestamptz), CAST(:window_upper AS timestamptz),
        CAST(:statuses AS text[]), CAST(:retried_only AS boolean),
        CAST(:builtin_codes AS text[]), CAST(:builtin_categories AS text[])
    )
""")


async def _call_function(
    resources: BrokerResources,
    statement: TextClause,
    parameters: Mapping[str, object],
) -> Sequence[RowMapping]:
    """Run one monitoring function statement in autocommit and return its rows.

    One round trip: no BEGIN and no COMMIT. Every monitoring function reads
    in the snapshot of its one statement and writes nothing.

    Raises:
        SQLAlchemyError: the connection checkout or the statement failed.
    """
    async with resources.session_factory() as session:
        connection = await autocommit_connection(session)
        result = await connection.execute(statement, parameters)
        return result.mappings().all()


def _column[T](row: RowMapping, name: str, kind: type[T]) -> T:
    """The value of a result column that the function declares non-NULL.

    A NULL or a value of another type breaks the function's result
    contract: the assert crashes loud.
    """
    value: object = row[name]
    assert isinstance(value, kind), (
        f'column {name}: expected {kind.__name__}, got {value!r}'
    )
    return value


def _uuid_column(row: RowMapping, name: str) -> uuid.UUID:
    """The ``uuid.UUID`` of a uuid result column that the function declares non-NULL.

    A NULL or a text that is not a UUID breaks the function's result
    contract: the ``TypeError`` or ``ValueError`` crashes loud and names the
    column.
    """
    return uuid_column_value(row[name], column=name)


def _data_corrupted(exc: SQLAlchemyError) -> HistoryContractError | None:
    """The history contract error of a data_corrupted refusal, or None for any other error."""
    match exc:
        case DBAPIError(orig=psycopg.Error(sqlstate=sqlstate) as cause) if (
            sqlstate == _DATA_CORRUPTED_SQLSTATE
        ):
            error = HistoryContractError(cause.diag.message_primary or str(cause))
            error.__cause__ = exc
            return error
        case _:
            return None


def _function_err(
    operation: str, exc: SQLAlchemyError,
) -> Err[MonitoringQueryDbOperationFailed | MonitoringQueryHistoryContractViolation]:
    """A data_corrupted refusal as a history contract violation; any other error as a database failure."""
    match _data_corrupted(exc):
        case HistoryContractError() as broken:
            return _contract_err(operation, broken)
        case None:
            return _db_err(operation, exc)


def _scope_parameters(
    window: MonitoringWindow,
    *,
    statuses: list[TaskStatus],
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
) -> dict[str, object]:
    """The window, the filters and the code taxonomy, as function arguments."""
    return {
        'window_lower': window.lower,
        'window_upper': window.upper,
        'statuses': [status.value for status in statuses],
        'task_names': list(task_names),
        'queue_names': list(queues),
        'worker_ids': list(workers),
        'error_codes': list(error_codes),
        'error_categories': [category.value for category in error_categories],
        'retried_only': retried_only,
        'builtin_codes': list(_TAXONOMY_CODES),
        'builtin_categories': list(_TAXONOMY_CATEGORIES),
    }


def _status_counts(rows: Sequence[RowMapping]) -> list[StatusCount]:
    """The seven status rows of ``horsies_monitoring_task_stats``, in their fixed order."""
    counts = [
        StatusCount(
            status=_column(row, 'status', str),
            count=_column(row, 'task_count', int),
        )
        for row in rows
    ]
    assert [count.status for count in counts] == [
        status.value for status in _STATUS_ORDER
    ], f'the stats function answered statuses {[count.status for count in counts]}'
    return counts


def _breakdown_counts(row: RowMapping) -> BreakdownCounts:
    """The counts of one breakdown function row."""
    return BreakdownCounts(
        total=_column(row, 'total', int),
        pending=_column(row, 'pending', int),
        claimed=_column(row, 'claimed', int),
        running=_column(row, 'running', int),
        completed=_column(row, 'completed', int),
        failed=_column(row, 'failed', int),
        cancelled=_column(row, 'cancelled', int),
        expired=_column(row, 'expired', int),
        retried=_column(row, 'retried', int),
    )


def _breakdown_group(row: RowMapping) -> BreakdownGroup:
    """The group of one GROUP row: a stored value, or the unclaimed tasks."""
    match _column(row, 'group_kind', str):
        case 'VALUE':
            return BreakdownValueGroup(value=_column(row, 'group_value', str))
        case 'UNCLAIMED':
            assert row['group_value'] is None, row['group_value']
            return BreakdownUnclaimedGroup()
        case other:
            raise AssertionError(f'breakdown group kind {other!r} is not VALUE or UNCLAIMED')


def _group_row(row: RowMapping) -> GroupRow:
    """One GROUP row of ``horsies_monitoring_task_breakdown``."""
    assert _column(row, 'row_kind', str) == 'GROUP', row['row_kind']
    counts = _breakdown_counts(row)
    return GroupRow(
        group=_breakdown_group(row),
        total=counts.total,
        pending=counts.pending,
        claimed=counts.claimed,
        running=counts.running,
        completed=counts.completed,
        failed=counts.failed,
        cancelled=counts.cancelled,
        expired=counts.expired,
        retried=counts.retried,
    )


def _breakdown(group_by: TaskGroupBy, rows: Sequence[RowMapping]) -> Breakdown:
    """The TOTAL row, then the GROUP rows in final order."""
    match rows:
        case [total_row, *group_rows]:
            pass
        case _:
            raise AssertionError('the breakdown function answered no TOTAL row')
    assert _column(total_row, 'row_kind', str) == 'TOTAL', total_row['row_kind']
    return Breakdown(
        group_by=group_by,
        groups=[_group_row(row) for row in group_rows],
        total=_breakdown_counts(total_row),
        group_count=_column(total_row, 'group_count', int),
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
    """
    parameters = _scope_parameters(
        window,
        statuses=[],
        task_names=task_names,
        queues=queues,
        workers=workers,
        error_codes=error_codes,
        error_categories=error_categories,
        retried_only=retried_only,
    )
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task stats query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(resources, _TASK_STATS_SQL, parameters)
    except SQLAlchemyError as exc:
        return _function_err('task stats query', exc)
    return Ok(_status_counts(rows))


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

    One statement (``horsies_monitoring_task_facets``): the values and their
    counts, ranked after the two sides sum, with ties in the database
    collation.
    """
    parameters = _scope_parameters(
        window,
        statuses=statuses,
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=retried_only,
    )
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task facets query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(resources, _TASK_FACETS_SQL, parameters)
    except SQLAlchemyError as exc:
        return _db_err('task facets query', exc)

    ranked: dict[str, list[FacetValue]] = {
        'worker': [],
        'task_name': [],
        'queue_name': [],
    }
    selected = {category.value for category in error_categories}
    category_totals: dict[str, int] = {}
    error_facets: list[ErrorFacet] = []
    for row in rows:
        facet = _column(row, 'facet', str)
        value = _column(row, 'facet_value', str)
        count = _column(row, 'facet_count', int)
        if facet != 'error_code':
            ranked[facet].append(FacetValue(value=value, count=count))
            continue
        category = _column(row, 'category', str)
        category_totals[category] = _column(row, 'category_total', int)
        if (not selected or category in selected) and len(error_facets) < _ERROR_FACET_CAP:
            error_facets.append(
                ErrorFacet(value=value, count=count, category=category),
            )

    return Ok(
        Facets(
            workers=ranked['worker'],
            task_names=ranked['task_name'],
            queues=ranked['queue_name'],
            error_codes=error_facets,
            error_category_totals=category_totals,
        )
    )


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
    """
    parameters = _scope_parameters(
        window,
        statuses=statuses,
        task_names=task_names,
        queues=queues,
        workers=workers,
        error_codes=error_codes,
        error_categories=error_categories,
        retried_only=retried_only,
    )
    parameters['group_by'] = group_by
    parameters['group_limit'] = limit.value
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task breakdown query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(resources, _TASK_BREAKDOWN_SQL, parameters)
    except SQLAlchemyError as exc:
        return _db_err('task breakdown query', exc)
    return Ok(_breakdown(group_by, rows))


# Rows come ordered by row_position: the TOTALS row (0), then the page rows.
_TASK_LIST_SQL = text("""
    SELECT row_kind, row_position, total_kind, live_total, history_total,
           history_estimate_plan, location, task_id, task_name, queue_name,
           priority, status, retry_count, max_retries, is_workflow_task,
           error_code, worker_id, worker_hostname, enqueued_at, started_at,
           terminal_at
    FROM horsies_monitoring_task_list(
        CAST(:window_lower AS timestamptz), CAST(:window_upper AS timestamptz),
        CAST(:statuses AS text[]), CAST(:task_names AS text[]),
        CAST(:queue_names AS text[]), CAST(:worker_ids AS text[]),
        CAST(:error_codes AS text[]), CAST(:error_categories AS text[]),
        CAST(:retried_only AS boolean),
        CAST(:builtin_codes AS text[]), CAST(:builtin_categories AS text[]),
        CAST(:sort_key AS text), CAST(:sort_direction AS text),
        CAST(:page_offset AS integer), CAST(:page_limit AS integer)
    )
    ORDER BY row_position
""")


def _sort_direction(sort_dir: SortDirection) -> str:
    """The sort direction as ``horsies_monitoring_task_list`` takes it."""
    match sort_dir:
        case 'asc':
            return 'ASC'
        case 'desc':
            return 'DESC'


def _list_total(row: RowMapping) -> int:
    """The total of the TOTALS row.

    ``EXACT``: the two counts. ``ESTIMATE_HISTORY_EMPTY``: the live
    estimate (the history table has no row). ``ESTIMATE_HISTORY_PLAN``: the
    live estimate plus the planner's row estimate of the history window.

    Raises:
        HistoryContractError: the EXPLAIN text of the history estimate does
            not carry a top plan row estimate.
    """
    assert _column(row, 'row_kind', str) == 'TOTALS', row['row_kind']
    live_total = _column(row, 'live_total', int)
    match _column(row, 'total_kind', str):
        case 'EXACT' | 'ESTIMATE_HISTORY_EMPTY':
            return live_total + _column(row, 'history_total', int)
        case 'ESTIMATE_HISTORY_PLAN':
            return live_total + plan_rows_from_explain(
                _column(row, 'history_estimate_plan', str),
            )
        case other:
            raise AssertionError(f'task list total kind {other!r} is not known')


def _live_list_row(row: RowMapping) -> TaskSummary:
    """A LIVE page row: no error code and no end time."""
    task_id = _uuid_column(row, 'task_id')
    stored_status = _column(row, 'status', str)
    status = _STATUS_BY_VALUE.get(stored_status)
    assert status is not None, f'live task {task_id} has status {stored_status!r}'
    enqueued_at = _column(row, 'enqueued_at', datetime)
    started_at = _optional_time(row, 'started_at')
    queue_s, exec_s = _live_spans(status, enqueued_at, started_at)
    return TaskSummary(
        id=task_id,
        task_name=_column(row, 'task_name', str),
        queue_name=_column(row, 'queue_name', str),
        status=status.value,
        priority=_column(row, 'priority', int),
        retry_count=_column(row, 'retry_count', int),
        max_retries=_column(row, 'max_retries', int),
        is_workflow_task=_column(row, 'is_workflow_task', bool),
        error_code=None,
        error_category=None,
        worker_hostname=_optional_text(row, 'worker_hostname'),
        worker_id=_optional_text(row, 'worker_id'),
        enqueued_at=enqueued_at,
        started_at=started_at,
        completed_at=None,
        failed_at=None,
        queue_s=queue_s,
        exec_s=exec_s,
    )


def _history_list_row(row: RowMapping) -> TaskSummary:
    """A HISTORY page row: the stored status text, and the terminal time as
    ``completed_at`` for COMPLETED, else as ``failed_at``."""
    status = _column(row, 'status', str)
    terminal_at = _column(row, 'terminal_at', datetime)
    enqueued_at = _column(row, 'enqueued_at', datetime)
    started_at = _optional_time(row, 'started_at')
    error_code = _optional_text(row, 'error_code')
    completed = status == TaskStatus.COMPLETED.value
    return TaskSummary(
        id=_uuid_column(row, 'task_id'),
        task_name=_column(row, 'task_name', str),
        queue_name=_column(row, 'queue_name', str),
        status=status,
        priority=_column(row, 'priority', int),
        retry_count=_column(row, 'retry_count', int),
        max_retries=_column(row, 'max_retries', int),
        is_workflow_task=_column(row, 'is_workflow_task', bool),
        error_code=nz(error_code),
        error_category=_category_value(error_code),
        worker_hostname=_optional_text(row, 'worker_hostname'),
        worker_id=_optional_text(row, 'worker_id'),
        enqueued_at=enqueued_at,
        started_at=started_at,
        completed_at=terminal_at if completed else None,
        failed_at=None if completed else terminal_at,
        queue_s=span_s(enqueued_at, started_at or terminal_at, live=False),
        exec_s=span_s(started_at, terminal_at, live=False),
    )


def _list_row(row: RowMapping) -> TaskSummary:
    """One TASK row of ``horsies_monitoring_task_list``, by its location."""
    assert _column(row, 'row_kind', str) == 'TASK', row['row_kind']
    match _column(row, 'location', str):
        case 'LIVE':
            return _live_list_row(row)
        case 'HISTORY':
            return _history_list_row(row)
        case other:
            raise AssertionError(f'task list location {other!r} is not LIVE or HISTORY')


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
        - ``Err(MonitoringQueryDbOperationFailed)``: the statement failed.
        - ``Err(MonitoringQueryEventLoopLimit)``: nothing ran.

    One statement (``horsies_monitoring_task_list``), so the page and the
    total read one snapshot: a task that moves to history during the read is
    on the page once or not at all.
    """
    parameters = _scope_parameters(
        window,
        statuses=statuses,
        task_names=task_names,
        queues=queues,
        workers=workers,
        error_codes=error_codes,
        error_categories=error_categories,
        retried_only=retried_only,
    )
    parameters['sort_key'] = sort_by
    parameters['sort_direction'] = _sort_direction(sort_dir)
    parameters['page_offset'] = page.offset
    parameters['page_limit'] = page.limit
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task list query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(resources, _TASK_LIST_SQL, parameters)
    except SQLAlchemyError as exc:
        return _db_err('task list query', exc)
    match rows:
        case [totals_row, *task_rows]:
            pass
        case _:
            raise AssertionError('the task list function answered no TOTALS row')
    try:
        total = _list_total(totals_row)
    except HistoryContractError as exc:
        return _contract_err('task list estimate decode', exc)
    return Ok(TaskListPage(rows=[_list_row(row) for row in task_rows], total=total))


# The detail columns that the task detail and the workflow node functions
# share, under the same names. The detail reads no result envelope.
_DETAIL_COLUMNS: Final = (
    'location, task_id, task_name, queue_name, priority, status, is_workflow_task, '
    'retry_count, max_retries, enqueued_at, started_at, good_until, worker_hostname, '
    'execution, terminal_at, error_code, final_failed_reason, attempt_archive_version, '
    'attempt_snapshot, attempt_snapshot_digest, workflow_id, workflow_task_index, '
    'absence_reason'
)

_TASK_DETAIL_SQL = text(
    f'SELECT {_DETAIL_COLUMNS} '
    'FROM horsies_monitoring_task_detail(CAST(:task_id AS uuid), FALSE, TRUE)'
)


@dataclass(frozen=True, slots=True)
class _FoundTask:
    """A task that the detail function found, live or in history, decoded."""

    leaf: LeafTaskInfo
    task_name: str
    queue_name: str
    priority: int
    is_workflow_task: bool
    error_code: str | None
    attempts: list[TaskAttemptInfo]
    workflow_id: str | None
    workflow_task_index: int | None


def _attempt_info(record: ArchiveAttemptRecord) -> TaskAttemptInfo:
    """Map one decoded attempt to its response shape."""
    return TaskAttemptInfo(
        attempt=record.attempt,
        outcome=record.outcome,
        will_retry=record.will_retry,
        error_code=nz(record.error_code),
        error_message=nz(record.error_message),
        failed_reason=nz(record.failed_reason),
        worker_hostname=record.worker_hostname,
        started_at=record.started_at,
        finished_at=record.finished_at,
    )


def _decoded_attempts(
    task_id: uuid.UUID,
    decoded: ArchiveDecodeResult[tuple[ArchiveAttemptRecord, ...]],
) -> list[TaskAttemptInfo]:
    """The attempts, oldest first.

    Raises:
        HistoryContractError: the attempt bytes do not decode.
    """
    match decoded:
        case DecodedArchiveValue(value=records):
            return [_attempt_info(record) for record in records]
        case _:
            raise HistoryContractError(
                f'the attempts of task {task_id} failed to decode: {decoded!r}'
            )


def _bytes_column(row: RowMapping, name: str) -> bytes:
    """A non-NULL bytea column as bytes."""
    value: object = row[name]
    match value:
        case bytes():
            return value
        case memoryview():
            return value.tobytes()
        case _:
            raise AssertionError(f'column {name}: expected bytes, got {value!r}')


def _optional_text(row: RowMapping, name: str) -> str | None:
    value: object = row[name]
    match value:
        case None | str():
            return value
        case _:
            raise AssertionError(f'column {name}: expected text, got {value!r}')


def _optional_time(row: RowMapping, name: str) -> datetime | None:
    value: object = row[name]
    match value:
        case None | datetime():
            return value
        case _:
            raise AssertionError(f'column {name}: expected a time, got {value!r}')


def _optional_int(row: RowMapping, name: str) -> int | None:
    value: object = row[name]
    match value:
        case None | int():
            return value
        case _:
            raise AssertionError(f'column {name}: expected an integer, got {value!r}')


def _optional_uuid(row: RowMapping, name: str) -> uuid.UUID | None:
    value: object = row[name]
    match value:
        case None:
            return None
        case _:
            return uuid_column_value(value, column=name)


def _live_found(row: RowMapping) -> _FoundTask:
    """A LIVE detail row: the live row and its attempts, encoded on read.

    The live table has no end time, error code or failed reason.

    Raises:
        HistoryContractError: the attempt bytes do not decode.
    """
    task_id = _uuid_column(row, 'task_id')
    stored_status = _column(row, 'status', str)
    status = _STATUS_BY_VALUE.get(stored_status)
    assert status is not None, f'live task {task_id} has status {stored_status!r}'
    enqueued_at = _column(row, 'enqueued_at', datetime)
    started_at = _optional_time(row, 'started_at')
    queue_s, exec_s = _live_spans(status, enqueued_at, started_at)
    return _FoundTask(
        leaf=LeafTaskInfo(
            task_id=task_id,
            status=status.value,
            error_code=None,
            failed_reason=None,
            retry_count=_column(row, 'retry_count', int),
            max_retries=_column(row, 'max_retries', int),
            enqueued_at=enqueued_at,
            started_at=started_at,
            completed_at=None,
            failed_at=None,
            queue_s=queue_s,
            exec_s=exec_s,
            worker_hostname=_optional_text(row, 'worker_hostname'),
            good_until=_optional_time(row, 'good_until'),
            execution=Execution(_column(row, 'execution', str)),
        ),
        task_name=_column(row, 'task_name', str),
        queue_name=_column(row, 'queue_name', str),
        priority=_column(row, 'priority', int),
        is_workflow_task=_column(row, 'is_workflow_task', bool),
        error_code=None,
        attempts=_decoded_attempts(
            task_id,
            decode_attempt_encoding(
                version=_column(row, 'attempt_archive_version', int),
                payload=_bytes_column(row, 'attempt_snapshot'),
            ),
        ),
        workflow_id=_optional_text(row, 'workflow_id'),
        workflow_task_index=_optional_int(row, 'workflow_task_index'),
    )


def _history_found(row: RowMapping) -> _FoundTask:
    """A HISTORY detail row: the history row and its attempt snapshot.

    The terminal time lands in ``completed_at`` for COMPLETED and in
    ``failed_at`` for every other terminal status.

    Raises:
        HistoryContractError: the stored status is not a task status, or
            the attempt snapshot does not decode or match its digest.
    """
    task_id = _uuid_column(row, 'task_id')
    stored_status = _column(row, 'status', str)
    match _STATUS_BY_VALUE.get(stored_status):
        case TaskStatus() as status:
            pass
        case None:
            raise HistoryContractError(
                f'history row {task_id} has status {stored_status!r}, which is not '
                'a task status'
            )
    terminal_at = _column(row, 'terminal_at', datetime)
    enqueued_at = _column(row, 'enqueued_at', datetime)
    started_at = _optional_time(row, 'started_at')
    error_code = _optional_text(row, 'error_code')
    completed = status is TaskStatus.COMPLETED
    return _FoundTask(
        leaf=LeafTaskInfo(
            task_id=task_id,
            status=status.value,
            error_code=nz(error_code),
            failed_reason=nz(_optional_text(row, 'final_failed_reason')),
            retry_count=_column(row, 'retry_count', int),
            max_retries=_column(row, 'max_retries', int),
            enqueued_at=enqueued_at,
            started_at=started_at,
            completed_at=terminal_at if completed else None,
            failed_at=None if completed else terminal_at,
            queue_s=span_s(enqueued_at, started_at or terminal_at, live=False),
            exec_s=span_s(started_at, terminal_at, live=False),
            worker_hostname=_optional_text(row, 'worker_hostname'),
            good_until=_optional_time(row, 'good_until'),
            execution=None,
        ),
        task_name=_column(row, 'task_name', str),
        queue_name=_column(row, 'queue_name', str),
        priority=_column(row, 'priority', int),
        is_workflow_task=_column(row, 'is_workflow_task', bool),
        error_code=error_code,
        attempts=_decoded_attempts(
            task_id,
            decode_attempt_snapshot(
                version=_column(row, 'attempt_archive_version', int),
                payload=_bytes_column(row, 'attempt_snapshot'),
                digest=_bytes_column(row, 'attempt_snapshot_digest'),
            ),
        ),
        workflow_id=_optional_text(row, 'workflow_id'),
        workflow_task_index=_optional_int(row, 'workflow_task_index'),
    )


def _found_or_absent(row: RowMapping) -> _FoundTask | TaskAbsenceReason:
    """A detail row by its location: the task, or the reason it is absent.

    Raises:
        HistoryContractError: a history row does not decode, or attempt
            bytes do not decode.
    """
    match _column(row, 'location', str):
        case 'LIVE':
            return _live_found(row)
        case 'HISTORY':
            return _history_found(row)
        case 'ABSENT':
            return TaskAbsenceReason(_column(row, 'absence_reason', str))
        case other:
            raise AssertionError(f'task detail location {other!r} is not LIVE, HISTORY or ABSENT')


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

    One statement (``horsies_monitoring_task_detail``), so every part comes
    from one snapshot.
    """
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task detail query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(
            resources, _TASK_DETAIL_SQL, {'task_id': str(task_id)},
        )
    except SQLAlchemyError as exc:
        return _function_err('task detail query', exc)
    match rows:
        case [row]:
            pass
        case _:
            raise AssertionError(f'the detail function answered {len(rows)} rows')
    try:
        found = _found_or_absent(row)
    except HistoryContractError as exc:
        return _contract_err('task detail history read', exc)
    match found:
        case TaskAbsenceReason() as reason:
            return Ok(TaskDetailNotFound(task_id=task_id, reason=reason))
        case _FoundTask():
            return Ok(
                TaskDetail(
                    leaf=found.leaf,
                    task_name=found.task_name,
                    queue_name=found.queue_name,
                    priority=found.priority,
                    is_workflow_task=found.is_workflow_task,
                    error_category=_category_value(found.error_code),
                    attempts=found.attempts,
                    workflow_id=found.workflow_id,
                    workflow_task_index=found.workflow_task_index,
                )
            )


# --------------------------------------------------------------------------- #
# Workflow queries
# --------------------------------------------------------------------------- #


def _node_error(task_index: int, stored: str | None) -> str | None:
    """The message of the error that a node's stored result holds.

    A node has no error column of its own: its stored result holds the
    error. No result (NULL), a result with a value, and an error with no
    message give None. No writer stores an empty text, so ``loads_json``
    refuses it as corrupt data, as the core's ``node_error_from_result``
    does.

    Raises:
        HistoryContractError: the stored result is not JSON (an empty text
            included), is not a task result envelope, or its error does not
            decode.
    """
    match stored:
        case None:
            return None
        case text:
            pass
    match loads_json(text):
        case Err(cause):
            raise HistoryContractError(
                f'workflow node {task_index}: the stored result is not JSON: {cause}'
            )
        case Ok(None):
            return None
        case Ok(raw):
            pass
    try:
        err_slot = validate_task_result_envelope(raw).get('err')
        match err_slot:
            case None:
                return None
            case _:
                return nz(decode_task_error(err_slot).message)
    except (StrictJsonError, ValueValidationError) as exc:
        raise HistoryContractError(
            f'workflow node {task_index}: the stored result does not decode: {exc}'
        ) from exc


_WORKFLOW_NAMES_SQL = text(
    'SELECT row_kind, row_position, workflow_name, list_state, row_bound '
    'FROM horsies_monitoring_workflow_names() ORDER BY row_position NULLS LAST'
)

_WORKFLOW_RUNS_SQL = text(
    'SELECT row_position, workflow_id, workflow_name, definition_key, status, '
    'created_at, completed_at '
    'FROM horsies_monitoring_workflow_runs('
    'CAST(:name AS text), CAST(:status AS text), CAST(:run_limit AS integer)) '
    'ORDER BY row_position'
)

_WORKFLOW_RUN_SQL = text(
    'SELECT row_kind, row_position, workflow_id, workflow_name, definition_key, '
    'status, created_at, started_at, completed_at, task_index, node_id, task_name, '
    'dependencies, allow_failed_deps, is_subworkflow, sub_workflow_id, child_total, '
    'child_failed '
    'FROM horsies_monitoring_workflow_run(CAST(:workflow_id AS uuid)) '
    'ORDER BY row_position NULLS FIRST'
)

_SCHEDULES_SQL = text(
    'SELECT row_kind, row_position, schedule_name, last_run_at, next_run_at, '
    'last_task_id, run_count, updated_at, list_state, row_bound '
    'FROM horsies_monitoring_schedules() ORDER BY row_position NULLS LAST'
)


def _list_end(rows: Sequence[RowMapping], item_kind: str) -> tuple[Sequence[RowMapping], bool]:
    """The item rows of a bounded list, and whether the function cut the list.

    The last row is the LIST_END row; its bound is ``LIST_ROW_BOUND``.
    """
    match rows:
        case [*items, end]:
            pass
        case _:
            raise AssertionError('the list function answered no LIST_END row')
    assert _column(end, 'row_kind', str) == 'LIST_END', end['row_kind']
    assert _column(end, 'row_bound', int) == LIST_ROW_BOUND, end['row_bound']
    assert all(_column(item, 'row_kind', str) == item_kind for item in items), item_kind
    match _column(end, 'list_state', str):
        case 'COMPLETE':
            return items, False
        case 'TRUNCATED':
            return items, True
        case other:
            raise AssertionError(f'list state {other!r} is not COMPLETE or TRUNCATED')


async def list_workflow_names(
    broker: PostgresBroker,
) -> MonitoringResult[list[str]]:
    """Distinct names of root workflow runs, for the run-picker filter.

    At most ``LIST_ROW_BOUND`` names, in the database collation; a longer
    list is cut, and the cut is logged as a warning. One statement
    (``horsies_monitoring_workflow_names``).
    """
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('workflow names query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(resources, _WORKFLOW_NAMES_SQL, {})
    except SQLAlchemyError as exc:
        return _db_err('workflow names query', exc)

    items, cut = _list_end(rows, 'NAME')
    if cut:
        _log_cut_list('workflow names', 'root workflow names')
    return Ok([_column(row, 'workflow_name', str) for row in items])


def _log_cut_list(read: str, rows: str) -> None:
    """Warn that a list read returned ``LIST_ROW_BOUND`` rows and more exist."""
    logger.warning(
        '%s: the list is cut at %d %s; more exist and are not returned',
        read,
        LIST_ROW_BOUND,
        rows,
    )


def _run_summary(row: RowMapping) -> WorkflowRunSummary:
    """A run's summary from a runs row or a RUN row."""
    created_at = _column(row, 'created_at', datetime)
    completed_at = _optional_time(row, 'completed_at')
    return WorkflowRunSummary(
        id=_column(row, 'workflow_id', str),
        name=_column(row, 'workflow_name', str),
        definition_key=_optional_text(row, 'definition_key'),
        status=_column(row, 'status', str),
        created_at=created_at,
        completed_at=completed_at,
        wall_s=elapsed_s(created_at, completed_at),
    )


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
    """
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('workflow runs query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(
            resources,
            _WORKFLOW_RUNS_SQL,
            {'name': name, 'status': status, 'run_limit': limit.value},
        )
    except SQLAlchemyError as exc:
        return _db_err('workflow runs query', exc)

    return Ok([_run_summary(row) for row in rows])


def _run_node(row: RowMapping) -> WorkflowNodeInfo:
    """One NODE row of ``horsies_monitoring_workflow_run``."""
    assert _column(row, 'row_kind', str) == 'NODE', row['row_kind']
    node_status = _column(row, 'status', str)
    started_at = _optional_time(row, 'started_at')
    completed_at = _optional_time(row, 'completed_at')
    return WorkflowNodeInfo(
        task_index=_column(row, 'task_index', int),
        node_id=_optional_text(row, 'node_id'),
        task_name=_column(row, 'task_name', str),
        node_status=node_status,
        is_subworkflow=_column(row, 'is_subworkflow', bool),
        sub_workflow_id=_optional_text(row, 'sub_workflow_id'),
        allow_failed_deps=_column(row, 'allow_failed_deps', bool),
        started_at=started_at,
        completed_at=completed_at,
        exec_s=_node_exec_s(node_status, started_at, completed_at),
        child_total=_optional_int(row, 'child_total'),
        child_failed=_optional_int(row, 'child_failed'),
    )


def _dependencies(row: RowMapping) -> list[int]:
    """The task indexes a node depends on."""
    value: object = row['dependencies']
    assert isinstance(value, list), f'dependencies: expected a list, got {value!r}'
    dependencies: list[int] = []
    for each in typing.cast(list[object], value):
        assert isinstance(each, int), f'dependency {each!r} is not an index'
        dependencies.append(each)
    return dependencies


def _run_detail(run_row: RowMapping, node_rows: Sequence[RowMapping]) -> WorkflowRunDetail:
    """The run, its nodes in task_index order, its edges and its failed nodes.

    An edge joins a dependency to the node that names it; a dependency on an
    index the run does not have makes no edge.
    """
    nodes = [_run_node(row) for row in node_rows]
    known_indices = {node.task_index for node in nodes}
    edges = [
        WorkflowEdge(from_index=dependency, to_index=node.task_index)
        for node, row in zip(nodes, node_rows, strict=True)
        for dependency in _dependencies(row)
        if dependency in known_indices
    ]
    failed_indices = [node.task_index for node in nodes if node.node_status == 'FAILED']
    return WorkflowRunDetail(
        run=_run_summary(run_row),
        nodes=nodes,
        edges=edges,
        failed_count=len(failed_indices),
        failed_indices=failed_indices,
    )


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
    """
    if not _is_uuid_text(workflow_id):
        return Ok(None)

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('workflow run detail query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(
            resources, _WORKFLOW_RUN_SQL, {'workflow_id': workflow_id},
        )
    except SQLAlchemyError as exc:
        return _function_err('workflow run detail query', exc)

    match rows:
        case [first, *node_rows]:
            pass
        case _:
            raise AssertionError('the run function answered no row')
    match _column(first, 'row_kind', str):
        case 'ABSENT':
            assert not node_rows, 'an ABSENT run has no node row'
            return Ok(None)
        case 'RUN':
            return Ok(_run_detail(first, node_rows))
        case other:
            raise AssertionError(f'workflow run row kind {other!r} is not RUN or ABSENT')


_WORKFLOW_NODE_SQL = text(
    'SELECT row_kind, node_task_index, node_id, node_task_name, node_status, '
    f'node_is_subworkflow, node_task_id, node_result, {_DETAIL_COLUMNS} '
    'FROM horsies_monitoring_workflow_node('
    'CAST(:workflow_id AS uuid), CAST(:task_index AS integer))'
)


def _node_detail(row: RowMapping) -> WorkflowTaskDetail:
    """A NODE row: the node, its error, and its backing task with the task's attempts.

    A node with no backing task (a sub-workflow node, or a task node before
    its task exists) and a node whose task is in neither place have no leaf
    and no attempt.

    Raises:
        HistoryContractError: the stored node result, the history row or the
            attempts do not decode.
    """
    task_index = _column(row, 'node_task_index', int)
    node_error = _node_error(task_index, _optional_text(row, 'node_result'))
    found: _FoundTask | TaskAbsenceReason | None
    match row['location']:
        case None:
            found = None
        case _:
            found = _found_or_absent(row)
    match found:
        case _FoundTask():
            leaf: LeafTaskInfo | None = found.leaf
            attempts = found.attempts
        case TaskAbsenceReason() | None:
            leaf = None
            attempts = []
    return WorkflowTaskDetail(
        task_index=task_index,
        node_id=_optional_text(row, 'node_id'),
        task_name=_column(row, 'node_task_name', str),
        node_status=_column(row, 'node_status', str),
        is_subworkflow=_column(row, 'node_is_subworkflow', bool),
        node_error=node_error,
        leaf=leaf,
        attempts=attempts,
    )


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
    """
    if not _is_uuid_text(workflow_id):
        return Ok(None)

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('workflow node detail query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(
            resources,
            _WORKFLOW_NODE_SQL,
            {'workflow_id': workflow_id, 'task_index': task_index},
        )
    except SQLAlchemyError as exc:
        return _function_err('workflow node detail query', exc)
    match rows:
        case [row]:
            pass
        case _:
            raise AssertionError(f'the node function answered {len(rows)} rows')
    match _column(row, 'row_kind', str):
        case 'ABSENT':
            return Ok(None)
        case 'NODE':
            pass
        case other:
            raise AssertionError(f'workflow node row kind {other!r} is not NODE or ABSENT')
    try:
        return Ok(_node_detail(row))
    except HistoryContractError as exc:
        return _contract_err('workflow node history read', exc)


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
    """
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('schedule state query', loop_limit)
        case Ok(resources):
            pass

    try:
        rows = await _call_function(resources, _SCHEDULES_SQL, {})
    except SQLAlchemyError as exc:
        return _db_err('schedule state query', exc)

    items, cut = _list_end(rows, 'SCHEDULE')
    if cut:
        _log_cut_list('schedule state', 'schedules')
    return Ok(
        [
            ScheduleStateInfo(
                schedule_name=_column(row, 'schedule_name', str),
                last_run_at=_optional_time(row, 'last_run_at'),
                next_run_at=_column(row, 'next_run_at', datetime),
                last_task_id=_optional_uuid(row, 'last_task_id'),
                run_count=_column(row, 'run_count', int),
                updated_at=_column(row, 'updated_at', datetime),
            )
            for row in items
        ]
    )
