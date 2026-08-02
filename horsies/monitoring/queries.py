"""Read queries backing the monitoring UI.

Every function takes the broker, opens one session, and returns a
``MonitoringResult``. ``SQLAlchemyError`` is caught at the session boundary
and mapped to ``MonitoringQueryError``; nothing else is caught. A row that
does not exist is ``Ok(None)``, not an error.

Sort and group columns are resolved through allowlist mappings keyed by
``Literal`` types, so no caller-supplied string reaches SQL.

Load characteristics: ``task_stats``, ``task_facets`` and ``task_breakdown``
are aggregates over ``horsies_tasks``; a full-scan GROUP BY is the expected
plan for them. Retention keeps the table bounded and the UI's poll cadences
are deliberately spaced; each open dashboard multiplies the load.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, Literal

from sqlalchemy import (
    ColumnElement,
    Select,
    SQLColumnExpression,
    and_,
    func,
    nulls_last,
    or_,
    select,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute, load_only

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_pg import (
    ScheduleStateModel,
    TaskAttemptModel,
    TaskModel,
)
from horsies.core.models.tasks import (
    BUILTIN_CODE_REGISTRY,
    ContractCode,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
)
from horsies.core.models.workflow_pg import WorkflowModel, WorkflowTaskModel
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import is_retryable_connection_error
from horsies.monitoring.errors import (
    MonitoringQueryError,
    MonitoringQueryErrorCode,
    MonitoringResult,
)
from horsies.monitoring.models import (
    Breakdown,
    ErrorCategory,
    ErrorFacet,
    FacetValue,
    Facets,
    GroupRow,
    LeafTaskInfo,
    ScheduleStateInfo,
    StatusCount,
    TaskAttemptInfo,
    TaskDetail,
    TaskListPage,
    TaskSummary,
    WorkflowEdge,
    WorkflowNodeInfo,
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

# Sort key -> the expression it orders by. ``queue_s`` and ``exec_s`` sort by
# the pure-SQL spans (dispatch->start, start->finish) so the table can be
# triaged by where time is spent. A live row's displayed duration counts up
# while its sort key stays NULL; that divergence is intentional.
_SORT_COLUMNS: dict[str, SQLColumnExpression[Any]] = {
    'enqueued_at': TaskModel.enqueued_at,
    'started_at': TaskModel.started_at,
    'completed_at': TaskModel.completed_at,
    'failed_at': TaskModel.failed_at,
    'status': TaskModel.status,
    'task_name': TaskModel.task_name,
    'queue_name': TaskModel.queue_name,
    'priority': TaskModel.priority,
    'retry_count': TaskModel.retry_count,
    'queue_s': TaskModel.started_at - TaskModel.enqueued_at,
    'exec_s': func.coalesce(TaskModel.completed_at, TaskModel.failed_at)
    - TaskModel.started_at,
}

# group_by value -> the model column it groups on.
_GROUP_COLUMNS: dict[str, InstrumentedAttribute[Any]] = {
    'worker': TaskModel.claimed_by_worker_id,
    'task_name': TaskModel.task_name,
    'queue': TaskModel.queue_name,
}

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

# Statuses in which a task is still waiting to start, so its queue wait is an
# open span that counts up.
_QUEUEING_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.PENDING, TaskStatus.CLAIMED}
)

# Distinct values offered per filter dimension, and the shorter slice of error
# codes offered in the dropdown. Neither is client-controllable.
_FACET_VALUE_CAP = 50
_ERROR_FACET_CAP = 30

# Built from horsies' own error-code enums so the mapping never drifts from
# the library. Any non-null code absent here is user-defined -> DOMAIN.
_CODE_TO_CATEGORY: dict[str, ErrorCategory] = {
    **{member.value: ErrorCategory.OPERATIONAL for member in OperationalErrorCode},
    **{member.value: ErrorCategory.CONTRACT for member in ContractCode},
    **{member.value: ErrorCategory.RETRIEVAL for member in RetrievalCode},
    **{member.value: ErrorCategory.OUTCOME for member in OutcomeCode},
}

# The codes each built-in family selects when filtering, inverted from the
# mapping above so a filter selects exactly the codes that mapping labels.
_CATEGORY_TO_CODES: dict[ErrorCategory, tuple[str, ...]] = {
    category: tuple(
        code for code, member in _CODE_TO_CATEGORY.items() if member is category
    )
    for category in (
        ErrorCategory.OPERATIONAL,
        ErrorCategory.CONTRACT,
        ErrorCategory.RETRIEVAL,
        ErrorCategory.OUTCOME,
    )
}

# Every code horsies defines. DOMAIN is the complement of this set, read from
# core's registry rather than a copy, so a code added to the library leaves the
# DOMAIN filter the moment it is registered.
_BUILTIN_CODES: tuple[str, ...] = tuple(BUILTIN_CODE_REGISTRY)


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


def _db_err(operation: str, exc: SQLAlchemyError) -> Err[MonitoringQueryError]:
    """Wrap a session-boundary failure, classifying transient errors."""
    return Err(
        MonitoringQueryError(
            code=MonitoringQueryErrorCode.DB_OPERATION_FAILED,
            message=f'{operation} failed: {exc}',
            retryable=is_retryable_connection_error(exc),
            exception=exc,
        )
    )


# --------------------------------------------------------------------------- #
# Row -> model mappers
# --------------------------------------------------------------------------- #
def _task_spans(task: TaskModel) -> tuple[int | None, int | None]:
    """``(queue_s, exec_s)`` for a task row.

    The queue span counts up only while the task is still waiting to start,
    and the execution span only while it is running. A terminal row missing
    its end timestamp yields None rather than a duration that grows forever.
    """
    end = task.completed_at or task.failed_at
    queue_s = span_s(
        task.enqueued_at,
        task.started_at or end,
        live=task.status in _QUEUEING_STATUSES,
    )
    exec_s = span_s(task.started_at, end, live=task.status is TaskStatus.RUNNING)
    return queue_s, exec_s


# The exact column set ``_task_summary`` reads. ``list_tasks`` loads only
# these; the payload columns (args, kwargs, result, task_options) stay
# unfetched, so a list page never ships or detoasts task payloads.
# ``raiseload=True`` turns any future access to an unlisted column into an
# immediate error instead of a silent lazy load.
_SUMMARY_COLUMNS: tuple[InstrumentedAttribute[Any], ...] = (
    TaskModel.id,
    TaskModel.task_name,
    TaskModel.queue_name,
    TaskModel.priority,
    TaskModel.status,
    TaskModel.retry_count,
    TaskModel.max_retries,
    TaskModel.is_workflow_task,
    TaskModel.error_code,
    TaskModel.worker_hostname,
    TaskModel.claimed_by_worker_id,
    TaskModel.enqueued_at,
    TaskModel.started_at,
    TaskModel.completed_at,
    TaskModel.failed_at,
)


def _task_summary(task: TaskModel) -> TaskSummary:
    """Map a task row to its list-view shape."""
    queue_s, exec_s = _task_spans(task)
    return TaskSummary(
        id=task.id,
        task_name=task.task_name,
        queue_name=task.queue_name,
        status=task.status.value,
        priority=task.priority,
        retry_count=task.retry_count,
        max_retries=task.max_retries,
        is_workflow_task=task.is_workflow_task,
        error_code=nz(task.error_code),
        error_category=_category_value(task.error_code),
        worker_hostname=task.worker_hostname,
        worker_id=task.claimed_by_worker_id,
        enqueued_at=task.enqueued_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        failed_at=task.failed_at,
        queue_s=queue_s,
        exec_s=exec_s,
    )


def _leaf_task(task: TaskModel) -> LeafTaskInfo:
    """Map a task row to the detail shape shared by tasks and workflow nodes."""
    queue_s, exec_s = _task_spans(task)
    return LeafTaskInfo(
        task_id=task.id,
        status=task.status.value,
        error_code=nz(task.error_code),
        failed_reason=nz(task.failed_reason),
        retry_count=task.retry_count,
        max_retries=task.max_retries,
        enqueued_at=task.enqueued_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        failed_at=task.failed_at,
        queue_s=queue_s,
        exec_s=exec_s,
        worker_hostname=task.worker_hostname,
        good_until=task.good_until,
    )


def _attempt(row: TaskAttemptModel) -> TaskAttemptInfo:
    """Map an attempt row to its response shape."""
    return TaskAttemptInfo(
        attempt=row.attempt,
        outcome=row.outcome,
        will_retry=row.will_retry,
        error_code=nz(row.error_code),
        error_message=nz(row.error_message),
        failed_reason=nz(row.failed_reason),
        worker_hostname=row.worker_hostname,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


def _run_summary(run: WorkflowModel) -> WorkflowRunSummary:
    """Map a workflow row to its summary shape."""
    return WorkflowRunSummary(
        id=run.id,
        name=run.name,
        definition_key=run.definition_key,
        status=run.status,
        created_at=run.created_at,
        completed_at=run.completed_at,
        wall_s=elapsed_s(run.created_at, run.completed_at),
    )


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
# Filter assembly
# --------------------------------------------------------------------------- #
def _category_predicate(category: ErrorCategory) -> ColumnElement[bool]:
    """The condition selecting tasks in one taxonomy family.

    A built-in family expands to its member codes. DOMAIN is their complement:
    any non-empty code the library does not define, which is exactly what
    ``categorize_error_code`` labels DOMAIN. Expansion happens here rather than
    in a caller so no client has to carry a copy of the code lists.
    """
    match category:
        case ErrorCategory.DOMAIN:
            return and_(
                TaskModel.error_code.is_not(None),
                TaskModel.error_code != '',
                TaskModel.error_code.not_in(_BUILTIN_CODES),
            )
        case (
            ErrorCategory.OPERATIONAL
            | ErrorCategory.CONTRACT
            | ErrorCategory.RETRIEVAL
            | ErrorCategory.OUTCOME
        ):
            return TaskModel.error_code.in_(_CATEGORY_TO_CODES[category])


def _scope_conditions(
    *,
    statuses: list[TaskStatus],
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
) -> list[ColumnElement[bool]]:
    """Build the WHERE conditions for the given task filters.

    Each dimension is multi-select: values within a dimension are OR-combined
    and dimensions are AND-combined. An empty list means no filter on that
    dimension. ``error_categories`` is the coarse form of ``error_codes`` and
    is an independent dimension: each family expands to a code predicate and
    the families are OR-combined. ``retried_only`` keeps only tasks retried at
    least once.
    """
    conditions: list[ColumnElement[bool]] = []
    if statuses:
        conditions.append(TaskModel.status.in_(statuses))
    if task_names:
        conditions.append(TaskModel.task_name.in_(task_names))
    if queues:
        conditions.append(TaskModel.queue_name.in_(queues))
    if workers:
        conditions.append(TaskModel.claimed_by_worker_id.in_(workers))
    if error_codes:
        conditions.append(TaskModel.error_code.in_(error_codes))
    if error_categories:
        conditions.append(
            or_(*(_category_predicate(category) for category in error_categories))
        )
    if retried_only:
        conditions.append(TaskModel.retry_count > 0)
    return conditions


def _facet_statement(
    column: InstrumentedAttribute[Any],
    scope: list[ColumnElement[bool]],
    *extra: ColumnElement[bool],
) -> Select[tuple[str | None, int]]:
    """Distinct values of one column with counts, capped for the dropdown."""
    counted = func.count().label('n')
    return (
        select(column, counted)
        .where(*scope, *extra)
        .group_by(column)
        .order_by(counted.desc())
        .limit(_FACET_VALUE_CAP)
    )


def _facet_values(rows: Sequence[tuple[str | None, int]]) -> list[FacetValue]:
    """Map facet rows to values.

    The NULL group is excluded in SQL — either by an explicit predicate or by
    the column being NOT NULL — so dropping it here only restates that to the
    type checker.
    """
    return [
        FacetValue(value=value, count=count)
        for value, count in rows
        if value is not None
    ]


# --------------------------------------------------------------------------- #
# Task queries
# --------------------------------------------------------------------------- #
async def task_stats(
    broker: PostgresBroker,
    *,
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
) -> MonitoringResult[list[StatusCount]]:
    """Task counts by status, for the overview cards.

    Scoped by every filter except status, because the cards are the status
    selector. Always returns all seven statuses in a fixed order, including
    zeros, so cards never appear, disappear, or reorder between polls.
    """
    scope = _scope_conditions(
        statuses=[],
        task_names=task_names,
        queues=queues,
        workers=workers,
        error_codes=error_codes,
        error_categories=error_categories,
        retried_only=retried_only,
    )
    stmt = (
        select(TaskModel.status, func.count()).where(*scope).group_by(TaskModel.status)
    )

    try:
        async with broker.session_factory() as session:
            rows = (await session.execute(stmt)).tuples().all()
    except SQLAlchemyError as exc:
        return _db_err('task stats query', exc)

    counts = {status: count for status, count in rows}
    return Ok(
        [
            StatusCount(status=status.value, count=counts.get(status, 0))
            for status in _STATUS_ORDER
        ]
    )


async def task_facets(
    broker: PostgresBroker,
    *,
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
    """
    scope = _scope_conditions(
        statuses=statuses,
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=retried_only,
    )
    workers_stmt = _facet_statement(
        TaskModel.claimed_by_worker_id,
        scope,
        TaskModel.claimed_by_worker_id.is_not(None),
    )
    names_stmt = _facet_statement(TaskModel.task_name, scope)
    queues_stmt = _facet_statement(TaskModel.queue_name, scope)
    # Error codes are not capped in SQL: every distinct code is needed for an
    # accurate per-category rollup. The dropdown list is sliced afterwards.
    error_count = func.count().label('n')
    errors_stmt = (
        select(TaskModel.error_code, error_count)
        .where(*scope, TaskModel.error_code.is_not(None), TaskModel.error_code != '')
        .group_by(TaskModel.error_code)
        .order_by(error_count.desc())
    )

    try:
        async with broker.session_factory() as session:
            worker_rows = (await session.execute(workers_stmt)).tuples().all()
            name_rows = (await session.execute(names_stmt)).tuples().all()
            queue_rows = (await session.execute(queues_stmt)).tuples().all()
            error_rows = (await session.execute(errors_stmt)).tuples().all()
    except SQLAlchemyError as exc:
        return _db_err('task facets query', exc)

    # error_code is non-null and non-empty here, so categorization never
    # returns None; DOMAIN is the fallback for user-defined codes.
    error_facets = [
        ErrorFacet(
            value=value,
            count=count,
            category=(categorize_error_code(value) or ErrorCategory.DOMAIN).value,
        )
        for value, count in error_rows
        if value is not None
    ]
    category_totals: dict[str, int] = {}
    for facet in error_facets:
        category_totals[facet.category] = (
            category_totals.get(facet.category, 0) + facet.count
        )

    selected = {category.value for category in error_categories}
    listed = (
        error_facets
        if not selected
        else [facet for facet in error_facets if facet.category in selected]
    )

    return Ok(
        Facets(
            workers=_facet_values(worker_rows),
            task_names=_facet_values(name_rows),
            queues=_facet_values(queue_rows),
            error_codes=listed[:_ERROR_FACET_CAP],
            error_category_totals=category_totals,
        )
    )


async def task_breakdown(
    broker: PostgresBroker,
    *,
    group_by: TaskGroupBy,
    statuses: list[TaskStatus],
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
    limit: int,
) -> MonitoringResult[Breakdown]:
    """Per-group status pivot with a rollup TOTAL row.

    Each group row carries per-status counts; the ``total`` row is the
    GROUPING rollup over every matching task, independent of ``limit``.
    Groups arrive count-descending and are capped to ``limit``, while
    ``group_count`` reports the true number of distinct groups.
    """
    column = _GROUP_COLUMNS[group_by]
    scope = _scope_conditions(
        statuses=statuses,
        task_names=task_names,
        queues=queues,
        workers=workers,
        error_codes=error_codes,
        error_categories=error_categories,
        retried_only=retried_only,
    )

    def _status_count(value: TaskStatus) -> ColumnElement[int]:
        return func.count().filter(TaskModel.status == value)

    rollup_flag = func.grouping(column).label('g')
    group_key = func.coalesce(column, 'unknown').label('grp')
    group_total = func.count().label('total')
    stmt = (
        select(
            rollup_flag,
            group_key,
            group_total,
            _status_count(TaskStatus.PENDING),
            _status_count(TaskStatus.CLAIMED),
            _status_count(TaskStatus.RUNNING),
            _status_count(TaskStatus.COMPLETED),
            _status_count(TaskStatus.FAILED),
            _status_count(TaskStatus.CANCELLED),
            _status_count(TaskStatus.EXPIRED),
            func.count().filter(TaskModel.retry_count > 0),
        )
        .where(*scope)
        .group_by(func.rollup(column))
        .order_by(rollup_flag, group_total.desc())
    )

    try:
        async with broker.session_factory() as session:
            rows = (await session.execute(stmt)).tuples().all()
    except SQLAlchemyError as exc:
        return _db_err('task breakdown query', exc)

    groups: list[GroupRow] = []
    total_row = GroupRow(
        group='TOTAL',
        total=0,
        pending=0,
        claimed=0,
        running=0,
        completed=0,
        failed=0,
        cancelled=0,
        expired=0,
        retried=0,
    )
    for row in rows:
        (
            is_rollup,
            key,
            total,
            pending,
            claimed,
            running,
            completed,
            failed,
            cancelled,
            expired,
            retried,
        ) = row
        built = GroupRow(
            group='TOTAL' if is_rollup == 1 else key,
            total=total,
            pending=pending,
            claimed=claimed,
            running=running,
            completed=completed,
            failed=failed,
            cancelled=cancelled,
            expired=expired,
            retried=retried,
        )
        if is_rollup == 1:
            total_row = built
        else:
            groups.append(built)

    return Ok(
        Breakdown(
            group_by=group_by,
            groups=groups[:limit],
            total=total_row,
            group_count=len(groups),
        )
    )


# Planner row estimate for the unfiltered task count. ``reltuples`` is
# refreshed by VACUUM / ANALYZE / autovacuum; -1 means the table was never
# sampled.
_ESTIMATED_TOTAL_SQL = text(
    'SELECT reltuples::bigint FROM pg_class WHERE oid = cast(:tablename AS regclass)'
)


async def _estimated_task_total(session: AsyncSession) -> int:
    """Estimated row count of ``horsies_tasks``; exact only when unsampled.

    Falls back to an exact count when the estimate is -1 (never sampled) —
    the one case where the estimate carries no information.
    """
    estimate: int = (
        await session.execute(
            _ESTIMATED_TOTAL_SQL, {'tablename': TaskModel.__tablename__}
        )
    ).scalar_one()
    if estimate >= 0:
        return estimate
    exact: int = (
        await session.execute(select(func.count()).select_from(TaskModel))
    ).scalar_one()
    return exact


async def list_tasks(
    broker: PostgresBroker,
    *,
    statuses: list[TaskStatus],
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
    sort_by: TaskSortField,
    sort_dir: SortDirection,
    offset: int,
    limit: int,
) -> MonitoringResult[TaskListPage]:
    """A paginated, server-sorted, server-filtered slice of tasks.

    ``total`` is the count matching the filters, not the length of ``rows``,
    so the UI can paginate without a second request. With filters active it
    is exact; on the unfiltered view it is the planner's row estimate
    (an exact count would rescan the table on every poll for a number the
    UI only uses to size the pager).
    """
    sort_column = _SORT_COLUMNS[sort_by]
    ordering = nulls_last(
        sort_column.asc() if sort_dir == 'asc' else sort_column.desc()
    )
    scope = _scope_conditions(
        statuses=statuses,
        task_names=task_names,
        queues=queues,
        workers=workers,
        error_codes=error_codes,
        error_categories=error_categories,
        retried_only=retried_only,
    )
    rows_stmt = (
        select(TaskModel)
        .options(load_only(*_SUMMARY_COLUMNS, raiseload=True))
        .where(*scope)
        .order_by(ordering)
        .limit(limit)
        .offset(offset)
    )

    try:
        async with broker.session_factory() as session:
            match scope:
                case []:
                    total = await _estimated_task_total(session)
                case _:
                    count_stmt = (
                        select(func.count()).select_from(TaskModel).where(*scope)
                    )
                    total = (await session.execute(count_stmt)).scalar_one()
            tasks = (await session.execute(rows_stmt)).scalars().all()
    except SQLAlchemyError as exc:
        return _db_err('task list query', exc)

    return Ok(TaskListPage(rows=[_task_summary(task) for task in tasks], total=total))


async def get_task_detail(
    broker: PostgresBroker,
    task_id: str,
) -> MonitoringResult[TaskDetail | None]:
    """A single task with its attempt history, or None when it does not exist.

    The attempt history is where a task's cause lives: ``error_message``
    carries the unhandled exception per try, including retries.
    """
    task_stmt = select(TaskModel).where(TaskModel.id == task_id)
    attempts_stmt = (
        select(TaskAttemptModel)
        .where(TaskAttemptModel.task_id == task_id)
        .order_by(TaskAttemptModel.attempt)
    )
    # One indexed hop to the node this task backs, if any. Kept as its own
    # statement rather than a join onto the task row: a standalone task is
    # the common case and this returns nothing for it.
    node_stmt = select(
        WorkflowTaskModel.workflow_id, WorkflowTaskModel.task_index
    ).where(WorkflowTaskModel.task_id == task_id)

    try:
        async with broker.session_factory() as session:
            task = (await session.execute(task_stmt)).scalar_one_or_none()
            if task is None:
                return Ok(None)
            attempt_rows = (await session.execute(attempts_stmt)).scalars().all()
            node = (await session.execute(node_stmt)).tuples().first()
    except SQLAlchemyError as exc:
        return _db_err('task detail query', exc)

    workflow_id, workflow_task_index = node if node is not None else (None, None)
    return Ok(
        TaskDetail(
            leaf=_leaf_task(task),
            task_name=task.task_name,
            queue_name=task.queue_name,
            priority=task.priority,
            is_workflow_task=task.is_workflow_task,
            error_category=_category_value(task.error_code),
            attempts=[_attempt(row) for row in attempt_rows],
            workflow_id=workflow_id,
            workflow_task_index=workflow_task_index,
        )
    )


# --------------------------------------------------------------------------- #
# Workflow queries
# --------------------------------------------------------------------------- #
async def list_workflow_names(
    broker: PostgresBroker,
) -> MonitoringResult[list[str]]:
    """Distinct names of root workflow runs, for the run-picker filter."""
    stmt = (
        select(WorkflowModel.name)
        .where(WorkflowModel.parent_workflow_id.is_(None))
        .distinct()
        .order_by(WorkflowModel.name)
    )

    try:
        async with broker.session_factory() as session:
            names = (await session.execute(stmt)).scalars().all()
    except SQLAlchemyError as exc:
        return _db_err('workflow names query', exc)

    return Ok(list(names))


async def list_workflow_runs(
    broker: PostgresBroker,
    *,
    name: str | None,
    status: str | None,
    limit: int,
) -> MonitoringResult[list[WorkflowRunSummary]]:
    """Recent root workflow runs, newest first.

    ``name`` and ``status`` are independent exact-match filters combined with
    AND; a status no run carries yields an empty list.
    """
    conditions: list[ColumnElement[bool]] = [WorkflowModel.parent_workflow_id.is_(None)]
    if name is not None:
        conditions.append(WorkflowModel.name == name)
    if status is not None:
        conditions.append(WorkflowModel.status == status)
    stmt = (
        select(WorkflowModel)
        .where(*conditions)
        .order_by(WorkflowModel.created_at.desc())
        .limit(limit)
    )

    try:
        async with broker.session_factory() as session:
            runs = (await session.execute(stmt)).scalars().all()
    except SQLAlchemyError as exc:
        return _db_err('workflow runs query', exc)

    return Ok([_run_summary(run) for run in runs])


async def get_workflow_run(
    broker: PostgresBroker,
    workflow_id: str,
) -> MonitoringResult[WorkflowRunDetail | None]:
    """A run's DAG — metadata, nodes, and dependency edges — or None if absent.

    Works for any run id, root or subworkflow, so a subworkflow node is
    drilled into by calling this again with that node's ``sub_workflow_id``.
    Edges pointing at an index the run does not have are dropped.
    """
    run_stmt = select(WorkflowModel).where(WorkflowModel.id == workflow_id)
    nodes_stmt = (
        select(WorkflowTaskModel)
        .where(WorkflowTaskModel.workflow_id == workflow_id)
        .order_by(WorkflowTaskModel.task_index)
    )

    try:
        async with broker.session_factory() as session:
            run = (await session.execute(run_stmt)).scalar_one_or_none()
            if run is None:
                return Ok(None)
            node_rows = (await session.execute(nodes_stmt)).scalars().all()

            # Direct-child status rollup for subworkflow nodes: one grouped
            # count over the child runs' node rows, keyed by child run id.
            sub_ids = [
                row.sub_workflow_id
                for row in node_rows
                if row.is_subworkflow and row.sub_workflow_id is not None
            ]
            child_rollup: dict[str, tuple[int, int]] = {}
            if sub_ids:
                rollup_stmt = (
                    select(
                        WorkflowTaskModel.workflow_id,
                        func.count(),
                        func.count().filter(WorkflowTaskModel.status == 'FAILED'),
                    )
                    .where(WorkflowTaskModel.workflow_id.in_(sub_ids))
                    .group_by(WorkflowTaskModel.workflow_id)
                )
                child_rollup = {
                    child_id: (total, failed)
                    for child_id, total, failed in (
                        (await session.execute(rollup_stmt)).tuples().all()
                    )
                }
    except SQLAlchemyError as exc:
        return _db_err('workflow run detail query', exc)

    known_indices = {row.task_index for row in node_rows}
    nodes: list[WorkflowNodeInfo] = []
    edges: list[WorkflowEdge] = []
    for row in node_rows:
        counts = (
            child_rollup.get(row.sub_workflow_id)
            if row.is_subworkflow and row.sub_workflow_id is not None
            else None
        )
        child_total, child_failed = counts if counts is not None else (None, None)
        nodes.append(
            WorkflowNodeInfo(
                task_index=row.task_index,
                node_id=row.node_id,
                task_name=row.task_name,
                node_status=row.status,
                is_subworkflow=row.is_subworkflow,
                sub_workflow_id=row.sub_workflow_id,
                allow_failed_deps=row.allow_failed_deps,
                started_at=row.started_at,
                completed_at=row.completed_at,
                exec_s=_node_exec_s(row.status, row.started_at, row.completed_at),
                child_total=child_total,
                child_failed=child_failed,
            )
        )
        for dep_index in row.dependencies:
            if dep_index in known_indices:
                edges.append(
                    WorkflowEdge(from_index=dep_index, to_index=row.task_index)
                )

    failed_indices = [node.task_index for node in nodes if node.node_status == 'FAILED']
    return Ok(
        WorkflowRunDetail(
            run=_run_summary(run),
            nodes=nodes,
            edges=edges,
            failed_count=len(failed_indices),
            failed_indices=failed_indices,
        )
    )


async def get_workflow_node(
    broker: PostgresBroker,
    workflow_id: str,
    task_index: int,
) -> MonitoringResult[WorkflowTaskDetail | None]:
    """Detail for one node: node error, backing task, and attempt history.

    Returns None when the run has no node at that index. Subworkflow nodes
    have no backing task; drill into the child run instead. ``leaf`` is also
    None when the backing task row has been removed by retention, while its
    attempt rows are still reported if present.
    """
    node_stmt = select(WorkflowTaskModel).where(
        WorkflowTaskModel.workflow_id == workflow_id,
        WorkflowTaskModel.task_index == task_index,
    )

    try:
        async with broker.session_factory() as session:
            node = (await session.execute(node_stmt)).scalar_one_or_none()
            if node is None:
                return Ok(None)

            task: TaskModel | None = None
            attempt_rows: list[TaskAttemptModel] = []
            if node.task_id is not None:
                task = (
                    await session.execute(
                        select(TaskModel).where(TaskModel.id == node.task_id)
                    )
                ).scalar_one_or_none()
                attempt_rows = list(
                    (
                        await session.execute(
                            select(TaskAttemptModel)
                            .where(TaskAttemptModel.task_id == node.task_id)
                            .order_by(TaskAttemptModel.attempt)
                        )
                    )
                    .scalars()
                    .all()
                )
    except SQLAlchemyError as exc:
        return _db_err('workflow node detail query', exc)

    return Ok(
        WorkflowTaskDetail(
            task_index=node.task_index,
            node_id=node.node_id,
            task_name=node.task_name,
            node_status=node.status,
            is_subworkflow=node.is_subworkflow,
            node_error=nz(node.error),
            leaf=_leaf_task(task) if task is not None else None,
            attempts=[_attempt(row) for row in attempt_rows],
        )
    )


# --------------------------------------------------------------------------- #
# Schedules
# --------------------------------------------------------------------------- #
async def list_schedules(
    broker: PostgresBroker,
) -> MonitoringResult[list[ScheduleStateInfo]]:
    """Recurring schedule states, soonest next-run first."""
    stmt = select(ScheduleStateModel).order_by(
        nulls_last(ScheduleStateModel.next_run_at.asc())
    )

    try:
        async with broker.session_factory() as session:
            rows = (await session.execute(stmt)).scalars().all()
    except SQLAlchemyError as exc:
        return _db_err('schedule state query', exc)

    return Ok(
        [
            ScheduleStateInfo(
                schedule_name=row.schedule_name,
                last_run_at=row.last_run_at,
                next_run_at=row.next_run_at,
                last_task_id=row.last_task_id,
                run_count=row.run_count,
                updated_at=row.updated_at,
            )
            for row in rows
        ]
    )
