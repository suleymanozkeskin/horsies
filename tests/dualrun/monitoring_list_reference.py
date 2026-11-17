# pyright: reportPrivateUsage=false
"""Reference of the dual-run of the task list: the client statements.

``list_tasks`` as it ran before it called ``horsies_monitoring_task_list``:
in one session, the totals (exact counts with any filter; the live
``reltuples`` estimate and the EXPLAIN of the history window with none),
the live page and the history page, each read to the page reach, merged in
client code. The decided order is applied (#1099): the sort value in the
database collation, then a live row before a history row, then the task id
ascending, with NULL keys last in both directions. The client merge
compared text keys in code-point order; this reference ranks them in the
database collation with one more statement, so the reference and the
function answer the same on any collation. A row carries its task id as
``uuid.UUID``.

The live and history filter builders also serve the aggregate reference
(``monitoring_reference.py``) and the detail reference.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Final, NamedTuple

from sqlalchemy import (
    ColumnElement,
    DateTime,
    Interval,
    Select,
    SQLColumnExpression,
    cast,
    false,
    func,
    null,
    nulls_last,
    select,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute, load_only

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.aggregates import (
    HISTORY_NONEMPTY_PROBE_SQL,
    HistoryScope,
    history_count_statement,
    history_estimate_statement,
    plan_rows_from_explain,
)
from horsies.core.history.reads.pages import (
    HistoryPageQuery,
    HistoryWindow,
    history_page_statement,
    history_sort_expression,
)
from horsies.core.models.task_pg import TaskModel
from horsies.core.models.tasks import BUILTIN_CODE_REGISTRY
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import uuid_column_value
from horsies.monitoring.errors import MonitoringResult
from horsies.monitoring.history_window import MonitoringWindow
from horsies.monitoring.models import ErrorCategory, TaskListPage, TaskSummary
from horsies.monitoring.pages import TaskPage
from tests.dualrun.reference.monitoring_queries import (
    _CODE_TO_CATEGORY,
    SortDirection,
    TaskSortField,
    _category_value,
    _contract_err,
    _db_err,
    _live_spans,
    _loop_limit_err,
    nz,
    span_s,
)


# Sort key -> the expression it orders by, tagged with nullability.
# ``nulls_last`` wraps only nullable expressions (a NOT NULL column keeps the
# ordering a plain btree provides).
class _SortKey(NamedTuple):
    expression: SQLColumnExpression[Any]
    nullable: bool


# A live row has no end time, so the sort keys built on one are NULL for every
# live row: the live side sorts them as NULL, after every history row.
_LIVE_END_TIME: SQLColumnExpression[Any] = cast(null(), DateTime(timezone=True))
_LIVE_EXECUTION_SPAN: SQLColumnExpression[Any] = cast(null(), Interval())

_SORT_COLUMNS: dict[str, _SortKey] = {
    'enqueued_at': _SortKey(TaskModel.enqueued_at, nullable=False),
    'started_at': _SortKey(TaskModel.started_at, nullable=True),
    'completed_at': _SortKey(_LIVE_END_TIME, nullable=True),
    'failed_at': _SortKey(_LIVE_END_TIME, nullable=True),
    'status': _SortKey(TaskModel.status, nullable=False),
    'task_name': _SortKey(TaskModel.task_name, nullable=False),
    'queue_name': _SortKey(TaskModel.queue_name, nullable=False),
    'priority': _SortKey(TaskModel.priority, nullable=False),
    'retry_count': _SortKey(TaskModel.retry_count, nullable=False),
    'queue_s': _SortKey(
        TaskModel.started_at - TaskModel.enqueued_at, nullable=True
    ),
    'exec_s': _SortKey(_LIVE_EXECUTION_SPAN, nullable=True),
}

# The sort keys whose values are text: the merge ranks them in the database
# collation.
_TEXT_SORT_FIELDS: Final = frozenset({'status', 'task_name', 'queue_name'})

_COLLATION_ORDER_SQL = text(
    'SELECT v.value FROM unnest(CAST(:sort_values AS text[])) AS v(value) '
    'GROUP BY v.value ORDER BY v.value'
)

# The side rank of the merge: on one sort value a live row comes first.
_LIVE_RANK: Final = 0
_HISTORY_RANK: Final = 1

# The codes each built-in family selects when filtering.
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

# Every code horsies defines; DOMAIN is its complement.
_BUILTIN_CODES: tuple[str, ...] = tuple(BUILTIN_CODE_REGISTRY)

# The exact column set ``_task_summary`` reads; the payload columns stay
# unfetched.
_SUMMARY_COLUMNS: tuple[InstrumentedAttribute[Any], ...] = (
    TaskModel.id,
    TaskModel.task_name,
    TaskModel.queue_name,
    TaskModel.priority,
    TaskModel.status,
    TaskModel.retry_count,
    TaskModel.max_retries,
    TaskModel.is_workflow_task,
    TaskModel.worker_hostname,
    TaskModel.claimed_by_worker_id,
    TaskModel.enqueued_at,
    TaskModel.started_at,
)

# Planner row estimate for the unfiltered task count.
_ESTIMATED_TOTAL_SQL = text(
    'SELECT reltuples::bigint FROM pg_class WHERE oid = cast(:tablename AS regclass)'
)


def _task_spans(task: TaskModel) -> tuple[int | None, int | None]:
    """``(queue_s, exec_s)`` for a live task row."""
    return _live_spans(task.status, task.enqueued_at, task.started_at)


def _task_summary(task: TaskModel) -> TaskSummary:
    """Map a live task row to its list-view shape."""
    queue_s, exec_s = _task_spans(task)
    return TaskSummary(
        id=uuid_column_value(task.id, column='horsies_tasks.id'),
        task_name=task.task_name,
        queue_name=task.queue_name,
        status=task.status.value,
        priority=task.priority,
        retry_count=task.retry_count,
        max_retries=task.max_retries,
        is_workflow_task=task.is_workflow_task,
        error_code=None,
        error_category=None,
        worker_hostname=task.worker_hostname,
        worker_id=task.claimed_by_worker_id,
        enqueued_at=task.enqueued_at,
        started_at=task.started_at,
        completed_at=None,
        failed_at=None,
        queue_s=queue_s,
        exec_s=exec_s,
    )


def _history_summary(row: Any) -> TaskSummary:
    """Map one history page row to the list-view shape."""
    status = str(row.status)
    completed_at = row.terminal_at if status == 'COMPLETED' else None
    failed_at = row.terminal_at if status != 'COMPLETED' else None
    return TaskSummary(
        id=uuid_column_value(row.task_id, column='task_id'),
        task_name=row.task_name,
        queue_name=row.queue_name,
        status=status,
        priority=row.priority,
        retry_count=row.retry_count,
        max_retries=row.max_retries,
        is_workflow_task=row.is_workflow_task,
        error_code=nz(row.error_code),
        error_category=_category_value(row.error_code),
        worker_hostname=row.last_worker_hostname,
        worker_id=row.last_claimed_worker_id,
        enqueued_at=row.enqueued_at,
        started_at=row.started_at,
        completed_at=completed_at,
        failed_at=failed_at,
        queue_s=span_s(
            row.enqueued_at, row.started_at or row.terminal_at, live=False
        ),
        exec_s=span_s(row.started_at, row.terminal_at, live=False),
    )


def _summary_sort_value(summary: TaskSummary, field: str) -> Any:
    """The merge key for one allowlisted sort field, as the SQL side orders it."""
    match field:
        case 'queue_s':
            if summary.started_at is None or summary.enqueued_at is None:
                return None
            return summary.started_at - summary.enqueued_at
        case 'exec_s':
            end = summary.completed_at or summary.failed_at
            if end is None or summary.started_at is None:
                return None
            return end - summary.started_at
        case _:
            return getattr(summary, field)


def history_scope(
    *,
    statuses: list[TaskStatus],
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
) -> HistoryScope:
    """The live filter set in history terms; DOMAIN is the complement of every built-in code."""
    families: list[tuple[str, ...]] = []
    domain_complement: tuple[str, ...] | None = None
    for category in error_categories:
        if category is ErrorCategory.DOMAIN:
            domain_complement = _BUILTIN_CODES
        else:
            families.append(_CATEGORY_TO_CODES[category])
    return HistoryScope(
        statuses=tuple(status.value for status in statuses),
        task_names=tuple(task_names),
        queue_names=tuple(queues),
        workers=tuple(workers),
        error_codes=tuple(error_codes),
        category_families=tuple(families),
        domain_complement=domain_complement,
        retried_only=retried_only,
    )


def scope_conditions(
    *,
    statuses: list[TaskStatus],
    task_names: list[str],
    queues: list[str],
    workers: list[str],
    error_codes: list[str],
    error_categories: list[ErrorCategory],
    retried_only: bool,
) -> list[ColumnElement[bool]]:
    """The live-table WHERE conditions; an error-code or category filter selects no live row."""
    conditions: list[ColumnElement[bool]] = []
    if statuses:
        conditions.append(TaskModel.status.in_(statuses))
    if task_names:
        conditions.append(TaskModel.task_name.in_(task_names))
    if queues:
        conditions.append(TaskModel.queue_name.in_(queues))
    if workers:
        conditions.append(TaskModel.claimed_by_worker_id.in_(workers))
    if error_codes or error_categories:
        conditions.append(false())
    if retried_only:
        conditions.append(TaskModel.retry_count > 0)
    return conditions


def _list_rows_statement(
    *,
    scope: list[ColumnElement[bool]],
    sort_by: TaskSortField,
    sort_dir: SortDirection,
    limit: int,
) -> Select[tuple[TaskModel]]:
    """The live page: the sort value, then the task id ascending."""
    sort_key = _SORT_COLUMNS[sort_by]
    directed = (
        sort_key.expression.asc()
        if sort_dir == 'asc'
        else sort_key.expression.desc()
    )
    ordering = nulls_last(directed) if sort_key.nullable else directed
    return (
        select(TaskModel)
        .options(load_only(*_SUMMARY_COLUMNS, raiseload=True))
        .where(*scope)
        .order_by(ordering, TaskModel.id.asc())
        .limit(limit)
    )


async def _estimated_task_total(session: AsyncSession) -> int:
    """Estimated row count of ``horsies_tasks``; exact only when never sampled."""
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


async def _estimated_history_total(
    session: AsyncSession,
    window: HistoryWindow,
    scope: HistoryScope,
) -> int:
    """Planner row estimate of the history window; 0 for an empty history table.

    Raises:
        HistoryContractError: the EXPLAIN payload does not decode.
    """
    nonempty = (
        await session.execute(text(HISTORY_NONEMPTY_PROBE_SQL))
    ).scalar_one()
    if not bool(nonempty):
        return 0
    estimate_sql, estimate_params = history_estimate_statement(window, scope)
    payload = (
        await session.execute(text(estimate_sql), estimate_params)
    ).scalar_one()
    return plan_rows_from_explain(payload)


async def _collation_ranks(
    session: AsyncSession, values: Sequence[str],
) -> dict[str, int]:
    """The position of each text value in the database collation."""
    ordered = (
        await session.execute(_COLLATION_ORDER_SQL, {'sort_values': list(values)})
    ).scalars().all()
    return {str(value): position for position, value in enumerate(ordered)}


def _merged(
    ranked: list[tuple[int, TaskSummary]],
    sort_by: TaskSortField,
    sort_dir: SortDirection,
    collation_ranks: dict[str, int],
) -> list[TaskSummary]:
    """The rows in the decided order: value (NULL last), LIVE before HISTORY, task id."""
    ranked.sort(key=lambda pair: (pair[0], pair[1].id))
    keyed: list[tuple[Any, TaskSummary]] = []
    for _, summary in ranked:
        value = _summary_sort_value(summary, sort_by)
        keyed.append(
            (collation_ranks[value] if sort_by in _TEXT_SORT_FIELDS else value, summary)
        )
    non_null = [pair for pair in keyed if pair[0] is not None]
    nulls = [summary for value, summary in keyed if value is None]
    non_null.sort(key=lambda pair: pair[0], reverse=sort_dir == 'desc')
    return [summary for _, summary in non_null] + nulls


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
    """The task list through the client statements, in the decided order."""
    scope = scope_conditions(
        statuses=statuses,
        task_names=task_names,
        queues=queues,
        workers=workers,
        error_codes=error_codes,
        error_categories=error_categories,
        retried_only=retried_only,
    )
    rows_stmt = _list_rows_statement(
        scope=scope, sort_by=sort_by, sort_dir=sort_dir, limit=page.reach,
    )
    filters = history_scope(
        statuses=statuses,
        task_names=task_names,
        queues=queues,
        workers=workers,
        error_codes=error_codes,
        error_categories=error_categories,
        retried_only=retried_only,
    )
    page_sql, page_params = history_page_statement(
        HistoryPageQuery(
            window=window.history,
            limit=page.reach,
            offset=0,
            scope=filters,
            order_by=(
                f'{history_sort_expression(sort_by, descending=sort_dir == "desc")}, '
                'task_id ASC'
            ),
        )
    )
    count_sql, count_params = history_count_statement(window.history, filters)

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task list query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            match scope:
                case []:
                    live_total = await _estimated_task_total(session)
                    history_total = await _estimated_history_total(
                        session, window.history, filters
                    )
                case _:
                    live_total = (
                        await session.execute(
                            select(func.count()).select_from(TaskModel).where(*scope)
                        )
                    ).scalar_one()
                    history_total = int(
                        (
                            await session.execute(text(count_sql), count_params)
                        ).scalar_one()
                    )
            tasks = (await session.execute(rows_stmt)).scalars().all()
            history_rows = (
                await session.execute(text(page_sql), page_params)
            ).all()
            ranked = [(_LIVE_RANK, _task_summary(task)) for task in tasks]
            ranked.extend(
                (_HISTORY_RANK, _history_summary(row)) for row in history_rows
            )
            collation_ranks: dict[str, int] = {}
            if sort_by in _TEXT_SORT_FIELDS:
                collation_ranks = await _collation_ranks(
                    session,
                    [str(_summary_sort_value(summary, sort_by)) for _, summary in ranked],
                )
    except SQLAlchemyError as exc:
        return _db_err('task list query', exc)
    except HistoryContractError as exc:
        return _contract_err('task list estimate decode', exc)

    ordered = _merged(ranked, sort_by, sort_dir, collation_ranks)
    return Ok(
        TaskListPage(
            rows=ordered[page.offset : page.reach],
            total=live_total + history_total,
        )
    )

