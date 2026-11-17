# pyright: reportPrivateUsage=false
"""Reference of the dual-run of the monitoring aggregate reads: the client statements.

``task_stats``, ``task_facets`` and ``task_breakdown`` as they ran before
they called ``horsies_monitoring_task_stats``,
``horsies_monitoring_task_facets`` and ``horsies_monitoring_task_breakdown``:
the live statement and the history statement in one session, merged in
client code. The decided changes of these reads are applied (the unclaimed
breakdown group, and a history status that is not a task status answering
``MonitoringQueryHistoryContractViolation``), so the reference and the
functions answer the same.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import ColumnElement, func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import InstrumentedAttribute

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.names import TASK_HISTORY_PARENT
from horsies.core.history.reads.aggregates import (
    BREAKDOWN_GROUP_COLUMNS,
    HistoryScope,
    history_scoped_status_counts_statement,
)
from horsies.core.history.reads.pages import HistoryWindow, history_scope_conditions
from horsies.core.models.task_pg import TaskModel
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring.errors import MonitoringResult
from horsies.monitoring.history_window import MonitoringWindow
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
    StatusCount,
)
from horsies.monitoring.pages import BreakdownLimit
from tests.dualrun.reference.monitoring_queries import (
    _ERROR_FACET_CAP,
    _STATUS_BY_VALUE,
    _STATUS_ORDER,
    TaskGroupBy,
    _contract_err,
    _db_err,
    _loop_limit_err,
)
from tests.dualrun.monitoring_list_reference import (
    _CATEGORY_TO_CODES,
    history_scope,
    scope_conditions,
)


# Distinct values offered per filter dimension.
_FACET_VALUE_CAP = 50

# group_by value -> the model column it groups on.
_GROUP_COLUMNS: dict[str, InstrumentedAttribute[Any]] = {
    'worker': TaskModel.claimed_by_worker_id,
    'task_name': TaskModel.task_name,
    'queue': TaskModel.queue_name,
}


# ``(facet, column)`` of each side. The live table has no error code, so the
# live side has no ``error_code`` facet.
_LIVE_FACET_COLUMNS: tuple[tuple[str, str], ...] = (
    ('worker', 'claimed_by_worker_id'),
    ('task_name', 'task_name'),
    ('queue_name', 'queue_name'),
)


_HISTORY_FACET_COLUMNS: tuple[tuple[str, str], ...] = (
    ('worker', 'last_claimed_worker_id'),
    ('task_name', 'task_name'),
    ('queue_name', 'queue_name'),
    ('error_code', 'error_code'),
)


def _facet_side_sql(
    facet_columns: tuple[tuple[str, str], ...],
    conditions: list[str],
    table: str,
) -> str:
    """One side of ``facets_statement``: a count per value of each facet column.

    The inner group can run in parallel, and GROUPING SETS cannot, so the
    sets read its small result, not the rows. ``GROUPING()`` sets a bit for
    each column that is not in the row's set; the set of column ``i`` of
    ``n`` has every bit but bit ``n - 1 - i``.
    """
    columns = ', '.join(column for _, column in facet_columns)
    all_bits = (1 << len(facet_columns)) - 1
    masks = [
        all_bits - (1 << (len(facet_columns) - 1 - index))
        for index in range(len(facet_columns))
    ]
    facet_arms = ' '.join(
        f"WHEN {mask} THEN '{facet}'"
        for mask, (facet, _) in zip(masks, facet_columns, strict=True)
    )
    value_arms = ' '.join(
        f'WHEN {mask} THEN {column}::text'
        for mask, (_, column) in zip(masks, facet_columns, strict=True)
    )
    grouping_sets = ', '.join(f'({column})' for _, column in facet_columns)
    return f"""
        SELECT CASE GROUPING({columns}) {facet_arms} END AS facet,
               CASE GROUPING({columns}) {value_arms} END AS facet_value,
               sum(group_count) AS facet_count
        FROM (
            SELECT {columns}, count(*) AS group_count
            FROM {table}
            WHERE {' AND '.join(conditions) or 'TRUE'}
            GROUP BY {columns}
        ) AS side_groups
        GROUP BY GROUPING SETS ({grouping_sets})
    """


def facets_statement(
    *,
    window: HistoryWindow,
    statuses: list[TaskStatus],
    retried_only: bool,
) -> tuple[str, dict[str, object]]:
    """Every facet of ``task_facets`` in one scan of each side.

    One ``GROUPING SETS`` per side counts its facet columns in one pass, and
    the sides sum per facet. The rank runs here, after the sum, so that ties
    follow the database collation. Error codes come from the history side
    only: a live row has none.

    An error code is a free string, so a window can hold as many codes as rows.
    The category of each code and the exact total of each category come from
    here, and only the top codes of each category are returned, so the result
    stays bounded and any category selection can fill its list from it.

    Returns:
        The SQL and its parameters. Rows hold ``facet``, ``facet_value`` and
        ``facet_count``, ordered by ``facet`` and overall rank. A row of the
        ``error_code`` facet also holds ``category`` and ``category_total``.
    """
    parameters: dict[str, object] = {
        'window_lower': window.lower,
        'window_upper': window.upper,
        'facet_limit': _FACET_VALUE_CAP,
        'error_limit': _ERROR_FACET_CAP,
    }
    category_arms: list[str] = []
    for category, codes in _CATEGORY_TO_CODES.items():
        parameter = f'category_{category.value.lower()}_codes'
        parameters[parameter] = list(codes)
        category_arms.append(
            f"WHEN facet_value = ANY(CAST(:{parameter} AS text[])) "
            f"THEN '{category.value}'"
        )
    common: list[str] = []
    if statuses:
        common.append('status = ANY(CAST(:status_filter AS text[]))')
        parameters['status_filter'] = [status.value for status in statuses]
    if retried_only:
        common.append('retry_count > 0')

    live = _facet_side_sql(_LIVE_FACET_COLUMNS, common, 'horsies_tasks')
    history = _facet_side_sql(
        _HISTORY_FACET_COLUMNS,
        [
            'terminal_at >= :window_lower',
            'terminal_at < :window_upper',
            *common,
        ],
        'horsies_task_history',
    )
    sql = f"""
        WITH lifecycle_facets AS (
            {live}
            UNION ALL
            {history}
        ), combined AS (
            SELECT facet, facet_value, sum(facet_count) AS facet_count
            FROM lifecycle_facets
            WHERE facet_value IS NOT NULL
              AND (facet <> 'error_code' OR facet_value <> '')
            GROUP BY facet, facet_value
        ), ranked AS (
            SELECT facet, facet_value, facet_count,
                   row_number() OVER (
                       PARTITION BY facet
                       ORDER BY facet_count DESC, facet_value
                   ) AS facet_rank,
                   CASE WHEN facet = 'error_code' THEN
                       CASE {' '.join(category_arms)}
                            ELSE '{ErrorCategory.DOMAIN.value}'
                       END
                   END AS category
            FROM combined
        ), scoped AS (
            SELECT facet, facet_value, facet_count, facet_rank, category,
                   row_number() OVER (
                       PARTITION BY facet, category ORDER BY facet_rank
                   ) AS category_rank,
                   sum(facet_count) OVER (
                       PARTITION BY facet, category
                   ) AS category_total
            FROM ranked
        )
        SELECT facet, facet_value, facet_count, category, category_total
        FROM scoped
        WHERE CASE facet
                  WHEN 'error_code' THEN category_rank <= :error_limit
                  ELSE facet_rank <= :facet_limit
              END
        ORDER BY facet, facet_rank
    """
    return sql, parameters


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
    """
    scope = scope_conditions(
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
    history_sql, history_params = history_scoped_status_counts_statement(
        window.history,
        history_scope(
            statuses=[],
            task_names=task_names,
            queues=queues,
            workers=workers,
            error_codes=error_codes,
            error_categories=error_categories,
            retried_only=retried_only,
        ),
    )

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task stats query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            rows = (await session.execute(stmt)).tuples().all()
            history_rows = (
                await session.execute(text(history_sql), history_params)
            ).all()
    except SQLAlchemyError as exc:
        return _db_err('task stats query', exc)

    counts = {status: count for status, count in rows}
    merged: dict[TaskStatus, int] = dict(counts)
    for row in history_rows:
        stored = str(row.status)
        match _STATUS_BY_VALUE.get(stored):
            case TaskStatus() as status:
                merged[status] = merged.get(status, 0) + int(row.terminal_count)
            case None:
                return _contract_err(
                    'task stats query',
                    HistoryContractError(
                        f'{int(row.terminal_count)} history row(s) in the window '
                        f'hold status {stored!r}, which is not a task status'
                    ),
                )
    return Ok(
        [
            StatusCount(status=status.value, count=merged.get(status, 0))
            for status in _STATUS_ORDER
        ]
    )


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
    """
    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task facets query', loop_limit)
        case Ok(resources):
            pass

    sql, params = facets_statement(
        window=window.history,
        statuses=statuses,
        retried_only=retried_only,
    )
    try:
        async with resources.session_factory() as session:
            rows = (await session.execute(text(sql), params)).all()
    except SQLAlchemyError as exc:
        return _db_err('task facets query', exc)

    ranked: dict[str, list[tuple[str, int]]] = {
        'worker': [],
        'task_name': [],
        'queue_name': [],
    }
    selected = {category.value for category in error_categories}
    category_totals: dict[str, int] = {}
    error_facets: list[ErrorFacet] = []
    for row in rows:
        value, count = str(row.facet_value), int(row.facet_count)
        if row.facet != 'error_code':
            ranked[row.facet].append((value, count))
            continue
        category_totals[row.category] = int(row.category_total)
        if (not selected or row.category in selected) and len(error_facets) < _ERROR_FACET_CAP:
            error_facets.append(
                ErrorFacet(value=value, count=count, category=row.category),
            )

    return Ok(
        Facets(
            workers=_facet_values(ranked['worker']),
            task_names=_facet_values(ranked['task_name']),
            queues=_facet_values(ranked['queue_name']),
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
    """Per-group status pivot with a rollup TOTAL row.

    Each group row carries per-status counts; ``total`` sums every group,
    independent of ``limit``. Groups arrive count-descending and are capped
    to ``limit``, while ``group_count`` reports the true number of distinct
    groups. For ``worker``, the tasks that no worker claimed are their own
    group (``BreakdownUnclaimedGroup``), apart from a worker named
    ``'unknown'``.
    """
    column = _GROUP_COLUMNS[group_by]
    scope = scope_conditions(
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
    group_total = func.count().label('total')
    stmt = (
        select(
            rollup_flag,
            column,
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

    history_group_column = {
        'worker': 'last_claimed_worker_id',
        'task_name': 'task_name',
        'queue': 'queue_name',
    }[group_by]
    history_sql, history_params = _history_breakdown_statement(
        window.history,
        history_scope(
            statuses=statuses,
            task_names=task_names,
            queues=queues,
            workers=workers,
            error_codes=error_codes,
            error_categories=error_categories,
            retried_only=retried_only,
        ),
        group_column=history_group_column,
    )

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task breakdown query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            rows = (await session.execute(stmt)).tuples().all()
            history_rows = (
                await session.execute(text(history_sql), history_params)
            ).all()
    except SQLAlchemyError as exc:
        return _db_err('task breakdown query', exc)

    merged: dict[BreakdownGroup, GroupRow] = {}
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
        if is_rollup == 1:
            continue
        group = _breakdown_group(key)
        merged[group] = GroupRow(
            group=group,
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
    for row in history_rows:
        group = _breakdown_group(row.group_value)
        status = str(row.status).lower()
        count = int(row.status_count)
        retried = int(row.retried_count)
        base = merged.get(group)
        if base is None:
            base = GroupRow(
                group=group, total=0, pending=0, claimed=0, running=0,
                completed=0, failed=0, cancelled=0, expired=0, retried=0,
            )
        update = {
            'total': base.total + count,
            'retried': base.retried + retried,
        }
        if status in ('completed', 'failed', 'cancelled', 'expired'):
            update[status] = getattr(base, status) + count
        merged[group] = base.model_copy(update=update)
    ordered = sorted(merged.values(), key=_breakdown_order)
    return Ok(
        Breakdown(
            group_by=group_by,
            groups=ordered[:limit.value],
            total=_breakdown_counts(ordered),
            group_count=len(ordered),
        )
    )


def _history_breakdown_statement(
    window: HistoryWindow,
    scope: HistoryScope,
    *,
    group_column: str,
) -> tuple[str, dict[str, object]]:
    """Per-group history status counts and retried totals.

    ``group_value`` is NULL for the rows whose group column is NULL (a
    task that no worker claimed).
    """
    assert group_column in BREAKDOWN_GROUP_COLUMNS, group_column
    conditions, parameters = history_scope_conditions(window, scope)
    sql = (
        f'SELECT {group_column} AS group_value, '
        'status, count(*) AS status_count, '
        'count(*) FILTER (WHERE retry_count > 0) AS retried_count '
        f'FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)} '
        f'GROUP BY {group_column}, status'
    )
    return sql, parameters


def _breakdown_group(value: str | None) -> BreakdownGroup:
    """The group of one stored group-column value; NULL is the unclaimed group."""
    match value:
        case None:
            return BreakdownUnclaimedGroup()
        case str():
            return BreakdownValueGroup(value=value)


def _breakdown_order(row: GroupRow) -> tuple[int, str, bool]:
    """Total, highest first, then the group text in code-point order.

    The unclaimed group sorts as the text ``'unknown'``, after a value
    group ``'unknown'`` with the same total.
    """
    match row.group:
        case BreakdownValueGroup(value=value):
            return (-row.total, value, False)
        case BreakdownUnclaimedGroup():
            return (-row.total, 'unknown', True)


def _breakdown_counts(groups: Sequence[GroupRow]) -> BreakdownCounts:
    """The sum of every group's counts."""
    return BreakdownCounts(
        total=sum(row.total for row in groups),
        pending=sum(row.pending for row in groups),
        claimed=sum(row.claimed for row in groups),
        running=sum(row.running for row in groups),
        completed=sum(row.completed for row in groups),
        failed=sum(row.failed for row in groups),
        cancelled=sum(row.cancelled for row in groups),
        expired=sum(row.expired for row in groups),
        retried=sum(row.retried for row in groups),
    )

