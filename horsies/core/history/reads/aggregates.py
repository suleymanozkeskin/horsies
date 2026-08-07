"""Time-scoped history aggregates through the partitioned parent.

An aggregate visits no leaf outside its requested anchor range: the
parent's RANGE key appears in every statement, so the planner prunes
the scan to in-range leaves. The claim is proven by an EXPLAIN-based
leaf-visit test, not asserted by wall-clock. Column discipline matches
the listing surfaces: grouping keys and counts only, never envelopes.
"""

from __future__ import annotations

from dataclasses import dataclass

from .pages import HistoryWindow, window_conditions
from ..names import TASK_HISTORY_PARENT


@dataclass(frozen=True, slots=True)
class HistoryStatusAggregate:
    """Terminal counts by status and kind over the window."""

    window: HistoryWindow


def history_status_aggregate_statement(
    query: HistoryStatusAggregate,
) -> tuple[str, dict[str, object]]:
    """Render the status/kind count statement and its bind parameters."""
    conditions, parameters = window_conditions(query.window)
    sql = (
        'SELECT status, terminalization_kind, count(*) AS terminal_count '
        f'FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)} '
        'GROUP BY status, terminalization_kind '
        'ORDER BY status, terminalization_kind'
    )
    return sql, parameters


@dataclass(frozen=True, slots=True)
class HistoryScope:
    """The monitoring filter set, expressed in history columns.

    Multi-value dimensions AND together and OR within one, mirroring
    the live scope exactly: `error_codes` is its own AND-dimension
    (an include-list), and the taxonomy is a second, independent
    AND-dimension whose arms OR together — each `category_families`
    entry is one built-in family's code list, and
    `domain_complement`, when not None, is the domain arm (a
    non-empty code outside the given built-in set). The primitive
    carries code lists, never the taxonomy itself.
    """

    statuses: tuple[str, ...] = ()
    task_names: tuple[str, ...] = ()
    queue_names: tuple[str, ...] = ()
    workers: tuple[str, ...] = ()
    error_codes: tuple[str, ...] = ()
    category_families: tuple[tuple[str, ...], ...] = ()
    domain_complement: tuple[str, ...] | None = None
    retried_only: bool = False


def history_scope_conditions(
    window: HistoryWindow,
    scope: HistoryScope,
) -> tuple[list[str], dict[str, object]]:
    """Window plus filters, as bind-only conditions."""
    conditions, parameters = window_conditions(window)
    for column, values in (
        ('status', scope.statuses),
        ('task_name', scope.task_names),
        ('queue_name', scope.queue_names),
        ('last_claimed_worker_id', scope.workers),
    ):
        if values:
            conditions.append(
                f'{column} = ANY(CAST(:{column}_filter AS text[]))'
            )
            parameters[f'{column}_filter'] = list(values)
    if scope.error_codes:
        conditions.append(
            'error_code = ANY(CAST(:error_code_filter AS text[]))'
        )
        parameters['error_code_filter'] = list(scope.error_codes)
    category_arms: list[str] = []
    for index, family in enumerate(scope.category_families):
        category_arms.append(
            f'error_code = ANY(CAST(:family_{index}_filter AS text[]))'
        )
        parameters[f'family_{index}_filter'] = list(family)
    if scope.domain_complement is not None:
        category_arms.append(
            "(error_code IS NOT NULL AND error_code <> '' "
            'AND error_code <> ALL('
            'CAST(:builtin_code_filter AS text[])))'
        )
        parameters['builtin_code_filter'] = list(scope.domain_complement)
    if category_arms:
        conditions.append('(' + ' OR '.join(category_arms) + ')')
    if scope.retried_only:
        conditions.append('retry_count > 0')
    return conditions, parameters


def history_scoped_status_counts_statement(
    window: HistoryWindow,
    scope: HistoryScope,
) -> tuple[str, dict[str, object]]:
    """Terminal counts by status under the full monitoring scope."""
    conditions, parameters = history_scope_conditions(window, scope)
    sql = (
        'SELECT status, count(*) AS terminal_count '
        f'FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)} '
        'GROUP BY status ORDER BY status'
    )
    return sql, parameters


BREAKDOWN_GROUP_COLUMNS: tuple[str, ...] = (
    'task_name',
    'queue_name',
    'last_claimed_worker_id',
)


def history_breakdown_statement(
    window: HistoryWindow,
    scope: HistoryScope,
    *,
    group_column: str,
) -> tuple[str, dict[str, object]]:
    """Per-group terminal status counts and retried totals."""
    if group_column not in BREAKDOWN_GROUP_COLUMNS:
        raise ValueError(
            f'unknown breakdown group column: {group_column!r}'
        )
    conditions, parameters = history_scope_conditions(window, scope)
    sql = (
        f"SELECT COALESCE({group_column}, 'unknown') AS group_value, "
        'status, count(*) AS status_count, '
        'count(*) FILTER (WHERE retry_count > 0) AS retried_count '
        f'FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)} '
        f'GROUP BY COALESCE({group_column}, \'unknown\'), status'
    )
    return sql, parameters


def history_count_statement(
    window: HistoryWindow,
    scope: HistoryScope,
) -> tuple[str, dict[str, object]]:
    """The scoped terminal-row count for list pagination totals."""
    conditions, parameters = history_scope_conditions(window, scope)
    sql = (
        f'SELECT count(*) FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)}'
    )
    return sql, parameters
