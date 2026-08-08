"""Time-scoped history aggregates through the partitioned parent.

An aggregate visits no leaf outside its requested anchor range: the
parent's RANGE key appears in every statement, so the planner prunes
the scan to in-range leaves. The claim is proven by an EXPLAIN-based
leaf-visit test, not asserted by wall-clock. Column discipline matches
the listing surfaces: grouping keys and counts only, never envelopes.
"""

from __future__ import annotations

from dataclasses import dataclass

from .pages import (
    HistoryScope,
    HistoryWindow,
    history_scope_conditions,
    window_conditions,
)
from ..names import TASK_HISTORY_PARENT

__all__ = [
    'HistoryScope',
    'HistoryStatusAggregate',
    'history_scope_conditions',
    'history_status_aggregate_statement',
    'history_scoped_status_counts_statement',
    'history_breakdown_statement',
    'history_count_statement',
    'BREAKDOWN_GROUP_COLUMNS',
]


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
