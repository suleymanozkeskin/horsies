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
