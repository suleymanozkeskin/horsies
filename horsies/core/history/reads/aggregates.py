"""Time-scoped history aggregates through the partitioned parent.

An aggregate visits no leaf outside its requested anchor range: the
parent's RANGE key appears in every statement, so the planner prunes
the scan to in-range leaves. The claim is proven by an EXPLAIN-based
leaf-visit test, not asserted by wall-clock. Column discipline matches
the listing surfaces: grouping keys and counts only, never envelopes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .pages import (
    HistoryScope,
    HistoryWindow,
    history_scope_conditions,
    window_conditions,
)
from ..errors import HistoryContractError
from ..names import TASK_HISTORY_PARENT

__all__ = [
    'HistoryScope',
    'HistoryStatusAggregate',
    'history_scope_conditions',
    'history_status_aggregate_statement',
    'history_scoped_status_counts_statement',
    'history_breakdown_statement',
    'history_count_statement',
    'history_estimate_statement',
    'plan_rows_from_explain',
    'BREAKDOWN_GROUP_COLUMNS',
    'HISTORY_NONEMPTY_PROBE_SQL',
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


HISTORY_NONEMPTY_PROBE_SQL = (
    f'SELECT EXISTS (SELECT 1 FROM {TASK_HISTORY_PARENT})'
)
"""First-row-bounded emptiness probe guarding the planner estimate.

The planner clamps every scanned relation to at least one row, and a
truncated leaf's ``reltuples`` is version-dependent (PG 18 resets it,
PG 16 keeps the stale value), so an estimate over a provably empty
parent is clamp noise, not information — the history sibling of the
live arm's ``reltuples = -1`` rule. The probe stops at the first live
tuple; it never scans past one."""


def history_estimate_statement(
    window: HistoryWindow,
    scope: HistoryScope,
) -> tuple[str, dict[str, object]]:
    """The planner-estimate companion to ``history_count_statement``.

    Same predicate, EXPLAIN instead of execution: the top plan node's
    row estimate serves the unfiltered pagination total, whose
    documented contract is a planner estimate. ``reltuples`` cannot
    serve here — it is a whole-relation figure and the history side is
    always window-scoped — so the estimate is the planner's own, over
    exactly the conditions the exact count would run.
    """
    conditions, parameters = history_scope_conditions(window, scope)
    sql = (
        'EXPLAIN (FORMAT JSON) '
        f'SELECT 1 FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)}'
    )
    return sql, parameters


def plan_rows_from_explain(payload: object) -> int:
    """The top plan node's row estimate from EXPLAIN (FORMAT JSON) output.

    Decodes fail-closed: any shape other than the documented one — a
    single-element array whose ``Plan`` object carries a numeric
    ``Plan Rows`` — raises ``HistoryContractError`` rather than flowing
    onward as a total. Accepts the payload already parsed (json column
    decoding) or as rendered text, the two forms drivers deliver.
    """
    match payload:
        case str() as rendered:
            try:
                parsed: object = json.loads(rendered)
            except json.JSONDecodeError as error:
                raise HistoryContractError(
                    'EXPLAIN payload is not valid JSON'
                ) from error
            return plan_rows_from_explain(parsed)
        case [{'Plan': {'Plan Rows': bool()}}, *_]:
            raise HistoryContractError(
                'EXPLAIN plan row estimate decoded as boolean'
            )
        case [{'Plan': {'Plan Rows': int() as rows}}, *_]:
            return rows
        case [{'Plan': {'Plan Rows': float() as rows}}, *_]:
            return int(rows)
        case _:
            raise HistoryContractError(
                'EXPLAIN payload did not carry a top plan row estimate'
            )
