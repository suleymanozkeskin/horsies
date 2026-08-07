"""History pages and facets: anchor-scoped, column-disciplined.

Listing surfaces enumerate their columns and never carry result,
rerun-input, or attempt envelopes — only the typed detail command
fetches those. Every statement is time-scoped by the anchor range
through the partitioned parent: the LIST -> RANGE layout prunes the
scan to in-range leaves across classes, which is exactly the budget's
allowed visit set.

These are statement builders returning (sql, parameters); execution
stays with the caller so composition into the monitoring layer at
cutover owns its own session and error handling.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Final

from ..names import TASK_HISTORY_PARENT


HISTORY_SUMMARY_COLUMNS: Final = (
    'task_id',
    'task_name',
    'queue_name',
    'priority',
    'status',
    'terminalization_kind',
    'terminal_at',
    'retention_class_key',
    'enqueued_at',
    'started_at',
    'created_at',
    'retry_count',
    'max_retries',
    'error_code',
    'last_claimed_worker_id',
    'last_worker_hostname',
    'workflow_id',
    'is_workflow_task',
)
"""The complete column set any listing statement may carry. Envelope
columns are deliberately absent; the pin proves it stays that way."""


class HistoryFacet(Enum):
    """Facetable columns; the enum is the whole surface."""

    TASK_NAME = 'task_name'
    QUEUE_NAME = 'queue_name'
    STATUS = 'status'
    TERMINALIZATION_KIND = 'terminalization_kind'
    ERROR_CODE = 'error_code'
    RETENTION_CLASS_KEY = 'retention_class_key'
    WORKER = 'last_claimed_worker_id'


class HistorySort(Enum):
    """Orderings the page surface offers."""

    TERMINAL_AT_DESC = 'terminal_at DESC'
    TERMINAL_AT_ASC = 'terminal_at ASC'


_SORT_FIELD_EXPRESSIONS: Final[dict[str, str]] = {
    'enqueued_at': 'enqueued_at',
    'started_at': 'started_at',
    'completed_at': (
        "CASE WHEN status = 'COMPLETED' THEN terminal_at END"
    ),
    'failed_at': (
        "CASE WHEN status <> 'COMPLETED' THEN terminal_at END"
    ),
    'status': 'status',
    'task_name': 'task_name',
    'queue_name': 'queue_name',
    'priority': 'priority',
    'retry_count': 'retry_count',
    'queue_s': '(started_at - enqueued_at)',
    'exec_s': '(terminal_at - started_at)',
}


def history_sort_expression(field: str, *, descending: bool) -> str:
    """The history-side ORDER BY for one allowlisted list-sort field.

    Field names are the monitoring allowlist's own; an unknown field
    raises rather than falling back, so a new sort key must take a
    position here before it can be offered. The terminal instant
    stands in for the status-conditional end stamps exactly as the
    summary mapping presents them.
    """
    expression = _SORT_FIELD_EXPRESSIONS.get(field)
    if expression is None:
        raise ValueError(f'unknown history sort field: {field!r}')
    return f'{expression} {"DESC" if descending else "ASC"}'


@dataclass(frozen=True, slots=True)
class HistoryWindow:
    """Half-open `[lower, upper)` anchor range every statement scopes to."""

    lower: datetime
    upper: datetime

    def __post_init__(self) -> None:
        if self.lower.tzinfo is None or self.upper.tzinfo is None:
            raise ValueError('history window bounds must be timezone-aware')
        if self.lower >= self.upper:
            raise ValueError('history window bounds must be increasing')


@dataclass(frozen=True, slots=True)
class HistoryPageQuery:
    """One filtered, sorted, bounded page over the window.

    Multi-value filters AND together across dimensions and OR within
    one; an empty tuple means the dimension is unfiltered. `order_by`
    overrides `sort` with an allowlisted field expression rendered by
    `history_sort_expression` — the monitoring list rides it so both
    lifecycle sides sort by the same key before the merge.
    """

    window: HistoryWindow
    limit: int
    offset: int = 0
    statuses: tuple[str, ...] = ()
    task_names: tuple[str, ...] = ()
    queue_names: tuple[str, ...] = ()
    workers: tuple[str, ...] = ()
    error_codes: tuple[str, ...] = ()
    retried_only: bool = False
    sort: HistorySort = HistorySort.TERMINAL_AT_DESC
    order_by: str | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= 500:
            raise ValueError('page limit must be between 1 and 500')
        if self.offset < 0:
            raise ValueError('page offset must be non-negative')


@dataclass(frozen=True, slots=True)
class HistoryFacetQuery:
    """Value counts for one facet over the window.

    Coarse scoping only (statuses, retried) — matching the
    monitoring facet discipline where narrowing one dimension never
    empties the others' option lists.
    """

    window: HistoryWindow
    facet: HistoryFacet
    limit: int = 50
    statuses: tuple[str, ...] = ()
    retried_only: bool = False

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= 200:
            raise ValueError('facet limit must be between 1 and 200')


def _filter_conditions(
    query: HistoryPageQuery,
) -> tuple[list[str], dict[str, object]]:
    conditions, parameters = window_conditions(query.window)
    for column, values in (
        ('status', query.statuses),
        ('task_name', query.task_names),
        ('queue_name', query.queue_names),
        ('last_claimed_worker_id', query.workers),
        ('error_code', query.error_codes),
    ):
        if values:
            conditions.append(
                f'{column} = ANY(CAST(:{column}_filter AS text[]))'
            )
            parameters[f'{column}_filter'] = list(values)
    if query.retried_only:
        conditions.append('retry_count > 0')
    return conditions, parameters


def history_page_statement(
    query: HistoryPageQuery,
) -> tuple[str, dict[str, object]]:
    """Render the page statement and its bind parameters."""
    conditions, parameters = _filter_conditions(query)
    parameters['limit'] = query.limit
    parameters['offset'] = query.offset
    order = (
        query.order_by if query.order_by is not None else query.sort.value
    )
    sql = (
        f'SELECT {", ".join(HISTORY_SUMMARY_COLUMNS)} '
        f'FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)} '
        f'ORDER BY {order} '
        'LIMIT :limit OFFSET :offset'
    )
    return sql, parameters


def history_facet_statement(
    query: HistoryFacetQuery,
) -> tuple[str, dict[str, object]]:
    """Render the facet-count statement and its bind parameters."""
    conditions, parameters = window_conditions(query.window)
    if query.statuses:
        conditions.append(
            'status = ANY(CAST(:status_filter AS text[]))'
        )
        parameters['status_filter'] = list(query.statuses)
    if query.retried_only:
        conditions.append('retry_count > 0')
    conditions.append(f'{query.facet.value} IS NOT NULL')
    parameters['limit'] = query.limit
    column = query.facet.value
    sql = (
        f'SELECT {column} AS facet_value, count(*) AS facet_count '
        f'FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)} '
        f'GROUP BY {column} '
        'ORDER BY facet_count DESC, facet_value '
        'LIMIT :limit'
    )
    return sql, parameters


def window_conditions(
    window: HistoryWindow,
) -> tuple[list[str], dict[str, object]]:
    return (
        [
            'retention_anchor_at >= :window_lower',
            'retention_anchor_at < :window_upper',
        ],
        {'window_lower': window.lower, 'window_upper': window.upper},
    )
