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
    'created_at',
    'retry_count',
    'max_retries',
    'error_code',
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


class HistorySort(Enum):
    """Orderings the page surface offers."""

    TERMINAL_AT_DESC = 'terminal_at DESC'
    TERMINAL_AT_ASC = 'terminal_at ASC'


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
    """One filtered, sorted, bounded page over the window."""

    window: HistoryWindow
    limit: int
    offset: int = 0
    status: str | None = None
    task_name: str | None = None
    queue_name: str | None = None
    sort: HistorySort = HistorySort.TERMINAL_AT_DESC

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= 500:
            raise ValueError('page limit must be between 1 and 500')
        if self.offset < 0:
            raise ValueError('page offset must be non-negative')


@dataclass(frozen=True, slots=True)
class HistoryFacetQuery:
    """Value counts for one facet over the window."""

    window: HistoryWindow
    facet: HistoryFacet
    limit: int = 50

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= 200:
            raise ValueError('facet limit must be between 1 and 200')


def history_page_statement(
    query: HistoryPageQuery,
) -> tuple[str, dict[str, object]]:
    """Render the page statement and its bind parameters."""
    conditions, parameters = window_conditions(query.window)
    for column, value in (
        ('status', query.status),
        ('task_name', query.task_name),
        ('queue_name', query.queue_name),
    ):
        if value is not None:
            conditions.append(f'{column} = :{column}')
            parameters[column] = value
    parameters['limit'] = query.limit
    parameters['offset'] = query.offset
    sql = (
        f'SELECT {", ".join(HISTORY_SUMMARY_COLUMNS)} '
        f'FROM {TASK_HISTORY_PARENT} '
        f'WHERE {" AND ".join(conditions)} '
        f'ORDER BY {query.sort.value} '
        'LIMIT :limit OFFSET :offset'
    )
    return sql, parameters


def history_facet_statement(
    query: HistoryFacetQuery,
) -> tuple[str, dict[str, object]]:
    """Render the facet-count statement and its bind parameters."""
    conditions, parameters = window_conditions(query.window)
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
