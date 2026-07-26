"""Shared request parsing and failure mapping for the monitoring routes.

Query values that name a column or a status are validated here rather than in
the query layer: an unknown value is a bad request, and the query functions
take types where it is unrepresentable.
"""

from __future__ import annotations

from fastapi import HTTPException, status

from horsies.core.types.status import TaskStatus
from horsies.monitoring import (
    ErrorCategory,
    MonitoringQueryError,
    TaskGroupBy,
    TaskSortField,
)

_STATUS_BY_VALUE: dict[str, TaskStatus] = {
    member.value: member for member in TaskStatus
}

_CATEGORY_BY_VALUE: dict[str, ErrorCategory] = {
    member.value: member for member in ErrorCategory
}


def parse_statuses(values: list[str]) -> list[TaskStatus]:
    """Resolve status filter strings, rejecting any the library does not define."""
    parsed: list[TaskStatus] = []
    for value in values:
        member = _STATUS_BY_VALUE.get(value)
        if member is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Unknown status {value!r}.',
            )
        parsed.append(member)
    return parsed


def parse_error_categories(values: list[str]) -> list[ErrorCategory]:
    """Resolve error-category filter strings against the taxonomy.

    The families are the query layer's vocabulary, not the caller's: an
    unrecognised name cannot be expanded to codes, so it is a bad request
    rather than a filter that silently matches nothing.
    """
    parsed: list[ErrorCategory] = []
    for value in values:
        member = _CATEGORY_BY_VALUE.get(value)
        if member is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Unknown error category {value!r}.',
            )
        parsed.append(member)
    return parsed


def parse_sort_by(value: str) -> TaskSortField:
    """Resolve a sort column name against the allowlist."""
    match value:
        case (
            'enqueued_at'
            | 'started_at'
            | 'completed_at'
            | 'failed_at'
            | 'status'
            | 'task_name'
            | 'queue_name'
            | 'priority'
            | 'retry_count'
            | 'queue_s'
            | 'exec_s'
        ):
            return value
        case _:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Unknown sort_by {value!r}.',
            )


def parse_group_by(value: str) -> TaskGroupBy:
    """Resolve a group column name against the allowlist."""
    match value:
        case 'worker' | 'task_name' | 'queue':
            return value
        case _:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Unknown group_by {value!r}.',
            )


def query_failed(surface: str, error: MonitoringQueryError) -> HTTPException:
    """Map a monitoring query failure to its response.

    The database is the dependency that failed, not the request, so this is
    503 rather than 500.
    """
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=f'{surface} query failed: {error.message}',
    )
