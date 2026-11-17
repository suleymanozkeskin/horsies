"""Shared request parsing and failure mapping for the monitoring routes.

Query values that name a column or a status are validated here rather than in
the query layer: an unknown value is a bad request, and the query functions
take types where it is unrepresentable.

A path id is text until ``parse_path_uuid`` makes it a ``uuid.UUID``. Text
that is not a UUID is refused with ``INVALID_INPUT`` before any read.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, assert_never
from uuid import UUID

from fastapi import HTTPException, status
from pydantic import TypeAdapter, ValidationError

from horsies.core.types.status import TaskStatus
from horsies.monitoring import (
    ErrorCategory,
    MonitoringQueryDbOperationFailed,
    MonitoringQueryError,
    MonitoringQueryEventLoopLimit,
    MonitoringQueryHistoryContractViolation,
    TaskGroupBy,
    TaskSortField,
)

_STATUS_BY_VALUE: dict[str, TaskStatus] = {
    member.value: member for member in TaskStatus
}

_CATEGORY_BY_VALUE: dict[str, ErrorCategory] = {
    member.value: member for member in ErrorCategory
}

INVALID_INPUT: Final = 'INVALID_INPUT'
"""The ``code`` of a 400 body for request text refused before any read."""

_UUID_TEXT: Final = TypeAdapter(UUID)
"""pydantic's UUID parser: the canonical form, 32 hex digits, braces, and the
``urn:uuid:`` prefix. The same parser that FastAPI runs for a ``uuid.UUID``
path parameter."""


@dataclass(frozen=True, slots=True)
class PathIdNotUuid:
    """A path id whose text is not a UUID."""

    parameter: str
    reason: str

    @property
    def detail(self) -> str:
        """The response detail: the path parameter and the parse error."""
        return f'Path parameter {self.parameter}: {self.reason}.'


class InvalidInput(Exception):
    """Raised by a route for request text refused before any read.

    The monitoring app answers it with 400 and the body
    ``{"code": "INVALID_INPUT", "detail": ...}``.
    """

    def __init__(self, refused: PathIdNotUuid) -> None:
        super().__init__(refused.detail)
        self.refused = refused


def parse_path_uuid(parameter: str, text: str) -> UUID | PathIdNotUuid:
    """Parse the text of a path id.

    Returns the UUID, or ``PathIdNotUuid`` with pydantic's parse error.
    Reads nothing.
    """
    try:
        return _UUID_TEXT.validate_python(text)
    except ValidationError as error:
        return PathIdNotUuid(
            parameter=parameter,
            reason='; '.join(detail['msg'] for detail in error.errors()),
        )


def require_path_uuid(parameter: str, text: str) -> UUID:
    """Parse the text of a path id for a route.

    Text that is not a UUID raises ``InvalidInput``; the app answers it with
    400 ``INVALID_INPUT``.
    """
    match parse_path_uuid(parameter, text):
        case UUID() as parsed:
            return parsed
        case PathIdNotUuid() as refused:
            raise InvalidInput(refused)


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

    A database failure and the event loop limit are 503: the dependency
    failed, not the request, and the state can clear. History data that
    does not decode is 500: the request was valid, and a repeat gives the
    same answer until the stored row changes.
    """
    detail = f'{surface} query failed: {error.message}'
    match error:
        case MonitoringQueryDbOperationFailed():
            return HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail,
            )
        case MonitoringQueryEventLoopLimit():
            return HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail,
            )
        case MonitoringQueryHistoryContractViolation():
            return HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=detail,
            )
        case _ as unreachable:
            assert_never(unreachable)
