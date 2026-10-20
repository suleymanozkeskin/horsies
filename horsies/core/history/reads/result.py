"""Task-result read primitive over the staged result function.

One statement resolves a task to its live result columns, its history
result columns, or absence. All probes of the function run in one
snapshot, so a task that moves to history between two probes cannot be
missed.

An absence is final only for a current function. A publisher of an
earlier release can add a leaf without rendering this function, and the
function then does not probe that leaf until the next coverage pass. A
caller that must not report a false absence confirms it through the
detail reader, which every release renders.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..errors import HistoryContractError
from ..names import TASK_RESULT_FUNCTION

_READ_TASK_RESULT_SQL = text(
    'SELECT location, task_name, status, live_result, '
    'result_payload, result_digest '
    f'FROM {TASK_RESULT_FUNCTION}(CAST(:task_id AS uuid))'
)


@dataclass(frozen=True, slots=True)
class LiveTaskResult:
    """The task is in the live table."""

    task_name: str
    status: str
    result: str | None


@dataclass(frozen=True, slots=True)
class HistoryResultAbsent:
    """The history row stores no result payload."""


@dataclass(frozen=True, slots=True)
class HistoryResultStored:
    """The stored result payload and its digest, not yet verified."""

    payload: bytes
    digest: bytes | None


type HistoryResultPayload = HistoryResultAbsent | HistoryResultStored


@dataclass(frozen=True, slots=True)
class HistoryTaskResult:
    """The task is in a history leaf."""

    task_name: str
    status: str
    payload: HistoryResultPayload


@dataclass(frozen=True, slots=True)
class TaskResultAbsent:
    """The function found no row for the task."""


type TaskResultRead = LiveTaskResult | HistoryTaskResult | TaskResultAbsent


async def read_task_result(
    connection: AsyncConnection,
    *,
    task_id: str,
) -> TaskResultRead:
    """Read the result columns of one task in one statement.

    Reads only. The function must exist: check `staged_result_published`
    first. A database error propagates and aborts the caller's transaction.
    A row with an unknown location or without a task name or status raises
    `HistoryContractError`.
    """
    row = (
        await connection.execute(_READ_TASK_RESULT_SQL, {'task_id': task_id})
    ).one_or_none()
    if row is None:
        return TaskResultAbsent()
    task_name = _required_text(row.task_name, 'task_name', task_id)
    status = _required_text(row.status, 'status', task_id)
    match row.location:
        case 'LIVE':
            return LiveTaskResult(
                task_name=task_name,
                status=status,
                result=row.live_result,
            )
        case 'HISTORY':
            return HistoryTaskResult(
                task_name=task_name,
                status=status,
                payload=_history_payload(row),
            )
        case location:
            raise HistoryContractError(
                f'staged result for task {task_id} returned unknown '
                f'location {location!r}'
            )


async def staged_result_published(connection: AsyncConnection) -> bool:
    """Whether the staged result function exists on this database."""
    return bool(
        (
            await connection.execute(
                text('SELECT to_regprocedure(:name) IS NOT NULL'),
                {'name': f'{TASK_RESULT_FUNCTION}(uuid)'},
            )
        ).scalar_one()
    )


def _history_payload(row: Any) -> HistoryResultPayload:
    if row.result_payload is None:
        return HistoryResultAbsent()
    return HistoryResultStored(
        payload=bytes(row.result_payload),
        digest=(
            bytes(row.result_digest) if row.result_digest is not None else None
        ),
    )


def _required_text(value: Any, column: str, task_id: str) -> str:
    if not isinstance(value, str):
        raise HistoryContractError(
            f'staged result for task {task_id} has no {column}'
        )
    return value
