"""The client side of the staged lookup: one statement, typed outcomes.

The client executes the database-owned function in one statement and one
snapshot and decodes the returned composite once. Absence means no retained
authoritative record existed in that snapshot; a row that cannot be decoded
is a contract violation and raises — a lookup must never report a broken
wire row as a missing task.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..errors import HistoryContractError
from ..names import TASK_LOOKUP_FUNCTION


@dataclass(frozen=True, slots=True)
class LiveTaskIdentity:
    """The task is live; the live row is authoritative."""

    task_id: str
    fingerprint_version: int
    command_fingerprint: bytes


@dataclass(frozen=True, slots=True)
class HistoryTaskIdentity:
    """The task is terminal; a retained history row is authoritative."""

    task_id: str
    fingerprint_version: int
    command_fingerprint: bytes


@dataclass(frozen=True, slots=True)
class TaskIdentityAbsent:
    """No retained authoritative record existed in the snapshot."""


type TaskIdentityLookup = (
    LiveTaskIdentity | HistoryTaskIdentity | TaskIdentityAbsent
)


async def lookup_task_identity(
    connection: AsyncConnection,
    task_id: str,
) -> TaskIdentityLookup:
    """Execute the staged lookup as one client statement."""
    row = (
        await connection.execute(
            text(
                f'SELECT found, location, task_id, fingerprint_version, '
                f'command_fingerprint '
                f'FROM {TASK_LOOKUP_FUNCTION}(CAST(:task_id AS uuid))'
            ),
            {'task_id': task_id},
        )
    ).one()
    return decode_lookup_row(
        found=row.found,
        location=row.location,
        found_task_id=row.task_id,
        fingerprint_version=row.fingerprint_version,
        command_fingerprint=row.command_fingerprint,
    )


def decode_lookup_row(
    *,
    found: Any,
    location: Any,
    found_task_id: Any,
    fingerprint_version: Any,
    command_fingerprint: Any,
) -> TaskIdentityLookup:
    if not isinstance(found, bool):
        raise HistoryContractError('lookup row found flag did not decode')
    if not found:
        if location is not None or found_task_id is not None:
            raise HistoryContractError(
                'absent lookup row carried location or identity values'
            )
        return TaskIdentityAbsent()
    if (
        not isinstance(location, str)
        or found_task_id is None
        or not isinstance(fingerprint_version, int)
        or isinstance(fingerprint_version, bool)
        or not isinstance(command_fingerprint, bytes)
    ):
        raise HistoryContractError('found lookup row did not decode')
    normalized_task_id = str(found_task_id)
    match location:
        case 'LIVE':
            return LiveTaskIdentity(
                task_id=normalized_task_id,
                fingerprint_version=fingerprint_version,
                command_fingerprint=command_fingerprint,
            )
        case 'HISTORY':
            return HistoryTaskIdentity(
                task_id=normalized_task_id,
                fingerprint_version=fingerprint_version,
                command_fingerprint=command_fingerprint,
            )
        case _:
            raise HistoryContractError(
                f'lookup row carried unknown location {location!r}'
            )
