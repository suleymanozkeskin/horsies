"""Maintenance sessions and program locks for the transcode executor.

The transcode claims its maintenance session through the REAL gate
module: one active session row gates the archive (the move's
availability function refuses while it exists), and finishing refuses
while a replacement job is incomplete. The program advisory lock
serializes transcode operations against each other; the gate-row lock
serializes them against gate transitions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..maintenance.gate import (
    ARCHIVE_ACCESS_GATE,
    ARCHIVE_MAINTENANCE_SESSIONS,
)
from .jobs import TRANSCODE_JOBS

_PROGRAM_LOCK_SEED: Final = 7412
_PROGRAM_LOCK_NAME: Final = 'horsies_archive_transcode_program'


@dataclass(frozen=True, slots=True)
class MaintenanceSession:
    """One active archive-maintenance session."""

    session_id: str


class MaintenanceStateError(Exception):
    """A maintenance transition was requested from the wrong state."""


async def lock_transcode_program(connection: AsyncConnection) -> None:
    """Serialize transcode operations; transaction-scoped."""
    await connection.execute(
        text(
            'SELECT pg_advisory_xact_lock('
            'hashtextextended(:name, :seed))'
        ),
        {'name': _PROGRAM_LOCK_NAME, 'seed': _PROGRAM_LOCK_SEED},
    )


async def lock_archive_gate_row(connection: AsyncConnection) -> None:
    """Hold the gate row against concurrent gate transitions."""
    await connection.execute(
        text(
            f'SELECT singleton FROM {ARCHIVE_ACCESS_GATE} '
            'WHERE singleton IS TRUE FOR UPDATE'
        )
    )


async def begin_transcode_maintenance(
    connection: AsyncConnection,
    *,
    session_id: str,
) -> MaintenanceSession:
    """Open the maintenance session the move's gate refuses under."""
    await lock_transcode_program(connection)
    await lock_archive_gate_row(connection)
    active = (
        await connection.execute(
            text(
                f'SELECT session_id FROM {ARCHIVE_MAINTENANCE_SESSIONS} '
                'WHERE ended_at IS NULL'
            )
        )
    ).scalar_one_or_none()
    if active is not None:
        raise MaintenanceStateError('archive maintenance is already active')
    await connection.execute(
        text(
            f'INSERT INTO {ARCHIVE_MAINTENANCE_SESSIONS} '
            '(session_id, started_at) '
            'VALUES (CAST(:session_id AS uuid), statement_timestamp())'
        ),
        {'session_id': session_id},
    )
    return MaintenanceSession(session_id=session_id)


async def finish_transcode_maintenance(
    connection: AsyncConnection,
    *,
    session_id: str,
) -> None:
    """Close the session; refused while a replacement job is unfinished."""
    await lock_transcode_program(connection)
    await lock_archive_gate_row(connection)
    unfinished = (
        await connection.execute(
            text(
                f'SELECT count(*) FROM {TRANSCODE_JOBS} '
                'WHERE maintenance_session_id = CAST(:session_id AS uuid) '
                "AND state <> 'COMPLETE'"
            ),
            {'session_id': session_id},
        )
    ).scalar_one()
    if unfinished:
        raise MaintenanceStateError(
            'archive maintenance has an unfinished replacement job'
        )
    ended = (
        await connection.execute(
            text(
                f'UPDATE {ARCHIVE_MAINTENANCE_SESSIONS} '
                'SET ended_at = statement_timestamp() '
                'WHERE session_id = CAST(:session_id AS uuid) '
                'AND ended_at IS NULL '
                'RETURNING session_id'
            ),
            {'session_id': session_id},
        )
    ).scalar_one_or_none()
    if ended is None:
        raise MaintenanceStateError(
            'archive maintenance session is not active'
        )


async def active_maintenance_session(
    connection: AsyncConnection,
) -> str | None:
    """The active session id, or None."""
    value = (
        await connection.execute(
            text(
                f'SELECT session_id FROM {ARCHIVE_MAINTENANCE_SESSIONS} '
                'WHERE ended_at IS NULL'
            )
        )
    ).scalar_one_or_none()
    return str(value) if value is not None else None
