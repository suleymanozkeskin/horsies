"""Key-reservation operations: the registry side of keyed enqueue.

Three database-owned functions over the reservation registry and nothing
else — no statement here touches live or history storage. Keyed enqueue
(a later build wave) calls the claim function inside the same transaction
as its live-task insert; terminalization calls the terminalize function
inside the terminal transaction; maintenance calls cleanup on its own
schedule. Keeping the registry mutations behind this seam is what lets
the reservation contract be exercised and reviewed before the live table
carries its cutover columns.

Contract, as ratified: a live reservation has no expiry; terminalization
starts the enqueue-snapshotted window at `terminal_at`; an expired
terminal reservation is reusable without mutating the old request's
identity; replay requires the same fingerprint, anything else within an
active window is a typed conflict. Cleanup deletes only expired terminal
rows, in bounded batches, and never touches live reservations.

The terminalize function addresses the row by key digest — the primary
key — and verifies task ownership in the predicate, returning a typed
miss instead of updating another request's reservation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..errors import HistoryContractError
from ...utils.db import uuid_column_value

KEY_RESERVATION_OUTCOME_TYPE = 'horsies_key_reservation_outcome'
KEY_RESERVATION_CLAIM_FUNCTION = 'horsies_key_reservation_claim'
KEY_RESERVATION_TERMINALIZE_FUNCTION = 'horsies_key_reservation_terminalize'
KEY_RESERVATION_CLEANUP_FUNCTION = 'horsies_key_reservation_cleanup'


KEY_RESERVATION_TERMINALIZE_BATCH_FUNCTION = (
    'horsies_key_reservation_terminalize_batch'
)


# ---------------------------------------------------------------------------
# Typed outcomes and client wrappers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReservationApplied:
    """The key is now reserved by this request."""

    task_id: UUID


@dataclass(frozen=True, slots=True)
class ReservationReplay:
    """An active reservation with the same fingerprint owns the key."""

    task_id: UUID


@dataclass(frozen=True, slots=True)
class ReservationConflict:
    """An active reservation with a different fingerprint owns the key."""

    task_id: UUID
    observed_fingerprint_version: int


type ReservationClaim = (
    ReservationApplied | ReservationReplay | ReservationConflict
)


async def claim_key_reservation(
    connection: AsyncConnection,
    *,
    key_digest: bytes,
    key_scope_version: int,
    reservation_window_seconds: int,
    fingerprint_version: int,
    fingerprint: bytes,
    task_id: UUID,
) -> ReservationClaim:
    """Claim, replay, or conflict — one statement in the caller's transaction."""
    row = (
        await connection.execute(
            text(
                f'SELECT outcome, task_id, observed_fingerprint_version '
                f'FROM {KEY_RESERVATION_CLAIM_FUNCTION}('
                f':key_digest, CAST(:key_scope_version AS smallint), '
                f"make_interval(secs => :window_seconds), "
                f'CAST(:fingerprint_version AS smallint), :fingerprint, '
                f'CAST(:task_id AS uuid))'
            ),
            {
                'key_digest': key_digest,
                'key_scope_version': key_scope_version,
                'window_seconds': reservation_window_seconds,
                'fingerprint_version': fingerprint_version,
                'fingerprint': fingerprint,
                'task_id': str(task_id),
            },
        )
    ).one()
    return decode_reservation_row(
        outcome=row.outcome,
        row_task_id=row.task_id,
        observed_fingerprint_version=row.observed_fingerprint_version,
    )


async def terminalize_key_reservation(
    connection: AsyncConnection,
    *,
    key_digest: bytes,
    task_id: str,
) -> bool:
    """Start the reservation's terminal window; False means no live row matched.

    This wrapper stamps the window from database statement time. The
    terminalization bodies do not call it: they invoke the SQL function
    inline with the transaction's own `terminal_at`, so the reservation
    window and the history row anchor come from the same instant.
    """
    updated = (
        await connection.execute(
            text(
                f'SELECT {KEY_RESERVATION_TERMINALIZE_FUNCTION}('
                f':key_digest, CAST(:task_id AS uuid), statement_timestamp())'
            ),
            {'key_digest': key_digest, 'task_id': task_id},
        )
    ).scalar_one()
    if not isinstance(updated, bool):
        raise HistoryContractError('reservation terminalize did not return boolean')
    return updated


async def cleanup_expired_reservations(
    connection: AsyncConnection,
    *,
    batch_size: int,
) -> int:
    """Delete at most one batch of expired terminal reservations."""
    if batch_size <= 0:
        raise ValueError('cleanup batch size must be positive')
    deleted = (
        await connection.execute(
            text(f'SELECT {KEY_RESERVATION_CLEANUP_FUNCTION}(:batch_size)'),
            {'batch_size': batch_size},
        )
    ).scalar_one()
    if isinstance(deleted, bool) or not isinstance(deleted, int):
        raise HistoryContractError('reservation cleanup did not return a count')
    return deleted


def decode_reservation_row(
    *,
    outcome: Any,
    row_task_id: Any,
    observed_fingerprint_version: Any,
) -> ReservationClaim:
    """Decode one claim outcome row, failing closed on contract breaks."""
    if not isinstance(outcome, str) or row_task_id is None:
        raise HistoryContractError('reservation outcome row did not decode')
    task_id = uuid_column_value(row_task_id, column='task_id')
    match outcome:
        case 'APPLIED':
            if observed_fingerprint_version is not None:
                raise HistoryContractError(
                    'applied reservation carried an observed fingerprint'
                )
            return ReservationApplied(task_id=task_id)
        case 'REPLAY':
            return ReservationReplay(task_id=task_id)
        case 'CONFLICT':
            if (
                isinstance(observed_fingerprint_version, bool)
                or not isinstance(observed_fingerprint_version, int)
            ):
                raise HistoryContractError(
                    'conflict outcome lacked the observed fingerprint version'
                )
            return ReservationConflict(
                task_id=task_id,
                observed_fingerprint_version=observed_fingerprint_version,
            )
        case _:
            raise HistoryContractError(
                f'unknown reservation outcome {outcome!r}'
            )
