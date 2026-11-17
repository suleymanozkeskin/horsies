"""Return claimed rows that no child started to the queue, in one statement.

``release_claims`` calls ``horsies_release_claims`` for the rows of one
worker. A row goes back to PENDING, with every claim and run field cleared,
only while it is CLAIMED by this worker at the claim generation of its
dispatch; any other row, a row claimed again included, is left as it is.
No attempt row is written. ``ReleasePlacement.ISOLATED`` also writes
``execution = 'ISOLATED'`` and wakes each queue of a released row once, with
the payload ``class``. ``SAME_CLASS`` sends no wake.

The statement runs alone on an autocommit connection: it commits when it
returns, and its row locks end with it. It never waits on an advisory lock.

``release_claims_in_core`` runs the same function through the core
(``CoreHandle.release_claims_async``). The worker calls it; ``release_claims``
stays for the dual-run reference of the worker's callers.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Final, Literal, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies._core import (
    ClaimsReleased,
    CoreHandle,
    NotHeldRow,
    ReleaseClaimsRequest,
    ReleasedRow,
    ReleaseFailed,
)
from horsies.core.lifecycle.outcomes import OutcomeDecodeError

MAX_RELEASE_ROWS: Final = 1000
"""Most rows of one ``horsies_release_claims`` call; the function refuses more
(22023). ``release_claims`` splits a longer list into calls of this size."""

RELEASE_CLAIMS_SQL = text("""
    SELECT ordinality, task_id, outcome
    FROM horsies_release_claims(
        CAST(:task_ids AS uuid[]),
        CAST(:claimed_ats AS TIMESTAMPTZ[]),
        :worker_id,
        :placement
    )
""")


class ReleasePlacement(Enum):
    """The execution class a released row keeps."""

    SAME_CLASS = 'SAME_CLASS'
    """The row keeps its execution class. No wake."""

    ISOLATED = 'ISOLATED'
    """The row gets ``execution = 'ISOLATED'``; each queue of a released row
    gets one ``class`` wake."""


@dataclass(frozen=True, slots=True)
class HeldClaim:
    """A row this worker claimed, at the claim generation of its dispatch."""

    task_id: str
    claimed_at: datetime


class ReleaseOutcome(Enum):
    """What the release did to one row."""

    RELEASED = 'RELEASED'
    """The row was CLAIMED by this worker at this generation; it is PENDING now."""

    NOT_HELD = 'NOT_HELD'
    """The row is absent, or its status, owner or claim generation differs.
    Nothing was written."""


@dataclass(frozen=True, slots=True)
class ClaimRelease:
    """The outcome of one row of a release."""

    claim: HeldClaim
    outcome: ReleaseOutcome


def _decode_outcome(value: object, task_id: str) -> ReleaseOutcome:
    match value:
        case 'RELEASED':
            return ReleaseOutcome.RELEASED
        case 'NOT_HELD':
            return ReleaseOutcome.NOT_HELD
        case _:
            raise OutcomeDecodeError(
                f'horsies_release_claims returned outcome {value!r} for task '
                f'{task_id}; expected RELEASED or NOT_HELD'
            )


def decode_release_rows(
    claims: Sequence[HeldClaim],
    rows: Sequence[Any],
) -> tuple[ClaimRelease, ...]:
    """Pair each returned row with the claim at its position. Pure.

    The function returns one row per input row, ordered by the 1-based
    position, with the input id in canonical text form (the engine's uuid
    loader returns text; claims carry the ids that the claim returned). A row count, position or id outside that
    contract raises ``OutcomeDecodeError``.
    """
    if len(rows) != len(claims):
        raise OutcomeDecodeError(
            f'horsies_release_claims returned {len(rows)} rows for '
            f'{len(claims)} claims'
        )
    decoded: list[ClaimRelease] = []
    for position, (claim, row) in enumerate(zip(claims, rows, strict=True), start=1):
        returned_id = str(row.task_id)
        if row.ordinality != position or returned_id != claim.task_id:
            raise OutcomeDecodeError(
                f'horsies_release_claims returned row {row.ordinality} for task '
                f'{returned_id}; expected row {position} for task {claim.task_id}'
            )
        decoded.append(
            ClaimRelease(
                claim=claim,
                outcome=_decode_outcome(row.outcome, claim.task_id),
            ),
        )
    return tuple(decoded)


def _release_parameters(
    claims: Sequence[HeldClaim],
    worker_id: str,
    placement: ReleasePlacement,
) -> dict[str, object]:
    return {
        'task_ids': [claim.task_id for claim in claims],
        'claimed_ats': [claim.claimed_at for claim in claims],
        'worker_id': worker_id,
        'placement': placement.value,
    }


async def release_claims(
    connection: AsyncConnection,
    *,
    worker_id: str,
    claims: Sequence[HeldClaim],
    placement: ReleasePlacement,
) -> tuple[ClaimRelease, ...]:
    """Release ``claims`` of ``worker_id``; one outcome per claim, in order.

    ``connection`` must be in autocommit: each call of the function commits
    when it returns. A list longer than ``MAX_RELEASE_ROWS`` is sent as
    several calls, each complete alone. No claims sends nothing. The task
    ids must be distinct: the function refuses a repeated id (22023).

    Raises:
        Exception: what the driver raises. The calls sent before the failing
            one committed; the failing call wrote nothing, or committed and
            its reply was lost.
        OutcomeDecodeError: a returned row outside the function's contract.
    """
    released: list[ClaimRelease] = []
    for start in range(0, len(claims), MAX_RELEASE_ROWS):
        chunk = claims[start : start + MAX_RELEASE_ROWS]
        result = await connection.execute(
            RELEASE_CLAIMS_SQL,
            _release_parameters(chunk, worker_id, placement),
        )
        released.extend(decode_release_rows(chunk, result.fetchall()))
    return tuple(released)


def _core_placement(placement: ReleasePlacement) -> Literal['SAME_CLASS', 'ISOLATED']:
    match placement:
        case ReleasePlacement.SAME_CLASS:
            return 'SAME_CLASS'
        case ReleasePlacement.ISOLATED:
            return 'ISOLATED'
        case _ as unreachable:
            assert_never(unreachable)


def _released_row_outcome(
    claim: HeldClaim,
    row: ReleasedRow | NotHeldRow,
) -> ClaimRelease:
    """The outcome of ``claim`` from the core's record at its position. Pure.

    A record of another task id contradicts the operation (one record per
    claim, in the request order): ``OutcomeDecodeError``.
    """
    if row.task_id != uuid.UUID(claim.task_id):
        raise OutcomeDecodeError(
            f'release_claims_async answered for task {row.task_id} at the '
            f'position of task {claim.task_id}'
        )
    match row:
        case ReleasedRow():
            return ClaimRelease(claim=claim, outcome=ReleaseOutcome.RELEASED)
        case NotHeldRow():
            return ClaimRelease(claim=claim, outcome=ReleaseOutcome.NOT_HELD)
        case _ as unreachable:
            assert_never(unreachable)


def _release_request(
    claims: Sequence[HeldClaim],
    worker_id: str,
    placement: ReleasePlacement,
) -> ReleaseClaimsRequest:
    """The core request of one call. A claim id that is not a UUID is a
    defect of the claim that gave it: ``ValueError``."""
    return ReleaseClaimsRequest(
        worker_id=worker_id,
        claims=[(uuid.UUID(claim.task_id), claim.claimed_at) for claim in claims],
        placement=_core_placement(placement),
    )


async def release_claims_in_core(
    handle: CoreHandle,
    *,
    worker_id: str,
    claims: Sequence[HeldClaim],
    placement: ReleasePlacement,
) -> tuple[ClaimRelease, ...]:
    """Release ``claims`` of ``worker_id`` through the core; one outcome per
    claim, in order.

    Each call of at most ``MAX_RELEASE_ROWS`` claims is one
    ``horsies_release_claims`` statement that commits alone. No claims sends
    nothing. The task ids must be distinct (the request refuses a repeated
    id with ``ValueError``).

    Raises:
        CoreFailure: a ``ReleaseFailed`` record raises its ``exception``
            (the core's class of the cause, with its retry rule). The calls
            sent before it committed; the failed call wrote nothing, or
            committed and its reply was lost.
        CoreError: the core of this process stopped or did not start.
        OutcomeDecodeError: a record count or task id outside the
            operation's contract.
    """
    released: list[ClaimRelease] = []
    for start in range(0, len(claims), MAX_RELEASE_ROWS):
        chunk = claims[start : start + MAX_RELEASE_ROWS]
        request = _release_request(chunk, worker_id, placement)
        match await handle.release_claims_async(request):
            case ClaimsReleased(rows=rows):
                if len(rows) != len(chunk):
                    raise OutcomeDecodeError(
                        f'release_claims_async answered {len(rows)} records for '
                        f'{len(chunk)} claims'
                    )
                released.extend(
                    _released_row_outcome(claim, row)
                    for claim, row in zip(chunk, rows, strict=True)
                )
            case ReleaseFailed(exception=exception):
                raise exception
            case _ as unreachable:
                assert_never(unreachable)
    return tuple(released)
