"""Advisory-lock serialization for per-leaf maintenance.

Every mutation of one leaf — create, detach, finalize, drop — serializes on
the same database-computed key derived from the class and the day-truncated
anchor, so two maintenance processes cannot interleave DDL on one leaf. The
key function is DDL installed with the catalog; computing the key in the
database keeps every process, whatever its Python version or hash seed,
agreeing on the same 64-bit value.

Transaction-scoped locks serve operations that run inside a caller-owned
transaction. Session-scoped locks serve the concurrent-detach path, which
must run on an autocommit connection where no transaction outlives the
statement; releasing verifies ownership because an unlock that returns
false means this code's lock discipline is broken somewhere.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import is_safe_identifier
from ..errors import LeafLockNotHeld
from ..names import LEAF_LOCK_KEY_FUNCTION


def _validate(class_key: str, anchor: datetime) -> None:
    if not class_key:
        raise ValueError('class key must be non-empty')
    if anchor.tzinfo is None:
        raise ValueError('history anchor must be timezone-aware')


class LeafLockAttempt(Enum):
    """Result of one nonblocking advisory or relation lock request."""

    ACQUIRED = 'ACQUIRED'
    BUSY = 'BUSY'


def _sqlstate(error: DBAPIError) -> str | None:
    original = getattr(error, 'orig', error)
    value = getattr(original, 'sqlstate', None)
    if isinstance(value, str):
        return value
    value = getattr(original, 'pgcode', None)
    return value if isinstance(value, str) else None


def is_lock_not_available(error: DBAPIError) -> bool:
    """Return true for PostgreSQL's lock-not-available SQLSTATE."""
    return _sqlstate(error) == '55P03'


async def try_lock_leaf_for_transaction(
    connection: AsyncConnection,
    *,
    class_key: str,
    anchor: datetime,
) -> LeafLockAttempt:
    """Try to hold the leaf lock until the transaction ends."""
    _validate(class_key, anchor)
    acquired = (
        await connection.execute(
            text(
                f"""
                SELECT pg_try_advisory_xact_lock(
                    {LEAF_LOCK_KEY_FUNCTION}(:class_key, :anchor)
                )
                """
            ),
            {'class_key': class_key, 'anchor': anchor},
        )
    ).scalar_one()
    return LeafLockAttempt.ACQUIRED if acquired else LeafLockAttempt.BUSY


async def try_lock_leaf_for_session(
    connection: AsyncConnection,
    *,
    class_key: str,
    anchor: datetime,
) -> LeafLockAttempt:
    """Try to hold the leaf lock until explicitly released."""
    _validate(class_key, anchor)
    acquired = (
        await connection.execute(
            text(
                f"""
                SELECT pg_try_advisory_lock(
                    {LEAF_LOCK_KEY_FUNCTION}(:class_key, :anchor)
                )
                """
            ),
            {'class_key': class_key, 'anchor': anchor},
        )
    ).scalar_one()
    return LeafLockAttempt.ACQUIRED if acquired else LeafLockAttempt.BUSY


async def try_lock_relation_exclusive_for_transaction(
    connection: AsyncConnection,
    relation_name: str,
) -> LeafLockAttempt:
    """Request the relation's DDL lock without waiting.

    PostgreSQL marks the transaction failed after ``LOCK ... NOWAIT`` returns
    SQLSTATE 55P03.  The savepoint contains that failure before it becomes the
    typed busy outcome.
    """
    if not is_safe_identifier(relation_name):
        raise ValueError(f'{relation_name!r} is not a safe relation name')
    try:
        async with connection.begin_nested():
            await connection.execute(
                text(f'LOCK TABLE {relation_name} ' 'IN ACCESS EXCLUSIVE MODE NOWAIT')
            )
    except DBAPIError as error:
        if is_lock_not_available(error):
            return LeafLockAttempt.BUSY
        raise
    return LeafLockAttempt.ACQUIRED


async def unlock_leaf_for_session(
    connection: AsyncConnection,
    *,
    class_key: str,
    anchor: datetime,
) -> None:
    """Release a session-scoped leaf lock this session must own."""
    _validate(class_key, anchor)
    released = (
        await connection.execute(
            text(
                f"""
                SELECT pg_advisory_unlock(
                    {LEAF_LOCK_KEY_FUNCTION}(:class_key, :anchor)
                )
                """
            ),
            {'class_key': class_key, 'anchor': anchor},
        )
    ).scalar_one()
    if not released:
        raise LeafLockNotHeld(
            f'session did not own the maintenance lock for class '
            f'{class_key!r} anchor {anchor.isoformat()}'
        )
