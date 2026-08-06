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

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..errors import LeafLockNotHeld
from ..names import LEAF_LOCK_KEY_FUNCTION


def _validate(class_key: str, anchor: datetime) -> None:
    if not class_key:
        raise ValueError('class key must be non-empty')
    if anchor.tzinfo is None:
        raise ValueError('history anchor must be timezone-aware')


async def lock_leaf_for_transaction(
    connection: AsyncConnection,
    *,
    class_key: str,
    anchor: datetime,
) -> None:
    """Hold the leaf's advisory lock until the transaction ends."""
    _validate(class_key, anchor)
    await connection.execute(
        text(
            f"""
            SELECT pg_advisory_xact_lock(
                {LEAF_LOCK_KEY_FUNCTION}(:class_key, :anchor)
            )
            """
        ),
        {'class_key': class_key, 'anchor': anchor},
    )


async def lock_leaf_for_session(
    connection: AsyncConnection,
    *,
    class_key: str,
    anchor: datetime,
) -> None:
    """Hold the leaf's advisory lock until explicitly released."""
    _validate(class_key, anchor)
    await connection.execute(
        text(
            f"""
            SELECT pg_advisory_lock(
                {LEAF_LOCK_KEY_FUNCTION}(:class_key, :anchor)
            )
            """
        ),
        {'class_key': class_key, 'anchor': anchor},
    )


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
