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
from uuid import uuid4

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


class IndexRelationState(Enum):
    """Ownership of one schema relation name at one catalog read."""

    ABSENT = 'ABSENT'
    ATTACHED = 'ATTACHED'
    FOREIGN = 'FOREIGN'


class IndexRemovalOutcome(Enum):
    """Result of the locked index removal step."""

    REMOVED = 'REMOVED'
    ABSENT = 'ABSENT'
    FOREIGN = 'FOREIGN'
    BUSY = 'BUSY'


class _IndexOwnerChanged(Exception):
    pass


_INDEX_RELATION_LOCK_TIMEOUT_MS = 2_000


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


async def try_lock_coverage_for_transaction(
    connection: AsyncConnection,
) -> LeafLockAttempt:
    """Try to serialize one damaged complete-coverage pass."""
    acquired = (
        await connection.execute(
            text(
                """
                SELECT pg_try_advisory_xact_lock(
                    hashtextextended('horsies:partition-coverage:v1', 1601)
                )
                """
            )
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


async def read_index_relation_state(
    connection: AsyncConnection,
    *,
    leaf_name: str,
    index_name: str,
) -> IndexRelationState:
    """Resolve whether `index_name` belongs to `leaf_name`."""
    for identifier in (leaf_name, index_name):
        if not is_safe_identifier(identifier):
            raise ValueError(f'{identifier!r} is not a safe relation name')
    state = (
        await connection.execute(
            text(
                """
                SELECT CASE
                    WHEN to_regclass(:index_name) IS NULL THEN 'ABSENT'
                    WHEN EXISTS (
                        SELECT 1
                        FROM pg_index
                        WHERE indexrelid = to_regclass(:index_name)
                          AND indrelid = to_regclass(:leaf_name)
                    ) THEN 'ATTACHED'
                    ELSE 'FOREIGN'
                END
                """
            ),
            {'leaf_name': leaf_name, 'index_name': index_name},
        )
    ).scalar_one()
    match str(state):
        case 'ABSENT':
            return IndexRelationState.ABSENT
        case 'ATTACHED':
            return IndexRelationState.ATTACHED
        case 'FOREIGN':
            return IndexRelationState.FOREIGN
        case unknown:
            raise AssertionError(f'unknown index relation state {unknown!r}')


async def remove_attached_index_for_repair(
    connection: AsyncConnection,
    *,
    leaf_name: str,
    index_name: str,
) -> IndexRemovalOutcome:
    """Lock, validate, and remove one malformed canonical index.

    PostgreSQL does not permit ``LOCK TABLE`` on an index. ``ALTER INDEX
    ... RENAME`` atomically resolves the canonical name and takes the index
    relation lock. The private name keeps that exact relation locked while
    ownership is checked again and while ``DROP INDEX`` removes it.

    The savepoint restores the canonical name when the locked relation is
    foreign. This closes a concurrent rename and name-reuse race without
    changing an unrelated relation.
    """
    for identifier in (leaf_name, index_name):
        if not is_safe_identifier(identifier):
            raise ValueError(f'{identifier!r} is not a safe relation name')
    temporary_name = f'horsies_index_repair_{uuid4().hex}'
    prior_lock_timeout = (
        await connection.execute(text("SELECT current_setting('lock_timeout')"))
    ).scalar_one()
    await connection.execute(
        text("SELECT set_config('lock_timeout', :value, true)"),
        {'value': f'{_INDEX_RELATION_LOCK_TIMEOUT_MS}ms'},
    )
    try:
        try:
            async with connection.begin_nested():
                await connection.execute(
                    text(f'ALTER INDEX {index_name} RENAME TO {temporary_name}')
                )
                state = await read_index_relation_state(
                    connection,
                    leaf_name=leaf_name,
                    index_name=temporary_name,
                )
                if state is not IndexRelationState.ATTACHED:
                    raise _IndexOwnerChanged
                await connection.execute(text(f'DROP INDEX {temporary_name}'))
        except _IndexOwnerChanged:
            return IndexRemovalOutcome.FOREIGN
        except DBAPIError as error:
            if is_lock_not_available(error):
                return IndexRemovalOutcome.BUSY
            if _sqlstate(error) not in {'42P01', '42809'}:
                raise
            state = await read_index_relation_state(
                connection,
                leaf_name=leaf_name,
                index_name=index_name,
            )
            match state:
                case IndexRelationState.ABSENT:
                    return IndexRemovalOutcome.ABSENT
                case IndexRelationState.ATTACHED | IndexRelationState.FOREIGN:
                    return IndexRemovalOutcome.FOREIGN
        return IndexRemovalOutcome.REMOVED
    finally:
        await connection.execute(
            text("SELECT set_config('lock_timeout', :value, true)"),
            {'value': str(prior_lock_timeout)},
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
