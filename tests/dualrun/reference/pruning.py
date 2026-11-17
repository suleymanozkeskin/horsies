"""The pruning pass, before the 0006 pruning functions.

``prune_expired_partitions`` (``python/horsies/core/history/maintenance/pruning.py``)
and the manager operations it called (``partitions/manager.py``:
``detach_expired_leaf``, ``finalize_interrupted_detach``,
``drop_detached_leaf`` under ``RETENTION``) sent, per pass:

- three candidate reads, each bounded by ``PruneBounds.candidates`` plus one:
  every expired finite leaf (the finalize phase), the expired heartbeat leaves
  and the expired period leaves;
- one Python inspection (``inspect_leaf``) per finalize candidate;
- per detach or finalize, on one autocommit connection: an inspection, the
  session leaf try-lock, the inspection again, the session timeouts, the
  ``DETACH PARTITION ... CONCURRENTLY | FINALIZE``, the catalog UPDATE of
  ``detached_at``, the reader freshness check and, when stale, the
  publication, a final inspection, and the restore of the timeouts and the
  unlock;
- per drop, in the caller's transaction: an inspection, the transaction leaf
  try-lock, the inspection again, the manifest check, the relation lock
  NOWAIT, ``DROP TABLE``, the delete of the clock exceptions and the catalog
  UPDATE.

The pass returns the production report types, so the two sides compare as
values. The inspection (``inspect_leaf``) and the lock helpers are the
production functions; they are unchanged by the switch.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Final

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    DETACH_STATEMENT_TIMEOUT_MS,
    InspectHistoryLeaf,
    LeafLifecycle,
    LeafRef,
    PruneBounds,
)
from horsies.core.history.heartbeats.partitioning import HeartbeatLeafSwept
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.history.maintenance.pruning import HistoryLeafSwept, PrunePass
from horsies.core.history.names import (
    CLOCK_SKEW_EXCEPTIONS,
    HEARTBEAT_CLASS_KEY,
    LEAF_CATALOG,
    RETENTION_CLASSES,
)
from horsies.core.history.outcomes import (
    DropRefusedLoaderReferences,
    LeafDetachable,
    LeafDetached,
    LeafDetachInterrupted,
    LeafDrop,
    LeafDropped,
    LeafInspection,
    LeafMaintenanceBusy,
    LeafSweep,
    PruneReach,
    counts_toward_prune_bound,
)
from horsies.core.history.partitions.catalog import decode_leaf_bounds, read_retention_class
from horsies.core.history.partitions.locks import (
    LeafLockAttempt,
    is_lock_not_available,
    try_lock_leaf_for_session,
    try_lock_leaf_for_transaction,
    try_lock_relation_exclusive_for_transaction,
    unlock_leaf_for_session,
)
from horsies.core.history.partitions.manager import inspect_leaf
from horsies.core.history.partitions.publication import LoaderPublication
from horsies.core.history.terminalization.duplicate_guard import DELETE_LEAF_EXCEPTIONS_SQL

EXPIRED_HISTORY_LEAVES_SQL: Final = f"""
SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
FROM {LEAF_CATALOG} AS c
JOIN {RETENTION_CLASSES} AS r ON r.class_key = c.class_key
WHERE r.class_kind = 'PERIOD'
  AND c.lower_bound_kind = 'AT'
  AND c.dropped_at IS NULL
  AND c.upper_anchor + r.retention_days * interval '1 day'
      <= statement_timestamp()
ORDER BY c.lower_anchor
LIMIT :limit
"""

EXPIRED_FINITE_LEAVES_SQL: Final = f"""
SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
FROM {LEAF_CATALOG} AS c
JOIN {RETENTION_CLASSES} AS r ON r.class_key = c.class_key
WHERE r.class_kind IN ('PERIOD', 'HEARTBEAT')
  AND c.lower_bound_kind = 'AT'
  AND c.dropped_at IS NULL
  AND c.upper_anchor
      + COALESCE(
          r.retention_days * interval '1 day',
          r.heartbeat_horizon_hours * interval '1 hour'
      )
      <= statement_timestamp()
ORDER BY c.lower_anchor
LIMIT :limit
"""

EXPIRED_HEARTBEAT_LEAVES_SQL: Final = f"""
SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
FROM {LEAF_CATALOG} AS c
JOIN {RETENTION_CLASSES} AS r
  ON r.class_key = c.class_key
WHERE c.class_key = '{HEARTBEAT_CLASS_KEY}'
  AND c.lower_bound_kind = 'AT'
  AND c.dropped_at IS NULL
  AND c.upper_anchor
      + r.heartbeat_horizon_hours * interval '1 hour'
      <= statement_timestamp()
ORDER BY c.lower_anchor
LIMIT :limit
"""


# ---- Manager operations under RETENTION ----


@dataclass(frozen=True, slots=True)
class _SessionTimeouts:
    statement_timeout: str
    lock_timeout: str


async def _read_session_timeouts(connection: AsyncConnection) -> _SessionTimeouts:
    return _SessionTimeouts(
        statement_timeout=(await connection.execute(text('SHOW statement_timeout'))).scalar_one(),
        lock_timeout=(await connection.execute(text('SHOW lock_timeout'))).scalar_one(),
    )


async def _set_session_timeouts(
    connection: AsyncConnection, *, statement_timeout_ms: int | None
) -> None:
    if statement_timeout_ms is not None:
        await connection.execute(
            text("SELECT set_config('statement_timeout', :value, false)"),
            {'value': f'{statement_timeout_ms}ms'},
        )
    await connection.execute(text("SELECT set_config('lock_timeout', '2000ms', false)"))


async def _cleanup_session_leaf_lock(
    connection: AsyncConnection,
    *,
    leaf: LeafRef,
    prior_timeouts: _SessionTimeouts | None,
    uncertain: bool,
) -> None:
    if uncertain:
        await connection.invalidate()
        return
    try:
        await connection.rollback()
        if prior_timeouts is not None:
            await connection.execute(
                text("SELECT set_config('statement_timeout', :value, false)"),
                {'value': prior_timeouts.statement_timeout},
            )
            await connection.execute(
                text("SELECT set_config('lock_timeout', :value, false)"),
                {'value': prior_timeouts.lock_timeout},
            )
        await unlock_leaf_for_session(
            connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
        )
    except BaseException as error:
        await connection.invalidate(error)
        raise


async def _try_session_leaf_lock(connection: AsyncConnection, leaf: LeafRef) -> LeafLockAttempt:
    try:
        return await try_lock_leaf_for_session(
            connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
        )
    except BaseException as error:
        await connection.invalidate(error)
        raise


async def _record_detached(connection: AsyncConnection, leaf_name: str) -> None:
    await connection.execute(
        text(
            f"""
            UPDATE {LEAF_CATALOG}
            SET detached_at = COALESCE(detached_at, statement_timestamp())
            WHERE leaf_name = :leaf_name
            """
        ),
        {'leaf_name': leaf_name},
    )


async def _republish_if_stale(connection: AsyncConnection, publisher: LoaderPublication) -> None:
    if not await publisher.is_current(connection):
        await publisher.republish(connection)


def _retention(leaf: LeafRef) -> InspectHistoryLeaf:
    return InspectHistoryLeaf(leaf=leaf, lifecycle=LeafLifecycle.RETENTION)


async def detach_expired_leaf_before(
    database: PartitionMaintenanceDatabase,
    leaf: LeafRef,
    publisher: LoaderPublication,
) -> LeafInspection | LeafMaintenanceBusy:
    """``detach_expired_leaf`` with the pass's statement timeout."""
    async with database.autocommit() as connection:
        inspection = await inspect_leaf(connection, _retention(leaf))
        match inspection:
            case LeafDetachable():
                pass
            case _:
                return inspection
        lock_acquired = False
        prior_timeouts: _SessionTimeouts | None = None
        cancelled = False
        try:
            if await _try_session_leaf_lock(connection, leaf) is LeafLockAttempt.BUSY:
                return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
            lock_acquired = True
            inspection = await inspect_leaf(connection, _retention(leaf))
            match inspection:
                case LeafDetachable():
                    pass
                case _:
                    return inspection
            prior_timeouts = await _read_session_timeouts(connection)
            await _set_session_timeouts(
                connection, statement_timeout_ms=DETACH_STATEMENT_TIMEOUT_MS
            )
            retention_class = await read_retention_class(connection, leaf.class_key)
            assert retention_class is not None and retention_class.finite_parent_name is not None
            try:
                await connection.execute(
                    text(
                        f'ALTER TABLE {retention_class.finite_parent_name} '
                        f'DETACH PARTITION {leaf.leaf_name} CONCURRENTLY'
                    )
                )
            except DBAPIError as error:
                if is_lock_not_available(error):
                    return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
                raise
            await _record_detached(connection, leaf.leaf_name)
            await _republish_if_stale(connection, publisher)
            return await inspect_leaf(connection, _retention(leaf))
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if lock_acquired:
                await _cleanup_session_leaf_lock(
                    connection, leaf=leaf, prior_timeouts=prior_timeouts, uncertain=cancelled
                )


async def finalize_interrupted_detach_before(
    database: PartitionMaintenanceDatabase,
    leaf: LeafRef,
    publisher: LoaderPublication,
) -> LeafInspection | LeafMaintenanceBusy:
    """``finalize_interrupted_detach`` under ``RETENTION`` with the pass's statement timeout."""
    async with database.autocommit() as connection:
        inspection = await inspect_leaf(connection, _retention(leaf))
        match inspection:
            case LeafDetached() | LeafDetachInterrupted():
                pass
            case _:
                return inspection
        lock_acquired = False
        prior_timeouts: _SessionTimeouts | None = None
        cancelled = False
        try:
            if await _try_session_leaf_lock(connection, leaf) is LeafLockAttempt.BUSY:
                return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
            lock_acquired = True
            inspection = await inspect_leaf(connection, _retention(leaf))
            match inspection:
                case LeafDetached():
                    await _record_detached(connection, leaf.leaf_name)
                    await _republish_if_stale(connection, publisher)
                    return await inspect_leaf(connection, _retention(leaf))
                case LeafDetachInterrupted():
                    pass
                case _:
                    return inspection
            prior_timeouts = await _read_session_timeouts(connection)
            await _set_session_timeouts(
                connection, statement_timeout_ms=DETACH_STATEMENT_TIMEOUT_MS
            )
            retention_class = await read_retention_class(connection, leaf.class_key)
            assert retention_class is not None and retention_class.finite_parent_name is not None
            try:
                await connection.execute(
                    text(
                        f'ALTER TABLE {retention_class.finite_parent_name} '
                        f'DETACH PARTITION {leaf.leaf_name} FINALIZE'
                    )
                )
            except DBAPIError as error:
                if is_lock_not_available(error):
                    return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
                raise
            await _record_detached(connection, leaf.leaf_name)
            await _republish_if_stale(connection, publisher)
            return await inspect_leaf(connection, _retention(leaf))
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if lock_acquired:
                await _cleanup_session_leaf_lock(
                    connection, leaf=leaf, prior_timeouts=prior_timeouts, uncertain=cancelled
                )


async def _delete_leaf_clock_exceptions(connection: AsyncConnection, leaf: LeafRef) -> None:
    installed = (
        await connection.execute(
            text('SELECT to_regclass(:name) IS NOT NULL'), {'name': CLOCK_SKEW_EXCEPTIONS}
        )
    ).scalar_one()
    if not installed:
        return
    await connection.execute(
        text(DELETE_LEAF_EXCEPTIONS_SQL),
        {'class_key': leaf.class_key, 'lower': leaf.bounds.lower, 'upper': leaf.bounds.upper},
    )


async def drop_detached_leaf_before(
    connection: AsyncConnection,
    leaf: LeafRef,
    publisher: LoaderPublication,
) -> LeafDrop:
    """``drop_detached_leaf`` under ``RETENTION``."""
    inspection = await inspect_leaf(connection, _retention(leaf))
    match inspection:
        case LeafDetached():
            pass
        case _:
            return inspection
    lock_attempt = await try_lock_leaf_for_transaction(
        connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
    )
    if lock_attempt is LeafLockAttempt.BUSY:
        return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    inspection = await inspect_leaf(connection, _retention(leaf))
    match inspection:
        case LeafDetached():
            pass
        case _:
            return inspection
    if await publisher.references_leaf(connection, leaf.leaf_name):
        return DropRefusedLoaderReferences(leaf_name=leaf.leaf_name)
    relation_lock = await try_lock_relation_exclusive_for_transaction(connection, leaf.leaf_name)
    if relation_lock is LeafLockAttempt.BUSY:
        return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    await connection.execute(text(f'DROP TABLE {leaf.leaf_name}'))
    await _delete_leaf_clock_exceptions(connection, leaf)
    await connection.execute(
        text(
            f"""
            UPDATE {LEAF_CATALOG}
            SET detached_at = COALESCE(detached_at, statement_timestamp()),
                dropped_at = statement_timestamp()
            WHERE leaf_name = :leaf_name
            """
        ),
        {'leaf_name': leaf.leaf_name},
    )
    return LeafDropped(leaf_name=leaf.leaf_name)


# ---- The pass ----


@dataclass(frozen=True, slots=True)
class _Candidates:
    leaves: tuple[LeafRef, ...]
    reach: PruneReach


async def _expired_candidates(
    connection: AsyncConnection, statement: str, bounds: PruneBounds
) -> _Candidates:
    rows = (await connection.execute(text(statement), {'limit': bounds.candidates + 1})).all()
    return _Candidates(
        leaves=tuple(
            LeafRef(
                leaf_name=row.leaf_name,
                class_key=row.class_key,
                bounds=decode_leaf_bounds(row.lower_anchor, row.upper_anchor),
            )
            for row in rows[: bounds.candidates]
        ),
        reach=(
            PruneReach.TRUNCATED if len(rows) > bounds.candidates else PruneReach.WITHIN_BOUNDS
        ),
    )


@dataclass(frozen=True, slots=True)
class _Finalized:
    leaves: tuple[str, ...]
    refusals: tuple[str, ...]
    errors: tuple[str, ...]
    reach: PruneReach


async def _finalize_interrupted_detaches(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    bounds: PruneBounds,
) -> _Finalized:
    async with database.connect() as connection:
        candidates = await _expired_candidates(connection, EXPIRED_FINITE_LEAVES_SQL, bounds)
        interrupted: list[LeafRef] = []
        for ref in candidates.leaves:
            match await inspect_leaf(connection, _retention(ref)):
                case LeafDetachInterrupted():
                    interrupted.append(ref)
                case _:
                    pass
    finalized: list[str] = []
    refusals: list[str] = []
    errors: list[str] = []
    for ref in interrupted:
        try:
            outcome = await finalize_interrupted_detach_before(database, ref, publisher)
        except SQLAlchemyError as error:
            errors.append(f'finalize {ref.leaf_name}: {error}')
            continue
        match outcome:
            case LeafDetached():
                finalized.append(ref.leaf_name)
            case _:
                refusals.append(f'finalize {ref.leaf_name}: {type(outcome).__name__}')
    return _Finalized(
        leaves=tuple(finalized),
        refusals=tuple(refusals),
        errors=tuple(errors),
        reach=candidates.reach,
    )


@dataclass(frozen=True, slots=True)
class _LeafSwept:
    detach: LeafInspection | LeafMaintenanceBusy
    drop: LeafDrop | None


async def _sweep_leaf(
    database: PartitionMaintenanceDatabase, publisher: LoaderPublication, ref: LeafRef
) -> _LeafSwept:
    detach = await detach_expired_leaf_before(database, ref, publisher)
    drop: LeafDrop | None = None
    detached = isinstance(detach, LeafDetached)
    if not detached:
        async with database.connect() as connection:
            detached = isinstance(await inspect_leaf(connection, _retention(ref)), LeafDetached)
    if detached:
        async with database.begin() as connection:
            drop = await drop_detached_leaf_before(connection, ref, publisher)
    return _LeafSwept(detach=detach, drop=drop)


async def _sweep(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    statement: str,
    bounds: PruneBounds,
) -> LeafSweep[tuple[LeafRef, _LeafSwept]]:
    async with database.connect() as connection:
        candidates = await _expired_candidates(connection, statement, bounds)
    swept: list[tuple[LeafRef, _LeafSwept]] = []
    errors: list[str] = []
    acted = 0
    reach = candidates.reach
    for ref in candidates.leaves:
        if acted >= bounds.acted_leaves:
            reach = PruneReach.TRUNCATED
            break
        try:
            entry = await _sweep_leaf(database, publisher, ref)
        except SQLAlchemyError as error:
            acted += 1
            errors.append(f'{ref.leaf_name}: {error}')
            continue
        acted += int(counts_toward_prune_bound(entry.detach, entry.drop))
        swept.append((ref, entry))
    return LeafSweep(swept=tuple(swept), errors=tuple(errors), acted=acted, reach=reach)


def _sweep_refusals(entries: tuple[tuple[str, LeafInspection | LeafMaintenanceBusy, LeafDrop | None], ...]) -> tuple[str, ...]:
    refusals: list[str] = []
    for leaf_name, detach, drop in entries:
        match drop:
            case LeafDropped():
                continue
            case None:
                refusals.append(f'{leaf_name}: {type(detach).__name__}')
            case _:
                refusals.append(f'{leaf_name}: {type(drop).__name__}')
    return tuple(refusals)


async def prune_expired_partitions_before(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    bounds: PruneBounds,
) -> PrunePass:
    """The pass: finalize phase, heartbeat sweep, history sweep."""
    finalized = await _finalize_interrupted_detaches(database, publisher, bounds)
    errors: list[str] = list(finalized.errors)
    heartbeat_swept: tuple[HeartbeatLeafSwept, ...] = ()
    heartbeat_acted = 0
    heartbeat_reach = PruneReach.WITHIN_BOUNDS
    try:
        heartbeat = await _sweep(database, publisher, EXPIRED_HEARTBEAT_LEAVES_SQL, bounds)
    except SQLAlchemyError as error:
        errors.append(f'heartbeat sweep: {error}')
    else:
        heartbeat_swept = tuple(
            HeartbeatLeafSwept(leaf_name=ref.leaf_name, detach=entry.detach, drop=entry.drop)
            for ref, entry in heartbeat.swept
        )
        heartbeat_acted = heartbeat.acted
        heartbeat_reach = heartbeat.reach
        errors.extend(f'heartbeat {error}' for error in heartbeat.errors)
    history = await _sweep(
        database,
        publisher,
        EXPIRED_HISTORY_LEAVES_SQL,
        replace(bounds, acted_leaves=bounds.acted_leaves - heartbeat_acted),
    )
    history_swept = tuple(
        HistoryLeafSwept(
            leaf_name=ref.leaf_name, class_key=ref.class_key, detach=entry.detach, drop=entry.drop
        )
        for ref, entry in history.swept
    )
    errors.extend(history.errors)
    match finalized.reach, heartbeat_reach, history.reach:
        case PruneReach.WITHIN_BOUNDS, PruneReach.WITHIN_BOUNDS, PruneReach.WITHIN_BOUNDS:
            reach = PruneReach.WITHIN_BOUNDS
        case _:
            reach = PruneReach.TRUNCATED
    return PrunePass(
        finalized_leaves=finalized.leaves,
        heartbeat_swept=heartbeat_swept,
        history_swept=history_swept,
        refusals=(
            *finalized.refusals,
            *_sweep_refusals(
                (
                    *((entry.leaf_name, entry.detach, entry.drop) for entry in heartbeat_swept),
                    *((entry.leaf_name, entry.detach, entry.drop) for entry in history_swept),
                )
            ),
        ),
        errors=tuple(errors),
        reach=reach,
    )
