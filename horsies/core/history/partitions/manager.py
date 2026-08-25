"""Partition lifecycle operations: create-ahead, detach, finalize, drop.

Every operation takes a typed command, classifies the leaf from catalog plus
`pg_catalog` under the leaf's advisory lock, and returns a typed outcome.
Refusals are outcomes; only contract violations raise.

Transactional operations (create, coverage, drop) run on a caller-owned
connection and commit with the caller. Detach and finalize manage their own
dedicated autocommit connection because `DETACH PARTITION ... CONCURRENTLY`
refuses to run inside a transaction block; they serialize on session-scoped
advisory locks instead of transaction-scoped ones.

Loader publication is part of leaf lifecycle, not an afterthought: creation
republishes the staged lookup function in the same transaction that attaches
the leaf, retirement republishes after detach, and drop is refused while the
published function still references the relation. The invariant is that no
snapshot exists in which a retained row is invisible to the loader and no
dropped relation remains probed by it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from ..maintenance.database import PartitionMaintenanceDatabase

from ..commands import (
    CreateDailyHistoryLeaf,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    EnsureLeafCoverage,
    FinalizeInterruptedLeafDetach,
    InspectHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from ..errors import HistoryParentAbsent
from ..names import (
    LEAF_CATALOG,
    TASK_HISTORY_FOREVER,
    WORKFLOW_PHASE2_PENDING,
)
from ..outcomes import (
    CatalogConflictKind,
    ClassIntervalMismatch,
    DropRefusedLoaderReferences,
    ForeverClassLeaf,
    LeafAlreadyConformant,
    LeafAttachment,
    LeafCatalogConflict,
    LeafCreated,
    LeafCreation,
    LeafDetachable,
    LeafDetached,
    LeafDetachInterrupted,
    LeafDrop,
    LeafDropped,
    LeafIndexRepaired,
    LeafInspection,
    LeafMaintenanceBusy,
    LeafMissing,
    LeafNotExpired,
    LeafPendingBlocked,
    RetentionClassAbsent,
)
from .catalog import (
    INDEX_SCHEMA_VERSION,
    LeafCatalogRow,
    LeafPhysicalState,
    RetentionClassRow,
    capture_partition_bound_utc,
    daily_leaf_name,
    database_now,
    leaf_enqueued_index_name,
    leaf_id_index_name,
    read_leaf_catalog_row,
    read_leaf_ordering_index_exists,
    read_leaf_physical_state,
    read_retention_class,
)
from .locks import (
    LeafLockAttempt,
    is_lock_not_available,
    try_lock_leaf_for_session,
    try_lock_leaf_for_transaction,
    try_lock_relation_exclusive_for_transaction,
    unlock_leaf_for_session,
)
from .publication import LoaderPublication
from ..phase2.quarantine import (
    QuarantineLeafBlockers,
    QuarantineRefused,
    quarantine_over_horizon_blockers,
)

_DAILY = timedelta(days=1)


@dataclass(frozen=True, slots=True)
class _DailyLeafCreationState:
    catalog: LeafCatalogRow | None
    physical: LeafPhysicalState
    ordering_index_exists: bool
    outcome: LeafCreation | None


def _history_class_parent(
    class_key: str, retention_class: RetentionClassRow
) -> str | None:
    """Return the RANGE parent for a history class.

    Finite classes persist their generated parent name. ``forever`` is the
    one reserved unbounded-retention class; its metadata deliberately has no
    duration, while its physical LIST child is a RANGE parent from schema
    v35 onward.
    """
    if class_key == 'forever' and retention_class.duration is None:
        return TASK_HISTORY_FOREVER
    return retention_class.finite_parent_name


async def inspect_leaf(
    connection: AsyncConnection,
    command: InspectHistoryLeaf,
) -> LeafInspection:
    """Classify one leaf without changing anything."""
    leaf = command.leaf
    retention_class = await read_retention_class(connection, leaf.class_key)
    match retention_class:
        case None:
            return RetentionClassAbsent(class_key=leaf.class_key)
        case RetentionClassRow(duration=None):
            return ForeverClassLeaf(class_key=leaf.class_key)
        case RetentionClassRow():
            pass
    duration = retention_class.duration
    parent_name = retention_class.finite_parent_name
    if duration is None or parent_name is None:
        raise AssertionError('finite class invariant enforced by the catalog reader')

    catalog = await read_leaf_catalog_row(connection, leaf.leaf_name)
    physical = await read_leaf_physical_state(
        connection,
        leaf_name=leaf.leaf_name,
        parent_name=parent_name,
        id_index_name=(
            catalog.id_index_name
            if catalog is not None
            else leaf_id_index_name(leaf.leaf_name)
        ),
    )
    expires_at = leaf.bounds.upper + duration

    if catalog is None:
        if physical.relation_exists:
            return LeafCatalogConflict(
                leaf_name=leaf.leaf_name,
                kind=CatalogConflictKind.RELATION_WITHOUT_CATALOG,
                detail='relation exists but the leaf catalog has no row for it',
            )
        return LeafMissing(
            leaf_name=leaf.leaf_name, cataloged=False, expires_at=expires_at
        )

    if (
        catalog.parent_name != parent_name
        or catalog.class_key != leaf.class_key
        or catalog.lower_anchor != leaf.bounds.lower
        or catalog.upper_anchor != leaf.bounds.upper
    ):
        return LeafCatalogConflict(
            leaf_name=leaf.leaf_name,
            kind=CatalogConflictKind.METADATA_MISMATCH,
            detail='catalog row disagrees with the requested class or bounds',
        )

    if not physical.relation_exists:
        if catalog.dropped_at is not None:
            return LeafDropped(leaf_name=leaf.leaf_name)
        return LeafMissing(
            leaf_name=leaf.leaf_name, cataloged=True, expires_at=expires_at
        )
    if not physical.parent_exists:
        raise HistoryParentAbsent(
            f'finite history parent {parent_name!r} does not exist'
        )

    if physical.detach_pending is not None and (
        physical.partition_bound != catalog.partition_bound
        or not physical.id_index_exists
    ):
        return LeafCatalogConflict(
            leaf_name=leaf.leaf_name,
            kind=CatalogConflictKind.PHYSICAL_NONCONFORMANT,
            detail='attached leaf bound or task-ID index disagrees with catalog',
        )

    blocker_count = await _pending_blocker_count(connection, leaf)
    if blocker_count:
        match physical.detach_pending:
            case None:
                attachment = LeafAttachment.DETACHED
            case True:
                attachment = LeafAttachment.DETACH_INTERRUPTED
            case False:
                attachment = LeafAttachment.ATTACHED
        return LeafPendingBlocked(
            leaf_name=leaf.leaf_name,
            blocker_count=blocker_count,
            expires_at=expires_at,
            attachment=attachment,
        )

    match physical.detach_pending:
        case None:
            return LeafDetached(leaf_name=leaf.leaf_name, expires_at=expires_at)
        case True:
            return LeafDetachInterrupted(
                leaf_name=leaf.leaf_name, expires_at=expires_at
            )
        case False:
            pass

    now = await database_now(connection)
    if expires_at <= now:
        return LeafDetachable(leaf_name=leaf.leaf_name, expires_at=expires_at)
    return LeafNotExpired(leaf_name=leaf.leaf_name, expires_at=expires_at)


async def create_daily_leaf(
    connection: AsyncConnection,
    command: CreateDailyHistoryLeaf,
    publisher: LoaderPublication,
) -> LeafCreation:
    """Create or verify one daily leaf inside the caller's transaction.

    On creation the staged loader is republished in the same transaction, so
    the leaf and the function that can read it become visible in one commit.
    """
    leaf = command.leaf
    retention_class = await read_retention_class(connection, leaf.class_key)
    match retention_class:
        case None:
            return RetentionClassAbsent(class_key=leaf.class_key)
        case RetentionClassRow(
            duration=None,
            partition_interval=None,
        ) if leaf.class_key == 'forever':
            pass
        case RetentionClassRow(partition_interval=interval) if interval != _DAILY:
            return ClassIntervalMismatch(
                class_key=leaf.class_key,
                partition_interval_days=(
                    interval.days if interval is not None else None
                ),
            )
        case RetentionClassRow():
            pass
    parent_name = _history_class_parent(leaf.class_key, retention_class)
    if parent_name is None:
        return ForeverClassLeaf(class_key=leaf.class_key)

    state = await _read_daily_leaf_creation_state(
        connection, leaf=leaf, parent_name=parent_name
    )
    if state.outcome is not None:
        return state.outcome

    lock_attempt = await try_lock_leaf_for_transaction(
        connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
    )
    if lock_attempt is LeafLockAttempt.BUSY:
        return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)

    state = await _read_daily_leaf_creation_state(
        connection, leaf=leaf, parent_name=parent_name
    )
    if state.outcome is not None:
        return state.outcome

    catalog = state.catalog
    relation_name = leaf.leaf_name if catalog is not None else parent_name
    relation_lock = await try_lock_relation_exclusive_for_transaction(
        connection, relation_name
    )
    if relation_lock is LeafLockAttempt.BUSY:
        return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)

    if catalog is not None:
        if not state.physical.id_index_exists:
            await connection.execute(
                text(
                    f'CREATE INDEX {catalog.id_index_name} '
                    f'ON {leaf.leaf_name} (task_id)'
                )
            )
        if not state.ordering_index_exists:
            await connection.execute(
                text(
                    f'CREATE INDEX '
                    f'{leaf_enqueued_index_name(leaf.leaf_name)} '
                    f'ON {leaf.leaf_name} (enqueued_at)'
                )
            )
        await connection.execute(text(f'ANALYZE {leaf.leaf_name}'))
        return LeafIndexRepaired(
            leaf_name=leaf.leaf_name, id_index_name=catalog.id_index_name
        )

    id_index_name = leaf_id_index_name(leaf.leaf_name)
    await connection.execute(
        text(
            f"""
            CREATE TABLE {leaf.leaf_name}
                PARTITION OF {parent_name}
                FOR VALUES FROM ('{leaf.bounds.lower.isoformat()}')
                    TO ('{leaf.bounds.upper.isoformat()}')
            """
        )
    )
    # Captured under the UTC rendering convention; every later
    # comparison captures the live bound the same way.
    recorded_bound = await capture_partition_bound_utc(connection, leaf.leaf_name)
    await connection.execute(
        text(
            f"""
            INSERT INTO {LEAF_CATALOG} (
                leaf_name, parent_name, class_key,
                lower_anchor, upper_anchor,
                index_schema_version, id_index_name, partition_bound,
                min_birth_at, min_birth_verified, created_at
            ) VALUES (
                :leaf_name, :parent_name, :class_key,
                :lower, :upper,
                :index_schema_version, :id_index_name, :partition_bound,
                NULL, TRUE, statement_timestamp()
            )
            """
        ),
        {
            'leaf_name': leaf.leaf_name,
            'parent_name': parent_name,
            'class_key': leaf.class_key,
            'lower': leaf.bounds.lower,
            'upper': leaf.bounds.upper,
            'index_schema_version': INDEX_SCHEMA_VERSION,
            'id_index_name': id_index_name,
            'partition_bound': recorded_bound,
        },
    )
    await connection.execute(
        text(f'CREATE INDEX {id_index_name} ON {leaf.leaf_name} (task_id)')
    )
    await connection.execute(
        text(
            f'CREATE INDEX {leaf_enqueued_index_name(leaf.leaf_name)} '
            f'ON {leaf.leaf_name} (enqueued_at)'
        )
    )
    await connection.execute(text(f'ANALYZE {leaf.leaf_name}'))
    await publisher.republish(connection)
    return LeafCreated(leaf_name=leaf.leaf_name, id_index_name=id_index_name)


async def _read_daily_leaf_creation_state(
    connection: AsyncConnection,
    *,
    leaf: LeafRef,
    parent_name: str,
) -> _DailyLeafCreationState:
    catalog = await read_leaf_catalog_row(connection, leaf.leaf_name)
    physical = await read_leaf_physical_state(
        connection,
        leaf_name=leaf.leaf_name,
        parent_name=parent_name,
        id_index_name=(
            catalog.id_index_name
            if catalog is not None
            else leaf_id_index_name(leaf.leaf_name)
        ),
    )
    if not physical.parent_exists:
        raise HistoryParentAbsent(f'history parent {parent_name!r} does not exist')
    if physical.relation_exists != (catalog is not None):
        return _DailyLeafCreationState(
            catalog=catalog,
            physical=physical,
            ordering_index_exists=False,
            outcome=LeafCatalogConflict(
                leaf_name=leaf.leaf_name,
                kind=CatalogConflictKind.RELATION_WITHOUT_CATALOG,
                detail=(
                    'relation exists without a catalog row'
                    if physical.relation_exists
                    else 'catalog row exists without a relation'
                ),
            ),
        )
    if catalog is None:
        return _DailyLeafCreationState(
            catalog=None,
            physical=physical,
            ordering_index_exists=False,
            outcome=None,
        )
    if (
        catalog.parent_name != parent_name
        or catalog.class_key != leaf.class_key
        or catalog.lower_anchor != leaf.bounds.lower
        or catalog.upper_anchor != leaf.bounds.upper
        or catalog.index_schema_version != INDEX_SCHEMA_VERSION
        or catalog.dropped_at is not None
    ):
        outcome: LeafCreation | None = LeafCatalogConflict(
            leaf_name=leaf.leaf_name,
            kind=CatalogConflictKind.METADATA_MISMATCH,
            detail='existing leaf metadata differs from the request',
        )
        return _DailyLeafCreationState(catalog, physical, False, outcome)
    if physical.partition_bound != catalog.partition_bound:
        outcome = LeafCatalogConflict(
            leaf_name=leaf.leaf_name,
            kind=CatalogConflictKind.PHYSICAL_NONCONFORMANT,
            detail='attached leaf partition bound differs from catalog',
        )
        return _DailyLeafCreationState(catalog, physical, False, outcome)
    ordering_index_exists = await read_leaf_ordering_index_exists(
        connection, leaf.leaf_name
    )
    outcome = (
        LeafAlreadyConformant(leaf_name=leaf.leaf_name)
        if physical.id_index_exists and ordering_index_exists
        else None
    )
    return _DailyLeafCreationState(catalog, physical, ordering_index_exists, outcome)


async def ensure_leaf_coverage(
    connection: AsyncConnection,
    command: EnsureLeafCoverage,
    publisher: LoaderPublication,
) -> tuple[LeafCreation, ...]:
    """Create every missing daily leaf from today through the horizon.

    Returns one outcome per required interval, oldest first. A refusal stops
    the pass: coverage with a hole in the middle is not coverage, and the
    caller must see the refusal rather than a partially satisfied horizon.
    """
    retention_class = await read_retention_class(connection, command.class_key)
    match retention_class:
        case None:
            return (RetentionClassAbsent(class_key=command.class_key),)
        case RetentionClassRow(
            duration=None,
            partition_interval=None,
        ) if command.class_key == 'forever':
            pass
        case RetentionClassRow(partition_interval=interval) if interval != _DAILY:
            return (
                ClassIntervalMismatch(
                    class_key=command.class_key,
                    partition_interval_days=(
                        interval.days if interval is not None else None
                    ),
                ),
            )
        case RetentionClassRow():
            pass
    parent_name = _history_class_parent(command.class_key, retention_class)
    if parent_name is None:
        return (ForeverClassLeaf(class_key=command.class_key),)

    now = await database_now(connection)
    today_lower = now.replace(hour=0, minute=0, second=0, microsecond=0)
    outcomes: list[LeafCreation] = []
    for day_offset in range(command.horizon_days + 1):
        lower = today_lower + timedelta(days=day_offset)
        bounds = LeafBounds(lower=lower, upper=lower + _DAILY)
        leaf = LeafRef(
            leaf_name=daily_leaf_name(parent_name, lower),
            class_key=command.class_key,
            bounds=bounds,
        )
        outcome = await create_daily_leaf(
            connection, CreateDailyHistoryLeaf(leaf=leaf), publisher
        )
        outcomes.append(outcome)
        match outcome:
            case LeafCreated() | LeafAlreadyConformant() | LeafIndexRepaired():
                continue
            case _:
                break
    return tuple(outcomes)


async def detach_expired_leaf(
    database: PartitionMaintenanceDatabase,
    command: DetachExpiredHistoryLeaf,
    publisher: LoaderPublication,
) -> LeafInspection | LeafMaintenanceBusy | QuarantineRefused:
    """Concurrently detach one leaf classified `LeafDetachable`.

    Any other classification is returned unchanged and nothing is detached,
    with one exception: an expired leaf blocked only by pending locators
    older than the command's quarantine horizon first runs the quarantine
    protocol on the same session-lock connection — each repoint is one
    committed statement, so every locator leaves the leaf durably before
    the re-inspection that clears it for detach. A quarantine refusal is
    returned as-is and the leaf stays pinned; a leaf that remains blocked
    (under-horizon locators, or drained-but-still-present blockers) is
    returned re-inspected. The quarantine pass inherits the
    bounded-relations invariant per statement — inherently per-leaf.

    After a successful detach the staged loader is republished without the
    leaf; the relation still exists until drop, so a reader holding the old
    function sees a standalone table, never a missing one.
    """
    leaf = command.leaf
    async with database.autocommit() as connection:
        inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
        match inspection:
            case LeafDetachable():
                pass
            case LeafPendingBlocked(
                attachment=LeafAttachment.ATTACHED
            ) if command.quarantine_horizon is not None:
                pass
            case _:
                return inspection

        lock_acquired = False
        prior_timeouts: _SessionTimeouts | None = None
        cancelled = False
        try:
            lock_attempt = await _try_session_leaf_lock(connection, leaf=leaf)
            if lock_attempt is LeafLockAttempt.BUSY:
                return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
            lock_acquired = True
            inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
            match inspection:
                case LeafDetachable():
                    pass
                case LeafPendingBlocked(
                    attachment=LeafAttachment.ATTACHED
                ) if command.quarantine_horizon is not None:
                    now = await database_now(connection)
                    if inspection.expires_at > now:
                        # Blockers on an unexpired leaf are in-flight
                        # drain traffic; quarantine exists to unpin
                        # detach, not to empty a live leaf early.
                        return inspection
                    quarantined = await quarantine_over_horizon_blockers(
                        connection,
                        QuarantineLeafBlockers(
                            leaf=leaf, horizon=command.quarantine_horizon
                        ),
                    )
                    match quarantined:
                        case QuarantineRefused():
                            return quarantined
                        case _:
                            pass
                    inspection = await inspect_leaf(
                        connection, InspectHistoryLeaf(leaf=leaf)
                    )
                    match inspection:
                        case LeafDetachable():
                            pass
                        case _:
                            return inspection
                case _:
                    return inspection
            prior_timeouts = await _read_session_timeouts(connection)
            await _set_session_timeouts(
                connection,
                statement_timeout_ms=command.statement_timeout_ms,
            )
            retention_class = await read_retention_class(connection, leaf.class_key)
            if retention_class is None or retention_class.finite_parent_name is None:
                raise AssertionError(
                    'detachable classification requires a finite retention class'
                )
            try:
                await connection.execute(
                    text(
                        f"""
                        ALTER TABLE {retention_class.finite_parent_name}
                        DETACH PARTITION {leaf.leaf_name} CONCURRENTLY
                        """
                    )
                )
            except DBAPIError as error:
                if is_lock_not_available(error):
                    return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
                raise
            await _record_detached(connection, leaf.leaf_name)
            await publisher.republish(connection)
            return await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if lock_acquired:
                await _cleanup_session_leaf_lock(
                    connection,
                    leaf=leaf,
                    prior_timeouts=prior_timeouts,
                    uncertain=cancelled,
                )


async def finalize_interrupted_detach(
    database: PartitionMaintenanceDatabase,
    command: FinalizeInterruptedLeafDetach,
    publisher: LoaderPublication,
) -> LeafInspection | LeafMaintenanceBusy:
    """Complete a concurrent detach that a crash left pending.

    Acts only on `LeafDetachInterrupted`; a leaf found already fully
    detached has its catalog record reconciled. Every other classification
    is returned unchanged — in particular a blocked interrupted detach stays
    pending until its recovery evidence drains or is quarantined.
    """
    leaf = command.leaf
    async with database.autocommit() as connection:
        inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
        match inspection:
            case LeafDetached() | LeafDetachInterrupted():
                pass
            case _:
                return inspection

        lock_acquired = False
        prior_timeouts: _SessionTimeouts | None = None
        cancelled = False
        try:
            lock_attempt = await _try_session_leaf_lock(connection, leaf=leaf)
            if lock_attempt is LeafLockAttempt.BUSY:
                return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
            lock_acquired = True
            inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
            match inspection:
                case LeafDetached():
                    await _record_detached(connection, leaf.leaf_name)
                    await publisher.republish(connection)
                    return await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
                case LeafDetachInterrupted():
                    pass
                case _:
                    return inspection
            prior_timeouts = await _read_session_timeouts(connection)
            await _set_session_timeouts(
                connection,
                statement_timeout_ms=command.statement_timeout_ms,
            )
            retention_class = await read_retention_class(connection, leaf.class_key)
            if retention_class is None or retention_class.finite_parent_name is None:
                raise AssertionError(
                    'interrupted-detach classification requires a finite class'
                )
            try:
                await connection.execute(
                    text(
                        f"""
                        ALTER TABLE {retention_class.finite_parent_name}
                        DETACH PARTITION {leaf.leaf_name} FINALIZE
                        """
                    )
                )
            except DBAPIError as error:
                if is_lock_not_available(error):
                    return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
                raise
            await _record_detached(connection, leaf.leaf_name)
            await publisher.republish(connection)
            return await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if lock_acquired:
                await _cleanup_session_leaf_lock(
                    connection,
                    leaf=leaf,
                    prior_timeouts=prior_timeouts,
                    uncertain=cancelled,
                )


async def drop_detached_leaf(
    connection: AsyncConnection,
    command: DropDetachedHistoryLeaf,
    publisher: LoaderPublication,
) -> LeafDrop:
    """Drop one already-detached leaf inside the caller's transaction.

    Refused while the published staged loader still references the relation:
    the regeneration gap blocks the drop, never the reader. The catalog
    retains the dropped leaf's row as durable memory that it existed.
    """
    leaf = command.leaf
    inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
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
    inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
    match inspection:
        case LeafDetached():
            pass
        case _:
            return inspection
    if await publisher.references_leaf(connection, leaf.leaf_name):
        return DropRefusedLoaderReferences(leaf_name=leaf.leaf_name)
    relation_lock = await try_lock_relation_exclusive_for_transaction(
        connection, leaf.leaf_name
    )
    if relation_lock is LeafLockAttempt.BUSY:
        return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    await connection.execute(text(f'DROP TABLE {leaf.leaf_name}'))
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


@dataclass(frozen=True, slots=True)
class _SessionTimeouts:
    statement_timeout: str
    lock_timeout: str


async def _read_session_timeouts(
    connection: AsyncConnection,
) -> _SessionTimeouts:
    return _SessionTimeouts(
        statement_timeout=(
            await connection.execute(text('SHOW statement_timeout'))
        ).scalar_one(),
        lock_timeout=(await connection.execute(text('SHOW lock_timeout'))).scalar_one(),
    )


async def _set_session_timeouts(
    connection: AsyncConnection,
    *,
    statement_timeout_ms: int | None,
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


async def _try_session_leaf_lock(
    connection: AsyncConnection,
    *,
    leaf: LeafRef,
) -> LeafLockAttempt:
    """Invalidate the connection when lock acquisition is uncertain."""
    try:
        return await try_lock_leaf_for_session(
            connection,
            class_key=leaf.class_key,
            anchor=leaf.bounds.lower,
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


async def _pending_blocker_count(
    connection: AsyncConnection,
    leaf: LeafRef,
) -> int:
    count = (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {WORKFLOW_PHASE2_PENDING}
                WHERE recovery_source = 'HISTORY'
                  AND history_class = :class_key
                  AND history_anchor >= :lower
                  AND history_anchor < :upper
                """
            ),
            {
                'class_key': leaf.class_key,
                'lower': leaf.bounds.lower,
                'upper': leaf.bounds.upper,
            },
        )
    ).scalar_one()
    if isinstance(count, bool) or not isinstance(count, int):
        raise AssertionError('count(*) did not decode as integer')
    return count
