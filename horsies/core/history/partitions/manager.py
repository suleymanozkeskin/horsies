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

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

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
from ..names import LEAF_CATALOG, WORKFLOW_PHASE2_PENDING
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
    LeafMissing,
    LeafNotExpired,
    LeafPendingBlocked,
    RetentionClassAbsent,
)
from .catalog import (
    INDEX_SCHEMA_VERSION,
    RetentionClassRow,
    daily_leaf_name,
    database_now,
    leaf_id_index_name,
    read_leaf_catalog_row,
    read_leaf_physical_state,
    read_retention_class,
)
from .locks import (
    lock_leaf_for_session,
    lock_leaf_for_transaction,
    unlock_leaf_for_session,
)
from .publication import LoaderPublication

from datetime import timedelta


_DAILY = timedelta(days=1)


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
        case RetentionClassRow(duration=None):
            return ForeverClassLeaf(class_key=leaf.class_key)
        case RetentionClassRow(partition_interval=interval) if interval != _DAILY:
            return ClassIntervalMismatch(
                class_key=leaf.class_key,
                partition_interval_days=(
                    interval.days if interval is not None else None
                ),
            )
        case RetentionClassRow():
            pass
    parent_name = retention_class.finite_parent_name
    if parent_name is None:
        raise AssertionError('finite class invariant enforced by the catalog reader')

    await lock_leaf_for_transaction(
        connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
    )

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

    if physical.relation_exists != (catalog is not None):
        return LeafCatalogConflict(
            leaf_name=leaf.leaf_name,
            kind=CatalogConflictKind.RELATION_WITHOUT_CATALOG,
            detail=(
                'relation exists without a catalog row'
                if physical.relation_exists
                else 'catalog row exists without a relation'
            ),
        )

    if catalog is not None:
        if (
            catalog.parent_name != parent_name
            or catalog.class_key != leaf.class_key
            or catalog.lower_anchor != leaf.bounds.lower
            or catalog.upper_anchor != leaf.bounds.upper
            or catalog.dropped_at is not None
        ):
            return LeafCatalogConflict(
                leaf_name=leaf.leaf_name,
                kind=CatalogConflictKind.METADATA_MISMATCH,
                detail='existing leaf metadata differs from the request',
            )
        if physical.partition_bound != catalog.partition_bound:
            return LeafCatalogConflict(
                leaf_name=leaf.leaf_name,
                kind=CatalogConflictKind.PHYSICAL_NONCONFORMANT,
                detail='attached leaf partition bound differs from catalog',
            )
        if not physical.id_index_exists:
            await connection.execute(
                text(
                    f'CREATE INDEX {catalog.id_index_name} '
                    f'ON {leaf.leaf_name} (task_id)'
                )
            )
            await connection.execute(text(f'ANALYZE {leaf.leaf_name}'))
            return LeafIndexRepaired(
                leaf_name=leaf.leaf_name, id_index_name=catalog.id_index_name
            )
        return LeafAlreadyConformant(leaf_name=leaf.leaf_name)

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
                :index_schema_version, :id_index_name,
                (SELECT pg_get_expr(c.relpartbound, c.oid)
                 FROM pg_class AS c
                 WHERE c.oid = to_regclass(:leaf_name)),
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
        },
    )
    await connection.execute(
        text(f'CREATE INDEX {id_index_name} ON {leaf.leaf_name} (task_id)')
    )
    await connection.execute(text(f'ANALYZE {leaf.leaf_name}'))
    await publisher.republish(connection)
    return LeafCreated(leaf_name=leaf.leaf_name, id_index_name=id_index_name)


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
        case RetentionClassRow(duration=None):
            return (ForeverClassLeaf(class_key=command.class_key),)
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
    parent_name = retention_class.finite_parent_name
    if parent_name is None:
        raise AssertionError('finite class invariant enforced by the catalog reader')

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
    engine: AsyncEngine,
    command: DetachExpiredHistoryLeaf,
    publisher: LoaderPublication,
) -> LeafInspection:
    """Concurrently detach one leaf classified `LeafDetachable`.

    Any other classification is returned unchanged and nothing is detached.
    After a successful detach the staged loader is republished without the
    leaf; the relation still exists until drop, so a reader holding the old
    function sees a standalone table, never a missing one.
    """
    leaf = command.leaf
    admin_engine = engine.execution_options(isolation_level='AUTOCOMMIT')
    async with admin_engine.connect() as connection:
        await lock_leaf_for_session(
            connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
        )
        prior_statement_timeout: str | None = None
        try:
            inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
            match inspection:
                case LeafDetachable():
                    pass
                case _:
                    return inspection
            if command.statement_timeout_ms is not None:
                prior_statement_timeout = (
                    await connection.execute(text('SHOW statement_timeout'))
                ).scalar_one()
                await connection.execute(
                    text("SELECT set_config('statement_timeout', :value, false)"),
                    {'value': f'{command.statement_timeout_ms}ms'},
                )
            retention_class = await read_retention_class(connection, leaf.class_key)
            if retention_class is None or retention_class.finite_parent_name is None:
                raise AssertionError(
                    'detachable classification requires a finite retention class'
                )
            await connection.execute(
                text(
                    f"""
                    ALTER TABLE {retention_class.finite_parent_name}
                    DETACH PARTITION {leaf.leaf_name} CONCURRENTLY
                    """
                )
            )
            await _record_detached(connection, leaf.leaf_name)
            await publisher.republish(connection)
            return await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
        finally:
            await connection.rollback()
            if prior_statement_timeout is not None:
                await connection.execute(
                    text("SELECT set_config('statement_timeout', :value, false)"),
                    {'value': prior_statement_timeout},
                )
            await unlock_leaf_for_session(
                connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
            )


async def finalize_interrupted_detach(
    engine: AsyncEngine,
    command: FinalizeInterruptedLeafDetach,
    publisher: LoaderPublication,
) -> LeafInspection:
    """Complete a concurrent detach that a crash left pending.

    Acts only on `LeafDetachInterrupted`; a leaf found already fully
    detached has its catalog record reconciled. Every other classification
    is returned unchanged — in particular a blocked interrupted detach stays
    pending until its recovery evidence drains or is quarantined.
    """
    leaf = command.leaf
    admin_engine = engine.execution_options(isolation_level='AUTOCOMMIT')
    async with admin_engine.connect() as connection:
        await lock_leaf_for_session(
            connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
        )
        try:
            inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
            match inspection:
                case LeafDetached():
                    await _record_detached(connection, leaf.leaf_name)
                    await publisher.republish(connection)
                    return await inspect_leaf(
                        connection, InspectHistoryLeaf(leaf=leaf)
                    )
                case LeafDetachInterrupted():
                    pass
                case _:
                    return inspection
            retention_class = await read_retention_class(connection, leaf.class_key)
            if retention_class is None or retention_class.finite_parent_name is None:
                raise AssertionError(
                    'interrupted-detach classification requires a finite class'
                )
            await connection.execute(
                text(
                    f"""
                    ALTER TABLE {retention_class.finite_parent_name}
                    DETACH PARTITION {leaf.leaf_name} FINALIZE
                    """
                )
            )
            await _record_detached(connection, leaf.leaf_name)
            await publisher.republish(connection)
            return await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
        finally:
            await connection.rollback()
            await unlock_leaf_for_session(
                connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
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
    await lock_leaf_for_transaction(
        connection, class_key=leaf.class_key, anchor=leaf.bounds.lower
    )
    inspection = await inspect_leaf(connection, InspectHistoryLeaf(leaf=leaf))
    match inspection:
        case LeafDetached():
            pass
        case _:
            return inspection
    if await publisher.references_leaf(connection, leaf.leaf_name):
        return DropRefusedLoaderReferences(leaf_name=leaf.leaf_name)
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
