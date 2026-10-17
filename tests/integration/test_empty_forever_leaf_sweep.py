"""The sweep of closed, empty daily `forever` leaves against real PostgreSQL.

Before the open-ended leaf, coverage created one daily `forever` leaf per
day. The sweep detaches and drops the closed ones that hold no rows, keeps
the ones with rows, finishes an interrupted detach on the next pass, and
detaches at most `MAX_FOREVER_DETACHES_PER_PASS` leaves per pass. It is
skipped before cutover completes and while archive maintenance is active.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    DetachEmptyForeverLeaf,
    DropDetachedHistoryLeaf,
    InspectHistoryLeaf,
    LeafBounds,
    LeafLifecycle,
    LeafRef,
)
from horsies.core.history.cutover.state import (
    CREATE_CUTOVER_STATE_TABLE_SQL,
    MARK_CUTOVER_COMPLETE_SQL,
)
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.history.maintenance.gate import ARCHIVE_MAINTENANCE_SESSIONS
from horsies.core.history.maintenance.pruning import (
    MAX_FOREVER_DETACHES_PER_PASS,
    ForeverLeafSwept,
    ForeverSweepRan,
    ForeverSweepSkipped,
    ForeverSweepSkipReason,
    prune_expired_partitions,
    sweep_empty_forever_leaves,
)
from horsies.core.history.names import LEAF_CATALOG, TASK_HISTORY_FOREVER
from horsies.core.history.outcomes import (
    LeafCreated,
    LeafDetached,
    LeafDetachInterrupted,
    LeafDropped,
    LeafKeptNonEmpty,
)
from horsies.core.history.partitions.catalog import daily_leaf_name, database_now
from horsies.core.history.partitions.forever import FOREVER_LEGACY_LEAF
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    detach_empty_forever_leaf,
    drop_detached_leaf,
    inspect_leaf,
)
from horsies.core.history.partitions.publication import UnpublishedLoader
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    frozen_history_row,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

history_schema = task_history_schema_fixture('task_history_it_forever_sweep')


@pytest_asyncio.fixture()
async def swept_schema(
    history_schema: HistorySchema,
) -> AsyncGenerator[HistorySchema, None]:
    """The history schema with the cutover marked complete."""
    async with history_schema.engine.begin() as connection:
        await connection.execute(CREATE_CUTOVER_STATE_TABLE_SQL)
        await connection.execute(MARK_CUTOVER_COMPLETE_SQL)
    yield history_schema


def _database(schema: HistorySchema) -> PartitionMaintenanceDatabase:
    return PartitionMaintenanceDatabase(schema.engine, connection_capacity=2)


async def _today(schema: HistorySchema) -> datetime:
    async with schema.engine.connect() as connection:
        now = await database_now(connection)
    return now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _forever_leaf(lower: datetime) -> LeafRef:
    return LeafRef(
        leaf_name=daily_leaf_name(TASK_HISTORY_FOREVER, lower),
        class_key='forever',
        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
    )


async def _seed_row(
    connection: AsyncConnection, terminal_at: datetime, *, relation: str
) -> None:
    await connection.execute(
        text(INSERT_HISTORY_ROW_SQL.replace('horsies_task_history', relation, 1)),
        frozen_history_row(
            task_id=str(uuid.uuid4()), class_key='forever', terminal_at=terminal_at
        ),
    )


async def _create_closed_leaves(
    schema: HistorySchema, lowers: list[datetime], *, seeded: bool
) -> list[LeafRef]:
    """Create daily forever leaves, optionally one row in each, then publish."""
    leaves = [_forever_leaf(lower) for lower in lowers]
    async with schema.engine.begin() as connection:
        for leaf in leaves:
            created = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=leaf), UnpublishedLoader()
            )
            assert isinstance(created, LeafCreated), created
            if seeded:
                await _seed_row(
                    connection,
                    leaf.bounds.lower + timedelta(hours=1),
                    relation='horsies_task_history',
                )
        await StagedLoaderPublisher().republish(connection)
    return leaves


async def _leaf_dropped(schema: HistorySchema, leaf_name: str) -> bool:
    async with schema.engine.connect() as connection:
        row = (
            await connection.execute(
                text(
                    f'SELECT detached_at IS NOT NULL, dropped_at IS NOT NULL, '
                    f'to_regclass(leaf_name) IS NULL FROM {LEAF_CATALOG} '
                    'WHERE leaf_name = :leaf_name'
                ),
                {'leaf_name': leaf_name},
            )
        ).one()
    return bool(row[0] and row[1] and row[2])


async def _sweep(schema: HistorySchema) -> ForeverSweepRan:
    sweep = await sweep_empty_forever_leaves(
        _database(schema), StagedLoaderPublisher()
    )
    assert isinstance(sweep, ForeverSweepRan), sweep
    assert sweep.errors == ()
    return sweep


async def test_the_sweep_drops_closed_empty_leaves_and_keeps_the_rest(
    swept_schema: HistorySchema,
) -> None:
    today = await _today(swept_schema)
    (empty,) = await _create_closed_leaves(
        swept_schema, [today - timedelta(days=2)], seeded=False
    )
    (full,) = await _create_closed_leaves(
        swept_schema, [today - timedelta(days=3)], seeded=True
    )
    async with swept_schema.engine.begin() as connection:
        # An empty relation under the legacy leaf name, cataloged with closed
        # one-day bounds, is still never a candidate.
        await connection.execute(
            text(
                f'CREATE TABLE {FOREVER_LEGACY_LEAF} '
                '(LIKE horsies_task_history INCLUDING DEFAULTS)'
            )
        )
        await connection.execute(
            text(
                f'INSERT INTO {LEAF_CATALOG} (leaf_name, parent_name, '
                'class_key, lower_anchor, upper_anchor, index_schema_version, '
                'id_index_name, partition_bound, min_birth_at, '
                'min_birth_verified, created_at) VALUES (:leaf_name, :parent, '
                "'forever', :lower, :upper, 1, :index_name, 'legacy', NULL, "
                'TRUE, NOW())'
            ),
            {
                'leaf_name': FOREVER_LEGACY_LEAF,
                'parent': TASK_HISTORY_FOREVER,
                'lower': today - timedelta(days=9),
                'upper': today - timedelta(days=8),
                'index_name': f'{FOREVER_LEGACY_LEAF}_task_idx',
            },
        )

    sweep = await _sweep(swept_schema)

    assert sweep.swept == (
        ForeverLeafSwept(empty.leaf_name, LeafDropped(leaf_name=empty.leaf_name)),
    )
    assert sweep.kept_non_empty == (full.leaf_name,)
    assert await _leaf_dropped(swept_schema, empty.leaf_name)
    assert not await _leaf_dropped(swept_schema, full.leaf_name)
    async with swept_schema.engine.connect() as connection:
        publisher = StagedLoaderPublisher()
        assert not await publisher.references_leaf(connection, empty.leaf_name)
        assert await publisher.references_leaf(connection, full.leaf_name)
        assert await publisher.is_current(connection)
        legacy_present = (
            await connection.execute(
                text('SELECT to_regclass(:name) IS NOT NULL'),
                {'name': FOREVER_LEGACY_LEAF},
            )
        ).scalar_one()
    assert legacy_present


async def test_the_prune_pass_counts_the_forever_drops(
    swept_schema: HistorySchema,
) -> None:
    today = await _today(swept_schema)
    (empty,) = await _create_closed_leaves(
        swept_schema, [today - timedelta(days=2)], seeded=False
    )
    pruned = await prune_expired_partitions(
        _database(swept_schema), StagedLoaderPublisher()
    )
    assert pruned.errors == ()
    assert pruned.forever_swept == (
        ForeverLeafSwept(empty.leaf_name, LeafDropped(leaf_name=empty.leaf_name)),
    )
    assert pruned.dropped_count == 1
    assert pruned.acted


async def test_a_detach_does_not_take_a_leaf_with_rows(
    swept_schema: HistorySchema,
) -> None:
    today = await _today(swept_schema)
    (full,) = await _create_closed_leaves(
        swept_schema, [today - timedelta(days=2)], seeded=True
    )
    outcome = await detach_empty_forever_leaf(
        _database(swept_schema),
        DetachEmptyForeverLeaf(leaf=full, statement_timeout_ms=None),
        StagedLoaderPublisher(),
    )
    assert outcome == LeafKeptNonEmpty(leaf_name=full.leaf_name)
    async with swept_schema.engine.connect() as connection:
        attached = (
            await connection.execute(
                text(
                    'SELECT EXISTS (SELECT 1 FROM pg_inherits '
                    'WHERE inhrelid = to_regclass(:leaf))'
                ),
                {'leaf': full.leaf_name},
            )
        ).scalar_one()
    assert attached


async def test_an_interrupted_detach_is_finished_by_the_next_sweep(
    swept_schema: HistorySchema,
) -> None:
    today = await _today(swept_schema)
    (leaf,) = await _create_closed_leaves(
        swept_schema, [today - timedelta(days=2)], seeded=False
    )
    # A long reader plus a bounded statement timeout interrupts the
    # concurrent detach mid-flight, leaving the leaf detach-pending.
    reader = await swept_schema.engine.connect()
    reader_transaction = await reader.begin()
    try:
        await reader.execute(text(f'SELECT count(*) FROM {leaf.leaf_name}'))
        with pytest.raises(DBAPIError):
            await detach_empty_forever_leaf(
                _database(swept_schema),
                DetachEmptyForeverLeaf(leaf=leaf, statement_timeout_ms=100),
                UnpublishedLoader(),
            )
    finally:
        await reader_transaction.rollback()
        await reader.close()
    async with swept_schema.engine.connect() as connection:
        inspection = await inspect_leaf(
            connection,
            InspectHistoryLeaf(
                leaf=leaf, lifecycle=LeafLifecycle.CLOSED_EMPTY_FOREVER
            ),
        )
    assert isinstance(inspection, LeafDetachInterrupted)

    sweep = await _sweep(swept_schema)

    assert sweep.swept == (
        ForeverLeafSwept(leaf.leaf_name, LeafDropped(leaf_name=leaf.leaf_name)),
    )
    assert await _leaf_dropped(swept_schema, leaf.leaf_name)


async def test_the_sweep_reaches_an_empty_leaf_behind_non_empty_ones(
    swept_schema: HistorySchema,
) -> None:
    today = await _today(swept_schema)
    full = await _create_closed_leaves(
        swept_schema,
        [today - timedelta(days=offset) for offset in range(34, 2, -1)],
        seeded=True,
    )
    (empty,) = await _create_closed_leaves(
        swept_schema, [today - timedelta(days=2)], seeded=False
    )

    sweep = await _sweep(swept_schema)

    assert len(sweep.kept_non_empty) == MAX_FOREVER_DETACHES_PER_PASS
    assert sweep.swept == (
        ForeverLeafSwept(empty.leaf_name, LeafDropped(leaf_name=empty.leaf_name)),
    )
    for leaf in full:
        assert not await _leaf_dropped(swept_schema, leaf.leaf_name)


async def test_the_sweep_stops_at_its_detach_bound(
    swept_schema: HistorySchema,
) -> None:
    today = await _today(swept_schema)
    leaves = await _create_closed_leaves(
        swept_schema,
        [
            today - timedelta(days=offset)
            for offset in range(2, 3 + MAX_FOREVER_DETACHES_PER_PASS)
        ],
        seeded=False,
    )

    first = await _sweep(swept_schema)
    second = await _sweep(swept_schema)

    assert sum(
        isinstance(entry.outcome, LeafDropped) for entry in first.swept
    ) == MAX_FOREVER_DETACHES_PER_PASS
    assert len(second.swept) == 1
    for leaf in leaves:
        assert await _leaf_dropped(swept_schema, leaf.leaf_name)


async def test_the_sweep_is_skipped_while_archive_maintenance_is_active(
    swept_schema: HistorySchema,
) -> None:
    today = await _today(swept_schema)
    (leaf,) = await _create_closed_leaves(
        swept_schema, [today - timedelta(days=2)], seeded=False
    )
    async with swept_schema.engine.begin() as connection:
        await connection.execute(
            text(
                f'INSERT INTO {ARCHIVE_MAINTENANCE_SESSIONS} '
                '(session_id, started_at) VALUES (CAST(:id AS uuid), NOW())'
            ),
            {'id': str(uuid.uuid4())},
        )
    sweep = await sweep_empty_forever_leaves(
        _database(swept_schema), StagedLoaderPublisher()
    )
    assert sweep == ForeverSweepSkipped(
        reason=ForeverSweepSkipReason.ARCHIVE_MAINTENANCE_ACTIVE
    )
    assert not await _leaf_dropped(swept_schema, leaf.leaf_name)


async def test_the_sweep_is_skipped_before_cutover_completes(
    history_schema: HistorySchema,
) -> None:
    today = await _today(history_schema)
    (leaf,) = await _create_closed_leaves(
        history_schema, [today - timedelta(days=2)], seeded=False
    )
    sweep = await sweep_empty_forever_leaves(
        _database(history_schema), StagedLoaderPublisher()
    )
    assert sweep == ForeverSweepSkipped(
        reason=ForeverSweepSkipReason.CUTOVER_NOT_COMPLETE
    )
    assert not await _leaf_dropped(history_schema, leaf.leaf_name)


async def test_a_drop_keeps_a_detached_leaf_that_gained_rows(
    swept_schema: HistorySchema,
) -> None:
    today = await _today(swept_schema)
    (leaf,) = await _create_closed_leaves(
        swept_schema, [today - timedelta(days=2)], seeded=False
    )
    detached = await detach_empty_forever_leaf(
        _database(swept_schema),
        DetachEmptyForeverLeaf(leaf=leaf, statement_timeout_ms=None),
        StagedLoaderPublisher(),
    )
    assert isinstance(detached, LeafDetached), detached
    async with swept_schema.engine.begin() as connection:
        # A row written straight into the detached leaf after its detach.
        await _seed_row(
            connection,
            leaf.bounds.lower + timedelta(hours=1),
            relation=leaf.leaf_name,
        )
    async with swept_schema.engine.begin() as connection:
        dropped = await drop_detached_leaf(
            connection,
            DropDetachedHistoryLeaf(
                leaf=leaf, lifecycle=LeafLifecycle.CLOSED_EMPTY_FOREVER
            ),
            StagedLoaderPublisher(),
        )
    assert dropped == LeafKeptNonEmpty(leaf_name=leaf.leaf_name)
    assert not await _leaf_dropped(swept_schema, leaf.leaf_name)
