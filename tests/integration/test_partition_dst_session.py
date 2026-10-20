"""Partition maintenance under a session timezone with DST.

The driver decodes timestamptz into the session's zone. In Europe/Berlin,
2026-10-25 00:00 UTC decodes to 02:00+02, inside the repeated hour; PEP 495
makes such a value compare unequal to the same instant in UTC, and the
difference of two values in one zone is wall-clock time. Every test here
runs maintenance on connections whose session timezone is Europe/Berlin
and pins that leaves on both sides of a fold verify, sweep, and drop as
they do under UTC.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    InspectHistoryLeaf,
    LeafBounds,
    LeafLifecycle,
    LeafRef,
)
from horsies.core.history.cutover.state import (
    CREATE_CUTOVER_STATE_TABLE_SQL,
    MARK_CUTOVER_COMPLETE_SQL,
)
from horsies.core.history.heartbeats.partitioning import (
    CreateHourlyHeartbeatLeaf,
    create_hourly_heartbeat_leaf,
    heartbeat_horizon,
    hourly_leaf_ref,
    register_heartbeat_class,
    sweep_expired_heartbeat_leaves,
)
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.history.maintenance.pruning import (
    ForeverSweepRan,
    sweep_empty_forever_leaves,
)
from horsies.core.history.names import TASK_HISTORY_FOREVER
from horsies.core.history.outcomes import (
    LeafAlreadyConformant,
    LeafCreated,
    LeafDetachable,
    LeafDetached,
    LeafDropped,
    LeafNotExpired,
)
from horsies.core.history.partitions.catalog import daily_leaf_name
from horsies.core.history.partitions.manager import create_daily_leaf, inspect_leaf
from horsies.core.history.partitions.publication import UnpublishedLoader
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from tests.integration.task_history_harness import (
    HistorySchema,
    register_class,
    task_history_schema_fixture,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DST_ZONE = 'Europe/Berlin'
CLASS_KEY = 'it_dst'
# The first pass of the repeated hour in Europe/Berlin, in each year.
COMING_FOLD = datetime(2026, 10, 25, tzinfo=UTC)
PAST_FOLD = datetime(2025, 10, 26, tzinfo=UTC)
HORIZON = heartbeat_horizon(
    stale_after=timedelta(minutes=10),
    finalizing_stale_after=timedelta(minutes=30),
    safety_factor=4,
)

history_schema = task_history_schema_fixture('task_history_it_dst')
heartbeat_schema = terminalization_schema_fixture(
    'task_history_it_dst_heartbeats', partitioned_heartbeats=True
)


async def _dst_engine(schema: HistorySchema) -> AsyncEngine:
    """An engine on the schema whose sessions run in `DST_ZONE`."""
    engine = create_async_engine(
        schema.engine.url,
        connect_args={
            'options': f'-csearch_path={schema.schema_name} -cTimeZone={DST_ZONE}'
        },
    )
    async with engine.connect() as connection:
        decoded = (
            await connection.execute(
                text('SELECT CAST(:instant AS timestamptz)'),
                {'instant': COMING_FOLD.isoformat()},
            )
        ).scalar_one()
    # The premise: this session decodes the fold instant into a value
    # that compares unequal to the same instant in UTC.
    assert decoded != COMING_FOLD, decoded
    return engine


@pytest_asyncio.fixture()
async def dst_history(
    history_schema: HistorySchema,
) -> AsyncGenerator[AsyncEngine, None]:
    async with history_schema.engine.begin() as connection:
        await connection.execute(CREATE_CUTOVER_STATE_TABLE_SQL)
        await connection.execute(MARK_CUTOVER_COMPLETE_SQL)
    engine = await _dst_engine(history_schema)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture()
async def dst_heartbeats(
    heartbeat_schema: HistorySchema,
) -> AsyncGenerator[AsyncEngine, None]:
    engine = await _dst_engine(heartbeat_schema)
    async with engine.begin() as connection:
        await register_heartbeat_class(connection, horizon=HORIZON)
    yield engine
    await engine.dispose()


def _daily_leaf(parent: str, class_key: str, lower: datetime) -> LeafRef:
    return LeafRef(
        leaf_name=daily_leaf_name(parent, lower),
        class_key=class_key,
        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
    )


async def test_daily_leaves_at_the_fold_verify_as_conformant(
    dst_history: AsyncEngine,
) -> None:
    async with dst_history.begin() as connection:
        parent = await register_class(connection, CLASS_KEY)
        leaves = [
            _daily_leaf(parent, CLASS_KEY, COMING_FOLD - timedelta(days=1)),
            _daily_leaf(parent, CLASS_KEY, COMING_FOLD),
        ]
        for leaf in leaves:
            created = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=leaf), UnpublishedLoader()
            )
            assert isinstance(created, LeafCreated), created

    async with dst_history.begin() as connection:
        for leaf in leaves:
            verified = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=leaf), UnpublishedLoader()
            )
            assert verified == LeafAlreadyConformant(leaf_name=leaf.leaf_name)
            inspection = await inspect_leaf(
                connection,
                InspectHistoryLeaf(leaf=leaf, lifecycle=LeafLifecycle.RETENTION),
            )
            assert isinstance(inspection, (LeafNotExpired, LeafDetachable))


async def test_hourly_leaves_at_the_fold_verify_as_conformant(
    dst_heartbeats: AsyncEngine,
) -> None:
    leaves = [
        hourly_leaf_ref(COMING_FOLD),
        hourly_leaf_ref(COMING_FOLD + timedelta(hours=1)),
    ]
    for expected in (LeafCreated, LeafAlreadyConformant):
        async with dst_heartbeats.begin() as connection:
            for leaf in leaves:
                outcome = await create_hourly_heartbeat_leaf(
                    connection, CreateHourlyHeartbeatLeaf(leaf=leaf)
                )
                assert isinstance(outcome, expected), outcome


async def test_the_heartbeat_sweep_drops_leaves_across_a_fold(
    dst_heartbeats: AsyncEngine,
) -> None:
    leaves = [
        hourly_leaf_ref(PAST_FOLD),
        hourly_leaf_ref(PAST_FOLD + timedelta(hours=1)),
    ]
    async with dst_heartbeats.begin() as connection:
        for leaf in leaves:
            created = await create_hourly_heartbeat_leaf(
                connection, CreateHourlyHeartbeatLeaf(leaf=leaf)
            )
            assert isinstance(created, LeafCreated), created

    swept = await sweep_expired_heartbeat_leaves(
        PartitionMaintenanceDatabase(dst_heartbeats, connection_capacity=2),
        UnpublishedLoader(),
    )

    assert [entry.leaf_name for entry in swept] == [
        leaf.leaf_name for leaf in leaves
    ]
    for entry in swept:
        assert isinstance(entry.detach, LeafDetached), entry
        assert entry.drop == LeafDropped(leaf_name=entry.leaf_name)


async def test_the_forever_sweep_drops_empty_leaves_across_a_fold(
    dst_history: AsyncEngine,
) -> None:
    leaves = [
        _daily_leaf(TASK_HISTORY_FOREVER, 'forever', PAST_FOLD - timedelta(days=1)),
        _daily_leaf(TASK_HISTORY_FOREVER, 'forever', PAST_FOLD),
    ]
    async with dst_history.begin() as connection:
        for leaf in leaves:
            created = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=leaf), UnpublishedLoader()
            )
            assert isinstance(created, LeafCreated), created
        await StagedLoaderPublisher().republish(connection)

    sweep = await sweep_empty_forever_leaves(
        PartitionMaintenanceDatabase(dst_history, connection_capacity=2),
        StagedLoaderPublisher(),
    )

    assert isinstance(sweep, ForeverSweepRan), sweep
    assert sweep.errors == ()
    assert [entry.leaf_name for entry in sweep.swept] == [
        leaf.leaf_name for leaf in leaves
    ]
    for entry in sweep.swept:
        assert entry.outcome == LeafDropped(leaf_name=entry.leaf_name)

