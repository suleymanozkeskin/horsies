"""Detach republishes the staged readers only when it changes them.

A detached history leaf leaves the attached set, so the readers are
republished once. A heartbeat leaf is never in the reader manifest, and a
detach the catalog already recorded changes nothing, so both keep a current
publication as it is.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    DetachExpiredHistoryLeaf,
    FinalizeInterruptedLeafDetach,
    LeafBounds,
    LeafLifecycle,
    LeafRef,
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
from horsies.core.history.outcomes import LeafCreated, LeafDetached
from horsies.core.history.partitions.catalog import daily_leaf_name, database_now
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    detach_expired_leaf,
    finalize_interrupted_detach,
)
from horsies.core.history.partitions.publication import LoaderRepublished
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from tests.integration.task_history_harness import (
    HistorySchema,
    day_bounds,
    register_class,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLASS_KEY = 'it_detach_rp'
HORIZON = heartbeat_horizon(
    stale_after=timedelta(minutes=10),
    finalizing_stale_after=timedelta(minutes=30),
    safety_factor=4,
)

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_detach_republication', partitioned_heartbeats=True
)


class CountingPublisher(StagedLoaderPublisher):
    """The real publisher, counting its republications."""

    def __init__(self) -> None:
        self.republished = 0

    async def republish(self, connection: AsyncConnection) -> LoaderRepublished:
        self.republished += 1
        return await super().republish(connection)


def _database(schema: HistorySchema) -> PartitionMaintenanceDatabase:
    return PartitionMaintenanceDatabase(schema.engine, connection_capacity=2)


async def _expired_history_leaf(schema: HistorySchema) -> LeafRef:
    async with schema.engine.begin() as connection:
        parent = await register_class(connection, CLASS_KEY)
        lower, _ = day_bounds(datetime.now(UTC) - timedelta(days=40))
        ref = LeafRef(
            leaf_name=daily_leaf_name(parent, lower),
            class_key=CLASS_KEY,
            bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
        )
        created = await create_daily_leaf(
            connection, CreateDailyHistoryLeaf(leaf=ref), StagedLoaderPublisher(),
        )
        assert isinstance(created, LeafCreated)
    return ref


async def test_history_detach_republishes_once_and_a_recorded_detach_does_not(
    terminalization_schema: HistorySchema,
) -> None:
    ref = await _expired_history_leaf(terminalization_schema)
    publisher = CountingPublisher()
    async with terminalization_schema.engine.connect() as connection:
        assert await publisher.is_current(connection)

    detached = await detach_expired_leaf(
        _database(terminalization_schema),
        DetachExpiredHistoryLeaf(
            leaf=ref, quarantine_horizon=None, statement_timeout_ms=None,
        ),
        publisher,
    )
    assert isinstance(detached, LeafDetached)
    assert publisher.republished == 1
    async with terminalization_schema.engine.connect() as connection:
        assert not await publisher.references_leaf(connection, ref.leaf_name)
        assert await publisher.is_current(connection)

    finalized = await finalize_interrupted_detach(
        _database(terminalization_schema),
        FinalizeInterruptedLeafDetach(
            leaf=ref, statement_timeout_ms=None, lifecycle=LeafLifecycle.RETENTION,
        ),
        publisher,
    )
    assert isinstance(finalized, LeafDetached)
    assert publisher.republished == 1


async def test_heartbeat_detach_keeps_a_current_publication(
    terminalization_schema: HistorySchema,
) -> None:
    await _expired_history_leaf(terminalization_schema)
    async with terminalization_schema.engine.begin() as connection:
        await register_heartbeat_class(connection, horizon=HORIZON)
        now = await database_now(connection)
        hour = now.replace(minute=0, second=0, microsecond=0)
        expired = hourly_leaf_ref(hour - timedelta(hours=6))
        for leaf in (expired, hourly_leaf_ref(hour)):
            created = await create_hourly_heartbeat_leaf(
                connection, CreateHourlyHeartbeatLeaf(leaf=leaf),
            )
            assert isinstance(created, LeafCreated)
    publisher = CountingPublisher()
    async with terminalization_schema.engine.connect() as connection:
        assert await publisher.is_current(connection)

    swept = await sweep_expired_heartbeat_leaves(
        _database(terminalization_schema), publisher,
    )
    assert [sweep.leaf_name for sweep in swept] == [expired.leaf_name]
    assert isinstance(swept[0].detach, LeafDetached)
    assert publisher.republished == 0
