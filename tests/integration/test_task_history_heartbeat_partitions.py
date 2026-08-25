"""Heartbeat partition lifecycle against real PostgreSQL.

The partitioned parent serves the real stale probe: registration derives
the horizon, coverage creates hourly leaves with the probe-shaped index,
heartbeats written through the parent land in their hour's leaf, the
staleness guard fails a silent runner and refuses on a fresh heartbeat
exactly as it does on flat storage, and the sweep detaches and drops a
leaf past its horizon while the catalog keeps the durable record.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.heartbeats.partitioning import (
    EnsureHeartbeatCoverage,
    HeartbeatClassRegistered,
    HeartbeatClassVerified,
    HeartbeatHorizonUpdated,
    create_hourly_heartbeat_leaf,
    CreateHourlyHeartbeatLeaf,
    ensure_heartbeat_coverage,
    heartbeat_horizon,
    hourly_leaf_ref,
    register_heartbeat_class,
    sweep_expired_heartbeat_leaves,
)
from horsies.core.history.outcomes import (
    LeafCreated,
    LeafDetached,
    LeafDropped,
)
from horsies.core.history.partitions.catalog import database_now
from horsies.core.history.partitions.publication import UnpublishedLoader
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase

from tests.integration.task_history_harness import (
    HistorySchema,
    insert_live_task,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_hb'
WORKER = 'worker-hb-1'
HORIZON = heartbeat_horizon(
    stale_after=timedelta(minutes=10),
    finalizing_stale_after=timedelta(minutes=30),
    safety_factor=4,
)

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_heartbeats', partitioned_heartbeats=True
)


async def prepare_heartbeat_storage(connection: AsyncConnection) -> None:
    registered = await register_heartbeat_class(connection, horizon=HORIZON)
    assert isinstance(registered, HeartbeatClassRegistered)
    outcomes = await ensure_heartbeat_coverage(
        connection, EnsureHeartbeatCoverage(horizon_hours=2)
    )
    assert all(isinstance(outcome, LeafCreated) for outcome in outcomes)


async def send_heartbeat(
    connection: AsyncConnection,
    task_id: str,
    *,
    age: timedelta,
) -> None:
    """Write one heartbeat, covering the hour it lands in.

    Coverage runs forward from the current hour and never creates a past
    one, because production writes heartbeats at `now` and never
    backdates. This helper DOES backdate, so an `age` that crosses an
    hour boundary lands where no leaf exists — which made callers fail
    for one minute in sixty, decided by where the run's wall clock fell.

    Creating the landing hour here fixes every caller at the point the
    backdating happens. It is idempotent: a caller that already created
    that hour (the recency-bound test does, deliberately) is unaffected.
    """
    landing = (await database_now(connection)) - age
    await create_hourly_heartbeat_leaf(
        connection,
        CreateHourlyHeartbeatLeaf(
            leaf=hourly_leaf_ref(
                landing.replace(minute=0, second=0, microsecond=0)
            )
        ),
    )
    await connection.execute(
        text(
            'INSERT INTO horsies_heartbeats '
            '(task_id, sender_id, role, sent_at) VALUES '
            "(CAST(:task_id AS uuid), :sender, 'runner', "
            'statement_timestamp() - :age)'
        ),
        {'task_id': task_id, 'sender': WORKER, 'age': age},
    )


async def fail_stale(connection: AsyncConnection, task_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_fail_stale_task('
                'CAST(:task_id AS uuid), 600000, 1800000, '
                '\'{"error":{"code":"STALE"}}\', \'STALE\', \'went silent\')'
            ),
            {'task_id': task_id},
        )
    ).one()


class TestRegistration:
    @pytest.mark.asyncio
    async def test_register_verify_and_horizon_update(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            first = await register_heartbeat_class(
                connection, horizon=HORIZON
            )
            assert first == HeartbeatClassRegistered(horizon=HORIZON)
            again = await register_heartbeat_class(
                connection, horizon=HORIZON
            )
            assert again == HeartbeatClassVerified(horizon=HORIZON)
            widened = await register_heartbeat_class(
                connection, horizon=HORIZON * 2
            )
            assert widened == HeartbeatHorizonUpdated(
                previous_horizon=HORIZON, horizon=HORIZON * 2
            )


class TestCoverageAndWrites:
    @pytest.mark.asyncio
    async def test_coverage_creates_probe_indexed_hourly_leaves(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_heartbeat_storage(connection)
            now = await database_now(connection)
            hour = now.replace(minute=0, second=0, microsecond=0)
            leaf = hourly_leaf_ref(hour)
            index_columns = (
                await connection.execute(
                    text(
                        'SELECT pg_get_indexdef(i.indexrelid) AS ddl '
                        'FROM pg_index i '
                        'WHERE i.indrelid = to_regclass(:leaf)'
                    ),
                    {'leaf': leaf.leaf_name},
                )
            ).scalar_one()
            assert '(task_id, role, sent_at DESC)' in index_columns

    @pytest.mark.asyncio
    async def test_heartbeats_route_through_the_parent_into_leaves(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await prepare_heartbeat_storage(connection)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            await send_heartbeat(connection, task_id, age=timedelta(0))
            now = await database_now(connection)
            hour = now.replace(minute=0, second=0, microsecond=0)
            leaf = hourly_leaf_ref(hour)
            in_leaf = (
                await connection.execute(
                    text(
                        f'SELECT count(*) FROM {leaf.leaf_name} '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert in_leaf == 1


class TestStaleProbeOnPartitionedStorage:
    @pytest.mark.asyncio
    async def test_silent_runner_fails_with_no_heartbeat_rows(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await prepare_heartbeat_storage(connection)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                started_at_offset=timedelta(minutes=-30),
            )
            outcome = await fail_stale(connection, task_id)
            assert outcome.outcome == 'APPLIED'

    @pytest.mark.asyncio
    async def test_a_heartbeat_across_the_hour_boundary_has_a_partition(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The boundary is MANUFACTURED, not waited for.

        The suite's own backdating used to cross into an hour that
        forward-only coverage never created, so it failed for one minute
        in sixty — and passed the rest of the time for no better reason
        than where the clock sat. A regression that inserts at a fixed
        small age would inherit that: green whenever the run happens to
        sit mid-hour.

        So this derives its age FROM the current instant to guarantee the
        crossing: enough to land in the previous hour whatever the minute.
        The guard cannot go quiet by scheduling accident.
        """
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await prepare_heartbeat_storage(connection)
            now = await database_now(connection)
            # One second past the top of this hour, i.e. always previous.
            crossing_age = timedelta(
                minutes=now.minute, seconds=now.second + 1
            )
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                started_at_offset=timedelta(minutes=-30),
            )
            await send_heartbeat(connection, task_id, age=crossing_age)

            landed = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_heartbeats '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert landed == 1, (
                'a heartbeat backdated across the hour boundary found no '
                'partition — coverage does not span what the suite writes'
            )

    @pytest.mark.asyncio
    async def test_fresh_heartbeat_refuses_through_the_partitions(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await prepare_heartbeat_storage(connection)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                started_at_offset=timedelta(minutes=-30),
            )
            await send_heartbeat(
                connection, task_id, age=timedelta(minutes=1)
            )
            outcome = await fail_stale(connection, task_id)
            assert outcome.outcome == 'SOURCE_STATE_CONFLICT'

    @pytest.mark.asyncio
    async def test_heartbeat_beyond_the_recency_bound_cannot_flip_verdict(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """A heartbeat older than every threshold satisfies the staleness
        comparison for all values, so the bound excluding it must leave
        the verdict identical to having no heartbeat at all."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await prepare_heartbeat_storage(connection)
            # The old heartbeat was written when its hour was current;
            # within the horizon that leaf still exists. Recreate it.
            now = await database_now(connection)
            hour = now.replace(minute=0, second=0, microsecond=0)
            for hours_back in (1, 2):
                created = await create_hourly_heartbeat_leaf(
                    connection,
                    CreateHourlyHeartbeatLeaf(
                        leaf=hourly_leaf_ref(
                            hour - timedelta(hours=hours_back)
                        )
                    ),
                )
                assert isinstance(created, LeafCreated)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                started_at_offset=timedelta(minutes=-90),
            )
            await send_heartbeat(
                connection, task_id, age=timedelta(minutes=80)
            )
            outcome = await fail_stale(connection, task_id)
            assert outcome.outcome == 'APPLIED'


class TestSweep:
    @pytest.mark.asyncio
    async def test_leaf_past_horizon_is_detached_and_dropped(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await register_heartbeat_class(connection, horizon=HORIZON)
            now = await database_now(connection)
            hour = now.replace(minute=0, second=0, microsecond=0)
            expired_lower = hour - timedelta(hours=6)
            expired = hourly_leaf_ref(expired_lower)
            created = await create_hourly_heartbeat_leaf(
                connection, CreateHourlyHeartbeatLeaf(leaf=expired)
            )
            assert isinstance(created, LeafCreated)
            current = await create_hourly_heartbeat_leaf(
                connection, CreateHourlyHeartbeatLeaf(leaf=hourly_leaf_ref(hour))
            )
            assert isinstance(current, LeafCreated)

        swept = await sweep_expired_heartbeat_leaves(
            PartitionMaintenanceDatabase(terminalization_schema.engine),
            UnpublishedLoader(),
        )
        assert len(swept) == 1
        assert swept[0].leaf_name == expired.leaf_name
        assert isinstance(swept[0].detach, LeafDetached)
        assert swept[0].drop == LeafDropped(leaf_name=expired.leaf_name)

        async with terminalization_schema.engine.begin() as connection:
            gone = (
                await connection.execute(
                    text('SELECT to_regclass(:leaf) IS NULL'),
                    {'leaf': expired.leaf_name},
                )
            ).scalar_one()
            assert gone
            record = (
                await connection.execute(
                    text(
                        'SELECT detached_at, dropped_at '
                        'FROM horsies_task_history_leaf_catalog '
                        'WHERE leaf_name = :leaf'
                    ),
                    {'leaf': expired.leaf_name},
                )
            ).one()
            assert record.detached_at is not None
            assert record.dropped_at is not None
            survivor = (
                await connection.execute(
                    text('SELECT to_regclass(:leaf) IS NOT NULL'),
                    {'leaf': hourly_leaf_ref(hour).leaf_name},
                )
            ).scalar_one()
            assert survivor

    @pytest.mark.asyncio
    async def test_sweep_with_nothing_expired_is_empty(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_heartbeat_storage(connection)
        swept = await sweep_expired_heartbeat_leaves(
            PartitionMaintenanceDatabase(terminalization_schema.engine),
            UnpublishedLoader(),
        )
        assert swept == ()
