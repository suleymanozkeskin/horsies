"""The retention pruning driver: configured durations are enforced.

The mechanism (detach, finalize, drop, and their refusals) is covered
by the partition lifecycle suites; this suite covers the CALLING — the
pass the maintenance tick runs. The defect shape it pins: a complete,
tested mechanism with no production caller leaves every configured
retention duration silently unenforced, so each test here goes red if
the driver stops driving, not merely if the mechanism breaks.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    DetachExpiredHistoryLeaf,
    InspectHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.heartbeats.partitioning import (
    CreateHourlyHeartbeatLeaf,
    create_hourly_heartbeat_leaf,
    heartbeat_horizon,
    hourly_leaf_ref,
    register_heartbeat_class,
)
from horsies.core.history.maintenance.pruning import (
    prune_expired_partitions,
)
from horsies.core.history.outcomes import (
    LeafCreated,
    LeafDetachInterrupted,
    LeafPendingBlocked,
)
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    database_now,
)
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    detach_expired_leaf,
    inspect_leaf,
)
from horsies.core.history.partitions.publication import UnpublishedLoader

from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    day_bounds,
    frozen_history_row,
    register_class,
    terminalization_schema_fixture,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope='function'),
]

UTC = timezone.utc
CLASS_KEY = 'it_prune'
HORIZON = heartbeat_horizon(
    stale_after=timedelta(minutes=10),
    finalizing_stale_after=timedelta(minutes=30),
    safety_factor=4,
)

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_prune_driver', partitioned_heartbeats=True
)


def _leaf_ref(
    parent_name: str, lower: datetime, class_key: str = CLASS_KEY
) -> LeafRef:
    return LeafRef(
        leaf_name=daily_leaf_name(parent_name, lower),
        class_key=class_key,
        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
    )


async def _make_history_leaf(
    schema: HistorySchema,
    parent_name: str,
    *,
    days_ago: int,
    class_key: str = CLASS_KEY,
) -> LeafRef:
    lower, _ = day_bounds(datetime.now(UTC) - timedelta(days=days_ago))
    ref = _leaf_ref(parent_name, lower, class_key)
    async with schema.engine.begin() as connection:
        outcome = await create_daily_leaf(
            connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
        )
        assert isinstance(outcome, LeafCreated)
    return ref


async def _relation_exists(schema: HistorySchema, leaf_name: str) -> bool:
    async with schema.engine.connect() as connection:
        return bool(
            (
                await connection.execute(
                    text('SELECT to_regclass(:name) IS NOT NULL'),
                    {'name': leaf_name},
                )
            ).scalar_one()
        )


async def _catalog_dropped(schema: HistorySchema, leaf_name: str) -> bool:
    async with schema.engine.connect() as connection:
        return bool(
            (
                await connection.execute(
                    text(
                        'SELECT dropped_at IS NOT NULL '
                        'FROM horsies_task_history_leaf_catalog '
                        'WHERE leaf_name = :leaf_name'
                    ),
                    {'leaf_name': leaf_name},
                )
            ).scalar_one()
        )


async def _seed_pending_locator(
    connection: AsyncConnection,
    ref: LeafRef,
    *,
    task_id: str,
) -> None:
    """One pending locator pinning `ref`, with its node row.

    The pin needs no history row: the detach's blocker count reads the
    pending table alone, by class and anchor range.
    """
    anchor = ref.bounds.lower + timedelta(hours=1)
    node_row_id = str(uuid4())
    workflow_id = str(uuid4())
    await connection.execute(
        text(
            'INSERT INTO horsies_workflow_tasks '
            '(id, workflow_id, task_id, task_index, node_id) VALUES '
            '(CAST(:node_row_id AS uuid), CAST(:workflow_id AS uuid), '
            'CAST(:task_id AS uuid), 0, :node_key)'
        ),
        {
            'node_row_id': node_row_id,
            'workflow_id': workflow_id,
            'task_id': task_id,
            'node_key': 'node-0',
        },
    )
    await connection.execute(
        text(
            """
            INSERT INTO horsies_workflow_phase2_pending (
                task_id, workflow_id, workflow_node_row_id,
                terminal_status, terminal_at, terminalization_kind,
                recovery_source, history_class, history_anchor,
                history_schema_version, result_digest,
                phase2_generation, created_at, attempt_count
            ) VALUES (
                :task_id, :workflow_id, :node_row_id,
                'COMPLETED', :anchor, 'COMPLETE_FUSED',
                'HISTORY', :class_key, :anchor,
                1, :digest, :generation,
                statement_timestamp(), 0
            )
            """
        ),
        {
            'task_id': task_id,
            'workflow_id': workflow_id,
            'node_row_id': node_row_id,
            'anchor': anchor,
            'class_key': CLASS_KEY,
            'digest': sha256(b'{}').digest(),
            'generation': str(uuid4()),
        },
    )


class TestHeartbeatPruning:
    async def test_overdue_heartbeat_leaf_is_detached_and_dropped(
        self, terminalization_schema: HistorySchema
    ) -> None:
        schema = terminalization_schema
        async with schema.engine.begin() as connection:
            await register_heartbeat_class(connection, horizon=HORIZON)
            now = await database_now(connection)
            hour = now.replace(minute=0, second=0, microsecond=0)
            expired = hourly_leaf_ref(hour - timedelta(hours=6))
            created = await create_hourly_heartbeat_leaf(
                connection, CreateHourlyHeartbeatLeaf(leaf=expired)
            )
            assert isinstance(created, LeafCreated)
            current = hourly_leaf_ref(hour)
            created_current = await create_hourly_heartbeat_leaf(
                connection, CreateHourlyHeartbeatLeaf(leaf=current)
            )
            assert isinstance(created_current, LeafCreated)

        pruned = await prune_expired_partitions(
            schema.engine, UnpublishedLoader()
        )

        assert pruned.dropped_count == 1
        assert pruned.errors == ()
        assert [entry.leaf_name for entry in pruned.heartbeat_swept] == [
            expired.leaf_name
        ]
        assert not await _relation_exists(schema, expired.leaf_name)
        assert await _catalog_dropped(schema, expired.leaf_name)
        assert await _relation_exists(schema, current.leaf_name)


class TestDeclaredDurationGovernsTheDrop:
    """A duration other than 30 days actually governs when a leaf drops.

    Every other suite registers classes at the default 30 days, so
    "arbitrary durations are supported" rested on the registration
    function accepting a `timedelta` — an acceptance argument, not a
    demonstration. The discriminating construction is TWO CLASSES WITH
    DIFFERENT DURATIONS HOLDING SAME-AGE LEAVES: age is held constant so
    the only thing that can explain divergent outcomes is the declared
    duration.
    """

    async def test_same_age_leaves_diverge_on_declared_duration(
        self, terminalization_schema: HistorySchema
    ) -> None:
        schema = terminalization_schema
        async with schema.engine.begin() as connection:
            short_parent = await register_class(
                connection, 'it_short_7d', duration_days=7
            )
            long_parent = await register_class(
                connection, 'it_long_30d', duration_days=30
            )

        # SAME age, ten days: past the 7-day horizon, inside the 30-day one.
        short_leaf = await _make_history_leaf(
            schema, short_parent, days_ago=10, class_key='it_short_7d'
        )
        long_leaf = await _make_history_leaf(
            schema, long_parent, days_ago=10, class_key='it_long_30d'
        )

        pruned = await prune_expired_partitions(
            schema.engine, UnpublishedLoader()
        )

        assert pruned.errors == ()
        assert not await _relation_exists(schema, short_leaf.leaf_name), (
            'a 7-day class kept a 10-day-old leaf — the declared duration '
            'is not governing the drop'
        )
        assert await _catalog_dropped(schema, short_leaf.leaf_name)
        assert await _relation_exists(schema, long_leaf.leaf_name), (
            'a 30-day class dropped a 10-day-old leaf — the drop is not '
            'reading the class duration'
        )
        assert not await _catalog_dropped(schema, long_leaf.leaf_name)
        assert [entry.leaf_name for entry in pruned.history_swept] == [
            short_leaf.leaf_name
        ]

    async def test_a_sub_day_duration_still_retains_its_whole_leaf(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """Retention is a MINIMUM: granularity cannot under-retain.

        A leaf spans one day and drops only once its whole day is past
        the duration, so a 1-hour class keeps yesterday's leaf until an
        hour after that day ends — never less than the declared hour.
        """
        schema = terminalization_schema
        async with schema.engine.begin() as connection:
            parent = await register_class(
                connection, 'it_subday', duration_days=1
            )
            await connection.execute(
                text(
                    'UPDATE horsies_retention_classes '
                    "SET duration = interval '1 hour' "
                    'WHERE class_key = :key'
                ),
                {'key': 'it_subday'},
            )
        today = await _make_history_leaf(
            schema, parent, days_ago=0, class_key='it_subday'
        )
        yesterday = await _make_history_leaf(
            schema, parent, days_ago=1, class_key='it_subday'
        )

        pruned = await prune_expired_partitions(
            schema.engine, UnpublishedLoader()
        )

        assert pruned.errors == ()
        assert await _relation_exists(schema, today.leaf_name), (
            "today's leaf is not yet a whole day old plus an hour"
        )
        assert not await _relation_exists(schema, yesterday.leaf_name)


class TestHistoryPruning:
    async def test_overdue_leaf_is_dropped_and_unexpired_leaf_untouched(
        self, terminalization_schema: HistorySchema
    ) -> None:
        schema = terminalization_schema
        async with schema.engine.begin() as connection:
            parent_name = await register_class(
                connection, CLASS_KEY, duration_days=30
            )
        expired = await _make_history_leaf(schema, parent_name, days_ago=40)
        fresh = await _make_history_leaf(schema, parent_name, days_ago=0)

        pruned = await prune_expired_partitions(
            schema.engine, UnpublishedLoader()
        )

        assert pruned.dropped_count == 1
        assert pruned.errors == ()
        assert [entry.leaf_name for entry in pruned.history_swept] == [
            expired.leaf_name
        ]
        assert not await _relation_exists(schema, expired.leaf_name)
        assert await _catalog_dropped(schema, expired.leaf_name)
        assert await _relation_exists(schema, fresh.leaf_name)
        assert not await _catalog_dropped(schema, fresh.leaf_name)

    async def test_pending_locator_refusal_is_reported_and_leaf_kept(
        self, terminalization_schema: HistorySchema
    ) -> None:
        schema = terminalization_schema
        async with schema.engine.begin() as connection:
            parent_name = await register_class(
                connection, CLASS_KEY, duration_days=30
            )
        pinned = await _make_history_leaf(schema, parent_name, days_ago=40)
        async with schema.engine.begin() as connection:
            await _seed_pending_locator(
                connection, pinned, task_id=str(uuid4())
            )

        pruned = await prune_expired_partitions(
            schema.engine, UnpublishedLoader()
        )

        assert pruned.dropped_count == 0
        assert len(pruned.history_swept) == 1
        entry = pruned.history_swept[0]
        assert isinstance(entry.detach, LeafPendingBlocked)
        assert entry.drop is None
        assert any(
            'LeafPendingBlocked' in refusal for refusal in pruned.refusals
        ), pruned.refusals
        assert await _relation_exists(schema, pinned.leaf_name)
        assert not await _catalog_dropped(schema, pinned.leaf_name)

    async def test_interrupted_detach_is_finalized_by_the_following_pass(
        self, terminalization_schema: HistorySchema
    ) -> None:
        schema = terminalization_schema
        async with schema.engine.begin() as connection:
            parent_name = await register_class(
                connection, CLASS_KEY, duration_days=30
            )
        expired = await _make_history_leaf(schema, parent_name, days_ago=40)

        # A long reader plus a bounded statement timeout interrupts the
        # concurrent detach mid-flight (the lifecycle suites' technique),
        # leaving the leaf detach-pending.
        reader = await schema.engine.connect()
        reader_transaction = await reader.begin()
        try:
            await reader.execute(
                text(f'SELECT count(*) FROM {parent_name}')
            )
            with pytest.raises(DBAPIError):
                await detach_expired_leaf(
                    schema.engine,
                    DetachExpiredHistoryLeaf(
                        leaf=expired,
                        quarantine_horizon=None,
                        statement_timeout_ms=100,
                    ),
                    UnpublishedLoader(),
                )
        finally:
            await reader_transaction.rollback()
            await reader.close()

        async with schema.engine.connect() as connection:
            inspection = await inspect_leaf(
                connection, InspectHistoryLeaf(leaf=expired)
            )
        assert isinstance(inspection, LeafDetachInterrupted)

        pruned = await prune_expired_partitions(
            schema.engine, UnpublishedLoader()
        )

        assert pruned.finalized_leaves == (expired.leaf_name,)
        assert pruned.dropped_count == 1
        assert not await _relation_exists(schema, expired.leaf_name)
        assert await _catalog_dropped(schema, expired.leaf_name)
