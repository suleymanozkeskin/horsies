"""Partition-bound conformance is session-timezone-independent.

`pg_get_expr` renders timestamptz literals in the session timezone, so
identical instants render differently across sessions — the bare-date
defect class in a second costume. The presence half proves the raw
rendering genuinely diverges between the two probe timezones (if it
ever stops diverging, the UTC pin can be reconsidered); the regression
then proves a leaf created under one side of UTC is judged conformant,
detachable, and healthy from a session on the other side, for both the
history and heartbeat creation paths. Both sides are exercised because
a single-timezone test proves nothing.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    CollectPartitionHealth,
    CreateDailyHistoryLeaf,
    InspectHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.heartbeats.partitioning import (
    CreateHourlyHeartbeatLeaf,
    heartbeat_horizon,
    hourly_leaf_ref,
    register_heartbeat_class,
)
from horsies.core.history.outcomes import (
    LeafAlreadyConformant,
    LeafCreated,
    LeafDetachable,
    LeafNotExpired,
)
from horsies.core.history.partitions.catalog import daily_leaf_name
from horsies.core.history.partitions.health import (
    collect_partition_health,
)
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    inspect_leaf,
)
from horsies.core.history.partitions.publication import UnpublishedLoader

from tests.integration.task_history_harness import (
    HistorySchema,
    register_class,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_tz'
WEST_OF_UTC = 'Etc/GMT+12'
EAST_OF_UTC = 'Etc/GMT-12'

history_schema = task_history_schema_fixture('task_history_it_timezones')


async def set_session_timezone(
    connection: AsyncConnection, timezone_name: str
) -> None:
    await connection.execute(
        text("SELECT set_config('timezone', :tz, false)"),
        {'tz': timezone_name},
    )


def leaf_for_today(parent: str) -> LeafRef:
    lower = datetime.now(UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return LeafRef(
        leaf_name=daily_leaf_name(parent, lower),
        class_key=CLASS_KEY,
        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
    )


class TestRenderingDivergesWithoutThePin:
    @pytest.mark.asyncio
    async def test_raw_rendering_differs_across_the_probe_timezones(
        self, history_schema: HistorySchema
    ) -> None:
        """The presence half: the defect is real between these probes."""
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
            ref = leaf_for_today(parent)
            created = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=ref),
                UnpublishedLoader(),
            )
            assert isinstance(created, LeafCreated)

            renderings: dict[str, str] = {}
            for timezone_name in (WEST_OF_UTC, EAST_OF_UTC):
                await set_session_timezone(connection, timezone_name)
                renderings[timezone_name] = (
                    await connection.execute(
                        text(
                            'SELECT pg_get_expr(c.relpartbound, c.oid) '
                            'FROM pg_class c '
                            'WHERE c.oid = to_regclass(:leaf)'
                        ),
                        {'leaf': ref.leaf_name},
                    )
                ).scalar_one()
            assert renderings[WEST_OF_UTC] != renderings[EAST_OF_UTC]


class TestConformanceIsSessionIndependent:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('creating_timezone', 'inspecting_timezone'),
        [(WEST_OF_UTC, EAST_OF_UTC), (EAST_OF_UTC, WEST_OF_UTC)],
    )
    async def test_leaf_created_on_one_side_is_conformant_on_the_other(
        self,
        history_schema: HistorySchema,
        creating_timezone: str,
        inspecting_timezone: str,
    ) -> None:
        from horsies.core.history.commands import EnsureLeafCoverage
        from horsies.core.history.partitions.manager import (
            ensure_leaf_coverage,
        )

        async with history_schema.engine.begin() as connection:
            await set_session_timezone(connection, creating_timezone)
            parent = await register_class(connection, CLASS_KEY)
            ref = leaf_for_today(parent)
            outcomes = await ensure_leaf_coverage(
                connection,
                EnsureLeafCoverage(class_key=CLASS_KEY, horizon_days=3),
                UnpublishedLoader(),
            )
            assert all(
                isinstance(outcome, LeafCreated) for outcome in outcomes
            )

        async with history_schema.engine.begin() as connection:
            await set_session_timezone(connection, inspecting_timezone)
            verified = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=ref),
                UnpublishedLoader(),
            )
            assert verified == LeafAlreadyConformant(
                leaf_name=ref.leaf_name
            ), verified
            inspection = await inspect_leaf(
                connection, InspectHistoryLeaf(leaf=ref)
            )
            assert isinstance(
                inspection, (LeafNotExpired, LeafDetachable)
            )
            report = await collect_partition_health(
                connection,
                CollectPartitionHealth(
                    class_key=CLASS_KEY, application_managed=True
                ),
            )
            assert report.is_healthy

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('creating_timezone', 'inspecting_timezone'),
        [(WEST_OF_UTC, EAST_OF_UTC), (EAST_OF_UTC, WEST_OF_UTC)],
    )
    async def test_heartbeat_leaf_conformance_crosses_utc_both_ways(
        self,
        history_schema: HistorySchema,
        creating_timezone: str,
        inspecting_timezone: str,
    ) -> None:
        horizon = heartbeat_horizon(
            stale_after=timedelta(minutes=10),
            finalizing_stale_after=timedelta(minutes=30),
            safety_factor=4,
        )
        from horsies.core.history.heartbeats.partitioning import (
            HEARTBEATS_PARTITIONED_DDL,
        )

        async with history_schema.engine.begin() as connection:
            parent_missing = (
                await connection.execute(
                    text(
                        "SELECT to_regclass('horsies_heartbeats') IS NULL"
                    )
                )
            ).scalar_one()
            if parent_missing:
                await connection.execute(text(HEARTBEATS_PARTITIONED_DDL))
            await set_session_timezone(connection, creating_timezone)
            await register_heartbeat_class(connection, horizon=horizon)
            hour = (await connection.execute(
                text('SELECT date_trunc(\'hour\', statement_timestamp())')
            )).scalar_one()
            leaf = hourly_leaf_ref(hour)
            created = await create_hourly(connection, leaf)
            assert isinstance(created, LeafCreated)

        async with history_schema.engine.begin() as connection:
            await set_session_timezone(connection, inspecting_timezone)
            verified = await create_hourly(connection, leaf)
            assert verified == LeafAlreadyConformant(
                leaf_name=leaf.leaf_name
            )


async def create_hourly(connection: AsyncConnection, leaf: LeafRef):  # type: ignore[no-untyped-def]
    from horsies.core.history.heartbeats.partitioning import (
        create_hourly_heartbeat_leaf,
    )

    return await create_hourly_heartbeat_leaf(
        connection, CreateHourlyHeartbeatLeaf(leaf=leaf)
    )
