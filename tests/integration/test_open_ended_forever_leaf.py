"""`forever` history lives in one open-ended leaf instead of one leaf per day.

The open-ended leaf covers `[lower, 9999-01-01)`. Coverage creates it once,
from the later of today and the largest attached `forever` upper anchor, so
it never overlaps the daily leaves made before it. A daily leaf requested
inside it is refused as covered, and health measures `forever` by its
largest upper anchor.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    OPEN_END_ANCHOR,
    CollectPartitionHealth,
    CreateDailyHistoryLeaf,
    EnsureLeafCoverage,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.names import LEAF_CATALOG, TASK_HISTORY_FOREVER
from horsies.core.history.outcomes import (
    CoverageBelowFloor,
    CoveredByOpenEndedLeaf,
    LeafAlreadyConformant,
    LeafCreated,
)
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    open_ended_leaf_name,
)
from horsies.core.history.partitions.health import collect_partition_health
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    ensure_leaf_coverage,
)
from horsies.core.history.partitions.publication import UnpublishedLoader
from tests.integration.task_history_harness import (
    HistorySchema,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

history_schema = task_history_schema_fixture('task_history_it_open_forever')


def _today() -> datetime:
    return datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _daily_forever_leaf(lower: datetime) -> LeafRef:
    return LeafRef(
        leaf_name=daily_leaf_name(TASK_HISTORY_FOREVER, lower),
        class_key='forever',
        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
    )


async def _forever_leaves(connection: AsyncConnection) -> list[tuple[str, datetime, datetime]]:
    rows = (await connection.execute(text(
        f"SELECT leaf_name, lower_anchor, upper_anchor FROM {LEAF_CATALOG} "
        "WHERE class_key = 'forever' AND detached_at IS NULL "
        "ORDER BY lower_anchor"
    ))).tuples().all()
    return [
        (str(name), lower.astimezone(UTC), upper.astimezone(UTC))
        for name, lower, upper in rows
    ]


async def _without_open_ended_leaf(connection: AsyncConnection) -> None:
    """Remove the open-ended leaf the schema's forever conversion created."""
    for name, _, upper in await _forever_leaves(connection):
        if upper == OPEN_END_ANCHOR:
            await connection.execute(text(f'DROP TABLE {name}'))
            await connection.execute(
                text(f'DELETE FROM {LEAF_CATALOG} WHERE leaf_name = :name'),
                {'name': name},
            )


async def _ensure_forever(connection: AsyncConnection) -> object:
    (outcome,) = await ensure_leaf_coverage(
        connection,
        EnsureLeafCoverage(class_key='forever', horizon_days=3),
        UnpublishedLoader(),
    )
    return outcome


async def test_coverage_creates_one_open_ended_leaf_and_then_nothing(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        await _without_open_ended_leaf(connection)
        assert isinstance(await _ensure_forever(connection), LeafCreated)
        assert isinstance(await _ensure_forever(connection), LeafAlreadyConformant)
        assert await _forever_leaves(connection) == [(
            open_ended_leaf_name(TASK_HISTORY_FOREVER, _today()),
            _today(),
            OPEN_END_ANCHOR,
        )]


async def test_the_open_ended_leaf_starts_after_existing_daily_leaves(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        await _without_open_ended_leaf(connection)
        for offset in range(4):
            created = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(
                    leaf=_daily_forever_leaf(_today() + timedelta(days=offset)),
                ),
                UnpublishedLoader(),
            )
            assert isinstance(created, LeafCreated)
        assert isinstance(await _ensure_forever(connection), LeafCreated)
        leaves = await _forever_leaves(connection)
        start = _today() + timedelta(days=4)
        assert leaves[-1] == (
            open_ended_leaf_name(TASK_HISTORY_FOREVER, start), start, OPEN_END_ANCHOR,
        )
        assert len(leaves) == 5


async def test_a_daily_leaf_inside_the_open_ended_leaf_is_refused_as_covered(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        await _without_open_ended_leaf(connection)
        assert isinstance(await _ensure_forever(connection), LeafCreated)
        requested = _daily_forever_leaf(_today() + timedelta(days=1))
        outcome = await create_daily_leaf(
            connection,
            CreateDailyHistoryLeaf(leaf=requested),
            UnpublishedLoader(),
        )
        assert outcome == CoveredByOpenEndedLeaf(
            leaf_name=requested.leaf_name,
            open_ended_leaf_name=open_ended_leaf_name(TASK_HISTORY_FOREVER, _today()),
        )
        assert len(await _forever_leaves(connection)) == 1


async def test_health_measures_forever_by_its_largest_upper_anchor(
    history_schema: HistorySchema,
) -> None:
    command = CollectPartitionHealth(class_key='forever', application_managed=True)
    async with history_schema.engine.begin() as connection:
        await _without_open_ended_leaf(connection)
        created = await create_daily_leaf(
            connection,
            CreateDailyHistoryLeaf(leaf=_daily_forever_leaf(_today())),
            UnpublishedLoader(),
        )
        assert isinstance(created, LeafCreated)
        only_today = await collect_partition_health(connection, command)
        assert any(isinstance(fault, CoverageBelowFloor) for fault in only_today.faults)

        assert isinstance(await _ensure_forever(connection), LeafCreated)
        covered = await collect_partition_health(connection, command)
        assert covered.is_healthy
