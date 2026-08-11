"""The reaper pass actually prunes expired leaves — wiring, not mechanism.

Every other test of the pruning path calls the sweep directly and so
supplies the caller itself. That proves the mechanism works WHEN CALLED
and can never prove anything calls it: the retention drop path shipped
complete, correctness-tested, and driven by nothing, because the
maintenance tick created leaves and never pruned them.

These tests enter ABOVE the wiring — `_run_reaper_pass`, the function the
reaper loop invokes — and assert BELOW it, on the leaf being gone from
the catalogs. They must never import or call the pruning entry point;
naming it here would reintroduce exactly the blindness they exist to
remove.

The seam above this one is still open by construction: the interval,
the cluster-wide gate and the breaker live in `_reaper_loop`, not in the
pass, so that a deployed worker reaches the pass at all remains proven
only by a unit test that replaces the pass with a mock.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.heartbeats.partitioning import (
    CreateHourlyHeartbeatLeaf,
    create_hourly_heartbeat_leaf,
    hourly_leaf_ref,
    register_heartbeat_class,
)
from horsies.core.history.outcomes import LeafCreated
from horsies.core.history.partitions.catalog import database_now
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.retention import RetentionConfig
from horsies.core.worker.runtime import _ReaperPassState
from horsies.core.worker.worker import Worker, WorkerConfig

# Comfortably past the six-hour heartbeat horizon, so the leaf is overdue
# under any plausible clock skew between test and server.
OVERDUE_BY = timedelta(hours=12)


def _reaper_worker(broker: PostgresBroker) -> Worker:
    """A Worker constructed only as far as `_run_reaper_pass` requires.

    The pass touches exactly three attributes of `self` — two health
    dicts it writes and one sibling method — and takes the broker it
    works through as a parameter. The constructor is pure assignment, so
    no pool, executor or listener is created; the mocks below are never
    reached.
    """
    config = WorkerConfig(
        dsn='postgresql+psycopg://unused:unused@localhost/unused',
        psycopg_dsn='postgresql://unused:unused@localhost/unused',
        queues=['default'],
    )
    return Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=config,
        broker=broker,
    )


def _pruning_pass_state() -> _ReaperPassState:
    """State that fires partition maintenance and nothing else.

    Retention cleanup is pushed to infinity so a shared integration
    database cannot have rows deleted underneath other tests; partition
    maintenance is due immediately.
    """
    return _ReaperPassState(
        next_retention_cleanup_at=float('inf'),
        next_partition_maintenance_at=0.0,
    )


async def _make_overdue_leaf(broker: PostgresBroker) -> tuple[str, str]:
    """Create one leaf past its horizon and one current. Returns names."""
    async with broker.async_engine.begin() as connection:
        await register_heartbeat_class(connection, horizon=timedelta(hours=6))
        now = await database_now(connection)
        hour = now.replace(minute=0, second=0, microsecond=0)

        # An hour the catalog REMEMBERS cannot be created again: a
        # dropped leaf keeps its row as durable evidence it existed, and
        # creation refuses rather than resurrect a retired anchor. On a
        # fresh database no hour is remembered; on a long-lived one an
        # earlier run may have dropped the hour this arithmetic picks, so
        # step further back until an unremembered one is found instead of
        # failing on the substrate.
        overdue = hourly_leaf_ref(hour - OVERDUE_BY)
        for extra_hours in range(0, 12):
            candidate = hourly_leaf_ref(
                hour - OVERDUE_BY - extra_hours * timedelta(hours=1)
            )
            remembered = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM '
                        'horsies_task_history_leaf_catalog '
                        'WHERE leaf_name = :leaf'
                    ),
                    {'leaf': candidate.leaf_name},
                )
            ).scalar_one()
            if not remembered:
                overdue = candidate
                break
        else:  # pragma: no cover - twelve remembered hours in a row
            raise AssertionError(
                'every candidate hour is remembered by the leaf catalog; '
                'the database carries drop-memory this test cannot work '
                'around'
            )

        created = await create_hourly_heartbeat_leaf(
            connection, CreateHourlyHeartbeatLeaf(leaf=overdue)
        )
        assert isinstance(created, LeafCreated), created

        current = hourly_leaf_ref(hour)
        # The current hour's leaf may already exist from coverage; either
        # outcome is fine, it exists either way.
        await create_hourly_heartbeat_leaf(
            connection, CreateHourlyHeartbeatLeaf(leaf=current)
        )
    return overdue.leaf_name, current.leaf_name


async def _leaf_exists(broker: PostgresBroker, leaf_name: str) -> bool:
    async with broker.async_engine.connect() as connection:
        return bool(
            (
                await connection.execute(
                    text('SELECT to_regclass(:leaf) IS NOT NULL'),
                    {'leaf': leaf_name},
                )
            ).scalar_one()
        )


async def _drop_leaf_if_present(broker: PostgresBroker, leaf_name: str) -> None:
    """Teardown for the case the pass did NOT prune — keeps the shared
    database clean when this test fails."""
    async with broker.async_engine.begin() as connection:
        await connection.execute(text(f'DROP TABLE IF EXISTS {leaf_name}'))
        await connection.execute(
            text(
                'DELETE FROM horsies_task_history_leaf_catalog '
                'WHERE leaf_name = :leaf'
            ),
            {'leaf': leaf_name},
        )


@pytest.mark.integration
class TestReaperPrunesExpiredLeaves:
    """The pass must prune; entering here is the whole point."""

    @pytest.mark.asyncio
    async def test_pass_drops_a_leaf_past_its_horizon(
        self, broker: PostgresBroker
    ) -> None:
        overdue_name, _current_name = await _make_overdue_leaf(broker)
        try:
            assert await _leaf_exists(broker, overdue_name), (
                'setup failed: the overdue leaf was not created'
            )

            worker = _reaper_worker(broker)
            await worker._run_reaper_pass(  # pyright: ignore[reportPrivateUsage]
                broker,
                RecoveryConfig(),
                RetentionConfig(),
                _pruning_pass_state(),
            )

            assert not await _leaf_exists(broker, overdue_name), (
                f'{overdue_name} is past its horizon and survived a reaper '
                'pass — the pruning mechanism exists but the pass does not '
                'call it'
            )
            async with broker.async_engine.connect() as connection:
                record = (
                    await connection.execute(
                        text(
                            'SELECT detached_at, dropped_at '
                            'FROM horsies_task_history_leaf_catalog '
                            'WHERE leaf_name = :leaf'
                        ),
                        {'leaf': overdue_name},
                    )
                ).one()
            assert record.detached_at is not None
            assert record.dropped_at is not None
        finally:
            await _drop_leaf_if_present(broker, overdue_name)

    @pytest.mark.asyncio
    async def test_pass_keeps_a_leaf_inside_its_horizon(
        self, broker: PostgresBroker
    ) -> None:
        """The complement: pruning that drops everything is not pruning."""
        overdue_name, current_name = await _make_overdue_leaf(broker)
        try:
            worker = _reaper_worker(broker)
            await worker._run_reaper_pass(  # pyright: ignore[reportPrivateUsage]
                broker,
                RecoveryConfig(),
                RetentionConfig(),
                _pruning_pass_state(),
            )
            assert await _leaf_exists(broker, current_name), (
                f'{current_name} is inside its horizon and was dropped'
            )
        finally:
            await _drop_leaf_if_present(broker, overdue_name)
