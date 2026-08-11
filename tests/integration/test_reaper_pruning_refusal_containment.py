"""A refusal is contained, named, and does not stop the pass.

The pruning pass's happy path is pinned at runtime elsewhere. Its
refusal behaviour was, until this file, determinate only by reading:
`_sweep_refusals` collects, per-leaf `try/except` contains, and the leaf
is left for the next tick — all true in the source, none of it executed
through the real caller.

These tests enter ABOVE the wiring, at `_run_reaper_pass`, and assert
below it. The load-bearing one is not that a blocked leaf survives — it
is that **an unblocked leaf in the same pass is still dropped**. That is
what makes a refusal safe rather than a pass-killer, and it is the last
property of the pruning pass resting on code reading alone.

A blocker is a `horsies_workflow_phase2_pending` row whose
`(recovery_source, history_class, history_anchor)` lands inside the
leaf's range; see `partitions/health.py`'s `blocker_count`. No history
row is required — the survey never joins the leaf's contents — so the
construction stays minimal and cannot accidentally prove something else.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.outcomes import LeafAlreadyConformant, LeafCreated
from horsies.core.history.partitions.catalog import database_now
from horsies.core.history.partitions.manager import create_daily_leaf
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.retention import RetentionConfig
from horsies.core.worker.runtime import _ReaperPassState
from horsies.core.worker.worker import Worker, WorkerConfig

# Comfortably past the default class's 30-day horizon.
BLOCKED_AGE = timedelta(days=45)
CLEAR_AGE = timedelta(days=40)


def _reaper_worker(broker: PostgresBroker) -> Worker:
    """A Worker built only as far as `_run_reaper_pass` needs."""
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
    """Fires partition maintenance; leaves row retention alone."""
    return _ReaperPassState(
        next_retention_cleanup_at=float('inf'),
        next_partition_maintenance_at=0.0,
    )


def _daily_leaf(day_start: datetime) -> LeafRef:
    return LeafRef(
        leaf_name=(
            f'horsies_task_history_{DEFAULT_RETENTION_CLASS_KEY}_'
            f'{day_start:%Y_%m_%d}'
        ),
        class_key=DEFAULT_RETENTION_CLASS_KEY,
        bounds=LeafBounds(
            lower=day_start, upper=day_start + timedelta(days=1)
        ),
    )


def _daily_leaf_for_age(age: timedelta) -> LeafRef:
    """Best-effort ref from the local clock, for teardown only."""
    from datetime import timezone as _tz
    day = (datetime.now(_tz.utc) - age).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return _daily_leaf(day)


async def _make_expired_leaf(
    broker: PostgresBroker, age: timedelta
) -> LeafRef:
    async with broker.async_engine.begin() as connection:
        now = await database_now(connection)
        day = (now - age).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        leaf = _daily_leaf(day)
        created = await create_daily_leaf(
            connection,
            CreateDailyHistoryLeaf(leaf=leaf),
            StagedLoaderPublisher(),
        )
        assert isinstance(created, (LeafCreated, LeafAlreadyConformant)), (
            created
        )
    return leaf


async def _block_leaf(broker: PostgresBroker, leaf: LeafRef) -> uuid.UUID:
    """Plant a pending locator whose anchor lands inside `leaf`.

    Requires a workflow and a node row only because of the composite FK
    on (workflow_node_row_id, workflow_id); neither participates in the
    blocker survey itself.
    """
    workflow_id = uuid.uuid4()
    node_id = uuid.uuid4()
    task_id = uuid.uuid4()
    anchor = leaf.bounds.lower + timedelta(hours=12)
    async with broker.async_engine.begin() as connection:
        await connection.execute(
            text(
                'INSERT INTO horsies_workflows '
                '(id, name, status, on_error, depth, '
                ' created_at, updated_at) '
                "VALUES (:id, :name, :status, 'FAIL_FAST', 0, "
                ' now(), now())'
            ),
            {
                'id': workflow_id,
                'name': 'refusal-containment',
                'status': 'COMPLETED',
            },
        )
        await connection.execute(
            text(
                'INSERT INTO horsies_workflow_tasks '
                '(id, workflow_id, task_index, task_name, queue_name, '
                ' priority, dependencies, allow_failed_deps, join_type, '
                ' status, is_subworkflow, created_at) '
                "VALUES (:id, :workflow_id, 0, :task_name, 'default', "
                " 100, ARRAY[]::int[], FALSE, 'ALL', :status, FALSE, now())"
            ),
            {
                'id': node_id,
                'workflow_id': workflow_id,
                'task_name': 'refusal.blocker',
                'status': 'COMPLETED',
            },
        )
        await connection.execute(
            text(
                'INSERT INTO horsies_workflow_phase2_pending ('
                '  task_id, workflow_id, workflow_node_row_id,'
                '  terminal_status, terminal_at, terminalization_kind,'
                '  recovery_source, history_class, history_anchor,'
                '  history_schema_version, result_digest,'
                '  phase2_generation, created_at, attempt_count'
                ') VALUES ('
                '  :task_id, :workflow_id, :node_id,'
                "  'COMPLETED', :anchor, 'COMPLETE_LOCKED',"
                "  'HISTORY', :history_class, :anchor,"
                '  1, :digest, :generation, now(), 0)'
            ),
            {
                'task_id': task_id,
                'workflow_id': workflow_id,
                'node_id': node_id,
                'anchor': anchor,
                'history_class': leaf.class_key,
                'generation': uuid.uuid4(),
                'digest': b'\x00' * 32,
            },
        )
    return workflow_id


async def _leaf_exists(broker: PostgresBroker, leaf: LeafRef) -> bool:
    async with broker.async_engine.connect() as connection:
        return bool(
            (
                await connection.execute(
                    text('SELECT to_regclass(:leaf) IS NOT NULL'),
                    {'leaf': leaf.leaf_name},
                )
            ).scalar_one()
        )


async def _cleanup(
    broker: PostgresBroker,
    leaves: tuple[LeafRef, ...],
    workflow_id: uuid.UUID | None,
) -> None:
    async with broker.async_engine.begin() as connection:
        if workflow_id is not None:
            # phase2_pending cascades from the node row.
            await connection.execute(
                text('DELETE FROM horsies_workflow_tasks '
                     'WHERE workflow_id = :id'),
                {'id': workflow_id},
            )
            await connection.execute(
                text('DELETE FROM horsies_workflows WHERE id = :id'),
                {'id': workflow_id},
            )
        for leaf in leaves:
            await connection.execute(
                text(f'DROP TABLE IF EXISTS {leaf.leaf_name}')
            )
            await connection.execute(
                text('DELETE FROM horsies_task_history_leaf_catalog '
                     'WHERE leaf_name = :leaf'),
                {'leaf': leaf.leaf_name},
            )


@pytest.mark.integration
class TestPruningRefusalContainment:
    @pytest.mark.asyncio
    async def test_a_blocked_leaf_survives_and_the_pass_returns(
        self, broker: PostgresBroker
    ) -> None:
        leaf = _daily_leaf_for_age(BLOCKED_AGE)
        workflow_id: uuid.UUID | None = None
        try:
            leaf = await _make_expired_leaf(broker, BLOCKED_AGE)
            workflow_id = await _block_leaf(broker, leaf)
            worker = _reaper_worker(broker)
            # The assertion is that this RETURNS rather than raising.
            await worker._run_reaper_pass(  # pyright: ignore[reportPrivateUsage]
                broker,
                RecoveryConfig(),
                RetentionConfig(),
                _pruning_pass_state(),
            )
            assert await _leaf_exists(broker, leaf), (
                f'{leaf.leaf_name} has a pending locator and must not be '
                'dropped'
            )
            async with broker.async_engine.connect() as connection:
                stamps = (
                    await connection.execute(
                        text(
                            'SELECT detached_at, dropped_at '
                            'FROM horsies_task_history_leaf_catalog '
                            'WHERE leaf_name = :leaf'
                        ),
                        {'leaf': leaf.leaf_name},
                    )
                ).one()
            assert stamps.detached_at is None
            assert stamps.dropped_at is None
        finally:
            await _cleanup(broker, (leaf,), workflow_id)

    @pytest.mark.asyncio
    async def test_a_refusal_does_not_stop_the_pass_dropping_others(
        self, broker: PostgresBroker
    ) -> None:
        """Per-leaf containment — the property that makes a refusal safe.

        Both leaves are past the horizon. One is blocked. The pass must
        refuse the blocked one and STILL drop the clear one.
        """
        blocked = _daily_leaf_for_age(BLOCKED_AGE)
        clear = _daily_leaf_for_age(CLEAR_AGE)
        workflow_id: uuid.UUID | None = None
        try:
            blocked = await _make_expired_leaf(broker, BLOCKED_AGE)
            clear = await _make_expired_leaf(broker, CLEAR_AGE)
            assert blocked.leaf_name != clear.leaf_name
            workflow_id = await _block_leaf(broker, blocked)
            worker = _reaper_worker(broker)
            await worker._run_reaper_pass(  # pyright: ignore[reportPrivateUsage]
                broker,
                RecoveryConfig(),
                RetentionConfig(),
                _pruning_pass_state(),
            )
            assert await _leaf_exists(broker, blocked), (
                'the blocked leaf was dropped despite its locator'
            )
            assert not await _leaf_exists(broker, clear), (
                f'{clear.leaf_name} is unblocked and past its horizon, but '
                'survived a pass in which another leaf was refused — one '
                'refusal stopped the sweep'
            )
        finally:
            await _cleanup(broker, (blocked, clear), workflow_id)
