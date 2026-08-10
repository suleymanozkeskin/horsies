"""The maintenance pass registers the classes config declares.

`ensure_partition_coverage` accepting `declared_classes` proves the
registrar works when called with them. It cannot prove the reaper passes
config through — that is a separate seam, and a threading mistake there
would leave every declaration silently inert while the registrar's own
tests stayed green.

So these tests enter ABOVE the wiring, at `_run_reaper_pass`, with a real
`RecoveryConfig`, and assert BELOW it on the registry row. They never call
`ensure_partition_coverage` directly; naming it here would reintroduce
exactly the blindness they exist to remove.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.recovery import RecoveryConfig, RetentionClassConfig
from horsies.core.worker.runtime import _ReaperPassState
from horsies.core.worker.worker import Worker, WorkerConfig

DECLARED_KEY = 'it_reaper_declared'
DECLARED_DURATION = timedelta(days=11)


def _reaper_worker(broker: PostgresBroker) -> Worker:
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


def _maintenance_state() -> _ReaperPassState:
    """Fires partition maintenance; leaves row retention alone."""
    return _ReaperPassState(
        next_retention_cleanup_at=float('inf'),
        next_partition_maintenance_at=0.0,
    )


def _config_declaring(key: str, duration: timedelta) -> RecoveryConfig:
    return RecoveryConfig(
        retention_classes=(
            RetentionClassConfig(key=key, duration=duration),
        )
    )


async def _registered_duration(
    broker: PostgresBroker, key: str
) -> timedelta | None:
    async with broker.async_engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    'SELECT duration FROM horsies_retention_classes '
                    'WHERE class_key = :key'
                ),
                {'key': key},
            )
        ).scalar_one_or_none()


async def _forget_class(broker: PostgresBroker, key: str) -> None:
    """Remove the class and everything the pass built for it.

    Order matters: the leaf catalog carries a foreign key to the class,
    and a registered class gets leaves created for it on the same pass,
    so the class row cannot go first.
    """
    async with broker.async_engine.begin() as connection:
        leaves = (
            await connection.execute(
                text(
                    'SELECT leaf_name FROM horsies_task_history_leaf_catalog '
                    'WHERE class_key = :key'
                ),
                {'key': key},
            )
        ).scalars().all()
        for leaf_name in leaves:
            await connection.execute(
                text(f'DROP TABLE IF EXISTS {leaf_name} CASCADE')
            )
        await connection.execute(
            text(
                'DELETE FROM horsies_task_history_leaf_catalog '
                'WHERE class_key = :key'
            ),
            {'key': key},
        )
        await connection.execute(
            text(f'DROP TABLE IF EXISTS horsies_task_history_{key} CASCADE')
        )
        await connection.execute(
            text(
                'DELETE FROM horsies_retention_classes WHERE class_key = :key'
            ),
            {'key': key},
        )


@pytest.mark.integration
class TestReaperRegistersDeclaredClasses:
    @pytest.mark.asyncio
    async def test_the_pass_registers_a_declared_class(
        self, broker: PostgresBroker
    ) -> None:
        await _forget_class(broker, DECLARED_KEY)
        try:
            assert await _registered_duration(broker, DECLARED_KEY) is None

            worker = _reaper_worker(broker)
            await worker._run_reaper_pass(  # pyright: ignore[reportPrivateUsage]
                broker,
                _config_declaring(DECLARED_KEY, DECLARED_DURATION),
                _maintenance_state(),
            )

            registered = await _registered_duration(broker, DECLARED_KEY)
            assert registered == DECLARED_DURATION, (
                f'{DECLARED_KEY!r} is declared in config but the maintenance '
                'pass did not register it — the declaration is inert'
            )
        finally:
            await _forget_class(broker, DECLARED_KEY)

    @pytest.mark.asyncio
    async def test_declaring_nothing_registers_nothing_extra(
        self, broker: PostgresBroker
    ) -> None:
        """The complement: the pass must not invent classes."""
        await _forget_class(broker, DECLARED_KEY)
        try:
            worker = _reaper_worker(broker)
            await worker._run_reaper_pass(  # pyright: ignore[reportPrivateUsage]
                broker, RecoveryConfig(), _maintenance_state()
            )
            assert await _registered_duration(broker, DECLARED_KEY) is None
        finally:
            await _forget_class(broker, DECLARED_KEY)
