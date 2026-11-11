"""Regression: stop() must not discard results of tasks finishing after stop.

stop() must drain the slot pool BEFORE it bounds the finalizers. A finalizer
waiting on a task that runs past its timeout would be cancelled, the task would
then finish anyway, and its result would be dropped: the row would stay RUNNING
until a reaper recorded the finished work as WORKER_CRASHED.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.finalize_batch import (
    FINALIZE_BATCH_CLOSE_TIMEOUT_S,
    FlushersCancelled,
    FlushersDrained,
)
from horsies.core.worker.slots import StopReport
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.unit]


def _make_worker() -> Worker:
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    listener = MagicMock()
    listener.close = AsyncMock()
    return Worker(
        session_factory=MagicMock(),
        listener=listener,
        cfg=cfg,
    )


class _SlowDrainPool:
    """Stands in for a pool whose last task ends during the drain."""

    def __init__(self, task_result: asyncio.Future[str]) -> None:
        self._task_result = task_result

    async def stop(self, drain_timeout_s: float) -> StopReport:
        await asyncio.sleep(0.5)
        self._task_result.set_result('result')
        return StopReport(())


@pytest.mark.asyncio
async def test_stop_persists_results_of_late_finishing_tasks() -> None:
    """A task finishing after the finalizer timeout still gets finalized.

    The task outruns finalizer_timeout_s. Correct ordering drains the pool
    first, after which the finalizer only has its persistence step left.
    """
    worker = _make_worker()
    task_result: asyncio.Future[str] = asyncio.get_running_loop().create_future()
    worker._slots = _SlowDrainPool(task_result)  # type: ignore[assignment]

    persisted: list[str] = []

    async def finalizer() -> None:
        persisted.append(await task_result)

    worker._spawn_background(finalizer(), name='finalize-test', finalizer=True)
    await asyncio.sleep(0.05)

    await worker.stop(finalizer_timeout_s=0.2)

    assert persisted == ['result'], (
        'Finalizer must persist the result of a task that finished after '
        'the finalizer timeout but before the pool drain completed'
    )


@pytest.mark.asyncio
async def test_stop_drains_for_the_configured_time() -> None:
    worker = _make_worker()
    worker.cfg.drain_timeout_s = 7.5
    pool = MagicMock()
    pool.stop = AsyncMock(return_value=StopReport(()))
    worker._slots = pool

    await worker.stop()

    pool.stop.assert_awaited_once_with(7.5)


@pytest.mark.asyncio
async def test_stop_force_cancels_finalizers_without_waiting() -> None:
    """force=True cancels finalizers immediately (results may be dropped)."""
    worker = _make_worker()

    persisted: list[str] = []
    block = asyncio.Event()

    async def finalizer() -> None:
        await block.wait()
        persisted.append('late')

    worker._spawn_background(finalizer(), name='finalize-test', finalizer=True)
    await asyncio.sleep(0.05)

    await worker.stop(force=True)

    assert persisted == [], 'force=True must not wait for finalizers'


@pytest.mark.asyncio
async def test_stop_closes_the_finalize_batcher_after_the_finalizer_drain() -> None:
    """A finalizer that the drain cancels leaves its row in the batcher; the
    batcher closes after the drain, so its flushers still write that row."""
    worker = _make_worker()
    finalizers_at_close: list[int] = []

    async def close(timeout_s: float) -> FlushersDrained:
        finalizers_at_close.append(len(worker._finalizer_tasks))
        assert timeout_s == FINALIZE_BATCH_CLOSE_TIMEOUT_S
        return FlushersDrained()

    worker._finalize_batcher.close = close  # type: ignore[method-assign]
    block = asyncio.Event()

    async def finalizer() -> None:
        await block.wait()

    worker._spawn_background(finalizer(), name='finalize-test', finalizer=True)
    await asyncio.sleep(0.05)

    await worker.stop(finalizer_timeout_s=0.05)

    assert finalizers_at_close == [0]


@pytest.mark.asyncio
async def test_stop_logs_the_rows_that_the_batcher_close_left(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = _make_worker()
    worker._finalize_batcher.close = AsyncMock(  # type: ignore[method-assign]
        return_value=FlushersCancelled(rows_in_flight=3, rows_unsent=2),
    )

    with caplog.at_level(logging.WARNING, logger='horsies.worker'):
        await worker.stop()

    assert any(
        '3 rows in a cancelled statement' in record.getMessage()
        and '2 rows not sent' in record.getMessage()
        for record in caplog.records
    )
