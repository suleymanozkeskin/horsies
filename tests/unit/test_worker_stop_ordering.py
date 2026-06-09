"""Regression: stop() must not discard results of tasks finishing after stop.

stop() previously drained finalizers (with a timeout) BEFORE shutting down
the executor. A finalizer blocked on a child task running past the timeout
was cancelled; executor.shutdown(wait=True) then completed the child anyway
and its result was dropped — the task row stayed RUNNING until a reaper
recorded the finished work as WORKER_CRASHED.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.worker.config import WorkerConfig
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


@pytest.mark.asyncio
async def test_stop_persists_results_of_late_finishing_tasks() -> None:
    """A task finishing after the finalizer timeout still gets finalized.

    The child outruns finalizer_timeout_s; correct ordering shuts the
    executor down first (bounding child completion), after which the
    finalizer only has its persistence step left.
    """
    worker = _make_worker()
    pool = ThreadPoolExecutor(max_workers=1)
    # Duck-typed stand-in for the process pool: stop() only calls
    # shutdown(wait=True, cancel_futures=True).
    worker._executor = pool  # type: ignore[assignment]

    persisted: list[str] = []

    def child() -> str:
        time.sleep(1.0)
        return 'result'

    child_future = pool.submit(child)

    async def finalizer() -> None:
        value: Any = await asyncio.wrap_future(child_future)
        persisted.append(str(value))

    worker._spawn_background(finalizer(), name='finalize-test', finalizer=True)
    await asyncio.sleep(0.05)

    await worker.stop(finalizer_timeout_s=0.2)

    assert persisted == ['result'], (
        'Finalizer must persist the result of a child that finished after '
        'the drain timeout but before executor shutdown completed'
    )


@pytest.mark.asyncio
async def test_stop_force_cancels_finalizers_without_waiting() -> None:
    """force=True cancels finalizers immediately (results may be dropped)."""
    worker = _make_worker()
    pool = ThreadPoolExecutor(max_workers=1)
    worker._executor = pool  # type: ignore[assignment]

    persisted: list[str] = []
    block = asyncio.Event()

    async def finalizer() -> None:
        await block.wait()
        persisted.append('late')

    worker._spawn_background(finalizer(), name='finalize-test', finalizer=True)
    await asyncio.sleep(0.05)

    await worker.stop(force=True)

    assert persisted == [], 'force=True must not wait for finalizers'
