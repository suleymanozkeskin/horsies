"""Warmup lands one call on every child, whatever their start-time spread.

Real spawned pools: each child stores the warmup barrier in its
initializer, as `_child_initializer` does, and one child finishes its
initializer later than the others. Every warmup call waits at the barrier,
so the calls must reach distinct children.
"""

from __future__ import annotations

import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Barrier

import pytest

from horsies.core.worker import worker as worker_module
from horsies.core.worker.runtime import (
    WarmablePool,
    WarmupIncompleteError,
    _set_child_warmup_barrier,
)
from horsies.core.worker.worker import Worker, WorkerConfig

pytestmark = [pytest.mark.unit]

PROCESSES = 3


def _initializer(
    barrier: Barrier,
    started: Synchronized[int],
    late_child_delay_seconds: float,
) -> None:
    """Store the barrier; the last child to start finishes late."""
    _set_child_warmup_barrier(barrier)
    with started.get_lock():
        started.value += 1
        order = started.value
    if order == PROCESSES:
        time.sleep(late_child_delay_seconds)


def _pool(late_child_delay_seconds: float) -> WarmablePool:
    context = multiprocessing.get_context('spawn')
    barrier = context.Barrier(PROCESSES)
    started = context.Value('i', 0)
    return WarmablePool(
        executor=ProcessPoolExecutor(
            max_workers=PROCESSES,
            mp_context=context,
            initializer=_initializer,
            initargs=(barrier, started, late_child_delay_seconds),
        ),
        warmup_barrier=barrier,
    )


def _worker() -> Worker:
    return Worker(
        session_factory=None,  # type: ignore[arg-type]
        listener=None,  # type: ignore[arg-type]
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
            processes=PROCESSES,
        ),
    )


class TestWarmupBarrier:
    @pytest.mark.asyncio
    async def test_a_late_child_still_receives_its_warmup_call(self) -> None:
        """A start-time spread of 1.5 s, far above one warmup call."""
        pool = _pool(late_child_delay_seconds=1.5)
        try:
            await _worker()._warm_executor(pool)  # pyright: ignore[reportPrivateUsage]
            children = {
                process.pid
                for process in pool.executor._processes.values()  # pyright: ignore[reportPrivateUsage]
            }
            assert len(children) == PROCESSES
            assert os.getpid() not in children
        finally:
            pool.executor.shutdown(wait=True, cancel_futures=True)

    @pytest.mark.asyncio
    async def test_a_child_later_than_the_bound_fails_with_the_cause(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(worker_module, 'CHILD_WARMUP_BARRIER_TIMEOUT_S', 0.5)
        pool = _pool(late_child_delay_seconds=3.0)
        try:
            with pytest.raises(
                WarmupIncompleteError,
                match=(
                    r'not all 3 child process\(es\) reached the warmup '
                    r'barrier within 0.5s'
                ),
            ):
                await _worker()._warm_executor(pool)  # pyright: ignore[reportPrivateUsage]
        finally:
            pool.warmup_barrier.abort()
            pool.executor.shutdown(wait=True, cancel_futures=True)
