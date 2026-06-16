"""E2E: max_memory_per_child_mb recycles children by retained RSS.

alloc_pid_task retains memory per child and returns its PID. With a memory
threshold set, a child's RSS ratchets up across tasks until it crosses the
threshold and the child recycles, so distinct PIDs exceed `processes` — while
every task still reaches a terminal state. A task that raises after allocating
is recorded as a failure without breaking the pool, and the worker keeps
recycling and processing.

The threshold (300MB) clears the warmed child baseline with margin so the
startup baseline guard does not hard-fail; the per-task allocation (120MB)
guarantees the threshold is crossed within the burst.
"""

from __future__ import annotations

import pytest

from horsies.core.types.result import is_err

from tests.e2e.helpers.assertions import assert_err, assert_ok, unwrap_send
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
PROCESSES = 2
BURST = 16
THRESHOLD_MB = 300
ALLOC_MB = 120


def _ready_check():
    handle = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            r = basic_tasks.healthcheck.send()
            if is_err(r):
                return False
            handle = r.ok_value
        return handle.get(timeout_ms=2000).is_ok()

    return _check


@pytest.mark.e2e
def test_max_memory_per_child_rotates_pids() -> None:
    """Children recycle as retained RSS crosses the threshold; all tasks OK."""
    with run_worker(
        DEFAULT_INSTANCE,
        processes=PROCESSES,
        extra_args=[f'--max-memory-per-child-mb={THRESHOLD_MB}'],
        ready_check=_ready_check(),
    ):
        handles = [
            unwrap_send(basic_tasks.alloc_pid_task.send(mb=ALLOC_MB))
            for _ in range(BURST)
        ]
        pids: set[int] = set()
        for handle in handles:
            result = handle.get(timeout_ms=30_000)
            assert_ok(result)
            pids.add(result.unwrap())
        assert len(pids) > PROCESSES, (
            f'expected > {PROCESSES} distinct child PIDs with memory recycling, '
            f'got {len(pids)}: {pids}'
        )


@pytest.mark.e2e
def test_memory_recycle_survives_task_exceptions() -> None:
    """A task that raises after allocating is failed cleanly; the pool survives.

    The raising tasks must persist as failures (the worker-loop exception
    branch still attaches the recycle marker), and the interleaved alloc tasks
    must all complete while children keep rotating — proving no BrokenProcessPool.
    """
    with run_worker(
        DEFAULT_INSTANCE,
        processes=PROCESSES,
        extra_args=[f'--max-memory-per-child-mb={THRESHOLD_MB}'],
        ready_check=_ready_check(),
    ):
        raise_handles = [
            unwrap_send(basic_tasks.alloc_then_raise_task.send(mb=ALLOC_MB))
            for _ in range(4)
        ]
        ok_handles = [
            unwrap_send(basic_tasks.alloc_pid_task.send(mb=ALLOC_MB))
            for _ in range(BURST)
        ]
        for handle in raise_handles:
            assert_err(handle.get(timeout_ms=30_000))
        pids: set[int] = set()
        for handle in ok_handles:
            result = handle.get(timeout_ms=30_000)
            assert_ok(result)
            pids.add(result.unwrap())
        assert len(pids) > PROCESSES, (
            f'worker stalled or pool broke: only {len(pids)} distinct PIDs '
            f'across {BURST} tasks: {pids}'
        )
