"""E2E: max_tasks_per_child recycles executor children while tasks complete.

The pid_task returns the executing child PID. Without recycling, every task
runs on one of the `processes` long-lived children, so the distinct-PID count
is bounded by the child count. With a recycle-after task limit, children are
retired and respawned during the burst, so distinct PIDs exceed the child count
— and every task must still reach a terminal OK state.
"""

from __future__ import annotations

import pytest

from horsies.core.types.result import is_err

from tests.e2e.helpers.assertions import assert_ok, unwrap_send
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks.profiles import RECYCLE_AFTER_2_TASKS, RECYCLE_OFF

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
# Both profiles run 2 isolated children.
PROCESSES = 2
BURST = 16
SLEEP_MS = 20


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


def _run_burst(timeout_ms: int) -> set[int]:
    """Send the burst, assert every task completes OK, return distinct PIDs."""
    handles = [
        unwrap_send(basic_tasks.pid_task.send(sleep_ms=SLEEP_MS))
        for _ in range(BURST)
    ]
    pids: set[int] = set()
    for handle in handles:
        result = handle.get(timeout_ms=timeout_ms)
        assert_ok(result)
        pids.add(result.unwrap())
    return pids


@pytest.mark.e2e
def test_recycle_disabled_bounds_pids_by_process_count() -> None:
    """recycle after=NoTaskLimit(): recycling off, distinct PIDs <= child count."""
    with run_worker(
        DEFAULT_INSTANCE,
        profile=RECYCLE_OFF,
        ready_check=_ready_check(),
    ):
        pids = _run_burst(timeout_ms=10_000)
        assert len(pids) <= PROCESSES, (
            f'expected <= {PROCESSES} distinct child PIDs with recycling off, '
            f'got {len(pids)}: {pids}'
        )


@pytest.mark.e2e
def test_max_tasks_per_child_rotates_pids() -> None:
    """recycle after=TaskLimit(tasks=2): children rotate, all tasks complete."""
    with run_worker(
        DEFAULT_INSTANCE,
        profile=RECYCLE_AFTER_2_TASKS,
        ready_check=_ready_check(),
    ):
        pids = _run_burst(timeout_ms=20_000)
        assert len(pids) > PROCESSES, (
            f'expected > {PROCESSES} distinct child PIDs with recycling, '
            f'got {len(pids)}: {pids}'
        )
