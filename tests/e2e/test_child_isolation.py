"""E2E: one child's failure stays with that child, through a real worker.

Each test enters through ``horsies worker`` and asserts on task rows. Worker
pids come from the rows, because the helper process may be ``uv``.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psutil
import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.preload import PreloadUnsupported, preload_support

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import unwrap_send
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_child_isolation as isolation
from tests.e2e.tasks import instance_child_isolation_prefetch as prefetch

INSTANCE = 'tests.e2e.tasks.instance_child_isolation:app'
PREFETCH_INSTANCE = 'tests.e2e.tasks.instance_child_isolation_prefetch:app'
SLOW_START_S = 60.0
FINISH_TIMEOUT_S = 40.0


@dataclass(slots=True)
class _StartPath:
    mode: ChildPreload = ChildPreload.NONE


_start_path = _StartPath()


@pytest.fixture(
    autouse=True,
    params=[ChildPreload.NONE, ChildPreload.LIBRARY, ChildPreload.APP],
    ids=['exec', 'library', 'app'],
)
def start_path(request: pytest.FixtureRequest) -> Iterator[None]:
    """Run every test here on each way a child can start. Preload needs Linux."""
    if request.param is not ChildPreload.NONE and isinstance(
        preload_support(),
        PreloadUnsupported,
    ):
        pytest.skip('this host cannot run the preload parent')
    _start_path.mode = request.param
    yield
    _start_path.mode = ChildPreload.NONE


def _worker(
    instance_path: str,
    *,
    extra_args: list[str] | None = None,
    **options: Any,
) -> Any:
    start_path_argument = f'--child-preload={_start_path.mode.value}'
    return run_worker(
        instance_path,
        extra_args=[*(extra_args or []), start_path_argument],
        **options,
    )


def _ready_check(task: Any) -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            sent = task.send()
            if is_err(sent):
                return False
            handle = sent.ok_value
        assert handle is not None
        return handle.get(timeout_ms=2000).is_ok()

    return _check


def _alive(pid: int) -> bool:
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


async def _until(predicate: Callable[[], bool], timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        assert time.monotonic() < deadline, 'condition not reached in time'
        await asyncio.sleep(0.05)


async def _row(broker: PostgresBroker, task_id: str) -> Any:
    async with current_resources(broker).session_factory() as session:
        result = await session.execute(
            text("""
                SELECT status, error_code, worker_pid
                FROM itest_task_rows
                WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
        return result.fetchone()


async def _running_child(broker: PostgresBroker, task_id: str) -> int:
    session_factory = current_resources(broker).session_factory
    await wait_for_status(session_factory, task_id, 'RUNNING', timeout_s=SLOW_START_S)
    row = await _row(broker, task_id)
    assert row.worker_pid is not None
    return int(row.worker_pid)


async def _assert_error(broker: PostgresBroker, task_id: str, code: str) -> None:
    row = await _row(broker, task_id)
    assert (row.status, row.error_code) == ('FAILED', code)


async def _wait_all(broker: PostgresBroker, task_ids: list[str], status: str) -> None:
    session_factory = current_resources(broker).session_factory
    for task_id in task_ids:
        await wait_for_status(session_factory, task_id, status, timeout_s=FINISH_TIMEOUT_S)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_leaves_healthy_siblings_running(
    isolation_broker: PostgresBroker,
) -> None:
    """#375: one timeout does not touch the other children."""
    with _worker(
        INSTANCE,
        processes=4,
        ready_check=_ready_check(isolation.healthcheck),
        timeout=SLOW_START_S,
    ):
        healthy = [
            unwrap_send(await isolation.sleep_task.send_async(seconds=4.0))
            for _ in range(3)
        ]
        children = [
            await _running_child(isolation_broker, handle.task_id) for handle in healthy
        ]
        hung = unwrap_send(await isolation.timeout_task.send_async(seconds=60.0))

        await wait_for_status(
            current_resources(isolation_broker).session_factory,
            hung.task_id,
            'FAILED',
            timeout_s=FINISH_TIMEOUT_S,
        )
        await _assert_error(isolation_broker, hung.task_id, 'TASK_TIMEOUT')
        await _wait_all(isolation_broker, [h.task_id for h in healthy], 'COMPLETED')
        assert all(_alive(pid) for pid in children)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_crash_leaves_healthy_siblings_running(
    isolation_broker: PostgresBroker,
) -> None:
    """#375: one crash fails that task only, at once and without the reaper."""
    with _worker(
        INSTANCE,
        processes=3,
        ready_check=_ready_check(isolation.healthcheck),
        timeout=SLOW_START_S,
    ):
        healthy = [
            unwrap_send(await isolation.sleep_task.send_async(seconds=4.0))
            for _ in range(2)
        ]
        victim = unwrap_send(await isolation.sleep_task.send_async(seconds=60.0))
        await _wait_all(
            isolation_broker,
            [handle.task_id for handle in [*healthy, victim]],
            'RUNNING',
        )
        os.kill(await _running_child(isolation_broker, victim.task_id), signal.SIGKILL)

        await wait_for_status(
            current_resources(isolation_broker).session_factory,
            victim.task_id,
            'FAILED',
            timeout_s=3.0,
        )
        await _assert_error(isolation_broker, victim.task_id, 'WORKER_CRASHED')
        await _wait_all(isolation_broker, [h.task_id for h in healthy], 'COMPLETED')


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_child_stops_when_the_parent_dies(
    isolation_broker: PostgresBroker,
) -> None:
    """#410: no orphan keeps running, heartbeating, or stamping the row."""
    with _worker(
        INSTANCE,
        processes=1,
        ready_check=_ready_check(isolation.healthcheck),
        timeout=SLOW_START_S,
    ):
        handle = unwrap_send(await isolation.sleep_task.send_async(seconds=60.0))
        child = await _running_child(isolation_broker, handle.task_id)
        parent = psutil.Process(child).ppid()
        os.kill(parent, signal.SIGKILL)

        await _until(lambda: not _alive(child), timeout_s=5.0)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_sigterm_to_every_process_still_drains(
    isolation_broker: PostgresBroker,
) -> None:
    """#411: systemd and Heroku signal every process, and tasks must finish."""
    with _worker(
        INSTANCE,
        processes=1,
        ready_check=_ready_check(isolation.healthcheck),
        timeout=SLOW_START_S,
    ):
        handle = unwrap_send(await isolation.sleep_task.send_async(seconds=3.0))
        child = await _running_child(isolation_broker, handle.task_id)
        parent = psutil.Process(child)
        parent = psutil.Process(parent.ppid())
        tree = [parent, *parent.children(recursive=True)]
        for process in tree:
            process.send_signal(signal.SIGTERM)

        await wait_for_status(
            current_resources(isolation_broker).session_factory,
            handle.task_id,
            'COMPLETED',
            timeout_s=FINISH_TIMEOUT_S,
        )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_does_not_count_the_wait_for_a_slot(
    isolation_prefetch_broker: PostgresBroker,
) -> None:
    """#412: each task runs 1.5 s of its 2 s limit, wherever it waited."""
    with _worker(
        PREFETCH_INSTANCE,
        processes=1,
        ready_check=_ready_check(prefetch.healthcheck),
        timeout=SLOW_START_S,
    ):
        handles = [
            unwrap_send(await prefetch.timeout_2s.send_async(seconds=1.5))
            for _ in range(3)
        ]
        await _wait_all(
            isolation_prefetch_broker,
            [handle.task_id for handle in handles],
            'COMPLETED',
        )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_kill_spares_the_next_task(
    isolation_prefetch_broker: PostgresBroker,
) -> None:
    """#413: a task ending near its deadline cannot cost the next task its run.

    The first task of each round ends close to its deadline. Whether it
    finishes or times out, the next task has no timeout and must complete.
    """
    session_factory = current_resources(isolation_prefetch_broker).session_factory
    with _worker(
        PREFETCH_INSTANCE,
        processes=1,
        ready_check=_ready_check(prefetch.healthcheck),
        timeout=SLOW_START_S,
    ):
        for round_index in range(20):
            body = 0.90 + 0.095 * (round_index % 20) / 19
            edge = unwrap_send(await prefetch.timeout_1s.send_async(seconds=body))
            following = unwrap_send(await prefetch.sleep_task.send_async(seconds=1.5))
            await wait_for_status(
                session_factory,
                following.task_id,
                'COMPLETED',
                timeout_s=FINISH_TIMEOUT_S,
            )
            assert edge.task_id


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_kill_reaches_task_subprocesses(
    isolation_broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    """#414: a timed-out task leaves no subprocess behind."""
    marker = tmp_path / 'subprocess_pid'
    with _worker(
        INSTANCE,
        processes=1,
        ready_check=_ready_check(isolation.healthcheck),
        timeout=SLOW_START_S,
    ):
        handle = unwrap_send(await isolation.spawn_then_hang.send_async(marker=str(marker)))
        await wait_for_status(
            current_resources(isolation_broker).session_factory,
            handle.task_id,
            'FAILED',
            timeout_s=FINISH_TIMEOUT_S,
        )
        await _assert_error(isolation_broker, handle.task_id, 'TASK_TIMEOUT')
        sub_pid = int(marker.read_text())
        try:
            await _until(lambda: not _alive(sub_pid), timeout_s=3.0)
        finally:
            if _alive(sub_pid):
                os.kill(sub_pid, signal.SIGKILL)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_forked_helper_does_not_hide_a_dead_child(
    isolation_broker: PostgresBroker,
) -> None:
    """#415: the worker sees the death while the helper still lives."""
    with _worker(
        INSTANCE,
        processes=1,
        ready_check=_ready_check(isolation.healthcheck),
        timeout=SLOW_START_S,
    ):
        handle = unwrap_send(
            await isolation.fork_helper_then_hang.send_async(helper_seconds=20.0),
        )
        child = await _running_child(isolation_broker, handle.task_id)
        await asyncio.sleep(0.5)
        os.kill(child, signal.SIGKILL)

        await wait_for_status(
            current_resources(isolation_broker).session_factory,
            handle.task_id,
            'FAILED',
            timeout_s=4.0,
        )
        await _assert_error(isolation_broker, handle.task_id, 'WORKER_CRASHED')


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize('start_method', ['spawn', 'forkserver'])
async def test_multiprocessing_works_inside_a_task(
    isolation_broker: PostgresBroker,
    start_method: str,
) -> None:
    """A task may start its own processes from the child program."""
    with _worker(
        INSTANCE,
        processes=1,
        ready_check=_ready_check(isolation.healthcheck),
        timeout=SLOW_START_S,
    ):
        handle = unwrap_send(
            await isolation.run_multiprocessing.send_async(start_method=start_method),
        )
        result = await handle.get_async(timeout_ms=int(FINISH_TIMEOUT_S * 1000))
        assert result.ok_value == '3'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_drain_deadline_records_the_crash_at_once(
    isolation_broker: PostgresBroker,
) -> None:
    """A task past --drain-timeout fails now, with no wait for the reaper."""
    with _worker(
        INSTANCE,
        processes=1,
        ready_check=_ready_check(isolation.healthcheck),
        timeout=SLOW_START_S,
        extra_args=['--drain-timeout=1'],
    ):
        handle = unwrap_send(await isolation.sleep_task.send_async(seconds=60.0))
        child = await _running_child(isolation_broker, handle.task_id)
        psutil.Process(psutil.Process(child).ppid()).send_signal(signal.SIGTERM)

        await wait_for_status(
            current_resources(isolation_broker).session_factory,
            handle.task_id,
            'FAILED',
            timeout_s=4.0,
        )
        await _assert_error(isolation_broker, handle.task_id, 'WORKER_CRASHED')
