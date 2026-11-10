"""E2E: ``SHARED_THREAD`` tasks on the shared thread children of a real worker.

Many ``def`` tasks run at once on the seat threads of the thread children. A
task that outruns its timeout cannot be cancelled: its child closes, and the
worker kills it when no task in it has time left. That task fails with
``TASK_TIMEOUT``; a neighbour with time left finishes first.

A thread row whose stored timeout the worker cannot use fails once with
``WORKER_SERIALIZATION_ERROR``, on a profile with an isolated child and on
``thread-only-1x4``, which has none.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import OperationalErrorCode, OutcomeCode
from horsies.core.task_decorator import TaskHandle

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import assert_ok, unwrap_send
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import shared_thread_tasks
from tests.e2e.tasks.profiles import (
    SPLIT_THREAD_2X4,
    SPLIT_THREAD_SEATS,
    THREAD_ONLY_1X4,
)

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
RESULT_TIMEOUT_MS = 60_000
SLEEP_S = 1.5
SLEEPERS = 6
NEIGHBOUR_S = 4.0


def _make_ready_check() -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(basic_tasks.healthcheck.send())
        return handle.get(timeout_ms=2000).is_ok()

    return _check


def _make_thread_ready_check() -> Callable[[], bool]:
    """A ready check that a worker with no isolated child can answer."""
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(shared_thread_tasks.thread_sleep.send(seconds=0))
        return handle.get(timeout_ms=2000).is_ok()

    return _check


READY_CHECKS: dict[str, Callable[[], Callable[[], bool]]] = {
    SPLIT_THREAD_2X4: _make_ready_check,
    THREAD_ONLY_1X4: _make_thread_ready_check,
}


async def _value(handle: TaskHandle[str]) -> str:
    result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)
    assert_ok(result)
    return result.unwrap()


async def _rows(
    broker: PostgresBroker, task_ids: list[str]
) -> dict[str, tuple[str, str | None]]:
    async with current_resources(broker).session_factory() as session:
        rows = await session.execute(
            text(
                'SELECT id, status, error_code FROM itest_task_rows WHERE id = ANY(:ids)'
            ),
            {'ids': task_ids},
        )
        return {str(row.id): (str(row.status), row.error_code) for row in rows}


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_thread_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished(
    broker: PostgresBroker,
) -> None:
    assert SLEEPERS + 1 <= 2 * SPLIT_THREAD_SEATS
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_THREAD_2X4,
        ready_check=_make_ready_check(),
    ):
        isolated = unwrap_send(await basic_tasks.pid_task.send_async())
        isolated_result = await asyncio.to_thread(
            isolated.get, timeout_ms=RESULT_TIMEOUT_MS
        )
        assert_ok(isolated_result)
        isolated_pid = str(isolated_result.unwrap())
        hang = unwrap_send(await shared_thread_tasks.thread_hang.send_async())
        sleepers = [
            unwrap_send(
                await shared_thread_tasks.thread_sleep.send_async(seconds=SLEEP_S)
            )
            for _ in range(SLEEPERS)
        ]
        stamps = [(await _value(handle)).split(':') for handle in sleepers]
        await wait_for_status(
            current_resources(broker).session_factory,
            hang.task_id,
            'FAILED',
            timeout_s=30.0,
        )
        rows = await _rows(broker, [hang.task_id, *(item.task_id for item in sleepers)])

    pids = {pid for pid, _thread, _started, _ended in stamps}
    threads = {thread for _pid, thread, _started, _ended in stamps}
    starts = [float(started) for _pid, _thread, started, _ended in stamps]
    ends = [float(ended) for _pid, _thread, _started, ended in stamps]
    assert rows[hang.task_id] == ('FAILED', OutcomeCode.TASK_TIMEOUT.value)
    assert [rows[item.task_id][0] for item in sleepers] == ['COMPLETED'] * SLEEPERS
    assert 1 <= len(pids) <= 2
    assert isolated_pid not in pids
    assert all(thread.startswith('horsies-seat-') for thread in threads)
    # One after another, 6 sleeps take 9 s; at once, about one sleep.
    assert max(ends) - min(starts) < SLEEPERS * SLEEP_S / 3
    assert max(starts) < min(ends)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_timed_out_task_ends_after_its_neighbour_with_time_left(
    broker: PostgresBroker,
) -> None:
    """One child: the timeout closes it at 2 s, the neighbour finishes at
    4 s, and only then is the child killed and the timeout written."""
    session_factory = current_resources(broker).session_factory
    with run_worker(
        DEFAULT_INSTANCE,
        profile=THREAD_ONLY_1X4,
        ready_check=_make_thread_ready_check(),
    ):
        hang = unwrap_send(await shared_thread_tasks.thread_hang.send_async())
        neighbour = unwrap_send(
            await shared_thread_tasks.thread_sleep.send_async(seconds=NEIGHBOUR_S),
        )
        neighbour_pid, _thread, _started, neighbour_end = (
            await _value(neighbour)
        ).split(':')
        await wait_for_status(session_factory, hang.task_id, 'FAILED', timeout_s=30.0)
        after = await _value(
            unwrap_send(await shared_thread_tasks.thread_sleep.send_async(seconds=0)),
        )

    async with session_factory() as session:
        hang_row = (
            await session.execute(
                text(
                    'SELECT error_code, EXTRACT(EPOCH FROM failed_at) AS failed_at '
                    'FROM itest_task_rows WHERE id = :id',
                ),
                {'id': hang.task_id},
            )
        ).one()
    assert hang_row.error_code == OutcomeCode.TASK_TIMEOUT.value
    assert float(hang_row.failed_at) >= float(neighbour_end)
    assert after.split(':')[0] != neighbour_pid


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize(
    'profile',
    [SPLIT_THREAD_2X4, THREAD_ONLY_1X4],
    ids=['isolated-and-thread-children', 'thread-children-only'],
)
async def test_a_thread_row_with_no_stored_timeout_fails_once(
    broker: PostgresBroker,
    profile: str,
) -> None:
    """The row is written while no worker runs, then its stored options lose
    the timeout that a ``SHARED_THREAD`` row needs. The worker fails it
    before the task resolves; it never runs, and never returns to the queue."""
    session_factory = current_resources(broker).session_factory
    handle = unwrap_send(await shared_thread_tasks.thread_sleep.send_async(seconds=0))
    async with session_factory() as session:
        stored = (
            await session.execute(
                text('SELECT task_options FROM horsies_tasks WHERE id = :id'),
                {'id': handle.task_id},
            )
        ).scalar_one()
        options = json.loads(stored)
        del options['timeout_ms']
        await session.execute(
            text('UPDATE horsies_tasks SET task_options = :opts WHERE id = :id'),
            {'opts': json.dumps(options), 'id': handle.task_id},
        )
        await session.commit()

    with run_worker(
        DEFAULT_INSTANCE,
        profile=profile,
        ready_check=READY_CHECKS[profile](),
    ):
        await wait_for_status(session_factory, handle.task_id, 'FAILED', timeout_s=30.0)

    async with session_factory() as session:
        row = (
            await session.execute(
                text(
                    'SELECT error_code, result, retry_count FROM itest_task_rows '
                    'WHERE id = :id',
                ),
                {'id': handle.task_id},
            )
        ).one()
    match row.result:
        case str() as text_value:
            result = json.loads(text_value)
        case decoded:
            result = decoded
    assert row.error_code == OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value
    assert row.retry_count == 0
    assert '"field": "timeout_ms"' in json.dumps(result)
