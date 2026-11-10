"""E2E: ``SHARED_ASYNC`` tasks on the shared async children of a real worker.

The worker runs the ``split-isolated-1-shared-2x8`` profile: one isolated
child for the ``def`` ready check, and two shared async children with 8 seats
each. Many shared tasks run at once in those two children; a task that
outruns its timeout is cancelled in its child and fails with
``TASK_TIMEOUT``, and its neighbours finish.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest
from sqlalchemy import text

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import OutcomeCode
from horsies.core.task_decorator import TaskHandle

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import assert_ok, unwrap_get_result, unwrap_send
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import shared_async_tasks
from tests.e2e.tasks.profiles import SPLIT_SHARED_2X8, SPLIT_SHARED_SEATS

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
RESULT_TIMEOUT_MS = 60_000
SLEEP_S = 1.5
SLEEPERS = 12


def _make_ready_check() -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(basic_tasks.healthcheck.send())
        return handle.get(timeout_ms=2000).is_ok()

    return _check


async def _value(handle: TaskHandle[str]) -> str:
    result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)
    assert_ok(result)
    return result.unwrap()


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_shared_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished(
    broker: PostgresBroker,
) -> None:
    assert SLEEPERS <= 2 * SPLIT_SHARED_SEATS
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_2X8,
        ready_check=_make_ready_check(),
    ):
        isolated = unwrap_send(await basic_tasks.pid_task.send_async())
        isolated_result = await asyncio.to_thread(
            isolated.get,
            timeout_ms=RESULT_TIMEOUT_MS,
        )
        assert_ok(isolated_result)
        isolated_pid = str(isolated_result.unwrap())
        hang = unwrap_send(await shared_async_tasks.shared_hang.send_async())
        sleepers = [
            unwrap_send(
                await shared_async_tasks.shared_sleep.send_async(seconds=SLEEP_S)
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

        async with current_resources(broker).session_factory() as session:
            hang_row = (
                await session.execute(
                    text('SELECT error_code FROM itest_task_rows WHERE id = :id'),
                    {'id': hang.task_id},
                )
            ).fetchone()
            statuses = (
                (
                    await session.execute(
                        text(
                            'SELECT status FROM itest_task_rows WHERE id = ANY(:ids)',
                        ),
                        {'ids': [handle.task_id for handle in sleepers]},
                    )
                )
                .scalars()
                .all()
            )

    pids = {pid for pid, _started, _ended in stamps}
    starts = [float(started) for _pid, started, _ended in stamps]
    ends = [float(ended) for _pid, _started, ended in stamps]
    assert hang_row is not None
    assert hang_row.error_code == OutcomeCode.TASK_TIMEOUT.value
    assert sorted(statuses) == ['COMPLETED'] * SLEEPERS
    assert 1 <= len(pids) <= 2
    assert isolated_pid not in pids
    # Run one after another, 12 sleeps take 18 s; at once, about one sleep.
    assert max(ends) - min(starts) < SLEEPERS * SLEEP_S / 3
    assert max(starts) < min(ends)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_the_start_hook_runs_on_the_loop_of_the_tasks(
    broker: PostgresBroker,
) -> None:
    _ = broker
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_2X8,
        ready_check=_make_ready_check(),
    ):
        reports = [
            await _value(
                unwrap_send(await shared_async_tasks.shared_hook_loop.send_async()),
            )
            for _ in range(4)
        ]

    assert all(report.endswith(':True') for report in reports)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_shared_task_sends_with_send_async(
    broker: PostgresBroker,
    app: Horsies,
) -> None:
    _ = broker
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_2X8,
        ready_check=_make_ready_check(),
    ):
        for x in (5, 6):
            sent_id = await _value(
                unwrap_send(await shared_async_tasks.shared_sends.send_async(x=x)),
            )
            result = unwrap_get_result(
                await asyncio.to_thread(
                    app.get_result,
                    sent_id,
                    timeout_ms=RESULT_TIMEOUT_MS,
                ),
            )
            assert_ok(result)
            assert result.unwrap() == x * 2
