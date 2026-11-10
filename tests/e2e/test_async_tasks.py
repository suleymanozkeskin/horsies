"""E2E: `async def` tasks run by a real worker.

A worker child keeps one event loop for its whole life. An async task runs
on that loop, and the loop runs only while an async task runs. A timeout
kills the child, as for a `def` task.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

import pytest
from sqlalchemy import text

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import OutcomeCode
from horsies.core.task_decorator import TaskHandle

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import (
    assert_ok,
    start_ok_in_thread,
    unwrap_get_result,
    unwrap_send,
)
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.helpers.workflow import wait_for_workflow_completion
from tests.e2e.tasks import async_tasks
from tests.e2e.tasks import basic as basic_tasks


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
RESULT_TIMEOUT_MS = 30_000


def _make_ready_check() -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(basic_tasks.healthcheck.send())
        return handle.get(timeout_ms=2000).is_ok()

    return _check


async def _result_of(handle: TaskHandle[str]) -> str:
    result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)
    assert_ok(result)
    return result.unwrap()


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_async_tasks_of_one_child_share_one_loop(broker: PostgresBroker) -> None:
    """Two async tasks in one child run on one loop. A def task between
    them sees no running loop and can run its own with asyncio.run()."""
    _ = broker
    with run_worker(DEFAULT_INSTANCE, processes=1, ready_check=_make_ready_check()):
        first = await _result_of(
            unwrap_send(await async_tasks.async_loop_report.send_async()),
        )
        between = await _result_of(
            unwrap_send(await async_tasks.sync_loop_report.send_async()),
        )
        last = await _result_of(
            unwrap_send(await async_tasks.async_loop_report.send_async()),
        )

    first_pid, first_loops = first.split(':')
    between_pid, between_running, between_ran = between.split(':')
    last_pid, last_loops = last.split(':')
    assert first_pid == between_pid == last_pid
    assert (first_loops, last_loops) == ('1', '1')
    assert (between_running, between_ran) == ('False', 'ran')


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_an_async_task_sends_with_send_async(
    broker: PostgresBroker,
    app: Horsies,
) -> None:
    """send_async in an async task enqueues a task that a worker runs.
    Two sends in one child use the broker set of the one task loop."""
    _ = broker
    with run_worker(DEFAULT_INSTANCE, processes=1, ready_check=_make_ready_check()):
        for x in (3, 4):
            sender = unwrap_send(await async_tasks.async_sends.send_async(x=x))
            sent_id = await _result_of(sender)
            result = unwrap_get_result(
                await asyncio.to_thread(
                    app.get_result,
                    sent_id,
                    timeout_ms=RESULT_TIMEOUT_MS,
                ),
            )
            assert_ok(result)
            assert result.unwrap() == x * 2


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_an_async_task_runs_as_a_workflow_node(broker: PostgresBroker) -> None:
    """A def node feeds an async def node, which feeds a def node."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = await start_ok_in_thread(async_tasks.spec_mixed_chain)

        status = await wait_for_workflow_completion(
            current_resources(broker).session_factory,
            handle.workflow_id,
            timeout_s=30.0,
        )
        assert status == 'COMPLETED'

        result = await asyncio.to_thread(handle.get, timeout_ms=1000)
        assert_ok(result)
        assert result.unwrap() == 12


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_an_async_task_timeout_kills_its_child_and_the_worker_survives(
    broker: PostgresBroker,
) -> None:
    """An async task that awaits past timeout_ms fails with TASK_TIMEOUT.
    The worker starts a new child, and a later async task completes."""
    with run_worker(DEFAULT_INSTANCE, processes=1, ready_check=_make_ready_check()):
        handle = unwrap_send(
            await async_tasks.async_timeout_sleeper.send_async(duration_ms=60_000),
        )

        await wait_for_status(
            current_resources(broker).session_factory,
            handle.task_id,
            'FAILED',
            timeout_s=20.0,
        )

        async with current_resources(broker).session_factory() as session:
            row = (
                await session.execute(
                    text("""
                        SELECT error_code, result
                        FROM itest_task_rows WHERE id = :id
                    """),
                    {'id': handle.task_id},
                )
            ).fetchone()
        assert row is not None
        assert row.error_code == OutcomeCode.TASK_TIMEOUT.value
        assert json.loads(row.result)['err']['error_code'] == {
            '__builtin_task_code__': OutcomeCode.TASK_TIMEOUT.value,
        }

        follow_up = await _result_of(
            unwrap_send(await async_tasks.async_loop_report.send_async()),
        )
        assert follow_up.endswith(':1')
