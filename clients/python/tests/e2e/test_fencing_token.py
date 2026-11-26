"""E2E: the fencing token that a task reads from its ``TaskRuntime``.

One worker with an isolated, a shared async and a shared thread child runs a
task of each execution mode; each returns the token of its run. A retry runs
with a greater value, and two reads in one attempt are equal. The Rust client
has the same tests in ``clients/rust/tests/worker/tests/layer1_tasks.rs`` and
``child_model.rs``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from horsies import FencingToken
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.task_decorator import TaskHandle

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import read_succeeded, succeeded_value, unwrap_send
from tests.e2e.helpers.db import read_task
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import fencing_tasks
from tests.e2e.tasks.profiles import SET_POOLS

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
RESULT_TIMEOUT_MS = 30_000


def _make_ready_check() -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(basic_tasks.healthcheck.send())
        return read_succeeded(handle.get(timeout_ms=2000))

    return _check


async def _value(handle: TaskHandle[Any]) -> Any:
    result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)
    return succeeded_value(result)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_each_execution_mode_gives_the_token_of_its_run(
    broker: PostgresBroker,
) -> None:
    _ = broker
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SET_POOLS,
        ready_check=_make_ready_check(),
    ):
        handles = [
            unwrap_send(await fencing_tasks.fence_isolated_def.send_async()),
            unwrap_send(await fencing_tasks.fence_isolated_async.send_async()),
            unwrap_send(await fencing_tasks.fence_shared_async.send_async()),
            unwrap_send(await fencing_tasks.fence_shared_thread.send_async()),
        ]
        tokens = [await _value(handle) for handle in handles]

    assert tokens == [
        FencingToken(task_id=handle.task_id, value=1) for handle in handles
    ]


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_two_reads_in_one_attempt_are_equal(broker: PostgresBroker) -> None:
    _ = broker
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SET_POOLS,
        ready_check=_make_ready_check(),
    ):
        handle = unwrap_send(await fencing_tasks.fence_reads_twice.send_async())
        first, second = await _value(handle)

    assert first == second == FencingToken(task_id=handle.task_id, value=1)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_retry_runs_with_a_greater_value(broker: PostgresBroker) -> None:
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SET_POOLS,
        ready_check=_make_ready_check(),
    ):
        handle = unwrap_send(await fencing_tasks.fence_retry.send_async())
        token = await _value(handle)

    async with current_resources(broker).session_factory() as session:
        task = await read_task(session, handle.task_id)
    assert task is not None
    assert task.retry_count == 1
    assert token == FencingToken(task_id=handle.task_id, value=2)
