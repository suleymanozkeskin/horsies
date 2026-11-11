"""E2E: each child kind opens the pool that its profile sets, and a shared
async child sizes its default executor.

One worker of ``set-pools-1-1x4-1x4`` runs an isolated child, a shared async
child and a shared thread child, each with a pool or an executor size that
differs from every default. A task on each child reports what its child
opened: the profile reaches the child through the CLI, the worker config,
the init frame and the boot of the child.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.task_decorator import TaskHandle

from tests.e2e.helpers.assertions import assert_ok, unwrap_send
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import shared_async_tasks, shared_thread_tasks
from tests.e2e.tasks.profiles import (
    SET_POOLS,
    SET_POOLS_EXECUTOR_THREADS,
    SET_POOLS_ISOLATED_BOUNDS,
    SET_POOLS_THREAD_BOUNDS,
)

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
RESULT_TIMEOUT_MS = 60_000


def _make_ready_check() -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(basic_tasks.healthcheck.send())
        return handle.get(timeout_ms=2000).is_ok()

    return _check


async def _value[Value](handle: TaskHandle[Value]) -> Value:
    result = await asyncio.to_thread(handle.get, timeout_ms=RESULT_TIMEOUT_MS)
    assert_ok(result)
    return result.unwrap()


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_each_child_kind_opens_the_pool_and_executor_of_its_profile(
    broker: PostgresBroker,
) -> None:
    _ = broker
    with run_worker(
        DEFAULT_INSTANCE,
        profile=SET_POOLS,
        ready_check=_make_ready_check(),
    ):
        isolated = await _value(
            unwrap_send(await basic_tasks.child_pool_bounds.send_async()),
        )
        thread = await _value(
            unwrap_send(await shared_thread_tasks.thread_pool_bounds.send_async()),
        )
        executor_threads = await _value(
            unwrap_send(
                await shared_async_tasks.shared_executor_threads.send_async(),
            ),
        )

    assert isolated == '{}:{}'.format(*SET_POOLS_ISOLATED_BOUNDS)
    assert thread == '{}:{}'.format(*SET_POOLS_THREAD_BOUNDS)
    assert executor_threads == SET_POOLS_EXECUTOR_THREADS
    assert executor_threads < shared_async_tasks.HELD_EXECUTOR_CALLS
