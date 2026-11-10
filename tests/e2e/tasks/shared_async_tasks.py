"""``SHARED_ASYNC`` task definitions for e2e tests.

A worker with the ``split-isolated-1-shared-2x8`` profile runs these on its
shared async children; a worker without shared children runs them isolated.
"""

from __future__ import annotations

import asyncio
import os
import time

from horsies.core.models.tasks import Execution, TaskError, TaskResult
from horsies.core.types.result import Err, Ok

from tests.e2e.tasks.basic import simple_task
from tests.e2e.tasks.instance import app

HANG_TIMEOUT_MS = 2_000
"""The timeout of ``e2e_shared_hang``: the smallest that keeps the test short."""

# The loop on which the start hook of this child ran.
_HOOK_LOOPS: list[asyncio.AbstractEventLoop] = []


@app.on_shared_async_child_start
async def remember_the_loop() -> None:
    """Keep the loop of the child, as a long-lived client would."""
    _HOOK_LOOPS.append(asyncio.get_running_loop())


@app.task(
    task_name='e2e_shared_sleep',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=30_000,
)
async def shared_sleep(*, seconds: float) -> TaskResult[str, TaskError]:
    """Sleep on the loop. Return ``pid:start:end`` in seconds since the epoch."""
    started = time.time()
    await asyncio.sleep(seconds)
    return TaskResult(ok=f'{os.getpid()}:{started}:{time.time()}')


@app.task(
    task_name='e2e_shared_hang',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=HANG_TIMEOUT_MS,
    cancel_grace_ms=1_000,
)
async def shared_hang() -> TaskResult[str, TaskError]:
    """Wait far past the timeout; the cancel at the deadline ends it."""
    await asyncio.sleep(3600)
    return TaskResult(ok='never')


@app.task(
    task_name='e2e_shared_hook_loop',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
)
async def shared_hook_loop() -> TaskResult[str, TaskError]:
    """Report whether the start hook ran on the loop of this task."""
    same = len(_HOOK_LOOPS) == 1 and _HOOK_LOOPS[0] is asyncio.get_running_loop()
    return TaskResult(ok=f'{os.getpid()}:{same}')


@app.task(
    task_name='e2e_shared_sends',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
)
async def shared_sends(*, x: int) -> TaskResult[str, TaskError]:
    """Send a task with ``send_async`` from a shared task. Return its id."""
    match await simple_task.send_async(x=x):
        case Ok(handle):
            return TaskResult(ok=handle.task_id)
        case Err(send_err):
            return TaskResult(
                err=TaskError(error_code='SEND_FAILED', message=send_err.message),
            )
