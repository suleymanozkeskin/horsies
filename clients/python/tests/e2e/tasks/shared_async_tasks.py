"""``SHARED_ASYNC`` task definitions for e2e tests.

A worker with the ``split-isolated-1-shared-2x8`` profile runs these on its
shared async children; a worker without shared children runs them isolated.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time

from horsies.core.models.tasks import Execution, RetryPolicy, TaskError
from horsies.core.types.result import Err, Ok, Result

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
    name='e2e_shared_sleep',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=30_000,
)
async def shared_sleep(*, seconds: float) -> Result[str, TaskError]:
    """Sleep on the loop. Return ``pid:start:end`` in seconds since the epoch."""
    started = time.time()
    await asyncio.sleep(seconds)
    return Ok(f'{os.getpid()}:{started}:{time.time()}')


@app.task(
    name='e2e_shared_task_send_pool',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=30_000,
)
async def shared_task_send_pool() -> Result[str, TaskError]:
    """The pool of the app broker that task code in this child opens when it
    sends: ``pool_size:max_overflow``."""
    config = app.get_broker().config
    return Ok(f'{config.pool_size}:{config.max_overflow}')


HELD_EXECUTOR_CALLS = 8
"""The default executor calls that ``e2e_shared_executor_threads`` holds at
once: more than any executor size that the e2e profiles set."""


@app.task(
    name='e2e_shared_executor_threads',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=30_000,
)
async def shared_executor_threads() -> Result[int, TaskError]:
    """The threads that run ``HELD_EXECUTOR_CALLS`` calls of the default
    executor, each held until all are sent: the size of that executor."""
    names: set[str] = set()
    lock = threading.Lock()
    release = threading.Event()

    def held_call() -> None:
        with lock:
            names.add(threading.current_thread().name)
        _ = release.wait(10.0)

    loop = asyncio.get_running_loop()
    pending = [
        loop.run_in_executor(None, held_call) for _ in range(HELD_EXECUTOR_CALLS)
    ]
    await asyncio.sleep(0.2)
    release.set()
    _ = await asyncio.gather(*pending)
    return Ok(len(names))


@app.task(
    name='e2e_shared_hang',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=HANG_TIMEOUT_MS,
    cancel_grace_ms=1_000,
)
async def shared_hang() -> Result[str, TaskError]:
    """Wait far past the timeout; the cancel at the deadline ends it."""
    await asyncio.sleep(3600)
    return Ok('never')


STOP_TIMEOUT_MS = 2_000
"""The timeout of the tasks that do not stop: short, to keep the tests short."""


def _first_attempt(marker: str) -> bool:
    """True on the first call with ``marker``: the call creates the file."""
    try:
        with open(marker, 'x', encoding='ascii') as created:
            created.write(str(os.getpid()))
    except FileExistsError:
        return False
    return True


@app.task(
    name='e2e_shared_stuck_once',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=STOP_TIMEOUT_MS,
    cancel_grace_ms=100,
    retry_policy=RetryPolicy.fixed([1], retry_for=['TASK_TIMEOUT']),
)
async def shared_stuck_once(*, marker: str) -> Result[str, TaskError]:
    """The first attempt ignores every cancel; the retry returns its pid."""
    if _first_attempt(marker):
        while True:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                continue
    return Ok(str(os.getpid()))


@app.task(
    name='e2e_shared_blocks_once',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=30_000,
    retry_policy=RetryPolicy.fixed([1], retry_for=['TASK_BLOCKED_LOOP']),
)
async def shared_blocks_once(*, marker: str) -> Result[str, TaskError]:
    """The first attempt blocks the event loop past the block limit; the
    retry returns its pid."""
    if _first_attempt(marker):
        time.sleep(30)
    return Ok(str(os.getpid()))


LAZY_CLIENT_TIMEOUT_MS = 1_000
LAZY_CLIENT_GRACE_MS = 1_500
"""The background task of the client outlives the deadline of the task that
opened it, and is cancelled after the grace."""

# The background task of a client opened on first use, in this child.
_LAZY_CLIENT: list[asyncio.Task[None]] = []


async def _keepalive() -> None:
    while True:
        await asyncio.sleep(0.05)


@app.task(
    name='e2e_shared_lazy_client',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=LAZY_CLIENT_TIMEOUT_MS,
    cancel_grace_ms=LAZY_CLIENT_GRACE_MS,
)
async def shared_lazy_client() -> Result[str, TaskError]:
    """Open a client on first use, not in the start hook: its background
    task carries the mark of this task. Return the pid."""
    if not _LAZY_CLIENT:
        _LAZY_CLIENT.append(asyncio.get_running_loop().create_task(_keepalive()))
    return Ok(str(os.getpid()))


@app.task(
    name='e2e_shared_hook_loop',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
)
async def shared_hook_loop() -> Result[str, TaskError]:
    """Report whether the start hook ran on the loop of this task."""
    same = len(_HOOK_LOOPS) == 1 and _HOOK_LOOPS[0] is asyncio.get_running_loop()
    return Ok(f'{os.getpid()}:{same}')


@app.task(
    name='e2e_shared_sends',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
)
async def shared_sends(*, x: int) -> Result[str, TaskError]:
    """Send a task with ``send_async`` from a shared task. Return its id."""
    match await simple_task.send_async(x=x):
        case Ok(handle):
            return Ok(str(handle.task_id))
        case Err(send_err):
            return Err(
                TaskError(error_code='SEND_FAILED', message=send_err.message),
            )
