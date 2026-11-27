"""Task definitions that return the fencing token of their run, one per
execution mode, for ``tests/e2e/test_fencing_token.py``.

A worker with the ``set-pools-1-1x4-1x4`` profile runs each on a child of
its class: the ``def`` and ``async def`` tasks on the isolated child, the
others on a shared async and a shared thread child.
"""

from __future__ import annotations

import asyncio
from enum import Enum

from horsies import FencingToken, TaskRuntime
from horsies.core.models.tasks import Execution, RetryPolicy, TaskError
from horsies.core.types.result import Err, Ok, Result

from tests.e2e.tasks.instance import app

SHARED_TIMEOUT_MS = 30_000
"""The timeout of the shared tasks: a shared class needs one."""


class FenceCode(str, Enum):
    """The code of the first attempt of ``e2e_fence_retry``, which the retry
    policy retries."""

    FENCE_FIRST_ATTEMPT = 'FENCE_FIRST_ATTEMPT'


RETRY_CODE = FenceCode.FENCE_FIRST_ATTEMPT.value
"""The text of `FenceCode.FENCE_FIRST_ATTEMPT`, which the first attempt
returns."""


@app.task(name='e2e_fence_isolated_def')
def fence_isolated_def(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
    """The token of this run, in a ``def`` body on an isolated child."""
    return Ok(runtime.fencing_token)


@app.task(name='e2e_fence_isolated_async')
async def fence_isolated_async(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
    """The token of this run, in an ``async def`` body on an isolated child."""
    await asyncio.sleep(0)
    return Ok(runtime.fencing_token)


@app.task(
    name='e2e_fence_shared_async',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
)
async def fence_shared_async(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
    """The token of this run, on a seat of a shared async child."""
    await asyncio.sleep(0)
    return Ok(runtime.fencing_token)


@app.task(
    name='e2e_fence_shared_thread',
    execution=Execution.SHARED_THREAD,
    timeout_ms=SHARED_TIMEOUT_MS,
)
def fence_shared_thread(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
    """The token of this run, on a seat of a shared thread child."""
    return Ok(runtime.fencing_token)


@app.task(name='e2e_fence_reads_twice')
async def fence_reads_twice(runtime: TaskRuntime) -> Result[list[FencingToken], TaskError]:
    """The token read at the start of the body and again after an await."""
    first = runtime.fencing_token
    await asyncio.sleep(0.05)
    return Ok([first, runtime.fencing_token])


@app.task(
    name='e2e_fence_retry',
    error_codes=FenceCode,
    retry_policy=RetryPolicy.fixed([1], retry_for=[FenceCode.FENCE_FIRST_ATTEMPT]),
)
def fence_retry(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
    """Fail the first attempt with ``RETRY_CODE``; return the token of the
    attempt after it."""
    token = runtime.fencing_token
    match token.value:
        case 1:
            return Err(
                TaskError(code=RETRY_CODE, message='first attempt'),
            )
        case _:
            return Ok(token)
