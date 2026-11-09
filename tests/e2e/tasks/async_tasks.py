"""`async def` task definitions for e2e tests.

IMPORTANT: TaskNode and WorkflowSpec instances MUST be at module scope
for the worker process to resolve TaskNode.index correctly.
"""

from __future__ import annotations

import asyncio
import os

from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import Err, Ok
from horsies.core.utils.loop_runner import in_running_event_loop

from tests.e2e.tasks.basic import simple_task
from tests.e2e.tasks.instance import app
from tests.e2e.tasks.workflows import double_task, produce_int_task

# Strong references, so that a freed loop cannot pass its id to a new one.
_SEEN_LOOPS: list[asyncio.AbstractEventLoop] = []


@app.task(task_name='e2e_async_loop_report')
async def async_loop_report() -> TaskResult[str, TaskError]:
    """Report the child pid and the count of distinct loops that ran here."""
    loop = asyncio.get_running_loop()
    if not any(seen is loop for seen in _SEEN_LOOPS):
        _SEEN_LOOPS.append(loop)
    await asyncio.sleep(0)
    return TaskResult(ok=f'{os.getpid()}:{len(_SEEN_LOOPS)}')


@app.task(task_name='e2e_sync_loop_report')
def sync_loop_report() -> TaskResult[str, TaskError]:
    """Report the child pid and a running loop, then run a loop of its own."""
    ran = asyncio.run(asyncio.sleep(0, 'ran'))
    return TaskResult(ok=f'{os.getpid()}:{in_running_event_loop()}:{ran}')


@app.task(task_name='e2e_async_sends')
async def async_sends(*, x: int) -> TaskResult[str, TaskError]:
    """Send a task with `send_async` from an async task. Return its id."""
    match await simple_task.send_async(x=x):
        case Ok(handle):
            return TaskResult(ok=handle.task_id)
        case Err(send_err):
            return TaskResult(
                err=TaskError(error_code='SEND_FAILED', message=send_err.message),
            )


@app.task(task_name='e2e_async_add_one')
async def async_add_one(
    *,
    upstream: TaskResult[int, TaskError],
) -> TaskResult[int, TaskError]:
    """Add one to the upstream value, after an await."""
    await asyncio.sleep(0)
    if upstream.is_err():
        return TaskResult(err=upstream.err_value)
    return TaskResult(ok=upstream.unwrap() + 1)


@app.task(task_name='e2e_async_timeout_sleeper', timeout_ms=2_000)
async def async_timeout_sleeper(*, duration_ms: int) -> TaskResult[str, TaskError]:
    """Await past the 2 s timeout_ms, so the parent kills the child."""
    await asyncio.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')


# =============================================================================
# A def node feeds an async def node, which feeds a def node: 5 -> 6 -> 12.
# =============================================================================

node_mixed_source = TaskNode(fn=produce_int_task, kwargs={'value': 5})
node_mixed_async = TaskNode(
    fn=async_add_one,
    waits_for=[node_mixed_source],
    args_from={'upstream': node_mixed_source},
)
node_mixed_sink = TaskNode(
    fn=double_task,
    waits_for=[node_mixed_async],
    args_from={'input_result': node_mixed_async},
)

spec_mixed_chain = app.workflow(
    name='e2e_async_mixed_chain',
    tasks=[node_mixed_source, node_mixed_async, node_mixed_sink],
    output=node_mixed_sink,
    definition_key='tests.e2e.tasks.async_tasks.e2e_async_mixed_chain.v1',
)
