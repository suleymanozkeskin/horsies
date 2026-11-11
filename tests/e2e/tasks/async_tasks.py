"""`async def` task definitions for e2e tests.

IMPORTANT: TaskNode and WorkflowSpec instances MUST be at module scope
for the worker process to resolve TaskNode.index correctly.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Any

from pydantic import BaseModel

from horsies.core.brokers.result_types import BrokerWaitInsideTask
from horsies.core.models.tasks import RetrievalCode, TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.utils.loop_runner import in_running_event_loop

from tests.e2e.tasks.basic import simple_task
from tests.e2e.tasks.instance import app
from tests.e2e.tasks.workflows import double_task, produce_int_task, spec_single_node

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


WAIT_PROBE_TIMEOUT_MS = 1_000
"""Timeout of each probed wait. A refused wait returns at once."""


class AsyncWaitProbe(BaseModel):
    """What an async task body got from each result wait it tried."""

    sent_task_id: str
    get_async_refused: bool
    app_get_result_async_refused: bool
    raw_async_refused: bool
    workflow_started: bool
    workflow_get_async_refused: bool


def _refused(result: TaskResult[Any, TaskError]) -> bool:
    return (
        result.is_err()
        and result.unwrap_err().error_code is RetrievalCode.WAIT_INSIDE_TASK
    )


@app.task(task_name='e2e_async_wait_inside_task')
async def async_wait_inside_task() -> TaskResult[AsyncWaitProbe, TaskError]:
    """Send a task and start a workflow, then try the async result waits.

    The test waits for the sent task from outside the worker, so a send that
    did not enqueue fails the test.
    """
    match await simple_task.send_async(x=1):
        case Ok(handle):
            pass
        case Err(send_err):
            return TaskResult(
                err=TaskError(error_code='E2E_SEND_FAILED', message=send_err.message),
            )
    read = await app.get_result_async(handle.task_id, WAIT_PROBE_TIMEOUT_MS)
    raw = await app.get_raw_result_async(handle.task_id, WAIT_PROBE_TIMEOUT_MS)
    started = await spec_single_node.start_async()
    return TaskResult(
        ok=AsyncWaitProbe(
            sent_task_id=handle.task_id,
            get_async_refused=_refused(
                await handle.get_async(timeout_ms=WAIT_PROBE_TIMEOUT_MS),
            ),
            app_get_result_async_refused=is_ok(read) and _refused(read.ok_value),
            raw_async_refused=is_err(raw)
            and isinstance(raw.err_value, BrokerWaitInsideTask),
            workflow_started=is_ok(started),
            workflow_get_async_refused=is_ok(started)
            and _refused(
                await started.ok_value.get_async(timeout_ms=WAIT_PROBE_TIMEOUT_MS),
            ),
        ),
    )


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


# =============================================================================
# An isolated async task that starts work and returns without awaiting it.
# =============================================================================

LEFT_WORK_SECONDS = 0.5
"""How long the left work runs: less than the 1 s that the child waits."""

LEFT_WORK_MARKER = os.path.join(
    tempfile.gettempdir(),
    'horsies-e2e-isolated-left-work.marker',
)
"""The left work writes ``ended`` here when it ends. The test removes the
file before it starts the task."""

# The loop holds only weak references to its tasks.
_LEFT_WORK: list[asyncio.Task[None]] = []


async def _write_marker_later(path: str) -> None:
    await asyncio.sleep(LEFT_WORK_SECONDS)
    with open(path, 'w', encoding='ascii') as marker:
        marker.write('ended')


@app.task(task_name='e2e_async_leaves_work')
async def async_leaves_work(*, marker_path: str) -> TaskResult[str, TaskError]:
    """Start work that writes the marker later, and return the pid without
    awaiting it."""
    _LEFT_WORK.append(
        asyncio.get_running_loop().create_task(_write_marker_later(marker_path)),
    )
    return TaskResult(ok=str(os.getpid()))


@app.task(task_name='e2e_read_marker')
def read_marker(
    *,
    marker_path: str,
    upstream: TaskResult[str, TaskError],
) -> TaskResult[str, TaskError]:
    """Return what the marker holds when this task runs, or ``absent``."""
    if upstream.is_err():
        return TaskResult(err=upstream.err_value)
    try:
        with open(marker_path, encoding='ascii') as marker:
            return TaskResult(ok=marker.read())
    except FileNotFoundError:
        return TaskResult(ok='absent')


node_left_work = TaskNode(
    fn=async_leaves_work,
    kwargs={'marker_path': LEFT_WORK_MARKER},
)
node_after_left_work = TaskNode(
    fn=read_marker,
    waits_for=[node_left_work],
    args_from={'upstream': node_left_work},
    kwargs={'marker_path': LEFT_WORK_MARKER},
)

spec_left_work = app.workflow(
    name='e2e_async_left_work',
    tasks=[node_left_work, node_after_left_work],
    output=node_after_left_work,
    definition_key='tests.e2e.tasks.async_tasks.e2e_async_left_work.v1',
)
