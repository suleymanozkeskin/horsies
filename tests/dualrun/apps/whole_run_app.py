"""The app of the whole-run worker pairs (`tests/dualrun/worker_run.py`).

Both sides of a pair run this module, each with its own database: the runner
sets `DUALRUN_WORKER_DATABASE_URL` in the environment of each worker. The
tasks behave the same on every run: no task reads the clock, a random
source, or the state of another task, so two runs of the same sends end in
the same rows.

Profiles, one per child kind, each with one child, so the tasks of a
scenario start in send order:
- `isolated-1`: one isolated child.
- `shared-async-1x4`: one shared async child with 4 seats.
- `shared-thread-1x4`: one shared thread child with 4 seats.
- `split-1-1x4`: one isolated child and one shared async child with 4 seats,
  for the rows that move to the isolated class.
- `isolated-1-recycle-2`: one isolated child, replaced after 2 tasks.

A task that waits for another step reads and writes marker files in the
directory that the runner sets in `DUALRUN_WORKER_MARKER_DIR`, a new one
for each side. The waits on markers make the order of a scenario fixed,
not its timing.
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import timedelta
from typing import Any, Final

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import Execution, RetryPolicy, TaskError, TaskResult
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    IsolatedRecycle,
    NoIsolatedChildren,
    SharedAsyncChildren,
    SharedThreadChildren,
    TaskLimit,
    WorkerProfile,
)
from horsies.core.worker.config import ChildPreload

DATABASE_URL_ENV: Final = 'DUALRUN_WORKER_DATABASE_URL'
"""The database of this side of the pair."""

MARKER_DIR_ENV: Final = 'DUALRUN_WORKER_MARKER_DIR'
"""The marker directory of this side of the pair."""

NEIGHBOUR_RUNNING: Final = 'neighbour-running'
"""Written by the neighbour of the blocked loop when its body runs."""

LOOP_BLOCKED: Final = 'loop-blocked'
"""Written by the first attempt that blocks the loop."""

LEFT_WORK_ENDED: Final = 'left-work-ended'
"""Written by the left work of an isolated async task."""

FIRST_NODE_RUNNING: Final = 'first-node-running'
"""Written by the first node of the pause chain when its body runs."""

FIRST_NODE_RELEASED: Final = 'first-node-released'
"""Written by the runner after the pause; the first node then returns."""

MARKER_WAIT_S: Final = 60.0
"""The longest wait of a task for a marker of another step."""

MARKER_POLL_S: Final = 0.05

LEFT_WORK_S: Final = 0.5
"""How long the left work runs: less than the 1 s that the child waits."""

DRAIN_TASK_S: Final = 2.0
"""How long a task of the drain scenario sleeps: longer than the runner's
poll, shorter than the drain timeout (5 s)."""

ISOLATED: Final = 'isolated-1'
SHARED_ASYNC: Final = 'shared-async-1x4'
SHARED_THREAD: Final = 'shared-thread-1x4'
SPLIT: Final = 'split-1-1x4'
ISOLATED_RECYCLED: Final = 'isolated-1-recycle-2'

SHORT_TIMEOUT_MS: Final = 1_000
"""The deadline of the tasks that run past it."""

PAST_THE_DEADLINE_S: Final = 30.0
"""How long a task that must time out sleeps: far past `SHORT_TIMEOUT_MS`."""

SHARED_TIMEOUT_MS: Final = 30_000
"""The deadline of a shared task that ends in time."""

RETRY_POLICY: Final = RetryPolicy.fixed([1, 1], auto_retry_for=['TRANSIENT'])
"""Two retries one second apart for the code `TRANSIENT`."""


def _marker(name: str) -> str:
    """The path of marker `name` in the marker directory of this side.

    Raises `KeyError` when the runner set no marker directory: only a worker
    of the runner runs these tasks.
    """
    return os.path.join(os.environ[MARKER_DIR_ENV], name)


def _write_marker(name: str) -> None:
    with open(_marker(name), 'w', encoding='ascii') as marker:
        marker.write('written')


def _first_call(name: str) -> bool:
    """True on the first call with `name` on this side: the call creates the
    marker."""
    try:
        with open(_marker(name), 'x', encoding='ascii') as created:
            created.write('written')
    except FileExistsError:
        return False
    return True


async def _wait_for_marker(name: str) -> bool:
    """Wait on the loop until marker `name` exists, at most
    `MARKER_WAIT_S`. False when the bound passed."""
    deadline = time.monotonic() + MARKER_WAIT_S
    while not os.path.exists(_marker(name)):
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(MARKER_POLL_S)
    return True


def _database_url() -> str:
    """The URL that the runner set. The module may be imported without it
    (a type check, a unit test of the runner); then it names no server."""
    return os.environ.get(DATABASE_URL_ENV, 'postgresql://unset@localhost/unset')


config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=SecretStr(_database_url()), pool_size=5, max_overflow=5
    ),
    recovery=RecoveryConfig(
        runner_heartbeat_interval_ms=1_000,
        claimer_heartbeat_interval_ms=1_000,
        running_stale_threshold_ms=6_000,
        finalizing_stale_threshold_ms=5_000,
        check_interval_ms=1_000,
    ),
    worker_profiles={
        ISOLATED: WorkerProfile(
            isolated=IsolatedChildren(count=1),
            preload=ChildPreload.NONE,
            drain_timeout=timedelta(seconds=5),
        ),
        SHARED_ASYNC: WorkerProfile(
            isolated=NoIsolatedChildren(),
            shared_async=SharedAsyncChildren(count=1, seats=4),
            preload=ChildPreload.NONE,
            drain_timeout=timedelta(seconds=5),
        ),
        SHARED_THREAD: WorkerProfile(
            isolated=NoIsolatedChildren(),
            shared_thread=SharedThreadChildren(count=1, seats=4),
            preload=ChildPreload.NONE,
            drain_timeout=timedelta(seconds=5),
        ),
        SPLIT: WorkerProfile(
            isolated=IsolatedChildren(count=1),
            shared_async=SharedAsyncChildren(count=1, seats=4),
            preload=ChildPreload.NONE,
            drain_timeout=timedelta(seconds=5),
        ),
        ISOLATED_RECYCLED: WorkerProfile(
            isolated=IsolatedChildren(
                count=1,
                recycle=IsolatedRecycle(after=TaskLimit(tasks=2)),
            ),
            preload=ChildPreload.NONE,
            drain_timeout=timedelta(seconds=5),
        ),
    },
)

app = Horsies(config)


@app.task(task_name='whole_run.double')
def double(*, value: int) -> TaskResult[int, TaskError]:
    """A plain success."""
    return TaskResult(ok=value * 2)


@app.task(task_name='whole_run.refuse')
def refuse(*, value: int) -> TaskResult[int, TaskError]:
    """A task error with no retry."""
    return TaskResult(err=TaskError(error_code='REFUSED', message=f'refused {value}'))


@app.task(task_name='whole_run.transient', retry_policy=RETRY_POLICY)
def transient(*, value: int) -> TaskResult[int, TaskError]:
    """A task error that the policy retries until the retries run out."""
    return TaskResult(
        err=TaskError(error_code='TRANSIENT', message=f'transient {value}')
    )


@app.task(task_name='whole_run.sleep_past_deadline', timeout_ms=SHORT_TIMEOUT_MS)
def sleep_past_deadline() -> TaskResult[str, TaskError]:
    """An isolated task that the parent kills at its deadline."""
    time.sleep(PAST_THE_DEADLINE_S)
    return TaskResult(ok='late')


@app.task(task_name='whole_run.crash')
def crash() -> TaskResult[str, TaskError]:
    """A child that exits in the middle of its task."""
    os._exit(9)


@app.task(
    task_name='whole_run.double_async',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
)
async def double_async(*, value: int) -> TaskResult[int, TaskError]:
    """A plain success on an async seat."""
    await asyncio.sleep(0)
    return TaskResult(ok=value * 2)


@app.task(
    task_name='whole_run.async_past_deadline',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHORT_TIMEOUT_MS,
)
async def async_past_deadline() -> TaskResult[str, TaskError]:
    """An async seat whose task the child cancels at its deadline."""
    await asyncio.sleep(PAST_THE_DEADLINE_S)
    return TaskResult(ok='late')


@app.task(
    task_name='whole_run.double_thread',
    execution=Execution.SHARED_THREAD,
    timeout_ms=SHARED_TIMEOUT_MS,
)
def double_thread(*, value: int) -> TaskResult[int, TaskError]:
    """A plain success on a thread seat."""
    return TaskResult(ok=value * 2)


@app.task(
    task_name='whole_run.async_neighbour',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
)
async def async_neighbour() -> TaskResult[str, TaskError]:
    """A seat that runs while its neighbour blocks the loop. Its policy does
    not retry `NEIGHBOUR_BLOCKED_LOOP`."""
    _write_marker(NEIGHBOUR_RUNNING)
    await asyncio.sleep(PAST_THE_DEADLINE_S)
    return TaskResult(ok='late')


@app.task(
    task_name='whole_run.blocks_loop_once',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
    retry_policy=RetryPolicy.fixed([1], auto_retry_for=['TASK_BLOCKED_LOOP']),
)
async def blocks_loop_once() -> TaskResult[str, TaskError]:
    """The first attempt waits until its neighbour runs, then blocks the
    loop past the block limit. The retry, in the isolated child, returns."""
    if not _first_call(LOOP_BLOCKED):
        return TaskResult(ok='unblocked')
    if not await _wait_for_marker(NEIGHBOUR_RUNNING):
        return TaskResult(
            err=TaskError(error_code='NO_NEIGHBOUR', message='the neighbour never ran'),
        )
    time.sleep(PAST_THE_DEADLINE_S)
    return TaskResult(ok='late')


# The loop holds only weak references to its tasks.
_LEFT_WORK: list[asyncio.Task[None]] = []


async def _left_work() -> None:
    await asyncio.sleep(LEFT_WORK_S)
    _write_marker(LEFT_WORK_ENDED)


@app.task(task_name='whole_run.leaves_work')
async def leaves_work(*, value: int) -> TaskResult[int, TaskError]:
    """An isolated async task that starts work and returns without awaiting
    it: `body_returned`, then `left_work_ended`."""
    _LEFT_WORK.append(asyncio.get_running_loop().create_task(_left_work()))
    return TaskResult(ok=value * 2)


@app.task(
    task_name='whole_run.sleep_then_double',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
)
async def sleep_then_double(*, value: int) -> TaskResult[int, TaskError]:
    """A seat that is still running when the stop arrives; the drain lets it
    end."""
    await asyncio.sleep(DRAIN_TASK_S)
    return TaskResult(ok=value * 2)


@app.task(
    task_name='whole_run.thread_past_deadline',
    execution=Execution.SHARED_THREAD,
    timeout_ms=SHORT_TIMEOUT_MS,
)
def thread_past_deadline() -> TaskResult[str, TaskError]:
    """A thread seat that cannot stop at its deadline: the child is killed
    when no run of it has time left."""
    time.sleep(PAST_THE_DEADLINE_S)
    return TaskResult(ok='late')


@app.task(task_name='whole_run.waits_for_release')
def waits_for_release() -> TaskResult[str, TaskError]:
    """The first node of the pause chain: it runs until the runner paused
    the workflow and wrote the release marker."""
    _write_marker(FIRST_NODE_RUNNING)
    deadline = time.monotonic() + MARKER_WAIT_S
    while not os.path.exists(_marker(FIRST_NODE_RELEASED)):
        if time.monotonic() > deadline:
            return TaskResult(
                err=TaskError(error_code='NOT_RELEASED', message='no release marker'),
            )
        time.sleep(MARKER_POLL_S)
    return TaskResult(ok='released')


class ChildChain(WorkflowDefinition[int]):
    """The sub-workflow of the chain: one task, built in the worker when its
    node is ready (`build_with`)."""

    name = 'whole_run_child_chain'
    definition_key = 'tests.dualrun.whole_run.child_chain.v1'

    @classmethod
    def build_with(cls, app: Horsies, **params: Any) -> WorkflowSpec[int]:
        match params:
            case {'value': int() as value}:
                pass
            case _:
                raise TypeError(f'{cls.name} takes value: int, not {params!r}')
        child = TaskNode(fn=double, kwargs={'value': value})
        return app.workflow(
            name=cls.name,
            tasks=[child],
            output=child,
            on_error=OnError.FAIL,
            definition_key=cls.definition_key,
            workflow_def_cls=cls,
        )


_chain_first = TaskNode(fn=double, kwargs={'value': 1}, node_id='first')
_chain_sub = SubWorkflowNode(
    workflow_def=ChildChain,
    kwargs={'value': 2},
    waits_for=[_chain_first],
    node_id='sub',
)
_chain_last = TaskNode(
    fn=double,
    kwargs={'value': 3},
    waits_for=[_chain_sub],
    node_id='last',
)
chain_with_subworkflow = app.workflow(
    name='whole_run_chain',
    tasks=[_chain_first, _chain_sub, _chain_last],
    output=_chain_last,
    on_error=OnError.FAIL,
    definition_key='tests.dualrun.whole_run.chain.v1',
)
"""A task, a sub-workflow whose child the worker builds, then a task."""

_pause_first = TaskNode(fn=waits_for_release, node_id='first')
_pause_second = TaskNode(
    fn=double,
    kwargs={'value': 4},
    waits_for=[_pause_first],
    node_id='second',
)
pause_chain = app.workflow(
    name='whole_run_pause_chain',
    tasks=[_pause_first, _pause_second],
    output=_pause_second,
    on_error=OnError.FAIL,
    definition_key='tests.dualrun.whole_run.pause_chain.v1',
)
"""A node that runs until the runner paused the workflow, then a node that
the pause holds."""
