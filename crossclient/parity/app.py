"""The Python app of the parity runs (`crossclient/parity/runner.py`).

The Rust app of `clients/rust/tests/parity` has the same task names, input field
names, codes, messages, options, profiles and workflows, so the same
scenario stores the same rows on each client. The runner sets
`HORSIES_PARITY_DATABASE_URL` for each process of a side: the worker, its
children and the send process.

Profiles, one child each, so the tasks of a scenario start in send order:
- `isolated-1`: one isolated child.
- `shared-async-1x4`: one shared async child with 4 seats.
- `shared-thread-1x4`: one shared thread child with 4 seats.

The schedule `parity_slots` sends `parity.add` (2 + 3) every hour and
catches up missed runs, at most two slots per fire. Only the scheduler
process of the `schedule_slots` scenario runs it.

Every send retains its input (`retain_rerun_input_default`), so a failed
task can be rerun.

Each task ends the same way on every run: no task reads the clock, a
random source, or the state of another task.
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import timedelta
from typing import Final

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.outcome import Succeeded, TaskOutcome
from horsies.core.models.queues import QueueMode
from horsies.core.models.schedule import IntervalSchedule, ScheduleConfig, TaskSchedule
from horsies.core.models.tasks import Execution, RetryPolicy, TaskError
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
)
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    NoIsolatedChildren,
    SharedAsyncChildren,
    SharedThreadChildren,
    WorkerProfile,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.config import ChildPreload

DATABASE_URL_ENV: Final = 'HORSIES_PARITY_DATABASE_URL'
"""The database of this side of the run."""

ISOLATED: Final = 'isolated-1'
SHARED_ASYNC: Final = 'shared-async-1x4'
SHARED_THREAD: Final = 'shared-thread-1x4'

SHORT_TIMEOUT_MS: Final = 1_000
"""The deadline of the tasks that run past it."""

PAST_THE_DEADLINE_S: Final = 30.0
"""How long a task that must time out sleeps: far past `SHORT_TIMEOUT_MS`."""

SHARED_TIMEOUT_MS: Final = 30_000
"""The deadline of a shared task that ends in time."""

DRAIN_TIMEOUT: Final = timedelta(seconds=5)

RETRY_POLICY: Final = RetryPolicy.fixed([1, 1], retry_for=['TRANSIENT'])
"""Two retries one second apart for the code `TRANSIENT`."""

CHAIN_NAME: Final = 'parity_chain'
CHAIN_DEFINITION_KEY: Final = 'crossclient.parity.chain.v1'
CHILD_NAME: Final = 'parity_child'
CHILD_DEFINITION_KEY: Final = 'crossclient.parity.child.v1'
PARENT_NAME: Final = 'parity_parent'
PARENT_DEFINITION_KEY: Final = 'crossclient.parity.parent.v1'

SCHEDULE_NAME: Final = 'parity_slots'
SLOT_TASK_NAME: Final = 'parity.add'
"""The task of each slot of the schedule."""
SCHEDULE_INTERVAL: Final = timedelta(hours=1)
"""Longer than any run: a slot is due only after the runner moves the
schedule back."""
SCHEDULE_MAX_CATCH_UP_RUNS: Final = 2
SCHEDULE_CHECK_INTERVAL_S: Final = 1


def _database_url() -> str:
    """The URL that the runner set. The module may be imported without it
    (a type check); then it names no server."""
    return os.environ.get(DATABASE_URL_ENV, 'postgresql://unset@localhost/unset')


config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=SecretStr(_database_url()),
        pool_size=5,
        max_overflow=5,
        retain_rerun_input_default=True,
    ),
    schedule=ScheduleConfig(
        schedules=[
            TaskSchedule(
                name=SCHEDULE_NAME,
                task_name=SLOT_TASK_NAME,
                pattern=IntervalSchedule(every=SCHEDULE_INTERVAL),
                kwargs={'a': 2, 'b': 3},
                timezone='UTC',
                catch_up_missed=True,
                max_catch_up_runs=SCHEDULE_MAX_CATCH_UP_RUNS,
            ),
        ],
        check_interval_seconds=SCHEDULE_CHECK_INTERVAL_S,
    ),
    worker_profiles={
        ISOLATED: WorkerProfile(
            isolated=IsolatedChildren(count=1),
            preload=ChildPreload.NONE,
            drain_timeout=DRAIN_TIMEOUT,
        ),
        SHARED_ASYNC: WorkerProfile(
            isolated=NoIsolatedChildren(),
            shared_async=SharedAsyncChildren(count=1, seats=4),
            preload=ChildPreload.NONE,
            drain_timeout=DRAIN_TIMEOUT,
        ),
        SHARED_THREAD: WorkerProfile(
            isolated=NoIsolatedChildren(),
            shared_thread=SharedThreadChildren(count=1, seats=4),
            preload=ChildPreload.NONE,
            drain_timeout=DRAIN_TIMEOUT,
        ),
    },
)

app = Horsies(config)


# ---- Isolated ----


@app.task(name='parity.add')
def add(*, a: int, b: int) -> Result[int, TaskError]:
    """A plain success."""
    return Ok(a + b)


@app.task(name='parity.refuse')
def refuse(*, value: int) -> Result[int, TaskError]:
    """A task error with no retry."""
    return Err(TaskError(code='REFUSED', message=f'refused {value}'))


@app.task(name='parity.transient', retry_policy=RETRY_POLICY)
def transient(*, value: int) -> Result[int, TaskError]:
    """A task error that the policy retries until the retries run out."""
    return Err(TaskError(code='TRANSIENT', message=f'transient {value}'))


@app.task(name='parity.sleep_past_deadline', timeout_ms=SHORT_TIMEOUT_MS)
def sleep_past_deadline() -> Result[str, TaskError]:
    """An isolated task that the parent kills at its deadline."""
    time.sleep(PAST_THE_DEADLINE_S)
    return Ok('late')


@app.task(name='parity.double')
def double(*, upstream: TaskOutcome[int, TaskError]) -> Result[int, TaskError]:
    """Twice the value of its upstream node."""
    match upstream:
        case Succeeded(value):
            return Ok(value * 2)
        case _:
            return Err(
                TaskError(code='UPSTREAM_FAILED', message='the upstream failed'),
            )


# ---- Shared async ----


@app.task(
    name='parity.add_async',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
)
async def add_async(*, a: int, b: int) -> Result[int, TaskError]:
    """A plain success on an async seat."""
    await asyncio.sleep(0)
    return Ok(a + b)


@app.task(
    name='parity.refuse_async',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
)
async def refuse_async(*, value: int) -> Result[int, TaskError]:
    await asyncio.sleep(0)
    return Err(TaskError(code='REFUSED', message=f'refused {value}'))


@app.task(
    name='parity.transient_async',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
    retry_policy=RETRY_POLICY,
)
async def transient_async(*, value: int) -> Result[int, TaskError]:
    await asyncio.sleep(0)
    return Err(TaskError(code='TRANSIENT', message=f'transient {value}'))


@app.task(
    name='parity.async_past_deadline',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHORT_TIMEOUT_MS,
)
async def async_past_deadline() -> Result[str, TaskError]:
    """The cancel at the deadline ends the sleep within the grace:
    `cancel_end` is `RETURNED_AFTER_SIGNAL`."""
    await asyncio.sleep(PAST_THE_DEADLINE_S)
    return Ok('late')


# ---- Shared thread ----


@app.task(
    name='parity.add_thread',
    execution=Execution.SHARED_THREAD,
    timeout_ms=SHARED_TIMEOUT_MS,
)
def add_thread(*, a: int, b: int) -> Result[int, TaskError]:
    return Ok(a + b)


@app.task(
    name='parity.refuse_thread',
    execution=Execution.SHARED_THREAD,
    timeout_ms=SHARED_TIMEOUT_MS,
)
def refuse_thread(*, value: int) -> Result[int, TaskError]:
    return Err(TaskError(code='REFUSED', message=f'refused {value}'))


@app.task(
    name='parity.transient_thread',
    execution=Execution.SHARED_THREAD,
    timeout_ms=SHARED_TIMEOUT_MS,
    retry_policy=RETRY_POLICY,
)
def transient_thread(*, value: int) -> Result[int, TaskError]:
    return Err(TaskError(code='TRANSIENT', message=f'transient {value}'))


@app.task(
    name='parity.thread_past_deadline',
    execution=Execution.SHARED_THREAD,
    timeout_ms=SHORT_TIMEOUT_MS,
)
def thread_past_deadline() -> Result[str, TaskError]:
    """A thread seat that cannot stop at its deadline: the child is killed
    when no run of it has time left."""
    time.sleep(PAST_THE_DEADLINE_S)
    return Ok('late')


# ---- Workflow ----

_chain_add = TaskNode(function=add, kwargs={'a': 2, 'b': 3}, node_id='add')
_chain_double = TaskNode(
    function=double,
    waits_for=[_chain_add],
    args_from={'upstream': _chain_add},
    node_id='double',
)
parity_chain = app.workflow(
    name=CHAIN_NAME,
    tasks=[_chain_add, _chain_double],
    output=_chain_double,
    on_error=OnError.FAIL,
    definition_key=CHAIN_DEFINITION_KEY,
)
"""Two nodes: the second takes the result of the first."""


class ParityChild(WorkflowDefinition[int]):
    """The child of `parity_parent`: one node, its output."""

    name = CHILD_NAME
    definition_key = CHILD_DEFINITION_KEY

    child_add = TaskNode(function=add, kwargs={'a': 2, 'b': 3}, node_id='add')

    config = WorkflowDefinitionConfig(output=child_add, on_error=OnError.FAIL)


_parent_child = SubWorkflowNode(workflow_def=ParityChild, node_id='child')
_parent_double = TaskNode(
    function=double,
    waits_for=[_parent_child],
    args_from={'upstream': _parent_child},
    node_id='double',
)
parity_parent = app.workflow(
    name=PARENT_NAME,
    tasks=[_parent_child, _parent_double],
    output=_parent_double,
    on_error=OnError.FAIL,
    definition_key=PARENT_DEFINITION_KEY,
)
"""A sub-workflow node of `ParityChild`, then a node that takes the child's
output."""
