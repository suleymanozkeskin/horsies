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
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import Execution, RetryPolicy, TaskError, TaskResult
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    NoIsolatedChildren,
    SharedAsyncChildren,
    SharedThreadChildren,
    WorkerProfile,
)
from horsies.core.worker.config import ChildPreload

DATABASE_URL_ENV: Final = 'DUALRUN_WORKER_DATABASE_URL'
"""The database of this side of the pair."""

ISOLATED: Final = 'isolated-1'
SHARED_ASYNC: Final = 'shared-async-1x4'
SHARED_THREAD: Final = 'shared-thread-1x4'

SHORT_TIMEOUT_MS: Final = 1_000
"""The deadline of the tasks that run past it."""

PAST_THE_DEADLINE_S: Final = 30.0
"""How long a task that must time out sleeps: far past `SHORT_TIMEOUT_MS`."""

SHARED_TIMEOUT_MS: Final = 30_000
"""The deadline of a shared task that ends in time."""

RETRY_POLICY: Final = RetryPolicy.fixed([1, 1], auto_retry_for=['TRANSIENT'])
"""Two retries one second apart for the code `TRANSIENT`."""


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
