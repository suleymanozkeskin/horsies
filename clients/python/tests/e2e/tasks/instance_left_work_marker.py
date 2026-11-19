"""App instance with small recovery thresholds, for the finalizing marker of
an isolated async task that leaves work on its task loop.

While the finalizing marker of a row is set, runner heartbeats skip the row,
and the reaper counts ``finalizing_stale_threshold_ms`` from the marker. A
marker set at the return of such a task, with these thresholds, lets the
reaper fail the row about 6 s later while the work still runs.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
import time

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskError, TaskResult

from tests.e2e.helpers.env import e2e_database_url
from tests.e2e.tasks.profiles import E2E_WORKER_PROFILES

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

RUNNING_STALE_THRESHOLD_MS = 6_000
"""The smallest threshold with auto_fail_stale_running on:
2 x (runner_heartbeat_interval_ms + 2 s)."""

FINALIZING_STALE_THRESHOLD_MS = 2_000
"""The smallest threshold: 2 x runner_heartbeat_interval_ms."""

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=5,
        max_overflow=5,
    ),
    recovery=RecoveryConfig(
        runner_heartbeat_interval_ms=1_000,
        running_stale_threshold_ms=RUNNING_STALE_THRESHOLD_MS,
        finalizing_stale_threshold_ms=FINALIZING_STALE_THRESHOLD_MS,
        check_interval_ms=1_000,
    ),
    worker_profiles=E2E_WORKER_PROFILES,
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app

# The loop holds only weak references to its tasks.
_LEFT_WORK: list[asyncio.Task[None]] = []


@app.task(task_name='e2e_left_work_marker_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


async def _hold_the_loop_then_write(path: str, seconds: float) -> None:
    """Hold the loop thread for ``seconds`` once the child waits for this
    work, so no cancel round can run, then write ``ended``."""
    await asyncio.sleep(0.05)
    time.sleep(seconds)
    with open(path, 'w', encoding='ascii') as marker:
        marker.write('ended')


@app.task(task_name='e2e_left_work_holds_the_loop')
async def left_work_holds_the_loop(
    *,
    marker_path: str,
    seconds: float,
) -> TaskResult[str, TaskError]:
    """Start work that holds the loop for ``seconds`` and then writes the
    marker, and return without awaiting it."""
    _LEFT_WORK.append(
        asyncio.get_running_loop().create_task(
            _hold_the_loop_then_write(marker_path, seconds),
        ),
    )
    return TaskResult(ok=f'returned:{os.getpid()}')


# Register tasks for worker subprocess discovery
app.discover_tasks(['tests.e2e.tasks.instance_left_work_marker'])
