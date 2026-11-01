"""App instance for child isolation e2e tests.

Short heartbeat and stale settings, so recovery paths finish inside a test.
"""

from __future__ import annotations

import os
import subprocess
import time

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskError, TaskResult

from tests.e2e.helpers.env import e2e_database_url

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(database_url=DB_URL, pool_size=5, max_overflow=5),
    recovery=RecoveryConfig(
        runner_heartbeat_interval_ms=1_000,
        claimer_heartbeat_interval_ms=1_000,
        running_stale_threshold_ms=5_000,
        finalizing_stale_threshold_ms=5_000,
        check_interval_ms=1_000,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app


@app.task(task_name='e2e_isolation_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_isolation_sleep')
def sleep_task(*, seconds: float) -> TaskResult[str, TaskError]:
    time.sleep(seconds)
    return TaskResult(ok=str(os.getpid()))


@app.task(task_name='e2e_isolation_timeout', timeout_ms=1_000)
def timeout_task(*, seconds: float) -> TaskResult[str, TaskError]:
    time.sleep(seconds)
    return TaskResult(ok=str(os.getpid()))


@app.task(task_name='e2e_isolation_spawn_timeout', timeout_ms=1_000)
def spawn_then_hang(*, marker: str) -> TaskResult[str, TaskError]:
    process = subprocess.Popen(['sleep', '120'])
    with open(marker, 'w', encoding='ascii') as target:
        target.write(str(process.pid))
    time.sleep(120)
    return TaskResult(ok='unreachable')


@app.task(task_name='e2e_isolation_fork_helper')
def fork_helper_then_hang(*, helper_seconds: float) -> TaskResult[str, TaskError]:
    if os.fork() == 0:
        time.sleep(helper_seconds)
        os._exit(0)
    time.sleep(120)
    return TaskResult(ok='unreachable')
