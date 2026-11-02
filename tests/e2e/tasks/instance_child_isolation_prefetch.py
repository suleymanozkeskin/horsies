"""App instance with a prefetch buffer: more claimed tasks than free children."""

from __future__ import annotations

import os
import time

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult

from tests.e2e.helpers.env import e2e_database_url

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    prefetch_buffer=3,
    claim_lease_ms=60_000,
    broker=PostgresConfig(database_url=DB_URL, pool_size=5, max_overflow=5),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app


@app.task(task_name='e2e_isolation_prefetch_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_isolation_prefetch_sleep')
def sleep_task(*, seconds: float) -> TaskResult[str, TaskError]:
    time.sleep(seconds)
    return TaskResult(ok=str(os.getpid()))


@app.task(task_name='e2e_isolation_prefetch_timeout_1s', timeout_ms=1_000)
def timeout_1s(*, seconds: float) -> TaskResult[str, TaskError]:
    time.sleep(seconds)
    return TaskResult(ok=str(os.getpid()))


@app.task(task_name='e2e_isolation_prefetch_timeout_2s', timeout_ms=2_000)
def timeout_2s(*, seconds: float) -> TaskResult[str, TaskError]:
    time.sleep(seconds)
    return TaskResult(ok=str(os.getpid()))
