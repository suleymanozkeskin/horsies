"""App instance whose on_child_process_start hook always fails.

Used to assert the fail-closed contract: the worker must exit at boot
with a legible message instead of restart-looping on the broken child.
"""

from __future__ import annotations

import os

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
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app


@app.on_child_process_start
def _always_fails() -> None:
    raise RuntimeError('deliberate hook failure for the fail-closed e2e test')


@app.task(task_name='e2e_hook_fail_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


app.discover_tasks(['tests.e2e.tasks.instance_child_hook_fail'])
