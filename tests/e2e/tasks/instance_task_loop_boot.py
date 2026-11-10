"""App instance with an ``async def`` task, whose children can fail to create
the task loop at boot.

Its child start hook is ``loop_creation_budget.fail_loop_creation_on_demand``.
The app has an async task, so each child creates its task loop at boot.
"""

from __future__ import annotations

import asyncio
import os

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult

from tests.e2e.helpers.env import e2e_database_url
from tests.e2e.tasks.loop_creation_budget import fail_loop_creation_on_demand

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=5,
        max_overflow=5,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker  # pyright: ignore[reportPrivateUsage]
broker.app = app

app.on_child_process_start(fail_loop_creation_on_demand)


@app.task(task_name='e2e_loop_boot_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_loop_boot_async_pid')
async def async_pid() -> TaskResult[int, TaskError]:
    """Runs on the task loop that its child created at boot."""
    await asyncio.sleep(0)
    return TaskResult(ok=os.getpid())


app.discover_tasks(['tests.e2e.tasks.instance_task_loop_boot'])
