"""App instance with only ``def`` tasks and the loop creation failure hook.

The app has no async task, so its children create no task loop at boot, and
the hook's budget stays unused.
"""

from __future__ import annotations

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


@app.task(task_name='e2e_loop_boot_sync_only_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


app.discover_tasks(['tests.e2e.tasks.instance_task_loop_boot_sync_only'])
