"""Control instance for the child-hook regression: NO hook registered.

Same app-owned QueuePool engine shape as instance_child_hook (distinct
application_name), without the per-child rebind. The post-burst floor
stays at roughly one pool per child, validating that the fixed
instance's ~0 floor is the hook's doing and that attribution works.
"""

from __future__ import annotations

import os

from sqlalchemy import Engine, create_engine, text

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult

from tests.e2e.helpers.env import e2e_database_url

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

APP_ENGINE_APPLICATION_NAME = 'e2e_hook_unfixed_engine'

app_engine: Engine = create_engine(
    DB_URL,
    pool_size=3,
    max_overflow=2,
    connect_args={'application_name': APP_ENGINE_APPLICATION_NAME},
)

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


@app.task(task_name='e2e_hook_unfixed_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_hook_unfixed_db_task')
def db_task(*, sleep_ms: int) -> TaskResult[str, TaskError]:
    """Check out the app-owned engine and hold a connection across a slow query."""
    with app_engine.connect() as conn:
        conn.execute(text('SELECT pg_sleep(:s)'), {'s': sleep_ms / 1000.0})
    return TaskResult(ok=f'ok:{sleep_ms}')


app.discover_tasks(['tests.e2e.tasks.instance_child_hook_unfixed'])
