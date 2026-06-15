"""App instance with the documented on_child_process_start body.

Owns a module-level SQLAlchemy engine (separate from horsies' broker),
tagged with a distinct application_name for pg_stat_activity attribution.
The hook disposes the inherited engine and rebinds it to NullPool in each
worker child, so the post-burst app-engine connection floor is ~0 instead
of one pool per child. Graduates tests/spike/conn_accum.
"""

from __future__ import annotations

import os

from sqlalchemy import Engine, NullPool, create_engine, text

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ.get("DB_PASSWORD", "")}@localhost:5432/horsies',
)

APP_ENGINE_APPLICATION_NAME = 'e2e_hook_fixed_engine'

_app_engine: Engine | None = None


def _build_engine(*, worker_child: bool) -> Engine:
    if worker_child:
        return create_engine(
            DB_URL,
            poolclass=NullPool,
            connect_args={'application_name': APP_ENGINE_APPLICATION_NAME},
        )
    return create_engine(
        DB_URL,
        pool_size=3,
        max_overflow=2,
        connect_args={'application_name': APP_ENGINE_APPLICATION_NAME},
    )


def get_engine() -> Engine:
    """Engine indirection: task code must read through this for the rebind to take effect."""
    global _app_engine
    if _app_engine is None:
        _app_engine = _build_engine(worker_child=False)
    return _app_engine


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
def _reset_db_for_child() -> None:
    """The canonical documented body: dispose inherited FDs, rebind pool policy."""
    global _app_engine
    if _app_engine is not None:
        _app_engine.dispose(close=False)
    _app_engine = _build_engine(worker_child=True)


@app.task(task_name='e2e_hook_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_hook_db_task')
def db_task(*, sleep_ms: int) -> TaskResult[str, TaskError]:
    """Check out the app-owned engine and hold a connection across a slow query."""
    with get_engine().connect() as conn:
        conn.execute(text('SELECT pg_sleep(:s)'), {'s': sleep_ms / 1000.0})
    return TaskResult(ok=f'ok:{sleep_ms}')


app.discover_tasks(['tests.e2e.tasks.instance_child_hook'])
