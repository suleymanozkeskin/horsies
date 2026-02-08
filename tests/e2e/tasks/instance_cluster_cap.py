"""App instance with cluster_wide_cap for e2e tests."""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)

# Cluster-wide cap of 2: only 2 tasks can be RUNNING across all workers
CLUSTER_WIDE_CAP = 2

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    cluster_wide_cap=CLUSTER_WIDE_CAP,
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker

# Import tasks to register them in current process
from tests.e2e.tasks import basic  # noqa: F401

# Also register for worker subprocess discovery
app.discover_tasks(
    [
        'tests.e2e.tasks.basic',
    ]
)

# Local tasks bound to THIS app's broker
from horsies.core.models.tasks import TaskResult, TaskError


@app.task(task_name='e2e_cluster_cap_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_cluster_cap_slow')
def slow_task(duration_ms: int) -> TaskResult[str, TaskError]:
    """Task that sleeps for specified duration."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')


@app.task(task_name='e2e_cluster_cap_fail')
def fail_task() -> TaskResult[str, TaskError]:
    """Task that always fails, for cap-leak testing."""
    return TaskResult(err=TaskError(error_code='DELIBERATE_FAIL', message='always fails'))
