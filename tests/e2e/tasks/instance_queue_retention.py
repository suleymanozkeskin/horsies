"""App instance mapping a queue to a retention duration.

The mapping is the subject: a worker booted from this module must route
a task sent on the mapped queue into the class the mapping derives, and
that can only happen if the derived class reaches the registrar and the
resolved key reaches the enqueue.

Maintenance intervals sit at their floors so a booted worker's first
ticks land inside a test's patience rather than a production cadence.
"""

from __future__ import annotations

import os
from datetime import timedelta

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.retention import RetentionConfig
from horsies.core.models.tasks import TaskError, TaskResult

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)

MAPPED_QUEUE = 'default'
MAPPED_DURATION = timedelta(days=7)
DERIVED_CLASS_KEY = 'q_default_7d'

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
    recovery=RecoveryConfig(check_interval_ms=1_000),
    retention=RetentionConfig(
        partition_maintenance_interval_s=60,
        retention_sweep_interval_s=30,
        queue_retention={MAPPED_QUEUE: MAPPED_DURATION},
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app


@app.task(task_name='e2e_queue_retention_task')
def mapped_task(*, x: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=x * 2)


app.discover_tasks(
    [
        'tests.e2e.tasks.instance_queue_retention',
    ]
)
