"""Recovery-mode app instance for e2e crash/stale-task tests.

Uses aggressive thresholds so reaper detects orphaned tasks in ~2-3 seconds
instead of the default 2-5 minutes.
"""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskResult, TaskError

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
    recovery=RecoveryConfig(
        runner_heartbeat_interval_ms=1_000,
        claimer_heartbeat_interval_ms=1_000,
        running_stale_threshold_ms=2_000,
        claimed_stale_threshold_ms=2_000,
        check_interval_ms=1_000,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker


@app.task(task_name='e2e_recovery_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_recovery_slow')
def slow_task(duration_ms: int) -> TaskResult[str, TaskError]:
    """Long-running task for crash-while-RUNNING tests."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')


# Register tasks for worker subprocess discovery
app.discover_tasks(['tests.e2e.tasks.instance_recovery'])
