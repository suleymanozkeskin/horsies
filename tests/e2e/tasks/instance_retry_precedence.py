"""App instance for e2e retry precedence and exception-name collision tests."""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import RetryPolicy, TaskError, TaskResult

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ.get("DB_PASSWORD", "")}@localhost:5432/horsies',
)

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    exception_mapper={
        ValueError: 'GLOBAL_VALUE_ERROR',
    },
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker


@app.task(task_name='e2e_retry_precedence_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(
    task_name='e2e_retry_precedence_task_mapper_wins_no_retry',
    retry_policy=RetryPolicy.fixed([1], auto_retry_for=['GLOBAL_VALUE_ERROR']),
    exception_mapper={ValueError: 'TASK_VALUE_ERROR'},
)
def task_mapper_wins_no_retry_task() -> TaskResult[str, TaskError]:
    raise ValueError('task mapper should win over global mapper')


@app.task(
    task_name='e2e_retry_precedence_task_mapper_wins_retry',
    retry_policy=RetryPolicy.fixed([1], auto_retry_for=['TASK_VALUE_ERROR']),
    exception_mapper={ValueError: 'TASK_VALUE_ERROR'},
)
def task_mapper_wins_retry_task() -> TaskResult[str, TaskError]:
    raise ValueError('task mapper should win and retry should trigger')


@app.task(
    task_name='e2e_retry_precedence_global_mapper_retry',
    retry_policy=RetryPolicy.fixed([1], auto_retry_for=['GLOBAL_VALUE_ERROR']),
)
def global_mapper_retry_task() -> TaskResult[str, TaskError]:
    raise ValueError('global mapper should trigger retry')


# Name intentionally collides with built-in ValueError to prove matching is class-based.
ValueErrorClone = type('ValueError', (Exception,), {})


@app.task(
    task_name='e2e_retry_precedence_exception_name_collision',
    retry_policy=RetryPolicy.fixed([1], auto_retry_for=['GLOBAL_VALUE_ERROR']),
)
def exception_name_collision_task() -> TaskResult[str, TaskError]:
    raise ValueErrorClone('same name as ValueError, different class object')


# Register local tasks for worker subprocess discovery
app.discover_tasks(['tests.e2e.tasks.instance_retry_precedence'])
