"""Soft-cap app instance for e2e lease contention tests.

Uses prefetch_buffer > 0 with a short claim_lease_ms so prefetched
claims expire quickly, exercising the lease-expiry branch in
_confirm_ownership_and_set_running.
"""

from __future__ import annotations

import os
import socket

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskResult, TaskError

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    prefetch_buffer=2,
    claim_lease_ms=500,
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


@app.task(task_name='e2e_softcap_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_softcap_slow')
def slow_task(duration_ms: int) -> TaskResult[str, TaskError]:
    """Task that sleeps for specified duration."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')


@app.task(task_name='e2e_softcap_blocker')
def blocker_task(duration_ms: int) -> TaskResult[str, TaskError]:
    """Single-process blocker used to keep a worker process busy during reclaim races."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'blocked_{duration_ms}')


@app.task(task_name='e2e_softcap_slow_idempotent')
def slow_idempotent_task(token: str, duration_ms: int) -> TaskResult[str, TaskError]:
    """Slow task with atomic file detection for double-execution testing.

    Sleeps first (creating process pool bottleneck that causes lease expiry),
    then uses O_CREAT|O_EXCL to detect if the same token was already executed.
    """
    import time

    time.sleep(duration_ms / 1000)

    log_dir = os.environ.get('E2E_IDEMPOTENT_LOG_DIR')
    if not log_dir:
        return TaskResult(
            err=TaskError(
                error_code='CONFIG_ERROR',
                message='E2E_IDEMPOTENT_LOG_DIR not set',
            )
        )

    token_file = os.path.join(log_dir, token)
    try:
        fd = os.open(token_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, b'executed')
        os.close(fd)
        return TaskResult(ok=f'executed:{token}')
    except FileExistsError:
        return TaskResult(
            err=TaskError(
                error_code='DOUBLE_EXECUTION',
                message=f'Token {token} already executed',
            )
        )


@app.task(task_name='e2e_softcap_db_ledger')
def db_ledger_task(token: str) -> TaskResult[str, TaskError]:
    """Record task-body entry in DB and enforce a single winner via unique token."""
    import psycopg

    db_url = DB_URL.replace('postgresql+psycopg://', 'postgresql://', 1)
    worker_pid = os.getpid()
    worker_identity = f'{socket.gethostname()}:{worker_pid}'

    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO e2e_execution_attempts(token, worker_identity, worker_pid)
                    VALUES (%s, %s, %s)
                    """,
                    (token, worker_identity, worker_pid),
                )
                cur.execute(
                    """
                    INSERT INTO e2e_execution_winner(token, worker_identity, worker_pid)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (token) DO NOTHING
                    RETURNING token
                    """,
                    (token, worker_identity, worker_pid),
                )
                winner_row = cur.fetchone()
            conn.commit()
    except Exception as exc:
        return TaskResult(
            err=TaskError(
                error_code='LEDGER_DB_ERROR',
                message=f'{type(exc).__name__}: {exc}',
                data={'token': token, 'worker_identity': worker_identity},
            )
        )

    if winner_row is None:
        return TaskResult(
            err=TaskError(
                error_code='DOUBLE_EXECUTION',
                message=f'Token {token} already won by another worker',
                data={'token': token, 'worker_identity': worker_identity},
            )
        )

    return TaskResult(ok=f'winner:{token}:{worker_identity}')
