"""An idle producer has no ``horsies-loop`` thread.

The sync facades call the core's blocking forms on the calling thread, so a
producer that sent a task and read it with the sync API starts no Python
thread of its own for those calls.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Final

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_ok
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

TASK_NAME: Final = 'idle_producer_echo'
LOOP_THREAD_NAME: Final = 'horsies-loop'
"""The name of the thread of the removed Python sync runner."""


@pytest.fixture(scope='module')
def schema_ready() -> None:
    """Create the schema with a default broker, then close that broker."""
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
    initialized = asyncio.run(broker.ensure_schema_initialized())
    closed = broker.close()
    assert initialized.is_ok(), initialized
    assert closed.is_ok(), closed


def _thread_names() -> list[str]:
    return [thread.name for thread in threading.enumerate()]


def test_an_idle_producer_has_no_loop_thread(schema_ready: None) -> None:
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr(DB_URL)),
        ),
    )

    @app.task(TASK_NAME)
    def idle_producer_echo(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    try:
        sent = idle_producer_echo.send(value=1)
        assert is_ok(sent), sent
        info = app.get_task_info(sent.ok_value.task_id)
        assert is_ok(info), info
        pinged = app.ping_database()
        assert is_ok(pinged), pinged

        assert LOOP_THREAD_NAME not in _thread_names()
    finally:
        closed = app.get_broker().close()
        assert closed.is_ok(), closed
        with psycopg.connect(DB_URL, autocommit=True) as connection:
            _ = connection.execute(
                'DELETE FROM horsies_tasks WHERE task_name = %s', (TASK_NAME,),
            )

    assert LOOP_THREAD_NAME not in _thread_names()
