"""A failed enqueue statement does not show the task's arguments.

The broker's engines are created with ``hide_parameters=True``. When the
enqueue INSERT fails (here: a lock timeout while another session holds the
table lock), the send returns ``ENQUEUE_FAILED`` whose message names the
cause and the statement, and neither the message nor the exception text
holds the bound values.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import SecretStr
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.task_send_types import TaskSendEnqueueFailed
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_err
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)

__all__ = ['make_database']

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ARGUMENT_MARKER = 'bound-value-marker-5d1e'
LOCK_TIMEOUT_MS = 200


def _with_lock_timeout(url: str) -> str:
    # Percent-encoded: the psycopg URL parser does not read '+' as a space.
    assert make_url(url).query == {}
    return f'{url}?options=-c%20lock_timeout%3D{LOCK_TIMEOUT_MS}'


def _secret_task(app: Horsies) -> Any:
    @app.task(task_name='hide_parameters_secret_task')
    def secret_task(*, secret: str) -> TaskResult[str, TaskError]:
        return TaskResult(ok=secret)

    return secret_task


async def test_failed_enqueue_hides_the_task_arguments(
    make_database: MakeDatabase,
) -> None:
    url = await make_database()
    config = PostgresConfig(database_url=SecretStr(_with_lock_timeout(url)))
    app = Horsies(AppConfig(queue_mode=QueueMode.DEFAULT, broker=config))
    broker = PostgresBroker(config)
    app._broker = broker  # pyright: ignore[reportPrivateUsage]
    broker.app = app
    secret_task = _secret_task(app)
    lock_engine = create_async_engine(url)
    try:
        initialized = await broker.ensure_schema_initialized()
        assert initialized.is_ok(), initialized
        async with lock_engine.begin() as lock_connection:
            await lock_connection.execute(
                text('LOCK TABLE horsies_tasks IN ACCESS EXCLUSIVE MODE'),
            )
            sent = await secret_task.send_async(secret=ARGUMENT_MARKER)
    finally:
        await lock_engine.dispose()
        await broker.close_async()

    assert is_err(sent), sent
    error = sent.err_value
    assert isinstance(error, TaskSendEnqueueFailed)
    assert 'canceling statement due to lock timeout' in error.message
    assert 'INSERT INTO horsies_tasks' in error.message
    assert ARGUMENT_MARKER not in error.message
    assert isinstance(error.exception, BaseException)
    assert ARGUMENT_MARKER not in str(error.exception)
