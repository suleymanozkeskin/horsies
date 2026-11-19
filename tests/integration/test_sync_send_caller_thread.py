"""Sync sends of a producer from many threads each write their row.

The sync send of a producer runs ``CoreHandle.enqueue`` on the calling
thread. Several threads that send through one app at the same time each get
their own task id, and each send writes one row.
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import Generator
from dataclasses import dataclass
from typing import Final

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.task_decorator import TaskFunction, TaskHandle
from horsies.core.types.result import Ok
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

TASK_NAME: Final = 'caller_thread_echo'
SENDER_THREADS: Final = 8
SENDS_PER_THREAD: Final = 25
THREAD_JOIN_BOUND_S: Final = 60.0


@pytest.fixture(scope='module')
def schema_ready() -> None:
    """Create the schema with a default broker, then close that broker."""
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
    initialized = asyncio.run(broker.ensure_schema_initialized())
    closed = broker.close()
    assert initialized.is_ok(), initialized
    assert closed.is_ok(), closed


@dataclass(frozen=True)
class EchoApp:
    app: Horsies
    echo: TaskFunction[..., int]
    broker: PostgresBroker


def _echo_app() -> EchoApp:
    """An app with one task, after its first send."""
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr(DB_URL)),
        ),
    )

    @app.task(TASK_NAME)
    def caller_thread_echo(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    first = caller_thread_echo.send(value=0)
    assert first.is_ok(), first
    return EchoApp(app=app, echo=caller_thread_echo, broker=app.get_broker())


def _close_and_clean(echo_app: EchoApp) -> None:
    closed = echo_app.broker.close()
    assert closed.is_ok(), closed
    with psycopg.connect(DB_URL, autocommit=True) as connection:
        _ = connection.execute('DELETE FROM horsies_tasks WHERE task_name = %s', (TASK_NAME,))


@pytest.fixture
def echo_app(schema_ready: None) -> Generator[EchoApp]:
    opened = _echo_app()
    yield opened
    _close_and_clean(opened)


def _task_id(sent: TaskSendResult[TaskHandle[int]]) -> uuid.UUID:
    match sent:
        case Ok(handle):
            return handle.task_id
        case _:
            raise AssertionError(f'expected a sent task, got {sent!r}')


def _rows(task_ids: list[uuid.UUID]) -> int:
    with psycopg.connect(DB_URL, autocommit=True) as connection:
        row = connection.execute(
            'SELECT count(*) FROM horsies_tasks WHERE id = ANY(%s::uuid[])',
            (task_ids,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def test_sends_from_many_threads_each_write_their_row(echo_app: EchoApp) -> None:
    sent: list[TaskSendResult[TaskHandle[int]]] = []

    def _sender() -> None:
        for value in range(SENDS_PER_THREAD):
            sent.append(echo_app.echo.send(value=value))

    senders = [threading.Thread(target=_sender) for _ in range(SENDER_THREADS)]
    for sender in senders:
        sender.start()
    for sender in senders:
        sender.join(THREAD_JOIN_BOUND_S)
        assert not sender.is_alive(), 'a sender did not finish'

    task_ids = [_task_id(outcome) for outcome in sent]
    assert len(set(task_ids)) == SENDER_THREADS * SENDS_PER_THREAD
    assert _rows(task_ids) == len(task_ids)
