"""A sync send of a producer writes its row on the calling thread.

``PostgresBroker.send_route`` decides. An unkeyed send of a process that
runs no task bodies, once the schema is initialized, runs the enqueue
INSERT on the calling thread through the broker's sync send engine. The
tests count the calls of the sync runner (``run_sync``), each of which
still runs, and the threads of the sync engine's checkouts.
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Any, Final

import psycopg
import pytest
from pydantic import SecretStr
from sqlalchemy import event

from horsies.core.app import Horsies
from horsies.core.brokers import postgres as postgres_module
from horsies.core.brokers.postgres import PostgresBroker, SyncSendRoute
from horsies.core.brokers.result_types import BrokerPayloadMismatch
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.task_send_types import TaskSendEnqueueFailed, TaskSendResult
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.process_role import ProcessRole
from horsies.core.task_decorator import TaskFunction, TaskHandle
from horsies.core.types.result import Err, Ok
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

TASK_NAME: Final = 'caller_thread_echo'
PLAIN_URL: Final = DB_URL.replace('postgresql+psycopg://', 'postgresql://', 1)
SENDER_THREADS: Final = 8
SENDS_PER_THREAD: Final = 25
THREAD_JOIN_BOUND_S: Final = 60.0
BACKEND_EXIT_POLLS: Final = 100
BACKEND_EXIT_POLL_S: Final = 0.02


@pytest.fixture(scope='module')
def schema_ready() -> None:
    """Create the schema with a default broker, then close that broker."""
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
    initialized = broker.run_sync(broker.ensure_schema_initialized)
    closed = broker.close()
    assert initialized.is_ok(), initialized
    assert closed.is_ok(), closed


@dataclass(frozen=True)
class EchoApp:
    app: Horsies
    echo: TaskFunction[..., int]
    broker: PostgresBroker


def _echo_app(*, pool_pre_ping: bool) -> EchoApp:
    """An app with one task, after its first send: that send checks the
    schema on the loop runner, so the next unkeyed sends take the calling
    thread."""
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(
                database_url=SecretStr(DB_URL),
                pool_pre_ping=pool_pre_ping,
            ),
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
    with psycopg.connect(PLAIN_URL, autocommit=True) as connection:
        _ = connection.execute('DELETE FROM horsies_tasks WHERE task_name = %s', (TASK_NAME,))


@pytest.fixture
def echo_app(schema_ready: None) -> Generator[EchoApp]:
    opened = _echo_app(pool_pre_ping=True)
    yield opened
    _close_and_clean(opened)


@pytest.fixture
def echo_app_without_ping(schema_ready: None) -> Generator[EchoApp]:
    opened = _echo_app(pool_pre_ping=False)
    yield opened
    _close_and_clean(opened)


def _bridged(broker: PostgresBroker, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """One entry per call of the sync runner; each call still runs."""
    calls: list[str] = []
    real_run_sync = broker.run_sync

    def _counted(coro_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        calls.append(coro_fn.__name__)
        return real_run_sync(coro_fn, *args, **kwargs)

    monkeypatch.setattr(broker, 'run_sync', _counted)
    return calls


def _task_id(sent: TaskSendResult[TaskHandle[int]]) -> str:
    match sent:
        case Ok(handle):
            return handle.task_id
        case _:
            raise AssertionError(f'expected a sent task, got {sent!r}')


def _rows(task_ids: list[str]) -> int:
    with psycopg.connect(PLAIN_URL, autocommit=True) as connection:
        row = connection.execute(
            'SELECT count(*) FROM horsies_tasks WHERE id = ANY(%s::uuid[])',
            (task_ids,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _sync_send_backend(broker: PostgresBroker) -> int:
    """The backend pid of the sync send engine's pooled connection."""
    engine = broker._sync_send_engine_open()  # pyright: ignore[reportPrivateUsage]
    with engine.connect() as connection:
        return int(connection.exec_driver_sql('SELECT pg_backend_pid()').scalar_one())


def _terminate_and_wait(pid: int) -> None:
    with psycopg.connect(PLAIN_URL, autocommit=True) as connection:
        row = connection.execute('SELECT pg_terminate_backend(%s)', (pid,)).fetchone()
        assert row is not None and row[0] is True, pid
        _wait_until_gone(connection, pid)


def _wait_until_gone(connection: psycopg.Connection[Any], pid: int) -> None:
    for _ in range(BACKEND_EXIT_POLLS):
        row = connection.execute(
            'SELECT count(*) FROM pg_stat_activity WHERE pid = %s', (pid,),
        ).fetchone()
        if row is not None and row[0] == 0:
            return
        time.sleep(BACKEND_EXIT_POLL_S)
    raise AssertionError(f'backend {pid} still runs')


def test_a_producer_send_writes_on_the_calling_thread(
    echo_app: EchoApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert echo_app.broker.send_route(None) is SyncSendRoute.CALLER_THREAD
    bridged = _bridged(echo_app.broker, monkeypatch)
    checkout_threads: list[int] = []
    engine = echo_app.broker._sync_send_engine_open()  # pyright: ignore[reportPrivateUsage]
    event.listen(
        engine.pool,
        'checkout',
        lambda *_: checkout_threads.append(threading.get_ident()),
    )

    task_id = _task_id(echo_app.echo.send(value=1))

    assert bridged == []
    assert checkout_threads == [threading.get_ident()]
    assert _rows([task_id]) == 1


def test_a_keyed_send_stays_on_the_loop_runner(
    echo_app: EchoApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridged = _bridged(echo_app.broker, monkeypatch)

    keyed = echo_app.echo.with_options(idempotency_key=f'key-{uuid.uuid4().hex}')
    task_id = _task_id(keyed.send(value=2))

    assert bridged == ['enqueue_async']
    assert _rows([task_id]) == 1


def test_task_code_in_a_worker_child_stays_on_the_loop_runner(
    echo_app: EchoApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(postgres_module, 'process_role', lambda: ProcessRole.RUNS_TASK_BODIES)
    bridged = _bridged(echo_app.broker, monkeypatch)

    task_id = _task_id(echo_app.echo.send(value=3))

    assert bridged == ['enqueue_async']
    assert _rows([task_id]) == 1


def test_a_taken_task_id_is_compared_on_the_loop_runner(
    echo_app: EchoApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The INSERT on the calling thread finds the id taken; the payload is
    compared on the loop runner: the same SHA is the same send, another
    SHA is a mismatch. Neither writes a second row."""
    task_id = mint_task_id()
    sha = hashlib.sha256(b'first payload').hexdigest()
    other_sha = hashlib.sha256(b'other payload').hexdigest()
    first = echo_app.broker.enqueue(
        TASK_NAME, task_id=task_id, enqueue_sha=sha, kwargs_json='{"value": 4}',
    )
    assert _task_id_of(first) == task_id
    bridged = _bridged(echo_app.broker, monkeypatch)

    replay = echo_app.broker.enqueue(
        TASK_NAME, task_id=task_id, enqueue_sha=sha, kwargs_json='{"value": 4}',
    )
    other = echo_app.broker.enqueue(
        TASK_NAME, task_id=task_id, enqueue_sha=other_sha, kwargs_json='{"value": 5}',
    )

    assert _task_id_of(replay) == task_id
    match other:
        case Err(BrokerPayloadMismatch()):
            pass
        case _:
            raise AssertionError(f'expected a payload mismatch, got {other!r}')
    assert bridged == ['_verify_taken_task_id', '_verify_taken_task_id']
    assert _rows([task_id]) == 1


def _task_id_of(enqueued: object) -> str:
    match enqueued:
        case Ok(str() as task_id):
            return task_id
        case _:
            raise AssertionError(f'expected an enqueued id, got {enqueued!r}')


def test_a_dead_connection_fails_its_send_as_retryable_then_the_next_send_reconnects(
    echo_app_without_ping: EchoApp,
) -> None:
    """With no ping, the send on a connection that the server closed fails
    as a retryable connection error, and SQLAlchemy replaces the connection:
    the next send writes its row."""
    _terminate_and_wait(_sync_send_backend(echo_app_without_ping.broker))

    failed = echo_app_without_ping.echo.send(value=6)
    sent = echo_app_without_ping.echo.send(value=7)

    match failed:
        case Err(TaskSendEnqueueFailed(retryable=True)):
            pass
        case _:
            raise AssertionError(f'expected a retryable enqueue failure, got {failed!r}')
    assert _rows([_task_id(sent)]) == 1


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


def test_close_gives_the_sync_send_connections_back(schema_ready: None) -> None:
    opened = _echo_app(pool_pre_ping=True)
    _ = _task_id(opened.echo.send(value=8))
    pid = _sync_send_backend(opened.broker)

    _close_and_clean(opened)

    with psycopg.connect(PLAIN_URL, autocommit=True) as connection:
        _wait_until_gone(connection, pid)
