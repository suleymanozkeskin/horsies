"""Sync task calls and sync workflow calls share one sync runner.

The pool has one connection. When one sync call holds it and another sync
call waits for it, the waiter must get the connection when the holder
returns it. A waiter on a different event loop is not woken by that return:
it gets the connection only when its own ``pool_timeout`` timer fires, and
after that every wait from the other loop fails with "bound to a different
event loop". The listener binds to one loop in the same way.

The calls run on threads that have no event loop, as in a sync application.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import SecretStr
from sqlalchemy import text

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import HandleErrorCode, HandleResult
from horsies.core.task_decorator import TaskFunction
from horsies.core.types.result import Ok, is_err, is_ok
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

POOL_TIMEOUT_SECONDS = 3
HOLD_SECONDS = 1.0
WAITER_START_DELAY_SECONDS = 0.2
# The holder returns the connection about 0.8 s after the waiter starts. A
# waiter that is not woken by that return finishes only at pool_timeout (3 s).
WAIT_BOUND_SECONDS = 1.5
THREAD_JOIN_BOUND_SECONDS = 15.0
RESULT_WAIT_MS = 300
PING_WINDOW_SECONDS = 0.3


@pytest.fixture(scope='module')
def schema_ready() -> None:
    """Create the schema with a default broker, then close that broker."""

    async def initialize() -> None:
        broker = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
        initialized = await broker.ensure_schema_initialized()
        closed = await broker.close_async()
        assert initialized.is_ok(), initialized
        assert closed.is_ok(), closed

    asyncio.run(initialize())


@dataclass(frozen=True)
class OneConnectionApp:
    app: Horsies
    echo: TaskFunction[..., int]


@pytest.fixture
def one_connection_app(schema_ready: None) -> Generator[OneConnectionApp, None, None]:
    """App whose broker pool holds exactly one connection, with one task."""
    app = Horsies(AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(
            database_url=SecretStr(DB_URL),
            pool_size=1,
            max_overflow=0,
            pool_timeout=POOL_TIMEOUT_SECONDS,
        ),
    ))

    @app.task('sync_bridge_echo')
    def sync_bridge_echo(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    yield OneConnectionApp(app=app, echo=sync_bridge_echo)
    closed = app.get_broker().close()
    assert closed.is_ok(), closed


async def _hold_connection(broker: PostgresBroker, seconds: float) -> None:
    async with broker.async_engine.connect() as connection:
        await connection.execute(text('SELECT pg_sleep(:s)'), {'s': seconds})


async def _hold_connection_result(
    broker: PostgresBroker, seconds: float,
) -> HandleResult[None]:
    await _hold_connection(broker, seconds)
    return Ok(None)


def _start_thread(target: Callable[[], Any]) -> threading.Thread:
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread


def _timed_call(call: Callable[[], Any]) -> tuple[Any, float]:
    """Run ``call`` on a thread with no event loop; fail if it does not return."""
    box: list[Any] = []
    started = time.monotonic()
    thread = _start_thread(lambda: box.append(call()))
    thread.join(THREAD_JOIN_BOUND_SECONDS)
    elapsed = time.monotonic() - started
    match box:
        case [outcome]:
            return outcome, elapsed
        case _:
            pytest.fail(f'call did not return within {THREAD_JOIN_BOUND_SECONDS}s')


def _join(thread: threading.Thread) -> None:
    thread.join(THREAD_JOIN_BOUND_SECONDS)
    assert not thread.is_alive(), 'connection holder did not finish'


def test_workflow_call_gets_connection_released_by_task_runner(
    one_connection_app: OneConnectionApp,
) -> None:
    broker = one_connection_app.app.get_broker()
    holder = _start_thread(
        lambda: broker._loop_runner.call(_hold_connection, broker, HOLD_SECONDS),
    )
    time.sleep(WAITER_START_DELAY_SECONDS)
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=str(uuid.uuid4()), broker=broker,
    )

    outcome, elapsed = _timed_call(handle.status)
    _join(holder)

    assert is_err(outcome), outcome
    assert outcome.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND
    assert elapsed < WAIT_BOUND_SECONDS, f'status() waited {elapsed:.2f}s'


def test_task_send_gets_connection_released_by_workflow_bridge(
    one_connection_app: OneConnectionApp,
) -> None:
    broker = one_connection_app.app.get_broker()
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=str(uuid.uuid4()), broker=broker,
    )
    holder = _start_thread(
        lambda: handle._sync_call(
            _hold_connection_result, 'hold', broker, HOLD_SECONDS,
        ),
    )
    time.sleep(WAITER_START_DELAY_SECONDS)
    echo = one_connection_app.echo

    outcome, elapsed = _timed_call(lambda: echo.send(value=1))
    _join(holder)

    assert is_ok(outcome), outcome
    assert elapsed < WAIT_BOUND_SECONDS, f'send() waited {elapsed:.2f}s'


def test_ping_workers_after_sync_workflow_wait(
    one_connection_app: OneConnectionApp,
) -> None:
    app = one_connection_app.app
    broker = app.get_broker()
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=str(uuid.uuid4()), broker=broker,
    )

    waited, _ = _timed_call(lambda: handle.get(timeout_ms=RESULT_WAIT_MS))
    pinged, _ = _timed_call(
        lambda: app.ping_workers(timeout_seconds=PING_WINDOW_SECONDS),
    )

    assert waited.is_err(), waited
    assert is_ok(pinged), pinged
