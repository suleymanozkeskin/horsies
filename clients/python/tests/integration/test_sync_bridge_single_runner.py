"""A sync workflow wait is followed by a working ``ping_workers``.

The pool has one connection. The sync facades call the core's blocking forms
on their own thread, on the core pool, so the connection of the wait is free
for the ping.

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

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.task_decorator import TaskFunction
from horsies.core.types.result import Err, Ok, Result, is_ok
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

POOL_TIMEOUT_SECONDS = 3
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
            pool_timeout_seconds=POOL_TIMEOUT_SECONDS,
        ),
    ))

    @app.task('sync_bridge_echo')
    def sync_bridge_echo(*, value: int) -> Result[int, TaskError]:
        return Ok(value)

    yield OneConnectionApp(app=app, echo=sync_bridge_echo)
    closed = app.get_broker().close()
    assert closed.is_ok(), closed


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


def test_ping_workers_after_sync_workflow_wait(
    one_connection_app: OneConnectionApp,
) -> None:
    app = one_connection_app.app
    broker = app.get_broker()
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=uuid.uuid4(), broker=broker,
    )

    waited, _ = _timed_call(lambda: handle.get(timeout_ms=RESULT_WAIT_MS))
    pinged, _ = _timed_call(
        lambda: app.ping_workers(timeout_seconds=PING_WINDOW_SECONDS),
    )

    assert isinstance(waited, Err), waited
    assert is_ok(pinged), pinged
