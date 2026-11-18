"""One broker serves the calls of several event loops and threads.

The broker's core has one pool and one listener for every event loop and
thread of the process. A sync and an async worker ping, each on its own
event loop or thread, both succeed on a pool of one connection.
"""

from __future__ import annotations

import asyncio
import threading
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
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.task_decorator import TaskFunction
from horsies.core.types.result import is_ok
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

POOL_TIMEOUT_SECONDS = 3
THREAD_JOIN_BOUND_SECONDS = 15.0
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


def _one_connection_config() -> PostgresConfig:
    return PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=1,
        max_overflow=0,
        pool_timeout=POOL_TIMEOUT_SECONDS,
    )


@dataclass(frozen=True)
class OneConnectionApp:
    app: Horsies
    broker: PostgresBroker
    echo: TaskFunction[..., int]


@pytest.fixture
def one_connection_app(schema_ready: None) -> Generator[OneConnectionApp, None, None]:
    """App whose broker pool holds exactly one connection, with one task."""
    app = Horsies(AppConfig(queue_mode=QueueMode.DEFAULT, broker=_one_connection_config()))

    @app.task('loop_sets_echo')
    def loop_sets_echo(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    yield OneConnectionApp(app=app, broker=app.get_broker(), echo=loop_sets_echo)
    closed = app.get_broker().close()
    assert closed.is_ok(), closed


def _run_on_thread(call: Callable[[], Any]) -> Any:
    """Run ``call`` on a thread with no event loop; fail if it does not return."""
    box: list[Any] = []
    thread = threading.Thread(target=lambda: box.append(call()), daemon=True)
    thread.start()
    thread.join(THREAD_JOIN_BOUND_SECONDS)
    match box:
        case [outcome]:
            return outcome
        case _:
            pytest.fail(f'call did not return within {THREAD_JOIN_BOUND_SECONDS}s')


class TestListenerPerLoop:
    @pytest.mark.parametrize('async_first', [True, False])
    def test_sync_and_async_pings_both_succeed(
        self, one_connection_app: OneConnectionApp, async_first: bool,
    ) -> None:
        app = one_connection_app.app

        def sync_ping() -> Any:
            return _run_on_thread(lambda: app.ping_workers(timeout_seconds=PING_WINDOW_SECONDS))

        def async_ping() -> Any:
            return asyncio.run(app.ping_workers_async(timeout_seconds=PING_WINDOW_SECONDS))

        match async_first:
            case True:
                outcomes = [async_ping(), sync_ping()]
            case False:
                outcomes = [sync_ping(), async_ping()]

        assert all(is_ok(outcome) for outcome in outcomes), outcomes
