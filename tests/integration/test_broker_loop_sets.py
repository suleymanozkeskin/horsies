"""One broker serves several event loops, each with its own resource set.

The pool has one connection, so a call that waits for another loop's
connection would show as a wait. Each loop's set has its own pool and
listener: a call never waits for a connection that another loop holds, a
loop that closed has its connections released at the next call, and close
reaches every set. Connections are identified by their backend pid in
``pg_stat_activity``.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.app import Horsies
from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import BrokerResources, ListenerEnabled
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.task_decorator import TaskFunction
from horsies.core.types.result import is_ok
from horsies.core.utils.url import to_psycopg_url
from tests.broker_resources_support import current_resources, resources_of
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

POOL_TIMEOUT_SECONDS = 3
HOLD_SECONDS = 1.0
WAITER_START_DELAY_SECONDS = 0.2
# The holder keeps the connection 0.8 s after the waiter starts. A waiter on
# the same pool finishes no sooner than that; a waiter with its own pool
# needs only its own round trip.
NO_WAIT_BOUND_SECONDS = 0.5
THREAD_JOIN_BOUND_SECONDS = 15.0
PING_WINDOW_SECONDS = 0.3
PROBE_CHANNEL = 'horsies_loop_sets_probe'


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
    """App whose broker pools hold exactly one connection per set, with one task."""
    app = Horsies(AppConfig(queue_mode=QueueMode.DEFAULT, broker=_one_connection_config()))

    @app.task('loop_sets_echo')
    def loop_sets_echo(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    yield OneConnectionApp(app=app, broker=app.get_broker(), echo=loop_sets_echo)
    closed = app.get_broker().close()
    assert closed.is_ok(), closed


async def _hold(engine: AsyncEngine, seconds: float) -> None:
    async with engine.connect() as connection:
        await connection.execute(text('SELECT pg_sleep(:s)'), {'s': seconds})


async def _hold_on_this_loop(broker: PostgresBroker, seconds: float) -> None:
    await _hold(current_resources(broker).engine, seconds)


async def _engine_pid(engine: AsyncEngine) -> int:
    async with engine.connect() as connection:
        pid = (await connection.execute(text('SELECT pg_backend_pid()'))).scalar_one()
    assert isinstance(pid, int)
    return pid


def _listener_of(resources: BrokerResources) -> PostgresListener:
    match resources.listener:
        case ListenerEnabled(listener=listener):
            return listener
        case _:
            raise AssertionError('expected a listener')


async def _pids_on_this_loop(broker: PostgresBroker) -> list[int]:
    """Open this loop's pool connection and both listener connections."""
    resources = await resources_of(broker)
    listener = _listener_of(resources)
    listened = await listener.listen(PROBE_CHANNEL)
    assert is_ok(listened), listened
    pids = [await _engine_pid(resources.engine)]
    for connection in (listener._dispatcher_conn, listener._command_conn):  # pyright: ignore[reportPrivateUsage]
        assert connection is not None
        pids.append(connection.info.backend_pid)
    return pids


def _alive(pids: list[int]) -> set[int]:
    with psycopg.connect(to_psycopg_url(DB_URL), autocommit=True) as connection:
        rows = connection.execute(
            'SELECT pid FROM pg_stat_activity WHERE pid = ANY(%s)', (pids,),
        ).fetchall()
    return {row[0] for row in rows}


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


class TestNoWaitAcrossLoops:
    def test_sync_send_does_not_wait_for_an_async_holder(
        self, one_connection_app: OneConnectionApp,
    ) -> None:
        broker, echo = one_connection_app.broker, one_connection_app.echo
        assert is_ok(echo.send(value=0))  # the sync set is ready

        async def main() -> float:
            assert is_ok(await echo.send_async(value=0))  # this loop's set is ready
            holder = asyncio.create_task(_hold_on_this_loop(broker, HOLD_SECONDS))
            await asyncio.sleep(WAITER_START_DELAY_SECONDS)
            started = time.monotonic()
            outcome = await asyncio.to_thread(lambda: echo.send(value=1))
            elapsed = time.monotonic() - started
            await holder
            assert is_ok(outcome), outcome
            return elapsed

        elapsed = asyncio.run(main())

        assert elapsed < NO_WAIT_BOUND_SECONDS, f'send() waited {elapsed:.2f}s'

    def test_async_send_does_not_wait_for_a_sync_holder(
        self, one_connection_app: OneConnectionApp,
    ) -> None:
        broker, echo = one_connection_app.broker, one_connection_app.echo
        assert is_ok(echo.send(value=0))

        async def main() -> float:
            assert is_ok(await echo.send_async(value=0))
            holder = asyncio.create_task(asyncio.to_thread(
                lambda: broker.run_sync(_hold_on_this_loop, broker, HOLD_SECONDS),
            ))
            await asyncio.sleep(WAITER_START_DELAY_SECONDS)
            started = time.monotonic()
            outcome = await echo.send_async(value=1)
            elapsed = time.monotonic() - started
            await holder
            assert is_ok(outcome), outcome
            return elapsed

        elapsed = asyncio.run(main())

        assert elapsed < NO_WAIT_BOUND_SECONDS, f'send_async() waited {elapsed:.2f}s'


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


class TestClosedLoopRelease:
    def test_next_loop_releases_the_closed_loops_connections(self, schema_ready: None) -> None:
        broker = PostgresBroker(_one_connection_config())

        async def contend() -> None:
            # Two checkouts on a pool of one: the second waits, so the
            # pool's wait queue binds to this loop.
            engine = (await resources_of(broker)).engine
            await asyncio.gather(_hold(engine, 0.2), _hold(engine, 0.2))

        async def first_loop() -> list[int]:
            await contend()
            return await _pids_on_this_loop(broker)

        pids = asyncio.run(first_loop())
        assert _alive(pids) == set(pids)

        asyncio.run(contend())

        assert _alive(pids) == set()
        assert is_ok(broker.close())

    def test_paused_loop_keeps_its_set_while_short_loops_come_and_go(
        self, schema_ready: None,
    ) -> None:
        """The pytest-asyncio shape: a session loop stays open, test loops close."""
        broker = PostgresBroker(_one_connection_config())
        paused = asyncio.new_event_loop()
        try:
            assert is_ok(paused.run_until_complete(broker.ping_database_async()))
            for _ in range(3):
                assert is_ok(asyncio.run(broker.ping_database_async()))
                assert len(broker._loop_sets) == 2  # pyright: ignore[reportPrivateUsage]
            assert is_ok(paused.run_until_complete(broker.ping_database_async()))
        finally:
            closed = paused.run_until_complete(broker.close_async())
            paused.close()
        assert is_ok(closed), closed


class TestCloseReachesEverySet:
    def test_close_async_closes_its_own_and_the_runner_set(self, schema_ready: None) -> None:
        broker = PostgresBroker(_one_connection_config())
        runner_pids = broker.run_sync(_pids_on_this_loop, broker)

        async def main() -> tuple[list[int], Any]:
            pids = await _pids_on_this_loop(broker)
            return pids, await broker.close_async()

        caller_pids, closed = asyncio.run(main())

        assert is_ok(closed), closed
        assert _alive([*runner_pids, *caller_pids]) == set()

    def test_sync_close_releases_a_closed_loops_set(self, schema_ready: None) -> None:
        broker = PostgresBroker(_one_connection_config())
        runner_pids = broker.run_sync(_pids_on_this_loop, broker)
        # Last call before close: no later selection releases this set first.
        caller_pids = asyncio.run(_pids_on_this_loop(broker))
        assert _alive(caller_pids) == set(caller_pids)

        closed = broker.close()

        assert is_ok(closed), closed
        assert _alive([*caller_pids, *runner_pids]) == set()

    def test_close_hands_off_to_the_loop_that_owns_a_set(self, schema_ready: None) -> None:
        broker = PostgresBroker(_one_connection_config())
        owner_loop = asyncio.new_event_loop()
        owner = threading.Thread(target=owner_loop.run_forever, name='owner', daemon=True)
        owner.start()
        try:
            owner_pids = asyncio.run_coroutine_threadsafe(
                _pids_on_this_loop(broker), owner_loop,
            ).result(THREAD_JOIN_BOUND_SECONDS)

            closed = asyncio.run(broker.close_async())

            assert is_ok(closed), closed
            assert _alive(owner_pids) == set()
        finally:
            owner_loop.call_soon_threadsafe(owner_loop.stop)
            owner.join(THREAD_JOIN_BOUND_SECONDS)
            owner_loop.close()
