"""The broker keeps one typed set of loop-bound resources per open event loop.

Construction and selection do no I/O: engines and the listener connect on
first use, so these brokers never reach a database. Close and release are
observed through recording fakes of ``PostgresListener`` and
``AsyncEngine.dispose``.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Generator
from contextlib import contextmanager
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.app import Horsies
from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.postgres import MAX_CALLER_EVENT_LOOPS, PostgresBroker
from horsies.core.brokers.resources import (
    BrokerResources,
    BuildForLoop,
    EventLoopLimit,
    LimitReached,
    ListenerDisabled,
    ListenerEnabled,
    LoopResources,
    LoopRole,
    SessionEngineSeparate,
    SessionEngineShared,
    UseExisting,
    decide_for_loop,
    split_closed_loops,
)
from horsies.core.brokers.result_types import BrokerErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Result, is_err, is_ok
from horsies.core.worker.child_runner import _discard_inherited_broker
from tests.broker_resources_support import fake_resources

RUNTIME_URL = 'postgresql+psycopg://u:p@localhost:5432/db'
SESSION_URL = 'postgresql+psycopg://u:p@localhost:6543/db'


def _broker(
    *,
    session_url: str | None = None,
    assume_initialized: bool = False,
) -> PostgresBroker:
    config = PostgresConfig(
        database_url=SecretStr(RUNTIME_URL),
        session_database_url=SecretStr(session_url) if session_url else None,
    )
    return PostgresBroker(config, assume_initialized=assume_initialized)


def _select_on(
    broker: PostgresBroker, loop: asyncio.AbstractEventLoop,
) -> Result[BrokerResources, EventLoopLimit]:
    return loop.run_until_complete(broker.select_resources())


def _selected(broker: PostgresBroker) -> BrokerResources:
    """Select on a fresh loop that closes afterwards."""
    async def select() -> Result[BrokerResources, EventLoopLimit]:
        return await broker.select_resources()

    result = asyncio.run(select())
    assert is_ok(result), result
    return result.ok_value


def _listener(resources: BrokerResources) -> PostgresListener:
    match resources.listener:
        case ListenerEnabled(listener=listener):
            return listener
        case ListenerDisabled():
            raise AssertionError('expected a listener')


class _Recorder:
    """Records which listener or engine was closed, and on which thread."""

    def __init__(self) -> None:
        self.closed_listeners: list[tuple[PostgresListener, str]] = []
        self.released_listeners: list[PostgresListener] = []
        self.disposed_engines: list[AsyncEngine] = []


@contextmanager
def _recording() -> Generator[_Recorder, None, None]:
    """Replace listener close/release and engine dispose with recorders.

    Each fake yields to its loop before it records, as the real ones await
    I/O: a cancelled close then records nothing.
    """
    recorder = _Recorder()

    async def close(listener: PostgresListener) -> None:
        await asyncio.sleep(0)
        recorder.closed_listeners.append((listener, threading.current_thread().name))

    async def release(listener: PostgresListener) -> None:
        await asyncio.sleep(0)
        recorder.released_listeners.append(listener)

    async def dispose(engine: AsyncEngine, close: bool = True) -> None:
        _ = close
        await asyncio.sleep(0)
        recorder.disposed_engines.append(engine)

    with (
        patch.object(PostgresListener, 'close', close),
        patch.object(PostgresListener, 'release_after_owner_loop_closed', release),
        patch.object(AsyncEngine, 'dispose', dispose),
    ):
        yield recorder


def _fake_entry(
    *,
    closed: bool = False,
    role: LoopRole = LoopRole.CALLER,
    thread_name: str = 'MainThread',
) -> LoopResources:
    loop = MagicMock(spec=asyncio.AbstractEventLoop)
    loop.is_closed.return_value = closed
    return LoopResources(
        loop=loop, thread_name=thread_name, role=role, resources=fake_resources(),
    )


@pytest.mark.unit
class TestBuildResources:
    def test_same_session_url_shares_the_runtime_engine(self) -> None:
        resources = _selected(_broker())

        assert isinstance(resources.session_engine, SessionEngineShared)
        assert resources.session_async_engine() is resources.engine

    def test_separate_session_url_gets_its_own_engine(self) -> None:
        resources = _selected(_broker(session_url=SESSION_URL))

        match resources.session_engine:
            case SessionEngineSeparate(engine=separate):
                assert separate is not resources.engine
                assert separate.url.port == 6543
                assert resources.session_async_engine() is separate
            case SessionEngineShared():
                pytest.fail('a different session URL must get its own engine')

    def test_listener_enabled_by_default(self) -> None:
        resources = _selected(_broker())

        assert isinstance(resources.listener, ListenerEnabled)

    def test_listener_disabled_when_assume_initialized(self) -> None:
        resources = _selected(_broker(assume_initialized=True))

        assert isinstance(resources.listener, ListenerDisabled)

    def test_construction_builds_no_set(self) -> None:
        broker = _broker()

        assert broker._loop_sets == ()


@pytest.mark.unit
class TestDecideForLoop:
    """The pure decision: one test per outcome."""

    def test_entry_for_the_loop_is_used(self) -> None:
        entry = _fake_entry()

        decision = decide_for_loop(
            (entry,), entry.loop, 'MainThread', LoopRole.CALLER, limit=1,
        )

        assert decision == UseExisting(resources=entry.resources)

    def test_new_caller_loop_below_the_limit_builds(self) -> None:
        loop = MagicMock(spec=asyncio.AbstractEventLoop)

        decision = decide_for_loop(
            (_fake_entry(),), loop, 'MainThread', LoopRole.CALLER, limit=2,
        )

        assert decision == BuildForLoop()

    def test_new_caller_loop_at_the_limit_names_every_held_thread(self) -> None:
        entries = (_fake_entry(thread_name='a'), _fake_entry(thread_name='b'))
        loop = MagicMock(spec=asyncio.AbstractEventLoop)

        decision = decide_for_loop(entries, loop, 'c', LoopRole.CALLER, limit=2)

        assert decision == LimitReached(
            loop_limit=EventLoopLimit(
                limit=2, held_thread_names=('a', 'b'), current_thread_name='c',
            ),
        )

    def test_runner_loop_builds_at_the_limit(self) -> None:
        entries = (_fake_entry(), _fake_entry())
        loop = MagicMock(spec=asyncio.AbstractEventLoop)

        decision = decide_for_loop(
            entries, loop, 'horsies-loop', LoopRole.SYNC_RUNNER, limit=2,
        )

        assert decision == BuildForLoop()

    def test_closed_and_runner_entries_do_not_count(self) -> None:
        entries = (
            _fake_entry(closed=True),
            _fake_entry(role=LoopRole.SYNC_RUNNER, thread_name='horsies-loop'),
        )
        loop = MagicMock(spec=asyncio.AbstractEventLoop)

        decision = decide_for_loop(entries, loop, 'MainThread', LoopRole.CALLER, limit=1)

        assert decision == BuildForLoop()


@pytest.mark.unit
class TestSplitClosedLoops:
    def test_splits_by_loop_state_and_reads_each_state_once(self) -> None:
        open_entry = _fake_entry()
        closed_entry = _fake_entry(closed=True)

        by_state = split_closed_loops((open_entry, closed_entry))

        assert by_state.open_loops == (open_entry,)
        assert by_state.closed_loops == (closed_entry,)
        for entry in (open_entry, closed_entry):
            assert isinstance(entry.loop, MagicMock)
            assert entry.loop.is_closed.call_count == 1


@pytest.mark.unit
class TestEventLoopLimit:
    def test_describe_names_the_limit_every_thread_and_the_retry_condition(self) -> None:
        loop_limit = EventLoopLimit(
            limit=2,
            held_thread_names=('MainThread', 'worker-1'),
            current_thread_name='worker-2',
        )

        text = loop_limit.describe()

        assert 'for 2 open event loops' in text
        assert "'MainThread', 'worker-1'" in text
        assert "'worker-2'" in text
        assert 'until one of those loops closes' in text


@pytest.mark.unit
class TestSelectResources:
    def test_same_loop_gets_the_same_set(self) -> None:
        broker = _broker()

        async def select_twice() -> tuple[BrokerResources, BrokerResources]:
            first = await broker.select_resources()
            second = await broker.select_resources()
            assert is_ok(first) and is_ok(second)
            return first.ok_value, second.ok_value

        first, second = asyncio.run(select_twice())

        assert first is second

    def test_open_loops_get_separate_sets(self) -> None:
        broker = _broker()
        loops = [asyncio.new_event_loop() for _ in range(2)]
        try:
            first, second = (_select_on(broker, loop) for loop in loops)
            assert is_ok(first) and is_ok(second)
            assert first.ok_value is not second.ok_value
            assert len(broker._loop_sets) == 2
        finally:
            for loop in loops:
                loop.close()

    def test_next_call_releases_the_set_of_a_closed_loop(self) -> None:
        broker = _broker()
        first = _selected(broker)

        with _recording() as recorder:
            second = _selected(broker)

        assert second is not first
        assert recorder.released_listeners == [_listener(first)]
        assert recorder.disposed_engines == [first.engine]
        assert [entry.resources for entry in broker._loop_sets] == [second]

    def test_failed_release_is_logged_and_the_call_succeeds(self) -> None:
        broker = _broker()
        first = _selected(broker)
        broker.logger = MagicMock()

        async def failing_release(listener: PostgresListener) -> None:
            raise OSError('close failed')

        with (
            _recording(),
            patch.object(
                PostgresListener, 'release_after_owner_loop_closed', failing_release,
            ),
        ):
            second = _selected(broker)

        assert second is not first
        broker.logger.warning.assert_called_once()
        args = broker.logger.warning.call_args.args
        assert args[1] == 'MainThread'
        assert args[2] == 'OSError'
        assert str(args[3]) == 'close failed'

    def test_release_is_bounded(self) -> None:
        broker = _broker()
        _selected(broker)
        broker.logger = MagicMock()

        async def stuck_release(listener: PostgresListener) -> None:
            await asyncio.sleep(10)

        with (
            _recording(),
            patch.object(
                PostgresListener, 'release_after_owner_loop_closed', stuck_release,
            ),
            patch(
                'horsies.core.brokers.postgres.DEAD_LOOP_RELEASE_TIMEOUT',
                timedelta(milliseconds=50),
            ),
        ):
            result = asyncio.run(asyncio.wait_for(broker.select_resources(), 2))

        assert is_ok(result)
        error = broker.logger.warning.call_args.args[3]
        assert isinstance(error, TimeoutError)
        assert 'did not finish within 0.05 s' in str(error)


@pytest.mark.unit
class TestLoopLimit:
    def _open_caller_loops(
        self, broker: PostgresBroker, count: int,
    ) -> list[asyncio.AbstractEventLoop]:
        loops = [asyncio.new_event_loop() for _ in range(count)]
        for loop in loops:
            assert is_ok(_select_on(broker, loop))
        return loops

    def test_loop_beyond_the_limit_gets_the_limit_error(self) -> None:
        broker = _broker()
        loops = self._open_caller_loops(broker, MAX_CALLER_EVENT_LOOPS)
        extra = asyncio.new_event_loop()
        try:
            result = _select_on(broker, extra)

            assert is_err(result)
            assert result.err_value == EventLoopLimit(
                limit=MAX_CALLER_EVENT_LOOPS,
                held_thread_names=('MainThread',) * MAX_CALLER_EVENT_LOOPS,
                current_thread_name='MainThread',
            )
            assert len(broker._loop_sets) == MAX_CALLER_EVENT_LOOPS
        finally:
            for loop in (*loops, extra):
                loop.close()

    def test_closed_loop_frees_a_place(self) -> None:
        broker = _broker()
        loops = self._open_caller_loops(broker, MAX_CALLER_EVENT_LOOPS)
        loops[0].close()
        extra = asyncio.new_event_loop()
        try:
            with _recording() as recorder:
                result = _select_on(broker, extra)

            assert is_ok(result)
            assert len(recorder.released_listeners) == 1
            assert len(broker._loop_sets) == MAX_CALLER_EVENT_LOOPS
        finally:
            for loop in (*loops, extra):
                loop.close()

    def test_sync_runner_is_not_counted(self) -> None:
        broker = _broker()
        loops = self._open_caller_loops(broker, MAX_CALLER_EVENT_LOOPS)
        try:
            result = broker.run_sync(broker.select_resources)

            assert is_ok(result)
            roles = [entry.role for entry in broker._loop_sets]
            assert roles.count(LoopRole.SYNC_RUNNER) == 1
        finally:
            broker._loop_runner.stop()
            for loop in loops:
                loop.close()


@pytest.mark.unit
class TestCloseAsync:
    def test_closes_the_running_loops_set_and_releases_a_closed_loops_set(self) -> None:
        broker = _broker()
        dead = _selected(broker)

        async def select_then_close() -> tuple[BrokerResources, Any]:
            selected = await broker.select_resources()
            assert is_ok(selected)
            return selected.ok_value, await broker.close_async()

        with _recording() as recorder:
            # The select releases the dead loop's set; close closes the new one.
            current, closed = asyncio.run(select_then_close())

        assert is_ok(closed)
        assert recorder.closed_listeners == [(_listener(current), 'MainThread')]
        assert recorder.released_listeners == [_listener(dead)]
        assert broker._loop_sets == ()

    def test_close_releases_a_closed_loops_set_it_holds(self) -> None:
        broker = _broker()
        loop = asyncio.new_event_loop()
        held = _select_on(broker, loop)
        assert is_ok(held)
        loop.close()

        async def close_only() -> Any:
            # No select: close_async itself must release the closed loop's set.
            return await broker.close_async()

        with _recording() as recorder:
            closed = asyncio.run(close_only())

        assert is_ok(closed)
        assert recorder.released_listeners == [_listener(held.ok_value)]
        assert recorder.closed_listeners == []

    def test_hands_off_to_another_open_loop(self) -> None:
        broker = _broker()
        owner_loop = asyncio.new_event_loop()
        owner = threading.Thread(target=owner_loop.run_forever, name='owner-thread', daemon=True)
        owner.start()
        try:
            held = asyncio.run_coroutine_threadsafe(broker.select_resources(), owner_loop).result(5)
            assert is_ok(held)

            with _recording() as recorder:
                closed = asyncio.run(broker.close_async())

            assert is_ok(closed)
            assert recorder.closed_listeners == [(_listener(held.ok_value), 'owner-thread')]
        finally:
            owner_loop.call_soon_threadsafe(owner_loop.stop)
            owner.join(5)
            owner_loop.close()

    def test_owner_loop_that_does_not_run_fails_within_the_bound(self) -> None:
        broker = _broker()
        paused_loop = asyncio.new_event_loop()
        try:
            held = _select_on(broker, paused_loop)
            assert is_ok(held)

            with (
                _recording() as recorder,
                patch(
                    'horsies.core.brokers.postgres.CLOSE_HANDOFF_TIMEOUT',
                    timedelta(milliseconds=50),
                ),
            ):
                closed = asyncio.run(asyncio.wait_for(broker.close_async(), 2))

                assert is_err(closed)
                assert closed.err_value.code == BrokerErrorCode.CLOSE_FAILED
                assert "thread 'MainThread'" in closed.err_value.message
                assert 'stays scheduled' in closed.err_value.message
                assert recorder.closed_listeners == []

                # The close stays scheduled: it runs when the loop runs again.
                paused_loop.run_until_complete(asyncio.sleep(0.05))

            assert recorder.closed_listeners == [(_listener(held.ok_value), 'MainThread')]
        finally:
            paused_loop.close()


    def test_close_cancelled_on_the_owner_loop_is_a_failure_not_a_cancel(self) -> None:
        broker = _broker()
        owner_loop = asyncio.new_event_loop()
        owner = threading.Thread(target=owner_loop.run_forever, name='owner-thread', daemon=True)
        owner.start()

        async def cancelled_close(listener: PostgresListener) -> None:
            raise asyncio.CancelledError

        try:
            held = asyncio.run_coroutine_threadsafe(broker.select_resources(), owner_loop).result(5)
            assert is_ok(held)

            with _recording(), patch.object(PostgresListener, 'close', cancelled_close):
                closed = asyncio.run(broker.close_async())

            assert is_err(closed)
            assert closed.err_value.code == BrokerErrorCode.CLOSE_FAILED
            assert "thread 'owner-thread' was cancelled on that loop" in closed.err_value.message
        finally:
            owner_loop.call_soon_threadsafe(owner_loop.stop)
            owner.join(5)
            owner_loop.close()

    def test_owner_loop_closed_during_the_hand_off_is_released_here(self) -> None:
        broker = _broker()
        loop = asyncio.new_event_loop()
        held = _select_on(broker, loop)
        assert is_ok(held)
        loop.close()
        (entry,) = broker._loop_sets

        with _recording() as recorder:
            # The close rule saw an open loop; it closed before the hand-off.
            errors = asyncio.run(broker._hand_off_close(entry))

        assert errors == []
        assert recorder.released_listeners == [_listener(held.ok_value)]
        assert recorder.closed_listeners == []


def _app_with(broker: PostgresBroker | None) -> Horsies:
    app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(RUNTIME_URL))))
    app._broker = broker
    return app


@pytest.mark.unit
class TestDiscardInheritedBroker:
    """A forked child drops the parent's pools without closing their connections."""

    def test_disposes_every_engine_of_every_set_with_close_false(self) -> None:
        broker = _broker(session_url=SESSION_URL)
        loops = [asyncio.new_event_loop() for _ in range(2)]
        sets: list[BrokerResources] = []
        for loop in loops:
            selected = _select_on(broker, loop)
            assert is_ok(selected)
            sets.append(selected.ok_value)
        app = _app_with(broker)
        engines: list[Engine] = []
        for resources in sets:
            separate = resources.session_engine
            assert isinstance(separate, SessionEngineSeparate)
            engines.extend([resources.engine.sync_engine, separate.engine.sync_engine])

        try:
            with patch('sqlalchemy.engine.Engine.dispose', autospec=True) as dispose:
                _discard_inherited_broker(app)

            assert [call.args[0] for call in dispose.call_args_list] == engines
            assert all(call.kwargs == {'close': False} for call in dispose.call_args_list)
            assert broker._loop_sets == ()
            assert app._broker is None
        finally:
            for loop in loops:
                loop.close()

    def test_shared_session_engine_is_disposed_once(self) -> None:
        broker = _broker()
        resources = _selected(broker)
        app = _app_with(broker)

        with patch.object(resources.engine.sync_engine, 'dispose') as runtime_dispose:
            _discard_inherited_broker(app)

        runtime_dispose.assert_called_once_with(close=False)
        assert app._broker is None

    def test_without_a_broker_does_nothing(self) -> None:
        app = _app_with(None)

        _discard_inherited_broker(app)

        assert app._broker is None
