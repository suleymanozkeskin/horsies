"""The handler child program (``horsies.core.worker.handler_child``).

The inbox of a handler, the decode of the wire text of an event, the typed
dispatch of each event type, ``def`` and ``async def`` handlers, a handler
that raises and a handler that blocks, the ``handler_stats`` frames, the
``on_handler_child_start`` hooks, the boot step, the control thread, and the
branch of the child program on the kind of its init frame.

The tests that serve a whole child run ``serve_handler_child`` on a thread,
over a socket pair: the test writes the frames of the worker on one end, and
records the frames of the child where it writes them
(``_send_child_frame``). The exit of the child raises in place of ending
the process.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import gc
import json
import logging
import socket
import threading
import time
import warnings
from collections.abc import Callable, Coroutine, Iterator
from pathlib import Path
from typing import Any, Final, NoReturn, cast

import pytest
from pydantic import SecretStr

from horsies import _core as core
from horsies._catalog import events
from horsies.core.app import Horsies
from horsies.core.event_handlers import EventHandler
from horsies.core.models.app import AppConfig
from horsies.core.models.boot_failure import AppLoadFailed, ThreadStartRefused
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import child_main, handler_child
from horsies.core.worker.child_frames import (
    BootFailedFrame,
    ChildFrame,
    HandlerCounts,
    HandlerStatsFrame,
    ReadyFrame,
)
from horsies.core.worker.child_main import ParentFrameReader, read_python_init_frame
from horsies.core.worker.child_runner import CHILD_HOOK_FAILURE_EXIT_CODE
from horsies.core.worker.handler_child import (
    EventDispatch,
    HandlerInbox,
    InboxClosed,
    InboxFull,
    NoWorkerEndedSpans,
    NotJsonText,
    OnLoop,
    Queued,
    RunningHandler,
    decode_event_text,
    run_handler_child_start_hooks,
    start_handlers,
    stats_frame,
)
from horsies.telemetry import AnyEvent, AttemptEnded, TaskClaimed
from tests.fixtures.core_frames import framed, handler_kind, init_frame, init_meta

pytestmark = [pytest.mark.unit]

_REPO_ROOT: Final = Path(__file__).resolve().parents[4]
_VECTORS: Final = (
    _REPO_ROOT / 'core' / 'tests' / 'fixtures' / 'event_wire' / 'vectors.json'
)
_WAIT_S: Final = 5.0
"""The longest wait of a test for a thread of the child."""


def _wire_texts() -> list[tuple[str, str]]:
    """The name and the wire text of each shared event vector, as the core
    writes it in an ``events`` frame."""
    document = cast('dict[str, Any]', json.loads(_VECTORS.read_text(encoding='utf-8')))
    vectors = cast('list[dict[str, Any]]', document['vectors'])
    return [
        (cast('str', vector['name']), json.dumps(vector['wire'], separators=(',', ':')))
        for vector in vectors
    ]


WIRE_TEXTS: Final = _wire_texts()


def _texts_of(tag: str) -> list[str]:
    return [text for _name, text in WIRE_TEXTS if json.loads(text)[0] == tag]


def _attempt_ended_text(attempt: int) -> str:
    """The wire text of an ``AttemptEnded`` event of ``attempt``."""
    wire = cast('list[Any]', json.loads(_texts_of('AttemptEnded')[0]))
    wire[6] = attempt
    return json.dumps(wire, separators=(',', ':'))


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://user:pass@localhost/db')
            ),
        ),
    )


def _named[F: Callable[..., object]](function: F, name: str) -> F:
    """``function`` with the qualified name ``name``, so that its handler
    name is unique in a test."""
    setattr(function, '__qualname__', name)
    return function


def _ignoring(name: str) -> Callable[[AnyEvent], None]:
    """A handler named ``name`` that does nothing."""

    def ignore(event: AnyEvent) -> None:
        del event

    return _named(ignore, name)


def _started(app: Horsies) -> tuple[RunningHandler, ...]:
    match start_handlers(app.get_event_handlers()):
        case Ok(running):
            return running
        case Err(failed):
            pytest.fail(f'the handlers start, got {failed}')


def _stop(running: tuple[RunningHandler, ...]) -> None:
    for one in running:
        one.inbox.close()
    for one in running:
        one.join(_WAIT_S)


def _wait_until(condition: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + _WAIT_S
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f'waited {_WAIT_S}s for {what}')
        time.sleep(0.005)


class TestHandlerInbox:
    def test_events_leave_in_the_order_they_came(self) -> None:
        inbox = HandlerInbox(4)
        first, second = (object.__new__(AttemptEnded), object.__new__(AttemptEnded))

        assert inbox.offer(first) == Queued()
        assert inbox.offer(second) == Queued()

        assert inbox.take() is first
        assert inbox.take() is second

    def test_a_full_inbox_refuses_the_event(self) -> None:
        inbox = HandlerInbox(2)
        for _ in range(2):
            assert inbox.offer(object.__new__(TaskClaimed)) == Queued()

        assert inbox.offer(object.__new__(TaskClaimed)) == InboxFull()
        assert inbox.waiting() == 2

    def test_a_closed_inbox_gives_the_events_left_then_closed(self) -> None:
        inbox = HandlerInbox(4)
        event = object.__new__(TaskClaimed)
        _ = inbox.offer(event)

        inbox.close()

        assert inbox.take() is event
        assert inbox.take() == InboxClosed()

    def test_a_take_waits_for_an_offer(self) -> None:
        inbox = HandlerInbox(1)
        taken: list[object] = []
        taker = threading.Thread(target=lambda: taken.append(inbox.take()))
        taker.start()
        event = object.__new__(TaskClaimed)

        _ = inbox.offer(event)
        taker.join(_WAIT_S)

        assert taken == [event]

    def test_a_close_ends_a_waiting_take(self) -> None:
        inbox = HandlerInbox(1)
        taken: list[object] = []
        taker = threading.Thread(target=lambda: taken.append(inbox.take()))
        taker.start()

        inbox.close()
        taker.join(_WAIT_S)

        assert taken == [InboxClosed()]


class TestDecodeEventText:
    @pytest.mark.parametrize(
        ('name', 'text'), WIRE_TEXTS, ids=[name for name, _ in WIRE_TEXTS]
    )
    def test_each_vector_decodes_to_the_event_of_its_tag(
        self, name: str, text: str
    ) -> None:
        match decode_event_text(text):
            case Ok(event):
                assert type(event).__name__ == json.loads(text)[0], name
            case Err(refused):
                pytest.fail(f'{name} decodes, got {refused}')

    def test_a_text_that_is_not_json_is_refused(self) -> None:
        match decode_event_text('["TaskClaimed",'):
            case Err(NotJsonText()):
                pass
            case other:
                pytest.fail(f'a cut text is not JSON text, got {other}')

    def test_a_text_that_the_strict_parser_refuses_is_not_json_text(self) -> None:
        """The codec parser refuses what `json.loads` takes: a non-finite
        number and a repeated key."""
        for text in ('["TaskClaimed", NaN]', '["TaskClaimed", {"a": 1, "a": 2}]'):
            match decode_event_text(text):
                case Err(NotJsonText()):
                    pass
                case other:
                    pytest.fail(f'{text} is not JSON text of the codec, got {other}')

    def test_an_unknown_event_type_is_refused_by_its_tag(self) -> None:
        match decode_event_text('["TaskTeleported",1]'):
            case Err(events.UnknownEventType(tag=tag)):
                assert tag == 'TaskTeleported'
            case other:
                pytest.fail(f'an unknown tag is refused, got {other}')


def _recorder(
    received: dict[str, list[AnyEvent]],
    event_class: type[AnyEvent],
) -> Callable[[AnyEvent], None]:
    received[event_class.__name__] = []

    def record(event: AnyEvent) -> None:
        received[event_class.__name__].append(event)

    return _named(record, f'record_{event_class.__name__}')


class TestTypedDispatch:
    def test_each_handler_gets_the_events_of_its_type_only_in_order(self) -> None:
        app = _make_app()
        received: dict[str, list[AnyEvent]] = {}
        for event_class in events.EVENT_TYPES:
            app.on_event(event_class)(_recorder(received, event_class))
        running = _started(app)
        dispatch = EventDispatch(running, NoWorkerEndedSpans())

        dispatch.dispatch([text for _name, text in WIRE_TEXTS])
        _stop(running)

        for event_class in events.EVENT_TYPES:
            expected: list[AnyEvent] = []
            for text in _texts_of(event_class.__name__):
                match decode_event_text(text):
                    case Ok(event):
                        expected.append(event)
                    case Err(refused):
                        pytest.fail(f'the vector decodes, got {refused}')
            assert received[event_class.__name__] == expected, event_class.__name__
            assert expected, f'the vectors hold a {event_class.__name__}'

    def test_an_event_of_a_type_that_no_handler_takes_goes_nowhere(self) -> None:
        app = _make_app()
        received: dict[str, list[AnyEvent]] = {}
        app.on_event(AttemptEnded)(_recorder(received, AttemptEnded))
        running = _started(app)

        EventDispatch(running, NoWorkerEndedSpans()).dispatch(_texts_of('TaskClaimed'))
        _stop(running)

        assert received == {'AttemptEnded': []}
        assert running[0].counts() == HandlerCounts(
            handler=running[0].handler.name,
            errors=0,
            dropped=0,
        )

    def test_a_text_that_does_not_decode_is_skipped_and_logged(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        app = _make_app()
        received: dict[str, list[AnyEvent]] = {}
        app.on_event(AttemptEnded)(_recorder(received, AttemptEnded))
        running = _started(app)

        with caplog.at_level(logging.ERROR):
            EventDispatch(running, NoWorkerEndedSpans()).dispatch(
                ['["AttemptEnded",1]', _attempt_ended_text(4)],
            )
        _stop(running)

        assert [
            cast('AttemptEnded', event).attempt for event in received['AttemptEnded']
        ] == [4]
        assert 'skipped an event that does not decode' in caplog.text
        assert 'a AttemptEnded event' in caplog.text


class TestHandlerKinds:
    def test_a_def_handler_runs_on_its_own_thread(self) -> None:
        app = _make_app()
        threads: list[threading.Thread] = []

        def on_end(event: AttemptEnded) -> None:
            del event
            threads.append(threading.current_thread())

        app.on_event(AttemptEnded)(on_end)
        running = _started(app)

        EventDispatch(running, NoWorkerEndedSpans()).dispatch([_attempt_ended_text(1)])
        _stop(running)

        assert len(threads) == 1
        assert threads[0] is not threading.main_thread()
        assert threads[0].name == 'horsies-handler-0'
        assert threads[0].daemon

    def test_an_async_def_handler_runs_on_the_loop_of_its_thread(self) -> None:
        app = _make_app()
        seen: list[tuple[str, asyncio.AbstractEventLoop]] = []

        async def first(event: AttemptEnded) -> None:
            del event
            await asyncio.sleep(0)
            seen.append(('first', asyncio.get_running_loop()))

        async def second(event: AttemptEnded) -> None:
            del event
            seen.append(('second', asyncio.get_running_loop()))

        app.on_event(AttemptEnded)(first)
        app.on_event(AttemptEnded)(second)
        running = _started(app)

        EventDispatch(running, NoWorkerEndedSpans()).dispatch([_attempt_ended_text(1)])
        _stop(running)

        loops = dict(seen)
        assert set(loops) == {'first', 'second'}
        match running[0].runner, running[1].runner:
            case OnLoop(loop=first_loop), OnLoop(loop=second_loop):
                assert loops['first'] is first_loop
                assert loops['second'] is second_loop
                assert first_loop is not second_loop
            case runners:
                pytest.fail(f'each async def handler has its loop, got {runners}')


class TestFailingHandlers:
    def test_a_raising_handler_is_counted_and_gets_the_next_event(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        app = _make_app()
        handled: list[int] = []

        def on_end(event: AttemptEnded) -> None:
            if event.attempt == 2:
                raise RuntimeError('the sink is down')
            handled.append(event.attempt)

        app.on_event(AttemptEnded)(on_end)
        running = _started(app)

        with caplog.at_level(logging.ERROR):
            EventDispatch(running, NoWorkerEndedSpans()).dispatch(
                [_attempt_ended_text(attempt) for attempt in (1, 2, 3)],
            )
            _stop(running)

        assert handled == [1, 3]
        assert running[0].counts().errors == 1
        assert running[0].handler.name in caplog.text
        assert 'RuntimeError: the sink is down' in caplog.text
        assert 'Traceback' in caplog.text

    def test_failures_after_the_first_are_logged_once_per_bound(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(handler_child, 'HANDLER_ERROR_LOG_EVERY', 3)
        app = _make_app()

        def on_end(event: AttemptEnded) -> None:
            raise ValueError(f'attempt {event.attempt}')

        app.on_event(AttemptEnded)(on_end)
        running = _started(app)

        with caplog.at_level(logging.ERROR):
            EventDispatch(running, NoWorkerEndedSpans()).dispatch(
                [_attempt_ended_text(attempt) for attempt in range(1, 8)],
            )
            _stop(running)

        failures = [
            record
            for record in caplog.records
            if 'Event handler' in record.getMessage()
        ]
        assert [
            record.getMessage().split('(failure ')[1].split(')')[0]
            for record in failures
        ] == [
            '1',
            '3',
            '6',
        ]
        assert failures[0].exc_info is not None
        assert all(record.exc_info is None for record in failures[1:])
        assert running[0].counts().errors == 7

    def test_a_raising_async_handler_leaves_its_loop_serving(self) -> None:
        app = _make_app()
        handled: list[int] = []

        async def on_end(event: AttemptEnded) -> None:
            if event.attempt == 1:
                raise RuntimeError('first fails')
            handled.append(event.attempt)

        app.on_event(AttemptEnded)(on_end)
        running = _started(app)

        EventDispatch(running, NoWorkerEndedSpans()).dispatch(
            [_attempt_ended_text(1), _attempt_ended_text(2)],
        )
        _stop(running)

        assert handled == [2]
        assert running[0].counts().errors == 1

    def test_a_def_handler_that_returns_a_coroutine_is_a_failure(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        app = _make_app()
        ran: list[int] = []

        async def work(attempt: int) -> None:
            ran.append(attempt)

        def on_end(event: AttemptEnded) -> Coroutine[Any, Any, None]:
            return work(event.attempt)

        app.on_event(AttemptEnded)(on_end)
        running = _started(app)

        with (
            warnings.catch_warnings(record=True) as caught,
            caplog.at_level(logging.ERROR),
        ):
            warnings.simplefilter('always')
            EventDispatch(running, NoWorkerEndedSpans()).dispatch(
                [_attempt_ended_text(1)]
            )
            _stop(running)
            _ = gc.collect()

        assert ran == []
        assert running[0].counts().errors == 1
        assert 'returned a coroutine, which was not awaited' in caplog.text
        assert not [
            warning for warning in caught if 'never awaited' in str(warning.message)
        ]


class TestBlockingHandler:
    def test_a_blocking_handler_drops_its_own_events_only(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(handler_child, 'HANDLER_QUEUE_EVENTS', 2)
        app = _make_app()
        entered = threading.Event()
        release = threading.Event()
        blocked: list[int] = []
        recorded: list[int] = []

        def blocking(event: AttemptEnded) -> None:
            entered.set()
            _ = release.wait(_WAIT_S)
            blocked.append(event.attempt)

        def recording(event: AttemptEnded) -> None:
            recorded.append(event.attempt)

        app.on_event(AttemptEnded)(blocking)
        app.on_event(AttemptEnded)(recording)
        running = _started(app)
        dispatch = EventDispatch(running, NoWorkerEndedSpans())

        dispatch.dispatch([_attempt_ended_text(1)])
        assert entered.wait(_WAIT_S)
        # One event at a time: the second handler takes each before the
        # next, so only the inbox of the blocked handler fills.
        for attempt in range(2, 11):
            dispatch.dispatch([_attempt_ended_text(attempt)])
            _wait_until(
                lambda: len(recorded) == attempt,
                f'the second handler to take {attempt}',
            )

        blocking_counts = running[0].counts()
        assert (blocking_counts.errors, blocking_counts.dropped) == (0, 7)
        assert running[1].counts() == HandlerCounts(
            handler=running[1].handler.name,
            errors=0,
            dropped=0,
        )
        release.set()
        _stop(running)
        assert blocked == [1, 2, 3]
        assert recorded == list(range(1, 11))


class _Exited(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


class _BootFailed(Exception):
    def __init__(self, frame: BootFailedFrame) -> None:
        super().__init__(frame)
        self.frame = frame


class _ServedChild:
    """A handler child served on a thread over a socket pair, with its
    frames recorded."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, app: Horsies) -> None:
        self.frames: list[ChildFrame] = []
        self.exit_codes: list[int] = []
        self.ready_seen: list[bool] = []
        self._lock = threading.Lock()
        self.worker_end, child_end = socket.socketpair()
        self._child_end = child_end

        def record(_sock: socket.socket, frame: ChildFrame) -> None:
            with self._lock:
                self.frames.append(frame)

        def end(code: int) -> NoReturn:
            raise _Exited(code)

        def failed_boot(_sock: socket.socket, failed: BootFailedFrame) -> NoReturn:
            raise _BootFailed(failed)

        monkeypatch.setattr(handler_child, '_send_child_frame', record)
        monkeypatch.setattr(handler_child, 'end_without_teardown', end)
        monkeypatch.setattr(handler_child, 'fail_boot', failed_boot)
        self._app = app
        self.boot_result: Result[Horsies, BootFailedFrame] = Ok(app)
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def _serve(self) -> None:
        init = init_frame(child_kind=handler_kind())
        match init.child_kind:
            case core.HandlerChildKind() as kind:
                pass
            case other:
                raise AssertionError(
                    f'the init frame names the handler kind, got {other}'
                )
        try:
            handler_child.serve_handler_child(
                self._child_end,
                ParentFrameReader(self._child_end),
                init,
                kind,
                lambda _init, _kind: self.boot_result,
            )
        except _Exited as exited:
            self.exit_codes.append(exited.code)
        except _BootFailed as failed:
            self.frames.append(failed.frame)

    def start(self) -> None:
        self._thread.start()

    def snapshot(self) -> list[ChildFrame]:
        with self._lock:
            return list(self.frames)

    def send(self, data: bytes) -> None:
        self.worker_end.sendall(data)

    def join(self) -> None:
        self._thread.join(_WAIT_S)
        assert not self._thread.is_alive(), 'the handler child ended'

    def close(self) -> None:
        self.worker_end.close()
        self._child_end.close()


def _events_bytes(texts: list[str]) -> bytes:
    return framed({'kind': 'events'}, f'[{",".join(texts)}]'.encode('utf-8'))


def _stop_bytes() -> bytes:
    return framed({'kind': 'stop'})


def _stats(frames: list[ChildFrame]) -> list[HandlerStatsFrame]:
    return [frame for frame in frames if isinstance(frame, HandlerStatsFrame)]


@pytest.fixture
def fast_stats(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(handler_child, 'HANDLER_STATS_INTERVAL_S', 0.02)
    yield


@pytest.mark.usefixtures('fast_stats')
class TestServedChild:
    def test_the_child_boots_runs_the_hook_serves_and_stops(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = _make_app()
        order: list[str] = []
        handled: list[int] = []

        def on_end(event: AttemptEnded) -> None:
            if event.attempt == 2:
                raise RuntimeError('refused by the sink')
            handled.append(event.attempt)

        app.on_event(AttemptEnded)(on_end)
        app.on_handler_child_start(lambda: order.append('hook'))
        child = _ServedChild(monkeypatch, app)
        child.start()
        _wait_until(lambda: bool(child.snapshot()), 'the ready frame')
        order.append(type(child.snapshot()[0]).__name__)

        child.send(
            _events_bytes([_attempt_ended_text(attempt) for attempt in (1, 2, 3)])
        )
        _wait_until(
            lambda: any(
                frame.handlers[0].errors == 1 for frame in _stats(child.snapshot())
            ),
            'a stats frame with the failure',
        )
        child.send(_stop_bytes())
        child.join()
        child.close()

        frames = child.snapshot()
        assert order == ['hook', 'ReadyFrame']
        assert isinstance(frames[0], ReadyFrame)
        assert handled == [1, 3]
        assert child.exit_codes == [0]
        last = frames[-1]
        assert isinstance(last, HandlerStatsFrame)
        assert last.handlers == (
            HandlerCounts(
                handler=app.get_event_handlers()[0].name, errors=1, dropped=0
            ),
        )

    def test_stats_frames_come_at_the_interval_with_the_totals(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = _make_app()
        app.on_event(TaskClaimed)(_ignoring('on_claim'))
        app.on_event(AttemptEnded)(_ignoring('on_end'))
        child = _ServedChild(monkeypatch, app)
        child.start()

        _wait_until(lambda: len(_stats(child.snapshot())) >= 3, 'three stats frames')
        child.send(_stop_bytes())
        child.join()
        child.close()

        names = tuple(handler.name for handler in app.get_event_handlers())
        for frame in _stats(child.snapshot()):
            assert tuple(counts.handler for counts in frame.handlers) == names
            assert all(
                (counts.errors, counts.dropped) == (0, 0) for counts in frame.handlers
            )

    def test_a_child_with_no_handler_serves_and_stops(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        child = _ServedChild(monkeypatch, _make_app())
        child.start()
        _wait_until(lambda: bool(child.snapshot()), 'the ready frame')

        child.send(_events_bytes(_texts_of('TaskClaimed')))
        child.send(_stop_bytes())
        child.join()
        child.close()

        last = child.snapshot()[-1]
        assert isinstance(last, HandlerStatsFrame)
        assert last.handlers == ()
        assert child.exit_codes == [0]

    def test_events_left_at_the_stop_are_logged_and_the_child_exits(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(handler_child, 'HANDLER_DRAIN_S', 0.05)
        app = _make_app()
        entered = threading.Event()
        release = threading.Event()

        def blocking(event: AttemptEnded) -> None:
            del event
            entered.set()
            _ = release.wait(_WAIT_S)

        app.on_event(AttemptEnded)(blocking)
        child = _ServedChild(monkeypatch, app)
        child.start()
        _wait_until(lambda: bool(child.snapshot()), 'the ready frame')

        with caplog.at_level(logging.WARNING):
            child.send(
                _events_bytes([_attempt_ended_text(attempt) for attempt in (1, 2, 3)])
            )
            assert entered.wait(_WAIT_S)
            child.send(_stop_bytes())
            child.join()
        release.set()
        child.close()

        assert child.exit_codes == [0]
        assert (
            f'Event handler {app.get_event_handlers()[0].name} did not take 2 events'
            in caplog.text
        )

    def test_a_failed_boot_sends_its_frame_and_no_ready_frame(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        child = _ServedChild(monkeypatch, _make_app())
        failed = BootFailedFrame(
            cause=AppLoadFailed(), error_name='ImportError', detail='no app'
        )
        child.boot_result = Err(failed)
        child.start()
        child.join()
        child.close()

        assert child.snapshot() == [failed]


class TestStartHooks:
    def test_the_hooks_run_in_order(self) -> None:
        calls: list[str] = []

        result = run_handler_child_start_hooks(
            [lambda: calls.append('first'), lambda: calls.append('second')],
        )

        assert result == Ok(None)
        assert calls == ['first', 'second']

    def test_a_raising_hook_ends_the_child_with_the_hook_code(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[str] = []

        def end(code: int) -> NoReturn:
            raise _Exited(code)

        def failing() -> None:
            raise RuntimeError('no tracer provider')

        monkeypatch.setattr(handler_child.os, '_exit', end)

        with pytest.raises(_Exited) as exited:
            _ = run_handler_child_start_hooks([failing, lambda: calls.append('after')])

        assert exited.value.code == CHILD_HOOK_FAILURE_EXIT_CODE
        assert calls == []

    def test_a_hung_hook_ends_the_child_after_its_bound(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        codes: list[int] = []
        ended = threading.Event()

        def record(code: int) -> None:
            codes.append(code)
            ended.set()

        monkeypatch.setattr(handler_child.os, '_exit', record)
        monkeypatch.setattr(handler_child, 'HANDLER_HOOK_TIMEOUT_S', 0.05)

        def hung() -> None:
            _ = ended.wait(_WAIT_S)

        with caplog.at_level(logging.CRITICAL):
            _ = run_handler_child_start_hooks([hung])

        assert codes == [CHILD_HOOK_FAILURE_EXIT_CODE]
        assert 'on_handler_child_start hook' in caplog.text

    def test_a_refused_watchdog_fails_the_boot_before_the_hook(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[str] = []

        def refuse(_thread: threading.Thread) -> None:
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(threading.Thread, 'start', refuse)

        result = run_handler_child_start_hooks([lambda: calls.append('ran')])

        match result:
            case Err(BootFailedFrame(cause=ThreadStartRefused())):
                pass
            case other:
                pytest.fail(f'a refused watchdog is a refused thread, got {other}')
        assert calls == []


class TestBoot:
    def test_an_import_failure_is_app_load_failed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def no_restore(_init: core.InitFrame) -> None:
            return None

        def failing_import(*_arguments: object) -> Horsies:
            raise RuntimeError('no module named app')

        monkeypatch.setattr(handler_child, 'restore_parent_state', no_restore)
        monkeypatch.setattr(handler_child, 'import_app_for_child', failing_import)
        init = init_frame(child_kind=handler_kind(task_send_pool=1))
        match init.child_kind:
            case core.HandlerChildKind() as kind:
                pass
            case other:
                pytest.fail(f'a handler kind, got {other}')

        assert handler_child.boot_handler_app(init, kind) == Err(
            BootFailedFrame(
                cause=AppLoadFailed(),
                error_name='RuntimeError',
                detail='no module named app',
            ),
        )

    def test_the_app_imports_with_the_send_pool_of_the_kind(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = _make_app()
        calls: list[tuple[object, ...]] = []

        def no_restore(_init: core.InitFrame) -> None:
            return None

        def importing(*arguments: object) -> Horsies:
            calls.append(arguments)
            return app

        monkeypatch.setattr(handler_child, 'restore_parent_state', no_restore)
        monkeypatch.setattr(handler_child, 'import_app_for_child', importing)
        init = init_frame(
            child_kind=handler_kind(task_send_pool=3), app_locator='app.main:app'
        )
        match init.child_kind:
            case core.HandlerChildKind() as kind:
                pass
            case other:
                pytest.fail(f'a handler kind, got {other}')

        assert handler_child.boot_handler_app(init, kind) == Ok(app)
        assert calls == [('app.main:app', [], [], logging.INFO, 3)]


class _ScriptedSocket:
    """Serves ``data``, then the end of the stream."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def recv(self, size: int) -> bytes:
        got, self._data = self._data[:size], self._data[size:]
        return got


class TestControlThread:
    def _read(self, monkeypatch: pytest.MonkeyPatch, data: bytes) -> int:
        def end(code: int) -> NoReturn:
            raise _Exited(code)

        monkeypatch.setattr(child_main.os, '_exit', end)
        reader = ParentFrameReader(cast('socket.socket', _ScriptedSocket(data)))
        with pytest.raises(_Exited) as exited:
            handler_child._read_handler_frames(
                reader,
                EventDispatch((), NoWorkerEndedSpans()),
                threading.Event(),
            )
        return exited.value.code

    def test_a_run_frame_is_a_bad_parent_frame(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from tests.fixtures.core_frames import run_meta

        run = framed(run_meta(task_id=__import__('uuid').UUID(int=1)), b'{}')

        assert self._read(monkeypatch, run) == child_main.BAD_PARENT_FRAME_EXIT_CODE

    def test_a_second_init_frame_is_a_bad_parent_frame(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        data = framed(init_meta(child_kind=handler_kind()))

        assert self._read(monkeypatch, data) == child_main.BAD_PARENT_FRAME_EXIT_CODE

    def test_a_parent_that_is_gone_ends_the_child(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        assert self._read(monkeypatch, b'') == child_main.PARENT_GONE_EXIT_CODE

    def test_the_stop_frame_sets_the_stop_and_ends_the_thread(self) -> None:
        stopped = threading.Event()
        reader = ParentFrameReader(
            cast('socket.socket', _ScriptedSocket(_stop_bytes()))
        )

        handler_child._read_handler_frames(
            reader, EventDispatch((), NoWorkerEndedSpans()), stopped
        )

        assert stopped.is_set()


class _Served(Exception):
    def __init__(self, program: str) -> None:
        super().__init__(program)
        self.program = program


class TestChildProgram:
    """``child_main.serve_program`` serves a task child or the handler
    child by the kind of the init frame; a forked task child
    (``read_init_frame``) still refuses the handler kind."""

    def _serve(
        self, monkeypatch: pytest.MonkeyPatch, child_kind: dict[str, object]
    ) -> str:
        def no_signals() -> None:
            return None

        def no_bind(_pid: int) -> None:
            return None

        def handler_program(*_arguments: object) -> NoReturn:
            raise _Served('handler')

        def task_program(*_arguments: object) -> NoReturn:
            raise _Served('task')

        monkeypatch.setattr(child_main, 'ignore_stop_signals', no_signals)
        monkeypatch.setattr(child_main, 'bind_to_parent', no_bind)
        monkeypatch.setattr(handler_child, 'serve_handler_child', handler_program)
        monkeypatch.setattr(child_main, '_serve_task_child', task_program)
        worker_end, child_end = socket.socketpair()
        worker_end.sendall(framed(init_meta(child_kind=child_kind)))
        try:
            with pytest.raises(_Served) as served:
                child_main.serve_program(
                    child_end,
                    1,
                    child_main.boot_app,
                    child_main.production_runners(),
                    child_main.boot_handler_child_app,
                )
        finally:
            worker_end.close()
            child_end.close()
        return served.value.program

    def test_the_handler_kind_serves_the_handler_child(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        assert self._serve(monkeypatch, handler_kind()) == 'handler'

    def test_a_task_kind_serves_a_task_child(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from tests.fixtures.core_frames import isolated_kind

        assert self._serve(monkeypatch, isolated_kind()) == 'task'

    def test_the_init_of_any_kind_reads_as_a_python_init(self) -> None:
        worker_end, child_end = socket.socketpair()
        worker_end.sendall(framed(init_meta(child_kind=handler_kind())))
        try:
            init = read_python_init_frame(child_end, ParentFrameReader(child_end))
        finally:
            worker_end.close()
            child_end.close()

        assert isinstance(init.child_kind, core.HandlerChildKind)


def test_a_stats_frame_holds_each_handler_once_in_order() -> None:
    app = _make_app()
    app.on_event(TaskClaimed)(_ignoring('first'))
    app.on_event(TaskClaimed)(_ignoring('second'))
    running = _started(app)

    frame = stats_frame(running)
    _stop(running)

    handlers: tuple[EventHandler, ...] = app.get_event_handlers()
    assert [counts.handler for counts in frame.handlers] == [
        handler.name for handler in handlers
    ]
