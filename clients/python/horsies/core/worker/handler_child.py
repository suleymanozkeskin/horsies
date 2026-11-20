"""The handler child of a worker: it runs the event handlers of the app.

The worker starts one handler child by ``exec`` when its app has an event
handler (``Horsies.on_event``); the init frame names the handler kind
(``child_main.serve_program``). The child runs no task. Its failures never
stop the worker and never touch a task: the worker boots a lost child again
after the slot backoff, and leaves a child whose boot fails for a cause that
repeats (an import error, a failed or hung start hook) down until the worker
restarts.

Boot, before the ready frame:

1. Import the app and its task modules (``boot_handler_app``); the imports
   register the handlers.
2. Run the ``on_handler_child_start`` hooks, each for at most
   ``HANDLER_HOOK_TIMEOUT_S``. A hook that raises or runs longer ends the
   child with ``CHILD_HOOK_FAILURE_EXIT_CODE``. The user sets the tracer
   provider here.
3. Start one ``HandlerInbox`` of ``HANDLER_QUEUE_EVENTS`` events and one
   thread per handler. An ``async def`` handler gets an event loop of its
   own, which its thread runs.
4. Choose the spans of the attempts that the worker ended from
   ``AppConfig.tracing`` (``ended_attempt_spans_of``), with the node reader
   of the app (``AppNodeReader``), and start the reader thread of the
   workflow nodes (``start_node_reader``): the workflow node of a workflow
   task whose attempt the worker ended is read through the core on that
   thread, never on the control thread.
5. Start the control thread, which reads the frames of the worker.

After the ready frame:

- The control thread decodes each event of an ``events`` frame with the
  generated decoder (``decode_event``). The span of an attempt that the
  worker ended comes first (``make_ended_attempt_span``: made at once, or
  queued for the read of its workflow node), then the event goes to the
  inbox of each handler of its type. A full inbox drops the event for that
  handler only and counts it. The control thread never waits for a
  handler or for the database.
- Each handler thread takes the events of its inbox in order and calls its
  handler. A handler that fails is counted and logged with its name: the
  first failure with its traceback, then one line per
  ``HANDLER_ERROR_LOG_EVERY`` failures. The thread goes on with the next
  event.
- The main thread sends a ``handler_stats`` frame every
  ``HANDLER_STATS_INTERVAL_S``: the totals of each handler since the child
  started, the spans made with no workflow node, and the RSS of the child.

At the stop frame each inbox closes, the handler threads and the reader
thread get ``HANDLER_DRAIN_S`` together for the events and the spans left,
each span still waiting for its node is made with no node
(``end_node_reads_at_stop``), the spans of the attempts that the worker
ended are flushed within ``ENDED_ATTEMPT_SPAN_FLUSH_BOUND_S``, the child
sends a last ``handler_stats`` frame and exits 0. The handler threads and
the reader thread are daemon threads: the exit does not wait for a handler
that blocks, and the events left in its inbox are logged.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import collections
import inspect
import os
import socket
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, NoReturn, assert_never

from horsies import _core as core
from horsies._catalog import events
from horsies._catalog.broker_errors import BrokerOperationFailed
from horsies._catalog.events import AnyEvent
from horsies.core.codec.json_io import loads_json
from horsies.core.event_handlers import EventHandler, HandlerChildStartHook, HandlerKind
from horsies.core.logging import get_logger
from horsies.core.models.boot_failure import (
    AppLoadFailed,
    TaskLoopCreateFailed,
    ThreadStartRefused,
    shortage_or,
)
from horsies.core.telemetry.ended_attempt_span import (
    EndedAttemptSpans,
    NodeNotLinked,
    NodeRead,
    NodeReadFailed,
    ended_attempt_spans_of,
    end_node_reads_at_stop,
    flush_ended_attempt_spans_at_stop,
    make_ended_attempt_span,
    span_read_counts_of,
    start_node_reader,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_frames import (
    BootFailedFrame,
    HandlerCounts,
    HandlerStatsFrame,
    ReadyFrame,
    boot_failed_frame,
)
from horsies.core.worker.child_main import (
    BAD_PARENT_FRAME_EXIT_CODE,
    PARENT_GONE_EXIT_CODE,
    HandlerBootFn,
    ParentFrameReader,
    _exit_bad_parent_frame,
    _read_rss,
    _send_child_frame,
    boot_failure_from_exception,
    end_without_teardown,
    fail_boot,
    flush_output,
    python_program_of,
    restore_parent_state,
)
from horsies.core.worker.child_runner import (
    CHILD_HOOK_FAILURE_EXIT_CODE,
    CHILD_HOOK_TIMEOUT_SECONDS,
    import_app_for_child,
    log_level_of,
)
from horsies.core.worker.run_return import InWorkflow

if TYPE_CHECKING:
    from horsies.core.app import Horsies

logger = get_logger('worker')

HANDLER_QUEUE_EVENTS: Final = 10_000
"""The most events that the inbox of one handler holds. An event that does
not fit is dropped for that handler and counted in its ``dropped``."""

HANDLER_STATS_INTERVAL_S: Final = 5.0
"""The time between two ``handler_stats`` frames."""

HANDLER_DRAIN_S: Final = 2.0
"""At the stop: the time that the handler threads get together for the
events left in their inboxes."""

HANDLER_ERROR_LOG_EVERY: Final = 1_000
"""After its first failure, a handler gets one log line per this many
failures."""

HANDLER_HOOK_TIMEOUT_S: Final = CHILD_HOOK_TIMEOUT_SECONDS
"""The longest run of one ``on_handler_child_start`` hook."""


@dataclass(frozen=True, slots=True)
class OnThread:
    """A ``def`` handler: its thread calls it."""


@dataclass(frozen=True, slots=True)
class OnLoop:
    """An ``async def`` handler: its thread awaits it on ``loop``, which
    only that thread runs."""

    loop: asyncio.AbstractEventLoop


type HandlerRunner = OnThread | OnLoop
"""How the thread of a handler calls it."""


@dataclass(frozen=True, slots=True)
class Queued:
    """The inbox took the event."""


@dataclass(frozen=True, slots=True)
class InboxFull:
    """The inbox holds its bound of events; the event is not in it."""


type Offered = Queued | InboxFull
"""What an offer of one event to an inbox gave."""


@dataclass(frozen=True, slots=True)
class InboxClosed:
    """The inbox is closed and holds no event."""


class HandlerInbox:
    """The bounded queue of the events of one handler. The control thread
    offers, the handler thread takes, the main thread closes. Safe between
    threads.

    ``offer`` never blocks: an inbox that holds ``capacity`` events refuses
    the event. ``take`` waits for an event; after ``close`` it gives the
    events left, then ``InboxClosed``.
    """

    def __init__(self, capacity: int) -> None:
        assert capacity >= 1, f'an inbox holds at least one event, got {capacity}'
        self._capacity: Final = capacity
        self._events: collections.deque[AnyEvent] = collections.deque()
        self._changed: Final = threading.Condition()
        self._closed = False

    def offer(self, event: AnyEvent) -> Offered:
        """Queue ``event`` when the inbox has room. An offer after ``close``
        is a defect: the control thread ends before the inboxes close."""
        with self._changed:
            assert not self._closed, 'an event was offered to a closed inbox'
            if len(self._events) >= self._capacity:
                return InboxFull()
            self._events.append(event)
            self._changed.notify()
            return Queued()

    def take(self) -> AnyEvent | InboxClosed:
        """The next event, after a wait for one; ``InboxClosed`` once the
        inbox is closed and empty."""
        with self._changed:
            while not self._events and not self._closed:
                _ = self._changed.wait()
            if self._events:
                return self._events.popleft()
            return InboxClosed()

    def close(self) -> None:
        """Close the inbox: ``take`` gives the events left, then
        ``InboxClosed``."""
        with self._changed:
            self._closed = True
            self._changed.notify_all()

    def waiting(self) -> int:
        """The events in the inbox now."""
        with self._changed:
            return len(self._events)


@dataclass(frozen=True, slots=True)
class Handled:
    """The handler returned."""


@dataclass(frozen=True, slots=True)
class HandlerRaised:
    """The handler raised ``exception``."""

    exception: BaseException


@dataclass(frozen=True, slots=True)
class DefReturnedAwaitable:
    """A ``def`` handler returned an awaitable of type ``type_name``: its
    work did not run. An ``async def`` function, or a function marked with
    ``inspect.markcoroutinefunction``, is awaited."""

    type_name: str


@dataclass(frozen=True, slots=True)
class AsyncReturnedNoAwaitable:
    """A coroutine function returned a value of type ``type_name``, which
    is not awaitable."""

    type_name: str


type HandlerFailure = HandlerRaised | DefReturnedAwaitable | AsyncReturnedNoAwaitable
"""Why one call of a handler failed."""


def _failure_text(failure: HandlerFailure) -> str:
    match failure:
        case HandlerRaised(exception=exception):
            return f'{type(exception).__name__}: {exception}'
        case DefReturnedAwaitable(type_name=type_name):
            return (
                f'the def handler returned a {type_name}, which was not awaited; '
                'use an async def function'
            )
        case AsyncReturnedNoAwaitable(type_name=type_name):
            return (
                f'the coroutine function returned a {type_name}, which is not awaitable'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _logged_at(count: int) -> bool:
    """Whether the failure of number ``count`` gets a log line: the first,
    then each ``HANDLER_ERROR_LOG_EVERY``-th."""
    return count == 1 or count % HANDLER_ERROR_LOG_EVERY == 0


class RunningHandler:
    """One handler of the child: its inbox, its thread and its counts.

    ``errors`` is written by the handler thread only, ``dropped`` by the
    control thread only; the main thread reads both for the stats frames.
    """

    def __init__(
        self,
        handler: EventHandler,
        runner: HandlerRunner,
        capacity: int,
        index: int,
    ) -> None:
        self.handler: Final = handler
        self.runner: Final = runner
        self.inbox: Final = HandlerInbox(capacity)
        self._errors = 0
        self._dropped = 0
        self._thread: Final = threading.Thread(
            target=self._serve,
            name=f'horsies-handler-{index}',
            daemon=True,
        )

    def start(self) -> Result[None, BootFailedFrame]:
        """Start the thread. ``Err``: the OS refused it
        (``ThreadStartRefused``)."""
        try:
            self._thread.start()
        except RuntimeError as exc:
            return Err(
                boot_failed_frame(
                    ThreadStartRefused(),
                    type(exc).__name__,
                    f'the thread of handler {self.handler.name}: {exc}',
                ),
            )
        return Ok(None)

    def offer(self, event: AnyEvent) -> None:
        """Control thread: queue ``event``, or drop and count it when the
        inbox is full."""
        match self.inbox.offer(event):
            case Queued():
                return
            case InboxFull():
                self._dropped += 1

    def counts(self) -> HandlerCounts:
        """The totals of the handler since the child started."""
        return HandlerCounts(
            handler=self.handler.name, errors=self._errors, dropped=self._dropped
        )

    def join(self, timeout_s: float) -> None:
        """Wait at most ``timeout_s`` for the thread to end."""
        self._thread.join(timeout_s)

    def _serve(self) -> None:
        """Handler thread: call the handler for each event of the inbox,
        until the inbox is closed and empty."""
        match self.runner:
            case OnLoop(loop=loop):
                asyncio.set_event_loop(loop)
            case OnThread():
                pass
        while True:
            match self.inbox.take():
                case InboxClosed():
                    return
                case event:
                    self._handle(event)

    def _handle(self, event: AnyEvent) -> None:
        """Call the handler for ``event``; count and log a failure."""
        try:
            ended = self._call(event)
        except BaseException as exc:
            # The handler is user code: whatever it raises is a failure of
            # that handler, counted and logged; the child goes on.
            self._failed(event, HandlerRaised(exception=exc))
            return
        match ended:
            case Handled():
                return
            case DefReturnedAwaitable() | AsyncReturnedNoAwaitable():
                self._failed(event, ended)

    def _call(
        self,
        event: AnyEvent,
    ) -> Handled | DefReturnedAwaitable | AsyncReturnedNoAwaitable:
        """One call of the handler. Raises what the handler raises."""
        function = self.handler.function
        match self.runner:
            case OnThread():
                returned: object = function(event)
                if inspect.iscoroutine(returned):
                    # Close it, so that the coroutine that never ran raises
                    # no warning when it is collected.
                    returned.close()
                    return DefReturnedAwaitable(type_name=type(returned).__name__)
                if inspect.isawaitable(returned):
                    return DefReturnedAwaitable(type_name=type(returned).__name__)
                return Handled()
            case OnLoop(loop=loop):
                awaited: object = function(event)
                if not inspect.isawaitable(awaited):
                    return AsyncReturnedNoAwaitable(type_name=type(awaited).__name__)
                _ = loop.run_until_complete(awaited)
                return Handled()

    def _failed(self, event: AnyEvent, failure: HandlerFailure) -> None:
        """Count a failure, and log it with the name of the handler."""
        self._errors += 1
        count = self._errors
        if not _logged_at(count):
            return
        match failure, count:
            case HandlerRaised(exception=exception), 1:
                traceback: BaseException | None = exception
            case _:
                traceback = None
        logger.error(
            'Event handler %s failed on a %s event (failure %d): %s',
            self.handler.name,
            type(event).__name__,
            count,
            _failure_text(failure),
            exc_info=traceback,
        )


def node_read_of(
    outcome: core.TaskWorkflowNodeLinked
    | core.TaskWorkflowNodeNotLinked
    | BrokerOperationFailed,
) -> NodeRead:
    """The node read of the outcome of ``CoreHandle.task_workflow_node``.
    Pure."""
    match outcome:
        case BrokerOperationFailed(code=code, message=message):
            return NodeReadFailed(detail=f'{code.value}: {message}')
        case core.TaskWorkflowNodeLinked(
            workflow_id=workflow_id,
            node_index=node_index,
            root_workflow_id=root_workflow_id,
        ):
            return InWorkflow(
                workflow_id=workflow_id,
                node_index=node_index,
                root_workflow_id=root_workflow_id,
            )
        case core.TaskWorkflowNodeNotLinked():
            return NodeNotLinked()
        case _ as unreachable:
            assert_never(unreachable)


class AppNodeReader:
    """Reads the workflow node of a task through the core handle of the
    app's broker (``CoreHandle.task_workflow_node``: one bounded statement,
    no schema step). The broker is built at the first read, so a child
    whose worker ends no attempt of a workflow task builds none."""

    def __init__(self, app: Horsies) -> None:
        self._app: Final = app

    def read(self, task_id: uuid.UUID) -> NodeRead:
        """The node that links ``task_id``. A broker that cannot be built
        and a call that the bridge refuses or ends at its bound
        (``CoreCallTimedOut``, ``CoreStopped``) are ``NodeReadFailed``."""
        try:
            handle = self._app.get_broker()._core_handle
            outcome = handle.task_workflow_node(task_id)
        except Exception as exc:
            # The broker construction raises HorsiesError; the bridge call
            # raises its call errors. Each is a failed read of this span.
            return NodeReadFailed(detail=f'{type(exc).__name__}: {exc}')
        return node_read_of(outcome)


@dataclass(frozen=True, slots=True)
class NotJsonText:
    """The wire text of an event is not JSON text."""

    detail: str


type EventTextRefusal = (
    NotJsonText | events.NotAnEvent | events.UnknownEventType | events.MalformedEvent
)
"""Why the wire text of one event is not an event of this contract version."""


def decode_event_text(text: str) -> Result[AnyEvent, EventTextRefusal]:
    """The event of the wire text of one event of an ``events`` frame
    (``events.decode_event``). The text is parsed by the strict codec
    parser (``loads_json``): an empty text, a non-finite number, a repeated
    key or a text nested too deep is ``NotJsonText``. Pure; never raises."""
    match loads_json(text):
        case Ok(wire_value):
            pass
        case Err(refused):
            return Err(NotJsonText(detail=str(refused)))
    match events.decode_event(wire_value):
        case Ok() as decoded:
            return decoded
        case Err(refused):
            return Err(refused)


def _refusal_text(refused: EventTextRefusal) -> str:
    match refused:
        case NotJsonText(detail=detail):
            return f'not JSON text: {detail}'
        case events.NotAnEvent():
            return 'not a JSON array that starts with an event type name'
        case events.UnknownEventType(tag=tag):
            return f'the event type {tag!r} is not in contract version {events.EVENT_CONTRACT_VERSION}'
        case events.MalformedEvent(event=event, path=path, reason=reason):
            return f'a {event} event, at {path or "its items"}: {reason}'
        case _ as unreachable:
            assert_never(unreachable)


class EventDispatch:
    """Gives each event of an ``events`` frame to the handlers of its type.
    The control thread only.

    For each event: the span of an attempt that the worker ended
    (``make_ended_attempt_span`` with ``ended_spans``), then an offer to the
    inbox of each handler of the event type. An event of a type that no handler takes goes to none.
    A wire text that does not decode is logged (the first, then one line
    per ``HANDLER_ERROR_LOG_EVERY``) and skipped; the other events of its
    frame still go out.
    """

    def __init__(
        self,
        running: Sequence[RunningHandler],
        ended_spans: EndedAttemptSpans,
    ) -> None:
        routes: dict[type[AnyEvent], list[RunningHandler]] = {}
        for one in running:
            routes.setdefault(one.handler.event_type.event_class, []).append(one)
        self._routes: Final[Mapping[type[AnyEvent], tuple[RunningHandler, ...]]] = {
            event_class: tuple(handlers) for event_class, handlers in routes.items()
        }
        self._ended_spans: Final = ended_spans
        self._refused = 0

    def dispatch(self, texts: Sequence[str]) -> None:
        """Decode and route each wire text of one ``events`` frame, in
        order."""
        for text in texts:
            match decode_event_text(text):
                case Ok(event):
                    self._route(event)
                case Err(refused):
                    self._skip(refused)

    def _route(self, event: AnyEvent) -> None:
        _ = make_ended_attempt_span(self._ended_spans, event)
        for one in self._routes.get(type(event), ()):
            one.offer(event)

    def _skip(self, refused: EventTextRefusal) -> None:
        self._refused += 1
        if _logged_at(self._refused):
            logger.error(
                'The handler child skipped an event that does not decode (%d so far): %s',
                self._refused,
                _refusal_text(refused),
            )


def stats_frame(
    running: Sequence[RunningHandler],
    ended_spans: EndedAttemptSpans,
) -> HandlerStatsFrame:
    """The ``handler_stats`` frame of now: the totals of each handler, the
    spans made with no workflow node (``span_read_counts_of``), and the RSS
    of the child."""
    return HandlerStatsFrame(
        handlers=tuple(one.counts() for one in running),
        span_reads=span_read_counts_of(ended_spans),
        rss=_read_rss(),
    )


def start_ended_span_reader(
    ended_spans: EndedAttemptSpans,
) -> Result[None, BootFailedFrame]:
    """Start the reader thread of the workflow nodes of the spans
    (``start_node_reader``). ``Err``: the OS refused the thread
    (``ThreadStartRefused``)."""
    match start_node_reader(ended_spans):
        case Ok():
            return Ok(None)
        case Err(refused):
            return Err(
                boot_failed_frame(
                    ThreadStartRefused(),
                    refused.exception_name,
                    f'the reader thread of the workflow nodes of spans: {refused.detail}',
                ),
            )


def boot_handler_app(
    init: core.InitFrame,
    kind: core.HandlerChildKind,
) -> Result[Horsies, BootFailedFrame]:
    """Restore the import state of the worker, then import the app and its
    task modules, which register the handlers. A handler that sends a task
    opens a pool of ``kind.task_send_pool`` connections. Opens no
    connection itself.

    ``Err``: an import that raises is ``AppLoadFailed``, which repeats, or a
    ``ResourceShortage`` for an errno of a resource limit
    (``boot_failure_from_exception``).
    """
    restore_parent_state(init)
    program = python_program_of(init)
    try:
        app = import_app_for_child(
            program.app_locator,
            program.imports,
            program.sys_path_roots,
            log_level_of(init.log_level),
            kind.task_send_pool,
        )
    except Exception as exc:
        return Err(boot_failure_from_exception(exc, AppLoadFailed()))
    return Ok(app)


def _exit_for_hung_hook(hook_name: str) -> None:
    """Watchdog target: a start hook ran past its bound; end the child."""
    logger.critical(
        "on_handler_child_start hook '%s' ran past %.0fs in the handler child %s; "
        'the child ends',
        hook_name,
        HANDLER_HOOK_TIMEOUT_S,
        os.getpid(),
    )
    flush_output()
    os._exit(CHILD_HOOK_FAILURE_EXIT_CODE)


def _start_hook_watchdog(hook_name: str) -> Result[threading.Timer, BootFailedFrame]:
    """Start the timer that ends the child when the hook ``hook_name`` runs
    past ``HANDLER_HOOK_TIMEOUT_S``. ``Err``: the OS refused its thread
    (``ThreadStartRefused``); the hook has not run then."""
    watchdog = threading.Timer(
        HANDLER_HOOK_TIMEOUT_S, _exit_for_hung_hook, args=(hook_name,)
    )
    watchdog.daemon = True
    try:
        watchdog.start()
    except RuntimeError as exc:
        return Err(
            boot_failed_frame(
                ThreadStartRefused(),
                type(exc).__name__,
                f'the watchdog of start hook {hook_name!r}: {exc}',
            ),
        )
    return Ok(watchdog)


def run_handler_child_start_hooks(
    hooks: Sequence[HandlerChildStartHook],
) -> Result[None, BootFailedFrame]:
    """Run the ``on_handler_child_start`` hooks in order, each under its
    watchdog.

    A hook that raises, or runs past ``HANDLER_HOOK_TIMEOUT_S``, ends the
    child with ``CHILD_HOOK_FAILURE_EXIT_CODE`` after a log line that names
    it: the worker leaves the handler child down and runs on. ``Err``: the
    OS refused the watchdog of a hook, before that hook ran.
    """
    for hook in hooks:
        hook_name = str(getattr(hook, '__qualname__', repr(hook)))
        match _start_hook_watchdog(hook_name):
            case Ok(watchdog):
                pass
            case Err(failed):
                return Err(failed)
        try:
            hook()
        except BaseException as exc:
            # The hook is user code: whatever it raises fails the boot.
            watchdog.cancel()
            logger.critical(
                "on_handler_child_start hook '%s' failed in the handler child %s: %s",
                hook_name,
                os.getpid(),
                exc,
                exc_info=exc,
            )
            flush_output()
            os._exit(CHILD_HOOK_FAILURE_EXIT_CODE)
        watchdog.cancel()
    return Ok(None)


def runner_of(handler: EventHandler) -> Result[HandlerRunner, BootFailedFrame]:
    """How the thread of ``handler`` calls it: ``OnThread`` for a ``def``
    handler, ``OnLoop`` with a new event loop for an ``async def`` handler.

    ``Err``: the event loop could not be created: ``ResourceShortage`` for
    an errno of a resource limit, else ``TaskLoopCreateFailed``, which
    repeats.
    """
    match handler.kind:
        case HandlerKind.DEF:
            return Ok(OnThread())
        case HandlerKind.ASYNC_DEF:
            try:
                loop = asyncio.new_event_loop()
            except Exception as exc:
                # An event loop policy of the app can raise any exception.
                return Err(
                    boot_failed_frame(
                        shortage_or(exc, TaskLoopCreateFailed()),
                        type(exc).__name__,
                        f'the event loop of handler {handler.name}: {exc}',
                    ),
                )
            return Ok(OnLoop(loop=loop))


def start_handlers(
    handlers: Sequence[EventHandler],
) -> Result[tuple[RunningHandler, ...], BootFailedFrame]:
    """Start the inbox and the thread of each handler, in registration
    order. ``Err``: the first handler whose event loop or thread did not
    start (``runner_of``, ``RunningHandler.start``); the child fails its
    boot then."""
    running: list[RunningHandler] = []
    for index, handler in enumerate(handlers):
        match runner_of(handler):
            case Ok(runner):
                pass
            case Err(failed):
                return Err(failed)
        one = RunningHandler(handler, runner, HANDLER_QUEUE_EVENTS, index)
        match one.start():
            case Ok():
                running.append(one)
            case Err(failed):
                return Err(failed)
    return Ok(tuple(running))


def _read_handler_frames(
    reader: ParentFrameReader,
    dispatch: EventDispatch,
    stopped: threading.Event,
) -> None:
    """Control thread body: route the events of each ``events`` frame, and
    set ``stopped`` at the stop frame, then end. A parent that is gone ends
    the process with ``PARENT_GONE_EXIT_CODE``; a frame of a task child,
    or a second init frame, with ``BAD_PARENT_FRAME_EXIT_CODE``."""
    try:
        while True:
            match reader.next_frame():
                case core.EventsFrame(events=texts):
                    dispatch.dispatch(texts)
                case core.StopFrame():
                    stopped.set()
                    return
                case core.InitFrame():
                    _exit_bad_parent_frame('Parent sent a second init frame')
                case core.RunFrame() | core.SharedRunFrame():
                    _exit_bad_parent_frame(
                        'Parent sent a run frame to the handler child'
                    )
                case _ as unreachable:
                    assert_never(unreachable)
    except (EOFError, OSError):
        os._exit(PARENT_GONE_EXIT_CODE)
    except Exception:
        logger.critical('The control thread of the handler child failed', exc_info=True)
        flush_output()
        os._exit(BAD_PARENT_FRAME_EXIT_CODE)


def start_handler_control_thread(
    reader: ParentFrameReader,
    dispatch: EventDispatch,
    stopped: threading.Event,
) -> Result[None, BootFailedFrame]:
    """Start the thread that reads the frames of the worker
    (``_read_handler_frames``). ``Err``: the OS refused the thread
    (``ThreadStartRefused``)."""
    control = threading.Thread(
        target=_read_handler_frames,
        args=(reader, dispatch, stopped),
        name='horsies-handler-control',
        daemon=True,
    )
    try:
        control.start()
    except RuntimeError as exc:
        return Err(
            boot_failed_frame(ThreadStartRefused(), type(exc).__name__, str(exc))
        )
    return Ok(None)


def exit_after_handler_stop(
    sock: socket.socket,
    running: Sequence[RunningHandler],
    ended_spans: EndedAttemptSpans,
) -> NoReturn:
    """After the stop frame: close each inbox, give the handler threads and
    the reader thread ``HANDLER_DRAIN_S`` together for the events and the
    spans left, make each span still waiting for its node with no node
    (``end_node_reads_at_stop``), log the events that are still left, flush
    the spans of the attempts that the worker ended
    (``flush_ended_attempt_spans_at_stop``), send the last ``handler_stats``
    frame, then exit 0 (``end_without_teardown``). The worker kills a child
    that has not exited within its stop bound."""
    for one in running:
        one.inbox.close()
    drain_end = time.monotonic() + HANDLER_DRAIN_S
    for one in running:
        one.join(max(0.0, drain_end - time.monotonic()))
    _ = end_node_reads_at_stop(ended_spans, max(0.0, drain_end - time.monotonic()))
    for one in running:
        left = one.inbox.waiting()
        if left > 0:
            logger.warning(
                'Event handler %s did not take %d events within %.1fs of the stop',
                one.handler.name,
                left,
                HANDLER_DRAIN_S,
            )
    _ = flush_ended_attempt_spans_at_stop(ended_spans)
    _send_child_frame(sock, stats_frame(running, ended_spans))
    end_without_teardown(0)


def serve_handler_child(
    sock: socket.socket,
    reader: ParentFrameReader,
    init: core.InitFrame,
    kind: core.HandlerChildKind,
    boot: HandlerBootFn,
) -> NoReturn:
    """Boot the handler child, serve the events of the worker until the
    stop frame, then end the process (see the module doc).

    Failure exits: a boot step that fails ends the process through
    ``fail_boot``, before the ready frame, with a ``BootFailedFrame`` that
    names the cause; a start hook that fails or hangs ends it with
    ``CHILD_HOOK_FAILURE_EXIT_CODE``. A parent that is gone or that sends a
    bad frame ends the process through ``os._exit``. A handler never ends
    the process.
    """
    match boot(init, kind):
        case Ok(app):
            pass
        case Err(failed):
            fail_boot(sock, failed)
    match run_handler_child_start_hooks(app.get_handler_child_start_hooks()):
        case Ok():
            pass
        case Err(failed):
            fail_boot(sock, failed)
    match start_handlers(app.get_event_handlers()):
        case Ok(running):
            pass
        case Err(failed):
            fail_boot(sock, failed)
    stopped = threading.Event()
    ended_spans = ended_attempt_spans_of(app.config.tracing, AppNodeReader(app))
    match start_ended_span_reader(ended_spans):
        case Ok():
            pass
        case Err(failed):
            fail_boot(sock, failed)
    dispatch = EventDispatch(running, ended_spans)
    match start_handler_control_thread(reader, dispatch, stopped):
        case Ok():
            pass
        case Err(failed):
            fail_boot(sock, failed)
    _send_child_frame(sock, ReadyFrame(pid=os.getpid(), rss=_read_rss()))
    logger.debug(
        'The handler child %s is ready with %d handlers', os.getpid(), len(running)
    )
    while not stopped.wait(HANDLER_STATS_INTERVAL_S):
        _send_child_frame(sock, stats_frame(running, ended_spans))
    exit_after_handler_stop(sock, running, ended_spans)
