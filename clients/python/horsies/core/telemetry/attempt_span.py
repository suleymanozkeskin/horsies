"""The span and the log context of each attempt that a worker child runs.

A worker child chooses its attempt spans once, at boot, from
`AppConfig.tracing` (`attempt_spans_of`, `use_attempt_spans`):

- `NoTracing`: `NoAttemptSpans`. Nothing imports `opentelemetry`.
- `OtelTracing`: `OtelAttemptSpans`, whose recorder
  (`horsies.core.telemetry.attempt_otel`) opens one span per attempt with
  the OpenTelemetry API.

Each body place of the child (an isolated child, a seat of a shared async
child, a seat of a shared thread child) runs the steps after the RUNNING
update through `run_in_attempt_span` or `run_in_attempt_span_async`. They
make the attempt the running task of the log context
(`horsies.core.logging.RunningTask`), open the span, make it the current
context around the steps, end it with the outcome of the run, and restore
both. With `NoAttemptSpans` they set the log context only.

Telemetry never changes the outcome of a run: a recorder (the user's SDK)
that raises when the span opens leaves the run with no span, and one that
raises later is logged once per process. A span covers what the body of the
run returned, or the timeout of a run whose deadline came first in its
child: each body place gives the order of the two (`BodyEndOrder`), read
when the steps return, and a `DeadlineFirst` ends the span with
`RunTimedOut` (`TASK_TIMEOUT`), the end that the child reports. A run that
the worker ends (an isolated timeout, a kill, a lost child) is not ended
here: the handler child of the worker makes its span.

`flush_attempt_spans_at_stop` flushes the spans of the child at its stop,
within `ATTEMPT_SPAN_FLUSH_BOUND_S`.

This module imports no `opentelemetry` module.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Awaitable, Callable
from contextvars import Token
from dataclasses import dataclass
from typing import Final, Protocol, assert_never

from horsies.core.logging import (
    NoRunningTask,
    NoSpanIds,
    RunningTask,
    SpanIds,
    enter_running_task,
    get_logger,
    leave_running_task,
)
from horsies.core.models.tasks import Execution
from horsies.core.models.tracing import NoTracing, OtelTracing
from horsies.core.worker.child_frames import RunTrace
from horsies.core.worker.run_return import (
    BodyInterrupted,
    EncodedResult,
    ResultEnvelope,
    ResultOutcome,
    RunWorkflow,
)

_logger = get_logger('telemetry')

ATTEMPT_SPAN_FLUSH_BOUND_S: Final = 1.0
"""The longest wait for the flush of the attempt spans at the stop of a
child. The worker kills a child that has not exited `CHILD_STOP_TIMEOUT`
(10 s, horsies-core) after the stop frame. Before the flush, a shared async
child closes its task loop (at most 7 s, `task_loop`) and its frame writer
(at most 1 s, `WRITER_CLOSE_TIMEOUT_S`): 7 s + 1 s + 1 s stays under 10 s."""

MILLISECONDS_PER_SECOND: Final = 1000
"""Converts the flush bound to the `timeout_millis` of `force_flush`."""


@dataclass(frozen=True, slots=True)
class AttemptSubject:
    """The attempt that a span covers: attempt `attempt` (from 1) of task
    `task_id` of the queue `queue_name`, for the claim of `worker_id`, in a
    child of the class `execution`. `workflow` is the workflow node that
    links the task, and `trace` the carrier that the send stored on the
    row."""

    task_id: str
    task_name: str
    queue_name: str
    attempt: int
    worker_id: str
    execution: Execution
    workflow: RunWorkflow
    trace: RunTrace


@dataclass(frozen=True, slots=True)
class ScopeRaised:
    """An exception left the steps of the run: a defect, which propagates.
    The span ends with it."""

    exception: BaseException


@dataclass(frozen=True, slots=True)
class RunTimedOut:
    """The deadline of the run came before the end of its body: the run
    ends `TASK_TIMEOUT`, and what the body returned is dropped."""


RUN_TIMED_OUT: Final = RunTimedOut()
"""The end of the span of a run whose deadline came first."""

type AttemptEnd = ResultOutcome | BodyInterrupted | RunTimedOut | ScopeRaised
"""How the steps of a run ended, for its span."""


@dataclass(frozen=True, slots=True)
class DeadlineFirst:
    """The deadline of the run fired in its child before the steps of the
    run returned: the run ends with the timeout."""


@dataclass(frozen=True, slots=True)
class BodyFirst:
    """The steps of the run returned before its deadline fired in its
    child, or the child holds no deadline of the run: the run ends with
    what the steps returned."""


type BodyEndOrder = DeadlineFirst | BodyFirst
"""Which came first in the child: the deadline of the run, or the end of its
steps."""


def isolated_body_end() -> BodyEndOrder:
    """The order of an isolated run: always `BodyFirst`. The deadline of an
    isolated run is the worker's: it kills the child, and the span never
    ends. Pure."""
    return BodyFirst()


class OpenAttemptSpan(Protocol):
    """The span of one attempt, open until `end`."""

    @property
    def ids(self) -> SpanIds | NoSpanIds:
        """The ids of the span, for the log context."""
        ...

    def make_current(self) -> Callable[[], None]:
        """Make the span the current context of the calling context. The
        returned function restores the context that it replaced; call it
        once, in the same context."""
        ...

    def end(self, end: AttemptEnd) -> None:
        """Set the status of the span from `end`, and end it. Called once."""
        ...


@dataclass(frozen=True, slots=True)
class SpansFlushed:
    """The provider exported every ended span."""


@dataclass(frozen=True, slots=True)
class FlushNotConfirmed:
    """The provider's `force_flush` returned that it did not export every
    span within its timeout."""


@dataclass(frozen=True, slots=True)
class ProviderHasNoFlush:
    """The global tracer provider has no `force_flush`: no SDK provider is
    set, so no span is held for export."""


type ProviderFlush = SpansFlushed | FlushNotConfirmed | ProviderHasNoFlush
"""What a flush of the tracer provider returned."""


class AttemptSpanRecorder(Protocol):
    """Opens the attempt spans of one child, and flushes them."""

    def open(self, subject: AttemptSubject) -> OpenAttemptSpan:
        """Open the span of the attempt `subject`."""
        ...

    def flush(self, timeout_millis: int) -> ProviderFlush:
        """Export the spans that ended, waiting at most `timeout_millis` as
        the provider applies it. May raise or block: the caller bounds it."""
        ...


@dataclass(frozen=True, slots=True)
class NoAttemptSpans:
    """Tracing is off: no span; the log context only."""


@dataclass(frozen=True, slots=True)
class OtelAttemptSpans:
    """A span per attempt through `recorder`."""

    recorder: AttemptSpanRecorder


type AttemptSpans = NoAttemptSpans | OtelAttemptSpans
"""The attempt spans of a child."""

NO_ATTEMPT_SPANS: Final = NoAttemptSpans()
"""The attempt spans of `NoTracing`, and of a process that has not chosen."""


def attempt_spans_of(tracing: OtelTracing | NoTracing) -> AttemptSpans:
    """The attempt spans of the tracing setting `tracing`. `OtelTracing`
    imports the OpenTelemetry API (`horsies.core.telemetry.attempt_otel`),
    which the config check found installed; `NoTracing` imports nothing."""
    match tracing:
        case NoTracing():
            return NO_ATTEMPT_SPANS
        case OtelTracing(parentage=parentage):
            from horsies.core.telemetry.attempt_otel import otel_attempt_recorder

            return OtelAttemptSpans(recorder=otel_attempt_recorder(parentage))
        case _ as unreachable:
            assert_never(unreachable)


class _ChildAttemptSpans:
    """The attempt spans of this process: `NO_ATTEMPT_SPANS` until the child
    boot chooses them (`use_attempt_spans`)."""

    def __init__(self) -> None:
        self.spans: AttemptSpans = NO_ATTEMPT_SPANS


_CHILD: Final = _ChildAttemptSpans()


def use_attempt_spans(spans: AttemptSpans) -> None:
    """Make `spans` the attempt spans of this process. The child boot calls
    it once, after the start hooks, where the user sets the SDK."""
    _CHILD.spans = spans


def current_attempt_spans() -> AttemptSpans:
    """The attempt spans of this process."""
    return _CHILD.spans


def run_in_attempt_span(
    subject: AttemptSubject,
    steps: Callable[[], EncodedResult | BodyInterrupted],
    body_end: Callable[[], BodyEndOrder],
) -> ResultEnvelope | BodyInterrupted:
    """Run `steps` as the attempt `subject`: in its log context and, with
    `OtelAttemptSpans`, inside its span, current on the calling thread.
    When the steps return, `body_end` gives which came first, the deadline
    of the run or the end of its steps; it is called once, with tracing on
    or off. The span ends with `RunTimedOut` after `DeadlineFirst`, else
    with the outcome of the steps. Returns what the steps returned, with
    the envelope only. An exception of `steps` ends the span with
    `ScopeRaised` and propagates; `body_end` is not called then."""
    span = _open(subject)
    task = enter_running_task(_running_task(subject, span))
    current = _make_current(span)
    try:
        ran = steps()
    except BaseException as raised:
        _leave(span, current, task, ScopeRaised(raised))
        raise
    _leave(span, current, task, _span_end(ran, body_end()))
    return _envelope_of(ran)


async def run_in_attempt_span_async(
    subject: AttemptSubject,
    steps: Callable[[], Awaitable[EncodedResult | BodyInterrupted]],
    body_end: Callable[[], BodyEndOrder],
) -> ResultEnvelope | BodyInterrupted:
    """`run_in_attempt_span` for steps that are awaited. Call it inside the
    task that runs the body: the log context and the span are set in the
    context of that task, so each task that the body starts copies them, and
    no other task sees them. Nothing is awaited after the steps, so the
    order that `body_end` gives is read in the same step of the loop as
    the end of the steps."""
    span = _open(subject)
    task = enter_running_task(_running_task(subject, span))
    current = _make_current(span)
    try:
        ran = await steps()
    except BaseException as raised:
        _leave(span, current, task, ScopeRaised(raised))
        raise
    _leave(span, current, task, _span_end(ran, body_end()))
    return _envelope_of(ran)


@dataclass(frozen=True, slots=True)
class _NoSpan:
    """The attempt has no span: tracing is off, or the recorder raised when
    the span opened."""


@dataclass(frozen=True, slots=True)
class _CurrentSpan:
    """The span is the current context; `restore` restores the one before."""

    restore: Callable[[], None]


type _SpanContext = _CurrentSpan | _NoSpan


def _open(subject: AttemptSubject) -> OpenAttemptSpan | _NoSpan:
    """The span of `subject`, or `_NoSpan` with tracing off or when the
    recorder raised `Exception` (logged once per process)."""
    match current_attempt_spans():
        case NoAttemptSpans():
            return _NoSpan()
        case OtelAttemptSpans(recorder=recorder):
            try:
                return recorder.open(subject)
            except Exception as failure:
                _RECORDER_FAILURES.report('open', failure)
                return _NoSpan()
        case _ as unreachable:
            assert_never(unreachable)


def _running_task(
    subject: AttemptSubject,
    span: OpenAttemptSpan | _NoSpan,
) -> RunningTask:
    """The log context of `subject` with the ids of `span`."""
    match span:
        case _NoSpan():
            ids: SpanIds | NoSpanIds = NoSpanIds()
        case _:
            ids = span.ids
    return RunningTask(
        task_id=subject.task_id,
        task_name=subject.task_name,
        attempt=subject.attempt,
        span=ids,
    )


def _make_current(span: OpenAttemptSpan | _NoSpan) -> _SpanContext:
    """Make `span` current. A recorder that raises leaves the context as it
    was (logged once per process)."""
    match span:
        case _NoSpan():
            return _NoSpan()
        case _:
            try:
                return _CurrentSpan(span.make_current())
            except Exception as failure:
                _RECORDER_FAILURES.report('attach', failure)
                return _NoSpan()


def _leave(
    span: OpenAttemptSpan | _NoSpan,
    current: _SpanContext,
    task: Token[RunningTask | NoRunningTask],
    end: AttemptEnd,
) -> None:
    """End `span` with `end`, restore the context that it replaced, then
    the running task that `task` replaced."""
    try:
        _close(span, current, end)
    finally:
        leave_running_task(task)


def _close(span: OpenAttemptSpan | _NoSpan, current: _SpanContext, end: AttemptEnd) -> None:
    """End `span` with `end`, then restore the context. An `Exception` of
    the recorder is logged once per process and goes no further."""
    match span:
        case _NoSpan():
            return
        case _:
            pass
    try:
        span.end(end)
    except Exception as failure:
        _RECORDER_FAILURES.report('end', failure)
    match current:
        case _CurrentSpan(restore=restore):
            try:
                restore()
            except Exception as failure:
                _RECORDER_FAILURES.report('detach', failure)
        case _NoSpan():
            pass
        case _ as unreachable:
            assert_never(unreachable)


def _span_end(ran: EncodedResult | BodyInterrupted, order: BodyEndOrder) -> AttemptEnd:
    """The end of a span whose steps returned `ran`, in the order `order`:
    the timeout when the deadline came first, else the outcome of the
    steps. Pure."""
    match order:
        case DeadlineFirst():
            return RUN_TIMED_OUT
        case BodyFirst():
            return _end_of(ran)
        case _ as unreachable:
            assert_never(unreachable)


def _end_of(ran: EncodedResult | BodyInterrupted) -> AttemptEnd:
    """The end of a span whose steps returned `ran`. Pure."""
    match ran:
        case EncodedResult(outcome=outcome):
            return outcome
        case BodyInterrupted():
            return ran
        case _ as unreachable:
            assert_never(unreachable)


def _envelope_of(ran: EncodedResult | BodyInterrupted) -> ResultEnvelope | BodyInterrupted:
    """What a body place returns for `ran`: the envelope, or the
    interruption. Pure."""
    match ran:
        case EncodedResult(envelope=envelope):
            return envelope
        case BodyInterrupted():
            return ran
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class NothingToFlush:
    """Tracing is off in this child."""


@dataclass(frozen=True, slots=True)
class FlushTimedOut:
    """The flush did not return within its bound. It goes on in a daemon
    thread, which does not hold the exit."""

    bound_s: float


@dataclass(frozen=True, slots=True)
class FlushRaised:
    """The provider's flush raised `exception`."""

    exception: Exception


@dataclass(frozen=True, slots=True)
class FlushThreadRefused:
    """The OS refused the thread of the flush: no flush ran."""

    detail: str


type StopFlush = (
    ProviderFlush | NothingToFlush | FlushTimedOut | FlushRaised | FlushThreadRefused
)
"""How the flush of the attempt spans at a child stop ended."""


def flush_attempt_spans_at_stop() -> StopFlush:
    """Flush the attempt spans of this child, within
    `ATTEMPT_SPAN_FLUSH_BOUND_S`, and log an end other than a flush or
    nothing to flush at WARNING. The child stops after it in every case:
    a flush never changes the end of a child. Nothing escapes but a
    defect."""
    match current_attempt_spans():
        case NoAttemptSpans():
            return NothingToFlush()
        case OtelAttemptSpans(recorder=recorder):
            flushed = bounded_flush(recorder.flush, ATTEMPT_SPAN_FLUSH_BOUND_S)
        case _ as unreachable:
            assert_never(unreachable)
    _log_stop_flush(flushed)
    return flushed


def bounded_flush(
    flush: Callable[[int], ProviderFlush],
    bound_s: float,
) -> ProviderFlush | FlushTimedOut | FlushRaised | FlushThreadRefused:
    """Run `flush` with a timeout of `bound_s` on a daemon thread, and wait
    for it at most `bound_s`. The SDK's batch processor does not bound its
    own export by the timeout it takes, so the wait here is the bound."""
    ends: queue.Queue[ProviderFlush | FlushRaised] = queue.Queue(maxsize=1)
    timeout_millis = max(1, int(bound_s * MILLISECONDS_PER_SECOND))

    def run_flush() -> None:
        try:
            ends.put_nowait(flush(timeout_millis))
        except Exception as failure:
            ends.put_nowait(FlushRaised(failure))

    thread = threading.Thread(target=run_flush, name='horsies-span-flush', daemon=True)
    try:
        thread.start()
    except RuntimeError as refused:
        return FlushThreadRefused(detail=str(refused))
    try:
        return ends.get(timeout=bound_s)
    except queue.Empty:
        return FlushTimedOut(bound_s=bound_s)


def _log_stop_flush(flushed: StopFlush) -> None:
    match flushed:
        case SpansFlushed() | NothingToFlush() | ProviderHasNoFlush():
            pass
        case FlushNotConfirmed():
            _logger.warning(
                'The tracer provider did not confirm the export of every '
                'attempt span at the child stop; spans may be lost',
            )
        case FlushTimedOut(bound_s=bound_s):
            _logger.warning(
                'The flush of the attempt spans did not end within %s s at '
                'the child stop; the child stops, and spans may be lost',
                bound_s,
            )
        case FlushRaised(exception=exception):
            _logger.warning(
                'The flush of the attempt spans raised at the child stop: '
                '%s: %s; the child stops, and spans may be lost',
                type(exception).__qualname__,
                exception,
            )
        case FlushThreadRefused(detail=detail):
            _logger.warning(
                'The OS refused the thread of the attempt span flush at the '
                'child stop (%s); the child stops with no flush',
                detail,
            )
        case _ as unreachable:
            assert_never(unreachable)


class _RecorderFailureLog:
    """Logs the first failure of the recorder in this process at WARNING;
    later failures are not logged."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._logged = False

    def report(self, stage: str, failure: Exception) -> None:
        with self._lock:
            first = not self._logged
            self._logged = True
        match first:
            case True:
                _logger.warning(
                    'the OpenTelemetry recorder raised when an attempt span '
                    'was at the %s stage: %s: %s. The task runs on; this span '
                    'is not complete. Later failures are not logged.',
                    stage,
                    type(failure).__qualname__,
                    failure,
                )
            case False:
                pass


_RECORDER_FAILURES: Final = _RecorderFailureLog()
