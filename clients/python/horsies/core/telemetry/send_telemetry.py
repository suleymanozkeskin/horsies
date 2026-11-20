"""The telemetry of the sends and the workflow starts of an app.

An app chooses its send telemetry once, from `AppConfig.tracing`
(`send_telemetry_of`):

- `NoTracing`: `NoSendTelemetry`. A send passes `NO_TRACE_CARRIER` to the
  core; nothing imports `opentelemetry`.
- `OtelTracing`: `OtelSendTelemetry`, whose recorder opens one span per task
  send (`send <task_name>`) and per workflow start (`start <workflow_name>`)
  with the OpenTelemetry API, injects the carrier with the configured
  propagator, and records the producer metrics of a task send
  (`horsies.core.telemetry.producer`).

`traced` and `traced_async` run one send or start inside its span. Telemetry
never changes the outcome of a send: a recorder that raises when the span
opens gives `NO_TRACE_CARRIER` and no span, and a recorder that raises when
the span ends is logged once per process. The exceptions of the send itself
pass through unchanged.

This module imports no `opentelemetry` module.
"""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol, assert_never
from uuid import UUID

from horsies._core import NoTraceCarrier, TraceCarrier
from horsies.core.logging import get_logger
from horsies.core.models.tracing import NoTracing, OtelTracing
from horsies.core.types.result import Err, Ok

if TYPE_CHECKING:
    # Annotations only: the workflows package imports this module.
    from horsies.core.models.task_send_types import TaskSendResult
    from horsies.core.workflows.start_types import WorkflowStartResult

type TraceArgument = NoTraceCarrier | TraceCarrier
"""The trace that a send or a workflow start passes to the core."""

NO_TRACE_CARRIER: Final = NoTraceCarrier()
"""The trace of a send with tracing off, or with no carrier to pass."""

_logger = get_logger('telemetry')


@dataclass(frozen=True, slots=True)
class TaskSendSubject:
    """The task send that a span covers."""

    task_name: str
    queue_name: str
    task_id: UUID


@dataclass(frozen=True, slots=True)
class WorkflowStartSubject:
    """The workflow start that a span covers. `workflow_id` is the id that
    the start uses: the caller's text, or the id minted for the start."""

    workflow_name: str
    workflow_id: str


@dataclass(frozen=True, slots=True)
class CallSucceeded:
    """The send or start returned `Ok`."""


@dataclass(frozen=True, slots=True)
class CallFailed:
    """The send or start returned `Err`. `error_code` is the `code` value of
    the error variant (`TaskSendErrorCode`, `WorkflowStartErrorCode`)."""

    error_code: str


@dataclass(frozen=True, slots=True)
class CallRaised:
    """The send or start raised. `exception_type` is the qualified name of
    the class of the exception."""

    exception_type: str


type CallEnd = CallSucceeded | CallFailed | CallRaised
"""How a send or a start ended."""


class OpenSpan(Protocol):
    """The span of one send or start, open until `end`."""

    @property
    def trace(self) -> TraceArgument:
        """The carrier of the span, for the core."""
        ...

    def end(self, call_end: CallEnd) -> None:
        """End the span with the outcome `call_end`, and record the metrics
        of the call. Called once."""
        ...


class SendRecorder(Protocol):
    """Opens the spans of the sends and the starts of one app."""

    def start_task_send(self, subject: TaskSendSubject) -> OpenSpan:
        """Open the span of the task send `subject`."""
        ...

    def start_workflow_start(self, subject: WorkflowStartSubject) -> OpenSpan:
        """Open the span of the workflow start `subject`."""
        ...


@dataclass(frozen=True, slots=True)
class NoSendTelemetry:
    """Tracing is off: no span, no metric, no carrier."""


@dataclass(frozen=True, slots=True)
class OtelSendTelemetry:
    """Spans and producer metrics through `recorder`."""

    recorder: SendRecorder


type SendTelemetry = NoSendTelemetry | OtelSendTelemetry
"""The send telemetry of an app."""

NO_SEND_TELEMETRY: Final = NoSendTelemetry()
"""The send telemetry of `NoTracing`."""


def send_telemetry_of(tracing: OtelTracing | NoTracing) -> SendTelemetry:
    """The send telemetry of the tracing setting `tracing`. `OtelTracing`
    imports the OpenTelemetry API (`horsies.core.telemetry.producer`), which
    the config check found installed; `NoTracing` imports nothing."""
    match tracing:
        case NoTracing():
            return NO_SEND_TELEMETRY
        case OtelTracing():
            from horsies.core.telemetry.producer import otel_send_recorder

            return OtelSendTelemetry(recorder=otel_send_recorder())
        case _ as unreachable:
            assert_never(unreachable)


def task_send_end(result: TaskSendResult[Any]) -> CallEnd:
    """The end of a task send that returned `result`. Pure."""
    match result:
        case Ok():
            return CallSucceeded()
        case Err(err_value=error):
            return CallFailed(error_code=error.code.value)


def workflow_start_end(result: WorkflowStartResult[Any]) -> CallEnd:
    """The end of a workflow start that returned `result`. Pure."""
    match result:
        case Ok():
            return CallSucceeded()
        case Err(err_value=error):
            return CallFailed(error_code=error.code.value)


def traced[R](
    open_span: Callable[[], OpenSpan],
    call: Callable[[TraceArgument], R],
    call_end: Callable[[R], CallEnd],
) -> R:
    """Run `call` with the carrier of the span that `open_span` opens, then
    end the span with `call_end` of the result, and return the result.

    An exception of `call` ends the span with `CallRaised` and propagates
    unchanged. An exception of the recorder never reaches the caller: when
    the span does not open, `call` runs with `NO_TRACE_CARRIER`; when it does
    not end, the failure is logged once per process.
    """
    match _opened(open_span):
        case _SpanNotOpened():
            return call(NO_TRACE_CARRIER)
        case span:
            pass
    try:
        result = call(span.trace)
    except BaseException as raised:
        _end(span, CallRaised(exception_type=type(raised).__qualname__))
        raise
    _end(span, call_end(result))
    return result


async def traced_async[R](
    open_span: Callable[[], OpenSpan],
    call: Callable[[TraceArgument], Awaitable[R]],
    call_end: Callable[[R], CallEnd],
) -> R:
    """`traced` for a call that is awaited. A cancel of the caller ends the
    span with `CallRaised` (`CancelledError`) and propagates."""
    match _opened(open_span):
        case _SpanNotOpened():
            return await call(NO_TRACE_CARRIER)
        case span:
            pass
    try:
        result = await call(span.trace)
    except BaseException as raised:
        _end(span, CallRaised(exception_type=type(raised).__qualname__))
        raise
    _end(span, call_end(result))
    return result


@dataclass(frozen=True, slots=True)
class _SpanNotOpened:
    """The recorder raised when it opened the span."""


def _opened(open_span: Callable[[], OpenSpan]) -> OpenSpan | _SpanNotOpened:
    """The span that `open_span` opens, or `_SpanNotOpened` when the
    recorder (the user's SDK) raised `Exception`; that failure is logged once
    per process."""
    try:
        return open_span()
    except Exception as failure:
        _RECORDER_FAILURES.report('open', failure)
        return _SpanNotOpened()


def _end(span: OpenSpan, call_end: CallEnd) -> None:
    """End `span`. An `Exception` of the recorder (the user's SDK) is logged
    once per process and goes no further."""
    try:
        span.end(call_end)
    except Exception as failure:
        _RECORDER_FAILURES.report('end', failure)


class _RecorderFailureLog:
    """Logs the first failure of a recorder in this process at WARNING; later
    failures are not logged."""

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
                    'the OpenTelemetry recorder raised when a send span was '
                    'at the %s stage: %s: %s. The send goes on; this span is '
                    'not complete. Later failures are not logged.',
                    stage,
                    type(failure).__qualname__,
                    failure,
                )
            case False:
                pass


_RECORDER_FAILURES: Final = _RecorderFailureLog()
