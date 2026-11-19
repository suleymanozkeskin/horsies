"""The span of each attempt that the worker ended, made in the handler child.

The task child opens the span of an attempt around its body, and a span is
exported only when it ends. When the worker ends the attempt itself (an
isolated task killed at its timeout, a shared async child killed for a
blocked loop, a child lost with its exit seen), the child is gone and that
span never ends. The `AttemptEnded` event of such an attempt
(`AttemptEndDecidedByWorker`) carries the facts of its span, and the handler
child makes the span from them.

The handler child chooses once, at boot, after its
`on_handler_child_start` hooks (where the user sets the tracer provider),
from `AppConfig.tracing` (`ended_attempt_spans_of`):

- `NoTracing`, or `OtelTracing(worker_ended_spans=WorkerEndedSpans.NO_SPANS)`:
  `NoEndedAttemptSpans`. Nothing imports `opentelemetry`, and an event costs
  one `match`.
- `OtelTracing(worker_ended_spans=WorkerEndedSpans.MAKE_SPANS)`:
  `OtelEndedAttemptSpans`, whose recorder
  (`horsies.core.telemetry.ended_attempt_otel`) makes the span with the
  OpenTelemetry API.

Telemetry never stops the handler child: a recorder (the user's SDK) that
raises is logged once per process, and the event still goes to the handlers.
`flush_ended_attempt_spans_at_stop` flushes the spans at the stop of the
child, within `ENDED_ATTEMPT_SPAN_FLUSH_BOUND_S`.

This module imports no `opentelemetry` module.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Final, Protocol, assert_never

from horsies._catalog.events import AnyEvent, AttemptEndDecidedByWorker, AttemptEnded
from horsies.core.logging import get_logger
from horsies.core.models.tracing import NoTracing, OtelTracing, WorkerEndedSpans
from horsies.core.telemetry.attempt_span import (
    FlushNotConfirmed,
    FlushRaised,
    FlushThreadRefused,
    FlushTimedOut,
    NothingToFlush,
    ProviderFlush,
    ProviderHasNoFlush,
    SpansFlushed,
    StopFlush,
    bounded_flush,
)

_logger = get_logger('telemetry')

ENDED_ATTEMPT_SPAN_FLUSH_BOUND_S: Final = 1.0
"""The longest wait for the flush of the spans at the stop of the handler
child. The worker kills a child that has not exited `CHILD_STOP_TIMEOUT`
(10 s, horsies-core) after the stop frame. Before the flush, the handler
threads get `HANDLER_DRAIN_S` (2 s): 2 s + 1 s and the last stats frame
stay under 10 s."""


class EndedAttemptSpanRecorder(Protocol):
    """Makes the spans of the attempts that the worker ended, and flushes
    them."""

    def make(self, ended: AttemptEnded, decided: AttemptEndDecidedByWorker) -> None:
        """Make and end the span of the attempt of `ended`, whose end is
        `decided`. May raise: the caller logs it."""
        ...

    def flush(self, timeout_millis: int) -> ProviderFlush:
        """Export the spans that ended, waiting at most `timeout_millis` as
        the provider applies it. May raise or block: the caller bounds it."""
        ...


@dataclass(frozen=True, slots=True)
class NoEndedAttemptSpans:
    """No span for an attempt that the worker ended: its event goes to the
    handlers only."""


@dataclass(frozen=True, slots=True)
class OtelEndedAttemptSpans:
    """A span for each attempt that the worker ended, through `recorder`."""

    recorder: EndedAttemptSpanRecorder


type EndedAttemptSpans = NoEndedAttemptSpans | OtelEndedAttemptSpans
"""What the handler child does for an attempt that the worker ended."""

NO_ENDED_ATTEMPT_SPANS: Final = NoEndedAttemptSpans()
"""The choice of `NoTracing` and of `WorkerEndedSpans.NO_SPANS`."""


def ended_attempt_spans_of(tracing: OtelTracing | NoTracing) -> EndedAttemptSpans:
    """The spans of the attempts that the worker ended under `tracing`, the
    setting that gives the `worker_ended_spans` of the worker manifest
    (`worker_ended_spans_of`). `MAKE_SPANS` imports the OpenTelemetry API
    (`horsies.core.telemetry.ended_attempt_otel`), which the config check
    found installed; `NO_SPANS` and `NoTracing` import nothing."""
    match tracing:
        case NoTracing():
            return NO_ENDED_ATTEMPT_SPANS
        case OtelTracing(parentage=parentage, worker_ended_spans=spans):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match spans:
        case WorkerEndedSpans.NO_SPANS:
            return NO_ENDED_ATTEMPT_SPANS
        case WorkerEndedSpans.MAKE_SPANS:
            from horsies.core.telemetry.ended_attempt_otel import (
                otel_ended_attempt_recorder,
            )

            return OtelEndedAttemptSpans(recorder=otel_ended_attempt_recorder(parentage))


@dataclass(frozen=True, slots=True)
class EndedSpanMade:
    """The recorder made and ended the span of the attempt."""


@dataclass(frozen=True, slots=True)
class EndedSpansOff:
    """The child makes no span: `NoEndedAttemptSpans`. The event was not
    read."""


@dataclass(frozen=True, slots=True)
class NotEndedByWorker:
    """The event is not the `AttemptEnded` of an attempt that the worker
    ended: no span."""


@dataclass(frozen=True, slots=True)
class EndedSpanRaised:
    """The recorder raised `exception`: the span is not complete. Logged
    once per process."""

    exception: Exception


type EndedSpanStep = EndedSpanMade | EndedSpansOff | NotEndedByWorker | EndedSpanRaised
"""What the step of one event did."""


def make_ended_attempt_span(spans: EndedAttemptSpans, event: AnyEvent) -> EndedSpanStep:
    """The step of the handler child for one event, before the handlers get
    it: with `OtelEndedAttemptSpans`, the span of an `AttemptEnded` whose end
    is `AttemptEndDecidedByWorker`. An `Exception` of the recorder is logged
    once per process and goes no further."""
    match spans:
        case NoEndedAttemptSpans():
            return EndedSpansOff()
        case OtelEndedAttemptSpans(recorder=recorder):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match event:
        case AttemptEnded(end=AttemptEndDecidedByWorker() as decided) as ended:
            pass
        case _:
            return NotEndedByWorker()
    try:
        recorder.make(ended, decided)
    except Exception as failure:
        _RECORDER_FAILURES.report(failure)
        return EndedSpanRaised(exception=failure)
    return EndedSpanMade()


def flush_ended_attempt_spans_at_stop(spans: EndedAttemptSpans) -> StopFlush:
    """Flush the spans of the attempts that the worker ended, within
    `ENDED_ATTEMPT_SPAN_FLUSH_BOUND_S`, and log an end other than a flush or
    nothing to flush at WARNING. The child stops after it in every case.
    Nothing escapes but a defect."""
    match spans:
        case NoEndedAttemptSpans():
            return NothingToFlush()
        case OtelEndedAttemptSpans(recorder=recorder):
            flushed = bounded_flush(recorder.flush, ENDED_ATTEMPT_SPAN_FLUSH_BOUND_S)
        case _ as unreachable:
            assert_never(unreachable)
    _log_stop_flush(flushed)
    return flushed


def _log_stop_flush(flushed: StopFlush) -> None:
    match flushed:
        case SpansFlushed() | NothingToFlush() | ProviderHasNoFlush():
            pass
        case FlushNotConfirmed():
            _logger.warning(
                'The tracer provider did not confirm the export of every span '
                'of an attempt that the worker ended at the handler child stop; '
                'spans may be lost',
            )
        case FlushTimedOut(bound_s=bound_s):
            _logger.warning(
                'The flush of the spans of the attempts that the worker ended '
                'did not end within %s s at the handler child stop; the child '
                'stops, and spans may be lost',
                bound_s,
            )
        case FlushRaised(exception=exception):
            _logger.warning(
                'The flush of the spans of the attempts that the worker ended '
                'raised at the handler child stop: %s: %s; the child stops, and '
                'spans may be lost',
                type(exception).__qualname__,
                exception,
            )
        case FlushThreadRefused(detail=detail):
            _logger.warning(
                'The OS refused the thread of the flush of the spans of the '
                'attempts that the worker ended at the handler child stop (%s); '
                'the child stops with no flush',
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

    def report(self, failure: Exception) -> None:
        with self._lock:
            first = not self._logged
            self._logged = True
        match first:
            case True:
                _logger.warning(
                    'the OpenTelemetry recorder raised when it made the span of '
                    'an attempt that the worker ended: %s: %s. The event still '
                    'goes to the handlers; this span is not complete. Later '
                    'failures are not logged.',
                    type(failure).__qualname__,
                    failure,
                )
            case False:
                pass


_RECORDER_FAILURES: Final = _RecorderFailureLog()
