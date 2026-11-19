"""The attempt spans of a worker child through the OpenTelemetry API: the
recorder of `AppConfig(tracing=OtelTracing())`.

This module imports `opentelemetry`. `attempt_spans_of` imports it only for
`OtelTracing`, whose config check found the API installed.

- An attempt is one span of kind CONSUMER, `run <task_name>`, around the
  steps of the run after its RUNNING update.
- Its parent: the configured propagator (`opentelemetry.propagate`)
  extracts the carrier that the send stored on the row. With `ChildOfSend`
  the extracted span is the parent. With `LinkedToSend` the span starts a
  trace of its own, with a link to the extracted span; the other entries of
  the extracted context (baggage) stay. A row with no carrier, or a carrier
  that holds no valid span, gives a span with no parent.
- Attributes: `messaging.system`, `messaging.operation.type` (`process`),
  `messaging.operation.name` (`run`), `messaging.message.id`,
  `horsies.task.id`, `horsies.task.name`, `horsies.task.execution`,
  `horsies.worker.id`.
- Status: unset when the envelope holds the task's value; ERROR with
  `error.type` = the error code when it holds a task error. The live
  exception that the task error keeps is recorded on the span first. A body
  that a refusal of the core ended, and an exception that left the steps,
  set ERROR with `error.type` = the class name of the exception, which is
  recorded.

The tracer and the propagator come from the global API at each use: a
provider that the user sets in a start hook of the child is used. A flush
calls `force_flush` of the global tracer provider when it has one.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, assert_never

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.trace import Link, Span, SpanKind, Status, StatusCode, Tracer
from opentelemetry.util.types import AttributeValue

from horsies import __version__
from horsies.core.logging import NoSpanIds, SpanIds
from horsies.core.models.tracing import ChildOfSend, LinkedToSend
from horsies.core.telemetry.attempt_span import (
    AttemptEnd,
    AttemptSubject,
    FlushNotConfirmed,
    ProviderFlush,
    ProviderHasNoFlush,
    ScopeRaised,
    SpansFlushed,
)
from horsies.core.worker.child_frames import NoTraceCarrier, StoredTraceCarrier
from horsies.core.worker.run_return import (
    BodyInterrupted,
    NothingRaised,
    RaisedInRun,
    ResultFailed,
    ResultSucceeded,
)

INSTRUMENTATION_SCOPE: Final = 'horsies'
"""The name of the tracer of horsies."""

MESSAGING_SYSTEM: Final = 'horsies'
"""The value of `messaging.system`."""

PROCESS_OPERATION_TYPE: Final = 'process'
"""`messaging.operation.type` of an attempt."""

RUN_OPERATION_NAME: Final = 'run'
"""`messaging.operation.name` of an attempt, and the first word of its span
name."""

MESSAGING_SYSTEM_KEY: Final = 'messaging.system'
MESSAGING_OPERATION_TYPE_KEY: Final = 'messaging.operation.type'
MESSAGING_OPERATION_NAME_KEY: Final = 'messaging.operation.name'
MESSAGING_MESSAGE_ID_KEY: Final = 'messaging.message.id'
ERROR_TYPE_KEY: Final = 'error.type'
TASK_ID_KEY: Final = 'horsies.task.id'
TASK_NAME_KEY: Final = 'horsies.task.name'
TASK_EXECUTION_KEY: Final = 'horsies.task.execution'
WORKER_ID_KEY: Final = 'horsies.worker.id'

TRACE_ID_HEX_DIGITS: Final = 32
SPAN_ID_HEX_DIGITS: Final = 16


def otel_attempt_recorder(parentage: ChildOfSend | LinkedToSend) -> OtelAttemptRecorder:
    """The recorder of the global tracer provider, with `parentage`."""
    return OtelAttemptRecorder(
        tracer=trace.get_tracer(INSTRUMENTATION_SCOPE, __version__),
        parentage=parentage,
        provider_flush=_global_provider_flush,
    )


class OtelAttemptRecorder:
    """Opens the attempt spans with `tracer`, placed by `parentage`, and
    flushes through `provider_flush`."""

    def __init__(
        self,
        *,
        tracer: Tracer,
        parentage: ChildOfSend | LinkedToSend,
        provider_flush: Callable[[int], ProviderFlush],
    ) -> None:
        self._tracer = tracer
        self._parentage = parentage
        self._provider_flush = provider_flush

    def open(self, subject: AttemptSubject) -> OtelAttemptSpan:
        """Open the span of `subject` (see the module doc)."""
        placed = place_attempt(_extracted(subject), self._parentage)
        span = self._tracer.start_span(
            f'{RUN_OPERATION_NAME} {subject.task_name}',
            context=placed.parent,
            kind=SpanKind.CONSUMER,
            attributes=attempt_attributes(subject),
            links=placed.links,
        )
        return OtelAttemptSpan(
            span=span,
            current=trace.set_span_in_context(span, placed.parent),
            ids=span_ids_of(span),
        )

    def flush(self, timeout_millis: int) -> ProviderFlush:
        """Flush the global tracer provider (`_global_provider_flush`)."""
        return self._provider_flush(timeout_millis)


@dataclass(frozen=True, slots=True)
class OtelAttemptSpan:
    """The open span of one attempt; `current` is the context in which it
    is the current span."""

    span: Span
    current: Context
    ids: SpanIds | NoSpanIds

    def make_current(self) -> Callable[[], None]:
        """Attach `current`; the returned function detaches it."""
        token = otel_context.attach(self.current)

        def restore() -> None:
            otel_context.detach(token)

        return restore

    def end(self, end: AttemptEnd) -> None:
        """Set the status from `end` (see the module doc), and end the span."""
        match end:
            case ResultSucceeded():
                pass
            case ResultFailed(error_code=code, raised=raised):
                record_raised(self.span, raised)
                mark_error(self.span, code)
            case BodyInterrupted(refusal=exception) | ScopeRaised(exception=exception):
                self.span.record_exception(exception)
                mark_error(self.span, type(exception).__qualname__)
            case _ as unreachable:
                assert_never(unreachable)
        self.span.end()


@dataclass(frozen=True, slots=True)
class PlacedAttempt:
    """Where an attempt span starts: the context of its parent, and its
    links."""

    parent: Context
    links: tuple[Link, ...]


def _extracted(subject: AttemptSubject) -> Context:
    """The context that the global propagator extracts from the carrier of
    `subject`, into an empty context; an empty context with no carrier."""
    match subject.trace:
        case StoredTraceCarrier(carrier=carrier):
            return propagate.extract(dict(carrier), context=Context())
        case NoTraceCarrier():
            return Context()
        case _ as unreachable:
            assert_never(unreachable)


def place_attempt(extracted: Context, parentage: ChildOfSend | LinkedToSend) -> PlacedAttempt:
    """The parent and the links of an attempt span from the context
    `extracted` from its carrier. Pure."""
    match parentage:
        case ChildOfSend():
            return PlacedAttempt(parent=extracted, links=())
        case LinkedToSend():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    send = trace.get_current_span(extracted).get_span_context()
    root = trace.set_span_in_context(trace.INVALID_SPAN, extracted)
    match send.is_valid:
        case True:
            return PlacedAttempt(parent=root, links=(Link(send),))
        case False:
            return PlacedAttempt(parent=root, links=())


def attempt_attributes(subject: AttemptSubject) -> dict[str, AttributeValue]:
    """The attributes of the span of `subject`. Pure."""
    return {
        MESSAGING_SYSTEM_KEY: MESSAGING_SYSTEM,
        MESSAGING_OPERATION_TYPE_KEY: PROCESS_OPERATION_TYPE,
        MESSAGING_OPERATION_NAME_KEY: RUN_OPERATION_NAME,
        MESSAGING_MESSAGE_ID_KEY: subject.task_id,
        TASK_ID_KEY: subject.task_id,
        TASK_NAME_KEY: subject.task_name,
        TASK_EXECUTION_KEY: subject.execution.value,
        WORKER_ID_KEY: subject.worker_id,
    }


def span_ids_of(span: Span) -> SpanIds | NoSpanIds:
    """The ids of `span` as lowercase hex, or `NoSpanIds` for a span that
    is not valid (no SDK, or a sampler that gave no span). Pure."""
    span_context = span.get_span_context()
    match span_context.is_valid:
        case True:
            return SpanIds(
                trace_id=format(span_context.trace_id, f'0{TRACE_ID_HEX_DIGITS}x'),
                span_id=format(span_context.span_id, f'0{SPAN_ID_HEX_DIGITS}x'),
            )
        case False:
            return NoSpanIds()


def record_raised(span: Span, raised: RaisedInRun | NothingRaised) -> None:
    """Record the live exception of a task error on `span`, as an event."""
    match raised:
        case RaisedInRun(exception=exception):
            span.record_exception(exception)
        case NothingRaised():
            pass
        case _ as unreachable:
            assert_never(unreachable)


def mark_error(span: Span, error_type: str) -> None:
    """Set the status ERROR and `error.type` on `span`."""
    span.set_attribute(ERROR_TYPE_KEY, error_type)
    span.set_status(Status(StatusCode.ERROR))


def _global_provider_flush(timeout_millis: int) -> ProviderFlush:
    """`force_flush(timeout_millis)` of the global tracer provider. The API's
    own providers have none (`ProviderHasNoFlush`); the SDK's returns
    whether every span was exported. May raise or block: the caller bounds
    it (`bounded_flush`)."""
    match getattr(trace.get_tracer_provider(), 'force_flush', None):
        case flush if callable(flush):
            flushed: object = flush(timeout_millis)
        case _:
            return ProviderHasNoFlush()
    match flushed:
        case True:
            return SpansFlushed()
        case _:
            return FlushNotConfirmed()
