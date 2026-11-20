"""The spans of the sends and the workflow starts, the carrier of a rerun, and
the producer metrics, through the OpenTelemetry API: the recorder of
`AppConfig(tracing=OtelTracing())`.

This module imports `opentelemetry`. `send_telemetry_of` imports it only for
`OtelTracing`, whose config check found the API installed.

- A task send is one span of kind PRODUCER, `send <task_name>`, around every
  broker call of the send (a resend of a transient failure included). A
  workflow start is one span `start <workflow_name>`.
- The span's parent is the current context of the caller. The carrier is
  injected with the configured propagator (`opentelemetry.propagate`), and
  the core gets it with the 16-byte trace id of the span. A span that is not
  valid (no SDK, or a sampler that gives a non-recording span with no
  parent), or a propagator that writes no key, gives `NO_TRACE_CARRIER`.
- A rerun opens no span. Its carrier is the one that the configured
  propagator injects for the caller's current context, with the trace id of
  the current span; a current span that is not valid, or a propagator that
  writes no key, gives `NO_TRACE_CARRIER`.
- A call that returns `Err` sets the status ERROR and `error.type` to the
  error code; one that raises sets ERROR and `error.type` to the exception
  class. A success leaves the status unset.
- A task send records `horsies.task.sent` (task, queue, result) and
  `horsies.task.send.duration` (queue), with the names, units and bucket
  bounds of the core's catalog (`horsies._catalog.producer_metrics`). A send
  that raised records neither: its result is outside the closed set of the
  `result` label.

The tracer and the meter come from the global providers of the
OpenTelemetry API. A provider that the user sets after the first send is
used from then on: the API hands out proxies until a provider is set.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Final, assert_never

from opentelemetry import metrics, propagate, trace
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer
from opentelemetry.util.types import AttributeValue

from horsies import __version__
from horsies._catalog.producer_metrics import (
    QUEUE_ATTRIBUTE,
    RESULT_ATTRIBUTE,
    SEND_OK_RESULT,
    TASK_ATTRIBUTE,
    TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS,
    TASK_SEND_DURATION_DESCRIPTION,
    TASK_SEND_DURATION_NAME,
    TASK_SEND_DURATION_UNIT,
    TASK_SENT_DESCRIPTION,
    TASK_SENT_NAME,
    TASK_SENT_UNIT,
)
from horsies._core import TraceCarrier
from horsies.core.telemetry.send_telemetry import (
    NO_TRACE_CARRIER,
    CallEnd,
    CallFailed,
    CallRaised,
    CallSucceeded,
    TaskSendSubject,
    TraceArgument,
    WorkflowStartSubject,
)

INSTRUMENTATION_SCOPE: Final = 'horsies'
"""The name of the tracer and the meter of horsies."""

MESSAGING_SYSTEM: Final = 'horsies'
"""The value of `messaging.system`."""

SEND_OPERATION: Final = 'send'
"""`messaging.operation.type` of a task send and of a workflow start, and
`messaging.operation.name` of a task send."""

START_OPERATION: Final = 'start'
"""`messaging.operation.name` of a workflow start."""

MESSAGING_SYSTEM_KEY: Final = 'messaging.system'
MESSAGING_OPERATION_TYPE_KEY: Final = 'messaging.operation.type'
MESSAGING_OPERATION_NAME_KEY: Final = 'messaging.operation.name'
MESSAGING_DESTINATION_NAME_KEY: Final = 'messaging.destination.name'
MESSAGING_MESSAGE_ID_KEY: Final = 'messaging.message.id'
ERROR_TYPE_KEY: Final = 'error.type'
TASK_ID_KEY: Final = 'horsies.task.id'
WORKFLOW_ID_KEY: Final = 'horsies.workflow.id'
WORKFLOW_NAME_KEY: Final = 'horsies.workflow.name'

TRACE_ID_BYTES: Final = 16
"""Bytes of a trace id (W3C Trace Context)."""

NANOSECONDS_PER_SECOND: Final = 1_000_000_000


def otel_send_recorder() -> OtelSendRecorder:
    """The recorder of the global tracer and meter providers."""
    return OtelSendRecorder(
        tracer=trace.get_tracer(INSTRUMENTATION_SCOPE, __version__),
        meter=metrics.get_meter(INSTRUMENTATION_SCOPE, __version__),
    )


class OtelSendRecorder:
    """Opens the spans of the sends and the starts with `tracer`, and records
    the producer metrics with the instruments of `meter`."""

    def __init__(self, *, tracer: Tracer, meter: Meter) -> None:
        self._tracer = tracer
        self.sent: Counter = meter.create_counter(
            TASK_SENT_NAME,
            unit=TASK_SENT_UNIT,
            description=TASK_SENT_DESCRIPTION,
        )
        self.send_duration: Histogram = meter.create_histogram(
            TASK_SEND_DURATION_NAME,
            unit=TASK_SEND_DURATION_UNIT,
            description=TASK_SEND_DURATION_DESCRIPTION,
            explicit_bucket_boundaries_advisory=TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS,
        )

    def start_task_send(self, subject: TaskSendSubject) -> OtelTaskSend:
        """Open the span of the task send `subject`, a child of the current
        context."""
        task_id = str(subject.task_id)
        attributes: dict[str, AttributeValue] = {
            MESSAGING_SYSTEM_KEY: MESSAGING_SYSTEM,
            MESSAGING_OPERATION_TYPE_KEY: SEND_OPERATION,
            MESSAGING_OPERATION_NAME_KEY: SEND_OPERATION,
            MESSAGING_DESTINATION_NAME_KEY: subject.queue_name,
            MESSAGING_MESSAGE_ID_KEY: task_id,
            TASK_ID_KEY: task_id,
            TASK_ATTRIBUTE: subject.task_name,
            QUEUE_ATTRIBUTE: subject.queue_name,
        }
        span = self._tracer.start_span(
            f'send {subject.task_name}',
            kind=SpanKind.PRODUCER,
            attributes=attributes,
        )
        return OtelTaskSend(
            span=span,
            trace=trace_of_span(span),
            subject=subject,
            recorder=self,
            started_ns=time.perf_counter_ns(),
        )

    def start_workflow_start(self, subject: WorkflowStartSubject) -> OtelWorkflowStart:
        """Open the span of the workflow start `subject`, a child of the
        current context."""
        attributes: dict[str, AttributeValue] = {
            MESSAGING_SYSTEM_KEY: MESSAGING_SYSTEM,
            MESSAGING_OPERATION_TYPE_KEY: SEND_OPERATION,
            MESSAGING_OPERATION_NAME_KEY: START_OPERATION,
            WORKFLOW_ID_KEY: subject.workflow_id,
            WORKFLOW_NAME_KEY: subject.workflow_name,
        }
        span = self._tracer.start_span(
            f'start {subject.workflow_name}',
            kind=SpanKind.PRODUCER,
            attributes=attributes,
        )
        return OtelWorkflowStart(span=span, trace=trace_of_span(span))

    def current_trace(self) -> TraceArgument:
        """The carrier of the caller's current context, for a rerun: the
        carrier of its current span (`trace_of_span`). Opens no span."""
        return trace_of_span(trace.get_current_span())


@dataclass(frozen=True, slots=True)
class OtelTaskSend:
    """The open span of one task send, and the start of its duration on the
    monotonic clock."""

    span: Span
    trace: TraceArgument
    subject: TaskSendSubject
    recorder: OtelSendRecorder
    started_ns: int

    def end(self, call_end: CallEnd) -> None:
        """End the span with `call_end` and record the producer metrics of a
        send that returned (see the module doc)."""
        elapsed_seconds = (time.perf_counter_ns() - self.started_ns) / NANOSECONDS_PER_SECOND
        match call_end:
            case CallSucceeded():
                result = SEND_OK_RESULT
            case CallFailed(error_code=code):
                mark_error(self.span, code)
                result = code
            case CallRaised(exception_type=exception_type):
                mark_error(self.span, exception_type)
                self.span.end()
                return
            case _ as unreachable:
                assert_never(unreachable)
        self.recorder.sent.add(
            1,
            {
                TASK_ATTRIBUTE: self.subject.task_name,
                QUEUE_ATTRIBUTE: self.subject.queue_name,
                RESULT_ATTRIBUTE: result,
            },
        )
        self.recorder.send_duration.record(
            elapsed_seconds,
            {QUEUE_ATTRIBUTE: self.subject.queue_name},
        )
        self.span.end()


@dataclass(frozen=True, slots=True)
class OtelWorkflowStart:
    """The open span of one workflow start."""

    span: Span
    trace: TraceArgument

    def end(self, call_end: CallEnd) -> None:
        """End the span with `call_end`."""
        match call_end:
            case CallSucceeded():
                pass
            case CallFailed(error_code=code):
                mark_error(self.span, code)
            case CallRaised(exception_type=exception_type):
                mark_error(self.span, exception_type)
            case _ as unreachable:
                assert_never(unreachable)
        self.span.end()


def mark_error(span: Span, error_type: str) -> None:
    """Set the status ERROR and `error.type` on `span`."""
    span.set_attribute(ERROR_TYPE_KEY, error_type)
    span.set_status(Status(StatusCode.ERROR))


def trace_of_span(span: Span) -> TraceArgument:
    """The carrier of `span` for the core: the map that the configured
    propagator injects for a context whose current span is `span`, and the
    trace id of the span. `NO_TRACE_CARRIER` for a span that is not valid or
    a map with no key."""
    span_context = span.get_span_context()
    match span_context.is_valid:
        case False:
            return NO_TRACE_CARRIER
        case True:
            pass
    carrier: dict[str, str] = {}
    propagate.inject(carrier, context=trace.set_span_in_context(span))
    match len(carrier):
        case 0:
            return NO_TRACE_CARRIER
        case _:
            return TraceCarrier(
                carrier,
                span_context.trace_id.to_bytes(TRACE_ID_BYTES, 'big'),
            )
