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
  `messaging.destination.name` (the queue), `horsies.task.id`,
  `horsies.task.name`, `horsies.task.attempt` (from 1),
  `horsies.task.execution`, `horsies.queue`, `horsies.worker.id`; for a task
  that a workflow node links, `horsies.workflow.id`, `horsies.workflow.node`
  (the index of the node) and `horsies.workflow.root_id` (the top-level
  workflow of its tree), as the start read them.
- Status: unset when the envelope holds the task's value; ERROR with
  `error.type` = the error code when it holds a task error. The live
  exception that the task error keeps is recorded on the span first. A run
  whose deadline came first in its child (a shared run, `RunTimedOut`) sets
  ERROR with `error.type` = `TASK_TIMEOUT`, the code that the worker
  records; what the body returned is not recorded. A body that a refusal
  of the core ended, and an exception that left the steps, set ERROR with
  `error.type` = the class name of the exception, which is recorded.

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
from horsies._catalog.task_codes import OutcomeCode
from horsies.core.logging import NoSpanIds, SpanIds
from horsies.core.models.tracing import ChildOfSend, LinkedToSend
from horsies.core.telemetry.attempt_span import (
    AttemptEnd,
    AttemptSubject,
    FlushNotConfirmed,
    ProviderFlush,
    ProviderHasNoFlush,
    RunTimedOut,
    ScopeRaised,
    SpansFlushed,
)
from horsies.core.worker.child_frames import NoTraceCarrier, RunTrace, StoredTraceCarrier
from horsies.core.worker.run_return import (
    BodyInterrupted,
    InWorkflow,
    NotInWorkflow,
    NothingRaised,
    RaisedInRun,
    ResultFailed,
    ResultSucceeded,
    RunWorkflow,
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
MESSAGING_DESTINATION_NAME_KEY: Final = 'messaging.destination.name'
ERROR_TYPE_KEY: Final = 'error.type'
TASK_ID_KEY: Final = 'horsies.task.id'
TASK_NAME_KEY: Final = 'horsies.task.name'
TASK_ATTEMPT_KEY: Final = 'horsies.task.attempt'
TASK_EXECUTION_KEY: Final = 'horsies.task.execution'
QUEUE_KEY: Final = 'horsies.queue'
WORKER_ID_KEY: Final = 'horsies.worker.id'
WORKFLOW_ID_KEY: Final = 'horsies.workflow.id'
WORKFLOW_NODE_KEY: Final = 'horsies.workflow.node'
WORKFLOW_ROOT_ID_KEY: Final = 'horsies.workflow.root_id'

TRACE_ID_HEX_DIGITS: Final = 32
SPAN_ID_HEX_DIGITS: Final = 16


def otel_attempt_recorder(parentage: ChildOfSend | LinkedToSend) -> OtelAttemptRecorder:
    """The recorder of the global tracer provider, with `parentage`."""
    return OtelAttemptRecorder(
        tracer=trace.get_tracer(INSTRUMENTATION_SCOPE, __version__),
        parentage=parentage,
        provider_flush=global_provider_flush,
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
        placed = place_attempt(context_of_trace(subject.trace), self._parentage)
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
        """Flush the global tracer provider (`global_provider_flush`)."""
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
            case RunTimedOut():
                mark_error(self.span, OutcomeCode.TASK_TIMEOUT.value)
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


def context_of_trace(trace: RunTrace) -> Context:
    """The context that the global propagator extracts from the stored
    carrier `trace`, into an empty context; an empty context with no
    carrier."""
    match trace:
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
        **run_attributes(
            task_id=subject.task_id,
            task_name=subject.task_name,
            queue_name=subject.queue_name,
            attempt=subject.attempt,
            execution=subject.execution.value,
            worker_id=subject.worker_id,
        ),
        **workflow_attributes(subject.workflow),
    }


def run_attributes(
    *,
    task_id: str,
    task_name: str,
    queue_name: str,
    attempt: int,
    execution: str,
    worker_id: str,
) -> dict[str, AttributeValue]:
    """The attributes of an attempt span besides its workflow attributes:
    attempt `attempt` (from 1) of task `task_id`, of the queue `queue_name`,
    in a child of the class `execution`, for the claim of `worker_id`. The
    span of an attempt that the worker ended has the same set. Pure."""
    return {
        MESSAGING_SYSTEM_KEY: MESSAGING_SYSTEM,
        MESSAGING_OPERATION_TYPE_KEY: PROCESS_OPERATION_TYPE,
        MESSAGING_OPERATION_NAME_KEY: RUN_OPERATION_NAME,
        MESSAGING_MESSAGE_ID_KEY: task_id,
        MESSAGING_DESTINATION_NAME_KEY: queue_name,
        TASK_ID_KEY: task_id,
        TASK_NAME_KEY: task_name,
        TASK_ATTEMPT_KEY: attempt,
        TASK_EXECUTION_KEY: execution,
        QUEUE_KEY: queue_name,
        WORKER_ID_KEY: worker_id,
    }


def workflow_attributes(workflow: RunWorkflow) -> dict[str, AttributeValue]:
    """The workflow attributes of an attempt span: the workflow id, the node
    index and the root workflow id for a task that a node links, none
    otherwise. Pure."""
    match workflow:
        case InWorkflow(
            workflow_id=workflow_id,
            node_index=node_index,
            root_workflow_id=root_workflow_id,
        ):
            return {
                WORKFLOW_ID_KEY: str(workflow_id),
                WORKFLOW_NODE_KEY: node_index,
                WORKFLOW_ROOT_ID_KEY: str(root_workflow_id),
            }
        case NotInWorkflow():
            return {}
        case _ as unreachable:
            assert_never(unreachable)


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


def global_provider_flush(timeout_millis: int) -> ProviderFlush:
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
