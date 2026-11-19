"""The span of each attempt that the worker ended, through the OpenTelemetry
API: the recorder of the handler child with
`OtelTracing(worker_ended_spans=WorkerEndedSpans.MAKE_SPANS)`.

This module imports `opentelemetry`. `ended_attempt_spans_of` imports it only
for `OtelTracing`, whose config check found the API installed.

The span stands in for the attempt span that the task child opened and
never ended:

- Kind CONSUMER, name `run <task_name>`, as the span of a child.
- Its parent: the configured propagator extracts the carrier that the send
  stored on the row, and the parentage of `OtelTracing` places the span
  (`attempt_otel.place_attempt`), as for the span of a child.
- Start: when the task reached its child (`placed_at`, wall clock of the
  worker). The child's span started after the start write of the child, so
  this span is longer by the transit of the run frame and that write. End:
  when the end reached the worker (`AttemptEnded.at`).
- Status ERROR with `error.type` = the error code of the attempt row
  (`TASK_TIMEOUT`, `TASK_BLOCKED_LOOP`, `NEIGHBOUR_BLOCKED_LOOP`,
  `WORKER_CRASHED`).
- The attributes of the span of a child (`attempt_otel.run_attributes`),
  with the kind of the child as `horsies.task.execution`, and
  `horsies.span.source` = `worker`. The worker does not know the workflow
  node of a workflow task (`WORKFLOW_NOT_KNOWN`): the span has no
  `horsies.workflow.*` attribute; the carrier still places it in the trace
  of the workflow start.

Spans that the task code made inside the killed attempt and that were
exported keep the id of the lost span as their parent: a trace shows them
next to this span, not under it.

The tracer and the propagator come from the global API at each use, so a
provider that the user sets in an `on_handler_child_start` hook is used.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Final, assert_never, cast

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Tracer
from opentelemetry.util.types import AttributeValue

from horsies import __version__
from horsies._catalog.events import (
    AttemptEndDecidedByWorker,
    AttemptEnded,
    RunTraceCarrier,
    RunTraceNoCarrier,
)
from horsies._catalog.events import RunTrace as EventTrace
from horsies.core.logging import get_logger
from horsies.core.models.tracing import ChildOfSend, LinkedToSend
from horsies.core.telemetry.attempt_otel import (
    INSTRUMENTATION_SCOPE,
    RUN_OPERATION_NAME,
    context_of_trace,
    global_provider_flush,
    mark_error,
    place_attempt,
    run_attributes,
)
from horsies.core.telemetry.attempt_span import ProviderFlush
from horsies.core.worker.child_frames import NoTraceCarrier, RunTrace, StoredTraceCarrier

_logger = get_logger('telemetry')

SPAN_SOURCE_KEY: Final = 'horsies.span.source'
"""The attribute that names who made an attempt span."""

WORKER_SPAN_SOURCE: Final = 'worker'
"""`horsies.span.source` of the span of an attempt that the worker ended."""

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)
_NANOS_PER_MICROSECOND: Final = 1_000


def otel_ended_attempt_recorder(
    parentage: ChildOfSend | LinkedToSend,
) -> OtelEndedAttemptRecorder:
    """The recorder of the global tracer provider, with `parentage`."""
    return OtelEndedAttemptRecorder(
        tracer=trace.get_tracer(INSTRUMENTATION_SCOPE, __version__),
        parentage=parentage,
        provider_flush=global_provider_flush,
    )


class OtelEndedAttemptRecorder:
    """Makes the spans of the attempts that the worker ended with `tracer`,
    placed by `parentage`, and flushes through `provider_flush`."""

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

    def make(self, ended: AttemptEnded, decided: AttemptEndDecidedByWorker) -> None:
        """Make and end the span of the attempt of `ended` (see the module
        doc)."""
        placed = place_attempt(
            context_of_trace(run_trace_of_event(ended.task_id, decided.trace)),
            self._parentage,
        )
        span = self._tracer.start_span(
            f'{RUN_OPERATION_NAME} {ended.task_name}',
            context=placed.parent,
            kind=SpanKind.CONSUMER,
            attributes=ended_attempt_attributes(ended, decided),
            links=placed.links,
            start_time=unix_nanos(decided.placed_at),
        )
        mark_error(span, decided.error_code.value)
        span.end(end_time=unix_nanos(ended.at))

    def flush(self, timeout_millis: int) -> ProviderFlush:
        """Flush the global tracer provider (`global_provider_flush`)."""
        return self._provider_flush(timeout_millis)


def ended_attempt_attributes(
    ended: AttemptEnded,
    decided: AttemptEndDecidedByWorker,
) -> dict[str, AttributeValue]:
    """The attributes of the span of the attempt of `ended`: the attributes
    of the span of a child, with no workflow attribute, and
    `horsies.span.source`. Pure."""
    return {
        **run_attributes(
            task_id=str(ended.task_id),
            task_name=ended.task_name,
            queue_name=ended.queue,
            attempt=ended.attempt,
            execution=decided.child_kind,
            worker_id=decided.worker_id,
        ),
        SPAN_SOURCE_KEY: WORKER_SPAN_SOURCE,
    }


@dataclass(frozen=True, slots=True)
class _CarrierNotAnObjectOfTexts:
    """The stored carrier text is not one JSON object of texts."""

    detail: str


def run_trace_of_event(task_id: uuid.UUID, trace_of_event: EventTrace) -> RunTrace:
    """The stored carrier of an event as the run frame of a child carries
    it: the map of the JSON object of texts that the core stored. The core
    stores only such a text, so a text that does not read is a defect of
    the core: it is logged at ERROR with the task id `task_id`, and the span
    gets no parent."""
    match trace_of_event:
        case RunTraceNoCarrier():
            return NoTraceCarrier()
        case RunTraceCarrier(text=text):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match _carrier_map(text):
        case _CarrierNotAnObjectOfTexts(detail=detail):
            _logger.error(
                'The stored trace carrier of task %s in its AttemptEnded event is '
                'not one JSON object of texts, which the core stores only: %s; '
                'the span of the attempt that the worker ended has no parent',
                task_id,
                detail,
            )
            return NoTraceCarrier()
        case carrier:
            return StoredTraceCarrier(carrier=MappingProxyType(dict(carrier)))


def _carrier_map(text: str) -> Mapping[str, str] | _CarrierNotAnObjectOfTexts:
    """The map of the JSON object of texts `text`. Pure."""
    try:
        loaded: object = json.loads(text)
    except (ValueError, RecursionError) as exc:
        return _CarrierNotAnObjectOfTexts(detail=f'not JSON text: {exc}')
    match loaded:
        case dict():
            items = cast('dict[object, object]', loaded)
        case _:
            return _CarrierNotAnObjectOfTexts(detail=f'a JSON {type(loaded).__name__}')
    carrier: dict[str, str] = {}
    for key, value in items.items():
        match (key, value):
            case (str(), str()):
                carrier[key] = value
            case _:
                return _CarrierNotAnObjectOfTexts(detail=f'the key {key!r} has no text value')
    return carrier


def unix_nanos(moment: datetime) -> int:
    """The nanoseconds since the Unix epoch of the aware time `moment`, to
    the microsecond that an event carries. Exact: no float. Pure."""
    return ((moment - _EPOCH) // timedelta(microseconds=1)) * _NANOS_PER_MICROSECOND
