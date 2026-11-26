"""The spans of the task sends and the workflow starts, the carrier that the
core gets (of a rerun too), and the producer metrics
(`AppConfig(tracing=OtelTracing())`).

The recorder is built on an SDK tracer provider with an in-memory span
exporter and an SDK meter provider with an in-memory reader, so no global
provider is set. The broker and the core handle are faked: the tests read the
carrier from the arguments of the core call.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from opentelemetry import propagate
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import (
    CarrierT,
    Getter,
    Setter,
    TextMapPropagator,
    default_getter,
    default_setter,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    InMemoryMetricReader,
    NumberDataPoint,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NoOpTracer, SpanKind, StatusCode, get_current_span

import horsies._core as core
from horsies._catalog.producer_metrics import (
    QUEUE_ATTRIBUTE,
    RESULT_ATTRIBUTE,
    TASK_ATTRIBUTE,
    TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS,
    TASK_SEND_DURATION_NAME,
    TASK_SENT_NAME,
)
from horsies.core.brokers.result_types import BrokerEnqueueFailed
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendEnqueueFailed
from horsies.core.models.tasks import TaskError
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.telemetry.producer import OtelSendRecorder
from horsies.core.telemetry.send_telemetry import (
    NO_SEND_TELEMETRY,
    CallEnd,
    OpenSpan,
    OtelSendTelemetry,
    SendTelemetry,
    TaskSendSubject,
    TraceArgument,
    WorkflowStartSubject,
    current_context_trace,
)
from horsies.core.types.result import Err, Ok, Result, is_err, is_ok
from horsies.core.workflows.lifecycle import start_workflow, start_workflow_async

pytestmark = [pytest.mark.unit]

TASK_NAME = 'test.charge'
WORKFLOW_ID = '9f1b7a2c-0d4e-4f61-8a23-5c6d7e8f9a0b'


@dataclass(frozen=True)
class Otel:
    """An SDK tracer provider and meter provider, read in memory."""

    spans: InMemorySpanExporter
    metrics: InMemoryMetricReader
    tracer_provider: TracerProvider
    telemetry: OtelSendTelemetry

    def finished(self) -> tuple[ReadableSpan, ...]:
        return self.spans.get_finished_spans()

    def one_span(self) -> ReadableSpan:
        spans = self.finished()
        assert len(spans) == 1, spans
        return spans[0]

    def points(self, name: str) -> list[Any]:
        data = self.metrics.get_metrics_data()
        if data is None:
            return []
        return [
            point
            for resource in data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == name
            for point in metric.data.data_points
        ]


@pytest.fixture
def otel() -> Otel:
    spans = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    metrics = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metrics])
    recorder = OtelSendRecorder(
        tracer=tracer_provider.get_tracer('horsies'),
        meter=meter_provider.get_meter('horsies'),
    )
    return Otel(
        spans=spans,
        metrics=metrics,
        tracer_provider=tracer_provider,
        telemetry=OtelSendTelemetry(recorder=recorder),
    )


def _app(
    telemetry: SendTelemetry,
    broker: MagicMock,
    *,
    resend_on_transient_err: bool = False,
) -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = resend_on_transient_err
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.send_telemetry.return_value = telemetry
    app.validate_queue_name.return_value = 'default'
    app.get_broker.return_value = broker
    return app


def _broker(answer: object) -> MagicMock:
    broker = MagicMock()
    broker.enqueue.return_value = answer
    broker.enqueue_async = AsyncMock(return_value=answer)
    return broker


def _wrapper(app: MagicMock) -> Any:
    def charge(*, amount: int) -> Result[int, TaskError]:
        return Ok(amount)

    return create_task_wrapper(charge, app, TASK_NAME)


def _sent_task_id(broker: MagicMock) -> uuid.UUID:
    task_id = broker.enqueue.call_args.kwargs['task_id']
    assert isinstance(task_id, uuid.UUID)
    return task_id


def _trace_of(call_kwargs: Mapping[str, Any]) -> TraceArgument:
    trace = call_kwargs['trace']
    assert isinstance(trace, (core.NoTraceCarrier, core.TraceCarrier))
    return trace


def _traceparent(span: ReadableSpan) -> str:
    context = span.get_span_context()
    assert context is not None
    return f'00-{context.trace_id:032x}-{context.span_id:016x}-{context.trace_flags:02x}'


def _enqueue_failed(*, retryable: bool) -> Err[BrokerEnqueueFailed]:
    return Err(BrokerEnqueueFailed(
        message='db gone',
        retryable=retryable,
        exception=ConnectionError('db gone'),
    ))


class TestTaskSendSpan:
    def test_a_send_is_one_producer_span_named_after_the_task(self, otel: Otel) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        result = _wrapper(_app(otel.telemetry, broker)).send(amount=5)

        assert is_ok(result)
        span = otel.one_span()
        task_id = str(_sent_task_id(broker))
        assert span.name == f'send {TASK_NAME}'
        assert span.kind is SpanKind.PRODUCER
        assert dict(span.attributes or {}) == {
            'messaging.system': 'horsies',
            'messaging.operation.type': 'send',
            'messaging.operation.name': 'send',
            'messaging.destination.name': 'default',
            'messaging.message.id': task_id,
            'horsies.task.id': task_id,
            'horsies.task.name': TASK_NAME,
            'horsies.queue': 'default',
        }
        assert span.status.status_code is StatusCode.UNSET

    def test_the_span_is_a_child_of_the_callers_span(self, otel: Otel) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        caller = otel.tracer_provider.get_tracer('caller')
        with caller.start_as_current_span('request') as parent:
            _ = _wrapper(_app(otel.telemetry, broker)).send(amount=5)
            parent_context = parent.get_span_context()

        send = next(span for span in otel.finished() if span.name.startswith('send '))
        assert send.parent is not None
        assert send.parent.span_id == parent_context.span_id
        assert send.parent.trace_id == parent_context.trace_id

    def test_the_core_gets_the_carrier_and_the_trace_id_of_the_span(
        self,
        otel: Otel,
    ) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        _ = _wrapper(_app(otel.telemetry, broker)).send(amount=5)

        span = otel.one_span()
        trace = _trace_of(broker.enqueue.call_args.kwargs)
        assert isinstance(trace, core.TraceCarrier)
        assert trace.carrier == {'traceparent': _traceparent(span)}
        context = span.get_span_context()
        assert context is not None
        assert trace.trace_id == context.trace_id.to_bytes(16, 'big')

    def test_the_configured_propagator_writes_the_carrier(self, otel: Otel) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        previous = propagate.get_global_textmap()
        propagate.set_global_textmap(_SpanIdPropagator())
        try:
            _ = _wrapper(_app(otel.telemetry, broker)).send(amount=5)
        finally:
            propagate.set_global_textmap(previous)

        span = otel.one_span()
        context = span.get_span_context()
        assert context is not None
        trace = _trace_of(broker.enqueue.call_args.kwargs)
        assert isinstance(trace, core.TraceCarrier)
        assert trace.carrier == {_SpanIdPropagator.KEY: f'{context.span_id:016x}'}

    @pytest.mark.asyncio
    async def test_an_async_send_is_traced(self, otel: Otel) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        result = await _wrapper(_app(otel.telemetry, broker)).send_async(amount=5)

        assert is_ok(result)
        span = otel.one_span()
        assert span.name == f'send {TASK_NAME}'
        trace = _trace_of(broker.enqueue_async.call_args.kwargs)
        assert isinstance(trace, core.TraceCarrier)
        assert trace.carrier == {'traceparent': _traceparent(span)}

    def test_a_schedule_is_traced(self, otel: Otel) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        result = _wrapper(_app(otel.telemetry, broker)).schedule(30, amount=5)

        assert is_ok(result)
        span = otel.one_span()
        assert span.name == f'send {TASK_NAME}'
        assert isinstance(_trace_of(broker.enqueue.call_args.kwargs), core.TraceCarrier)

    def test_a_resend_of_a_transient_failure_stays_in_one_span(self, otel: Otel) -> None:
        broker = MagicMock()
        broker.enqueue.side_effect = [_enqueue_failed(retryable=True), Ok(uuid.uuid4())]
        app = _app(otel.telemetry, broker, resend_on_transient_err=True)
        with patch('horsies.core.task_decorator.time.sleep'):
            result = _wrapper(app).send(amount=5)

        assert is_ok(result)
        span = otel.one_span()
        traces = [_trace_of(call.kwargs) for call in broker.enqueue.call_args_list]
        assert len(traces) == 2
        assert traces[0] is traces[1]
        assert isinstance(traces[0], core.TraceCarrier)
        assert traces[0].carrier == {'traceparent': _traceparent(span)}

    def test_a_failed_send_sets_error_with_its_code(self, otel: Otel) -> None:
        broker = _broker(_enqueue_failed(retryable=False))
        result = _wrapper(_app(otel.telemetry, broker)).send(amount=5)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendEnqueueFailed)
        span = otel.one_span()
        assert span.status.status_code is StatusCode.ERROR
        assert (span.attributes or {})['error.type'] == 'ENQUEUE_FAILED'

    def test_a_send_that_raises_ends_its_span_and_raises(self, otel: Otel) -> None:
        broker = MagicMock()
        broker.enqueue.side_effect = _BrokerDefect('core stopped')

        with pytest.raises(_BrokerDefect):
            _wrapper(_app(otel.telemetry, broker)).send(amount=5)

        span = otel.one_span()
        assert span.status.status_code is StatusCode.ERROR
        assert (span.attributes or {})['error.type'] == '_BrokerDefect'
        assert otel.points(TASK_SENT_NAME) == []
        assert otel.points(TASK_SEND_DURATION_NAME) == []


class TestNoSpan:
    def test_tracing_off_makes_no_span_and_passes_no_carrier(self, otel: Otel) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        result = _wrapper(_app(NO_SEND_TELEMETRY, broker)).send(amount=5)

        assert is_ok(result)
        assert otel.finished() == ()
        assert isinstance(_trace_of(broker.enqueue.call_args.kwargs), core.NoTraceCarrier)

    def test_a_span_that_is_not_valid_passes_no_carrier(self) -> None:
        meter_provider = MeterProvider(metric_readers=[InMemoryMetricReader()])
        telemetry = OtelSendTelemetry(
            recorder=OtelSendRecorder(
                tracer=NoOpTracer(),
                meter=meter_provider.get_meter('horsies'),
            ),
        )
        broker = _broker(Ok(uuid.uuid4()))
        result = _wrapper(_app(telemetry, broker)).send(amount=5)

        assert is_ok(result)
        assert isinstance(_trace_of(broker.enqueue.call_args.kwargs), core.NoTraceCarrier)

    def test_a_recorder_that_raises_at_the_open_leaves_the_send_untraced(self) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        telemetry = OtelSendTelemetry(recorder=_RaisingRecorder(at_open=True))
        result = _wrapper(_app(telemetry, broker)).send(amount=5)

        assert is_ok(result)
        assert isinstance(_trace_of(broker.enqueue.call_args.kwargs), core.NoTraceCarrier)

    def test_a_recorder_that_raises_at_the_end_does_not_change_the_send(self) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        telemetry = OtelSendTelemetry(recorder=_RaisingRecorder(at_open=False))
        result = _wrapper(_app(telemetry, broker)).send(amount=5)

        assert is_ok(result)
        assert isinstance(_trace_of(broker.enqueue.call_args.kwargs), core.NoTraceCarrier)


class TestProducerMetrics:
    def test_a_send_counts_one_ok_and_records_its_duration(self, otel: Otel) -> None:
        broker = _broker(Ok(uuid.uuid4()))
        _ = _wrapper(_app(otel.telemetry, broker)).send(amount=5)

        sent = otel.points(TASK_SENT_NAME)
        assert len(sent) == 1
        assert isinstance(sent[0], NumberDataPoint)
        assert sent[0].value == 1
        assert dict(sent[0].attributes or {}) == {
            TASK_ATTRIBUTE: TASK_NAME,
            QUEUE_ATTRIBUTE: 'default',
            RESULT_ATTRIBUTE: 'ok',
        }
        durations = otel.points(TASK_SEND_DURATION_NAME)
        assert len(durations) == 1
        duration = durations[0]
        assert isinstance(duration, HistogramDataPoint)
        assert duration.count == 1
        assert dict(duration.attributes or {}) == {QUEUE_ATTRIBUTE: 'default'}
        assert tuple(duration.explicit_bounds) == tuple(
            float(bound) for bound in TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS
        )

    def test_a_failed_send_counts_its_error_code(self, otel: Otel) -> None:
        broker = _broker(_enqueue_failed(retryable=False))
        _ = _wrapper(_app(otel.telemetry, broker)).send(amount=5)

        sent = otel.points(TASK_SENT_NAME)
        assert len(sent) == 1
        assert (sent[0].attributes or {})[RESULT_ATTRIBUTE] == 'ENQUEUE_FAILED'


class TestWorkflowStartSpan:
    @staticmethod
    def _spec() -> Any:
        spec = MagicMock()
        spec.name = 'nightly'
        spec.nodes = []
        spec.workflow_definition = None
        spec.output = None
        return spec

    @staticmethod
    def _broker(telemetry: SendTelemetry) -> MagicMock:
        broker = MagicMock()
        broker.app.send_telemetry.return_value = telemetry
        started = MagicMock(spec=core.WorkflowStarted)
        broker._core_handle.start_workflow.return_value = started
        broker._core_handle.start_workflow_async = AsyncMock(return_value=started)
        return broker

    @staticmethod
    @contextmanager
    def _patched_request() -> Generator[MagicMock]:
        with (
            patch('horsies.core.workflows.start_tree.top_node_rows', return_value=()),
            patch(
                'horsies.core.workflows.spec_data.workflow_spec_json',
                return_value=Ok('{}'),
            ),
            patch('horsies._core.WorkflowStartRequest') as request_class,
        ):
            yield request_class

    @pytest.mark.asyncio
    async def test_a_start_is_one_producer_span_and_passes_its_carrier(
        self,
        otel: Otel,
    ) -> None:
        broker = self._broker(otel.telemetry)
        with self._patched_request() as request_class:
            result = await start_workflow_async(self._spec(), broker, WORKFLOW_ID)

            assert is_ok(result)
            span = otel.one_span()
            assert span.name == 'start nightly'
            assert span.kind is SpanKind.PRODUCER
            assert dict(span.attributes or {}) == {
                'messaging.system': 'horsies',
                'messaging.operation.type': 'send',
                'messaging.operation.name': 'start',
                'horsies.workflow.id': WORKFLOW_ID,
                'horsies.workflow.name': 'nightly',
            }
            trace = _trace_of(request_class.call_args.kwargs)
            assert isinstance(trace, core.TraceCarrier)
            assert trace.carrier == {'traceparent': _traceparent(span)}

    def test_a_blocking_start_is_traced(self, otel: Otel) -> None:
        broker = self._broker(otel.telemetry)
        with self._patched_request() as request_class:
            result = start_workflow(self._spec(), broker, WORKFLOW_ID)

            assert is_ok(result)
            span = otel.one_span()
            assert span.name == 'start nightly'
            assert isinstance(_trace_of(request_class.call_args.kwargs), core.TraceCarrier)

    @pytest.mark.asyncio
    async def test_a_refused_start_sets_error_with_its_code(self, otel: Otel) -> None:
        broker = self._broker(otel.telemetry)
        with self._patched_request():
            result = await start_workflow_async(self._spec(), broker, 'not-a-uuid')

            assert is_err(result)
            span = otel.one_span()
            assert span.status.status_code is StatusCode.ERROR
            assert (span.attributes or {})['error.type'] == 'VALIDATION_FAILED'

    @pytest.mark.asyncio
    async def test_a_broker_of_no_app_passes_no_carrier(self, otel: Otel) -> None:
        broker = self._broker(otel.telemetry)
        broker.app = None
        with self._patched_request() as request_class:
            result = await start_workflow_async(self._spec(), broker, WORKFLOW_ID)

            assert is_ok(result)
            assert otel.finished() == ()
            assert isinstance(_trace_of(request_class.call_args.kwargs), core.NoTraceCarrier)


class TestRerunCarrier:
    def test_a_rerun_gets_the_carrier_of_the_current_span_and_opens_no_span(
        self,
        otel: Otel,
    ) -> None:
        caller = otel.tracer_provider.get_tracer('caller')
        with caller.start_as_current_span('request') as parent:
            trace = current_context_trace(otel.telemetry)
            parent_context = parent.get_span_context()

        assert isinstance(trace, core.TraceCarrier)
        request = otel.one_span()
        assert request.name == 'request'
        assert trace.carrier == {'traceparent': _traceparent(request)}
        assert trace.trace_id == parent_context.trace_id.to_bytes(16, 'big')

    def test_the_configured_propagator_writes_the_rerun_carrier(self, otel: Otel) -> None:
        caller = otel.tracer_provider.get_tracer('caller')
        previous = propagate.get_global_textmap()
        propagate.set_global_textmap(_SpanIdPropagator())
        try:
            with caller.start_as_current_span('request') as parent:
                trace = current_context_trace(otel.telemetry)
                parent_context = parent.get_span_context()
        finally:
            propagate.set_global_textmap(previous)

        assert isinstance(trace, core.TraceCarrier)
        assert trace.carrier == {_SpanIdPropagator.KEY: f'{parent_context.span_id:016x}'}

    def test_a_rerun_outside_a_span_gets_no_carrier(self, otel: Otel) -> None:
        assert isinstance(current_context_trace(otel.telemetry), core.NoTraceCarrier)

    def test_with_tracing_off_a_rerun_gets_no_carrier(self, otel: Otel) -> None:
        caller = otel.tracer_provider.get_tracer('caller')
        with caller.start_as_current_span('request'):
            trace = current_context_trace(NO_SEND_TELEMETRY)

        assert isinstance(trace, core.NoTraceCarrier)

    def test_a_recorder_that_raises_gives_a_rerun_no_carrier(self, otel: Otel) -> None:
        telemetry = OtelSendTelemetry(recorder=_RaisingRecorder(at_open=True))
        caller = otel.tracer_provider.get_tracer('caller')
        with caller.start_as_current_span('request'):
            trace = current_context_trace(telemetry)

        assert isinstance(trace, core.NoTraceCarrier)


class _BrokerDefect(Exception):
    """A defect that the fake broker raises."""


class _SpanIdPropagator(TextMapPropagator):
    """A propagator that writes the span id of the current span under one
    key of its own."""

    KEY = 'x-test-span-id'

    def extract(
        self,
        carrier: CarrierT,
        context: Context | None = None,
        getter: Getter[CarrierT] = default_getter,
    ) -> Context:
        return context if context is not None else Context()

    def inject(
        self,
        carrier: CarrierT,
        context: Context | None = None,
        setter: Setter[CarrierT] = default_setter,
    ) -> None:
        span_context = get_current_span(context).get_span_context()
        setter.set(carrier, self.KEY, f'{span_context.span_id:016x}')

    @property
    def fields(self) -> set[str]:
        return {self.KEY}


@dataclass(frozen=True)
class _RaisingSpan:
    """A span whose end raises, as a user's span processor can."""

    trace: TraceArgument

    def end(self, call_end: CallEnd) -> None:
        raise RuntimeError(f'span processor failed at {call_end!r}')


@dataclass(frozen=True)
class _RaisingRecorder:
    """A recorder that raises when it opens a span (`at_open`), or whose
    spans raise when they end. Its rerun carrier raises."""

    at_open: bool

    def _open(self) -> OpenSpan:
        match self.at_open:
            case True:
                raise RuntimeError('tracer failed')
            case False:
                return _RaisingSpan(trace=core.NoTraceCarrier())

    def start_task_send(self, subject: TaskSendSubject) -> OpenSpan:
        return self._open()

    def start_workflow_start(self, subject: WorkflowStartSubject) -> OpenSpan:
        return self._open()

    def current_trace(self) -> TraceArgument:
        raise RuntimeError('propagator failed')
