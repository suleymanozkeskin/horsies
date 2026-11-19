"""The span of each attempt that the worker ended, made in the handler child.

The events come as the wire text of an ``events`` frame, as the core writes
them, through the dispatch of the control thread (``EventDispatch``). The
spans go to the in-memory exporter of the OpenTelemetry SDK through a
recorder of a provider of the test; the global tracer provider is not set.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import logging
import subprocess
import sys
import textwrap
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final, cast

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import SecretStr

from horsies._catalog import events
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import Execution
from horsies.core.models.tracing import (
    ChildOfSend,
    LinkedToSend,
    NoTracing,
    OtelTracing,
    WorkerEndedSpans,
)
from horsies.core.telemetry import attempt_otel, ended_attempt_otel
from horsies.core.telemetry.attempt_span import AttemptSubject, NothingToFlush, SpansFlushed
from horsies.core.telemetry.ended_attempt_otel import OtelEndedAttemptRecorder
from horsies.core.telemetry.ended_attempt_span import (
    NO_ENDED_ATTEMPT_SPANS,
    EndedAttemptSpans,
    EndedSpanMade,
    EndedSpanRaised,
    EndedSpansOff,
    NoEndedAttemptSpans,
    NotEndedByWorker,
    OtelEndedAttemptSpans,
    ended_attempt_spans_of,
    flush_ended_attempt_spans_at_stop,
    make_ended_attempt_span,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_frames import NoTraceCarrier, StoredTraceCarrier
from horsies.core.worker.core_run import WorkerLaunch, worker_manifest_json
from horsies.core.worker.handler_child import EventDispatch, decode_event_text
from horsies.core.worker.run_return import NotInWorkflow
from horsies.core.worker_manifest import worker_ended_spans_manifest

pytestmark = [pytest.mark.unit]

TASK_ID: Final = uuid.UUID('0192f6a1-7b3c-7d4e-8f00-0123456789ab')
TASK_NAME: Final = 'send_email'
QUEUE_NAME: Final = 'mail'
WORKER_ID: Final = '7c9e6679-7425-40de-944b-e07fc1f90ae7'
ATTEMPT: Final = 2
PLACED_NANOS: Final = 1_700_000_000_000_123_456
"""When the task reached its child, on the wire (nanoseconds)."""
ENDED_NANOS: Final = 1_700_000_030_500_654_321
"""When the end reached the worker: the `at` of the event (nanoseconds)."""
RUN_NANOS: Final = 30_500_000_000
SPAN_NAME: Final = f'run {TASK_NAME}'


def _to_microsecond(nanos: int) -> int:
    """`nanos` cut to the microsecond that a decoded event keeps."""
    return nanos // 1_000 * 1_000


@dataclass(frozen=True, slots=True)
class _Traced:
    """The exporter, the tracer of the sends, and the spans of a test."""

    exporter: InMemorySpanExporter
    provider: TracerProvider

    def spans(self) -> tuple[ReadableSpan, ...]:
        return self.exporter.get_finished_spans()

    def named(self, name: str) -> list[ReadableSpan]:
        return [span for span in self.spans() if span.name == name]

    def recorder(self, parentage: ChildOfSend | LinkedToSend) -> OtelEndedAttemptSpans:
        return OtelEndedAttemptSpans(
            recorder=OtelEndedAttemptRecorder(
                tracer=self.provider.get_tracer('horsies'),
                parentage=parentage,
                provider_flush=lambda _timeout_millis: SpansFlushed(),
            ),
        )

    def send_carrier(self) -> tuple[str, ReadableSpan]:
        """The carrier text that the core stores for a send in a span, as
        its encoder writes it, and that span."""
        send = self.provider.get_tracer('test').start_span('send', kind=SpanKind.PRODUCER)
        carrier: dict[str, str] = {}
        TraceContextTextMapPropagator().inject(carrier, context=trace.set_span_in_context(send))
        send.end()
        text = json.dumps(dict(sorted(carrier.items())), separators=(',', ':'))
        return text, self.named('send')[0]


@pytest.fixture
def traced() -> Iterator[_Traced]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield _Traced(exporter=exporter, provider=provider)
    provider.shutdown()


def _ended_text(
    *,
    reason: str,
    code: str,
    execution: str,
    child_kind: str,
    trace_wire: list[object],
    workflow: str = 'NOT_IN_WORKFLOW',
) -> str:
    """The wire text of the `AttemptEnded` of an attempt that the worker
    ended, as the core writes it in an `events` frame."""
    wire: list[object] = [
        'AttemptEnded',
        ENDED_NANOS,
        str(TASK_ID),
        TASK_NAME,
        QUEUE_NAME,
        execution,
        ATTEMPT,
        [
            'DecidedByWorker',
            reason,
            code,
            trace_wire,
            PLACED_NANOS,
            child_kind,
            WORKER_ID,
            workflow,
        ],
        RUN_NANOS,
    ]
    return json.dumps(wire, separators=(',', ':'))


def _event(text: str) -> events.AnyEvent:
    match decode_event_text(text):
        case Ok(event):
            return event
        case refused:
            raise AssertionError(f'the wire text decodes, got {refused}')


@dataclass(frozen=True, slots=True)
class _WorkerEnd:
    """One way the worker ends an attempt: the reason and its code, the
    class of the task and the kind of its child."""

    reason: str
    code: str
    execution: str
    child_kind: str


TIMEOUT_KILL: Final = _WorkerEnd('TIMED_OUT', 'TASK_TIMEOUT', 'ISOLATED', 'ISOLATED')
CRASH: Final = _WorkerEnd('CHILD_LOST', 'WORKER_CRASHED', 'SHARED_THREAD', 'SHARED_THREAD')
BLOCKED_LOOP: Final = _WorkerEnd(
    'TASK_BLOCKED_LOOP', 'TASK_BLOCKED_LOOP', 'SHARED_ASYNC', 'SHARED_ASYNC'
)
NEIGHBOUR_BLOCKED_LOOP: Final = _WorkerEnd(
    'NEIGHBOUR_BLOCKED_LOOP', 'NEIGHBOUR_BLOCKED_LOOP', 'SHARED_ASYNC', 'SHARED_ASYNC'
)
ROUTED_TIMEOUT_KILL: Final = _WorkerEnd('TIMED_OUT', 'TASK_TIMEOUT', 'SHARED_THREAD', 'ISOLATED')
"""A task of a shared class that ran on an isolated child (the route rule)."""


def _expected_attributes(end: _WorkerEnd) -> dict[str, object]:
    return {
        'messaging.system': 'horsies',
        'messaging.operation.type': 'process',
        'messaging.operation.name': 'run',
        'messaging.message.id': str(TASK_ID),
        'messaging.destination.name': QUEUE_NAME,
        'horsies.task.id': str(TASK_ID),
        'horsies.task.name': TASK_NAME,
        'horsies.task.attempt': ATTEMPT,
        'horsies.task.execution': end.child_kind,
        'horsies.queue': QUEUE_NAME,
        'horsies.worker.id': WORKER_ID,
        'horsies.span.source': 'worker',
        'error.type': end.code,
    }


class TestTheSpanOfAnAttemptThatTheWorkerEnded:
    @pytest.mark.parametrize(
        'end',
        [TIMEOUT_KILL, CRASH, BLOCKED_LOOP, NEIGHBOUR_BLOCKED_LOOP, ROUTED_TIMEOUT_KILL],
        ids=['timeout_kill', 'crash', 'blocked_loop', 'neighbour_blocked_loop', 'routed'],
    )
    def test_each_end_makes_one_span_under_the_send_with_its_times_status_and_attributes(
        self,
        traced: _Traced,
        end: _WorkerEnd,
    ) -> None:
        carrier, send = traced.send_carrier()
        text = _ended_text(
            reason=end.reason,
            code=end.code,
            execution=end.execution,
            child_kind=end.child_kind,
            trace_wire=['Carrier', carrier],
        )

        EventDispatch((), traced.recorder(ChildOfSend())).dispatch([text])

        made = traced.named(SPAN_NAME)
        assert len(made) == 1, [span.name for span in traced.spans()]
        span = made[0]
        assert send.context is not None and span.context is not None
        assert span.parent is not None
        assert span.parent.span_id == send.context.span_id
        assert span.context.trace_id == send.context.trace_id
        assert span.kind == SpanKind.CONSUMER
        assert span.start_time == _to_microsecond(PLACED_NANOS)
        assert span.end_time == _to_microsecond(ENDED_NANOS)
        assert span.status.status_code == StatusCode.ERROR
        assert dict(span.attributes or {}) == _expected_attributes(end)
        assert list(span.links) == []

    def test_the_span_has_the_attributes_of_the_span_of_a_child(self, traced: _Traced) -> None:
        carrier, _send = traced.send_carrier()
        EventDispatch((), traced.recorder(ChildOfSend())).dispatch(
            [
                _ended_text(
                    reason=TIMEOUT_KILL.reason,
                    code=TIMEOUT_KILL.code,
                    execution=TIMEOUT_KILL.execution,
                    child_kind=TIMEOUT_KILL.child_kind,
                    trace_wire=['Carrier', carrier],
                ),
            ],
        )
        child_made = attempt_otel.attempt_attributes(
            AttemptSubject(
                task_id=str(TASK_ID),
                task_name=TASK_NAME,
                queue_name=QUEUE_NAME,
                attempt=ATTEMPT,
                worker_id=WORKER_ID,
                execution=Execution.ISOLATED,
                workflow=NotInWorkflow(),
                trace=NoTraceCarrier(),
            ),
        )

        span = traced.named(SPAN_NAME)[0]
        worker_made = dict(span.attributes or {})
        assert set(worker_made) - {'horsies.span.source', 'error.type'} == set(child_made)
        assert {key: worker_made[key] for key in child_made} == child_made

    def test_a_task_with_no_carrier_gets_a_root_span(self, traced: _Traced) -> None:
        EventDispatch((), traced.recorder(ChildOfSend())).dispatch(
            [
                _ended_text(
                    reason=CRASH.reason,
                    code=CRASH.code,
                    execution=CRASH.execution,
                    child_kind=CRASH.child_kind,
                    trace_wire=['NoCarrier'],
                ),
            ],
        )

        made = traced.named(SPAN_NAME)
        assert len(made) == 1
        assert made[0].parent is None

    def test_linked_to_send_starts_a_trace_with_a_link_to_the_send(
        self,
        traced: _Traced,
    ) -> None:
        carrier, send = traced.send_carrier()
        EventDispatch((), traced.recorder(LinkedToSend())).dispatch(
            [
                _ended_text(
                    reason=TIMEOUT_KILL.reason,
                    code=TIMEOUT_KILL.code,
                    execution=TIMEOUT_KILL.execution,
                    child_kind=TIMEOUT_KILL.child_kind,
                    trace_wire=['Carrier', carrier],
                ),
            ],
        )

        span = traced.named(SPAN_NAME)[0]
        assert send.context is not None and span.context is not None
        assert span.parent is None
        assert span.context.trace_id != send.context.trace_id
        assert [link.context.span_id for link in span.links] == [send.context.span_id]

    def test_a_workflow_task_has_no_workflow_attribute_and_stays_in_the_trace(
        self,
        traced: _Traced,
    ) -> None:
        carrier, send = traced.send_carrier()
        EventDispatch((), traced.recorder(ChildOfSend())).dispatch(
            [
                _ended_text(
                    reason=TIMEOUT_KILL.reason,
                    code=TIMEOUT_KILL.code,
                    execution=TIMEOUT_KILL.execution,
                    child_kind=TIMEOUT_KILL.child_kind,
                    trace_wire=['Carrier', carrier],
                    workflow='WORKFLOW_NOT_KNOWN',
                ),
            ],
        )

        span = traced.named(SPAN_NAME)[0]
        assert send.context is not None and span.context is not None
        assert not [key for key in (span.attributes or {}) if key.startswith('horsies.workflow')]
        assert span.context.trace_id == send.context.trace_id

    def test_an_end_that_the_child_reported_or_not_known_and_other_events_make_no_span(
        self,
        traced: _Traced,
    ) -> None:
        spans = traced.recorder(ChildOfSend())
        reported = _event(
            json.dumps(
                [
                    'AttemptEnded', ENDED_NANOS, str(TASK_ID), TASK_NAME, QUEUE_NAME,
                    'ISOLATED', 1, ['ReportedByChild', ['Completed'], ['NotReported']],
                    RUN_NANOS,
                ],
            ),
        )
        not_known = _event(
            json.dumps(
                [
                    'AttemptEnded', ENDED_NANOS, str(TASK_ID), TASK_NAME, QUEUE_NAME,
                    'ISOLATED', 1, ['NotKnown', 4242], RUN_NANOS,
                ],
            ),
        )
        recovered = _event(
            json.dumps(
                [
                    'TaskRecovered', ENDED_NANOS, str(TASK_ID), TASK_NAME, QUEUE_NAME,
                    'RUNNING_FAILED',
                ],
            ),
        )

        steps = [make_ended_attempt_span(spans, event) for event in (reported, not_known, recovered)]

        assert steps == [NotEndedByWorker(), NotEndedByWorker(), NotEndedByWorker()]
        assert traced.spans() == ()


class TestNoSpans:
    def test_no_spans_and_no_tracing_choose_no_span(self) -> None:
        assert (
            ended_attempt_spans_of(OtelTracing(worker_ended_spans=WorkerEndedSpans.NO_SPANS))
            == NO_ENDED_ATTEMPT_SPANS
        )
        assert ended_attempt_spans_of(NoTracing()) == NO_ENDED_ATTEMPT_SPANS

    def test_make_spans_is_the_default_of_otel_tracing(self) -> None:
        assert OtelTracing().worker_ended_spans == WorkerEndedSpans.MAKE_SPANS
        assert isinstance(ended_attempt_spans_of(OtelTracing()), OtelEndedAttemptSpans)
        assert (
            OtelTracing.model_validate({'worker_ended_spans': 'NO_SPANS'}).worker_ended_spans
            == WorkerEndedSpans.NO_SPANS
        )

    def test_no_spans_makes_no_span_of_an_attempt_that_the_worker_ended(
        self,
        traced: _Traced,
    ) -> None:
        carrier, _send = traced.send_carrier()
        event = _event(
            _ended_text(
                reason=TIMEOUT_KILL.reason,
                code=TIMEOUT_KILL.code,
                execution=TIMEOUT_KILL.execution,
                child_kind=TIMEOUT_KILL.child_kind,
                trace_wire=['Carrier', carrier],
            ),
        )

        step = make_ended_attempt_span(NO_ENDED_ATTEMPT_SPANS, event)

        assert step == EndedSpansOff()
        assert traced.named(SPAN_NAME) == []

    @pytest.mark.parametrize(
        ('tracing', 'expected'),
        [
            (OtelTracing(), 'MAKE_SPANS'),
            (OtelTracing(worker_ended_spans=WorkerEndedSpans.NO_SPANS), 'NO_SPANS'),
            (NoTracing(), 'NO_SPANS'),
        ],
    )
    def test_the_worker_manifest_carries_the_switch(
        self,
        tracing: OtelTracing | NoTracing,
        expected: str,
    ) -> None:
        assert worker_ended_spans_manifest(tracing) == expected

    @pytest.mark.parametrize(
        ('tracing', 'expected'),
        [(OtelTracing(), 'MAKE_SPANS'), (NoTracing(), 'NO_SPANS')],
    )
    def test_the_manifest_of_an_app_carries_the_switch_of_its_tracing(
        self,
        tracing: OtelTracing | NoTracing,
        expected: str,
    ) -> None:
        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url=SecretStr('postgresql://user:pass@localhost/db'),
                ),
                tracing=tracing,
            ),
        )
        launch = WorkerLaunch(
            app_locator='app.main:app',
            imports=(),
            sys_path_roots=(),
            profile='default',
            loglevel='INFO',
        )

        match worker_manifest_json(app, launch, logging.getLogger('test')):
            case Ok(text):
                document = cast('dict[str, Any]', json.loads(text))
            case Err(refused):
                pytest.fail(f'the manifest builds, got {refused}')

        assert document['worker_ended_spans'] == expected
        assert document['handlers'] == {'kind': 'NO_HANDLERS'}

    def test_without_otel_nothing_imports_opentelemetry(self) -> None:
        wire = _ended_text(
            reason=TIMEOUT_KILL.reason,
            code=TIMEOUT_KILL.code,
            execution=TIMEOUT_KILL.execution,
            child_kind=TIMEOUT_KILL.child_kind,
            trace_wire=['NoCarrier'],
        )
        code = textwrap.dedent(
            f'''
            import sys
            from horsies.core.models.tracing import NoTracing, OtelTracing, WorkerEndedSpans
            from horsies.core.telemetry.ended_attempt_span import (
                ended_attempt_spans_of,
                flush_ended_attempt_spans_at_stop,
                make_ended_attempt_span,
            )
            from horsies.core.worker.handler_child import decode_event_text

            event = decode_event_text({wire!r}).unwrap()
            for tracing in (NoTracing(), OtelTracing(worker_ended_spans=WorkerEndedSpans.NO_SPANS)):
                spans = ended_attempt_spans_of(tracing)
                print(make_ended_attempt_span(spans, event))
                print(flush_ended_attempt_spans_at_stop(spans))
            print(sorted(m for m in sys.modules if m.split('.')[0] == 'opentelemetry'))
            ''',
        )
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines() == [
            'EndedSpansOff()',
            'NothingToFlush()',
            'EndedSpansOff()',
            'NothingToFlush()',
            '[]',
        ]


class _RaisingRecorder:
    """A recorder whose span raises, as a broken SDK would."""

    def __init__(self) -> None:
        self.calls = 0

    def make(
        self,
        ended: events.AttemptEnded,
        decided: events.AttemptEndDecidedByWorker,
    ) -> None:
        del ended, decided
        self.calls += 1
        raise RuntimeError('the exporter is gone')

    def flush(self, timeout_millis: int) -> SpansFlushed:
        del timeout_millis
        return SpansFlushed()


class TestARecorderThatRaises:
    def test_it_is_logged_once_and_the_dispatch_goes_on(
        self,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from horsies.core.telemetry import ended_attempt_span

        monkeypatch.setattr(
            ended_attempt_span,
            '_RECORDER_FAILURES',
            ended_attempt_span._RecorderFailureLog(),
        )
        recorder = _RaisingRecorder()
        spans: EndedAttemptSpans = OtelEndedAttemptSpans(recorder=recorder)
        event = _event(
            _ended_text(
                reason=CRASH.reason,
                code=CRASH.code,
                execution=CRASH.execution,
                child_kind=CRASH.child_kind,
                trace_wire=['NoCarrier'],
            ),
        )

        with caplog.at_level(logging.WARNING, logger='horsies.telemetry'):
            steps = [make_ended_attempt_span(spans, event) for _ in range(3)]

        assert recorder.calls == 3
        assert all(isinstance(step, EndedSpanRaised) for step in steps)
        warnings = [r for r in caplog.records if r.name == 'horsies.telemetry']
        assert len(warnings) == 1
        assert 'the exporter is gone' in warnings[0].getMessage()


class TestTheFlushAtTheStop:
    def test_spans_off_have_nothing_to_flush(self) -> None:
        assert flush_ended_attempt_spans_at_stop(NoEndedAttemptSpans()) == NothingToFlush()

    def test_the_recorder_flushes_its_provider(self, traced: _Traced) -> None:
        assert flush_ended_attempt_spans_at_stop(traced.recorder(ChildOfSend())) == SpansFlushed()


class TestTheFactsOfTheSpan:
    def test_a_time_converts_to_whole_nanoseconds_with_no_float(self) -> None:
        moment = datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo=timezone.utc)
        assert ended_attempt_otel.unix_nanos(moment) == 1_700_000_000_123_456_000

    def test_a_stored_carrier_reads_as_the_carrier_of_a_run(self) -> None:
        carrier = '{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}'
        assert ended_attempt_otel.run_trace_of_event(
            TASK_ID,
            events.RunTraceCarrier(text=carrier),
        ) == StoredTraceCarrier(
            carrier={'traceparent': '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'},
        )
        assert ended_attempt_otel.run_trace_of_event(
            TASK_ID, events.RunTraceNoCarrier()
        ) == NoTraceCarrier()

    @pytest.mark.parametrize('text', ['not json', '[1]', '{"traceparent": 1}'])
    def test_a_carrier_that_is_not_an_object_of_texts_gives_no_parent_and_is_logged(
        self,
        text: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.ERROR, logger='horsies.telemetry'):
            read = ended_attempt_otel.run_trace_of_event(
                TASK_ID,
                events.RunTraceCarrier(text=text),
            )

        assert read == NoTraceCarrier()
        assert any(str(TASK_ID) in record.getMessage() for record in caplog.records)

    def test_a_made_span_is_reported_as_made(self, traced: _Traced) -> None:
        event = _event(
            _ended_text(
                reason=TIMEOUT_KILL.reason,
                code=TIMEOUT_KILL.code,
                execution=TIMEOUT_KILL.execution,
                child_kind=TIMEOUT_KILL.child_kind,
                trace_wire=['NoCarrier'],
            ),
        )
        assert make_ended_attempt_span(traced.recorder(ChildOfSend()), event) == EndedSpanMade()
