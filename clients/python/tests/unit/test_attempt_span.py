"""The span and the log context of each attempt in a worker child.

Real registered tasks run at the three body places, from version-3 run
frames as the core decodes them: an isolated child
(``child_main.run_child_task``), a seat of a shared async child
(``shared_runner.run_seat_task``) and a seat of a shared thread child
(``thread_runner.run_thread_seat_task``). The database steps around the body
are replaced. The spans go to the in-memory exporter of the OpenTelemetry
SDK, through a recorder that each test installs (``use_attempt_spans``);
the global tracer provider is not set.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
import textwrap
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Final, Protocol
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode, Tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import SecretStr

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.logging import (
    NoRunningTask,
    NoSpanIds,
    RunningTask,
    SpanIds,
    TaskContextFilter,
    running_task,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import Execution, TaskError
from horsies.core.models.tracing import ChildOfSend, LinkedToSend
from horsies.core.telemetry import attempt_otel, attempt_span
from horsies.core.telemetry.attempt_otel import OtelAttemptRecorder
from horsies.core.telemetry.attempt_span import (
    NO_ATTEMPT_SPANS,
    AttemptEnd,
    AttemptSubject,
    BodyEndOrder,
    BodyFirst,
    DeadlineFirst,
    FlushNotConfirmed,
    FlushRaised,
    FlushTimedOut,
    NothingToFlush,
    OperatorCancelFirst,
    OtelAttemptSpans,
    ProviderFlush,
    ProviderHasNoFlush,
    RunCancelledByOperator,
    RunTimedOut,
    ScopeRaised,
    SpansFlushed,
    bounded_flush,
    flush_attempt_spans_at_stop,
    run_in_attempt_span,
    run_in_attempt_span_async,
    use_attempt_spans,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import child_main, child_runner, shared_runner, thread_runner
from horsies.core.worker.child_frames import (
    NoTraceCarrier,
    RunTrace,
    StoredTraceCarrier,
    ThreadSeatWork,
)
from horsies.core.worker.child_runner import StartAllowed
from horsies.core.worker.run_cancel import RunCancel
from horsies.core.worker.run_return import (
    BodyInterrupted,
    EncodedResult,
    InWorkflow,
    NotInWorkflow,
    NothingRaised,
    RaisedInRun,
    ResultEnvelope,
    ResultFailed,
    ResultSucceeded,
    RunReturn,
    RunWorkflow,
    SetRunning,
    StartedRunning,
)
from horsies.core.worker.task_loop import ExecutorShutDown, TaskLoop
from tests.fixtures.core_frames import (
    NO_CARRIER,
    QUEUE_NAME,
    async_work,
    carrier_trace,
    init_frame,
    run_frame,
    shared_run_frame,
    thread_work,
)
from tests.fixtures.run_return import STARTED, STARTED_AT

pytestmark = [pytest.mark.unit]

TASK_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000a15')
WORKFLOW_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-00000000f015')
ROOT_WORKFLOW_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-00000000f001')
WORKER_ID: Final = 'worker-1'
BODY_LOGGER: Final = 'horsies.test_attempt_span'
SCENARIO_TIMEOUT_S: Final = 5.0
"""The longest wait of a test for a thread of the code under test."""

APP = Horsies(
    AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://u:p@localhost/unused'),
        ),
    ),
)


class _BodyTracer:
    """The tracer that the task bodies use for a span of their own: the
    tracer of the provider of the running test."""

    def __init__(self) -> None:
        self.tracer: Tracer = trace.NoOpTracer()


_BODY = _BodyTracer()


def _body_work() -> None:
    """What each body does: one span of its own, and one log record in it."""
    with _BODY.tracer.start_as_current_span('user work'):
        logging.getLogger(BODY_LOGGER).warning('inside the body')


@APP.task(task_name='isolated_work')
def isolated_work() -> Result[int, TaskError]:
    _body_work()
    return Ok(1)


@APP.task(task_name='isolated_async_work')
async def isolated_async_work() -> Result[int, TaskError]:
    await asyncio.sleep(0)
    _body_work()
    return Ok(1)


@APP.task(task_name='seat_work', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def seat_work() -> Result[int, TaskError]:
    await asyncio.sleep(0)
    _body_work()
    return Ok(1)


@APP.task(task_name='thread_work', execution=Execution.SHARED_THREAD, timeout_ms=10_000)
def thread_seat_work() -> Result[int, TaskError]:
    _body_work()
    return Ok(1)


@APP.task(task_name='returns_error')
def returns_error() -> Result[int, TaskError]:
    return Err(TaskError(error_code='LIMIT_REACHED', message='over the limit'))


@APP.task(task_name='raises')
def raises() -> Result[int, TaskError]:
    raise ValueError('bad input')


class _Records(logging.Handler):
    """Keeps the records of the body logger, with the task context filter of
    the horsies handler."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []
        self.addFilter(TaskContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@dataclass(frozen=True, slots=True)
class _Traced:
    """The exporter of a test, its tracer, and the body records."""

    exporter: InMemorySpanExporter
    tracer: Tracer
    records: _Records

    def spans(self) -> tuple[ReadableSpan, ...]:
        return self.exporter.get_finished_spans()

    def one(self, name: str) -> ReadableSpan:
        named = [span for span in self.spans() if span.name == name]
        assert len(named) == 1, [span.name for span in self.spans()]
        return named[0]


def _install(provider: TracerProvider, parentage: ChildOfSend | LinkedToSend) -> None:
    use_attempt_spans(
        OtelAttemptSpans(
            recorder=OtelAttemptRecorder(
                tracer=provider.get_tracer('horsies'),
                parentage=parentage,
                provider_flush=lambda timeout_millis: SpansFlushed(),
            ),
        ),
    )


@pytest.fixture
def traced() -> Iterator[_Traced]:
    """Attempt spans with `ChildOfSend` to an in-memory exporter."""
    yield from _traced(ChildOfSend())


@pytest.fixture
def traced_linked() -> Iterator[_Traced]:
    """Attempt spans with `LinkedToSend` to an in-memory exporter."""
    yield from _traced(LinkedToSend())


def _traced(parentage: ChildOfSend | LinkedToSend) -> Iterator[_Traced]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    records = _Records()
    body_logger = logging.getLogger(BODY_LOGGER)
    body_logger.addHandler(records)
    _install(provider, parentage)
    _BODY.tracer = provider.get_tracer('user')
    yield _Traced(exporter=exporter, tracer=provider.get_tracer('test'), records=records)
    use_attempt_spans(NO_ATTEMPT_SPANS)
    _BODY.tracer = trace.NoOpTracer()
    body_logger.removeHandler(records)
    provider.shutdown()


@pytest.fixture
def task_loop() -> Iterator[TaskLoop]:
    """The loop of an isolated child whose app has an async task."""
    loop = TaskLoop()
    loop.open().unwrap()
    yield loop
    _ = loop.close()


def _send_carrier(traced: _Traced) -> tuple[dict[str, object], ReadableSpan]:
    """The trace of a run frame whose task a span sent, and that span."""
    send = traced.tracer.start_span('send', kind=SpanKind.PRODUCER)
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier, context=trace.set_span_in_context(send))
    send.end()
    return carrier_trace(carrier), traced.one('send')


def _run_isolated(
    task_name: str,
    trace_meta: dict[str, object],
    task_loop: TaskLoop,
    started: StartedRunning = STARTED,
) -> RunReturn:
    with (
        patch.object(
            child_runner,
            '_confirm_ownership_and_set_running',
            MagicMock(return_value=started),
        ),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)),
    ):
        return child_main.run_child_task(
            run_frame(
                task_id=TASK_ID,
                task_name=task_name,
                attempt=2,
                trace=trace_meta,
            ),
            init_frame(),
            task_loop,
            RunCancel(),
        )


def _body_first() -> BodyEndOrder:
    """The order of a run whose steps end before its deadline."""
    return BodyFirst()


def _deadline_first() -> BodyEndOrder:
    """The order of a run whose deadline fired in its child first."""
    return DeadlineFirst()


def _run_seat(
    task_name: str,
    trace_meta: dict[str, object],
    task_loop: TaskLoop,
    started: StartedRunning = STARTED,
) -> RunReturn:
    return _run_seat_in_order(task_name, trace_meta, task_loop, started, _body_first)


def _run_seat_in_order(
    task_name: str,
    trace_meta: dict[str, object],
    task_loop: TaskLoop,
    started: StartedRunning,
    body_end: Callable[[], BodyEndOrder],
) -> RunReturn:
    _ = task_loop
    frame = shared_run_frame(
        dispatch=1,
        task_id=TASK_ID,
        task_name=task_name,
        work=async_work(10_000, 1_000),
        attempt=2,
        trace=trace_meta,
    )
    with (
        patch.object(shared_runner, 'confirm_until_not_resumed', AsyncMock(return_value=started)),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)),
    ):
        return asyncio.run(shared_runner.run_seat_task(frame, body_end, RunCancel()))


def _thread_seat_work(frame: core.SharedRunFrame) -> ThreadSeatWork:
    match frame.work:
        case core.ThreadTaskWork() | core.StoredOptionsRefused() as work:
            return work
        case core.AsyncTaskWork():
            raise AssertionError(f'run {frame.dispatch} is not a thread seat run')


def _run_thread_seat(
    task_name: str,
    trace_meta: dict[str, object],
    task_loop: TaskLoop,
    started: StartedRunning = STARTED,
) -> RunReturn:
    """Run on a thread of its own, as a seat thread does."""
    return _run_thread_seat_in_order(task_name, trace_meta, task_loop, started, _body_first)


def _run_thread_seat_in_order(
    task_name: str,
    trace_meta: dict[str, object],
    task_loop: TaskLoop,
    started: StartedRunning,
    body_end: Callable[[], BodyEndOrder],
) -> RunReturn:
    """Run on a thread of its own, as a seat thread does, with the order
    that `body_end` gives."""
    _ = task_loop
    frame = shared_run_frame(
        dispatch=1,
        task_id=TASK_ID,
        task_name=task_name,
        work=thread_work(10_000),
        attempt=2,
        trace=trace_meta,
    )
    returned: list[RunReturn] = []

    def seat() -> None:
        match thread_runner.run_thread_seat_task(
            frame, _thread_seat_work(frame), StartAllowed, body_end, RunCancel(),
        ):
            case SetRunning() as ran:
                returned.append(ran)
            case other:
                raise AssertionError(f'the seat run did not set RUNNING: {other!r}')

    with (
        patch.object(thread_runner, '_confirm_until_not_resumed', MagicMock(return_value=started)),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)),
    ):
        seat_thread = threading.Thread(target=seat, name='horsies-test-seat')
        seat_thread.start()
        seat_thread.join(SCENARIO_TIMEOUT_S)
    assert not seat_thread.is_alive()
    assert len(returned) == 1
    return returned[0]


class _PlaceRun(Protocol):
    """Runs a task at one body place, from the trace of its run frame, with
    the start ``started``."""

    def __call__(
        self,
        task_name: str,
        trace_meta: dict[str, object],
        task_loop: TaskLoop,
        started: StartedRunning = ...,
        /,
    ) -> RunReturn: ...


@dataclass(frozen=True, slots=True)
class _Place:
    """A body place, the task it runs, and the class of its child."""

    task_name: str
    run: _PlaceRun
    execution: Execution


PLACES: Final = (
    pytest.param(_Place('isolated_work', _run_isolated, Execution.ISOLATED), id='isolated-def'),
    pytest.param(
        _Place('isolated_async_work', _run_isolated, Execution.ISOLATED),
        id='isolated-async-def',
    ),
    pytest.param(_Place('seat_work', _run_seat, Execution.SHARED_ASYNC), id='shared-async'),
    pytest.param(
        _Place('thread_work', _run_thread_seat, Execution.SHARED_THREAD),
        id='shared-thread',
    ),
)


def _hex_trace_id(span: ReadableSpan) -> str:
    assert span.context is not None
    return format(span.context.trace_id, '032x')


def _hex_span_id(span: ReadableSpan) -> str:
    assert span.context is not None
    return format(span.context.span_id, '016x')


class TestTheSpanOfEachPlace:
    @pytest.mark.parametrize('place', PLACES)
    def test_the_attempt_span_is_a_consumer_child_of_the_send_span(
        self,
        place: _Place,
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        trace_meta, send = _send_carrier(traced)

        returned = place.run(place.task_name, trace_meta, task_loop)

        assert isinstance(returned, SetRunning), returned
        attempt = traced.one(f'run {place.task_name}')
        assert attempt.kind == SpanKind.CONSUMER
        assert attempt.parent is not None and send.context is not None
        assert attempt.parent.span_id == send.context.span_id
        assert _hex_trace_id(attempt) == _hex_trace_id(send)
        assert attempt.links == ()
        assert attempt.status.status_code == StatusCode.UNSET
        assert dict(attempt.attributes or {}) == {
            'messaging.system': 'horsies',
            'messaging.operation.type': 'process',
            'messaging.operation.name': 'run',
            'messaging.message.id': str(TASK_ID),
            'messaging.destination.name': QUEUE_NAME,
            'horsies.task.id': str(TASK_ID),
            'horsies.task.name': place.task_name,
            'horsies.task.attempt': 2,
            'horsies.task.execution': place.execution.value,
            'horsies.queue': QUEUE_NAME,
            'horsies.worker.id': WORKER_ID,
        }

    @pytest.mark.parametrize('place', PLACES)
    def test_the_attempt_span_of_a_workflow_task_names_its_workflow_node_and_root(
        self,
        place: _Place,
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        trace_meta, _ = _send_carrier(traced)

        started = StartedRunning(
            STARTED_AT,
            InWorkflow(workflow_id=WORKFLOW_ID, node_index=4, root_workflow_id=ROOT_WORKFLOW_ID),
        )

        returned = place.run(place.task_name, trace_meta, task_loop, started)

        assert isinstance(returned, SetRunning), returned
        attributes = dict(traced.one(f'run {place.task_name}').attributes or {})
        assert attributes['horsies.workflow.id'] == str(WORKFLOW_ID)
        assert attributes['horsies.workflow.node'] == 4
        assert attributes['horsies.workflow.root_id'] == str(ROOT_WORKFLOW_ID)
        assert attributes['horsies.task.attempt'] == 2
        assert attributes['horsies.queue'] == QUEUE_NAME

    @pytest.mark.parametrize('place', PLACES)
    def test_a_span_of_the_body_nests_under_the_attempt_span(
        self,
        place: _Place,
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        trace_meta, _ = _send_carrier(traced)

        _ = place.run(place.task_name, trace_meta, task_loop)

        attempt = traced.one(f'run {place.task_name}')
        user = traced.one('user work')
        assert user.parent is not None and attempt.context is not None
        assert user.parent.span_id == attempt.context.span_id
        assert _hex_trace_id(user) == _hex_trace_id(attempt)

    @pytest.mark.parametrize('place', PLACES)
    def test_a_record_of_the_body_names_its_task_its_attempt_and_the_attempt_span(
        self,
        place: _Place,
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        trace_meta, _ = _send_carrier(traced)

        _ = place.run(place.task_name, trace_meta, task_loop)

        attempt = traced.one(f'run {place.task_name}')
        [record] = traced.records.records
        assert record.__dict__['task_id'] == str(TASK_ID)
        assert record.__dict__['task_name'] == place.task_name
        assert record.__dict__['attempt'] == 2
        assert record.__dict__['trace_id'] == _hex_trace_id(attempt)
        assert record.__dict__['span_id'] == _hex_span_id(attempt)
        assert running_task() == NoRunningTask()


class TestTheParentOfTheSpan:
    def test_linked_to_send_starts_a_trace_with_a_link_to_the_send_span(
        self,
        traced_linked: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        trace_meta, send = _send_carrier(traced_linked)

        _ = _run_isolated('isolated_work', trace_meta, task_loop)

        attempt = traced_linked.one('run isolated_work')
        assert attempt.parent is None
        assert send.context is not None
        assert [(link.context.trace_id, link.context.span_id) for link in attempt.links] == [
            (send.context.trace_id, send.context.span_id),
        ]
        assert _hex_trace_id(attempt) != _hex_trace_id(send)
        user = traced_linked.one('user work')
        assert user.parent is not None and attempt.context is not None
        assert user.parent.span_id == attempt.context.span_id

    @pytest.mark.parametrize(
        'fixture',
        [pytest.param('traced', id='child-of-send'), pytest.param('traced_linked', id='linked')],
    )
    def test_a_task_sent_with_no_carrier_has_a_span_with_no_parent(
        self,
        fixture: str,
        request: pytest.FixtureRequest,
        task_loop: TaskLoop,
    ) -> None:
        traced: _Traced = request.getfixturevalue(fixture)

        _ = _run_isolated('isolated_work', NO_CARRIER, task_loop)

        attempt = traced.one('run isolated_work')
        assert attempt.parent is None
        assert attempt.links == ()


class TestTheStatusOfTheSpan:
    def test_a_task_error_sets_error_and_its_code(
        self,
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        _ = _run_isolated('returns_error', NO_CARRIER, task_loop)

        attempt = traced.one('run returns_error')
        assert attempt.status.status_code == StatusCode.ERROR
        assert (attempt.attributes or {})['error.type'] == 'LIMIT_REACHED'
        assert [event.name for event in attempt.events] == []

    def test_an_exception_of_the_body_is_recorded_before_its_mapped_code(
        self,
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        _ = _run_isolated('raises', NO_CARRIER, task_loop)

        attempt = traced.one('run raises')
        assert attempt.status.status_code == StatusCode.ERROR
        assert (attempt.attributes or {})['error.type'] == 'UNHANDLED_EXCEPTION'
        [event] = attempt.events
        assert event.name == 'exception'
        assert (event.attributes or {})['exception.type'] == 'ValueError'

    def test_a_task_that_does_not_resolve_sets_its_worker_code(
        self,
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        _ = _run_isolated('not_registered', NO_CARRIER, task_loop)

        attempt = traced.one('run not_registered')
        assert attempt.status.status_code == StatusCode.ERROR
        assert (attempt.attributes or {})['error.type'] == 'WORKER_RESOLUTION_ERROR'

    @pytest.mark.parametrize(
        ('task_name', 'run_in_order'),
        [
            pytest.param('seat_work', _run_seat_in_order, id='shared-async'),
            pytest.param('thread_work', _run_thread_seat_in_order, id='shared-thread'),
        ],
    )
    def test_a_deadline_that_came_first_sets_the_timeout(
        self,
        task_name: str,
        run_in_order: Callable[
            [str, dict[str, object], TaskLoop, StartedRunning, Callable[[], BodyEndOrder]],
            RunReturn,
        ],
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        """The deadline of a shared run fired in its child before its steps
        returned: the one span of the attempt has the end that the child
        reports, ERROR with `TASK_TIMEOUT`; what the body returned is not
        on it."""
        trace_meta, send = _send_carrier(traced)

        returned = run_in_order(task_name, trace_meta, task_loop, STARTED, _deadline_first)

        assert isinstance(returned, SetRunning), returned
        [attempt] = [span for span in traced.spans() if span.name == f'run {task_name}']
        assert attempt.status.status_code == StatusCode.ERROR
        assert (attempt.attributes or {})['error.type'] == 'TASK_TIMEOUT'
        assert 'horsies.span.source' not in (attempt.attributes or {})
        assert attempt.events == ()
        assert attempt.parent is not None and send.context is not None
        assert attempt.parent.span_id == send.context.span_id


def _subject(
    trace_value: RunTrace = NoTraceCarrier(),
    workflow: RunWorkflow = NotInWorkflow(),
) -> AttemptSubject:
    return AttemptSubject(
        task_id=str(TASK_ID),
        task_name='task',
        queue_name='emails',
        attempt=1,
        worker_id=WORKER_ID,
        execution=Execution.ISOLATED,
        workflow=workflow,
        trace=trace_value,
    )


class TestTheAttributesOfASubject:
    """``attempt_attributes``: the attributes of the span of an attempt."""

    def test_a_task_that_no_node_links_has_no_workflow_attribute(self) -> None:
        assert attempt_otel.attempt_attributes(_subject()) == {
            'messaging.system': 'horsies',
            'messaging.operation.type': 'process',
            'messaging.operation.name': 'run',
            'messaging.message.id': str(TASK_ID),
            'messaging.destination.name': 'emails',
            'horsies.task.id': str(TASK_ID),
            'horsies.task.name': 'task',
            'horsies.task.attempt': 1,
            'horsies.task.execution': 'ISOLATED',
            'horsies.queue': 'emails',
            'horsies.worker.id': WORKER_ID,
        }

    def test_a_workflow_task_has_the_workflow_id_the_node_index_and_the_root(self) -> None:
        workflow = InWorkflow(workflow_id=WORKFLOW_ID, node_index=0, root_workflow_id=ROOT_WORKFLOW_ID)
        attributes = attempt_otel.attempt_attributes(_subject(workflow=workflow))
        assert attributes['horsies.workflow.id'] == str(WORKFLOW_ID)
        assert attributes['horsies.workflow.node'] == 0
        assert attributes['horsies.workflow.root_id'] == str(ROOT_WORKFLOW_ID)
        assert attempt_otel.workflow_attributes(workflow) == {
            'horsies.workflow.id': str(WORKFLOW_ID),
            'horsies.workflow.node': 0,
            'horsies.workflow.root_id': str(ROOT_WORKFLOW_ID),
        }
        assert attempt_otel.workflow_attributes(NotInWorkflow()) == {}


class TestTheEndOfEachOutcome:
    """``OtelAttemptSpan.end`` for each end of the steps of a run."""

    @pytest.mark.parametrize(
        ('end', 'error_type', 'events'),
        [
            pytest.param(ResultSucceeded(), None, [], id='succeeded'),
            pytest.param(ResultFailed('MY_CODE', NothingRaised()), 'MY_CODE', [], id='failed'),
            pytest.param(
                ResultFailed('TASK_EXCEPTION', RaisedInRun(KeyError('k'))),
                'TASK_EXCEPTION',
                ['exception'],
                id='failed-with-exception',
            ),
            pytest.param(
                BodyInterrupted(core.CoreStopped('log forwarder', 'planned panic')),
                'CoreStopped',
                ['exception'],
                id='interrupted',
            ),
            pytest.param(
                ScopeRaised(RuntimeError('defect')),
                'RuntimeError',
                ['exception'],
                id='scope-raised',
            ),
        ],
    )
    def test_the_status_follows_the_end(
        self,
        end: AttemptEnd,
        error_type: str | None,
        events: list[str],
        traced: _Traced,
    ) -> None:
        recorder = OtelAttemptRecorder(
            tracer=traced.tracer,
            parentage=ChildOfSend(),
            provider_flush=lambda timeout_millis: SpansFlushed(),
        )

        recorder.open(_subject()).end(end)

        [span] = traced.spans()
        assert (span.attributes or {}).get('error.type') == error_type
        expected_status = StatusCode.UNSET if error_type is None else StatusCode.ERROR
        assert span.status.status_code == expected_status
        assert [event.name for event in span.events] == events


def _ok_steps() -> EncodedResult:
    return EncodedResult(ResultEnvelope('{"ok": 1}'), ResultSucceeded())


async def _ok_steps_async() -> EncodedResult:
    return _ok_steps()


class TestTracingOff:
    def test_a_run_with_no_attempt_spans_makes_no_span_and_keeps_the_log_context(
        self,
        traced: _Traced,
        task_loop: TaskLoop,
    ) -> None:
        trace_meta, _ = _send_carrier(traced)
        use_attempt_spans(NO_ATTEMPT_SPANS)

        returned = _run_isolated('isolated_work', trace_meta, task_loop)

        assert isinstance(returned, SetRunning), returned
        assert [span.name for span in traced.spans()] == ['send', 'user work']
        [record] = traced.records.records
        assert record.__dict__['task_id'] == str(TASK_ID)
        assert record.__dict__['attempt'] == 2
        assert record.__dict__['trace_id'] is None
        assert record.__dict__['span_id'] is None

    def test_the_body_places_import_no_opentelemetry_module(self) -> None:
        code = textwrap.dedent(
            '''
            import sys

            class BlockOpenTelemetry:
                def find_spec(self, name, path=None, target=None):
                    if name == 'opentelemetry' or name.startswith('opentelemetry.'):
                        raise ModuleNotFoundError(f'No module named {name!r}', name=name)
                    return None

            sys.meta_path.insert(0, BlockOpenTelemetry())

            from horsies.core.models.tasks import Execution
            from horsies.core.models.tracing import NoTracing
            from horsies.core.telemetry.attempt_span import (
                AttemptSubject, attempt_spans_of, flush_attempt_spans_at_stop,
                isolated_body_end, run_in_attempt_span, use_attempt_spans,
            )
            from horsies.core.worker import child_main, shared_runner, thread_runner
            from horsies.core.worker.child_frames import NoTraceCarrier
            from horsies.core.worker.run_return import (
                EncodedResult, NotInWorkflow, ResultEnvelope, ResultSucceeded,
            )

            use_attempt_spans(attempt_spans_of(NoTracing()))
            returned = run_in_attempt_span(
                AttemptSubject(
                    task_id='t', task_name='task', queue_name='q', attempt=1, worker_id='w',
                    execution=Execution.ISOLATED, workflow=NotInWorkflow(), trace=NoTraceCarrier(),
                ),
                lambda: EncodedResult(ResultEnvelope('{}'), ResultSucceeded()),
                isolated_body_end,
            )
            assert returned == ResultEnvelope('{}'), returned
            print(flush_attempt_spans_at_stop())
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
        assert result.stdout.splitlines() == ['NothingToFlush()', '[]']


class TestSharedAsyncSeats:
    def test_two_seats_of_one_loop_each_see_their_own_span_and_task(
        self,
        traced: _Traced,
    ) -> None:
        both_inside = asyncio.Barrier(2)
        seen: dict[str, tuple[str, RunningTask | NoRunningTask]] = {}

        async def steps(name: str) -> EncodedResult:
            await both_inside.wait()
            span_id = format(trace.get_current_span().get_span_context().span_id, '016x')
            seen[name] = (span_id, running_task())
            return _ok_steps()

        def subject(name: str) -> AttemptSubject:
            return AttemptSubject(
                task_id=name,
                task_name=name,
                queue_name=QUEUE_NAME,
                attempt=1,
                worker_id=WORKER_ID,
                execution=Execution.SHARED_ASYNC,
                workflow=NotInWorkflow(),
                trace=NoTraceCarrier(),
            )

        async def two_seats() -> None:
            async with asyncio.timeout(SCENARIO_TIMEOUT_S):
                _ = await asyncio.gather(
                    run_in_attempt_span_async(subject('a'), lambda: steps('a'), _body_first),
                    run_in_attempt_span_async(subject('b'), lambda: steps('b'), _body_first),
                )

        asyncio.run(two_seats())

        for name in ('a', 'b'):
            span = traced.one(f'run {name}')
            span_id, task = seen[name]
            assert span_id == _hex_span_id(span)
            assert task == RunningTask(
                task_id=name,
                task_name=name,
                attempt=1,
                span=SpanIds(trace_id=_hex_trace_id(span), span_id=span_id),
            )


class _RaisingRecorder:
    """A recorder whose span does not open."""

    def open(self, subject: AttemptSubject) -> attempt_otel.OtelAttemptSpan:
        raise RuntimeError(f'no span for {subject.task_name}')

    def flush(self, timeout_millis: int) -> ProviderFlush:
        return SpansFlushed()


class TestARecorderThatFails:
    def test_a_span_that_does_not_open_leaves_the_run_unchanged_and_logs_once(
        self,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(attempt_span, '_RECORDER_FAILURES', attempt_span._RecorderFailureLog())
        use_attempt_spans(OtelAttemptSpans(recorder=_RaisingRecorder()))
        try:
            with caplog.at_level(logging.WARNING, logger='horsies.telemetry'):
                first = run_in_attempt_span(_subject(), _ok_steps, _body_first)
                second = run_in_attempt_span(_subject(), _ok_steps, _body_first)
        finally:
            use_attempt_spans(NO_ATTEMPT_SPANS)

        assert first == second == ResultEnvelope('{"ok": 1}')
        warnings = [r for r in caplog.records if r.name == 'horsies.telemetry']
        assert len(warnings) == 1
        assert 'open stage' in warnings[0].getMessage()

    def test_steps_that_raise_end_the_span_and_propagate(self, traced: _Traced) -> None:
        def defect() -> EncodedResult:
            raise RuntimeError('a defect of the steps')

        with pytest.raises(RuntimeError, match='a defect of the steps'):
            _ = run_in_attempt_span(_subject(), defect, _body_first)

        span = traced.one('run task')
        assert (span.attributes or {})['error.type'] == 'RuntimeError'
        assert running_task() == NoRunningTask()
        assert not trace.get_current_span().get_span_context().is_valid


class _RecordedSpan:
    """A span that keeps the end that it gets."""

    def __init__(self, ends: list[AttemptEnd]) -> None:
        self._ends = ends

    @property
    def ids(self) -> SpanIds | NoSpanIds:
        return NoSpanIds()

    def make_current(self) -> Callable[[], None]:
        return lambda: None

    def end(self, end: AttemptEnd) -> None:
        self._ends.append(end)


class _EndRecorder:
    """A recorder whose spans keep their ends in `ends`."""

    def __init__(self) -> None:
        self.ends: list[AttemptEnd] = []

    def open(self, subject: AttemptSubject) -> _RecordedSpan:
        _ = subject
        return _RecordedSpan(self.ends)

    def flush(self, timeout_millis: int) -> ProviderFlush:
        _ = timeout_millis
        return SpansFlushed()


class TestTheOrderAtTheEndOfTheSteps:
    """The order that a body place gives, read once when the steps return,
    decides the end of the span; the run returns what the steps returned in
    both orders: the end that the child reports is the seat's."""

    def test_a_deadline_that_came_first_ends_the_span_with_the_timeout(self) -> None:
        recorder = _EndRecorder()
        use_attempt_spans(OtelAttemptSpans(recorder=recorder))
        try:
            returned = run_in_attempt_span(_subject(), _ok_steps, _deadline_first)
        finally:
            use_attempt_spans(NO_ATTEMPT_SPANS)

        assert recorder.ends == [RunTimedOut()]
        assert returned == ResultEnvelope('{"ok": 1}')

    def test_an_operator_cancel_that_came_first_ends_the_span_with_the_cancel(
        self,
    ) -> None:
        recorder = _EndRecorder()
        use_attempt_spans(OtelAttemptSpans(recorder=recorder))
        try:
            returned = run_in_attempt_span(_subject(), _ok_steps, OperatorCancelFirst)
        finally:
            use_attempt_spans(NO_ATTEMPT_SPANS)

        assert recorder.ends == [RunCancelledByOperator()]
        assert returned == ResultEnvelope('{"ok": 1}')

    def test_steps_that_came_first_end_the_span_with_their_outcome(self) -> None:
        recorder = _EndRecorder()
        use_attempt_spans(OtelAttemptSpans(recorder=recorder))
        try:
            returned = asyncio.run(
                run_in_attempt_span_async(_subject(), _ok_steps_async, _body_first),
            )
        finally:
            use_attempt_spans(NO_ATTEMPT_SPANS)

        assert recorder.ends == [ResultSucceeded()]
        assert returned == ResultEnvelope('{"ok": 1}')

    def test_with_tracing_off_the_order_is_still_read_once(self) -> None:
        asked: list[BodyEndOrder] = []

        def body_end() -> BodyEndOrder:
            asked.append(BodyFirst())
            return BodyFirst()

        use_attempt_spans(NO_ATTEMPT_SPANS)
        returned = run_in_attempt_span(_subject(), _ok_steps, body_end)

        assert asked == [BodyFirst()]
        assert returned == ResultEnvelope('{"ok": 1}')

    def test_steps_that_raise_do_not_read_the_order(self) -> None:
        asked: list[BodyEndOrder] = []

        def body_end() -> BodyEndOrder:
            asked.append(DeadlineFirst())
            return DeadlineFirst()

        def defect() -> EncodedResult:
            raise RuntimeError('a defect of the steps')

        with pytest.raises(RuntimeError, match='a defect of the steps'):
            _ = run_in_attempt_span(_subject(), defect, body_end)

        assert asked == []


class TestTheFlushAtStop:
    def test_a_flush_that_returns_gives_its_end_with_the_bound_in_milliseconds(self) -> None:
        asked: list[int] = []

        def flush(timeout_millis: int) -> ProviderFlush:
            asked.append(timeout_millis)
            return SpansFlushed()

        assert bounded_flush(flush, 0.5) == SpansFlushed()
        assert asked == [500]

    def test_a_flush_that_raises_is_returned_not_raised(self) -> None:
        failure = RuntimeError('exporter down')

        def flush(timeout_millis: int) -> ProviderFlush:
            raise failure

        assert bounded_flush(flush, 0.5) == FlushRaised(failure)

    def test_a_flush_that_blocks_ends_at_its_bound(self) -> None:
        release = threading.Event()

        def flush(timeout_millis: int) -> ProviderFlush:
            _ = release.wait(SCENARIO_TIMEOUT_S)
            return SpansFlushed()

        began = time.monotonic()
        flushed = bounded_flush(flush, 0.2)
        waited = time.monotonic() - began
        release.set()

        assert flushed == FlushTimedOut(bound_s=0.2)
        assert 0.2 <= waited < 1.0

    def test_a_child_with_no_attempt_spans_has_nothing_to_flush(self) -> None:
        use_attempt_spans(NO_ATTEMPT_SPANS)

        assert flush_attempt_spans_at_stop() == NothingToFlush()

    def test_an_unconfirmed_flush_at_stop_is_logged(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        recorder = OtelAttemptRecorder(
            tracer=trace.NoOpTracer(),
            parentage=ChildOfSend(),
            provider_flush=lambda timeout_millis: FlushNotConfirmed(),
        )
        use_attempt_spans(OtelAttemptSpans(recorder=recorder))
        try:
            with caplog.at_level(logging.WARNING, logger='horsies.telemetry'):
                flushed = flush_attempt_spans_at_stop()
        finally:
            use_attempt_spans(NO_ATTEMPT_SPANS)

        assert flushed == FlushNotConfirmed()
        assert 'did not confirm' in caplog.records[-1].getMessage()

    def test_the_stop_flushes_before_the_exit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        steps: list[str] = []

        class _Exited(Exception):
            pass

        def flushed() -> NothingToFlush:
            steps.append('flush')
            return NothingToFlush()

        def ended(code: int) -> None:
            steps.append(f'exit {code}')
            raise _Exited

        monkeypatch.setattr(child_main, 'flush_attempt_spans_at_stop', flushed)
        monkeypatch.setattr(child_main, 'end_without_teardown', ended)

        with pytest.raises(_Exited):
            child_main.exit_after_stop(ExecutorShutDown())

        assert steps == ['flush', 'exit 0']


class _FlushingProvider:
    def __init__(self, flushed: bool) -> None:
        self.flushed = flushed
        self.asked: list[int] = []

    def force_flush(self, timeout_millis: int) -> bool:
        self.asked.append(timeout_millis)
        return self.flushed


class TestTheProviderFlush:
    @pytest.mark.parametrize(
        ('provider', 'expected'),
        [
            pytest.param(_FlushingProvider(True), SpansFlushed(), id='flushed'),
            pytest.param(_FlushingProvider(False), FlushNotConfirmed(), id='not-confirmed'),
            pytest.param(trace.ProxyTracerProvider(), ProviderHasNoFlush(), id='api-provider'),
        ],
    )
    def test_the_flush_follows_the_global_provider(
        self,
        provider: object,
        expected: ProviderFlush,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(attempt_otel.trace, 'get_tracer_provider', lambda: provider)

        assert attempt_otel.global_provider_flush(250) == expected


class TestTheOutcomeOfAResult:
    def test_a_value_succeeds(self) -> None:
        assert child_runner.result_outcome_of(Ok(3)) == ResultSucceeded()

    def test_a_user_code_fails_with_its_text(self) -> None:
        out: Result[int, TaskError] = Err(
            TaskError(error_code='LIMIT_REACHED', message='m'),
        )
        assert child_runner.result_outcome_of(out) == ResultFailed('LIMIT_REACHED', NothingRaised())

    def test_a_built_in_code_fails_with_its_value_and_its_exception(self) -> None:
        exception = ValueError('v')
        out: Result[int, TaskError] = Err(
            TaskError(
                error_code=child_runner.OperationalErrorCode.TASK_EXCEPTION,
                message='m',
                exception=exception,
            ),
        )
        assert child_runner.result_outcome_of(out) == ResultFailed(
            'TASK_EXCEPTION',
            RaisedInRun(exception),
        )


def test_the_span_ids_of_an_invalid_span_are_none() -> None:
    assert attempt_otel.span_ids_of(trace.INVALID_SPAN) == NoSpanIds()


def test_a_stored_carrier_with_a_valid_span_parents_the_attempt(traced: _Traced) -> None:
    send = traced.tracer.start_span('send')
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier, context=trace.set_span_in_context(send))
    send.end()
    recorder = OtelAttemptRecorder(
        tracer=traced.tracer,
        parentage=ChildOfSend(),
        provider_flush=lambda timeout_millis: SpansFlushed(),
    )

    opened = recorder.open(_subject(StoredTraceCarrier(carrier=carrier)))
    opened.end(ResultSucceeded())

    attempt = traced.one('run task')
    assert attempt.parent is not None
    assert attempt.parent.span_id == send.get_span_context().span_id
