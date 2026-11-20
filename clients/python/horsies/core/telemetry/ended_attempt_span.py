"""The span of each attempt that the worker ended, made in the handler child.

The task child opens the span of an attempt around its body, and a span is
exported only when it ends. When the worker ends the attempt itself with no
result frame of the child (an isolated task killed at its timeout, a shared
child killed for a blocked loop or past its deadlines, a child lost with its
exit seen), that span never ends. The `AttemptEnded` event of such an
attempt (`AttemptEndDecidedByWorker`) carries the facts of its span, and the
handler child makes the span from them. An end that the child reported, a
shared run that its child ended at the deadline included
(`ChildResultTimedOut`), has the span of the child, with the timeout: the
handler child makes none, so one attempt has one span.

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

The worker does not know the workflow node of a workflow task: the start
write of the child read it, and the event carries `WORKFLOW_NOT_KNOWN`. For
such an attempt the handler child reads the node by the task id with one
bounded read of the core (`WorkflowNodeReader`, `CoreHandle.task_workflow_node`),
so the span has the workflow attributes of the span of a child. The read
never runs on the control thread, which routes the events to the handlers:

- The control thread puts the span in the node read queue
  (`NodeReadQueue`, at most `NODE_READ_QUEUE_SPANS`) and goes on. A full
  queue makes the span at once with `WorkflowNotKnown`.
- One reader thread (`start_node_reader`) reads the node of each span in
  turn, within `TASK_WORKFLOW_NODE_READ_END` of the core (12 s at most, 2 s
  when the cancel of a statement past its bound is answered), then makes
  the span.
- At the stop, the reader thread gets the drain time of the handler
  threads; each span left is made with `WorkflowNotKnown`
  (`end_node_reads_at_stop`).

A read that finds no node leaves the span with no workflow attribute
(`WorkflowNotKnown`). A read that fails (`read_failed`), and a span made
with no read (`not_read`), are counted (`SpanReadFailures`, sent in the
`handler_stats` frames to the metric
`horsies.spans.worker_ended.read_failures`) and logged: the first of each
reason, then one line per `NODE_READ_FAILURE_LOG_EVERY`.

Telemetry never stops the handler child: a recorder (the user's SDK) that
raises is logged once per process, and the event still goes to the handlers.
`flush_ended_attempt_spans_at_stop` flushes the spans at the stop of the
child, within `ENDED_ATTEMPT_SPAN_FLUSH_BOUND_S`.

This module imports no `opentelemetry` module.
"""

from __future__ import annotations

import collections
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
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
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_frames import NO_SPAN_READS, SpanReadCounts
from horsies.core.worker.run_return import InWorkflow, NotInWorkflow

_logger = get_logger('telemetry')

ENDED_ATTEMPT_SPAN_FLUSH_BOUND_S: Final = 1.0
"""The longest wait for the flush of the spans at the stop of the handler
child. The worker kills a child that has not exited `CHILD_STOP_TIMEOUT`
(10 s, horsies-core) after the stop frame. Before the flush, the handler
threads get `HANDLER_DRAIN_S` (2 s): 2 s + 1 s and the last stats frame
stay under 10 s."""


NODE_READ_QUEUE_SPANS: Final = 1_000
"""The most spans that wait for the node read of their workflow task. The
control thread never waits for a read: a span that does not fit is made at
once with `WorkflowNotKnown` and counted `not_read`."""

NODE_READ_FAILURE_LOG_EVERY: Final = 1_000
"""After the first span of each reason made with no workflow node, one log
line per this many of that reason."""


@dataclass(frozen=True, slots=True)
class WorkflowNotKnown:
    """A workflow node links the task, and the handler child has no node for
    it: the read failed, found no node, or did not run. The span has no
    workflow attribute."""


type EndedWorkflow = InWorkflow | NotInWorkflow | WorkflowNotKnown
"""The workflow node of the task of an attempt that the worker ended, for
its span."""


@dataclass(frozen=True, slots=True)
class NodeNotLinked:
    """No workflow node links the task now: its workflow was deleted."""


@dataclass(frozen=True, slots=True)
class NodeReadFailed:
    """The node read failed: `detail` names the cause."""

    detail: str


type NodeRead = InWorkflow | NodeNotLinked | NodeReadFailed
"""What one node read gave."""


class WorkflowNodeReader(Protocol):
    """Reads the workflow node of a task by its id."""

    def read(self, task_id: uuid.UUID) -> NodeRead:
        """The node that links `task_id`, within the bound of the read. Never
        raises: a failure is `NodeReadFailed`."""
        ...


class NotReadCause(Enum):
    """Why the node read of a span did not run."""

    QUEUE_FULL = 'QUEUE_FULL'
    """The node read queue held `NODE_READ_QUEUE_SPANS` spans."""
    CHILD_STOPPED = 'CHILD_STOPPED'
    """The handler child stopped before the read ended."""


def _not_read_text(cause: NotReadCause) -> str:
    match cause:
        case NotReadCause.QUEUE_FULL:
            return 'the node read queue is full'
        case NotReadCause.CHILD_STOPPED:
            return 'the handler child stopped before the read ended'


class SpanReadFailures:
    """The spans of attempts of workflow tasks that the worker ended, which
    this child made with no workflow node since it started, by reason
    (`horsies.spans.worker_ended.read_failures` in the worker parent).
    Safe between threads: the reader thread records `read_failed`; the
    control thread and the stop record `not_read`; the main thread reads
    the totals for the `handler_stats` frames."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._read_failed = 0
        self._not_read = 0

    def counts(self) -> SpanReadCounts:
        """The totals since the child started."""
        with self._lock:
            return SpanReadCounts(read_failed=self._read_failed, not_read=self._not_read)

    def record_read_failed(self, task_id: uuid.UUID, detail: str) -> None:
        """Count one failed node read of `task_id`, and log it: the first,
        then one line per `NODE_READ_FAILURE_LOG_EVERY`."""
        with self._lock:
            self._read_failed += 1
            count = self._read_failed
        if count == 1 or count % NODE_READ_FAILURE_LOG_EVERY == 0:
            _logger.warning(
                'The workflow node of task %s was not read for the span of its '
                'attempt that the worker ended (%d failed reads so far): %s. '
                'The span has no workflow attribute.',
                task_id,
                count,
                detail,
            )

    def record_not_read(self, task_id: uuid.UUID, cause: NotReadCause) -> None:
        """Count one span of `task_id` made with no node read, and log it:
        the first, then one line per `NODE_READ_FAILURE_LOG_EVERY`."""
        with self._lock:
            self._not_read += 1
            count = self._not_read
        if count == 1 or count % NODE_READ_FAILURE_LOG_EVERY == 0:
            _logger.warning(
                'The span of the attempt of task %s that the worker ended was '
                'made with no workflow node read (%d so far): %s. The span has '
                'no workflow attribute.',
                task_id,
                count,
                _not_read_text(cause),
            )


@dataclass(frozen=True, slots=True)
class WaitingSpan:
    """The span of an attempt of a workflow task that the worker ended,
    which waits for the read of its workflow node."""

    ended: AttemptEnded
    decided: AttemptEndDecidedByWorker


@dataclass(frozen=True, slots=True)
class SpanQueued:
    """The node read queue took the span."""


@dataclass(frozen=True, slots=True)
class ReadQueueFull:
    """The node read queue holds `NODE_READ_QUEUE_SPANS` spans; the span is
    not in it."""


type ReadOffered = SpanQueued | ReadQueueFull
"""What an offer of one span to the node read queue gave."""


@dataclass(frozen=True, slots=True)
class ReadQueueClosed:
    """The node read queue is closed: the reader thread ends."""


@dataclass(frozen=True, slots=True)
class SpanClaimed:
    """The reader thread makes the span that it read the node of."""


@dataclass(frozen=True, slots=True)
class TakenAtStop:
    """The stop took the span while its node was read, and made it."""


type ReadClaim = SpanClaimed | TakenAtStop
"""Who makes the span whose node read ended."""


@dataclass(frozen=True, slots=True)
class _ReaderIdle:
    """The reader thread holds no span."""


@dataclass(frozen=True, slots=True)
class _Reading:
    """The reader thread reads the node of `waiting`."""

    waiting: WaitingSpan


@dataclass(frozen=True, slots=True)
class _Making:
    """The reader thread makes the span whose node it read."""


type _ReaderState = _ReaderIdle | _Reading | _Making


class NodeReadQueue:
    """The bounded queue of the spans that wait for their node read, and
    the span of the reader thread. The control thread offers, the reader
    thread takes, claims and ends each span, the main thread closes it at
    the stop. Safe between threads.

    `offer` never blocks: a queue that holds `capacity` spans refuses the
    span. `close` takes the spans left and the span whose node is read; the
    span that the reader thread makes then stays with the reader thread.
    """

    def __init__(self, capacity: int) -> None:
        assert capacity >= 1, f'the node read queue holds at least one span, got {capacity}'
        self._capacity: Final = capacity
        self._spans: collections.deque[WaitingSpan] = collections.deque()
        self._changed: Final = threading.Condition()
        self._reader: _ReaderState = _ReaderIdle()
        self._closed = False

    def offer(self, waiting: WaitingSpan) -> ReadOffered:
        """Queue `waiting` when the queue has room. An offer after `close`
        is a defect: the control thread ends before the stop closes it."""
        with self._changed:
            assert not self._closed, 'a span was offered to a closed node read queue'
            if len(self._spans) >= self._capacity:
                return ReadQueueFull()
            self._spans.append(waiting)
            self._changed.notify_all()
            return SpanQueued()

    def take(self) -> WaitingSpan | ReadQueueClosed:
        """Reader thread: the next span, after a wait for one;
        `ReadQueueClosed` once the queue is closed."""
        with self._changed:
            while not self._spans and not self._closed:
                _ = self._changed.wait()
            if self._closed:
                return ReadQueueClosed()
            waiting = self._spans.popleft()
            self._reader = _Reading(waiting=waiting)
            return waiting

    def claim(self) -> ReadClaim:
        """Reader thread, after the read: `SpanClaimed` when the reader
        thread makes the span; `TakenAtStop` when `close` took it."""
        with self._changed:
            if self._closed:
                return TakenAtStop()
            self._reader = _Making()
            return SpanClaimed()

    def made(self) -> None:
        """Reader thread: the claimed span is made."""
        with self._changed:
            self._reader = _ReaderIdle()
            self._changed.notify_all()

    def wait_idle(self, timeout_s: float) -> None:
        """Main thread: wait at most `timeout_s` for an empty queue and a
        reader thread that holds no span."""
        deadline = time.monotonic() + timeout_s
        with self._changed:
            while self._spans or self._reader_holds_a_span():
                left = deadline - time.monotonic()
                if left <= 0:
                    return
                _ = self._changed.wait(left)

    def _reader_holds_a_span(self) -> bool:
        match self._reader:
            case _ReaderIdle():
                return False
            case _Reading() | _Making():
                return True
            case _ as unreachable:
                assert_never(unreachable)

    def close(self) -> tuple[WaitingSpan, ...]:
        """Main thread, at the stop: close the queue, and take the spans
        left and the span whose node the reader thread reads."""
        with self._changed:
            self._closed = True
            left = list(self._spans)
            self._spans.clear()
            match self._reader:
                case _Reading(waiting=waiting):
                    left.insert(0, waiting)
                    self._reader = _ReaderIdle()
                case _ReaderIdle() | _Making():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            self._changed.notify_all()
            return tuple(left)


class EndedAttemptSpanRecorder(Protocol):
    """Makes the spans of the attempts that the worker ended, and flushes
    them."""

    def make(
        self,
        ended: AttemptEnded,
        decided: AttemptEndDecidedByWorker,
        workflow: EndedWorkflow,
    ) -> None:
        """Make and end the span of the attempt of `ended`, whose end is
        `decided` and whose workflow node is `workflow`. May raise: the
        caller logs it. Called from the control thread, the reader thread
        and the main thread."""
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
    """A span for each attempt that the worker ended, through `recorder`.
    The node of a workflow task is read through `nodes` on the reader
    thread (`start_node_reader`), from `read_queue`; the spans made with no
    node are counted in `read_failures`."""

    recorder: EndedAttemptSpanRecorder
    nodes: WorkflowNodeReader
    read_failures: SpanReadFailures = field(default_factory=SpanReadFailures)
    read_queue: NodeReadQueue = field(
        default_factory=lambda: NodeReadQueue(NODE_READ_QUEUE_SPANS),
    )


type EndedAttemptSpans = NoEndedAttemptSpans | OtelEndedAttemptSpans
"""What the handler child does for an attempt that the worker ended."""

NO_ENDED_ATTEMPT_SPANS: Final = NoEndedAttemptSpans()
"""The choice of `NoTracing` and of `WorkerEndedSpans.NO_SPANS`."""


def ended_attempt_spans_of(
    tracing: OtelTracing | NoTracing,
    nodes: WorkflowNodeReader,
) -> EndedAttemptSpans:
    """The spans of the attempts that the worker ended under `tracing`, the
    setting that gives the `worker_ended_spans` of the worker manifest
    (`worker_ended_spans_of`), reading the node of a workflow task through
    `nodes`. `MAKE_SPANS` imports the OpenTelemetry API
    (`horsies.core.telemetry.ended_attempt_otel`), which the config check
    found installed; `NO_SPANS` and `NoTracing` import nothing. The caller
    starts the reader thread (`start_node_reader`)."""
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

            return OtelEndedAttemptSpans(
                recorder=otel_ended_attempt_recorder(parentage),
                nodes=nodes,
            )


def span_read_counts_of(spans: EndedAttemptSpans) -> SpanReadCounts:
    """The spans made with no workflow node since the child started, for
    the `handler_stats` frame."""
    match spans:
        case NoEndedAttemptSpans():
            return NO_SPAN_READS
        case OtelEndedAttemptSpans(read_failures=read_failures):
            return read_failures.counts()
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class ReaderThreadRefused:
    """The OS refused the reader thread: `exception_name`, `detail`."""

    exception_name: str
    detail: str


def start_node_reader(spans: EndedAttemptSpans) -> Result[None, ReaderThreadRefused]:
    """Start the reader thread of `OtelEndedAttemptSpans`, a daemon thread
    that reads the node of each waiting span, then makes it
    (`read_nodes_for_spans`); `NoEndedAttemptSpans` starts none. `Err`: the
    OS refused the thread."""
    match spans:
        case NoEndedAttemptSpans():
            return Ok(None)
        case OtelEndedAttemptSpans() as otel:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    reader = threading.Thread(
        target=read_nodes_for_spans,
        args=(otel,),
        name='horsies-span-node-reader',
        daemon=True,
    )
    try:
        reader.start()
    except RuntimeError as refused:
        return Err(ReaderThreadRefused(exception_name=type(refused).__name__, detail=str(refused)))
    return Ok(None)


def read_nodes_for_spans(spans: OtelEndedAttemptSpans) -> None:
    """Reader thread body: for each span of `spans.read_queue`, read its
    node (one bounded read of the core), then make it, until the queue is
    closed. A failed read gives `WorkflowNotKnown` and is counted
    `read_failed`. A span that the stop took while its node was read is
    made by the stop."""
    while True:
        match spans.read_queue.take():
            case ReadQueueClosed():
                return
            case WaitingSpan() as waiting:
                pass
            case _ as unreachable:
                assert_never(unreachable)
        task_id = waiting.ended.task_id
        workflow = workflow_of_read(task_id, spans.nodes.read(task_id), spans.read_failures)
        match spans.read_queue.claim():
            case SpanClaimed():
                _ = _make_span(spans.recorder, waiting.ended, waiting.decided, workflow)
                spans.read_queue.made()
            case TakenAtStop():
                pass
            case _ as unreachable:
                assert_never(unreachable)


def workflow_of_read(
    task_id: uuid.UUID,
    read: NodeRead,
    read_failures: SpanReadFailures,
) -> EndedWorkflow:
    """The workflow node of the span of `task_id` that `read` gave:
    `WorkflowNotKnown` when the read found no node, or failed (counted
    `read_failed` in `read_failures`)."""
    match read:
        case InWorkflow() as node:
            return node
        case NodeNotLinked():
            return WorkflowNotKnown()
        case NodeReadFailed(detail=detail):
            read_failures.record_read_failed(task_id, detail)
            return WorkflowNotKnown()
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class EndedSpanMade:
    """The recorder made and ended the span of the attempt, with the
    workflow node `workflow`."""

    workflow: EndedWorkflow


@dataclass(frozen=True, slots=True)
class EndedSpanWaitsForNode:
    """The span of the attempt of a workflow task waits in the node read
    queue; the reader thread makes it."""


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


type EndedSpanStep = (
    EndedSpanMade | EndedSpanWaitsForNode | EndedSpansOff | NotEndedByWorker | EndedSpanRaised
)
"""What the step of one event did."""


def make_ended_attempt_span(spans: EndedAttemptSpans, event: AnyEvent) -> EndedSpanStep:
    """The step of the control thread for one event, before the handlers
    get it: with `OtelEndedAttemptSpans`, the span of an `AttemptEnded`
    whose end is `AttemptEndDecidedByWorker`. A plain task gets its span at
    once. A workflow task (`WORKFLOW_NOT_KNOWN`) goes to the node read
    queue; a full queue makes its span at once with `WorkflowNotKnown`,
    counted `not_read`. Never waits for a read. An `Exception` of the
    recorder is logged once per process and goes no further."""
    match spans:
        case NoEndedAttemptSpans():
            return EndedSpansOff()
        case OtelEndedAttemptSpans() as otel:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match event:
        case AttemptEnded(end=AttemptEndDecidedByWorker() as decided) as ended:
            pass
        case _:
            return NotEndedByWorker()
    match decided.workflow:
        case 'NOT_IN_WORKFLOW':
            return _make_span(otel.recorder, ended, decided, NotInWorkflow())
        case 'WORKFLOW_NOT_KNOWN':
            pass
    match otel.read_queue.offer(WaitingSpan(ended=ended, decided=decided)):
        case SpanQueued():
            return EndedSpanWaitsForNode()
        case ReadQueueFull():
            otel.read_failures.record_not_read(ended.task_id, NotReadCause.QUEUE_FULL)
            return _make_span(otel.recorder, ended, decided, WorkflowNotKnown())
        case _ as unreachable:
            assert_never(unreachable)


def end_node_reads_at_stop(spans: EndedAttemptSpans, timeout_s: float) -> int:
    """At the stop, after the control thread ended: give the reader thread
    at most `timeout_s` for the spans that wait, then close the queue and
    make each span left with `WorkflowNotKnown`, counted `not_read`. The
    span that the reader thread makes at the close stays with it. Returns
    the spans made here."""
    match spans:
        case NoEndedAttemptSpans():
            return 0
        case OtelEndedAttemptSpans() as otel:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    otel.read_queue.wait_idle(timeout_s)
    left = otel.read_queue.close()
    for waiting in left:
        otel.read_failures.record_not_read(waiting.ended.task_id, NotReadCause.CHILD_STOPPED)
        _ = _make_span(otel.recorder, waiting.ended, waiting.decided, WorkflowNotKnown())
    return len(left)


def _make_span(
    recorder: EndedAttemptSpanRecorder,
    ended: AttemptEnded,
    decided: AttemptEndDecidedByWorker,
    workflow: EndedWorkflow,
) -> EndedSpanMade | EndedSpanRaised:
    """Make the span through `recorder`; an `Exception` of the recorder is
    logged once per process."""
    try:
        recorder.make(ended, decided, workflow)
    except Exception as failure:
        _RECORDER_FAILURES.report(failure)
        return EndedSpanRaised(exception=failure)
    return EndedSpanMade(workflow=workflow)


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
