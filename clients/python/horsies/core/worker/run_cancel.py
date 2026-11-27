"""The cancel of one run in a worker child: its state, which the body reads
through ``TaskRuntime.cancel_state``, and where the run is, so that an
operator cancel goes to the body only.

The worker sends ``CancelRunFrame`` when an operator cancelled the task of a
run (``cancel_task`` with ``policy=CancelPolicy.INCLUDE_RUNNING``): the
cancel statement moved the row to history and recorded the attempt as
``CANCELLED``. The child gives the cancel to the task code and reports the end of the run as
cancelled, whatever the body returned.

The phases of a run, in order:

- ``BEFORE_BODY``: the start write may still run. A cancel is kept: the
  child must not cancel the start write. When the start write returns, the
  body does not start (``SkipTheBody``, phase ``SKIPPED``), and the end is
  cancelled with the body ``NOT_STARTED``. A start write that ends the run
  another way (a lost claim, a stopped core) reports that end, and the
  cancel is dropped.
- ``IN_BODY``: the body may run. A cancel goes to it: an ``async def`` body
  gets ``asyncio.CancelledError`` at its next ``await``; a ``def`` body reads
  ``cancel_state``.
- ``AFTER_BODY``: the body ended; the run reports its end next. A cancel now
  makes the end cancelled.
- ``ENDED``: the end of the run is fixed. A later cancel is ignored: the
  frame of the end and the cancel crossed, and the worker writes nothing for
  that end (the row is in history).

The first cause wins: a deadline after an operator cancel, or an operator
cancel after a deadline, changes nothing. One lock guards the state, so the
control thread of an isolated child, the seat loop of a shared async child,
and the threads of a shared thread child can each request a cancel.
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, assert_never

from horsies.core.task_runtime import (
    CancelAlreadySet,
    CancelCause,
    CancelNotRequested,
    CancelRequested,
    CancelRequestOutcome,
    CancelSet,
    RunCancelCell,
    RunCancelState,
)
from horsies.core.worker.run_return import CancelledBody


class BodyPhase(Enum):
    """Where one run is, for the delivery of an operator cancel."""

    BEFORE_BODY = 'before_body'
    IN_BODY = 'in_body'
    AFTER_BODY = 'after_body'
    SKIPPED = 'skipped'
    """An operator cancel came before the body: the body did not start."""
    ENDED = 'ended'


@dataclass(frozen=True, slots=True)
class StartTheBody:
    """No operator cancel came before the body: the body starts."""


@dataclass(frozen=True, slots=True)
class SkipTheBody:
    """An operator cancel came before the body: the body does not start,
    and the end is cancelled with the body ``NOT_STARTED``."""


type BodyStart = StartTheBody | SkipTheBody
"""Whether the body of a run starts, read once after its start write."""


@dataclass(frozen=True, slots=True)
class CancelKept:
    """The cancel was recorded before the body: the body will not start."""


@dataclass(frozen=True, slots=True)
class CancelToBody:
    """The cancel was recorded while the body may run: the caller gives it
    to the body now (a shared async seat cancels its seat task). An isolated
    child gave it to the asyncio task of its body already."""


@dataclass(frozen=True, slots=True)
class CancelAfterBody:
    """The cancel was recorded after the body ended: the end of the run is
    cancelled, and nothing is given to the body."""


@dataclass(frozen=True, slots=True)
class CancelEarlierCause:
    """A cancel was requested before, for ``cause``: the request changed
    nothing."""

    cause: CancelCause


@dataclass(frozen=True, slots=True)
class CancelAfterEnd:
    """The end of the run is fixed: the request changed nothing."""


type OperatorCancel = (
    CancelKept | CancelToBody | CancelAfterBody | CancelEarlierCause | CancelAfterEnd
)
"""What an operator cancel did to one run."""


@dataclass(frozen=True, slots=True)
class EndNotCancelled:
    """No operator cancel reached the run before its end: the run reports
    what it returned."""


@dataclass(frozen=True, slots=True)
class EndCancelled:
    """An operator cancel reached the run before its end: the run reports a
    cancelled end, and what the body returned is dropped."""

    body: CancelledBody


type CancelEnd = EndNotCancelled | EndCancelled
"""Whether a run ends as cancelled, fixed once."""


@dataclass(frozen=True, slots=True)
class _NoBodyTask:
    """No asyncio task of the body is watched."""


@dataclass(frozen=True, slots=True)
class _BodyTask:
    """The asyncio task of an ``async def`` body of an isolated child, on
    ``loop``, which the child runs on its main thread."""

    loop: asyncio.AbstractEventLoop
    task: asyncio.Task[Any]


class RunCancel:
    """The cancel of one run: its state (``cell``, which the runtime of the
    run reads) and its phase. Safe between threads."""

    __slots__ = ('_body_task', '_cell', '_lock', '_phase')

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cell = RunCancelCell()
        self._phase = BodyPhase.BEFORE_BODY
        self._body_task: _NoBodyTask | _BodyTask = _NoBodyTask()

    @property
    def cell(self) -> RunCancelCell:
        """The cancel state of the run, for its ``TaskRuntime``."""
        return self._cell

    def phase(self) -> BodyPhase:
        """The phase of the run now."""
        with self._lock:
            return self._phase

    def request_operator(self) -> OperatorCancel:
        """An operator cancelled the task of this run (``CancelRunFrame``).

        Records the cause ``OPERATOR`` unless an earlier cause or the end
        came first. While the body of an isolated child runs on its task
        loop, schedules the cancel of the asyncio task of the body on that
        loop (``call_soon_threadsafe``). Never blocks on the body.
        """
        with self._lock:
            phase = self._phase
            match phase:
                case BodyPhase.ENDED:
                    return CancelAfterEnd()
                case (
                    BodyPhase.BEFORE_BODY
                    | BodyPhase.IN_BODY
                    | BodyPhase.AFTER_BODY
                    | BodyPhase.SKIPPED
                ):
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            match self._cell.request(CancelCause.OPERATOR):
                case CancelAlreadySet(cause=cause):
                    return CancelEarlierCause(cause=cause)
                case CancelSet():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            match phase:
                case BodyPhase.BEFORE_BODY:
                    return CancelKept()
                case BodyPhase.IN_BODY:
                    _cancel_from_other_thread(self._body_task)
                    return CancelToBody()
                case BodyPhase.AFTER_BODY:
                    return CancelAfterBody()
                case BodyPhase.SKIPPED:
                    raise AssertionError(
                        f'a first operator cancel came in phase {phase.value}',
                    )
                case _ as unreachable:
                    assert_never(unreachable)

    def request_deadline(self) -> CancelRequestOutcome:
        """The deadline of the run fired in its child: record the cause
        ``DEADLINE`` unless a cause was recorded first. Gives the body no
        signal: the deadline path of the child does that itself."""
        with self._lock:
            return self._cell.request(CancelCause.DEADLINE)

    def begin_body(self) -> BodyStart:
        """The start write set the row RUNNING: start the body, or skip it
        when an operator cancel came first. Call it once per run, from
        ``BEFORE_BODY``; another phase is a defect: ``AssertionError``."""
        with self._lock:
            match self._phase:
                case BodyPhase.BEFORE_BODY:
                    pass
                case (
                    BodyPhase.IN_BODY
                    | BodyPhase.AFTER_BODY
                    | BodyPhase.SKIPPED
                    | BodyPhase.ENDED
                ):
                    raise AssertionError(f'a body began in phase {self._phase.value}')
                case _ as unreachable:
                    assert_never(unreachable)
            match _requested_by_operator(self._cell.state()):
                case True:
                    self._phase = BodyPhase.SKIPPED
                    return SkipTheBody()
                case False:
                    self._phase = BodyPhase.IN_BODY
                    return StartTheBody()

    def watch_body_task(self, loop: asyncio.AbstractEventLoop, task: asyncio.Task[Any]) -> None:
        """The ``async def`` body of an isolated child runs as ``task`` on
        ``loop``, which the calling thread is about to run. A cancel that
        came before the task existed is given to it at its first step
        (``call_soon``), so the body starts and gets ``CancelledError`` at
        its first ``await``."""
        with self._lock:
            self._body_task = _BodyTask(loop=loop, task=task)
            match self._phase:
                case BodyPhase.IN_BODY:
                    pass
                case (
                    BodyPhase.BEFORE_BODY
                    | BodyPhase.AFTER_BODY
                    | BodyPhase.SKIPPED
                    | BodyPhase.ENDED
                ):
                    raise AssertionError(
                        f'a body task was watched in phase {self._phase.value}',
                    )
                case _ as unreachable:
                    assert_never(unreachable)
            match _requested_by_operator(self._cell.state()):
                case True:
                    _cancel_on_loop_thread(self._body_task)
                case False:
                    pass

    def end_body(self) -> None:
        """The body ended (it returned, raised, or was mapped from a
        cancel). A later cancel changes the end only. Call it once after
        ``StartTheBody``."""
        with self._lock:
            match self._phase:
                case BodyPhase.IN_BODY:
                    self._phase = BodyPhase.AFTER_BODY
                    self._body_task = _NoBodyTask()
                case (
                    BodyPhase.BEFORE_BODY
                    | BodyPhase.AFTER_BODY
                    | BodyPhase.SKIPPED
                    | BodyPhase.ENDED
                ):
                    raise AssertionError(f'a body ended in phase {self._phase.value}')
                case _ as unreachable:
                    assert_never(unreachable)

    def fix_end(self) -> CancelEnd:
        """Fix whether the run ends as cancelled; a later cancel is ignored.
        A run whose body began (or was skipped) and that an operator
        cancelled ends cancelled. A run that stopped before its body, or
        whose start ended it, calls this too: its operator cancel, if any, is
        dropped. Call it once per run."""
        with self._lock:
            phase = self._phase
            self._phase = BodyPhase.ENDED
            self._body_task = _NoBodyTask()
            match phase:
                case BodyPhase.ENDED:
                    raise AssertionError('the end of a run was fixed twice')
                case BodyPhase.BEFORE_BODY:
                    return EndNotCancelled()
                case BodyPhase.SKIPPED:
                    return EndCancelled(body=CancelledBody.NOT_STARTED)
                case BodyPhase.IN_BODY | BodyPhase.AFTER_BODY:
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            match _requested_by_operator(self._cell.state()):
                case True:
                    return EndCancelled(body=CancelledBody.RAN)
                case False:
                    return EndNotCancelled()

    def operator_requested(self) -> bool:
        """Whether the recorded cause is an operator cancel."""
        return _requested_by_operator(self._cell.state())


def _requested_by_operator(state: RunCancelState) -> bool:
    """Whether ``state`` records an operator cancel. Pure."""
    match state:
        case CancelNotRequested():
            return False
        case CancelRequested(cause=cause):
            match cause:
                case CancelCause.OPERATOR:
                    return True
                case CancelCause.DEADLINE:
                    return False
                case _ as unreachable:
                    assert_never(unreachable)
        case _ as unreachable:
            assert_never(unreachable)


def _cancel_from_other_thread(body_task: _NoBodyTask | _BodyTask) -> None:
    """Schedule the cancel of the watched body task on its loop, from a
    thread that does not run the loop (the control thread). No task yet:
    ``watch_body_task`` gives the cancel when the task exists. A closed loop
    means the body ended: nothing is left to cancel."""
    match body_task:
        case _NoBodyTask():
            return
        case _BodyTask(loop=loop, task=task):
            try:
                _ = loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                return
        case _ as unreachable:
            assert_never(unreachable)


def _cancel_on_loop_thread(body_task: _NoBodyTask | _BodyTask) -> None:
    """Schedule the cancel of the watched body task on its loop, from the
    thread that is about to run the loop. The first step of the task runs
    first, so the body starts and gets the cancel at its first ``await``."""
    match body_task:
        case _NoBodyTask():
            return
        case _BodyTask(loop=loop, task=task):
            _ = loop.call_soon(task.cancel)
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class NotThisRun:
    """The child does not run the run that the cancel names now: it ended,
    or it is another run. The cancel is ignored."""


@dataclass(frozen=True, slots=True)
class _NoRunNow:
    """The isolated child runs no task now."""


@dataclass(frozen=True, slots=True)
class _RunNow:
    """The isolated child runs the task ``task_id``."""

    task_id: uuid.UUID
    cancel: RunCancel


class IsolatedRuns:
    """The run of an isolated child, for the control thread, which reads the
    cancel frames while the main thread runs the task. Safe between
    threads."""

    __slots__ = ('_lock', '_now')

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._now: _NoRunNow | _RunNow = _NoRunNow()

    def begin(self, task_id: uuid.UUID) -> RunCancel:
        """Main thread: the run of ``task_id`` starts; its cancel is new. A
        run while another runs is a defect: ``AssertionError``."""
        cancel = RunCancel()
        with self._lock:
            match self._now:
                case _NoRunNow():
                    self._now = _RunNow(task_id=task_id, cancel=cancel)
                case _RunNow(task_id=running):
                    raise AssertionError(
                        f'run {task_id} began while run {running} runs',
                    )
                case _ as unreachable:
                    assert_never(unreachable)
        return cancel

    def end(self) -> None:
        """Main thread: the end of the run was reported; a later cancel is
        ignored."""
        with self._lock:
            self._now = _NoRunNow()

    def request(self, task_id: uuid.UUID) -> OperatorCancel | NotThisRun:
        """Control thread: an operator cancelled the task ``task_id``
        (``RunCancel.request_operator`` of its run). Never blocks on the
        body."""
        with self._lock:
            now = self._now
        match now:
            case _RunNow(task_id=running, cancel=cancel) if running == task_id:
                return cancel.request_operator()
            case _RunNow() | _NoRunNow():
                return NotThisRun()
            case _ as unreachable:
                assert_never(unreachable)
