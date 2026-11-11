"""The serve loop of a shared thread child.

A shared thread child runs up to ``seats`` ``def`` tasks of
``Execution.SHARED_THREAD`` at once. Its main thread takes the frames that
the control thread hands over (``ParentFrameInbox``) and gives each run to a
seat: one of ``seats`` threads that the child starts at boot. A seat that
ends its run takes the next one at once.

The child holds the deadline of each run, on one deadline thread. A thread
cannot be cancelled: at ``timeout_ms`` the child reports the deadline with
``DeadlineFiredFrame``, and the run goes on. The parent then places no new
run on the child, and kills it when no run in it has time left. The end of a
run is fixed when its task returns: ``SeatTimedOut`` after a deadline that
fired, whatever the task returned, else ``SeatFinished``. A run whose task
returned has no deadline left. A ``SeatRefusal`` runs no task code and has no
deadline.

After the return, a run that set its row RUNNING and returned before its
deadline writes its finalizing marker, at most until that deadline; the seat
reports the end after it. A run that timed out writes none: the runner
heartbeats of the worker cover the row until the worker reads the end.

Frames leave through the writer thread of ``FrameWriter``. A seat and the
deadline thread queue the frames of a run while they hold the lock of the
run table, so the deadline report of a run always comes before its result.

C code that holds the GIL stops every thread of the child, the deadline
thread and the writer too: no report leaves the child, and the backstop
timer of the parent kills it.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import functools
import gc
import os
import queue
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Final, NoReturn, Protocol, assert_never

from horsies.core.models.boot_failure import ThreadStartRefused
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.child_main import (
    BAD_PARENT_FRAME_EXIT_CODE,
    _read_rss,
    _send_child_frame,
    exit_after_stop,
    fail_boot,
)
from horsies.core.worker.child_protocol import (
    BootFailedFrame,
    ChildFrame,
    DeadlineFiredFrame,
    InitFrame,
    ReadyFrame,
    RssReading,
    RunFrame,
    SeatEnd,
    SeatFinished,
    SeatRefusal,
    SeatTask,
    SeatTimedOut,
    SharedResultFrame,
    SharedRunFrame,
    StopFrame,
    ThreadSeatTask,
    ThreadSeatWork,
    boot_failed_frame,
)
from horsies.core.worker.shared_child import (
    CHILD_DEFECT_EXIT_CODE,
    FRAMES_PER_SEAT,
    WRITER_CLOSE_TIMEOUT_S,
    FrameWriter,
    MarkerBeforeDeadline,
    MarkerWindow,
    MarkerWithoutDeadline,
    NoMarker,
    SeatReturn,
    SetRunning,
    StoppedBeforeRunning,
    _exit_for_defect,
)
from horsies.core.worker.task_loop import TaskLoop

DEADLINE_WAIT_MAX_S: Final = 5.0
"""The longest wait of the deadline thread before it reads the run table
again. A run that is added or that ends wakes it earlier."""

THREAD_STOP_WAIT_S: Final = 1.0
"""The longest wait for the deadline thread, and for each seat thread, to end
at the stop. They are idle then: the parent sends the stop frame only to a
child with no run in flight."""


class FrameSink(Protocol):
    """Where the run table queues frames: ``FrameWriter``. Neither call blocks."""

    def send(self, frame: ChildFrame) -> None: ...

    def send_result(self, task_name: str, frame: SharedResultFrame) -> None: ...


class ThreadSeatProgram(Protocol):
    """How a shared thread child runs one task, on the calling seat thread."""

    def run(self, frame: SharedRunFrame, work: ThreadSeatWork) -> SeatReturn:
        """Run the task of ``frame``, whose work is ``work``, up to its
        result; write no finalizing marker. An exception that escapes is a
        defect: the child ends at once with ``CHILD_DEFECT_EXIT_CODE``."""
        ...

    def mark_finalizing(self, frame: SharedRunFrame, window: MarkerWindow) -> None:
        """Write the finalizing marker of the run of ``frame``, whose task
        returned, within ``window``. A failed or skipped write is logged
        only; an exception that escapes is a defect, as for ``run``."""
        ...


class _Deadline(Enum):
    """Whether the deadline of a run has fired."""

    PENDING = 'pending'
    FIRED = 'fired'


@dataclass(frozen=True, slots=True)
class _NoDeadline:
    """The run of a ``SeatRefusal``: no task code runs, so no deadline."""


@dataclass(frozen=True, slots=True)
class _DeadlineAt:
    """The deadline of a run fires at ``seconds`` on the clock of the table."""

    seconds: float


@dataclass(frozen=True, slots=True)
class _Returned:
    """The task of the run returned, and ``end`` is the end of the run. The
    run has no deadline left; it ends when its seat ends it."""

    end: SeatEnd


@dataclass(slots=True)
class _ThreadRun:
    """One run in flight. ``state`` turns ``FIRED`` once, and only for a run
    with a deadline, then ``_Returned`` once, when its task returns."""

    frame: SharedRunFrame
    deadline_at: _DeadlineAt | _NoDeadline
    state: _Deadline | _Returned


class _TableState(Enum):
    """Whether the deadline thread serves the table."""

    SERVING = 'serving'
    STOPPED = 'stopped'


def _deadline_at(work: ThreadSeatWork, now: float) -> _DeadlineAt | _NoDeadline:
    """The time on the table clock at which the deadline of ``work`` fires."""
    match work:
        case ThreadSeatTask(timeout_ms=timeout_ms):
            return _DeadlineAt(now + timeout_ms / 1000.0)
        case SeatRefusal():
            return _NoDeadline()
        case _ as unreachable:
            assert_never(unreachable)


def _end_at_return(run: _ThreadRun, returned: SeatReturn) -> SeatEnd:
    """The end of a run whose task returned ``returned``: the timeout after
    a deadline that fired, also when the task returned a value, else what
    the task returned. A second return is a defect."""
    match run.state, returned:
        case _Deadline.FIRED, _:
            return SeatTimedOut()
        case _Deadline.PENDING, SetRunning(wire=wire) | StoppedBeforeRunning(wire=wire):
            ok, result_json, failed_reason = wire
            return SeatFinished(
                ok=ok,
                result_json=result_json,
                failed_reason=failed_reason,
            )
        case _Returned(), _:
            raise AssertionError(f'run {run.frame.dispatch} returned twice')
        case _ as unreachable:
            assert_never(unreachable)


def _marker_window(
    run: _ThreadRun,
    returned: SeatReturn,
    end: SeatEnd,
    now: float,
) -> MarkerWindow | NoMarker:
    """Whether the seat writes the finalizing marker of a run whose task
    returned, and within what time. ``now`` is on the clock of the table.

    Only a run that set its row RUNNING and returned before its deadline
    writes it, at most until that deadline: the end report of the run then
    comes before the backstop of the worker. A run that timed out writes
    none: the worker writes its outcome after the read, as at a kill, and
    its beats cover the row until then.
    """
    match end, returned, run.deadline_at:
        case SeatTimedOut(), _, _:
            return NoMarker()
        case SeatFinished(), StoppedBeforeRunning(), _:
            return NoMarker()
        case SeatFinished(), SetRunning(), _NoDeadline():
            return MarkerWithoutDeadline()
        case SeatFinished(), SetRunning(), _DeadlineAt(seconds=at) if at > now:
            return MarkerBeforeDeadline(seconds=at - now)
        case SeatFinished(), SetRunning(), _DeadlineAt():
            return NoMarker()
        case _ as unreachable:
            assert_never(unreachable)


class ThreadRunTable:
    """The runs in flight of a shared thread child, with their deadlines.

    One lock guards the table. The main thread adds each run, the seat
    thread of a run ends it, and the deadline thread fires the deadlines.
    Each frame of a run is queued on ``writer`` while the lock is held, so
    the frames of a run leave in the order of its events. ``clock`` gives
    seconds that never go back. Holds at most ``seats`` runs.
    """

    def __init__(
        self,
        seats: int,
        writer: FrameSink,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        assert seats >= 1, f'a shared thread child has at least one seat, got {seats}'
        self._seats = seats
        self._writer = writer
        self._clock = clock
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._runs: dict[int, _ThreadRun] = {}
        self._state = _TableState.SERVING

    def add(self, frame: SharedRunFrame, work: ThreadSeatWork) -> None:
        """Hold a new run and start its deadline. ``work`` is ``frame.work``.

        A run when every seat is taken, or a dispatch number in use, is a
        parent defect: the process ends with ``BAD_PARENT_FRAME_EXIT_CODE``.
        """
        with self._lock:
            if len(self._runs) >= self._seats:
                _exit_for_defect(
                    f'parent sent run {frame.dispatch} while all {self._seats} '
                    'seats are taken',
                    BAD_PARENT_FRAME_EXIT_CODE,
                )
            if frame.dispatch in self._runs:
                _exit_for_defect(
                    f'parent sent run {frame.dispatch} twice',
                    BAD_PARENT_FRAME_EXIT_CODE,
                )
            self._runs[frame.dispatch] = _ThreadRun(
                frame=frame,
                deadline_at=_deadline_at(work, self._clock()),
                state=_Deadline.PENDING,
            )
            self._changed.notify_all()

    def returned(self, dispatch: int, returned: SeatReturn) -> MarkerWindow | NoMarker:
        """Fix the end of run ``dispatch``, whose task returned ``returned``
        (``_end_at_return``): the run has no deadline left. Returns whether
        its seat writes the finalizing marker, and within what time
        (``_marker_window``). Called once per run, by its seat thread."""
        with self._lock:
            run = self._runs[dispatch]
            end = _end_at_return(run, returned)
            run.state = _Returned(end)
            self._changed.notify_all()
            return _marker_window(run, returned, end, self._clock())

    def finish(self, dispatch: int, rss: RssReading) -> None:
        """End run ``dispatch``, whose task returned, and queue its result
        frame with the end fixed at the return. Called once per run, by its
        seat thread, after ``returned``."""
        with self._lock:
            run = self._runs.pop(dispatch)
            match run.state:
                case _Returned(end=end):
                    pass
                case _Deadline.PENDING | _Deadline.FIRED:
                    raise AssertionError(
                        f'run {dispatch} ended before its task returned',
                    )
                case _ as unreachable:
                    assert_never(unreachable)
            self._writer.send_result(
                run.frame.task_name,
                SharedResultFrame(
                    dispatch=dispatch,
                    task_id=run.frame.task_id,
                    end=end,
                    rss=rss,
                ),
            )
            self._changed.notify_all()

    def count(self) -> int:
        """The runs in flight."""
        with self._lock:
            return len(self._runs)

    def serve_deadlines(self) -> None:
        """The body of the deadline thread: fire each deadline at its time,
        until ``stop``. Waits at most ``DEADLINE_WAIT_MAX_S`` between reads."""
        with self._lock:
            while True:
                match self._state:
                    case _TableState.STOPPED:
                        return
                    case _TableState.SERVING:
                        pass
                    case _ as unreachable:
                        assert_never(unreachable)
                self._changed.wait(self._fire_due())

    def _fire_due(self) -> float:
        """Fire every pending deadline at or before now, and return the wait
        until the next one, at most ``DEADLINE_WAIT_MAX_S``. Lock held."""
        now = self._clock()
        wait_s = DEADLINE_WAIT_MAX_S
        for dispatch, run in self._runs.items():
            match run.deadline_at:
                case _NoDeadline():
                    continue
                case _DeadlineAt(seconds=deadline_at):
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            match run.state:
                case _Deadline.FIRED | _Returned():
                    continue
                case _Deadline.PENDING if deadline_at <= now:
                    run.state = _Deadline.FIRED
                    self._writer.send(
                        DeadlineFiredFrame(
                            dispatch=dispatch, task_id=run.frame.task_id
                        ),
                    )
                case _Deadline.PENDING:
                    wait_s = min(wait_s, deadline_at - now)
                case _ as unreachable:
                    assert_never(unreachable)
        return wait_s

    def stop(self) -> None:
        """End ``serve_deadlines``."""
        with self._lock:
            self._state = _TableState.STOPPED
            self._changed.notify_all()


@dataclass(frozen=True, slots=True)
class _SeatJob:
    """A run for a seat, with its work narrowed to the thread kind."""

    frame: SharedRunFrame
    work: ThreadSeatWork


@dataclass(frozen=True, slots=True)
class _SeatStop:
    """The seat thread that takes this ends."""


def _start_thread(thread: threading.Thread) -> Result[None, BootFailedFrame]:
    """Start ``thread``. ``Err``: the OS refused it (``ThreadStartRefused``)."""
    try:
        thread.start()
    except RuntimeError as exc:
        return Err(
            boot_failed_frame(ThreadStartRefused(), type(exc).__name__, str(exc)),
        )
    return Ok(None)


class ThreadSeats:
    """``seats`` threads. Each takes the next run, runs it with ``program``,
    and ends it in ``table``. A free thread takes the next run at once.

    An exception that escapes ``program.run`` is a defect: the process ends
    with ``CHILD_DEFECT_EXIT_CODE``.
    """

    def __init__(
        self,
        seats: int,
        table: ThreadRunTable,
        program: ThreadSeatProgram,
        read_rss: Callable[[], RssReading],
    ) -> None:
        self._table = table
        self._program = program
        self._read_rss = read_rss
        self._jobs: queue.SimpleQueue[_SeatJob | _SeatStop] = queue.SimpleQueue()
        self._threads = tuple(
            threading.Thread(
                target=self._serve,
                name=f'horsies-seat-{index}',
                daemon=True,
            )
            for index in range(seats)
        )

    def start(self) -> Result[None, BootFailedFrame]:
        """Start every seat thread. ``Err``: the OS refused one
        (``ThreadStartRefused``); the threads already started stay idle."""
        for thread in self._threads:
            match _start_thread(thread):
                case Ok():
                    pass
                case Err() as refused:
                    return refused
        return Ok(None)

    def give(self, frame: SharedRunFrame, work: ThreadSeatWork) -> None:
        """Hand a run to the next free seat thread. Never blocks."""
        self._jobs.put(_SeatJob(frame, work))

    def stop(self, timeout_s: float) -> None:
        """End every seat thread. Waits at most ``timeout_s`` for each."""
        for _ in self._threads:
            self._jobs.put(_SeatStop())
        for thread in self._threads:
            thread.join(timeout_s)

    def _serve(self) -> None:
        while True:
            match self._jobs.get():
                case _SeatStop():
                    return
                case _SeatJob(frame=frame, work=work):
                    self._run(frame, work)
                case _ as unreachable:
                    assert_never(unreachable)

    def _run(self, frame: SharedRunFrame, work: ThreadSeatWork) -> None:
        """Run one task, fix its end at its return, write its finalizing
        marker when one is due, then end the run."""
        returned = self._run_or_exit(frame, work)
        match self._table.returned(frame.dispatch, returned):
            case NoMarker():
                pass
            case MarkerBeforeDeadline() | MarkerWithoutDeadline() as window:
                self._mark_or_exit(frame, window)
            case _ as unreachable:
                assert_never(unreachable)
        self._table.finish(frame.dispatch, self._read_rss())

    def _run_or_exit(self, frame: SharedRunFrame, work: ThreadSeatWork) -> SeatReturn:
        try:
            return self._program.run(frame, work)
        except BaseException as exc:
            _exit_for_defect(
                f'task {frame.task_id} escaped the seat runner: '
                f'{type(exc).__name__}: {exc}',
                CHILD_DEFECT_EXIT_CODE,
            )

    def _mark_or_exit(self, frame: SharedRunFrame, window: MarkerWindow) -> None:
        try:
            self._program.mark_finalizing(frame, window)
        except BaseException as exc:
            _exit_for_defect(
                f'the finalizing marker of task {frame.task_id} escaped the '
                f'seat runner: {type(exc).__name__}: {exc}',
                CHILD_DEFECT_EXIT_CODE,
            )


def _seat_work_of(frame: SharedRunFrame) -> ThreadSeatWork:
    """The work of a run for a thread seat. The task work of a shared async
    child is a parent defect: the process ends with
    ``BAD_PARENT_FRAME_EXIT_CODE``."""
    match frame.work:
        case ThreadSeatTask() | SeatRefusal() as work:
            return work
        case SeatTask():
            _exit_for_defect(
                f'parent sent run {frame.dispatch} of a shared async child to '
                'a shared thread child',
                BAD_PARENT_FRAME_EXIT_CODE,
            )
        case _ as unreachable:
            assert_never(unreachable)


class RunTaker(Protocol):
    """Where ``serve_runs`` hands each run: ``ThreadSeats``. Never blocks."""

    def give(self, frame: SharedRunFrame, work: ThreadSeatWork) -> None: ...


def serve_runs(
    inbox: ParentFrameInbox,
    table: ThreadRunTable,
    seats: RunTaker,
) -> None:
    """Take frames from ``inbox`` until the stop frame. Returns then.

    A run frame starts its deadline in ``table`` and goes to a seat. A stop
    frame while runs are in flight, a frame of an isolated child, or a run
    with the work of a shared async child is a parent defect: the process
    ends with ``BAD_PARENT_FRAME_EXIT_CODE``.
    """
    while True:
        match inbox.get():
            case SharedRunFrame() as frame:
                work = _seat_work_of(frame)
                table.add(frame, work)
                seats.give(frame, work)
            case StopFrame():
                in_flight = table.count()
                if in_flight:
                    _exit_for_defect(
                        f'parent sent a stop frame while {in_flight} runs are '
                        'in flight',
                        BAD_PARENT_FRAME_EXIT_CODE,
                    )
                return
            case RunFrame():
                _exit_for_defect(
                    'parent sent an isolated run frame to a shared thread child',
                    BAD_PARENT_FRAME_EXIT_CODE,
                )
            case _ as unreachable:
                assert_never(unreachable)


def _start_or_fail_boot(
    sock: socket.socket,
    start: Callable[[], Result[None, BootFailedFrame]],
) -> None:
    match start():
        case Ok():
            pass
        case Err(failed):
            fail_boot(sock, failed)


def serve_thread(
    sock: socket.socket,
    init: InitFrame,
    work: ParentFrameInbox,
    task_loop: TaskLoop,
    seats: int,
    program: ThreadSeatProgram,
) -> NoReturn:
    """Run a shared thread child from its threads to its exit.

    The sync boot ran (``child_main.boot_app``): the app is imported, the
    start hooks ran, and the sync pool of the child is open. ``task_loop``
    is not created: a thread child runs no ``async def`` task. The writer
    thread, the deadline thread and the ``seats`` seat threads start first;
    a refused thread fails the boot (``ThreadStartRefused``). After the stop
    frame the threads end, each bounded by ``THREAD_STOP_WAIT_S``, the writer
    drains, bounded by ``WRITER_CLOSE_TIMEOUT_S``, and ``exit_after_stop``
    ends the process. ``init`` is not read again.
    """
    _ = init
    writer = FrameWriter(
        functools.partial(_send_child_frame, sock),
        capacity=FRAMES_PER_SEAT * seats + 1,
    )
    table = ThreadRunTable(seats, writer)
    deadlines = threading.Thread(
        target=table.serve_deadlines,
        name='horsies-child-deadlines',
        daemon=True,
    )
    seat_threads = ThreadSeats(seats, table, program, _read_rss)
    _start_or_fail_boot(sock, writer.start)
    _start_or_fail_boot(sock, functools.partial(_start_thread, deadlines))
    _start_or_fail_boot(sock, seat_threads.start)
    gc.collect()
    writer.send(ReadyFrame(pid=os.getpid(), rss=_read_rss()))
    serve_runs(work, table, seat_threads)
    table.stop()
    deadlines.join(THREAD_STOP_WAIT_S)
    seat_threads.stop(THREAD_STOP_WAIT_S)
    writer.close(WRITER_CLOSE_TIMEOUT_S)
    exit_after_stop(task_loop.close())
