"""The serve loop of a shared async child.

A shared async child runs its task loop on its main thread from its start
steps to its stop frame. Run frames arrive while the loop runs: the control
thread hands each frame to the loop through ``ParentFrameInbox``, and the
task starts at once on a free seat, as an asyncio task. Up to ``seats``
tasks run at once.

Each run runs in a context that holds its mark (``run_mark``), so each task
and each default executor item that the run starts carries the mark. A run
ends when its coroutine has ended and no work with its mark is alive; only
then does the child report the end with ``SharedResultFrame``. A
``SeatRefusal`` runs no task code and has no deadline.

The child holds the deadline of each task. The stop of a run, in order:

- T (``timeout_ms`` after the start): cancel the task, report
  ``DeadlineFiredFrame``. The child then waits for the coroutine and for the
  marked work. From here the end is ``SeatTimedOut``, whatever the task
  returned.
- T + G (``cancel_grace_ms`` later), when anything of the run is alive:
  report ``SecondCancelFrame``, cancel the task and its marked asyncio
  tasks, then cancel the marked tasks found again in ``EXTRA_CANCEL_ROUNDS``
  more rounds, ``CANCEL_ROUND_INTERVAL_S`` apart.
- When anything of the run is still alive one interval after the last
  round: report ``RunStuckFrame``. The run keeps its seat. The parent
  closes the child, and kills it when every run left in it is stuck.

A ``LoopWatchdog`` thread reports a loop that has not turned for the block
limit (``LoopBlockedFrame``); the parent kills the child at once.

No frame write blocks the loop. The loop hands each frame to a writer thread
through a bounded queue, and that thread encodes and writes it. Each run
gives at most ``FRAMES_PER_SEAT`` frames, and the parent sends a run only to
a seat that it saw free, so the queue holds at most that many frames per
seat, and ``FRAMES_PER_CHILD`` more.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import contextvars
import functools
import gc
import logging
import os
import queue
import socket
import threading
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, NoReturn, Protocol, assert_never

from horsies.core.logging import get_logger
from horsies.core.models.boot_failure import ThreadStartRefused
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_inbox import NoFrameYet, ParentFrameInbox
from horsies.core.worker.child_main import (
    BAD_PARENT_FRAME_EXIT_CODE,
    _bounded_result,
    _read_rss,
    _send_child_frame,
    exit_after_stop,
    fail_boot,
    flush_output,
)
from horsies.core.worker.child_protocol import (
    MAX_LEFTOVER_TASK_NAMES,
    BootFailedFrame,
    ChildFrame,
    DeadlineFiredFrame,
    InitFrame,
    ReadyFrame,
    RssReading,
    RunFrame,
    RunStuckFrame,
    SecondCancelFrame,
    SeatEnd,
    SeatFinished,
    SeatRefusal,
    SeatTask,
    SeatTimedOut,
    SharedResultFrame,
    SharedRunFrame,
    StopFrame,
    ThreadSeatTask,
    WireResult,
    boot_failed_frame,
)
from horsies.core.worker.loop_watchdog import LoopWatchdog
from horsies.core.worker.run_mark import RUN_MARK, MarkedWork, RunMark, ScanWhen
from horsies.core.worker.task_loop import TaskLoop, _coroutine_name

logger = get_logger('worker')

FRAMES_PER_SEAT: Final = 4
"""The most frames that one run gives: a deadline report, a second cancel
report, a stuck report, and a result."""

FRAMES_PER_CHILD: Final = 2
"""The frames of a child outside its runs: the ready frame, and one report of
a blocked loop."""

EXTRA_CANCEL_ROUNDS: Final = 2
"""The cancel rounds after the second cancel. In the measured set one more
round 0.1 s after the second cancel ended every asyncio leftover within
106 ms: an asyncpg pool refill task, and SQLAlchemy closes that turn their
first cancel into a bounded cleanup of their own."""

CANCEL_ROUND_INTERVAL_S: Final = 0.1
"""The time between two cancel rounds, and from the last round to the stuck
check."""

WRITER_CLOSE_TIMEOUT_S: Final = 1.0
"""The longest wait for the writer thread at the stop. The parent read every
result before it sent the stop frame, so the queue is empty then."""

CHILD_DEFECT_EXIT_CODE: Final = 1
"""A defect inside the child, as when a task escapes the task runner."""


class StartedSeats(Protocol):
    """The seats of a shared async child after its start steps."""

    async def run(self, frame: SharedRunFrame) -> WireResult:
        """Run the task of ``frame`` on a seat. Returns its wire result.

        A cancel of the calling task ends the run: the cancel propagates, or
        the run returns, whichever the task body does. Any other exception
        that escapes is a defect.
        """
        ...

    async def stop(self) -> None:
        """Release what ``start`` opened, bounded. Runs after the last task."""
        ...


class SeatProgram(Protocol):
    """How a shared async child starts its seats, on its task loop."""

    async def start(self, init: InitFrame) -> Result[StartedSeats, BootFailedFrame]:
        """Run the start steps on the task loop, before the ready frame.

        ``Err``: the boot fails with this frame. A start hook that fails
        ends the process itself, as at the sync boot.
        """
        ...


def _exit_for_defect(message: str, exit_code: int) -> NoReturn:
    """Log a defect, flush, and end the process at once."""
    logger.critical(message)
    logging.shutdown()
    flush_output()
    os._exit(exit_code)


@dataclass(frozen=True, slots=True)
class _WriterStop:
    """The writer thread ends after the frames before this one."""


@dataclass(frozen=True, slots=True)
class _ResultToWrite:
    """A result frame, and the name of its task for the size bound."""

    task_name: str
    frame: SharedResultFrame


def bounded_result_frame(task_name: str, frame: SharedResultFrame) -> SharedResultFrame:
    """Make a result frame safe to write: a result too large to store becomes
    a typed failure, and a long failure reason is cut, as for an isolated
    child. A timed-out end has no result and passes unchanged. Pure."""
    match frame.end:
        case SeatFinished(ok=ok, result_json=result_json, failed_reason=reason):
            bounded_ok, bounded_json, bounded_reason = _bounded_result(
                task_name,
                (ok, result_json, reason),
            )
            return SharedResultFrame(
                dispatch=frame.dispatch,
                task_id=frame.task_id,
                end=SeatFinished(bounded_ok, bounded_json, bounded_reason),
                rss=frame.rss,
            )
        case SeatTimedOut():
            return frame
        case _ as unreachable:
            assert_never(unreachable)


class FrameWriter:
    """Writes child frames from a thread of its own, in the order they are sent.

    ``send`` and ``send_result`` put a frame in a queue of ``capacity``
    frames and return at once. The thread bounds each result, then encodes
    and writes each frame with ``write``, so no encode of a large result runs
    on the loop. A full queue is a defect of the caller: the process ends at
    once. A failed write ends the process in ``write`` (the parent is gone).
    """

    def __init__(self, write: Callable[[ChildFrame], None], capacity: int) -> None:
        assert capacity >= 1, f'a frame writer holds at least one frame, got {capacity}'
        self._write = write
        self._capacity = capacity
        # One place more, for the stop marker.
        self._queue: queue.Queue[ChildFrame | _ResultToWrite | _WriterStop] = (
            queue.Queue(capacity + 1)
        )
        self._thread = threading.Thread(
            target=self._run,
            name='horsies-child-writer',
            daemon=True,
        )

    def start(self) -> Result[None, BootFailedFrame]:
        """Start the thread. ``Err``: the OS refused it (``ThreadStartRefused``)."""
        try:
            self._thread.start()
        except RuntimeError as exc:
            return Err(
                boot_failed_frame(ThreadStartRefused(), type(exc).__name__, str(exc)),
            )
        return Ok(None)

    def send(self, frame: ChildFrame) -> None:
        """Queue one frame. Never blocks."""
        self._put(frame)

    def send_result(self, task_name: str, frame: SharedResultFrame) -> None:
        """Queue one result frame of task ``task_name``. Never blocks."""
        self._put(_ResultToWrite(task_name, frame))

    def _put(self, item: ChildFrame | _ResultToWrite) -> None:
        if self._queue.qsize() >= self._capacity:
            _exit_for_defect(
                f'the frame writer holds {self._capacity} frames, its bound',
                CHILD_DEFECT_EXIT_CODE,
            )
        self._queue.put_nowait(item)

    def close(self, timeout_s: float) -> None:
        """Write what is queued, then end the thread. Waits at most ``timeout_s``."""
        self._queue.put_nowait(_WriterStop())
        self._thread.join(timeout_s)

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            match item:
                case _WriterStop():
                    return
                case _ResultToWrite(task_name=task_name, frame=frame):
                    self._write(bounded_result_frame(task_name, frame))
                case _:
                    self._write(item)


class _Stop(Enum):
    """How far the stop of a run went. A run with a deadline moves forward
    through these, one step at a time; a refusal stays ``NO_DEADLINE``."""

    NO_DEADLINE = 'no_deadline'
    BEFORE_DEADLINE = 'before_deadline'
    CANCELLED = 'cancelled'
    SECOND_CANCEL = 'second_cancel'
    STUCK = 'stuck'


@dataclass(frozen=True, slots=True)
class _NoTimer:
    """No step of the stop of the run is due."""


@dataclass(slots=True)
class _Seat:
    """One run in flight.

    ``stop`` moves forward only. ``timer`` is the next step of the stop.
    ``watched`` holds the marked tasks whose end the run waits for: a task
    leaves it when it ends, and the run checks its end when it is empty.
    """

    frame: SharedRunFrame
    mark: RunMark
    task: asyncio.Task[WireResult]
    stop: _Stop
    timer: asyncio.TimerHandle | _NoTimer
    watched: set[asyncio.Task[Any]]


class SeatLoop:
    """The runs of one shared async child, on its task loop.

    Build it on the running task loop. ``seats`` is the most runs at once.
    ``read_rss`` reads the RSS of the child for each result frame.
    ``install_marks`` sets the task factory and the default executor that
    mark the work of each run: call it once, before the start hooks.
    """

    def __init__(
        self,
        seats: int,
        writer: FrameWriter,
        read_rss: Callable[[], RssReading],
    ) -> None:
        self._capacity = seats
        self._writer = writer
        self._read_rss = read_rss
        self._loop = asyncio.get_running_loop()
        self._marked = MarkedWork(self._loop, self._item_done)
        self._seats: dict[int, _Seat] = {}

    def install_marks(self) -> None:
        """Mark the work of each run from now on (``MarkedWork.install``)."""
        self._marked.install()

    async def serve(self, inbox: ParentFrameInbox, started: StartedSeats) -> None:
        """Take frames from ``inbox`` until the stop frame. Returns then.

        Each run frame runs on ``started``. A run frame when every seat is
        taken, a dispatch number in use, a stop frame while runs are in
        flight, or a frame of an isolated child is a parent defect: the
        process ends with ``BAD_PARENT_FRAME_EXIT_CODE``.
        """
        arrived = asyncio.Event()
        inbox.call_on_put(
            functools.partial(_wake_from_thread, self._loop, arrived),
        )
        while True:
            frame = await _next_frame(inbox, arrived)
            match frame:
                case SharedRunFrame():
                    self._start(frame, started)
                case StopFrame():
                    if self._seats:
                        _exit_for_defect(
                            f'parent sent a stop frame while {len(self._seats)} '
                            'runs are in flight',
                            BAD_PARENT_FRAME_EXIT_CODE,
                        )
                    return
                case RunFrame():
                    _exit_for_defect(
                        'parent sent an isolated run frame to a shared async child',
                        BAD_PARENT_FRAME_EXIT_CODE,
                    )
                case _ as unreachable:
                    assert_never(unreachable)

    def _start(self, frame: SharedRunFrame, started: StartedSeats) -> None:
        """Start the run of ``frame`` on a free seat, in a context with its
        mark, with the deadline of a ``SeatTask``."""
        if len(self._seats) >= self._capacity:
            _exit_for_defect(
                f'parent sent run {frame.dispatch} while all {self._capacity} '
                'seats are taken',
                BAD_PARENT_FRAME_EXIT_CODE,
            )
        if frame.dispatch in self._seats:
            _exit_for_defect(
                f'parent sent run {frame.dispatch} twice',
                BAD_PARENT_FRAME_EXIT_CODE,
            )
        mark = RunMark(dispatch=frame.dispatch, task_id=frame.task_id)
        self._marked.open(mark)
        context = contextvars.copy_context()
        _ = context.run(RUN_MARK.set, mark)
        task = self._loop.create_task(
            started.run(frame),
            name=f'horsies-seat-{frame.dispatch}',
            context=context,
        )
        stop: _Stop
        timer: asyncio.TimerHandle | _NoTimer
        match frame.work:
            case SeatTask(timeout_ms=timeout_ms):
                stop = _Stop.BEFORE_DEADLINE
                timer = self._loop.call_later(
                    timeout_ms / 1000.0,
                    self._fire,
                    frame.dispatch,
                )
            case SeatRefusal():
                stop = _Stop.NO_DEADLINE
                timer = _NoTimer()
            case ThreadSeatTask():
                _exit_for_defect(
                    f'parent sent run {frame.dispatch} of a shared thread child '
                    'to a shared async child',
                    BAD_PARENT_FRAME_EXIT_CODE,
                )
            case _ as unreachable:
                assert_never(unreachable)
        self._seats[frame.dispatch] = _Seat(frame, mark, task, stop, timer, set())
        task.add_done_callback(functools.partial(self._finish, frame.dispatch))

    def _ended(self, seat: _Seat) -> bool:
        """Whether the coroutine of the run and all its marked work ended."""
        return (
            seat.task.done()
            and self._marked.alive(
                seat.mark,
                seat.task,
                ScanWhen.RECORD_EMPTY,
            ).is_empty()
        )

    def _fire(self, dispatch: int) -> None:
        """The deadline of run ``dispatch`` (T): cancel its task, report the
        cancel, and start the cancel grace.

        A run whose coroutine and marked work already ended keeps its
        result: a pending callback reports it.
        """
        seat = self._seats[dispatch]
        assert seat.stop is _Stop.BEFORE_DEADLINE, f'run {dispatch} fired twice'
        if self._ended(seat):
            seat.timer = _NoTimer()
            self._check_end(dispatch)
            return
        seat.stop = _Stop.CANCELLED
        if not seat.task.done():
            seat.task.cancel()
        self._writer.send(
            DeadlineFiredFrame(dispatch=dispatch, task_id=seat.frame.task_id),
        )
        seat.timer = self._loop.call_later(
            _grace_s(seat.frame),
            self._second_cancel,
            dispatch,
        )

    def _second_cancel(self, dispatch: int) -> None:
        """The cancel grace of run ``dispatch`` ran out (T + G). When anything
        of the run is alive: report it, and start the cancel rounds."""
        seat = self._seats[dispatch]
        if self._ended(seat):
            seat.timer = _NoTimer()
            self._check_end(dispatch)
            return
        seat.stop = _Stop.SECOND_CANCEL
        self._writer.send(
            SecondCancelFrame(dispatch=dispatch, task_id=seat.frame.task_id),
        )
        self._cancel_round(dispatch, 0)

    def _cancel_round(self, dispatch: int, rounds_sent: int) -> None:
        """Cancel the task of the run and the marked tasks alive now. The
        rounds after the second cancel are bounded by
        ``EXTRA_CANCEL_ROUNDS``; the stuck check follows the last one."""
        seat = self._seats[dispatch]
        alive = self._marked.alive(seat.mark, seat.task, ScanWhen.ALWAYS)
        if seat.task.done() and alive.is_empty():
            seat.timer = _NoTimer()
            self._check_end(dispatch)
            return
        if not seat.task.done():
            seat.task.cancel()
        for task in alive.tasks:
            task.cancel()
        self._watch(dispatch, seat, alive.tasks)
        if rounds_sent < EXTRA_CANCEL_ROUNDS:
            seat.timer = self._loop.call_later(
                CANCEL_ROUND_INTERVAL_S,
                self._cancel_round,
                dispatch,
                rounds_sent + 1,
            )
        else:
            seat.timer = self._loop.call_later(
                CANCEL_ROUND_INTERVAL_S,
                self._declare_stuck,
                dispatch,
            )

    def _declare_stuck(self, dispatch: int) -> None:
        """After the last cancel round: report a run that is still alive as
        stuck. It keeps its seat until it ends or the child is killed."""
        seat = self._seats[dispatch]
        seat.timer = _NoTimer()
        alive = self._marked.alive(seat.mark, seat.task, ScanWhen.ALWAYS)
        own = () if seat.task.done() else (seat.task,)
        tasks = (*own, *alive.tasks)
        if not tasks and alive.executor_items == 0:
            self._check_end(dispatch)
            return
        seat.stop = _Stop.STUCK
        names = sorted(_coroutine_name(task) for task in tasks)
        self._writer.send(
            RunStuckFrame(
                dispatch=dispatch,
                task_id=seat.frame.task_id,
                tasks=len(tasks),
                executor_items=alive.executor_items,
                names=tuple(names[:MAX_LEFTOVER_TASK_NAMES]),
            ),
        )

    def _watch(
        self,
        dispatch: int,
        seat: _Seat,
        tasks: frozenset[asyncio.Task[Any]],
    ) -> None:
        """Wait for the end of each of ``tasks`` that is not watched yet."""
        for task in tasks - seat.watched:
            seat.watched.add(task)
            task.add_done_callback(functools.partial(self._watched_done, dispatch))

    def _watched_done(self, dispatch: int, task: asyncio.Task[Any]) -> None:
        """A watched task ended. A run that is no longer in flight ended
        before this callback ran."""
        seat = self._seats.get(dispatch)
        if seat is None:
            return
        seat.watched.discard(task)
        if not seat.watched:
            self._check_end(dispatch)

    def _item_done(self, mark: RunMark) -> None:
        """A marked work item ended (``MarkedWork``)."""
        seat = self._seats.get(mark.dispatch)
        if seat is None or seat.mark != mark:
            return
        if not seat.watched:
            self._check_end(mark.dispatch)

    def _finish(self, dispatch: int, task: asyncio.Task[WireResult]) -> None:
        """The task of run ``dispatch`` has ended. Before its deadline, a
        cancel or an exception that escaped the seat runner is a defect. A
        step of the stop can report the run before this callback runs."""
        seat = self._seats.get(dispatch)
        if seat is None:
            return
        match seat.stop:
            case _Stop.NO_DEADLINE | _Stop.BEFORE_DEADLINE:
                _exit_for_end_defect(seat, task)
            case _Stop.CANCELLED | _Stop.SECOND_CANCEL | _Stop.STUCK:
                pass
            case _ as unreachable:
                assert_never(unreachable)
        self._check_end(dispatch)

    def _check_end(self, dispatch: int) -> None:
        """Report the end of run ``dispatch`` when its coroutine has ended
        and no marked work of it is alive; else wait for that work. A run
        that is no longer in flight was reported already."""
        seat = self._seats.get(dispatch)
        if seat is None or not seat.task.done():
            return
        alive = self._marked.alive(seat.mark, seat.task, ScanWhen.RECORD_EMPTY)
        if not alive.is_empty():
            self._watch(dispatch, seat, alive.tasks)
            return
        match seat.timer:
            case asyncio.TimerHandle():
                seat.timer.cancel()
            case _NoTimer():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        del self._seats[dispatch]
        self._marked.close(seat.mark)
        self._writer.send_result(
            seat.frame.task_name,
            SharedResultFrame(
                dispatch=dispatch,
                task_id=seat.frame.task_id,
                end=_end_of(seat),
                rss=self._read_rss(),
            ),
        )


async def _next_frame(
    inbox: ParentFrameInbox,
    arrived: asyncio.Event,
) -> SharedRunFrame | StopFrame | RunFrame:
    """The next frame of the inbox. Waits on ``arrived``, which each later
    ``put`` sets, so no frame is missed between the check and the wait."""
    while True:
        match inbox.take():
            case NoFrameYet():
                arrived.clear()
                match inbox.take():
                    case NoFrameYet():
                        await arrived.wait()
                    case SharedRunFrame() | StopFrame() | RunFrame() as frame:
                        return frame
                    case _ as unreachable:
                        assert_never(unreachable)
            case SharedRunFrame() | StopFrame() | RunFrame() as frame:
                return frame
            case _ as unreachable:
                assert_never(unreachable)


def _grace_s(frame: SharedRunFrame) -> float:
    """The cancel grace of a run with a deadline, in seconds."""
    match frame.work:
        case SeatTask(cancel_grace_ms=cancel_grace_ms):
            return cancel_grace_ms / 1000.0
        case SeatRefusal() | ThreadSeatTask():
            raise AssertionError(
                f'run {frame.dispatch} has no cancel grace on a shared async child',
            )
        case _ as unreachable:
            assert_never(unreachable)


def _exit_for_end_defect(seat: _Seat, task: asyncio.Task[WireResult]) -> None:
    """End the process when the task of a run before its deadline was
    cancelled, or when an exception escaped the seat runner: both are
    defects."""
    if task.cancelled():
        _exit_for_defect(
            f'run {seat.frame.dispatch} (task {seat.frame.task_id}) was cancelled '
            'without its deadline',
            CHILD_DEFECT_EXIT_CODE,
        )
    error = task.exception()
    if error is not None:
        _exit_for_defect(
            f'task {seat.frame.task_id} escaped the seat runner: '
            f'{type(error).__name__}: {error}',
            CHILD_DEFECT_EXIT_CODE,
        )


def _end_of(seat: _Seat) -> SeatEnd:
    """The end of a run whose coroutine and marked work ended. After a
    deadline that fired, the end is the timeout, also when the task returned
    a value."""
    match seat.stop:
        case _Stop.CANCELLED | _Stop.SECOND_CANCEL | _Stop.STUCK:
            return SeatTimedOut()
        case _Stop.NO_DEADLINE | _Stop.BEFORE_DEADLINE:
            _exit_for_end_defect(seat, seat.task)
            ok, result_json, failed_reason = seat.task.result()
            return SeatFinished(
                ok=ok,
                result_json=result_json,
                failed_reason=failed_reason,
            )
        case _ as unreachable:
            assert_never(unreachable)


def _wake_from_thread(loop: asyncio.AbstractEventLoop, arrived: asyncio.Event) -> None:
    """Set ``arrived`` on ``loop`` from the control thread. A closed loop
    means the child is past its stop: nothing waits any more."""
    try:
        loop.call_soon_threadsafe(arrived.set)
    except RuntimeError:
        return


async def _live(
    sock: socket.socket,
    init: InitFrame,
    work: ParentFrameInbox,
    seats: int,
    program: SeatProgram,
    writer: FrameWriter,
) -> None:
    """Mark the work of each run, start the seats, start the loop watchdog,
    report ready, serve until the stop frame, then stop the watchdog and
    the seats. Runs on the task loop for the whole life of the child."""
    seat_loop = SeatLoop(seats, writer, _read_rss)
    seat_loop.install_marks()
    match await program.start(init):
        case Ok(started):
            pass
        case Err(failed):
            fail_boot(sock, failed)
    gc.collect()
    watchdog = LoopWatchdog(asyncio.get_running_loop(), writer.send)
    match watchdog.start():
        case Ok():
            pass
        case Err(failed):
            fail_boot(sock, failed)
    writer.send(ReadyFrame(pid=os.getpid(), rss=_read_rss()))
    await seat_loop.serve(work, started)
    watchdog.stop()
    await started.stop()


def serve_shared(
    sock: socket.socket,
    init: InitFrame,
    work: ParentFrameInbox,
    task_loop: TaskLoop,
    seats: int,
    program: SeatProgram,
) -> NoReturn:
    """Run a shared async child from its start steps to its exit.

    ``task_loop`` is open. The writer thread starts first; a refused thread
    fails the boot (``ThreadStartRefused``). The marks of the runs are set
    before ``program.start``, which runs on the loop; an ``Err`` fails the
    boot, before the ready frame, and so does a refused watchdog thread.
    After the stop frame the watchdog and the seats stop, the loop closes
    (bounded as for an isolated child), the writer drains, bounded by
    ``WRITER_CLOSE_TIMEOUT_S``, and ``exit_after_stop`` ends the process.
    """
    writer = FrameWriter(
        functools.partial(_send_child_frame, sock),
        capacity=FRAMES_PER_SEAT * seats + FRAMES_PER_CHILD,
    )
    match writer.start():
        case Ok():
            pass
        case Err(failed):
            fail_boot(sock, failed)
    task_loop.run(_live(sock, init, work, seats, program, writer))
    executor = task_loop.close()
    writer.close(WRITER_CLOSE_TIMEOUT_S)
    exit_after_stop(executor)
