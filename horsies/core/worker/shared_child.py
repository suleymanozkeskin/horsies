"""The serve loop of a shared async child.

A shared async child runs its task loop on its main thread from its start
steps to its stop frame. Run frames arrive while the loop runs: the control
thread hands each frame to the loop through ``ParentFrameInbox``, and the
task starts at once on a free seat, as an asyncio task. Up to ``seats``
tasks run at once.

The child holds the deadline of each task. ``timeout_ms`` after the task
starts, the loop cancels the task (``Task.cancel``) and reports the cancel
with ``DeadlineFiredFrame``. A ``SeatRefusal`` runs no task code and has no
deadline. When the coroutine has ended, the child reports
the end with ``SharedResultFrame``: ``SeatTimedOut`` after a deadline that
fired, whatever the task returned, else ``SeatFinished`` with its result.

No frame write blocks the loop. The loop hands each frame to a writer thread
through a bounded queue, and that thread encodes and writes it. Each run
gives at most two frames (a deadline report and a result), and the parent
sends a run only to a seat that it saw free, so the queue never holds more
than two frames per seat.

Not built here, and left to later steps: a second cancel after the cancel
grace, the close of a child whose task does not stop, a watchdog of a
blocked loop, and a mark on the work that a task starts. A task that ignores
its cancel keeps its seat until it ends or the child is killed.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
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
from typing import Final, NoReturn, Protocol, assert_never

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
    WireResult,
    boot_failed_frame,
)
from horsies.core.worker.task_loop import TaskLoop

logger = get_logger('worker')

FRAMES_PER_SEAT: Final = 2
"""The most frames that one run gives: a deadline report and a result."""

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


class _Deadline(Enum):
    """Whether the deadline of a run has fired."""

    PENDING = 'pending'
    FIRED = 'fired'


@dataclass(frozen=True, slots=True)
class _NoDeadline:
    """The run of a ``SeatRefusal``: no task code runs, so no deadline."""


@dataclass(slots=True)
class _Seat:
    """One run in flight. ``deadline_state`` turns ``FIRED`` once, and only
    for a run with a timer."""

    frame: SharedRunFrame
    task: asyncio.Task[WireResult]
    deadline: asyncio.TimerHandle | _NoDeadline
    deadline_state: _Deadline


class SeatLoop:
    """The runs of one shared async child, on its task loop.

    ``seats`` is the most runs at once. ``read_rss`` reads the RSS of the
    child for each result frame.
    """

    def __init__(
        self,
        seats: int,
        started: StartedSeats,
        writer: FrameWriter,
        read_rss: Callable[[], RssReading],
    ) -> None:
        self._capacity = seats
        self._started = started
        self._writer = writer
        self._read_rss = read_rss
        self._seats: dict[int, _Seat] = {}

    async def serve(self, inbox: ParentFrameInbox) -> None:
        """Take frames from ``inbox`` until the stop frame. Returns then.

        A run frame when every seat is taken, a dispatch number in use, a
        stop frame while runs are in flight, or a frame of an isolated child
        is a parent defect: the process ends with
        ``BAD_PARENT_FRAME_EXIT_CODE``.
        """
        arrived = asyncio.Event()
        inbox.call_on_put(
            functools.partial(_wake_from_thread, asyncio.get_running_loop(), arrived),
        )
        while True:
            frame = await _next_frame(inbox, arrived)
            match frame:
                case SharedRunFrame():
                    self._start(frame)
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

    def _start(self, frame: SharedRunFrame) -> None:
        """Start the run of ``frame`` on a free seat, with the deadline of a
        ``SeatTask``."""
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
        loop = asyncio.get_running_loop()
        task = loop.create_task(
            self._started.run(frame),
            name=f'horsies-seat-{frame.dispatch}',
        )
        deadline: asyncio.TimerHandle | _NoDeadline
        match frame.work:
            case SeatTask(timeout_ms=timeout_ms):
                deadline = loop.call_later(
                    timeout_ms / 1000.0,
                    self._fire,
                    frame.dispatch,
                )
            case SeatRefusal():
                deadline = _NoDeadline()
            case _ as unreachable:
                assert_never(unreachable)
        self._seats[frame.dispatch] = _Seat(frame, task, deadline, _Deadline.PENDING)
        task.add_done_callback(functools.partial(self._finish, frame.dispatch))

    def _fire(self, dispatch: int) -> None:
        """The deadline of run ``dispatch``: cancel its task, report the cancel.

        A run whose task already ended keeps its result: the done callback
        has not run yet, and it reports the result.
        """
        seat = self._seats.get(dispatch)
        if seat is None or seat.task.done():
            return
        seat.deadline_state = _Deadline.FIRED
        seat.task.cancel()
        self._writer.send(
            DeadlineFiredFrame(dispatch=dispatch, task_id=seat.frame.task_id)
        )

    def _finish(self, dispatch: int, task: asyncio.Task[WireResult]) -> None:
        """The task of run ``dispatch`` has ended: free the seat, report the end."""
        seat = self._seats.pop(dispatch)
        match seat.deadline:
            case asyncio.TimerHandle():
                seat.deadline.cancel()
            case _NoDeadline():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        self._writer.send_result(
            seat.frame.task_name,
            SharedResultFrame(
                dispatch=dispatch,
                task_id=seat.frame.task_id,
                end=_end_of(seat, task),
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


def _end_of(seat: _Seat, task: asyncio.Task[WireResult]) -> SeatEnd:
    """The end of a run. After a deadline that fired, the end is the timeout,
    also when the task returned a value. A task cancelled without its deadline,
    or one whose exception escaped the seat runner, is a defect."""
    match seat.deadline_state:
        case _Deadline.FIRED:
            return SeatTimedOut()
        case _Deadline.PENDING:
            pass
        case _ as unreachable:
            assert_never(unreachable)
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
    ok, result_json, failed_reason = task.result()
    return SeatFinished(ok=ok, result_json=result_json, failed_reason=failed_reason)


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
    """Start the seats, report ready, serve until the stop frame, then stop
    the seats. Runs on the task loop for the whole life of the child."""
    match await program.start(init):
        case Ok(started):
            pass
        case Err(failed):
            fail_boot(sock, failed)
    gc.collect()
    writer.send(ReadyFrame(pid=os.getpid(), rss=_read_rss()))
    await SeatLoop(seats, started, writer, _read_rss).serve(work)
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
    fails the boot (``ThreadStartRefused``). ``program.start`` runs on the
    loop and an ``Err`` fails the boot, before the ready frame. After the
    stop frame the seats stop, the loop closes (bounded as for an isolated
    child), the writer drains, bounded by ``WRITER_CLOSE_TIMEOUT_S``, and
    ``exit_after_stop`` ends the process.
    """
    writer = FrameWriter(
        functools.partial(_send_child_frame, sock),
        capacity=FRAMES_PER_SEAT * seats + 1,
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
