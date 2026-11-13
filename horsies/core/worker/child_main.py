"""Child program: ``python -m horsies.core.worker.child_main <fd> <parent pid>``.

The init frame names the kind of the child. An isolated child has two
threads. The control thread owns socket reads, so EOF means the parent is
gone, whatever the main thread is doing. The main thread boots the app, runs
tasks one at a time, and writes results. An ``async def`` task runs on the
child's one task loop (``task_loop.TaskLoop``), on the main thread. The child
creates that loop at boot, before it reports ready, when its app has an
``async def`` task.

A shared async child runs up to its seats ``async def`` tasks at once on its
task loop, which runs from the ready frame to the stop frame; see
``shared_child``. A shared thread child runs up to its seats ``def`` tasks at
once, each on a seat thread; see ``thread_child``.

When the parent is gone the child exits at once. Nothing can save its result,
and a run that continues could overlap a retry of the same task.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import atexit
import ctypes
import gc
import logging
import multiprocessing
import os
import signal
import socket
import sys
import threading
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Final, NoReturn, assert_never

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.logging import get_logger
from horsies.core.models.boot_failure import (
    AppLoadFailed,
    ChildPoolSetupFailed,
    ReportedBootCause,
    ThreadStartRefused,
    ThreadStartRefusedError,
    shortage_or,
)
from horsies.core.models.tasks import OperationalErrorCode, TaskError, TaskResult
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    MAX_BODY_BYTES,
    MAX_META_BYTES,
    BodyReturnedFrame,
    BootFailedFrame,
    ChildFrame,
    FrameOverBound,
    FrameSizes,
    InitFrame,
    IsolatedChild,
    LeftWorkEnd,
    LeftWorkEndedFrame,
    LeftWorkStuckFrame,
    LoopHoldsNoTask,
    LoopHoldsTasks,
    NoTaskLoop,
    ParentFrame,
    ReadyFrame,
    ResultFrame,
    RssMb,
    RssReading,
    RssUnreadable,
    RunFrame,
    SeatFinished,
    SharedAsyncChild,
    SharedRunFrame,
    SharedThreadChild,
    StopFrame,
    WireResult,
    boot_failed_frame,
    decode_parent_frame,
    encode_child_frame,
    parse_header,
)
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.run_return import (
    IsolatedMarkerWindow,
    MarkerAfterLeftWork,
    MarkerAtReturn,
    RunReturn,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.runtime import _read_current_rss_mb
from horsies.core.worker.task_loop import (
    AsyncTaskPresence,
    ExecutorAfterClose,
    ExecutorShutDown,
    ExecutorThreadsLeft,
    LeftWorkOutcome,
    LeftWorkStuck,
    LoopCreateFailed,
    TaskLoop,
)

if TYPE_CHECKING:
    from horsies.core.worker.shared_child import SeatProgram
    from horsies.core.worker.thread_child import ThreadSeatProgram

logger = get_logger('worker')

PARENT_GONE_EXIT_CODE: Final = 71
BAD_PARENT_FRAME_EXIT_CODE: Final = 72
BAD_ARGUMENTS_EXIT_CODE: Final = 73
# After a stop frame, when threads of the task loop's executor still run:
# the child leaves without waiting for them, so ``atexit`` handlers do not run.
STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE: Final = 74

_JOIN_NON_DAEMON_THREADS: Final[Callable[[], None]] = getattr(threading, '_shutdown')
"""The first step of ``sys.exit``: the callbacks that ``concurrent.futures``
and other modules registered with ``threading`` (they stop their executors),
then a join of every non-daemon thread. Read at import, so a Python without
it fails the child at boot, not at its stop."""
# At boot, after ``BootFailedFrame``: the frame names the cause.
BOOT_FAILED_EXIT_CODE: Final = 75
INIT_FRAME_TIMEOUT_S: Final = 30.0
CONTROL_READ_BUFFER_BYTES: Final = 1 << 16
"""The first size of the read buffer of the control thread. Every complete
parent frame of one read is taken from it before the next read."""
CONTROL_DIRECT_BODY_BYTES: Final = 1 << 16
"""A parent frame body larger than this is read straight into a buffer of its
own size, not into the read buffer."""
CONTROL_READ_BUFFER_BOUND: Final = (
    FRAME_HEADER.size + MAX_META_BYTES + CONTROL_DIRECT_BODY_BYTES
)
"""The largest size of the read buffer: a header, the largest metadata, and a
body that is not read straight into its own buffer."""
JOINED_CHILD_WRITE_BODY_BYTES: Final = 1 << 16
"""A child frame with a body up to this size goes to the socket in one write:
header, metadata and body joined. A larger body goes in a write of its own, so
it is not copied."""
# Metadata holds this text as JSON, and one character takes at most 12 bytes
# there, so the reason stays well under ``MAX_META_BYTES``.
MAX_FAILED_REASON_CHARS: Final = 64 * 1024

_PR_SET_PDEATHSIG: Final = 1

# A boot step returns whether the booted app has an ``async def`` task, or the
# frame that reports why it failed.
BootFn = Callable[[InitFrame], Result[AsyncTaskPresence, BootFailedFrame]]
# Runs one task of an isolated child, up to its wire tuple, and writes no
# finalizing marker.
RunTaskFn = Callable[[RunFrame, InitFrame, TaskLoop], RunReturn]
# Writes the finalizing marker of an isolated run that set its row RUNNING
# and has ended. Logs a failed write; nothing escapes.
MarkFinalizingFn = Callable[[RunFrame, IsolatedMarkerWindow], None]


@dataclass(frozen=True, slots=True)
class TaskRunners:
    """How a child runs tasks: ``run_task`` runs one task of an isolated
    child and ``mark_finalizing`` writes its finalizing marker, ``seats``
    runs the seats of a shared async child, and ``threads`` runs one task on
    a seat of a shared thread child."""

    run_task: RunTaskFn
    mark_finalizing: MarkFinalizingFn
    seats: SeatProgram
    threads: ThreadSeatProgram


def _recv_exact(sock: socket.socket, count: int) -> bytes:
    """Read exactly ``count`` bytes. Raises ``EOFError`` when the peer is gone."""
    data = bytearray(count)
    view = memoryview(data)
    filled = 0
    while filled < count:
        got = sock.recv_into(view[filled:])
        if got == 0:
            raise EOFError
        filled += got
    return bytes(data)


def _sizes_or_exit(header: bytes) -> FrameSizes:
    """The sizes of a parent frame header. A header over the bounds is a
    defect of the parent: exit loud."""
    match parse_header(header):
        case Ok(sizes):
            return sizes
        case Err(FrameOverBound() as over):
            logger.critical('Parent sent a frame over the bounds: %s', over)
            os._exit(BAD_PARENT_FRAME_EXIT_CODE)


def _parent_frame_or_exit(meta: bytes, body: bytes) -> ParentFrame:
    """Decode a parent frame. A bad frame from the parent is a defect: exit
    loud."""
    match decode_parent_frame(meta, body):
        case Ok(frame):
            return frame
        case Err(malformed):
            logger.critical('Parent sent a bad frame: %s', malformed.reason)
            os._exit(BAD_PARENT_FRAME_EXIT_CODE)


def _recv_parent_frame(sock: socket.socket) -> ParentFrame:
    """Read exactly one frame, and no byte after it. A bad frame from the
    parent is a defect: exit loud."""
    sizes = _sizes_or_exit(_recv_exact(sock, FRAME_HEADER.size))
    meta = _recv_exact(sock, sizes.meta_bytes)
    body = _recv_exact(sock, sizes.body_bytes)
    return _parent_frame_or_exit(meta, body)


class ParentFrameReader:
    """Reads the parent frames of the control thread from a blocking socket.

    One ``recv_into`` fills the free tail of the read buffer, and
    ``next_frame`` takes the frames from it in order: it reads again only when
    the next frame has not fully arrived. A body larger than
    ``CONTROL_DIRECT_BODY_BYTES`` is read straight into a buffer of its own.
    The read buffer stays within ``CONTROL_READ_BUFFER_BOUND``. Not safe to
    share between threads.
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buffer = bytearray(CONTROL_READ_BUFFER_BYTES)
        self._start: int = 0
        """The first byte of the read buffer that no returned frame used."""
        self._end: int = 0
        """The end of the bytes that arrived in the read buffer."""

    def next_frame(self) -> ParentFrame:
        """The next parent frame.

        Raises:
            EOFError: the parent closed the socket.
            OSError: the socket failed.

        A bad frame from the parent ends the process (``_sizes_or_exit``,
        ``_parent_frame_or_exit``).
        """
        header_end = FRAME_HEADER.size
        self._fill_to(header_end)
        sizes = _sizes_or_exit(
            bytes(self._buffer[self._start : self._start + header_end]),
        )
        meta_end = header_end + sizes.meta_bytes
        if sizes.body_bytes > CONTROL_DIRECT_BODY_BYTES:
            self._fill_to(meta_end)
            meta = bytes(self._buffer[self._start + header_end : self._start + meta_end])
            self._start += meta_end
            return _parent_frame_or_exit(meta, self._take_body(sizes.body_bytes))
        frame_end = meta_end + sizes.body_bytes
        self._fill_to(frame_end)
        meta = bytes(self._buffer[self._start + header_end : self._start + meta_end])
        body = bytes(self._buffer[self._start + meta_end : self._start + frame_end])
        self._start += frame_end
        return _parent_frame_or_exit(meta, body)

    def _fill_to(self, count: int) -> None:
        """Read until ``count`` bytes from ``_start`` have arrived. The bytes
        at hand move to the front first when the frame would not fit the
        buffer, and the buffer grows to ``count`` when it is smaller."""
        assert count <= CONTROL_READ_BUFFER_BOUND, (
            f'a read of {count} bytes is over the read buffer bound of '
            f'{CONTROL_READ_BUFFER_BOUND} bytes'
        )
        if self._start == self._end:
            self._start = self._end = 0
        if self._end - self._start >= count:
            return
        if len(self._buffer) - self._start < count:
            pending = self._end - self._start
            self._buffer[:pending] = self._buffer[self._start : self._end]
            self._start, self._end = 0, pending
            if len(self._buffer) < count:
                self._buffer.extend(bytes(count - len(self._buffer)))
        view = memoryview(self._buffer)
        while self._end - self._start < count:
            got = self._sock.recv_into(view[self._end :])
            if got == 0:
                raise EOFError
            self._end += got

    def _take_body(self, body_bytes: int) -> bytes:
        """A large body: the part at hand, then the rest read straight into
        a buffer of the body's size."""
        body = bytearray(body_bytes)
        at_hand = min(self._end - self._start, body_bytes)
        body[:at_hand] = self._buffer[self._start : self._start + at_hand]
        self._start += at_hand
        view = memoryview(body)
        filled = at_hand
        while filled < body_bytes:
            got = self._sock.recv_into(view[filled:])
            if got == 0:
                raise EOFError
            filled += got
        return bytes(body)


def _send_child_frame(sock: socket.socket, frame: ChildFrame) -> None:
    """Write one frame: one write up to ``JOINED_CHILD_WRITE_BODY_BYTES`` of
    body, else the body in a write of its own. A failed write means the
    parent is gone."""
    encoded = encode_child_frame(frame)
    try:
        if len(encoded.body) <= JOINED_CHILD_WRITE_BODY_BYTES:
            sock.sendall(encoded.head + encoded.body)
        else:
            sock.sendall(encoded.head)
            sock.sendall(encoded.body)
    except OSError:
        os._exit(PARENT_GONE_EXIT_CODE)


def _read_parent_frames(
    sock: socket.socket,
    work: ParentFrameInbox,
) -> None:
    """Control thread body. Ends the process when the parent is gone."""
    reader = ParentFrameReader(sock)
    try:
        while True:
            match reader.next_frame():
                case InitFrame():
                    logger.critical('Parent sent a second init frame')
                    os._exit(BAD_PARENT_FRAME_EXIT_CODE)
                case RunFrame() | SharedRunFrame() | StopFrame() as frame:
                    work.put(frame)
                case _ as unreachable:
                    assert_never(unreachable)
    except (EOFError, OSError):
        os._exit(PARENT_GONE_EXIT_CODE)
    except Exception:
        logger.critical('Control thread failed', exc_info=True)
        os._exit(BAD_PARENT_FRAME_EXIT_CODE)


def bind_to_parent(parent_pid: int) -> None:
    """Exit with the parent, also when C code in a task holds the GIL.

    Linux only: the control thread needs the GIL, this kernel signal does not.
    The ``getppid`` check closes the race with a parent that died before the
    ``prctl`` call. The parent must spawn from a long-lived thread, because
    the signal fires when the spawning thread ends.
    """
    if sys.platform == 'linux':
        ctypes.CDLL(None, use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)
    if os.getppid() != parent_pid:
        os._exit(PARENT_GONE_EXIT_CODE)


def ignore_stop_signals() -> None:
    """Leave shutdown to the parent, also when a signal reaches every process.

    SIGTERM gets a handler, not ``SIG_IGN``: ``exec`` resets a handler but
    keeps an ignored signal, and an ignored SIGTERM would make
    ``Popen.terminate()`` in task code do nothing.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, lambda _signum, _frame: None)


def read_init_frame(sock: socket.socket) -> InitFrame:
    """Read the first frame, bounded. Exits when the parent never sends it."""
    sock.settimeout(INIT_FRAME_TIMEOUT_S)
    try:
        frame = _recv_parent_frame(sock)
    except (EOFError, OSError):
        os._exit(PARENT_GONE_EXIT_CODE)
    sock.settimeout(None)
    match frame:
        case InitFrame():
            return frame
        case RunFrame() | SharedRunFrame() | StopFrame():
            logger.critical('First parent frame was not an init frame')
            os._exit(BAD_PARENT_FRAME_EXIT_CODE)
        case _ as unreachable:
            assert_never(unreachable)


def _read_rss() -> RssReading:
    rss_mb = _read_current_rss_mb()
    return RssUnreadable() if rss_mb is None else RssMb(rss_mb)


def bounded_text(text: str, limit: int) -> str:
    """Make free text safe for a metadata field: valid UTF-8, and bounded.

    The text can come from an exception, so it may hold a lone surrogate, which
    JSON text cannot encode, or it may be any size.
    """
    safe = text.encode('utf-8', 'backslashreplace').decode('utf-8')
    if len(safe) <= limit:
        return safe
    return f'{safe[:limit]} [cut from {len(safe)} characters]'


def _bounded_result(task_name: str, wire: WireResult) -> WireResult:
    """Make a wire result safe to frame.

    A result too large to ever be stored becomes a typed failure. A long
    failure reason is cut: the result beside it stays valid.
    """
    ok, result_json, failed_reason = wire
    size = len(result_json.encode('utf-8'))
    if size <= MAX_BODY_BYTES:
        if failed_reason is not None:
            failed_reason = bounded_text(failed_reason, MAX_FAILED_REASON_CHARS)
        return ok, result_json, failed_reason
    message = (
        f'result of task {task_name} is {size} bytes, above the '
        f'{MAX_BODY_BYTES} byte bound'
    )
    logger.error(message)
    failure: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
            message=message,
            data={'task_name': task_name, 'result_bytes': size},
        ),
    )
    return True, serialize_error_payload(failure), message


def flush_output() -> None:
    """Flush before ``os._exit``, which skips it. A closed stream is no failure."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except (OSError, ValueError):
            continue


def _run_or_exit(
    run_task: RunTaskFn,
    frame: RunFrame,
    init: InitFrame,
    task_loop: TaskLoop,
) -> RunReturn:
    """Run one task. Anything that escapes ends this process at once.

    A normal exit would wait for every non-daemon thread that the task started,
    so the slot would stay busy with no bound. The parent sees this exit and
    recovers the row by its status.
    """
    try:
        return run_task(frame, init, task_loop)
    except SystemExit as exc:
        flush_output()
        os._exit(exc.code if isinstance(exc.code, int) else 1)
    except BaseException:
        logger.critical('Task %s escaped the task runner', frame.task_id, exc_info=True)
        flush_output()
        os._exit(1)


class _AfterRun(Enum):
    """Whether an isolated child may take another task after a run."""

    SERVES = 'serves'
    """The run left no asyncio task on the task loop."""

    TO_RETIRE = 'to_retire'
    """The body of the run returned with asyncio tasks on the task loop. The
    child ended them, or reported them stuck, and the parent replaces it:
    cancelled code can leave broken state in the process."""


def _refuse_run_on_a_child_to_retire(after_run: _AfterRun, frame: RunFrame) -> None:
    """Exit loud when an earlier run of this child left work on its loop.

    The child reports that work (``BodyReturnedFrame``), and the parent
    replaces the child. So a run frame here is a defect in the parent.
    """
    match after_run:
        case _AfterRun.SERVES:
            return
        case _AfterRun.TO_RETIRE:
            logger.critical(
                'Parent sent task %s to a child where an earlier task left work '
                'on the task loop; such a child must be retired',
                frame.task_id,
            )
            flush_output()
            os._exit(BAD_PARENT_FRAME_EXIT_CODE)


def _end_left_work_or_exit(task_loop: TaskLoop, frame: RunFrame) -> LeftWorkOutcome:
    """Run ``TaskLoop.end_left_work``. Anything that escapes it ends this
    process at once, as for the run itself (``_run_or_exit``).

    A ``SystemExit`` or ``KeyboardInterrupt`` that the left work raises
    leaves the loop. A normal exit would wait for every non-daemon thread
    that the task started, with no bound. The parent sees this exit, and
    writes what the body returned.
    """
    try:
        return task_loop.end_left_work()
    except SystemExit as exc:
        flush_output()
        os._exit(exc.code if isinstance(exc.code, int) else 1)
    except BaseException:
        logger.critical(
            'Work that task %s left on the task loop escaped it',
            frame.task_id,
            exc_info=True,
        )
        flush_output()
        os._exit(1)


def _report_run(
    sock: socket.socket,
    frame: RunFrame,
    returned: RunReturn,
    task_loop: TaskLoop,
    mark_finalizing: MarkFinalizingFn,
) -> _AfterRun:
    """Report the end of one run of an isolated child to the parent.

    The finalizing marker is the last write of a run that set its row
    RUNNING (``SetRunning``), and it is written only when the run has ended:

    - No asyncio task left on the task loop: the marker (``MarkerAtReturn``),
      then one ``ResultFrame``, as before.
    - Tasks left: ``BodyReturnedFrame`` with what the body returned at once,
      then ``TaskLoop.end_left_work``. Work that ended gives the marker
      (``MarkerAfterLeftWork``), then ``LeftWorkEndedFrame``. Work that did
      not stop gives ``LeftWorkStuckFrame`` and no marker: the parent kills
      the child, and its runner heartbeats cover the row until the exit.

    A run that stopped before its RUNNING update (``StoppedBeforeRunning``)
    writes no marker. The wire tuple is bounded here (``_bounded_result``).
    A parent that is gone ends the process (``_send_child_frame``).
    """
    match returned:
        case SetRunning(wire=wire) | StoppedBeforeRunning(wire=wire):
            ok, result_json, failed_reason = _bounded_result(frame.task_name, wire)
        case _ as unreachable:
            assert_never(unreachable)
    match task_loop.report():
        case NoTaskLoop() | LoopHoldsNoTask() as nothing_left:
            _mark_when_set_running(frame, returned, MarkerAtReturn(), mark_finalizing)
            _send_child_frame(
                sock,
                ResultFrame(
                    task_id=frame.task_id,
                    ok=ok,
                    result_json=result_json,
                    failed_reason=failed_reason,
                    rss=_read_rss(),
                    task_loop=nothing_left,
                ),
            )
            return _AfterRun.SERVES
        case LoopHoldsTasks() as left:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    _send_child_frame(
        sock,
        BodyReturnedFrame(
            task_id=frame.task_id,
            returned=SeatFinished(ok, result_json, failed_reason),
            left=left,
        ),
    )
    match _end_left_work_or_exit(task_loop, frame):
        case LeftWorkEnd() as end:
            _mark_when_set_running(
                frame,
                returned,
                MarkerAfterLeftWork(),
                mark_finalizing,
            )
            _send_child_frame(
                sock,
                LeftWorkEndedFrame(task_id=frame.task_id, end=end, rss=_read_rss()),
            )
        case LeftWorkStuck(still=still):
            _send_child_frame(
                sock,
                LeftWorkStuckFrame(task_id=frame.task_id, still=still),
            )
        case _ as unreachable:
            assert_never(unreachable)
    return _AfterRun.TO_RETIRE


def _mark_when_set_running(
    frame: RunFrame,
    returned: RunReturn,
    window: IsolatedMarkerWindow,
    mark_finalizing: MarkFinalizingFn,
) -> None:
    """Write the finalizing marker of a run that set its row RUNNING. A run
    that stopped before its RUNNING update has no marker."""
    match returned:
        case SetRunning():
            mark_finalizing(frame, window)
        case StoppedBeforeRunning():
            return
        case _ as unreachable:
            assert_never(unreachable)


def boot_failure_from_exception(
    exc: Exception,
    step_cause: AppLoadFailed | ChildPoolSetupFailed,
) -> BootFailedFrame:
    """Name the cause of an exception that a boot step raised.

    A ``ThreadStartRefusedError`` is ``ThreadStartRefused``, which can clear;
    the frame names the error of the refused start. An ``OSError`` whose
    errno is in ``CLEARABLE_ERRNOS`` is a ``ResourceShortage``, which can
    clear. Any other exception is ``step_cause``, which repeats. Only the
    exception itself is read, not its ``__cause__``. Pure.
    """
    match exc:
        case ThreadStartRefusedError(refusal=refusal):
            return boot_failed_frame(
                ThreadStartRefused(),
                type(refusal).__name__,
                str(exc),
            )
        case _:
            cause: ReportedBootCause = shortage_or(exc, step_cause)
            return boot_failed_frame(cause, type(exc).__name__, str(exc))


def start_control_thread(
    sock: socket.socket,
    work: ParentFrameInbox,
) -> Result[None, BootFailedFrame]:
    """Start the thread that reads parent frames into ``work``.

    ``Err``: the OS refused the thread (``RuntimeError`` from
    ``Thread.start``), so the cause is ``ThreadStartRefused``. Nothing else
    changed then.
    """
    control = threading.Thread(
        target=_read_parent_frames,
        args=(sock, work),
        name='horsies-child-control',
        daemon=True,
    )
    try:
        control.start()
    except RuntimeError as exc:
        return Err(
            boot_failed_frame(ThreadStartRefused(), type(exc).__name__, str(exc)),
        )
    return Ok(None)


def open_task_loop_at_boot(
    presence: AsyncTaskPresence,
) -> Result[TaskLoop, BootFailedFrame]:
    """Return the task loop of this child, open when its app has an
    ``async def`` task, and not created when it has none.

    ``Err``: the loop could not be created. The cause is that of
    ``LoopCreateFailed``: ``ResourceShortage`` for an errno in
    ``CLEARABLE_ERRNOS``, else ``TaskLoopCreateFailed``. The caller fails the
    boot before the ready frame, so no task reaches this child.
    """
    task_loop = TaskLoop()
    match presence:
        case AsyncTaskPresence.NO_ASYNC_TASK:
            return Ok(task_loop)
        case AsyncTaskPresence.HAS_ASYNC_TASKS:
            match task_loop.open():
                case Ok():
                    return Ok(task_loop)
                case Err(failed):
                    return Err(_loop_create_failure(failed))


def _loop_create_failure(failed: LoopCreateFailed) -> BootFailedFrame:
    return boot_failed_frame(failed.cause, failed.error_name, failed.detail)


def fail_boot(sock: socket.socket, failed: BootFailedFrame) -> NoReturn:
    """Report a failed boot to the parent, then end the process.

    The child has run no task, so it does not wait for threads that boot
    started: it logs the cause, flushes its log handlers and its output, and
    leaves with ``os._exit`` and ``BOOT_FAILED_EXIT_CODE``. A parent that is
    gone ends the process with ``PARENT_GONE_EXIT_CODE`` instead.
    """
    logger.error('Child boot failed: %s', failed.describe())
    _send_child_frame(sock, failed)
    logging.shutdown()
    flush_output()
    os._exit(BOOT_FAILED_EXIT_CODE)


def _boot_or_exit(
    sock: socket.socket,
    init: InitFrame,
    boot: BootFn,
    work: ParentFrameInbox,
) -> TaskLoop:
    """Start the control thread that fills ``work``, boot the app, and open
    the task loop.

    An isolated child opens the loop when its app has an ``async def`` task.
    A shared async child always opens it: its tasks run on it. A shared
    thread child never opens it: it runs no ``async def`` task. Each step
    that fails ends the process through ``fail_boot``, with the cause of
    that step.
    """
    match start_control_thread(sock, work):
        case Ok():
            pass
        case Err(failed):
            fail_boot(sock, failed)
    match boot(init):
        case Ok(presence):
            pass
        case Err(failed):
            fail_boot(sock, failed)
    match init.child_kind:
        case IsolatedChild():
            pass
        case SharedAsyncChild():
            presence = AsyncTaskPresence.HAS_ASYNC_TASKS
        case SharedThreadChild():
            presence = AsyncTaskPresence.NO_ASYNC_TASK
        case _ as unreachable:
            assert_never(unreachable)
    match open_task_loop_at_boot(presence):
        case Ok(task_loop):
            return task_loop
        case Err(failed):
            fail_boot(sock, failed)


def exit_after_stop(executor: ExecutorAfterClose) -> NoReturn:
    """End the process after a stop frame and the task loop close.

    ``ExecutorShutDown``: ``end_without_teardown(0)``: the child waits for its
    non-daemon threads and runs its ``atexit`` handlers, as ``sys.exit(0)``
    does, and skips the interpreter teardown.

    ``ExecutorThreadsLeft``: a normal exit would join the executor threads
    that still run, with no bound, until the parent kills the process group
    after ``CHILD_STOP_TIMEOUT_S``. That kill also ends the background
    processes that a retired child keeps. So the child flushes its log
    handlers and its output, and leaves with ``os._exit`` and
    ``STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE``. ``atexit`` handlers do not
    run.
    """
    match executor:
        case ExecutorShutDown():
            end_without_teardown(0)
        case ExecutorThreadsLeft():
            logger.warning(
                'Threads of the task loop executor still run after the stop; '
                'the child exits without waiting for them',
            )
            logging.shutdown()
            flush_output()
            os._exit(STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE)
        case _ as unreachable:
            assert_never(unreachable)


def end_without_teardown(code: int) -> NoReturn:
    """End the process with ``code`` after the steps that ``sys.exit`` runs
    before the interpreter teardown: the join of the non-daemon threads (with
    the stop of the executors that registered with ``threading``), the
    ``atexit`` handlers, and a flush of the output. Then ``os._exit(code)``.

    The teardown clears every module and frees every object. In a child that
    the preload parent forked, that writes to the pages the child shares with
    it, so the kernel copies each of them while the child ends: the memory of
    an ending child grows by about the size of the shared library. Python
    does not promise ``__del__`` for objects that still exist at exit, and
    those calls are the part that no longer runs. ``multiprocessing`` ends
    its forked children with ``os._exit`` too.

    An exception from a step is printed, and the process still ends with
    ``code``, as an error in an ``atexit`` handler leaves the code of
    ``sys.exit``.
    """
    try:
        _JOIN_NON_DAEMON_THREADS()
        atexit._run_exitfuncs()  # pyright: ignore[reportPrivateUsage] - what sys.exit runs next
    except BaseException:
        traceback.print_exc()
    finally:
        flush_output()
        os._exit(code)


def serve(
    sock: socket.socket,
    parent_pid: int,
    boot: BootFn,
    runners: TaskRunners,
) -> NoReturn:
    """Run the child until the parent sends a stop frame, then end the process.

    After ``boot``, the child creates its ``TaskLoop`` when ``boot`` reports
    an ``async def`` task in the app, or when the child is a shared async
    child, and only then sends its ready frame. An isolated child runs
    ``runners.run_task`` for each run frame, a shared async child runs its
    seats with ``runners.seats`` (``shared_child.serve_shared``), and a
    shared thread child runs each task with ``runners.threads`` on a seat
    thread (``thread_child.serve_thread``).

    Failure exits: a control thread that the OS refuses, an ``Err`` from
    ``boot``, and a loop that cannot be created each end the boot through
    ``fail_boot``, before the ready frame, with a ``BootFailedFrame`` that
    names the cause. An exception that escapes ``boot`` is a defect: it
    propagates, so the child exits non-zero with no frame. A frame of the
    other kind of child, or a run frame after a report that this child must
    be retired, is a parent defect and ends the process with
    ``BAD_PARENT_FRAME_EXIT_CODE``. A parent that is gone or that sends a bad
    frame ends the process through ``os._exit``, and so does an exception
    from ``run_task``: the parent sees the exit and recovers the row by its
    status. Does not write to the database itself.
    """
    ignore_stop_signals()
    bind_to_parent(parent_pid)
    init = read_init_frame(sock)
    work = ParentFrameInbox()
    task_loop = _boot_or_exit(sock, init, boot, work)
    match init.child_kind:
        case IsolatedChild():
            _serve_isolated(sock, init, work, task_loop, runners)
        case SharedAsyncChild() as kind:
            from horsies.core.worker.shared_child import serve_shared

            serve_shared(sock, init, work, task_loop, kind, runners.seats)
        case SharedThreadChild(seats=seats):
            from horsies.core.worker.thread_child import serve_thread

            serve_thread(sock, init, work, task_loop, seats, runners.threads)
        case _ as unreachable:
            assert_never(unreachable)


def _serve_isolated(
    sock: socket.socket,
    init: InitFrame,
    work: ParentFrameInbox,
    task_loop: TaskLoop,
    runners: TaskRunners,
) -> NoReturn:
    """Run tasks one at a time until the stop frame, then end the process.

    Every task of the child gets ``task_loop``. ``runners.run_task`` runs
    each task, and ``_report_run`` reports the end of the run after the work
    that the task left on that loop ended, with the finalizing marker of
    ``runners.mark_finalizing`` as its last write. The stop frame closes the
    loop, bounded by ``LOOP_CLOSE_TIMEOUT_S``, and ``exit_after_stop`` ends
    the process.
    """
    gc.collect()
    _send_child_frame(sock, ReadyFrame(pid=os.getpid(), rss=_read_rss()))
    after_run = _AfterRun.SERVES
    while True:
        match work.get():
            case RunFrame() as frame:
                _refuse_run_on_a_child_to_retire(after_run, frame)
                after_run = _report_run(
                    sock,
                    frame,
                    _run_or_exit(runners.run_task, frame, init, task_loop),
                    task_loop,
                    runners.mark_finalizing,
                )
            case SharedRunFrame():
                logger.critical('Parent sent a shared run frame to an isolated child')
                flush_output()
                os._exit(BAD_PARENT_FRAME_EXIT_CODE)
            case StopFrame():
                exit_after_stop(task_loop.close())
            case _ as unreachable:
                assert_never(unreachable)


def restore_parent_state(init: InitFrame) -> None:
    """Restore the import state and the names that the worker's process had."""
    sys.path[:] = list(init.parent_sys_path)
    sys.argv[:] = list(init.parent_argv)
    multiprocessing.current_process().name = init.process_name


def boot_app(init: InitFrame) -> Result[AsyncTaskPresence, BootFailedFrame]:
    """Restore the parent's import state, import the app, then run the
    per-child start steps of the kind of the child.

    An isolated child and a shared thread child open their sync connection
    pool here. A shared async child opens no pool here: it opens an async
    pool on its event loop, after the ready steps of ``shared_child``.

    ``Ok``: whether the booted app has an ``async def`` task, which decides
    whether the child creates its task loop.

    ``Err``: the cause comes from ``boot_failure_from_exception``. An
    exception from locating or importing the app or a task module is
    ``AppLoadFailed``. An exception from the per-child start steps, after the
    start hooks, is ``ChildPoolSetupFailed``. A start hook that fails does
    not return here: it ends the process with
    ``CHILD_HOOK_FAILURE_EXIT_CODE``.
    """
    from horsies.core.worker.child_runner import (
        async_task_presence,
        finish_child_start,
        finish_shared_child_start,
        import_app_for_child,
    )

    restore_parent_state(init)
    try:
        app = import_app_for_child(
            init.app_locator,
            list(init.imports),
            list(init.sys_path_roots),
            init.loglevel,
            init.child_kind.task_send_pool,
        )
    except Exception as exc:
        return Err(boot_failure_from_exception(exc, AppLoadFailed()))
    try:
        match init.child_kind:
            case IsolatedChild(pool=pool) | SharedThreadChild(pool=pool):
                finish_child_start(
                    app,
                    init.database_url,
                    dict(init.connect_kwargs),
                    pool,
                )
            case SharedAsyncChild():
                finish_shared_child_start(app)
            case _ as unreachable:
                assert_never(unreachable)
    except Exception as exc:
        return Err(boot_failure_from_exception(exc, ChildPoolSetupFailed()))
    return Ok(async_task_presence(app))


def run_child_task(frame: RunFrame, init: InitFrame, task_loop: TaskLoop) -> RunReturn:
    """Run the task of ``frame`` (``child_runner._run_task_entry``). Writes
    no finalizing marker."""
    from horsies.core.worker.child_runner import _run_task_entry

    return _run_task_entry(
        frame.task_name,
        frame.args_json,
        frame.kwargs_json,
        frame.task_id,
        frame.worker_id,
        frame.is_workflow_task,
        frame.timeout,
        frame.claimed_at,
        task_loop=task_loop,
    )


def mark_child_task_finalizing(
    frame: RunFrame,
    window: IsolatedMarkerWindow,
) -> None:
    """Write the finalizing marker of the isolated run of ``frame``, which
    set its row RUNNING and has ended
    (``child_runner.mark_isolated_run_finalizing``)."""
    from horsies.core.worker.child_runner import mark_isolated_run_finalizing

    mark_isolated_run_finalizing(
        frame.task_id,
        frame.worker_id,
        frame.claimed_at,
        window,
    )


def production_runners() -> TaskRunners:
    """The runners of a worker child: ``run_child_task`` and
    ``mark_child_task_finalizing`` for an isolated child, the Postgres seat
    program for a shared async child, and the Postgres thread seats for a
    shared thread child."""
    from horsies.core.worker.shared_runner import PostgresSeatProgram
    from horsies.core.worker.thread_runner import PostgresThreadSeats

    return TaskRunners(
        run_task=run_child_task,
        mark_finalizing=mark_child_task_finalizing,
        seats=PostgresSeatProgram(),
        threads=PostgresThreadSeats(),
    )


def parse_arguments(argv: Sequence[str]) -> tuple[int, int] | None:
    """Return (socket fd, parent pid), or None when the arguments are invalid."""
    if len(argv) != 2 or not argv[0].isdecimal() or not argv[1].isdecimal():
        return None
    return int(argv[0]), int(argv[1])


def main(argv: Sequence[str]) -> NoReturn:
    """Program entry. After a stop frame, ``serve`` ends the process."""
    parsed = parse_arguments(argv)
    if parsed is None:
        sys.stderr.write(f'child_main: bad arguments: {list(argv)!r}\n')
        sys.exit(BAD_ARGUMENTS_EXIT_CODE)
    fd, parent_pid = parsed
    serve(socket.socket(fileno=fd), parent_pid, boot_app, production_runners())


if __name__ == '__main__':
    main(sys.argv[1:])
