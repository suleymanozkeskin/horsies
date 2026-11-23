"""Child program: ``python -m horsies.core.worker.child_main <fd> <parent pid>``.

The child speaks the core frame codec at version 2 (``horsies._core``,
``child_frames``). The init frame names the protocol version first: a frame
of another version ends the child with ``PROTOCOL_MISMATCH_EXIT_CODE``, after
a log line with both numbers. The init frame names the kind of the child. An isolated child has two
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

The init frame of a handler child makes this program the handler child of
the worker: it runs the event handlers of the app and no task; see
``handler_child``.

When the parent is gone the child exits at once. Nothing can save its result,
and a run that continues could overlap a retry of the same task.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import atexit
import collections
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
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from types import ModuleType
from typing import TYPE_CHECKING, Final, NoReturn, assert_never

from horsies import _core as core
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
from horsies.core.telemetry.attempt_span import flush_attempt_spans_at_stop
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_frames import (
    MAX_BODY_BYTES,
    PROTOCOL_NUMBERS,
    AttemptStart,
    BodyReturnedFrame,
    BootFailedFrame,
    ChildFrame,
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
    RunEnd,
    SeatEnd,
    SeatTimedOut,
    StartNotReported,
    StartReported,
    boot_failed_frame,
    encode_child_frame,
    run_trace_of,
)
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.run_return import (
    CoreRefusal,
    CoreStoppedAfterBody,
    CoreStoppedBeforeStart,
    MarkerEnd,
    ResultEnvelope,
    RunReturn,
    SetRunning,
    StoppedBeforeRunning,
    end_after_marker,
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
    from horsies.core.app import Horsies
    from horsies.core.worker.shared_child import SeatProgram
    from horsies.core.worker.thread_child import ThreadSeatProgram

logger = get_logger('worker')

PARENT_GONE_EXIT_CODE: Final = PROTOCOL_NUMBERS.parent_gone_exit_code
BAD_PARENT_FRAME_EXIT_CODE: Final = PROTOCOL_NUMBERS.bad_parent_frame_exit_code
BAD_ARGUMENTS_EXIT_CODE: Final = PROTOCOL_NUMBERS.bad_arguments_exit_code
# After a stop frame, when threads of the task loop's executor still run:
# the child leaves without waiting for them, so ``atexit`` handlers do not run.
STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE: Final = (
    PROTOCOL_NUMBERS.stopped_with_executor_threads_exit_code
)
# At boot, after ``BootFailedFrame``: the frame names the cause.
BOOT_FAILED_EXIT_CODE: Final = PROTOCOL_NUMBERS.boot_failed_exit_code
# The init frame names another protocol version: the child and the worker
# come from different builds.
PROTOCOL_MISMATCH_EXIT_CODE: Final = PROTOCOL_NUMBERS.protocol_mismatch_exit_code
# The core of the child stopped, or this process is a fork of the child: the
# child ends itself at its next core call (``end_child_after_core_refusal``).
CORE_STOPPED_EXIT_CODE: Final = PROTOCOL_NUMBERS.core_stopped_exit_code
CHILD_DEFECT_EXIT_CODE: Final = 1
"""A defect inside the child, as when a task escapes the task runner."""
INIT_FRAME_TIMEOUT_S: Final = 30.0
READ_CHUNK_BYTES: Final = 1 << 16
"""The most bytes that one read of the parent socket takes."""

_PR_SET_PDEATHSIG: Final = 1

# A boot step returns whether the booted app has an ``async def`` task, or the
# frame that reports why it failed.
BootFn = Callable[[core.InitFrame], Result[AsyncTaskPresence, BootFailedFrame]]
# Runs one task of an isolated child, up to its result, and writes no
# finalizing marker.
RunTaskFn = Callable[[core.RunFrame, core.InitFrame, TaskLoop], RunReturn]
# Writes the finalizing marker of an isolated run that set its row RUNNING
# and has ended. Logs a failed write; nothing escapes. Returns
# ``MarkerMetStoppedCore`` when the core of the child stopped.
MarkFinalizingFn = Callable[[core.RunFrame], MarkerEnd]
# The boot step of a handler child: the app with its event handlers, or the
# frame that reports why it failed.
type HandlerBootFn = Callable[
    [core.InitFrame, core.HandlerChildKind],
    Result[Horsies, BootFailedFrame],
]


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


def _exit_bad_parent_frame(message: str) -> NoReturn:
    """A bad frame from the parent is a defect of the parent: exit loud."""
    logger.critical(message)
    flush_output()
    os._exit(BAD_PARENT_FRAME_EXIT_CODE)


def _exit_protocol_mismatch(mismatch: core.ParentProtocolMismatch) -> NoReturn:
    """An init frame of another protocol version: log both numbers and exit
    with the protocol-mismatch code."""
    logger.critical(
        'The worker sent an init frame of protocol %s; this child speaks '
        'protocol %s. The worker and its children come from different builds',
        mismatch.received,
        mismatch.expected,
    )
    flush_output()
    os._exit(PROTOCOL_MISMATCH_EXIT_CODE)


def core_refusal_text(refusal: CoreRefusal) -> str:
    """The cause of ``refusal``, for the last log line of the child. Pure."""
    detail = '; '.join(str(argument) for argument in refusal.args)
    match refusal:
        case core.CoreStopped():
            return (
                'the core of this child stopped after a panic in a core '
                f'background task ({detail})'
            )
        case core.ForkedAfterCoreStart():
            return (
                'this process is a fork of a child whose core had started '
                f'({detail})'
            )
        case _ as unreachable:
            assert_never(unreachable)


def end_child_after_core_refusal(refusal: CoreRefusal, moment: str) -> NoReturn:
    """End this child after its core refused a call: log the cause as the
    last line of the child, flush the log handlers and the output, and leave
    with ``os._exit(CORE_STOPPED_EXIT_CODE)``. ``moment`` names where the
    refusal came, for example the start of a task.

    The worker reads the code: a child that holds no task is replaced; a
    child that holds a task takes the crashed-child path, and the retry
    policy of the task applies. ``atexit`` handlers do not run.
    """
    logger.error(
        '%s: %s; the child ends itself with exit code %s',
        moment,
        core_refusal_text(refusal),
        CORE_STOPPED_EXIT_CODE,
    )
    logging.shutdown()
    flush_output()
    os._exit(CORE_STOPPED_EXIT_CODE)


class ParentFrameReader:
    """Reads the parent frames from a blocking socket, with the read buffer
    of the core codec (``core.FrameReader``) and its bounds.

    One ``recv`` takes at most ``READ_CHUNK_BYTES``; ``next_frame`` reads
    again only when no whole frame is at hand. The main thread reads the init
    frame with it, then the control thread reads every later frame with the
    same reader, so no byte that arrived is lost between them. Not safe to
    share between threads at the same time.
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._reader = core.FrameReader()
        self._ready: collections.deque[ParentFrame] = collections.deque()

    def next_frame(self) -> ParentFrame:
        """The next parent frame.

        Raises:
            EOFError: the parent closed the socket.
            OSError: the socket failed.

        A frame that the core refuses ends the process: a header over the
        bounds or a malformed frame with the bad-frame code, an init frame of
        another protocol version with the protocol-mismatch code.
        """
        while not self._ready:
            data = self._sock.recv(READ_CHUNK_BYTES)
            if not data:
                raise EOFError
            match self._reader.feed(data):
                case list() as frames:
                    self._ready.extend(frames)
                case core.FrameOverBound(meta_bytes=meta_bytes, body_bytes=body_bytes):
                    _exit_bad_parent_frame(
                        f'Parent sent a frame over the bounds: {meta_bytes} metadata '
                        f'bytes, {body_bytes} body bytes',
                    )
                case core.FrameRefused(reason=reason):
                    _exit_bad_parent_frame(f'Parent sent a bad frame: {reason}')
                case core.ParentProtocolMismatch() as mismatch:
                    _exit_protocol_mismatch(mismatch)
                case _ as unreachable:
                    assert_never(unreachable)
        return self._ready.popleft()


def _send_child_frame(sock: socket.socket, frame: ChildFrame) -> None:
    """Write one frame in one write. A failed write means the parent is
    gone. A frame over a bound of the header is a defect of the child: every
    body is bounded before (``bounded_end``), and every text is cut by the
    core."""
    match encode_child_frame(frame):
        case bytes() as encoded:
            pass
        case core.FrameTooLarge(part=part, bytes=size, bound=bound):
            logger.critical(
                'The child built a %s frame whose %s has %s bytes, over the bound '
                'of %s',
                type(frame).__name__,
                part,
                size,
                bound,
            )
            flush_output()
            os._exit(CHILD_DEFECT_EXIT_CODE)
        case _ as unreachable:
            assert_never(unreachable)
    try:
        sock.sendall(encoded)
    except OSError:
        os._exit(PARENT_GONE_EXIT_CODE)


def _read_parent_frames(
    reader: ParentFrameReader,
    work: ParentFrameInbox,
) -> None:
    """Control thread body. Ends the process when the parent is gone."""
    try:
        while True:
            match reader.next_frame():
                case core.InitFrame():
                    _exit_bad_parent_frame('Parent sent a second init frame')
                case core.EventsFrame():
                    _exit_bad_parent_frame('Parent sent an events frame to a task child')
                case core.RunFrame() | core.SharedRunFrame() | core.StopFrame() as frame:
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


def read_python_init_frame(
    sock: socket.socket,
    reader: ParentFrameReader,
) -> core.InitFrame:
    """Read the first frame with ``reader``, bounded, for a child of any
    kind. Exits when the parent never sends it, when it is another frame,
    and when it names a program other than the Python child
    (``python_program_of``)."""
    sock.settimeout(INIT_FRAME_TIMEOUT_S)
    try:
        frame = reader.next_frame()
    except (EOFError, OSError):
        os._exit(PARENT_GONE_EXIT_CODE)
    sock.settimeout(None)
    match frame:
        case core.InitFrame(program=core.PythonProgram()):
            return frame
        case core.InitFrame():
            _exit_bad_parent_frame('Parent sent the init frame of a Rust child')
        case core.RunFrame() | core.SharedRunFrame() | core.EventsFrame() | core.StopFrame():
            _exit_bad_parent_frame('First parent frame was not an init frame')
        case _ as unreachable:
            assert_never(unreachable)


def read_init_frame(sock: socket.socket, reader: ParentFrameReader) -> core.InitFrame:
    """Read the init frame of a task child (``read_python_init_frame``).
    Exits also when it names the kind of a handler child: a forked child and
    the preload process serve task children only (``task_child_kind_of``);
    the worker starts its handler child by ``exec`` (``serve_program``)."""
    init = read_python_init_frame(sock, reader)
    match init.child_kind:
        case core.HandlerChildKind():
            _exit_bad_parent_frame(
                'Parent sent the init frame of a handler child to a task child',
            )
        case core.IsolatedChildKind() | core.SharedAsyncChildKind() | core.SharedThreadChildKind():
            return init
        case _ as unreachable:
            assert_never(unreachable)


type TaskChildKind = (
    core.IsolatedChildKind | core.SharedAsyncChildKind | core.SharedThreadChildKind
)
"""The kind of a child that runs tasks."""


def task_child_kind_of(init: core.InitFrame) -> TaskChildKind:
    """The task kind of an init frame that ``read_init_frame`` took. The
    kind of a handler child here is a defect: ``AssertionError``."""
    match init.child_kind:
        case core.IsolatedChildKind() | core.SharedAsyncChildKind() | core.SharedThreadChildKind():
            return init.child_kind
        case core.HandlerChildKind():
            raise AssertionError('read_init_frame refuses the init frame of a handler child')
        case _ as unreachable:
            assert_never(unreachable)


def python_program_of(init: core.InitFrame) -> core.PythonProgram:
    """The Python program of an init frame that ``read_init_frame`` took. A
    Rust program here is a defect: ``AssertionError``."""
    match init.program:
        case core.PythonProgram() as program:
            return program
        case 'RUST':
            raise AssertionError('read_init_frame takes only the init frame of a Python child')
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


def bounded_envelope(task_name: str, result: ResultEnvelope) -> ResultEnvelope:
    """Make a result envelope safe to frame: an envelope too large to ever
    be stored (over ``MAX_BODY_BYTES``) becomes ``WORKER_SERIALIZATION_ERROR``.
    Logs that case."""
    size = len(result.text.encode('utf-8'))
    if size <= MAX_BODY_BYTES:
        return result
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
    return ResultEnvelope(serialize_error_payload(failure))


def bounded_end(task_name: str, end: RunEnd) -> RunEnd:
    """``bounded_envelope`` of a result; a stop passes unchanged."""
    match end:
        case ResultEnvelope():
            return bounded_envelope(task_name, end)
        case CoreStoppedAfterBody(result=result, stopped=stopped):
            return CoreStoppedAfterBody(
                result=bounded_envelope(task_name, result),
                stopped=stopped,
            )
        case StoppedBeforeRunning() | CoreStoppedBeforeStart():
            return end
        case _ as unreachable:
            assert_never(unreachable)


def end_child_after_frame(end: SeatEnd, task_id: uuid.UUID) -> None:
    """Call after the frame that reports ``end`` is written. An end that met
    the stopped core of the child ends the child
    (``end_child_after_core_refusal``):

    - ``CoreStoppedBeforeStart`` went out as ``OWNERSHIP_UNCONFIRMED``, so
      the worker releases its claim with no attempt charged.
    - ``CoreStoppedAfterBody`` went out with the result of the body, so the
      worker writes it as for any run.

    Every other end returns."""
    match end:
        case CoreStoppedBeforeStart(stopped=stopped):
            end_child_after_core_refusal(stopped, f'At the start of task {task_id}')
        case CoreStoppedAfterBody(stopped=stopped):
            end_child_after_core_refusal(stopped, f'After the body of task {task_id}')
        case ResultEnvelope() | StoppedBeforeRunning() | SeatTimedOut():
            return
        case _ as unreachable:
            assert_never(unreachable)


def flush_output() -> None:
    """Flush before ``os._exit``, which skips it. A closed stream is no failure."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except (OSError, ValueError):
            continue


def _run_or_exit(
    run_task: RunTaskFn,
    frame: core.RunFrame,
    init: core.InitFrame,
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


def _refuse_run_on_a_child_to_retire(after_run: _AfterRun, frame: core.RunFrame) -> None:
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


def _end_left_work_or_exit(task_loop: TaskLoop, frame: core.RunFrame) -> LeftWorkOutcome:
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
    frame: core.RunFrame,
    returned: RunReturn,
    task_loop: TaskLoop,
    mark_finalizing: MarkFinalizingFn,
) -> _AfterRun:
    """Report the end of one run of an isolated child to the parent.

    The finalizing marker is the last write of a run that set its row
    RUNNING (``SetRunning``), and it is written only when the run has ended:

    - No asyncio task left on the task loop: the marker, then one
      ``ResultFrame``.
    - Tasks left: ``BodyReturnedFrame`` with what the body returned at once,
      then ``TaskLoop.end_left_work``. Work that ended gives the marker,
      then ``LeftWorkEndedFrame``. Work that did not stop gives
      ``LeftWorkStuckFrame`` and no marker: the parent kills the child, and
      its runner heartbeats cover the row until the exit.

    The marker has the bound of the core handle of the child in both cases
    (``child_runner.LEFT_WORK_MARKER_BOUND_S``).

    A run that stopped before its RUNNING update (``StoppedBeforeRunning``)
    writes no marker and ran no task code, so it left no work. The result
    envelope is bounded here (``bounded_envelope``). A parent that is gone
    ends the process (``_send_child_frame``).

    The child ends itself once the frame of a run that met its stopped core
    is written: a start that the core refused (``CoreStoppedBeforeStart``),
    or a marker that met the stopped core after the body finished
    (``MarkerMetStoppedCore``): the frame reports the result of the body,
    and the worker writes it (``end_child_after_frame``).
    """
    started: AttemptStart
    match returned:
        case SetRunning(result=result, started_at=started_at):
            end: RunEnd = bounded_envelope(frame.task_name, result)
            started = StartReported(started_at)
        case StoppedBeforeRunning() | CoreStoppedBeforeStart():
            end = returned
            started = StartNotReported()
        case _ as unreachable:
            assert_never(unreachable)
    match task_loop.report(), end, started:
        case NoTaskLoop() | LoopHoldsNoTask() as nothing_left, _, _:
            reported = _marked_end(frame, end, mark_finalizing)
            _send_child_frame(
                sock,
                ResultFrame(
                    task_id=frame.task_id,
                    end=reported,
                    started=started,
                    rss=_read_rss(),
                    task_loop=nothing_left,
                ),
            )
            end_child_after_frame(reported, frame.task_id)
            return _AfterRun.SERVES
        case (
            LoopHoldsTasks() as left,
            ResultEnvelope() as body_result,
            StartReported(started_at=body_started_at),
        ):
            pass
        case LoopHoldsTasks(), ResultEnvelope(), StartNotReported():
            raise AssertionError(
                f'task {frame.task_id} returned a body with no start',
            )
        case (
            LoopHoldsTasks(),
            StoppedBeforeRunning() | CoreStoppedBeforeStart() | CoreStoppedAfterBody(),
            _,
        ):
            raise AssertionError(
                f'task {frame.task_id} stopped before running and left work on '
                'the task loop',
            )
        case _ as unreachable:
            assert_never(unreachable)
    _send_child_frame(
        sock,
        BodyReturnedFrame(
            task_id=frame.task_id,
            returned=body_result,
            started_at=body_started_at,
            left=left,
        ),
    )
    match _end_left_work_or_exit(task_loop, frame):
        case LeftWorkEnd() as left_end:
            marker = mark_finalizing(frame)
            _send_child_frame(
                sock,
                LeftWorkEndedFrame(task_id=frame.task_id, end=left_end, rss=_read_rss()),
            )
            end_child_after_frame(end_after_marker(body_result, marker), frame.task_id)
        case LeftWorkStuck(still=still):
            _send_child_frame(
                sock,
                LeftWorkStuckFrame(task_id=frame.task_id, still=still),
            )
        case _ as unreachable:
            assert_never(unreachable)
    return _AfterRun.TO_RETIRE


def _marked_end(
    frame: core.RunFrame,
    end: RunEnd,
    mark_finalizing: MarkFinalizingFn,
) -> RunEnd:
    """The end that a run reports after its finalizing marker. A run whose
    body finished (``ResultEnvelope``) writes the marker first; a marker
    that met the stopped core of the child gives ``CoreStoppedAfterBody``
    (``end_after_marker``). A run that stopped before its RUNNING update
    writes no marker."""
    match end:
        case ResultEnvelope():
            return end_after_marker(end, mark_finalizing(frame))
        case StoppedBeforeRunning() | CoreStoppedBeforeStart():
            return end
        case CoreStoppedAfterBody():
            raise AssertionError(
                f'task {frame.task_id} came to its marker with a marker written',
            )
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
    reader: ParentFrameReader,
    work: ParentFrameInbox,
) -> Result[None, BootFailedFrame]:
    """Start the thread that reads parent frames into ``work``.

    ``Err``: the OS refused the thread (``RuntimeError`` from
    ``Thread.start``), so the cause is ``ThreadStartRefused``. Nothing else
    changed then.
    """
    control = threading.Thread(
        target=_read_parent_frames,
        args=(reader, work),
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
    reader: ParentFrameReader,
    init: core.InitFrame,
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
    match start_control_thread(reader, work):
        case Ok():
            pass
        case Err(failed):
            fail_boot(sock, failed)
    match boot(init):
        case Ok(presence):
            pass
        case Err(failed):
            fail_boot(sock, failed)
    match task_child_kind_of(init):
        case core.IsolatedChildKind():
            pass
        case core.SharedAsyncChildKind():
            presence = AsyncTaskPresence.HAS_ASYNC_TASKS
        case core.SharedThreadChildKind():
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

    First the attempt spans of the child are flushed, within
    ``ATTEMPT_SPAN_FLUSH_BOUND_S`` (``flush_attempt_spans_at_stop``); a flush
    that fails or does not end is logged, and the exit goes on.

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
    _ = flush_attempt_spans_at_stop()
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


class ExitStep(Enum):
    """A private CPython function that ``sys.exit`` runs, with no argument,
    before the interpreter teardown. The value is its module and its name
    there. The members are in the order that ``sys.exit`` runs them."""

    JOIN_NON_DAEMON_THREADS = (threading, '_shutdown')
    """The callbacks that ``concurrent.futures`` and other modules registered
    with ``threading`` (they stop their executors), then a join of every
    non-daemon thread."""

    RUN_ATEXIT_HANDLERS = (atexit, '_run_exitfuncs')
    """The ``atexit`` handlers."""

    def __init__(self, module: ModuleType, attribute: str) -> None:
        self.module: Final = module
        self.attribute: Final = attribute

    @property
    def dotted_name(self) -> str:
        """For example ``threading._shutdown``."""
        return f'{self.module.__name__}.{self.attribute}'

    def look_up(self) -> object:
        """The value at the name of this step, or None when its module has no
        such name. Calls nothing."""
        return getattr(self.module, self.attribute, None)


@dataclass(frozen=True, slots=True)
class ExitStepsFound:
    """Both exit steps are functions on this Python."""

    join_non_daemon_threads: Callable[[], object]
    run_atexit_handlers: Callable[[], object]


@dataclass(frozen=True, slots=True)
class ExitStepMissing:
    """This Python has no function at the name of ``step``."""

    step: ExitStep


type ExitStepsLookup = ExitStepsFound | ExitStepMissing


def look_up_exit_steps() -> ExitStepsLookup:
    """Find the exit steps on this Python. Reads ``threading`` and
    ``atexit``, and calls nothing. ``ExitStepMissing`` names the first
    missing step, in the order of ``ExitStep``."""
    join_non_daemon_threads = ExitStep.JOIN_NON_DAEMON_THREADS.look_up()
    if not callable(join_non_daemon_threads):
        return ExitStepMissing(ExitStep.JOIN_NON_DAEMON_THREADS)
    run_atexit_handlers = ExitStep.RUN_ATEXIT_HANDLERS.look_up()
    if not callable(run_atexit_handlers):
        return ExitStepMissing(ExitStep.RUN_ATEXIT_HANDLERS)
    return ExitStepsFound(join_non_daemon_threads, run_atexit_handlers)


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

    The steps are private CPython functions (``ExitStep``). They are looked
    up here, at the stop, so a Python without one of them still starts its
    children. On such a Python the child logs the missing name and ends
    through ``sys.exit(code)``, which runs the same steps and the teardown.
    """
    match look_up_exit_steps():
        case ExitStepsFound(
            join_non_daemon_threads=join_non_daemon_threads,
            run_atexit_handlers=run_atexit_handlers,
        ):
            try:
                _ = join_non_daemon_threads()
                _ = run_atexit_handlers()
            except BaseException:
                traceback.print_exc()
            finally:
                flush_output()
                os._exit(code)
        case ExitStepMissing(step=step):
            logger.warning(
                'This Python has no %s; the child ends through sys.exit(%d), '
                'with the interpreter teardown',
                step.dotted_name,
                code,
            )
            sys.exit(code)
        case _ as unreachable:
            assert_never(unreachable)


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
    reader = ParentFrameReader(sock)
    init = read_init_frame(sock, reader)
    _serve_task_child(sock, reader, init, boot, runners)


def serve_program(
    sock: socket.socket,
    parent_pid: int,
    boot: BootFn,
    runners: TaskRunners,
    handler_boot: HandlerBootFn,
) -> NoReturn:
    """Run the child program that the worker starts by ``exec``: a task
    child as ``serve`` runs it, or the handler child of the worker when the
    init frame names the handler kind (``handler_child.serve_handler_child``,
    which boots the app with ``handler_boot``). Ends the process."""
    ignore_stop_signals()
    bind_to_parent(parent_pid)
    reader = ParentFrameReader(sock)
    init = read_python_init_frame(sock, reader)
    match init.child_kind:
        case core.HandlerChildKind() as kind:
            from horsies.core.worker.handler_child import serve_handler_child

            serve_handler_child(sock, reader, init, kind, handler_boot)
        case core.IsolatedChildKind() | core.SharedAsyncChildKind() | core.SharedThreadChildKind():
            _serve_task_child(sock, reader, init, boot, runners)
        case _ as unreachable:
            assert_never(unreachable)


def _serve_task_child(
    sock: socket.socket,
    reader: ParentFrameReader,
    init: core.InitFrame,
    boot: BootFn,
    runners: TaskRunners,
) -> NoReturn:
    """Boot the task child of ``init`` and serve its runs until the stop
    frame (``serve``)."""
    work = ParentFrameInbox()
    task_loop = _boot_or_exit(sock, reader, init, boot, work)
    match task_child_kind_of(init):
        case core.IsolatedChildKind():
            _serve_isolated(sock, init, work, task_loop, runners)
        case core.SharedAsyncChildKind() as kind:
            from horsies.core.worker.shared_child import serve_shared

            serve_shared(sock, init, work, task_loop, kind, runners.seats)
        case core.SharedThreadChildKind(seats=seats):
            from horsies.core.worker.thread_child import serve_thread

            serve_thread(sock, init, work, task_loop, seats, runners.threads)
        case _ as unreachable:
            assert_never(unreachable)


def _serve_isolated(
    sock: socket.socket,
    init: core.InitFrame,
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
            case core.RunFrame() as frame:
                _refuse_run_on_a_child_to_retire(after_run, frame)
                after_run = _report_run(
                    sock,
                    frame,
                    _run_or_exit(runners.run_task, frame, init, task_loop),
                    task_loop,
                    runners.mark_finalizing,
                )
            case core.SharedRunFrame():
                _exit_bad_parent_frame('Parent sent a shared run frame to an isolated child')
            case core.StopFrame():
                exit_after_stop(task_loop.close())
            case _ as unreachable:
                assert_never(unreachable)


def restore_parent_state(init: core.InitFrame) -> None:
    """Restore the import state and the names that the worker's process had."""
    program = python_program_of(init)
    sys.path[:] = list(program.parent_sys_path)
    sys.argv[:] = list(program.parent_argv)
    multiprocessing.current_process().name = init.process_name


def boot_app(init: core.InitFrame) -> Result[AsyncTaskPresence, BootFailedFrame]:
    """Restore the parent's import state, import the app, then run the
    per-child start steps.

    Every kind builds its core handle from ``init`` here
    (``child_runner.finish_child_start``); every write of the child goes
    through it. A shared async child runs its async start hooks afterwards,
    on its event loop (``shared_child``).

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
        import_app_for_child,
        log_level_of,
    )

    restore_parent_state(init)
    program = python_program_of(init)
    try:
        app = import_app_for_child(
            program.app_locator,
            program.imports,
            program.sys_path_roots,
            log_level_of(init.log_level),
            init.child_kind.task_send_pool,
        )
    except Exception as exc:
        return Err(boot_failure_from_exception(exc, AppLoadFailed()))
    try:
        finish_child_start(app, init)
    except Exception as exc:
        return Err(boot_failure_from_exception(exc, ChildPoolSetupFailed()))
    return Ok(async_task_presence(app))


def run_child_task(
    frame: core.RunFrame,
    init: core.InitFrame,
    task_loop: TaskLoop,
) -> RunReturn:
    """Run the task of ``frame`` (``child_runner._run_task_entry``). Writes
    no finalizing marker."""
    from horsies.core.worker.child_runner import _run_task_entry, isolated_work_of

    return _run_task_entry(
        frame.task_name,
        frame.input,
        frame.task_id,
        frame.worker_id,
        isolated_work_of(frame.work),
        claimed_at=frame.claimed_at,
        task_loop=task_loop,
        orphan_self_heal=frame.orphan_self_heal,
        queue_name=frame.queue_name,
        attempt=frame.attempt,
        trace=run_trace_of(frame.trace),
    )


def mark_child_task_finalizing(frame: core.RunFrame) -> MarkerEnd:
    """Write the finalizing marker of the isolated run of ``frame``, which
    set its row RUNNING and has ended (``child_runner.mark_run_finalizing``)."""
    from horsies.core.worker.child_runner import mark_run_finalizing

    return mark_run_finalizing(frame.task_id, frame.worker_id, frame.claimed_at)


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


def boot_handler_child_app(
    init: core.InitFrame,
    kind: core.HandlerChildKind,
) -> Result[Horsies, BootFailedFrame]:
    """The boot step of a handler child
    (``handler_child.boot_handler_app``)."""
    from horsies.core.worker.handler_child import boot_handler_app

    return boot_handler_app(init, kind)


def main(argv: Sequence[str]) -> NoReturn:
    """Program entry. After a stop frame, ``serve_program`` ends the
    process."""
    parsed = parse_arguments(argv)
    if parsed is None:
        sys.stderr.write(f'child_main: bad arguments: {list(argv)!r}\n')
        sys.exit(BAD_ARGUMENTS_EXIT_CODE)
    fd, parent_pid = parsed
    serve_program(
        socket.socket(fileno=fd),
        parent_pid,
        boot_app,
        production_runners(),
        boot_handler_child_app,
    )


if __name__ == '__main__':
    main(sys.argv[1:])
