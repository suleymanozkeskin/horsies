"""Child program: ``python -m horsies.core.worker.child_main <fd> <parent pid>``.

The child has two threads. The control thread owns socket reads, so EOF means
the parent is gone, whatever the main thread is doing. The main thread boots
the app, runs tasks one at a time, and writes results. An ``async def`` task
runs on the child's one task loop (``task_loop.TaskLoop``), on the main thread.
The child creates that loop at boot, before it reports ready, when its app has
an ``async def`` task.

When the parent is gone the child exits at once. Nothing can save its result,
and a run that continues could overlap a retry of the same task.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import ctypes
import gc
import logging
import multiprocessing
import os
import queue
import signal
import socket
import sys
import threading
from collections.abc import Callable, Sequence
from typing import Final, NoReturn, assert_never

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.logging import get_logger
from horsies.core.models.tasks import OperationalErrorCode, TaskError, TaskResult
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    MAX_BODY_BYTES,
    ChildFrame,
    FrameOverBound,
    InitFrame,
    LoopCreateFailedFrame,
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
    StopFrame,
    decode_parent_frame,
    encode_child_frame,
    parse_header,
)
from horsies.core.worker.runtime import _read_current_rss_mb
from horsies.core.worker.task_loop import (
    AsyncTaskPresence,
    ExecutorAfterClose,
    ExecutorShutDown,
    ExecutorThreadsLeft,
    LoopCreateFailed,
    TaskLoop,
)

logger = get_logger('worker')

PARENT_GONE_EXIT_CODE: Final = 71
BAD_PARENT_FRAME_EXIT_CODE: Final = 72
BAD_ARGUMENTS_EXIT_CODE: Final = 73
# After a stop frame, when threads of the task loop's executor still run:
# the child leaves without waiting for them, so ``atexit`` handlers do not run.
STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE: Final = 74
# At boot, after ``LoopCreateFailedFrame``: the app has an ``async def`` task,
# and the child could not create the event loop for it.
TASK_LOOP_CREATE_FAILED_EXIT_CODE: Final = 75
INIT_FRAME_TIMEOUT_S: Final = 30.0
# Metadata holds this text as JSON, and one character takes at most 12 bytes
# there, so the reason stays well under ``MAX_META_BYTES``.
MAX_FAILED_REASON_CHARS: Final = 64 * 1024

_PR_SET_PDEATHSIG: Final = 1

WireResult = tuple[bool, str, str | None]
BootFn = Callable[[InitFrame], AsyncTaskPresence]
RunTaskFn = Callable[[RunFrame, InitFrame, TaskLoop], WireResult]


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


def _recv_parent_frame(sock: socket.socket) -> ParentFrame:
    """Read one frame. A bad frame from the parent is a defect: exit loud."""
    match parse_header(_recv_exact(sock, FRAME_HEADER.size)):
        case Ok(sizes):
            pass
        case Err(FrameOverBound() as over):
            logger.critical('Parent sent a frame over the bounds: %s', over)
            os._exit(BAD_PARENT_FRAME_EXIT_CODE)
    meta = _recv_exact(sock, sizes.meta_bytes)
    body = _recv_exact(sock, sizes.body_bytes)
    match decode_parent_frame(meta, body):
        case Ok(frame):
            return frame
        case Err(malformed):
            logger.critical('Parent sent a bad frame: %s', malformed.reason)
            os._exit(BAD_PARENT_FRAME_EXIT_CODE)


def _send_child_frame(sock: socket.socket, frame: ChildFrame) -> None:
    """Write one frame. A failed write means the parent is gone."""
    encoded = encode_child_frame(frame)
    try:
        sock.sendall(encoded.head)
        sock.sendall(encoded.body)
    except OSError:
        os._exit(PARENT_GONE_EXIT_CODE)


def _read_parent_frames(
    sock: socket.socket,
    work: queue.SimpleQueue[RunFrame | StopFrame],
) -> None:
    """Control thread body. Ends the process when the parent is gone."""
    try:
        while True:
            match _recv_parent_frame(sock):
                case InitFrame():
                    logger.critical('Parent sent a second init frame')
                    os._exit(BAD_PARENT_FRAME_EXIT_CODE)
                case RunFrame() | StopFrame() as frame:
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
        case RunFrame() | StopFrame():
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


def _bounded_result(frame: RunFrame, wire: WireResult) -> WireResult:
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
        f'result of task {frame.task_name} is {size} bytes, above the '
        f'{MAX_BODY_BYTES} byte bound'
    )
    logger.error(message)
    failure: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
            message=message,
            data={'task_name': frame.task_name, 'result_bytes': size},
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
) -> WireResult:
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


def _refuse_run_on_a_child_to_retire(task_loop: TaskLoop, frame: RunFrame) -> None:
    """Exit loud when the task loop report says this child must be retired.

    The child reports work left on its loop in its result frame, and the
    parent retires the child. So a run frame here is a defect in the parent.
    """
    match task_loop.report():
        case NoTaskLoop() | LoopHoldsNoTask():
            return
        case LoopHoldsTasks() as leftover:
            logger.critical(
                'Parent sent task %s to a child whose task loop holds %d task(s) '
                'of an earlier task (%s); such a child must be retired',
                frame.task_id,
                leftover.count,
                leftover.names_text(),
            )
        case _ as unreachable:
            assert_never(unreachable)
    flush_output()
    os._exit(BAD_PARENT_FRAME_EXIT_CODE)


def open_task_loop_at_boot(
    sock: socket.socket,
    presence: AsyncTaskPresence,
) -> TaskLoop:
    """Return the task loop of this child, open when its app has an
    ``async def`` task, and not created when it has none.

    When the loop cannot be created, the child sends ``LoopCreateFailedFrame``
    and exits with ``TASK_LOOP_CREATE_FAILED_EXIT_CODE``, before its ready
    frame. The parent counts that as a failed boot attempt, and no task
    reaches this child.
    """
    task_loop = TaskLoop()
    match presence:
        case AsyncTaskPresence.NO_ASYNC_TASK:
            return task_loop
        case AsyncTaskPresence.HAS_ASYNC_TASKS:
            match task_loop.open():
                case Ok():
                    return task_loop
                case Err(failed):
                    _fail_boot_without_task_loop(sock, failed)


def _fail_boot_without_task_loop(
    sock: socket.socket,
    failed: LoopCreateFailed,
) -> NoReturn:
    """Report the failed loop creation to the parent, then end the process.

    The child has run no task, so it does not wait for threads that boot
    started: it flushes its log handlers and its output, and leaves with
    ``os._exit``.
    """
    _send_child_frame(
        sock,
        LoopCreateFailedFrame(
            error_name=bounded_text(failed.error_name, MAX_FAILED_REASON_CHARS),
            detail=bounded_text(failed.detail, MAX_FAILED_REASON_CHARS),
        ),
    )
    logging.shutdown()
    flush_output()
    os._exit(TASK_LOOP_CREATE_FAILED_EXIT_CODE)


def exit_after_stop(executor: ExecutorAfterClose) -> NoReturn:
    """End the process after a stop frame and the task loop close.

    ``ExecutorShutDown``: ``sys.exit(0)``, so ``atexit`` handlers run.

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
            sys.exit(0)
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


def serve(
    sock: socket.socket,
    parent_pid: int,
    boot: BootFn,
    run_task: RunTaskFn,
) -> NoReturn:
    """Run the child until the parent sends a stop frame, then end the process.

    After ``boot``, the child creates its ``TaskLoop`` when ``boot`` reports
    an ``async def`` task in the app, and only then sends its ready frame.
    Every task of the child gets that ``TaskLoop``. Each result frame reports
    the state of that loop. The stop frame closes it, bounded by
    ``LOOP_CLOSE_TIMEOUT_S``, and ``exit_after_stop`` ends the process.

    Failure exits: a loop that cannot be created ends the boot through
    ``open_task_loop_at_boot``, before the ready frame. A run frame after a
    report that this child must be retired is a parent defect and ends the
    process with ``BAD_PARENT_FRAME_EXIT_CODE``. An exception from ``boot``
    propagates, so the child exits non-zero before it reports ready. A parent
    that is gone or that sends a bad frame ends the process through
    ``os._exit``, and so does an exception from ``run_task``: the parent sees
    the exit and recovers the row by its status. Does not write to the
    database itself.
    """
    ignore_stop_signals()
    bind_to_parent(parent_pid)
    init = read_init_frame(sock)
    work: queue.SimpleQueue[RunFrame | StopFrame] = queue.SimpleQueue()
    threading.Thread(
        target=_read_parent_frames,
        args=(sock, work),
        name='horsies-child-control',
        daemon=True,
    ).start()
    task_loop = open_task_loop_at_boot(sock, boot(init))
    gc.collect()
    _send_child_frame(sock, ReadyFrame(pid=os.getpid(), rss=_read_rss()))
    while True:
        match work.get():
            case RunFrame() as frame:
                _refuse_run_on_a_child_to_retire(task_loop, frame)
                ok, result_json, failed_reason = _bounded_result(
                    frame,
                    _run_or_exit(run_task, frame, init, task_loop),
                )
                _send_child_frame(
                    sock,
                    ResultFrame(
                        task_id=frame.task_id,
                        ok=ok,
                        result_json=result_json,
                        failed_reason=failed_reason,
                        rss=_read_rss(),
                        task_loop=task_loop.report(),
                    ),
                )
            case StopFrame():
                exit_after_stop(task_loop.close())
            case _ as unreachable:
                assert_never(unreachable)


def restore_parent_state(init: InitFrame) -> None:
    """Restore the import state and the names that the worker's process had."""
    sys.path[:] = list(init.parent_sys_path)
    sys.argv[:] = list(init.parent_argv)
    multiprocessing.current_process().name = init.process_name


def boot_app(init: InitFrame) -> AsyncTaskPresence:
    """Restore the parent's import state, then run the child initializer.

    Returns whether the booted app has an ``async def`` task, which decides
    whether the child creates its task loop.
    """
    from horsies.core.worker.child_runner import (
        _child_initializer,
        async_task_presence,
    )
    from horsies.core.worker.current import get_current_app

    restore_parent_state(init)
    _child_initializer(
        init.app_locator,
        list(init.imports),
        list(init.sys_path_roots),
        init.loglevel,
        init.database_url,
        dict(init.connect_kwargs),
        init.child_pool_min_size,
        init.child_pool_max_size,
        init.child_pool_check,
    )
    return async_task_presence(get_current_app())


def run_child_task(frame: RunFrame, init: InitFrame, task_loop: TaskLoop) -> WireResult:
    from horsies.core.worker.child_runner import _run_task_entry

    return _run_task_entry(
        frame.task_name,
        frame.args_json,
        frame.kwargs_json,
        frame.task_id,
        init.database_url,
        frame.worker_id,
        frame.runner_heartbeat_interval_ms,
        frame.is_workflow_task,
        frame.timeout,
        frame.claimed_at,
        task_loop=task_loop,
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
    serve(socket.socket(fileno=fd), parent_pid, boot_app, run_child_task)


if __name__ == '__main__':
    main(sys.argv[1:])
