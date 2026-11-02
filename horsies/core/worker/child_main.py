"""Child program: ``python -m horsies.core.worker.child_main <fd> <parent pid>``.

The child has two threads. The control thread owns socket reads, so EOF means
the parent is gone, whatever the main thread is doing. The main thread boots
the app, runs tasks one at a time, and writes results.

When the parent is gone the child exits at once. Nothing can save its result,
and a run that continues could overlap a retry of the same task.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import ctypes
import gc
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

logger = get_logger('worker')

PARENT_GONE_EXIT_CODE: Final = 71
BAD_PARENT_FRAME_EXIT_CODE: Final = 72
BAD_ARGUMENTS_EXIT_CODE: Final = 73
INIT_FRAME_TIMEOUT_S: Final = 30.0
# Metadata holds this text as JSON, and one character takes at most 12 bytes
# there, so the reason stays well under ``MAX_META_BYTES``.
MAX_FAILED_REASON_CHARS: Final = 64 * 1024

_PR_SET_PDEATHSIG: Final = 1

WireResult = tuple[bool, str, str | None]
BootFn = Callable[[InitFrame], None]
RunTaskFn = Callable[[RunFrame, InitFrame], WireResult]


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


def _bind_to_parent(parent_pid: int) -> None:
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


def _ignore_stop_signals() -> None:
    """Leave shutdown to the parent, also when a signal reaches every process.

    SIGTERM gets a handler, not ``SIG_IGN``: ``exec`` resets a handler but
    keeps an ignored signal, and an ignored SIGTERM would make
    ``Popen.terminate()`` in task code do nothing.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, lambda _signum, _frame: None)


def _read_init_frame(sock: socket.socket) -> InitFrame:
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


def _flush_output() -> None:
    """Flush before ``os._exit``, which skips it. A closed stream is no failure."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except (OSError, ValueError):
            continue


def _run_or_exit(run_task: RunTaskFn, frame: RunFrame, init: InitFrame) -> WireResult:
    """Run one task. Anything that escapes ends this process at once.

    A normal exit would wait for every non-daemon thread that the task started,
    so the slot would stay busy with no bound. The parent sees this exit and
    recovers the row by its status.
    """
    try:
        return run_task(frame, init)
    except SystemExit as exc:
        _flush_output()
        os._exit(exc.code if isinstance(exc.code, int) else 1)
    except BaseException:
        logger.critical('Task %s escaped the task runner', frame.task_id, exc_info=True)
        _flush_output()
        os._exit(1)


def serve(
    sock: socket.socket,
    parent_pid: int,
    boot: BootFn,
    run_task: RunTaskFn,
) -> None:
    """Run the child until the parent sends a stop frame.

    Failure exits: an exception from ``boot`` propagates, so the child exits
    non-zero before it reports ready. A parent that is gone or that sends a
    bad frame ends the process through ``os._exit``, and so does an exception
    from ``run_task``: the parent sees the exit and recovers the row by its
    status. Does not write to the database itself.
    """
    _ignore_stop_signals()
    _bind_to_parent(parent_pid)
    init = _read_init_frame(sock)
    work: queue.SimpleQueue[RunFrame | StopFrame] = queue.SimpleQueue()
    threading.Thread(
        target=_read_parent_frames,
        args=(sock, work),
        name='horsies-child-control',
        daemon=True,
    ).start()
    boot(init)
    gc.collect()
    _send_child_frame(sock, ReadyFrame(pid=os.getpid(), rss=_read_rss()))
    while True:
        match work.get():
            case RunFrame() as frame:
                ok, result_json, failed_reason = _bounded_result(
                    frame,
                    _run_or_exit(run_task, frame, init),
                )
                _send_child_frame(
                    sock,
                    ResultFrame(
                        task_id=frame.task_id,
                        ok=ok,
                        result_json=result_json,
                        failed_reason=failed_reason,
                        rss=_read_rss(),
                    ),
                )
            case StopFrame():
                return
            case _ as unreachable:
                assert_never(unreachable)


def _boot_app(init: InitFrame) -> None:
    """Restore the parent's import state, then run the child initializer."""
    from horsies.core.worker.child_runner import _child_initializer

    sys.path[:] = list(init.parent_sys_path)
    sys.argv[:] = list(init.parent_argv)
    multiprocessing.current_process().name = init.process_name
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


def _run_task(frame: RunFrame, init: InitFrame) -> WireResult:
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
        frame.timeout_ms,
        frame.claimed_at,
    )


def parse_arguments(argv: Sequence[str]) -> tuple[int, int] | None:
    """Return (socket fd, parent pid), or None when the arguments are invalid."""
    if len(argv) != 2 or not argv[0].isdecimal() or not argv[1].isdecimal():
        return None
    return int(argv[0]), int(argv[1])


def main(argv: Sequence[str]) -> NoReturn:
    """Program entry. Exits 0 after a stop frame, so ``atexit`` handlers run."""
    parsed = parse_arguments(argv)
    if parsed is None:
        sys.stderr.write(f'child_main: bad arguments: {list(argv)!r}\n')
        sys.exit(BAD_ARGUMENTS_EXIT_CODE)
    fd, parent_pid = parsed
    serve(socket.socket(fileno=fd), parent_pid, _boot_app, _run_task)
    sys.exit(0)


if __name__ == '__main__':
    main(sys.argv[1:])
