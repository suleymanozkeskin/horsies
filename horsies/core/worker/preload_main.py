"""The preload parent: one exec'd process that forks worker children.

Run as ``python -m horsies.core.worker.preload_main <socket fd> <worker pid>
<mode>``. Linux only.

It imports code once, freezes the heap, and forks a child for each request, so
children start in milliseconds and share those pages. It runs no task and opens
no connection. It stays single-threaded, because a fork copies only the calling
thread.

Each fork is a double fork. The middle process exits at once, so the child
reparents to the worker, which holds ``PR_SET_CHILD_SUBREAPER`` during the
spawn. The worker is then the real parent: it reaps the child, and the child's
parent-death signal follows the worker. The death of this process touches no
running child.
"""

from __future__ import annotations

import gc
import importlib
import os
import socket
import sys
import threading
import time
from collections.abc import Callable, Sequence
from typing import Final, NoReturn, assert_never

from horsies.core.logging import get_logger
from horsies.core.worker.child_main import (
    BAD_ARGUMENTS_EXIT_CODE,
    BAD_PARENT_FRAME_EXIT_CODE,
    PARENT_GONE_EXIT_CODE,
    BootFn,
    RunTaskFn,
    bind_to_parent,
    boot_app,
    ignore_stop_signals,
    read_init_frame,
    restore_parent_state,
    run_child_task,
    boot_with_preloaded_app,
    serve,
)
from horsies.core.worker.child_protocol import (
    ChildForkedFrame,
    ForkFailedFrame,
    InitFrame,
    PreloadFrame,
    PreloadReadyFrame,
    PreloadRefusedFrame,
    encode_preload_frame,
)
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.preload_protocol import SPAWN_REQUEST

logger = get_logger('worker')

REPARENT_FAILED_EXIT_CODE: Final = 74
REPARENT_TIMEOUT_S: Final = 5.0
REPARENT_POLL_S: Final = 0.0002
# Importing these gives a child the library code that it would otherwise load
# after its start. Task code stays out, so module state stays per child.
LIBRARY_MODULES: Final[tuple[str, ...]] = (
    'horsies',
    'horsies.core.app',
    'horsies.core.brokers.postgres',
    'horsies.core.worker.child_runner',
)

PrepareFn = Callable[[InitFrame], BootFn]


def _send_frame(ctrl: socket.socket, frame: PreloadFrame) -> None:
    """Write one frame. A failed write means the worker is gone."""
    try:
        ctrl.sendall(encode_preload_frame(frame).head)
    except OSError:
        os._exit(PARENT_GONE_EXIT_CODE)


def _prepare_library(init: InitFrame) -> BootFn:
    """Import the library only. Each child still imports the app itself."""
    restore_parent_state(init)
    for module in LIBRARY_MODULES:
        importlib.import_module(module)
    return boot_app


def _prepare_app(init: InitFrame) -> BootFn:
    """Import the library and the app. Children then share module state."""
    from horsies.core.worker.child_runner import import_app_for_child

    _prepare_library(init)
    app = import_app_for_child(
        init.app_locator,
        list(init.imports),
        list(init.sys_path_roots),
        init.loglevel,
    )
    return boot_with_preloaded_app(app)


def _open_socket_fds(ctrl_fd: int) -> list[int]:
    """Return the socket descriptors that a fork would share between children.

    The standard streams stay out: under a supervisor they can be sockets, and
    every process shares them on purpose.
    """
    found: list[int] = []
    for entry in os.listdir('/proc/self/fd'):
        fd = int(entry)
        if fd in (0, 1, 2, ctrl_fd):
            continue
        try:
            target = os.readlink(f'/proc/self/fd/{fd}')
        except OSError:
            # The descriptor of this listing, closed by now.
            continue
        if target.startswith('socket:'):
            found.append(fd)
    return found


def fork_refusal(ctrl_fd: int) -> str | None:
    """Return why a fork from this process is not safe, or None.

    A fork copies a lock in whatever state another thread left it, and every
    child would share a socket on one connection.
    """
    current = threading.current_thread()
    threads = [thread.name for thread in threading.enumerate() if thread is not current]
    if threads:
        return f'threads started during import: {threads}'
    sockets = _open_socket_fds(ctrl_fd)
    if sockets:
        return f'sockets opened during import: descriptors {sockets}'
    return None


def _wait_for_reparent(worker_pid: int) -> None:
    """Wait until the middle process is gone and the worker is the parent.

    ``serve`` binds this child to its real parent, so it must not run before.
    """
    deadline = time.monotonic() + REPARENT_TIMEOUT_S
    while os.getppid() != worker_pid:
        if time.monotonic() > deadline:
            os._exit(REPARENT_FAILED_EXIT_CODE)
        time.sleep(REPARENT_POLL_S)


def _run_child(
    child_fd: int,
    worker_pid: int,
    boot: BootFn,
    run_task: RunTaskFn,
) -> NoReturn:
    """Become a worker child. Exits 0 after a stop frame, so ``atexit`` runs."""
    os.setsid()
    _wait_for_reparent(worker_pid)
    serve(socket.socket(fileno=child_fd), worker_pid, boot, run_task)
    sys.exit(0)


def _run_middle(
    ctrl: socket.socket,
    child_fd: int,
    write_end: int,
    worker_pid: int,
    boot: BootFn,
    run_task: RunTaskFn,
) -> NoReturn:
    """Fork the child, report its pid, and exit, which orphans the child."""
    try:
        child = os.fork()
    except OSError:
        os._exit(1)
    if child == 0:
        ctrl.close()
        os.close(write_end)
        _run_child(child_fd, worker_pid, boot, run_task)
    os.write(write_end, str(child).encode('ascii'))
    os._exit(0)


def _fork_child(
    ctrl: socket.socket,
    child_fd: int,
    worker_pid: int,
    boot: BootFn,
    run_task: RunTaskFn,
) -> PreloadFrame:
    """Run one double fork. Returns after the middle process is reaped."""
    read_end, write_end = os.pipe()
    try:
        middle = os.fork()
    except OSError as exc:
        os.close(read_end)
        os.close(write_end)
        return ForkFailedFrame(f'{type(exc).__name__}: {exc}')
    if middle == 0:
        os.close(read_end)
        _run_middle(ctrl, child_fd, write_end, worker_pid, boot, run_task)
    os.close(write_end)
    reported = os.read(read_end, 32)
    os.close(read_end)
    os.waitpid(middle, 0)
    if not reported:
        return ForkFailedFrame('the middle process could not fork the child')
    return ChildForkedFrame(pid=int(reported))


def _serve_requests(
    ctrl: socket.socket,
    worker_pid: int,
    boot: BootFn,
    run_task: RunTaskFn,
) -> NoReturn:
    """Fork one child per request, until the worker closes the socket."""
    while True:
        try:
            message, fds, _flags, _address = socket.recv_fds(ctrl, 1, 1)
        except OSError:
            os._exit(PARENT_GONE_EXIT_CODE)
        if not message:
            os._exit(0)
        if message != SPAWN_REQUEST or len(fds) != 1:
            logger.critical('Worker sent a bad spawn request')
            os._exit(BAD_PARENT_FRAME_EXIT_CODE)
        frame = _fork_child(ctrl, fds[0], worker_pid, boot, run_task)
        os.close(fds[0])
        _send_frame(ctrl, frame)


def run(
    ctrl: socket.socket,
    worker_pid: int,
    prepare: PrepareFn,
    run_task: RunTaskFn,
) -> NoReturn:
    """Prepare once, report, then serve spawn requests.

    Failure exits: an import that raises, or a state that is not safe to fork
    from, becomes a refused frame and exit 1. The worker decides what that
    means for this mode.
    """
    ignore_stop_signals()
    bind_to_parent(worker_pid)
    init = read_init_frame(ctrl)
    try:
        boot = prepare(init)
    except Exception as exc:
        _send_frame(ctrl, PreloadRefusedFrame(f'{type(exc).__name__}: {exc}'))
        os._exit(1)
    refusal = fork_refusal(ctrl.fileno())
    if refusal is not None:
        _send_frame(ctrl, PreloadRefusedFrame(refusal))
        os._exit(1)
    gc.collect()
    gc.freeze()
    _send_frame(ctrl, PreloadReadyFrame(pid=os.getpid()))
    _serve_requests(ctrl, worker_pid, boot, run_task)


def parse_arguments(argv: Sequence[str]) -> tuple[int, int, ChildPreload] | None:
    """Return (socket fd, worker pid, mode), or None when they are invalid."""
    if len(argv) != 3 or not argv[0].isdecimal() or not argv[1].isdecimal():
        return None
    try:
        mode = ChildPreload(argv[2])
    except ValueError:
        return None
    return int(argv[0]), int(argv[1]), mode


def main(argv: Sequence[str]) -> NoReturn:
    """Program entry."""
    parsed = parse_arguments(argv)
    if parsed is None:
        sys.stderr.write(f'preload_main: bad arguments: {list(argv)!r}\n')
        sys.exit(BAD_ARGUMENTS_EXIT_CODE)
    fd, worker_pid, mode = parsed
    ctrl = socket.socket(fileno=fd)
    match mode:
        case ChildPreload.LIBRARY:
            run(ctrl, worker_pid, _prepare_library, run_child_task)
        case ChildPreload.APP:
            run(ctrl, worker_pid, _prepare_app, run_child_task)
        case ChildPreload.NONE:
            sys.stderr.write('preload_main: mode none starts no preload parent\n')
            sys.exit(BAD_ARGUMENTS_EXIT_CODE)
        case _ as unreachable:
            assert_never(unreachable)


if __name__ == '__main__':
    main(sys.argv[1:])
