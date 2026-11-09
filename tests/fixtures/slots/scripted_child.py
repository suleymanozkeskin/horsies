"""A child program with scripted behavior, for slot pool tests.

It speaks the real protocol through ``child_main.serve``. Only boot and task
execution are replaced: ``task_name`` selects an operation, ``kwargs_json``
holds its argument, and ``app_locator`` selects the boot behavior. It needs no
database.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import ctypes
import gc
import os
import signal
import socket
import subprocess
import sys
import threading
import time

from horsies.core.worker.child_main import WireResult, _send_child_frame, serve
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    InitFrame,
    ResultFrame,
    RssUnreadable,
    RunFrame,
    StopFrame,
    encode_parent_frame,
)
from horsies.core.worker.child_runner import CHILD_HOOK_FAILURE_EXIT_CODE
from horsies.core.utils.loop_runner import in_running_event_loop
from horsies.core.worker import task_loop as task_loop_module
from horsies.core.worker.task_loop import TaskLoop

_sock: socket.socket
# The loop holds only weak references to its tasks.
_kept_tasks: list[asyncio.Task[None]] = []
# Strong references, so that a freed loop cannot pass its id to a new one.
_seen_loops: list[asyncio.AbstractEventLoop] = []


def _pid_result() -> WireResult:
    return True, str(os.getpid()), None


def _boot(init: InitFrame) -> None:
    mode, _, argument = init.app_locator.partition(':')
    match mode:
        case 'ok':
            return
        case 'hook_fails':
            os._exit(CHILD_HOOK_FAILURE_EXIT_CODE)
        case 'raises':
            raise RuntimeError('scripted boot failure')
        case 'hangs':
            time.sleep(3600)
        case 'fails_first':
            # Fails until a counter file holds enough marks.
            with open(argument, 'a+', encoding='ascii') as marks:
                marks.write('x')
                marks.seek(0)
                if len(marks.read()) < int(init.imports[0]):
                    raise RuntimeError('scripted transient boot failure')
        case 'killed_always':
            os.kill(os.getpid(), signal.SIGKILL)
        case 'killed_after_first' | 'fails_after_first':
            # The first boots pass, then the next boots end, while marks last.
            with open(argument, 'a+', encoding='ascii') as marks:
                marks.write('x')
                marks.seek(0)
                seen = len(marks.read())
            first_boots = int(init.imports[1])
            if first_boots < seen <= first_boots + int(init.imports[0]):
                if mode == 'killed_after_first':
                    os.kill(os.getpid(), signal.SIGKILL)
                raise RuntimeError('scripted replacement boot failure')
        case 'parent_identity':
            return
        case 'close_timeout':
            # A short close deadline keeps a test of the close bound fast.
            setattr(task_loop_module, 'LOOP_CLOSE_TIMEOUT_S', float(argument))
        case _:
            raise AssertionError(mode)


def _socket_count() -> int:
    """Count the sockets this process holds, past the standard streams."""
    count = 0
    for entry in os.listdir('/proc/self/fd'):
        fd = int(entry)
        try:
            count += fd > 2 and os.readlink(f'/proc/self/fd/{fd}').startswith('socket:')
        except OSError:
            continue
    return count


async def _loops_seen() -> str:
    """Count the distinct loops that ran a ``loop_count`` operation."""
    loop = asyncio.get_running_loop()
    if not any(seen is loop for seen in _seen_loops):
        _seen_loops.append(loop)
    return str(len(_seen_loops))


async def _mark_when_cancelled(path: str) -> None:
    """Write the marker only on a cancel. A coroutine that is closed at
    interpreter exit gets GeneratorExit, which must not write it."""
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        with open(path, 'w', encoding='ascii') as target:
            target.write('cancelled')
        raise


async def _leave_pending(path: str) -> str:
    _kept_tasks.append(asyncio.get_running_loop().create_task(_mark_when_cancelled(path)))
    await asyncio.sleep(0)
    return str(os.getpid())


async def _thread_outlives_wait(seconds: float) -> str:
    """Stop waiting for an executor thread that keeps running."""
    try:
        await asyncio.wait_for(asyncio.to_thread(time.sleep, seconds), 0.05)
    except TimeoutError:
        return str(os.getpid())
    raise AssertionError('the executor thread ended within the wait')


def _no_running_loop() -> str:
    """Report a running loop, then run one with ``asyncio.run``."""
    return f'{in_running_event_loop()}|{asyncio.run(asyncio.sleep(0, "ran"))}'


def _half_frame() -> bytes:
    encoded = encode_parent_frame(StopFrame())
    return encoded.head[: FRAME_HEADER.size] + b'x' * 8


def _run(frame: RunFrame, init: InitFrame, task_loop: TaskLoop) -> WireResult:
    argument = frame.kwargs_json or ''
    match frame.task_name:
        case 'echo':
            return True, argument, None
        case 'loop_count':
            return True, task_loop.run(_loops_seen()).unwrap(), None
        case 'no_running_loop':
            return True, _no_running_loop(), None
        case 'leave_pending':
            return True, task_loop.run(_leave_pending(argument)).unwrap(), None
        case 'thread_outlives_wait':
            return True, task_loop.run(_thread_outlives_wait(float(argument))).unwrap(), None
        case 'pid':
            return _pid_result()
        case 'freeze_count':
            return True, str(gc.get_freeze_count()), None
        case 'socket_count':
            return True, str(_socket_count()), None
        case 'state':
            return True, f'{sys.argv!r}|{sys.path[:1]!r}|{__debug__}', None
        case 'zero_header':
            _sock.sendall(FRAME_HEADER.pack(0, 0))
            time.sleep(60)
            return _pid_result()
        case 'long_reason':
            return True, 'kept', 'r' * (2 << 20)
        case 'raise_with_thread':
            threading.Thread(target=time.sleep, args=(60,)).start()
            raise RuntimeError('escaped with a live thread')
        case 'sleep':
            time.sleep(float(argument))
            return _pid_result()
        case 'hang':
            time.sleep(3600)
            return _pid_result()
        case 'gil_hang':
            ctypes.PyDLL(None).sleep(3600)
            return _pid_result()
        case 'exit_zero':
            os._exit(0)
        case 'exit_after_result':
            _send_child_frame(
                _sock,
                ResultFrame(frame.task_id, True, 'complete', None, RssUnreadable()),
            )
            os._exit(1)
        case 'die_mid_frame':
            _sock.sendall(_half_frame())
            os.kill(os.getpid(), signal.SIGKILL)
            return _pid_result()
        case 'stop_mid_frame':
            _sock.sendall(_half_frame())
            os.kill(os.getpid(), signal.SIGSTOP)
            return _pid_result()
        case 'kill_self':
            os.kill(os.getpid(), signal.SIGKILL)
            return _pid_result()
        case 'segv':
            ctypes.string_at(8)
            return _pid_result()
        case 'abort':
            os.abort()
        case 'raises':
            raise RuntimeError('scripted task defect')
        case 'garbage':
            _sock.sendall(b'\xff' * 64)
            time.sleep(3600)
            return _pid_result()
        case 'bad_json_frame':
            _sock.sendall(FRAME_HEADER.pack(3, 0) + b'{{{')
            time.sleep(3600)
            return _pid_result()
        case 'wrong_task_id':
            _send_child_frame(
                _sock,
                ResultFrame('another-task', True, '', None, RssUnreadable()),
            )
            time.sleep(3600)
            return _pid_result()
        case 'close_socket':
            os.close(_sock.fileno())
            time.sleep(3600)
            return _pid_result()
        case 'fork_helper_hang':
            if os.fork() == 0:
                time.sleep(float(argument))
                os._exit(0)
            time.sleep(3600)
            return _pid_result()
        case 'fork_helper_result_then_die':
            if os.fork() == 0:
                time.sleep(float(argument))
                os._exit(0)
            _send_child_frame(
                _sock,
                ResultFrame(frame.task_id, True, 'complete', None, RssUnreadable()),
            )
            os.kill(os.getpid(), signal.SIGKILL)
            return _pid_result()
        case 'spawn_sub_hang':
            process = subprocess.Popen(['sleep', '60'])
            with open(argument, 'w', encoding='ascii') as target:
                target.write(str(process.pid))
            time.sleep(3600)
            return _pid_result()
        case 'spawn_sub':
            process = subprocess.Popen(['sleep', '60'])
            return True, str(process.pid), None
        case 'sub_terminate':
            process = subprocess.Popen(['sleep', '60'])
            process.terminate()
            return True, str(process.wait(timeout=5)), None
        case 'ignore_term_sleep':
            time.sleep(float(argument))
            return True, 'survived', None
        case _:
            raise AssertionError(frame.task_name)


def main() -> None:
    global _sock
    _sock = socket.socket(fileno=int(sys.argv[1]))
    serve(_sock, int(sys.argv[2]), _boot, _run)


if __name__ == '__main__':
    main()
