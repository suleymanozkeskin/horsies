"""A child program with scripted behavior, for slot pool tests.

It speaks the real protocol through ``child_main.serve``. Only boot and task
execution are replaced: ``task_name`` selects an operation, ``kwargs_json``
holds its argument, and ``app_locator`` selects the boot behavior. It needs no
database.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import ctypes
import gc
import os
import signal
import socket
import subprocess
import sys
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

_sock: socket.socket


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
        case 'parent_identity':
            return
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


def _half_frame() -> bytes:
    encoded = encode_parent_frame(StopFrame())
    return encoded.head[: FRAME_HEADER.size] + b'x' * 8


def _run(frame: RunFrame, init: InitFrame) -> WireResult:
    argument = frame.kwargs_json or ''
    match frame.task_name:
        case 'echo':
            return True, argument, None
        case 'pid':
            return _pid_result()
        case 'freeze_count':
            return True, str(gc.get_freeze_count()), None
        case 'socket_count':
            return True, str(_socket_count()), None
        case 'state':
            return True, f'{sys.argv!r}|{sys.path[:1]!r}|{__debug__}', None
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
    sys.exit(0)


if __name__ == '__main__':
    main()
