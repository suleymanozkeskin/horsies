"""A child program with scripted behavior, for slot pool tests.

It speaks the real protocol through ``child_main.serve``. Only boot and task
execution are replaced: ``task_name`` selects an operation, ``kwargs_json``
holds its argument, and ``app_locator`` selects the boot behavior. It needs no
database.

A shared async child runs the real seat loop of ``shared_child``. Only its
seat program is replaced: ``_ScriptedSeatProgram`` starts with no pool, and
each run is a scripted coroutine (``_seat_run``).

Each boot that passes reports an app with ``async def`` tasks, so ``serve``
creates the task loop, except the ``no_async`` modes. The ``loop_*`` modes make
that creation raise ``OSError(EMFILE)``. The ``report_*`` and ``short_*`` modes
return a boot failure frame with a named cause, as ``child_main.boot_app``
does.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import ctypes
import errno
import gc
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Coroutine
from typing import Any

from horsies.core.models.boot_failure import (
    AppLoadFailed,
    ChildPoolSetupFailed,
    ResourceShortage,
    ShortageErrno,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_main import (
    TaskRunners,
    WireResult,
    _send_child_frame,
    serve,
)
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    BootFailedFrame,
    InitFrame,
    NoTaskLoop,
    ResultFrame,
    RssUnreadable,
    RunFrame,
    SharedRunFrame,
    StopFrame,
    boot_failed_frame,
    encode_parent_frame,
)
from horsies.core.worker.shared_child import StartedSeats
from horsies.core.worker.child_runner import CHILD_HOOK_FAILURE_EXIT_CODE
from horsies.core.utils.loop_runner import in_running_event_loop
from horsies.core.worker import loop_watchdog as loop_watchdog_module
from horsies.core.worker import task_loop as task_loop_module
from horsies.core.worker.task_loop import AsyncTaskPresence, TaskLoop

_sock: socket.socket
# The loop holds only weak references to its tasks.
_kept_tasks: list[asyncio.Task[None]] = []
# Strong references, so that a freed loop cannot pass its id to a new one.
_seen_loops: list[asyncio.AbstractEventLoop] = []


def _pid_result() -> WireResult:
    return True, str(os.getpid()), None


def _no_descriptors() -> asyncio.AbstractEventLoop:
    raise OSError(errno.EMFILE, 'Too many open files')


def _boot_mark(path: str) -> int:
    """Add one mark for this boot. Return the marks so far, this one included."""
    with open(path, 'a+', encoding='ascii') as marks:
        marks.write('x')
        marks.seek(0)
        return len(marks.read())


def _loop_boot(mode: str, argument: str, init: InitFrame) -> AsyncTaskPresence:
    """Boot modes for the task loop creation at boot."""
    match mode:
        case 'no_async':
            return AsyncTaskPresence.NO_ASYNC_TASK
        case 'no_async_no_descriptors':
            asyncio.new_event_loop = _no_descriptors
            return AsyncTaskPresence.NO_ASYNC_TASK
        case 'loop_create_fails':
            asyncio.new_event_loop = _no_descriptors
        case 'loop_fails_first':
            # The first boots cannot create the loop, then boots pass.
            if _boot_mark(argument) <= int(init.imports[0]):
                asyncio.new_event_loop = _no_descriptors
        case 'loop_fails_after_first':
            # The first boots pass, then the next boots cannot create the loop.
            seen = _boot_mark(argument)
            first_boots = int(init.imports[1])
            if first_boots < seen <= first_boots + int(init.imports[0]):
                asyncio.new_event_loop = _no_descriptors
        case _:
            raise AssertionError(mode)
    return AsyncTaskPresence.HAS_ASYNC_TASKS


_SHORT_OF_DESCRIPTORS = boot_failed_frame(
    ResourceShortage(ShortageErrno.EMFILE),
    'OSError',
    '[Errno 24] Too many open files',
)


def _slot_index(init: InitFrame) -> int:
    """The slot of this child: the pool names each child ``<name>-<slot>``."""
    return int(init.process_name.rpartition('-')[2])


def _report_boot(
    mode: str,
    argument: str,
    init: InitFrame,
) -> Result[AsyncTaskPresence, BootFailedFrame]:
    """Boot modes that return a boot failure frame with a named cause."""
    match mode:
        case 'report_app_load':
            return Err(boot_failed_frame(AppLoadFailed(), 'ImportError', 'scripted'))
        case 'report_pool_setup':
            return Err(
                boot_failed_frame(ChildPoolSetupFailed(), 'ValueError', 'scripted'),
            )
        case 'short_always':
            return Err(_SHORT_OF_DESCRIPTORS)
        case 'short_on_slot':
            # Only the slot named in the argument is short of descriptors.
            if _slot_index(init) == int(argument):
                return Err(_SHORT_OF_DESCRIPTORS)
        case 'short_first':
            # The first boots are short of descriptors, then boots pass.
            if _boot_mark(argument) <= int(init.imports[0]):
                return Err(_SHORT_OF_DESCRIPTORS)
        case 'short_after_first':
            # The first boots pass, then the next boots are short of descriptors.
            seen = _boot_mark(argument)
            first_boots = int(init.imports[1])
            if first_boots < seen <= first_boots + int(init.imports[0]):
                return Err(_SHORT_OF_DESCRIPTORS)
        case 'short_then_hang':
            # The slot in imports[0] is short of descriptors once, then hangs.
            if _slot_index(init) == int(init.imports[0]):
                if _boot_mark(argument) == 1:
                    return Err(_SHORT_OF_DESCRIPTORS)
                time.sleep(3600)
        case _:
            raise AssertionError(mode)
    return Ok(AsyncTaskPresence.HAS_ASYNC_TASKS)


def _boot(init: InitFrame) -> Result[AsyncTaskPresence, BootFailedFrame]:
    mode, _, argument = init.app_locator.partition(':')
    match mode:
        case 'ok':
            pass
        case (
            'no_async'
            | 'no_async_no_descriptors'
            | 'loop_create_fails'
            | 'loop_fails_first'
            | 'loop_fails_after_first'
        ):
            return Ok(_loop_boot(mode, argument, init))
        case (
            'report_app_load'
            | 'report_pool_setup'
            | 'short_always'
            | 'short_on_slot'
            | 'short_first'
            | 'short_after_first'
            | 'short_then_hang'
        ):
            return _report_boot(mode, argument, init)
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
        case 'parent_identity' | 'seat_start_fails' | 'seat_hook_fails':
            pass
        case 'close_timeout':
            # A short close deadline keeps a test of the close bound fast.
            setattr(task_loop_module, 'LOOP_CLOSE_TIMEOUT_S', float(argument))
        case 'block_limit':
            # A short block limit keeps a test of the loop watchdog fast.
            setattr(loop_watchdog_module, 'LOOP_BLOCK_LIMIT_MS', int(argument))
        case _:
            raise AssertionError(mode)
    return Ok(AsyncTaskPresence.HAS_ASYNC_TASKS)


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


async def _async_pid() -> str:
    """Await all the work it starts, so the loop holds nothing after it."""
    await asyncio.get_running_loop().create_task(asyncio.sleep(0))
    return str(os.getpid())


async def _ignore_cancel() -> None:
    while True:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            continue


async def _idle_executor_beside_a_stubborn_task() -> str:
    """Leave the default executor idle, and a task that ignores the cancel."""
    await asyncio.to_thread(int)
    _kept_tasks.append(asyncio.get_running_loop().create_task(_ignore_cancel()))
    await asyncio.sleep(0)
    return str(os.getpid())


async def _thread_outlives_wait(seconds: float) -> str:
    """Stop waiting for an executor thread that keeps running."""
    try:
        await asyncio.wait_for(asyncio.to_thread(time.sleep, seconds), 0.05)
    except TimeoutError:
        return str(os.getpid())
    raise AssertionError('the executor thread ended within the wait')


def _run_async(task_loop: TaskLoop, coroutine: Coroutine[Any, Any, str]) -> str:
    """Run on the loop that ``serve`` opened at boot, as the child runner does."""
    return task_loop.run(coroutine)


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
            return True, _run_async(task_loop, _loops_seen()), None
        case 'no_running_loop':
            return True, _no_running_loop(), None
        case 'async_pid':
            return True, _run_async(task_loop, _async_pid()), None
        case 'loop_state':
            return True, type(task_loop.state).__name__, None
        case 'leave_pending':
            return True, _run_async(task_loop, _leave_pending(argument)), None
        case 'idle_executor_beside_a_stubborn_task':
            pid = _run_async(task_loop, _idle_executor_beside_a_stubborn_task())
            return True, pid, None
        case 'thread_outlives_wait':
            return True, _run_async(task_loop, _thread_outlives_wait(float(argument))), None
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
                ResultFrame(frame.task_id, True, 'complete', None, RssUnreadable(), NoTaskLoop()),
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
                ResultFrame('another-task', True, '', None, RssUnreadable(), NoTaskLoop()),
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
                ResultFrame(frame.task_id, True, 'complete', None, RssUnreadable(), NoTaskLoop()),
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


_ticks: list[int] = [0]


async def _tick_forever() -> None:
    """Count passes of the loop while no run is in flight."""
    while True:
        _ticks[0] += 1
        await asyncio.sleep(0.005)


async def _wait_for_file(path: str) -> str:
    """Hold the seat until ``path`` exists, then return the pid."""
    while not os.path.exists(path):
        await asyncio.sleep(0.005)
    return str(os.getpid())


async def _swallow_one_cancel(seconds: float) -> str:
    """Catch the first cancel, then return a value after ``seconds``."""
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await asyncio.sleep(seconds)
    return 'late value'


async def _ignore_every_cancel() -> str:
    while True:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            continue


async def _leave_marked_task(seconds: float) -> str:
    """Start a task that the run does not await, and return at once. The
    task carries the mark of the run, so the run ends when it ends."""
    _kept_tasks.append(asyncio.get_running_loop().create_task(asyncio.sleep(seconds)))
    return str(os.getpid())


async def _leave_direct_task(seconds: float) -> str:
    """As aiohttp builds its DNS lookup task on Python 3.12 and 3.13: with
    ``asyncio.Task(...)``, which the task factory does not see."""
    loop = asyncio.get_running_loop()
    _kept_tasks.append(asyncio.Task(asyncio.sleep(seconds), loop=loop))
    return str(os.getpid())


async def _thread_outlives_cancel(seconds: float) -> str:
    """Wait for a sync call in the default executor; a cancel ends the
    coroutine, and the thread runs on."""
    await asyncio.to_thread(time.sleep, seconds)
    return str(os.getpid())


async def _suppress_then_work(seconds: float) -> str:
    """Lose the first cancel with ``suppress``, then work on."""
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.Event().wait()
    await asyncio.sleep(seconds)
    return 'late value'


async def _stamp(seconds: float) -> str:
    """Sleep on the loop; report when the run started and ended."""
    started = time.time()
    await asyncio.sleep(seconds)
    return f'{os.getpid()}:{started}:{time.time()}'


async def _seat_run(frame: SharedRunFrame) -> WireResult:
    """One scripted run on a seat. ``kwargs_json`` holds the argument."""
    argument = frame.kwargs_json or ''
    match frame.task_name:
        case 'echo':
            return True, argument, None
        case 'pid':
            return True, str(os.getpid()), None
        case 'sleep':
            await asyncio.sleep(float(argument))
            return True, str(os.getpid()), None
        case 'stamp':
            return True, await _stamp(float(argument)), None
        case 'wait_for_file':
            return True, await _wait_for_file(argument), None
        case 'swallow_one_cancel':
            return True, await _swallow_one_cancel(float(argument)), None
        case 'suppress_then_work':
            return True, await _suppress_then_work(float(argument)), None
        case 'leave_marked_task':
            return True, await _leave_marked_task(float(argument)), None
        case 'leave_direct_task':
            return True, await _leave_direct_task(float(argument)), None
        case 'thread_outlives_cancel':
            return True, await _thread_outlives_cancel(float(argument)), None
        case 'ignore_every_cancel':
            return True, await _ignore_every_cancel(), None
        case 'start_ticker':
            # An empty context carries no run mark, as a task that a start
            # hook creates: the run ends while the ticker runs on.
            _kept_tasks.append(
                asyncio.get_running_loop().create_task(
                    _tick_forever(),
                    context=contextvars.Context(),
                ),
            )
            return True, str(_ticks[0]), None
        case 'read_ticks':
            return True, str(_ticks[0]), None
        case 'block':
            time.sleep(float(argument))
            return True, str(os.getpid()), None
        case 'gil_hang':
            ctypes.PyDLL(None).sleep(3600)
            return True, str(os.getpid()), None
        case 'gil_hang_after_cancel':
            # The loop is alive at the deadline, then frozen by C code that
            # holds the GIL, so the end of the grace is never reported.
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                ctypes.PyDLL(None).sleep(3600)
            return True, str(os.getpid()), None
        case 'kill_self':
            await asyncio.sleep(float(argument or '0'))
            os.kill(os.getpid(), signal.SIGKILL)
            return True, str(os.getpid()), None
        case 'raises':
            raise RuntimeError('scripted seat defect')
        case 'big_result':
            return True, 'x' * int(argument), None
        case _:
            raise AssertionError(frame.task_name)


class _ScriptedSeats:
    async def run(self, frame: SharedRunFrame) -> WireResult:
        return await _seat_run(frame)

    async def stop(self) -> None:
        return None


class _ScriptedSeatProgram:
    """Starts with no pool. ``seat_start_fails`` and ``seat_hook_fails`` are
    the two ways a start fails."""

    async def start(self, init: InitFrame) -> Result[StartedSeats, BootFailedFrame]:
        mode, _, _argument = init.app_locator.partition(':')
        match mode:
            case 'seat_start_fails':
                return Err(
                    boot_failed_frame(ChildPoolSetupFailed(), 'ValueError', 'scripted'),
                )
            case 'seat_hook_fails':
                os._exit(CHILD_HOOK_FAILURE_EXIT_CODE)
            case _:
                return Ok(_ScriptedSeats())


RUNNERS = TaskRunners(run_task=_run, seats=_ScriptedSeatProgram())


def _refuse_thread(_thread: threading.Thread) -> None:
    raise RuntimeError("can't start new thread")


def main() -> None:
    global _sock
    _sock = socket.socket(fileno=int(sys.argv[1]))
    if os.environ.get('HORSIES_TEST_REFUSE_THREADS') == '1':
        # As the OS does at its thread limit: ``Thread.start`` raises.
        setattr(threading.Thread, 'start', _refuse_thread)
    serve(_sock, int(sys.argv[2]), _boot, RUNNERS)


if __name__ == '__main__':
    main()
