"""Cases of the fork rule: a process forked without ``exec`` after the core
started refuses every core call with ``ForkedAfterCoreStart``; a process
forked before the core started starts its own core.

A forked child reports through a pipe and ends with ``os._exit``. The parent
waits for it within ``FORK_CHILD_BOUND_S`` and kills it past the bound, so a
call that hangs in the child is reported, not waited for.
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import select
import signal
import time
import traceback
from collections.abc import Callable
from typing import Final, NoReturn

from horsies._core import (
    CoreHandle,
    ForkedAfterCoreStart,
    core_state,
    mint_task_id,
)
from tests.extension.programs.report import JsonValue, Report, run_case
from tests.extension.programs.support import database_url, manifest_json

FORK_CHILD_BOUND_S: Final = 10.0
"""The longest time a forked child may take from the fork to its exit."""

CHILD_FAILED_EXIT: Final = 70
"""The exit code of a fork child whose calls raised an unexpected exception."""

CHILD_POLL_S: Final = 0.02
"""The pause between two checks for the end of a forked child."""

PIPE_READ_BYTES: Final = 65_536
"""Bytes read from the child's pipe per read."""

CHILD_REPORT_MAX_BYTES: Final = 1_048_576
"""The largest report a child may write; more ends the read."""


def _handle() -> CoreHandle:
    return CoreHandle(
        manifest_json(database_url(), schema='ASSUME_INITIALIZED', echo=False),
        pool_profile='PRODUCER',
    )


def _refusal(call: Callable[[], object]) -> Report:
    """How a call ended in a fork child: its outcome class, or
    ``ForkedAfterCoreStart`` with its arguments."""
    try:
        outcome = call()
        return {'ended': 'returned', 'class': type(outcome).__name__}
    except ForkedAfterCoreStart as refused:
        return {
            'ended': 'raised',
            'class': 'ForkedAfterCoreStart',
            'arguments': [str(argument) for argument in refused.args],
        }


async def _async_ping(handle: CoreHandle) -> object:
    return await handle.ping_database_async()


def _calls_after_start(handle: CoreHandle) -> Report:
    """The calls of a child forked after the core started."""
    return {
        'pid': os.getpid(),
        'core_state': core_state(),
        'pure': _refusal(mint_task_id),
        'blocking': _refusal(handle.ping_database),
        'async': _refusal(lambda: asyncio.run(_async_ping(handle))),
    }


def _calls_before_start(handle: CoreHandle) -> Report:
    """The calls of a child forked before the core started."""
    state_before = core_state()
    ping = handle.ping_database()
    return {
        'pid': os.getpid(),
        'core_state_before': state_before,
        'ping': type(ping).__name__,
        'core_state_after': core_state(),
    }


def _forked(child_calls: Callable[[], Report]) -> Report:
    """Fork; the child runs ``child_calls`` and writes its report to a pipe.
    The parent's report: the child's report, its exit status, and the time
    from the fork to the child's end."""
    read_end, write_end = os.pipe()
    forked_at = time.monotonic()
    pid = os.fork()
    if pid == 0:
        _report_and_exit(read_end, write_end, child_calls)
    os.close(write_end)
    payload = _read_child_report(read_end, forked_at + FORK_CHILD_BOUND_S)
    os.close(read_end)
    status = _child_status(pid, forked_at + FORK_CHILD_BOUND_S)
    child_report: JsonValue = json.loads(payload) if payload else None
    return {
        'child_pid': pid,
        'child': child_report,
        'child_status': status,
        'child_seconds': time.monotonic() - forked_at,
    }


def _report_and_exit(
    read_end: int, write_end: int, child_calls: Callable[[], Report],
) -> NoReturn:
    """The body of a fork child: write the report of ``child_calls`` to the
    pipe and exit with 0. The child never returns into the parent's code:
    any exception is printed and the child exits with ``CHILD_FAILED_EXIT``."""
    exit_code = CHILD_FAILED_EXIT
    try:
        os.close(read_end)
        payload = json.dumps(child_calls()).encode('utf-8')
        with os.fdopen(write_end, 'wb') as pipe:
            _ = pipe.write(payload)
        exit_code = 0
    except BaseException:
        traceback.print_exc()
    finally:
        os._exit(exit_code)


def _read_child_report(read_end: int, deadline: float) -> bytes:
    """The bytes the child wrote until it closed the pipe, the deadline
    passed, or ``CHILD_REPORT_MAX_BYTES`` arrived."""
    chunks: list[bytes] = []
    size = 0
    while size < CHILD_REPORT_MAX_BYTES:
        left = deadline - time.monotonic()
        if left <= 0:
            break
        ready, _writable, _failed = select.select([read_end], [], [], left)
        if not ready:
            break
        chunk = os.read(read_end, PIPE_READ_BYTES)
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
    return b''.join(chunks)


def _child_status(pid: int, deadline: float) -> str:
    """``exited <code>``, ``signalled <number>``, or ``killed after the
    bound`` for a child that did not end by ``deadline``."""
    while True:
        ended_pid, status = os.waitpid(pid, os.WNOHANG)
        if ended_pid == pid:
            match os.waitstatus_to_exitcode(status):
                case int() as code if code >= 0:
                    return f'exited {code}'
                case code:
                    return f'signalled {-code}'
        if time.monotonic() >= deadline:
            os.kill(pid, signal.SIGKILL)
            _ = os.waitpid(pid, 0)
            return 'killed after the bound'
        time.sleep(CHILD_POLL_S)


def fork_after_start() -> Report:
    """The core starts with a ping; then a fork child calls the core."""
    handle = _handle()
    ping = handle.ping_database()
    forked = _forked(lambda: _calls_after_start(handle))
    ping_after_fork = handle.ping_database()
    return {
        'pid': os.getpid(),
        'ping': type(ping).__name__,
        'forked': forked,
        'parent_ping_after_fork': type(ping_after_fork).__name__,
        'parent_core_state': core_state(),
    }


def fork_before_start() -> Report:
    """A fork child calls the core before any core call of the parent."""
    handle = _handle()
    forked = _forked(lambda: _calls_before_start(handle))
    return {
        'pid': os.getpid(),
        'forked': forked,
        'parent_core_state': core_state(),
    }


def _calls_in_pool_worker() -> Report:
    """A core call in a worker of a fork-start ``multiprocessing.Pool``.
    Module level, so the pool can name it."""
    return {
        'pid': os.getpid(),
        'core_state': core_state(),
        'pure': _refusal(mint_task_id),
    }


def fork_pool_after_start() -> Report:
    """The core starts with a ping; then a worker of a ``multiprocessing.Pool``
    of the ``fork`` start method (the default of Python 3.13 on Linux) calls
    the core."""
    handle = _handle()
    ping = handle.ping_database()
    context = multiprocessing.get_context('fork')
    started = time.monotonic()
    with context.Pool(processes=1) as pool:
        pool_report = pool.apply_async(_calls_in_pool_worker).get(timeout=FORK_CHILD_BOUND_S)
    return {
        'pid': os.getpid(),
        'ping': type(ping).__name__,
        'worker': pool_report,
        'worker_seconds': time.monotonic() - started,
    }


if __name__ == '__main__':
    run_case(
        {
            'fork_after_start': fork_after_start,
            'fork_before_start': fork_before_start,
            'fork_pool_after_start': fork_pool_after_start,
        },
    )
