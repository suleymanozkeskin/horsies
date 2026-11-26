"""Task definitions that an operator cancels while they run, one per
execution mode and body kind, for ``tests/e2e/test_cancel_running.py``.

Each body writes files in ``marker_dir``, which the test reads:

- ``started``: the body runs; the file holds the process id of its child.
- ``cancelled``: the body saw the cancel; the file holds the cause.
- ``returned``: a body that ignores the cancel returned.

A worker with the ``set-pools-1-1x4-1x4`` profile runs each on a child of its
class: the ``ISOLATED`` tasks on the isolated child, the others on a shared
async and a shared thread child.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import assert_never

from horsies import CancelNotRequested, CancelRequested, TaskRuntime
from horsies.core.models.tasks import Execution, TaskError
from horsies.core.types.result import Ok, Result

from tests.e2e.tasks.instance import app

SHARED_TIMEOUT_MS = 60_000
"""The timeout of the shared tasks: far past the cancel of each test."""

BODY_LIMIT_S = 60.0
"""The longest run of a body that waits for its cancel."""

POLL_S = 0.02
"""The time between two reads of the cancel state of a ``def`` body."""

IGNORED_CANCEL_RUN_S = 3.0
"""How long a body that ignores its cancel runs on after it started."""

HARD_STOP_GRACE_MS = 500
"""The cancel grace of the isolated task that the worker kills after its
cancel."""


def _write(marker_dir: str, name: str, text: str) -> None:
    """Write ``text`` to the file ``name`` of ``marker_dir``, at once."""
    path = Path(marker_dir) / name
    partial = path.with_suffix('.partial')
    partial.write_text(text)
    partial.replace(path)


def _started(marker_dir: str) -> None:
    _write(marker_dir, 'started', str(os.getpid()))


def _cause_text(runtime: TaskRuntime) -> str:
    match runtime.cancel_state():
        case CancelRequested(cause=cause):
            return cause.value
        case CancelNotRequested():
            return 'NOT_REQUESTED'
        case _ as unreachable:
            assert_never(unreachable)


async def _wait_for_cancel(runtime: TaskRuntime, marker_dir: str) -> Result[str, TaskError]:
    """An ``async def`` body: wait until the cancel reaches the ``await``."""
    _started(marker_dir)
    try:
        await asyncio.sleep(BODY_LIMIT_S)
    except asyncio.CancelledError:
        _write(marker_dir, 'cancelled', _cause_text(runtime))
        raise
    return Ok('never cancelled')


def _poll_for_cancel(runtime: TaskRuntime, marker_dir: str) -> Result[str, TaskError]:
    """A ``def`` body: read the cancel state between steps; stop at the
    cancel and return a value, which the cancel drops."""
    _started(marker_dir)
    give_up_at = time.monotonic() + BODY_LIMIT_S
    while time.monotonic() < give_up_at:
        match runtime.cancel_state():
            case CancelRequested():
                _write(marker_dir, 'cancelled', _cause_text(runtime))
                return Ok('stopped at the cancel')
            case CancelNotRequested():
                time.sleep(POLL_S)
            case _ as unreachable:
                assert_never(unreachable)
    return Ok('never cancelled')


@app.task(task_name='e2e_cancel_isolated_async')
async def cancel_isolated_async(
    runtime: TaskRuntime, *, marker_dir: str
) -> Result[str, TaskError]:
    return await _wait_for_cancel(runtime, marker_dir)


@app.task(task_name='e2e_cancel_isolated_def')
def cancel_isolated_def(runtime: TaskRuntime, *, marker_dir: str) -> Result[str, TaskError]:
    return _poll_for_cancel(runtime, marker_dir)


@app.task(
    task_name='e2e_cancel_shared_async',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SHARED_TIMEOUT_MS,
)
async def cancel_shared_async(
    runtime: TaskRuntime, *, marker_dir: str
) -> Result[str, TaskError]:
    return await _wait_for_cancel(runtime, marker_dir)


@app.task(
    task_name='e2e_cancel_shared_thread',
    execution=Execution.SHARED_THREAD,
    timeout_ms=SHARED_TIMEOUT_MS,
)
def cancel_shared_thread(runtime: TaskRuntime, *, marker_dir: str) -> Result[str, TaskError]:
    return _poll_for_cancel(runtime, marker_dir)


@app.task(task_name='e2e_cancel_isolated_hard_stop', cancel_grace_ms=HARD_STOP_GRACE_MS)
def cancel_isolated_hard_stop(*, marker_dir: str) -> Result[str, TaskError]:
    """Ignores its cancel: the worker kills its child after the grace."""
    _started(marker_dir)
    time.sleep(BODY_LIMIT_S)
    _write(marker_dir, 'returned', 'returned')
    return Ok('ran to the end')


@app.task(task_name='e2e_cancel_isolated_no_grace')
def cancel_isolated_no_grace(*, marker_dir: str) -> Result[str, TaskError]:
    """Ignores its cancel and returns later: with no grace, nothing is
    killed."""
    _started(marker_dir)
    time.sleep(IGNORED_CANCEL_RUN_S)
    _write(marker_dir, 'returned', 'returned')
    return Ok('ran to the end')
