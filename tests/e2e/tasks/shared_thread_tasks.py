"""``SHARED_THREAD`` task definitions for e2e tests.

A worker with a thread profile (``split-isolated-1-thread-2x4``,
``thread-only-1x4``) runs these on its shared thread children; a worker
without thread children runs them isolated.
"""

from __future__ import annotations

import os
import threading
import time

from horsies.core.models.tasks import Execution, TaskError, TaskResult

from tests.e2e.tasks.instance import app

HANG_TIMEOUT_MS = 2_000
"""The timeout of ``e2e_thread_hang``: short, so the test stays short."""


@app.task(
    task_name='e2e_thread_sleep',
    execution=Execution.SHARED_THREAD,
    timeout_ms=30_000,
)
def thread_sleep(*, seconds: float) -> TaskResult[str, TaskError]:
    """Sleep on the seat thread. Return ``pid:thread:start:end``, the times in
    seconds since the epoch."""
    started = time.time()
    time.sleep(seconds)
    name = threading.current_thread().name
    return TaskResult(ok=f'{os.getpid()}:{name}:{started}:{time.time()}')


@app.task(
    task_name='e2e_thread_hang',
    execution=Execution.SHARED_THREAD,
    timeout_ms=HANG_TIMEOUT_MS,
)
def thread_hang() -> TaskResult[str, TaskError]:
    """Sleep far past the timeout; a thread cannot be cancelled, so the
    worker kills the child when no task in it has time left."""
    time.sleep(3600)
    return TaskResult(ok='never')
