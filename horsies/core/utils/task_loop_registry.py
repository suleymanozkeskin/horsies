"""Identity of the event loops that run task bodies in a worker child.

A worker child runs its ``async def`` tasks on one event loop that the library
owns: ``horsies.core.worker.task_loop.TaskLoop``. The broker serves that loop
as it serves its own sync runner loop: the loop does not count against the
limit of caller loops. This module holds the identity of such loops, so the
broker can tell them apart without importing the worker.

The registry holds a loop from its creation until its close, so it holds at
most the task loops that are open in this process: one in a worker child.
"""

from __future__ import annotations

import asyncio
import threading

_lock = threading.Lock()
_task_loops: set[asyncio.AbstractEventLoop] = set()


def register_task_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Record ``loop`` as a task loop. Call it once, right after creation."""
    with _lock:
        _task_loops.add(loop)


def unregister_task_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Forget ``loop``. Call it when the loop closes. An unknown loop is no error."""
    with _lock:
        _task_loops.discard(loop)


def is_task_loop(loop: asyncio.AbstractEventLoop) -> bool:
    """Whether ``loop`` is an open task loop of this process. Compares identity."""
    with _lock:
        return loop in _task_loops
