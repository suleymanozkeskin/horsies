"""The event loop that runs ``async def`` tasks in a worker child.

A child creates the loop when its first async task starts, and keeps it until
the child stops. An async client or pool that a task keeps in module state is
bound to this loop, so a later async task in the same child can use it.

The loop runs only while an async task runs. Between tasks the loop is stopped
and not closed. A sync task runs on the same thread with no running event
loop, so it can call ``asyncio.run()`` and the sync send methods.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Coroutine, Set
from dataclasses import dataclass
from typing import Any, Final, TypeVar, assert_never

from horsies.core.logging import get_logger
from horsies.core.utils.loop_runner import in_running_event_loop

logger = get_logger('worker')

# The child closes its loop after a stop frame. The parent kills a child that
# has not exited CHILD_STOP_TIMEOUT_S (10 s) after that frame, so the close
# must end before that time.
LOOP_CLOSE_TIMEOUT_S: Final = 5.0

R = TypeVar('R')


@dataclass(frozen=True, slots=True)
class LoopNotCreated:
    """No async task has run in this process."""


@dataclass(frozen=True, slots=True)
class LoopOpen:
    """The loop exists. It runs only while ``TaskLoop.run`` runs."""

    loop: asyncio.AbstractEventLoop


@dataclass(frozen=True, slots=True)
class LoopClosed:
    """The process stops. No task runs after this state."""


type LoopState = LoopNotCreated | LoopOpen | LoopClosed


class TaskLoop:
    """One event loop for every async task of one child process."""

    def __init__(self) -> None:
        self._state: LoopState = LoopNotCreated()

    @property
    def state(self) -> LoopState:
        return self._state

    def run(self, coroutine: Coroutine[Any, Any, R]) -> R:
        """Run one coroutine to its end on the loop, then stop the loop.

        Creates the loop on the first call. The coroutine runs in a copy of
        the caller's context. The value or the exception of the coroutine
        comes back unchanged. Tasks that the coroutine starts and does not
        await stay on the stopped loop, and they continue in a later call.

        A call while an event loop runs on this thread, or after ``close``,
        is a defect in the caller. It closes the coroutine unrun and raises
        ``AssertionError``.
        """
        if in_running_event_loop():
            coroutine.close()
            raise AssertionError(
                'TaskLoop.run was called while an event loop runs on this thread',
            )
        match self._state:
            case LoopNotCreated():
                loop = asyncio.new_event_loop()
                self._state = LoopOpen(loop)
            case LoopOpen(loop=loop):
                pass
            case LoopClosed():
                coroutine.close()
                raise AssertionError('TaskLoop.run was called after close')
            case _ as unreachable:
                assert_never(unreachable)
        return loop.run_until_complete(coroutine)

    def close(self) -> None:
        """Cancel the tasks left on the loop, then close the loop.

        Waits at most ``LOOP_CLOSE_TIMEOUT_S`` in total, and logs what did
        not end in that time. Closes the loop also when a step fails.
        A second call does nothing.
        """
        match self._state:
            case LoopNotCreated():
                self._state = LoopClosed()
            case LoopOpen(loop=loop):
                self._state = LoopClosed()
                _close_loop(loop)
            case LoopClosed():
                pass
            case _ as unreachable:
                assert_never(unreachable)


def _seconds_left(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _close_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Close the loop the way ``asyncio.run`` does, within one deadline."""
    deadline = time.monotonic() + LOOP_CLOSE_TIMEOUT_S
    try:
        _cancel_leftover_tasks(loop, deadline)
        _run_until(loop, loop.shutdown_asyncgens(), deadline, 'async generators')
        _run_until(loop, loop.shutdown_default_executor(), deadline, 'default executor')
    finally:
        loop.close()


def _run_until(
    loop: asyncio.AbstractEventLoop,
    step: Coroutine[Any, Any, None],
    deadline: float,
    what: str,
) -> None:
    """Run one close step until ``deadline``. A step past it is cancelled and logged."""
    try:
        loop.run_until_complete(asyncio.wait_for(step, _seconds_left(deadline)))
    except TimeoutError:
        logger.warning(
            'The %s of the task loop did not shut down within %ss',
            what,
            LOOP_CLOSE_TIMEOUT_S,
        )


def _cancel_leftover_tasks(loop: asyncio.AbstractEventLoop, deadline: float) -> None:
    """Cancel each task left on the loop, and wait for them until ``deadline``."""
    leftover = asyncio.all_tasks(loop)
    if not leftover:
        return
    for task in leftover:
        task.cancel()
    done, pending = loop.run_until_complete(
        asyncio.wait(leftover, timeout=_seconds_left(deadline)),
    )
    _log_failed_leftovers(done)
    if pending:
        logger.warning(
            '%d task(s) left on the task loop did not end within %ss of the cancel',
            len(pending),
            LOOP_CLOSE_TIMEOUT_S,
        )


def _log_failed_leftovers(done: Set[asyncio.Task[Any]]) -> None:
    """Log a leftover task that ended with an exception, not with the cancel."""
    for task in done:
        if task.cancelled():
            continue
        error = task.exception()
        if error is not None:
            logger.warning(
                'A task left on the task loop failed while it stopped: %s: %s',
                type(error).__name__,
                error,
            )
