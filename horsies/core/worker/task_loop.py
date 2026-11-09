"""The event loop that runs ``async def`` tasks in a worker child.

A child creates the loop when its first async task starts, and keeps it until
the child stops. An async client or pool that a task keeps in module state is
bound to this loop, so a later async task in the same child can use it.

The loop runs only while an async task runs. Between tasks the loop is stopped
and not closed. A sync task runs on the same thread with no running event
loop, so it can call ``asyncio.run()`` and the sync send methods.

A task must await the work that it starts. ``tasks_left`` counts what is still
on the loop after a task, and the worker replaces a child that reports any, so
no later task shares the loop with that work.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Coroutine, Set
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, TypeVar, assert_never

from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.loop_runner import in_running_event_loop
from horsies.core.worker.child_protocol import LoopHoldsNoTask, LoopHoldsTasks, LoopTasks

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
class ExecutorShutDown:
    """The default executor of the loop shut down, or the loop had none."""


@dataclass(frozen=True, slots=True)
class ExecutorThreadsLeft:
    """The default executor did not shut down by the close deadline.

    Its threads can still run work that a task started with
    ``asyncio.to_thread()`` or ``loop.run_in_executor(None, ...)``. A normal
    interpreter exit joins those threads, and nothing bounds that wait.
    """


type ExecutorAfterClose = ExecutorShutDown | ExecutorThreadsLeft


@dataclass(frozen=True, slots=True)
class LoopClosed:
    """The process stops. No task runs after this state."""

    executor: ExecutorAfterClose


type LoopState = LoopNotCreated | LoopOpen | LoopClosed


class _CloseStep(Enum):
    """How one step of the loop close ended."""

    ENDED = 'ended'
    PAST_DEADLINE = 'past_deadline'


@dataclass(frozen=True, slots=True)
class LoopCreateFailed:
    """The OS refused a resource that a new event loop needs.

    ``asyncio.new_event_loop()`` opens a selector and a socket pair, so a
    process at its file descriptor limit (EMFILE) gets this. The coroutine
    did not run. Retry condition: a later run tries again, and it succeeds
    once the process has descriptors free.
    """

    error_name: str
    detail: str

    def describe(self) -> str:
        """Subject and cause, for a log line and a failure reason."""
        return (
            'the event loop for async tasks could not be created: '
            f'{self.error_name}: {self.detail}'
        )


class TaskLoop:
    """One event loop for every async task of one child process."""

    def __init__(self) -> None:
        self._state: LoopState = LoopNotCreated()

    @property
    def state(self) -> LoopState:
        return self._state

    def run(self, coroutine: Coroutine[Any, Any, R]) -> Result[R, LoopCreateFailed]:
        """Run one coroutine to its end on the loop, then stop the loop.

        Creates the loop on the first call. The coroutine runs in a copy of
        the caller's context. The value of the coroutine comes back in
        ``Ok``, and its exception propagates unchanged. Tasks that the
        coroutine starts and does not await stay on the stopped loop:
        ``tasks_left`` counts them.

        ``Err(LoopCreateFailed)``: the loop could not be created. The
        coroutine is closed unrun, the state stays ``LoopNotCreated``, and
        the next call tries again.

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
                match _new_loop():
                    case Ok(loop):
                        self._state = LoopOpen(loop)
                    case Err(failed):
                        coroutine.close()
                        return Err(failed)
            case LoopOpen(loop=loop):
                pass
            case LoopClosed():
                coroutine.close()
                raise AssertionError('TaskLoop.run was called after close')
            case _ as unreachable:
                assert_never(unreachable)
        return Ok(loop.run_until_complete(coroutine))

    def tasks_left(self) -> LoopTasks:
        """Count the tasks on the loop that are not done. Reads only.

        Call it between runs, on the thread that runs them. A call after
        ``close`` is a defect in the caller: ``AssertionError``.
        """
        match self._state:
            case LoopNotCreated():
                return LoopHoldsNoTask()
            case LoopOpen(loop=loop):
                count = len(asyncio.all_tasks(loop))
                if count == 0:
                    return LoopHoldsNoTask()
                return LoopHoldsTasks(count=count)
            case LoopClosed():
                raise AssertionError('TaskLoop.tasks_left was called after close')
            case _ as unreachable:
                assert_never(unreachable)

    def close(self) -> ExecutorAfterClose:
        """Cancel the tasks left on the loop, then close the loop.

        Waits at most ``LOOP_CLOSE_TIMEOUT_S`` in total, and logs what did
        not end in that time. Closes the loop also when a step fails.
        Returns whether threads of the default executor can still run, which
        decides how the process can exit. A second call changes nothing and
        returns the answer of the first.
        """
        match self._state:
            case LoopNotCreated():
                self._state = LoopClosed(ExecutorShutDown())
            case LoopOpen(loop=loop):
                # A step that raises still leaves the loop closed. Until the
                # executor step ends, its threads can still run.
                self._state = LoopClosed(ExecutorThreadsLeft())
                self._state = LoopClosed(_close_loop(loop))
            case LoopClosed():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        return self._state.executor


def _new_loop() -> Result[asyncio.AbstractEventLoop, LoopCreateFailed]:
    """Create an event loop. An ``OSError`` of the OS becomes ``LoopCreateFailed``."""
    try:
        return Ok(asyncio.new_event_loop())
    except OSError as exc:
        failed = LoopCreateFailed(error_name=type(exc).__name__, detail=str(exc))
        logger.error('%s', failed.describe())
        return Err(failed)


def _seconds_left(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _close_loop(loop: asyncio.AbstractEventLoop) -> ExecutorAfterClose:
    """Close the loop the way ``asyncio.run`` does, within one deadline."""
    deadline = time.monotonic() + LOOP_CLOSE_TIMEOUT_S
    try:
        _cancel_leftover_tasks(loop, deadline)
        _run_until(loop, loop.shutdown_asyncgens(), deadline, 'async generators')
        executor_step = _run_until(
            loop,
            loop.shutdown_default_executor(),
            deadline,
            'default executor',
        )
    finally:
        loop.close()
    match executor_step:
        case _CloseStep.ENDED:
            return ExecutorShutDown()
        case _CloseStep.PAST_DEADLINE:
            return ExecutorThreadsLeft()


def _run_until(
    loop: asyncio.AbstractEventLoop,
    step: Coroutine[Any, Any, None],
    deadline: float,
    what: str,
) -> _CloseStep:
    """Run one close step until ``deadline``. A step past it is cancelled and logged."""
    try:
        loop.run_until_complete(asyncio.wait_for(step, _seconds_left(deadline)))
    except TimeoutError:
        logger.warning(
            'The %s of the task loop did not shut down within %ss',
            what,
            LOOP_CLOSE_TIMEOUT_S,
        )
        return _CloseStep.PAST_DEADLINE
    return _CloseStep.ENDED


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
