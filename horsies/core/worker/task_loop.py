"""The event loop that runs ``async def`` tasks in a worker child.

A child whose app has an ``async def`` task creates the loop with ``open`` at
boot, before it reports ready, and keeps it until the child stops. A child that
cannot create it fails its boot, so no task reaches it. A child whose app has
no ``async def`` task creates no loop. An async client or pool that a task
keeps in module state is bound to this loop, so a later async task in the same
child can use it.

The loop runs only while an async task runs. Between tasks the loop is stopped
and not closed. A sync task runs on the same thread with no running event
loop, so it can call ``asyncio.run()`` and the sync send methods.

A task must await the work that it starts. ``report`` counts the asyncio tasks
that are still on the loop after a task, and the worker replaces a child that
reports any. It sees asyncio tasks only: a callback that ``call_soon`` or
``call_later`` scheduled, and executor work that the task did not await, are
not counted, and they can still run during a later task of the same child.
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
from horsies.core.utils.task_loop_registry import register_task_loop, unregister_task_loop
from horsies.core.worker.child_protocol import (
    MAX_LEFTOVER_NAME_CHARS,
    MAX_LEFTOVER_TASK_NAMES,
    LoopHoldsNoTask,
    LoopHoldsTasks,
    NoTaskLoop,
    TaskLoopReport,
)

logger = get_logger('worker')

# The child closes its loop after a stop frame. The parent kills a child that
# has not exited CHILD_STOP_TIMEOUT_S (10 s) after that frame, so the close
# must end before that time.
LOOP_CLOSE_TIMEOUT_S: Final = 5.0

R = TypeVar('R')


class AsyncTaskPresence(Enum):
    """Whether the app of a booted child has an ``async def`` task.

    The boot step of a child returns it, and the child creates the task loop
    only for ``HAS_ASYNC_TASKS``.
    """

    HAS_ASYNC_TASKS = 'has_async_tasks'
    NO_ASYNC_TASK = 'no_async_task'


@dataclass(frozen=True, slots=True)
class LoopCreateFailed:
    """The runtime refused a new event loop.

    ``asyncio.new_event_loop()`` opens a selector and a socket pair, so a
    process at its file descriptor limit (EMFILE) gets this, and so does an
    event loop policy that raises. The child gets it at boot, before it
    reports ready, so no task reached it.
    Retry condition: the child fails its boot, and the worker starts a new
    child for the next boot attempt. That attempt succeeds once the process
    has descriptors free.
    """

    error_name: str
    detail: str

    def describe(self) -> str:
        """Subject and cause, for a log line and a report."""
        return (
            'the event loop for async tasks could not be created: '
            f'{self.error_name}: {self.detail}'
        )


@dataclass(frozen=True, slots=True)
class LoopNotCreated:
    """The child has no loop: its app has no ``async def`` task, or the
    boot step has not created the loop yet."""


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


class TaskLoop:
    """One event loop for every async task of one child process."""

    def __init__(self) -> None:
        self._state: LoopState = LoopNotCreated()

    @property
    def state(self) -> LoopState:
        return self._state

    def open(self) -> Result[None, LoopCreateFailed]:
        """Create the loop when this child has none. Call it before ``run``.

        The child calls it at boot, before it reports ready, when its app has
        an ``async def`` task.

        ``Err(LoopCreateFailed)``: the runtime refused the loop. The state
        stays ``LoopNotCreated``, and the child fails its boot. A call on an
        open loop changes nothing. A call after ``close`` is a defect in the
        caller: ``AssertionError``.
        """
        match self._state:
            case LoopNotCreated():
                match _new_loop():
                    case Ok(loop):
                        self._state = LoopOpen(loop)
                        return Ok(None)
                    case Err(failed):
                        return Err(failed)
            case LoopOpen():
                return Ok(None)
            case LoopClosed():
                raise AssertionError('TaskLoop.open was called after close')
            case _ as unreachable:
                assert_never(unreachable)

    def run(self, coroutine: Coroutine[Any, Any, R]) -> R:
        """Run one coroutine to its end on the open loop, then stop the loop.

        The coroutine runs in a copy of the caller's context. Its value comes
        back, and its exception propagates unchanged. Tasks that the
        coroutine starts and does not await stay on the stopped loop:
        ``report`` counts them.

        A call while an event loop runs on this thread, or while the loop is
        not open, is a defect in the caller. It closes the coroutine unrun
        and raises ``AssertionError``.
        """
        if in_running_event_loop():
            coroutine.close()
            raise AssertionError(
                'TaskLoop.run was called while an event loop runs on this thread',
            )
        match self._state:
            case LoopOpen(loop=loop):
                return loop.run_until_complete(coroutine)
            case LoopNotCreated():
                coroutine.close()
                raise AssertionError('TaskLoop.run was called before open')
            case LoopClosed():
                coroutine.close()
                raise AssertionError('TaskLoop.run was called after close')
            case _ as unreachable:
                assert_never(unreachable)

    def report(self) -> TaskLoopReport:
        """What the loop holds after a task, with the names of the leftover
        tasks, bounded by ``MAX_LEFTOVER_TASK_NAMES``.

        Reads only. Call it between runs, on the thread that runs them. A call
        after ``close`` is a defect in the caller: ``AssertionError``.
        """
        match self._state:
            case LoopNotCreated():
                return NoTaskLoop()
            case LoopOpen(loop=loop):
                return _leftover_report(asyncio.all_tasks(loop))
            case LoopClosed():
                raise AssertionError('TaskLoop.report was called after close')
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
                try:
                    self._state = LoopClosed(_close_loop(loop))
                finally:
                    unregister_task_loop(loop)
            case LoopClosed():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        return self._state.executor


def _leftover_report(
    leftover: Set[asyncio.Task[Any]],
) -> LoopHoldsNoTask | LoopHoldsTasks:
    """Count the tasks that are not done. Name the first
    ``MAX_LEFTOVER_TASK_NAMES`` of them, in sorted order."""
    if not leftover:
        return LoopHoldsNoTask()
    names = sorted(_coroutine_name(task) for task in leftover)
    return LoopHoldsTasks(
        count=len(leftover),
        names=tuple(names[:MAX_LEFTOVER_TASK_NAMES]),
    )


def _coroutine_name(task: asyncio.Task[Any]) -> str:
    """The ``__qualname__`` of the coroutine of ``task``, safe for a frame field.

    A coroutine object of a C extension can lack ``__qualname__``; the task
    name stands in then. The name is made valid UTF-8, because user code can
    set ``__qualname__`` to any string, and cut to ``MAX_LEFTOVER_NAME_CHARS``.
    """
    match getattr(task.get_coro(), '__qualname__', None):
        case str() as qualname:
            name = qualname
        case _:
            name = task.get_name()
    safe = name.encode('utf-8', 'backslashreplace').decode('utf-8')
    return safe[:MAX_LEFTOVER_NAME_CHARS]


def _new_loop() -> Result[asyncio.AbstractEventLoop, LoopCreateFailed]:
    """Create an event loop. An ``Exception`` from the creation becomes
    ``LoopCreateFailed``, with its type and message.

    ``asyncio.new_event_loop()`` runs the runtime the child depends on: the
    event loop policy, which user code can set, and the OS, which can refuse
    a selector or a socket pair (EMFILE) or memory. A failure there is a
    failure of that runtime, not a horsies defect, so it takes the boot
    failure path: the child reports it and exits before it is ready.
    ``KeyboardInterrupt`` and ``SystemExit`` are not ``Exception`` and stay
    uncaught.

    A created loop is registered as a task loop, so a broker does not count
    it against its limit of caller loops.
    """
    try:
        loop = asyncio.new_event_loop()
    except Exception as exc:
        failed = LoopCreateFailed(error_name=type(exc).__name__, detail=str(exc))
        logger.error('%s', failed.describe())
        return Err(failed)
    register_task_loop(loop)
    return Ok(loop)


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
    match _leftover_report(pending):
        case LoopHoldsNoTask():
            pass
        case LoopHoldsTasks() as still_running:
            logger.warning(
                '%d task(s) left on the task loop did not end within %ss of the '
                'cancel: %s',
                still_running.count,
                LOOP_CLOSE_TIMEOUT_S,
                still_running.names_text(),
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
