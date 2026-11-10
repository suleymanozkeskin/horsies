"""The seat program of a shared async child: its start, and one task run.

``PostgresSeatProgram.start`` runs on the task loop of the child, after the
sync boot (the app import and the ``on_child_process_start`` hooks): it opens
the async connection pool of the child, then runs the
``on_shared_async_child_start`` hooks, each bounded. ``PostgresSeats.run``
runs one task as a coroutine on that loop: the RUNNING transition, the task
body, the finalizing marker. Every write goes through the async pool
(``shared_lifecycle``), so no write blocks the loop.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import os
import threading
from collections.abc import Callable, Coroutine
from typing import Any, Final, assert_never

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from horsies.core.logging import get_logger
from horsies.core.models.boot_failure import ChildPoolSetupFailed
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.task_decorator import await_task_call_in_worker
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_main import (
    boot_failure_from_exception,
    flush_output,
)
from horsies.core.worker.child_pool import open_shared_async_pool
from horsies.core.worker.child_protocol import (
    BootFailedFrame,
    InitFrame,
    SeatRefusal,
    SeatTask,
    SharedRunFrame,
    WireResult,
)
from horsies.core.worker import child_runner
from horsies.core.worker.child_runner import (
    CHILD_HOOK_FAILURE_EXIT_CODE,
    OnSharedSeat,
    _encode_task_outcome,
    _prepare_task_call,
    _PreparedCall,
    _stored_timeout_refusal_response,
    _task_exception_response,
)
from horsies.core.worker.current import get_current_app
from horsies.core.worker.shared_child import StartedSeats
from horsies.core.worker.shared_lifecycle import (
    confirm_until_not_resumed,
    mark_task_finalizing,
)

logger = get_logger('worker')

SHARED_POOL_CLOSE_TIMEOUT_S: Final = 1.0
"""The longest wait for the async pool to close at the stop. With the loop
close and the writer drain it stays under the 10 s that the parent gives a
child after its stop frame."""


class PostgresSeats:
    """The seats of a started shared async child, with its async pool."""

    def __init__(self, pool: AsyncConnectionPool[AsyncConnection[Any]]) -> None:
        self._pool = pool

    async def run(self, frame: SharedRunFrame) -> WireResult:
        """Run the task of ``frame``. The same steps and outcomes as
        ``child_runner._run_task_entry``, on the async pool."""
        return await run_seat_task(self._pool, frame)

    async def stop(self) -> None:
        """Close the async pool, bounded by ``SHARED_POOL_CLOSE_TIMEOUT_S``.
        A failed close is logged: the process exits next."""
        try:
            await self._pool.close(timeout=SHARED_POOL_CLOSE_TIMEOUT_S)
        except Exception as exc:
            logger.warning('Closing the async pool of the child failed: %s', exc)


class PostgresSeatProgram:
    """The seat program of a worker's shared async child."""

    async def start(self, init: InitFrame) -> Result[StartedSeats, BootFailedFrame]:
        """Open the async pool, then run the async start hooks.

        ``Err``: the pool could not be set up (``ChildPoolSetupFailed``, or a
        ``ResourceShortage`` for an errno that can clear). A hook that fails
        or hangs ends the process with ``CHILD_HOOK_FAILURE_EXIT_CODE``.
        """
        try:
            pool = await open_shared_async_pool(
                init.database_url,
                connect_kwargs=init.connect_kwargs,
                min_size=init.child_pool_min_size,
                max_size=init.child_pool_max_size,
                check_on_checkout=init.child_pool_check,
            )
        except Exception as exc:
            return Err(boot_failure_from_exception(exc, ChildPoolSetupFailed()))
        await run_shared_async_start_hooks()
        return Ok(PostgresSeats(pool))


async def run_shared_async_start_hooks() -> None:
    """Run each ``on_shared_async_child_start`` hook of the app, in order.

    Each hook is bounded by ``CHILD_HOOK_TIMEOUT_SECONDS``, by a thread that
    ends the process: a hook that blocks the loop cannot stop that thread.
    A hook that raises ends the process at once. Both exits use
    ``CHILD_HOOK_FAILURE_EXIT_CODE``, so the parent stops the worker.
    """
    for hook in get_current_app().get_shared_async_child_start_hooks():
        hook_name = getattr(hook, '__qualname__', repr(hook))
        watchdog = threading.Timer(
            child_runner.CHILD_HOOK_TIMEOUT_SECONDS,
            child_runner._exit_for_hung_hook,
            args=(hook_name,),
        )
        watchdog.daemon = True
        watchdog.start()
        try:
            await hook()
        except BaseException as exc:
            watchdog.cancel()
            logger.critical(
                "on_shared_async_child_start hook '%s' failed in child %s: %s",
                hook_name,
                os.getpid(),
                exc,
            )
            flush_output()
            os._exit(CHILD_HOOK_FAILURE_EXIT_CODE)
        watchdog.cancel()
        logger.debug(
            "[child %s] on_shared_async_child_start hook '%s' completed",
            os.getpid(),
            hook_name,
        )


type _TaskCall = (
    TaskResult[Any, TaskError] | Coroutine[Any, Any, TaskResult[Any, TaskError]]
)
"""What a call of a task wrapper returns: a result, or a coroutine of one."""


async def _seat_call_result(
    task_name: str,
    called: _TaskCall,
    map_exception: Callable[[BaseException], TaskResult[Any, TaskError]],
) -> TaskResult[Any, TaskError]:
    """Await the coroutine of an ``async def`` task call on the running loop.

    A ``CancelledError`` that leaves the coroutine goes through
    ``map_exception``, the mapping of the task
    (``await_task_call_in_worker``). A seat runs only ``async def`` tasks,
    whose wrapper returns a coroutine; ``_prepare_task_call`` refused the
    others. Another value is a defect: ``AssertionError``.
    """
    match called:
        case Coroutine():
            return await await_task_call_in_worker(called, map_exception)
        case _:
            raise AssertionError(
                f'task wrapper of {task_name!r} returned {type(called).__name__} '
                'on a shared async seat',
            )


async def run_seat_task(
    pool: AsyncConnectionPool[AsyncConnection[Any]],
    frame: SharedRunFrame,
) -> WireResult:
    """Run one task on a seat and return its wire tuple.

    The steps of ``_run_task_entry``: the RUNNING transition with the first
    runner heartbeat (a stop before user code returns its wire tuple), the
    task call, the result encode, and the finalizing marker in ``finally``.
    A ``SeatRefusal`` returns ``WORKER_SERIALIZATION_ERROR`` after the RUNNING
    transition, before it resolves the task, as ``_run_task_entry`` does for
    a refused stored timeout. An exception of the body, a cancel among them,
    becomes ``TASK_EXCEPTION``; the seat loop reports a cancel of its deadline
    as the timeout. A cancel during the RUNNING transition or the marker
    propagates, and the seat loop reports it.
    """
    logger.info(f'Starting task execution: {frame.task_name} ({frame.task_id})')
    ownership_result = await confirm_until_not_resumed(
        pool,
        frame.task_id,
        frame.worker_id,
        frame.is_workflow_task,
        frame.claimed_at,
    )
    if ownership_result is not None:
        return ownership_result
    try:
        match frame.work:
            case SeatRefusal(refusal=refusal):
                return _stored_timeout_refusal_response(frame.task_name, refusal)
            case SeatTask():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        prepared = _prepare_task_call(
            frame.task_name,
            frame.args_json,
            frame.kwargs_json,
            OnSharedSeat(),
        )
        match prepared:
            case _PreparedCall(task=task, args=args, kwargs=kwargs):
                pass
            case tuple():
                return prepared
        out = await _seat_call_result(
            frame.task_name,
            task(*args, **kwargs),
            task._map_exception,
        )
        logger.info(f'Task execution completed: {frame.task_name} ({frame.task_id})')
        return _encode_task_outcome(frame.task_name, task, out)
    except BaseException as e:
        return _task_exception_response(frame.task_name, e)
    finally:
        await mark_task_finalizing(
            pool,
            frame.task_id,
            frame.worker_id,
            frame.claimed_at,
        )
