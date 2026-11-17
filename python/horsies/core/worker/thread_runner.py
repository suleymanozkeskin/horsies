"""The seat program of a shared thread child: one task run on a seat thread.

A shared thread child boots as an isolated child does (``child_main.boot_app``):
the app import, the ``on_child_process_start`` hooks, and the sync connection
pool of the child, with the bounds of ``SharedThreadChildren.pool``.
Every seat thread writes through that pool, and waits for a connection at
most ``CHILD_POOL_CONNECTION_WAIT_S``. ``run_thread_seat_task`` runs one task
on the calling seat thread: the RUNNING transition and the task body, with
the sync writes of ``child_runner``. ``mark_thread_seat_finalizing`` writes
the finalizing marker after the return, when the run table asks for it.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, Final, assert_never

from horsies.core.logging import get_logger
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.worker.child_protocol import (
    SeatRefusal,
    SeatTask,
    SharedRunFrame,
    ThreadSeatTask,
    ThreadSeatWork,
    WireResult,
)
from horsies.core.worker.shared_child import (
    MarkerBeforeDeadline,
    MarkerWindow,
    MarkerWithoutDeadline,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.thread_child import ThreadSeatReturn
from horsies.core.worker.child_pool import CHILD_POOL_CONNECTION_WAIT_S
from horsies.core.worker.child_runner import (
    CANCEL_REQUEST_TIMEOUT_S,
    _PRE_START_CONFIRM_ATTEMPTS,
    DeadlineBeforeStart,
    StartCheck,
    StatementsBound,
    StatementsLimit,
    StatementsUnbounded,
    OnSharedThreadSeat,
    _confirm_until_not_resumed,
    _encode_task_outcome,
    _mark_task_finalizing,
    _prepare_task_call,
    _PreparedCall,
    _stored_timeout_refusal_response,
    _task_exception_response,
)

logger = get_logger('worker')


class PostgresThreadSeats:
    """The seats of a worker's shared thread child, on its sync pool."""

    def run(
        self,
        frame: SharedRunFrame,
        work: ThreadSeatWork,
        start_check: Callable[[], StartCheck],
    ) -> ThreadSeatReturn:
        """Run the task of ``frame`` on the calling seat thread; see
        ``run_thread_seat_task``."""
        return run_thread_seat_task(frame, work, start_check)

    def mark_finalizing(self, frame: SharedRunFrame, window: MarkerWindow) -> None:
        """Write the finalizing marker of the run of ``frame``; see
        ``mark_thread_seat_finalizing``."""
        mark_thread_seat_finalizing(frame, window)


type _TaskCall = (
    TaskResult[Any, TaskError] | Coroutine[Any, Any, TaskResult[Any, TaskError]]
)
"""What a call of a task wrapper returns: a result, or a coroutine of one."""


def _thread_call_result(
    task_name: str, called: _TaskCall
) -> TaskResult[Any, TaskError]:
    """The result of a call of a ``def`` task wrapper.

    A thread seat runs only ``def`` tasks, whose wrapper returns the result;
    ``_prepare_task_call`` refused the others. Another value is a defect:
    ``AssertionError``.
    """
    match called:
        case TaskResult():
            return called
        case _:
            raise AssertionError(
                f'task wrapper of {task_name!r} returned {type(called).__name__} '
                'on a shared thread seat',
            )


REFUSAL_STATEMENTS_TIMEOUT_S: Final = 10.0
"""The longest time that the statements of one write of a refusal on a
shared thread child take once the write holds its connection: each attempt
of the RUNNING transition, and the finalizing marker."""

THREAD_REFUSAL_WRITES_BOUND_S: Final = (_PRE_START_CONFIRM_ATTEMPTS + 1) * (
    CHILD_POOL_CONNECTION_WAIT_S
    + REFUSAL_STATEMENTS_TIMEOUT_S
    + CANCEL_REQUEST_TIMEOUT_S
)
"""The longest time that the writes of a refusal take on a shared thread
child: each attempt of the RUNNING transition and the finalizing marker,
each with the wait for a pooled connection, its statements, and the cancel
request of a statement past its bound. The refusal backstop of the parent
(``slots.SHARED_REFUSAL_BACKSTOP_S``) comes after it."""


def _statements_of(work: ThreadSeatWork) -> StatementsLimit:
    """The bound of the statements of the writes of a run: a refusal's
    writes are bounded, so that the refusal backstop of the parent never
    meets a database that does not answer; a task keeps the wait of its
    deadline backstop."""
    match work:
        case SeatRefusal():
            return StatementsBound(REFUSAL_STATEMENTS_TIMEOUT_S)
        case ThreadSeatTask():
            return StatementsUnbounded()
        case _ as unreachable:
            assert_never(unreachable)


def run_thread_seat_task(
    frame: SharedRunFrame,
    work: ThreadSeatWork,
    start_check: Callable[[], StartCheck],
) -> ThreadSeatReturn:
    """Run one task on the calling seat thread, up to its wire tuple.

    The steps and outcomes of ``child_runner._run_task_entry`` but the
    finalizing marker: the RUNNING transition with the first runner
    heartbeat, then the task call and the result encode. ``start_check``
    runs once the pool gave a connection, before the RUNNING update: a
    deadline that fired during the wait returns ``DeadlineBeforeStart``, and
    the task does not start; its row stays CLAIMED. A stop before the
    RUNNING update commits returns ``StoppedBeforeRunning``; every other end
    returns ``SetRunning``. A ``SeatRefusal`` returns
    ``WORKER_SERIALIZATION_ERROR`` after the RUNNING transition, before it
    resolves the task. An ``async def`` task cannot run on a thread seat:
    ``WORKER_RESOLUTION_ERROR``. An exception of the body becomes
    ``TASK_EXCEPTION``. A deadline does not stop the body: the run table
    fixes the end at the return. The seat writes the marker after the
    return (``mark_thread_seat_finalizing``).
    """
    logger.info(f'Starting task execution: {frame.task_name} ({frame.task_id})')
    ownership_result = _confirm_until_not_resumed(
        frame.task_id,
        frame.worker_id,
        frame.is_workflow_task,
        frame.claimed_at,
        start_check=start_check,
        statements=_statements_of(work),
    )
    match ownership_result:
        case None:
            return SetRunning(_run_after_running(frame, work))
        case tuple():
            return StoppedBeforeRunning(ownership_result)
        case DeadlineBeforeStart():
            logger.info(
                'Task %s (%s) did not start: its deadline fired while it waited '
                'for a pooled connection; it stays claimed',
                frame.task_name,
                frame.task_id,
            )
            return ownership_result
        case _ as unreachable:
            assert_never(unreachable)


def _run_after_running(frame: SharedRunFrame, work: ThreadSeatWork) -> WireResult:
    """The steps of a run after its RUNNING update, as in
    ``_run_task_entry``. Any exception becomes ``TASK_EXCEPTION``."""
    try:
        match work:
            case SeatRefusal(refusal=refusal):
                return _stored_timeout_refusal_response(frame.task_name, refusal)
            case ThreadSeatTask():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        prepared = _prepare_task_call(
            frame.task_name,
            frame.args_json,
            frame.kwargs_json,
            OnSharedThreadSeat(),
        )
        match prepared:
            case _PreparedCall(task=task, args=args, kwargs=kwargs):
                pass
            case tuple():
                return prepared
        out = _thread_call_result(frame.task_name, task(*args, **kwargs))
        logger.info(f'Task execution completed: {frame.task_name} ({frame.task_id})')
        return _encode_task_outcome(frame.task_name, task, out)
    except BaseException as e:
        return _task_exception_response(frame.task_name, e)


def _refusal_or_task_statements(frame: SharedRunFrame) -> StatementsLimit:
    """The bound of the statements of the writes of the run of ``frame``,
    as ``_statements_of``. A frame of a shared async run on a thread seat is
    a defect: ``AssertionError``."""
    match frame.work:
        case SeatRefusal() | ThreadSeatTask() as work:
            return _statements_of(work)
        case SeatTask():
            raise AssertionError(
                f'run {frame.dispatch} of a shared async child reached a thread seat',
            )
        case _ as unreachable:
            assert_never(unreachable)


def mark_thread_seat_finalizing(frame: SharedRunFrame, window: MarkerWindow) -> None:
    """Write the finalizing marker of a run whose task returned, within
    ``window``, on the sync pool of the child.

    ``MarkerBeforeDeadline`` bounds the wait for a pooled connection by the
    time left before the deadline of the run; ``MarkerWithoutDeadline``
    keeps the wait of the pool (``CHILD_POOL_CONNECTION_WAIT_S``). A write
    that gets no connection in time is logged and skipped
    (``_mark_task_finalizing``). The statement of a task is not cut: a
    database that holds it delays the end report of the run. The statement
    of a refusal is bounded (``_statements_of``).
    """
    match window:
        case MarkerWithoutDeadline():
            _mark_task_finalizing(
                frame.task_id,
                frame.worker_id,
                frame.claimed_at,
                statements=_refusal_or_task_statements(frame),
            )
        case MarkerBeforeDeadline(seconds=seconds):
            _mark_task_finalizing(
                frame.task_id,
                frame.worker_id,
                frame.claimed_at,
                connection_wait_s=seconds,
            )
        case _ as unreachable:
            assert_never(unreachable)
