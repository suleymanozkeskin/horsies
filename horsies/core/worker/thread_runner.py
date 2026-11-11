"""The seat program of a shared thread child: one task run on a seat thread.

A shared thread child boots as an isolated child does (``child_main.boot_app``):
the app import, the ``on_child_process_start`` hooks, and the sync connection
pool of the child, with the sizes of ``PostgresConfig.worker_child_pool_*``.
Every seat thread writes through that pool, and waits for a connection at
most ``CHILD_POOL_CONNECTION_WAIT_S``. ``run_thread_seat_task`` runs one task
on the calling seat thread: the RUNNING transition and the task body, with
the sync writes of ``child_runner``. ``mark_thread_seat_finalizing`` writes
the finalizing marker after the return, when the run table asks for it.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any, assert_never

from horsies.core.logging import get_logger
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.worker.child_protocol import (
    SeatRefusal,
    SharedRunFrame,
    ThreadSeatTask,
    ThreadSeatWork,
    WireResult,
)
from horsies.core.worker.shared_child import (
    MarkerBeforeDeadline,
    MarkerWindow,
    MarkerWithoutDeadline,
    SeatReturn,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.child_runner import (
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

    def run(self, frame: SharedRunFrame, work: ThreadSeatWork) -> SeatReturn:
        """Run the task of ``frame`` on the calling seat thread; see
        ``run_thread_seat_task``."""
        return run_thread_seat_task(frame, work)

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


def run_thread_seat_task(frame: SharedRunFrame, work: ThreadSeatWork) -> SeatReturn:
    """Run one task on the calling seat thread, up to its wire tuple.

    The steps and outcomes of ``child_runner._run_task_entry`` but the
    finalizing marker: the RUNNING transition with the first runner
    heartbeat, then the task call and the result encode. A stop before the
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
    )
    if ownership_result is not None:
        return StoppedBeforeRunning(ownership_result)
    return SetRunning(_run_after_running(frame, work))


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


def mark_thread_seat_finalizing(frame: SharedRunFrame, window: MarkerWindow) -> None:
    """Write the finalizing marker of a run whose task returned, within
    ``window``, on the sync pool of the child.

    ``MarkerBeforeDeadline`` bounds the wait for a pooled connection by the
    time left before the deadline of the run; ``MarkerWithoutDeadline``
    keeps the wait of the pool (``CHILD_POOL_CONNECTION_WAIT_S``). A write
    that gets no connection in time is logged and skipped
    (``_mark_task_finalizing``). The statement itself is not cut: a database
    that holds it delays the end report of the run.
    """
    match window:
        case MarkerWithoutDeadline():
            _mark_task_finalizing(frame.task_id, frame.worker_id, frame.claimed_at)
        case MarkerBeforeDeadline(seconds=seconds):
            _mark_task_finalizing(
                frame.task_id,
                frame.worker_id,
                frame.claimed_at,
                connection_wait_s=seconds,
            )
        case _ as unreachable:
            assert_never(unreachable)
