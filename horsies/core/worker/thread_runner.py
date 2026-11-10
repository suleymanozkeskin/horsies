"""The seat program of a shared thread child: one task run on a seat thread.

A shared thread child boots as an isolated child does (``child_main.boot_app``):
the app import, the ``on_child_process_start`` hooks, and the sync connection
pool of the child, with the sizes of ``PostgresConfig.worker_child_pool_*``.
Every seat thread writes through that pool, and waits for a connection at
most ``CHILD_POOL_CONNECTION_WAIT_S``. ``run_thread_seat_task`` runs one task
on the calling seat thread: the RUNNING transition, the task body, and the
finalizing marker, with the sync writes of ``child_runner``.
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

    def run(self, frame: SharedRunFrame, work: ThreadSeatWork) -> WireResult:
        """Run the task of ``frame`` on the calling seat thread; see
        ``run_thread_seat_task``."""
        return run_thread_seat_task(frame, work)


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


def run_thread_seat_task(frame: SharedRunFrame, work: ThreadSeatWork) -> WireResult:
    """Run one task on the calling seat thread and return its wire tuple.

    The steps and outcomes of ``child_runner._run_task_entry``: the RUNNING
    transition with the first runner heartbeat (a stop before user code
    returns its wire tuple), the task call, the result encode, and the
    finalizing marker in ``finally``. A ``SeatRefusal`` returns
    ``WORKER_SERIALIZATION_ERROR`` after the RUNNING transition, before it
    resolves the task. An ``async def`` task cannot run on a thread seat:
    ``WORKER_RESOLUTION_ERROR``. An exception of the body becomes
    ``TASK_EXCEPTION``. A deadline does not stop the body: the seat loop
    reports the run as timed out when it ends after its deadline.
    """
    logger.info(f'Starting task execution: {frame.task_name} ({frame.task_id})')
    ownership_result = _confirm_until_not_resumed(
        frame.task_id,
        frame.worker_id,
        frame.is_workflow_task,
        frame.claimed_at,
    )
    if ownership_result is not None:
        return ownership_result
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
    finally:
        _mark_task_finalizing(frame.task_id, frame.worker_id, frame.claimed_at)
