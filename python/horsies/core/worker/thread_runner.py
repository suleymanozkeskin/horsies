"""The seat program of a shared thread child: one task run on a seat thread.

A shared thread child boots as an isolated child does (``child_main.boot_app``):
the app import, the ``on_child_process_start`` hooks, and the core handle of
the child, with the pool of the child kind in the init frame
(``SharedThreadChildren.pool``). Every seat thread writes through that
handle. ``run_thread_seat_task`` runs one task on the calling seat thread:
the RUNNING transition and the task body, with the sync writes of
``child_runner``. ``mark_thread_seat_finalizing`` writes the finalizing
marker after the return, when the run table asks for it.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, Final, assert_never

from horsies import _core as core
from horsies.core.logging import get_logger
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.worker.child_frames import ThreadSeatWork, stored_refusal_of
from horsies.core.worker.run_return import (
    BodyInterrupted,
    CoreStoppedBeforeStart,
    MarkerEnd,
    ResultEnvelope,
)
from horsies.core.worker.shared_child import StoppedBeforeRunning
from horsies.core.worker.thread_child import ThreadSeatReturn
from horsies.core.worker.child_runner import (
    DeadlineBeforeStart,
    StartCheck,
    OnSharedThreadSeat,
    _confirm_until_not_resumed,
    _prepare_task_call,
    _PreparedCall,
    _stored_timeout_refusal_response,
    _task_exception_response,
    encoded_body_end,
    mark_run_finalizing,
    set_running_or_end_child,
)
from horsies.core.worker.shared_lifecycle import REFUSAL_WRITES_BOUND_S

logger = get_logger('worker')


class PostgresThreadSeats:
    """The seats of a worker's shared thread child, on the core handle of
    the child."""

    def run(
        self,
        frame: core.SharedRunFrame,
        work: ThreadSeatWork,
        start_check: Callable[[], StartCheck],
    ) -> ThreadSeatReturn:
        """Run the task of ``frame`` on the calling seat thread; see
        ``run_thread_seat_task``."""
        return run_thread_seat_task(frame, work, start_check)

    def mark_finalizing(self, frame: core.SharedRunFrame) -> MarkerEnd:
        """Write the finalizing marker of the run of ``frame``; see
        ``mark_thread_seat_finalizing``."""
        return mark_thread_seat_finalizing(frame)


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


THREAD_REFUSAL_WRITES_BOUND_S: Final = REFUSAL_WRITES_BOUND_S
"""The longest time that the writes of a refusal take on a shared thread
child: the writes of a shared async child (``REFUSAL_WRITES_BOUND_S``),
through the same core handle with the same bounds. The refusal backstop of
the parent (``slots.SHARED_REFUSAL_BACKSTOP_S``) comes after it."""


def run_thread_seat_task(
    frame: core.SharedRunFrame,
    work: ThreadSeatWork,
    start_check: Callable[[], StartCheck],
) -> ThreadSeatReturn:
    """Run one task on the calling seat thread, up to its result.

    The steps and outcomes of ``child_runner._run_task_entry`` but the
    finalizing marker: the RUNNING transition with the first runner
    heartbeat, then the task call and the result encode. ``start_check``
    runs right before the RUNNING update and gives the time left before the
    deadline of the run; the core checks it again once the pool gave a
    connection. A deadline that passed first returns ``DeadlineBeforeStart``,
    and the task does not start; its row stays CLAIMED. A stop before the
    RUNNING update commits returns ``StoppedBeforeRunning``, or
    ``CoreStoppedBeforeStart`` when the core of the child stopped; every
    other end returns ``SetRunning``. Refused stored options
    (``core.StoredOptionsRefused``) return ``WORKER_SERIALIZATION_ERROR``
    after the RUNNING transition, before the task is resolved. An ``async def`` task cannot run on a thread seat:
    ``WORKER_RESOLUTION_ERROR``. An exception of the body becomes
    ``TASK_EXCEPTION``. A deadline does not stop the body: the run table
    fixes the end at the return. The seat writes the marker after the
    return (``mark_thread_seat_finalizing``). A body that a refusal of the
    core of the child ended does not return: the child ends at once
    (``set_running_or_end_child``).
    """
    logger.info(f'Starting task execution: {frame.task_name} ({frame.task_id})')
    ownership_result = _confirm_until_not_resumed(
        str(frame.task_id),
        frame.worker_id,
        frame.claimed_at,
        orphan_self_heal=frame.orphan_self_heal,
        start_check=start_check,
    )
    match ownership_result:
        case None:
            return set_running_or_end_child(
                str(frame.task_id),
                _run_after_running(frame, work),
            )
        case StoppedBeforeRunning() | CoreStoppedBeforeStart():
            return ownership_result
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


def _run_after_running(
    frame: core.SharedRunFrame,
    work: ThreadSeatWork,
) -> ResultEnvelope | BodyInterrupted:
    """The steps of a run after its RUNNING update, as in
    ``_run_task_entry``. Any exception becomes ``TASK_EXCEPTION``. A body
    that a refusal of the core ended gives ``BodyInterrupted``
    (``encoded_body_end``)."""
    try:
        match work:
            case core.StoredOptionsRefused() as refused:
                return _stored_timeout_refusal_response(
                    frame.task_name, stored_refusal_of(refused),
                )
            case core.ThreadTaskWork():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        prepared = _prepare_task_call(frame.task_name, frame.input, OnSharedThreadSeat())
        match prepared:
            case _PreparedCall(task=task, args=args, kwargs=kwargs):
                pass
            case ResultEnvelope():
                return prepared
        out = _thread_call_result(frame.task_name, task(*args, **kwargs))
        logger.info(f'Task execution completed: {frame.task_name} ({frame.task_id})')
        return encoded_body_end(frame.task_name, task, out)
    except BaseException as e:
        return _task_exception_response(frame.task_name, e)


def mark_thread_seat_finalizing(frame: core.SharedRunFrame) -> MarkerEnd:
    """Write the finalizing marker of a run whose task returned, through the
    core handle of the child, on the calling seat thread
    (``child_runner.mark_run_finalizing``): within the bound of the core.
    A failed or skipped write is logged only. A core that stopped gives
    ``MarkerMetStoppedCore``: the run reports its result, and the child
    then ends itself (``mark_run_finalizing``).
    """
    return mark_run_finalizing(frame.task_id, frame.worker_id, frame.claimed_at)
