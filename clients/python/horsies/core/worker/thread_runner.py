"""The seat program of a shared thread child: one task run on a seat thread.

A shared thread child boots as an isolated child does (``child_main.boot_app``):
the app import, the ``on_child_start`` hooks, and the core handle of
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
from typing import Any, assert_never

from horsies import _core as core
from horsies.core.logging import get_logger
from horsies.core.models.tasks import Execution, TaskError
from horsies.core.types.result import Err, Ok, Result
from horsies.core.telemetry.attempt_span import BodyEndOrder, run_in_attempt_span
from horsies.core.worker.child_frames import ThreadSeatWork, stored_refusal_of
from horsies.core.worker.run_return import (
    BodyInterrupted,
    CancelledBody,
    CoreStoppedBeforeStart,
    EncodedResult,
    MarkerEnd,
    ResultEnvelope,
    RunCancelled,
    RunWorkflow,
    StartedRunning,
)
from horsies.core.worker.run_cancel import RunCancel, SkipTheBody, StartTheBody
from horsies.core.worker.shared_child import StoppedBeforeRunning
from horsies.core.worker.thread_child import ThreadSeatReturn
from horsies.core.worker.child_runner import (
    DeadlineBeforeStart,
    StartCheck,
    OnSharedThreadSeat,
    _confirm_until_not_resumed,
    _prepare_task_call,
    _PreparedCall,
    run_runtime,
    _stored_timeout_refusal_response,
    _task_exception_response,
    encoded_body_end,
    mark_run_finalizing,
    set_running_or_end_child,
    shared_attempt_subject,
)

logger = get_logger('worker')


class PostgresThreadSeats:
    """The seats of a worker's shared thread child, on the core handle of
    the child."""

    def run(
        self,
        frame: core.SharedRunFrame,
        work: ThreadSeatWork,
        start_check: Callable[[], StartCheck],
        body_end: Callable[[], BodyEndOrder],
        cancel: RunCancel,
    ) -> ThreadSeatReturn:
        """Run the task of ``frame`` on the calling seat thread; see
        ``run_thread_seat_task``."""
        return run_thread_seat_task(frame, work, start_check, body_end, cancel)

    def mark_finalizing(self, frame: core.SharedRunFrame) -> MarkerEnd:
        """Write the finalizing marker of the run of ``frame``; see
        ``mark_thread_seat_finalizing``."""
        return mark_thread_seat_finalizing(frame)


type _TaskCall = (
    Result[Any, TaskError] | Coroutine[Any, Any, Result[Any, TaskError]]
)
"""What a call of a task wrapper returns: a result, or a coroutine of one."""


def _thread_call_result(
    task_name: str, called: _TaskCall
) -> Result[Any, TaskError]:
    """The result of a call of a ``def`` task wrapper.

    A thread seat runs only ``def`` tasks, whose wrapper returns the result;
    ``_prepare_task_call`` refused the others. Another value is a defect:
    ``AssertionError``.
    """
    match called:
        case Ok() | Err():
            return called
        case _:
            raise AssertionError(
                f'task wrapper of {task_name!r} returned {type(called).__name__} '
                'on a shared thread seat',
            )


def run_thread_seat_task(
    frame: core.SharedRunFrame,
    work: ThreadSeatWork,
    start_check: Callable[[], StartCheck],
    body_end: Callable[[], BodyEndOrder],
    cancel: RunCancel,
) -> ThreadSeatReturn:
    """Run one task on the calling seat thread, up to its result.

    The steps and outcomes of ``child_runner._run_task_entry`` but the
    finalizing marker: the RUNNING transition, then the task call and the
    result encode. ``start_check``
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
    ``TASK_EXCEPTION``. A deadline does not stop the body: ``body_end``
    fixes the order of the deadline and the end of the steps in the run
    table when the steps return, and the attempt span ends with it. The
    seat writes the marker after the return (``mark_thread_seat_finalizing``). A body that a refusal of the
    core of the child ended does not return: the child ends at once
    (``set_running_or_end_child``).

    ``cancel`` marks the body of the run (``RunCancel.begin_body``,
    ``RunCancel.end_body``), and the runtime of the body reads its cell: an
    operator cancel or the deadline sets it, and the body reads it with
    ``TaskRuntime.cancel_state``. An operator cancel that came before the
    body skips it: ``RunCancelled`` with ``NOT_STARTED``.
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
        case StartedRunning() as started:
            match cancel.begin_body():
                case SkipTheBody():
                    logger.info(
                        f'Task {frame.task_name} ({frame.task_id}) was cancelled before '
                        'its body started; the body does not run',
                    )
                    return RunCancelled(
                        body=CancelledBody.NOT_STARTED, started_at=started.started_at,
                    )
                case StartTheBody():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            ran = _run_after_running(frame, work, started.workflow, body_end, cancel)
            cancel.end_body()
            return set_running_or_end_child(str(frame.task_id), started, ran)
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
    workflow: RunWorkflow,
    body_end: Callable[[], BodyEndOrder],
    cancel: RunCancel,
) -> ResultEnvelope | BodyInterrupted:
    """The steps of a run after its RUNNING update (``_thread_steps``),
    inside the span and the log context of its attempt, whose start gave the
    workflow node ``workflow`` (``run_in_attempt_span``). The span is current
    on the calling seat thread only, so the other seats do not see it. A
    deadline that came first (``body_end``) ends the span with the timeout,
    the end that the run table reports."""
    return run_in_attempt_span(
        shared_attempt_subject(frame, Execution.SHARED_THREAD, workflow),
        lambda: _thread_steps(frame, work, cancel),
        body_end,
    )


def _thread_steps(
    frame: core.SharedRunFrame,
    work: ThreadSeatWork,
    cancel: RunCancel,
) -> EncodedResult | BodyInterrupted:
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
        prepared = _prepare_task_call(
            frame.task_name,
            frame.input,
            OnSharedThreadSeat(),
            run_runtime(frame.task_id, frame.attempt, frame.max_retries, cancel.cell),
        )
        match prepared:
            case _PreparedCall(task=task, args=args, kwargs=kwargs):
                pass
            case EncodedResult():
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
