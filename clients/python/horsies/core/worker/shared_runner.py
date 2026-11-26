"""The seat program of a shared async child: its start, and one task run.

``PostgresSeatProgram.start`` runs on the task loop of the child, after the
sync boot (the app import, the ``on_child_process_start`` hooks and the core
handle of the child): it runs the ``on_shared_async_child_start`` hooks,
each bounded. ``PostgresSeats.run`` runs one task as a coroutine on that
loop: the RUNNING transition and the task body.
``PostgresSeats.mark_finalizing`` writes the finalizing marker, which the
seat loop asks for at the end of the run. Every write goes through the
awaitable methods of the core handle (``shared_lifecycle``), so no write
blocks the loop.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Coroutine
from typing import Any, assert_never

from horsies import _core as core
from horsies.core.logging import get_logger
from horsies.core.models.boot_failure import (
    ChildPoolSetupFailed,
    ThreadStartRefusedError,
)
from horsies.core.models.tasks import Execution, TaskError
from horsies.core.task_decorator import await_task_call_in_worker
from horsies.core.telemetry.attempt_span import BodyEndOrder, run_in_attempt_span_async
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_main import (
    boot_failure_from_exception,
    flush_output,
)
from horsies.core.worker.child_frames import BootFailedFrame, stored_refusal_of
from horsies.core.worker import child_runner
from horsies.core.worker.child_runner import (
    CHILD_HOOK_FAILURE_EXIT_CODE,
    OnSharedSeat,
    _prepare_task_call,
    _PreparedCall,
    run_runtime,
    _stored_timeout_refusal_response,
    _task_exception_response,
    encoded_body_end,
    set_running_or_end_child,
    shared_attempt_subject,
)
from horsies.core.worker.current import get_current_app
from horsies.core.worker.shared_child import (
    SeatReturn,
    StartedSeats,
    StoppedBeforeRunning,
)
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
from horsies.core.worker.shared_lifecycle import (
    confirm_until_not_resumed,
    mark_task_finalizing,
)

logger = get_logger('worker')


class PostgresSeats:
    """The seats of a started shared async child."""

    async def run(
        self,
        frame: core.SharedRunFrame,
        body_end: Callable[[], BodyEndOrder],
        cancel: RunCancel,
    ) -> SeatReturn:
        """Run the task of ``frame``. The same steps and outcomes as
        ``child_runner.run_isolated_task`` but the marker, through the core
        handle of the child. ``body_end`` orders the deadline and the end
        of the steps for the attempt span; ``cancel`` is the cancel of the
        run."""
        return await run_seat_task(frame, body_end, cancel)

    async def mark_finalizing(self, frame: core.SharedRunFrame) -> MarkerEnd:
        """Write the finalizing marker of the run of ``frame``
        (``mark_seat_finalizing``)."""
        return await mark_seat_finalizing(frame)


class PostgresSeatProgram:
    """The seat program of a worker's shared async child."""

    async def start(self) -> Result[StartedSeats, BootFailedFrame]:
        """Run the async start hooks of the app.

        ``Err``: the OS refused the watchdog of a hook
        (``ThreadStartRefused``). A hook that fails or hangs ends the
        process with ``CHILD_HOOK_FAILURE_EXIT_CODE``.
        """
        try:
            await run_shared_async_start_hooks()
        except ThreadStartRefusedError as exc:
            return Err(boot_failure_from_exception(exc, ChildPoolSetupFailed()))
        return Ok(PostgresSeats())


async def run_shared_async_start_hooks() -> None:
    """Run each ``on_shared_async_child_start`` hook of the app, in order.

    Each hook is bounded by ``CHILD_HOOK_TIMEOUT_SECONDS``, by a thread that
    ends the process: a hook that blocks the loop cannot stop that thread.
    A hook that raises ends the process at once. Both exits use
    ``CHILD_HOOK_FAILURE_EXIT_CODE``, so the parent stops the worker.

    Raises:
        ThreadStartRefusedError: the OS refused the watchdog of a hook,
            before that hook ran.
    """
    for hook in get_current_app().get_shared_async_child_start_hooks():
        hook_name = getattr(hook, '__qualname__', repr(hook))
        watchdog = child_runner.start_hook_watchdog(hook_name)
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
    Result[Any, TaskError] | Coroutine[Any, Any, Result[Any, TaskError]]
)
"""What a call of a task wrapper returns: a result, or a coroutine of one."""


async def _seat_call_result(
    task_name: str,
    called: _TaskCall,
    map_exception: Callable[[BaseException], Result[Any, TaskError]],
) -> Result[Any, TaskError]:
    """Await the coroutine of an ``async def`` task call on the running loop.

    ``await_task_call_in_worker`` maps each exception that it takes from
    the body (a ``CancelledError``, a ``GeneratorExit``) through
    ``map_exception``, the mapping of the task. A call that returned while a
    cancel request is left on the seat task ends as a cancel too
    (``_without_left_cancel``). A seat runs only ``async def`` tasks, whose
    wrapper returns a coroutine; ``_prepare_task_call`` refused the others.
    Another value is a defect: ``AssertionError``.
    """
    match called:
        case Coroutine():
            mapped: list[BaseException] = []

            def map_and_note(exc: BaseException) -> Result[Any, TaskError]:
                mapped.append(exc)
                return map_exception(exc)

            out = await await_task_call_in_worker(called, map_and_note)
            match mapped:
                case []:
                    return _without_left_cancel(out, map_exception)
                case _:
                    return out
        case _:
            raise AssertionError(
                f'task wrapper of {task_name!r} returned {type(called).__name__} '
                'on a shared async seat',
            )


def _without_left_cancel(
    out: Result[Any, TaskError],
    map_exception: Callable[[BaseException], Result[Any, TaskError]],
) -> Result[Any, TaskError]:
    """Clear a cancel request that a returned call left on the seat task.

    A body can cancel its own task (``asyncio.current_task().cancel()``, or
    a ``call_later`` of that cancel) and return before an ``await`` takes
    the cancel. The seat task would then end cancelled before its deadline.
    The requests (``Task.cancelling()``) are cleared here, and the call
    ends as a cancel of the body: ``map_exception`` maps a
    ``CancelledError``, as for a cancel that reached the body at an
    ``await``. A body that caught a cancel and did not call ``uncancel()``
    leaves its request too, and ends the same way. A request of a deadline
    cancel changes nothing: the seat loop ends that run with the timeout.
    An operator cancel stays recorded in the cancel of the run
    (``RunCancel``), which this does not touch: the seat loop ends that run
    as cancelled. With no request left, ``out`` returns unchanged.
    """
    seat_task = asyncio.current_task()
    assert seat_task is not None, 'a seat call runs in its seat task'
    left_requests = seat_task.cancelling()
    if left_requests == 0:
        return out
    for _ in range(left_requests):
        _ = seat_task.uncancel()
    return map_exception(asyncio.CancelledError())


async def run_seat_task(
    frame: core.SharedRunFrame,
    body_end: Callable[[], BodyEndOrder],
    cancel: RunCancel,
) -> SeatReturn:
    """Run one task on a seat, up to its result.

    The steps of ``run_isolated_task`` but the finalizing marker: the
    RUNNING transition through the core handle of the child, then the task
    call and the result encode (``_run_after_running``). A stop before the
    RUNNING update commits returns ``StoppedBeforeRunning``; every other end
    returns ``SetRunning``, but a body that a refusal of the core of the
    child ended: the child ends at once (``set_running_or_end_child``). The
    seat loop writes the marker at the end of the run
    (``PostgresSeats.mark_finalizing``). A cancel during the RUNNING
    transition propagates, and the seat loop reports it.

    ``cancel`` marks the body of the run (``RunCancel.begin_body``,
    ``RunCancel.end_body``): the seat loop gives an operator cancel to the
    seat task only while the body may run. An operator cancel that came
    before the body skips it: ``RunCancelled`` with ``NOT_STARTED``.
    """
    logger.info(f'Starting task execution: {frame.task_name} ({frame.task_id})')
    ownership_result = await confirm_until_not_resumed(
        str(frame.task_id),
        frame.worker_id,
        frame.claimed_at,
        orphan_self_heal=frame.orphan_self_heal,
    )
    match ownership_result:
        case StartedRunning() as started:
            pass
        case StoppedBeforeRunning() | CoreStoppedBeforeStart():
            return ownership_result
        case _ as unreachable:
            assert_never(unreachable)
    match cancel.begin_body():
        case SkipTheBody():
            logger.info(
                f'Task {frame.task_name} ({frame.task_id}) was cancelled before its '
                'body started; the body does not run',
            )
            return RunCancelled(body=CancelledBody.NOT_STARTED, started_at=started.started_at)
        case StartTheBody():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    ran = await _run_after_running(frame, started.workflow, body_end, cancel)
    cancel.end_body()
    return set_running_or_end_child(str(frame.task_id), started, ran)


async def _run_after_running(
    frame: core.SharedRunFrame,
    workflow: RunWorkflow,
    body_end: Callable[[], BodyEndOrder],
    cancel: RunCancel,
) -> ResultEnvelope | BodyInterrupted:
    """The steps of a run after its RUNNING update (``_seat_steps``), inside
    the span and the log context of its attempt, whose start gave the
    workflow node ``workflow`` (``run_in_attempt_span_async``). They start
    inside the seat coroutine, so the span is current in the context of this
    seat task only: the body, and each task that the body starts, sees it;
    the other seats do not. A deadline that came first (``body_end``) ends
    the span with the timeout, the end that the seat loop reports."""
    return await run_in_attempt_span_async(
        shared_attempt_subject(frame, Execution.SHARED_ASYNC, workflow),
        lambda: _seat_steps(frame, cancel),
        body_end,
    )


async def _seat_steps(
    frame: core.SharedRunFrame,
    cancel: RunCancel,
) -> EncodedResult | BodyInterrupted:
    """The steps of a run after its RUNNING update, as in
    ``_run_task_entry``.

    Refused stored options (``core.StoredOptionsRefused``) return
    ``WORKER_SERIALIZATION_ERROR`` before the task is resolved, as
    ``_run_task_entry`` does. The body runs through ``_seat_call_result``, which maps a
    cancel of the body; the seat loop reports a cancel of its deadline as
    the timeout. Any other exception becomes ``TASK_EXCEPTION``. A body
    that a refusal of the core ended gives ``BodyInterrupted``
    (``encoded_body_end``).
    """
    try:
        match frame.work:
            case core.StoredOptionsRefused() as refused:
                return _stored_timeout_refusal_response(
                    frame.task_name, stored_refusal_of(refused),
                )
            case core.AsyncTaskWork():
                pass
            case core.ThreadTaskWork():
                # The seat loop ends the child before such a run starts.
                raise AssertionError(
                    f'run {frame.dispatch} of a shared thread child reached a '
                    'shared async seat',
                )
            case _ as unreachable:
                assert_never(unreachable)
        prepared = _prepare_task_call(
            frame.task_name,
            frame.input,
            OnSharedSeat(),
            run_runtime(frame.task_id, frame.attempt, cancel.cell),
        )
        match prepared:
            case _PreparedCall(task=task, args=args, kwargs=kwargs):
                pass
            case EncodedResult():
                return prepared
        out = await _seat_call_result(
            frame.task_name,
            task(*args, **kwargs),
            task._map_exception,
        )
        logger.info(f'Task execution completed: {frame.task_name} ({frame.task_id})')
        return encoded_body_end(frame.task_name, task, out)
    except BaseException as e:
        return _task_exception_response(frame.task_name, e)


async def mark_seat_finalizing(frame: core.SharedRunFrame) -> MarkerEnd:
    """Write the finalizing marker of a run that ended
    (``shared_lifecycle.mark_task_finalizing``): within the bound of the
    core, ``FINALIZING_MARKER_TIMEOUT_S``. A failed or late write is logged
    only: the marker is best effort, and the end of the run follows at
    once. ``MarkerMetStoppedCore``: the core of the child stopped; the run
    reports its result, and the child then ends itself. Nothing escapes but
    a cancel of the calling task.
    """
    return await mark_task_finalizing(frame.task_id, frame.worker_id, frame.claimed_at)
