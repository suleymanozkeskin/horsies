"""Type pin: ``@app.task()`` on a function whose first parameter takes the
``TaskRuntime``, and the attempt number of ``TaskRuntime.for_test``.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. The decorator
gives a ``RuntimeTaskFunction`` for a ``def`` function and an
``AsyncRuntimeTaskFunction`` for an ``async def`` function, in each
execution class. The direct call takes the runtime first. The sends, the
schedules and ``node()`` take the parameters of the task after the runtime,
and only these. ``TaskRuntime.for_test`` takes an ``AttemptNumber``, not an
``int``, and a ``RunCancelState``. ``cancel_state()`` gives the closed
``RunCancelState``, which a ``match`` covers with its two variants, and the
closed ``CancelCause`` of ``CancelRequested`` with its two members. Each wrong
use below has an ignore comment, and the config makes an unneeded ignore
comment an error. So an overload set that puts the runtime in
the send parameters, that drops it from the direct call, or that gives the
wrong function kind fails the check.
"""

from __future__ import annotations

import uuid
from collections.abc import Coroutine
from typing import Any, assert_never, assert_type

from horsies import (
    AttemptNumber,
    AttemptNumberRefused,
    CancelCause,
    CancelNotRequested,
    CancelRequested,
    Execution,
    FencingToken,
    Horsies,
    MaxRetries,
    MaxRetriesRefused,
    RetriesLeft,
    RunCancelState,
    TaskError,
    TaskNode,
    TaskRuntime,
)
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.task_decorator import (
    AsyncRuntimeTaskFunction,
    RuntimeTaskFunction,
    TaskHandle,
)
from horsies.core.types.result import Ok, Result


def sync_forms(app: Horsies, runtime: TaskRuntime) -> None:
    @app.task('pin_runtime_sync')
    def with_parameters(runtime: TaskRuntime, *, order_id: str) -> Result[int, TaskError]:
        return Ok(runtime.fencing_token.value + len(order_id))

    @app.task('pin_runtime_sync_only')
    def runtime_only(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
        return Ok(runtime.fencing_token)

    as_runtime: RuntimeTaskFunction[..., int] = with_parameters
    as_runtime_only: RuntimeTaskFunction[[], FencingToken] = runtime_only
    del as_runtime, as_runtime_only

    assert_type(with_parameters(runtime, order_id='o-1'), Result[int, TaskError])
    assert_type(runtime_only(runtime), Result[FencingToken, TaskError])
    with_parameters(order_id='o-1')  # pyright: ignore[reportCallIssue]
    runtime_only()  # pyright: ignore[reportCallIssue]
    with_parameters('not a runtime', order_id='o-1')  # pyright: ignore[reportArgumentType]

    assert_type(with_parameters.send(order_id='o-1'), TaskSendResult[TaskHandle[int]])
    assert_type(runtime_only.send(), TaskSendResult[TaskHandle[FencingToken]])
    assert_type(
        with_parameters.with_options(delay_seconds=5).send(order_id='o-1'),
        TaskSendResult[TaskHandle[int]],
    )
    assert_type(with_parameters.node()(order_id='o-1'), TaskNode[int])
    assert_type(runtime_only.node()(), TaskNode[FencingToken])
    with_parameters.send(runtime, order_id='o-1')  # pyright: ignore[reportCallIssue]
    with_parameters.send(runtime=runtime, order_id='o-1')  # pyright: ignore[reportCallIssue]
    with_parameters.send()  # pyright: ignore[reportCallIssue]
    runtime_only.send(runtime)  # pyright: ignore[reportCallIssue]
    with_parameters.with_options(delay_seconds=5).send(runtime, order_id='o-1')  # pyright: ignore[reportCallIssue]
    with_parameters.node()(runtime=runtime, order_id='o-1')  # pyright: ignore[reportCallIssue]


async def async_forms(app: Horsies, runtime: TaskRuntime) -> None:
    @app.task('pin_runtime_async')
    async def with_parameters(runtime: TaskRuntime, *, order_id: str) -> Result[str, TaskError]:
        return Ok(f'{order_id}:{runtime.fencing_token.value}')

    @app.task('pin_runtime_async_only')
    async def runtime_only(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
        return Ok(runtime.fencing_token)

    as_runtime: AsyncRuntimeTaskFunction[..., str] = with_parameters
    as_sync: RuntimeTaskFunction[..., str] = with_parameters  # pyright: ignore[reportAssignmentType]
    del as_runtime, as_sync

    pending = assert_type(
        with_parameters(runtime, order_id='o-1'),
        Coroutine[Any, Any, Result[str, TaskError]],
    )
    pending_only = assert_type(
        runtime_only(runtime),
        Coroutine[Any, Any, Result[FencingToken, TaskError]],
    )
    del pending, pending_only
    with_parameters(order_id='o-1')  # pyright: ignore[reportCallIssue]

    assert_type(with_parameters.send(order_id='o-1'), TaskSendResult[TaskHandle[str]])
    assert_type(
        await with_parameters.send_async(order_id='o-1'),
        TaskSendResult[TaskHandle[str]],
    )
    assert_type(runtime_only.send(), TaskSendResult[TaskHandle[FencingToken]])
    assert_type(with_parameters.node()(order_id='o-1'), TaskNode[str])
    with_parameters.send(runtime, order_id='o-1')  # pyright: ignore[reportCallIssue]
    with_parameters.send(runtime=runtime, order_id='o-1')  # pyright: ignore[reportCallIssue]
    runtime_only.send(runtime)  # pyright: ignore[reportCallIssue]


def shared_forms(app: Horsies, runtime: TaskRuntime) -> None:
    @app.task('pin_runtime_shared_async', execution=Execution.SHARED_ASYNC, timeout_ms=5_000)
    async def shared_async(runtime: TaskRuntime, *, count: int) -> Result[int, TaskError]:
        return Ok(runtime.fencing_token.value + count)

    @app.task('pin_runtime_shared_thread', execution=Execution.SHARED_THREAD, timeout_ms=5_000)
    def shared_thread(runtime: TaskRuntime, *, count: int) -> Result[int, TaskError]:
        return Ok(runtime.fencing_token.value + count)

    as_async: AsyncRuntimeTaskFunction[..., int] = shared_async
    as_sync: RuntimeTaskFunction[..., int] = shared_thread
    del as_async, as_sync

    assert_type(shared_thread(runtime, count=1), Result[int, TaskError])
    assert_type(shared_async.send(count=1), TaskSendResult[TaskHandle[int]])
    assert_type(shared_thread.send(count=1), TaskSendResult[TaskHandle[int]])
    shared_async.send(runtime, count=1)  # pyright: ignore[reportCallIssue]
    shared_thread.send(runtime=runtime, count=1)  # pyright: ignore[reportCallIssue]

    @app.task(
        'pin_runtime_bad_async_on_def', execution=Execution.SHARED_ASYNC, timeout_ms=5_000
    )  # pyright: ignore[reportArgumentType, reportCallIssue, reportUntypedFunctionDecorator]
    def async_class_on_def(runtime: TaskRuntime, *, count: int) -> Result[int, TaskError]:
        return Ok(runtime.fencing_token.value + count)

    @app.task(
        'pin_runtime_bad_thread_on_async', execution=Execution.SHARED_THREAD, timeout_ms=5_000
    )  # pyright: ignore[reportArgumentType, reportCallIssue, reportUntypedFunctionDecorator]
    async def thread_class_on_async(runtime: TaskRuntime, *, count: int) -> Result[int, TaskError]:
        return Ok(runtime.fencing_token.value + count)

    del async_class_on_def, thread_class_on_async


def the_test_helper_takes_an_attempt_number(task_id: uuid.UUID, attempt: AttemptNumber) -> None:
    assert_type(
        AttemptNumber.parse(2),
        Result[AttemptNumber, AttemptNumberRefused],
    )
    assert_type(TaskRuntime.for_test(task_id=task_id), TaskRuntime)
    assert_type(TaskRuntime.for_test(task_id=task_id, attempt=attempt), TaskRuntime)
    TaskRuntime.for_test(task_id=task_id, attempt=2)  # pyright: ignore[reportArgumentType]
    TaskRuntime.for_test(task_id=task_id, max_retries=3)  # pyright: ignore[reportArgumentType]
    assert_type(MaxRetries.parse(3), Result[MaxRetries, MaxRetriesRefused])


def the_attempt_facts_are_typed(runtime: TaskRuntime) -> None:
    assert_type(runtime.task_id, uuid.UUID)
    assert_type(runtime.attempt, AttemptNumber)
    assert_type(runtime.max_retries, MaxRetries)
    assert_type(runtime.retries_left, RetriesLeft)


def the_cancel_state_is_closed(task_id: uuid.UUID, runtime: TaskRuntime) -> None:
    assert_type(runtime.cancel_state(), RunCancelState)
    match runtime.cancel_state():
        case CancelNotRequested():
            pass
        case CancelRequested(cause=cause):
            match cause:
                case CancelCause.OPERATOR:
                    pass
                case CancelCause.DEADLINE:
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
        case _ as unreachable:
            assert_never(unreachable)
    assert_type(
        TaskRuntime.for_test(
            task_id=task_id,
            cancel_state=CancelRequested(cause=CancelCause.OPERATOR),
        ),
        TaskRuntime,
    )
    TaskRuntime.for_test(task_id=task_id, cancel_state=CancelCause.OPERATOR)  # pyright: ignore[reportArgumentType]
    CancelRequested(cause='OPERATOR')  # pyright: ignore[reportArgumentType]
