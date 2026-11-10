"""Type pin: the execution rules of ``@app.task()`` that a type checker sees.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. A
``SHARED_ASYNC`` declaration needs ``timeout_ms`` and decorates only an
``async def`` function; a ``SHARED_THREAD`` declaration needs ``timeout_ms``,
takes no ``cancel_grace_ms``, and decorates only a ``def`` function;
``ISOLATED`` (the default) takes both kinds and no grace. A class known only
at run time needs ``timeout_ms``. Each wrong use below has an ignore comment,
and the config makes an unneeded ignore comment an error. So an overload set
that accepts one of these uses fails the check.
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any, assert_type

from horsies import Execution, Horsies, TaskError, TaskResult
from horsies.core.task_decorator import AsyncTaskFunction, TaskFunction


def register(app: Horsies, chosen: Execution) -> None:
    @app.task('pin_shared_async', execution=Execution.SHARED_ASYNC, timeout_ms=5_000)
    async def shared_async(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task(
        'pin_shared_async_grace',
        execution=Execution.SHARED_ASYNC,
        timeout_ms=5_000,
        cancel_grace_ms=1_000,
    )
    async def shared_async_grace(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_shared_thread', execution=Execution.SHARED_THREAD, timeout_ms=5_000)
    def shared_thread(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_isolated', execution=Execution.ISOLATED)
    async def isolated(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_chosen', execution=chosen, timeout_ms=5_000)
    def chosen_at_run_time(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    pending = assert_type(
        shared_async(value=1),
        Coroutine[Any, Any, TaskResult[int, TaskError]],
    )
    assert_type(shared_thread(value=1), TaskResult[int, TaskError])
    as_async: AsyncTaskFunction[..., int] = shared_async_grace
    as_isolated: AsyncTaskFunction[..., int] = isolated
    as_chosen: TaskFunction[..., int] = chosen_at_run_time
    as_sync: TaskFunction[..., int] = shared_async  # pyright: ignore[reportAssignmentType]
    del pending, as_async, as_isolated, as_chosen, as_sync

    @app.task(
        'pin_bad_async_on_def', execution=Execution.SHARED_ASYNC, timeout_ms=5_000
    )  # pyright: ignore[reportArgumentType]
    def async_class_on_def(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task(
        'pin_bad_thread_on_async', execution=Execution.SHARED_THREAD, timeout_ms=5_000
    )  # pyright: ignore[reportArgumentType]
    async def thread_class_on_async(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_bad_no_timeout', execution=Execution.SHARED_ASYNC)  # pyright: ignore[reportArgumentType]
    async def shared_without_timeout(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task(
        'pin_bad_thread_grace',
        execution=Execution.SHARED_THREAD,  # pyright: ignore[reportArgumentType]
        timeout_ms=5_000,
        cancel_grace_ms=1_000,
    )
    def thread_with_grace(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_bad_isolated_grace', cancel_grace_ms=1_000)  # pyright: ignore[reportCallIssue, reportUntypedFunctionDecorator]
    def isolated_with_grace(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_bad_chosen_no_timeout', execution=chosen)  # pyright: ignore[reportArgumentType]
    def chosen_without_timeout(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    del (
        async_class_on_def,
        thread_class_on_async,
        shared_without_timeout,
        thread_with_grace,
        isolated_with_grace,
        chosen_without_timeout,
    )
