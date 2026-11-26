"""Type pin: ``@app.task()`` accepts an ``async def`` task function.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. The decorator
gives an ``AsyncTaskFunction``. Its direct call returns a coroutine of the
``Result``. Its send methods and ``node()`` are those of a ``def`` task.
Each wrong use below has an ignore comment, and the config makes an unneeded
ignore comment an error. So a decorator type that types the direct call as a
``Result``, or that makes an async task a ``TaskFunction``, fails the
check.
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any, assert_type

from horsies import Horsies, Ok, Result, TaskError, TaskNode
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.task_decorator import AsyncTaskFunction, TaskFunction, TaskHandle


def register(app: Horsies) -> None:
    @app.task('typing_pin_sync_task')
    def sync_task(*, value: int) -> Result[int, TaskError]:
        return Ok(value)

    @app.task('typing_pin_async_task', timeout_ms=5_000)
    async def async_task(*, value: int) -> Result[str, TaskError]:
        return Ok(str(value))

    assert_type(sync_task(value=1), Result[int, TaskError])
    pending = assert_type(
        async_task(value=1),
        Coroutine[Any, Any, Result[str, TaskError]],
    )
    assert_type(async_task.send(value=1), TaskSendResult[TaskHandle[str]])
    assert_type(async_task.node()(value=1), TaskNode[str])
    as_async: AsyncTaskFunction[..., str] = async_task
    as_sync: TaskFunction[..., str] = async_task  # pyright: ignore[reportAssignmentType]
    awaited: Result[str, TaskError] = async_task(value=1)  # pyright: ignore[reportAssignmentType]
    del pending, as_async, as_sync, awaited
