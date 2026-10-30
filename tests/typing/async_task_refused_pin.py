"""Type pin: ``@app.task()`` refuses an ``async def`` task function.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. The decorator
expects a function that returns ``TaskResult``; an ``async def`` returns a
coroutine. The config makes an unneeded ignore comment an error, so a
decorator type that accepts an ``async def`` fails the check.
"""

from __future__ import annotations

from horsies import Horsies, TaskError, TaskResult


def register(app: Horsies) -> None:
    @app.task('typing_pin_sync_task')
    def sync_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('typing_pin_async_task')  # pyright: ignore[reportArgumentType]
    async def async_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    del sync_task, async_task
