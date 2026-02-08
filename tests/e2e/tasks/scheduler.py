"""Scheduler task definitions for e2e tests (Layer 3)."""

from __future__ import annotations

from horsies.core.models.tasks import TaskResult, TaskError

from tests.e2e.tasks.instance_scheduler import app


@app.task(task_name='e2e_scheduled_simple')
def scheduled_simple_task() -> TaskResult[str, TaskError]:
    """Simple task used for scheduler tests."""
    return TaskResult(ok='scheduled_executed')


@app.task(task_name='e2e_scheduled_with_args')
def scheduled_with_args_task(value: int) -> TaskResult[int, TaskError]:
    """Scheduled task that accepts arguments."""
    return TaskResult(ok=value * 2)


@app.task(task_name='e2e_catch_up_task')
def catch_up_task() -> TaskResult[str, TaskError]:
    """Task used to verify catch-up missed runs logic."""
    return TaskResult(ok='catch_up_executed')
