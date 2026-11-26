"""Scheduler task definitions for e2e tests (Layer 3)."""

from __future__ import annotations

from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result

from tests.e2e.tasks.instance_scheduler import app


@app.task(task_name='e2e_scheduled_simple')
def scheduled_simple_task() -> Result[str, TaskError]:
    """Simple task used for scheduler tests."""
    return Ok('scheduled_executed')


@app.task(task_name='e2e_scheduled_with_args')
def scheduled_with_args_task(*, value: int) -> Result[int, TaskError]:
    """Scheduled task that accepts arguments."""
    return Ok(value * 2)


@app.task(task_name='e2e_catch_up_task')
def catch_up_task() -> Result[str, TaskError]:
    """Task used to verify catch-up missed runs logic."""
    return Ok('catch_up_executed')


@app.task(task_name='e2e_cron_task')
def cron_task() -> Result[str, TaskError]:
    """Task used to verify CronSchedule enqueue/firing."""
    return Ok('cron_executed')
