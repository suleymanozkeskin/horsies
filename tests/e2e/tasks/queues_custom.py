"""Custom queue tasks for e2e tests."""

from __future__ import annotations

from horsies.core.models.tasks import TaskResult, TaskError

from tests.e2e.tasks.instance_custom import app


@app.task(task_name='e2e_high', queue_name='high')
def high_task() -> TaskResult[str, TaskError]:
    return TaskResult(ok='high')


@app.task(task_name='e2e_normal', queue_name='normal')
def normal_task() -> TaskResult[str, TaskError]:
    return TaskResult(ok='normal')


@app.task(task_name='e2e_low', queue_name='low')
def low_task() -> TaskResult[str, TaskError]:
    import time

    time.sleep(0.05)  # Small delay to ensure deterministic ordering in priority tests
    return TaskResult(ok='low')


@app.task(task_name='e2e_custom_slow', queue_name='high')
def slow_task(duration_ms: int) -> TaskResult[str, TaskError]:
    """Task that sleeps for specified duration (for custom queue tests)."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')
