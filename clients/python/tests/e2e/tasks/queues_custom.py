"""Custom queue tasks for e2e tests."""

from __future__ import annotations

from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result

from tests.e2e.tasks.instance_custom import app


@app.task(name='e2e_high', queue='high')
def high_task() -> Result[str, TaskError]:
    return Ok('high')


@app.task(name='e2e_normal', queue='normal')
def normal_task() -> Result[str, TaskError]:
    return Ok('normal')


@app.task(name='e2e_low', queue='low')
def low_task() -> Result[str, TaskError]:
    import time

    time.sleep(0.05)  # Small delay to ensure deterministic ordering in priority tests
    return Ok('low')


@app.task(name='e2e_custom_slow', queue='high')
def slow_task(*, duration_ms: int) -> Result[str, TaskError]:
    """Task that sleeps for specified duration (for custom queue tests)."""
    import time

    time.sleep(duration_ms / 1000)
    return Ok(f'slept_{duration_ms}')
