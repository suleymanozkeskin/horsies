"""The sentinel task. Imported only by startup validation, never directly."""

from __future__ import annotations

from horsies.core.models.tasks import TaskError, TaskResult

from tests.unit.web_cli_sentinel import app


@app.task(task_name='web_cli_sentinel_task')
def web_cli_sentinel_task(*, value: int) -> TaskResult[int, TaskError]:
    """Echo the value; its registration is the whole point."""
    return TaskResult(ok=value)
