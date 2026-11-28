"""The sentinel task. Imported only by startup validation, never directly."""

from __future__ import annotations

from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result

from tests.unit.web_cli_sentinel import app


@app.task(name='web_cli_sentinel_task')
def web_cli_sentinel_task(*, value: int) -> Result[int, TaskError]:
    """Echo the value; its registration is the whole point."""
    return Ok(value)
