'''Task definitions for PgBouncer e2e smoke tests.'''

from __future__ import annotations

from horsies.core.models.tasks import TaskError, TaskResult
from tests.e2e.tasks.instance_pgbouncer import app


@app.task(task_name="pgbouncer_e2e_healthcheck")
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok="ready")


@app.task(task_name="pgbouncer_e2e_double")
def double(*, value: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=value * 2)


@app.task(task_name="pgbouncer_e2e_workflow_step")
def workflow_step(*, value: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=value + 1)
