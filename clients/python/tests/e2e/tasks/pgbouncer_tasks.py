'''Task definitions for PgBouncer e2e smoke tests.'''

from __future__ import annotations

from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result
from tests.e2e.tasks.instance_pgbouncer import app


@app.task(name="pgbouncer_e2e_healthcheck")
def healthcheck() -> Result[str, TaskError]:
    return Ok("ready")


@app.task(name="pgbouncer_e2e_double")
def double(*, value: int) -> Result[int, TaskError]:
    return Ok(value * 2)


@app.task(name="pgbouncer_e2e_workflow_step")
def workflow_step(*, value: int) -> Result[int, TaskError]:
    return Ok(value + 1)
