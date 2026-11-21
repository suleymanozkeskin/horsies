'''Workflow definitions for PgBouncer e2e smoke tests.'''

from __future__ import annotations

from horsies.core.models.workflow import TaskNode
from tests.e2e.tasks.instance_pgbouncer import app
from tests.e2e.tasks.pgbouncer_tasks import workflow_step


node_step = TaskNode(fn=workflow_step, kwargs={"value": 41})

spec_smoke = app.workflow(
    name="pgbouncer_e2e_smoke",
    tasks=[node_step],
    output=node_step,
    definition_key="tests.e2e.pgbouncer_smoke.v1",
)
