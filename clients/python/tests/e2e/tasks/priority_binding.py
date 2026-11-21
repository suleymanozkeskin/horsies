"""Tasks + workflow for the subworkflow priority-binding claim-order e2e.

The child workflow's ``build_with`` returns a direct ``WorkflowSpec(...)`` (the
documented parameterized-subworkflow pattern) so its scrape node reaches the
engine unbound — the exact path the priority-binding fix corrects. The scrape
node and a direct-send task both target the 'serial' queue (priority 30); under
a single-slot worker their claim order proves the bound priority drives intra-
queue ordering, not a hardcoded 100.

Module-scope spec/nodes are required so the worker subprocess resolves indices.
"""

from __future__ import annotations

import time
from typing import Any

from horsies.core.app import Horsies
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)

from tests.e2e.tasks.instance_priority_binding import app


@app.task(task_name='pb_ready', queue_name='ctrl')
def ready_task() -> TaskResult[str, TaskError]:
    """Readiness probe on the control queue (never holds the serial slot)."""
    return TaskResult(ok='ready')


@app.task(task_name='pb_blocker', queue_name='serial')
def blocker_task(*, duration_ms: int) -> TaskResult[str, TaskError]:
    """Occupies the single serial slot so scrape + direct queue behind it."""
    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'blocked_{duration_ms}')


@app.task(task_name='pb_scrape', queue_name='serial')
def scrape_task() -> TaskResult[int, TaskError]:
    """Subworkflow child task on the serial queue (priority 30 when bound)."""
    time.sleep(0.05)
    return TaskResult(ok=1)


@app.task(task_name='pb_direct', queue_name='serial')
def direct_task() -> TaskResult[int, TaskError]:
    """Direct-send task on the serial queue; .send() resolves priority to 30."""
    time.sleep(0.05)
    return TaskResult(ok=2)


class PriorityChildWorkflow(WorkflowDefinition[int]):
    """Child workflow built via a DIRECT WorkflowSpec — the unbound path."""

    name = 'pb_priority_child'
    definition_key = 'tests.e2e.pb.priority_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, **params: Any) -> WorkflowSpec[int]:
        # Direct WorkflowSpec(...) — bypasses app.workflow() binding, exactly
        # like the documented parameterized-subworkflow pattern. The scrape node
        # carries queue=None, priority=None until the engine binds it.
        scrape = TaskNode(fn=scrape_task, node_id='scrape')
        return WorkflowSpec(
            name=cls.name,
            tasks=[scrape],
            output=scrape,
            on_error=OnError.FAIL,
        )


# Module-scope parent spec: a single SubWorkflowNode expanding the child above.
_parent_child_node = SubWorkflowNode(
    workflow_def=PriorityChildWorkflow,
    node_id='pb_parent_child',
)
spec_priority_parent = app.workflow(
    name='pb_priority_parent',
    tasks=[_parent_child_node],
    output=_parent_child_node,
    on_error=OnError.FAIL,
    definition_key='tests.e2e.pb.priority_parent.v1',
)
