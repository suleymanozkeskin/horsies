"""The worker's phase 2 of a node with no dependents does not read them.

Phase 1 returns whether a node of the same workflow lists the node in its
dependencies (``node_has_dependents``). The dependencies of a workflow are
written with its nodes, in the transaction that starts the workflow, and no
statement changes them, so the value is a fact of the DAG. The worker's
phase 2 of a node that has no dependents calls
``horsies_workflow_node_finish`` with dependents ``'NONE'``: the function
does not read them.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import is_ok
from horsies.core.workflows.lifecycle import start_workflow_async
from tests.broker_resources_support import current_resources
from tests.integration.conftest import finish_task

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

NODE_FINISH_CALL = 'horsies_workflow_node_finish('
"""The phase-2 function, in the text of the step statement."""


def _make_task(app: Horsies) -> Any:
    @app.task(task_name=f'dependents_{uuid.uuid4().hex[:8]}')
    def fn(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return fn


async def _start(app: Horsies, broker: PostgresBroker, nodes: list[Any]) -> str:
    spec = app.workflow(
        f'dependents_{uuid.uuid4().hex[:6]}',
        nodes,
        definition_key=f'dependents-{uuid.uuid4().hex[:6]}',
    )
    started = await start_workflow_async(spec, broker)
    assert is_ok(started), started
    return str(started.ok_value.workflow_id)


async def test_phase_two_of_a_leaf_does_not_read_dependents(
    clean_workflow_tables: None,  # noqa: ARG001
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """A -> B: phase 2 of A reads its dependents and enqueues B; phase 2 of
    B, a leaf, passes dependents ``'NONE'`` and completes the workflow.

    ``finish_task`` runs phase 2 on the broker's engine, so the statements
    are recorded there."""
    fn = _make_task(app)
    a = TaskNode(fn=fn, kwargs={'value': 0})
    b = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[a])
    workflow_id = await _start(app, broker, [a, b])
    dependents: list[object] = []

    def _record(
        conn: Any, cursor: Any, statement: str, params: Any, context: Any, many: Any
    ) -> None:
        del conn, cursor, context, many
        if NODE_FINISH_CALL in statement:
            dependents.append(params['dependents'])

    sync_engine = current_resources(broker).engine.sync_engine
    event.listen(sync_engine, 'before_cursor_execute', _record)
    try:
        await finish_task(session, broker, workflow_id, 0, TaskResult(ok=0))
        of_a = list(dependents)
        dependents.clear()
        await finish_task(session, broker, workflow_id, 1, TaskResult(ok=1))
        of_b = list(dependents)
    finally:
        event.remove(sync_engine, 'before_cursor_execute', _record)

    assert of_a == ['READ']
    assert of_b == ['NONE']
    status = (
        await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf'),
            {'wf': workflow_id},
        )
    ).scalar_one()
    assert status == 'COMPLETED'
