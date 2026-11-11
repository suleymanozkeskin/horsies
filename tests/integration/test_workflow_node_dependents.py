"""``has_dependents`` of the node write, and the dependents read it saves.

``COMPLETE_WORKFLOW_TASK_SQL`` returns whether a node of the same workflow
lists the node in its dependencies. The dependencies of a workflow are
written with its nodes, in the transaction that starts the workflow, and no
statement changes them, so the value is a fact of the DAG. Phase 2 of a node
that has no dependents does not read its dependents.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import WF_TASK_TERMINAL_VALUES, TaskNode
from horsies.core.types.result import is_ok
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.sql import COMPLETE_WORKFLOW_TASK_SQL
from tests.integration.conftest import complete_task

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

DEPENDENTS_READ = 'SELECT d.task_index, d.dependencies'
"""The start of the dependents read, ``GET_DEPENDENTS_EVAL_SQL``."""


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
    return started.ok_value.workflow_id


async def _start_fan_in(app: Horsies, broker: PostgresBroker) -> str:
    """A (0), B (1) waits for A, C (2) waits for A and B."""
    fn = _make_task(app)
    a = TaskNode(fn=fn, kwargs={'value': 0})
    b = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[a])
    c = TaskNode(fn=fn, kwargs={'value': 2}, waits_for=[a, b])
    return await _start(app, broker, [a, b, c])


async def _start_chain_of_four(app: Horsies, broker: PostgresBroker) -> str:
    """0 -> 1 -> 2 -> 3: node 3 lists index 2."""
    fn = _make_task(app)
    nodes: list[Any] = [TaskNode(fn=fn, kwargs={'value': 0})]
    for value in range(1, 4):
        nodes.append(TaskNode(fn=fn, kwargs={'value': value}, waits_for=[nodes[-1]]))
    return await _start(app, broker, nodes)


async def _has_dependents(
    session: AsyncSession, workflow_id: str, task_index: int
) -> bool:
    """Run the node write of one node and return its ``has_dependents``.

    The node gets a task id when it has none (the write finds a node by
    it). The transaction rolls back, so the workflow does not change.
    """
    task_id = (
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET task_id = COALESCE(task_id, CAST(:new_id AS uuid)),
                    status = 'RUNNING'
                WHERE workflow_id = :wf AND task_index = :idx
                RETURNING task_id
            """),
            {'wf': workflow_id, 'idx': task_index, 'new_id': str(uuid.uuid4())},
        )
    ).scalar_one()
    row = (
        await session.execute(
            COMPLETE_WORKFLOW_TASK_SQL,
            {
                'tid': str(task_id),
                'status': 'COMPLETED',
                'result': '{}',
                'terminal_states': WF_TASK_TERMINAL_VALUES,
                'wake_channel': None,
                'wake_payload': None,
            },
        )
    ).one()
    await session.rollback()
    assert row.cas_won, row
    has_dependents = row.has_dependents
    assert isinstance(has_dependents, bool), row
    return has_dependents


async def test_a_node_that_another_node_waits_for_has_dependents(
    clean_workflow_tables: None,  # noqa: ARG001
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """The head of the DAG and a node that a fan-in waits for."""
    workflow_id = await _start_fan_in(app, broker)

    assert await _has_dependents(session, workflow_id, 0) is True
    assert await _has_dependents(session, workflow_id, 1) is True


async def test_a_leaf_has_no_dependents_when_another_workflow_waits_for_its_index(
    clean_workflow_tables: None,  # noqa: ARG001
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """Only the nodes of the same workflow count: in the other workflow a
    node lists index 2, and node 2 of the fan-in is still a leaf."""
    fan_in = await _start_fan_in(app, broker)
    chain = await _start_chain_of_four(app, broker)

    assert await _has_dependents(session, fan_in, 2) is False
    assert await _has_dependents(session, chain, 2) is True


async def test_a_node_whose_dependent_is_terminal_still_has_dependents(
    clean_workflow_tables: None,  # noqa: ARG001
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """The value is a fact of the DAG, not of the state of the dependents:
    the dependents read decides what a terminal dependent needs."""
    workflow_id = await _start_fan_in(app, broker)
    await session.execute(
        text("""
            UPDATE horsies_workflow_tasks SET status = 'SKIPPED'
            WHERE workflow_id = :wf AND task_index = 2
        """),
        {'wf': workflow_id},
    )
    await session.commit()

    assert await _has_dependents(session, workflow_id, 1) is True


async def test_phase_two_of_a_leaf_sends_no_dependents_read(
    clean_workflow_tables: None,  # noqa: ARG001
    app: Horsies,
    broker: PostgresBroker,
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """A -> B: phase 2 of A reads its dependents and enqueues B; phase 2 of
    B, a leaf, sends no dependents read and completes the workflow.

    ``complete_task`` runs phase 2 on ``session``, so the statements are
    recorded on its engine."""
    fn = _make_task(app)
    a = TaskNode(fn=fn, kwargs={'value': 0})
    b = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[a])
    workflow_id = await _start(app, broker, [a, b])
    sent: list[str] = []

    def _record(
        conn: Any, cursor: Any, statement: str, params: Any, context: Any, many: Any
    ) -> None:
        del conn, cursor, params, context, many
        sent.append(' '.join(statement.split()))

    sync_engine = engine.sync_engine
    event.listen(sync_engine, 'before_cursor_execute', _record)
    try:
        await complete_task(session, broker, workflow_id, 0, TaskResult(ok=0))
        reads_of_a = [s for s in sent if s.startswith(DEPENDENTS_READ)]
        sent.clear()
        await complete_task(session, broker, workflow_id, 1, TaskResult(ok=1))
        reads_of_b = [s for s in sent if s.startswith(DEPENDENTS_READ)]
    finally:
        event.remove(sync_engine, 'before_cursor_execute', _record)

    assert len(reads_of_a) == 1, reads_of_a
    assert reads_of_b == []
    status = (
        await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf'),
            {'wf': workflow_id},
        )
    ).scalar_one()
    assert status == 'COMPLETED'
