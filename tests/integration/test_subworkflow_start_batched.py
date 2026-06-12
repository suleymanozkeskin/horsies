"""Integration tests: bulk subworkflow child start.

Pins the flat-statement child-start shape (one bulk node insert + one bulk
root-task insert instead of 1 INSERT per child node + 3 statements per
child root) and its equivalence: fast child roots land ENQUEUED + linked +
claimable with child-scoped meta, dependents stay PENDING, and corrupt
root task_options demote to the per-node path failing THAT child root.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import uuid
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import SubWorkflowNode, TaskNode, WorkflowDefinition
from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import start_workflow_async

pytestmark = [pytest.mark.integration]


def _child_def(
    app: Horsies, n_children: int, *, shape: str, corrupt_root: bool = False,
) -> type[WorkflowDefinition[int]]:
    """shape='flat': all roots. shape='chain': single root chain."""
    suffix = uuid.uuid4().hex[:8]

    @app.task(task_name=f'bulk_child_{suffix}')
    def child_task(value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    corrupt_fn: Any = None
    if corrupt_root:
        @app.task(task_name=f'bulk_child_corrupt_{suffix}')
        def corrupt_task(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        corrupt_task.task_options_json = '{not-json'  # type: ignore[attr-defined]
        corrupt_fn = corrupt_task

    class BulkChild(WorkflowDefinition[int]):
        name = f'bulk_child_wf_{suffix}'
        definition_key = f'bulk-child-{suffix}'

        @classmethod
        def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
            nodes: list[TaskNode[int]] = []
            for i in range(n_children):
                fn = corrupt_fn if (corrupt_root and i == 0) else child_task
                waits = [nodes[-1]] if (shape == 'chain' and nodes) else []
                nodes.append(TaskNode(fn=fn, kwargs={'value': i}, waits_for=waits))
            return app.workflow(name=cls.name, tasks=list(nodes), output=nodes[0])

    return BulkChild


async def _start_parent(
    app: Horsies, broker: PostgresBroker, child_def: type[WorkflowDefinition[int]],
) -> str:
    sub = SubWorkflowNode(workflow_def=child_def)
    spec = app.workflow(
        f'bulk_parent_{uuid.uuid4().hex[:6]}', [sub],
        definition_key=f'bulk-parent-{uuid.uuid4().hex[:6]}',
    )
    r = await start_workflow_async(spec, broker)
    assert not is_err(r), r
    return r.ok_value.workflow_id


async def _child_id(session: AsyncSession, parent_id: str) -> str:
    cid = (
        await session.execute(
            text('SELECT id FROM horsies_workflows WHERE parent_workflow_id = :p'),
            {'p': parent_id},
        )
    ).scalar()
    assert cid is not None
    return str(cid)


@pytest.mark.asyncio(loop_scope='function')
class TestBulkChildStart:
    async def test_statement_count_flat_in_children_and_shape(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Regression for the O(C + 3R) child start: a 50-child flat child
        ran 206 statements (1 INSERT per node + 3 per root); the bulk path
        issues the same flat count for flat and chain shapes alike."""
        counts: dict[str, int] = {}
        for shape in ('flat', 'chain'):
            counter = {'n': 0}

            def _count(conn: Any, cursor: Any, stmt: Any, params: Any, ctx: Any, many: Any) -> None:
                counter['n'] += 1

            child_def = _child_def(app, 20, shape=shape)
            event.listen(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
            try:
                await _start_parent(app, broker, child_def)
            finally:
                event.remove(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
            counts[shape] = counter['n']

        # Flatness across shapes is the claim (old: flat=86, chain=29).
        assert counts['flat'] == counts['chain'], counts
        assert counts['flat'] <= 9, counts

    async def test_child_rows_equivalent_to_per_node_path(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Fast child roots: ENQUEUED + linked + claimable task rows with
        CHILD-scoped meta; chain dependents stay PENDING."""
        child_def = _child_def(app, 3, shape='chain')
        parent_id = await _start_parent(app, broker, child_def)
        child_id = await _child_id(session, parent_id)

        rows = (
            await session.execute(
                text("""
                    SELECT wt.task_index, wt.status, wt.task_id, wt.started_at,
                           t.status AS task_status, t.kwargs, t.enqueue_sha
                    FROM horsies_workflow_tasks wt
                    LEFT JOIN horsies_tasks t ON t.id = wt.task_id
                    WHERE wt.workflow_id = :wf ORDER BY wt.task_index
                """),
                {'wf': child_id},
            )
        ).fetchall()
        assert len(rows) == 3

        root = rows[0]
        assert root.status == 'ENQUEUED'
        assert root.task_id is not None
        assert root.started_at is not None
        assert root.task_status == 'PENDING'  # claimable
        assert root.enqueue_sha
        # dumps_json uses compact separators — assert the exact wire form.
        assert f'"workflow_id":"{child_id}"' in root.kwargs

        for dep in rows[1:]:
            assert dep.status == 'PENDING'
            assert dep.task_id is None

    async def test_corrupt_root_options_fail_that_child_root_only(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Equivalence with the per-node path: corrupt decorator-attached
        task_options on a child ROOT fail that child root (FAILED inside
        the child workflow), not the parent node — the bulk path demotes
        the root to the per-node enqueue instead of fast-pathing it."""
        child_def = _child_def(app, 2, shape='flat', corrupt_root=True)
        parent_id = await _start_parent(app, broker, child_def)
        child_id = await _child_id(session, parent_id)

        child_rows = (
            await session.execute(
                text("""
                    SELECT task_index, status FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf ORDER BY task_index
                """),
                {'wf': child_id},
            )
        ).fetchall()
        statuses = {r.task_index: r.status for r in child_rows}
        assert statuses[0] == 'FAILED'  # the corrupt root
        assert statuses[1] == 'ENQUEUED'  # the good sibling root

        parent_node = (
            await session.execute(
                text("""
                    SELECT status FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf AND task_index = 0
                """),
                {'wf': parent_id},
            )
        ).scalar()
        # Parent node tracks the child workflow (RUNNING), not the
        # root's failure — same as the per-node path.
        assert parent_node == 'RUNNING'
