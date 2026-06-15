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
    on_error: Any = None,
) -> type[WorkflowDefinition[int]]:
    """shape='flat': all roots. shape='chain': single root chain."""
    suffix = uuid.uuid4().hex[:8]

    @app.task(task_name=f'bulk_child_{suffix}')
    def child_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    corrupt_fn: Any = None
    if corrupt_root:
        @app.task(task_name=f'bulk_child_corrupt_{suffix}')
        def corrupt_task(*, value: int) -> TaskResult[int, TaskError]:
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
            kwargs: dict[str, Any] = {}
            if on_error is not None:
                kwargs['on_error'] = on_error
            return app.workflow(
                name=cls.name, tasks=list(nodes), output=nodes[0], **kwargs,
            )

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

    async def test_pause_policy_slow_root_failure_reverts_fast_siblings(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Child on_error=PAUSE + corrupt (demoted) root + good fast root:
        the slow root's failure pauses the child BEFORE the fast roots'
        task rows land, so the fast sibling reverts to READY with no task
        row — a paused child gains no runnable rows. Resume re-enqueues."""
        from horsies.core.models.workflow import OnError

        child_def = _child_def(
            app, 2, shape='flat', corrupt_root=True, on_error=OnError.PAUSE,
        )
        parent_id = await _start_parent(app, broker, child_def)
        child_id = await _child_id(session, parent_id)

        rows = (
            await session.execute(
                text("""
                    SELECT task_index, status, task_id FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf ORDER BY task_index
                """),
                {'wf': child_id},
            )
        ).fetchall()
        statuses = {r.task_index: r.status for r in rows}
        assert statuses[0] == 'FAILED'  # the corrupt root
        assert statuses[1] == 'READY'  # reverted, not ENQUEUED
        assert rows[1].task_id is None  # no runnable task row

        child_status = (
            await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :wf'),
                {'wf': child_id},
            )
        ).scalar()
        assert child_status == 'PAUSED'

        n_tasks = (
            await session.execute(
                text("""
                    SELECT COUNT(*) FROM horsies_tasks t
                    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                    WHERE wt.workflow_id = :wf
                """),
                {'wf': child_id},
            )
        ).scalar()
        assert n_tasks == 0  # zero runnable rows under the paused child

        from horsies.core.workflows.lifecycle import resume_workflow

        resume_r = await resume_workflow(broker, child_id)
        assert not is_err(resume_r), resume_r
        good_status = (
            await session.execute(
                text("""
                    SELECT status FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf AND task_index = 1
                """),
                {'wf': child_id},
            )
        ).scalar()
        assert good_status == 'ENQUEUED'

    async def test_nested_parent_propagation_pause_reverts_child_fast_siblings(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Nested variant of the pause gate: a child workflow's slow root
        can synchronously fail/finalize its own child and queue propagation
        back into the child. Drain that propagation before the child's fast
        root task rows land."""
        from horsies.core.models.workflow import OnError

        suffix = uuid.uuid4().hex[:8]

        @app.task(task_name=f'nested_pause_fast_{suffix}')
        def fast_task(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        @app.task(task_name=f'nested_pause_bad_{suffix}')
        def bad_grandchild_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)

        bad_grandchild_task.task_options_json = '{not-json'  # type: ignore[attr-defined]

        class BadGrandchild(WorkflowDefinition[int]):
            name = f'nested_pause_grandchild_{suffix}'
            definition_key = f'nested-pause-grandchild-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
                node = TaskNode(fn=bad_grandchild_task)
                return app.workflow(name=cls.name, tasks=[node], output=node)

        class PausingChild(WorkflowDefinition[int]):
            name = f'nested_pause_child_{suffix}'
            definition_key = f'nested-pause-child-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
                fast = TaskNode(fn=fast_task, kwargs={'value': 1})
                nested = SubWorkflowNode(workflow_def=BadGrandchild)
                return app.workflow(
                    name=cls.name,
                    tasks=[fast, nested],
                    output=fast,
                    on_error=OnError.PAUSE,
                )

        parent_id = await _start_parent(app, broker, PausingChild)
        child_id = await _child_id(session, parent_id)

        rows = (
            await session.execute(
                text("""
                    SELECT task_index, status, task_id FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf ORDER BY task_index
                """),
                {'wf': child_id},
            )
        ).fetchall()
        statuses = {r.task_index: r.status for r in rows}
        assert statuses[0] == 'READY'
        assert rows[0].task_id is None
        assert statuses[1] == 'FAILED'

        child_status = (
            await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :wf'),
                {'wf': child_id},
            )
        ).scalar()
        assert child_status == 'PAUSED'

        n_tasks = (
            await session.execute(
                text("""
                    SELECT COUNT(*) FROM horsies_tasks t
                    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                    WHERE wt.workflow_id = :wf
                """),
                {'wf': child_id},
            )
        ).scalar()
        assert n_tasks == 0

        from horsies.core.workflows.lifecycle import resume_workflow

        resume_r = await resume_workflow(broker, child_id)
        assert not is_err(resume_r), resume_r
        fast_status = (
            await session.execute(
                text("""
                    SELECT status FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf AND task_index = 0
                """),
                {'wf': child_id},
            )
        ).scalar()
        assert fast_status == 'ENQUEUED'
