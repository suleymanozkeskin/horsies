"""Integration tests for SubWorkflowNode conditional execution (run_when/skip_when).

Covers skip_when and run_when on SubWorkflowNode — the SubWorkflowNode counterpart
of TestRunWhenSkipWhen in test_workflow_conditions.py (which only covers TaskNode).
"""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import loads_json, task_result_from_json
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowDefinition,
    WorkflowContext,
    OnError,
)
from horsies.core.workflows.engine import on_workflow_task_complete

from .conftest import make_simple_task, start_ok


# A trivial child workflow for the SubWorkflowNode to reference.
class TrivialChild(WorkflowDefinition[int]):
    name = 'trivial_child_for_cond_bug'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='trivial_child_task_cond_bug')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestSubWorkflowRunWhenSkipWhen:
    """SubWorkflowNode condition evaluation (regression suite for get_node fix)."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text(
                'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'
            )
        )
        await session.commit()
        return session, broker, app

    async def _complete_task(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
        result: TaskResult[Any, TaskError],
    ) -> None:
        """Simulate task completion by task_index."""
        res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = res.fetchone()
        assert row and row[0], f'Task at index {task_index} not found in DB'
        await on_workflow_task_complete(session, row[0], result)
        await session.commit()

    async def _get_task_status(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> str:
        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = result.fetchone()
        return row[0] if row else 'NOT_FOUND'

    async def _get_task_result(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> TaskResult[Any, TaskError] | None:
        """Get stored workflow task result payload for a node."""
        result = await session.execute(
            text("""
                SELECT result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = result.fetchone()
        if row is None or row[0] is None:
            return None
        parsed = loads_json(row[0]).unwrap()
        return task_result_from_json(parsed).unwrap()

    async def _get_workflow_status(
        self,
        session: AsyncSession,
        workflow_id: str,
    ) -> str:
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': workflow_id},
        )
        row = result.fetchone()
        return row[0] if row else 'NOT_FOUND'

    # ------------------------------------------------------------------
    # skip_when
    # ------------------------------------------------------------------

    async def test_subworkflow_skip_when_true_should_skip(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Regression: SubWorkflowNode with skip_when=True is SKIPPED.

        Previously get_task_node() returned None for SubWorkflowNode
        (isinstance guard), so conditions were silently ignored.
        """
        session, broker, app = setup

        root_fn = make_simple_task(app, 'root_for_sub_cond_bug')
        node_root: TaskNode[int] = TaskNode(fn=root_fn, kwargs={'value': 1})

        def should_skip(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_root)
            return result.is_ok() and result.unwrap() > 0

        node_sub: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=TrivialChild,
            waits_for=[node_root],
            workflow_ctx_from=[node_root],
            skip_when=should_skip,
        )

        spec = app.workflow(
            name='parent_sub_cond_bug',
            tasks=[node_root, node_sub],
            on_error=OnError.FAIL,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        status_sub = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_sub == 'SKIPPED', (
            f'SubWorkflowNode should be SKIPPED (skip_when: 2 > 0 is True), '
            f'but got status={status_sub!r}.'
        )
        result_sub = await self._get_task_result(session, handle.workflow_id, 1)
        assert result_sub is not None
        assert result_sub.is_err()
        assert result_sub.unwrap_err().error_code == 'WORKFLOW_CONDITION_SKIP_WHEN_TRUE'

    async def test_subworkflow_skip_when_false_should_run(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SubWorkflowNode with skip_when=False proceeds to ENQUEUED."""
        session, broker, app = setup

        root_fn = make_simple_task(app, 'root_for_sub_skip_false')
        node_root: TaskNode[int] = TaskNode(fn=root_fn, kwargs={'value': 1})

        # skip when result > 100 — root completes with ok=2, so False → run.
        def should_skip(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_root)
            return result.is_ok() and result.unwrap() > 100

        node_sub: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=TrivialChild,
            waits_for=[node_root],
            workflow_ctx_from=[node_root],
            skip_when=should_skip,
        )

        spec = app.workflow(
            name='parent_sub_skip_false',
            tasks=[node_root, node_sub],
            on_error=OnError.FAIL,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        status_sub = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_sub == 'ENQUEUED', (
            f'SubWorkflowNode should be ENQUEUED (skip_when: 2 > 100 is False), '
            f'but got status={status_sub!r}.'
        )

    # ------------------------------------------------------------------
    # run_when
    # ------------------------------------------------------------------

    async def test_subworkflow_run_when_false_should_skip(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Regression: SubWorkflowNode with run_when=False is SKIPPED.

        Same root cause as skip_when — get_task_node() missed SubWorkflowNode.
        """
        session, broker, app = setup

        root_fn = make_simple_task(app, 'root_for_sub_run_cond_bug')
        node_root: TaskNode[int] = TaskNode(fn=root_fn, kwargs={'value': 1})

        def should_run(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_root)
            return result.is_ok() and result.unwrap() > 100

        node_sub: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=TrivialChild,
            waits_for=[node_root],
            workflow_ctx_from=[node_root],
            run_when=should_run,
        )

        spec = app.workflow(
            name='parent_sub_run_cond_bug',
            tasks=[node_root, node_sub],
            on_error=OnError.FAIL,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        status_sub = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_sub == 'SKIPPED', (
            f'SubWorkflowNode should be SKIPPED (run_when: 2 > 100 is False), '
            f'but got status={status_sub!r}.'
        )
        result_sub = await self._get_task_result(session, handle.workflow_id, 1)
        assert result_sub is not None
        assert result_sub.is_err()
        assert result_sub.unwrap_err().error_code == 'WORKFLOW_CONDITION_RUN_WHEN_FALSE'

    async def test_subworkflow_run_when_true_should_run(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SubWorkflowNode with run_when=True proceeds to ENQUEUED."""
        session, broker, app = setup

        root_fn = make_simple_task(app, 'root_for_sub_run_true')
        node_root: TaskNode[int] = TaskNode(fn=root_fn, kwargs={'value': 1})

        # run when result < 100 — root completes with ok=2, so True → run.
        def should_run(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_root)
            return result.is_ok() and result.unwrap() < 100

        node_sub: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=TrivialChild,
            waits_for=[node_root],
            workflow_ctx_from=[node_root],
            run_when=should_run,
        )

        spec = app.workflow(
            name='parent_sub_run_true',
            tasks=[node_root, node_sub],
            on_error=OnError.FAIL,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        status_sub = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_sub == 'ENQUEUED', (
            f'SubWorkflowNode should be ENQUEUED (run_when: 2 < 100 is True), '
            f'but got status={status_sub!r}.'
        )

    # ------------------------------------------------------------------
    # Error path
    # ------------------------------------------------------------------

    async def test_subworkflow_condition_error_fails_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SubWorkflowNode condition that raises is FAILED with stored reason."""
        session, broker, app = setup

        root_fn = make_simple_task(app, 'root_for_sub_cond_error')
        node_root: TaskNode[int] = TaskNode(fn=root_fn, kwargs={'value': 1})

        def broken_condition(_ctx: WorkflowContext) -> bool:
            raise ValueError('Deliberate error in SubWorkflowNode condition')

        node_sub: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=TrivialChild,
            waits_for=[node_root],
            workflow_ctx_from=[node_root],
            run_when=broken_condition,
        )

        spec = app.workflow(
            name='parent_sub_cond_error',
            tasks=[node_root, node_sub],
            on_error=OnError.FAIL,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        status_sub = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_sub == 'FAILED', (
            f'SubWorkflowNode should be FAILED (condition raised), '
            f'but got status={status_sub!r}.'
        )
        result_sub = await self._get_task_result(session, handle.workflow_id, 1)
        assert result_sub is not None
        assert result_sub.is_err()
        assert result_sub.unwrap_err().error_code == 'WORKFLOW_CONDITION_EVALUATION_ERROR'

        wf_status = await self._get_workflow_status(session, handle.workflow_id)
        assert wf_status == 'FAILED'
