"""Reproducer for _cascade_resume_to_children missing completion check.

Scenario:
  Parent workflow has a regular task (fails) + SubWorkflowNode (both roots).
  Parent uses on_error=PAUSE. Child also uses on_error=PAUSE with A -> B.

  1. Start parent -> parent_task_a enqueued, child starts (C -> D)
  2. Fail child task C -> child PAUSED
  3. Fail parent task_a -> parent PAUSED
  4. Resume parent -> cascade resume child -> D gets SKIPPED -> all terminal

Expected: child workflow transitions to FAILED.
Bug: child stays RUNNING because _cascade_resume_to_children never calls
     check_workflow_completion for the child.
"""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowDefinition,
    OnError,
)
from horsies.core.types.result import Ok
from horsies.core.workflows.engine import (
    on_workflow_task_complete,
    resume_workflow,
)

from .conftest import make_simple_task, start_ok


# ---------------------------------------------------------------------------
# Child workflow: two tasks C -> D, on_error=PAUSE
# ---------------------------------------------------------------------------


class PausingChildWorkflow(WorkflowDefinition[int]):
    """Child workflow with a dependency chain and PAUSE error policy."""

    name = 'pausing_child_workflow'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='pausing_child_c')
        def child_task_c() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task(task_name='pausing_child_d')
        def child_task_d(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value * 2)

        node_c: TaskNode[int] = TaskNode(fn=child_task_c)
        node_d: TaskNode[int] = TaskNode(fn=child_task_d, kwargs={'value': 1}, waits_for=[node_c])

        return app.workflow(
            name=cls.name,
            tasks=[node_c, node_d],
            output=node_d,
            on_error=OnError.PAUSE,
        )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestCascadeResumeMissingCompletionCheck:
    """Reproduce: child workflow stuck RUNNING after cascade resume."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'),
        )
        await session.commit()
        return session, broker, app

    async def test_cascade_resume_child_all_terminal_completes(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """After cascade resume, child with all terminal tasks should finalize.

        Parent: parent_task_a (fails) + SubWorkflowNode (both roots), on_error=PAUSE.
        Child: C -> D (on_error=PAUSE).

        1. Fail child C -> child PAUSED
        2. Fail parent_task_a -> parent PAUSED
        3. Resume parent -> cascades to child -> D SKIPPED -> child should be FAILED
        """
        session, broker, app = setup

        parent_task_fn = make_simple_task(app, 'cascade_parent_task_a')

        node_a: TaskNode[int] = TaskNode(fn=parent_task_fn, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=PausingChildWorkflow,
        )

        parent_spec = app.workflow(
            name='cascade_resume_parent',
            tasks=[node_a, node_child],
            output=node_child,
            on_error=OnError.PAUSE,
        )

        handle = await start_ok(parent_spec, broker)

        # Get child workflow ID from parent's SubWorkflowNode (task_index=1)
        row_res = await session.execute(
            text("""
                SELECT sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = row_res.fetchone()
        assert row is not None
        child_wf_id = row[0]
        assert isinstance(child_wf_id, str)

        # Get child task C's task_id (task_index=0 in child workflow)
        child_task_res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': child_wf_id},
        )
        child_task_row = child_task_res.fetchone()
        assert child_task_row is not None
        child_task_c_id = child_task_row[0]
        assert isinstance(child_task_c_id, str)

        # 1. Fail child task C -> child should PAUSE
        await on_workflow_task_complete(
            session,
            child_task_c_id,
            TaskResult(err=TaskError(error_code='CHILD_FAIL', message='C failed')),
            broker,
        )
        await session.commit()
        session.expire_all()

        child_status_res = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': child_wf_id},
        )
        child_status = child_status_res.scalar_one()
        assert child_status == 'PAUSED', f'Expected child PAUSED, got {child_status}'

        # 2. Fail parent task_a -> parent should PAUSE
        parent_task_res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        parent_task_row = parent_task_res.fetchone()
        assert parent_task_row is not None
        parent_task_a_id = parent_task_row[0]
        assert isinstance(parent_task_a_id, str)

        await on_workflow_task_complete(
            session,
            parent_task_a_id,
            TaskResult(err=TaskError(error_code='PARENT_FAIL', message='A failed')),
            broker,
        )
        await session.commit()
        session.expire_all()

        parent_status_res = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        parent_status = parent_status_res.scalar_one()
        assert parent_status == 'PAUSED', f'Expected parent PAUSED, got {parent_status}'

        # 3. Resume parent -> should cascade to child
        resume_result = await resume_workflow(broker, handle.workflow_id)
        assert isinstance(resume_result, Ok), f'Expected Ok, got {resume_result}'
        assert resume_result.ok_value is True

        session.expire_all()

        # Child task D should be SKIPPED (dep C failed, allow_failed_deps=False)
        child_d_res = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': child_wf_id},
        )
        child_d_status = child_d_res.scalar_one()
        assert child_d_status == 'SKIPPED', (
            f'Expected child task D SKIPPED, got {child_d_status}'
        )

        # BUG: child workflow should be FAILED (all tasks terminal)
        # but _cascade_resume_to_children doesn't call check_workflow_completion
        child_final_res = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': child_wf_id},
        )
        child_final_status = child_final_res.scalar_one()
        assert child_final_status == 'FAILED', (
            f'Expected child FAILED after cascade resume, got {child_final_status}'
        )
