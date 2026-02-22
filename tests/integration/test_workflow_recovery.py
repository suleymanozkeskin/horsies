"""Integration tests for workflow recovery logic."""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import loads_json, task_result_from_json
from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowDefinition,
    WorkflowSpec,
    OnError,
)
from horsies.core.workflows.engine import on_workflow_task_complete
from horsies.core.workflows.recovery import recover_stuck_workflows

from .conftest import make_simple_task, make_failing_task, make_workflow_spec, start_ok
from horsies.core.models.workflow import SuccessPolicy, SuccessCase


class RecoveryChildWorkflow(WorkflowDefinition[int]):
    """Minimal child workflow for subworkflow recovery tests."""

    name = 'recovery_child_workflow'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec:
        @app.task(task_name='recovery_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=42)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowRecovery:
    """Tests for recover_stuck_workflows()."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_recover_ready_not_enqueued(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """READY tasks with NULL task_id get enqueued."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_ready_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(broker=broker, name='recover_ready', tasks=[node_a])

        handle = await start_ok(spec, broker)

        # Simulate crash: set task to READY but clear task_id
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', task_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        # Should have recovered 1 task
        assert recovered == 1

        # Task should now have task_id
        result = await session.execute(
            text("""
                SELECT task_id, status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] is not None  # task_id set
        assert row[1] == 'ENQUEUED'

    async def test_recover_completed_not_marked(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """All done, no failures -> COMPLETED with result persisted."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_completed_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_completed', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        # Simulate: task completed but workflow not updated
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 10}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        # Workflow still RUNNING with no result
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, result = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Workflow should be COMPLETED with result
        result = await session.execute(
            text('SELECT status, result FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] == 'COMPLETED'
        assert row[1] is not None  # result persisted

    async def test_recover_failed_not_marked(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """All done, has failures -> FAILED with result and error persisted."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_failed_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker, name='recover_failed', tasks=[node_a], on_error=OnError.FAIL
        )

        handle = await start_ok(spec, broker)

        # Simulate: task failed but workflow not updated (error not set on workflow)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED', result = '{"err": {"error_code": "TEST_ERROR", "message": "Test failure"}}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        # Workflow still RUNNING with no error
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, result = NULL, error = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Workflow should be FAILED with result and error
        result = await session.execute(
            text('SELECT status, result, error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] == 'FAILED'
        assert row[1] is not None  # result persisted
        assert row[2] is not None  # error derived from failed task
        assert 'TEST_ERROR' in row[2]  # error contains task error

    async def test_recover_failed_preserves_existing_error(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery preserves existing error, does not overwrite."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_preserve_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker,
            name='recover_preserve',
            tasks=[node_a],
            on_error=OnError.FAIL,
        )

        handle = await start_ok(spec, broker)

        # Simulate: task failed, workflow error already set (from earlier failure handling)
        existing_error = '{"error_code": "ORIGINAL_ERROR", "message": "Original error"}'
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED', result = '{"err": {"error_code": "TASK_ERROR", "message": "Task failure"}}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        # Workflow still RUNNING but has existing error
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, result = NULL, error = :error
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id, 'error': existing_error},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Workflow should be FAILED with original error preserved
        result = await session.execute(
            text('SELECT status, result, error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] == 'FAILED'
        assert row[1] is not None  # result persisted
        assert row[2] is not None  # error preserved
        assert 'ORIGINAL_ERROR' in row[2]  # original error kept
        assert 'TASK_ERROR' not in row[2]  # task error not used

    async def test_recover_paused_not_touched(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """PAUSED workflows not modified by recovery."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_paused_a')
        task_b = make_simple_task(app, 'recover_paused_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, kwargs={'value': 1}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='recover_paused',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_ok(spec, broker)

        # Set workflow to PAUSED with pending tasks
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'PAUSED'
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        # B stays PENDING
        await session.commit()

        # Run recovery
        await recover_stuck_workflows(session)
        await session.commit()

        # Workflow should still be PAUSED (not touched)
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'PAUSED'

        # B should still be PENDING (not skipped by recovery)
        task_result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert task_result.fetchone()[0] == 'PENDING'

    async def test_recover_idempotent(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Running recovery twice is safe."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_idempotent_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_idempotent', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        # Simulate stuck READY task
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', task_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery twice
        recovered1 = await recover_stuck_workflows(session)
        await session.commit()

        recovered2 = await recover_stuck_workflows(session)
        await session.commit()

        # First run should recover, second should find nothing
        assert recovered1 == 1
        assert recovered2 == 0

        # Task should be ENQUEUED (not double-enqueued)
        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'ENQUEUED'

    async def test_recover_completed_sets_timestamp(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery sets completed_at when finalizing a stuck workflow."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_notify_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(broker=broker, name='recover_notify', tasks=[node_a])

        handle = await start_ok(spec, broker)

        # Simulate: all tasks done, workflow not updated
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 10}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery (this sends NOTIFY internally)
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        # Verify workflow is now completed (NOTIFY would have been sent)
        assert recovered == 1
        result = await session.execute(
            text('SELECT status, completed_at FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] == 'COMPLETED'
        assert row[1] is not None  # completed_at set

    async def test_recover_respects_success_policy(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery uses success_policy to determine COMPLETED vs FAILED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_sp_a')
        task_b = make_failing_task(app, 'recover_sp_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b)

        # Success policy: workflow succeeds if A completes
        policy = SuccessPolicy(cases=[SuccessCase(required=[node_a])])

        spec = make_workflow_spec(
            broker=broker,
            name='recover_sp',
            tasks=[node_a, node_b],
            success_policy=policy,
        )

        handle = await start_ok(spec, broker)

        # Simulate: both tasks terminal, A completed, B failed
        # Workflow stuck in RUNNING
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 2}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED', result = '{"err": {"error_code": "TEST"}}'
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Workflow should be COMPLETED (success case [A] is satisfied)
        # even though B failed
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'COMPLETED'

    async def test_recover_crashed_worker_workflow_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: workflow_task RUNNING but underlying task already FAILED (worker crash)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_crash_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_crash', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        # Simulate worker crash: tasks.status = FAILED, workflow_tasks.status = RUNNING
        # First get the task_id
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Set tasks row to FAILED (as reaper would)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'FAILED',
                    result = '{"err": {"error_code": "WORKER_CRASHED", "message": "Worker died"}}'
                WHERE id = :tid
            """),
            {'tid': task_id},
        )

        # Set workflow_tasks to RUNNING (simulating crash before on_workflow_task_complete)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered == 1

        # workflow_tasks should now be FAILED
        wt_check = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        assert wt_check.fetchone()[0] == 'FAILED'

        # Workflow should be FAILED (single task failed)
        wf_check = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert wf_check.fetchone()[0] == 'FAILED'

    async def test_recover_crashed_worker_dependent_propagation(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: After crash recovery of A (FAILED), dependent B gets SKIPPED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_dep_a')
        task_b = make_simple_task(app, 'recover_dep_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b = TaskNode(fn=task_b, kwargs={'value': 1}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='recover_dep', tasks=[node_a, node_b]
        )

        handle = await start_ok(spec, broker)

        # Get task_id for node_a
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Simulate worker crash on task A
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'FAILED',
                    result = '{"err": {"error_code": "WORKER_CRASHED", "message": "Worker died"}}'
                WHERE id = :tid
            """),
            {'tid': task_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered == 1

        # Task A should be FAILED
        wt_a = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        assert wt_a.fetchone()[0] == 'FAILED'

        # Task B should be SKIPPED (dependency failed, allow_failed_deps=False)
        wt_b = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert wt_b.fetchone()[0] == 'SKIPPED'

    async def test_recover_crashed_worker_idempotent(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Running recovery twice: second run should recover 0 for Case 1.7."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_idem_crash_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_idem_crash', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        # Get task_id
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Simulate crash
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'FAILED',
                    result = '{"err": {"error_code": "WORKER_CRASHED", "message": "Worker died"}}'
                WHERE id = :tid
            """),
            {'tid': task_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # First recovery
        recovered1 = await recover_stuck_workflows(session, broker)
        await session.commit()

        # Second recovery
        recovered2 = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered1 == 1
        assert recovered2 == 0

    async def test_recover_cancelled_task_missing_result(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: CANCELLED task with missing result maps to TASK_CANCELLED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_cancel_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_cancel', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Simulate cancellation with no stored result
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CANCELLED',
                    result = NULL
                WHERE id = :tid
            """),
            {'tid': task_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered == 1

        wt_result = await session.execute(
            text("""
                SELECT status, result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        status, result_json = wt_result.fetchone()
        assert status == 'FAILED'

        task_result = task_result_from_json(loads_json(result_json).unwrap()).unwrap()
        assert task_result.is_err()
        assert task_result.err is not None
        assert task_result.err.error_code == LibraryErrorCode.TASK_CANCELLED

    async def test_recover_completed_task_missing_result(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: COMPLETED task with missing result maps to RESULT_NOT_AVAILABLE."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_missing_result_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_missing_result', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Simulate completed task with no stored result
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'COMPLETED',
                    result = NULL
                WHERE id = :tid
            """),
            {'tid': task_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered == 1

        wt_result = await session.execute(
            text("""
                SELECT status, result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        status, result_json = wt_result.fetchone()
        assert status == 'FAILED'

        task_result = task_result_from_json(loads_json(result_json).unwrap()).unwrap()
        assert task_result.is_err()
        assert task_result.err is not None
        assert task_result.err.error_code == LibraryErrorCode.RESULT_NOT_AVAILABLE

    # ── Case 0: PENDING tasks with all deps terminal ──

    async def test_recover_pending_deps_succeeded(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 0: PENDING task whose deps all COMPLETED gets enqueued."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_pend_ok_a')
        task_b = make_simple_task(app, 'recover_pend_ok_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='recover_pend_ok', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # Simulate: A completed but B stuck at PENDING (race condition)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 2}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # B should now be ENQUEUED with task_id set
        result = await session.execute(
            text("""
                SELECT status, task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'ENQUEUED'
        assert row[1] is not None

    async def test_recover_pending_deps_failed_skip(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 0: PENDING task with failed dep and allow_failed_deps=False gets SKIPPED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_pend_fail_a')
        task_b = make_simple_task(app, 'recover_pend_fail_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, kwargs={'value': 1}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='recover_pend_fail', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # Simulate: A failed but B stuck at PENDING
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = '{"err": {"error_code": "TEST_FAIL", "message": "fail"}}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        # 2 recoveries: Case 0 skips B, then Case 2+3 finalizes workflow (all tasks terminal)
        assert recovered == 2

        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'SKIPPED'

        # Workflow should also be FAILED (cascading finalization)
        wf_result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert wf_result.fetchone()[0] == 'FAILED'

    async def test_recover_pending_deps_failed_allow_continue(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 0: PENDING task with failed dep and allow_failed_deps=True gets ENQUEUED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_pend_allow_a')
        task_b = make_simple_task(app, 'recover_pend_allow_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b, kwargs={'value': 1}, waits_for=[node_a], allow_failed_deps=True,
        )
        spec = make_workflow_spec(
            broker=broker, name='recover_pend_allow', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # Simulate: A failed but B stuck at PENDING
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = '{"err": {"error_code": "TEST_FAIL", "message": "fail"}}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        result = await session.execute(
            text("""
                SELECT status, task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'ENQUEUED'
        assert row[1] is not None

    # ── Case 1.5: READY SubWorkflowNodes not started ──

    async def test_recover_subworkflow_ready_without_broker(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.5: READY subworkflow with no broker resets to PENDING."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_sub_nb_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=RecoveryChildWorkflow,
            waits_for=[node_a],
        )

        spec = app.workflow(
            name='recover_sub_no_broker',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Simulate: A completed, SubWorkflowNode stuck at READY with NULL sub_workflow_id
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 2}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', sub_workflow_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Recovery without broker
        recovered = await recover_stuck_workflows(session, broker=None)
        await session.commit()

        assert recovered == 1

        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'PENDING'

    async def test_recover_subworkflow_ready_with_broker(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.5: READY subworkflow with broker starts the child workflow."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_sub_wb_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=RecoveryChildWorkflow,
            waits_for=[node_a],
        )

        spec = app.workflow(
            name='recover_sub_with_broker',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Simulate: A completed, SubWorkflowNode stuck at READY with NULL sub_workflow_id
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 2}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', sub_workflow_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Recovery with broker — should start the child workflow
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered == 1

        result = await session.execute(
            text("""
                SELECT status, sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] in ('ENQUEUED', 'RUNNING')
        assert row[1] is not None  # sub_workflow_id set

    # ── Case 1.6: Child completed but parent node not updated ──

    async def test_recover_child_completed_parent_not_updated(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.6: Child workflow COMPLETED but parent node still RUNNING."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=RecoveryChildWorkflow,
        )

        spec = app.workflow(
            name='recover_child_done',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Get the child workflow ID
        wt_result = await session.execute(
            text("""
                SELECT sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        child_id = wt_result.fetchone()[0]
        assert child_id is not None

        # Get child's task_id and complete it normally
        child_task_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :child_id AND task_index = 0
            """),
            {'child_id': child_id},
        )
        child_task_id = child_task_result.fetchone()[0]
        assert child_task_id is not None

        await on_workflow_task_complete(
            session, child_task_id, TaskResult(ok=42), broker,
        )
        await session.commit()

        # Now simulate a crash: revert parent node back to RUNNING
        # (as if the on_subworkflow_complete callback was interrupted)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        # Also revert parent workflow to RUNNING
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Recovery should detect the completed child and update the parent node
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered >= 1

        # Parent node should now be COMPLETED
        parent_row = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        assert parent_row.fetchone()[0] == 'COMPLETED'

    # ── Case 1.7: FAILED task with missing result ──

    async def test_recover_failed_task_missing_result(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: FAILED task with NULL result maps to WORKER_CRASHED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_failed_missing_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_failed_missing', tasks=[node_a],
        )

        handle = await start_ok(spec, broker)

        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Simulate: task FAILED with no result stored (worker crashed before writing result)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'FAILED', result = NULL
                WHERE id = :tid
            """),
            {'tid': task_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered == 1

        wt_result = await session.execute(
            text("""
                SELECT status, result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        status, result_json = wt_result.fetchone()
        assert status == 'FAILED'

        task_result = task_result_from_json(loads_json(result_json).unwrap()).unwrap()
        assert task_result.is_err()
        assert task_result.err is not None
        assert task_result.err.error_code == LibraryErrorCode.WORKER_CRASHED

    # ── Case 2+3: Success policy failure branch ──

    async def test_recover_success_policy_not_satisfied(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 2+3: All tasks terminal but no success case met -> FAILED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_sp_fail_a')
        task_b = make_failing_task(app, 'recover_sp_fail_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b)

        # Both A and B are required — B failing means no case is satisfied
        policy = SuccessPolicy(cases=[SuccessCase(required=[node_a, node_b])])

        spec = make_workflow_spec(
            broker=broker,
            name='recover_sp_fail',
            tasks=[node_a, node_b],
            success_policy=policy,
        )

        handle = await start_ok(spec, broker)

        # Simulate: A completed, B failed, workflow stuck RUNNING
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 2}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = '{"err": {"error_code": "TASK_FAIL", "message": "failed"}}'
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, error = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        result = await session.execute(
            text('SELECT status, error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'FAILED'
        assert row[1] is not None
        # Error should be from the first failed required task (TASK_FAIL)
        assert 'TASK_FAIL' in row[1]
