"""Integration tests for conditional branching (run_when/skip_when)."""

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
from horsies.core.models.workflow import TaskNode, WorkflowContext
from horsies.core.task_decorator import TaskFunction
from horsies.core.workflows.engine import on_workflow_task_complete

from .conftest import make_simple_task, make_workflow_spec, start_ok


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestRunWhenSkipWhen:
    """Tests for run_when/skip_when conditional execution."""

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

    async def _complete_task(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
        result: TaskResult[Any, TaskError],
    ) -> None:
        """Helper to simulate task completion."""
        res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = res.fetchone()
        if row and row[0]:
            await on_workflow_task_complete(session, row[0], result)
            await session.commit()

    async def _get_task_status(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> str:
        """Get workflow task status."""
        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = result.fetchone()
        return row[0] if row else 'NOT_FOUND'

    async def _get_workflow_status(
        self,
        session: AsyncSession,
        workflow_id: str,
    ) -> str:
        """Get workflow status."""
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': workflow_id},
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

    def _make_ctx_task(
        self,
        app: Horsies,
        name: str,
    ) -> TaskFunction[[int, WorkflowContext | None], int]:
        @app.task(task_name=name)
        def task_with_ctx(
            value: int,
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            _ = workflow_ctx
            return TaskResult(ok=value * 2)

        return task_with_ctx

    async def test_skip_when_true_skips_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Task is SKIPPED when skip_when returns True."""
        session, broker, app = setup

        # Create tasks
        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        # Condition: skip if result > 5
        def should_skip(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_a)
            return result.is_ok() and result.unwrap() > 5

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=should_skip,
        )

        spec = make_workflow_spec(
            name='test_skip_when',
            tasks=[node_a, node_b],
            broker=broker,
        )

        # Start workflow
        handle = await start_ok(spec, broker)

        # Complete task A (5 * 2 = 10, which is > 5)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # Task B should be SKIPPED (skip_when returned True)
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'
        result_b = await self._get_task_result(session, handle.workflow_id, 1)
        assert result_b is not None
        assert result_b.is_err()
        assert result_b.unwrap_err().error_code == 'WORKFLOW_CONDITION_SKIP_WHEN_TRUE'

    async def test_skip_when_false_runs_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Task runs when skip_when returns False."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx_false')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 2})

        # Condition: skip if result > 10
        def should_skip(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_a)
            return result.is_ok() and result.unwrap() > 10

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 3},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=should_skip,
        )

        spec = make_workflow_spec(
            name='test_skip_when_false',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete task A (2 * 2 = 4, which is <= 10)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=4))

        # Task B should be ENQUEUED (skip_when returned False)
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'ENQUEUED'

    async def test_run_when_false_skips_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Task is SKIPPED when run_when returns False."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx_run_false')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        # Condition: run only if result < 5
        def should_run(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_a)
            return result.is_ok() and result.unwrap() < 5

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            run_when=should_run,
        )

        spec = make_workflow_spec(
            name='test_run_when_false',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete task A (5 * 2 = 10, which is >= 5)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # Task B should be SKIPPED (run_when returned False)
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'
        result_b = await self._get_task_result(session, handle.workflow_id, 1)
        assert result_b is not None
        assert result_b.is_err()
        assert result_b.unwrap_err().error_code == 'WORKFLOW_CONDITION_RUN_WHEN_FALSE'

    async def test_run_when_true_runs_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Task runs when run_when returns True."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx_run_true')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})

        # Condition: run only if result < 5
        def should_run(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_a)
            return result.is_ok() and result.unwrap() < 5

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            run_when=should_run,
        )

        spec = make_workflow_spec(
            name='test_run_when_true',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete task A (1 * 2 = 2, which is < 5)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        # Task B should be ENQUEUED (run_when returned True)
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'ENQUEUED'

    async def test_skip_when_has_priority_over_run_when(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """skip_when has priority: if both are set, skip_when is evaluated first."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx_skip_priority')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        # Both conditions would normally conflict
        def should_skip(_ctx: WorkflowContext) -> bool:
            return True  # Always skip

        def should_run(_ctx: WorkflowContext) -> bool:
            return True  # Would normally run

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=should_skip,
            run_when=should_run,
        )

        spec = make_workflow_spec(
            name='test_skip_priority',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # skip_when has priority, so task should be SKIPPED
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'

    async def test_condition_error_fails_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """If condition evaluation raises an error, task is FAILED with a stored reason."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx_condition_error')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        def broken_condition(_ctx: WorkflowContext) -> bool:
            raise ValueError('Deliberate error in condition')

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            run_when=broken_condition,
        )

        spec = make_workflow_spec(
            name='test_condition_error',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # Condition error -> FAILED
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'FAILED'
        result_b = await self._get_task_result(session, handle.workflow_id, 1)
        assert result_b is not None
        assert result_b.is_err()
        assert result_b.unwrap_err().error_code == 'WORKFLOW_CONDITION_EVALUATION_ERROR'
        wf_status = await self._get_workflow_status(session, handle.workflow_id)
        assert wf_status == 'FAILED'

    async def test_conditions_use_workflow_ctx_from_subset(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Conditions use workflow_ctx_from subset, not all dependencies."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'cond_subset_a')
        task_b = make_simple_task(app, 'cond_subset_b')
        task_c = self._make_ctx_task(app, 'cond_subset_c_ctx')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task_b, kwargs={'value': 2})

        def should_run(ctx: WorkflowContext) -> bool:
            # node_b is not in workflow_ctx_from, so this should raise KeyError
            ctx.result_for(node_b)
            return True

        node_c: TaskNode[int] = TaskNode(
            fn=task_c,
            kwargs={'value': 3},
            waits_for=[node_a, node_b],
            workflow_ctx_from=[node_a],
            run_when=should_run,
        )

        spec = make_workflow_spec(
            name='cond_subset',
            tasks=[node_a, node_b, node_c],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A and B so conditions evaluate
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))

        # Condition raises KeyError -> treated as failure
        status_c = await self._get_task_status(session, handle.workflow_id, 2)
        assert status_c == 'FAILED'
        result_c = await self._get_task_result(session, handle.workflow_id, 2)
        assert result_c is not None
        assert result_c.is_err()
        assert result_c.unwrap_err().error_code == 'WORKFLOW_CONDITION_EVALUATION_ERROR'
        wf_status = await self._get_workflow_status(session, handle.workflow_id)
        assert wf_status == 'FAILED'

    # ------------------------------------------------------------------
    # Error paths & edge cases
    # ------------------------------------------------------------------

    async def test_skip_when_error_fails_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """If skip_when raises an exception, task is FAILED with stored reason."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx_skip_error')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        def broken_skip(_ctx: WorkflowContext) -> bool:
            raise RuntimeError('Deliberate error in skip_when')

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=broken_skip,
        )

        spec = make_workflow_spec(
            name='test_skip_when_error',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # skip_when raised -> FAILED
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'FAILED'
        result_b = await self._get_task_result(session, handle.workflow_id, 1)
        assert result_b is not None
        assert result_b.is_err()
        assert result_b.unwrap_err().error_code == 'WORKFLOW_CONDITION_EVALUATION_ERROR'
        wf_status = await self._get_workflow_status(session, handle.workflow_id)
        assert wf_status == 'FAILED'

    async def test_both_conditions_skip_when_false_run_when_false(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """skip_when=False falls through; run_when=False causes SKIP."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx_both_false')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        def skip_false(_ctx: WorkflowContext) -> bool:
            return False  # Don't skip

        def run_false(_ctx: WorkflowContext) -> bool:
            return False  # Don't run

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=skip_false,
            run_when=run_false,
        )

        spec = make_workflow_spec(
            name='test_both_false',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # skip_when=False → check run_when → run_when=False → SKIPPED
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'

    async def test_both_conditions_skip_when_false_run_when_true(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """skip_when=False falls through; run_when=True proceeds to enqueue."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'task_a')
        task_b = self._make_ctx_task(app, 'task_b_ctx_both_pass')
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        def skip_false(_ctx: WorkflowContext) -> bool:
            return False

        def run_true(_ctx: WorkflowContext) -> bool:
            return True

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=skip_false,
            run_when=run_true,
        )

        spec = make_workflow_spec(
            name='test_both_pass',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # skip_when=False → check run_when → run_when=True → ENQUEUED
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'ENQUEUED'

    # ------------------------------------------------------------------
    # Downstream propagation
    # ------------------------------------------------------------------

    async def test_skipped_task_cascades_to_dependent(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Downstream of a condition-SKIPPED task is also SKIPPED (allow_failed_deps=False)."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'cascade_a')
        task_b = self._make_ctx_task(app, 'cascade_b_ctx')
        task_c = self._make_ctx_task(app, 'cascade_c_ctx')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        def always_skip(_ctx: WorkflowContext) -> bool:
            return True

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=always_skip,
        )

        # C depends on B (default allow_failed_deps=False)
        node_c: TaskNode[int] = TaskNode(
            fn=task_c,
            kwargs={'value': 20},
            waits_for=[node_b],
            workflow_ctx_from=[node_b],
        )

        spec = make_workflow_spec(
            name='test_cascade_skip',
            tasks=[node_a, node_b, node_c],
            broker=broker,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # B is SKIPPED by condition
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'

        # C should also be SKIPPED (upstream skipped, allow_failed_deps=False)
        status_c = await self._get_task_status(session, handle.workflow_id, 2)
        assert status_c == 'SKIPPED'

    async def test_skipped_task_dependent_with_allow_failed_deps(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Downstream of a SKIPPED task proceeds when allow_failed_deps=True."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'allow_skip_a')
        task_b = self._make_ctx_task(app, 'allow_skip_b_ctx')
        task_c = self._make_ctx_task(app, 'allow_skip_c_ctx')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        def always_skip(_ctx: WorkflowContext) -> bool:
            return True

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=always_skip,
        )

        # C depends on B but tolerates failed/skipped deps
        node_c: TaskNode[int] = TaskNode(
            fn=task_c,
            kwargs={'value': 20},
            waits_for=[node_b],
            workflow_ctx_from=[node_b],
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            name='test_allow_skip_dep',
            tasks=[node_a, node_b, node_c],
            broker=broker,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # B is SKIPPED by condition
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'

        # C should be ENQUEUED (allow_failed_deps=True lets it proceed)
        status_c = await self._get_task_status(session, handle.workflow_id, 2)
        assert status_c == 'ENQUEUED'

    # ------------------------------------------------------------------
    # Workflow-level outcome
    # ------------------------------------------------------------------

    async def test_workflow_completes_when_task_skipped_by_condition(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Workflow reaches COMPLETED when a task is skipped by condition and remaining tasks succeed."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'wf_complete_a')
        task_b = self._make_ctx_task(app, 'wf_complete_b_ctx')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        def always_skip(_ctx: WorkflowContext) -> bool:
            return True

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=always_skip,
        )

        spec = make_workflow_spec(
            name='test_wf_complete_skip',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # B is SKIPPED by condition
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'

        # All tasks terminal (A=COMPLETED, B=SKIPPED) → workflow COMPLETED
        wf_status = await self._get_workflow_status(session, handle.workflow_id)
        assert wf_status == 'COMPLETED'

    # ------------------------------------------------------------------
    # Interaction with allow_failed_deps
    # ------------------------------------------------------------------

    async def test_condition_evaluated_after_failed_dep_with_allow(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With allow_failed_deps=True and a failed dep, conditions are still evaluated."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'failed_dep_cond_a')
        task_b = self._make_ctx_task(app, 'failed_dep_cond_b_ctx')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

        # Condition: run only if upstream succeeded
        def run_if_ok(ctx: WorkflowContext) -> bool:
            result = ctx.result_for(node_a)
            return result.is_ok()

        node_b: TaskNode[int] = TaskNode(
            fn=task_b,
            kwargs={'value': 10},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            allow_failed_deps=True,
            run_when=run_if_ok,
        )

        spec = make_workflow_spec(
            name='test_failed_dep_cond',
            tasks=[node_a, node_b],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete task A with failure
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Deliberate')),
        )

        # allow_failed_deps=True lets B reach condition evaluation
        # run_when checks is_ok() → False → SKIPPED
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'

    # ------------------------------------------------------------------
    # Context building fallback
    # ------------------------------------------------------------------

    async def test_condition_without_workflow_ctx_from_uses_all_deps(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Without workflow_ctx_from, condition context is built from all dependencies."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'no_ctx_from_a')
        task_b = make_simple_task(app, 'no_ctx_from_b')
        task_c = self._make_ctx_task(app, 'no_ctx_from_c_ctx')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task_b, kwargs={'value': 2})

        def check_both_deps(ctx: WorkflowContext) -> bool:
            # Without workflow_ctx_from, both deps should be in context
            has_a = ctx.has_result(node_a)
            has_b = ctx.has_result(node_b)
            return has_a and has_b

        # No workflow_ctx_from set — engine falls back to all dependencies
        node_c: TaskNode[int] = TaskNode(
            fn=task_c,
            kwargs={'value': 3},
            waits_for=[node_a, node_b],
            run_when=check_both_deps,
        )

        spec = make_workflow_spec(
            name='test_no_ctx_from',
            tasks=[node_a, node_b, node_c],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))

        # Condition sees both deps via fallback → returns True → ENQUEUED
        status_c = await self._get_task_status(session, handle.workflow_id, 2)
        assert status_c == 'ENQUEUED'
