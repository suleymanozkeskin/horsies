"""Integration tests for composed workflow patterns.

These tests cross two axes:
- **Features**: join modes, args_from, conditions (skip_when), allow_failed_deps, success_policy
- **Topologies**: diamond, fan-out, wide fan-in, mixed DAG

Existing tests cover features on simple topologies and topologies with default
feature settings. This file fills the gap by composing multiple features on
non-trivial DAG shapes.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import loads_json
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import TaskNode, WorkflowContext, SuccessPolicy, SuccessCase, OnError
from horsies.core.workflows.engine import on_workflow_task_complete

from .conftest import (
    make_simple_task,
    make_simple_ctx_task,
    make_failing_task,
    make_recovery_task,
    make_workflow_spec,
    start_ok,
)


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestComposedPatterns:
    """Tests composing multiple workflow features on non-trivial DAG topologies."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        """Clean tables and return fixtures."""
        await session.execute(
            text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE')
        )
        await session.commit()
        return session, broker, app

    # -----------------------------------------------------------------
    # Helpers (hardened: assert instead of silent no-op)
    # -----------------------------------------------------------------

    async def _complete_task(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
        result: TaskResult[Any, TaskError],
    ) -> None:
        """Simulate task completion via on_workflow_task_complete."""
        res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = res.fetchone()
        assert row is not None, (
            f'No workflow_task row for workflow_id={workflow_id}, task_index={task_index}'
        )
        assert row[0] is not None, (
            f'task_id is NULL for workflow_id={workflow_id}, task_index={task_index} '
            f'— task was never enqueued'
        )
        await on_workflow_task_complete(session, row[0], result)
        await session.commit()

    async def _get_task_status(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> str:
        """Get workflow task status by index."""
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
        """Get workflow status by id."""
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': workflow_id},
        )
        row = result.fetchone()
        assert row is not None, f'No workflow row for workflow_id={workflow_id}'
        return row[0]

    async def _get_task_kwargs(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> dict[str, Any]:
        """Get kwargs stored in the tasks table for the workflow task."""
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = wt_result.fetchone()
        assert row is not None and row[0] is not None, (
            f'task_id is NULL for workflow_id={workflow_id}, task_index={task_index}'
        )
        task_result = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :tid'),
            {'tid': row[0]},
        )
        kwargs_row = task_result.fetchone()
        assert kwargs_row is not None and kwargs_row[0] is not None
        return cast(dict[str, Any], loads_json(kwargs_row[0]).unwrap())

    # -----------------------------------------------------------------
    # Test 1: Diamond + join="any" on sink
    # -----------------------------------------------------------------

    async def test_diamond_join_any_on_sink(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C]→D(join='any'): D ENQUEUED when first arm succeeds."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'dja_a')
        task_b = make_simple_task(app, 'dja_b')
        task_c = make_simple_task(app, 'dja_c')
        task_d = make_simple_task(app, 'dja_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(
            fn=task_d,
            kwargs={'value': 4},
            waits_for=[node_b, node_c],
            join='any',
        )

        spec = make_workflow_spec(
            name='diamond_join_any',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # Complete B → D ENQUEUED immediately (join='any' satisfied)
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # C still ENQUEUED (running independently)
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # Complete D → complete C → workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 2: Diamond + join="any", first arm fails
    # -----------------------------------------------------------------

    async def test_diamond_join_any_first_arm_fails(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C]→D(join='any'): B fails → D stays PENDING. C succeeds → D ENQUEUED."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'djaf_a')
        task_b = make_simple_task(app, 'djaf_b')
        task_c = make_simple_task(app, 'djaf_c')
        task_d = make_simple_task(app, 'djaf_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(
            fn=task_d,
            kwargs={'value': 4},
            waits_for=[node_b, node_c],
            join='any',
        )

        spec = make_workflow_spec(
            name='diamond_join_any_fail',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # B fails → D stays PENDING (join='any' waits for at least one success)
        fail_result: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='ARM_FAIL', message='B failed'),
        )
        await self._complete_task(session, handle.workflow_id, 1, fail_result)
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # C succeeds → D ENQUEUED
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # Complete D → workflow FAILED (B failed, no success_policy override)
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'FAILED'

    # -----------------------------------------------------------------
    # Test 3: Diamond + allow_failed_deps + args_from on sink
    # -----------------------------------------------------------------

    async def test_diamond_allow_failed_deps_on_sink(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C]→D(allow_failed_deps, args_from={b_result: B, c_result: C}).

        B fails, C succeeds → D ENQUEUED with both results injected.
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'dafd_a')
        task_b = make_failing_task(app, 'dafd_b')
        task_c = make_simple_task(app, 'dafd_c')

        # Receiver task that accepts two injected results
        @app.task(task_name='dafd_d')
        def diamond_sink(
            b_result: TaskResult[str, TaskError],
            c_result: TaskResult[int, TaskError],
        ) -> TaskResult[str, TaskError]:
            b_status = 'err' if b_result.is_err() else 'ok'
            c_status = 'err' if c_result.is_err() else 'ok'
            return TaskResult(ok=f'b={b_status},c={c_status}')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 5}, waits_for=[node_a])
        node_d = TaskNode(
            fn=diamond_sink,
            waits_for=[node_b, node_c],
            args_from={'b_result': node_b, 'c_result': node_c},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            name='diamond_afd_args',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # B fails
        await self._complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='B_FAILED', message='B failed')),
        )
        # D still PENDING (C not terminal yet)
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # C succeeds → all deps terminal → D ENQUEUED (allow_failed_deps=True)
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=100))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # Verify D's kwargs contain both injected results
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 3)
        b_injected = cast(dict[str, Any], loads_json(kwargs['b_result']['data']).unwrap())
        c_injected = cast(dict[str, Any], loads_json(kwargs['c_result']['data']).unwrap())
        # B failed → err present
        assert 'err' in b_injected
        assert b_injected['err']['error_code'] == 'B_FAILED'
        # C succeeded → ok present
        assert 'ok' in c_injected

        # Complete D → workflow FAILED (B failed, no success_policy override)
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok='done'))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'FAILED'

    # -----------------------------------------------------------------
    # Test 4: Fan-out + success_policy
    # -----------------------------------------------------------------

    async def test_fan_out_with_success_policy(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C,D], success_policy requires only B.

        B completes, C and D fail → workflow COMPLETED.
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'fsp_a')
        task_b = make_simple_task(app, 'fsp_b')
        task_c = make_failing_task(app, 'fsp_c')
        task_d = make_failing_task(app, 'fsp_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, waits_for=[node_a])
        node_d = TaskNode(fn=task_d, waits_for=[node_a])

        policy = SuccessPolicy(cases=[SuccessCase(required=[node_b])])

        spec = make_workflow_spec(
            name='fan_out_success_policy',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
            success_policy=policy,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C, D ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # B completes successfully
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))

        # C and D fail
        await self._complete_task(
            session,
            handle.workflow_id,
            2,
            TaskResult(err=TaskError(error_code='C_FAIL', message='C failed')),
        )
        await self._complete_task(
            session,
            handle.workflow_id,
            3,
            TaskResult(err=TaskError(error_code='D_FAIL', message='D failed')),
        )

        # C and D are FAILED
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'FAILED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'FAILED'

        # Workflow COMPLETED (success_policy requires only B)
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 5: Diamond + skip_when cascades to sink
    # -----------------------------------------------------------------

    async def test_diamond_skip_when_on_one_arm_cascades(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C]→D: C has skip_when(always true), D has allow_failed_deps=False.

        A completes → B ENQUEUED, C SKIPPED → D SKIPPED (dep SKIPPED, not allowed).
        Workflow COMPLETED (no FAILED task).
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'dskip_a')
        task_c_ctx = make_simple_ctx_task(app, 'dskip_c_ctx')
        task_b = make_simple_task(app, 'dskip_b')
        task_d = make_simple_task(app, 'dskip_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})

        def always_skip(_ctx: WorkflowContext) -> bool:
            return True

        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(
            fn=task_c_ctx,
            kwargs={'value': 3},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=always_skip,
        )
        node_d = TaskNode(
            fn=task_d,
            kwargs={'value': 4},
            waits_for=[node_b, node_c],
            allow_failed_deps=False,
        )

        spec = make_workflow_spec(
            name='diamond_skip_cascade',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B ENQUEUED, C SKIPPED (skip_when=True)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'SKIPPED'

        # D still PENDING (B not terminal yet)
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # Complete B → both deps terminal, C is SKIPPED → D SKIPPED (allow_failed_deps=False)
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'SKIPPED'

        # Workflow COMPLETED (no FAILED task — only COMPLETED and SKIPPED)
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 6: Diamond + skip_when + allow_failed_deps on sink
    # -----------------------------------------------------------------

    async def test_diamond_skip_when_with_allow_failed_deps_on_sink(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C]→D: C has skip_when(always true), D has allow_failed_deps=True.

        A completes → B ENQUEUED, C SKIPPED → D ENQUEUED (tolerates skipped dep).
        Complete B, complete D → workflow COMPLETED.
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'dska_a')
        task_c_ctx = make_simple_ctx_task(app, 'dska_c_ctx')
        task_b = make_simple_task(app, 'dska_b')
        task_d = make_simple_task(app, 'dska_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})

        def always_skip(_ctx: WorkflowContext) -> bool:
            return True

        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(
            fn=task_c_ctx,
            kwargs={'value': 3},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=always_skip,
        )
        node_d = TaskNode(
            fn=task_d,
            kwargs={'value': 4},
            waits_for=[node_b, node_c],
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            name='diamond_skip_allow',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B ENQUEUED, C SKIPPED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'SKIPPED'

        # Complete B → both deps terminal → D ENQUEUED (allow_failed_deps=True)
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # Complete D → workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 7: Wide fan-in + quorum join on sink
    # -----------------------------------------------------------------

    async def test_wide_fan_in_quorum_on_sink(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C,D,E]→F(join='quorum', min_success=2).

        B and C complete → F ENQUEUED immediately (quorum met, D and E still PENDING).
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'wfq_a')
        task_b = make_simple_task(app, 'wfq_b')
        task_c = make_simple_task(app, 'wfq_c')
        task_d = make_simple_task(app, 'wfq_d')
        task_e = make_simple_task(app, 'wfq_e')
        task_f = make_simple_task(app, 'wfq_f')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_a])
        node_e = TaskNode(fn=task_e, kwargs={'value': 5}, waits_for=[node_a])
        node_f = TaskNode(
            fn=task_f,
            kwargs={'value': 0},
            waits_for=[node_b, node_c, node_d, node_e],
            join='quorum',
            min_success=2,
        )

        spec = make_workflow_spec(
            name='wide_quorum',
            tasks=[node_a, node_b, node_c, node_d, node_e, node_f],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C, D, E all ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        for idx in range(1, 5):
            assert await self._get_task_status(session, handle.workflow_id, idx) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 5) == 'PENDING'

        # Complete B → F still PENDING (only 1 success, need 2)
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 5) == 'PENDING'

        # Complete C → F ENQUEUED (2 successes, quorum met)
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 5) == 'ENQUEUED'

        # D and E still ENQUEUED (running independently)
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'ENQUEUED'

        # Complete F, then D and E → workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 5, TaskResult(ok=0))
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        await self._complete_task(session, handle.workflow_id, 4, TaskResult(ok=50))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 8: Mixed DAG + recovery branch
    # -----------------------------------------------------------------

    async def test_mixed_dag_failure_with_recovery_branch(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C], B→D(allow_failed_deps, args_from={input_result: B}), [C,D]→E.

        B fails → D runs (recovery). C and D complete → E ENQUEUED.
        Workflow FAILED (B failed, no success_policy to override).
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'mdr_a')
        task_b = make_failing_task(app, 'mdr_b')
        task_c = make_simple_task(app, 'mdr_c')
        task_d = make_recovery_task(app, 'mdr_d')
        task_e = make_simple_task(app, 'mdr_e')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(
            fn=task_d,
            waits_for=[node_b],
            args_from={'input_result': node_b},
            allow_failed_deps=True,
        )
        node_e = TaskNode(fn=task_e, kwargs={'value': 5}, waits_for=[node_c, node_d])

        spec = make_workflow_spec(
            name='mixed_recovery',
            tasks=[node_a, node_b, node_c, node_d, node_e],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # B fails → D ENQUEUED (allow_failed_deps=True)
        await self._complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='B_FAILED', message='B failed')),
        )
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'FAILED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'
        # E still PENDING (waiting for C and D)
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'PENDING'

        # C completes → E still PENDING (D not terminal yet)
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'PENDING'

        # D completes (recovery value 999) → E ENQUEUED
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=999))
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'ENQUEUED'

        # Complete E → workflow FAILED (B failed, no success_policy to override)
        await self._complete_task(session, handle.workflow_id, 4, TaskResult(ok=50))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'FAILED'

    # =================================================================
    # Extreme edge cases: 3+ features stacked, semantic tension
    # =================================================================

    # -----------------------------------------------------------------
    # Test 9: join="any" + args_from on partially-terminal deps
    # -----------------------------------------------------------------

    async def test_join_any_with_args_from_partial_terminal(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C]→D(join='any', args_from={b_result: B, c_result: C}).

        B completes → join='any' satisfied, D ENQUEUED.
        C is still running → c_result kwarg is NOT injected (non-terminal dep
        has no result to inject). D receives only b_result.
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'jap_a')
        task_b = make_simple_task(app, 'jap_b')
        task_c = make_simple_task(app, 'jap_c')

        @app.task(task_name='jap_d')
        def partial_receiver(
            b_result: TaskResult[int, TaskError] | None = None,
            c_result: TaskResult[int, TaskError] | None = None,
        ) -> TaskResult[str, TaskError]:
            return TaskResult(ok=f'b={b_result is not None},c={c_result is not None}')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(
            fn=partial_receiver,
            waits_for=[node_b, node_c],
            args_from={'b_result': node_b, 'c_result': node_c},
            join='any',
        )

        spec = make_workflow_spec(
            name='join_any_args_partial',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # Complete B → D ENQUEUED (join='any' satisfied)
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # C is still ENQUEUED (not terminal)
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # D's kwargs: b_result present, c_result absent (C not terminal)
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 3)
        assert 'b_result' in kwargs, 'b_result should be injected (B is terminal)'
        assert 'c_result' not in kwargs, (
            'c_result should NOT be injected (C is not terminal when join=any fires)'
        )

        # Finish both branches → workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok='done'))
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 10: Quorum + condition-skip shrinking the success pool
    # -----------------------------------------------------------------

    async def test_quorum_with_condition_skip_makes_impossible(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C,D]→E(quorum, min_success=2).

        C has skip_when(always true).
        A completes → B ENQUEUED, C SKIPPED, D ENQUEUED.
        B fails → completed=0, failed=1, skipped=1, remaining=1 (D).
        max_possible = 0 + 1 = 1 < 2 → E SKIPPED (quorum impossible).
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'qcs_a')
        task_b = make_simple_task(app, 'qcs_b')
        task_c_ctx = make_simple_ctx_task(app, 'qcs_c_ctx')
        task_d = make_simple_task(app, 'qcs_d')
        task_e = make_simple_task(app, 'qcs_e')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})

        def always_skip(_ctx: WorkflowContext) -> bool:
            return True

        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(
            fn=task_c_ctx,
            kwargs={'value': 3},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=always_skip,
        )
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_a])
        node_e = TaskNode(
            fn=task_e,
            kwargs={'value': 0},
            waits_for=[node_b, node_c, node_d],
            join='quorum',
            min_success=2,
        )

        spec = make_workflow_spec(
            name='quorum_skip_impossible',
            tasks=[node_a, node_b, node_c, node_d, node_e],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B ENQUEUED, C SKIPPED, D ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'SKIPPED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # E still PENDING (quorum not met, but still possible: max_possible = 0 + 2 = 2)
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'PENDING'

        # B fails → completed=0, failed=1, skipped=1, remaining=1
        # max_possible = 0 + 1 = 1 < min_success=2 → E SKIPPED
        await self._complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='B_FAIL', message='B failed')),
        )
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'SKIPPED'

        # Complete D (still running) → workflow FAILED due to B failure
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'FAILED'

    # -----------------------------------------------------------------
    # Test 11: Recovery branch that itself fails (double failure)
    # -----------------------------------------------------------------

    async def test_recovery_branch_itself_fails(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→B(allow_failed_deps, args_from)→C→D.

        A fails → B runs (recovery) but also fails → C SKIPPED → D SKIPPED.
        Workflow FAILED.
        """
        session, broker, app = setup

        task_a = make_failing_task(app, 'rff_a')
        task_c = make_simple_task(app, 'rff_c')
        task_d = make_simple_task(app, 'rff_d')

        # "Recovery" task that always fails too
        @app.task(task_name='rff_b')
        def failing_recovery(
            input_result: TaskResult[str, TaskError],
        ) -> TaskResult[int, TaskError]:
            return TaskResult(
                err=TaskError(error_code='RECOVERY_ALSO_FAILED', message='Recovery failed too'),
            )

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=failing_recovery,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )
        node_c = TaskNode(fn=task_c, kwargs={'value': 1}, waits_for=[node_b])
        node_d = TaskNode(fn=task_d, kwargs={'value': 2}, waits_for=[node_c])

        spec = make_workflow_spec(
            name='double_failure',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # A fails → B ENQUEUED (allow_failed_deps=True)
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='A_FAIL', message='A failed')),
        )
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'FAILED'
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'

        # B also fails (recovery failed) → C SKIPPED → D SKIPPED
        await self._complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(
                err=TaskError(error_code='RECOVERY_ALSO_FAILED', message='Recovery failed too'),
            ),
        )
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'FAILED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'SKIPPED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'SKIPPED'

        # Workflow FAILED (two tasks failed)
        assert await self._get_workflow_status(session, handle.workflow_id) == 'FAILED'

    # -----------------------------------------------------------------
    # Test 12: success_policy + allow_failed_deps + recovery on diamond
    # -----------------------------------------------------------------

    async def test_success_policy_with_recovery_on_diamond(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C], B→D(allow_failed_deps, args_from), success_policy requires D.

        B fails, C fails → D runs (recovery), D completes.
        Workflow COMPLETED (policy requires only D, D completed).
        Without success_policy, this would be FAILED.
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'sprd_a')
        task_b = make_failing_task(app, 'sprd_b')
        task_c = make_failing_task(app, 'sprd_c')
        task_d = make_recovery_task(app, 'sprd_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, waits_for=[node_a])
        node_d = TaskNode(
            fn=task_d,
            waits_for=[node_b],
            args_from={'input_result': node_b},
            allow_failed_deps=True,
        )

        policy = SuccessPolicy(cases=[SuccessCase(required=[node_d])])

        spec = make_workflow_spec(
            name='success_policy_recovery',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
            success_policy=policy,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # B fails → D ENQUEUED (allow_failed_deps=True)
        await self._complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='B_FAIL', message='B failed')),
        )
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # C also fails
        await self._complete_task(
            session,
            handle.workflow_id,
            2,
            TaskResult(err=TaskError(error_code='C_FAIL', message='C failed')),
        )

        # D completes (recovery value)
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=999))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'COMPLETED'

        # Workflow COMPLETED — success_policy requires only D, and D succeeded
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 13: Both diamond arms condition-SKIPPED, sink allow_failed_deps
    # -----------------------------------------------------------------

    async def test_both_arms_skipped_sink_allow_failed_deps(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C]→D(allow_failed_deps=True, args_from={b_result: B, c_result: C}).

        Both B and C have skip_when(always true).
        A completes → B SKIPPED, C SKIPPED → D ENQUEUED (allow_failed_deps=True).
        D receives UPSTREAM_SKIPPED sentinels for both kwargs.
        Workflow COMPLETED (no FAILED task).
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'bas_a')
        task_b_ctx = make_simple_ctx_task(app, 'bas_b_ctx')
        task_c_ctx = make_simple_ctx_task(app, 'bas_c_ctx')

        @app.task(task_name='bas_d')
        def all_skipped_sink(
            b_result: TaskResult[int, TaskError] | None = None,
            c_result: TaskResult[int, TaskError] | None = None,
        ) -> TaskResult[str, TaskError]:
            return TaskResult(ok='received')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})

        def always_skip(_ctx: WorkflowContext) -> bool:
            return True

        node_b = TaskNode(
            fn=task_b_ctx,
            kwargs={'value': 2},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=always_skip,
        )
        node_c = TaskNode(
            fn=task_c_ctx,
            kwargs={'value': 3},
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            skip_when=always_skip,
        )
        node_d = TaskNode(
            fn=all_skipped_sink,
            waits_for=[node_b, node_c],
            args_from={'b_result': node_b, 'c_result': node_c},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            name='both_arms_skipped',
            tasks=[node_a, node_b, node_c, node_d],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B SKIPPED, C SKIPPED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'SKIPPED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'SKIPPED'

        # D ENQUEUED (allow_failed_deps=True, all deps terminal)
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # Verify D received UPSTREAM_SKIPPED sentinels
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 3)
        b_injected = cast(dict[str, Any], loads_json(kwargs['b_result']['data']).unwrap())
        c_injected = cast(dict[str, Any], loads_json(kwargs['c_result']['data']).unwrap())
        assert 'err' in b_injected
        assert b_injected['err']['error_code'] == 'UPSTREAM_SKIPPED'
        assert 'err' in c_injected
        assert c_injected['err']['error_code'] == 'UPSTREAM_SKIPPED'

        # Complete D → workflow COMPLETED (no FAILED task)
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=42))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 14: join="any" on intermediate node, not sink
    # -----------------------------------------------------------------

    async def test_join_any_on_intermediate_chains_downstream(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A→[B,C]→D(join='any')→E.

        B completes → D ENQUEUED (join='any'). D completes → E ENQUEUED.
        C still running independently. Complete C, E → workflow COMPLETED.
        Verifies early-triggered intermediate node chains correctly to downstream.
        """
        session, broker, app = setup

        task_a = make_simple_task(app, 'jai_a')
        task_b = make_simple_task(app, 'jai_b')
        task_c = make_simple_task(app, 'jai_c')
        task_d = make_simple_task(app, 'jai_d')
        task_e = make_simple_task(app, 'jai_e')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(
            fn=task_d,
            kwargs={'value': 4},
            waits_for=[node_b, node_c],
            join='any',
        )
        node_e = TaskNode(fn=task_e, kwargs={'value': 5}, waits_for=[node_d])

        spec = make_workflow_spec(
            name='join_any_intermediate',
            tasks=[node_a, node_b, node_c, node_d, node_e],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A → B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'PENDING'

        # Complete B → D ENQUEUED (join='any')
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'
        # E still PENDING (D not terminal)
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'PENDING'

        # Complete D → E ENQUEUED (D's downstream chains correctly)
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'ENQUEUED'

        # C still running independently
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # Complete E and C → workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 4, TaskResult(ok=50))
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Test 15: on_error=PAUSE overrides success_policy
    # -----------------------------------------------------------------

    async def test_pause_fires_before_success_policy_evaluation(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """[A,B] independent roots, success_policy requires only B, on_error=PAUSE.

        A fails → workflow PAUSED immediately, even though B could still
        succeed and satisfy the policy. PAUSE takes priority.
        """
        session, broker, app = setup

        task_a = make_failing_task(app, 'psp_a')
        task_b = make_simple_task(app, 'psp_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, kwargs={'value': 1})

        policy = SuccessPolicy(cases=[SuccessCase(required=[node_b])])

        spec = make_workflow_spec(
            name='pause_overrides_policy',
            tasks=[node_a, node_b],
            broker=broker,
            on_error=OnError.PAUSE,
            success_policy=policy,
        )

        handle = await start_ok(spec, broker)

        # Both A and B are ENQUEUED (independent roots)
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'

        # A fails → workflow PAUSED immediately
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='A_FAIL', message='A failed')),
        )
        assert await self._get_workflow_status(session, handle.workflow_id) == 'PAUSED'

        # B can still complete (was already enqueued before PAUSE)
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=42))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'COMPLETED'

        # But workflow stays PAUSED — PAUSE overrides success_policy
        assert await self._get_workflow_status(session, handle.workflow_id) == 'PAUSED'
