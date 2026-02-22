"""Integration tests for OR-join and quorum join semantics."""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import TaskNode

from .conftest import (
    complete_task,
    get_task_status,
    make_simple_task,
    make_simple_ctx_task,
    make_failing_task,
    make_workflow_spec,
    start_ok,
)


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestOrJoin:
    """Tests for join='any' (OR-join) semantics."""

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

    async def test_any_join_runs_when_first_dep_succeeds(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With join='any', task runs as soon as one dependency succeeds."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='any',
        )

        spec = make_workflow_spec(
            name='test_any_join',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete just task A
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        # Aggregator should be ENQUEUED (join='any' satisfied by A)
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'ENQUEUED'

    async def test_any_join_skips_when_all_deps_fail(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With join='any', task is SKIPPED if all dependencies fail."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        fail_task = make_failing_task(app, 'fail_task')
        node_a: TaskNode[int] = TaskNode(fn=fail_task)
        node_b: TaskNode[int] = TaskNode(fn=fail_task)

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b],
            join='any',
        )

        spec = make_workflow_spec(
            name='test_any_join_all_fail',
            tasks=[node_a, node_b, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete both tasks with failures
        await complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='A failed')),
        )
        await complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='FAIL', message='B failed')),
        )

        # Aggregator should be SKIPPED (no dep succeeded)
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'SKIPPED'

    async def test_any_join_stays_pending_while_possible(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With join='any', task stays PENDING while success is still possible."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        fail_task = make_failing_task(app, 'fail_task')
        node_a: TaskNode[str] = TaskNode(fn=fail_task)
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 5})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b],
            join='any',
        )

        spec = make_workflow_spec(
            name='test_any_join_pending',
            tasks=[node_a, node_b, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A with failure
        await complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='A failed')),
        )

        # Aggregator should still be PENDING (B might succeed)
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'PENDING'

        # Now complete B with success
        await complete_task(session, handle.workflow_id, 1, TaskResult(ok=10))

        # Aggregator should now be ENQUEUED
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'ENQUEUED'

    async def test_any_join_waits_for_ctx_deps(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """
        join='any' may become true early, but if workflow_ctx_from deps are not terminal,
        the task should not be enqueued yet.
        """
        session, broker, app = setup

        task = make_simple_ctx_task(app, 'ctx_any_task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b],
            join='any',
            workflow_ctx_from=[node_b],
        )

        spec = make_workflow_spec(
            name='test_any_join_ctx_wait',
            tasks=[node_a, node_b, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A only (join condition satisfied)
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        # Aggregator should remain PENDING until ctx deps are terminal
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'PENDING'

        # Complete B (ctx dep)
        await complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))

        # Now aggregator can be ENQUEUED
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'ENQUEUED'

    # -- New boundary / edge-case tests --

    async def test_any_join_single_dep_succeeds(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With join='any' and a single dep, success → ENQUEUED (min deps boundary)."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a],
            join='any',
        )

        spec = make_workflow_spec(
            name='test_any_single_ok',
            tasks=[node_a, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        status_agg = await get_task_status(session, handle.workflow_id, 1)
        assert status_agg == 'ENQUEUED'

    async def test_any_join_single_dep_fails(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With join='any' and a single dep, failure → SKIPPED (min deps boundary)."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        fail_task = make_failing_task(app, 'fail_task')
        node_a: TaskNode[str] = TaskNode(fn=fail_task)

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a],
            join='any',
        )

        spec = make_workflow_spec(
            name='test_any_single_fail',
            tasks=[node_a, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)
        await complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='A failed')),
        )

        status_agg = await get_task_status(session, handle.workflow_id, 1)
        assert status_agg == 'SKIPPED'

    async def test_any_join_cascade_skips_downstream(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A fails → B(any) SKIPPED → C(all) SKIPPED via cascade propagation."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        fail_task = make_failing_task(app, 'fail_task')

        node_a: TaskNode[str] = TaskNode(fn=fail_task)

        node_b: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a],
            join='any',
        )

        # C depends on B with join='all' (default)
        node_c: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_b],
        )

        spec = make_workflow_spec(
            name='test_any_cascade',
            tasks=[node_a, node_b, node_c],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # A fails → B should be SKIPPED → C should be SKIPPED (cascade)
        await complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='A failed')),
        )

        status_b = await get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'

        status_c = await get_task_status(session, handle.workflow_id, 2)
        assert status_c == 'SKIPPED'

    async def test_any_join_idempotent_after_enqueue(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """After aggregator is ENQUEUED, completing another dep does not change its status."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b],
            join='any',
        )

        spec = make_workflow_spec(
            name='test_any_idempotent',
            tasks=[node_a, node_b, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # A succeeds → aggregator ENQUEUED
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'ENQUEUED'

        # B succeeds → aggregator should still be ENQUEUED (guard at line 1457)
        await complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'ENQUEUED'

    async def test_any_join_out_of_order_completion(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Completing the last-index dep first still triggers ENQUEUED (no ordering assumption)."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='any',
        )

        spec = make_workflow_spec(
            name='test_any_ooo',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete last dep (index 2) first
        await complete_task(session, handle.workflow_id, 2, TaskResult(ok=6))

        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'ENQUEUED'

    async def test_any_join_ctx_dep_fails(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx_from dep fails (terminal) → aggregator still proceeds if join is met."""
        session, broker, app = setup

        task = make_simple_ctx_task(app, 'ctx_any_fail')
        fail_task = make_failing_task(app, 'fail_ctx')

        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[str] = TaskNode(fn=fail_task)

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b],
            join='any',
            workflow_ctx_from=[node_b],
        )

        spec = make_workflow_spec(
            name='test_any_ctx_fail',
            tasks=[node_a, node_b, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # A succeeds (join met) and B fails (ctx dep terminal)
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        await complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='FAIL', message='B failed')),
        )

        # Aggregator should be ENQUEUED: join='any' satisfied + ctx dep is terminal (failed)
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'ENQUEUED'

    async def test_any_join_ctx_dep_skipped(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx_from dep is SKIPPED (terminal) → aggregator proceeds if join is met."""
        session, broker, app = setup

        task = make_simple_ctx_task(app, 'ctx_any_skip')
        fail_task = make_failing_task(app, 'fail_skip')

        # B depends on A (which will fail), so B gets SKIPPED via cascade
        node_a: TaskNode[str] = TaskNode(fn=fail_task)
        node_b: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 1},
            waits_for=[node_a],
        )
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})

        # Aggregator: join='any', ctx from B (which will be SKIPPED)
        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_b, node_c],
            join='any',
            workflow_ctx_from=[node_b],
        )

        spec = make_workflow_spec(
            name='test_any_ctx_skip',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # A fails → B gets SKIPPED (cascade), then C succeeds
        await complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='A failed')),
        )
        # B should be SKIPPED now (cascade from A failing, join='all' default)
        status_b = await get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED'

        # C succeeds → join='any' met + ctx dep B is terminal (SKIPPED)
        await complete_task(session, handle.workflow_id, 2, TaskResult(ok=4))

        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'ENQUEUED'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestQuorumJoin:
    """Tests for join='quorum' semantics."""

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

    async def test_quorum_runs_when_threshold_met(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With join='quorum', task runs when min_success deps succeed."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        # Aggregator needs 2 out of 3 to succeed
        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='quorum',
            min_success=2,
        )

        spec = make_workflow_spec(
            name='test_quorum',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A and B
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        await complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))

        # Aggregator should be ENQUEUED (2 >= min_success)
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'ENQUEUED'

    async def test_quorum_skips_when_impossible(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With join='quorum', task is SKIPPED if threshold becomes impossible."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        fail_task = make_failing_task(app, 'fail_task')
        node_a: TaskNode[str] = TaskNode(fn=fail_task)
        node_b: TaskNode[str] = TaskNode(fn=fail_task)
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        # Aggregator needs 2 out of 3 to succeed
        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='quorum',
            min_success=2,
        )

        spec = make_workflow_spec(
            name='test_quorum_impossible',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A and B with failures
        await complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='A failed')),
        )
        await complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='FAIL', message='B failed')),
        )

        # Aggregator should be SKIPPED (max possible = 1, need 2)
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'SKIPPED'

    async def test_quorum_stays_pending_while_possible(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """With join='quorum', task stays PENDING while threshold is still achievable."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        fail_task = make_failing_task(app, 'fail_task')
        node_a: TaskNode[str] = TaskNode(fn=fail_task)
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        # Aggregator needs 2 out of 3 to succeed
        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='quorum',
            min_success=2,
        )

        spec = make_workflow_spec(
            name='test_quorum_pending',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A with failure
        await complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='A failed')),
        )

        # Aggregator should still be PENDING (B and C can still succeed)
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'PENDING'

        # Complete B with success
        await complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))

        # Still PENDING (need one more)
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'PENDING'

        # Complete C with success
        await complete_task(session, handle.workflow_id, 2, TaskResult(ok=6))

        # Now ENQUEUED (2 >= min_success)
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'ENQUEUED'

    async def test_quorum_join_waits_for_ctx_deps(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """
        join='quorum' may be satisfied early, but workflow_ctx_from deps must be terminal
        before enqueueing the task.
        """
        session, broker, app = setup

        task = make_simple_ctx_task(app, 'ctx_quorum_task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b],
            join='quorum',
            min_success=1,
            workflow_ctx_from=[node_b],
        )

        spec = make_workflow_spec(
            name='test_quorum_join_ctx_wait',
            tasks=[node_a, node_b, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A only (quorum satisfied)
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        # Aggregator should remain PENDING until ctx deps are terminal
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'PENDING'

        # Complete B (ctx dep)
        await complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))

        # Now aggregator can be ENQUEUED
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'ENQUEUED'

    # -- New boundary / edge-case tests --

    async def test_quorum_min_success_equals_total_deps(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """min_success=3 with 3 deps — all must succeed (degenerate case)."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='quorum',
            min_success=3,
        )

        spec = make_workflow_spec(
            name='test_quorum_all',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # Complete A and B → still PENDING (need 3)
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        await complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))

        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'PENDING'

        # Complete C → now ENQUEUED (3 >= 3)
        await complete_task(session, handle.workflow_id, 2, TaskResult(ok=6))

        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'ENQUEUED'

    async def test_quorum_min_success_one(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """min_success=1 with 3 deps — first success → ENQUEUED (degenerate to any)."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 2})
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='quorum',
            min_success=1,
        )

        spec = make_workflow_spec(
            name='test_quorum_one',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # First success → ENQUEUED immediately
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'ENQUEUED'

    async def test_quorum_cascade_skips_downstream(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A,B fail → D(quorum, min=2) SKIPPED → E(all) SKIPPED via cascade."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        fail_task = make_failing_task(app, 'fail_task')

        node_a: TaskNode[str] = TaskNode(fn=fail_task)
        node_b: TaskNode[str] = TaskNode(fn=fail_task)
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        node_d: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='quorum',
            min_success=2,
        )

        # E depends on D with join='all' (default)
        node_e: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_d],
        )

        spec = make_workflow_spec(
            name='test_quorum_cascade',
            tasks=[node_a, node_b, node_c, node_d, node_e],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # A and B fail → D SKIPPED (max_possible=1 < min_success=2) → E SKIPPED
        await complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='A failed')),
        )
        await complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='FAIL', message='B failed')),
        )

        status_d = await get_task_status(session, handle.workflow_id, 3)
        assert status_d == 'SKIPPED'

        status_e = await get_task_status(session, handle.workflow_id, 4)
        assert status_e == 'SKIPPED'

    async def test_quorum_ctx_dep_fails(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx_from dep fails (terminal) → aggregator still ENQUEUED if quorum met."""
        session, broker, app = setup

        task = make_simple_ctx_task(app, 'ctx_quorum_fail')
        fail_task = make_failing_task(app, 'fail_ctx_q')

        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[str] = TaskNode(fn=fail_task)

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b],
            join='quorum',
            min_success=1,
            workflow_ctx_from=[node_b],
        )

        spec = make_workflow_spec(
            name='test_quorum_ctx_fail',
            tasks=[node_a, node_b, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # A succeeds (quorum met) and B fails (ctx dep terminal)
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        await complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='FAIL', message='B failed')),
        )

        # Aggregator should be ENQUEUED: quorum satisfied + ctx dep terminal
        status_agg = await get_task_status(session, handle.workflow_id, 2)
        assert status_agg == 'ENQUEUED'

    async def test_quorum_exact_threshold_boundary(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """3 deps, min_success=2: 1 success + 1 fail → PENDING, then success → ENQUEUED."""
        session, broker, app = setup

        task = make_simple_task(app, 'task')
        fail_task = make_failing_task(app, 'fail_task')

        node_a: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 1})
        node_b: TaskNode[str] = TaskNode(fn=fail_task)
        node_c: TaskNode[int] = TaskNode(fn=task, kwargs={'value': 3})

        node_aggregator: TaskNode[int] = TaskNode(
            fn=task,
            kwargs={'value': 0},
            waits_for=[node_a, node_b, node_c],
            join='quorum',
            min_success=2,
        )

        spec = make_workflow_spec(
            name='test_quorum_boundary',
            tasks=[node_a, node_b, node_c, node_aggregator],
            broker=broker,
        )

        handle = await start_ok(spec, broker)

        # A succeeds → completed=1, still PENDING (need 2)
        await complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'PENDING'

        # B fails → completed=1, failed=1, remaining=1, max_possible=2 → still PENDING
        await complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='FAIL', message='B failed')),
        )
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'PENDING'

        # C succeeds → completed=2 >= min_success=2 → ENQUEUED
        await complete_task(session, handle.workflow_id, 2, TaskResult(ok=6))
        status_agg = await get_task_status(session, handle.workflow_id, 3)
        assert status_agg == 'ENQUEUED'
