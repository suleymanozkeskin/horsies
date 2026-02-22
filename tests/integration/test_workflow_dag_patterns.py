"""Integration tests for DAG execution patterns."""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import TaskNode
from horsies.core.worker.worker import CLAIM_SQL
from horsies.core.workflows.engine import on_workflow_task_complete

from .conftest import make_simple_task, make_workflow_spec, start_ok


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestDAGPatterns:
    """Tests for various DAG topologies executing correctly."""

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
        result: TaskResult[int, TaskError],
    ) -> None:
        """Helper to simulate task completion."""
        # Get task_id for the workflow_task
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
        """Get status of a workflow task."""
        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = result.fetchone()
        return row[0] if row else 'NOT_FOUND'

    async def _get_task_id(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> str | None:
        """Get task_id for a workflow task."""
        result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = result.fetchone()
        return row[0] if row else None

    async def _get_workflow_status(
        self,
        session: AsyncSession,
        workflow_id: str,
    ) -> str:
        """Get status of a workflow."""
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': workflow_id},
        )
        row = result.fetchone()
        assert row is not None, f'No workflow row for workflow_id={workflow_id}'
        return row[0]

    async def test_linear_chain(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> B -> C executes in order."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'linear_a')
        task_b = make_simple_task(app, 'linear_b')
        task_c = make_simple_task(app, 'linear_c')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_b])

        spec = make_workflow_spec(
            broker=broker, name='linear', tasks=[node_a, node_b, node_c]
        )

        handle = await start_ok(spec, broker)

        # A is ENQUEUED, B and C are PENDING
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'PENDING'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'PENDING'

        # Complete A -> B should become ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert (
            await self._get_task_status(session, handle.workflow_id, 0) == 'COMPLETED'
        )
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'PENDING'

        # Complete B -> C should become ENQUEUED
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert (
            await self._get_task_status(session, handle.workflow_id, 1) == 'COMPLETED'
        )
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # Complete C -> workflow should be COMPLETED
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert (
            await self._get_task_status(session, handle.workflow_id, 2) == 'COMPLETED'
        )
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_fan_out(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> [B, C, D] - all children run after A."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'fan_out_a')
        task_b = make_simple_task(app, 'fan_out_b')
        task_c = make_simple_task(app, 'fan_out_c')
        task_d = make_simple_task(app, 'fan_out_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='fan_out', tasks=[node_a, node_b, node_c, node_d]
        )

        handle = await start_ok(spec, broker)

        # A is ENQUEUED, B, C, D are PENDING
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'PENDING'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'PENDING'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # Complete A -> B, C, D should all become ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # Complete all children -> workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_ready_siblings_priority_order(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """READY siblings are claimed in priority order (lower numeric first)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'prio_a')
        task_b = make_simple_task(app, 'prio_b')
        task_c = make_simple_task(app, 'prio_c')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a], priority=10)
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a], priority=50)

        spec = make_workflow_spec(
            broker=broker, name='prio_ready', tasks=[node_a, node_b, node_c]
        )

        handle = await start_ok(spec, broker)

        # Mark root task as completed in tasks table so claim SQL won't pick it
        root_task_id = await self._get_task_id(session, handle.workflow_id, 0)
        assert root_task_id is not None
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'COMPLETED', completed_at = NOW(), updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': root_task_id},
        )
        await session.commit()

        # Complete A -> B and C become ENQUEUED (tasks table has PENDING rows)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        task_b_id = await self._get_task_id(session, handle.workflow_id, 1)
        task_c_id = await self._get_task_id(session, handle.workflow_id, 2)
        assert task_b_id is not None
        assert task_c_id is not None

        # Claim one task from the default queue; lower priority should be claimed first
        claim_result = await session.execute(
            CLAIM_SQL,
            {
                'queue': 'default',
                'lim': 1,
                'worker_id': 'test-worker',
                'claim_expires_at': None,
            },
        )
        claimed_row = claim_result.fetchone()
        assert claimed_row is not None
        claimed_task_id = claimed_row[0]

        assert claimed_task_id == task_b_id

    async def test_fan_in(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """[A, B, C] -> D - D runs after all parents."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'fan_in_a')
        task_b = make_simple_task(app, 'fan_in_b')
        task_c = make_simple_task(app, 'fan_in_c')
        task_d = make_simple_task(app, 'fan_in_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2})
        node_c = TaskNode(fn=task_c, kwargs={'value': 3})
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_a, node_b, node_c])

        spec = make_workflow_spec(
            broker=broker, name='fan_in', tasks=[node_a, node_b, node_c, node_d]
        )

        handle = await start_ok(spec, broker)

        # A, B, C are ENQUEUED (roots), D is PENDING
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # Complete A -> D still PENDING
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # Complete B -> D still PENDING
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # Complete C -> D should become ENQUEUED
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # Complete D -> workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_diamond(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> [B, C] -> D - classic diamond pattern."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'diamond_a')
        task_b = make_simple_task(app, 'diamond_b')
        task_c = make_simple_task(app, 'diamond_c')
        task_d = make_simple_task(app, 'diamond_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_b, node_c])

        spec = make_workflow_spec(
            broker=broker, name='diamond', tasks=[node_a, node_b, node_c, node_d]
        )

        handle = await start_ok(spec, broker)

        # Complete A -> B, C become ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # Complete B -> D still PENDING
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # Complete C -> D becomes ENQUEUED
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # Complete D -> workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_deep_linear(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> B -> C -> D -> E - deep chain."""
        session, broker, app = setup
        tasks = [make_simple_task(app, f'deep_{i}') for i in range(5)]

        nodes = [TaskNode(fn=tasks[0], kwargs={'value': 0})]
        for i in range(1, 5):
            nodes.append(TaskNode(fn=tasks[i], kwargs={'value': i}, waits_for=[nodes[i - 1]]))

        spec = make_workflow_spec(broker=broker, name='deep', tasks=nodes)

        handle = await start_ok(spec, broker)

        # Complete each in sequence
        for i in range(5):
            assert (
                await self._get_task_status(session, handle.workflow_id, i)
                == 'ENQUEUED'
            )
            await self._complete_task(
                session, handle.workflow_id, i, TaskResult(ok=i * 10)
            )
            assert (
                await self._get_task_status(session, handle.workflow_id, i)
                == 'COMPLETED'
            )

        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_multiple_roots(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """[A, B] -> C - multiple starting points."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'multi_root_a')
        task_b = make_simple_task(app, 'multi_root_b')
        task_c = make_simple_task(app, 'multi_root_c')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2})
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a, node_b])

        spec = make_workflow_spec(
            broker=broker, name='multi_root', tasks=[node_a, node_b, node_c]
        )

        handle = await start_ok(spec, broker)

        # Both roots ENQUEUED
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'PENDING'

        # Complete both roots -> C becomes ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # Complete C -> workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_multiple_terminals(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> [B, C] with B, C as outputs (no explicit output)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'multi_term_a')
        task_b = make_simple_task(app, 'multi_term_b')
        task_c = make_simple_task(app, 'multi_term_c')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])

        # No explicit output - both B and C are terminals
        spec = make_workflow_spec(
            broker=broker, name='multi_terminal', tasks=[node_a, node_b, node_c]
        )

        handle = await start_ok(spec, broker)

        # Complete A
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # Complete B and C
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_mixed_pattern(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Complex DAG: A -> [B, C], B -> D, [C, D] -> E."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'mixed_a')
        task_b = make_simple_task(app, 'mixed_b')
        task_c = make_simple_task(app, 'mixed_c')
        task_d = make_simple_task(app, 'mixed_d')
        task_e = make_simple_task(app, 'mixed_e')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_b])
        node_e = TaskNode(fn=task_e, kwargs={'value': 5}, waits_for=[node_c, node_d])

        spec = make_workflow_spec(
            broker=broker, name='mixed', tasks=[node_a, node_b, node_c, node_d, node_e]
        )

        handle = await start_ok(spec, broker)

        # A -> B, C
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # B -> D
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # E needs both C and D
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'PENDING'

        # C completes, E still needs D
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'PENDING'

        # D completes, E now ready
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'ENQUEUED'

        # Complete E -> workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 4, TaskResult(ok=50))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Boundary cases
    # -----------------------------------------------------------------

    async def test_single_task_workflow(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Single root node with no dependencies completes the workflow."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'single_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(broker=broker, name='single', tasks=[node_a])

        handle = await start_ok(spec, broker)

        assert await self._get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'

        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'COMPLETED'
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_wide_fan_out_fan_in(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> [B0..B9] -> C - wide parallelism with 10 middle tasks."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'wide_root')
        middle_tasks = [make_simple_task(app, f'wide_mid_{i}') for i in range(10)]
        task_c = make_simple_task(app, 'wide_sink')

        node_a = TaskNode(fn=task_a, kwargs={'value': 0})
        middle_nodes = [
            TaskNode(fn=middle_tasks[i], kwargs={'value': i + 1}, waits_for=[node_a])
            for i in range(10)
        ]
        node_c = TaskNode(fn=task_c, kwargs={'value': 99}, waits_for=middle_nodes)

        spec = make_workflow_spec(
            broker=broker,
            name='wide_fan',
            tasks=[node_a, *middle_nodes, node_c],
        )

        handle = await start_ok(spec, broker)

        # Root ENQUEUED, all middle PENDING, sink PENDING
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'
        for i in range(1, 11):
            assert await self._get_task_status(session, handle.workflow_id, i) == 'PENDING'
        assert await self._get_task_status(session, handle.workflow_id, 11) == 'PENDING'

        # Complete root -> all 10 middle tasks ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=0))
        for i in range(1, 11):
            assert await self._get_task_status(session, handle.workflow_id, i) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 11) == 'PENDING'

        # Complete all middle tasks -> sink ENQUEUED
        for i in range(1, 11):
            await self._complete_task(
                session, handle.workflow_id, i, TaskResult(ok=i * 10),
            )
        assert await self._get_task_status(session, handle.workflow_id, 11) == 'ENQUEUED'

        # Complete sink -> workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 11, TaskResult(ok=999))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    async def test_fan_in_reverse_completion_order(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """[A, B, C] -> D - completing parents in reverse order still unlocks D."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'rev_a')
        task_b = make_simple_task(app, 'rev_b')
        task_c = make_simple_task(app, 'rev_c')
        task_d = make_simple_task(app, 'rev_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2})
        node_c = TaskNode(fn=task_c, kwargs={'value': 3})
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_a, node_b, node_c])

        spec = make_workflow_spec(
            broker=broker, name='rev_fan_in', tasks=[node_a, node_b, node_c, node_d],
        )

        handle = await start_ok(spec, broker)

        # Complete in reverse index order: C, B, A
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'ENQUEUED'

        # Complete D -> workflow COMPLETED
        await self._complete_task(session, handle.workflow_id, 3, TaskResult(ok=40))
        assert await self._get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

    # -----------------------------------------------------------------
    # Failure propagation through topologies
    # -----------------------------------------------------------------

    async def test_diamond_one_arm_fails(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> [B, C] -> D; B fails -> D SKIPPED after C completes, workflow FAILED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'dfail_a')
        task_b = make_simple_task(app, 'dfail_b')
        task_c = make_simple_task(app, 'dfail_c')
        task_d = make_simple_task(app, 'dfail_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_b, node_c])

        spec = make_workflow_spec(
            broker=broker, name='diamond_fail', tasks=[node_a, node_b, node_c, node_d],
        )

        handle = await start_ok(spec, broker)

        # Complete A -> B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # B fails -> D stays PENDING (still waiting for C to become terminal)
        fail_result: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='DELIBERATE_FAIL', message='B failed'),
        )
        await self._complete_task(session, handle.workflow_id, 1, fail_result)
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'FAILED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'PENDING'

        # C completes -> all deps terminal, D SKIPPED (allow_failed_deps=False)
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'SKIPPED'

        assert await self._get_workflow_status(session, handle.workflow_id) == 'FAILED'

    async def test_fan_out_root_fails(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> [B, C, D]; A fails -> all children SKIPPED, workflow FAILED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'rfail_a')
        task_b = make_simple_task(app, 'rfail_b')
        task_c = make_simple_task(app, 'rfail_c')
        task_d = make_simple_task(app, 'rfail_d')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='root_fail', tasks=[node_a, node_b, node_c, node_d],
        )

        handle = await start_ok(spec, broker)

        # A fails -> B, C, D should all become SKIPPED
        fail_result: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='DELIBERATE_FAIL', message='Root failed'),
        )
        await self._complete_task(session, handle.workflow_id, 0, fail_result)
        assert await self._get_task_status(session, handle.workflow_id, 0) == 'FAILED'
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'SKIPPED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'SKIPPED'
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'SKIPPED'

        assert await self._get_workflow_status(session, handle.workflow_id) == 'FAILED'

    async def test_mixed_pattern_mid_node_fails(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A -> [B, C], B -> D, [C, D] -> E; B fails -> D SKIPPED, E SKIPPED, workflow FAILED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'mfail_a')
        task_b = make_simple_task(app, 'mfail_b')
        task_c = make_simple_task(app, 'mfail_c')
        task_d = make_simple_task(app, 'mfail_d')
        task_e = make_simple_task(app, 'mfail_e')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])
        node_d = TaskNode(fn=task_d, kwargs={'value': 4}, waits_for=[node_b])
        node_e = TaskNode(fn=task_e, kwargs={'value': 5}, waits_for=[node_c, node_d])

        spec = make_workflow_spec(
            broker=broker,
            name='mid_fail',
            tasks=[node_a, node_b, node_c, node_d, node_e],
        )

        handle = await start_ok(spec, broker)

        # Complete A -> B, C ENQUEUED
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await self._get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'
        assert await self._get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # B fails -> D SKIPPED (only dep failed), E still PENDING (waiting for C and D)
        fail_result: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='DELIBERATE_FAIL', message='B failed'),
        )
        await self._complete_task(session, handle.workflow_id, 1, fail_result)
        assert await self._get_task_status(session, handle.workflow_id, 3) == 'SKIPPED'
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'PENDING'

        # C completes -> E's deps are now C(COMPLETED) + D(SKIPPED), E SKIPPED
        await self._complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))
        assert await self._get_task_status(session, handle.workflow_id, 4) == 'SKIPPED'

        assert await self._get_workflow_status(session, handle.workflow_id) == 'FAILED'
