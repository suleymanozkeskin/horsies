"""Integration tests for error handling policies."""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import TaskNode, WorkflowSpec, OnError
from horsies.core.errors import WorkflowValidationError
from horsies.core.workflows.engine import start_workflow_async, on_workflow_task_complete
from horsies.core.codec.serde import loads_json

from .conftest import (
    make_simple_task,
    make_failing_task,
    make_recovery_task,
    make_args_receiver_task,
    make_workflow_spec,
)
from horsies.core.models.workflow import SuccessPolicy, SuccessCase


# =============================================================================
# Shared helpers (deduplicated from per-class methods)
# =============================================================================

_SetupTuple = tuple[AsyncSession, PostgresBroker, Horsies]


async def _complete_task(
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
    if row and row[0]:
        await on_workflow_task_complete(session, row[0], result)
        await session.commit()


async def _get_workflow_status(
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


async def _get_task_status(
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


async def _get_workflow_error(
    session: AsyncSession,
    workflow_id: str,
) -> dict[str, Any] | None:
    """Get parsed error JSON from workflow row."""
    result = await session.execute(
        text('SELECT error FROM horsies_workflows WHERE id = :wf_id'),
        {'wf_id': workflow_id},
    )
    row = result.fetchone()
    if row and row[0]:
        return loads_json(row[0])  # type: ignore[return-value]
    return None


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestOnErrorFail:
    """Tests for on_error=FAIL behavior."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_fail_all_succeed_completes(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Happy path: no failures under on_error=FAIL results in COMPLETED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'fail_happy_a')
        task_b = make_simple_task(app, 'fail_happy_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='fail_happy',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=4))

        assert await _get_workflow_status(session, handle.workflow_id) == 'COMPLETED'

        # No error stored
        error = await _get_workflow_error(session, handle.workflow_id)
        assert error is None

    async def test_fail_stores_error_keeps_running(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Error stored, status stays RUNNING until DAG resolves (when allow_failed_deps tasks exist)."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'fail_error_a')
        task_b = make_recovery_task(app, 'fail_error_b')

        node_a = TaskNode(fn=task_a)
        # B has allow_failed_deps=True, so it will still run after A fails
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker,
            name='fail_keeps_running',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Workflow should still be RUNNING because B can still run
        status = await _get_workflow_status(session, handle.workflow_id)
        assert status == 'RUNNING'

        # Error should be stored with correct content
        error = await _get_workflow_error(session, handle.workflow_id)
        assert error is not None
        assert error['error_code'] == 'TEST_FAIL'

        # B should be ENQUEUED (can run with failed dep)
        assert await _get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'

    async def test_fail_continues_propagation_injects_error(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Dependency propagation continues and injects error result into downstream kwargs."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'fail_prop_a')
        task_b = make_recovery_task(app, 'fail_prop_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker,
            name='fail_propagation',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='INJECTED_ERR', message='Injected')),
        )

        # B should be ENQUEUED (allow_failed_deps=True)
        assert await _get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'

        # Verify injected kwargs contain the error from A
        wt_result = await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = :wf_id AND task_index = 1'
            ),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        task_result = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :tid'),
            {'tid': task_id},
        )
        kwargs = loads_json(task_result.fetchone()[0])
        injected = loads_json(kwargs['input_result']['data'])
        assert 'err' in injected
        assert injected['err']['error_code'] == 'INJECTED_ERR'

    async def test_fail_allow_failed_deps_true_runs(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Task with allow_failed_deps=True executes after upstream failure."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'afd_true_a')
        task_b = make_recovery_task(app, 'afd_true_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker,
            name='afd_true',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # B is ENQUEUED
        assert await _get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'

    async def test_fail_allow_failed_deps_false_skipped(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Task with allow_failed_deps=False gets SKIPPED when dep fails."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'afd_false_a')
        task_b = make_simple_task(app, 'afd_false_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            args=(1,),
            waits_for=[node_a],
            allow_failed_deps=False,  # Default, explicit for clarity
        )

        spec = make_workflow_spec(
            broker=broker,
            name='afd_false',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # B should be SKIPPED
        assert await _get_task_status(session, handle.workflow_id, 1) == 'SKIPPED'

    async def test_fail_skipped_propagates(
        self,
        setup: _SetupTuple,
    ) -> None:
        """SKIPPED status propagates to dependents."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'skip_prop_a')
        task_b = make_simple_task(app, 'skip_prop_b')
        task_c = make_simple_task(app, 'skip_prop_c')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])
        node_c = TaskNode(fn=task_c, args=(2,), waits_for=[node_b])

        spec = make_workflow_spec(
            broker=broker,
            name='skip_propagation',
            tasks=[node_a, node_b, node_c],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # B should be SKIPPED
        assert await _get_task_status(session, handle.workflow_id, 1) == 'SKIPPED'

        # C should also be SKIPPED (waits for SKIPPED B)
        assert await _get_task_status(session, handle.workflow_id, 2) == 'SKIPPED'

    async def test_fail_final_status_failed(
        self,
        setup: _SetupTuple,
    ) -> None:
        """After DAG resolves, workflow status is FAILED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'final_fail_a')
        task_b = make_simple_task(app, 'final_fail_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='final_failed',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A -> B gets SKIPPED -> DAG is resolved
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Workflow should now be FAILED (all tasks terminal)
        status = await _get_workflow_status(session, handle.workflow_id)
        assert status == 'FAILED'

    async def test_fail_error_stored_as_taskerror(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Error column contains TaskError, not TaskResult."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'taskerror_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker, name='taskerror', tasks=[node_a], on_error=OnError.FAIL
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A with specific error
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(
                err=TaskError(
                    error_code='SPECIFIC_CODE',
                    message='Specific message',
                    data={'key': 'value'},
                )
            ),
        )

        # Check error column
        error_data = await _get_workflow_error(session, handle.workflow_id)
        assert error_data is not None

        # Should be TaskError structure, not TaskResult
        assert error_data['error_code'] == 'SPECIFIC_CODE'
        assert error_data['message'] == 'Specific message'
        # Should NOT have 'ok' or top-level TaskResult structure
        assert 'ok' not in error_data

    async def test_fail_multiple_failures_last_overwrites_error(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Second failure overwrites workflow error (SET_WORKFLOW_ERROR_SQL is unconditional)."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'multi_fail_a')
        task_b = make_failing_task(app, 'multi_fail_b')

        # Two independent roots, both will fail
        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b)

        spec = make_workflow_spec(
            broker=broker,
            name='multi_fail',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A with FIRST_ERROR
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FIRST_ERROR', message='First')),
        )

        # Fail B with SECOND_ERROR
        await _complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='SECOND_ERROR', message='Second')),
        )

        # Workflow is FAILED
        assert await _get_workflow_status(session, handle.workflow_id) == 'FAILED'

        # SET_WORKFLOW_ERROR_SQL does unconditional SET error = :error,
        # but MARK_WORKFLOW_FAILED_SQL uses COALESCE(:error, error).
        # At finalization, _get_workflow_failure_error is called with no success_policy,
        # which returns first failed task's error (ORDER BY task_index ASC).
        # However, COALESCE(:error, error) means: if new error is non-null, use it;
        # if null, keep existing. So the finalization error (first task's) wins.
        error_data = await _get_workflow_error(session, handle.workflow_id)
        assert error_data is not None
        # The finalization recomputes error from first failed task (task_index=0)
        assert error_data['error_code'] == 'FIRST_ERROR'

    async def test_fail_with_allow_failed_deps_output(
        self,
        setup: _SetupTuple,
    ) -> None:
        """A fails, B (allow_failed_deps=True) recovers, output=B returns B's result."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'output_fail_a')
        task_b = make_recovery_task(app, 'output_fail_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker,
            name='output_recovery',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
            output=node_b,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # B runs and recovers
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=999))

        # Workflow should be FAILED (A failed) but have result from B
        assert await _get_workflow_status(session, handle.workflow_id) == 'FAILED'

        # Check result is from B
        result = await session.execute(
            text('SELECT result FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        result_json = result.fetchone()[0]
        result_data = loads_json(result_json)
        assert result_data.get('ok') == 999

    async def test_skip_propagates_through_chain(
        self,
        setup: _SetupTuple,
    ) -> None:
        """A fails, B (allow_failed_deps=False) SKIPPED, C waits for B also SKIPPED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'chain_skip_a')
        task_b = make_simple_task(app, 'chain_skip_b')
        task_c = make_simple_task(app, 'chain_skip_c')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b, args=(1,), waits_for=[node_a], allow_failed_deps=False
        )
        node_c = TaskNode(
            fn=task_c, args=(2,), waits_for=[node_b], allow_failed_deps=False
        )

        spec = make_workflow_spec(
            broker=broker,
            name='chain_skip',
            tasks=[node_a, node_b, node_c],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # B SKIPPED, C also SKIPPED (no deadlock)
        assert await _get_task_status(session, handle.workflow_id, 1) == 'SKIPPED'
        assert await _get_task_status(session, handle.workflow_id, 2) == 'SKIPPED'

        # Workflow resolves to FAILED
        assert await _get_workflow_status(session, handle.workflow_id) == 'FAILED'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestOnErrorPause:
    """Tests for on_error=PAUSE behavior."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_pause_sets_paused_immediately(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Status becomes PAUSED on first failure."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'pause_immed_a')
        task_b = make_simple_task(app, 'pause_immed_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='pause_immediate',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Workflow should be PAUSED immediately
        status = await _get_workflow_status(session, handle.workflow_id)
        assert status == 'PAUSED'

    async def test_pause_stores_error(
        self,
        setup: _SetupTuple,
    ) -> None:
        """PAUSE_WORKFLOW_ON_ERROR_SQL stores error content in the error column."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'pause_error_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker, name='pause_error', tasks=[node_a], on_error=OnError.PAUSE
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(
                err=TaskError(
                    error_code='PAUSE_ERR',
                    message='Paused due to error',
                )
            ),
        )

        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

        # Error column should contain the TaskError content
        error_data = await _get_workflow_error(session, handle.workflow_id)
        assert error_data is not None
        assert error_data['error_code'] == 'PAUSE_ERR'
        assert error_data['message'] == 'Paused due to error'
        # Should be TaskError, not TaskResult
        assert 'ok' not in error_data

    async def test_pause_stops_propagation(
        self,
        setup: _SetupTuple,
    ) -> None:
        """No new tasks enqueued after PAUSE."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'pause_stop_a')
        task_b = make_simple_task(app, 'pause_stop_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            args=(1,),
            waits_for=[node_a],
            allow_failed_deps=True,  # Would run with FAIL policy
        )

        spec = make_workflow_spec(
            broker=broker,
            name='pause_stops',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # B should NOT be enqueued (PAUSE stops propagation)
        status = await _get_task_status(session, handle.workflow_id, 1)
        assert status == 'PENDING'

    async def test_pause_pending_stays_pending(
        self,
        setup: _SetupTuple,
    ) -> None:
        """PENDING tasks remain PENDING after PAUSE."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'pause_pending_a')
        task_b = make_simple_task(app, 'pause_pending_b')
        task_c = make_simple_task(app, 'pause_pending_c')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])
        node_c = TaskNode(fn=task_c, args=(2,), waits_for=[node_b])

        spec = make_workflow_spec(
            broker=broker,
            name='pause_pending',
            tasks=[node_a, node_b, node_c],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # B and C should remain PENDING
        assert await _get_task_status(session, handle.workflow_id, 1) == 'PENDING'
        assert await _get_task_status(session, handle.workflow_id, 2) == 'PENDING'

    async def test_pause_blocks_propagation_from_other_task(
        self,
        setup: _SetupTuple,
    ) -> None:
        """A and B roots, C waits for A. A fails (PAUSE), B completes. C stays PENDING."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'pause_guard_a')
        task_b = make_simple_task(app, 'pause_guard_b')
        task_c = make_simple_task(app, 'pause_guard_c')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,))
        node_c = TaskNode(fn=task_c, args=(2,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='pause_guard',
            tasks=[node_a, node_b, node_c],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # A and B are both ENQUEUED (roots)
        assert await _get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'
        assert await _get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'

        # Fail A -> workflow PAUSED
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )
        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

        # Now B completes (was already enqueued)
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=100))

        # C should still be PENDING (PAUSE guard blocks propagation)
        assert await _get_task_status(session, handle.workflow_id, 2) == 'PENDING'

        # Workflow still PAUSED
        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

    async def test_pause_already_enqueued_may_complete(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Tasks already enqueued can finish (B enqueued before A fails)."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'pause_enqueued_a')
        task_b = make_simple_task(app, 'pause_enqueued_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,))  # Independent root

        spec = make_workflow_spec(
            broker=broker,
            name='pause_enqueued',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Both A and B are ENQUEUED
        assert await _get_task_status(session, handle.workflow_id, 0) == 'ENQUEUED'
        assert await _get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'

        # Fail A -> PAUSED
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )
        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

        # B can still complete (was already enqueued)
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=200))
        assert (
            await _get_task_status(session, handle.workflow_id, 1) == 'COMPLETED'
        )

    async def test_pause_second_failure_does_not_overwrite_error(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Second failure while PAUSED does not overwrite the original error."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'pause_double_a')
        task_b = make_failing_task(app, 'pause_double_b')

        # Two independent roots, both will fail
        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b)

        spec = make_workflow_spec(
            broker=broker,
            name='pause_double',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A with FIRST_ERROR -> workflow PAUSED
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FIRST_ERROR', message='First')),
        )
        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

        # B was already enqueued and also fails
        # PAUSE_WORKFLOW_ON_ERROR_SQL has WHERE status = 'RUNNING', so it's a no-op
        await _complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='SECOND_ERROR', message='Second')),
        )

        # Original error preserved (second SET_WORKFLOW_ERROR_SQL targets RUNNING, not PAUSED)
        error_data = await _get_workflow_error(session, handle.workflow_id)
        assert error_data is not None
        assert error_data['error_code'] == 'FIRST_ERROR'

        # Still PAUSED (not overwritten to something else)
        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

    async def test_pause_guard_blocks_pending_to_ready(
        self,
        setup: _SetupTuple,
    ) -> None:
        """
        _try_make_ready_and_enqueue does not transition PENDING -> READY
        when workflow is PAUSED.
        """
        from horsies.core.workflows.engine import _try_make_ready_and_enqueue

        session, broker, app = setup
        task_a = make_simple_task(app, 'guard_pending_a')
        task_b = make_simple_task(app, 'guard_pending_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='guard_pending',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Manually set workflow to PAUSED
        await session.execute(
            text("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = :wf_id"),
            {'wf_id': handle.workflow_id},
        )

        # Manually mark A as COMPLETED (bypass on_workflow_task_complete which has its own guard)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result, completed_at = NOW()
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id, 'result': '{"ok": 10}'},
        )
        await session.commit()

        # Try to make B ready - the guard should block this
        await _try_make_ready_and_enqueue(session, broker, handle.workflow_id, 1)
        await session.commit()

        # B should still be PENDING (guard blocked PENDING -> READY)
        b_status = await _get_task_status(session, handle.workflow_id, 1)
        assert b_status == 'PENDING', f'Expected PENDING but got {b_status}'

    async def test_pause_guard_blocks_ready_to_enqueued(
        self,
        setup: _SetupTuple,
    ) -> None:
        """
        _enqueue_workflow_task does not transition READY -> ENQUEUED
        when workflow is PAUSED.
        """
        from horsies.core.workflows.engine import _enqueue_workflow_task

        session, broker, app = setup
        task_a = make_simple_task(app, 'guard_ready_a')
        task_b = make_simple_task(app, 'guard_ready_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='guard_ready',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Manually set B to READY (bypassing the normal flow)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY'
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )

        # Set workflow to PAUSED
        await session.execute(
            text("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = :wf_id"),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Verify B is READY
        assert await _get_task_status(session, handle.workflow_id, 1) == 'READY'

        # Try to enqueue B - the guard should block this
        result = await _enqueue_workflow_task(session, handle.workflow_id, 1, {})
        await session.commit()

        # Result should be None (guard blocked the enqueue)
        assert result is None, f'Expected None but got {result}'

        # B should still be READY (not ENQUEUED)
        b_status = await _get_task_status(session, handle.workflow_id, 1)
        assert b_status == 'READY', f'Expected READY but got {b_status}'

        # Verify no task row was created for B
        wt_result = await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = :wf_id AND task_index = 1'
            ),
            {'wf_id': handle.workflow_id},
        )
        wt_row = wt_result.fetchone()
        assert wt_row is not None
        assert wt_row[0] is None, 'task_id should be NULL (not enqueued)'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestResume:
    """Tests for workflow resume functionality."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_resume_unblocks_pending_tasks(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Resume re-evaluates PENDING tasks and makes them READY/ENQUEUED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'resume_pending_a')
        task_b = make_simple_task(app, 'resume_pending_b')
        task_c = make_simple_task(app, 'resume_pending_c')

        # A fails, B succeeds, C waits for B
        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,))
        node_c = TaskNode(fn=task_c, args=(2,), waits_for=[node_b])

        spec = make_workflow_spec(
            broker=broker,
            name='resume_pending',
            tasks=[node_a, node_b, node_c],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A -> PAUSE
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )
        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

        # Complete B while paused
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=100))

        # C should still be PENDING (PAUSE guard blocked it)
        assert await _get_task_status(session, handle.workflow_id, 2) == 'PENDING'

        # Resume workflow
        resumed = await handle.resume_async()
        assert resumed is True

        # Expire session cache to see changes from resume (committed in different session)
        session.expire_all()

        # Workflow should be RUNNING
        assert await _get_workflow_status(session, handle.workflow_id) == 'RUNNING'

        # C should now be ENQUEUED (resume re-evaluated PENDING tasks)
        c_status = await _get_task_status(session, handle.workflow_id, 2)
        assert c_status == 'ENQUEUED', f'Expected ENQUEUED but got {c_status}'

    async def test_resume_enqueues_ready_tasks(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Resume enqueues tasks that were READY at pause time."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'resume_ready_a')
        task_b = make_simple_task(app, 'resume_ready_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='resume_ready',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Manually set workflow to PAUSED
        await session.execute(
            text("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = :wf_id"),
            {'wf_id': handle.workflow_id},
        )
        # Manually set B to READY (simulating it was blocked during enqueue)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY'
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Verify B is READY
        assert await _get_task_status(session, handle.workflow_id, 1) == 'READY'

        # Resume workflow
        resumed = await handle.resume_async()
        assert resumed is True

        # B should now be ENQUEUED
        b_status = await _get_task_status(session, handle.workflow_id, 1)
        assert b_status == 'ENQUEUED', f'Expected ENQUEUED but got {b_status}'

        # Verify task_id was created
        wt_result = await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = :wf_id AND task_index = 1'
            ),
            {'wf_id': handle.workflow_id},
        )
        wt_row = wt_result.fetchone()
        assert wt_row is not None
        assert wt_row[0] is not None, 'task_id should be set after resume'

    async def test_resume_noop_when_not_paused(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Resume returns False and does nothing when workflow is not PAUSED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'resume_noop_a')

        node_a = TaskNode(fn=task_a, args=(1,))

        spec = make_workflow_spec(
            broker=broker,
            name='resume_noop',
            tasks=[node_a],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Workflow is RUNNING
        assert await _get_workflow_status(session, handle.workflow_id) == 'RUNNING'

        # Resume should be no-op (returns False)
        resumed = await handle.resume_async()
        assert resumed is False

        # Status unchanged
        assert await _get_workflow_status(session, handle.workflow_id) == 'RUNNING'

    async def test_resume_noop_when_completed(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Resume returns False when workflow is already COMPLETED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'resume_completed_a')

        node_a = TaskNode(fn=task_a, args=(1,))

        spec = make_workflow_spec(
            broker=broker,
            name='resume_completed',
            tasks=[node_a],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A -> workflow COMPLETED
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert (
            await _get_workflow_status(session, handle.workflow_id) == 'COMPLETED'
        )

        # Resume should be no-op
        resumed = await handle.resume_async()
        assert resumed is False

        # Status unchanged
        assert (
            await _get_workflow_status(session, handle.workflow_id) == 'COMPLETED'
        )

    async def test_resume_noop_when_failed(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Resume returns False when workflow is in terminal FAILED state."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'resume_failed_a')

        node_a = TaskNode(fn=task_a)

        spec = make_workflow_spec(
            broker=broker,
            name='resume_failed',
            tasks=[node_a],
            on_error=OnError.FAIL,  # FAIL policy: goes to FAILED, not PAUSED
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A -> workflow FAILED (single task, on_error=FAIL)
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )
        assert await _get_workflow_status(session, handle.workflow_id) == 'FAILED'

        # Resume should be no-op on terminal FAILED state
        resumed = await handle.resume_async()
        assert resumed is False

        # Status unchanged
        assert await _get_workflow_status(session, handle.workflow_id) == 'FAILED'

    async def test_resume_all_tasks_terminal_completes(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Resume when all tasks are already terminal transitions to final status.

        Single task fails -> PAUSE -> resume -> no more tasks to run -> FAILED.
        Resume sets RUNNING, then re-evaluates; since all tasks are terminal,
        _check_workflow_completion is NOT called by resume itself, but
        the workflow should still transition when evaluated.
        """
        session, broker, app = setup
        task_a = make_failing_task(app, 'resume_terminal_a')
        task_b = make_simple_task(app, 'resume_terminal_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='resume_terminal',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A -> PAUSED
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )
        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

        # Resume -> B is still PENDING, resume evaluates it -> B gets SKIPPED
        resumed = await handle.resume_async()
        assert resumed is True

        session.expire_all()

        # B should be SKIPPED (dep A failed, allow_failed_deps=False)
        assert await _get_task_status(session, handle.workflow_id, 1) == 'SKIPPED'

        # Now all tasks are terminal. Check what state the workflow ended up in.
        # resume_workflow doesn't call _check_workflow_completion directly,
        # but _try_make_ready_and_enqueue -> SKIP cascade -> _process_dependents ->
        # eventually _check_workflow_completion should fire.
        # If it didn't, we may end up stuck in RUNNING - that would be a bug to catch.
        final_status = await _get_workflow_status(session, handle.workflow_id)
        assert final_status == 'FAILED', (
            f'Expected FAILED after resume with all terminal tasks, got {final_status}'
        )

    async def test_resume_then_complete_remains_failed(
        self,
        setup: _SetupTuple,
    ) -> None:
        """After resume, remaining tasks complete but workflow is FAILED due to earlier error.

        A (fails) -> PAUSE -> resume -> B completes -> workflow FAILED.
        Error column still set from A's failure. _evaluate_workflow_success checks
        `not has_error and failed == 0`, so with has_error=True, workflow is FAILED.
        """
        session, broker, app = setup
        task_a = make_failing_task(app, 'resume_complete_a')
        task_b = make_simple_task(app, 'resume_complete_b')

        # A and B are independent roots
        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,))

        spec = make_workflow_spec(
            broker=broker,
            name='resume_complete',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A -> PAUSED
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )
        assert await _get_workflow_status(session, handle.workflow_id) == 'PAUSED'

        # B was already enqueued, complete it while paused
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=42))

        # Resume workflow
        resumed = await handle.resume_async()
        assert resumed is True

        session.expire_all()

        # All tasks terminal now. Workflow should be FAILED because A failed
        # (error column has_error=True, failed=1).
        final_status = await _get_workflow_status(session, handle.workflow_id)
        assert final_status == 'FAILED', (
            f'Expected FAILED after resume with prior error, got {final_status}'
        )

        # Error should still contain A's error
        error_data = await _get_workflow_error(session, handle.workflow_id)
        assert error_data is not None
        assert error_data['error_code'] == 'TEST_FAIL'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestAllowFailedDeps:
    """Tests for allow_failed_deps behavior."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_allow_failed_deps_receives_err_result(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Task receives TaskResult with is_err()=True."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'afd_err_a')
        task_b = make_recovery_task(app, 'afd_err_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker, name='afd_err', tasks=[node_a, node_b], on_error=OnError.FAIL
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='ERR_CODE', message='Error message')),
        )

        # B is enqueued with failed result injected
        assert await _get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'

        # Check the injected kwargs contain error
        wt_result = await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = :wf_id AND task_index = 1'
            ),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        task_result = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :tid'),
            {'tid': task_id},
        )
        kwargs = loads_json(task_result.fetchone()[0])
        injected = loads_json(kwargs['input_result']['data'])
        assert 'err' in injected

    async def test_allow_failed_deps_can_recover(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Recovery task can produce successful result."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'afd_recover_a')
        task_b = make_recovery_task(app, 'afd_recover_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker,
            name='afd_recover',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
            output=node_b,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST', message='Test')),
        )

        # B recovers with success
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=999))

        # Check result
        wf_result = await session.execute(
            text('SELECT result FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        result_data = loads_json(wf_result.fetchone()[0])
        assert result_data.get('ok') == 999

    async def test_allow_failed_deps_chain(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Chain of allow_failed_deps tasks."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'afd_chain_a')
        task_b = make_recovery_task(app, 'afd_chain_b')
        task_c = make_args_receiver_task(app, 'afd_chain_c')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )
        node_c = TaskNode(
            fn=task_c,
            waits_for=[node_b],
            args_from={'input_result': node_b},
        )

        spec = make_workflow_spec(
            broker=broker,
            name='afd_chain',
            tasks=[node_a, node_b, node_c],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST', message='Test')),
        )

        # B recovers
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=100))

        # C should be enqueued (B succeeded)
        assert await _get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

    async def test_allow_failed_deps_partial(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Some deps failed, some succeeded."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'afd_partial_a')
        task_b = make_simple_task(app, 'afd_partial_b')

        @app.task(task_name='partial_receiver')
        def partial_receiver(
            failed_input: TaskResult[Any, TaskError],
            success_input: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            if failed_input.is_err() and success_input.is_ok():
                return TaskResult(ok=success_input.unwrap() + 1000)
            return TaskResult(
                err=TaskError(error_code='UNEXPECTED', message='Unexpected state')
            )

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(50,))
        node_c = TaskNode(
            fn=partial_receiver,
            waits_for=[node_a, node_b],
            args_from={'failed_input': node_a, 'success_input': node_b},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker,
            name='afd_partial',
            tasks=[node_a, node_b, node_c],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # A fails, B succeeds
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='Failed')),
        )
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=100))

        # C should be enqueued (allow_failed_deps=True)
        assert await _get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

    async def test_allow_failed_deps_receives_upstream_skipped(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Task receives UPSTREAM_SKIPPED sentinel when dependency is SKIPPED."""
        session, broker, app = setup
        task_fail = make_failing_task(app, 'afd_skipped_fail')
        task_skip = make_simple_task(app, 'afd_skipped_skip')
        task_recv = make_args_receiver_task(app, 'afd_skipped_recv')

        # A (fails) -> B (skipped) -> C (allow_failed_deps=True)
        node_a = TaskNode(fn=task_fail)
        node_b = TaskNode(fn=task_skip, args=(1,), waits_for=[node_a])
        node_c = TaskNode(
            fn=task_recv,
            waits_for=[node_b],
            args_from={'input_result': node_b},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker,
            name='afd_skipped',
            tasks=[node_a, node_b, node_c],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='Failed')),
        )

        # B should be SKIPPED
        assert await _get_task_status(session, handle.workflow_id, 1) == 'SKIPPED'

        # C should be ENQUEUED
        assert await _get_task_status(session, handle.workflow_id, 2) == 'ENQUEUED'

        # Check injected kwargs
        wt_result = await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = :wf_id AND task_index = 2'
            ),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        task_result = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :tid'),
            {'tid': task_id},
        )
        kwargs = loads_json(task_result.fetchone()[0])

        # input_result should be the sentinel
        assert 'input_result' in kwargs
        injected = loads_json(kwargs['input_result']['data'])

        assert 'err' in injected
        assert injected['err']['error_code'] == 'UPSTREAM_SKIPPED'

    async def test_allow_failed_deps_without_args_from(
        self,
        setup: _SetupTuple,
    ) -> None:
        """allow_failed_deps=True without args_from still enqueues the task (cleanup pattern)."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'afd_no_args_a')
        task_b = make_simple_task(app, 'afd_no_args_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            args=(42,),
            waits_for=[node_a],
            allow_failed_deps=True,
            # No args_from - task doesn't receive upstream result
        )

        spec = make_workflow_spec(
            broker=broker,
            name='afd_no_args',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # B should be ENQUEUED (allow_failed_deps=True, no args_from needed)
        assert await _get_task_status(session, handle.workflow_id, 1) == 'ENQUEUED'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestValidationGuards:
    """Tests for workflow validation guards."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_direct_workflow_spec_fails_without_queue(
        self,
        setup: _SetupTuple,
    ) -> None:
        """WorkflowSpec without resolved queue fails at start_workflow_async()."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'unresolved_queue')

        # Create TaskNode without resolving queue (bypasses app.workflow())
        node_a = TaskNode(fn=task_a, args=(1,))
        # Direct construction - queue is None
        from horsies.core.models.workflow import WorkflowSpec

        spec = WorkflowSpec(name='unresolved', tasks=[node_a], broker=broker)

        # start_workflow_async should reject this
        with pytest.raises(
            WorkflowValidationError, match='TaskNode has unresolved queue'
        ):
            await start_workflow_async(spec, broker)

    async def test_direct_workflow_spec_fails_without_priority(
        self,
        setup: _SetupTuple,
    ) -> None:
        """WorkflowSpec without resolved priority fails at start_workflow_async()."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'unresolved_priority')

        # Create TaskNode with queue but no priority
        node_a = TaskNode(fn=task_a, args=(1,))
        node_a.queue = 'default'  # Set queue manually
        # priority is still None

        from horsies.core.models.workflow import WorkflowSpec

        spec = WorkflowSpec(name='unresolved_prio', tasks=[node_a], broker=broker)

        # start_workflow_async should reject this
        with pytest.raises(
            WorkflowValidationError, match='TaskNode has unresolved priority'
        ):
            await start_workflow_async(spec, broker)


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestSuccessPolicy:
    """Tests for success_policy behavior."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_default_behavior_any_failure_fails(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Without success_policy, any task failure -> FAILED (default behavior)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'sp_default_a')
        task_b = make_failing_task(app, 'sp_default_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b)

        # No success_policy set
        spec = make_workflow_spec(
            broker=broker,
            name='sp_default',
            tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A successfully
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        # Fail B
        await _complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Workflow should be FAILED
        assert await _get_workflow_status(session, handle.workflow_id) == 'FAILED'

    async def test_success_case_satisfied_completes(
        self,
        setup: _SetupTuple,
    ) -> None:
        """If a success case is satisfied, workflow is COMPLETED even if other tasks fail."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'sp_satisfied_a')
        task_b = make_failing_task(app, 'sp_satisfied_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b)

        # Success policy: workflow succeeds if A completes
        policy = SuccessPolicy(cases=[SuccessCase(required=[node_a])])

        spec = make_workflow_spec(
            broker=broker,
            name='sp_satisfied',
            tasks=[node_a, node_b],
            success_policy=policy,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A successfully
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        # Fail B
        await _complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Workflow should be COMPLETED (case [A] is satisfied)
        assert (
            await _get_workflow_status(session, handle.workflow_id) == 'COMPLETED'
        )

    async def test_all_cases_unsatisfied_fails(
        self,
        setup: _SetupTuple,
    ) -> None:
        """If no success case is satisfied, workflow is FAILED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'sp_unsatisfied_a')
        task_b = make_simple_task(app, 'sp_unsatisfied_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])

        # Success policy: workflow succeeds only if B completes
        # But B waits for A, so if A fails, B is SKIPPED
        policy = SuccessPolicy(cases=[SuccessCase(required=[node_b])])

        spec = make_workflow_spec(
            broker=broker,
            name='sp_unsatisfied',
            tasks=[node_a, node_b],
            success_policy=policy,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A -> B gets SKIPPED
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Workflow should be FAILED (B is SKIPPED, not COMPLETED)
        assert await _get_workflow_status(session, handle.workflow_id) == 'FAILED'

    async def test_optional_fails_but_case_satisfied_completes(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Optional tasks can fail without failing the workflow if a case is satisfied."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'sp_optional_a')
        task_b = make_failing_task(app, 'sp_optional_b')
        task_c = make_simple_task(app, 'sp_optional_c')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b)  # Will fail - marked as optional
        node_c = TaskNode(fn=task_c, args=(2,))

        # Success policy: workflow succeeds if A completes, B is optional
        policy = SuccessPolicy(
            cases=[SuccessCase(required=[node_a])],
            optional=[node_b],
        )

        spec = make_workflow_spec(
            broker=broker,
            name='sp_optional',
            tasks=[node_a, node_b, node_c],
            success_policy=policy,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A successfully
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=2))

        # Fail B (optional)
        await _complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Complete C successfully
        await _complete_task(session, handle.workflow_id, 2, TaskResult(ok=4))

        # Workflow should be COMPLETED (case [A] is satisfied, B is optional)
        assert (
            await _get_workflow_status(session, handle.workflow_id) == 'COMPLETED'
        )

    async def test_multiple_success_cases_any_satisfied(
        self,
        setup: _SetupTuple,
    ) -> None:
        """If any of multiple success cases is satisfied, workflow is COMPLETED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'sp_multi_a')
        task_b = make_simple_task(app, 'sp_multi_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,))

        # Success policy: workflow succeeds if A OR B completes
        policy = SuccessPolicy(
            cases=[
                SuccessCase(required=[node_a]),
                SuccessCase(required=[node_b]),
            ]
        )

        spec = make_workflow_spec(
            broker=broker,
            name='sp_multi',
            tasks=[node_a, node_b],
            success_policy=policy,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Complete B successfully
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=2))

        # Workflow should be COMPLETED (case [B] is satisfied)
        assert (
            await _get_workflow_status(session, handle.workflow_id) == 'COMPLETED'
        )

    async def test_success_case_not_met_error(
        self,
        setup: _SetupTuple,
    ) -> None:
        """When no case is satisfied and no required task failed, set WORKFLOW_SUCCESS_CASE_NOT_MET."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'sp_notmet_a')
        task_b = make_simple_task(app, 'sp_notmet_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])

        # Success policy: require B (which will be SKIPPED due to A failure)
        policy = SuccessPolicy(cases=[SuccessCase(required=[node_b])])

        spec = make_workflow_spec(
            broker=broker,
            name='sp_notmet',
            tasks=[node_a, node_b],
            success_policy=policy,
        )

        handle = await start_workflow_async(spec, broker)

        # Fail A -> B gets SKIPPED
        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test')),
        )

        # Workflow should be FAILED
        assert await _get_workflow_status(session, handle.workflow_id) == 'FAILED'

        # Required task (B) was SKIPPED, so no required task FAILED.
        # Expect the sentinel WORKFLOW_SUCCESS_CASE_NOT_MET.
        error_data = await _get_workflow_error(session, handle.workflow_id)
        assert error_data is not None

        # A failed first, but B (required) was SKIPPED not FAILED
        # So we get the sentinel error
        assert error_data['error_code'] == 'WORKFLOW_SUCCESS_CASE_NOT_MET'
