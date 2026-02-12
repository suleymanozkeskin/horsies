"""Integration tests for WorkflowHandle API."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowHandle,
    WorkflowHandleMissingIdError,
    WorkflowStatus,
    WorkflowTaskStatus,
    OnError,
)
from horsies.core.workflows.engine import start_workflow_async, on_workflow_task_complete

from .conftest import make_simple_task, make_failing_task, make_workflow_spec


# =============================================================================
# Shared helpers
# =============================================================================


async def _complete_task(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
    result: TaskResult[Any, TaskError],
) -> None:
    """Simulate task completion by looking up the task_id and calling the engine."""
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


# =============================================================================
# TestWorkflowHandleStatus
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleStatus:
    """Tests for WorkflowHandle.status()."""

    async def test_status_running(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns RUNNING while executing."""
        task_a = make_simple_task(app, 'status_running_a')
        task_b = make_simple_task(app, 'status_running_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='status_running', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        status = await handle.status_async()
        assert status == WorkflowStatus.RUNNING

    async def test_status_completed(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns COMPLETED after success."""
        task_a = make_simple_task(app, 'status_completed_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='status_completed', tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        status = await handle.status_async()
        assert status == WorkflowStatus.COMPLETED

    async def test_status_failed(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns FAILED after failure."""
        task_a = make_failing_task(app, 'status_failed_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker, name='status_failed', tasks=[node_a], on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='Test')),
        )

        status = await handle.status_async()
        assert status == WorkflowStatus.FAILED

    async def test_status_paused(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns PAUSED when paused."""
        task_a = make_failing_task(app, 'status_paused_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker, name='status_paused', tasks=[node_a], on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='Test')),
        )

        status = await handle.status_async()
        assert status == WorkflowStatus.PAUSED

    async def test_status_nonexistent_raises_value_error(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """status_async() raises ValueError for nonexistent workflow_id."""
        handle = WorkflowHandle(
            workflow_id=str(uuid.uuid4()),
            broker=broker,
        )

        with pytest.raises(ValueError, match='not found'):
            await handle.status_async()


# =============================================================================
# TestWorkflowHandleGet
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleGet:
    """Tests for WorkflowHandle.get()."""

    async def test_get_completed_returns_result(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns TaskResult for completed workflow."""
        task_a = make_simple_task(app, 'get_completed_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='get_completed', tasks=[node_a], output=node_a,
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=100))

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_ok()
        assert result.unwrap() == 100

    async def test_get_failed_returns_error(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns TaskResult with error for failed workflow."""
        task_a = make_failing_task(app, 'get_failed_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker, name='get_failed', tasks=[node_a], on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_ERROR', message='Test failure')),
        )

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_err()
        assert result.unwrap_err().error_code == 'TEST_ERROR'

    async def test_get_paused_returns_paused_error(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns WORKFLOW_PAUSED error for paused workflow."""
        task_a = make_failing_task(app, 'get_paused_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker, name='get_paused', tasks=[node_a], on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='Test')),
        )

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_err()
        assert result.unwrap_err().error_code == 'WORKFLOW_PAUSED'

    async def test_get_timeout(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns timeout error after timeout_ms."""
        task_a = make_simple_task(app, 'get_timeout_a')
        task_b = make_simple_task(app, 'get_timeout_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_a.queue = 'timeout_queue'
        node_a.priority = 100
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_b.queue = 'timeout_queue'
        node_b.priority = 100

        spec = make_workflow_spec(
            broker=broker,
            name='get_timeout',
            tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        # Don't complete any tasks - workflow stays RUNNING
        result = await handle.get_async(timeout_ms=500)  # Short timeout
        assert result.is_err()
        assert 'WAIT_TIMEOUT' in str(result.unwrap_err().error_code)

    async def test_get_cancelled_returns_cancelled_error(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """get_async() returns WORKFLOW_CANCELLED error after cancellation."""
        task_a = make_simple_task(app, 'get_cancelled_a')
        task_b = make_simple_task(app, 'get_cancelled_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='get_cancelled', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        await handle.cancel_async()

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_err()
        assert result.unwrap_err().error_code == 'WORKFLOW_CANCELLED'


# =============================================================================
# TestWorkflowHandleResults
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleResults:
    """Tests for WorkflowHandle.results(), result_for(), and tasks()."""

    async def test_results_returns_all(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """results() returns dict of all task results."""
        task_a = make_simple_task(app, 'results_all_a')
        task_b = make_simple_task(app, 'results_all_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='results_all', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete both
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))

        results = await handle.results_async()
        # Keys are node_id values (workflow_name:task_index by default)
        assert 'results_all:0' in results
        assert 'results_all:1' in results
        assert results['results_all:0'].unwrap() == 10
        assert results['results_all:1'].unwrap() == 20

    async def test_result_for_returns_typed_result(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """result_for() returns TaskResult for TaskNode/NodeKey."""
        task_a = make_simple_task(app, 'result_for_a')
        task_b = make_simple_task(app, 'result_for_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='result_for', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))

        result_a = await handle.result_for_async(node_a.key())
        result_b = handle.result_for(node_b)

        assert result_a.unwrap() == 10
        assert result_b.unwrap() == 20

    async def test_result_for_returns_error_when_not_ready(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """result_for() returns TaskResult with RESULT_NOT_READY error if task not completed."""
        task_a = make_simple_task(app, 'result_for_not_ready_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='result_for_not_ready', tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        # Call result_for without completing the task
        result = await handle.result_for_async(node_a)

        assert result.is_err()
        assert result.unwrap_err().error_code == LibraryErrorCode.RESULT_NOT_READY

    async def test_result_for_missing_node_id_raises(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """result_for_async() raises WorkflowHandleMissingIdError when node_id is None."""
        task_a = make_simple_task(app, 'result_for_no_id_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='result_for_no_id', tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        # Create a bare TaskNode not assigned to any spec — node_id is None
        orphan_node: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        assert orphan_node.node_id is None

        with pytest.raises(WorkflowHandleMissingIdError):
            await handle.result_for_async(orphan_node)

    async def test_results_empty_when_no_tasks_completed(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """results_async() returns empty dict when no tasks have completed."""
        task_a = make_simple_task(app, 'results_empty_a')
        task_b = make_simple_task(app, 'results_empty_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='results_empty', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        results = await handle.results_async()
        assert results == {}

    async def test_tasks_returns_info(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """tasks() returns WorkflowTaskInfo list."""
        task_a = make_simple_task(app, 'tasks_info_a')
        task_b = make_simple_task(app, 'tasks_info_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='tasks_info', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        tasks = await handle.tasks_async()
        assert len(tasks) == 2
        assert tasks[0].name == 'tasks_info_a'
        assert tasks[1].name == 'tasks_info_b'

    async def test_tasks_ordered_by_index(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Tasks returned in index order."""
        task_a = make_simple_task(app, 'tasks_order_a')
        task_b = make_simple_task(app, 'tasks_order_b')
        task_c = make_simple_task(app, 'tasks_order_c')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2})
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a, node_b])

        spec = make_workflow_spec(
            broker=broker, name='tasks_order', tasks=[node_a, node_b, node_c],
        )

        handle = await start_workflow_async(spec, broker)

        tasks = await handle.tasks_async()
        assert tasks[0].index == 0
        assert tasks[1].index == 1
        assert tasks[2].index == 2

    async def test_tasks_status_fields(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """tasks_async() returns correct status for root vs dependent tasks."""
        task_a = make_simple_task(app, 'tasks_status_a')
        task_b = make_simple_task(app, 'tasks_status_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='tasks_status', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        tasks = await handle.tasks_async()
        # Root task (A) should be enqueued, dependent task (B) should be pending
        assert tasks[0].status == WorkflowTaskStatus.ENQUEUED
        assert tasks[1].status == WorkflowTaskStatus.PENDING
        # Neither has a result yet
        assert tasks[0].result is None
        assert tasks[1].result is None

    async def test_tasks_completed_fields(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Completed task has result, started_at, and completed_at populated."""
        task_a = make_simple_task(app, 'tasks_fields_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='tasks_fields', tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=42))

        tasks = await handle.tasks_async()
        assert len(tasks) == 1

        task_info = tasks[0]
        assert task_info.status == WorkflowTaskStatus.COMPLETED
        assert task_info.result is not None
        assert task_info.result.is_ok()
        assert task_info.result.unwrap() == 42
        assert task_info.completed_at is not None


# =============================================================================
# TestWorkflowHandleCancel
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleCancel:
    """Tests for WorkflowHandle.cancel()."""

    async def test_cancel_marks_cancelled(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """cancel() sets status CANCELLED."""
        task_a = make_simple_task(app, 'cancel_a')
        task_b = make_simple_task(app, 'cancel_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(broker=broker, name='cancel', tasks=[node_a, node_b])

        handle = await start_workflow_async(spec, broker)

        await handle.cancel_async()

        status = await handle.status_async()
        assert status == WorkflowStatus.CANCELLED

    async def test_cancel_skips_pending(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Pending tasks become SKIPPED."""
        task_a = make_simple_task(app, 'cancel_skip_a')
        task_b = make_simple_task(app, 'cancel_skip_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='cancel_skip', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        # B is PENDING
        await handle.cancel_async()

        tasks = await handle.tasks_async()
        # B should be SKIPPED
        assert tasks[1].status == WorkflowTaskStatus.SKIPPED

    async def test_cancel_completed_is_noop(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """cancel_async() on a COMPLETED workflow is a safe no-op."""
        task_a = make_simple_task(app, 'cancel_noop_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='cancel_noop', tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete the workflow first
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        status_before = await handle.status_async()
        assert status_before == WorkflowStatus.COMPLETED

        # Cancel on already-completed should not change status
        await handle.cancel_async()

        status_after = await handle.status_async()
        assert status_after == WorkflowStatus.COMPLETED


# =============================================================================
# TestWorkflowHandleOutput
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleOutput:
    """Tests for explicit output task."""

    async def test_output_returns_task_result(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """With output=TaskNode, returns that result."""
        task_a = make_simple_task(app, 'output_a')
        task_b = make_simple_task(app, 'output_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='output_explicit', tasks=[node_a, node_b], output=node_b,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete both
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=200))

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_ok()
        assert result.unwrap() == 200  # B's result, not A's

    async def test_no_output_returns_terminal_dict(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Without output, returns dict of terminals wrapped in TaskResult."""
        task_a = make_simple_task(app, 'no_output_a')
        task_b = make_simple_task(app, 'no_output_b')
        task_c = make_simple_task(app, 'no_output_c')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        # Both B and C are terminals (no one waits for them)
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_c = TaskNode(fn=task_c, kwargs={'value': 3}, waits_for=[node_a])

        # No explicit output
        spec = make_workflow_spec(
            broker=broker, name='no_output', tasks=[node_a, node_b, node_c],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete all
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))
        await _complete_task(session, handle.workflow_id, 2, TaskResult(ok=30))

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_ok()

        # Result should be dict of terminal task results with unique keys
        terminal_dict = result.unwrap()
        assert isinstance(terminal_dict, dict)
        # Keys are node_id values
        assert 'no_output:1' in terminal_dict
        assert 'no_output:2' in terminal_dict

    async def test_output_failed_returns_failed(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """If output task failed, returns its error."""
        task_a = make_simple_task(app, 'output_fail_a')
        task_b = make_failing_task(app, 'output_fail_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='output_failed',
            tasks=[node_a, node_b],
            output=node_b,
            on_error=OnError.FAIL,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A, then fail B
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await _complete_task(
            session,
            handle.workflow_id,
            1,
            TaskResult(
                err=TaskError(error_code='OUTPUT_FAIL', message='Output failed'),
            ),
        )

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_err()
        assert result.unwrap_err().error_code == 'OUTPUT_FAIL'

    async def test_duplicate_task_names_preserved(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Same task function used multiple times has distinct result keys."""
        task_fetch = make_simple_task(app, 'fetch_data')

        # Same task used twice in parallel
        node_a = TaskNode(fn=task_fetch, kwargs={'value': 1})
        node_b = TaskNode(fn=task_fetch, kwargs={'value': 2})

        spec = make_workflow_spec(
            broker=broker, name='dup_tasks', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete both with different values
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=100))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=200))

        results = await handle.results_async()

        # Both results preserved with unique keys (node_id = workflow_name:task_index)
        assert len(results) == 2
        assert 'dup_tasks:0' in results
        assert 'dup_tasks:1' in results
        assert results['dup_tasks:0'].unwrap() == 100
        assert results['dup_tasks:1'].unwrap() == 200

    async def test_final_result_unique_keys_no_collision(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Final result dict uses unique keys when no output task specified."""
        task_process = make_simple_task(app, 'process')

        # Same task as two terminal nodes
        node_a = TaskNode(fn=task_process, kwargs={'value': 10})
        node_b = TaskNode(fn=task_process, kwargs={'value': 20})

        spec = make_workflow_spec(
            broker=broker, name='dup_terminal', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete both
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=111))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=222))

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_ok()

        final_dict = result.unwrap()
        assert isinstance(final_dict, dict)
        # Both terminal tasks have unique keys (node_id = workflow_name:task_index)
        assert len(final_dict) == 2
        assert 'dup_terminal:0' in final_dict
        assert 'dup_terminal:1' in final_dict


# =============================================================================
# TestWorkflowHandlePauseResume
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandlePauseResume:
    """Tests for WorkflowHandle.pause() and resume()."""

    async def test_pause_running_returns_true(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """pause_async() on a RUNNING workflow returns True and sets PAUSED."""
        task_a = make_simple_task(app, 'pause_run_a')
        task_b = make_simple_task(app, 'pause_run_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='pause_running', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        assert await handle.status_async() == WorkflowStatus.RUNNING

        paused = await handle.pause_async()
        assert paused is True

        assert await handle.status_async() == WorkflowStatus.PAUSED

    async def test_pause_non_running_returns_false(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """pause_async() on a COMPLETED workflow returns False (no-op)."""
        task_a = make_simple_task(app, 'pause_completed_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='pause_completed', tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete the workflow
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        assert await handle.status_async() == WorkflowStatus.COMPLETED

        paused = await handle.pause_async()
        assert paused is False

        # Status unchanged
        assert await handle.status_async() == WorkflowStatus.COMPLETED

    async def test_resume_paused_returns_true(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """resume_async() on a PAUSED workflow returns True and sets RUNNING."""
        task_a = make_simple_task(app, 'resume_paused_a')
        task_b = make_simple_task(app, 'resume_paused_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='resume_paused', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        # Pause first
        paused = await handle.pause_async()
        assert paused is True

        resumed = await handle.resume_async()
        assert resumed is True

        assert await handle.status_async() == WorkflowStatus.RUNNING

    async def test_resume_non_paused_returns_false(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """resume_async() on a RUNNING workflow returns False (no-op)."""
        task_a = make_simple_task(app, 'resume_running_a')
        task_b = make_simple_task(app, 'resume_running_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='resume_running', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)

        assert await handle.status_async() == WorkflowStatus.RUNNING

        resumed = await handle.resume_async()
        assert resumed is False

        # Status unchanged
        assert await handle.status_async() == WorkflowStatus.RUNNING

    async def test_pause_resume_round_trip(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Workflow can be paused and resumed, then completed normally."""
        task_a = make_simple_task(app, 'roundtrip_a')
        task_b = make_simple_task(app, 'roundtrip_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='roundtrip', tasks=[node_a, node_b],
        )

        handle = await start_workflow_async(spec, broker)
        assert await handle.status_async() == WorkflowStatus.RUNNING

        # Pause
        assert await handle.pause_async() is True
        assert await handle.status_async() == WorkflowStatus.PAUSED

        # Resume
        assert await handle.resume_async() is True
        assert await handle.status_async() == WorkflowStatus.RUNNING

        # Complete both tasks
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))

        assert await handle.status_async() == WorkflowStatus.COMPLETED

        results = await handle.results_async()
        assert results['roundtrip:0'].unwrap() == 10
        assert results['roundtrip:1'].unwrap() == 20
