"""Integration tests for WorkflowHandle API."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowHandle,
    WorkflowStatus,
    WorkflowTaskStatus,
    OnError,
    HandleErrorCode,
)
from horsies.core.types.result import is_ok, is_err
from horsies.core.workflows.engine import on_workflow_task_complete
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker

from .conftest import make_simple_task, make_failing_task, make_workflow_spec, start_ok


# =============================================================================
# Shared helpers
# =============================================================================


async def _complete_task(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
    result: TaskResult[Any, TaskError],
) -> None:
    """Simulate task completion by looking up the task_id and calling the engine.

    Polls for task_id with a short timeout to handle the case where a
    predecessor's completion has just enqueued this task (setting task_id)
    but the commit hasn't been visible yet.
    """
    deadline = time.monotonic() + 2.0
    while True:
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
            return
        if time.monotonic() >= deadline:
            raise AssertionError(
                f'Task {task_index} in workflow {workflow_id} has no task_id '
                f'after 2s (row={row}). Task was never enqueued.',
            )
        await asyncio.sleep(0.05)


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

        handle = await start_ok(spec, broker)

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING

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

        handle = await start_ok(spec, broker)

        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.COMPLETED

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

        handle = await start_ok(spec, broker)

        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='Test')),
        )

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.FAILED

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

        handle = await start_ok(spec, broker)

        await _complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='FAIL', message='Test')),
        )

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.PAUSED

    async def test_status_nonexistent_returns_not_found(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """status_async() returns Err(WORKFLOW_NOT_FOUND) for nonexistent workflow_id."""
        handle = WorkflowHandle(
            workflow_id=str(uuid.uuid4()),
            broker=broker,
        )

        status_r = await handle.status_async()
        assert is_err(status_r)
        assert status_r.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND
        assert status_r.err_value.retryable is False


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

        handle = await start_ok(spec, broker)

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

        handle = await start_ok(spec, broker)

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

        handle = await start_ok(spec, broker)

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

        handle = await start_ok(spec, broker)

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

        handle = await start_ok(spec, broker)

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        result = await handle.get_async(timeout_ms=1000)
        assert result.is_err()
        assert result.unwrap_err().error_code == 'WORKFLOW_CANCELLED'

    async def test_get_nonexistent_workflow_returns_not_found(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """get_async() returns TaskResult(err=WORKFLOW_NOT_FOUND) for nonexistent workflow."""
        handle = WorkflowHandle(
            workflow_id=str(uuid.uuid4()),
            broker=broker,
        )

        result = await handle.get_async(timeout_ms=500)
        assert result.is_err()
        assert result.unwrap_err().error_code == LibraryErrorCode.WORKFLOW_NOT_FOUND


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

        handle = await start_ok(spec, broker)

        # Complete both
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))

        results_r = await handle.results_async()
        assert is_ok(results_r)
        results = results_r.ok_value
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

        handle = await start_ok(spec, broker)

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

        handle = await start_ok(spec, broker)

        # Call result_for without completing the task
        result = await handle.result_for_async(node_a)

        assert result.is_err()
        assert result.unwrap_err().error_code == LibraryErrorCode.RESULT_NOT_READY

    async def test_result_for_missing_node_id_returns_error(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """result_for_async() returns WORKFLOW_CTX_MISSING_ID when node_id is None."""
        task_a = make_simple_task(app, 'result_for_no_id_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='result_for_no_id', tasks=[node_a],
        )

        handle = await start_ok(spec, broker)

        # Create a bare TaskNode not assigned to any spec — node_id is None
        orphan_node: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        assert orphan_node.node_id is None

        result = await handle.result_for_async(orphan_node)
        assert result.is_err()
        assert result.unwrap_err().error_code == LibraryErrorCode.WORKFLOW_CTX_MISSING_ID
        assert 'node_id is not set' in (result.unwrap_err().message or '')

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

        handle = await start_ok(spec, broker)

        results_r = await handle.results_async()
        assert is_ok(results_r)
        assert results_r.ok_value == {}

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

        handle = await start_ok(spec, broker)

        tasks_r = await handle.tasks_async()
        assert is_ok(tasks_r)
        tasks = tasks_r.ok_value
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

        handle = await start_ok(spec, broker)

        tasks_r = await handle.tasks_async()
        assert is_ok(tasks_r)
        tasks = tasks_r.ok_value
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

        handle = await start_ok(spec, broker)

        tasks_r = await handle.tasks_async()
        assert is_ok(tasks_r)
        tasks = tasks_r.ok_value
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

        handle = await start_ok(spec, broker)

        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=42))

        tasks_r = await handle.tasks_async()
        assert is_ok(tasks_r)
        tasks = tasks_r.ok_value
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

        handle = await start_ok(spec, broker)

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.CANCELLED

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

        handle = await start_ok(spec, broker)

        # B is PENDING
        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        tasks_r = await handle.tasks_async()
        assert is_ok(tasks_r)
        # B should be SKIPPED
        assert tasks_r.ok_value[1].status == WorkflowTaskStatus.SKIPPED

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

        handle = await start_ok(spec, broker)

        # Complete the workflow first
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.COMPLETED

        # Cancel on already-completed should not change status
        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.COMPLETED

    async def test_cancel_leaves_no_pending_ready_enqueued_workflow_tasks(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Cancel guarantees no workflow_task remains in queueable states."""
        task_a = make_simple_task(app, 'cancel_guarantee_states_a')
        task_b = make_simple_task(app, 'cancel_guarantee_states_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='cancel_guarantee_states', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)
        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        rows = (
            await session.execute(
                text("""
                    SELECT status FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf_id
                """),
                {'wf_id': handle.workflow_id},
            )
        ).fetchall()
        statuses = {row[0] for row in rows}
        assert statuses
        assert not statuses.intersection({'PENDING', 'READY', 'ENQUEUED'})

        root_row = (
            await session.execute(
                text("""
                    SELECT wt.status, t.status, t.claimed
                    FROM horsies_workflow_tasks wt
                    JOIN horsies_tasks t ON t.id = wt.task_id
                    WHERE wt.workflow_id = :wf_id AND wt.task_index = 0
                """),
                {'wf_id': handle.workflow_id},
            )
        ).fetchone()
        assert root_row is not None
        assert root_row[0] == 'SKIPPED'
        assert root_row[1] == 'CANCELLED'
        assert root_row[2] is False

    async def test_cancel_marks_claimed_not_started_task_not_claimable(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Cancel turns ENQUEUED+CLAIMED-not-started work into CANCELLED+SKIPPED."""
        task_a = make_simple_task(app, 'cancel_guarantee_claimed_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='cancel_guarantee_claimed', tasks=[node_a],
        )

        handle = await start_ok(spec, broker)
        row = (
            await session.execute(
                text("""
                    SELECT task_id FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf_id AND task_index = 0
                """),
                {'wf_id': handle.workflow_id},
            )
        ).fetchone()
        assert row is not None and row[0] is not None
        task_id = row[0]

        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW(),
                    claimed_by_worker_id = 'test-worker',
                    claim_expires_at = NOW() + INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': task_id},
        )
        await session.commit()

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        task_row = (
            await session.execute(
                text("""
                    SELECT status, claimed, claimed_at, claimed_by_worker_id, claim_expires_at
                    FROM horsies_tasks
                    WHERE id = :task_id
                """),
                {'task_id': task_id},
            )
        ).fetchone()
        assert task_row is not None
        assert task_row[0] == 'CANCELLED'
        assert task_row[1] is False
        assert task_row[2] is None
        assert task_row[3] is None
        assert task_row[4] is None

        wf_task_row = (
            await session.execute(
                text("""
                    SELECT status FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf_id AND task_index = 0
                """),
                {'wf_id': handle.workflow_id},
            )
        ).fetchone()
        assert wf_task_row is not None
        assert wf_task_row[0] == 'SKIPPED'

    async def test_cancel_nonexistent_returns_not_found(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """cancel_async() returns Err(WORKFLOW_NOT_FOUND) for nonexistent workflow."""
        handle = WorkflowHandle(
            workflow_id=str(uuid.uuid4()),
            broker=broker,
        )

        cancel_r = await handle.cancel_async()
        assert is_err(cancel_r)
        assert cancel_r.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND
        assert cancel_r.err_value.retryable is False
        assert cancel_r.err_value.operation == 'cancel'

    async def test_worker_filter_rejects_cancelled_workflow_claimed_rows(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Claim pipeline must not dispatch rows from CANCELLED workflows."""
        task_a = make_simple_task(app, 'cancel_guarantee_worker_filter_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='cancel_guarantee_worker_filter', tasks=[node_a],
        )

        handle = await start_ok(spec, broker)
        row = (
            await session.execute(
                text("""
                    SELECT task_id FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf_id AND task_index = 0
                """),
                {'wf_id': handle.workflow_id},
            )
        ).fetchone()
        assert row is not None and row[0] is not None
        task_id = row[0]

        # Simulate claim/cancel race: task is CLAIMED while workflow is CANCELLED.
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'CANCELLED', updated_at = NOW()
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW(),
                    claimed_by_worker_id = 'worker-race',
                    claim_expires_at = NOW() + INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': task_id},
        )
        await session.commit()

        cfg = WorkerConfig(
            dsn=broker.config.database_url,
            psycopg_dsn=broker.listener.database_url,
            queues=['default'],
        )
        worker = Worker(
            session_factory=broker.session_factory,
            listener=MagicMock(),
            cfg=cfg,
        )
        filtered = await worker._filter_nonrunnable_workflow_tasks(
            [{'id': task_id}],
        )
        assert filtered == []

        task_row = (
            await session.execute(
                text("""
                    SELECT status, claimed, claim_expires_at
                    FROM horsies_tasks
                    WHERE id = :task_id
                """),
                {'task_id': task_id},
            )
        ).fetchone()
        assert task_row is not None
        assert task_row[0] == 'CANCELLED'
        assert task_row[1] is False
        assert task_row[2] is None

        wf_task_row = (
            await session.execute(
                text("""
                    SELECT status FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf_id AND task_index = 0
                """),
                {'wf_id': handle.workflow_id},
            )
        ).fetchone()
        assert wf_task_row is not None
        assert wf_task_row[0] == 'SKIPPED'


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

        handle = await start_ok(spec, broker)

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

        handle = await start_ok(spec, broker)

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

        handle = await start_ok(spec, broker)

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

        handle = await start_ok(spec, broker)

        # Complete both with different values
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=100))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=200))

        results_r = await handle.results_async()
        assert is_ok(results_r)
        results = results_r.ok_value

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

        handle = await start_ok(spec, broker)

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
        """pause_async() on a RUNNING workflow returns Ok(True) and sets PAUSED."""
        task_a = make_simple_task(app, 'pause_run_a')
        task_b = make_simple_task(app, 'pause_run_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='pause_running', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING

        pause_r = await handle.pause_async()
        assert is_ok(pause_r)
        assert pause_r.ok_value is True

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.PAUSED

    async def test_pause_non_running_returns_false(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """pause_async() on a COMPLETED workflow returns Ok(False) (no-op)."""
        task_a = make_simple_task(app, 'pause_completed_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='pause_completed', tasks=[node_a],
        )

        handle = await start_ok(spec, broker)

        # Complete the workflow
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.COMPLETED

        pause_r = await handle.pause_async()
        assert is_ok(pause_r)
        assert pause_r.ok_value is False

        # Status unchanged
        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.COMPLETED

    async def test_resume_paused_returns_true(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """resume_async() on a PAUSED workflow returns Ok(True) and sets RUNNING."""
        task_a = make_simple_task(app, 'resume_paused_a')
        task_b = make_simple_task(app, 'resume_paused_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='resume_paused', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # Pause first
        pause_r = await handle.pause_async()
        assert is_ok(pause_r)
        assert pause_r.ok_value is True

        resume_r = await handle.resume_async()
        assert is_ok(resume_r)
        assert resume_r.ok_value is True

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING

    async def test_resume_non_paused_returns_false(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """resume_async() on a RUNNING workflow returns Ok(False) (no-op)."""
        task_a = make_simple_task(app, 'resume_running_a')
        task_b = make_simple_task(app, 'resume_running_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='resume_running', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING

        resume_r = await handle.resume_async()
        assert is_ok(resume_r)
        assert resume_r.ok_value is False

        # Status unchanged
        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING

    async def test_pause_nonexistent_returns_not_found(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """pause_async() returns Err(WORKFLOW_NOT_FOUND) for nonexistent workflow."""
        handle = WorkflowHandle(
            workflow_id=str(uuid.uuid4()),
            broker=broker,
        )

        pause_r = await handle.pause_async()
        assert is_err(pause_r)
        assert pause_r.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND
        assert pause_r.err_value.retryable is False
        assert pause_r.err_value.operation == 'pause'

    async def test_resume_nonexistent_returns_not_found(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """resume_async() returns Err(WORKFLOW_NOT_FOUND) for nonexistent workflow."""
        handle = WorkflowHandle(
            workflow_id=str(uuid.uuid4()),
            broker=broker,
        )

        resume_r = await handle.resume_async()
        assert is_err(resume_r)
        assert resume_r.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND
        assert resume_r.err_value.retryable is False
        assert resume_r.err_value.operation == 'resume'

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

        handle = await start_ok(spec, broker)
        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING

        # Pause
        pause_r = await handle.pause_async()
        assert is_ok(pause_r)
        assert pause_r.ok_value is True
        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.PAUSED

        # Resume
        resume_r = await handle.resume_async()
        assert is_ok(resume_r)
        assert resume_r.ok_value is True
        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING

        # Complete both tasks
        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await _complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.COMPLETED

        results_r = await handle.results_async()
        assert is_ok(results_r)
        results = results_r.ok_value
        assert results['roundtrip:0'].unwrap() == 10
        assert results['roundtrip:1'].unwrap() == 20


# =============================================================================
# TestWorkflowHandleInfraErrors
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleInfraErrors:
    """Tests for infrastructure error handling in WorkflowHandle methods."""

    async def test_status_db_error_returns_err_db_operation_failed(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """status_async() returns Err(DB_OPERATION_FAILED) on DB failure."""
        task_a = make_simple_task(app, 'status_db_err_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='status_db_err', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        # Patch session.execute to raise OperationalError
        mock_session = AsyncMock()
        mock_session.execute.side_effect = OperationalError(
            'connection lost', {}, Exception('test'),
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch.object(broker, 'session_factory', return_value=mock_ctx):
            status_r = await handle.status_async()

        assert is_err(status_r)
        assert status_r.err_value.code == HandleErrorCode.DB_OPERATION_FAILED
        assert status_r.err_value.operation == 'status'
        assert status_r.err_value.retryable is True

    async def test_get_db_error_returns_taskresult_broker_error(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """get_async() returns TaskResult(err=BROKER_ERROR) when status_async fails."""
        task_a = make_simple_task(app, 'get_db_err_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='get_db_err', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        # Patch session to raise on execute
        mock_session = AsyncMock()
        mock_session.execute.side_effect = OperationalError(
            'connection lost', {}, Exception('test'),
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch.object(broker, 'session_factory', return_value=mock_ctx):
            result = await handle.get_async(timeout_ms=500)

        assert result.is_err()
        assert result.unwrap_err().error_code == LibraryErrorCode.BROKER_ERROR

    async def test_cancel_db_error_returns_err_db_operation_failed(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """cancel_async() returns Err(DB_OPERATION_FAILED) on DB failure."""
        task_a = make_simple_task(app, 'cancel_db_err_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='cancel_db_err', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        mock_session = AsyncMock()
        mock_session.execute.side_effect = OperationalError(
            'connection lost', {}, Exception('test'),
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch.object(broker, 'session_factory', return_value=mock_ctx):
            cancel_r = await handle.cancel_async()

        assert is_err(cancel_r)
        assert cancel_r.err_value.code == HandleErrorCode.DB_OPERATION_FAILED
        assert cancel_r.err_value.operation == 'cancel'

    async def test_results_db_error_returns_err_db_operation_failed(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """results_async() returns Err(DB_OPERATION_FAILED) on DB failure."""
        task_a = make_simple_task(app, 'results_db_err_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='results_db_err', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        mock_session = AsyncMock()
        mock_session.execute.side_effect = OperationalError(
            'connection lost', {}, Exception('test'),
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch.object(broker, 'session_factory', return_value=mock_ctx):
            results_r = await handle.results_async()

        assert is_err(results_r)
        assert results_r.err_value.code == HandleErrorCode.DB_OPERATION_FAILED
        assert results_r.err_value.operation == 'results'

    async def test_handle_methods_do_not_leak_db_exceptions(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """No handle method leaks exceptions (except result_for with None node_id)."""
        handle = WorkflowHandle(
            workflow_id=str(uuid.uuid4()),
            broker=broker,
        )

        # All wrap-strategy methods return HandleResult (not raise)
        status_r = await handle.status_async()
        assert is_err(status_r)
        assert status_r.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND

        cancel_r = await handle.cancel_async()
        assert is_err(cancel_r)
        assert cancel_r.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND

        pause_r = await handle.pause_async()
        assert is_err(pause_r)
        assert pause_r.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND

        resume_r = await handle.resume_async()
        assert is_err(resume_r)
        assert resume_r.err_value.code == HandleErrorCode.WORKFLOW_NOT_FOUND

        results_r = await handle.results_async()
        assert is_ok(results_r)
        assert results_r.ok_value == {}

        tasks_r = await handle.tasks_async()
        assert is_ok(tasks_r)
        assert tasks_r.ok_value == []

        # Fold-strategy: get returns typed error
        get_r = await handle.get_async(timeout_ms=500)
        assert get_r.is_err()
        assert get_r.unwrap_err().error_code == LibraryErrorCode.WORKFLOW_NOT_FOUND


# =============================================================================
# TestListenerFallbackContract
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestListenerFallbackContract:
    """Verify the listener-failure → polling-fallback contract at integration level.

    Tests prove that:
    - Operational listener failures (Err) silently degrade to DB polling.
    - Programming errors (TypeError) propagate immediately (handle) or are
      wrapped as BROKER_ERROR (broker), never silently swallowed into polling.
    """

    # -- WorkflowHandle.get_async --

    async def test_handle_get_async_polls_on_listener_err(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Operational listener Err triggers polling fallback; get_async still returns Ok."""
        from horsies.core.brokers.result_types import (
            BrokerErrorCode,
            BrokerOperationError,
        )
        from horsies.core.types.result import Err as Err_

        task_a = make_simple_task(app, 'listen_fallback_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 50})
        spec = make_workflow_spec(
            broker=broker, name='listen_fallback', tasks=[node_a], output=node_a,
        )
        handle = await start_ok(spec, broker)

        await _complete_task(session, handle.workflow_id, 0, TaskResult(ok=100))

        listen_err = Err_(BrokerOperationError(
            code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
            message='forced test failure',
            retryable=True,
        ))
        with patch.object(
            broker.listener, 'listen', new=AsyncMock(return_value=listen_err),
        ):
            result = await handle.get_async(timeout_ms=5000)

        assert result.is_ok(), f'Expected Ok, got {result}'
        assert result.unwrap() == 100

    async def test_handle_get_async_propagates_programming_error(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Programming error from listener.listen must propagate, not degrade to polling."""
        task_a = make_simple_task(app, 'listen_prog_err_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='listen_prog_err', tasks=[node_a], output=node_a,
        )
        handle = await start_ok(spec, broker)

        with patch.object(
            broker.listener, 'listen', new=AsyncMock(side_effect=TypeError('test bug')),
        ):
            with pytest.raises(TypeError, match='test bug'):
                await handle.get_async(timeout_ms=1000)

    # -- PostgresBroker.get_result_async --

    async def test_broker_get_result_polls_on_listener_err(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
    ) -> None:
        """Operational listener Err triggers polling fallback; get_result_async still returns Ok."""
        from horsies.core.brokers.result_types import (
            BrokerErrorCode,
            BrokerOperationError,
        )
        from horsies.core.types.result import Err as Err_

        enqueue_r = await broker.enqueue_async('dummy_task', (), {}, 'default')
        assert enqueue_r.is_ok()
        task_id = enqueue_r.ok_value

        # Complete the task directly in DB
        result_json = '{"__task_result__":true,"ok":42,"err":null}'
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'COMPLETED',
                    result = :result_json,
                    completed_at = now()
                WHERE id = :tid
            """),
            {'tid': task_id, 'result_json': result_json},
        )
        await session.commit()

        listen_err = Err_(BrokerOperationError(
            code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
            message='forced test failure',
            retryable=True,
        ))
        with patch.object(
            broker.listener, 'listen', new=AsyncMock(return_value=listen_err),
        ):
            result = await broker.get_result_async(task_id, timeout_ms=5000)

        assert result.is_ok(), f'Expected Ok, got {result}'
        assert result.unwrap() == 42

    async def test_broker_get_result_wraps_programming_error_as_broker_error(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """Programming error from listener.listen is caught by outer safety net
        and returned as TaskResult(err=BROKER_ERROR) with original exception."""
        enqueue_r = await broker.enqueue_async('dummy_task', (), {}, 'default')
        assert enqueue_r.is_ok()
        task_id = enqueue_r.ok_value

        with patch.object(
            broker.listener, 'listen', new=AsyncMock(side_effect=TypeError('test bug')),
        ):
            result = await broker.get_result_async(task_id, timeout_ms=1000)

        assert result.is_err()
        err = result.unwrap_err()
        assert err.error_code == LibraryErrorCode.BROKER_ERROR
        assert isinstance(err.exception, TypeError)
        assert 'test bug' in str(err.exception)
