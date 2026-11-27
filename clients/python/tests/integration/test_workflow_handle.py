"""Integration tests for WorkflowHandle API."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Any

import psycopg
import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig, SessionDatabaseSeparate
from horsies.core.models.schema_mode import SchemaMode
from horsies.core.models.outcome import (
    Failed,
    ReadDatabaseError,
    ReadResultNotReady,
    ReadWaitTimeout,
    ReadWorkflowContextMissingId,
    ReadWorkflowNotFound,
    Succeeded,
)
from horsies.core.models.tasks import TaskError, OutcomeCode
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowHandle,
    WorkflowStatus,
    WorkflowNodeStatus,
    OnError,
    WorkflowHandleDatabaseOperationFailed,
    WorkflowHandleErrorCode,
    WorkflowHandleStoredDataInvalid,
)
from horsies.core.codec.json_io import SerializationError, dumps_json
from horsies.core.codec.typed import encode_task_error
from horsies._core import CoreConnectionError
from horsies.core.types.result import Err, Ok, is_ok, is_err

from .conftest import DB_URL, make_simple_task, make_failing_task, make_workflow_spec, start_ok
from tests.integration.conftest import finish_task
from tests.broker_resources_support import current_resources


# =============================================================================
# Shared helpers
# =============================================================================


UNREACHABLE_DATABASE_URL = 'postgresql://postgres:none@127.0.0.1:1/none'
"""A port where no server listens: each core call fails at the connect."""


@pytest_asyncio.fixture
async def unreachable_broker() -> AsyncGenerator[PostgresBroker, None]:
    """A broker whose core cannot reach its database: a handle on it gets
    the core's retryable database failure for each call."""
    broken = PostgresBroker(
        PostgresConfig(database_url=SecretStr(UNREACHABLE_DATABASE_URL)),
        schema_mode=SchemaMode.ASSUME_INITIALIZED,
    )
    yield broken
    _ = await broken.close_async()


@pytest_asyncio.fixture
async def listener_refused_broker() -> AsyncGenerator[PostgresBroker, None]:
    """A broker that reads the suite database and whose listener cannot
    connect. Its session URL is on the port of ``UNREACHABLE_DATABASE_URL``,
    so the listener's connect is refused, and a ``get()`` wait on it polls
    the workflow row. The session URL names a database of its own, so the
    listener of the process for it is new.

    The broker runs no schema install (``SchemaMode.OBSERVE_ONLY``):
    the install runs on the session URL."""
    reader = PostgresBroker(
        PostgresConfig(
            database_url=SecretStr(DB_URL),
            session_database=SessionDatabaseSeparate(url=SecretStr(
                f'postgresql://postgres:none@127.0.0.1:1/listener_refused_{uuid.uuid4().hex}',
            )),
        ),
        schema_mode=SchemaMode.OBSERVE_ONLY,
    )
    yield reader
    _ = await reader.close_async()


async def _workflow_completed_at(
    session: AsyncSession,
    workflow_id: str,
) -> datetime | None:
    """The stored completed_at of one workflow row, read in a new snapshot."""
    await session.commit()
    return (
        await session.execute(
            text('SELECT completed_at FROM horsies_workflows WHERE id = :id'),
            {'id': workflow_id},
        )
    ).scalar_one()


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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='status_running', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING


    async def test_status_nonexistent_returns_not_found(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """status_async() returns Err(WORKFLOW_NOT_FOUND) for nonexistent workflow_id."""
        handle = WorkflowHandle(
            workflow_id=uuid.uuid4(),
            broker=broker,
        )

        status_r = await handle.status_async()
        assert is_err(status_r)
        assert status_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND
        assert status_r.err_value.retryable is False


# =============================================================================
# TestWorkflowHandleGet
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleGet:
    """Tests for WorkflowHandle.get()."""


    async def test_paused_expiry_sets_completed_at_on_the_skipped_nodes(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """The PENDING node that the expiry of a paused workflow skips gets completed_at."""
        failing = make_failing_task(app, 'expiry_skip_completed_at_a')
        waiting = make_simple_task(app, 'expiry_skip_completed_at_b')
        node_a = TaskNode(function=failing)
        node_b = TaskNode(function=waiting, kwargs={'value': 1}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker,
            name='expiry_skip_completed_at',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )
        handle = await start_ok(spec, broker)
        await finish_task(
            session,
            broker,
            str(handle.workflow_id),
            0,
            Err(TaskError(code='FAIL', message='pause')),
        )

        # The core refuses an age of 0; the workflow paused more than 1 µs ago.
        handle_of_core = broker._core_handle  # pyright: ignore[reportPrivateUsage]
        match await handle_of_core.expire_paused_workflows_async(older_than_us=1, batch_size=1):
            case core.PausedExpired(expired=expired):
                assert expired == 1
            case core.PausedExpiryFailed(message=message):
                pytest.fail(f'the paused expiry failed: {message}')
        row = (
            await session.execute(
                text("""
                    SELECT status, completed_at FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf_id AND task_index = 1
                """),
                {'wf_id': handle.workflow_id},
            )
        ).one()
        assert row.status == 'SKIPPED'
        assert row.completed_at is not None


    async def test_get_timeout(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Returns timeout error after wait_ms."""
        task_a = make_simple_task(app, 'get_timeout_a')
        task_b = make_simple_task(app, 'get_timeout_b')

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_a.queue = 'timeout_queue'
        node_a.priority = 100
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])
        node_b.queue = 'timeout_queue'
        node_b.priority = 100

        spec = make_workflow_spec(
            broker=broker,
            name='get_timeout',
            tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # Don't complete any tasks - workflow stays RUNNING
        result = await handle.get_async(wait_ms=500)  # Short timeout
        assert isinstance(result, Err) and isinstance(result.err_value, ReadWaitTimeout), result

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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='get_cancelled', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        result = await handle.get_async(wait_ms=1000)
        match result:
            case Ok(Failed(error=error)):
                assert error.code is OutcomeCode.WORKFLOW_CANCELLED
            case _:
                pytest.fail(f'expected a failed outcome, got {result!r}')

    async def test_get_nonexistent_workflow_returns_not_found(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """get_async() returns Err(ReadWorkflowNotFound) for nonexistent workflow."""
        handle = WorkflowHandle(
            workflow_id=uuid.uuid4(),
            broker=broker,
        )

        result = await handle.get_async(wait_ms=500)
        assert isinstance(result, Err) and isinstance(result.err_value, ReadWorkflowNotFound), result


# =============================================================================
# TestWorkflowHandleResults
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleResults:
    """Tests for WorkflowHandle.results(), result_for(), and tasks()."""


    async def test_result_for_returns_error_when_not_ready(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """result_for() returns Err(ReadResultNotReady) if task not completed."""
        task_a = make_simple_task(app, 'result_for_not_ready_a')

        node_a = TaskNode(function=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='result_for_not_ready', tasks=[node_a],
        )

        handle = await start_ok(spec, broker)

        # Call result_for without completing the task
        result = await handle.result_for_async(node_a)

        assert isinstance(result, Err) and isinstance(result.err_value, ReadResultNotReady), result

    async def test_result_for_missing_node_id_returns_error(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """result_for_async() returns WORKFLOW_CONTEXT_MISSING_ID when node_id is None."""
        task_a = make_simple_task(app, 'result_for_no_id_a')

        node_a = TaskNode(function=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='result_for_no_id', tasks=[node_a],
        )

        handle = await start_ok(spec, broker)

        # Create a bare TaskNode not assigned to any spec — node_id is None
        orphan_node: TaskNode[int] = TaskNode(function=task_a, kwargs={'value': 1})
        assert orphan_node.node_id is None

        result = await handle.result_for_async(orphan_node)
        match result:
            case Err(ReadWorkflowContextMissingId(message=message)):
                assert 'node_id is not set' in message
            case _:
                pytest.fail(f'expected Err(ReadWorkflowContextMissingId), got {result!r}')

    async def test_results_empty_when_no_tasks_completed(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """results_async() returns empty dict when no tasks have completed."""
        task_a = make_simple_task(app, 'results_empty_a')
        task_b = make_simple_task(app, 'results_empty_b')

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

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
        """tasks() returns WorkflowNodeInfo list."""
        task_a = make_simple_task(app, 'tasks_info_a')
        task_b = make_simple_task(app, 'tasks_info_b')

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='tasks_info', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        tasks_r = await handle.nodes_async()
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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2})
        node_c = TaskNode(function=task_c, kwargs={'value': 3}, waits_for=[node_a, node_b])

        spec = make_workflow_spec(
            broker=broker, name='tasks_order', tasks=[node_a, node_b, node_c],
        )

        handle = await start_ok(spec, broker)

        tasks_r = await handle.nodes_async()
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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='tasks_status', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        tasks_r = await handle.nodes_async()
        assert is_ok(tasks_r)
        tasks = tasks_r.ok_value
        # Root task (A) should be enqueued, dependent task (B) should be pending
        assert tasks[0].status == WorkflowNodeStatus.ENQUEUED
        assert tasks[1].status == WorkflowNodeStatus.PENDING
        # Neither has a result yet
        assert tasks[0].result is None
        assert tasks[1].result is None


# =============================================================================
# TestWorkflowHandleSubWorkflowFields
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleSubWorkflowFields:
    """Tests for sub_workflow_id and sub_workflow_summary fields on WorkflowNodeInfo."""

    async def test_regular_task_has_none_subworkflow_fields(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Regular TaskNode has sub_workflow_id=None and sub_workflow_summary=None."""
        task_a = make_simple_task(app, 'subwf_regular_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='subwf_regular', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        tasks_r = await handle.nodes_async()
        assert is_ok(tasks_r)
        task_info = tasks_r.ok_value[0]
        assert task_info.sub_workflow_id is None
        assert task_info.sub_workflow_summary is None

    async def test_subworkflow_node_exposes_sub_workflow_id(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """When sub_workflow_id is set (child started), tasks_async() exposes it."""
        task_a = make_simple_task(app, 'subwf_id_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='subwf_id', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        # Insert a dummy child workflow row to satisfy FK constraint
        child_wf_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, parent_workflow_id,
                     parent_task_index, root_workflow_id, sent_at, created_at,
                     updated_at)
                VALUES (:child_id, 'child_wf', 'RUNNING', 'fail', 1, :wf_id, 0,
                        :wf_id, NOW(), NOW(), NOW())
            """),
            {'child_id': child_wf_id, 'wf_id': handle.workflow_id},
        )
        # Simulate: engine linked a child workflow to this node
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET is_subworkflow = TRUE, sub_workflow_id = :child_id,
                    task_id = NULL, max_retries = NULL, execution = NULL,
                    good_until = NULL, retention_class_key = NULL,
                    sub_workflow_params = 'NOT_GIVEN'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'child_id': child_wf_id, 'wf_id': handle.workflow_id},
        )
        await session.commit()

        tasks_r = await handle.nodes_async()
        assert is_ok(tasks_r)
        task_info = tasks_r.ok_value[0]
        assert task_info.sub_workflow_id == child_wf_id
        assert task_info.sub_workflow_summary is None

    async def test_subworkflow_node_with_valid_summary(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Completed subworkflow node has both sub_workflow_id and sub_workflow_summary."""
        task_a = make_simple_task(app, 'subwf_summary_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='subwf_summary', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        child_wf_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, parent_workflow_id,
                     parent_task_index, root_workflow_id, sent_at, created_at,
                     updated_at)
                VALUES (:child_id, 'child_wf', 'COMPLETED', 'fail', 1, :wf_id, 0,
                        :wf_id, NOW(), NOW(), NOW())
            """),
            {'child_id': child_wf_id, 'wf_id': handle.workflow_id},
        )
        # Bare summary dict — the shape SubWorkflowSummary.to_json() now writes
        # (the legacy __dataclass__ envelope was retired).
        summary_json = (
            '{"status": "COMPLETED", "output": null,'
            ' "total_nodes": 3, "completed_nodes": 3,'
            ' "failed_nodes": 0, "skipped_nodes": 0,'
            ' "error_summary": null}'
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET is_subworkflow = TRUE,
                    sub_workflow_id = :child_id,
                    sub_workflow_summary = :summary,
                    task_id = NULL, max_retries = NULL, execution = NULL,
                    good_until = NULL, retention_class_key = NULL,
                    sub_workflow_params = 'NOT_GIVEN'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'child_id': child_wf_id,
                'summary': summary_json,
                'wf_id': handle.workflow_id,
            },
        )
        await session.commit()

        tasks_r = await handle.nodes_async()
        assert is_ok(tasks_r)
        task_info = tasks_r.ok_value[0]
        assert task_info.sub_workflow_id == child_wf_id
        assert task_info.sub_workflow_summary is not None

        summary = task_info.sub_workflow_summary
        assert summary.status == WorkflowStatus.COMPLETED
        assert summary.total_nodes == 3
        assert summary.completed_nodes == 3
        assert summary.failed_nodes == 0
        assert summary.skipped_nodes == 0
        assert summary.error_summary is None

    async def test_subworkflow_node_with_corrupt_summary_returns_err(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Corrupt sub_workflow_summary JSON is Err(WorkflowHandleStoredDataInvalid) with the cause."""
        task_a = make_simple_task(app, 'subwf_corrupt_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='subwf_corrupt', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        child_wf_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, parent_workflow_id,
                     parent_task_index, root_workflow_id, sent_at, created_at,
                     updated_at)
                VALUES (:child_id, 'child_wf', 'FAILED', 'fail', 1, :wf_id, 0,
                        :wf_id, NOW(), NOW(), NOW())
            """),
            {'child_id': child_wf_id, 'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET is_subworkflow = TRUE,
                    sub_workflow_id = :child_id,
                    sub_workflow_summary = :summary,
                    task_id = NULL, max_retries = NULL, execution = NULL,
                    good_until = NULL, retention_class_key = NULL,
                    sub_workflow_params = 'NOT_GIVEN'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'child_id': child_wf_id,
                'summary': '{not valid json!!!',
                'wf_id': handle.workflow_id,
            },
        )
        await session.commit()

        tasks_r = await handle.nodes_async()
        assert is_err(tasks_r)
        err = tasks_r.err_value
        assert isinstance(err, WorkflowHandleStoredDataInvalid)
        assert err.code == WorkflowHandleErrorCode.STORED_DATA_INVALID
        assert isinstance(err.cause, SerializationError)
        assert 'sub_workflow_summary JSON corrupt' in err.message

    async def test_subworkflow_node_with_bad_status_summary_returns_err(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Valid-JSON summary with an unrecognized status propagates as Err.

        Regression: an invalid persisted status used to be silently coerced to
        WorkflowStatus.FAILED, masquerading as a genuine child failure. It must
        now surface as Err(WorkflowHandleStoredDataInvalid) with the ValueError.
        """
        task_a = make_simple_task(app, 'subwf_badstatus_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker, name='subwf_badstatus', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        child_wf_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, parent_workflow_id,
                     parent_task_index, root_workflow_id, sent_at, created_at,
                     updated_at)
                VALUES (:child_id, 'child_wf', 'COMPLETED', 'fail', 1, :wf_id, 0,
                        :wf_id, NOW(), NOW(), NOW())
            """),
            {'child_id': child_wf_id, 'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET is_subworkflow = TRUE,
                    sub_workflow_id = :child_id,
                    sub_workflow_summary = :summary,
                    task_id = NULL, max_retries = NULL, execution = NULL,
                    good_until = NULL, retention_class_key = NULL,
                    sub_workflow_params = 'NOT_GIVEN'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'child_id': child_wf_id,
                'summary': '{"status": "NOT_A_STATUS", "total_nodes": 1}',
                'wf_id': handle.workflow_id,
            },
        )
        await session.commit()

        tasks_r = await handle.nodes_async()
        assert is_err(tasks_r)
        err = tasks_r.err_value
        assert isinstance(err, WorkflowHandleStoredDataInvalid)
        assert err.code == WorkflowHandleErrorCode.STORED_DATA_INVALID
        assert isinstance(err.cause, ValueError)
        assert 'sub_workflow_summary invalid for node' in err.message


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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(broker=broker, name='cancel', tasks=[node_a, node_b])

        handle = await start_ok(spec, broker)

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.CANCELLED

    async def test_cancel_sets_completed_at_on_every_cancelled_workflow(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """The cancelled root and its cancelled child both get completed_at."""
        task_a = make_simple_task(app, 'cancel_completed_at_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='cancel_completed_at', tasks=[node_a],
        )
        handle = await start_ok(spec, broker)
        child_workflow_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, parent_workflow_id,
                     parent_task_index, root_workflow_id, sent_at, created_at,
                     started_at, updated_at)
                VALUES
                    (:child_id, 'cancel_completed_at_child', 'PAUSED', 'fail', 1,
                     :parent_id, 0, :parent_id, NOW(), NOW(), NOW(), NOW())
            """),
            {'child_id': child_workflow_id, 'parent_id': handle.workflow_id},
        )
        await session.commit()

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        rows = (
            await session.execute(
                text("""
                    SELECT id, status, completed_at FROM horsies_workflows
                    WHERE id IN (:root_id, :child_id)
                """),
                {'root_id': handle.workflow_id, 'child_id': child_workflow_id},
            )
        ).fetchall()
        assert len(rows) == 2
        for row in rows:
            assert row.status == 'CANCELLED', row
            assert row.completed_at is not None, row

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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='cancel_skip', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # B is PENDING
        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        tasks_r = await handle.nodes_async()
        assert is_ok(tasks_r)
        # B should be SKIPPED
        assert tasks_r.ok_value[1].status == WorkflowNodeStatus.SKIPPED

    async def test_cancel_sets_completed_at_on_the_skipped_nodes(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """The PENDING node and the ENQUEUED node that the cancel skips get completed_at."""
        task_a = make_simple_task(app, 'cancel_skip_completed_at_a')
        task_b = make_simple_task(app, 'cancel_skip_completed_at_b')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='cancel_skip_completed_at', tasks=[node_a, node_b],
        )
        handle = await start_ok(spec, broker)

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        rows = (
            await session.execute(
                text("""
                    SELECT task_index, status, completed_at FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf_id ORDER BY task_index
                """),
                {'wf_id': handle.workflow_id},
            )
        ).fetchall()
        assert [(row.task_index, row.status) for row in rows] == [
            (0, 'SKIPPED'), (1, 'SKIPPED'),
        ]
        assert all(row.completed_at is not None for row in rows), rows


    async def test_cancel_does_not_deadlock_against_completion_lock_order(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        engine: AsyncEngine,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """N6 regression: cancel locks the workflow row before workflow_tasks.

        The node finish (horsies_workflow_node_finish) locks the workflow
        row, then the workflow_task row. A peer transaction holds locks in that order
        while cancel runs concurrently. With the correct cancel order,
        cancel waits on the workflow row holding no workflow_task locks,
        the peer's second lock succeeds, and both sides finish. With the
        inverted (pre-fix) order this exact interleaving deadlocks and
        Postgres aborts one side (SQLSTATE 40P01).
        """
        task_a = make_simple_task(app, 'cancel_lock_order_a')
        task_b = make_simple_task(app, 'cancel_lock_order_b')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='cancel_lock_order', tasks=[node_a, node_b],
        )
        handle = await start_ok(spec, broker)

        async with AsyncSession(engine, expire_on_commit=False) as peer:
            # Completion order, step 1: workflow row lock (the found CTE's
            # FOR UPDATE OF w).
            await peer.execute(
                text('SELECT id FROM horsies_workflows WHERE id = :wf_id FOR UPDATE'),
                {'wf_id': handle.workflow_id},
            )

            cancel_task = asyncio.ensure_future(handle.cancel_async())
            # Let cancel reach its first lock and block on the workflow row.
            await asyncio.sleep(0.5)
            assert not cancel_task.done(), (
                'cancel must be blocked on the workflow row lock'
            )

            # Completion order, step 2: workflow_task row lock. Succeeds only
            # while the blocked cancel holds no workflow_task locks; with the
            # pre-fix lock order this deadlocks (one side aborts with 40P01).
            await asyncio.wait_for(
                peer.execute(
                    text("""
                        SELECT id FROM horsies_workflow_tasks
                        WHERE workflow_id = :wf_id AND task_index = 0
                        FOR UPDATE
                    """),
                    {'wf_id': handle.workflow_id},
                ),
                timeout=10.0,
            )
            await peer.commit()

        cancel_r = await asyncio.wait_for(cancel_task, timeout=15.0)
        assert is_ok(cancel_r)

        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.CANCELLED

    async def test_cancel_sets_completed_at_on_descendants(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """The cascade sets completed_at on each child workflow it cancels."""
        task_a = make_simple_task(app, 'descendant_cancel_completed_at_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker,
            name='descendant_cancel_completed_at',
            tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        child_workflow_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, parent_workflow_id,
                     parent_task_index, root_workflow_id, sent_at, created_at,
                     started_at, updated_at)
                VALUES
                    (:child_id, 'descendant_completed_at_child', 'RUNNING',
                     'fail', 1, :parent_id, 0, :parent_id, NOW(), NOW(),
                     NOW(), NOW())
            """),
            {
                'child_id': child_workflow_id,
                'parent_id': handle.workflow_id,
            },
        )
        await session.commit()

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        child_status = (
            await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :id'),
                {'id': child_workflow_id},
            )
        ).scalar_one()
        assert child_status == 'CANCELLED'
        assert await _workflow_completed_at(session, child_workflow_id) is not None

    async def test_descendant_cancel_uses_completion_lock_order(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        engine: AsyncEngine,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Descendant cancellation locks its workflow row before its nodes."""
        task_a = make_simple_task(app, 'descendant_cancel_lock_order_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker,
            name='descendant_cancel_lock_order',
            tasks=[node_a],
        )
        handle = await start_ok(spec, broker)

        child_workflow_id = str(uuid.uuid4())
        child_node_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, parent_workflow_id,
                     parent_task_index, root_workflow_id, sent_at, created_at,
                     started_at, updated_at)
                VALUES
                    (:child_id, 'descendant_lock_child', 'RUNNING', 'fail', 1,
                     :parent_id, 0, :parent_id, NOW(), NOW(), NOW(), NOW())
            """),
            {
                'child_id': child_workflow_id,
                'parent_id': handle.workflow_id,
            },
        )
        await session.execute(
            text("""
                INSERT INTO horsies_workflow_tasks
                    (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name,
                     input, queue_name, priority,
                     dependencies, allow_failed_deps, join_type,
                     is_subworkflow, status, created_at,
                     max_retries, execution, retention_class_key)
                VALUES
                    ('NONE', :node_id, :child_id, 0, 'child_node',
                     'descendant_cancel_lock_order_a', '{}',
                     'default', 100, '{}', FALSE, 'all', FALSE,
                     'PENDING', NOW(),
                     0, 'ISOLATED', 'standard_30d')
            """),
            {'node_id': child_node_id, 'child_id': child_workflow_id},
        )
        await session.commit()

        async with AsyncSession(engine, expire_on_commit=False) as peer:
            await peer.execute(
                text('SELECT id FROM horsies_workflows WHERE id = :id FOR UPDATE'),
                {'id': child_workflow_id},
            )

            cancel_task = asyncio.ensure_future(handle.cancel_async())
            await asyncio.sleep(0.5)
            assert not cancel_task.done(), (
                'cancel must be blocked on the descendant workflow row lock'
            )

            await asyncio.wait_for(
                peer.execute(
                    text("""
                        SELECT id FROM horsies_workflow_tasks
                        WHERE id = :id
                        FOR UPDATE
                    """),
                    {'id': child_node_id},
                ),
                timeout=10.0,
            )
            await peer.commit()

        cancel_result = await asyncio.wait_for(cancel_task, timeout=15.0)
        assert is_ok(cancel_result)
        child_status = (
            await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :id'),
                {'id': child_workflow_id},
            )
        ).scalar_one()
        assert child_status == 'CANCELLED'

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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])
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

        # The cancelled backing task moved to history; the live table
        # holds no terminal rows.
        root_row = (
            await session.execute(
                text("""
                    SELECT wt.status, h.status
                    FROM horsies_workflow_tasks wt
                    JOIN horsies_task_history h ON h.task_id = CAST(wt.task_id AS uuid)
                    WHERE wt.workflow_id = :wf_id AND wt.task_index = 0
                """),
                {'wf_id': handle.workflow_id},
            )
        ).fetchone()
        assert root_row is not None
        assert root_row[0] == 'SKIPPED'
        assert root_row[1] == 'CANCELLED'

    async def test_cancel_marks_claimed_not_started_task_not_claimable(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """Cancel turns ENQUEUED+CLAIMED-not-started work into CANCELLED+SKIPPED."""
        task_a = make_simple_task(app, 'cancel_guarantee_claimed_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
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
                    claimed_at = NOW(),
                    claimed_by_worker_id = 'test-worker',
                    claim_expires_at = NOW() + INTERVAL '5 minutes',
                    attempt_claims = 1,
                    first_claimed_at = NOW(),
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': task_id},
        )
        await session.commit()

        cancel_r = await handle.cancel_async()
        assert is_ok(cancel_r)

        # The cancel moved the row to history: nothing claimable remains
        # in the live table, and the history row records the operation.
        live_row = (
            await session.execute(
                text("""
                    SELECT 1 FROM horsies_tasks
                    WHERE id = CAST(:task_id AS uuid)
                """),
                {'task_id': task_id},
            )
        ).fetchone()
        assert live_row is None
        history_row = (
            await session.execute(
                text("""
                    SELECT status, terminalization_kind
                    FROM horsies_task_history
                    WHERE task_id = CAST(:task_id AS uuid)
                """),
                {'task_id': task_id},
            )
        ).fetchone()
        assert history_row is not None
        assert history_row[0] == 'CANCELLED'
        assert history_row[1] == 'WORKFLOW_CANCEL_WORKFLOW'

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
            workflow_id=uuid.uuid4(),
            broker=broker,
        )

        cancel_r = await handle.cancel_async()
        assert is_err(cancel_r)
        assert cancel_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND
        assert cancel_r.err_value.retryable is False


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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

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

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

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
            workflow_id=uuid.uuid4(),
            broker=broker,
        )

        pause_r = await handle.pause_async()
        assert is_err(pause_r)
        assert pause_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND
        assert pause_r.err_value.retryable is False

    async def test_resume_nonexistent_returns_not_found(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """resume_async() returns Err(WORKFLOW_NOT_FOUND) for nonexistent workflow."""
        handle = WorkflowHandle(
            workflow_id=uuid.uuid4(),
            broker=broker,
        )

        resume_r = await handle.resume_async()
        assert is_err(resume_r)
        assert resume_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND
        assert resume_r.err_value.retryable is False


# =============================================================================
# TestWorkflowHandleInfraErrors
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestUnknownStoredWorkflowStatus:
    """horsies_workflows.status takes only the WorkflowStatus values.

    The CHECK horsies_workflows_status_known refuses any other value, so a
    handle never reads one; no path writes the row outside the database.
    """

    async def test_the_database_refuses_an_unknown_status(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        task_a = make_simple_task(app, 'unknown_status_refused_a')
        spec = make_workflow_spec(
            broker=broker,
            name='unknown_status_refused',
            tasks=[TaskNode(function=task_a, kwargs={'value': 1})],
        )
        handle = await start_ok(spec, broker)

        with pytest.raises(IntegrityError) as raised:
            await session.execute(
                text("UPDATE horsies_workflows SET status = 'BOGUS' WHERE id = :wf_id"),
                {'wf_id': handle.workflow_id},
            )
        await session.rollback()

        assert isinstance(raised.value.orig, psycopg.errors.CheckViolation)
        assert raised.value.orig.diag.constraint_name == 'horsies_workflows_status_known'
        status_r = await handle.status_async()
        assert is_ok(status_r)
        assert status_r.ok_value == WorkflowStatus.RUNNING


def _assert_failed_connect(
    error: object, label: str, subject: str, workflow_id: str,
) -> None:
    """A handle operation whose server refuses the connection, as the core
    builds it. The core pool opens its connection at the first statement, so
    the statement fails and its workflow error wraps the broker error:
    ``WorkflowHandleDatabaseOperationFailed`` with Python's message (the operation's label,
    the workflow id and the workflow error's text, ``broker error: ...``),
    retryable, and the core's class of the cause with the operation as its
    subject and the broker error's own text."""
    assert type(error) is WorkflowHandleDatabaseOperationFailed
    assert error.workflow_id == workflow_id
    assert error.retryable is True
    prefix = f'{label} failed for workflow {workflow_id}: '
    assert error.message.startswith(prefix)
    cause_text = error.message.removeprefix(prefix)
    assert type(error.cause) is CoreConnectionError
    assert error.cause.subject == subject
    assert error.cause.message != ''
    assert cause_text == f'broker error: {error.cause.message}'
    assert error.cause.retryable is True
    assert str(error.cause) == f'{subject}: {error.cause.message}'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowHandleInfraErrors:
    """Tests for infrastructure error handling in WorkflowHandle methods."""

    async def _started_on_an_unreachable_database(
        self,
        broker: PostgresBroker,
        unreachable_broker: PostgresBroker,
        app: Horsies,
        name: str,
    ) -> WorkflowHandle[Any]:
        """A workflow started on ``broker``, and a handle of it on a broker
        whose core cannot reach the database. The handle's reads and writes
        run in the core; each fails at its first statement, where the core
        pool opens its connection."""
        task_a = make_simple_task(app, f'{name}_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(broker=broker, name=name, tasks=[node_a])
        started = await start_ok(spec, broker)
        return WorkflowHandle(workflow_id=started.workflow_id, broker=unreachable_broker)

    async def test_status_db_error_returns_err_db_operation_failed(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        unreachable_broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """status_async() returns Err(DATABASE_OPERATION_FAILED) on DB failure."""
        handle = await self._started_on_an_unreachable_database(
            broker, unreachable_broker, app, 'status_db_err',
        )

        status_r = await handle.status_async()

        assert is_err(status_r)
        assert status_r.err_value.code == WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED
        assert status_r.err_value.workflow_id == str(handle.workflow_id)
        assert status_r.err_value.retryable is True
        _assert_failed_connect(
            status_r.err_value, 'Status', 'workflow_status', str(handle.workflow_id),
        )

    async def test_get_db_error_returns_taskresult_broker_error(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        unreachable_broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """get_async() returns Err(ReadDatabaseError) when the read of the wait
        fails."""
        handle = await self._started_on_an_unreachable_database(
            broker, unreachable_broker, app, 'get_db_err',
        )

        result = await handle.get_async(wait_ms=500)

        match result:
            case Err(ReadDatabaseError(message=message)):
                # The core's message of the failed wait, as the handle built it.
                assert message.startswith(f'Get failed for workflow {handle.workflow_id}: ')
            case _:
                pytest.fail(f'expected Err(ReadDatabaseError), got {result!r}')

    async def test_result_for_db_error_names_the_node(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        unreachable_broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """result_for_async() and result_for() return Err(ReadDatabaseError) when
        the read fails, with Python 0.7's message, which names the node."""
        task_a = make_simple_task(app, 'result_for_db_err_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        spec = make_workflow_spec(
            broker=broker, name='result_for_db_err', tasks=[node_a],
        )
        started = await start_ok(spec, broker)
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=started.workflow_id, broker=unreachable_broker,
        )
        assert node_a.node_id is not None
        prefix = (
            f'DB query failed for result_for node {node_a.node_id} '
            f'in workflow {handle.workflow_id}: '
        )

        async_result = await handle.result_for_async(node_a)
        sync_result = await asyncio.to_thread(handle.result_for, node_a)

        for result in (async_result, sync_result):
            match result:
                case Err(ReadDatabaseError(message=message)):
                    assert message.startswith(prefix)
                    assert message.removeprefix(prefix) != ''
                case _:
                    pytest.fail(f'expected Err(ReadDatabaseError), got {result!r}')

    async def test_cancel_db_error_returns_err_db_operation_failed(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        unreachable_broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """cancel_async() returns Err(DATABASE_OPERATION_FAILED) on DB failure, and
        the workflow is not cancelled."""
        handle = await self._started_on_an_unreachable_database(
            broker, unreachable_broker, app, 'cancel_db_err',
        )

        cancel_r = await handle.cancel_async()

        assert is_err(cancel_r)
        assert cancel_r.err_value.code == WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED
        assert cancel_r.err_value.retryable is True
        _assert_failed_connect(
            cancel_r.err_value, 'Cancel', 'cancel_workflow', str(handle.workflow_id),
        )
        status = (
            await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
                {'wf_id': handle.workflow_id},
            )
        ).scalar_one()
        assert status == 'RUNNING'

    async def test_results_db_error_returns_err_db_operation_failed(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        unreachable_broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """results_async() returns Err(DATABASE_OPERATION_FAILED) on DB failure."""
        handle = await self._started_on_an_unreachable_database(
            broker, unreachable_broker, app, 'results_db_err',
        )

        results_r = await handle.results_async()

        assert is_err(results_r)
        assert results_r.err_value.code == WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED
        assert results_r.err_value.workflow_id == str(handle.workflow_id)
        _assert_failed_connect(
            results_r.err_value, 'Results', 'workflow_node_results', str(handle.workflow_id),
        )

    async def test_handle_methods_do_not_leak_db_exceptions(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """No handle method leaks exceptions (except result_for with None node_id)."""
        handle = WorkflowHandle(
            workflow_id=uuid.uuid4(),
            broker=broker,
        )

        # All wrap-strategy methods return HandleResult (not raise)
        status_r = await handle.status_async()
        assert is_err(status_r)
        assert status_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND

        cancel_r = await handle.cancel_async()
        assert is_err(cancel_r)
        assert cancel_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND

        pause_r = await handle.pause_async()
        assert is_err(pause_r)
        assert pause_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND

        resume_r = await handle.resume_async()
        assert is_err(resume_r)
        assert resume_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND

        results_r = await handle.results_async()
        assert is_err(results_r)
        assert results_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND

        tasks_r = await handle.nodes_async()
        assert is_err(tasks_r)
        assert tasks_r.err_value.code == WorkflowHandleErrorCode.WORKFLOW_NOT_FOUND

        # Fold-strategy: get returns typed error
        get_r = await handle.get_async(wait_ms=500)
        assert isinstance(get_r, Err) and isinstance(get_r.err_value, ReadWorkflowNotFound), get_r


# =============================================================================
# TestListenerFallbackContract
# =============================================================================


POLLED_WAIT_LIMIT_MS = 10_000
"""The wait limit of a ``get()`` that the end of the workflow answers. The
end comes long before it."""

END_DURING_THE_WAIT_AFTER_S = 1.5
"""When a test ends the workflow after its ``get()`` started: after the first
poll (1 s, ``WORKFLOW_RESULT_REPOLL_POLLING``), so the wait has read the
RUNNING row again by then."""

SHORT_WAIT_LIMIT_MS = 1_500
"""A wait limit that passes while the wait polls a RUNNING workflow."""

WAIT_ANSWER_BOUND_S = 15.0
"""The longest a test waits for the answer of a ``get()``: more than each
wait limit of these tests, so a wait that does not end fails the test."""


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestListenerFallbackContract:
    """The wait of ``WorkflowHandle.get_async`` when its listener cannot
    subscribe, and a programming error of the wait.

    The wait and its listener run in the core
    (``CoreHandle.wait_workflow_result_async``). On a broker whose listener
    connect is refused (``listener_refused_broker``) the wait cannot
    subscribe on ``workflow_done``: it reads the workflow row again every
    second. A programming error of the wait (a ``TypeError``) propagates
    through ``get_async``; it is never folded into an answer.
    """

    async def _running_on_a_listener_refused_broker(
        self,
        broker: PostgresBroker,
        listener_refused_broker: PostgresBroker,
        app: Horsies,
        name: str,
    ) -> WorkflowHandle[Any]:
        """A workflow of one node started on ``broker``, and a handle of it
        on ``listener_refused_broker``. Checks that the workflow is RUNNING:
        that read opens the connection of the handle's core pool."""
        task_a = make_simple_task(app, f'{name}_a')
        node_a = TaskNode(function=task_a, kwargs={'value': 50})
        spec = make_workflow_spec(broker=broker, name=name, tasks=[node_a], output=node_a)
        started = await start_ok(spec, broker)
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=started.workflow_id,
            broker=listener_refused_broker,
            output_type=started.output_type,
        )
        status_r = await handle.status_async()
        assert is_ok(status_r), f'Expected Ok, got {status_r}'
        assert status_r.ok_value == WorkflowStatus.RUNNING
        return handle

    async def test_get_async_polls_to_the_end_when_the_listener_connect_is_refused(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        listener_refused_broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """A wait on a RUNNING workflow whose listener connect is refused
        polls the workflow row. The workflow ends during the wait, and a poll
        answers its result."""
        handle = await self._running_on_a_listener_refused_broker(
            broker, listener_refused_broker, app, 'listen_refused_poll',
        )

        wait = asyncio.create_task(handle.get_async(wait_ms=POLLED_WAIT_LIMIT_MS))
        await asyncio.sleep(END_DURING_THE_WAIT_AFTER_S)
        assert not wait.done(), (
            f'the wait answered before the workflow ended: {wait.result()!r}'
        )
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(100))
        result = await asyncio.wait_for(wait, timeout=WAIT_ANSWER_BOUND_S)

        assert result == Ok(Succeeded(100)), f'Expected Ok(Succeeded(100)), got {result}'

    async def test_get_async_answers_wait_timeout_when_the_limit_passes_while_polling(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        listener_refused_broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """A wait on a RUNNING workflow whose listener connect is refused
        polls the workflow row, and the workflow does not end. At the wait
        limit ``get_async`` answers ``WAIT_TIMEOUT``: it waited for the whole
        limit, and it did not hang."""
        handle = await self._running_on_a_listener_refused_broker(
            broker, listener_refused_broker, app, 'listen_refused_timeout',
        )

        started_at = time.monotonic()
        result = await asyncio.wait_for(
            handle.get_async(wait_ms=SHORT_WAIT_LIMIT_MS),
            timeout=WAIT_ANSWER_BOUND_S,
        )
        waited_s = time.monotonic() - started_at

        assert isinstance(result, Err) and isinstance(result.err_value, ReadWaitTimeout), (
            f'Expected Err(ReadWaitTimeout), got {result}'
        )
        assert waited_s >= SHORT_WAIT_LIMIT_MS / 1000, (
            f'the wait answered after {waited_s:.3f} s, '
            f'before its limit of {SHORT_WAIT_LIMIT_MS} ms'
        )


# =============================================================================
# TestPausedExpiryAge
# =============================================================================


async def _paused_workflow(
    session: AsyncSession,
    broker: PostgresBroker,
    app: Horsies,
    name: str,
) -> str:
    """A one-node workflow with ``on_error=PAUSE`` whose task failed: PAUSED."""
    node = TaskNode(function=make_failing_task(app, name))
    spec = make_workflow_spec(
        broker=broker, name=name, tasks=[node], on_error=OnError.PAUSE,
    )
    handle = await start_ok(spec, broker)
    await finish_task(
        session,
        broker,
        str(handle.workflow_id),
        0,
        Err(TaskError(code='FAIL', message='pause')),
    )
    return str(handle.workflow_id)


def _expiry_error_json(older_than: timedelta) -> str:
    """A stored error for a paused expiry; the age tests read only the status."""
    match dumps_json(
        encode_task_error(
            TaskError(
                code=OutcomeCode.WORKFLOW_EXPIRED,
                message=f'paused_workflow_cancel_after elapsed: {older_than}',
            ),
        ),
    ):
        case Ok(error_json):
            return error_json
        case Err(refused):
            raise AssertionError(refused)


_EXPIRE_PAUSED_WORKFLOW_SQL = text("""
    SELECT outcome, observed_status, expired_task_ids
    FROM horsies_workflow_expire_paused(
        CAST(:wf_id AS uuid), CAST(:age_us AS bigint), CAST(:error AS text)
    )
""")
"""The expiry of one PAUSED workflow older than the age in microseconds, as
the core's paused expiry calls it."""


async def _expire_each_at_one_now(
    broker: PostgresBroker,
    *,
    older_than: timedelta,
    paused_for: dict[str, timedelta],
) -> tuple[list[str], dict[str, str]]:
    """Set ``updated_at`` of each workflow, then call
    ``horsies_workflow_expire_paused`` for it with the age in microseconds,
    as the core's paused expiry does, all in one transaction on one
    connection; then roll the transaction back.

    ``now()`` is the start of that transaction for every statement, so the
    distance of each workflow to the age is exact.

    Returns the outcome of each call and the status of each workflow, read
    before the rollback.
    """
    engine = current_resources(broker).engine
    age_us = older_than // timedelta(microseconds=1)
    async with engine.connect() as connection:
        transaction = await connection.begin()
        try:
            for workflow_id, paused in paused_for.items():
                _ = await connection.execute(
                    text(
                        'UPDATE horsies_workflows SET updated_at = now()'
                        " - CAST(:paused_us AS bigint) * interval '1 microsecond'"
                        ' WHERE id = CAST(:wf_id AS uuid)'
                    ),
                    {
                        'paused_us': paused // timedelta(microseconds=1),
                        'wf_id': workflow_id,
                    },
                )
            outcomes: list[str] = []
            for workflow_id in paused_for:
                outcomes.append(
                    (
                        await connection.execute(
                            _EXPIRE_PAUSED_WORKFLOW_SQL,
                            {
                                'wf_id': workflow_id,
                                'age_us': age_us,
                                'error': _expiry_error_json(older_than),
                            },
                        )
                    ).one().outcome,
                )
            statuses: dict[str, str] = {}
            for workflow_id in paused_for:
                statuses[workflow_id] = (
                    await connection.execute(
                        text(
                            'SELECT status FROM horsies_workflows'
                            ' WHERE id = CAST(:wf_id AS uuid)'
                        ),
                        {'wf_id': workflow_id},
                    )
                ).scalar_one()
            return outcomes, statuses
        finally:
            await transaction.rollback()


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestPausedExpiryAge:
    """The paused age goes to the database as exact microseconds.

    The reaper's paused expiry runs in the core
    (``ReaperMixin._expire_paused_workflows``,
    ``CoreHandle.expire_paused_workflows_async``): it sends
    ``older_than // timedelta(microseconds=1)``, reads the candidates and calls
    ``horsies_workflow_expire_paused`` for each, in autocommit.
    """


    async def test_an_age_below_one_millisecond_is_exact(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> None:
        """An age of 500 microseconds keeps the workflow paused for 400 and
        expires the one paused for 600. Two transactions of the core never
        share one ``now()``, so the test calls the function of the core's
        expiry in one transaction with the setup."""
        kept = await _paused_workflow(session, broker, app, 'expiry_us_kept')
        expired = await _paused_workflow(session, broker, app, 'expiry_us_expired')

        outcomes, statuses = await _expire_each_at_one_now(
            broker,
            older_than=timedelta(microseconds=500),
            paused_for={
                kept: timedelta(microseconds=400),
                expired: timedelta(microseconds=600),
            },
        )

        assert outcomes == ['NOT_EXPIRED', 'EXPIRED']
        assert statuses == {kept: 'PAUSED', expired: 'EXPIRED'}
