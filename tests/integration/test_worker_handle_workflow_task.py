"""Integration tests for Worker._handle_workflow_task_if_needed().

Covers lines 1500-1525: the CHECK_WORKFLOW_TASK_EXISTS_SQL lookup,
broker resolution from self._app, and delegation to on_workflow_task_complete.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(engine: AsyncEngine) -> Worker:
    """Construct a Worker wired to the test DB."""
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    return Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)


async def _insert_task(session: AsyncSession) -> str:
    """Insert a minimal horsies_tasks row in RUNNING state."""
    task_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at)
            VALUES
                (:id, 'handle_wf_test', 'default', 100, '[]', '{}',
                 'RUNNING', NOW(), NOW(), NOW(), FALSE, 0,
                 0, NOW())
        """),
        {'id': task_id},
    )
    await session.commit()
    return task_id


async def _link_task_to_workflow(session: AsyncSession, task_id: str) -> str:
    """Create minimal workflow + workflow_task rows linking task_id.

    Returns the workflow_id.
    """
    wf_id = str(uuid.uuid4())
    wt_id = str(uuid.uuid4())

    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES
                (:id, 'test_wf', 'RUNNING', 'FAIL', 0, :id,
                 NOW(), NOW(), NOW(), NOW())
        """),
        {'id': wf_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                 queue_name, priority, dependencies, allow_failed_deps, join_type,
                 is_subworkflow, status, task_id, created_at)
            VALUES
                (:id, :wf_id, 0, 'node_0', 'handle_wf_test', '[]', '{}',
                 'default', 100, '{}', FALSE, 'all',
                 FALSE, 'ENQUEUED', :task_id, NOW())
        """),
        {'id': wt_id, 'wf_id': wf_id, 'task_id': task_id},
    )
    await session.commit()
    return wf_id


def _make_ok_result() -> TaskResult[str, TaskError]:
    """Create a simple ok TaskResult."""
    return TaskResult(ok='handle-wf-test-value')


# ---------------------------------------------------------------------------
# I-4a: Task not in horsies_workflow_tasks → early return
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_non_workflow_task_returns_without_delegation(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task with no workflow_task row → returns immediately,
    on_workflow_task_complete is never called."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)
    result = _make_ok_result()

    with patch(
        'horsies.core.workflows.engine.on_workflow_task_complete',
        new_callable=AsyncMock,
    ) as mock_complete:
        await worker._handle_workflow_task_if_needed(session, task_id, result)

    mock_complete.assert_not_awaited()


# ---------------------------------------------------------------------------
# I-4b: Task in workflow, app present → delegates with broker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_task_delegates_with_broker(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task linked to a workflow + _app is set →
    on_workflow_task_complete called with broker from get_broker()."""
    task_id = await _insert_task(session)
    await _link_task_to_workflow(session, task_id)
    worker = _make_worker(engine)
    result = _make_ok_result()

    mock_broker = MagicMock()
    mock_app = MagicMock()
    mock_app.get_broker.return_value = mock_broker
    worker._app = mock_app

    with patch(
        'horsies.core.workflows.engine.on_workflow_task_complete',
        new_callable=AsyncMock,
    ) as mock_complete:
        await worker._handle_workflow_task_if_needed(session, task_id, result)

    mock_complete.assert_awaited_once_with(session, task_id, result, mock_broker)
    mock_app.get_broker.assert_called_once()


# ---------------------------------------------------------------------------
# I-4c: Task in workflow, app is None → delegates with broker=None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_task_delegates_with_broker_none_when_no_app(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task linked to a workflow + _app is None →
    on_workflow_task_complete called with broker=None."""
    task_id = await _insert_task(session)
    await _link_task_to_workflow(session, task_id)
    worker = _make_worker(engine)
    result = _make_ok_result()

    worker._app = None

    with patch(
        'horsies.core.workflows.engine.on_workflow_task_complete',
        new_callable=AsyncMock,
    ) as mock_complete:
        await worker._handle_workflow_task_if_needed(session, task_id, result)

    mock_complete.assert_awaited_once_with(session, task_id, result, None)
