"""Integration tests for Worker._finalize_workflow_phase().

Covers phase-2 of finalization: workflow advancement delegation,
NOTIFY wake signals, queue-name channel routing, exception handling,
and the non-fatal NOTIFY failure swallow.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.serde import dumps_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import (
    Worker,
    _FINALIZE_STAGE_PHASE2,
)

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


async def _insert_task(
    session: AsyncSession,
    *,
    queue_name: str = 'default',
    status: str = 'COMPLETED',
) -> str:
    """Insert a horsies_tasks row and return its id."""
    task_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at)
            VALUES
                (:id, 'wf_phase_test', :queue, 100, '[]', '{}',
                 :status, NOW(), NOW(), NOW(), FALSE, 0,
                 0, NOW())
        """),
        {'id': task_id, 'queue': queue_name, 'status': status},
    )
    await session.commit()
    return task_id


async def _link_task_to_workflow(session: AsyncSession, task_id: str) -> str:
    """Create minimal workflow + workflow_task rows linking task_id to a workflow.

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
                 queue_name, priority, dependencies, args_from, workflow_ctx_from,
                 allow_failed_deps, join_type, min_success, task_options, status,
                 is_subworkflow, task_id, created_at)
            VALUES
                (:id, :wf_id, 0, 'node_0', 'wf_phase_test', '[]', '{}',
                 'default', 100, '{}', NULL, NULL,
                 FALSE, 'all', NULL, NULL, 'ENQUEUED',
                 FALSE, :task_id, NOW())
        """),
        {'id': wt_id, 'wf_id': wf_id, 'task_id': task_id},
    )
    await session.commit()
    return wf_id


def _make_ok_task_result() -> TaskResult[str, TaskError]:
    """Create a simple ok TaskResult for passing to phase-2."""
    return TaskResult(ok='test-value')


# ---------------------------------------------------------------------------
# Test 1: Non-workflow task — NOTIFY fires, returns Ok(None)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_non_workflow_task_notifies_and_returns_ok(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task not in a workflow → _handle_workflow_task_if_needed is a no-op,
    NOTIFY fires for capacity wake, returns Ok(None)."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)
    tr = _make_ok_task_result()

    result = await worker._finalize_workflow_phase(task_id, tr)

    assert is_ok(result)
    assert result.ok_value is None


# ---------------------------------------------------------------------------
# Test 2: Workflow-linked task — delegates to on_workflow_task_complete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_task_delegates_to_engine(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task linked to a workflow → on_workflow_task_complete is called."""
    task_id = await _insert_task(session)
    await _link_task_to_workflow(session, task_id)
    worker = _make_worker(engine)
    tr = _make_ok_task_result()

    # _handle_workflow_task_if_needed does a lazy import:
    #   from horsies.core.workflows.engine import on_workflow_task_complete
    # Patch at the source module where the name is resolved.
    with patch(
        'horsies.core.workflows.engine.on_workflow_task_complete',
        new_callable=AsyncMock,
    ) as mock_complete:
        result = await worker._finalize_workflow_phase(task_id, tr)

    assert is_ok(result)
    mock_complete.assert_awaited_once()
    # Verify task_id was passed (session, task_id, result, broker)
    call_args = mock_complete.call_args
    assert call_args[0][1] == task_id


# ---------------------------------------------------------------------------
# Test 3: NOTIFY failure is swallowed — Ok(None) still returned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_notify_failure_swallowed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """NOTIFY SQL raising → swallowed by inner try/except, Ok(None) returned."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)
    tr = _make_ok_task_result()

    # Patch GET_TASK_QUEUE_NAME_SQL execution to raise inside the NOTIFY block.
    # We do this by making _handle_workflow_task_if_needed succeed but
    # replacing the NOTIFY SQL constant with something that raises.
    original_sf = worker.sf

    call_count = 0

    def patched_sf() -> Any:
        """Return a session whose execute raises on NOTIFY calls."""
        real_session = original_sf()

        class NotifyFailSession:
            """Wraps the real session, failing on NOTIFY-related queries."""

            def __init__(self, inner: Any) -> None:
                self._inner = inner

            async def __aenter__(self) -> 'NotifyFailSession':
                self._s = await self._inner.__aenter__()
                return self

            async def __aexit__(self, *args: Any) -> None:
                await self._inner.__aexit__(*args)

            async def execute(self, stmt: Any, params: Any = None) -> Any:
                nonlocal call_count
                stmt_str = str(stmt) if hasattr(stmt, 'text') else str(stmt)
                # Let CHECK_WORKFLOW_TASK_EXISTS through, fail on pg_notify
                if 'pg_notify' in stmt_str:
                    raise RuntimeError('NOTIFY channel unavailable')
                call_count += 1
                if params is not None:
                    return await self._s.execute(stmt, params)
                return await self._s.execute(stmt)

            async def commit(self) -> None:
                await self._s.commit()

        return NotifyFailSession(real_session)

    worker.sf = patched_sf  # type: ignore[assignment]

    result = await worker._finalize_workflow_phase(task_id, tr)

    assert is_ok(result)
    assert result.ok_value is None


# ---------------------------------------------------------------------------
# Test 4: _handle_workflow_task_if_needed raises → Err with stage=phase2
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_handler_exception_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Exception in _handle_workflow_task_if_needed → Err(_FinalizeError)."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)
    tr = _make_ok_task_result()

    worker._handle_workflow_task_if_needed = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError('workflow engine exploded'),
    )

    result = await worker._finalize_workflow_phase(task_id, tr)

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_PHASE2
    assert err.retryable is False  # RuntimeError is not a retryable connection error
    assert 'workflow engine exploded' in err.data['exception']


# ---------------------------------------------------------------------------
# Test 5: Queue name is used in NOTIFY channel
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_queue_name_used_in_notify_channel(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task with queue='high-priority' → NOTIFY sent to task_queue_high-priority."""
    task_id = await _insert_task(session, queue_name='high-priority')
    worker = _make_worker(engine)
    tr = _make_ok_task_result()

    # Capture the NOTIFY calls
    executed_stmts: list[tuple[str, dict[str, Any]]] = []
    original_sf = worker.sf

    def capturing_sf() -> Any:
        real_session = original_sf()

        class CapturingSession:
            def __init__(self, inner: Any) -> None:
                self._inner = inner

            async def __aenter__(self) -> 'CapturingSession':
                self._s = await self._inner.__aenter__()
                return self

            async def __aexit__(self, *args: Any) -> None:
                await self._inner.__aexit__(*args)

            async def execute(self, stmt: Any, params: Any = None) -> Any:
                stmt_text = getattr(stmt, 'text', str(stmt))
                if params and 'pg_notify' in stmt_text:
                    executed_stmts.append((stmt_text, dict(params)))
                if params is not None:
                    return await self._s.execute(stmt, params)
                return await self._s.execute(stmt)

            async def commit(self) -> None:
                await self._s.commit()

        return CapturingSession(real_session)

    worker.sf = capturing_sf  # type: ignore[assignment]

    result = await worker._finalize_workflow_phase(task_id, tr)

    assert is_ok(result)
    # Find the queue-specific NOTIFY
    queue_notifies = [
        params for (_, params) in executed_stmts
        if params.get('c2', '').startswith('task_queue_')
    ]
    assert len(queue_notifies) == 1
    assert queue_notifies[0]['c2'] == 'task_queue_high-priority'
