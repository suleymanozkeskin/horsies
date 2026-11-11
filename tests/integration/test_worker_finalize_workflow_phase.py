"""Integration tests for Worker._finalize_workflow_phase().

Covers phase-2 of finalization: workflow advancement delegation,
NOTIFY wake signals, queue-name channel routing, exception handling,
and a NOTIFY refused by the server, which is a phase-2 error.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.json_io import dumps_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import (
    Worker,
    _FINALIZE_STAGE_PHASE2,
)
from tests.integration.conftest import compute_test_enqueue_sha
from tests.integration.history_seeding import force_terminal

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
    """Seed a task in the given status and return its id.

    A terminal status arrives through terminalization's move: the row is
    born running, then forced terminal into history.
    """
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(
        task_name='wf_phase_test',
        queue_name=queue_name,
    )
    is_terminal = status in ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'wf_phase_test', :queue, 100, '[]', '{}',
                 :status, :sent_at, NOW(), NOW(), FALSE, 0,
                 0, NOW(), :enqueue_sha,
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'queue': queue_name,
            'status': 'RUNNING' if is_terminal else status,
            'sent_at': sent_at,
            'enqueue_sha': sha,
        },
    )
    if is_terminal:
        await force_terminal(session, task_id, status=status)
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

    result = await worker._finalize_workflow_phase(
        task_id,
        tr,
        queue_name='high-priority',
        task_name='wf_phase_test',
    )

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
        result = await worker._finalize_workflow_phase(
            task_id, tr, task_name='wf_phase_test',
        )

    assert is_ok(result)
    mock_complete.assert_awaited_once()
    # Verify task_id was passed (session, task_id, result, broker)
    call_args = mock_complete.call_args
    assert call_args[0][1] == task_id


# ---------------------------------------------------------------------------
# Test 3: a NOTIFY that fails on the server is a phase-2 error
# ---------------------------------------------------------------------------

# A channel name of 64 bytes or more is refused by the server ("channel name
# too long"), after the node write of the same transaction ran.
_TOO_LONG_QUEUE = 'q' * 60


async def _insert_running_node(session: AsyncSession) -> str:
    """A RUNNING workflow node and the id of the task that backs it."""
    task_id = str(uuid.uuid4())
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:wf, 'wf_phase_notify', 'RUNNING', 'FAIL', 0, :wf,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'wf': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES (:id, :wf, 0, 'node-0', 'wf_phase_test', '[]', '{}',
                    'default', 100, '{}', FALSE, 'all', FALSE, 'RUNNING',
                    CAST(:task_id AS uuid), NOW())
        """),
        {'id': str(uuid.uuid4()), 'wf': workflow_id, 'task_id': task_id},
    )
    await session.commit()
    return task_id


@pytest.mark.asyncio(loop_scope='function')
async def test_a_notify_that_fails_on_the_server_is_a_phase2_error(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The node write and the wake share one transaction. A wake refused by
    the server aborts it, so the writes are either committed or reported:
    never a silent rollback behind Ok(None)."""
    task_id = await _insert_running_node(session)
    worker = _make_worker(engine)

    result = await worker._finalize_workflow_phase(
        task_id,
        _make_ok_task_result(),
        queue_name=_TOO_LONG_QUEUE,
        task_name='wf_phase_test',
    )

    node_status = (
        await session.execute(
            text(
                'SELECT status FROM horsies_workflow_tasks '
                'WHERE task_id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).scalar_one()
    match result:
        case Ok():
            assert (
                node_status == 'COMPLETED'
            ), 'phase 2 returned Ok but its node write did not commit'
        case Err(err_value=error):
            assert error.stage == _FINALIZE_STAGE_PHASE2
            assert node_status == 'RUNNING'
    assert is_err(result), 'a NOTIFY refused by the server is a phase-2 error'
    assert result.err_value.data is not None
    assert 'channel name too long' in result.err_value.data['exception']


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

    result = await worker._finalize_workflow_phase(
        task_id,
        tr,
        queue_name='high-priority',
        task_name='wf_phase_test',
    )

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

            @property
            def info(self) -> Any:
                return self._s.info

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

    result = await worker._finalize_workflow_phase(
        task_id,
        tr,
        queue_name='high-priority',
        task_name='wf_phase_test',
    )

    assert is_ok(result)
    # Find the queue-specific NOTIFY
    queue_notifies = [
        params for (_, params) in executed_stmts
        if params.get('c2', '').startswith('task_queue_')
    ]
    assert len(queue_notifies) == 1
    assert queue_notifies[0]['c2'] == 'task_queue_high-priority'
