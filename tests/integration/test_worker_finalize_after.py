"""Integration tests for Worker._finalize_after().

Covers the orchestration logic that:
1. Awaits the child-process future
2. Runs phase-1 (persist terminal state)
3. Runs phase-2 (workflow advancement + NOTIFY)
4. Propagates errors and clears retry attempts

The raw SQL guards are tested in test_finalize_status_guard.py.
Phase-1 branching is tested in test_worker_persist_terminal_state.py.
"""

from __future__ import annotations

import asyncio
import uuid
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.serde import dumps_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import (
    Worker,
    _FinalizeError,
    _FINALIZE_STAGE_PHASE1,
    _FINALIZE_STAGE_PHASE2,
    _FINALIZE_STAGE_FUTURE,
)
from tests.integration.conftest import compute_test_enqueue_sha

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


async def _insert_running_task(session: AsyncSession) -> str:
    """Insert a minimal horsies_tasks row in RUNNING state."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='finalize_after_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha)
            VALUES
                (:id, 'finalize_after_test', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 0, NOW(), :enqueue_sha)
        """),
        {'id': task_id, 'sent_at': sent_at, 'enqueue_sha': sha},
    )
    await session.commit()
    return task_id


async def _get_task_status(session: AsyncSession, task_id: str) -> str:
    """Read current status of a task."""
    row = (
        await session.execute(
            text('SELECT status FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'Task {task_id} not found'
    return str(row[0])


def _make_resolved_future(
    ok: bool,
    result_json: str,
    failed_reason: str | None,
) -> asyncio.Future[tuple[bool, str, str | None]]:
    """Create an already-resolved Future with the given (ok, json, reason) tuple."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[tuple[bool, str, str | None]] = loop.create_future()
    fut.set_result((ok, result_json, failed_reason))
    return fut


def _make_failed_future(
    exc: BaseException,
) -> asyncio.Future[tuple[bool, str, str | None]]:
    """Create a Future that raises the given exception on await."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[tuple[bool, str, str | None]] = loop.create_future()
    fut.set_exception(exc)
    return fut


def _serialize_ok(value: object) -> str:
    """Serialize TaskResult(ok=value) to JSON string."""
    r = dumps_json(TaskResult(ok=value))
    assert not is_err(r), f'Serialization failed: {r}'
    return r.ok_value


def _make_finalize_error(task_id: str, stage: str) -> _FinalizeError:
    """Create a _FinalizeError for testing."""
    return _FinalizeError(
        error_code='TEST_ERROR',
        message='test error',
        stage=stage,
        task_id=task_id,
        retryable=False,
    )


# ---------------------------------------------------------------------------
# Test 1: Happy path — success completes and clears retry attempts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_happy_path_success_completes_and_clears(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Future resolves ok → phase-1 COMPLETED, phase-2 runs, retry attempts cleared."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok('hello')
    fut = _make_resolved_future(ok=True, result_json=result_json, failed_reason=None)

    # Seed retry attempts so we can verify they get cleared
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE1)] = 2
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE2)] = 1

    result = await worker._finalize_after(fut, task_id)

    assert is_ok(result)
    assert result.ok_value is None
    assert await _get_task_status(session, task_id) == 'COMPLETED'
    # Both retry stages must be cleared
    assert (task_id, _FINALIZE_STAGE_PHASE1) not in worker._finalize_retry_attempts
    assert (task_id, _FINALIZE_STAGE_PHASE2) not in worker._finalize_retry_attempts


# ---------------------------------------------------------------------------
# Test 2: Phase-1 skip (CLAIM_LOST) → Ok(None), DB stays RUNNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase1_skip_returns_ok_none(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """CLAIM_LOST → phase-1 returns Ok(None) → _finalize_after returns Ok(None)."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_resolved_future(ok=False, result_json='', failed_reason='CLAIM_LOST')

    # Seed phase-1 retry attempts to verify they get cleared on skip
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE1)] = 1

    result = await worker._finalize_after(fut, task_id)

    assert is_ok(result)
    assert result.ok_value is None
    assert await _get_task_status(session, task_id) == 'RUNNING'
    assert (task_id, _FINALIZE_STAGE_PHASE1) not in worker._finalize_retry_attempts


# ---------------------------------------------------------------------------
# Test 3: Phase-1 Err → propagated, no phase-2 attempted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase1_err_propagated(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Phase-1 Err is propagated as-is; phase-2 never runs."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok('value')
    fut = _make_resolved_future(ok=True, result_json=result_json, failed_reason=None)

    # Inject phase-1 failure
    expected_err = _make_finalize_error(task_id, _FINALIZE_STAGE_PHASE1)
    worker._persist_task_terminal_state = AsyncMock(  # type: ignore[method-assign]
        return_value=Err(expected_err),
    )
    # Phase-2 should NOT be called
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Ok(None),
    )

    result = await worker._finalize_after(fut, task_id)

    assert is_err(result)
    assert result.err_value is expected_err
    worker._finalize_workflow_phase.assert_not_awaited()


# ---------------------------------------------------------------------------
# Test 4: Phase-2 Err propagated but DB stays terminal (durability)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase2_err_propagated_but_db_stays_terminal(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Phase-2 failure → Err returned, but DB row remains COMPLETED (phase-1 durable)."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok('durable-value')
    fut = _make_resolved_future(ok=True, result_json=result_json, failed_reason=None)

    # Let phase-1 run against real DB, but inject phase-2 failure
    phase2_err = _make_finalize_error(task_id, _FINALIZE_STAGE_PHASE2)
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Err(phase2_err),
    )

    result = await worker._finalize_after(fut, task_id)

    assert is_err(result)
    assert result.err_value is phase2_err
    # Phase-1 committed: DB must reflect COMPLETED
    assert await _get_task_status(session, task_id) == 'COMPLETED'


# ---------------------------------------------------------------------------
# Test 5: BrokenProcessPool → non-retryable Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_broken_process_pool_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """BrokenProcessPool → _handle_broken_pool called, Err returned."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_failed_future(BrokenProcessPool('pool is dead'))

    # Mock _handle_broken_pool to avoid executor restart side-effects
    worker._handle_broken_pool = AsyncMock()  # type: ignore[method-assign]

    result = await worker._finalize_after(fut, task_id)

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_FUTURE
    assert err.retryable is False
    assert 'Broken process pool' in err.message
    worker._handle_broken_pool.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test 6: Generic future exception → requeue attempted, Err returned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_generic_future_exception_requeues_and_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """RuntimeError from future → _requeue_claimed_task called, Err returned."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_failed_future(RuntimeError('child process exploded'))

    # Mock requeue to avoid needing CLAIMED state + worker_instance_id match
    worker._requeue_claimed_task = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(value='NOT_OWNER_OR_NOT_CLAIMED'),
    )

    result = await worker._finalize_after(fut, task_id)

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_FUTURE
    assert 'child process exploded' in err.message
    worker._requeue_claimed_task.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test 7: Phase-1 Ok(None) clears only phase-1 retry attempts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase1_ok_none_clears_only_phase1_attempts(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Skip path clears phase-1 attempts but leaves phase-2 attempts intact."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_resolved_future(
        ok=False, result_json='', failed_reason='OWNERSHIP_UNCONFIRMED',
    )

    # Seed both retry stages
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE1)] = 3
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE2)] = 2

    result = await worker._finalize_after(fut, task_id)

    assert is_ok(result)
    # Phase-1 cleared
    assert (task_id, _FINALIZE_STAGE_PHASE1) not in worker._finalize_retry_attempts
    # Phase-2 untouched
    assert worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE2)] == 2


# ---------------------------------------------------------------------------
# Test 8: Full success clears both retry stages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_full_success_clears_both_retry_stages(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Full success path (phase-1 Ok(tr) + phase-2 Ok) clears both stages."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok(42)
    fut = _make_resolved_future(ok=True, result_json=result_json, failed_reason=None)

    # Seed both stages
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE1)] = 1
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE2)] = 1

    result = await worker._finalize_after(fut, task_id)

    assert is_ok(result)
    assert (task_id, _FINALIZE_STAGE_PHASE1) not in worker._finalize_retry_attempts
    assert (task_id, _FINALIZE_STAGE_PHASE2) not in worker._finalize_retry_attempts
    assert await _get_task_status(session, task_id) == 'COMPLETED'
