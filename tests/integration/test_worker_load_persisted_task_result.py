"""Integration tests for Worker._load_persisted_task_result().

Covers the phase-2 replay method that reads a terminal task's persisted
result from DB: not-found, non-terminal, null result, corrupt JSON,
invalid TaskResult structure, and valid success/error results.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.serde import dumps_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker, _FINALIZE_STAGE_PHASE2

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
    status: str = 'RUNNING',
    result: str | None = None,
) -> str:
    """Insert a horsies_tasks row with given status and result column."""
    task_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, result)
            VALUES
                (:id, 'load_result_test', 'default', 100, '[]', '{}',
                 :status, NOW(), NOW(), NOW(), FALSE, 0,
                 0, NOW(), :result)
        """),
        {'id': task_id, 'status': status, 'result': result},
    )
    await session.commit()
    return task_id


def _serialize_ok(value: object) -> str:
    """Serialize TaskResult(ok=value) to JSON string."""
    r = dumps_json(TaskResult(ok=value))
    assert not is_err(r), f'Serialization failed: {r}'
    return r.ok_value


def _serialize_err(error_code: str, message: str) -> str:
    """Serialize TaskResult(err=TaskError(...)) to JSON string."""
    r = dumps_json(
        TaskResult(err=TaskError(error_code=error_code, message=message)),
    )
    assert not is_err(r), f'Serialization failed: {r}'
    return r.ok_value


# ---------------------------------------------------------------------------
# Test 1: Task row not found → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_task_not_found_returns_err(
    engine: AsyncEngine,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Nonexistent task_id → Err with 'not found' message."""
    worker = _make_worker(engine)
    fake_id = str(uuid.uuid4())

    result = await worker._load_persisted_task_result(fake_id)

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_PHASE2
    assert 'not found' in err.message
    assert err.retryable is False


# ---------------------------------------------------------------------------
# Test 2: Task exists but not terminal (RUNNING) → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_not_terminal_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """RUNNING task → Err with 'unavailable' message."""
    task_id = await _insert_task(session, status='RUNNING', result=None)
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'unavailable' in err.message
    assert err.data is not None
    assert err.data['status'] == 'RUNNING'


# ---------------------------------------------------------------------------
# Test 3: Terminal but result column is NULL → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_terminal_null_result_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """COMPLETED task with result=NULL → Err with 'unavailable' message."""
    task_id = await _insert_task(session, status='COMPLETED', result=None)
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'unavailable' in err.message


# ---------------------------------------------------------------------------
# Test 4: Corrupt JSON in result column → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_corrupt_json_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """COMPLETED task with corrupt JSON in result → Err with 'corrupt' message."""
    task_id = await _insert_task(
        session, status='COMPLETED', result='not json {',
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'corrupt' in err.message


# ---------------------------------------------------------------------------
# Test 5: Valid JSON but not a TaskResult structure → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_invalid_task_result_structure_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Valid JSON but missing __task_result__ marker → Err with 'deserialize failed'."""
    task_id = await _insert_task(
        session, status='COMPLETED', result='{"random": 1}',
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'deserialize failed' in err.message


# ---------------------------------------------------------------------------
# Test 6: COMPLETED with valid ok result → Ok(TaskResult) with is_ok()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_completed_ok_result_returns_task_result(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """COMPLETED task with valid ok result → Ok(TaskResult(ok='hello'))."""
    result_json = _serialize_ok('hello')
    task_id = await _insert_task(
        session, status='COMPLETED', result=result_json,
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_ok(result)
    tr = result.ok_value
    assert tr.is_ok()
    assert tr.unwrap() == 'hello'


# ---------------------------------------------------------------------------
# Test 7: FAILED with valid error result → Ok(TaskResult) with is_err()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_failed_err_result_returns_task_result(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """FAILED task with valid error result → Ok(TaskResult(err=TaskError(...)))."""
    result_json = _serialize_err('DELIBERATE_FAIL', 'test failure')
    task_id = await _insert_task(
        session, status='FAILED', result=result_json,
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_ok(result)
    tr = result.ok_value
    assert tr.is_err()
    task_error = tr.unwrap_err()
    assert task_error.error_code == 'DELIBERATE_FAIL'
    assert task_error.message == 'test failure'
