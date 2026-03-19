"""Integration tests for Worker._persist_task_terminal_state().

Covers the Python branching logic inside phase-1 finalization:
which SQL runs, JSON parsing, retry decisions, and return values
based on (ok, result_json_str, failed_reason) inputs.

The raw SQL guards are tested in test_finalize_status_guard.py.
Full pipeline (phase-1 → phase-2) is tested in test_worker_finalize_two_phase.py.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.serde import dumps_json, serialize_error_payload
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

NOW = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(engine: AsyncEngine) -> Worker:
    """Construct a Worker wired to the test DB (no listener needed)."""
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
    sent_at, sha = compute_test_enqueue_sha(task_name='persist_terminal_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha)
            VALUES
                (:id, 'persist_terminal_test', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 0, NOW(), :enqueue_sha)
        """),
        {'id': task_id, 'sent_at': sent_at, 'enqueue_sha': sha},
    )
    await session.commit()
    return task_id


async def _insert_running_task_with_retry(
    session: AsyncSession,
    *,
    good_until: datetime,
    retry_count: int = 0,
    max_retries: int = 3,
    intervals: list[int] | None = None,
    auto_retry_for: list[str] | None = None,
) -> str:
    """Insert RUNNING task with retry policy metadata."""
    if intervals is None:
        intervals = [1, 1, 1]
    if auto_retry_for is None:
        auto_retry_for = ['TRANSIENT']

    task_id = str(uuid.uuid4())
    task_options = json.dumps({
        'retry_policy': {
            'max_retries': max_retries,
            'intervals': intervals,
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': auto_retry_for,
        },
        'good_until': good_until.isoformat(),
    })

    sent_at, sha = compute_test_enqueue_sha(
        task_name='persist_retry_test',
        good_until=good_until,
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status, sent_at,
                 created_at, updated_at, claimed, retry_count, max_retries, started_at,
                 good_until, task_options, enqueue_sha)
            VALUES
                (:id, 'persist_retry_test', 'default', 100, '[]', '{}', 'RUNNING', :sent_at,
                 NOW(), NOW(), FALSE, :retry_count, :max_retries, NOW(),
                 :good_until, :task_options, :enqueue_sha)
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'retry_count': retry_count,
            'max_retries': max_retries,
            'good_until': good_until,
            'task_options': task_options,
        },
    )
    await session.commit()
    return task_id


async def _get_task_row(
    session: AsyncSession,
    task_id: str,
) -> tuple[str, str | None, str | None, int]:
    """Read (status, result, failed_reason, retry_count) for a task."""
    row = (
        await session.execute(
            text(
                'SELECT status, result, failed_reason, retry_count '
                'FROM horsies_tasks WHERE id = :id'
            ),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'Task {task_id} not found'
    return (str(row[0]), row[1], row[2], int(row[3]))


async def _set_task_status(
    session: AsyncSession,
    task_id: str,
    status: str,
) -> None:
    """Force-set a task's status (simulating reaper intervention)."""
    await session.execute(
        text('UPDATE horsies_tasks SET status = :status WHERE id = :id'),
        {'status': status, 'id': task_id},
    )
    await session.commit()


def _serialize_ok(value: object) -> str:
    """Serialize a TaskResult(ok=value) to JSON string."""
    r = dumps_json(TaskResult(ok=value))
    assert not is_err(r), f'Serialization failed: {r}'
    return r.ok_value


def _serialize_err(error_code: str, message: str) -> str:
    """Serialize a TaskResult(err=TaskError(...)) to JSON string."""
    r = dumps_json(
        TaskResult(err=TaskError(error_code=error_code, message=message)),
    )
    assert not is_err(r), f'Serialization failed: {r}'
    return r.ok_value


# ---------------------------------------------------------------------------
# Group A: Skip paths (ok=False, special failed_reason)
# Lines 1116-1127 — short-circuit before any SQL, DB stays RUNNING.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_claim_lost_skips(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """failed_reason=CLAIM_LOST → Ok(None), task stays RUNNING."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=False,
        result_json_str='',
        failed_reason='CLAIM_LOST',
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, *_ = await _get_task_row(session, task_id)
    assert status == 'RUNNING'


@pytest.mark.asyncio(loop_scope='function')
async def test_ownership_unconfirmed_skips(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """failed_reason=OWNERSHIP_UNCONFIRMED → Ok(None), task stays RUNNING."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=False,
        result_json_str='',
        failed_reason='OWNERSHIP_UNCONFIRMED',
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, *_ = await _get_task_row(session, task_id)
    assert status == 'RUNNING'


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_check_failed_skips(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """failed_reason=WORKFLOW_CHECK_FAILED → Ok(None), task stays RUNNING."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=False,
        result_json_str='',
        failed_reason='WORKFLOW_CHECK_FAILED',
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, *_ = await _get_task_row(session, task_id)
    assert status == 'RUNNING'


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_stopped_reason_skips(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """failed_reason=WORKFLOW_STOPPED → Ok(None), task stays RUNNING."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=False,
        result_json_str='',
        failed_reason='WORKFLOW_STOPPED',
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, *_ = await _get_task_row(session, task_id)
    assert status == 'RUNNING'


# ---------------------------------------------------------------------------
# Group B: Worker-level failure (ok=False, generic/None reason)
# Lines 1131-1147
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_worker_failure_marks_failed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Generic worker failure → Ok(None), task becomes FAILED with reason."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=False,
        result_json_str='',
        failed_reason='Worker crash',
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, _, failed_reason, _ = await _get_task_row(session, task_id)
    assert status == 'FAILED'
    assert failed_reason == 'Worker crash'


@pytest.mark.asyncio(loop_scope='function')
async def test_worker_failure_reaper_already_reclaimed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Reaper pre-set to FAILED → guard blocks update, stays FAILED, Ok(None)."""
    task_id = await _insert_running_task(session)
    await _set_task_status(session, task_id, 'FAILED')
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=False,
        result_json_str='',
        failed_reason='Worker crash',
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, *_ = await _get_task_row(session, task_id)
    assert status == 'FAILED'


# ---------------------------------------------------------------------------
# Group C: Corrupt/invalid result JSON (ok=True)
# Lines 1150-1181
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_corrupt_json_marks_serialization_error(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Unparseable JSON → FAILED with WORKER_SERIALIZATION_ERROR."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=True,
        result_json_str='not json {',
        failed_reason=None,
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, result_json, _, _ = await _get_task_row(session, task_id)
    assert status == 'FAILED'
    assert result_json is not None
    assert 'WORKER_SERIALIZATION_ERROR' in result_json


@pytest.mark.asyncio(loop_scope='function')
async def test_invalid_task_result_structure_marks_error(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Valid JSON but not a TaskResult shape → FAILED with WORKER_SERIALIZATION_ERROR."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=True,
        result_json_str='{"random": 1}',
        failed_reason=None,
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, result_json, _, _ = await _get_task_row(session, task_id)
    assert status == 'FAILED'
    assert result_json is not None
    assert 'WORKER_SERIALIZATION_ERROR' in result_json


# ---------------------------------------------------------------------------
# Group D: Error result with WORKFLOW_STOPPED code
# Lines 1185-1191
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_err_workflow_stopped_code_skips(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """TaskResult with err.error_code=WORKFLOW_STOPPED → Ok(None), stays RUNNING."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_err('WORKFLOW_STOPPED', 'workflow was stopped')

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=True,
        result_json_str=result_json,
        failed_reason=None,
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, *_ = await _get_task_row(session, task_id)
    assert status == 'RUNNING'


# ---------------------------------------------------------------------------
# Group E: Error result, retry logic
# Lines 1194-1224
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_err_no_retry_marks_failed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Error result with no retry config → Ok(tr) with tr.is_err(), task FAILED."""
    task_id = await _insert_running_task(session)  # max_retries=0
    worker = _make_worker(engine)
    result_json = _serialize_err('TRANSIENT', 'some transient error')

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=True,
        result_json_str=result_json,
        failed_reason=None,
    )

    assert is_ok(result)
    tr = result.ok_value
    assert tr is not None
    assert tr.is_err()
    status, db_result, _, _ = await _get_task_row(session, task_id)
    assert status == 'FAILED'
    assert db_result == result_json


@pytest.mark.asyncio(loop_scope='function')
async def test_err_retry_scheduled_to_pending(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Retryable error with capacity → Ok(None), task becomes PENDING, retry_count=1."""
    good_until = datetime.now(timezone.utc) + timedelta(hours=1)
    task_id = await _insert_running_task_with_retry(
        session,
        good_until=good_until,
        retry_count=0,
        max_retries=3,
        intervals=[1, 1, 1],
        auto_retry_for=['TRANSIENT'],
    )
    worker = _make_worker(engine)
    result_json = _serialize_err('TRANSIENT', 'retryable error')

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=True,
        result_json_str=result_json,
        failed_reason=None,
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, _, _, retry_count = await _get_task_row(session, task_id)
    assert status == 'PENDING'
    assert retry_count == 1


@pytest.mark.asyncio(loop_scope='function')
async def test_err_retry_expired_falls_through_to_failed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Retryable error but next_retry >> good_until → falls through to FAILED."""
    # good_until is very soon, but retry interval is 1 hour — next_retry will exceed it
    good_until = datetime.now(timezone.utc) + timedelta(seconds=2)
    task_id = await _insert_running_task_with_retry(
        session,
        good_until=good_until,
        retry_count=0,
        max_retries=3,
        intervals=[3600, 3600, 3600],
        auto_retry_for=['TRANSIENT'],
    )
    worker = _make_worker(engine)
    result_json = _serialize_err('TRANSIENT', 'retryable but expired')

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=True,
        result_json_str=result_json,
        failed_reason=None,
    )

    assert is_ok(result)
    tr = result.ok_value
    assert tr is not None
    assert tr.is_err()
    status, db_result, _, _ = await _get_task_row(session, task_id)
    assert status == 'FAILED'
    assert db_result == result_json


# ---------------------------------------------------------------------------
# Group F: Success path
# Lines 1225-1238
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_success_marks_completed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Valid ok TaskResult → Ok(tr) with tr.is_ok(), task COMPLETED."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok('hello')

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=True,
        result_json_str=result_json,
        failed_reason=None,
    )

    assert is_ok(result)
    tr = result.ok_value
    assert tr is not None
    assert tr.is_ok()
    assert tr.unwrap() == 'hello'
    status, db_result, _, _ = await _get_task_row(session, task_id)
    assert status == 'COMPLETED'
    assert db_result == result_json


@pytest.mark.asyncio(loop_scope='function')
async def test_success_reaper_reclaimed_no_mutation(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Reaper pre-set to FAILED → guard blocks COMPLETED, Ok(None)."""
    task_id = await _insert_running_task(session)
    await _set_task_status(session, task_id, 'FAILED')
    worker = _make_worker(engine)
    result_json = _serialize_ok('late success')

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=NOW,
        ok=True,
        result_json_str=result_json,
        failed_reason=None,
    )

    assert is_ok(result)
    assert result.ok_value is None
    status, *_ = await _get_task_row(session, task_id)
    assert status == 'FAILED'
