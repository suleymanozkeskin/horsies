"""Integration tests for task attempt history (horsies_task_attempts).

Covers attempt-row creation at every terminal-state path:
1. Success → COMPLETED attempt
2. Terminal domain failure → FAILED attempt, task error_code set
3. Retryable domain failure → FAILED attempt with will_retry=True
4. Worker-level failure → WORKER_FAILURE attempt
5. Stale runner cleanup → FAILED attempt with WORKER_CRASHED
5b. Stale runner cleanup with retry policy → FAILED attempt, will_retry=True, task PENDING
6. Replay idempotency → no duplicate rows on re-finalization
7. Pre-exec aborts → no attempt row written
8. Expired PENDING → EXPIRED status with TASK_EXPIRED result, no attempt row
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import dumps_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

NOW = datetime.now(timezone.utc)


@pytest.fixture(autouse=True)
def _ensure_schema(broker: PostgresBroker) -> None:  # noqa: ARG001
    """Ensure DB schema (including horsies_task_attempts) exists before tests."""


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


async def _insert_running_task(
    session: AsyncSession,
    *,
    task_name: str = 'attempt_history_test',
    retry_count: int = 0,
    max_retries: int = 0,
    worker_id: str = 'w-test-1',
    worker_hostname: str = 'host-test',
    worker_pid: int = 9999,
    worker_process_name: str = 'proc-test',
    good_until: datetime | None = None,
    task_options: str | None = None,
) -> str:
    """Insert a RUNNING task with worker identity for attempt context."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(
        task_name=task_name,
        good_until=good_until,
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha,
                 claimed_by_worker_id, worker_hostname, worker_pid,
                 worker_process_name, good_until, task_options)
            VALUES
                (:id, :task_name, 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, :retry_count,
                 :max_retries, NOW(), :enqueue_sha,
                 :worker_id, :worker_hostname, :worker_pid,
                 :worker_process_name, :good_until, :task_options)
        """),
        {
            'id': task_id,
            'task_name': task_name,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'retry_count': retry_count,
            'max_retries': max_retries,
            'worker_id': worker_id,
            'worker_hostname': worker_hostname,
            'worker_pid': worker_pid,
            'worker_process_name': worker_process_name,
            'good_until': good_until,
            'task_options': task_options,
        },
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
    """Insert RUNNING task with retry policy metadata and worker identity."""
    if intervals is None:
        intervals = [1, 1, 1]
    if auto_retry_for is None:
        auto_retry_for = ['TRANSIENT']

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

    return await _insert_running_task(
        session,
        task_name='attempt_retry_test',
        retry_count=retry_count,
        max_retries=max_retries,
        good_until=good_until,
        task_options=task_options,
    )


async def _get_attempts(
    session: AsyncSession,
    task_id: str,
) -> list[dict[str, Any]]:
    """Read all attempt rows for a task, ordered by attempt ASC."""
    result = await session.execute(
        text("""
            SELECT task_id, attempt, outcome, will_retry,
                   started_at, finished_at,
                   error_code, error_message, failed_reason,
                   worker_id, worker_hostname, worker_pid, worker_process_name
            FROM horsies_task_attempts
            WHERE task_id = :task_id
            ORDER BY attempt ASC
        """),
        {'task_id': task_id},
    )
    rows = result.fetchall()
    cols = [
        'task_id', 'attempt', 'outcome', 'will_retry',
        'started_at', 'finished_at',
        'error_code', 'error_message', 'failed_reason',
        'worker_id', 'worker_hostname', 'worker_pid', 'worker_process_name',
    ]
    return [dict(zip(cols, row)) for row in rows]


async def _get_task_error_code(
    session: AsyncSession,
    task_id: str,
) -> str | None:
    """Read horsies_tasks.error_code for a task."""
    row = (
        await session.execute(
            text('SELECT error_code FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'Task {task_id} not found'
    return row[0]


def _serialize_ok(value: object) -> str:
    r = dumps_json(TaskResult(ok=value))
    assert not is_err(r)
    return r.ok_value


def _serialize_err(error_code: str, message: str) -> str:
    r = dumps_json(
        TaskResult(err=TaskError(error_code=error_code, message=message)),
    )
    assert not is_err(r)
    return r.ok_value


# ---------------------------------------------------------------------------
# 1. Success → COMPLETED attempt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_success_writes_completed_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Successful TaskResult writes one COMPLETED attempt row."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok(42)

    result = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=True,
        result_json_str=result_json, failed_reason=None,
    )

    assert is_ok(result)
    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 1

    att = attempts[0]
    assert att['attempt'] == 1
    assert att['outcome'] == 'COMPLETED'
    assert att['will_retry'] is False
    assert att['error_code'] is None
    assert att['error_message'] is None
    assert att['failed_reason'] is None
    assert att['worker_id'] == 'w-test-1'
    assert att['worker_hostname'] == 'host-test'
    assert att['worker_pid'] == 9999
    assert att['worker_process_name'] == 'proc-test'
    assert att['started_at'] is not None
    assert att['finished_at'] is not None


# ---------------------------------------------------------------------------
# 2. Terminal domain failure → FAILED attempt + task error_code
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_terminal_failure_writes_failed_attempt_and_error_code(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Terminal error writes FAILED attempt and sets horsies_tasks.error_code."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_err('DOMAIN_ERROR', 'something broke')

    result = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=True,
        result_json_str=result_json, failed_reason=None,
    )

    assert is_ok(result)
    tr = result.ok_value
    assert tr is not None and tr.is_err()

    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 1

    att = attempts[0]
    assert att['attempt'] == 1
    assert att['outcome'] == 'FAILED'
    assert att['will_retry'] is False
    assert att['error_code'] == 'DOMAIN_ERROR'
    assert att['error_message'] == 'something broke'
    assert att['failed_reason'] is None

    # Task row must also have error_code
    task_ec = await _get_task_error_code(session, task_id)
    assert task_ec == 'DOMAIN_ERROR'


# ---------------------------------------------------------------------------
# 3. Retryable failure → FAILED attempt with will_retry=True
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_retryable_failure_writes_attempt_with_will_retry(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Retryable error with capacity writes FAILED attempt, will_retry=True,
    and leaves horsies_tasks.error_code as NULL."""
    good_until = datetime.now(timezone.utc) + timedelta(hours=1)
    task_id = await _insert_running_task_with_retry(
        session, good_until=good_until,
        retry_count=0, max_retries=3,
        intervals=[1, 1, 1], auto_retry_for=['TRANSIENT'],
    )
    worker = _make_worker(engine)
    result_json = _serialize_err('TRANSIENT', 'retryable error')

    result = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=True,
        result_json_str=result_json, failed_reason=None,
    )

    assert is_ok(result)
    assert result.ok_value is None  # retry path returns None

    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 1

    att = attempts[0]
    assert att['attempt'] == 1
    assert att['outcome'] == 'FAILED'
    assert att['will_retry'] is True
    assert att['error_code'] == 'TRANSIENT'
    assert att['error_message'] == 'retryable error'

    # Task error_code stays NULL during retry
    task_ec = await _get_task_error_code(session, task_id)
    assert task_ec is None


# ---------------------------------------------------------------------------
# 4. Worker-level failure → WORKER_FAILURE attempt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_worker_failure_writes_worker_failure_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Worker crash (ok=False) writes one WORKER_FAILURE attempt row."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=False,
        result_json_str='', failed_reason='Segfault in child',
    )

    assert is_ok(result)
    assert result.ok_value is None

    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 1

    att = attempts[0]
    assert att['attempt'] == 1
    assert att['outcome'] == 'WORKER_FAILURE'
    assert att['will_retry'] is False
    assert att['error_code'] is None
    assert att['error_message'] is None
    assert att['failed_reason'] == 'Segfault in child'
    assert att['worker_id'] == 'w-test-1'

    # Task error_code stays NULL for worker-level failures
    task_ec = await _get_task_error_code(session, task_id)
    assert task_ec is None


# ---------------------------------------------------------------------------
# 5. Stale runner cleanup → FAILED attempt with WORKER_CRASHED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_stale_cleanup_writes_failed_attempt_worker_crashed(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """mark_stale_tasks_as_failed writes one FAILED attempt with WORKER_CRASHED."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='stale_attempt_test')
    # Insert a RUNNING task with started_at far in the past and no heartbeat
    stale_started_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha,
                 claimed_by_worker_id, worker_hostname, worker_pid,
                 worker_process_name)
            VALUES
                (:id, 'stale_attempt_test', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 0, :stale_started_at, :enqueue_sha,
                 'w-stale-1', 'stale-host', 1234, 'stale-proc')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'stale_started_at': stale_started_at,
        },
    )
    await session.commit()

    # Use a very short threshold so the task is definitely stale
    result = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=1_000)

    assert is_ok(result)
    assert result.ok_value >= 1

    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 1

    att = attempts[0]
    assert att['attempt'] == 1
    assert att['outcome'] == 'FAILED'
    assert att['will_retry'] is False
    assert att['error_code'] == 'WORKER_CRASHED'
    assert att['error_message'] is not None
    assert 'Worker process crashed' in att['error_message']
    assert att['failed_reason'] is not None
    assert att['worker_id'] == 'w-stale-1'
    assert att['worker_hostname'] == 'stale-host'
    assert att['worker_pid'] == 1234
    assert att['worker_process_name'] == 'stale-proc'

    task_ec = await _get_task_error_code(session, task_id)
    assert task_ec == 'WORKER_CRASHED'


# ---------------------------------------------------------------------------
# 5b. Stale runner cleanup with retry policy → FAILED attempt, will_retry=True
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_stale_cleanup_with_retry_policy_schedules_retry(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """mark_stale_tasks_as_failed retries when WORKER_CRASHED is in auto_retry_for."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='stale_retry_test')
    stale_started_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    task_options = json.dumps({
        'task_name': 'stale_retry_test',
        'retry_policy': {
            'max_retries': 3,
            'intervals': [60, 300, 900],
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': ['WORKER_CRASHED'],
        },
    })
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha, task_options,
                 claimed_by_worker_id, worker_hostname, worker_pid,
                 worker_process_name)
            VALUES
                (:id, 'stale_retry_test', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 3, :stale_started_at, :enqueue_sha, :task_options,
                 'w-stale-retry', 'stale-host', 9999, 'stale-proc')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'stale_started_at': stale_started_at,
            'task_options': task_options,
        },
    )
    await session.commit()

    result = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=1_000)

    assert is_ok(result)
    assert result.ok_value >= 1

    # Task should now be PENDING (retried), not FAILED
    row = (
        await session.execute(
            text('SELECT status, retry_count, next_retry_at, error_code FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    assert row[0] == 'PENDING', f'Expected PENDING after retry, got {row[0]}'
    assert row[1] == 1, f'Expected retry_count=1, got {row[1]}'
    assert row[2] is not None, 'next_retry_at should be set'
    assert row[3] is None, 'error_code should be cleared on retry'

    # Attempt row should say will_retry=True
    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 1
    att = attempts[0]
    assert att['attempt'] == 1
    assert att['outcome'] == 'FAILED'
    assert att['will_retry'] is True
    assert att['error_code'] == 'WORKER_CRASHED'
    assert att['worker_id'] == 'w-stale-retry'


# ---------------------------------------------------------------------------
# 6. Replay idempotency → no duplicate attempt rows
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_replay_does_not_create_duplicate_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Calling _persist_task_terminal_state twice for the same task
    results in exactly one attempt row due to UPSERT idempotency."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok('first')

    # First finalization: succeeds
    r1 = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=True,
        result_json_str=result_json, failed_reason=None,
    )
    assert is_ok(r1)

    attempts_after_first = await _get_attempts(session, task_id)
    assert len(attempts_after_first) == 1

    # Second finalization: task is no longer RUNNING → SELECT FOR UPDATE
    # returns no row → early exit with Ok(None), no attempt written.
    r2 = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=True,
        result_json_str=result_json, failed_reason=None,
    )
    assert is_ok(r2)
    assert r2.ok_value is None  # guard caught it

    attempts_after_second = await _get_attempts(session, task_id)
    assert len(attempts_after_second) == 1  # still just one


# ---------------------------------------------------------------------------
# 7. Pre-exec aborts → no attempt row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_claim_lost_writes_no_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """CLAIM_LOST abort does not write any attempt row."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=False,
        result_json_str='', failed_reason='CLAIM_LOST',
    )

    assert is_ok(result)
    assert result.ok_value is None
    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_stopped_error_code_writes_no_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """WORKFLOW_STOPPED in TaskResult error_code does not write attempt row."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_err('WORKFLOW_STOPPED', 'workflow was stopped')

    result = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=True,
        result_json_str=result_json, failed_reason=None,
    )

    assert is_ok(result)
    assert result.ok_value is None
    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 0


# ---------------------------------------------------------------------------
# 8. Corrupt JSON → FAILED attempt with WORKER_SERIALIZATION_ERROR
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_corrupt_json_writes_serialization_error_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Corrupt result JSON writes a FAILED attempt with WORKER_SERIALIZATION_ERROR."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)

    result = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=True,
        result_json_str='not json {', failed_reason=None,
    )

    assert is_ok(result)
    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 1

    att = attempts[0]
    assert att['attempt'] == 1
    assert att['outcome'] == 'FAILED'
    assert att['will_retry'] is False
    assert att['error_code'] == 'WORKER_SERIALIZATION_ERROR'
    assert att['error_message'] is not None
    assert 'corrupt' in att['error_message'].lower()

    task_ec = await _get_task_error_code(session, task_id)
    assert task_ec == 'WORKER_SERIALIZATION_ERROR'


# ---------------------------------------------------------------------------
# 9. Attempt number increments with retry_count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_attempt_number_reflects_retry_count(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A task with retry_count=2 writes attempt=3."""
    task_id = await _insert_running_task(session, retry_count=2)
    worker = _make_worker(engine)
    result_json = _serialize_ok('after retries')

    result = await worker._persist_task_terminal_state(
        task_id=task_id, now=NOW, ok=True,
        result_json_str=result_json, failed_reason=None,
    )

    assert is_ok(result)
    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 1
    assert attempts[0]['attempt'] == 3
    assert attempts[0]['outcome'] == 'COMPLETED'


# ---------------------------------------------------------------------------
# 8. Expired PENDING task → EXPIRED status with TASK_EXPIRED result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_expire_pending_task_transitions_to_expired(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """expire_pending_tasks transitions PENDING + past good_until to EXPIRED."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='expire_test')
    past_good_until = datetime.now(timezone.utc) - timedelta(minutes=10)
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, enqueue_sha, good_until)
            VALUES
                (:id, 'expire_test', 'default', 100, '[]', '{}',
                 'PENDING', :sent_at, NOW(), NOW(), FALSE, 0,
                 0, :enqueue_sha, :good_until)
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'good_until': past_good_until,
        },
    )
    await session.commit()

    result = await broker.expire_pending_tasks()

    assert is_ok(result)
    assert result.ok_value >= 1

    row = (
        await session.execute(
            text('SELECT status, error_code, result FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    assert row[0] == 'EXPIRED', f'Expected EXPIRED, got {row[0]}'
    assert row[1] == 'TASK_EXPIRED'
    assert row[2] is not None
    assert 'TASK_EXPIRED' in row[2]

    # No attempt row should exist (task never started)
    attempts = await _get_attempts(session, task_id)
    assert len(attempts) == 0
