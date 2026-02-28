"""Integration tests for finalize-path status guards.

Proves that a late-returning worker cannot overwrite a task's status after the
reaper has already reclaimed it.  Each test inserts a task in RUNNING state,
simulates the reaper moving it to FAILED/PENDING, then fires the guarded SQL
and asserts the UPDATE is a no-op (RETURNING yields no row).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.worker.worker import (
    GET_TASK_RETRY_CONFIG_SQL,
    GET_TASK_RETRY_INFO_SQL,
    MARK_TASK_COMPLETED_SQL,
    MARK_TASK_FAILED_SQL,
    MARK_TASK_FAILED_WORKER_SQL,
    SCHEDULE_TASK_RETRY_SQL,
)

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _insert_running_task(session: AsyncSession) -> str:
    """Insert a minimal horsies_tasks row in RUNNING state and return its id."""
    task_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at)
            VALUES
                (:id, 'guard_test', 'default', 100, '[]', '{}',
                 'RUNNING', NOW(), NOW(), NOW(), FALSE, 0,
                 3, NOW())
        """),
        {'id': task_id},
    )
    await session.flush()
    return task_id


async def _insert_running_task_with_retry(
    session: AsyncSession,
    *,
    good_until: datetime,
    retry_count: int = 0,
    max_retries: int = 3,
    intervals: list[int] | None = None,
) -> str:
    """Insert RUNNING task row with retry policy metadata."""
    if intervals is None:
        intervals = [1, 1, 1]

    task_id = str(uuid.uuid4())
    task_options = json.dumps({
        'retry_policy': {
            'max_retries': max_retries,
            'intervals': intervals,
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': ['TRANSIENT'],
        },
        'good_until': good_until.isoformat(),
    })

    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status, sent_at,
                 created_at, updated_at, claimed, retry_count, max_retries, started_at,
                 good_until, task_options)
            VALUES
                (:id, 'guard_test_retry', 'default', 100, '[]', '{}', 'RUNNING', NOW(),
                 NOW(), NOW(), FALSE, :retry_count, :max_retries, NOW(),
                 :good_until, :task_options)
        """),
        {
            'id': task_id,
            'retry_count': retry_count,
            'max_retries': max_retries,
            'good_until': good_until,
            'task_options': task_options,
        },
    )
    await session.flush()
    return task_id


async def _get_task_status(session: AsyncSession, task_id: str) -> str:
    """Read current status of a task."""
    result = await session.execute(
        text("SELECT status FROM horsies_tasks WHERE id = :id"),
        {'id': task_id},
    )
    row = result.fetchone()
    assert row is not None, f'Task {task_id} not found'
    return str(row[0])


async def _set_task_status(
    session: AsyncSession,
    task_id: str,
    status: str,
) -> None:
    """Force-set a task's status (simulating reaper intervention)."""
    await session.execute(
        text("UPDATE horsies_tasks SET status = :status WHERE id = :id"),
        {'status': status, 'id': task_id},
    )
    await session.flush()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_complete_guard_blocks_when_not_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """MARK_TASK_COMPLETED_SQL is a no-op when task is no longer RUNNING."""
    task_id = await _insert_running_task(session)

    # Simulate reaper marking task as FAILED
    await _set_task_status(session, task_id, 'FAILED')

    # Late finalize attempt — should return no rows
    now = datetime.now(timezone.utc)
    result = await session.execute(
        MARK_TASK_COMPLETED_SQL,
        {'now': now, 'result_json': '{"ok": "late_result"}', 'id': task_id},
    )
    returned_row = result.fetchone()

    assert returned_row is None, (
        'MARK_TASK_COMPLETED_SQL must be a no-op when status != RUNNING'
    )
    assert await _get_task_status(session, task_id) == 'FAILED', (
        'Task status must remain FAILED after blocked finalize'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_complete_guard_succeeds_when_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """MARK_TASK_COMPLETED_SQL works normally when task IS running."""
    task_id = await _insert_running_task(session)

    now = datetime.now(timezone.utc)
    result = await session.execute(
        MARK_TASK_COMPLETED_SQL,
        {'now': now, 'result_json': '{"ok": "result"}', 'id': task_id},
    )
    returned_row = result.fetchone()

    assert returned_row is not None, (
        'MARK_TASK_COMPLETED_SQL must return a row when status == RUNNING'
    )
    assert await _get_task_status(session, task_id) == 'COMPLETED'


@pytest.mark.asyncio(loop_scope='function')
async def test_fail_guard_blocks_when_not_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """MARK_TASK_FAILED_SQL is a no-op when task is no longer RUNNING."""
    task_id = await _insert_running_task(session)

    # Simulate reaper rescheduling a retry (RUNNING → PENDING)
    await _set_task_status(session, task_id, 'PENDING')

    now = datetime.now(timezone.utc)
    result = await session.execute(
        MARK_TASK_FAILED_SQL,
        {'now': now, 'result_json': '{"err": {"error_code": "LATE"}}', 'id': task_id},
    )
    returned_row = result.fetchone()

    assert returned_row is None, (
        'MARK_TASK_FAILED_SQL must be a no-op when status != RUNNING'
    )
    assert await _get_task_status(session, task_id) == 'PENDING', (
        'Task status must remain PENDING after blocked finalize'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_fail_worker_guard_blocks_when_not_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """MARK_TASK_FAILED_WORKER_SQL is a no-op when task is no longer RUNNING."""
    task_id = await _insert_running_task(session)

    # Simulate reaper intervention
    await _set_task_status(session, task_id, 'FAILED')

    now = datetime.now(timezone.utc)
    result = await session.execute(
        MARK_TASK_FAILED_WORKER_SQL,
        {'now': now, 'reason': 'late worker failure', 'id': task_id},
    )
    returned_row = result.fetchone()

    assert returned_row is None, (
        'MARK_TASK_FAILED_WORKER_SQL must be a no-op when status != RUNNING'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_retry_guard_blocks_when_not_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """SCHEDULE_TASK_RETRY_SQL is a no-op when task is no longer RUNNING."""
    task_id = await _insert_running_task(session)

    # Simulate reaper already marking FAILED
    await _set_task_status(session, task_id, 'FAILED')

    next_retry = datetime.now(timezone.utc)
    result = await session.execute(
        SCHEDULE_TASK_RETRY_SQL,
        {'id': task_id, 'retry_count': 1, 'next_retry_at': next_retry},
    )
    returned_row = result.fetchone()

    assert returned_row is None, (
        'SCHEDULE_TASK_RETRY_SQL must be a no-op when status != RUNNING'
    )
    assert await _get_task_status(session, task_id) == 'FAILED', (
        'Task status must remain FAILED after blocked retry'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_retry_guard_succeeds_when_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """SCHEDULE_TASK_RETRY_SQL works normally when task IS running."""
    task_id = await _insert_running_task(session)

    next_retry = datetime.now(timezone.utc)
    result = await session.execute(
        SCHEDULE_TASK_RETRY_SQL,
        {'id': task_id, 'retry_count': 1, 'next_retry_at': next_retry},
    )
    returned_row = result.fetchone()

    assert returned_row is not None, (
        'SCHEDULE_TASK_RETRY_SQL must return a row when status == RUNNING'
    )
    assert await _get_task_status(session, task_id) == 'PENDING'


@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_then_complete_race_sequence(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Full race scenario: reaper marks FAILED, late worker tries COMPLETED.

    Timeline:
      T=0  Task is RUNNING
      T=1  Reaper marks FAILED (simulated via MARK_STALE_TASK_FAILED_SQL pattern)
      T=2  Late worker finalize fires MARK_TASK_COMPLETED_SQL → blocked
      T=3  Assert: task is FAILED, result is reaper's WORKER_CRASHED, not worker's
    """
    task_id = await _insert_running_task(session)

    # T=1: Reaper marks FAILED with WORKER_CRASHED result
    reaper_result = '{"err": {"error_code": "WORKER_CRASHED", "message": "stale"}}'
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = 'FAILED',
                failed_at = NOW(),
                result = :result,
                updated_at = NOW()
            WHERE id = :id AND status = 'RUNNING'
        """),
        {'id': task_id, 'result': reaper_result},
    )
    await session.flush()

    # T=2: Late worker finalize fires — should be blocked
    now = datetime.now(timezone.utc)
    late_result = '{"ok": "I completed successfully!"}'
    comp_res = await session.execute(
        MARK_TASK_COMPLETED_SQL,
        {'now': now, 'result_json': late_result, 'id': task_id},
    )
    assert comp_res.fetchone() is None, 'Late COMPLETED must be blocked'

    # T=3: Verify reaper's result is preserved
    row = (
        await session.execute(
            text("SELECT status, result FROM horsies_tasks WHERE id = :id"),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    assert row[0] == 'FAILED', f'Expected FAILED, got {row[0]}'
    assert row[1] == reaper_result, (
        f'Reaper result must be preserved, got: {row[1]}'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_then_retry_race_sequence(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Race scenario: reaper marks FAILED, late worker tries to schedule retry.

    Timeline:
      T=0  Task is RUNNING
      T=1  Reaper marks FAILED
      T=2  Late worker tries SCHEDULE_TASK_RETRY_SQL → blocked
      T=3  Assert: task is FAILED, not PENDING (retry was blocked)
    """
    task_id = await _insert_running_task(session)

    # T=1: Reaper marks FAILED
    await _set_task_status(session, task_id, 'FAILED')

    # T=2: Late worker tries to schedule a retry
    next_retry = datetime.now(timezone.utc)
    retry_res = await session.execute(
        SCHEDULE_TASK_RETRY_SQL,
        {'id': task_id, 'retry_count': 1, 'next_retry_at': next_retry},
    )
    assert retry_res.fetchone() is None, 'Late retry must be blocked'

    # T=3: Task must remain FAILED
    assert await _get_task_status(session, task_id) == 'FAILED'


@pytest.mark.asyncio(loop_scope='function')
async def test_retry_info_sql_returns_good_until_and_db_now(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """GET_TASK_RETRY_INFO_SQL should include good_until and db_now."""
    expected_good_until = datetime.now(timezone.utc) + timedelta(seconds=60)
    task_id = await _insert_running_task_with_retry(
        session, good_until=expected_good_until
    )

    row = (
        await session.execute(GET_TASK_RETRY_INFO_SQL, {'id': task_id})
    ).fetchone()
    assert row is not None

    good_until = row.good_until
    db_now = row.db_now
    assert good_until is not None
    assert db_now is not None
    if good_until.tzinfo is None:
        good_until = good_until.replace(tzinfo=timezone.utc)
    if db_now.tzinfo is None:
        db_now = db_now.replace(tzinfo=timezone.utc)
    assert abs((good_until - expected_good_until).total_seconds()) < 2.0
    assert abs((datetime.now(timezone.utc) - db_now).total_seconds()) < 10.0


@pytest.mark.asyncio(loop_scope='function')
async def test_retry_config_sql_returns_good_until_and_db_now(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """GET_TASK_RETRY_CONFIG_SQL should include good_until and db_now."""
    past_good_until = datetime.now(timezone.utc) - timedelta(seconds=10)
    task_id = await _insert_running_task_with_retry(
        session,
        good_until=past_good_until,
        intervals=[60],
    )

    row = (
        await session.execute(GET_TASK_RETRY_CONFIG_SQL, {'id': task_id})
    ).fetchone()
    assert row is not None

    good_until = row.good_until
    db_now = row.db_now
    assert good_until is not None
    assert db_now is not None
    if good_until.tzinfo is None:
        good_until = good_until.replace(tzinfo=timezone.utc)
    if db_now.tzinfo is None:
        db_now = db_now.replace(tzinfo=timezone.utc)
    assert good_until < db_now
