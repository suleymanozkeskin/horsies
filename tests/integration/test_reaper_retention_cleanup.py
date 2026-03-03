"""Integration tests for the reaper retention cleanup SQL.

Verifies that the five DELETE statements used by the reaper loop
correctly prune old rows while preserving recent and non-terminal records.
The mock-based orchestration tests live in test_worker_helpers.py;
these tests validate the SQL against real DB rows.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.worker.sql import (
    DELETE_EXPIRED_HEARTBEATS_SQL,
    DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL,
    DELETE_EXPIRED_TASKS_SQL,
    WORKFLOW_TERMINAL_VALUES,
    TASK_TERMINAL_VALUES,
)

pytestmark = [pytest.mark.integration]

# Retention window used in all tests — 24 hours.
_RETENTION_HOURS = 24


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _count(session: AsyncSession, table: str) -> int:
    """Count rows in a table."""
    row = (await session.execute(text(f'SELECT count(*) FROM {table}'))).fetchone()  # noqa: S608
    assert row is not None
    return int(row[0])


async def _truncate_retention_tables(session: AsyncSession) -> None:
    """Truncate all tables touched by retention cleanup."""
    await session.execute(text(
        'TRUNCATE horsies_heartbeats, horsies_worker_states, '
        'horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'
    ))
    await session.commit()


# ---------------------------------------------------------------------------
# I-8a: Heartbeats — old deleted, recent kept
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_expired_heartbeats_deleted(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """Heartbeat rows older than retention_hours are deleted; recent ones survive."""
    await _truncate_retention_tables(session)

    # Insert an old heartbeat (48h ago) and a recent one (1h ago)
    await session.execute(text("""
        INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at)
        VALUES
            (:old_id, 'worker-1', 'claimer', NOW() - INTERVAL '48 hours'),
            (:new_id, 'worker-1', 'claimer', NOW() - INTERVAL '1 hour')
    """), {'old_id': str(uuid.uuid4()), 'new_id': str(uuid.uuid4())})
    await session.commit()

    assert await _count(session, 'horsies_heartbeats') == 2

    result = await session.execute(
        DELETE_EXPIRED_HEARTBEATS_SQL,
        {'retention_hours': _RETENTION_HOURS},
    )
    await session.commit()

    assert int(result.rowcount or 0) == 1
    assert await _count(session, 'horsies_heartbeats') == 1


# ---------------------------------------------------------------------------
# I-8b: Worker states — old deleted, recent kept
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_expired_worker_states_deleted(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """Worker state rows older than retention_hours are deleted; recent ones survive."""
    await _truncate_retention_tables(session)

    # Insert old and recent worker state rows.
    # WorkerStateModel has many NOT NULL columns — supply them all.
    await session.execute(text("""
        INSERT INTO horsies_worker_states
            (worker_id, snapshot_at, hostname, pid, processes,
             max_claim_batch, max_claim_per_worker,
             queues, tasks_running, tasks_claimed,
             worker_started_at)
        VALUES
            ('w1', NOW() - INTERVAL '48 hours', 'host', 1, 1,
             10, 10,
             '{default}', 0, 0,
             NOW() - INTERVAL '48 hours'),
            ('w1', NOW() - INTERVAL '1 hour', 'host', 1, 1,
             10, 10,
             '{default}', 0, 0,
             NOW() - INTERVAL '2 hours')
    """))
    await session.commit()

    assert await _count(session, 'horsies_worker_states') == 2

    result = await session.execute(
        DELETE_EXPIRED_WORKER_STATES_SQL,
        {'retention_hours': _RETENTION_HOURS},
    )
    await session.commit()

    assert int(result.rowcount or 0) == 1
    assert await _count(session, 'horsies_worker_states') == 1


# ---------------------------------------------------------------------------
# I-8c: Terminal workflows + workflow_tasks — old deleted, running untouched
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_expired_terminal_workflows_deleted(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """Terminal workflows (COMPLETED/FAILED) older than retention_hours are deleted
    along with their workflow_tasks. RUNNING workflows are untouched."""
    await _truncate_retention_tables(session)

    old_completed_wf = str(uuid.uuid4())
    old_running_wf = str(uuid.uuid4())
    recent_completed_wf = str(uuid.uuid4())

    # Old COMPLETED workflow (48h ago) — should be deleted
    await session.execute(text("""
        INSERT INTO horsies_workflows
            (id, name, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at, completed_at)
        VALUES
            (:id, 'wf_old_done', 'COMPLETED', 'FAIL', 0, :id,
             NOW(), NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours')
    """), {'id': old_completed_wf})

    # Old RUNNING workflow (48h ago) — should survive (not terminal)
    await session.execute(text("""
        INSERT INTO horsies_workflows
            (id, name, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at)
        VALUES
            (:id, 'wf_old_running', 'RUNNING', 'FAIL', 0, :id,
             NOW(), NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours')
    """), {'id': old_running_wf})

    # Recent COMPLETED workflow (1h ago) — should survive (within retention)
    await session.execute(text("""
        INSERT INTO horsies_workflows
            (id, name, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at, completed_at)
        VALUES
            (:id, 'wf_recent_done', 'COMPLETED', 'FAIL', 0, :id,
             NOW(), NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
             NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour')
    """), {'id': recent_completed_wf})

    # Add workflow_tasks to old_completed and old_running workflows
    for wf_id in (old_completed_wf, old_running_wf):
        await session.execute(text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                 queue_name, priority, dependencies, allow_failed_deps, join_type,
                 is_subworkflow, status, created_at)
            VALUES
                (:id, :wf_id, 0, 'node_0', 'retention_test', '[]', '{}',
                 'default', 100, '{}', FALSE, 'all',
                 FALSE, 'COMPLETED', NOW())
        """), {'id': str(uuid.uuid4()), 'wf_id': wf_id})

    await session.commit()

    assert await _count(session, 'horsies_workflows') == 3
    assert await _count(session, 'horsies_workflow_tasks') == 2

    wf_params = {
        'retention_hours': _RETENTION_HOURS,
        'wf_terminal_states': WORKFLOW_TERMINAL_VALUES,
    }

    # Delete workflow_tasks first (FK dependency), then workflows
    wt_result = await session.execute(DELETE_EXPIRED_WORKFLOW_TASKS_SQL, wf_params)
    wf_result = await session.execute(DELETE_EXPIRED_WORKFLOWS_SQL, wf_params)
    await session.commit()

    # Only the old completed workflow's task should be deleted
    assert int(wt_result.rowcount or 0) == 1
    # Only the old completed workflow itself should be deleted
    assert int(wf_result.rowcount or 0) == 1

    # Survivors: old_running + recent_completed workflows
    assert await _count(session, 'horsies_workflows') == 2
    # Survivor: old_running's workflow_task
    assert await _count(session, 'horsies_workflow_tasks') == 1


# ---------------------------------------------------------------------------
# I-8d: Terminal tasks deleted, protected by non-terminal workflow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_expired_terminal_tasks_deleted_but_protected_by_workflow(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """Terminal tasks older than retention_hours are deleted — UNLESS they're
    linked to a non-terminal workflow. RUNNING tasks are never deleted."""
    await _truncate_retention_tables(session)

    old_completed_task = str(uuid.uuid4())
    old_running_task = str(uuid.uuid4())
    recent_completed_task = str(uuid.uuid4())
    old_completed_protected = str(uuid.uuid4())

    # 1. Old COMPLETED task (48h ago) — should be deleted
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at)
        VALUES
            (:id, 'ret_test', 'default', 100, '[]', '{}',
             'COMPLETED', NOW(), NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', FALSE, 0,
             0, NOW() - INTERVAL '48 hours')
    """), {'id': old_completed_task})

    # 2. Old RUNNING task (48h ago) — should survive (not terminal)
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries)
        VALUES
            (:id, 'ret_test', 'default', 100, '[]', '{}',
             'RUNNING', NOW(), NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', FALSE, 0, 0)
    """), {'id': old_running_task})

    # 3. Recent COMPLETED task (1h ago) — should survive (within retention)
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at)
        VALUES
            (:id, 'ret_test', 'default', 100, '[]', '{}',
             'COMPLETED', NOW(), NOW() - INTERVAL '1 hour',
             NOW() - INTERVAL '1 hour', FALSE, 0,
             0, NOW() - INTERVAL '1 hour')
    """), {'id': recent_completed_task})

    # 4. Old COMPLETED task linked to a RUNNING workflow — should survive (protected)
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at)
        VALUES
            (:id, 'ret_test', 'default', 100, '[]', '{}',
             'COMPLETED', NOW(), NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', FALSE, 0,
             0, NOW() - INTERVAL '48 hours')
    """), {'id': old_completed_protected})

    # Create a RUNNING workflow linking to the protected task
    running_wf = str(uuid.uuid4())
    await session.execute(text("""
        INSERT INTO horsies_workflows
            (id, name, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at)
        VALUES
            (:id, 'wf_running', 'RUNNING', 'FAIL', 0, :id,
             NOW(), NOW(), NOW(), NOW())
    """), {'id': running_wf})
    await session.execute(text("""
        INSERT INTO horsies_workflow_tasks
            (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
             queue_name, priority, dependencies, allow_failed_deps, join_type,
             is_subworkflow, status, task_id, created_at)
        VALUES
            (:id, :wf_id, 0, 'node_0', 'ret_test', '[]', '{}',
             'default', 100, '{}', FALSE, 'all',
             FALSE, 'COMPLETED', :task_id, NOW())
    """), {'id': str(uuid.uuid4()), 'wf_id': running_wf, 'task_id': old_completed_protected})

    await session.commit()

    assert await _count(session, 'horsies_tasks') == 4

    task_params = {
        'retention_hours': _RETENTION_HOURS,
        'wf_terminal_states': WORKFLOW_TERMINAL_VALUES,
        'task_terminal_states': TASK_TERMINAL_VALUES,
    }

    result = await session.execute(DELETE_EXPIRED_TASKS_SQL, task_params)
    await session.commit()

    # Only old_completed_task should be deleted.
    # old_running (not terminal), recent_completed (within retention),
    # old_completed_protected (linked to RUNNING workflow) all survive.
    assert int(result.rowcount or 0) == 1
    assert await _count(session, 'horsies_tasks') == 3

    # Verify the right task was deleted
    surviving = (await session.execute(
        text('SELECT id FROM horsies_tasks ORDER BY id'),
    )).fetchall()
    surviving_ids = {row[0] for row in surviving}
    assert old_completed_task not in surviving_ids
    assert old_running_task in surviving_ids
    assert recent_completed_task in surviving_ids
    assert old_completed_protected in surviving_ids
