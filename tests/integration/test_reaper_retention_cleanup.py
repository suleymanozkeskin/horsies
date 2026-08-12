"""Integration tests for the reaper retention cleanup SQL.

Verifies that the five DELETE statements used by the reaper loop
correctly prune old rows while preserving recent and non-terminal records.
The mock-based orchestration tests live in test_worker_helpers.py;
these tests validate the SQL against real DB rows.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from tests.integration.conftest import compute_test_enqueue_sha

from horsies.core.brokers.postgres import PostgresBroker

from horsies.core.schemas.indexes import TASK_TERMINAL_STATUS_SQL_LITERALS
from horsies.core.worker.sql import (
    DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL,
)

pytestmark = [pytest.mark.integration]

# Retention window used in all tests — 24 hours.
_RETENTION_HOURS = 24

# Large enough that every test below drains in a single batch unless it
# exercises batching explicitly.
_BATCH_SIZE = 10_000


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
        {'retention_hours': _RETENTION_HOURS, 'batch_size': _BATCH_SIZE},
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
        'batch_size': _BATCH_SIZE,
    }

    # One workflow-batched statement: node rows purge in its CTE.
    wf_result = await session.execute(DELETE_EXPIRED_WORKFLOWS_SQL, wf_params)
    await session.commit()

    # rowcount counts workflows: only the old completed one.
    assert int(wf_result.rowcount or 0) == 1

    # Survivors: old_running + recent_completed workflows
    assert await _count(session, 'horsies_workflows') == 2
    # Survivor: old_running's workflow_task
    assert await _count(session, 'horsies_workflow_tasks') == 1


async def _insert_doomed_workflow(
    session: AsyncSession, *, name: str, node_count: int
) -> str:
    """A terminal 48h-old workflow with ``node_count`` COMPLETED node rows."""
    workflow_id = str(uuid.uuid4())
    await session.execute(text("""
        INSERT INTO horsies_workflows
            (id, name, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at, completed_at)
        VALUES
            (:id, :name, 'COMPLETED', 'FAIL', 0, :id,
             NOW(), NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours')
    """), {'id': workflow_id, 'name': name})
    for index in range(node_count):
        await session.execute(text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 created_at)
            VALUES
                (:id, :wf_id, :task_index, :node_id, 'retention_budget_test',
                 '[]', '{}', 'default', 100, '{}', FALSE, 'all',
                 FALSE, 'COMPLETED', NOW())
        """), {
            'id': str(uuid.uuid4()),
            'wf_id': workflow_id,
            'task_index': index,
            'node_id': f'node_{index}',
        })
    return workflow_id


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_delete_batches_by_node_budget(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """One statement deletes workflows only while their running node total
    fits :batch_size; the remainder drains on the next batch."""
    _ = engine
    await _truncate_retention_tables(session)

    for index in range(4):
        await _insert_doomed_workflow(
            session, name=f'wf_budget_{index}', node_count=3
        )
    await session.commit()

    params = {'retention_hours': _RETENTION_HOURS, 'batch_size': 6}

    first = await session.execute(DELETE_EXPIRED_WORKFLOWS_SQL, params)
    await session.commit()
    # 4 workflows x 3 nodes against a budget of 6 node rows: two fit.
    assert int(first.rowcount or 0) == 2
    assert await _count(session, 'horsies_workflows') == 2
    assert await _count(session, 'horsies_workflow_tasks') == 6

    second = await session.execute(DELETE_EXPIRED_WORKFLOWS_SQL, params)
    await session.commit()
    assert int(second.rowcount or 0) == 2
    assert await _count(session, 'horsies_workflows') == 0
    assert await _count(session, 'horsies_workflow_tasks') == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_larger_than_budget_still_drains(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """A workflow with more nodes than the whole budget deletes alone
    (position = 1 in the budget window) instead of starving forever."""
    _ = engine
    await _truncate_retention_tables(session)

    await _insert_doomed_workflow(session, name='wf_jumbo', node_count=9)
    await session.commit()

    result = await session.execute(
        DELETE_EXPIRED_WORKFLOWS_SQL,
        {'retention_hours': _RETENTION_HOURS, 'batch_size': 4},
    )
    await session.commit()

    assert int(result.rowcount or 0) == 1
    assert await _count(session, 'horsies_workflows') == 0
    assert await _count(session, 'horsies_workflow_tasks') == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_retention_keeps_terminal_workflow_with_live_task(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """Lever 1: retention must not orphan a live task row.

    A terminal+old workflow whose workflow_task still references a non-terminal
    task is retained whole — workflow, workflow_task, and task all survive — so
    the task never loses its linkage. Cleanup is only deferred until the backing
    task becomes terminal (which orphan self-heal guarantees).
    """
    _ = engine
    await _truncate_retention_tables(session)

    workflow_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())
    sent_at, enqueue_sha = compute_test_enqueue_sha(
        task_name='workflow_join_barrier',
        queue_name='normal',
        sent_at=datetime.now(timezone.utc) - timedelta(hours=48),
    )

    await session.execute(text("""
        INSERT INTO horsies_workflows
            (id, name, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at, completed_at)
        VALUES
            (:id, 'wf_policy_complete_with_live_task', 'COMPLETED', 'FAIL', 0, :id,
             NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours')
    """), {'id': workflow_id})
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, enqueued_at, created_at, updated_at,
             claimed, claimed_at, claimed_by_worker_id, claim_expires_at,
             retry_count, max_retries, enqueue_sha, is_workflow_task,
             retention_class_key, command_fingerprint_version,
             command_fingerprint, retain_rerun_input,
             prepared_rerun_input_disposition)
        VALUES
            (:id, 'workflow_join_barrier', 'normal', 50, '[]', '{}',
             'CLAIMED', :sent_at, NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours',
             TRUE, NOW() - INTERVAL '48 hours', 'worker-retention-repro',
             NOW() + INTERVAL '5 minutes', 0, 0, :enqueue_sha, TRUE,
             'standard_30d', 1,
             sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
             FALSE, 'DECLINED_BY_POLICY')
    """), {
        'id': task_id,
        'sent_at': sent_at,
        'enqueue_sha': enqueue_sha,
    })
    await session.execute(text("""
        INSERT INTO horsies_workflow_tasks
            (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
             queue_name, priority, dependencies, allow_failed_deps, join_type,
             is_subworkflow, status, task_id, created_at)
        VALUES
            (:id, :wf_id, 0, 'join', 'workflow_join_barrier', '[]', '{}',
             'normal', 50, '{}', FALSE, 'all',
             FALSE, 'ENQUEUED', :task_id, NOW() - INTERVAL '48 hours')
    """), {
        'id': str(uuid.uuid4()),
        'wf_id': workflow_id,
        'task_id': task_id,
    })
    await session.commit()

    params = {
        'retention_hours': _RETENTION_HOURS,
        'batch_size': _BATCH_SIZE,
        'excluded_queues': [],
    }
    # Only the workflow sweep runs: live tasks are never terminal, and
    # terminal records leave by partition drop rather than by a delete.
    await session.execute(DELETE_EXPIRED_WORKFLOWS_SQL, params)
    await session.commit()

    row = (
        await session.execute(
            text("""
                SELECT t.status, COUNT(wt.task_id) AS refs
                FROM horsies_tasks t
                LEFT JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                WHERE t.id = :task_id
                GROUP BY t.status
            """),
            {'task_id': task_id},
        )
    ).one()

    # Retention left the row whole: the non-terminal task keeps its linkage.
    assert row.status == 'CLAIMED'
    assert int(row.refs or 0) == 1
    wf_present = (
        await session.execute(
            text('SELECT 1 FROM horsies_workflows WHERE id = :wf'),
            {'wf': workflow_id},
        )
    ).fetchone() is not None
    assert wf_present, 'workflow retained until its backing task is terminal'
