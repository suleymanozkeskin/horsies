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
    DELETE_EXPIRED_HEARTBEATS_SQL,
    DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL,
    DELETE_EXPIRED_TASKS_SQL,
    DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL,
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
        {'retention_hours': _RETENTION_HOURS, 'batch_size': _BATCH_SIZE},
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
             retry_count, max_retries, enqueue_sha, is_workflow_task)
        VALUES
            (:id, 'workflow_join_barrier', 'normal', 50, '[]', '{}',
             'CLAIMED', :sent_at, NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours',
             TRUE, NOW() - INTERVAL '48 hours', 'worker-retention-repro',
             NOW() + INTERVAL '5 minutes', 0, 0, :enqueue_sha, TRUE)
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
    await session.execute(DELETE_EXPIRED_WORKFLOW_TASKS_SQL, params)
    await session.execute(DELETE_EXPIRED_WORKFLOWS_SQL, params)
    await session.execute(DELETE_EXPIRED_TASKS_SQL, params)
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
    sent_at_1, sha_1 = compute_test_enqueue_sha(
        task_name='ret_test',
        sent_at=datetime.now(timezone.utc) - timedelta(hours=48),
    )
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at, enqueue_sha)
        VALUES
            (:id, 'ret_test', 'default', 100, '[]', '{}',
             'COMPLETED', :sent_at, NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', FALSE, 0,
             0, NOW() - INTERVAL '48 hours', :enqueue_sha)
    """), {'id': old_completed_task, 'sent_at': sent_at_1, 'enqueue_sha': sha_1})

    # 2. Old RUNNING task (48h ago) — should survive (not terminal)
    sent_at_2, sha_2 = compute_test_enqueue_sha(
        task_name='ret_test',
        sent_at=datetime.now(timezone.utc) - timedelta(hours=48),
    )
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, enqueue_sha)
        VALUES
            (:id, 'ret_test', 'default', 100, '[]', '{}',
             'RUNNING', :sent_at, NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', FALSE, 0, 0, :enqueue_sha)
    """), {'id': old_running_task, 'sent_at': sent_at_2, 'enqueue_sha': sha_2})

    # 3. Recent COMPLETED task (1h ago) — should survive (within retention)
    sent_at_3, sha_3 = compute_test_enqueue_sha(
        task_name='ret_test',
        sent_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at, enqueue_sha)
        VALUES
            (:id, 'ret_test', 'default', 100, '[]', '{}',
             'COMPLETED', :sent_at, NOW() - INTERVAL '1 hour',
             NOW() - INTERVAL '1 hour', FALSE, 0,
             0, NOW() - INTERVAL '1 hour', :enqueue_sha)
    """), {'id': recent_completed_task, 'sent_at': sent_at_3, 'enqueue_sha': sha_3})

    # 4. Old COMPLETED task linked to a RUNNING workflow — should survive (protected)
    sent_at_4, sha_4 = compute_test_enqueue_sha(
        task_name='ret_test',
        sent_at=datetime.now(timezone.utc) - timedelta(hours=48),
    )
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at, enqueue_sha)
        VALUES
            (:id, 'ret_test', 'default', 100, '[]', '{}',
             'COMPLETED', :sent_at, NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', FALSE, 0,
             0, NOW() - INTERVAL '48 hours', :enqueue_sha)
    """), {'id': old_completed_protected, 'sent_at': sent_at_4, 'enqueue_sha': sha_4})

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
        'batch_size': _BATCH_SIZE,
        'excluded_queues': [],
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


# ---------------------------------------------------------------------------
# Batched deletes: LIMIT bounds each statement, repeated runs drain backlog
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_expired_tasks_delete_is_bounded_by_batch_size(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """Each execution deletes at most batch_size rows; repeated executions
    drain the backlog; ineligible rows survive every batch."""
    _ = engine
    await _truncate_retention_tables(session)

    # Five old terminal tasks (eligible) + one recent (ineligible).
    for hours_ago, count in ((48, 5), (1, 1)):
        for _i in range(count):
            sent_at, sha = compute_test_enqueue_sha(
                task_name='ret_batch_test',
                sent_at=datetime.now(timezone.utc) - timedelta(hours=hours_ago),
            )
            await session.execute(text("""
                INSERT INTO horsies_tasks
                    (id, task_name, queue_name, priority, args, kwargs,
                     status, sent_at, created_at, updated_at, claimed, retry_count,
                     max_retries, completed_at, enqueue_sha)
                VALUES
                    (:id, 'ret_batch_test', 'default', 100, '[]', '{}',
                     'COMPLETED', :sent_at, NOW() - (:h || ' hours')::interval,
                     NOW() - (:h || ' hours')::interval, FALSE, 0,
                     0, NOW() - (:h || ' hours')::interval, :enqueue_sha)
            """), {
                'id': str(uuid.uuid4()),
                'sent_at': sent_at,
                'h': hours_ago,
                'enqueue_sha': sha,
            })
    await session.commit()

    params = {
        'retention_hours': _RETENTION_HOURS,
        'batch_size': 2,
        'excluded_queues': [],
    }
    rowcounts: list[int] = []
    for _round in range(4):
        result = await session.execute(DELETE_EXPIRED_TASKS_SQL, params)
        await session.commit()
        rowcounts.append(int(result.rowcount or 0))

    assert rowcounts == [2, 2, 1, 0], 'batches bounded, backlog drained'
    assert await _count(session, 'horsies_tasks') == 1  # recent task survives


@pytest.mark.asyncio(loop_scope='function')
async def test_expired_tasks_delete_purges_attempts_set_wise(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """Deleting expired tasks removes their attempt history in the same
    statement; attempts of surviving tasks are untouched.

    Also pins the CTE form's rowcount contract: the statement reports
    deleted PARENT rows only (the batching loop compares rowcount against
    batch_size), never parent + attempts.
    """
    _ = engine
    await _truncate_retention_tables(session)

    doomed_task = str(uuid.uuid4())
    surviving_task = str(uuid.uuid4())

    for task_id, hours_ago in ((doomed_task, 48), (surviving_task, 1)):
        sent_at, sha = compute_test_enqueue_sha(
            task_name='ret_attempts_test',
            sent_at=datetime.now(timezone.utc) - timedelta(hours=hours_ago),
        )
        await session.execute(text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, completed_at, enqueue_sha)
            VALUES
                (:id, 'ret_attempts_test', 'default', 100, '[]', '{}',
                 'COMPLETED', :sent_at, NOW() - (:h || ' hours')::interval,
                 NOW() - (:h || ' hours')::interval, FALSE, 0,
                 0, NOW() - (:h || ' hours')::interval, :enqueue_sha)
        """), {
            'id': task_id,
            'sent_at': sent_at,
            'h': hours_ago,
            'enqueue_sha': sha,
        })

    # Two attempts on the doomed task (a retry history), one on the survivor.
    await session.execute(text("""
        INSERT INTO horsies_task_attempts
            (task_id, attempt, outcome, will_retry, started_at, finished_at)
        VALUES
            (:doomed, 1, 'FAILED', TRUE, NOW(), NOW()),
            (:doomed, 2, 'COMPLETED', FALSE, NOW(), NOW()),
            (:survivor, 1, 'COMPLETED', FALSE, NOW(), NOW())
    """), {'doomed': doomed_task, 'survivor': surviving_task})
    await session.commit()

    assert await _count(session, 'horsies_task_attempts') == 3

    result = await session.execute(
        DELETE_EXPIRED_TASKS_SQL,
        {
            'retention_hours': _RETENTION_HOURS,
            'batch_size': _BATCH_SIZE,
            'excluded_queues': [],
        },
    )
    await session.commit()

    # rowcount counts parent task rows only, not the purged attempts.
    assert int(result.rowcount or 0) == 1

    surviving_attempts = (await session.execute(text("""
        SELECT task_id, count(*) FROM horsies_task_attempts GROUP BY task_id
    """))).fetchall()
    assert {(row[0], int(row[1])) for row in surviving_attempts} == {
        (surviving_task, 1),
    }, 'doomed attempts purged; survivor history intact'


# ---------------------------------------------------------------------------
# Per-queue retention overrides (queue_terminal_record_retention_hours)
# ---------------------------------------------------------------------------


async def _insert_plain_task(
    session: AsyncSession,
    task_id: str,
    *,
    queue_name: str,
    hours_ago: int,
    is_workflow_task: bool = False,
) -> None:
    """Insert a COMPLETED task aged hours_ago on the given queue."""
    sent_at, sha = compute_test_enqueue_sha(
        task_name='ret_queue_test',
        queue_name=queue_name,
        sent_at=datetime.now(timezone.utc) - timedelta(hours=hours_ago),
    )
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at, enqueue_sha, is_workflow_task)
        VALUES
            (:id, 'ret_queue_test', :queue_name, 100, '[]', '{}',
             'COMPLETED', :sent_at, NOW() - (:h || ' hours')::interval,
             NOW() - (:h || ' hours')::interval, FALSE, 0,
             0, NOW() - (:h || ' hours')::interval, :enqueue_sha, :is_wf)
    """), {
        'id': task_id,
        'queue_name': queue_name,
        'sent_at': sent_at,
        'h': hours_ago,
        'enqueue_sha': sha,
        'is_wf': is_workflow_task,
    })


@pytest.mark.asyncio(loop_scope='function')
async def test_queue_override_deletes_only_its_queues_plain_tasks(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """The per-queue delete removes only eligible PLAIN tasks of its queue:
    other queues, rows inside the override window, and workflow-backing
    rows on the same queue all survive. Attempts of the doomed row are
    purged in the same statement."""
    _ = engine
    await _truncate_retention_tables(session)

    doomed = str(uuid.uuid4())
    too_recent = str(uuid.uuid4())
    other_queue = str(uuid.uuid4())
    workflow_backed = str(uuid.uuid4())

    await _insert_plain_task(session, doomed, queue_name='bulk', hours_ago=2)
    await _insert_plain_task(
        session, too_recent, queue_name='bulk', hours_ago=0,
    )
    await _insert_plain_task(
        session, other_queue, queue_name='default', hours_ago=2,
    )
    await _insert_plain_task(
        session, workflow_backed, queue_name='bulk', hours_ago=2,
        is_workflow_task=True,
    )
    await session.execute(text("""
        INSERT INTO horsies_task_attempts
            (task_id, attempt, outcome, will_retry, started_at, finished_at)
        VALUES (:doomed, 1, 'COMPLETED', FALSE, NOW(), NOW())
    """), {'doomed': doomed})
    await session.commit()

    result = await session.execute(
        DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL,
        {
            'retention_hours': 1,
            'queue_name': 'bulk',
            'batch_size': _BATCH_SIZE,
        },
    )
    await session.commit()

    assert int(result.rowcount or 0) == 1
    surviving = {
        row[0] for row in
        (await session.execute(text('SELECT id FROM horsies_tasks'))).fetchall()
    }
    assert surviving == {too_recent, other_queue, workflow_backed}
    assert await _count(session, 'horsies_task_attempts') == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_global_delete_excludes_override_queues(
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """:excluded_queues shields override queues from the global-window
    delete; an empty array excludes nothing."""
    _ = engine
    await _truncate_retention_tables(session)

    bulk_task = str(uuid.uuid4())
    default_task = str(uuid.uuid4())
    await _insert_plain_task(session, bulk_task, queue_name='bulk', hours_ago=48)
    await _insert_plain_task(
        session, default_task, queue_name='default', hours_ago=48,
    )
    await session.commit()

    result = await session.execute(
        DELETE_EXPIRED_TASKS_SQL,
        {
            'retention_hours': _RETENTION_HOURS,
            'batch_size': _BATCH_SIZE,
            'excluded_queues': ['bulk'],
        },
    )
    await session.commit()
    assert int(result.rowcount or 0) == 1
    surviving = {
        row[0] for row in
        (await session.execute(text('SELECT id FROM horsies_tasks'))).fetchall()
    }
    assert surviving == {bulk_task}, 'override queue shielded from global delete'

    result = await session.execute(
        DELETE_EXPIRED_TASKS_SQL,
        {
            'retention_hours': _RETENTION_HOURS,
            'batch_size': _BATCH_SIZE,
            'excluded_queues': [],
        },
    )
    await session.commit()
    assert int(result.rowcount or 0) == 1
    assert await _count(session, 'horsies_tasks') == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_expired_tasks_delete_uses_retention_index(
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
    session: AsyncSession,
) -> None:
    """The eligibility predicate is planned via idx_horsies_tasks_retention.

    Pins the literal-statuses contract: the partial index only serves the
    delete while the statement's status literals imply the index predicate.
    A bound-array regression (status = ANY(:param)) or a drifted COALESCE
    expression would drop back to a seq scan and fail here.
    """
    await _truncate_retention_tables(session)

    # Production shape: many terminal rows inside the retention window (the
    # low-selectivity case that poisons the plain status index) and one
    # eligible row. With fresh statistics the planner picks the retention
    # index only if its predicate is provably implied by the statement.
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at, enqueue_sha)
        SELECT gen_random_uuid()::text, 'ret_idx_test', 'default', 100, '[]', '{}',
               'COMPLETED', NOW(), NOW(), NOW(), FALSE, 0,
               0, NOW(), 'ret-idx-test-sha'
        FROM generate_series(1, 500)
    """))
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at, enqueue_sha)
        VALUES
            (gen_random_uuid()::text, 'ret_idx_test', 'default', 100, '[]', '{}',
             'COMPLETED', NOW(), NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', FALSE, 0,
             0, NOW() - INTERVAL '48 hours', 'ret-idx-test-sha')
    """))
    await session.commit()
    await session.execute(text('ANALYZE horsies_tasks'))

    explain_sql = text(f"""
        EXPLAIN SELECT t.id
        FROM horsies_tasks t
        WHERE t.status IN ({TASK_TERMINAL_STATUS_SQL_LITERALS})
          AND COALESCE(t.completed_at, t.failed_at, t.updated_at, t.created_at)
              < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
    """)
    # A 500-row test table fits in a few pages, so the planner still
    # prefers a seq scan; disable it to force the index-vs-index choice
    # that a production-sized heap produces on its own.
    await session.execute(text('SET enable_seqscan = off'))
    plan = '\n'.join(
        str(row[0]) for row in
        (await session.execute(
            explain_sql, {'retention_hours': _RETENTION_HOURS},
        )).fetchall()
    )
    await session.execute(text('SET enable_seqscan = on'))

    # Either retention partial index proves the contract (status literals
    # imply the partial predicate; COALESCE expression matches). At seeded
    # scale the two cost the same and the planner's pick between them is
    # arbitrary; predicate drift falls off both and fails here.
    assert (
        'idx_horsies_tasks_retention' in plan
        or 'idx_horsies_tasks_queue_retention' in plan
    ), plan


async def _explain_analyze_plan(
    session: AsyncSession,
    statement_sql: str,
    params: dict[str, object],
) -> str:
    """EXPLAIN ANALYZE a statement, roll back, return the executed plan.

    ANALYZE executes the statement, so the assertion covers the plan the
    executor actually ran, not just the planner's proposal. The rollback
    undoes both the DELETE and the seqscan toggle (a non-LOCAL SET inside
    an aborted transaction is reverted with it), so every statement sees
    the same seeded state. seqscan is disabled because the seeded tables
    fit in a few pages, where the planner would prefer a seq scan and hide
    the index-vs-index choice a production-sized heap produces on its own.
    """
    await session.execute(text('SET enable_seqscan = off'))
    rows = (await session.execute(
        text('EXPLAIN (ANALYZE, BUFFERS) ' + statement_sql), params,
    )).fetchall()
    plan = '\n'.join(str(row[0]) for row in rows)
    await session.rollback()
    return plan


@pytest.mark.asyncio(loop_scope='function')
async def test_retention_delete_statements_plan_on_retention_indexes(
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
    session: AsyncSession,
) -> None:
    """The production retention DELETEs execute via their retention indexes.

    Unlike the eligibility-predicate test above, this EXPLAIN ANALYZEs the
    exact statements the reaper executes (rolled back), so any drift in the
    statement itself — status literals replaced by bound arrays, a reworded
    COALESCE, an added predicate the partial index cannot serve — falls off
    the index plan and fails here, not in production.
    """
    await _truncate_retention_tables(session)

    # Production shape for both tables: many terminal rows inside the
    # retention window and one eligible row — the case where the planner
    # previously chose a stop-early pkey walk whose LIMIT never filled.
    await session.execute(text("""
        INSERT INTO horsies_workflows
            (id, name, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at, completed_at)
        SELECT gen_random_uuid()::text, 'ret_plan_wf_test', 'COMPLETED', 'FAIL', 0,
               gen_random_uuid()::text,
               NOW(), NOW(), NOW(), NOW(), NOW()
        FROM generate_series(1, 500)
    """))
    await session.execute(text("""
        INSERT INTO horsies_workflows
            (id, name, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at, completed_at)
        VALUES
            (gen_random_uuid()::text, 'ret_plan_wf_test', 'COMPLETED', 'FAIL', 0,
             gen_random_uuid()::text,
             NOW(), NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours')
    """))
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at, enqueue_sha)
        SELECT gen_random_uuid()::text, 'ret_plan_task_test', 'default', 100, '[]', '{}',
               'COMPLETED', NOW(), NOW(), NOW(), FALSE, 0,
               0, NOW(), 'ret-plan-test-sha'
        FROM generate_series(1, 500)
    """))
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, created_at, updated_at, claimed, retry_count,
             max_retries, completed_at, enqueue_sha)
        VALUES
            (gen_random_uuid()::text, 'ret_plan_task_test', 'default', 100, '[]', '{}',
             'COMPLETED', NOW(), NOW() - INTERVAL '48 hours',
             NOW() - INTERVAL '48 hours', FALSE, 0,
             0, NOW() - INTERVAL '48 hours', 'ret-plan-test-sha')
    """))
    # One attempt per task: the tasks delete must purge these set-wise via
    # its purged_attempts CTE, visible below as a plan node.
    await session.execute(text("""
        INSERT INTO horsies_task_attempts
            (task_id, attempt, outcome, will_retry, started_at, finished_at)
        SELECT id, 1, 'COMPLETED', FALSE, NOW(), NOW()
        FROM horsies_tasks
    """))
    await session.commit()
    await session.execute(text(
        'ANALYZE horsies_workflows, horsies_tasks, horsies_task_attempts'
    ))
    # ANALYZE's pg_statistic writes are MVCC-transactional; commit them so
    # the per-statement rollbacks below revert only the DELETE and the
    # seqscan toggle, not the gathered statistics — otherwise statements
    # after the first are planned on stale stats and the test becomes
    # order-sensitive.
    await session.commit()

    params: dict[str, object] = {
        'retention_hours': _RETENTION_HOURS,
        'batch_size': _BATCH_SIZE,
        'excluded_queues': [],
    }

    workflows_plan = await _explain_analyze_plan(
        session, DELETE_EXPIRED_WORKFLOWS_SQL.text, params,
    )
    assert 'idx_horsies_workflows_retention' in workflows_plan, workflows_plan

    workflow_tasks_plan = await _explain_analyze_plan(
        session, DELETE_EXPIRED_WORKFLOW_TASKS_SQL.text, params,
    )
    assert 'idx_horsies_workflows_retention' in workflow_tasks_plan, (
        workflow_tasks_plan
    )

    # Non-empty exclusion exercises the production shape when overrides
    # exist; the exclusion is a heap filter and must not change the plan.
    # Either retention partial index proves the contract at seeded scale
    # (see test_expired_tasks_delete_uses_retention_index).
    tasks_plan = await _explain_analyze_plan(
        session,
        DELETE_EXPIRED_TASKS_SQL.text,
        {**params, 'excluded_queues': ['bulk']},
    )
    assert (
        'idx_horsies_tasks_retention' in tasks_plan
        or 'idx_horsies_tasks_queue_retention' in tasks_plan
    ), tasks_plan
    # The set-wise attempts purge is a plan node; the per-row FK cascade it
    # replaces surfaces only as trigger time. Dropping the purged_attempts
    # CTE removes this node and fails here — the revert-proof for R1.
    assert 'Delete on horsies_task_attempts' in tasks_plan, tasks_plan
    # Node presence proves the shape, not the work: a ModifyTable node
    # reports rows=0 without RETURNING even when it deleted rows. Assert
    # the scan feeding the attempts Delete produced non-zero actual rows
    # (matching the `actual ... rows=` clause — the cost estimate on the
    # same line also contains `rows=`).
    attempts_scan = re.search(
        r'uq_horsies_task_attempts_task_attempt[^\n]*'
        r'\(actual[^)]*rows=([\d.]+) loops=(\d+)\)',
        tasks_plan,
    )
    assert attempts_scan is not None, tasks_plan
    attempts_rows_scanned = (
        float(attempts_scan.group(1)) * int(attempts_scan.group(2))
    )
    assert attempts_rows_scanned > 0, tasks_plan

    # Per-queue override delete plans on the v15 queue-leading composite,
    # not the v11 expression index — an override window's recent cutoff
    # makes every other queue's retained rows heap-filter misses on v11.
    queue_plan = await _explain_analyze_plan(
        session,
        DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL.text,
        {
            'retention_hours': _RETENTION_HOURS,
            'queue_name': 'default',
            'batch_size': _BATCH_SIZE,
        },
    )
    assert 'idx_horsies_tasks_queue_retention' in queue_plan, queue_plan
