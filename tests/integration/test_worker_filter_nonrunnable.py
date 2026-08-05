"""Integration tests for Worker._filter_nonrunnable_workflow_tasks().

Covers lines 765-826: the post-claim guard that filters out tasks
belonging to PAUSED or CANCELLED workflows, mutating both the
horsies_tasks and horsies_workflow_tasks rows accordingly.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
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


async def _insert_claimed_task(
    session: AsyncSession,
    *,
    claimed_by_worker_id: str | None = None,
) -> str:
    """Insert a horsies_tasks row in CLAIMED state (post-claim)."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='filter_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, claimed_at, claimed_by_worker_id, enqueue_sha)
            VALUES
                (:id, 'filter_test', 'default', 100, '[]', '{}',
                 'CLAIMED', :sent_at, NOW(), NOW(), TRUE, 0,
                 0, NOW(), :claimed_by_worker_id, :enqueue_sha)
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_by_worker_id': claimed_by_worker_id,
            'enqueue_sha': sha,
        },
    )
    return task_id


async def _link_task_to_workflow(
    session: AsyncSession,
    task_id: str,
    *,
    wf_status: str = 'RUNNING',
    wt_status: str = 'ENQUEUED',
) -> str:
    """Create workflow (with given status) + workflow_task rows.

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
                (:id, 'test_wf', :status, 'FAIL', 0, :id,
                 NOW(), NOW(), NOW(), NOW())
        """),
        {'id': wf_id, 'status': wf_status},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                 queue_name, priority, dependencies, allow_failed_deps, join_type,
                 is_subworkflow, status, task_id, created_at)
            VALUES
                (:id, :wf_id, 0, 'node_0', 'filter_test', '[]', '{}',
                 'default', 100, '{}', FALSE, 'all',
                 FALSE, :wt_status, :task_id, NOW())
        """),
        {'id': wt_id, 'wf_id': wf_id, 'task_id': task_id, 'wt_status': wt_status},
    )
    return wf_id


async def _get_task_status(session: AsyncSession, task_id: str) -> str:
    """Read horsies_tasks.status for a given task_id."""
    row = (
        await session.execute(
            text('SELECT status FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'Task {task_id} not found'
    return str(row[0])


async def _get_terminalization_kind(
    session: AsyncSession,
    task_id: str,
) -> str | None:
    return (
        await session.execute(
            text(
                'SELECT terminalization_kind FROM horsies_tasks '
                'WHERE id = :id'
            ),
            {'id': task_id},
        )
    ).scalar_one()


async def _get_task_claim_row(session: AsyncSession, task_id: str) -> tuple[str, bool, str | None]:
    """Read task status/claim ownership fields for a task."""
    row = (
        await session.execute(
            text("""
                SELECT status, claimed, claimed_by_worker_id
                FROM horsies_tasks
                WHERE id = :id
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'Task {task_id} not found'
    return str(row[0]), bool(row[1]), row[2]


def _make_rows(*task_ids: str) -> list[dict[str, Any]]:
    """Build the minimal claimed-row dicts the method expects."""
    return [{'id': tid, 'task_name': 'filter_test', 'args': '[]', 'kwargs': '{}'} for tid in task_ids]


# ---------------------------------------------------------------------------
# I-7a: No workflow tasks → rows pass through unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_no_workflow_tasks_pass_through(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Tasks not linked to any workflow pass through unchanged."""
    tid_a = await _insert_claimed_task(session)
    tid_b = await _insert_claimed_task(session)
    await session.commit()

    worker = _make_worker(engine)
    rows = _make_rows(tid_a, tid_b)

    result = await worker._filter_nonrunnable_workflow_tasks(rows)

    assert len(result) == 2
    returned_ids = {r['id'] for r in result}
    assert returned_ids == {tid_a, tid_b}

    # DB status unchanged
    assert await _get_task_status(session, tid_a) == 'CLAIMED'
    assert await _get_task_status(session, tid_b) == 'CLAIMED'


# ---------------------------------------------------------------------------
# I-7b: PAUSED workflow → task cancelled, workflow_task reset
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_paused_workflow_cancels_and_resets(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task in a PAUSED workflow → excluded from return,
    horsies_tasks → CANCELLED, horsies_workflow_tasks → READY with task_id=NULL."""
    worker = _make_worker(engine)
    tid = await _insert_claimed_task(
        session,
        claimed_by_worker_id=worker.worker_instance_id,
    )
    await _link_task_to_workflow(session, tid, wf_status='PAUSED')
    await session.commit()

    rows = _make_rows(tid)

    result = await worker._filter_nonrunnable_workflow_tasks(rows)

    # Excluded from return
    assert result == []

    # horsies_tasks: cancelled so claimed work does not leak back into the queue
    assert await _get_task_status(session, tid) == 'CANCELLED'
    assert await _get_terminalization_kind(session, tid) == (
        'PAUSE_ABANDON_CLAIM_BATCH'
    )

    # horsies_workflow_tasks: reset to READY with task_id NULLed
    wt_row = (
        await session.execute(
            text("""
                SELECT status, task_id
                FROM horsies_workflow_tasks
                WHERE workflow_id IN (
                    SELECT w.id FROM horsies_workflows w
                    JOIN horsies_workflow_tasks wt2 ON wt2.workflow_id = w.id
                    WHERE w.status = 'PAUSED'
                )
                AND node_id = 'node_0'
            """),
        )
    ).fetchone()
    assert wt_row is not None
    assert wt_row[0] == 'READY'
    assert wt_row[1] is None  # task_id cleared


@pytest.mark.asyncio(loop_scope='function')
async def test_paused_retry_window_task_cancels_and_resets_running_node(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A paused retry-window CLAIMED task resets even when workflow_task is RUNNING."""
    worker = _make_worker(engine)
    tid = await _insert_claimed_task(
        session,
        claimed_by_worker_id=worker.worker_instance_id,
    )
    await _link_task_to_workflow(session, tid, wf_status='PAUSED', wt_status='RUNNING')
    await session.commit()

    result = await worker._filter_nonrunnable_workflow_tasks(_make_rows(tid))

    assert result == []
    assert await _get_task_status(session, tid) == 'CANCELLED'
    wt_row = (
        await session.execute(
            text("""
                SELECT status, task_id
                FROM horsies_workflow_tasks
                WHERE node_id = 'node_0'
            """),
        )
    ).fetchone()
    assert wt_row is not None
    assert wt_row[0] == 'READY'
    assert wt_row[1] is None


# ---------------------------------------------------------------------------
# I-7c: CANCELLED workflow → task cancelled, workflow_task skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_cancelled_workflow_cancels_and_skips(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task in a CANCELLED workflow → excluded from return,
    horsies_tasks → CANCELLED, horsies_workflow_tasks → SKIPPED."""
    worker = _make_worker(engine)
    tid = await _insert_claimed_task(
        session,
        claimed_by_worker_id=worker.worker_instance_id,
    )
    wf_id = await _link_task_to_workflow(session, tid, wf_status='CANCELLED')
    await session.commit()

    rows = _make_rows(tid)

    result = await worker._filter_nonrunnable_workflow_tasks(rows)

    # Excluded from return
    assert result == []

    # horsies_tasks: cancelled
    assert await _get_task_status(session, tid) == 'CANCELLED'
    assert await _get_terminalization_kind(session, tid) == (
        'WORKFLOW_CANCEL_CLAIM_BATCH'
    )

    # horsies_workflow_tasks: skipped
    wt_row = (
        await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': wf_id},
        )
    ).fetchone()
    assert wt_row is not None
    assert wt_row[0] == 'SKIPPED'


# ---------------------------------------------------------------------------
# I-7d: Mixed set → only runnable tasks returned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_mixed_runnable_paused_cancelled(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Mix of no-workflow, PAUSED, and CANCELLED tasks →
    only the no-workflow task is returned."""
    worker = _make_worker(engine)
    tid_ok = await _insert_claimed_task(session)
    tid_paused = await _insert_claimed_task(
        session,
        claimed_by_worker_id=worker.worker_instance_id,
    )
    tid_cancelled = await _insert_claimed_task(
        session,
        claimed_by_worker_id=worker.worker_instance_id,
    )

    # tid_ok: no workflow link (should pass through)
    await _link_task_to_workflow(session, tid_paused, wf_status='PAUSED')
    wf_id_cancelled = await _link_task_to_workflow(session, tid_cancelled, wf_status='CANCELLED')
    await session.commit()

    rows = _make_rows(tid_ok, tid_paused, tid_cancelled)

    result = await worker._filter_nonrunnable_workflow_tasks(rows)

    # Only the non-workflow task survives
    assert len(result) == 1
    assert result[0]['id'] == tid_ok

    # Verify DB mutations on the blocked tasks
    assert await _get_task_status(session, tid_ok) == 'CLAIMED'
    assert await _get_task_status(session, tid_paused) == 'CANCELLED'
    assert await _get_task_status(session, tid_cancelled) == 'CANCELLED'

    # Cancelled workflow_task → SKIPPED
    wt_row = (
        await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': wf_id_cancelled},
        )
    ).fetchone()
    assert wt_row is not None
    assert wt_row[0] == 'SKIPPED'


@pytest.mark.asyncio(loop_scope='function')
async def test_paused_workflow_does_not_reset_task_owned_by_another_worker(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A worker must not unclaim another worker's PAUSED workflow task."""
    worker = _make_worker(engine)
    other_worker_id = f'{worker.worker_instance_id}-other'
    tid = await _insert_claimed_task(session, claimed_by_worker_id=other_worker_id)
    wf_id = await _link_task_to_workflow(session, tid, wf_status='PAUSED')
    await session.commit()

    result = await worker._filter_nonrunnable_workflow_tasks(_make_rows(tid))

    assert result == []
    assert await _get_task_claim_row(session, tid) == (
        'CLAIMED',
        True,
        other_worker_id,
    )
    wt_row = (
        await session.execute(
            text("""
                SELECT status, task_id
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': wf_id},
        )
    ).fetchone()
    assert wt_row is not None
    assert wt_row[0] == 'ENQUEUED'
    assert wt_row[1] == tid


@pytest.mark.asyncio(loop_scope='function')
async def test_cancelled_workflow_does_not_reset_task_owned_by_another_worker(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A worker must not cancel another worker's CANCELLED workflow task."""
    worker = _make_worker(engine)
    other_worker_id = f'{worker.worker_instance_id}-other'
    tid = await _insert_claimed_task(session, claimed_by_worker_id=other_worker_id)
    wf_id = await _link_task_to_workflow(session, tid, wf_status='CANCELLED')
    await session.commit()

    result = await worker._filter_nonrunnable_workflow_tasks(_make_rows(tid))

    assert result == []
    assert await _get_task_claim_row(session, tid) == (
        'CLAIMED',
        True,
        other_worker_id,
    )
    wt_row = (
        await session.execute(
            text("""
                SELECT status, task_id
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': wf_id},
        )
    ).fetchone()
    assert wt_row is not None
    assert wt_row[0] == 'ENQUEUED'
    assert wt_row[1] == tid
