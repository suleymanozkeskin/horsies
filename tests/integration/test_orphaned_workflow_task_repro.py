"""Focused repro for orphaned workflow task claim-budget leaks."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.types.result import is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.sql import COUNT_IN_FLIGHT_FOR_WORKER_SQL
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_enqueue_sha


async def _seed_orphaned_claimed_workflow_task(
    session: AsyncSession,
    *,
    worker_id: str,
) -> str:
    task_id = str(uuid4())
    sent_at, enqueue_sha = compute_test_enqueue_sha(
        task_name='workflow_join_barrier',
        queue_name='default',
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs,
                status, sent_at, enqueued_at, created_at, updated_at,
                claimed, claimed_at, claimed_by_worker_id, claim_expires_at,
                retry_count, max_retries, enqueue_sha, is_workflow_task
            ) VALUES (
                :id, 'workflow_join_barrier', 'default', 100, '[]', '{}',
                'CLAIMED', :sent_at, NOW(), NOW(), NOW(),
                TRUE, NOW(), :worker_id, NOW() + INTERVAL '5 minutes',
                0, 0, :enqueue_sha, TRUE
            )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'worker_id': worker_id,
            'enqueue_sha': enqueue_sha,
        },
    )
    await session.commit()
    return task_id


async def _task_status(session: AsyncSession, task_id: str) -> str:
    row = (
        await session.execute(
            text('SELECT status FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).one()
    return str(row.status)


async def _terminalization_kind(
    session: AsyncSession,
    task_id: str,
) -> str | None:
    row = (
        await session.execute(
            text(
                'SELECT terminalization_kind FROM horsies_tasks WHERE id = :id'
            ),
            {'id': task_id},
        )
    ).one()
    value = row.terminalization_kind
    return str(value) if value is not None else None


async def _in_flight_for_worker(session: AsyncSession, worker_id: str) -> int:
    row = (
        await session.execute(
            COUNT_IN_FLIGHT_FOR_WORKER_SQL,
            {'wid': worker_id},
        )
    ).one()
    return int(row.cnt or 0)


async def _link_runnable_workflow_node(
    session: AsyncSession,
    task_id: str,
) -> None:
    workflow_id = str(uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES
                (:id, 'orphan_guard_workflow', 'RUNNING', 'FAIL', 0, :id,
                 NOW(), NOW(), NOW(), NOW())
        """),
        {'id': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES
                (:id, :workflow_id, 0, 'orphan_guard_node',
                 'workflow_join_barrier', '[]', '{}', 'default', 100, '{}',
                 FALSE, 'all', FALSE, 'RUNNING', :task_id, NOW())
        """),
        {
            'id': str(uuid4()),
            'workflow_id': workflow_id,
            'task_id': task_id,
        },
    )
    await session.commit()


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_check_failed_cancels_orphan_and_frees_budget(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
    db_url: str,
) -> None:
    """Lever 2: finalizing WORKFLOW_CHECK_FAILED on a genuine orphan cancels it.

    A CLAIMED workflow task with no live workflow_task linkage can never reach
    RUNNING. Finalization must give it a terminal disposition (CANCELLED) and
    release the claim instead of leaving it CLAIMED, so in-flight budget frees
    and the reaper no longer requeues it.
    """
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-repro'
    task_id = await _seed_orphaned_claimed_workflow_task(
        session,
        worker_id=worker_id,
    )
    assert await _in_flight_for_worker(session, worker_id) == 1

    worker = Worker(
        session_factory=broker.session_factory,
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn=db_url,
            psycopg_dsn=db_url.replace('+psycopg', ''),
            queues=['default'],
            recovery_config=RecoveryConfig(),  # flag defaults True
        ),
    )
    worker.worker_instance_id = worker_id

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=False,
        result_json_str='',
        failed_reason='WORKFLOW_CHECK_FAILED',
        task_name='workflow_join_barrier',
        queue_name='default',
        is_workflow_task=True,
    )

    assert is_ok(result)
    assert await _task_status(session, task_id) == 'CANCELLED'
    assert await _terminalization_kind(session, task_id) == 'CANCEL_ORPHAN'
    assert await _in_flight_for_worker(session, worker_id) == 0


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_check_failed_disabled_leaves_orphan_claimed(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
    db_url: str,
) -> None:
    """With self-heal disabled, finalize leaves the orphan CLAIMED (inspection mode).

    The flag gates the finalize-time cancellation symmetrically with the reaper
    step: off => not cancelled at finalize, and (separately) not requeued by the
    reaper either.
    """
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-disabled'
    task_id = await _seed_orphaned_claimed_workflow_task(session, worker_id=worker_id)

    worker = Worker(
        session_factory=broker.session_factory,
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn=db_url,
            psycopg_dsn=db_url.replace('+psycopg', ''),
            queues=['default'],
            recovery_config=RecoveryConfig(
                auto_terminate_orphaned_workflow_tasks=False,
            ),
        ),
    )
    worker.worker_instance_id = worker_id

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=False,
        result_json_str='',
        failed_reason='WORKFLOW_CHECK_FAILED',
        task_name='workflow_join_barrier',
        queue_name='default',
        is_workflow_task=True,
    )

    assert is_ok(result)
    assert await _task_status(session, task_id) == 'CLAIMED'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_finalize_does_not_cancel_a_task_with_a_runnable_node_link(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
    db_url: str,
) -> None:
    """The live-link guard refusal remains a successful finalize no-op."""
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-live-link'
    task_id = await _seed_orphaned_claimed_workflow_task(
        session,
        worker_id=worker_id,
    )
    await _link_runnable_workflow_node(session, task_id)
    worker = Worker(
        session_factory=broker.session_factory,
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn=db_url,
            psycopg_dsn=db_url.replace('+psycopg', ''),
            queues=['default'],
            recovery_config=RecoveryConfig(),
        ),
    )
    worker.worker_instance_id = worker_id

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=False,
        result_json_str='',
        failed_reason='WORKFLOW_CHECK_FAILED',
        task_name='workflow_join_barrier',
        queue_name='default',
        is_workflow_task=True,
    )

    assert is_ok(result)
    assert await _task_status(session, task_id) == 'CLAIMED'
    assert await _terminalization_kind(session, task_id) is None
    assert await _in_flight_for_worker(session, worker_id) == 1


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_terminates_orphaned_workflow_task(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    """Lever 3b: the reaper self-heals an orphan even without a dispatch cycle."""
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-reaper'
    task_id = await _seed_orphaned_claimed_workflow_task(session, worker_id=worker_id)

    result = await broker.terminate_orphaned_workflow_tasks()

    assert is_ok(result)
    assert result.ok_value == 1
    assert await _task_status(session, task_id) == 'CANCELLED'
    assert await _terminalization_kind(session, task_id) == 'CANCEL_ORPHAN_SWEEP'
    assert await _in_flight_for_worker(session, worker_id) == 0


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_requeue_skips_orphaned_workflow_task(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    """Lever 3a: requeue must not re-dispatch an orphan (the churn engine)."""
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-requeue'
    task_id = await _seed_orphaned_claimed_workflow_task(session, worker_id=worker_id)
    # Make the claim stale so it would otherwise be requeued.
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET claimed_at = NOW() - INTERVAL '3600 seconds'
            WHERE id = :id
        """),
        {'id': task_id},
    )
    await session.commit()

    result = await broker.requeue_stale_claimed(stale_threshold_ms=1_000)

    assert is_ok(result)
    assert result.ok_value == 0, 'orphan must not be requeued'
    assert await _task_status(session, task_id) == 'CLAIMED'
