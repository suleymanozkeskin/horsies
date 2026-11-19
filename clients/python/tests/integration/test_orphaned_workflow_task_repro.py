"""Focused repro for orphaned workflow task claim-budget leaks."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_ok
from tests.integration.conftest import compute_test_command_fingerprint

# The claim generation of every row seeded here, which the fences of the
# worker check.
CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


async def _seed_orphaned_claimed_workflow_task(
    session: AsyncSession,
    *,
    worker_id: str,
) -> str:
    task_id = str(uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name='workflow_join_barrier',
        queue_name='default',
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, status, sent_at,
                enqueued_at, created_at, updated_at, claimed_at, claimed_by_worker_id,
                claim_expires_at, retry_count, max_retries, is_workflow_task,
                retention_class_key, command_fingerprint_version, command_fingerprint,
                retain_rerun_input, execution
            ) VALUES (
                :id, 'workflow_join_barrier', 'default', 100, '{}', 'CLAIMED',
                :sent_at, NOW(), NOW(), NOW(), :claimed_at, :worker_id,
                NOW() + INTERVAL '5 minutes', 0, 0, TRUE, 'standard_30d', NULL, NULL,
                FALSE, 'ISOLATED'
            )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_at': CLAIMED_AT,
            'worker_id': worker_id,
            'command_fingerprint': fingerprint,
        },
    )
    await session.commit()
    return task_id


async def _task_status(session: AsyncSession, task_id: str) -> str:
    row = (
        await session.execute(
            text(
                'SELECT status FROM itest_task_rows '
                'WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).one()
    return str(row.status)


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
