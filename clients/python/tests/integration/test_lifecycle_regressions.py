# pyright: reportPrivateUsage=false
"""Regression tests for verified task lifecycle correctness issues."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_ok
from horsies.core.workflows.lifecycle import pause_workflow_async
from tests.integration.conftest import compute_test_command_fingerprint

pytestmark = [pytest.mark.integration]


async def _insert_retry_window_workflow_task(
    session: AsyncSession,
    *,
    worker_id: str,
) -> tuple[str, str, str]:
    workflow_id = str(uuid.uuid4())
    workflow_task_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(task_name='lifecycle_regression_task')
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, sent_at, created_at, updated_at,
                 depth, root_workflow_id)
            VALUES
                (:workflow_id, 'retry-window-regression', 'RUNNING', 'fail',
                 NOW(), NOW(), NOW(), 0, :workflow_id)
        """),
        {'workflow_id': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, claimed_at,
                    claimed_by_worker_id, claim_expires_at, retry_count, max_retries,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution
                ) VALUES (
                    :task_id, 'lifecycle_regression_task', 'default', 100, '{}',
                    'CLAIMED', :sent_at, NOW(), NOW(), NOW(), NOW(), :worker_id,
                    NOW() + INTERVAL '60 seconds', 1, 3, TRUE, 'standard_30d', NULL,
                    NULL, FALSE, 'ISOLATED'
                )
        """),
        {
            'task_id': task_id,
            'sent_at': sent_at,
            'worker_id': worker_id,
            'command_fingerprint': fingerprint,
        },
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies, args_from,
                 workflow_ctx_from, allow_failed_deps, join_type, task_options,
                 status, task_id, is_subworkflow, created_at, started_at,
                 max_retries, execution, retention_class_key)
            VALUES
                ('NONE', :workflow_task_id, :workflow_id, 0, 'retry_node',
                 'lifecycle_regression_task', '{}', 'default', 100,
                 ARRAY[]::integer[], NULL, NULL, FALSE, 'all', NULL,
                 'RUNNING', :task_id, FALSE, NOW(), NOW(),
                 0, 'ISOLATED', 'standard_30d')
        """),
        {
            'workflow_task_id': workflow_task_id,
            'workflow_id': workflow_id,
            'task_id': task_id,
        },
    )
    await session.commit()
    return workflow_id, workflow_task_id, task_id


@pytest.mark.asyncio(loop_scope='function')
async def test_pause_resets_retry_window_claimed_workflow_task(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Pause remains resumable when a retry-window workflow task is CLAIMED."""
    workflow_id, workflow_task_id, task_id = await _insert_retry_window_workflow_task(
        session,
        worker_id='retry-window-worker',
    )

    result = await pause_workflow_async(broker, uuid.UUID(workflow_id))

    assert is_ok(result)
    assert result.ok_value is True
    row = (
        await session.execute(
            text("""
                SELECT w.status AS workflow_status,
                       wt.status AS workflow_task_status,
                       wt.task_id AS workflow_task_task_id,
                       t.status AS task_status,
                       t.error_code,
                       t.terminalization_kind
                FROM horsies_workflows w
                JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
                JOIN itest_task_rows t ON t.id = CAST(:task_id AS uuid)
                WHERE w.id = :workflow_id
                  AND wt.id = :workflow_task_id
            """),
            {
                'workflow_id': workflow_id,
                'workflow_task_id': workflow_task_id,
                'task_id': task_id,
            },
        )
    ).fetchone()
    assert row is not None
    assert row.workflow_status == 'PAUSED'
    assert row.workflow_task_status == 'READY'
    assert row.workflow_task_task_id is None
    assert row.task_status == 'CANCELLED'
    assert row.error_code == 'TASK_CANCELLED'
    assert row.terminalization_kind == 'PAUSE_ABANDON_WORKFLOW'


