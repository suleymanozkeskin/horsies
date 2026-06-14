"""Integration repros around success_policy completion timing."""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.workflows.engine import check_workflow_completion

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio(loop_scope='function')
async def test_success_policy_does_not_finalize_with_live_non_required_task(
    clean_workflow_tables: None,
    session: AsyncSession,
) -> None:
    """A satisfied success_policy is evaluated only after all nodes are terminal."""
    _ = clean_workflow_tables
    workflow_id = str(uuid.uuid4())
    policy = json.dumps({'cases': [{'required_indices': [0]}]})

    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, success_policy, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at)
            VALUES
                (:id, 'policy_waits_for_all_terminal', 'RUNNING', 'FAIL',
                 CAST(:policy AS JSONB), 0, :id, NOW(), NOW(), NOW(), NOW())
        """),
        {'id': workflow_id, 'policy': policy},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 created_at)
            VALUES
                (:completed_id, :workflow_id, 0, 'required', 'required_task',
                 '[]', '{}', 'default', 100, '{}', FALSE, 'all', FALSE,
                 'COMPLETED', NOW()),
                (:live_id, :workflow_id, 1, 'non_required', 'non_required_task',
                 '[]', '{}', 'default', 100, '{}', FALSE, 'all', FALSE,
                 'ENQUEUED', NOW())
        """),
        {
            'completed_id': str(uuid.uuid4()),
            'live_id': str(uuid.uuid4()),
            'workflow_id': workflow_id,
        },
    )
    await session.commit()

    await check_workflow_completion(session, workflow_id)
    await session.commit()

    row = (
        await session.execute(
            text("""
                SELECT status, completed_at
                FROM horsies_workflows
                WHERE id = :workflow_id
            """),
            {'workflow_id': workflow_id},
        )
    ).one()

    assert row.status == 'RUNNING'
    assert row.completed_at is None
