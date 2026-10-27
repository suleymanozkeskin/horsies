"""The child's workflow-node handoff on the CLAIMED -> RUNNING transition.

``_confirm_ownership_and_set_running`` moves the linked node to RUNNING in
the same transaction as the task. A retried attempt finds its node already
RUNNING; that node must keep its tuple and its first ``started_at``.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.worker.worker import (
    _confirm_ownership_and_set_running,
    _initialize_worker_pool,
)
from tests.integration.conftest import compute_test_enqueue_sha
from tests.broker_resources_support import listener_of

pytestmark = [pytest.mark.integration]

WORKER_ID = 'node-handoff-worker'
SEEDED_NODE_STARTED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
WORKFLOW_CHECK_FAILED = (False, '', 'WORKFLOW_CHECK_FAILED')


async def _seed_claimed_workflow_task(
    session: AsyncSession,
    claimed_at: datetime,
) -> str:
    """Insert a CLAIMED workflow task owned by WORKER_ID."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='node_handoff')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries, claimed_at,
                 claimed_by_worker_id, enqueue_sha, is_workflow_task,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'node_handoff', 'default', 100, '[]', '{}',
                 'CLAIMED', :sent_at, NOW(), NOW(), NOW(),
                 TRUE, 0, 0, :claimed_at,
                 :worker_id, :sha, TRUE,
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': WORKER_ID,
            'sha': sha,
        },
    )
    return task_id


async def _seed_running_workflow_with_node(
    session: AsyncSession,
    task_id: str,
    node_status: str,
) -> None:
    """Link the task to a RUNNING workflow through one node in node_status."""
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'node_handoff_wf', 'RUNNING', 'FAIL', 0, :id,
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
                 task_id, started_at, created_at)
            VALUES (:id, :workflow_id, 0, 'node_0', 'node_handoff', '[]',
                    '{}', 'default', 100, '{}', FALSE, 'all', FALSE,
                    :node_status, :task_id, :started_at, NOW())
        """),
        {
            'id': str(uuid.uuid4()),
            'workflow_id': workflow_id,
            'node_status': node_status,
            'task_id': task_id,
            'started_at': SEEDED_NODE_STARTED_AT,
        },
    )


async def _node_state(
    session: AsyncSession,
    task_id: str,
) -> tuple[str, datetime | None, str]:
    """Return the linked node's status, started_at, and physical tuple id."""
    row = (
        await session.execute(
            text("""
                SELECT status, started_at, ctid::text
                FROM horsies_workflow_tasks
                WHERE task_id = CAST(:task_id AS uuid)
            """),
            {'task_id': task_id},
        )
    ).fetchone()
    assert row is not None, f'node for task {task_id} not found'
    return str(row[0]), row[1], str(row[2])


async def _task_status(session: AsyncSession, task_id: str) -> str:
    row = (
        await session.execute(
            text('SELECT status FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'task {task_id} not found'
    return str(row[0])


async def _arrange(
    session: AsyncSession,
    broker: PostgresBroker,
    node_status: str | None,
) -> tuple[str, datetime]:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    task_id = await _seed_claimed_workflow_task(session, claimed_at)
    match node_status:
        case None:
            pass
        case str():
            await _seed_running_workflow_with_node(session, task_id, node_status)
    await session.commit()  # the child confirm runs on its own connection
    _initialize_worker_pool(listener_of(broker).database_url)
    return task_id, claimed_at


@pytest.mark.asyncio(loop_scope='function')
async def test_running_node_keeps_its_tuple_and_first_start(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A retried attempt does not rewrite a node that is already RUNNING."""
    task_id, claimed_at = await _arrange(session, broker, 'RUNNING')
    _, _, ctid_before = await _node_state(session, task_id)

    result = _confirm_ownership_and_set_running(task_id, WORKER_ID, True, claimed_at)

    assert result is None
    assert await _task_status(session, task_id) == 'RUNNING'
    status, started_at, ctid_after = await _node_state(session, task_id)
    assert status == 'RUNNING'
    assert started_at == SEEDED_NODE_STARTED_AT
    assert ctid_after == ctid_before


@pytest.mark.parametrize('node_status', ['ENQUEUED', 'READY', 'PENDING'])
@pytest.mark.asyncio(loop_scope='function')
async def test_waiting_node_moves_to_running_with_a_new_start(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
    node_status: str,
) -> None:
    """A node that is not RUNNING yet moves to RUNNING and is stamped now."""
    task_id, claimed_at = await _arrange(session, broker, node_status)

    result = _confirm_ownership_and_set_running(task_id, WORKER_ID, True, claimed_at)

    assert result is None
    assert await _task_status(session, task_id) == 'RUNNING'
    status, started_at, _ = await _node_state(session, task_id)
    assert status == 'RUNNING'
    assert started_at is not None and started_at > SEEDED_NODE_STARTED_AT


@pytest.mark.parametrize('node_status', ['COMPLETED', 'FAILED', 'SKIPPED'])
@pytest.mark.asyncio(loop_scope='function')
async def test_terminal_node_fails_the_workflow_check(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
    node_status: str,
) -> None:
    """A terminal node rolls the task back to CLAIMED and is not written."""
    task_id, claimed_at = await _arrange(session, broker, node_status)
    _, _, ctid_before = await _node_state(session, task_id)

    result = _confirm_ownership_and_set_running(task_id, WORKER_ID, True, claimed_at)

    assert result == WORKFLOW_CHECK_FAILED
    assert await _task_status(session, task_id) == 'CLAIMED'
    status, started_at, ctid_after = await _node_state(session, task_id)
    assert status == node_status
    assert started_at == SEEDED_NODE_STARTED_AT
    assert ctid_after == ctid_before


@pytest.mark.asyncio(loop_scope='function')
async def test_missing_node_fails_the_workflow_check(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A workflow task with no linked node rolls back to CLAIMED."""
    task_id, claimed_at = await _arrange(session, broker, None)

    result = _confirm_ownership_and_set_running(task_id, WORKER_ID, True, claimed_at)

    assert result == WORKFLOW_CHECK_FAILED
    assert await _task_status(session, task_id) == 'CLAIMED'
