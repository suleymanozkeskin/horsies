"""The child's workflow-node handoff on the CLAIMED -> RUNNING transition.

``_confirm_ownership_and_set_running`` moves the linked node to RUNNING in
the same statement as the task. A retried attempt finds its node already
RUNNING; that node must keep its tuple and its first ``started_at``. A node
that is not linked fails the workflow check; with the orphan self-heal of
the run on, the stop before start cancels the task instead.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.worker.child_runner import _confirm_ownership_and_set_running
from horsies.core.worker.run_return import StartedRunning, StoppedBeforeRunning
from tests.integration.conftest import compute_test_command_fingerprint

pytestmark = [pytest.mark.integration]

WORKER_ID = 'node-handoff-worker'
SEEDED_NODE_STARTED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
WORKFLOW_CHECK_FAILED = StoppedBeforeRunning('NODE_NOT_LINKED')
"""The stop of a workflow task whose node is not linked, with the orphan
self-heal of the run off."""


async def _seed_claimed_workflow_task(
    session: AsyncSession,
    claimed_at: datetime,
) -> str:
    """Insert a CLAIMED workflow task owned by WORKER_ID."""
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(task_name='node_handoff')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, retry_count, max_retries,
                    claimed_at, claimed_by_worker_id, is_workflow_task,
                    retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution,
                    claim_expires_at
                ) VALUES (
                    :id, 'node_handoff', 'default', 100, '{}', 'CLAIMED', :sent_at,
                    NOW(), NOW(), NOW(), 0, 0, :claimed_at, :worker_id, TRUE,
                    'standard_30d', NULL, NULL, FALSE, 'ISOLATED',
                    NOW() + INTERVAL '5 minutes'
                )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': WORKER_ID,
            'command_fingerprint': fingerprint,
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
            VALUES (:id, 'node_handoff_wf', 'RUNNING', 'fail', 0, :id,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'id': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, started_at, created_at,
                 max_retries, execution, retention_class_key)
            VALUES ('NONE', :id, :workflow_id, 0, 'node_0', 'node_handoff', '{}',
                    'default', 100, '{}', FALSE, 'all', FALSE,
                    :node_status, :task_id, :started_at, NOW(),
                    0, 'ISOLATED', 'standard_30d')
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
    _ = broker
    return task_id, claimed_at


async def _beat_count(session: AsyncSession, task_id: str) -> int:
    count = await session.scalar(
        text("""
            SELECT COUNT(*) FROM horsies_heartbeats
            WHERE task_id = :task_id AND role = 'runner'
        """),
        {'task_id': task_id},
    )
    assert isinstance(count, int)
    return count


@pytest.mark.asyncio(loop_scope='function')
async def test_a_start_writes_no_runner_beat(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The start writes no runner heartbeat: the worker process writes them."""
    task_id, claimed_at = await _arrange(session, broker, 'ENQUEUED')

    result = _confirm_ownership_and_set_running(
        task_id,
        WORKER_ID,
        claimed_at=claimed_at,
        orphan_self_heal=False,
    )

    assert isinstance(result, StartedRunning), result
    assert await _task_status(session, task_id) == 'RUNNING'
    assert await _beat_count(session, task_id) == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_a_start_that_does_not_apply_writes_no_beat(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A stale claim generation changes nothing, so no orphan beat exists."""
    task_id, claimed_at = await _arrange(session, broker, 'ENQUEUED')

    result = _confirm_ownership_and_set_running(
        task_id,
        WORKER_ID,
        claimed_at=claimed_at + timedelta(seconds=1),
        orphan_self_heal=False,
    )

    assert result == StoppedBeforeRunning('CLAIM_LOST')
    assert await _task_status(session, task_id) == 'CLAIMED'
    assert await _beat_count(session, task_id) == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_a_failed_workflow_check_keeps_no_beat(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The rollback of the workflow check removes the beat with the transition."""
    task_id, claimed_at = await _arrange(session, broker, 'COMPLETED')

    result = _confirm_ownership_and_set_running(
        task_id,
        WORKER_ID,
        claimed_at=claimed_at,
        orphan_self_heal=False,
    )

    assert result == WORKFLOW_CHECK_FAILED
    assert await _task_status(session, task_id) == 'CLAIMED'
    assert await _beat_count(session, task_id) == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_running_node_keeps_its_tuple_and_first_start(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A retried attempt does not rewrite a node that is already RUNNING."""
    task_id, claimed_at = await _arrange(session, broker, 'RUNNING')
    _, _, ctid_before = await _node_state(session, task_id)

    result = _confirm_ownership_and_set_running(
        task_id,
        WORKER_ID,
        claimed_at=claimed_at,
        orphan_self_heal=False,
    )

    assert isinstance(result, StartedRunning), result
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

    result = _confirm_ownership_and_set_running(
        task_id,
        WORKER_ID,
        claimed_at=claimed_at,
        orphan_self_heal=False,
    )

    assert isinstance(result, StartedRunning), result
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

    result = _confirm_ownership_and_set_running(
        task_id,
        WORKER_ID,
        claimed_at=claimed_at,
        orphan_self_heal=False,
    )

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

    result = _confirm_ownership_and_set_running(
        task_id,
        WORKER_ID,
        claimed_at=claimed_at,
        orphan_self_heal=False,
    )

    assert result == WORKFLOW_CHECK_FAILED
    assert await _task_status(session, task_id) == 'CLAIMED'


@pytest.mark.parametrize('node_status', [None, 'COMPLETED'], ids=['missing', 'terminal'])
@pytest.mark.asyncio(loop_scope='function')
async def test_a_node_that_is_not_linked_with_the_self_heal_on_cancels_the_task(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
    node_status: str | None,
) -> None:
    """The stop before start cancels the orphan (ORPHAN_CANCELLED) in the
    statement after the start: the row moves to history as CANCELLED
    (CANCEL_ORPHAN), with no beat."""
    task_id, claimed_at = await _arrange(session, broker, node_status)

    result = _confirm_ownership_and_set_running(
        task_id,
        WORKER_ID,
        claimed_at=claimed_at,
        orphan_self_heal=True,
    )

    assert result == StoppedBeforeRunning('ORPHAN_CANCELLED')
    live = await session.scalar(
        text('SELECT COUNT(*) FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
        {'id': task_id},
    )
    history = (
        await session.execute(
            text("""
                SELECT status::text, terminalization_kind::text
                FROM horsies_task_history WHERE task_id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).one()
    assert live == 0
    assert tuple(history) == ('CANCELLED', 'CANCEL_ORPHAN')
    assert await _beat_count(session, task_id) == 0
