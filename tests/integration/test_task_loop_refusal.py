"""A child that could not create its task loop returns its task to the queue.

The child stops before the RUNNING update, so the row is still CLAIMED and no
task code ran. Finalize releases the claim through the fenced unclaim: the row
goes back to PENDING, with no attempt row and no retry used. A workflow node
keeps its status and its task id, which match the PENDING row, as they do
after a dispatch that never reached a child.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.types.result import is_ok
from horsies.core.worker.child_protocol import RssUnreadable, TaskLoopUnavailable
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.slots import TaskFinished
from horsies.core.worker.worker import Worker
from tests.fixtures.slots.handles import finished_handle
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

TEST_WORKER_ID = 'w-task-loop-refusal-test'
CLAIM_GENERATION = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
OTHER_GENERATION = datetime(2026, 11, 9, 12, 5, tzinfo=timezone.utc)
TASK_NAME = 'task_loop_refusal_test'

_TASK_LOOP_REFUSED = TaskFinished(
    False,
    '',
    'TASK_LOOP_UNAVAILABLE',
    RssUnreadable(),
    TaskLoopUnavailable(detail='OSError: [Errno 24] Too many open files'),
)


def _make_worker(engine: AsyncEngine) -> Worker:
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
        listener=MagicMock(),
        cfg=cfg,
    )
    worker.worker_instance_id = TEST_WORKER_ID
    return worker


async def _insert_claimed_task(
    session: AsyncSession,
    *,
    is_workflow_task: bool,
) -> str:
    """Insert a task that this worker claimed at ``CLAIM_GENERATION``."""
    task_id = str(uuid.uuid4())
    sent_at, enqueue_sha = compute_test_enqueue_sha(task_name=TASK_NAME)
    await session.execute(
        text("""
            INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs,
                status, sent_at, enqueued_at, created_at, updated_at,
                claimed, claimed_at, claimed_by_worker_id, claim_expires_at,
                retry_count, max_retries, enqueue_sha, is_workflow_task,
                retention_class_key, command_fingerprint_version,
                command_fingerprint, retain_rerun_input,
                prepared_rerun_input_disposition
            ) VALUES (
                :id, :task_name, 'default', 100, '[]', '{}',
                'CLAIMED', :sent_at, NOW(), NOW(), NOW(),
                TRUE, :claimed_at, :worker_id, NOW() + INTERVAL '5 minutes',
                0, 3, :enqueue_sha, :is_workflow_task,
                'standard_30d', 1,
                sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                FALSE, 'NEVER_ELIGIBLE'
            )
        """),
        {
            'id': task_id,
            'task_name': TASK_NAME,
            'sent_at': sent_at,
            'claimed_at': CLAIM_GENERATION,
            'worker_id': TEST_WORKER_ID,
            'enqueue_sha': enqueue_sha,
            'is_workflow_task': is_workflow_task,
        },
    )
    await session.commit()
    return task_id


async def _link_enqueued_workflow_node(session: AsyncSession, task_id: str) -> str:
    """Link the task to a node that is ENQUEUED, as a claimed node is."""
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES
                (:id, 'task_loop_refusal_workflow', 'RUNNING', 'FAIL', 0, :id,
                 NOW(), NOW(), NOW(), NOW())
        """),
        {'id': workflow_id},
    )
    node_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES
                (:id, :workflow_id, 0, 'task_loop_refusal_node',
                 :task_name, '[]', '{}', 'default', 100, '{}',
                 FALSE, 'all', FALSE, 'ENQUEUED', :task_id, NOW())
        """),
        {
            'id': node_id,
            'workflow_id': workflow_id,
            'task_name': TASK_NAME,
            'task_id': task_id,
        },
    )
    await session.commit()
    return node_id


async def _task_row(session: AsyncSession, task_id: str) -> tuple[str, bool, str | None, int]:
    row = (
        await session.execute(
            text("""
                SELECT status, claimed, claimed_by_worker_id, retry_count
                FROM horsies_tasks WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).one()
    return str(row.status), bool(row.claimed), row.claimed_by_worker_id, int(row.retry_count)


async def _attempt_count(session: AsyncSession, task_id: str) -> int:
    row = (
        await session.execute(
            text(
                'SELECT count(*) AS n FROM horsies_task_attempts '
                'WHERE task_id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).one()
    return int(row.n)


async def _node_row(session: AsyncSession, node_id: str) -> tuple[str, str | None]:
    row = (
        await session.execute(
            text("""
                SELECT status, CAST(task_id AS text) AS task_id
                FROM horsies_workflow_tasks WHERE id = CAST(:id AS uuid)
            """),
            {'id': node_id},
        )
    ).one()
    return str(row.status), row.task_id


@pytest.mark.asyncio(loop_scope='function')
async def test_a_plain_task_returns_to_the_queue_with_no_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id = await _insert_claimed_task(session, is_workflow_task=False)
    worker = _make_worker(engine)

    result = await worker._finalize_after(
        finished_handle(_TASK_LOOP_REFUSED),
        task_id,
        is_workflow_task=False,
        task_name=TASK_NAME,
        claimed_at=CLAIM_GENERATION,
    )

    assert is_ok(result)
    assert await _task_row(session, task_id) == ('PENDING', False, None, 0)
    assert await _attempt_count(session, task_id) == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_a_workflow_task_returns_to_the_queue_and_its_node_still_matches(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The node stays ENQUEUED with the same task id, so the engine neither
    enqueues a second task for it nor loses the PENDING one."""
    task_id = await _insert_claimed_task(session, is_workflow_task=True)
    node_id = await _link_enqueued_workflow_node(session, task_id)
    worker = _make_worker(engine)

    result = await worker._finalize_after(
        finished_handle(_TASK_LOOP_REFUSED),
        task_id,
        is_workflow_task=True,
        task_name=TASK_NAME,
        claimed_at=CLAIM_GENERATION,
    )

    assert is_ok(result)
    assert await _task_row(session, task_id) == ('PENDING', False, None, 0)
    assert await _attempt_count(session, task_id) == 0
    assert await _node_row(session, node_id) == ('ENQUEUED', task_id)


@pytest.mark.asyncio(loop_scope='function')
async def test_the_release_is_fenced_to_the_claim_generation(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A report from an older claim of the same row releases nothing."""
    task_id = await _insert_claimed_task(session, is_workflow_task=False)
    worker = _make_worker(engine)

    result = await worker._finalize_after(
        finished_handle(_TASK_LOOP_REFUSED),
        task_id,
        is_workflow_task=False,
        task_name=TASK_NAME,
        claimed_at=OTHER_GENERATION,
    )

    assert is_ok(result)
    assert await _task_row(session, task_id) == ('CLAIMED', True, TEST_WORKER_ID, 0)
    assert await _attempt_count(session, task_id) == 0
