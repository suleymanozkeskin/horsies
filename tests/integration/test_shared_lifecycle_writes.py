"""The lifecycle writes of a shared async child against a real database.

A shared async child sets its row RUNNING and writes the first runner
heartbeat through the awaitable methods of its core handle, and marks the
row finalizing on an async connection pool that it opens on its event loop.
These tests run those writes, with the real functions, on the core handle of
the suite and on such a pool.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import OutcomeCode
from horsies.core.worker import shared_lifecycle
from horsies.core.worker.child_pool import _initialize_worker_pool, open_shared_async_pool
from horsies.core.worker.shared_lifecycle import (
    confirm_until_not_resumed,
    mark_task_finalizing,
)
from tests.broker_resources_support import listener_of
from tests.fixtures.child_core import wait_for_start_statements_to_end
from tests.integration.conftest import compute_test_command_fingerprint
from tests.integration.test_pause_abandon_after_resume import (
    WORKER_ID as PAUSE_WORKER_ID,
    _node_and_task,
    _seed_paused_workflow_with_claimed_node,
)
from tests.integration.test_workflow_node_handoff import (
    WORKER_ID as NODE_WORKER_ID,
    _seed_claimed_workflow_task,
    _seed_running_workflow_with_node,
)

pytestmark = [pytest.mark.integration]

WORKER_ID = 'shared-lifecycle-worker'


@pytest_asyncio.fixture(loop_scope='function')
async def shared_pool(
    broker: PostgresBroker,
) -> AsyncGenerator[AsyncConnectionPool[AsyncConnection[Any]], None]:
    """The async pool of the child for the finalizing marker. The sync pool
    of the child opens as well: the reference start of a dual-run runs on
    it."""
    _initialize_worker_pool(listener_of(broker).database_url)
    pool = await open_shared_async_pool(
        listener_of(broker).database_url,
        connect_kwargs={},
        min_size=0,
        max_size=2,
        check_on_checkout=True,
    )
    yield pool
    await pool.close(timeout=5.0)


async def _seed_claimed_task(
    session: AsyncSession,
    *,
    claimed_at: datetime,
    good_until: datetime | None = None,
) -> str:
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name='shared_lifecycle',
        good_until=good_until,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, retry_count, max_retries,
                    claimed_at, claimed_by_worker_id, claim_expires_at, good_until,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution
                ) VALUES (
                    :id, 'shared_lifecycle', 'default', 100, '{}', 'CLAIMED', :sent_at,
                    NOW(), NOW(), NOW(), 0, 0, :claimed_at, :worker_id,
                    NOW() + interval '1 minute', :good_until, FALSE, 'standard_30d', 1,
                    :command_fingerprint, FALSE, 'ISOLATED'
                )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': WORKER_ID,
            'good_until': good_until,
            'command_fingerprint': fingerprint,
        },
    )
    await session.commit()
    return task_id


async def _row(session: AsyncSession, task_id: str) -> Any:
    return (
        await session.execute(
            text("""
                SELECT status, worker_pid, finalizing_at, finalizing_by_worker_id
                FROM horsies_tasks WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).one_or_none()


async def _runner_beats(session: AsyncSession, task_id: str) -> list[str]:
    rows = await session.execute(
        text("""
            SELECT sender_id FROM horsies_heartbeats
            WHERE task_id = CAST(:id AS uuid) AND role = 'runner'
        """),
        {'id': task_id},
    )
    return [str(row.sender_id) for row in rows]


@pytest.mark.asyncio(loop_scope='function')
async def test_the_running_update_writes_the_first_beat_and_the_marker_follows(
    session: AsyncSession,
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task_id = await _seed_claimed_task(session, claimed_at=claimed_at)

    started = await confirm_until_not_resumed(
        task_id,
        WORKER_ID,
        False,
        claimed_at,
        orphan_self_heal=True,
    )
    running = await _row(session, task_id)
    await session.commit()
    await mark_task_finalizing(shared_pool, task_id, WORKER_ID, claimed_at)
    marked = await _row(session, task_id)

    assert started is None
    assert running.status == 'RUNNING'
    assert running.worker_pid == os.getpid()
    assert running.finalizing_at is None
    assert await _runner_beats(session, task_id) == [f'{WORKER_ID}:{os.getpid()}']
    assert marked.finalizing_at is not None
    assert marked.finalizing_by_worker_id == WORKER_ID


@pytest.mark.asyncio(loop_scope='function')
async def test_a_running_update_that_waits_on_a_row_lock_ends_at_its_bound(
    session: AsyncSession,
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],  # noqa: ARG001
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Another session holds the row lock. The core sends the cancel request
    of the start at its statement bound (``RUNNING_STATEMENTS_TIMEOUT_S``):
    the run stops before user code with OWNERSHIP_UNCONFIRMED, before the
    wait of the seat (``RUNNING_ATTEMPT_TIMEOUT_S``) ends. The cancelled
    start does not commit once the lock is gone: the row stays CLAIMED with
    no beat."""
    statements_s = shared_lifecycle.RUNNING_STATEMENTS_TIMEOUT_S
    cancel_end_s = shared_lifecycle.CANCELLED_WRITE_END_S
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task_id = await _seed_claimed_task(session, claimed_at=claimed_at)
    await session.execute(
        text('SELECT id FROM horsies_tasks WHERE id = CAST(:id AS uuid) FOR UPDATE'),
        {'id': task_id},
    )
    started = time.monotonic()

    try:
        result = await asyncio.wait_for(
            confirm_until_not_resumed(
                task_id,
                WORKER_ID,
                False,
                claimed_at,
                orphan_self_heal=True,
            ),
            timeout=shared_lifecycle.RUNNING_ATTEMPT_TIMEOUT_S + cancel_end_s,
        )
        elapsed_s = time.monotonic() - started
    finally:
        await session.rollback()

    assert result == (False, '', 'OWNERSHIP_UNCONFIRMED')
    assert statements_s <= elapsed_s < statements_s + cancel_end_s
    assert await wait_for_start_statements_to_end(session) == 0
    assert (await _row(session, task_id)).status == 'CLAIMED'
    assert await _runner_beats(session, task_id) == []


@pytest.mark.asyncio(loop_scope='function')
async def test_a_claim_of_another_generation_is_lost(
    session: AsyncSession,
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task_id = await _seed_claimed_task(session, claimed_at=claimed_at)

    result = await confirm_until_not_resumed(
        task_id,
        WORKER_ID,
        False,
        claimed_at - timedelta(seconds=30),
        orphan_self_heal=True,
    )

    assert result == (False, '', 'CLAIM_LOST')
    assert (await _row(session, task_id)).status == 'CLAIMED'
    assert await _runner_beats(session, task_id) == []


@pytest.mark.asyncio(loop_scope='function')
async def test_a_claim_past_its_deadline_expires_before_start(
    session: AsyncSession,
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    task_id = await _seed_claimed_task(
        session,
        claimed_at=claimed_at,
        good_until=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    result = await confirm_until_not_resumed(
        task_id,
        WORKER_ID,
        False,
        claimed_at,
        orphan_self_heal=True,
    )

    assert result == (False, '', OutcomeCode.TASK_EXPIRED.value)
    assert await _runner_beats(session, task_id) == []


@pytest.mark.asyncio(loop_scope='function')
async def test_a_paused_workflow_abandons_the_claim_through_the_async_path(
    session: AsyncSession,
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    workflow_id, task_id = await _seed_paused_workflow_with_claimed_node(
        session,
        claimed_at,
    )

    result = await confirm_until_not_resumed(
        task_id,
        PAUSE_WORKER_ID,
        True,
        claimed_at,
        orphan_self_heal=True,
    )

    assert result == (False, '', 'WORKFLOW_STOPPED')
    node_status, node_task, workflow_status, _live = await _node_and_task(
        session,
        workflow_id,
        task_id,
    )
    assert (node_status, node_task, workflow_status) == ('READY', None, 'PAUSED')


@pytest.mark.asyncio(loop_scope='function')
async def test_a_failed_workflow_check_undoes_the_running_update(
    session: AsyncSession,
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A workflow task's start is one statement: a node that is not linked
    writes no RUNNING update and no first beat. With the orphan self-heal of
    the run off, the stop before start leaves the row CLAIMED."""
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    task_id = await _seed_claimed_workflow_task(session, claimed_at)
    await _seed_running_workflow_with_node(session, task_id, 'COMPLETED')
    await session.commit()

    result = await confirm_until_not_resumed(
        task_id,
        NODE_WORKER_ID,
        True,
        claimed_at,
        orphan_self_heal=False,
    )

    assert result == (False, '', 'WORKFLOW_CHECK_FAILED')
    assert (await _row(session, task_id)).status == 'CLAIMED'
    assert await _runner_beats(session, task_id) == []


@pytest.mark.asyncio(loop_scope='function')
async def test_a_node_that_is_not_linked_with_the_self_heal_on_cancels_the_orphan(
    session: AsyncSession,
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],  # noqa: ARG001
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """With the orphan self-heal of the run on, the stop before start moves
    the row to history as CANCELLED (CANCEL_ORPHAN): no RUNNING update, no
    first beat."""
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    task_id = await _seed_claimed_workflow_task(session, claimed_at)
    await _seed_running_workflow_with_node(session, task_id, 'COMPLETED')
    await session.commit()

    result = await confirm_until_not_resumed(
        task_id,
        NODE_WORKER_ID,
        True,
        claimed_at,
        orphan_self_heal=True,
    )

    assert result == (False, '', 'ORPHAN_CANCELLED')
    assert await _row(session, task_id) is None
    history = (
        await session.execute(
            text("""
                SELECT status::text AS status,
                       terminalization_kind::text AS terminalization_kind
                FROM horsies_task_history
                WHERE task_id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).one()
    assert (history.status, history.terminalization_kind) == ('CANCELLED', 'CANCEL_ORPHAN')
    assert await _runner_beats(session, task_id) == []
