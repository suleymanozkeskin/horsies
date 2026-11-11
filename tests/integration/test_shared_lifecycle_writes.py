"""The lifecycle writes of a shared async child against a real database.

A shared async child sets its row RUNNING, writes the first runner heartbeat
and marks the row finalizing on an async connection pool that it opens on its
event loop. These tests run those writes, with the real statements and the
real terminalization functions, on such a pool.
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
from horsies.core.worker.child_pool import open_shared_async_pool
from horsies.core.worker.child_runner import _PRE_START_CONFIRM_ATTEMPTS
from horsies.core.worker.shared_lifecycle import (
    confirm_until_not_resumed,
    mark_task_finalizing,
)
from tests.broker_resources_support import listener_of
from tests.integration.conftest import compute_test_enqueue_sha
from tests.integration.test_pause_abandon_after_resume import (
    WORKER_ID as PAUSE_WORKER_ID,
    _node_and_task,
    _seed_paused_workflow_with_claimed_node,
)

pytestmark = [pytest.mark.integration]

WORKER_ID = 'shared-lifecycle-worker'


@pytest_asyncio.fixture(loop_scope='function')
async def shared_pool(
    broker: PostgresBroker,
) -> AsyncGenerator[AsyncConnectionPool[AsyncConnection[Any]], None]:
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
    sent_at, sha = compute_test_enqueue_sha(
        task_name='shared_lifecycle',
        good_until=good_until,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries, claimed_at,
                 claimed_by_worker_id, claim_expires_at, good_until,
                 enqueue_sha, is_workflow_task, retention_class_key,
                 command_fingerprint_version, command_fingerprint,
                 retain_rerun_input, prepared_rerun_input_disposition)
            VALUES
                (:id, 'shared_lifecycle', 'default', 100, '[]', '{}',
                 'CLAIMED', :sent_at, NOW(), NOW(), NOW(),
                 TRUE, 0, 0, :claimed_at,
                 :worker_id, NOW() + interval '1 minute', :good_until,
                 :sha, FALSE, 'standard_30d',
                 1, sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': WORKER_ID,
            'good_until': good_until,
            'sha': sha,
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
        shared_pool,
        task_id,
        WORKER_ID,
        False,
        claimed_at,
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
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],
    clean_workflow_tables: None,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Another session holds the row lock. Each attempt of the RUNNING
    transition ends at its bound: the run stops before user code with
    OWNERSHIP_UNCONFIRMED, the row stays CLAIMED, and the pool serves the
    next write once the lock is gone."""
    monkeypatch.setattr(shared_lifecycle, 'RUNNING_ATTEMPT_TIMEOUT_S', 0.5)
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
                shared_pool,
                task_id,
                WORKER_ID,
                False,
                claimed_at,
            ),
            timeout=10.0,
        )
    finally:
        await session.rollback()
    elapsed_s = time.monotonic() - started

    assert result == (False, '', 'OWNERSHIP_UNCONFIRMED')
    assert elapsed_s < _PRE_START_CONFIRM_ATTEMPTS * (
        0.5 + shared_lifecycle.CANCELLED_WRITE_END_S
    )
    assert (await _row(session, task_id)).status == 'CLAIMED'
    assert await _runner_beats(session, task_id) == []
    assert (
        await confirm_until_not_resumed(
            shared_pool,
            task_id,
            WORKER_ID,
            False,
            claimed_at,
        )
        is None
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_a_claim_of_another_generation_is_lost(
    session: AsyncSession,
    shared_pool: AsyncConnectionPool[AsyncConnection[Any]],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task_id = await _seed_claimed_task(session, claimed_at=claimed_at)

    result = await confirm_until_not_resumed(
        shared_pool,
        task_id,
        WORKER_ID,
        False,
        claimed_at - timedelta(seconds=30),
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
        shared_pool,
        task_id,
        WORKER_ID,
        False,
        claimed_at,
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
        shared_pool,
        task_id,
        PAUSE_WORKER_ID,
        True,
        claimed_at,
    )

    assert result == (False, '', 'WORKFLOW_STOPPED')
    node_status, node_task, workflow_status, _live = await _node_and_task(
        session,
        workflow_id,
        task_id,
    )
    assert (node_status, node_task, workflow_status) == ('READY', None, 'PAUSED')
