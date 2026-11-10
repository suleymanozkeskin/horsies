"""The runner heartbeat statement of the worker process, against Postgres.

The worker writes one statement per interval for every task its children
hold. Each row is fenced: RUNNING, owned by this worker, at the claim
generation of the dispatch, and without the finalizing marker. The reaper
reads the newest runner beat, so a beat that lands keeps a running task out
of stale recovery, and a fence that fails leaves the row to it.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_ok
from horsies.core.worker.live_dispatches import LiveDispatch
from horsies.core.worker.runner_heartbeat import BeatsWritten, PostgresBeatWriter
from tests.broker_resources_support import listener_of
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

WORKER_ID = 'beat-worker-1'
OTHER_WORKER_ID = 'beat-worker-2'
STARTED_LONG_AGO = timedelta(minutes=30)


async def _insert_running_task(
    session: AsyncSession,
    *,
    claimed_at: datetime,
    worker_id: str = WORKER_ID,
    status: str = 'RUNNING',
    finalizing_at: datetime | None = None,
) -> str:
    """Insert one task row that started long ago, with no heartbeat."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='runner_beat_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, claimed_at, claimed_by_worker_id, started_at,
                 finalizing_at, enqueue_sha, is_workflow_task,
                 worker_hostname, worker_pid, worker_process_name,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'runner_beat_test', 'default', 100, '[]', '{}',
                 :status, :sent_at, NOW(), NOW(), FALSE, 0,
                 0, :claimed_at, :worker_id, :started_at,
                 :finalizing_at, :enqueue_sha, FALSE,
                 'beat-host', 4242, 'beat-proc',
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'status': status,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': worker_id,
            'started_at': datetime.now(timezone.utc) - STARTED_LONG_AGO,
            'finalizing_at': finalizing_at,
            'enqueue_sha': sha,
        },
    )
    return task_id


async def _beats(session: AsyncSession, task_id: str) -> list[Any]:
    rows = await session.execute(
        text("""
            SELECT sender_id, role, pid
            FROM horsies_heartbeats
            WHERE task_id = CAST(:task_id AS uuid)
        """),
        {'task_id': task_id},
    )
    return list(rows.fetchall())


async def _status(session: AsyncSession, task_id: str) -> str:
    status = await session.scalar(
        text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
        {'id': task_id},
    )
    assert isinstance(status, str)
    return status


def _writer(broker: PostgresBroker) -> PostgresBeatWriter:
    return PostgresBeatWriter(
        database_url=listener_of(broker).database_url,
        connect_kwargs={},
        worker_id=WORKER_ID,
    )


def _dispatch(
    task_id: str, claimed_at: datetime | None, ticket: int = 1
) -> LiveDispatch:
    return LiveDispatch(ticket=ticket, task_id=task_id, claimed_at=claimed_at, pid=1)


def _claimed_at() -> datetime:
    return datetime.now(timezone.utc) - timedelta(minutes=31)


@pytest.mark.asyncio(loop_scope='function')
async def test_a_running_row_of_this_claim_gets_a_beat(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = _claimed_at()
    task_id = await _insert_running_task(session, claimed_at=claimed_at)
    await session.commit()
    writer = _writer(broker)

    try:
        outcome = writer.write((_dispatch(task_id, claimed_at),))
    finally:
        writer.close()

    assert outcome == BeatsWritten(rows=1)
    assert [tuple(row) for row in await _beats(session, task_id)] == [
        (WORKER_ID, 'runner', os.getpid()),
    ]


@pytest.mark.parametrize(
    ('case', 'row_worker', 'row_status', 'generation_shift', 'finalizing'),
    [
        ('claimed, not started', WORKER_ID, 'CLAIMED', timedelta(0), False),
        ('owned by another worker', OTHER_WORKER_ID, 'RUNNING', timedelta(0), False),
        ('claimed again since', WORKER_ID, 'RUNNING', timedelta(seconds=5), False),
        ('child ended the attempt', WORKER_ID, 'RUNNING', timedelta(0), True),
        ('back in the queue', WORKER_ID, 'PENDING', timedelta(0), False),
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_a_row_outside_the_fence_gets_no_beat(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    case: str,
    row_worker: str,
    row_status: str,
    generation_shift: timedelta,
    finalizing: bool,
) -> None:
    dispatch_claimed_at = _claimed_at()
    task_id = await _insert_running_task(
        session,
        claimed_at=dispatch_claimed_at + generation_shift,
        worker_id=row_worker,
        status=row_status,
        finalizing_at=datetime.now(timezone.utc) if finalizing else None,
    )
    await session.commit()
    writer = _writer(broker)

    try:
        outcome = writer.write((_dispatch(task_id, dispatch_claimed_at),))
    finally:
        writer.close()

    assert outcome == BeatsWritten(rows=0), case
    assert await _beats(session, task_id) == [], case


@pytest.mark.asyncio(loop_scope='function')
async def test_a_null_generation_fences_on_the_worker_only(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    ours = await _insert_running_task(session, claimed_at=_claimed_at())
    theirs = await _insert_running_task(
        session,
        claimed_at=_claimed_at(),
        worker_id=OTHER_WORKER_ID,
    )
    await session.commit()
    writer = _writer(broker)

    try:
        outcome = writer.write(
            (_dispatch(ours, None, ticket=1), _dispatch(theirs, None, ticket=2)),
        )
    finally:
        writer.close()

    assert outcome == BeatsWritten(rows=1)
    assert len(await _beats(session, ours)) == 1
    assert await _beats(session, theirs) == []


@pytest.mark.asyncio(loop_scope='function')
async def test_one_statement_beats_every_fenced_row(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed = [_claimed_at() - timedelta(seconds=index) for index in range(3)]
    task_ids = [
        await _insert_running_task(session, claimed_at=claimed_at)
        for claimed_at in claimed
    ]
    await session.commit()
    dispatches = tuple(
        _dispatch(task_id, claimed_at, ticket=index)
        for index, (task_id, claimed_at) in enumerate(zip(task_ids, claimed), start=1)
    )
    writer = _writer(broker)

    try:
        outcome = writer.write(dispatches)
    finally:
        writer.close()

    assert outcome == BeatsWritten(rows=3)
    for task_id in task_ids:
        assert len(await _beats(session, task_id)) == 1


@pytest.mark.asyncio(loop_scope='function')
async def test_the_reaper_leaves_a_beaten_row_and_fails_an_unbeaten_one(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Enter at the reaper: only the newest runner beat decides staleness."""
    beaten_claim = _claimed_at()
    beaten = await _insert_running_task(session, claimed_at=beaten_claim)
    unbeaten = await _insert_running_task(session, claimed_at=_claimed_at())
    await session.commit()
    writer = _writer(broker)
    try:
        assert writer.write((_dispatch(beaten, beaten_claim),)) == BeatsWritten(rows=1)
    finally:
        writer.close()

    result = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=60_000)

    assert is_ok(result)
    assert await _status(session, beaten) == 'RUNNING'
    assert await _status(session, unbeaten) == 'FAILED'
