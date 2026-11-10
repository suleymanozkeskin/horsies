"""The lifecycle writes of a shared thread child against a real database.

A shared thread child runs each task on a seat thread. The thread sets its
row RUNNING with the first runner heartbeat and marks it finalizing through
the one sync pool of the child, which has fewer connections than seats:
threads wait for each other there, bounded. These tests run
``run_thread_seat_task`` on real threads with the real statements.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.tasks import (
    Execution,
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.worker.child_pool import _get_worker_pool, _initialize_worker_pool
from horsies.core.worker.child_protocol import (
    SeatRefusal,
    SharedRunFrame,
    ThreadSeatTask,
    ThreadSeatWork,
    WireResult,
)
from horsies.core.worker.current import set_current_app
from horsies.core.worker.stored_timeout import TimeoutValueInvalid
from horsies.core.worker.thread_runner import run_thread_seat_task
from tests.broker_resources_support import listener_of
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

WORKER_ID = 'thread-lifecycle-worker'
SEATS = 6
TASK_NAME = 'thread_lifecycle_sleep'
BODY_S = 0.5
JOIN_S = 60.0


async def _seed_claimed_task(session: AsyncSession, *, claimed_at: datetime) -> str:
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name=TASK_NAME, good_until=None)
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries, claimed_at,
                 claimed_by_worker_id, claim_expires_at, good_until,
                 enqueue_sha, is_workflow_task, retention_class_key,
                 command_fingerprint_version, command_fingerprint,
                 retain_rerun_input, prepared_rerun_input_disposition,
                 execution)
            VALUES
                (:id, :task_name, 'default', 100, '[]', '{}',
                 'CLAIMED', :sent_at, NOW(), NOW(), NOW(),
                 TRUE, 0, 0, :claimed_at,
                 :worker_id, NOW() + interval '1 minute', NULL,
                 :sha, FALSE, 'standard_30d',
                 1, sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY',
                 'SHARED_THREAD')
        """),
        {
            'id': task_id,
            'task_name': TASK_NAME,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': WORKER_ID,
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
    ).one()


async def _runner_beats(session: AsyncSession, task_id: str) -> list[str]:
    rows = await session.execute(
        text("""
            SELECT sender_id FROM horsies_heartbeats
            WHERE task_id = CAST(:id AS uuid) AND role = 'runner'
        """),
        {'id': task_id},
    )
    return [str(row.sender_id) for row in rows]


def _frame(task_id: str, claimed_at: datetime, work: ThreadSeatWork) -> SharedRunFrame:
    return SharedRunFrame(
        dispatch=1,
        task_id=task_id,
        task_name=TASK_NAME,
        args_json='[]',
        kwargs_json=json.dumps({}),
        worker_id=WORKER_ID,
        is_workflow_task=False,
        claimed_at=claimed_at,
        work=work,
    )


def _task_result(payload: str) -> TaskResult[str, TaskError]:
    """The task result of a wire payload, with a ``str`` ok value."""
    return decode_task_result(loads_json(payload).unwrap(), str)


def _run_on_seats(frames: list[SharedRunFrame]) -> list[WireResult]:
    """Run each frame on its own seat thread at once; return the wire results
    in frame order."""
    results: list[WireResult | None] = [None] * len(frames)

    def seat(index: int, frame: SharedRunFrame) -> None:
        match frame.work:
            case ThreadSeatTask() | SeatRefusal() as work:
                results[index] = run_thread_seat_task(frame, work)
            case _:
                raise AssertionError(frame.work)

    threads = [
        threading.Thread(target=seat, args=(index, frame), name=f'horsies-seat-{index}')
        for index, frame in enumerate(frames)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(JOIN_S)
    assert all(result is not None for result in results), 'a seat did not return'
    return [result for result in results if result is not None]


@pytest_asyncio.fixture(loop_scope='function')
async def thread_app(app: Horsies, broker: PostgresBroker) -> Horsies:
    """The app of the child, with one SHARED_THREAD task, and the sync pool
    of the child open with fewer connections than ``SEATS``."""

    @app.task(task_name=TASK_NAME, execution=Execution.SHARED_THREAD, timeout_ms=30_000)
    def thread_lifecycle_sleep() -> TaskResult[str, TaskError]:
        started = time.time()
        time.sleep(BODY_S)
        return TaskResult(ok=f'{started}:{time.time()}')

    _initialize_worker_pool(listener_of(broker).database_url, max_size=2)
    set_current_app(app)
    _ = thread_lifecycle_sleep
    return app


@pytest.mark.asyncio(loop_scope='function')
async def test_seat_threads_share_a_pool_smaller_than_the_seats(
    session: AsyncSession,
    thread_app: Horsies,  # noqa: ARG001
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    assert _get_worker_pool().max_size < SEATS
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task_ids = [
        await _seed_claimed_task(session, claimed_at=claimed_at) for _ in range(SEATS)
    ]
    frames = [
        _frame(task_id, claimed_at, ThreadSeatTask(timeout_ms=30_000))
        for task_id in task_ids
    ]

    wires = _run_on_seats(frames)

    spans: list[tuple[float, float]] = []
    for wire in wires:
        ok, payload, reason = wire
        assert (ok, reason) == (True, None)
        started, ended = _task_result(payload).unwrap().split(':')
        spans.append((float(started), float(ended)))
    assert max(start for start, _end in spans) < min(end for _start, end in spans)
    for task_id in task_ids:
        row = await _row(session, task_id)
        assert row.status == 'RUNNING'
        assert row.worker_pid == os.getpid()
        assert row.finalizing_at is not None
        assert row.finalizing_by_worker_id == WORKER_ID
        assert await _runner_beats(session, task_id) == [f'{WORKER_ID}:{os.getpid()}']


@pytest.mark.asyncio(loop_scope='function')
async def test_a_claim_of_another_generation_is_lost_on_a_seat_thread(
    session: AsyncSession,
    thread_app: Horsies,  # noqa: ARG001
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task_id = await _seed_claimed_task(session, claimed_at=claimed_at)
    stale = _frame(
        task_id,
        claimed_at - timedelta(seconds=30),
        ThreadSeatTask(timeout_ms=30_000),
    )

    assert _run_on_seats([stale]) == [(False, '', 'CLAIM_LOST')]
    assert (await _row(session, task_id)).status == 'CLAIMED'
    assert await _runner_beats(session, task_id) == []


@pytest.mark.asyncio(loop_scope='function')
async def test_a_refusal_sets_the_row_running_and_fails_before_resolve(
    session: AsyncSession,
    thread_app: Horsies,  # noqa: ARG001
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task_id = await _seed_claimed_task(session, claimed_at=claimed_at)
    refusal = SeatRefusal(
        refusal=TimeoutValueInvalid(
            reason='stored timeout_ms is absent for execution SHARED_THREAD',
        ),
    )

    [wire] = _run_on_seats([_frame(task_id, claimed_at, refusal)])

    ok, payload, _reason = wire
    assert ok is True
    error = _task_result(payload).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_SERIALIZATION_ERROR
    row = await _row(session, task_id)
    assert row.status == 'RUNNING'
    assert row.finalizing_at is not None
