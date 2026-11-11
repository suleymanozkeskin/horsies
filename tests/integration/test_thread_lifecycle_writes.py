"""The lifecycle writes of a shared thread child against a real database.

A shared thread child runs each task on a seat thread. The thread sets its
row RUNNING with the first runner heartbeat and marks it finalizing through
the one sync pool of the child, which has fewer connections than seats:
threads wait for each other there, bounded. These tests run the real seat
threads (``ThreadSeats``, ``ThreadRunTable``, ``PostgresThreadSeats``) with
the real statements; a recording writer stands in for the frame writer.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import dataclasses
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
    ChildFrame,
    RssMb,
    SeatFinished,
    SeatRefusal,
    SeatTimedOut,
    SharedResultFrame,
    SharedRunFrame,
    ThreadSeatTask,
    ThreadSeatWork,
    WireResult,
)
from horsies.core.worker.current import set_current_app
from horsies.core.worker.stored_timeout import TimeoutValueInvalid
from horsies.core.worker.thread_child import ThreadRunTable, ThreadSeats
from horsies.core.worker.thread_runner import PostgresThreadSeats
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
        dispatch=1,  # _run_on_seats numbers the runs
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


class _RecordingWriter:
    """Records the frames of the run table, as the writer thread would."""

    def __init__(self) -> None:
        self.frames: list[ChildFrame] = []
        self._lock = threading.Lock()

    def send(self, frame: ChildFrame) -> None:
        with self._lock:
            self.frames.append(frame)

    def send_result(self, task_name: str, frame: SharedResultFrame) -> None:
        _ = task_name
        self.send(frame)

    def results(self) -> list[SharedResultFrame]:
        with self._lock:
            return [item for item in self.frames if isinstance(item, SharedResultFrame)]


def _run_on_seats(frames: list[SharedRunFrame]) -> list[WireResult]:
    """Run each frame on its own seat thread at once, through the run table
    and the seat program of the child; return the wire of each end in frame
    order. Run ``n`` gets dispatch number ``n + 1``."""
    writer = _RecordingWriter()
    table = ThreadRunTable(len(frames), writer)
    seats = ThreadSeats(len(frames), table, PostgresThreadSeats(), lambda: RssMb(1.0))
    assert seats.start().is_ok()
    for index, frame in enumerate(frames):
        numbered = dataclasses.replace(frame, dispatch=index + 1)
        match numbered.work:
            case ThreadSeatTask() | SeatRefusal() as work:
                table.add(numbered, work)
                seats.give(numbered, work)
            case _:
                raise AssertionError(numbered.work)
    deadline = time.monotonic() + JOIN_S
    while len(writer.results()) < len(frames):
        assert time.monotonic() < deadline, 'a seat did not end its run'
        time.sleep(0.01)
    seats.stop(JOIN_S)
    ends = {frame.dispatch: frame.end for frame in writer.results()}
    wires: list[WireResult] = []
    for dispatch in range(1, len(frames) + 1):
        end = ends[dispatch]
        assert isinstance(end, SeatFinished), end
        wires.append((end.ok, end.result_json, end.failed_reason))
    return wires


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


@pytest.mark.asyncio(loop_scope='function')
async def test_a_seat_that_gets_its_connection_after_the_deadline_leaves_the_row_claimed(
    session: AsyncSession,
    thread_app: Horsies,  # noqa: ARG001
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Every connection of the pool is held, so the seat thread waits; the
    deadline of its run fires during the wait. When the seat gets its
    connection, it does not set the row RUNNING: the row stays CLAIMED with
    no runner heartbeat, and the run ends with the timeout."""
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    task_id = await _seed_claimed_task(session, claimed_at=claimed_at)
    work = ThreadSeatTask(timeout_ms=300)
    frame = _frame(task_id, claimed_at, work)
    pool = _get_worker_pool()
    held = [pool.getconn() for _ in range(pool.max_size)]
    writer = _RecordingWriter()
    table = ThreadRunTable(1, writer)
    deadlines = threading.Thread(target=table.serve_deadlines, daemon=True)
    deadlines.start()
    seats = ThreadSeats(1, table, PostgresThreadSeats(), lambda: RssMb(1.0))
    assert seats.start().is_ok()
    try:
        table.add(frame, work)
        seats.give(frame, work)
        await asyncio.sleep(0.8)
        assert writer.results() == []
    finally:
        for connection in held:
            pool.putconn(connection)
    ends_by = time.monotonic() + JOIN_S
    while not writer.results():
        assert time.monotonic() < ends_by, 'the seat did not end its run'
        await asyncio.sleep(0.01)
    seats.stop(JOIN_S)
    table.stop()
    deadlines.join(JOIN_S)

    assert [type(item).__name__ for item in writer.frames] == [
        'DeadlineFiredFrame',
        'SharedResultFrame',
    ]
    assert writer.results()[0].end == SeatTimedOut()
    row = await _row(session, task_id)
    assert row.status == 'CLAIMED'
    assert row.finalizing_at is None
    assert await _runner_beats(session, task_id) == []
