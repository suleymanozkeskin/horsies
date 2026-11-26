"""Layer 2 e2e: worker health API against a real worker process.

Unlike the integration test (which uses a fake in-test responder), these spin
a REAL `horsies worker` subprocess, so they exercise the worker's actual ping
subscription, `_ping_responder_loop`, `_handle_ping`, and worker-state writes.
A worker that fails to subscribe to the ping channel at startup — or leaks/
crashes during start — would not reply here.

The class reads (`get_execution_class_report_async`, `get_execution_class_coverage_async`)
read the snapshot that a real worker of a shared profile writes: its budgets
and the classes each one serves.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.class_coverage import (
    BudgetLoad,
    StreamCoverage,
    StreamServed,
    StreamUnserved,
    WorkerExecutionClassLoad,
)
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok, is_ok
from horsies.core.types.status import TaskStatus

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import read_succeeded, unwrap_send
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import shared_async_tasks
from tests.e2e.tasks.profiles import SHARED_ONLY_1X4, SPLIT_SHARED_2X8, SPLIT_SHARED_SEATS

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'

ISOLATED = Execution.ISOLATED
ASYNC = Execution.SHARED_ASYNC
THREAD = Execution.SHARED_THREAD


def _make_ready_check():
    """Ready check that polls the e2e healthcheck task until it completes."""
    handle = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            r = basic_tasks.healthcheck.send()
            if not is_ok(r):
                return False
            handle = r.ok_value
        info = handle.info()
        return (
            is_ok(info)
            and info.ok_value is not None
            and info.ok_value.status == TaskStatus.COMPLETED
        )

    return _check


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='session')
async def test_ping_database_real(broker: PostgresBroker) -> None:
    """ping_database_async succeeds against the real e2e database."""
    result = await broker.ping_database_async()
    assert is_ok(result)
    assert result.ok_value.latency_ms >= 0.0


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='session')
async def test_ping_workers_reaches_real_worker(broker: PostgresBroker) -> None:
    """A live worker replies to a broadcast ping (real responder round-trip)."""
    ready = _make_ready_check()
    with run_worker(DEFAULT_INSTANCE, profile='isolated-1', ready_check=ready):
        # Retry the broadcast briefly: the worker is ready (healthcheck passed),
        # but allow for ping-subscription/dispatcher warmup.
        pongs: list = []
        for _ in range(10):
            result = await broker.ping_workers_async(timeout_seconds=2.0)
            assert is_ok(result)
            pongs = result.ok_value
            if pongs:
                break
            await asyncio.sleep(0.5)

        assert pongs, 'no live worker replied to broadcast ping'
        pong = pongs[0]
        assert pong.worker_id
        assert pong.pid > 0
        assert pong.round_trip_ms >= 0.0

        # Fast gate: min_responses=1 returns well before the timeout window.
        loop = asyncio.get_running_loop()
        start = loop.time()
        gate = await broker.ping_workers_async(timeout_seconds=5.0, min_responses=1)
        elapsed = loop.time() - start
        assert is_ok(gate)
        assert len(gate.ok_value) == 1
        assert elapsed < 4.0, 'min_responses=1 should not wait the full window'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='session')
async def test_ping_and_worker_state_correlate(broker: PostgresBroker) -> None:
    """The responding worker also appears in list_worker_states (same worker_id)."""
    ready = _make_ready_check()
    with run_worker(DEFAULT_INSTANCE, profile='isolated-1', ready_check=ready):
        # Get a responsive worker id via ping.
        responsive_id: str | None = None
        for _ in range(10):
            ping = await broker.ping_workers_async(timeout_seconds=2.0)
            assert is_ok(ping)
            if ping.ok_value:
                responsive_id = ping.ok_value[0].worker_id
                break
            await asyncio.sleep(0.5)
        assert responsive_id is not None, 'worker never replied to ping'

        # The worker writes a state snapshot on its heartbeat loop; poll for it.
        snapshot = None
        for _ in range(20):
            states = await broker.list_worker_states_async()
            assert is_ok(states)
            match = [s for s in states.ok_value if s.worker_id == responsive_id]
            if match:
                snapshot = match[0]
                break
            await asyncio.sleep(0.5)

        assert (
            snapshot is not None
        ), 'responsive worker never appeared in list_worker_states'
        assert snapshot.pid > 0
        assert 'default' in snapshot.queues

        # Single-worker read path agrees with the list.
        single = await broker.get_worker_state_async(responsive_id)
        assert is_ok(single)
        assert single.ok_value is not None
        assert single.ok_value.worker_id == responsive_id


def _make_shared_ready_check():
    """Ready check for a worker with no isolated child: a shared async task
    that completes."""
    handle = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(shared_async_tasks.shared_sleep.send(seconds=0))
        return read_succeeded(handle.get(timeout_ms=2000))

    return _check


async def _forget_other_workers(session: AsyncSession) -> None:
    """Delete the snapshots of the workers of earlier tests, which stay live
    for `WORKER_STATE_LIVE_WINDOW_SECONDS` after their exit, so the class reads
    see only the worker of this test."""
    await session.execute(text('DELETE FROM horsies_worker_states'))
    await session.commit()


async def _pending_later(session: AsyncSession, task_id: UUID) -> None:
    """Move the start of a PENDING row an hour ahead: the row stays PENDING,
    and no worker claims it before the read."""
    await session.execute(
        text(
            "UPDATE horsies_tasks SET enqueued_at = now() + interval '1 hour' "
            "WHERE id = :id AND status = 'PENDING'"
        ),
        {'id': task_id},
    )
    await session.commit()


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='session')
async def test_the_report_gives_the_seats_of_the_shared_budget(
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """A worker of 1 isolated child and 2 shared async children: the report
    gives its isolated budget, which serves ISOLATED and SHARED_THREAD rows,
    and its shared budget of every seat, with the row it runs; the pending
    SHARED_ASYNC stream is served with the seats of the shared budget."""
    await _forget_other_workers(session)
    pending = unwrap_send(await shared_async_tasks.shared_sleep.send_async(seconds=0))
    await _pending_later(session, pending.task_id)

    with run_worker(
        DEFAULT_INSTANCE,
        profile=SPLIT_SHARED_2X8,
        ready_check=_make_ready_check(),
    ):
        running = unwrap_send(await shared_async_tasks.shared_sleep.send_async(seconds=20))
        await wait_for_status(
            current_resources(broker).session_factory,
            running.task_id,
            'RUNNING',
            timeout_s=30.0,
        )
        worker_id = (
            await session.execute(
                text('SELECT claimed_by_worker_id FROM horsies_tasks WHERE id = :id'),
                {'id': running.task_id},
            )
        ).scalar_one()
        await session.commit()

        match await broker.get_execution_class_report_async():
            case Ok(report):
                pass
            case Err(error):
                raise AssertionError(error.message)

    seats = 2 * SPLIT_SHARED_SEATS
    (worker,) = report.workers
    assert worker == WorkerExecutionClassLoad(
        worker_id=worker_id,
        snapshot_at=worker.snapshot_at,
        budgets=(
            BudgetLoad(ISOLATED, 1, (ISOLATED, THREAD), in_flight=0),
            BudgetLoad(ASYNC, seats, (ASYNC,), in_flight=1),
        ),
    )
    (stream,) = report.streams
    assert (stream.queue, stream.execution) == ('default', ASYNC)
    assert stream.coverage == StreamServed(workers=1, capacity=seats)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='session')
async def test_a_worker_with_only_shared_children_serves_only_shared_rows(
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """A worker of 1 shared async child of 4 seats serves the SHARED_ASYNC
    stream of its queue, and no ISOLATED row: that stream is unserved."""
    await _forget_other_workers(session)
    unserved = unwrap_send(await basic_tasks.healthcheck.send_async())
    pending = unwrap_send(await shared_async_tasks.shared_sleep.send_async(seconds=0))
    await _pending_later(session, pending.task_id)

    with run_worker(
        DEFAULT_INSTANCE,
        profile=SHARED_ONLY_1X4,
        ready_check=_make_shared_ready_check(),
    ):
        match await broker.get_execution_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)
        status = (
            await session.execute(
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
                {'id': unserved.task_id},
            )
        ).scalar_one()
        await session.commit()

    assert status == 'PENDING'
    assert coverage.streams == (
        StreamCoverage('default', ISOLATED, StreamUnserved()),
        StreamCoverage('default', ASYNC, StreamServed(workers=1, capacity=4)),
    )
