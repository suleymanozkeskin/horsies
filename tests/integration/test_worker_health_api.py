"""Integration tests for the worker/database health API against a real Postgres.

Covers:
- ``ping_database_async`` round-trip latency through the live pool.
- Worker-state reads (latest-per-worker, single, history) over real rows.
- ``ping_workers_async`` end-to-end NOTIFY round-trip with a live responder
  that speaks the real ``WorkerPongPayload`` contract.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.models.health import (
    WORKER_PING_CHANNEL,
    WorkerPingRequest,
    WorkerPongPayload,
)
from horsies.core.types.result import is_ok
from horsies.core.utils.url import to_psycopg_url


_INSERT_WORKER_STATE = text("""
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid, processes,
        max_claim_batch, max_claim_per_worker, queues,
        tasks_running, tasks_claimed, worker_started_at
    ) VALUES (
        :worker_id, :snapshot_at, :hostname, :pid, :processes,
        :max_claim_batch, :max_claim_per_worker, :queues,
        :tasks_running, :tasks_claimed, :worker_started_at
    )
""")


@pytest_asyncio.fixture
async def worker_state_seed(
    session: AsyncSession,
) -> AsyncGenerator[str, None]:
    """Insert a few snapshots for one worker; clean them up afterwards."""
    worker_id = f'itest-{uuid.uuid4().hex}'
    started = datetime.now(timezone.utc) - timedelta(minutes=5)
    base = {
        'worker_id': worker_id,
        'hostname': 'itest-host',
        'pid': 4242,
        'processes': 4,
        'max_claim_batch': 8,
        'max_claim_per_worker': 4,
        'queues': ['default', 'priority'],
        'worker_started_at': started,
    }
    # Three snapshots with increasing tasks_running and snapshot_at.
    for i, running in enumerate((0, 1, 3)):
        await session.execute(
            _INSERT_WORKER_STATE,
            {
                **base,
                'snapshot_at': started + timedelta(seconds=10 * (i + 1)),
                'tasks_running': running,
                'tasks_claimed': 0,
            },
        )
    await session.commit()
    yield worker_id
    await session.execute(
        text('DELETE FROM horsies_worker_states WHERE worker_id = :wid'),
        {'wid': worker_id},
    )
    await session.commit()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ping_database_returns_latency(broker: PostgresBroker) -> None:
    """ping_database_async succeeds and reports non-negative latency."""
    result = await broker.ping_database_async()

    assert is_ok(result)
    assert result.ok_value.latency_ms >= 0.0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_worker_states_returns_latest_per_worker(
    broker: PostgresBroker,
    worker_state_seed: str,
) -> None:
    """The seeded worker appears once, carrying its newest snapshot."""
    result = await broker.list_worker_states_async()

    assert is_ok(result)
    seeded = [s for s in result.ok_value if s.worker_id == worker_state_seed]
    assert len(seeded) == 1, 'DISTINCT ON should collapse to one row per worker'
    assert seeded[0].tasks_running == 3  # newest snapshot
    assert seeded[0].queues == ['default', 'priority']


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_worker_state_single_and_missing(
    broker: PostgresBroker,
    worker_state_seed: str,
) -> None:
    """Latest snapshot for a known worker; None for an unknown one."""
    found = await broker.get_worker_state_async(worker_state_seed)
    assert is_ok(found)
    assert found.ok_value is not None
    assert found.ok_value.tasks_running == 3

    missing = await broker.get_worker_state_async('no-such-worker')
    assert is_ok(missing)
    assert missing.ok_value is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_worker_state_history_newest_first(
    broker: PostgresBroker,
    worker_state_seed: str,
) -> None:
    """History returns all snapshots ordered newest-first; limit bounds it."""
    full = await broker.get_worker_state_history_async(worker_state_seed)
    assert is_ok(full)
    snaps = full.ok_value
    assert len(snaps) == 3
    running = [s.tasks_running for s in snaps]
    assert running == [3, 1, 0], 'expected newest-first ordering'

    limited = await broker.get_worker_state_history_async(worker_state_seed, limit=1)
    assert is_ok(limited)
    assert len(limited.ok_value) == 1
    assert limited.ok_value[0].tasks_running == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ping_workers_roundtrip(broker: PostgresBroker) -> None:
    """A live responder on the real NOTIFY path is collected as a pong."""
    psycopg_url = to_psycopg_url(broker.config.effective_session_database_url)
    responder = PostgresListener(psycopg_url)
    fake_worker_id = f'itest-{uuid.uuid4().hex}'

    start_r = await responder.start()
    assert is_ok(start_r)
    listen_r = await responder.listen(WORKER_PING_CHANNEL)
    assert is_ok(listen_r)
    ping_queue = listen_r.ok_value

    async def respond_once() -> None:
        notify = await ping_queue.get()
        req = WorkerPingRequest.model_validate(loads_json(notify.payload).ok_value)
        pong = WorkerPongPayload(
            correlation_id=req.correlation_id,
            worker_id=fake_worker_id,
            hostname='itest-host',
            pid=4242,
        )
        async with broker.session_factory() as s:
            await s.execute(
                text('SELECT pg_notify(:ch, :p)'),
                {'ch': req.reply_channel, 'p': dumps_json(pong.model_dump()).ok_value},
            )
            await s.commit()

    responder_task = asyncio.create_task(respond_once())
    try:
        result = await broker.ping_workers_async(timeout_seconds=5.0)
    finally:
        await asyncio.wait_for(responder_task, timeout=5.0)
        await responder.close()

    assert is_ok(result)
    pong_ids = [p.worker_id for p in result.ok_value]
    assert fake_worker_id in pong_ids
    pong = next(p for p in result.ok_value if p.worker_id == fake_worker_id)
    assert pong.round_trip_ms >= 0.0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ping_workers_targeted_to_absent_worker_times_out(
    broker: PostgresBroker,
) -> None:
    """Targeting a worker that never replies returns an empty pong list."""
    result = await broker.ping_workers_async(
        target_worker_id='definitely-not-running',
        timeout_seconds=1.0,
    )

    assert is_ok(result)
    assert result.ok_value == []
