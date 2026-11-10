"""Integration tests for the worker/database health API against a real Postgres.

Covers:
- ``ping_database_async`` round-trip latency through the live pool.
- Worker-state reads (latest-per-worker, single, history) over real rows.
- ``ping_workers_async`` end-to-end NOTIFY round-trip with a live responder
  that speaks the real ``WorkerPongPayload`` contract.
"""

from __future__ import annotations

import asyncio
import json
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
from horsies.core.models.boot_failure import ResourceShortage, ShortageErrno
from horsies.core.models.health import (
    WORKER_PING_CHANNEL,
    AllSlotsServing,
    ClassBudget,
    ClassBudgets,
    ClassCapacityNotRecorded,
    DownSlot,
    SlotHealthNotRecorded,
    SlotsDown,
    WorkerPingRequest,
    WorkerPongPayload,
    slots_down_json,
)
from horsies.core.models.tasks import Execution
from horsies.core.types.result import is_ok
from horsies.core.utils.url import to_psycopg_url
from tests.broker_resources_support import current_resources


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
    assert len(seeded) == 1, 'expected exactly one row per worker'
    assert seeded[0].tasks_running == 3  # newest snapshot
    assert seeded[0].queues == ['default', 'priority']


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_worker_states_covers_all_workers_including_stale(
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """Every distinct worker appears exactly once with its newest snapshot.

    Regression for the recursive skip-scan form of ``LIST_WORKER_STATES_SQL``:
    the walk must visit all worker_ids (not stop after the first), and a
    worker whose snapshots are all old must still be listed.
    """
    prefix = f'itest-{uuid.uuid4().hex}'
    now = datetime.now(timezone.utc)
    base = {
        'hostname': 'itest-host',
        'pid': 4242,
        'processes': 4,
        'max_claim_batch': 8,
        'max_claim_per_worker': 4,
        'queues': ['default'],
        'tasks_claimed': 0,
        'worker_started_at': now - timedelta(days=3),
    }
    # (worker suffix, [(snapshot age, tasks_running), ...]) — newest last.
    seeds: dict[str, list[tuple[timedelta, int]]] = {
        f'{prefix}-fresh': [
            (timedelta(seconds=30), 1),
            (timedelta(seconds=10), 5),
        ],
        f'{prefix}-stale': [
            (timedelta(days=2), 4),
            (timedelta(days=1), 7),
        ],
        f'{prefix}-single': [(timedelta(seconds=20), 2)],
    }
    for worker_id, snapshots in seeds.items():
        for age, running in snapshots:
            await session.execute(
                _INSERT_WORKER_STATE,
                {
                    **base,
                    'worker_id': worker_id,
                    'snapshot_at': now - age,
                    'tasks_running': running,
                },
            )
    await session.commit()

    try:
        result = await broker.list_worker_states_async()

        assert is_ok(result)
        seeded = {
            s.worker_id: s for s in result.ok_value if s.worker_id in seeds
        }
        assert set(seeded) == set(seeds), 'every seeded worker must be listed'
        assert seeded[f'{prefix}-fresh'].tasks_running == 5
        assert seeded[f'{prefix}-stale'].tasks_running == 7
        assert seeded[f'{prefix}-single'].tasks_running == 2
    finally:
        await session.execute(
            text('DELETE FROM horsies_worker_states WHERE worker_id LIKE :p'),
            {'p': f'{prefix}%'},
        )
        await session.commit()


_INSERT_WORKER_STATE_WITH_SLOTS = text("""
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid, processes,
        max_claim_batch, max_claim_per_worker, queues,
        tasks_running, tasks_claimed, worker_started_at, slots_down
    ) VALUES (
        :worker_id, NOW(), 'itest-host', 4242, 2,
        2, 2, ARRAY['default'],
        0, 0, NOW(), CAST(:slots_down AS JSONB)
    )
""")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_state_reads_the_slot_health(
    broker: PostgresBroker,
    session: AsyncSession,
    worker_state_seed: str,
) -> None:
    """A row with down slots reads as ``SlotsDown``, an empty list as
    ``AllSlotsServing``, and a row with no value as ``SlotHealthNotRecorded``."""
    prefix = f'itest-{uuid.uuid4().hex}'
    down = DownSlot(
        slot=1,
        cause=ResourceShortage(ShortageErrno.EMFILE),
        detail='OSError: [Errno 24] Too many open files',
        down_since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        failed_boots=2,
    )
    for suffix, slots_down in (('degraded', [down]), ('serving', [])):
        await session.execute(
            _INSERT_WORKER_STATE_WITH_SLOTS,
            {
                'worker_id': f'{prefix}-{suffix}',
                'slots_down': json.dumps(slots_down_json(slots_down)),
            },
        )
    await session.commit()
    try:
        degraded = await broker.get_worker_state_async(f'{prefix}-degraded')
        serving = await broker.get_worker_state_async(f'{prefix}-serving')
        not_recorded = await broker.get_worker_state_async(worker_state_seed)

        assert is_ok(degraded) and degraded.ok_value is not None
        assert degraded.ok_value.slot_health == SlotsDown((down,))
        assert is_ok(serving) and serving.ok_value is not None
        assert serving.ok_value.slot_health == AllSlotsServing()
        assert is_ok(not_recorded) and not_recorded.ok_value is not None
        assert not_recorded.ok_value.slot_health == SlotHealthNotRecorded()
    finally:
        await session.execute(
            text('DELETE FROM horsies_worker_states WHERE worker_id LIKE :p'),
            {'p': f'{prefix}%'},
        )
        await session.commit()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_an_unreadable_slot_health_fails_the_read(
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """A stored value of another shape is a monitoring query failure, with
    the worker named, not a quiet default."""
    worker_id = f'itest-{uuid.uuid4().hex}'
    await session.execute(
        _INSERT_WORKER_STATE_WITH_SLOTS,
        {'worker_id': worker_id, 'slots_down': json.dumps({'slot': 1})},
    )
    await session.commit()
    try:
        result = await broker.get_worker_state_async(worker_id)

        assert not is_ok(result)
        assert worker_id in result.err_value.message
        assert 'slots_down is unreadable' in result.err_value.message
    finally:
        await session.execute(
            text('DELETE FROM horsies_worker_states WHERE worker_id = :wid'),
            {'wid': worker_id},
        )
        await session.commit()


_INSERT_WORKER_STATE_WITH_CAPACITY = text("""
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid, processes,
        max_claim_batch, max_claim_per_worker, queues,
        tasks_running, tasks_claimed, worker_started_at, class_capacity
    ) VALUES (
        :worker_id, NOW(), 'itest-host', 4242, 2,
        2, 2, ARRAY['default'],
        0, 0, NOW(), CAST(:class_capacity AS JSONB)
    )
""")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_state_reads_the_class_capacity(
    broker: PostgresBroker,
    session: AsyncSession,
    worker_state_seed: str,
) -> None:
    """A row with budgets reads as ``ClassBudgets`` in every read, and a row
    with no value as ``ClassCapacityNotRecorded``."""
    worker_id = f'itest-{uuid.uuid4().hex}'
    stored = {
        'SHARED_ASYNC': {'capacity': 40, 'serves': ['SHARED_ASYNC']},
        'ISOLATED': {'capacity': 2, 'serves': ['ISOLATED', 'SHARED_THREAD']},
    }
    await session.execute(
        _INSERT_WORKER_STATE_WITH_CAPACITY,
        {'worker_id': worker_id, 'class_capacity': json.dumps(stored)},
    )
    await session.commit()
    expected = ClassBudgets(
        (
            ClassBudget(
                Execution.ISOLATED,
                2,
                (Execution.ISOLATED, Execution.SHARED_THREAD),
            ),
            ClassBudget(Execution.SHARED_ASYNC, 40, (Execution.SHARED_ASYNC,)),
        ),
    )
    try:
        latest = await broker.get_worker_state_async(worker_id)
        history = await broker.get_worker_state_history_async(worker_id)
        listed = await broker.list_worker_states_async()
        not_recorded = await broker.get_worker_state_async(worker_state_seed)

        assert is_ok(latest) and latest.ok_value is not None
        assert latest.ok_value.class_capacity == expected
        assert is_ok(history)
        assert [snap.class_capacity for snap in history.ok_value] == [expected]
        assert is_ok(listed)
        assert [
            snap.class_capacity
            for snap in listed.ok_value
            if snap.worker_id == worker_id
        ] == [expected]
        assert is_ok(not_recorded) and not_recorded.ok_value is not None
        assert not_recorded.ok_value.class_capacity == ClassCapacityNotRecorded()
    finally:
        await session.execute(
            text('DELETE FROM horsies_worker_states WHERE worker_id = :wid'),
            {'wid': worker_id},
        )
        await session.commit()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_an_unreadable_class_capacity_fails_the_read(
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """An unknown budget name is a monitoring query failure, with the worker
    named, not a quiet default."""
    worker_id = f'itest-{uuid.uuid4().hex}'
    await session.execute(
        _INSERT_WORKER_STATE_WITH_CAPACITY,
        {
            'worker_id': worker_id,
            'class_capacity': json.dumps(
                {'EXCLUSIVE': {'capacity': 1, 'serves': ['EXCLUSIVE']}},
            ),
        },
    )
    await session.commit()
    try:
        result = await broker.get_worker_state_async(worker_id)

        assert not is_ok(result)
        assert worker_id in result.err_value.message
        assert 'class_capacity is unreadable' in result.err_value.message
        assert "'EXCLUSIVE'" in result.err_value.message
    finally:
        await session.execute(
            text('DELETE FROM horsies_worker_states WHERE worker_id = :wid'),
            {'wid': worker_id},
        )
        await session.commit()


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
        async with current_resources(broker).session_factory() as s:
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
