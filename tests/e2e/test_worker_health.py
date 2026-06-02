"""Layer 2 e2e: worker health API against a real worker process.

Unlike the integration test (which uses a fake in-test responder), these spin
a REAL `horsies worker` subprocess, so they exercise the worker's actual ping
subscription, `_ping_responder_loop`, `_handle_ping`, and worker-state writes.
A worker that fails to subscribe to the ping channel at startup — or leaks/
crashes during start — would not reply here.
"""

from __future__ import annotations

import asyncio

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_ok

from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks

DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'


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
        return handle.get(timeout_ms=2000).is_ok()

    return _check


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_ping_database_real(broker: PostgresBroker) -> None:
    """ping_database_async succeeds against the real e2e database."""
    result = await broker.ping_database_async()
    assert is_ok(result)
    assert result.ok_value.latency_ms >= 0.0


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_ping_workers_reaches_real_worker(broker: PostgresBroker) -> None:
    """A live worker replies to a broadcast ping (real responder round-trip)."""
    ready = _make_ready_check()
    with run_worker(DEFAULT_INSTANCE, processes=1, ready_check=ready):
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
@pytest.mark.asyncio
async def test_ping_and_worker_state_correlate(broker: PostgresBroker) -> None:
    """The responding worker also appears in list_worker_states (same worker_id)."""
    ready = _make_ready_check()
    with run_worker(DEFAULT_INSTANCE, processes=1, ready_check=ready):
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
