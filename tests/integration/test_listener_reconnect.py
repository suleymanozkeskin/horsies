"""Integration test for PostgresListener dispatcher reconnect after connection kill."""

from __future__ import annotations

import asyncio
import os

import psycopg
import pytest
from psycopg import Notify

from horsies.core.brokers.listener import PostgresListener

# Raw psycopg URL (no +psycopg scheme) — matches what PostgresBroker passes to PostgresListener
DB_URL = f'postgresql://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies'

CHANNEL = "test_reconnect"
RECV_TIMEOUT = 5.0


async def _drain_queue(queue: asyncio.Queue[Notify]) -> list[Notify]:
    """Drain all currently buffered notifications from a queue without blocking."""
    items: list[Notify] = []
    while True:
        try:
            items.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break
    return items


async def _wait_for_reconnect_delivery(
    helper_conn: psycopg.AsyncConnection[tuple[object, ...]],
    queue: asyncio.Queue[Notify],
    channel: str,
    timeout_s: float = 8.0,
    per_probe_wait_s: float = 0.4,
) -> None:
    """Wait until a post-kill probe notification is actually delivered."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    attempt = 0

    while asyncio.get_running_loop().time() < deadline:
        attempt += 1
        payload = f"reconnect_probe_{attempt}"
        await helper_conn.execute(
            "SELECT pg_notify(%s, %s)",
            (channel, payload),
        )

        probe_deadline = min(
            deadline,
            asyncio.get_running_loop().time() + per_probe_wait_s,
        )
        while asyncio.get_running_loop().time() < probe_deadline:
            remaining = probe_deadline - asyncio.get_running_loop().time()
            try:
                notification = await asyncio.wait_for(queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            if notification.payload == payload:
                return

    raise TimeoutError(
        f"Listener did not deliver post-kill notifications on channel '{channel}' "
        f"within {timeout_s:.1f}s"
    )


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope="function")
class TestDispatcherReconnect:
    """Verify the dispatcher reconnects and re-LISTENs after its backend is killed."""

    async def test_dispatcher_reconnects_after_connection_kill(self) -> None:
        """
        Kill the dispatcher's PG backend via pg_terminate_backend() and confirm
        that the listener recovers: re-establishes the connection, re-issues
        LISTEN, and delivers notifications sent after the kill.
        """
        listener = PostgresListener(DB_URL)
        helper_conn: psycopg.AsyncConnection[tuple[object, ...]] | None = None

        try:
            # -- Arrange --
            start_r = await listener.start()
            assert start_r.is_ok(), f'Listener start failed: {start_r.err_value}'
            listen_r = await listener.listen(CHANNEL)
            assert listen_r.is_ok(), f'Listen failed: {listen_r.err_value}'
            queue = listen_r.ok_value

            helper_conn = await psycopg.AsyncConnection.connect(
                DB_URL,
                autocommit=True,
            )

            # -- Pre-kill verify --
            await helper_conn.execute(f"NOTIFY {CHANNEL}, 'before'")
            notification = await asyncio.wait_for(queue.get(), timeout=RECV_TIMEOUT)
            assert notification.payload == "before"

            # -- Kill dispatcher connection --
            assert listener._dispatcher_conn is not None
            backend_pid: int = listener._dispatcher_conn.info.backend_pid
            await helper_conn.execute(
                "SELECT pg_terminate_backend(%s)",
                (backend_pid,),
            )

            # Drain any stale/error artefacts that arrived during the kill window.
            await _drain_queue(queue)

            # -- Post-reconnect verify (event-driven, no fixed sleep) --
            await _wait_for_reconnect_delivery(helper_conn, queue, CHANNEL)

        finally:
            await listener.close()
            if helper_conn is not None and not helper_conn.closed:
                await helper_conn.close()
