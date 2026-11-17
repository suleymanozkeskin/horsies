"""Behavioral probe for PostgreSQL session affinity."""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import psycopg
from psycopg import sql


SESSION_CAPABILITY_TIMEOUT_SECONDS = 2.0


async def probe_session_capability(
    database_url: str,
    *,
    timeout_seconds: float = SESSION_CAPABILITY_TIMEOUT_SECONDS,
) -> None:
    """Prove that LISTEN state remains on one checked-out session.

    A URL or port number cannot prove pool mode.  Transaction-pooled PgBouncer
    can accept the LISTEN statement but cannot deliver notifications to an idle
    client session.  The bounded delivery probe tests the required behavior.
    """
    channel = f'horsies_check_{uuid.uuid4().hex}'
    payload = 'ok'
    listener = None
    notifier = None
    wait_task: asyncio.Task[None] | None = None

    async def wait_for_notification() -> None:
        if listener is None:
            raise AssertionError('listener connection was not created')
        async for notify in listener.notifies():
            if notify.channel == channel and notify.payload == payload:
                return

    try:
        listener = await psycopg.AsyncConnection.connect(
            database_url,
            autocommit=True,
        )
        await listener.execute(sql.SQL('LISTEN {}').format(sql.Identifier(channel)))
        wait_task = asyncio.create_task(wait_for_notification())

        notifier = await psycopg.AsyncConnection.connect(
            database_url,
            autocommit=True,
        )
        await notifier.execute(
            'SELECT pg_notify(%s, %s)',
            (channel, payload),
        )
        await asyncio.wait_for(wait_task, timeout=timeout_seconds)
    finally:
        if wait_task is not None and not wait_task.done():
            wait_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await wait_task
        if listener is not None:
            with contextlib.suppress(Exception):
                await listener.execute(
                    sql.SQL('UNLISTEN {}').format(sql.Identifier(channel))
                )
        if notifier is not None:
            with contextlib.suppress(Exception):
                await notifier.close()
        if listener is not None:
            with contextlib.suppress(Exception):
                await listener.close()
