"""A broker that waits for the schema lock does not block an index build.

The lock holder installs recovery indexes with ``CREATE INDEX CONCURRENTLY``
on an engine connection, not on the connection that holds the lock. That
build waits for every snapshot older than its own. A waiter must not hold a
snapshot while it waits for the schema lock: the cycle runs through the
holder's client process, so PostgreSQL does not detect it and both wait.
"""

from __future__ import annotations

import asyncio

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

WAITER_VISIBLE_TIMEOUT_SECONDS = 5.0
WAITER_VISIBLE_POLL_SECONDS = 0.01
WAITER_RELEASE_TIMEOUT_SECONDS = 5.0
INDEX_BUILD_STATEMENT_TIMEOUT = '2s'
PROBE_INDEX_NAME = 'itest_schema_lock_wait_probe'


async def _wait_until_lock_waiter_is_visible(
    observer: psycopg.AsyncConnection[tuple[object, ...]],
    holder_pid: int,
) -> None:
    """Return once another backend is running an advisory-lock statement."""

    async def _poll() -> None:
        while True:
            cursor = await observer.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid NOT IN (pg_backend_pid(), %s)
                      AND query LIKE '%%advisory_lock(%%'
                )
                """,
                (holder_pid,),
            )
            row = await cursor.fetchone()
            assert row is not None
            if bool(row[0]):
                return
            await asyncio.sleep(WAITER_VISIBLE_POLL_SECONDS)

    await asyncio.wait_for(_poll(), WAITER_VISIBLE_TIMEOUT_SECONDS)


@pytest.mark.asyncio(loop_scope='function')
async def test_waiting_migrator_does_not_block_a_concurrent_index_build(
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    waiter_broker = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
    first_key = (
        *waiter_broker._legacy_schema_advisory_keys(),
        waiter_broker._schema_advisory_key(),
    )[0]
    psycopg_url = to_psycopg_url(DB_URL)
    holder = await psycopg.AsyncConnection.connect(psycopg_url, autocommit=True)
    builder = await psycopg.AsyncConnection.connect(psycopg_url, autocommit=True)
    observer = await psycopg.AsyncConnection.connect(psycopg_url, autocommit=True)

    async def _hold_schema_lock_briefly() -> None:
        async with waiter_broker._schema_session_lock():
            pass

    waiter: asyncio.Task[None] | None = None
    try:
        await holder.execute('SELECT pg_advisory_lock(%s)', (first_key,))
        holder_pid = holder.info.backend_pid
        waiter = asyncio.create_task(_hold_schema_lock_briefly())
        await _wait_until_lock_waiter_is_visible(observer, holder_pid)

        await builder.execute(
            f"SET statement_timeout = '{INDEX_BUILD_STATEMENT_TIMEOUT}'",
        )
        await builder.execute(
            f'CREATE INDEX CONCURRENTLY {PROBE_INDEX_NAME} '
            'ON horsies_workflows (name)',
        )
        await holder.execute('SELECT pg_advisory_unlock(%s)', (first_key,))
        await asyncio.wait_for(waiter, WAITER_RELEASE_TIMEOUT_SECONDS)
    finally:
        if waiter is not None and not waiter.done():
            waiter.cancel()
        await holder.close()
        await builder.close()
        await observer.execute(f'DROP INDEX IF EXISTS {PROBE_INDEX_NAME}')
        await observer.close()
        _ = await waiter_broker.close_async()
