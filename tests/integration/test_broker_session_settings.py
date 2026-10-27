# pyright: reportPrivateUsage=false
"""Broker connections run without JIT where the session keeps the setting.

A direct or session-pooled connection runs ``SET jit = off`` once, in
autocommit, so the setting survives a rollback. In PgBouncer transaction
mode the runtime pool keeps the server's JIT setting, because a session
SET would leak to other clients; the session pool still disables JIT.
"""

from __future__ import annotations

import psycopg
import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# A different URL string for the same database, so the broker builds a
# separate session engine.
SESSION_URL = f'{DB_URL}{"&" if "?" in DB_URL else "?"}application_name=horsies_session'


async def _jit_setting(engine: AsyncEngine) -> str:
    async with engine.connect() as connection:
        return str((await connection.execute(text('SHOW jit'))).scalar_one())


async def _server_jit_setting() -> str:
    async with await psycopg.AsyncConnection.connect(
        to_psycopg_url(DB_URL), autocommit=True,
    ) as connection:
        row = await (await connection.execute('SHOW jit')).fetchone()
        assert row is not None
        return str(row[0])


async def test_direct_connections_run_without_jit() -> None:
    broker = PostgresBroker(PostgresConfig(
        database_url=SecretStr(DB_URL),
        session_database_url=SecretStr(SESSION_URL),
    ))
    single = broker.create_single_connection_engine()
    try:
        assert current_resources(broker).session_async_engine() is not current_resources(broker).engine
        assert await _jit_setting(current_resources(broker).engine) == 'off'
        assert await _jit_setting(current_resources(broker).session_async_engine()) == 'off'
        assert await _jit_setting(single) == 'off'
    finally:
        await single.dispose()
        await broker.close_async()


async def test_setting_survives_a_rollback_of_the_first_transaction() -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
    try:
        async with current_resources(broker).engine.connect() as connection:
            await connection.execute(text('SELECT 1'))
            await connection.rollback()
            jit = (await connection.execute(text('SHOW jit'))).scalar_one()
        assert jit == 'off'
    finally:
        await broker.close_async()


async def test_transaction_mode_runtime_pool_keeps_the_server_setting() -> None:
    broker = PostgresBroker(PostgresConfig(
        database_url=SecretStr(DB_URL),
        session_database_url=SecretStr(SESSION_URL),
        pgbouncer_transaction_mode=True,
    ))
    single = broker.create_single_connection_engine()
    try:
        server_setting = await _server_jit_setting()
        assert await _jit_setting(current_resources(broker).engine) == server_setting
        assert await _jit_setting(single) == server_setting
        assert await _jit_setting(current_resources(broker).session_async_engine()) == 'off'
    finally:
        await single.dispose()
        await broker.close_async()
