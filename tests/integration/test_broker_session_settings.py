# pyright: reportPrivateUsage=false
"""Broker connections run without JIT where the session keeps the setting.

A direct or session-pooled connection runs ``SET jit = off`` once, in
autocommit, so the setting survives a rollback. In PgBouncer transaction
mode the runtime pool keeps the server's JIT setting, because a session
SET would leak to other clients; the session pool still disables JIT.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from tests.integration.conftest import DB_URL
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


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


