"""Every horsies database connection carries libpq tcp_user_timeout.

Keepalives do not probe while sent data is unacknowledged, so a write to a
dead connection is bounded only by tcp_user_timeout. The value is read back
from each open connection's libpq parameters.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, cast

import psycopg
import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import is_err
from horsies.core.utils.url import to_psycopg_url
from horsies.core.worker.child_pool import (
    _cleanup_worker_pool,  # pyright: ignore[reportPrivateUsage]
    _get_worker_pool,  # pyright: ignore[reportPrivateUsage]
    _initialize_worker_pool,  # pyright: ignore[reportPrivateUsage]
)
from horsies.web.events import EventBroadcaster

pytestmark = [pytest.mark.integration]

TIMEOUT_MS = 12_345


def _split_config(db_url: str) -> PostgresConfig:
    """A config whose session URL differs, so the broker builds both
    engines."""
    separator = '&' if '?' in db_url else '?'
    return PostgresConfig(
        database_url=SecretStr(db_url),
        session_database_url=SecretStr(
            f'{db_url}{separator}application_name=horsies_session'
        ),
        tcp_user_timeout_ms=TIMEOUT_MS,
    )


def _timeout_of(
    connection: psycopg.Connection[Any] | psycopg.AsyncConnection[Any],
) -> str:
    return connection.info.get_parameters()['tcp_user_timeout']


async def _engine_timeout(engine: AsyncEngine) -> str:
    async with engine.connect() as connection:
        raw = await connection.get_raw_connection()
        driver = cast('psycopg.AsyncConnection[Any]', raw.driver_connection)
        assert isinstance(driver, psycopg.AsyncConnection)
        return _timeout_of(driver)


@pytest_asyncio.fixture()
async def broker(db_url: str) -> AsyncGenerator[PostgresBroker, None]:
    broker = PostgresBroker(_split_config(db_url))
    try:
        yield broker
    finally:
        await broker.close_async()


@pytest.mark.asyncio
async def test_both_broker_engines_carry_the_timeout(
    broker: PostgresBroker,
) -> None:
    session_engine = broker._session_engine  # pyright: ignore[reportPrivateUsage]
    assert session_engine is not broker.async_engine
    assert await _engine_timeout(broker.async_engine) == str(TIMEOUT_MS)
    assert await _engine_timeout(session_engine) == str(TIMEOUT_MS)


@pytest.mark.asyncio
async def test_both_listener_connections_carry_the_timeout(
    broker: PostgresBroker,
) -> None:
    listener = broker.listener
    started = await listener.start()
    assert not is_err(started), started
    try:
        for connection in (
            listener._dispatcher_conn,  # pyright: ignore[reportPrivateUsage]
            listener._command_conn,  # pyright: ignore[reportPrivateUsage]
        ):
            assert connection is not None
            assert _timeout_of(connection) == str(TIMEOUT_MS)
    finally:
        await listener.close()


@pytest.mark.asyncio
async def test_the_monitoring_listener_carries_the_timeout(db_url: str) -> None:
    broadcaster = EventBroadcaster(
        to_psycopg_url(db_url), tcp_user_timeout_ms=TIMEOUT_MS,
    )
    listener = broadcaster._build_listener()  # pyright: ignore[reportPrivateUsage]
    assert isinstance(listener, PostgresListener)
    started = await listener.start()
    assert not is_err(started), started
    try:
        connection = listener._command_conn  # pyright: ignore[reportPrivateUsage]
        assert connection is not None
        assert _timeout_of(connection) == str(TIMEOUT_MS)
    finally:
        await listener.close()


def test_child_pool_connections_carry_the_timeout(db_url: str) -> None:
    config = _split_config(db_url)
    _cleanup_worker_pool()
    _initialize_worker_pool(
        to_psycopg_url(db_url),
        connect_kwargs=config.pooled_connect_args,
    )
    try:
        with _get_worker_pool().connection() as connection:
            assert _timeout_of(connection) == str(TIMEOUT_MS)
    finally:
        _cleanup_worker_pool()
