'''PgBouncer pool-mode contract tests.'''

from __future__ import annotations

import asyncio
from collections.abc import Generator
from datetime import datetime, timezone
from uuid import uuid4

import psycopg
import pytest

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import is_err, is_ok
from horsies.core.utils.fingerprint import enqueue_fingerprint
from horsies.core.utils.url import to_psycopg_url
from tests.pgbouncer_utils import (
    PgbouncerUrls,
    isolated_pgbouncer_database,
    skip_if_pgbouncer_disabled,
)


skip_if_pgbouncer_disabled()

pytestmark = [pytest.mark.integration, pytest.mark.pgbouncer]


@pytest.fixture
def pgbouncer_urls() -> Generator[PgbouncerUrls, None, None]:
    with isolated_pgbouncer_database("horsies_pgbouncer_contract") as urls:
        yield urls


async def _notify(database_url: str, channel: str, payload: str) -> None:
    conn = await psycopg.AsyncConnection.connect(
        to_psycopg_url(database_url),
        autocommit=True,
    )
    try:
        await conn.execute(
            "SELECT pg_notify(%s, %s)",
            (channel, payload),
        )
    finally:
        await conn.close()


async def _enqueue_probe(broker: PostgresBroker, task_name: str = "pgbouncer_probe"):
    sent_at = datetime.now(timezone.utc)
    task_id = str(uuid4())
    enqueue_sha = enqueue_fingerprint(
        task_name=task_name,
        queue_name="default",
        priority=100,
        args_json="[]",
        kwargs_json="{}",
        sent_at=sent_at,
        good_until=None,
        enqueue_delay_seconds=None,
        task_options=None,
    )
    return await broker.enqueue_async(
        task_name,
        task_id=task_id,
        enqueue_sha=enqueue_sha,
        args_json="[]",
        kwargs_json="{}",
        sent_at=sent_at,
    )


@pytest.mark.asyncio(loop_scope="function")
async def test_transaction_pool_uses_pooled_runtime_and_direct_session(
    pgbouncer_urls: PgbouncerUrls,
) -> None:
    config = PostgresConfig(
        database_url=pgbouncer_urls.transaction,
        session_database_url=pgbouncer_urls.direct,
        pgbouncer_transaction_mode=True,
        pool_size=2,
        max_overflow=0,
    )
    app = Horsies(AppConfig(broker=config))
    broker = PostgresBroker(config)
    try:
        schema_r = await broker.ensure_schema_initialized()
        assert is_ok(schema_r)

        errors = await asyncio.to_thread(app.check, live=True)
        assert errors == []

        enqueue_r = await _enqueue_probe(broker)
        assert is_ok(enqueue_r)
    finally:
        await broker.close_async()


def test_transaction_pool_rejected_as_session_url(
    pgbouncer_urls: PgbouncerUrls,
) -> None:
    app = Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=pgbouncer_urls.transaction,
                session_database_url=pgbouncer_urls.transaction,
                pgbouncer_transaction_mode=True,
            )
        )
    )

    errors = app.check(live=True)

    assert len(errors) == 1
    assert errors[0].notes is not None
    assert "session_database_url appears to be transaction-pooled" in errors[0].notes[0]


@pytest.mark.asyncio(loop_scope="function")
async def test_session_pool_works_without_transaction_mode(
    pgbouncer_urls: PgbouncerUrls,
) -> None:
    config = PostgresConfig(
        database_url=pgbouncer_urls.session,
        pool_size=2,
        max_overflow=0,
    )
    broker = PostgresBroker(config)
    try:
        schema_r = await broker.ensure_schema_initialized()
        assert is_ok(schema_r)

        enqueue_r = await _enqueue_probe(broker, task_name="pgbouncer_session_probe")
        assert is_ok(enqueue_r)

        channel = f"pgbouncer_session_probe_{uuid4().hex}"
        listen_r = await asyncio.wait_for(
            broker.listener.listen(channel),
            timeout=5.0,
        )
        assert is_ok(listen_r)
        queue = listen_r.ok_value

        await _notify(pgbouncer_urls.direct, channel, "ok")
        notify = await asyncio.wait_for(queue.get(), timeout=2.0)
        assert notify.channel == channel
        assert notify.payload == "ok"
    finally:
        await broker.close_async()


@pytest.mark.asyncio(loop_scope="function")
async def test_statement_pool_fails_loudly_for_runtime_transactions(
    pgbouncer_urls: PgbouncerUrls,
) -> None:
    config = PostgresConfig(
        database_url=pgbouncer_urls.statement,
        session_database_url=pgbouncer_urls.direct,
        pgbouncer_transaction_mode=True,
        pool_size=2,
        max_overflow=0,
    )
    broker = PostgresBroker(config)
    try:
        schema_r = await broker.ensure_schema_initialized()
        assert is_ok(schema_r)

        enqueue_r = await _enqueue_probe(broker, task_name="pgbouncer_statement_probe")
        assert is_err(enqueue_r)
        assert enqueue_r.err_value.code.value == "ENQUEUE_FAILED"
        assert enqueue_r.err_value.retryable is True
    finally:
        await broker.close_async()
