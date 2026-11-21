'''PgBouncer pool-mode contract tests.'''

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.errors import ConfigurationError
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import is_err, is_ok
from tests.pgbouncer_utils import (
    PgbouncerUrls,
    isolated_pgbouncer_database,
    skip_if_pgbouncer_disabled,
)


skip_if_pgbouncer_disabled()

pytestmark = [pytest.mark.integration, pytest.mark.pgbouncer]


@pytest.fixture
def pgbouncer_urls() -> Generator[PgbouncerUrls, None, None]:
    with isolated_pgbouncer_database("pgbouncer_contract") as urls:
        yield urls


async def _enqueue_probe(broker: PostgresBroker, task_name: str = "pgbouncer_probe"):
    sent_at = datetime.now(timezone.utc)
    task_id = uuid4()
    return await broker.enqueue_async(
        task_name,
        task_id=task_id,
        send_kind=SendKind.FIRST,
        input_json="{}",
        sent_at=sent_at,
    )


def test_transaction_pool_rejected_as_session_url(
    pgbouncer_urls: PgbouncerUrls,
) -> None:
    with pytest.raises(
        ConfigurationError,
        match='session_database_url must not use the transaction-pool URL',
    ):
        PostgresConfig(
            database_url=pgbouncer_urls.transaction,
            session_database_url=pgbouncer_urls.transaction,
            pgbouncer_transaction_mode=True,
        )


@pytest.mark.asyncio(loop_scope="function")
async def test_statement_pool_fails_loudly_for_runtime_transactions(
    pgbouncer_urls: PgbouncerUrls,
) -> None:
    """The first use of a statement-pooled main URL is refused: the pool
    cannot keep a transaction, so a send again fails the same way and is not
    retryable. The message names the cause."""
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
        assert enqueue_r.err_value.retryable is False
        assert "PgBouncer statement pooling" in enqueue_r.err_value.message
    finally:
        await broker.close_async()
