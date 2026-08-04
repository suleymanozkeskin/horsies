"""Bringing a measurement database up to the schema under test.

The harness measures statements against the real schema, so the schema has to
be installed before anything is timed. It lives here rather than in a shell
snippet inside a workflow file: a migration invocation embedded in YAML is
neither type-checked nor runnable locally, and this one has to work on both
supported server versions.
"""

from __future__ import annotations

import asyncio

from horsies.core.brokers.postgres import PostgresBroker
from pydantic import SecretStr

from horsies.core.models.broker import PostgresConfig


def apply_schema(dsn: str) -> None:
    """Install or upgrade the schema at `dsn`, then release the connection."""
    asyncio.run(_apply(dsn))


async def _apply(dsn: str) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(dsn)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()
