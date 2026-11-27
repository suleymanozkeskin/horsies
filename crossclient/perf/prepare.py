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
from horsies.core.brokers.result_types import DatabaseOperationFailed
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Result
from pydantic import SecretStr


def apply_schema(dsn: str) -> Result[None, DatabaseOperationFailed]:
    """Install the schema at `dsn`, or check it, then close the broker. Writes.

    Returns the result of ``PostgresBroker.ensure_schema_initialized``: ``Err``
    names why the install failed or was refused, and the caller reports it
    instead of measuring a database without the schema.
    """
    return asyncio.run(_apply(dsn))


async def _apply(dsn: str) -> Result[None, DatabaseOperationFailed]:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(dsn)))
    try:
        return await broker.ensure_schema_initialized()
    finally:
        # Explicit discard: the install's own result is what the caller
        # reports; a failed close of the broker afterwards changes no schema.
        _ = await broker.close_async()
