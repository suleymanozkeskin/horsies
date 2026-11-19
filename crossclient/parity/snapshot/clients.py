"""Python clients of a dual-run database, for the candidate side of an operation dual-run."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from crossclient.parity.snapshot.databases import DualRunDatabase


@asynccontextmanager
async def open_broker(database: DualRunDatabase) -> AsyncGenerator[PostgresBroker, None]:
    """A broker on ``database``, whose baseline is installed; closed at exit.

    Raises ``AssertionError`` when the broker refuses the schema.
    """
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        match await broker.ensure_schema_initialized():
            case Err(refused):
                raise AssertionError(f'the broker refused the dual-run database: {refused}')
            case Ok():
                pass
        yield broker
    finally:
        _ = await broker.close_async()


def install_as_a_client(database: DualRunDatabase) -> None:
    """The schema install that a client's broker runs before its first operation.

    The install ends with the coverage pass of the default retention config
    (``PostgresBroker.ensure_schema_initialized``). A side that runs raw
    statements runs it first, when the other side runs its operation through
    a broker of ``open_broker``: both sides then start their operation from
    the state that the install leaves, and the pair compares the operation.

    Raises ``AssertionError`` when the broker refuses the schema.
    """

    async def install() -> None:
        async with open_broker(database):
            pass

    asyncio.run(install())
