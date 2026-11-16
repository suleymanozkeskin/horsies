"""Python clients of a dual-run database, for the candidate side of an operation dual-run."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from tests.dualrun.databases import DualRunDatabase


def sqlalchemy_url(database: DualRunDatabase) -> str:
    """The database URL with the psycopg driver of SQLAlchemy."""
    return database.url.replace('postgresql://', 'postgresql+psycopg://', 1)


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
