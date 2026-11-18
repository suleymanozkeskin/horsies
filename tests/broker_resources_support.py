"""Test-side SQL access to the database of a broker.

A broker runs every statement in the core and holds no SQLAlchemy engine. A
test that seeds rows or reads them directly calls ``current_resources(broker)``:
an engine and a session factory on the broker's ``database_url``, built once
per broker and kept while the broker lives.

- The engine has no pool (``NullPool``): a session or a connection opens its
  own database connection and closes it at its end. No connection outlives
  the event loop that opened it, so the engine serves every event loop of the
  test process.
- The engine reads uuid columns as text (``read_uuid_columns_as_text``).
- The connect arguments are the broker config's ``pooled_connect_args``: the
  TCP settings, and no prepared statements in PgBouncer transaction mode.
"""

from __future__ import annotations

import weakref
from dataclasses import dataclass
from typing import Any

from psycopg.types.string import TextLoader
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.utils.url import sqlalchemy_url


@dataclass(frozen=True, slots=True)
class DatabaseAccess:
    """An engine and a session factory on the database of one broker."""

    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]


_ACCESS: weakref.WeakKeyDictionary[PostgresBroker, DatabaseAccess] = (
    weakref.WeakKeyDictionary()
)
"""The access of each live broker; an entry goes with its broker."""


def _load_uuid_as_text(dbapi_connection: Any, _connection_record: Any) -> None:
    """The ``connect`` listener: uuid columns of the new connection read as text."""
    dbapi_connection.driver_connection.adapters.register_loader('uuid', TextLoader)


def read_uuid_columns_as_text(engine: AsyncEngine) -> None:
    """Make every connection of ``engine`` read uuid columns as text.

    The tests compare identity columns with the text of the id. One loader on
    the driver gives every raw ``text()`` read that presentation. No I/O at
    registration.
    """
    event.listen(engine.sync_engine, 'connect', _load_uuid_as_text)


def _built_access(broker: PostgresBroker) -> DatabaseAccess:
    """A new engine and session factory for ``broker``. No I/O."""
    engine = create_async_engine(
        sqlalchemy_url(broker.config.database_url.get_secret_value()),
        poolclass=NullPool,
        connect_args=broker.config.pooled_connect_args,
    )
    read_uuid_columns_as_text(engine)
    return DatabaseAccess(
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )


def current_resources(broker: PostgresBroker) -> DatabaseAccess:
    """The test-side access to the database of ``broker``, built on the first
    call for that broker. No I/O: a session connects when it is used."""
    match _ACCESS.get(broker):
        case DatabaseAccess() as access:
            return access
        case None:
            access = _built_access(broker)
            _ACCESS[broker] = access
            return access
