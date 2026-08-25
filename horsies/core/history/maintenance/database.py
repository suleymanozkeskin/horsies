"""Typed access to the session-capable partition-maintenance database.

Partition maintenance uses PostgreSQL session state and DDL.  A plain
``AsyncEngine`` does not state whether it points at PostgreSQL directly or at a
transaction pooler.  This wrapper is the production API boundary: callers that
need partition DDL or session advisory locks must receive this type instead of
the runtime engine used for task traffic.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class PartitionMaintenanceDatabase:
    """A direct or session-capable PostgreSQL engine.

    Construction is owned by ``PostgresBroker``.  Tests can wrap their direct
    PostgreSQL engine explicitly.  The class does not infer capability from a
    URL or port number.
    """

    __slots__ = ('_engine',)

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @asynccontextmanager
    async def connect(self) -> AsyncGenerator[AsyncConnection]:
        """Check out one session-capable physical connection."""
        async with self._engine.connect() as connection:
            yield connection

    @asynccontextmanager
    async def begin(self) -> AsyncGenerator[AsyncConnection]:
        """Open one maintenance transaction."""
        async with self._engine.begin() as connection:
            yield connection

    @asynccontextmanager
    async def autocommit(self) -> AsyncGenerator[AsyncConnection]:
        """Pin one physical connection for session state and autocommit DDL."""
        engine = self._engine.execution_options(isolation_level='AUTOCOMMIT')
        async with engine.connect() as connection:
            yield connection
