"""Loop-bound broker resources and the result of selecting them.

An ``AsyncEngine`` pool and a ``PostgresListener`` bind to the event loop
that first waits on them. A ``BrokerResources`` value is therefore used
only on the event loop it was built for. Public entry points get it from
``PostgresBroker.select_resources()`` once and pass it to their helpers.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.brokers.listener import PostgresListener
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase


@dataclass(frozen=True, slots=True)
class ListenerEnabled:
    """The broker owns a listener for LISTEN/NOTIFY waits."""

    listener: PostgresListener


@dataclass(frozen=True, slots=True)
class ListenerDisabled:
    """The broker was built with ``assume_initialized=True``: it has no listener.

    Waits that would listen poll instead.
    """


type ListenerState = ListenerEnabled | ListenerDisabled


@dataclass(frozen=True, slots=True)
class SessionEngineShared:
    """The session URL equals the runtime URL: the runtime engine serves both."""


@dataclass(frozen=True, slots=True)
class SessionEngineSeparate:
    """A separate engine for the session-capable URL (LISTEN, session locks)."""

    engine: AsyncEngine


type SessionEngine = SessionEngineShared | SessionEngineSeparate


@dataclass(frozen=True, slots=True)
class BrokerResources:
    """Loop-bound resources of one broker. Used only on the loop they were built for.

    Fields:
        engine: runtime engine (pooled, ``database_url``).
        session_factory: sessions on ``engine``.
        session_engine: engine for the session-capable URL.
        partition_maintenance: partition maintenance database on the
            session engine.
        listener: the listener, or ``ListenerDisabled``.
    """

    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    session_engine: SessionEngine
    partition_maintenance: PartitionMaintenanceDatabase
    listener: ListenerState

    def session_async_engine(self) -> AsyncEngine:
        """The engine that serves the session-capable URL."""
        match self.session_engine:
            case SessionEngineShared():
                return self.engine
            case SessionEngineSeparate(engine=separate):
                return separate


@dataclass(frozen=True, slots=True)
class EventLoopConflict:
    """The broker's async resources are bound to another open event loop.

    Retry condition: repeating the call on the current loop returns the
    same conflict. It clears when the bound loop closes.

    Fields:
        bound_thread_name: thread that runs the loop holding the resources.
        current_thread_name: thread of the loop that made this call.
    """

    bound_thread_name: str
    current_thread_name: str

    def describe(self) -> str:
        """Subject, cause and retry condition, for an error message."""
        return (
            'the async resources of this broker are in use by the open event '
            f'loop on thread {self.bound_thread_name!r}; this call ran on '
            f'thread {self.current_thread_name!r}. Repeating the call on this '
            'loop returns the same error until that loop closes. Use one '
            'event loop per broker for async calls, or a second broker.'
        )
