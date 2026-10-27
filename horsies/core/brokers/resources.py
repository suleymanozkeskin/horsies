"""Loop-bound broker resources and the result of selecting them.

An ``AsyncEngine`` pool and a ``PostgresListener`` bind to the event loop
that first waits on them. A ``BrokerResources`` value is therefore used
only on the event loop it was built for. The broker keeps one set per open
event loop, up to a limit. Public entry points get the set for the running
loop from ``PostgresBroker.select_resources()`` once and pass it to their
helpers.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum

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
class EventLoopLimit:
    """The broker already holds resource sets for its limit of open event loops.

    The broker's sync runner loop does not count against the limit.

    Retry condition: repeating the call on the current loop returns the
    same error until one of the loops that hold a set closes.

    Fields:
        limit: the number of open event loops, other than the sync runner
            loop, that a broker serves.
        held_thread_names: thread of each open loop that holds a set and
            counts against the limit.
        current_thread_name: thread of the loop that made this call.
    """

    limit: int
    held_thread_names: tuple[str, ...]
    current_thread_name: str

    def describe(self) -> str:
        """Subject, cause and retry condition, for an error message."""
        held = ', '.join(repr(name) for name in self.held_thread_names)
        return (
            f'this broker already holds async resources for {self.limit} open '
            f'event loops (on threads {held}); this call ran on a new event '
            f'loop on thread {self.current_thread_name!r}. Repeating the call '
            'on this loop returns the same error until one of those loops '
            'closes. Close event loops that are done, or use a second broker.'
        )


class LoopRole(Enum):
    """Which kind of event loop a resource set serves."""

    # The broker's own sync runner loop. It does not count against the limit.
    SYNC_RUNNER = 'sync_runner'
    # Any other event loop.
    CALLER = 'caller'


@dataclass(frozen=True, slots=True)
class LoopResources:
    """A resource set and the event loop it was built for.

    The entry holds a strong reference to ``loop``, so the loop object stays
    alive and its identity is not reused while the entry exists.

    Fields:
        loop: the event loop the set was built for.
        thread_name: thread that ran ``loop`` when the set was built.
        role: whether ``loop`` is the broker's sync runner loop.
        resources: the set.
    """

    loop: asyncio.AbstractEventLoop
    thread_name: str
    role: LoopRole
    resources: BrokerResources


@dataclass(frozen=True, slots=True)
class UseExisting:
    """A set already serves the loop."""

    resources: BrokerResources


@dataclass(frozen=True, slots=True)
class BuildForLoop:
    """No set serves the loop, and the limit allows a new one."""


@dataclass(frozen=True, slots=True)
class LimitReached:
    """No set serves the loop, and the limit allows no new one."""

    loop_limit: EventLoopLimit


type LoopDecision = UseExisting | BuildForLoop | LimitReached


def decide_for_loop(
    entries: tuple[LoopResources, ...],
    loop: asyncio.AbstractEventLoop,
    thread_name: str,
    role: LoopRole,
    limit: int,
) -> LoopDecision:
    """Decide how ``loop`` gets its resource set. Pure: no I/O, no state change.

    Entries whose loop is closed do not count against ``limit``; the caller
    releases them. A ``SYNC_RUNNER`` loop never reaches the limit.
    """
    for entry in entries:
        if entry.loop is loop:
            return UseExisting(resources=entry.resources)
    match role:
        case LoopRole.SYNC_RUNNER:
            return BuildForLoop()
        case LoopRole.CALLER:
            held = tuple(
                entry.thread_name
                for entry in entries
                if entry.role is LoopRole.CALLER and not entry.loop.is_closed()
            )
            if len(held) < limit:
                return BuildForLoop()
            return LimitReached(
                loop_limit=EventLoopLimit(
                    limit=limit,
                    held_thread_names=held,
                    current_thread_name=thread_name,
                ),
            )


@dataclass(frozen=True, slots=True)
class LoopEntriesByState:
    """Entries split by whether their event loop is closed."""

    open_loops: tuple[LoopResources, ...]
    closed_loops: tuple[LoopResources, ...]


def split_closed_loops(entries: tuple[LoopResources, ...]) -> LoopEntriesByState:
    """Split entries by loop state. Reads each loop's state once. Pure."""
    open_loops: list[LoopResources] = []
    closed_loops: list[LoopResources] = []
    for entry in entries:
        match entry.loop.is_closed():
            case True:
                closed_loops.append(entry)
            case False:
                open_loops.append(entry)
    return LoopEntriesByState(
        open_loops=tuple(open_loops), closed_loops=tuple(closed_loops),
    )
