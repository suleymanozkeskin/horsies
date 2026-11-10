"""Whether a worker profile fits the machine and the Postgres server.

These are facts that only a running machine or a live server knows, so the
static profile validation in ``AppConfig`` cannot check them:

- The sum of the per-child memory limits against the RAM of the machine.
  The worker logs a warning when the sum is above the RAM, and starts:
  a deployment can move between machine sizes.
- The Postgres connections that one worker can open against the
  ``max_connections`` of the server, less the reserved slots. A worker
  logs a warning at boot; ``horsies check --live`` reports an error.

The readers do I/O and return a closed outcome. The comparisons are pure.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, assert_never

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.models.broker import PostgresConfig
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    MemoryLimit,
    NoIsolatedChildren,
    NoMemoryLimit,
    WorkerProfile,
)

LISTENER_CONNECTIONS_PER_WORKER: Final = 2
"""Connections of the LISTEN/NOTIFY listener of a worker: one that waits for
notifications and one that runs LISTEN commands."""

CONNECTION_LIMIT_READ_TIMEOUT_S: Final = 10.0
"""The longest wait for the server to answer the connection limit query."""

_BYTES_PER_MB: Final = 1024 * 1024

_CONNECTION_LIMIT_SQL: Final = text(
    """
    SELECT
        current_setting('max_connections')::int,
        current_setting('superuser_reserved_connections')::int,
        COALESCE(current_setting('reserved_connections', true), '0')::int
    """
)


@dataclass(frozen=True, slots=True)
class MachineMemory:
    """The total RAM of the machine, in MB."""

    mb: int


@dataclass(frozen=True, slots=True)
class MachineMemoryUnreadable:
    """The RAM of the machine could not be read."""

    reason: str


def read_machine_memory() -> MachineMemory | MachineMemoryUnreadable:
    """Read the total RAM of the machine through ``psutil``.

    Inside a container this is the RAM of the host, not a cgroup limit.
    """
    import psutil

    try:
        total_bytes = psutil.virtual_memory().total
    except (OSError, psutil.Error) as exc:
        return MachineMemoryUnreadable(reason=f'{type(exc).__name__}: {exc}')
    return MachineMemory(mb=total_bytes // _BYTES_PER_MB)


@dataclass(frozen=True, slots=True)
class MemoryLimitsFit:
    """The sum of the per-child memory limits is at or below the RAM."""

    total_mb: int
    machine_mb: int


@dataclass(frozen=True, slots=True)
class MemoryLimitsExceedMachine:
    """The sum of the per-child memory limits is above the RAM."""

    total_mb: int
    machine_mb: int


@dataclass(frozen=True, slots=True)
class MemoryLimitsNotSet:
    """A mode of the profile has children with no memory limit, so no sum
    exists."""


def compare_memory_limits(
    profile: WorkerProfile,
    machine: MachineMemory,
) -> MemoryLimitsFit | MemoryLimitsExceedMachine | MemoryLimitsNotSet:
    """Compare the sum over the children of their memory limits with the RAM."""
    match profile.isolated:
        case IsolatedChildren(count=count, recycle=recycle):
            match recycle.above:
                case MemoryLimit(mb=mb):
                    total_mb = count * mb
                case NoMemoryLimit():
                    return MemoryLimitsNotSet()
                case _ as unreachable_limit:
                    assert_never(unreachable_limit)
        case NoIsolatedChildren():
            total_mb = 0
        case _ as unreachable:
            assert_never(unreachable)
    if total_mb > machine.mb:
        return MemoryLimitsExceedMachine(total_mb=total_mb, machine_mb=machine.mb)
    return MemoryLimitsFit(total_mb=total_mb, machine_mb=machine.mb)


@dataclass(frozen=True, slots=True)
class ServerConnectionLimit:
    """The connection limit of the server.

    ``reserved`` is ``superuser_reserved_connections`` plus
    ``reserved_connections`` (Postgres 16 and later): slots that an
    ordinary role cannot use.
    """

    max_connections: int
    reserved: int

    def usable(self) -> int:
        """The slots that an ordinary role can use."""
        return max(0, self.max_connections - self.reserved)


@dataclass(frozen=True, slots=True)
class ServerConnectionLimitUnreadable:
    """The connection limit could not be read from the server."""

    reason: str


async def read_server_connection_limit(
    session_factory: Callable[[], AsyncSession],
) -> ServerConnectionLimit | ServerConnectionLimitUnreadable:
    """Read the connection limit with one query on one pooled connection.

    Waits at most ``CONNECTION_LIMIT_READ_TIMEOUT_S``. A database error, an
    OS error, or the timeout returns ``ServerConnectionLimitUnreadable``.
    """
    try:
        async with asyncio.timeout(CONNECTION_LIMIT_READ_TIMEOUT_S):
            async with session_factory() as session:
                result = await session.execute(_CONNECTION_LIMIT_SQL)
                max_connections, superuser_reserved, reserved = result.one()
    except (SQLAlchemyError, OSError, TimeoutError) as exc:
        return ServerConnectionLimitUnreadable(
            reason=f'{type(exc).__name__}: {exc}',
        )
    return ServerConnectionLimit(
        max_connections=int(max_connections),
        reserved=int(superuser_reserved) + int(reserved),
    )


@dataclass(frozen=True, slots=True)
class WorkerConnectionNeed:
    """The Postgres connections that one worker can open, by part."""

    parent_pool: int
    listener: int
    children: int
    per_child: int

    def total(self) -> int:
        return self.parent_pool + self.listener + self.children * self.per_child

    def formula(self) -> str:
        return (
            f'parent pool {self.parent_pool} + listener {self.listener} + '
            f'{self.children} children x worker_child_pool_max_size '
            f'{self.per_child} = {self.total()}'
        )


def worker_connection_need(
    profile: WorkerProfile,
    broker: PostgresConfig,
) -> WorkerConnectionNeed:
    """The connections that one worker of this profile can open at most."""
    parent = broker.worker_runtime_config()
    return WorkerConnectionNeed(
        parent_pool=parent.pool_size + parent.max_overflow,
        listener=LISTENER_CONNECTIONS_PER_WORKER,
        children=profile.child_count(),
        per_child=broker.worker_child_pool_max_size,
    )


@dataclass(frozen=True, slots=True)
class ConnectionsFit:
    """One worker of the profile fits in the usable slots of the server."""

    need: WorkerConnectionNeed
    limit: ServerConnectionLimit


@dataclass(frozen=True, slots=True)
class ConnectionsExceedServer:
    """One worker of the profile can open more connections than the server
    has usable slots."""

    need: WorkerConnectionNeed
    limit: ServerConnectionLimit

    def describe(self) -> str:
        return (
            f'one worker can open {self.need.total()} Postgres connections; '
            f'the server has {self.limit.usable()} usable slots '
            f'(max_connections {self.limit.max_connections} - reserved '
            f'{self.limit.reserved})'
        )


def compare_connections(
    profile: WorkerProfile,
    broker: PostgresConfig,
    limit: ServerConnectionLimit,
) -> ConnectionsFit | ConnectionsExceedServer:
    """Compare the connections of one worker with the usable server slots."""
    need = worker_connection_need(profile, broker)
    if need.total() > limit.usable():
        return ConnectionsExceedServer(need=need, limit=limit)
    return ConnectionsFit(need=need, limit=limit)


CONNECTIONS_HELP: Final = (
    'lower the isolated count or worker_child_pool_max_size, raise '
    'max_connections, or send the pooled connections through a transaction '
    'pooler (pgbouncer_transaction_mode=True)'
)
"""What to change when a profile needs more connections than the server has."""
