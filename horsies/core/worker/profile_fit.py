"""Whether a worker profile fits the machine and the Postgres server.

These are facts that only a running machine or a live server knows, so the
static profile validation in ``AppConfig`` cannot check them:

- The sum of the per-child memory limits against the memory that the
  worker can use: the cgroup limit of its container when that is below the
  host RAM, else the host RAM. The worker logs a warning when the sum is
  above it, and starts: a deployment can move between machine sizes.
- The Postgres connections that one worker can open against the
  ``max_connections`` of the server, less the reserved slots. A worker
  logs a warning at boot; ``horsies check --live`` reports an error.

The readers do I/O and return a closed outcome. The comparisons are pure.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
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

RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER: Final = 1
"""The connection over which the worker process writes the runner heartbeats
of the RUNNING tasks of its children (``horsies.core.worker.runner_heartbeat``)."""

CONNECTION_LIMIT_READ_TIMEOUT_S: Final = 10.0
"""The longest wait for the server to answer the connection limit query."""

_BYTES_PER_MB: Final = 1024 * 1024

CGROUP_V2_MEMORY_MAX: Final = Path('/sys/fs/cgroup/memory.max')
"""The memory limit of the cgroup of this process under cgroup v2."""

CGROUP_V1_MEMORY_LIMIT: Final = Path('/sys/fs/cgroup/memory/memory.limit_in_bytes')
"""The memory limit of the cgroup of this process under cgroup v1."""

CGROUP_FILE_MAX_BYTES: Final = 64
"""The most bytes read from a cgroup limit file. A limit is one decimal
number of at most 20 digits, or ``max``."""

_CGROUP_NO_LIMIT: Final = 'max'

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
    """The total RAM of the host, in MB."""

    mb: int


@dataclass(frozen=True, slots=True)
class MachineMemoryUnreadable:
    """The RAM of the host could not be read."""

    reason: str


def read_machine_memory() -> MachineMemory | MachineMemoryUnreadable:
    """Read the total RAM of the host through ``psutil``.

    Inside a container this is the RAM of the host, not a cgroup limit; see
    ``read_cgroup_memory_limit``.
    """
    import psutil

    try:
        total_bytes = psutil.virtual_memory().total
    except (OSError, psutil.Error) as exc:
        return MachineMemoryUnreadable(reason=f'{type(exc).__name__}: {exc}')
    return MachineMemory(mb=total_bytes // _BYTES_PER_MB)


@dataclass(frozen=True, slots=True)
class CgroupLimit:
    """The cgroup of this process limits its memory to ``limit_bytes``."""

    limit_bytes: int


@dataclass(frozen=True, slots=True)
class NoCgroupLimit:
    """No cgroup memory limit applies: the limit file says ``max``, or no
    limit file exists (no cgroup memory controller, or not Linux)."""


@dataclass(frozen=True, slots=True)
class CgroupUnreadable:
    """A cgroup limit file exists but could not be read or parsed."""

    reason: str


@dataclass(frozen=True, slots=True)
class _CgroupFileAbsent:
    """The limit file does not exist."""


def _read_cgroup_file(path: Path) -> str | _CgroupFileAbsent | CgroupUnreadable:
    """The text of a cgroup limit file, read up to ``CGROUP_FILE_MAX_BYTES``."""
    try:
        with path.open('rb') as handle:
            raw = handle.read(CGROUP_FILE_MAX_BYTES + 1)
    except FileNotFoundError:
        return _CgroupFileAbsent()
    except OSError as exc:
        return CgroupUnreadable(reason=f'{path}: {type(exc).__name__}: {exc}')
    if len(raw) > CGROUP_FILE_MAX_BYTES:
        return CgroupUnreadable(
            reason=f'{path}: more than {CGROUP_FILE_MAX_BYTES} bytes',
        )
    try:
        return raw.decode('ascii').strip()
    except UnicodeDecodeError:
        return CgroupUnreadable(reason=f'{path}: not ASCII text')


def _parse_cgroup_value(
    path: Path,
    value: str,
) -> CgroupLimit | NoCgroupLimit | CgroupUnreadable:
    """Parse ``max`` or a decimal number of bytes."""
    if value == _CGROUP_NO_LIMIT:
        return NoCgroupLimit()
    if value.isascii() and value.isdigit():
        return CgroupLimit(limit_bytes=int(value))
    return CgroupUnreadable(reason=f'{path}: unexpected content {value!r}')


def read_cgroup_memory_limit(
    v2_path: Path = CGROUP_V2_MEMORY_MAX,
    v1_path: Path = CGROUP_V1_MEMORY_LIMIT,
) -> CgroupLimit | NoCgroupLimit | CgroupUnreadable:
    """Read the memory limit of the cgroup of this process.

    The cgroup v2 file is read when it exists, else the cgroup v1 file. No
    file means no cgroup limit. The cgroup v1 value for "no limit" is a
    number far above any RAM; ``usable_memory`` then keeps the host RAM.
    """
    match _read_cgroup_file(v2_path):
        case str() as value:
            return _parse_cgroup_value(v2_path, value)
        case CgroupUnreadable() as unreadable:
            return unreadable
        case _CgroupFileAbsent():
            pass
        case _ as unreachable_v2:
            assert_never(unreachable_v2)
    match _read_cgroup_file(v1_path):
        case str() as value:
            return _parse_cgroup_value(v1_path, value)
        case CgroupUnreadable() as unreadable:
            return unreadable
        case _CgroupFileAbsent():
            return NoCgroupLimit()
        case _ as unreachable_v1:
            assert_never(unreachable_v1)


class MemorySource(Enum):
    """Where the memory that a worker can use comes from."""

    CONTAINER_LIMIT = 'container limit'
    HOST_RAM = 'host RAM'


@dataclass(frozen=True, slots=True)
class UsableMemory:
    """The memory that the worker can use, in MB, and where it comes from."""

    mb: int
    source: MemorySource


@dataclass(frozen=True, slots=True)
class UsableMemoryUnreadable:
    """Neither a cgroup limit nor the host RAM could be read."""

    reason: str


def usable_memory(
    host: MachineMemory | MachineMemoryUnreadable,
    cgroup: CgroupLimit | NoCgroupLimit | CgroupUnreadable,
) -> UsableMemory | UsableMemoryUnreadable:
    """The smaller of the cgroup limit and the host RAM.

    A cgroup limit at or above the host RAM, which includes the cgroup v1
    value for "no limit", leaves the host RAM. An unreadable cgroup file
    leaves the host RAM; the caller reports it.
    """
    match (cgroup, host):
        case (CgroupLimit(limit_bytes=limit_bytes), MachineMemory(mb=host_mb)):
            container_mb = limit_bytes // _BYTES_PER_MB
            if container_mb < host_mb:
                return UsableMemory(
                    mb=container_mb, source=MemorySource.CONTAINER_LIMIT
                )
            return UsableMemory(mb=host_mb, source=MemorySource.HOST_RAM)
        case (CgroupLimit(limit_bytes=limit_bytes), MachineMemoryUnreadable()):
            return UsableMemory(
                mb=limit_bytes // _BYTES_PER_MB,
                source=MemorySource.CONTAINER_LIMIT,
            )
        case (NoCgroupLimit() | CgroupUnreadable(), MachineMemory(mb=host_mb)):
            return UsableMemory(mb=host_mb, source=MemorySource.HOST_RAM)
        case (
            NoCgroupLimit()
            | CgroupUnreadable(),
            MachineMemoryUnreadable(reason=reason),
        ):
            return UsableMemoryUnreadable(reason=reason)
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class MemoryLimitsFit:
    """The sum of the per-child memory limits is at or below the usable
    memory (``machine_mb``)."""

    total_mb: int
    machine_mb: int


@dataclass(frozen=True, slots=True)
class MemoryLimitsExceedMachine:
    """The sum of the per-child memory limits is above the usable memory
    (``machine_mb``)."""

    total_mb: int
    machine_mb: int


@dataclass(frozen=True, slots=True)
class MemoryLimitsNotSet:
    """A mode of the profile has children with no memory limit, so no sum
    exists."""


def compare_memory_limits(
    profile: WorkerProfile,
    machine: UsableMemory,
) -> MemoryLimitsFit | MemoryLimitsExceedMachine | MemoryLimitsNotSet:
    """Compare the sum over the children of their memory limits with the
    memory that the worker can use."""
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
    runner_heartbeat: int
    children: int
    per_child: int

    def total(self) -> int:
        return (
            self.parent_pool
            + self.listener
            + self.runner_heartbeat
            + self.children * self.per_child
        )

    def formula(self) -> str:
        return (
            f'parent pool {self.parent_pool} + listener {self.listener} + '
            f'runner heartbeat {self.runner_heartbeat} + '
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
        runner_heartbeat=RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
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
