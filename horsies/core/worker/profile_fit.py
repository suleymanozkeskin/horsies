"""Whether a worker profile fits the machine and the Postgres server.

These are facts that only a running machine or a live server knows, so the
static profile validation in ``AppConfig`` cannot check them:

- The sum of the per-child memory limits against the memory that the
  worker can use: the cgroup limit of its container when that is below the
  host RAM, else the host RAM. The worker logs a warning when the sum is
  above it, and starts: a deployment can move between machine sizes.
- The Postgres connections that one worker opens itself against the
  ``max_connections`` of the server, less the reserved slots. A worker
  logs a warning at boot; ``horsies check --live`` reports an error. Task
  code that sends opens more, and each message states how many.

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
    NoSharedAsyncChildren,
    NoSharedThreadChildren,
    SharedAsyncChildren,
    SharedThreadChildren,
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

PROC_SELF_CGROUP: Final = Path('/proc/self/cgroup')
"""The cgroups of this process, one line per hierarchy:
``<id>:<controllers>:<path>``. The cgroup v2 line is ``0::<path>``."""

CGROUP_V2_ROOT: Final = Path('/sys/fs/cgroup')
"""The mount point of the cgroup v2 hierarchy."""

CGROUP_V1_MEMORY_ROOT: Final = Path('/sys/fs/cgroup/memory')
"""The mount point of the cgroup v1 memory controller."""

CGROUP_V2_LIMIT_FILE: Final = 'memory.max'
CGROUP_V1_LIMIT_FILE: Final = 'memory.limit_in_bytes'

CGROUP_FILE_MAX_BYTES: Final = 64
"""The most bytes read from a cgroup limit file. A limit is one decimal
number of at most 20 digits, or ``max``."""

PROC_CGROUP_MAX_BYTES: Final = 64 * 1024
"""The most bytes read from ``/proc/self/cgroup``. It has one line per
hierarchy, about 15, each with a path of a few hundred bytes at most."""

CGROUP_PATH_MAX_DEPTH: Final = 32
"""The most path parts of a cgroup that the reader walks. A deeper path
is not resolved, and the reader falls back to the limit files at the
mount points."""

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


@dataclass(frozen=True, slots=True)
class CgroupRoots:
    """Where the reader finds the cgroup files. Tests point it at fake files."""

    proc_self_cgroup: Path = PROC_SELF_CGROUP
    v2_root: Path = CGROUP_V2_ROOT
    v1_memory_root: Path = CGROUP_V1_MEMORY_ROOT


@dataclass(frozen=True, slots=True)
class InCgroup:
    """The process is in this group of a hierarchy, given as path parts."""

    parts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NoCgroupPath:
    """``/proc/self/cgroup`` names no usable group for the hierarchy: no
    line, a path that leaves the mount point (``..``), or a path deeper than
    ``CGROUP_PATH_MAX_DEPTH``."""


type CgroupPathOf = InCgroup | NoCgroupPath


@dataclass(frozen=True, slots=True)
class ProcCgroup:
    """The groups of this process that hold a memory limit."""

    v2: CgroupPathOf
    v1_memory: CgroupPathOf


def _group_of(path: str) -> CgroupPathOf:
    parts = tuple(part for part in path.split('/') if part)
    if '..' in parts or len(parts) > CGROUP_PATH_MAX_DEPTH:
        return NoCgroupPath()
    return InCgroup(parts)


def parse_proc_cgroup(text: str) -> ProcCgroup:
    """The cgroup v2 group and the cgroup v1 memory group of ``text``.

    A line that has not three fields is skipped. Pure.
    """
    v2: CgroupPathOf = NoCgroupPath()
    v1_memory: CgroupPathOf = NoCgroupPath()
    for line in text.splitlines():
        fields = line.split(':', 2)
        match fields:
            case ['0', '', path]:
                v2 = _group_of(path)
            case [_, controllers, path] if 'memory' in controllers.split(','):
                v1_memory = _group_of(path)
            case _:
                pass
    return ProcCgroup(v2=v2, v1_memory=v1_memory)


def _read_proc_cgroup(path: Path) -> ProcCgroup | _CgroupFileAbsent:
    """Read ``/proc/self/cgroup``. Absent, unreadable or too long: absent."""
    try:
        with path.open('rb') as handle:
            raw = handle.read(PROC_CGROUP_MAX_BYTES + 1)
    except OSError:
        return _CgroupFileAbsent()
    if len(raw) > PROC_CGROUP_MAX_BYTES:
        return _CgroupFileAbsent()
    return parse_proc_cgroup(raw.decode('utf-8', errors='replace'))


def _limit_files(root: Path, group: CgroupPathOf, file_name: str) -> tuple[Path, ...]:
    """The limit file of the group, then of each parent group up to ``root``."""
    match group:
        case InCgroup(parts=parts):
            return tuple(
                root.joinpath(*parts[:depth], file_name)
                for depth in range(len(parts), -1, -1)
            )
        case NoCgroupPath():
            return (root / file_name,)
        case _ as unreachable:
            assert_never(unreachable)


def _limit_of(
    path: Path,
) -> CgroupLimit | NoCgroupLimit | CgroupUnreadable | _CgroupFileAbsent:
    match _read_cgroup_file(path):
        case str() as value:
            return _parse_cgroup_value(path, value)
        case CgroupUnreadable() | _CgroupFileAbsent() as other:
            return other
        case _ as unreachable:
            assert_never(unreachable)


def _smallest_limit(
    files: tuple[Path, ...],
) -> CgroupLimit | NoCgroupLimit | CgroupUnreadable | _CgroupFileAbsent:
    """The smallest limit of ``files``: a group is limited by each parent.

    ``max`` is no limit at that level. Absent when no file exists. The first
    unreadable file is the outcome.
    """
    smallest: CgroupLimit | NoCgroupLimit | _CgroupFileAbsent = _CgroupFileAbsent()
    for path in files:
        match _limit_of(path), smallest:
            case CgroupUnreadable() as unreadable, _:
                return unreadable
            case _CgroupFileAbsent(), _:
                pass
            case NoCgroupLimit(), _CgroupFileAbsent():
                smallest = NoCgroupLimit()
            case NoCgroupLimit(), _:
                pass
            case CgroupLimit() as limit, CgroupLimit(limit_bytes=held) if (
                held <= limit.limit_bytes
            ):
                pass
            case CgroupLimit() as limit, _:
                smallest = limit
            case _ as unreachable:
                assert_never(unreachable)
    return smallest


def read_cgroup_memory_limit(
    roots: CgroupRoots = CgroupRoots(),
) -> CgroupLimit | NoCgroupLimit | CgroupUnreadable:
    """Read the memory limit of the cgroup of this process.

    ``/proc/self/cgroup`` names the group of the process: the cgroup v1
    memory group when one is named, else the cgroup v2 group. The limit is
    the smallest of the group and its parent groups, up to the mount point
    (a systemd ``MemoryMax`` of a service or a slice, or a container seen
    with ``--cgroupns=host``). Without ``/proc/self/cgroup``, the reader
    takes the cgroup v2 file at the mount point, else the cgroup v1 file
    there. No file means no cgroup limit. The cgroup v1 value for "no
    limit" is a number far above any RAM; ``usable_memory`` then keeps the
    host RAM.
    """
    match _read_proc_cgroup(roots.proc_self_cgroup):
        case ProcCgroup(v1_memory=InCgroup() as group):
            chains = (_limit_files(roots.v1_memory_root, group, CGROUP_V1_LIMIT_FILE),)
        case ProcCgroup(v2=InCgroup() as group):
            chains = (_limit_files(roots.v2_root, group, CGROUP_V2_LIMIT_FILE),)
        case ProcCgroup() | _CgroupFileAbsent():
            chains = (
                (roots.v2_root / CGROUP_V2_LIMIT_FILE,),
                (roots.v1_memory_root / CGROUP_V1_LIMIT_FILE,),
            )
        case _ as unreachable:
            assert_never(unreachable)
    for files in chains:
        match _smallest_limit(files):
            case CgroupLimit() | NoCgroupLimit() | CgroupUnreadable() as outcome:
                return outcome
            case _CgroupFileAbsent():
                pass
            case _ as unreachable_chain:
                assert_never(unreachable_chain)
    return NoCgroupLimit()


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


def _mode_memory_mb(
    count: int,
    limit: MemoryLimit | NoMemoryLimit,
) -> int | MemoryLimitsNotSet:
    """The sum of the memory limits of ``count`` children of one mode."""
    match limit:
        case MemoryLimit(mb=mb):
            return count * mb
        case NoMemoryLimit():
            return MemoryLimitsNotSet()
        case _ as unreachable:
            assert_never(unreachable)


def _isolated_memory_mb(
    isolated: IsolatedChildren | NoIsolatedChildren,
) -> int | MemoryLimitsNotSet:
    match isolated:
        case IsolatedChildren(count=count, recycle=recycle):
            return _mode_memory_mb(count, recycle.above)
        case NoIsolatedChildren():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _shared_async_memory_mb(
    shared_async: SharedAsyncChildren | NoSharedAsyncChildren,
) -> int | MemoryLimitsNotSet:
    match shared_async:
        case SharedAsyncChildren(count=count, recycle=recycle):
            return _mode_memory_mb(count, recycle.above)
        case NoSharedAsyncChildren():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _shared_thread_memory_mb(
    shared_thread: SharedThreadChildren | NoSharedThreadChildren,
) -> int | MemoryLimitsNotSet:
    match shared_thread:
        case SharedThreadChildren(count=count, recycle=recycle):
            return _mode_memory_mb(count, recycle.above)
        case NoSharedThreadChildren():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def compare_memory_limits(
    profile: WorkerProfile,
    machine: UsableMemory,
) -> MemoryLimitsFit | MemoryLimitsExceedMachine | MemoryLimitsNotSet:
    """Compare the sum over the children of their memory limits with the
    memory that the worker can use.

    The sum covers every mode. A mode with children and no memory limit has
    no sum, so the profile has none: ``MemoryLimitsNotSet``.
    """
    total_mb = 0
    for mode_mb in (
        _isolated_memory_mb(profile.isolated),
        _shared_async_memory_mb(profile.shared_async),
        _shared_thread_memory_mb(profile.shared_thread),
    ):
        match mode_mb:
            case MemoryLimitsNotSet():
                return mode_mb
            case int():
                total_mb += mode_mb
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
    """The Postgres connections of one worker, by part.

    ``total`` counts the connections that the worker opens itself: its
    pool, its listener, its runner heartbeat connection, and the pool of
    each child. Task code that sends through the app opens the app's
    broker in its child: a pool of ``task_broker_per_loop`` connections for
    each event loop that it sends from. ``total`` does not count those.
    ``with_task_sends`` counts one such loop in each child.
    """

    parent_pool: int
    listener: int
    runner_heartbeat: int
    children: int
    per_child: int
    task_broker_per_loop: int

    def total(self) -> int:
        return (
            self.parent_pool
            + self.listener
            + self.runner_heartbeat
            + self.children * self.per_child
        )

    def task_sends(self) -> int:
        """The connections that task code adds with one sending loop per child."""
        return self.children * self.task_broker_per_loop

    def with_task_sends(self) -> int:
        return self.total() + self.task_sends()

    def formula(self) -> str:
        return (
            f'parent pool {self.parent_pool} + listener {self.listener} + '
            f'runner heartbeat {self.runner_heartbeat} + '
            f'{self.children} children x worker_child_pool_max_size '
            f'{self.per_child} = {self.total()}'
        )

    def task_sends_note(self) -> str:
        """What ``total`` does not count, with its size for this profile."""
        return (
            'not counted: task code that sends opens a pool of '
            f'worker_pool_size + worker_max_overflow = {self.task_broker_per_loop} '
            'connections in its child for each event loop that it sends from; '
            f'with one such loop in each child, {self.children} x '
            f'{self.task_broker_per_loop} = {self.task_sends()} more, '
            f'{self.with_task_sends()} in all'
        )


def worker_connection_need(
    profile: WorkerProfile,
    broker: PostgresConfig,
) -> WorkerConnectionNeed:
    """The connections of one worker of this profile.

    A child builds the app's broker from the worker runtime config, as the
    worker process does, so a pool of task code has the size of the parent
    pool.
    """
    parent = broker.worker_runtime_config()
    return WorkerConnectionNeed(
        parent_pool=parent.pool_size + parent.max_overflow,
        listener=LISTENER_CONNECTIONS_PER_WORKER,
        runner_heartbeat=RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
        children=profile.child_count(),
        per_child=broker.worker_child_pool_max_size,
        task_broker_per_loop=parent.pool_size + parent.max_overflow,
    )


@dataclass(frozen=True, slots=True)
class ConnectionsFit:
    """The connections that one worker of the profile opens itself fit in the
    usable slots of the server. Task code that sends is not compared."""

    need: WorkerConnectionNeed
    limit: ServerConnectionLimit

    def describe(self) -> str:
        return (
            f'one worker opens up to {self.need.total()} Postgres connections '
            f'itself, of {self.limit.usable()} usable on the server; '
            f'{self.need.task_sends_note()}'
        )


@dataclass(frozen=True, slots=True)
class ConnectionsExceedServer:
    """The connections that one worker of the profile opens itself are more
    than the usable slots of the server."""

    need: WorkerConnectionNeed
    limit: ServerConnectionLimit

    def describe(self) -> str:
        return (
            f'one worker opens up to {self.need.total()} Postgres connections '
            f'itself, not counting task code; the server has '
            f'{self.limit.usable()} usable slots (max_connections '
            f'{self.limit.max_connections} - reserved {self.limit.reserved})'
        )


def compare_connections(
    profile: WorkerProfile,
    broker: PostgresConfig,
    limit: ServerConnectionLimit,
) -> ConnectionsFit | ConnectionsExceedServer:
    """Compare the connections that one worker opens itself with the usable
    server slots. Task code that sends is not compared: whether it sends,
    and from how many event loops, is known only at run time."""
    need = worker_connection_need(profile, broker)
    if need.total() > limit.usable():
        return ConnectionsExceedServer(need=need, limit=limit)
    return ConnectionsFit(need=need, limit=limit)


CONNECTIONS_HELP: Final = (
    'lower the child counts or worker_child_pool_max_size, raise '
    'max_connections, or send the pooled connections through a transaction '
    'pooler (pgbouncer_transaction_mode=True)'
)
"""What to change when a profile needs more connections than the server has."""
