"""Named worker profiles: the settings of one worker, kept in the app config.

``horsies worker <app> --profile <name>`` starts a worker with the profile of
that name from ``AppConfig.worker_profiles``. A profile states:

- how many children the worker runs for each execution mode,
- when a child is replaced (recycled),
- the connection pool of each child, and the default executor of a shared
  async child,
- the pool that task code opens in a child when it sends,
- the connection pool of the worker process itself,
- how many tasks the worker claims,
- how long a stopping worker waits for running tasks,
- what a Linux worker imports before it forks children.

Three execution modes have children here:

- isolated: an isolated child runs one task at a time in its own process;
- shared async: a shared async child runs up to ``seats`` ``async def`` tasks
  of ``Execution.SHARED_ASYNC`` at once, as coroutines on one event loop;
- shared thread: a shared thread child runs up to ``seats`` ``def`` tasks of
  ``Execution.SHARED_THREAD`` at once, each on a thread of its own.

The number of children of a worker is the sum of the counts of its modes; no
separate total exists. A child never changes its mode.

Each value is bounded where it is built. The checks that need more than one
value (at least one child, claim limits against capacity, the ``min_size`` of
a child pool against its resolved ``max_size``) run in ``AppConfig``
validation, which knows the profile name and reports the field path, for
example ``worker_profiles['batch'].claim.claimed_per_worker``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Annotated, Final, Literal, Union, assert_never

from pydantic import BaseModel, ConfigDict, Field

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.child_sizing import (
    DEFAULT_ISOLATED_POOL_CONNECTIONS,
    DEFAULT_TASK_SEND_POOL_CONNECTIONS,
    parent_pool_connections,
    shared_async_executor_threads,
    shared_async_pool_connections,
    shared_thread_pool_connections,
)
from horsies.core.worker.config import ChildPreload

MAX_ISOLATED_CHILDREN: Final = 1024
"""The most isolated children that one profile starts."""

MAX_TASK_LIMIT: Final = 2_147_483_647
"""The largest task count of a limit: the largest Postgres ``integer``.

The claim limits go to the claim function as that type."""

MAX_MEMORY_LIMIT_MB: Final = 16 * 1024 * 1024
"""The largest memory limit of one child: 16 TiB, in MB."""

MAX_SHARED_ASYNC_CHILDREN: Final = 1024
"""The most shared async children that one profile starts."""

MAX_SEATS_PER_SHARED_ASYNC_CHILD: Final = 1000
"""The most tasks that one shared async child runs at once.

A seat that waits on the network holds at least one socket, and the common
default limit of open files of one process is 1024 (``ulimit -n``). Above
1000 seats a child runs out of file descriptors before its seats are full.
"""

MAX_SHARED_THREAD_CHILDREN: Final = 1024
"""The most shared thread children that one profile starts."""

MAX_SEATS_PER_SHARED_THREAD_CHILD: Final = 256
"""The most tasks that one shared thread child runs at once.

Each seat is an OS thread with a stack of its own: 8 MiB of address space by
default on Linux with glibc, 512 KiB on macOS. 256 seats reserve 2 GiB of
address space in one child on Linux. More threads than CPUs add no
throughput to CPU work; seats above the CPU count help only tasks that wait
in sync I/O.
"""

MAX_CHILD_POOL_CONNECTIONS: Final = 1000
"""The most connections in the connection pool of one child.

Each connection holds a socket, and the common default limit of open files
of one process is 1024 (``ulimit -n``).
"""

MAX_EXECUTOR_THREADS: Final = 256
"""The most threads in the default executor of one shared async child.

Each thread has a stack of its own: 8 MiB of address space by default on
Linux with glibc. 256 threads reserve 2 GiB of address space in one child.
"""

DEFAULT_RECYCLE_AFTER_TASKS: Final = 100
"""Tasks after which a child is replaced when the profile does not say."""

DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT: Final = 100
"""Tasks per seat after which a shared child is replaced when the profile
does not say."""

DEFAULT_DRAIN_TIMEOUT: Final = timedelta(seconds=300)
"""Time that a stopping worker waits for running tasks by default."""

MAX_DRAIN_TIMEOUT: Final = timedelta(hours=24)
"""The longest time that a stopping worker waits for running tasks."""

PROFILE_NAME_MAX_LENGTH: Final = 64
"""The most characters in a profile name."""

_PROFILE_NAME: Final = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.-]*')
"""Letters, digits, ``_``, ``.`` and ``-``; the first is not ``.`` or ``-``,
so the name cannot read as a command-line option."""


class TaskLimit(BaseModel):
    """A limit of ``tasks`` tasks, from 1 to ``MAX_TASK_LIMIT``."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['task_limit'] = 'task_limit'
    tasks: int = Field(ge=1, le=MAX_TASK_LIMIT, description='Number of tasks')


class NoTaskLimit(BaseModel):
    """No limit on the number of tasks."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_task_limit'] = 'no_task_limit'


class MemoryLimit(BaseModel):
    """A limit of ``mb`` MB of resident memory (RSS), from 1 to
    ``MAX_MEMORY_LIMIT_MB``.

    No fixed floor applies here: a constant cannot know the memory of a
    booted child of an app. At startup the worker refuses a limit at or
    below the measured child baseline.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['memory_limit'] = 'memory_limit'
    mb: int = Field(ge=1, le=MAX_MEMORY_LIMIT_MB, description='Megabytes of RSS')


class NoMemoryLimit(BaseModel):
    """No limit on the resident memory of a child."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_memory_limit'] = 'no_memory_limit'


TaskLimitSetting = Annotated[
    Union[TaskLimit, NoTaskLimit],
    Field(discriminator='kind'),
]
MemoryLimitSetting = Annotated[
    Union[MemoryLimit, NoMemoryLimit],
    Field(discriminator='kind'),
]


class PoolSize(BaseModel):
    """A pool of at most ``connections`` connections, from 1 to
    ``MAX_CHILD_POOL_CONNECTIONS``. The bound applies to every pool of one
    process: the pool of a child, the pool that task code opens when it
    sends, and the pool of the worker process."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['pool_size'] = 'pool_size'
    connections: int = Field(
        ge=1,
        le=MAX_CHILD_POOL_CONNECTIONS,
        description='Most connections in the pool',
    )


class PoolSizeFromSeats(BaseModel):
    """The most connections of the pool follow the seats:
    ``min(cap, max(floor, ceil(seats / seats_per_connection)))``
    (``horsies.core.models.child_sizing``).

    - The pool of a shared child follows the seats of the child, with a
      floor of 2 and the ``seats_per_connection`` and ``cap`` of its kind.
    - The pool of the worker process (``ParentPool``) follows the seats of
      the profile, with ``PARENT_POOL_CONNECTIONS_FLOOR``,
      ``PARENT_SEATS_PER_POOL_CONNECTION`` and
      ``PARENT_POOL_CONNECTIONS_CAP``.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['pool_size_from_seats'] = 'pool_size_from_seats'


PoolSizeSetting = Annotated[
    Union[PoolSize, PoolSizeFromSeats],
    Field(discriminator='kind'),
]


class ExecutorThreads(BaseModel):
    """A default executor of ``threads`` threads, from 1 to
    ``MAX_EXECUTOR_THREADS``."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['executor_threads'] = 'executor_threads'
    threads: int = Field(
        ge=1,
        le=MAX_EXECUTOR_THREADS,
        description='Threads of the default executor',
    )


class ExecutorThreadsFromSeats(BaseModel):
    """The threads of the default executor follow the seats of the child:
    ``min(cap, max(2, ceil(seats / seats_per_thread)))``
    (``horsies.core.models.child_sizing``)."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['executor_threads_from_seats'] = 'executor_threads_from_seats'


ExecutorThreadsSetting = Annotated[
    Union[ExecutorThreads, ExecutorThreadsFromSeats],
    Field(discriminator='kind'),
]


@dataclass(frozen=True, slots=True)
class ResolvedPool:
    """The connection pool of one child as it opens: from ``min_size`` to
    ``max_size`` connections, and a check of each connection on checkout
    when ``check`` is true.

    ``0 <= min_size <= max_size <= MAX_CHILD_POOL_CONNECTIONS`` and
    ``max_size >= 1``: AppConfig validation refuses a profile that resolves
    to other values.
    """

    min_size: int
    max_size: int
    check: bool


def _default_isolated_pool_size() -> PoolSize:
    return PoolSize(connections=DEFAULT_ISOLATED_POOL_CONNECTIONS)


def _default_task_send_pool() -> PoolSize:
    return PoolSize(connections=DEFAULT_TASK_SEND_POOL_CONNECTIONS)


class IsolatedPool(BaseModel):
    """The connection pool that each isolated child opens.

    - ``min_size``: connections that the pool keeps open, from 0 to
      ``MAX_CHILD_POOL_CONNECTIONS``. With 0, a connection opens at its
      first use.
    - ``max_size``: the most connections of the pool.
    - ``check``: check each connection when a write takes it from the pool.
      Each check is a database round trip; turn it off on high-RTT links
      where the per-statement retry handling already covers stale
      connections.

    AppConfig validation refuses a ``min_size`` above ``max_size``.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    min_size: int = Field(
        default=0,
        ge=0,
        le=MAX_CHILD_POOL_CONNECTIONS,
        description='Connections that the pool keeps open',
    )
    max_size: PoolSize = Field(default_factory=_default_isolated_pool_size)
    check: bool = Field(
        default=True,
        description='Check each connection on checkout',
    )

    def resolved(self) -> ResolvedPool:
        """The pool as each isolated child opens it."""
        return ResolvedPool(
            min_size=self.min_size,
            max_size=self.max_size.connections,
            check=self.check,
        )


class SharedPool(BaseModel):
    """The connection pool that each shared child opens. Its seats share it.

    - ``min_size``: connections that the pool keeps open, from 0 to
      ``MAX_CHILD_POOL_CONNECTIONS``. With 0, a connection opens at its
      first use.
    - ``max_size``: the most connections of the pool: ``PoolSize``, or
      ``PoolSizeFromSeats()`` to derive it from the seats of the child.
    - ``check``: check each connection when a write takes it from the pool,
      as for ``IsolatedPool``.

    AppConfig validation refuses a ``min_size`` above the resolved
    ``max_size``.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    min_size: int = Field(
        default=0,
        ge=0,
        le=MAX_CHILD_POOL_CONNECTIONS,
        description='Connections that the pool keeps open',
    )
    max_size: PoolSizeSetting = Field(default_factory=PoolSizeFromSeats)
    check: bool = Field(
        default=True,
        description='Check each connection on checkout',
    )

    def resolved(self, from_seats: int) -> ResolvedPool:
        """The pool as each shared child opens it. ``from_seats`` is the
        size that the kind of the child derives from its seats."""
        match self.max_size:
            case PoolSize(connections=connections):
                max_size = connections
            case PoolSizeFromSeats():
                max_size = from_seats
            case _ as unreachable:
                assert_never(unreachable)
        return ResolvedPool(
            min_size=self.min_size,
            max_size=max_size,
            check=self.check,
        )


def _default_recycle_after() -> TaskLimit:
    return TaskLimit(tasks=DEFAULT_RECYCLE_AFTER_TASKS)


class IsolatedRecycle(BaseModel):
    """When the worker replaces an isolated child with a new one.

    Each child has its own counters, so children are replaced at different
    times, not all at once.

    - ``after``: replace a child after it completes this many tasks.
    - ``above``: replace a child when its own RSS reaches this limit. The
      child reports its RSS with each result. A task can go above the limit
      while it runs: this bounds memory that stays after a task, not the
      peak of a task.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    after: TaskLimitSetting = Field(default_factory=_default_recycle_after)
    above: MemoryLimitSetting = Field(default_factory=NoMemoryLimit)


class IsolatedChildren(BaseModel):
    """``count`` isolated children: each runs one task at a time in its own
    process, and opens the connection pool ``pool``. ``count`` is from 1 to
    ``MAX_ISOLATED_CHILDREN``.

    Task code that sends through the app (``app.get_broker()``) opens a pool
    of up to ``task_send_pool`` connections in its child, with no overflow,
    for each event loop that it sends from.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['isolated_children'] = 'isolated_children'
    count: int = Field(
        ge=1,
        le=MAX_ISOLATED_CHILDREN,
        description='Number of isolated children',
    )
    recycle: IsolatedRecycle = Field(default_factory=IsolatedRecycle)
    pool: IsolatedPool = Field(default_factory=IsolatedPool)
    task_send_pool: PoolSize = Field(default_factory=_default_task_send_pool)

    def resolved_pool(self) -> ResolvedPool:
        """The pool that each child opens."""
        return self.pool.resolved()


class NoIsolatedChildren(BaseModel):
    """The profile starts no isolated children."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_isolated_children'] = 'no_isolated_children'


IsolatedSetting = Annotated[
    Union[IsolatedChildren, NoIsolatedChildren],
    Field(discriminator='kind'),
]


def _default_recycle_after_per_seat() -> TaskLimit:
    return TaskLimit(tasks=DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT)


class SharedRecycle(BaseModel):
    """When the worker replaces a shared child with a new one.

    - ``after_per_seat``: replace a child after it completes
      ``after_per_seat.tasks × seats`` tasks. The limit counts per seat, so
      a child with many seats is not replaced after a few seconds of work.
    - ``above``: replace a child when its own RSS reaches this limit. The
      child reports its RSS after each task. Its tasks share the process, so
      this bounds the memory of all of them together.

    A child that reaches a limit closes: it takes no new task, its running
    tasks finish, and then it stops and a new child starts.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    after_per_seat: TaskLimitSetting = Field(
        default_factory=_default_recycle_after_per_seat,
    )
    above: MemoryLimitSetting = Field(default_factory=NoMemoryLimit)


class SharedAsyncChildren(BaseModel):
    """``count`` shared async children, each with ``seats`` seats.

    A shared async child runs up to ``seats`` tasks of
    ``Execution.SHARED_ASYNC`` at once, as coroutines on one event loop, and
    a free seat takes the next task at once. ``count`` is from 1 to
    ``MAX_SHARED_ASYNC_CHILDREN``; ``seats`` is from 1 to
    ``MAX_SEATS_PER_SHARED_ASYNC_CHILD``.

    Each child opens the async connection pool ``pool``, which its seats
    share. ``executor`` sizes the default executor of its event loop
    (``asyncio.to_thread``, ``loop.run_in_executor(None, ...)``). An
    ``on_shared_async_child_start`` hook that sets a default executor of its
    own replaces it.

    Task code that sends through the app (``app.get_broker()``) opens a pool
    of up to ``task_send_pool`` connections in its child, with no overflow,
    for each event loop that it sends from. The seats of a child send from
    its one event loop, so they share that pool.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['shared_async_children'] = 'shared_async_children'
    count: int = Field(
        ge=1,
        le=MAX_SHARED_ASYNC_CHILDREN,
        description='Number of shared async children',
    )
    seats: int = Field(
        ge=1,
        le=MAX_SEATS_PER_SHARED_ASYNC_CHILD,
        description='Tasks that one child runs at once',
    )
    recycle: SharedRecycle = Field(default_factory=SharedRecycle)
    pool: SharedPool = Field(default_factory=SharedPool)
    executor: ExecutorThreadsSetting = Field(
        default_factory=ExecutorThreadsFromSeats,
    )
    task_send_pool: PoolSize = Field(default_factory=_default_task_send_pool)

    def resolved_pool(self) -> ResolvedPool:
        """The pool that each child opens."""
        return self.pool.resolved(shared_async_pool_connections(self.seats))

    def resolved_executor_threads(self) -> int:
        """The threads of the default executor of each child."""
        match self.executor:
            case ExecutorThreads(threads=threads):
                return threads
            case ExecutorThreadsFromSeats():
                return shared_async_executor_threads(self.seats)
            case _ as unreachable:
                assert_never(unreachable)


class NoSharedAsyncChildren(BaseModel):
    """The profile starts no shared async children."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_shared_async_children'] = 'no_shared_async_children'


SharedAsyncSetting = Annotated[
    Union[SharedAsyncChildren, NoSharedAsyncChildren],
    Field(discriminator='kind'),
]


class SharedThreadChildren(BaseModel):
    """``count`` shared thread children, each with ``seats`` seats.

    A shared thread child runs up to ``seats`` tasks of
    ``Execution.SHARED_THREAD`` at once, each on a thread of its own, and a
    free seat takes the next task at once. ``count`` is from 1 to
    ``MAX_SHARED_THREAD_CHILDREN``; ``seats`` is from 1 to
    ``MAX_SEATS_PER_SHARED_THREAD_CHILD``. Each child opens the sync
    connection pool ``pool``, which its seat threads share.

    Task code that sends through the app (``app.get_broker()``) opens a pool
    of up to ``task_send_pool`` connections in its child, with no overflow,
    for each event loop that it sends from. A sync send runs on the one loop
    of the broker, so the seat threads of a child share that pool.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['shared_thread_children'] = 'shared_thread_children'
    count: int = Field(
        ge=1,
        le=MAX_SHARED_THREAD_CHILDREN,
        description='Number of shared thread children',
    )
    seats: int = Field(
        ge=1,
        le=MAX_SEATS_PER_SHARED_THREAD_CHILD,
        description='Tasks that one child runs at once',
    )
    recycle: SharedRecycle = Field(default_factory=SharedRecycle)
    pool: SharedPool = Field(default_factory=SharedPool)
    task_send_pool: PoolSize = Field(default_factory=_default_task_send_pool)

    def resolved_pool(self) -> ResolvedPool:
        """The pool that each child opens. Its seat threads share it."""
        return self.pool.resolved(shared_thread_pool_connections(self.seats))


class NoSharedThreadChildren(BaseModel):
    """The profile starts no shared thread children."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_shared_thread_children'] = 'no_shared_thread_children'


SharedThreadSetting = Annotated[
    Union[SharedThreadChildren, NoSharedThreadChildren],
    Field(discriminator='kind'),
]


class ClaimLimits(BaseModel):
    """Limits on the tasks that the worker claims.

    The capacity of a worker is its number of seats (``seat_count``) plus
    ``AppConfig.prefetch_buffer``: it never holds more tasks than that. A
    limit above the capacity has no effect, and validation refuses it.

    - ``per_queue_per_pass``: the most tasks that one claim pass takes from
      one queue. With ``NoTaskLimit()``, one pass fills the free capacity.
    - ``claimed_per_worker``: the most CLAIMED tasks (claimed, not yet
      running) that the worker holds. With ``NoTaskLimit()``, the capacity
      is the limit. It counts the CLAIMED rows of every class, so rows that
      one class prefetched can stop the claims of another class whose seats
      are free.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    per_queue_per_pass: TaskLimitSetting = Field(default_factory=NoTaskLimit)
    claimed_per_worker: TaskLimitSetting = Field(default_factory=NoTaskLimit)


class ParentPool(BaseModel):
    """The connection pool of the worker process, the parent of the children.

    The worker process claims tasks and writes the outcome of each task
    through this pool; its claimer heartbeat, health and reaper loops share
    it. The runner heartbeats use a connection of their own. It
    holds at most ``max_size`` connections and has no overflow
    (``pool_size = max_size``, ``max_overflow = 0``), so the connection
    check counts it exactly.

    - ``max_size``: ``PoolSize(connections=N)``, or ``PoolSizeFromSeats()``
      to derive it from the seats of the profile: one connection for each
      ``PARENT_SEATS_PER_POOL_CONNECTION`` seats, from
      ``PARENT_POOL_CONNECTIONS_FLOOR`` to ``PARENT_POOL_CONNECTIONS_CAP``
      (``horsies.core.models.child_sizing``).
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    max_size: PoolSizeSetting = Field(default_factory=PoolSizeFromSeats)

    def resolved(self, from_seats: int) -> int:
        """The most connections of the pool. ``from_seats`` is the size that
        the profile derives from its seats."""
        match self.max_size:
            case PoolSize(connections=connections):
                return connections
            case PoolSizeFromSeats():
                return from_seats
            case _ as unreachable:
                assert_never(unreachable)


def _default_drain_timeout() -> timedelta:
    return DEFAULT_DRAIN_TIMEOUT


class WorkerProfile(BaseModel):
    """The settings of one worker.

    - ``isolated``: the isolated children.
    - ``shared_async``: the shared async children.
    - ``shared_thread``: the shared thread children. At least one child of
      any mode is required.
    - ``pool``: the connection pool of the worker process (``ParentPool``).
    - ``claim``: the claim limits.
    - ``drain_timeout``: how long a stopping worker waits for running tasks,
      above 0 and at most ``MAX_DRAIN_TIMEOUT``. At this deadline it kills
      the children that still run a task, and each of those tasks takes the
      crash path. Set it below the stop window of the supervisor.
    - ``preload``: what a Linux worker imports once before it forks
      children. Other platforms start every child with ``exec``.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    isolated: IsolatedSetting = Field(default_factory=NoIsolatedChildren)
    shared_async: SharedAsyncSetting = Field(default_factory=NoSharedAsyncChildren)
    shared_thread: SharedThreadSetting = Field(
        default_factory=NoSharedThreadChildren,
    )
    pool: ParentPool = Field(default_factory=ParentPool)
    claim: ClaimLimits = Field(default_factory=ClaimLimits)
    drain_timeout: timedelta = Field(
        default_factory=_default_drain_timeout,
        gt=timedelta(0),
        le=MAX_DRAIN_TIMEOUT,
    )
    preload: ChildPreload = ChildPreload.LIBRARY

    def child_count(self) -> int:
        """The number of children of the worker: the sum over the modes."""
        return (
            _isolated_count(self.isolated)
            + _shared_async_count(self.shared_async)
            + _shared_thread_count(self.shared_thread)
        )

    def seat_count(self) -> int:
        """The tasks that the children of the worker run at once: one per
        isolated child, and ``seats`` per shared child."""
        match self.shared_async:
            case SharedAsyncChildren(count=count, seats=seats):
                async_seats = count * seats
            case NoSharedAsyncChildren():
                async_seats = 0
            case _ as unreachable:
                assert_never(unreachable)
        return (
            _isolated_count(self.isolated)
            + async_seats
            + _shared_thread_seats(self.shared_thread)
        )

    def resolved_parent_pool(self) -> int:
        """The most connections of the pool of the worker process: the
        resolved ``pool.max_size``."""
        return self.pool.resolved(parent_pool_connections(self.seat_count()))


def _isolated_count(isolated: IsolatedChildren | NoIsolatedChildren) -> int:
    match isolated:
        case IsolatedChildren(count=count):
            return count
        case NoIsolatedChildren():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _shared_async_count(
    shared_async: SharedAsyncChildren | NoSharedAsyncChildren,
) -> int:
    match shared_async:
        case SharedAsyncChildren(count=count):
            return count
        case NoSharedAsyncChildren():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _shared_thread_count(
    shared_thread: SharedThreadChildren | NoSharedThreadChildren,
) -> int:
    match shared_thread:
        case SharedThreadChildren(count=count):
            return count
        case NoSharedThreadChildren():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _shared_thread_seats(
    shared_thread: SharedThreadChildren | NoSharedThreadChildren,
) -> int:
    match shared_thread:
        case SharedThreadChildren(count=count, seats=seats):
            return count * seats
        case NoSharedThreadChildren():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _describe_task_limit(limit: TaskLimit | NoTaskLimit) -> str:
    match limit:
        case TaskLimit(tasks=tasks):
            return f'{tasks} tasks'
        case NoTaskLimit():
            return 'no limit'
        case _ as unreachable:
            assert_never(unreachable)


def _describe_memory_limit(limit: MemoryLimit | NoMemoryLimit) -> str:
    match limit:
        case MemoryLimit(mb=mb):
            return f'{mb} MB'
        case NoMemoryLimit():
            return 'no limit'
        case _ as unreachable:
            assert_never(unreachable)


class _SizeOrigin(Enum):
    """Where a resolved size of a child comes from."""

    PROFILE = 'profile'
    SEATS = 'seats'


def _origin_text(origin: _SizeOrigin) -> str:
    match origin:
        case _SizeOrigin.PROFILE:
            return ''
        case _SizeOrigin.SEATS:
            return ', derived from seats'
        case _ as unreachable:
            assert_never(unreachable)


def _describe_pool(pool: ResolvedPool, origin: _SizeOrigin) -> str:
    """The resolved pool of one child, and where its max size comes from."""
    check = 'on' if pool.check else 'off'
    return (
        f'pool {pool.min_size} to {pool.max_size} connections'
        f'{_origin_text(origin)}, check {check}'
    )


def _pool_size_origin(max_size: PoolSize | PoolSizeFromSeats) -> _SizeOrigin:
    match max_size:
        case PoolSize():
            return _SizeOrigin.PROFILE
        case PoolSizeFromSeats():
            return _SizeOrigin.SEATS
        case _ as unreachable:
            assert_never(unreachable)


def _describe_task_send_pool(task_send_pool: PoolSize) -> str:
    return f'task_send_pool {task_send_pool.connections} connections'


def _describe_parent_pool(profile: WorkerProfile) -> str:
    """The resolved pool of the worker process, and where its size comes
    from."""
    connections = profile.resolved_parent_pool()
    match _pool_size_origin(profile.pool.max_size):
        case _SizeOrigin.PROFILE:
            return f'pool={connections} connections'
        case _SizeOrigin.SEATS:
            return (
                f'pool={connections} connections '
                f'(derived from {profile.seat_count()} seats)'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _executor_origin(
    executor: ExecutorThreads | ExecutorThreadsFromSeats,
) -> _SizeOrigin:
    match executor:
        case ExecutorThreads():
            return _SizeOrigin.PROFILE
        case ExecutorThreadsFromSeats():
            return _SizeOrigin.SEATS
        case _ as unreachable:
            assert_never(unreachable)


def _describe_isolated(isolated: IsolatedChildren | NoIsolatedChildren) -> str:
    match isolated:
        case IsolatedChildren(count=count, recycle=recycle):
            return (
                f'isolated={count} (recycle after '
                f'{_describe_task_limit(recycle.after)}, above '
                f'{_describe_memory_limit(recycle.above)}; '
                f'{_describe_pool(isolated.resolved_pool(), _SizeOrigin.PROFILE)}; '
                f'{_describe_task_send_pool(isolated.task_send_pool)})'
            )
        case NoIsolatedChildren():
            return 'isolated=none'
        case _ as unreachable:
            assert_never(unreachable)


def _describe_shared_async(
    shared_async: SharedAsyncChildren | NoSharedAsyncChildren,
) -> str:
    match shared_async:
        case SharedAsyncChildren(count=count, seats=seats, recycle=recycle):
            pool = _describe_pool(
                shared_async.resolved_pool(),
                _pool_size_origin(shared_async.pool.max_size),
            )
            return (
                f'shared_async={count}x{seats} seats (recycle after '
                f'{_describe_task_limit(recycle.after_per_seat)} per seat, '
                f'above {_describe_memory_limit(recycle.above)}; {pool}; '
                f'executor {shared_async.resolved_executor_threads()} threads'
                f'{_origin_text(_executor_origin(shared_async.executor))}; '
                f'{_describe_task_send_pool(shared_async.task_send_pool)})'
            )
        case NoSharedAsyncChildren():
            return 'shared_async=none'
        case _ as unreachable:
            assert_never(unreachable)


def _describe_shared_thread(
    shared_thread: SharedThreadChildren | NoSharedThreadChildren,
) -> str:
    match shared_thread:
        case SharedThreadChildren(count=count, seats=seats, recycle=recycle):
            pool = _describe_pool(
                shared_thread.resolved_pool(),
                _pool_size_origin(shared_thread.pool.max_size),
            )
            return (
                f'shared_thread={count}x{seats} seats (recycle after '
                f'{_describe_task_limit(recycle.after_per_seat)} per seat, '
                f'above {_describe_memory_limit(recycle.above)}; {pool}; '
                f'{_describe_task_send_pool(shared_thread.task_send_pool)})'
            )
        case NoSharedThreadChildren():
            return 'shared_thread=none'
        case _ as unreachable:
            assert_never(unreachable)


def describe_worker_profile(profile: WorkerProfile) -> str:
    """One line that states every setting of a profile, for logs."""
    return (
        f'{_describe_isolated(profile.isolated)}, '
        f'{_describe_shared_async(profile.shared_async)}, '
        f'{_describe_shared_thread(profile.shared_thread)}, '
        f'{_describe_parent_pool(profile)}, '
        f'claim per queue per pass: '
        f'{_describe_task_limit(profile.claim.per_queue_per_pass)}, '
        f'claimed per worker: '
        f'{_describe_task_limit(profile.claim.claimed_per_worker)}, '
        f'drain_timeout={profile.drain_timeout.total_seconds():g}s, '
        f'preload={profile.preload.value}'
    )


def profile_path(name: str) -> str:
    """The config path of a profile, as a user writes it in Python."""
    return f'worker_profiles[{name!r}]'


def _profile_error(
    message: str, notes: list[str], help_text: str
) -> ConfigurationError:
    return ConfigurationError(
        message=message,
        code=ErrorCode.CONFIG_INVALID_WORKER_PROFILE,
        notes=notes,
        help_text=help_text,
    )


def profile_name_errors(name: str) -> list[ConfigurationError]:
    """Refuse a name that a ``--profile`` argument cannot carry as it is."""
    if len(name) <= PROFILE_NAME_MAX_LENGTH and _PROFILE_NAME.fullmatch(name):
        return []
    return [
        _profile_error(
            f'{profile_path(name)}: the profile name is not valid',
            [
                f'a name has 1 to {PROFILE_NAME_MAX_LENGTH} characters: '
                "letters, digits, '_', '.' and '-'",
                "the first character is not '.' or '-'",
            ],
            'rename the profile, for example to "default" or "batch-high-memory"',
        ),
    ]


def profile_children_errors(
    name: str, profile: WorkerProfile
) -> list[ConfigurationError]:
    """Refuse a profile that starts no children."""
    if profile.child_count() >= 1:
        return []
    return [
        _profile_error(
            f'{profile_path(name)}: the profile starts no children',
            ['a worker needs at least one child to run a task'],
            f'set {profile_path(name)}.isolated, '
            f'{profile_path(name)}.shared_async or '
            f'{profile_path(name)}.shared_thread, for example '
            'isolated=IsolatedChildren(count=4)',
        ),
    ]


def _capacity_formula(profile: WorkerProfile, prefetch_buffer: int) -> str:
    """The capacity of a worker of this profile, term by term."""
    terms = [f'{_isolated_count(profile.isolated)} isolated children']
    match profile.shared_async:
        case SharedAsyncChildren(count=count, seats=seats):
            terms.append(f'{count} shared async children x {seats} seats')
        case NoSharedAsyncChildren():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match profile.shared_thread:
        case SharedThreadChildren(count=count, seats=seats):
            terms.append(f'{count} shared thread children x {seats} seats')
        case NoSharedThreadChildren():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    terms.append(f'prefetch_buffer {prefetch_buffer}')
    return 'capacity = ' + ' + '.join(terms)


def _claim_limit_error(
    path: str,
    tasks: int,
    profile: WorkerProfile,
    prefetch_buffer: int,
) -> ConfigurationError:
    capacity = profile.seat_count() + prefetch_buffer
    return _profile_error(
        f'{path}: {tasks} tasks is above the capacity of the worker ({capacity})',
        [
            _capacity_formula(profile, prefetch_buffer),
            'the worker never holds more tasks than its capacity, so this '
            'limit has no effect',
        ],
        f'set the limit to {capacity} or less, or use NoTaskLimit()',
    )


def profile_claim_errors(
    name: str,
    profile: WorkerProfile,
    prefetch_buffer: int,
) -> list[ConfigurationError]:
    """Refuse a claim limit above the capacity of the worker.

    ``prefetch_buffer`` is the validated ``AppConfig.prefetch_buffer``
    (0 or more). The capacity is the number of seats plus it.
    """
    capacity = profile.seat_count() + prefetch_buffer
    claim_path = f'{profile_path(name)}.claim'
    limits = {
        f'{claim_path}.per_queue_per_pass': profile.claim.per_queue_per_pass,
        f'{claim_path}.claimed_per_worker': profile.claim.claimed_per_worker,
    }
    errors: list[ConfigurationError] = []
    for path, limit in limits.items():
        match limit:
            case TaskLimit(tasks=tasks) if tasks > capacity:
                errors.append(
                    _claim_limit_error(path, tasks, profile, prefetch_buffer),
                )
            case TaskLimit() | NoTaskLimit():
                pass
            case _ as unreachable:
                assert_never(unreachable)
    return errors


def _pool_size_note(pool: SharedPool, seats: int, max_size: int) -> str:
    """Where the max size of a shared pool comes from."""
    match pool.max_size:
        case PoolSize(connections=connections):
            return f'max_size=PoolSize(connections={connections})'
        case PoolSizeFromSeats():
            return (
                f'max_size=PoolSizeFromSeats() resolves to {max_size} '
                f'connections for {seats} seats'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _pool_bounds_errors(
    path: str,
    pool: ResolvedPool,
    size_note: str,
) -> list[ConfigurationError]:
    """Refuse a pool whose ``min_size`` is above its resolved ``max_size``."""
    if pool.min_size <= pool.max_size:
        return []
    return [
        _profile_error(
            f'{path}.min_size: {pool.min_size} is above the pool max_size '
            f'({pool.max_size})',
            [
                size_note,
                'a pool keeps min_size connections open and never opens '
                'more than max_size',
            ],
            f'set min_size to {pool.max_size} or less, or set '
            f'max_size=PoolSize(connections={pool.min_size}) or more',
        ),
    ]


def profile_pool_errors(name: str, profile: WorkerProfile) -> list[ConfigurationError]:
    """Refuse a child pool whose ``min_size`` is above its resolved
    ``max_size``, for each mode with children."""
    errors: list[ConfigurationError] = []
    match profile.isolated:
        case IsolatedChildren(pool=pool) as isolated:
            errors.extend(
                _pool_bounds_errors(
                    f'{profile_path(name)}.isolated.pool',
                    isolated.resolved_pool(),
                    f'max_size=PoolSize(connections={pool.max_size.connections})',
                ),
            )
        case NoIsolatedChildren():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match profile.shared_async:
        case SharedAsyncChildren(pool=pool, seats=seats) as shared_async:
            resolved = shared_async.resolved_pool()
            errors.extend(
                _pool_bounds_errors(
                    f'{profile_path(name)}.shared_async.pool',
                    resolved,
                    _pool_size_note(pool, seats, resolved.max_size),
                ),
            )
        case NoSharedAsyncChildren():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match profile.shared_thread:
        case SharedThreadChildren(pool=pool, seats=seats) as shared_thread:
            resolved = shared_thread.resolved_pool()
            errors.extend(
                _pool_bounds_errors(
                    f'{profile_path(name)}.shared_thread.pool',
                    resolved,
                    _pool_size_note(pool, seats, resolved.max_size),
                ),
            )
        case NoSharedThreadChildren():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    return errors


def worker_profiles_shape_errors(
    profiles: Mapping[str, WorkerProfile],
) -> list[ConfigurationError]:
    """The name, children and pool errors of every profile, in name order."""
    errors: list[ConfigurationError] = []
    for name in sorted(profiles):
        errors.extend(profile_name_errors(name))
        errors.extend(profile_children_errors(name, profiles[name]))
        errors.extend(profile_pool_errors(name, profiles[name]))
    return errors


def worker_profiles_claim_errors(
    profiles: Mapping[str, WorkerProfile],
    prefetch_buffer: int,
) -> list[ConfigurationError]:
    """The claim errors of every profile with children, in name order.

    ``prefetch_buffer`` is the validated ``AppConfig.prefetch_buffer`` (0 or
    more). A profile with no children is skipped: the shape check already
    refuses it, and it has no capacity to compare with.
    """
    errors: list[ConfigurationError] = []
    for name in sorted(profiles):
        profile = profiles[name]
        if profile.child_count() >= 1:
            errors.extend(profile_claim_errors(name, profile, prefetch_buffer))
    return errors
