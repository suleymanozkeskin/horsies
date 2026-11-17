"""The default sizes of the connection pools of a worker and of the default
executor of each child kind.

An isolated child runs one task at a time, so its pool has a fixed default
size. A shared child derives a size from its seats when its profile sets
``PoolSizeFromSeats()`` or ``ExecutorThreadsFromSeats()``, the defaults. The
worker process derives the size of its own pool from the seats of all its
children when the profile sets ``ParentPool(max_size=PoolSizeFromSeats())``,
the default:

    size = min(cap, max(floor, ceil(seats / seats_per_unit)))

Each derived size has its own ``seats_per_unit``, ``floor`` and ``cap``. The
pool that task code opens in a child when it sends has a fixed default size.
Every constant that decides a default size is in this module. The module is
pure.

The worker process opens two pools of the parent pool size: the pool of its
engine and the pool of its core handle. The parent pool has a minimum,
``PARENT_POOL_MIN_CONNECTIONS``: one connection of the engine pool, which the
claim loop holds under load, and ``PARENT_POOL_RESERVE`` for the other users
of each pool. The flushers of the batched success write check out connections
of the core pool. AppConfig validation refuses an explicit size below the
minimum. A derived size is at least ``PARENT_POOL_CONNECTIONS_FLOOR``, which
is above the minimum.

A shared child writes 2 statements per task through its pool (the RUNNING
update and the finalizing marker), each one round trip in autocommit. On
average its pool keeps this many connections busy:

    2 x tasks per second x (RTT + server time + the delay of the child)

A seat whose task takes 0.1 s, 3.5 ms from Postgres, keeps about one
connection in 12 busy, so one connection per 10 seats leaves room for bursts.
A shared async child with many seats is CPU-bound, and the delay of its event
loop then grows the time of each write; its cap is the pool size where its
rate stops rising.

The measured values in the docstrings below come from one host (Apple M2,
8 cores) and tasks that sleep 0.1 s. Postgres 17 ran in a 4-vCPU VM, 0.25 ms
away or 3.5 ms more (netem), unless a docstring names Postgres 18 on the host
itself.
"""

from __future__ import annotations

from typing import Final

DEFAULT_ISOLATED_POOL_CONNECTIONS: Final = 1
"""The most connections in the pool of an isolated child when its profile
does not say. An isolated child runs one task at a time, and its two writes
run one after the other, so it never uses a second connection: 14 isolated
children gave the same rate with 1 and with 2 (106 tasks/s at 3.5 ms)."""

POOL_CONNECTIONS_FLOOR: Final = 2
"""The fewest connections of a pool size derived from the seats."""

SHARED_ASYNC_SEATS_PER_POOL_CONNECTION: Final = 10
"""Seats of a shared async child per connection of a derived pool size.
100 seats at 3.5 ms: 10 connections give 790-810 tasks/s, as 20 do; 5 give
570-590."""

SHARED_ASYNC_POOL_CONNECTIONS_CAP: Final = 40
"""The most connections of a pool size derived from the seats of a shared
async child. 1000 seats, at 0.25 ms and at 3.5 ms: 40 connections give
1,690-1,760 tasks/s, 80 give 1,700-1,730, 20 give 1,290-1,570 and 10 give
970-1,350. The child is CPU-bound from 20 connections up."""

SHARED_THREAD_SEATS_PER_POOL_CONNECTION: Final = 10
"""Seats of a shared thread child per connection of a derived pool size.
24 seats: 2 connections give the rate of 6 (181-191 tasks/s at 3.5 ms).
64 seats: 5 give the rate of 10 and 20 (503-520 tasks/s at 3.5 ms)."""

SHARED_THREAD_POOL_CONNECTIONS_CAP: Final = 26
"""The most connections of a pool size derived from the seats of a shared
thread child: the size that the most seats (256) derive, so the rule does
not cut a thread pool below one connection per 10 seats. 256 seats, Postgres
18 on the host itself, one run each: 26 connections give 1,750 tasks/s, 10
give 1,660, 40 give 1,520."""

FINALIZE_BATCH_FLUSHERS: Final = 2
"""The steady flushers of the batched success write (``FinalizeBatcher``):
they write whenever rows wait. Each one runs one statement at a time through
the worker's core handle, whose pool has the size of the parent pool."""

PARENT_POOL_CLAIM_CONNECTIONS: Final = 1
"""The connection of the engine pool of the worker process that the claim
loop holds while it claims (``HeldConnection``)."""

PARENT_POOL_RESERVE: Final = 2
"""Connections of each pool of the worker process that stay free for its
other users: the claim loop leaves them free on the engine pool, and the
flushers of the batched success write leave them free on the core pool
(``parent_pool_flusher_bound``). Under load the held connections stay held,
so the other users get only these.

On the engine pool the reaper holds the most at once, 2:

1. its gate session (``ReaperMixin._reaper_loop``), for the whole pass;
2. one checkout of the pass body (``_run_reaper_pass``) at a time.

Each other user of the engine pool holds one connection for one statement or
one transaction, and waits in the pool queue while both are taken: the
claimer heartbeat, the worker-state snapshot, the ping reply, the workflow
and class checks of a claim pass (beside the connection of the claim loop),
and for each task its phase 2, parent propagation and retry NOTIFY writes.

On the core pool each write that runs in the core holds one connection for
one statement: for each task its failure, retry, workflow success and claim
release writes, and the stale-task recovery of a reaper pass. The core pool
of a worker process keeps the same count free from its batch checkouts.

The runner heartbeats and the listener have connections of their own."""

PARENT_POOL_MIN_CONNECTIONS: Final = PARENT_POOL_CLAIM_CONNECTIONS + PARENT_POOL_RESERVE
"""The fewest connections of the parent pool: the claim loop and
``PARENT_POOL_RESERVE`` on the engine pool. On the core pool, which has the
same size, it runs one flusher of the batched success write beside
``PARENT_POOL_RESERVE``. AppConfig validation refuses an explicit
``ParentPool`` size below it (HRS-218)."""

PARENT_POOL_CONNECTIONS_FLOOR: Final = 5
"""The fewest connections of a parent pool size derived from the seats of
the worker. It is above ``PARENT_POOL_MIN_CONNECTIONS``: at 5, the core pool
runs the ``FINALIZE_BATCH_FLUSHERS`` steady flushers and one surge flusher
beside ``PARENT_POOL_RESERVE`` (``parent_pool_flusher_bound``), and the
engine pool has 2 connections beside the claim loop and the reserve."""

PARENT_SEATS_PER_POOL_CONNECTION: Final = 10
"""Seats of the worker per connection of a derived parent pool size. A
plain success is written in batches and keeps few connections busy: one
worker of 1000 seats used at most 3 to 8 at 970-1,760 tasks/s. A failure is
written per task with 2 statements, so a worker whose tasks fail needs about
2 x failures per second x (RTT + server time) connections."""

PARENT_POOL_CONNECTIONS_CAP: Final = 20
"""The most connections of a parent pool size derived from the seats of the
worker. 1000 seats whose tasks all fail, at 3.5 ms: 20 connections give
1,010 tasks/s, all 20 busy; 5 give 500. 4 shared async children of 250
seats at 3.5 ms: 20 give 1,740 tasks/s with at most 14 busy; 5 give
1,510-1,640."""

DEFAULT_TASK_SEND_POOL_CONNECTIONS: Final = 2
"""The connections of the pool that task code opens in a child when it sends,
for each event loop that it sends from, when the profile does not say."""

EXECUTOR_THREADS_FLOOR: Final = 2
"""The fewest threads of a default executor size derived from the seats."""

SHARED_ASYNC_SEATS_PER_EXECUTOR_THREAD: Final = 1
"""Seats of a shared async child per thread of a derived default executor
size."""

SHARED_ASYNC_EXECUTOR_THREADS_CAP: Final = 32
"""The most threads of a default executor size derived from the seats of a
shared async child: the cap of the asyncio default executor."""


def size_from_seats(seats: int, seats_per_unit: int, floor: int, cap: int) -> int:
    """``min(cap, max(floor, ceil(seats / seats_per_unit)))``.

    ``seats`` and ``seats_per_unit`` are 1 or more, and ``floor`` is at most
    ``cap``. Another input is a defect: ``AssertionError``.
    """
    assert seats >= 1, f'seats must be >= 1; got {seats}'
    assert seats_per_unit >= 1, f'seats_per_unit must be >= 1; got {seats_per_unit}'
    assert 1 <= floor <= cap, f'need 1 <= floor <= cap; got {floor} and {cap}'
    units = (seats + seats_per_unit - 1) // seats_per_unit
    return min(cap, max(floor, units))


def shared_async_pool_connections(seats: int) -> int:
    """The pool size that a shared async child with ``seats`` seats derives."""
    return size_from_seats(
        seats,
        SHARED_ASYNC_SEATS_PER_POOL_CONNECTION,
        POOL_CONNECTIONS_FLOOR,
        SHARED_ASYNC_POOL_CONNECTIONS_CAP,
    )


def shared_thread_pool_connections(seats: int) -> int:
    """The pool size that a shared thread child with ``seats`` seats derives."""
    return size_from_seats(
        seats,
        SHARED_THREAD_SEATS_PER_POOL_CONNECTION,
        POOL_CONNECTIONS_FLOOR,
        SHARED_THREAD_POOL_CONNECTIONS_CAP,
    )


def shared_async_executor_threads(seats: int) -> int:
    """The default executor size that a shared async child with ``seats``
    seats derives."""
    return size_from_seats(
        seats,
        SHARED_ASYNC_SEATS_PER_EXECUTOR_THREAD,
        EXECUTOR_THREADS_FLOOR,
        SHARED_ASYNC_EXECUTOR_THREADS_CAP,
    )


def parent_pool_connections(seats: int) -> int:
    """The parent pool size that a worker with ``seats`` seats derives.

    ``seats`` is the seat count of the profile, 0 or more. A profile with no
    children, which AppConfig validation refuses, has 0 seats and gets
    ``PARENT_POOL_CONNECTIONS_FLOOR``. A negative input is a defect:
    ``AssertionError``.
    """
    assert seats >= 0, f'seats must be >= 0; got {seats}'
    if seats == 0:
        return PARENT_POOL_CONNECTIONS_FLOOR
    return size_from_seats(
        seats,
        PARENT_SEATS_PER_POOL_CONNECTION,
        PARENT_POOL_CONNECTIONS_FLOOR,
        PARENT_POOL_CONNECTIONS_CAP,
    )


def parent_pool_flusher_bound(connections: int) -> int:
    """The most flushers of the batched success write, steady and surge
    together, on a parent pool of ``connections`` connections: the core pool
    of the worker, which has that size, less ``PARENT_POOL_RESERVE``. The
    claim loop holds a connection of the engine pool, not of the core pool.
    The core pool keeps the same count free from its batch checkouts, so the
    flushers fill its batch checkouts and no more.

    ``connections`` is at least ``PARENT_POOL_MIN_CONNECTIONS``: AppConfig
    validation refuses a smaller explicit size, and a derived size is at
    least the floor. A smaller value is a defect: ``AssertionError``.
    """
    assert connections >= PARENT_POOL_MIN_CONNECTIONS, (
        f'a parent pool of {connections} connections is below the minimum '
        f'of {PARENT_POOL_MIN_CONNECTIONS}'
    )
    return connections - PARENT_POOL_RESERVE
