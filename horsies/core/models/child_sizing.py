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
"""

from __future__ import annotations

from typing import Final

DEFAULT_ISOLATED_POOL_CONNECTIONS: Final = 2
"""The most connections in the pool of an isolated child when its profile
does not say."""

POOL_CONNECTIONS_FLOOR: Final = 2
"""The fewest connections of a pool size derived from the seats."""

SHARED_ASYNC_SEATS_PER_POOL_CONNECTION: Final = 10
"""Seats of a shared async child per connection of a derived pool size."""

SHARED_ASYNC_POOL_CONNECTIONS_CAP: Final = 10
"""The most connections of a pool size derived from the seats of a shared
async child."""

SHARED_THREAD_SEATS_PER_POOL_CONNECTION: Final = 4
"""Seats of a shared thread child per connection of a derived pool size."""

SHARED_THREAD_POOL_CONNECTIONS_CAP: Final = 10
"""The most connections of a pool size derived from the seats of a shared
thread child."""

PARENT_POOL_CONNECTIONS_FLOOR: Final = 5
"""The fewest connections of a parent pool size derived from the seats of
the worker. Provisional: a measurement of the finalize rate sets it."""

PARENT_SEATS_PER_POOL_CONNECTION: Final = 10
"""Seats of the worker per connection of a derived parent pool size.
Provisional: a measurement of the finalize rate sets it."""

PARENT_POOL_CONNECTIONS_CAP: Final = 20
"""The most connections of a parent pool size derived from the seats of the
worker. Provisional: a measurement of the finalize rate sets it."""

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
