"""The default sizes of the connection pool and the default executor of each
child kind.

An isolated child runs one task at a time, so its pool has a fixed default
size. A shared child derives a size from its seats when its profile sets
``PoolSizeFromSeats()`` or ``ExecutorThreadsFromSeats()``, the defaults:

    size = min(cap, max(floor, ceil(seats / seats_per_unit)))

Each derived size has its own ``seats_per_unit`` and ``cap``. Every constant
that decides a default size is in this module. The module is pure.
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
