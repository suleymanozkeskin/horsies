"""Per-process connection pools for child worker processes.

An isolated child and a shared thread child open a sync pool
(``_initialize_worker_pool``); the seat threads of a shared thread child share
it. A shared async child opens an async pool on its event loop instead
(``open_shared_async_pool``): its lifecycle writes must not block the loop.
Each has the bounds of the ``pool`` of the child kind in the worker profile
(``IsolatedChildren.pool``, ``SharedAsyncChildren.pool``,
``SharedThreadChildren.pool``), which the parent sends in the init frame.

Every connection of these pools is in autocommit: a statement is its own
transaction, and a one-statement write is one round trip, with no BEGIN and
no COMMIT. A write that needs several statements to commit together opens a
transaction itself with an explicit ``BEGIN`` and ends it with ``commit()``
or ``rollback()`` (``child_runner``, ``shared_lifecycle``).
"""

from __future__ import annotations

import atexit
from collections.abc import Mapping
from typing import Any, Final

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from horsies.core.models.boot_failure import ThreadStartRefusedError

CHILD_POOL_MAX_LIFETIME_S: Final = 300.0
"""Each pooled connection of a child is replaced after this time."""

CHILD_POOL_CONNECTION_WAIT_S: Final = 30.0
"""The longest wait of a lifecycle write of a child for a connection of its
sync pool: the default wait of a psycopg pool. The seat threads of a shared
thread child can wait for each other here."""

SHARED_POOL_CONNECTION_WAIT_S: Final = CHILD_POOL_CONNECTION_WAIT_S
"""The longest wait of a lifecycle write of a shared async child for a pooled
connection: the same wait as for the sync pool."""


def autocommit_connect_kwargs(
    connect_kwargs: Mapping[str, object] | None,
) -> dict[str, object]:
    """The psycopg connect kwargs of a child pool: ``connect_kwargs`` with
    autocommit on. Does no I/O."""
    return {**(connect_kwargs or {}), 'autocommit': True}


# ---------- Per-process connection pool (initialized in child processes) ----------
_worker_pool: ConnectionPool | None = None


def _get_worker_pool() -> ConnectionPool:
    """Get the per-process connection pool. Raises if not initialized."""
    if _worker_pool is None:
        raise RuntimeError(
            'Worker connection pool not initialized. '
            'This function must be called from a child worker process.'
        )
    return _worker_pool


def _cleanup_worker_pool() -> None:
    """Clean up the connection pool on process exit."""
    global _worker_pool
    if _worker_pool is not None:
        try:
            _worker_pool.close()
        except Exception:
            pass
        _worker_pool = None


def _initialize_worker_pool(
    database_url: str,
    *,
    connect_kwargs: Mapping[str, object] | None = None,
    min_size: int = 0,
    max_size: int = 2,
    check_on_checkout: bool = False,
) -> None:
    """
    Initialize the per-process connection pool.

    In production: Called by _child_initializer in spawned worker processes.
    In tests: Can be called directly to set up the pool for direct _run_task_entry calls.

    Args:
        connect_kwargs: psycopg connect kwargs for each pooled connection
            (TCP keepalives, plus prepare_threshold=None under PgBouncer
            transaction mode). Built once from
            ``PostgresConfig.pooled_connect_args``. The pool adds
            ``autocommit=True``.
        check_on_checkout: check each connection when a write takes it
            from the pool, one round trip each. Off by default, as the
            ``check`` of ``IsolatedPool`` and ``SharedPool``.

    Raises:
        ValueError: invalid pool sizing.
        ThreadStartRefusedError: the OS refused a thread of the pool. The
            boot names the cause ``ThreadStartRefused``.
        Exception: conninfo parse errors raise from the constructor — in
            production out of the child initializer, killing the child.
            Connection failures do NOT raise here: the open starts the
            pool without connecting (min_size defaults to 0), so an
            unreachable database surfaces at first acquisition as a
            PoolTimeout inside task pre-flight / heartbeat containment.
    """
    global _worker_pool
    if _worker_pool is not None:
        return  # Already initialized
    if min_size < 0:
        raise ValueError('min_size must be >= 0')
    if max_size < 1:
        raise ValueError('max_size must be >= 1')
    if min_size > max_size:
        raise ValueError('min_size must be <= max_size')
    # check_on_checkout=False skips the per-checkout health-check round
    # trip (a full RTT on remote links); callers' statement-level error
    # containment then absorbs the rare stale connection instead.
    check = ConnectionPool.check_connection if check_on_checkout else None
    pool: ConnectionPool = ConnectionPool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        max_lifetime=CHILD_POOL_MAX_LIFETIME_S,
        check=check,
        open=False,
        timeout=CHILD_POOL_CONNECTION_WAIT_S,
        kwargs=autocommit_connect_kwargs(connect_kwargs),
    )
    # The open starts the threads of the pool, and starts nothing else that
    # raises RuntimeError: such an error is a refused thread start.
    try:
        pool.open()
    except RuntimeError as exc:
        raise ThreadStartRefusedError('a thread of the connection pool', exc) from exc
    _worker_pool = pool
    atexit.register(_cleanup_worker_pool)


async def open_shared_async_pool(
    database_url: str,
    *,
    connect_kwargs: Mapping[str, object],
    min_size: int,
    max_size: int,
    check_on_checkout: bool,
) -> AsyncConnectionPool[AsyncConnection[Any]]:
    """Open the async pool of a shared async child, on the running loop.

    The sizes, the checkout check and autocommit are those of the sync pool
    of an isolated child. Opens without a connection when ``min_size`` is 0; an
    unreachable database shows at the first write, inside its containment.

    Raises:
        ValueError: invalid pool sizing.
        Exception: a conninfo that does not parse raises from the
            constructor; the caller fails the boot with it.
    """
    if min_size < 0:
        raise ValueError('min_size must be >= 0')
    if max_size < 1:
        raise ValueError('max_size must be >= 1')
    if min_size > max_size:
        raise ValueError('min_size must be <= max_size')
    check = AsyncConnectionPool.check_connection if check_on_checkout else None
    pool: AsyncConnectionPool[AsyncConnection[Any]] = AsyncConnectionPool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        max_lifetime=CHILD_POOL_MAX_LIFETIME_S,
        check=check,
        open=False,
        timeout=SHARED_POOL_CONNECTION_WAIT_S,
        kwargs=autocommit_connect_kwargs(connect_kwargs),
    )
    await pool.open(wait=False)
    return pool


_CHILD_POOL_API = (_get_worker_pool, _initialize_worker_pool)
