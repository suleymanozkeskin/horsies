"""Per-process connection pool for child worker processes."""

from __future__ import annotations

import atexit
from collections.abc import Mapping

from psycopg_pool import ConnectionPool


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
    check_on_checkout: bool = True,
) -> None:
    """
    Initialize the per-process connection pool.

    In production: Called by _child_initializer in spawned worker processes.
    In tests: Can be called directly to set up the pool for direct _run_task_entry calls.

    Args:
        connect_kwargs: psycopg connect kwargs for each pooled connection
            (TCP keepalives, plus prepare_threshold=None under PgBouncer
            transaction mode). Built once from
            ``PostgresConfig.pooled_connect_args``.

    Raises:
        ValueError: invalid pool sizing.
        Exception: conninfo parse errors raise from the constructor — in
            production out of the child initializer, killing the child.
            Connection failures do NOT raise here: ``open=True`` starts the
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
    _worker_pool = ConnectionPool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        max_lifetime=300.0,
        check=check,
        open=True,
        kwargs=dict(connect_kwargs) if connect_kwargs else {},
    )
    atexit.register(_cleanup_worker_pool)


_CHILD_POOL_API = (_get_worker_pool, _initialize_worker_pool)
