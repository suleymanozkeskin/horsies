"""Per-process connection pool for child worker processes."""

from __future__ import annotations

import atexit

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


def _initialize_worker_pool(database_url: str) -> None:
    """
    Initialize the per-process connection pool.

    In production: Called by _child_initializer in spawned worker processes.
    In tests: Can be called directly to set up the pool for direct _run_task_entry calls.
    """
    global _worker_pool
    if _worker_pool is not None:
        return  # Already initialized
    _worker_pool = ConnectionPool(
        database_url,
        min_size=1,
        max_size=5,
        max_lifetime=300.0,
        check=ConnectionPool.check_connection,
        open=True,
    )
    atexit.register(_cleanup_worker_pool)


_CHILD_POOL_API = (_get_worker_pool, _initialize_worker_pool)
