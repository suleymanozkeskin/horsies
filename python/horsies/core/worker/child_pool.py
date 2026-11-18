"""The core handle of a child worker process.

Each child builds one core handle from its init frame
(``_initialize_child_core_handle``, ``CoreChildHandle.from_init``): the
database URL, the connection settings and the pool of the child kind stay
in the core. Every write of the child goes through it: the start of each
run, its miss path and the finalizing marker.

``_initialize_worker_pool`` opens the psycopg pool of a version-1 child. No
child of this package opens it: only the Python worker parent module
(``worker``) imports the name, which goes with that module.
"""

from __future__ import annotations

import atexit
from collections.abc import Mapping
from typing import Final

from psycopg_pool import ConnectionPool

from horsies import _core as core
from horsies._core import CoreChildHandle
from horsies.core.models.boot_failure import ThreadStartRefusedError

CHILD_POOL_MAX_LIFETIME_S: Final = 300.0
"""Each pooled connection of a child is replaced after this time."""

CHILD_POOL_CONNECTION_WAIT_S: Final = 30.0
"""The longest wait for a connection of the pool of
``_initialize_worker_pool``: the default wait of a psycopg pool."""


def autocommit_connect_kwargs(
    connect_kwargs: Mapping[str, object] | None,
) -> dict[str, object]:
    """The psycopg connect kwargs of a child pool: ``connect_kwargs`` with
    autocommit on. Does no I/O."""
    return {**(connect_kwargs or {}), 'autocommit': True}


# ---------- The psycopg pool of a version-1 child ----------
_worker_pool: ConnectionPool | None = None


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
    Initialize the psycopg pool of a version-1 child. No child of this
    package calls it; see the module docstring.

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


# ---------- Per-process core handle (built in child processes) ----------
_child_core_handle: CoreChildHandle | None = None


def _get_child_core_handle() -> CoreChildHandle:
    """The core handle of this child process.

    Raises:
        RuntimeError: the process built no handle; a defect of the caller,
            which runs outside a child worker process.
    """
    if _child_core_handle is None:
        raise RuntimeError(
            'The core handle of the child is not built. '
            'This function must be called from a child worker process.'
        )
    return _child_core_handle


def _initialize_child_core_handle(init: core.InitFrame) -> None:
    """Build the core handle of this child process from its init frame
    (``CoreChildHandle.from_init``): the database URL, the connection
    settings and the pool of the child kind stay in the core. The handle
    keeps ``min_size`` connections open and, with ``check``, pings each
    connection at checkout. Building it starts nothing:
    the first call starts the core of this process and connects. A process
    that has a handle keeps it."""
    global _child_core_handle
    if _child_core_handle is not None:
        return
    _child_core_handle = CoreChildHandle.from_init(init)


def _drop_child_core_handle() -> None:
    """Forget the core handle of this process, so that a test can build one
    for another database. Closes nothing: the pools of the handle close when
    the core drops it."""
    global _child_core_handle
    _child_core_handle = None


_CHILD_POOL_API = (
    _initialize_worker_pool,
    _get_child_core_handle,
    _initialize_child_core_handle,
    _drop_child_core_handle,
)
