"""The core handle of a child worker process.

Each child builds one core handle from its init frame
(``_initialize_child_core_handle``, ``CoreChildHandle.from_init``): the
database URL, the connection settings and the pool of the child kind stay
in the core. Every write of the child goes through it: the start of each
run, its miss path and the finalizing marker. The functions are private to
the child runtime: ``child_runner`` and ``shared_lifecycle`` call them, and
tests drop the handle with ``_drop_child_core_handle``.
"""

from __future__ import annotations

from horsies import _core as core
from horsies._core import CoreChildHandle


# ---------- Per-process core handle (built in child processes) ----------
_child_core_handle: CoreChildHandle | None = None


def _get_child_core_handle() -> CoreChildHandle:  # pyright: ignore[reportUnusedFunction]
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


def _initialize_child_core_handle(init: core.InitFrame) -> None:  # pyright: ignore[reportUnusedFunction]
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


def _drop_child_core_handle() -> None:  # pyright: ignore[reportUnusedFunction]
    """Forget the core handle of this process, so that a test can build one
    for another database. Closes nothing: the pools of the handle close when
    the core drops it."""
    global _child_core_handle
    _child_core_handle = None


