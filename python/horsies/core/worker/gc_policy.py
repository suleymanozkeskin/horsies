"""Garbage collection of the worker process after its start."""

from __future__ import annotations

import gc


def freeze_startup_objects() -> None:
    """Collect garbage, then move every object that exists now to the
    permanent generation (``gc.freeze``).

    The objects that the start of the process made (modules, the app, its
    tasks, pools, children) live as long as the process. Later collections do
    not scan frozen objects, so the pauses of the oldest generation stay short
    while the event loop runs. The collection runs first, so no garbage of the
    start is frozen. Call it once, when the start of the process is complete.
    Does no I/O and cannot fail.
    """
    gc.collect()
    gc.freeze()
