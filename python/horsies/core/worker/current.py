# app/core/worker/current.py
from __future__ import annotations
from typing import Optional
from horsies.core.app import Horsies

_current_app: Optional[Horsies] = None


def set_current_app(app: Horsies) -> None:
    global _current_app
    _current_app = app


def get_current_app() -> Horsies:
    """Return the app bound to this process.

    Raises:
        RuntimeError: no app has been set — called outside a worker child
            or before the child initializer ran. Intentional usage guard,
            not a recoverable condition; inside ``_run_task_entry`` it is
            folded into a WORKER_RESOLUTION_ERROR wire outcome.
    """
    if _current_app is None:
        raise RuntimeError('No current app set in this process')
    return _current_app
