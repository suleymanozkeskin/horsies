# showcase/hemline/tasks/__init__.py
"""Hemline's task modules.

Every task here is keyword-only, returns `TaskResult`, and reports database
trouble through one shared conversion so the dashboard shows the failing
operation instead of a stack trace.
"""

from __future__ import annotations

from horsies import TaskError

from ..domain import STORE_UNAVAILABLE
from ..store import StoreError


def store_failure(error: StoreError) -> TaskError:
    """Convert a database failure into a task error the dashboard can read."""
    return TaskError(
        error_code=STORE_UNAVAILABLE,
        message=f'{error.operation} failed: {error.message}',
        data={'operation': error.operation},
    )
