# core/types/status.py
"""
Core types and enums used throughout the application.
This module imports no other application module but the generated catalogs
(`horsies._catalog`), which import nothing from the package.
"""

from enum import Enum

from horsies._catalog.status import TaskAttemptOutcome as TaskAttemptOutcome


class TaskStatus(Enum):
    """Task execution status"""

    PENDING = 'PENDING'  # It awaits to be a candidate for execution.
    # Default status when the task is sent.

    CLAIMED = (
        'CLAIMED'  # It has been claimed by a worker but not yet started executing.
    )

    RUNNING = 'RUNNING'  # It is being executed by a process.

    COMPLETED = 'COMPLETED'  # It has been executed successfully.

    FAILED = 'FAILED'  # It has failed to be executed.
    CANCELLED = 'CANCELLED'  # It has been cancelled.
    EXPIRED = 'EXPIRED'  # It was never claimed before good_until passed.

    @property
    def is_terminal(self) -> bool:
        """Whether this status represents a final state (no further transitions)."""
        return self in TASK_TERMINAL_STATES


TASK_TERMINAL_STATES: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.EXPIRED,
    }
)
