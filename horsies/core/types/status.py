# core/types/status.py
"""
Core types and enums used throughout the application.
This module should not import from other application modules.
"""

from enum import Enum


class TaskStatus(Enum):
    """Task execution status"""

    PENDING = 'pending'  # It awaits to be a candidate for execution.
    # Default status when the task is sent.

    CLAIMED = (
        'claimed'  # It has been claimed by a worker but not yet started executing.
    )

    RUNNING = 'running'  # It is being executed by a process.

    COMPLETED = 'completed'  # It has been executed successfully.

    FAILED = 'failed'  # It has failed to be executed.
    CANCELLED = 'cancelled'  # It has been cancelled.
    REQUEUED = 'requeued'  # It has been requeued after a failure.

    @property
    def is_terminal(self) -> bool:
        """Whether this status represents a final state (no further transitions)."""
        return self in TASK_TERMINAL_STATES


TASK_TERMINAL_STATES: frozenset[TaskStatus] = frozenset({
    TaskStatus.COMPLETED,
    TaskStatus.FAILED,
    TaskStatus.CANCELLED,
})
