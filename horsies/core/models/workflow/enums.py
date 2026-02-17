"""Workflow status enums and type variables."""

from __future__ import annotations

from enum import Enum
from typing import TypeVar

# TypeVar for TaskNode generic parameter (the "ok" type of TaskResult)
OkT = TypeVar('OkT')
OkT_co = TypeVar('OkT_co', covariant=True)
OutT = TypeVar('OutT')


# =============================================================================
# Enums
# =============================================================================


class WorkflowStatus(str, Enum):
    """
    Status of a workflow instance.

    State machine:
        PENDING → RUNNING → COMPLETED
                         → FAILED (on task failure with on_error=FAIL)
                         → PAUSED (on task failure with on_error=PAUSE)
                         → CANCELLED (user requested)
    """

    PENDING = 'PENDING'
    """Created but not yet started"""

    RUNNING = 'RUNNING'
    """At least one task executing or ready"""

    COMPLETED = 'COMPLETED'
    """All tasks terminal and success criteria met"""

    FAILED = 'FAILED'
    """A task failed and on_error=FAIL (or no success case satisfied)"""

    PAUSED = 'PAUSED'
    """A task failed with on_error=PAUSE; awaiting resume() or cancel()"""

    CANCELLED = 'CANCELLED'
    """User cancelled via WorkflowHandle.cancel()"""

    @property
    def is_terminal(self) -> bool:
        """Whether this status represents a final state (no further transitions)."""
        return self in WORKFLOW_TERMINAL_STATES


WORKFLOW_TERMINAL_STATES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.COMPLETED,
        WorkflowStatus.FAILED,
        WorkflowStatus.CANCELLED,
    }
)


class WorkflowTaskStatus(str, Enum):
    """
    Status of a single task/node within a workflow.

    State machine:
        PENDING → READY → ENQUEUED → RUNNING → COMPLETED
                                             → FAILED
                       → SKIPPED (deps failed and allow_failed_deps=False)
    """

    PENDING = 'PENDING'
    """Waiting for dependencies to become terminal"""

    READY = 'READY'
    """Dependencies satisfied, waiting to be enqueued"""

    ENQUEUED = 'ENQUEUED'
    """Task created in tasks table, waiting for worker"""

    RUNNING = 'RUNNING'
    """Worker is executing the task (or child workflow is running)"""

    COMPLETED = 'COMPLETED'
    """Task succeeded (TaskResult.is_ok())"""

    FAILED = 'FAILED'
    """Task failed (TaskResult.is_err())"""

    SKIPPED = 'SKIPPED'
    """Skipped due to: upstream failure, condition, or quorum impossible"""

    @property
    def is_terminal(self) -> bool:
        """Whether this status represents a final state (no further transitions)."""
        return self in WORKFLOW_TASK_TERMINAL_STATES


WORKFLOW_TASK_TERMINAL_STATES: frozenset[WorkflowTaskStatus] = frozenset(
    {
        WorkflowTaskStatus.COMPLETED,
        WorkflowTaskStatus.FAILED,
        WorkflowTaskStatus.SKIPPED,
    }
)

WF_TASK_TERMINAL_VALUES: list[str] = [s.value for s in WORKFLOW_TASK_TERMINAL_STATES]


class OnError(str, Enum):
    """
    Error handling policy for workflows when a task fails.
    """

    FAIL = 'fail'
    """Continue DAG resolution but mark workflow as will-fail. Skip tasks without allow_failed_deps."""

    PAUSE = 'pause'
    """Pause workflow immediately. No new tasks enqueued until resume()."""


class SubWorkflowRetryMode(str, Enum):
    """
    Retry behavior for subworkflows (only RERUN_FAILED_ONLY currently supported).
    """

    RERUN_FAILED_ONLY = 'rerun_failed_only'
    """Re-run only failed/cancelled child tasks (default, only supported mode)"""

    RERUN_ALL = 'rerun_all'
    """Re-run entire child workflow from scratch (not yet implemented)"""

    NO_RERUN = 'no_rerun'
    """Re-evaluate success policy without re-running (not yet implemented)"""
