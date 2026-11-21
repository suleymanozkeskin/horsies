"""Workflow status enums and type variables.

`WorkflowStatus`, `WorkflowTaskStatus` and their terminal-state constants
are the classes generated from horsies-core (`horsies._catalog.status`).
"""

from __future__ import annotations

from enum import Enum
from typing import TypeVar

from horsies._catalog.status import (
    WORKFLOW_TASK_TERMINAL_STATES as WORKFLOW_TASK_TERMINAL_STATES,
)
from horsies._catalog.status import (
    WORKFLOW_TERMINAL_STATES as WORKFLOW_TERMINAL_STATES,
)
from horsies._catalog.status import WorkflowStatus as WorkflowStatus
from horsies._catalog.status import WorkflowTaskStatus as WorkflowTaskStatus

# TypeVar for TaskNode generic parameter (the "ok" type of TaskResult)
OkT = TypeVar('OkT')
OkT_co = TypeVar('OkT_co', covariant=True)
OutT = TypeVar('OutT')


# =============================================================================
# Enums
# =============================================================================

_NODE_STATUS_BY_STORED_VALUE: dict[str, WorkflowTaskStatus] = {
    status.value: status for status in WorkflowTaskStatus
}


def stored_node_status(
    *, workflow_id: str, node_id: str, stored: str,
) -> WorkflowTaskStatus:
    """Return the node status that the database row stores.

    The CHECK ``horsies_workflow_tasks_status_known`` allows only the
    ``WorkflowTaskStatus`` values, so another value means the CHECK is
    missing or broken. Raises ``AssertionError`` that names the workflow,
    the node and the stored value.
    """
    status = _NODE_STATUS_BY_STORED_VALUE.get(stored)
    if status is None:
        raise AssertionError(
            f'workflow {workflow_id} node {node_id} stores status {stored!r}, '
            'which is not a WorkflowTaskStatus member; the CHECK '
            'horsies_workflow_tasks_status_known must forbid it'
        )
    return status


class OnError(str, Enum):
    """
    Error handling policy for workflows when a task fails.
    """

    FAIL = 'fail'
    """Continue DAG resolution but mark workflow as will-fail. Skip tasks without allow_failed_deps."""

    PAUSE = 'pause'
    """Pause workflow immediately. No new tasks enqueued until resume()."""
