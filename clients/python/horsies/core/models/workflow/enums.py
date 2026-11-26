"""Workflow status enums and type variables.

`WorkflowStatus`, `WorkflowNodeStatus` and their terminal-state constants
are the classes generated from horsies-core (`horsies._catalog.status`).
"""

from __future__ import annotations

from enum import Enum
from typing import TypeVar

from horsies._catalog.status import (
    WORKFLOW_NODE_TERMINAL_STATES as WORKFLOW_NODE_TERMINAL_STATES,
)
from horsies._catalog.status import (
    WORKFLOW_TERMINAL_STATES as WORKFLOW_TERMINAL_STATES,
)
from horsies._catalog.status import WorkflowStatus as WorkflowStatus
from horsies._catalog.status import WorkflowNodeStatus as WorkflowNodeStatus

# TypeVar for TaskNode generic parameter (the "ok" type of its Result)
OkT = TypeVar('OkT')
OkT_co = TypeVar('OkT_co', covariant=True)
OutT = TypeVar('OutT')
# TypeVars for the type of the custom codes of a node's task (`TaskError[C]`):
# `str`, or the `Enum` class that the task declares.
CodeT = TypeVar('CodeT', default=str)
CodeT_co = TypeVar('CodeT_co', covariant=True, default=str)


# =============================================================================
# Enums
# =============================================================================

_NODE_STATUS_BY_STORED_VALUE: dict[str, WorkflowNodeStatus] = {
    status.value: status for status in WorkflowNodeStatus
}


def stored_node_status(
    *, workflow_id: str, node_id: str, stored: str,
) -> WorkflowNodeStatus:
    """Return the node status that the database row stores.

    The CHECK ``horsies_workflow_tasks_status_known`` allows only the
    ``WorkflowNodeStatus`` values, so another value means the CHECK is
    missing or broken. Raises ``AssertionError`` that names the workflow,
    the node and the stored value.
    """
    status = _NODE_STATUS_BY_STORED_VALUE.get(stored)
    if status is None:
        raise AssertionError(
            f'workflow {workflow_id} node {node_id} stores status {stored!r}, '
            'which is not a WorkflowNodeStatus member; the CHECK '
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
