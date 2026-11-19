"""The node rows of a workflow start: the client's part of the start tree.

The core builds the tree (``CoreHandle.start_workflow_async``,
``horsies_workflow_start``) from these rows and from the child spec source
of the broker, which builds each pre-built child.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from horsies.core.models.workflow import SubWorkflowNode, TaskNode
from horsies.core.workflows.child_build import (
    NodeOwner,
    NodeRow,
    subworkflow_node_row,
    task_node_row,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.workflow import WorkflowSpec


def top_node_rows(
    broker: PostgresBroker, spec: WorkflowSpec[Any],
) -> tuple[NodeRow, ...]:
    """The node rows of a spec that ``app.workflow()`` built and the start checked.

    Each task node has its queue (``resolve_node_queue``) and priority set.

    Raises:
        SerializationError: as ``task_node_row`` (a defect).
    """
    from horsies.core.task_decorator import resolve_node_queue

    rows: list[NodeRow] = []
    for node in spec.tasks:
        assert not node.args, (
            f"{type(node).__name__} '{node.name}' reached the start with "
            f'{len(node.args)} positional args; spec construction refuses them'
        )
        match node:
            case SubWorkflowNode():
                rows.append(subworkflow_node_row(node, NodeOwner.START))
            case TaskNode():
                assert node.priority is not None, (
                    f"TaskNode '{node.name}' reached the start with priority=None; "
                    'start prevalidation refuses it'
                )
                rows.append(
                    task_node_row(
                        broker, node, NodeOwner.START, resolve_node_queue(node), node.priority,
                    ),
                )
    return tuple(rows)


