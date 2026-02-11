"""Registry for WorkflowSpec objects.

Enables condition evaluation by providing access to TaskNode and SubWorkflowNode
objects at runtime. Workers that import workflow modules will automatically
register the specs.

NOTE: For conditions to work, the workflow module must be imported
in the worker process so specs are registered.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from weakref import WeakValueDictionary

if TYPE_CHECKING:
    from horsies.core.models.workflow import TaskNode, SubWorkflowNode, WorkflowSpec

# Registry: (workflow_name, task_index) -> TaskNode | SubWorkflowNode
# Use weak references so specs can be garbage collected
_nodes_by_spec: WeakValueDictionary[tuple[str, int], Any] = WeakValueDictionary()

# Strong reference to keep specs alive during execution
_active_specs: dict[str, 'WorkflowSpec[Any]'] = {}


def register_workflow_spec(spec: 'WorkflowSpec[Any]') -> None:
    """
    Register a WorkflowSpec for condition evaluation and subworkflow lookup.

    Called automatically when WorkflowSpec is created.
    Workers need to import the same module to have access to conditions.
    """
    _active_specs[spec.name] = spec
    for node in spec.tasks:
        if node.index is not None:
            _nodes_by_spec[(spec.name, node.index)] = node


def unregister_workflow_spec(name: str) -> None:
    """Remove a workflow spec from the registry."""
    if name in _active_specs:
        spec = _active_specs[name]
        for node in spec.tasks:
            if node.index is not None:
                key = (name, node.index)
                if key in _nodes_by_spec:
                    del _nodes_by_spec[key]
        del _active_specs[name]


def get_task_node(workflow_name: str, task_index: int) -> 'TaskNode[Any] | None':
    """
    Look up a TaskNode by workflow name and task index.

    Returns None if not found (workflow not registered in this process).
    """
    from typing import cast
    from horsies.core.models.workflow import TaskNode

    node = _nodes_by_spec.get((workflow_name, task_index))
    if node is not None and isinstance(node, TaskNode):
        return cast('TaskNode[Any]', node)
    return None


def get_subworkflow_node(
    workflow_name: str, task_index: int
) -> 'SubWorkflowNode[Any] | None':
    """
    Look up a SubWorkflowNode by workflow name and task index.

    Returns None if not found (workflow not registered in this process).
    """
    from typing import cast
    from horsies.core.models.workflow import SubWorkflowNode

    node = _nodes_by_spec.get((workflow_name, task_index))
    if node is not None and isinstance(node, SubWorkflowNode):
        return cast('SubWorkflowNode[Any]', node)
    return None


def get_node(
    workflow_name: str, task_index: int
) -> 'TaskNode[Any] | SubWorkflowNode[Any] | None':
    """
    Look up any node (TaskNode or SubWorkflowNode) by workflow name and task index.

    Returns None if not found (workflow not registered in this process).
    """
    return _nodes_by_spec.get((workflow_name, task_index))


def is_workflow_registered(name: str) -> bool:
    """Check if a workflow is registered."""
    return name in _active_specs
