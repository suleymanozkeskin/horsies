"""Registry for WorkflowSpec objects.

Provides access to TaskNode and SubWorkflowNode objects at runtime for
subworkflow dispatch and node lookup. Workers that import workflow modules
will automatically register the specs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from weakref import WeakValueDictionary

if TYPE_CHECKING:
    from horsies.core.models.workflow import (
        TaskNode,
        SubWorkflowNode,
        WorkflowDefinition,
        WorkflowSpec,
    )

# Registry: (workflow_name, task_index) -> TaskNode | SubWorkflowNode
# Use weak references so specs can be garbage collected
_nodes_by_spec: WeakValueDictionary[tuple[str, int], Any] = WeakValueDictionary()

# Strong reference to keep specs alive during execution
_active_specs: dict[str, 'WorkflowSpec[Any]'] = {}
_workflow_defs_by_key: dict[str, 'type[WorkflowDefinition[Any]]'] = {}


def _remove_spec_nodes(name: str, spec: 'WorkflowSpec[Any]') -> None:
    """Remove node index entries for a previously-registered spec."""
    for node in spec.tasks:
        if node.index is not None:
            _nodes_by_spec.pop((name, node.index), None)


def register_workflow_spec(spec: 'WorkflowSpec[Any]') -> None:
    """
    Register a WorkflowSpec for subworkflow dispatch and node lookup.

    Called automatically when WorkflowSpec is created with SubWorkflowNodes.
    """
    existing = _active_specs.get(spec.name)
    if existing is not None and existing is not spec:
        _remove_spec_nodes(spec.name, existing)
    _active_specs[spec.name] = spec
    for node in spec.tasks:
        if node.index is not None:
            _nodes_by_spec[(spec.name, node.index)] = node


def unregister_workflow_spec(name: str) -> None:
    """Remove a workflow spec from the registry."""
    if name in _active_specs:
        spec = _active_specs[name]
        _remove_spec_nodes(name, spec)
        del _active_specs[name]


def clear_workflow_registry() -> None:
    """Clear all registered workflow specs and node lookups."""
    _active_specs.clear()
    _nodes_by_spec.clear()


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


def register_workflow_definition(
    workflow_def: 'type[WorkflowDefinition[Any]]',
) -> None:
    """Register a WorkflowDefinition by its explicit definition_key key."""
    definition_key = getattr(workflow_def, 'definition_key', None)
    if not isinstance(definition_key, str) or not definition_key.strip():
        return
    existing = _workflow_defs_by_key.get(definition_key)
    if existing is not None and existing is not workflow_def:
        from horsies.core.errors import ErrorCode, WorkflowValidationError

        raise WorkflowValidationError(
            (
                f"definition_key '{definition_key}' is already registered to "
                f"'{existing.__qualname__}'"
            ),
            code=ErrorCode.WORKFLOW_DUPLICATE_DEFINITION_KEY,
        )
    _workflow_defs_by_key[definition_key] = workflow_def


def unregister_workflow_definition(definition_key: str) -> None:
    """Remove a workflow definition from the definition_key registry."""
    _workflow_defs_by_key.pop(definition_key, None)


def get_workflow_definition(
    definition_key: str,
) -> 'type[WorkflowDefinition[Any]] | None':
    """Look up a WorkflowDefinition by its explicit definition_key key."""
    return _workflow_defs_by_key.get(definition_key)


def clear_workflow_definition_registry() -> None:
    """Clear definition_key registrations."""
    _workflow_defs_by_key.clear()
