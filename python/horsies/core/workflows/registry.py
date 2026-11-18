"""Registry for WorkflowSpec objects and WorkflowDefinition classes.

A WorkflowSpec with SubWorkflowNodes registers itself by its name when it is
created. A WorkflowDefinition registers by its ``definition_key``; the child
build of a sub-workflow node loads the definition by that key.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from weakref import WeakValueDictionary

if TYPE_CHECKING:
    from horsies.core.models.workflow import (
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
    Register a WorkflowSpec by its name.

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
            help_text='give each WorkflowDefinition its own definition_key',
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
