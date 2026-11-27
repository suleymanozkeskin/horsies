"""Registry for WorkflowDefinition classes.

A WorkflowDefinition registers by its ``definition_key``; the child build of a
sub-workflow node loads the definition by that key.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from horsies.core.models.workflow import WorkflowDefinition

_workflow_defs_by_key: dict[str, 'type[WorkflowDefinition[Any]]'] = {}


def register_workflow_definition(
    workflow_def: 'type[WorkflowDefinition[Any]]',
) -> None:
    """Register a WorkflowDefinition by its explicit definition_key key."""
    definition_key = getattr(workflow_def, 'definition_key', None)
    if not isinstance(definition_key, str) or not definition_key.strip():
        return
    existing = _workflow_defs_by_key.get(definition_key)
    if existing is not None and existing is not workflow_def:
        from horsies.core.errors import HorsiesErrorCode, WorkflowValidationError

        raise WorkflowValidationError(
            (
                f"definition_key '{definition_key}' is already registered to "
                f"'{existing.__qualname__}'"
            ),
            code=HorsiesErrorCode.WORKFLOW_DUPLICATE_DEFINITION_KEY,
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
