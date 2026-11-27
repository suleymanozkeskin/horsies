"""Tests for workflow registry lifecycle helpers."""

from __future__ import annotations

import pytest

from horsies.core.errors import HorsiesErrorCode, WorkflowValidationError
from horsies.core.workflows.registry import (
    clear_workflow_definition_registry,
    get_workflow_definition,
    register_workflow_definition,
    unregister_workflow_definition,
)


@pytest.mark.unit
class TestWorkflowRegistry:
    """Tests for register/unregister and global cleanup behavior."""

    def setup_method(self) -> None:
        clear_workflow_definition_registry()

    def teardown_method(self) -> None:
        clear_workflow_definition_registry()

    def test_register_workflow_definition_by_type(self) -> None:
        workflow_def = type('WorkflowA', (), {'definition_key': 'tests.workflow_a.v1'})
        register_workflow_definition(workflow_def)

        assert get_workflow_definition('tests.workflow_a.v1') is workflow_def

    def test_unregister_workflow_definition_removes_entry(self) -> None:
        workflow_def = type('WorkflowB', (), {'definition_key': 'tests.workflow_b.v1'})
        register_workflow_definition(workflow_def)

        unregister_workflow_definition('tests.workflow_b.v1')

        assert get_workflow_definition('tests.workflow_b.v1') is None

    def test_duplicate_definition_key_raises(self) -> None:
        first = type('WorkflowC', (), {'definition_key': 'tests.workflow_c.v1'})
        second = type('WorkflowD', (), {'definition_key': 'tests.workflow_c.v1'})
        register_workflow_definition(first)

        with pytest.raises(WorkflowValidationError) as exc_info:
            register_workflow_definition(second)
        assert exc_info.value.code == HorsiesErrorCode.WORKFLOW_DUPLICATE_DEFINITION_KEY
