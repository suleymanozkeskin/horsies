"""Tests for workflow registry lifecycle helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.workflows.registry import (
    clear_workflow_definition_registry,
    clear_workflow_registry,
    get_workflow_definition,
    get_node,
    is_workflow_registered,
    register_workflow_definition,
    register_workflow_spec,
    unregister_workflow_definition,
    unregister_workflow_spec,
)


@dataclass
class _FakeNode:
    index: int | None


@dataclass
class _FakeSpec:
    name: str
    tasks: list[_FakeNode]


@pytest.mark.unit
class TestWorkflowRegistry:
    """Tests for register/unregister and global cleanup behavior."""

    def setup_method(self) -> None:
        clear_workflow_registry()
        clear_workflow_definition_registry()

    def teardown_method(self) -> None:
        clear_workflow_registry()
        clear_workflow_definition_registry()

    def test_clear_workflow_registry_removes_all_entries(self) -> None:
        spec = _FakeSpec(name='wf_a', tasks=[_FakeNode(0), _FakeNode(1)])
        register_workflow_spec(spec)
        assert is_workflow_registered('wf_a')
        assert get_node('wf_a', 0) is not None

        clear_workflow_registry()

        assert not is_workflow_registered('wf_a')
        assert get_node('wf_a', 0) is None

    def test_register_same_name_replaces_old_nodes(self) -> None:
        original = _FakeSpec(name='wf_b', tasks=[_FakeNode(0), _FakeNode(1)])
        replacement = _FakeSpec(name='wf_b', tasks=[_FakeNode(0), _FakeNode(2)])

        register_workflow_spec(original)
        assert get_node('wf_b', 1) is not None

        register_workflow_spec(replacement)

        assert get_node('wf_b', 1) is None
        assert get_node('wf_b', 2) is not None

    def test_unregister_workflow_spec_removes_nodes(self) -> None:
        spec = _FakeSpec(name='wf_c', tasks=[_FakeNode(3)])
        register_workflow_spec(spec)
        assert get_node('wf_c', 3) is not None

        unregister_workflow_spec('wf_c')

        assert not is_workflow_registered('wf_c')
        assert get_node('wf_c', 3) is None

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
        assert exc_info.value.code == ErrorCode.WORKFLOW_DUPLICATE_DEFINITION_KEY

    def test_clear_workflow_registry_keeps_definition_registry(self) -> None:
        workflow_def = type('WorkflowE', (), {'definition_key': 'tests.workflow_e.v1'})
        register_workflow_definition(workflow_def)

        clear_workflow_registry()

        assert get_workflow_definition('tests.workflow_e.v1') is workflow_def
