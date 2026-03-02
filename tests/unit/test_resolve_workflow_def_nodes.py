"""Regression tests for _resolve_workflow_def_nodes (BUG 3).

Verifies that the engine's fallback node resolver creates copies
instead of mutating class-level WorkflowDefinition node attributes.
"""

from __future__ import annotations

from typing import Any
from dataclasses import dataclass

import pytest

from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.task_send_types import TaskSendError, TaskSendResult
from horsies.core.task_decorator import TaskHandle, TaskFunction, NodeFactory
from horsies.core.types.result import Ok
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowDefinition,
    WorkflowContext,
)
from horsies.core.workflows.engine import _resolve_workflow_def_nodes


@dataclass
class MockFn(TaskFunction[Any, Any]):
    """Minimal TaskFunction mock for unit tests."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(self, *args: Any, **kwargs: Any) -> TaskResult[Any, TaskError]:
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def retry_send(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    async def retry_send_async(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def retry_schedule(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def node(self, **kwargs: Any) -> NodeFactory[Any, Any]:
        return NodeFactory(fn=self, **kwargs)  # type: ignore[arg-type]


@dataclass
class MockFnWithCtx(TaskFunction[Any, Any]):
    """MockFn that accepts workflow_ctx (passes E010 validator)."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(
        self,
        *args: Any,
        workflow_ctx: WorkflowContext | None = None,
        **kwargs: Any,
    ) -> TaskResult[Any, TaskError]:
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def retry_send(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    async def retry_send_async(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def retry_schedule(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def node(self, **kwargs: Any) -> NodeFactory[Any, Any]:
        return NodeFactory(fn=self, **kwargs)  # type: ignore[arg-type]


fn_a = MockFn(task_name='task_a')
fn_b = MockFn(task_name='task_b')
fn_ctx = MockFnWithCtx(task_name='task_ctx')


def _fresh_workflow_class() -> type[WorkflowDefinition[Any]]:
    """Create a fresh WorkflowDefinition subclass with unstamped nodes.

    Returns a new class each call so tests are isolated from each other
    and from any prior build() calls.
    """

    class FreshWorkflow(WorkflowDefinition[int]):
        name = 'fresh-wf'
        fetch = TaskNode(fn=fn_a)
        process = TaskNode(fn=fn_b, waits_for=[fetch])

    return FreshWorkflow


@pytest.mark.unit
class TestResolveWorkflowDefNodes:
    """_resolve_workflow_def_nodes returns copies, never mutates class-level nodes."""

    def test_returns_correct_index_to_node_mapping(self) -> None:
        """Returned dict maps positional index to the corresponding node copy."""
        wf_cls = _fresh_workflow_class()

        result = _resolve_workflow_def_nodes(wf_cls)

        assert set(result.keys()) == {0, 1}
        assert result[0].node_id == 'fetch'
        assert result[0].index == 0
        assert result[1].node_id == 'process'
        assert result[1].index == 1

    def test_class_level_nodes_not_mutated(self) -> None:
        """Class-level node attributes remain None after resolve.

        BUG 3 regression: the old _node_from_workflow_def wrote index and
        node_id directly onto class-level node objects.
        """
        wf_cls = _fresh_workflow_class()

        # Capture originals before calling resolve
        nodes = wf_cls.get_workflow_nodes()
        assert nodes is not None
        original_fetch = nodes[0][1]
        original_process = nodes[1][1]

        _resolve_workflow_def_nodes(wf_cls)

        assert original_fetch.index is None, (
            'Class-level fetch.index mutated by _resolve_workflow_def_nodes'
        )
        assert original_fetch.node_id is None, (
            'Class-level fetch.node_id mutated by _resolve_workflow_def_nodes'
        )
        assert original_process.index is None, (
            'Class-level process.index mutated by _resolve_workflow_def_nodes'
        )
        assert original_process.node_id is None, (
            'Class-level process.node_id mutated by _resolve_workflow_def_nodes'
        )

    def test_returned_copies_are_not_class_level_objects(self) -> None:
        """Returned nodes are distinct objects from the class-level originals."""
        wf_cls = _fresh_workflow_class()
        nodes = wf_cls.get_workflow_nodes()
        assert nodes is not None

        result = _resolve_workflow_def_nodes(wf_cls)

        assert result[0] is not nodes[0][1]
        assert result[1] is not nodes[1][1]

    def test_idempotent_across_multiple_calls(self) -> None:
        """Multiple calls produce fresh copies each time; class nodes stay clean."""
        wf_cls = _fresh_workflow_class()
        nodes = wf_cls.get_workflow_nodes()
        assert nodes is not None
        original_fetch = nodes[0][1]

        result1 = _resolve_workflow_def_nodes(wf_cls)
        result2 = _resolve_workflow_def_nodes(wf_cls)

        # Each call returns independent copies
        assert result1[0] is not result2[0]
        assert result1[1] is not result2[1]

        # Class-level still untouched
        assert original_fetch.index is None
        assert original_fetch.node_id is None

    def test_callables_shared_not_copied(self) -> None:
        """fn references are shared by the copy, not duplicated."""
        wf_cls = _fresh_workflow_class()

        result = _resolve_workflow_def_nodes(wf_cls)

        assert result[0].fn is fn_a
        assert result[1].fn is fn_b

    def test_empty_workflow_returns_empty_dict(self) -> None:
        """WorkflowDefinition with no nodes returns an empty mapping."""

        class EmptyWorkflow(WorkflowDefinition[int]):
            name = 'empty'

        result = _resolve_workflow_def_nodes(EmptyWorkflow)

        assert result == {}

    def test_workflow_ctx_from_remapped_to_copies(self) -> None:
        """workflow_ctx_from refs point at stamped copies, not class-level originals."""

        class CtxWorkflow(WorkflowDefinition[int]):
            name = 'ctx-wf'
            fetch = TaskNode(fn=fn_a)
            process = TaskNode(
                fn=fn_ctx,
                waits_for=[fetch],
                workflow_ctx_from=[fetch],
            )

        nodes = CtxWorkflow.get_workflow_nodes()
        assert nodes is not None
        class_fetch = nodes[0][1]

        result = _resolve_workflow_def_nodes(CtxWorkflow)

        # workflow_ctx_from on the copy should point at the copy of fetch,
        # not the class-level fetch
        assert result[1].workflow_ctx_from is not None
        assert result[1].workflow_ctx_from[0] is result[0]
        assert result[1].workflow_ctx_from[0] is not class_fetch

    def test_pre_stamped_nodes_preserved(self) -> None:
        """Nodes that already have index/node_id keep their values (guard check)."""

        class PreStamped(WorkflowDefinition[int]):
            name = 'pre-stamped'
            fetch = TaskNode(fn=fn_a, node_id='custom-id')

        # Manually stamp index to simulate a prior build()
        PreStamped.fetch.index = 0

        result = _resolve_workflow_def_nodes(PreStamped)

        assert result[0].node_id == 'custom-id'
        assert result[0].index == 0
