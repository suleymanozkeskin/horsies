"""Unit tests for WorkflowSpec input isolation (BUG 2 regression).

Verifies that WorkflowSpec.__post_init__ copies the input node graph
so each spec has independent nodes. Covers identity checks,
cross-reference remapping, kwargs isolation, explicit node_id preservation,
and validator preservation for invalid refs.
"""

from __future__ import annotations

from typing import Any
from dataclasses import dataclass

import pytest

from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.task_send_types import TaskSendError, TaskSendResult
from horsies.core.task_decorator import TaskHandle, TaskFunction, NodeFactory
from horsies.core.types.result import Ok
from horsies.core.errors import MultipleValidationErrors
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowSpec,
    WorkflowContext,
    WorkflowDefinition,
    WorkflowValidationError,
    OnError,
    SuccessPolicy,
    SuccessCase,
)


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
fn_c = MockFn(task_name='task_c')
fn_ctx = MockFnWithCtx(task_name='task_ctx')


@pytest.mark.unit
class TestSpecIsolation:
    """WorkflowSpec.__post_init__ copies the input node graph."""

    # ------------------------------------------------------------------
    # Identity: spec holds copies, not originals
    # ------------------------------------------------------------------

    def test_spec_tasks_are_independent_copies(self) -> None:
        """Spec's task list contains copies, not the original node objects."""
        a = TaskNode(fn=fn_a)
        b = TaskNode(fn=fn_b, waits_for=[a])

        spec = WorkflowSpec(name='wf', tasks=[a, b])

        assert spec.tasks[0] is not a
        assert spec.tasks[1] is not b

    # ------------------------------------------------------------------
    # Cross-reference remapping
    # ------------------------------------------------------------------

    def test_waits_for_remapped_to_copies(self) -> None:
        """waits_for refs point at the spec's own copies, not the originals."""
        a = TaskNode(fn=fn_a)
        b = TaskNode(fn=fn_b, waits_for=[a])

        spec = WorkflowSpec(name='wf', tasks=[a, b])

        assert spec.tasks[1].waits_for[0] is spec.tasks[0]
        assert spec.tasks[1].waits_for[0] is not a

    def test_args_from_remapped_to_copies(self) -> None:
        """args_from values point at the spec's own copies."""
        a = TaskNode(fn=fn_a)
        b = TaskNode(fn=fn_b, waits_for=[a], args_from={'data': a})

        spec = WorkflowSpec(name='wf', tasks=[a, b])

        assert spec.tasks[1].args_from['data'] is spec.tasks[0]
        assert spec.tasks[1].args_from['data'] is not a

    def test_workflow_ctx_from_remapped_to_copies(self) -> None:
        """workflow_ctx_from refs point at the spec's own copies."""
        a = TaskNode(fn=fn_a)
        b = TaskNode(fn=fn_ctx, waits_for=[a], workflow_ctx_from=[a])

        spec = WorkflowSpec(name='wf', tasks=[a, b])

        assert spec.tasks[1].workflow_ctx_from is not None
        assert spec.tasks[1].workflow_ctx_from[0] is spec.tasks[0]
        assert spec.tasks[1].workflow_ctx_from[0] is not a

    def test_output_remapped_to_copy(self) -> None:
        """output ref points at the spec's own copy."""
        a = TaskNode(fn=fn_a)

        spec = WorkflowSpec(name='wf', tasks=[a], output=a)

        assert spec.output is spec.tasks[0]
        assert spec.output is not a

    def test_success_policy_remapped_to_copies(self) -> None:
        """SuccessPolicy required and optional refs point at spec's own copies."""
        a = TaskNode(fn=fn_a)
        b = TaskNode(fn=fn_b, waits_for=[a])

        policy = SuccessPolicy(
            cases=[SuccessCase(required=[a])],
            optional=[b],
        )
        spec = WorkflowSpec(name='wf', tasks=[a, b], success_policy=policy)

        assert spec.success_policy is not None
        assert spec.success_policy.cases[0].required[0] is spec.tasks[0]
        assert spec.success_policy.cases[0].required[0] is not a
        assert spec.success_policy.optional is not None
        assert spec.success_policy.optional[0] is spec.tasks[1]
        assert spec.success_policy.optional[0] is not b

    # ------------------------------------------------------------------
    # kwargs isolation
    # ------------------------------------------------------------------

    def test_kwargs_isolated_from_original(self) -> None:
        """Mutating original kwargs after spec construction does not affect the spec."""
        original_kwargs = {'url': 'https://example.com'}
        a = TaskNode(fn=fn_a, kwargs=original_kwargs)

        spec = WorkflowSpec(name='wf', tasks=[a])

        assert spec.tasks[0].kwargs is not original_kwargs
        assert spec.tasks[0].kwargs == {'url': 'https://example.com'}

        # Mutate original — spec must be unaffected
        original_kwargs['url'] = 'https://mutated.com'
        assert spec.tasks[0].kwargs['url'] == 'https://example.com'

    # ------------------------------------------------------------------
    # Node reuse across specs
    # ------------------------------------------------------------------

    def test_node_reuse_across_specs_independent_ids(self) -> None:
        """Reusing a node across two different-name specs produces correct node_ids.

        BUG 2 core fix: each spec has its own copy with the correct node_id,
        regardless of how many specs the same node is passed to.
        """
        node = TaskNode(fn=fn_a)

        spec1 = WorkflowSpec(name='alpha', tasks=[node])
        spec2 = WorkflowSpec(name='beta', tasks=[node])

        # Each spec has independent, correct node_id
        assert spec1.tasks[0].node_id == 'alpha:0'
        assert spec2.tasks[0].node_id == 'beta:0'
        assert spec1.tasks[0] is not spec2.tasks[0]

    def test_explicit_node_id_preserved_on_reuse(self) -> None:
        """User-provided explicit node_id survives reuse across multiple specs."""
        node = TaskNode(fn=fn_a, node_id='custom-stable-id')

        spec1 = WorkflowSpec(name='alpha', tasks=[node])
        spec2 = WorkflowSpec(name='beta', tasks=[node])

        assert spec1.tasks[0].node_id == 'custom-stable-id'
        assert spec2.tasks[0].node_id == 'custom-stable-id'
        assert spec1.tasks[0] is not spec2.tasks[0]

    def test_manual_node_id_override_after_auto_derive(self) -> None:
        """Manual node_id set after auto-derive is preserved by the next spec.

        Regression: _node_id_auto_derived was sticky — once True from spec1's
        auto-derive, a manual override between specs was still treated as
        stale and reset.
        """
        node = TaskNode(fn=fn_a)

        spec1 = WorkflowSpec(name='alpha', tasks=[node])
        assert spec1.tasks[0].node_id == 'alpha:0'

        # User manually overrides node_id between spec constructions
        node.node_id = 'manual-after-first-spec'

        spec2 = WorkflowSpec(name='beta', tasks=[node])
        assert spec2.tasks[0].node_id == 'manual-after-first-spec'

    # ------------------------------------------------------------------
    # Condition lambda runtime coupling
    # ------------------------------------------------------------------

    def test_condition_closure_reads_original_node_id(self) -> None:
        """Condition lambdas that capture originals can resolve via ctx.result_for().

        This tests the real runtime coupling: lambdas close over original node
        objects and call ctx.result_for(original_node), which reads node.node_id
        to look up the result. The spec must ensure original.node_id is available
        for this lookup to succeed.
        """
        a = TaskNode(fn=fn_a)
        b = TaskNode(fn=fn_ctx, waits_for=[a], workflow_ctx_from=[a])

        spec = WorkflowSpec(name='wf', tasks=[a, b])

        # Build a WorkflowContext the way the engine would: keyed by
        # the spec copy's node_id.
        expected_result = TaskResult[int, TaskError](ok=42)
        ctx = WorkflowContext(
            workflow_id='test-wf-id',
            task_index=1,
            task_name='task_ctx',
            results_by_id={spec.tasks[0].node_id: expected_result},
        )

        # The closure captures `a` (original), not spec.tasks[0] (copy).
        # This must resolve successfully — proves the runtime coupling works.
        resolved = ctx.result_for(a)
        assert resolved.is_ok()
        assert resolved.unwrap() == 42

    # ------------------------------------------------------------------
    # Invalid refs preserved for validators
    # ------------------------------------------------------------------

    def test_invalid_output_ref_preserved_for_validator(self) -> None:
        """Output pointing outside tasks still raises validation error after copy."""
        a = TaskNode(fn=fn_a)
        orphan = TaskNode(fn=fn_b)

        with pytest.raises((WorkflowValidationError, MultipleValidationErrors)):
            WorkflowSpec(name='wf', tasks=[a], output=orphan)

    def test_invalid_success_policy_ref_preserved_for_validator(self) -> None:
        """SuccessPolicy with ref outside tasks still raises validation error after copy."""
        a = TaskNode(fn=fn_a)
        orphan = TaskNode(fn=fn_b)

        policy = SuccessPolicy(
            cases=[SuccessCase(required=[orphan])],
        )
        with pytest.raises((WorkflowValidationError, MultipleValidationErrors)):
            WorkflowSpec(name='wf', tasks=[a], success_policy=policy)

    # ------------------------------------------------------------------
    # Callables shared, not copied
    # ------------------------------------------------------------------

    def test_callables_shared_not_copied(self) -> None:
        """fn, run_when, skip_when are shared by reference (not deep-copied)."""
        def my_run_when(_ctx: WorkflowContext) -> bool:
            return True

        def my_skip_when(_ctx: WorkflowContext) -> bool:
            return False

        a = TaskNode(fn=fn_a)
        b = TaskNode(
            fn=fn_ctx,
            waits_for=[a],
            workflow_ctx_from=[a],
            run_when=my_run_when,
            skip_when=my_skip_when,
        )

        spec = WorkflowSpec(name='wf', tasks=[a, b])

        assert spec.tasks[0].fn is fn_a
        assert spec.tasks[1].fn is fn_ctx
        assert spec.tasks[1].run_when is my_run_when
        assert spec.tasks[1].skip_when is my_skip_when
