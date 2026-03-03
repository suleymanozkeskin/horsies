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

    def test_fn_callable_shared_not_copied(self) -> None:
        """fn callable is shared by reference (not deep-copied)."""
        a = TaskNode(fn=fn_a)
        b = TaskNode(
            fn=fn_ctx,
            waits_for=[a],
            workflow_ctx_from=[a],
        )

        spec = WorkflowSpec(name='wf', tasks=[a, b])

        assert spec.tasks[0].fn is fn_a
        assert spec.tasks[1].fn is fn_ctx

    # ------------------------------------------------------------------
    # Failed spec does not mutate originals
    # ------------------------------------------------------------------

    def test_failed_spec_does_not_mutate_originals(self) -> None:
        """A WorkflowSpec that fails validation must not back-propagate to originals.

        Regression: back-propagation ran before validation, so even invalid
        specs mutated the caller's node objects with index/node_id values.
        """
        a = TaskNode(fn=fn_a)
        orphan = TaskNode(fn=fn_b)

        assert a.node_id is None
        assert a.index is None

        with pytest.raises((WorkflowValidationError, MultipleValidationErrors)):
            WorkflowSpec(name='wf', tasks=[a], output=orphan)

        # Originals must be untouched after a failed spec
        assert a.node_id is None
        assert a.index is None


# =============================================================================
# Metaclass stamping (WorkflowDefinitionMeta)
# =============================================================================


_meta_node_a: TaskNode[int] = TaskNode(fn=fn_a)
_meta_node_b: TaskNode[int] = TaskNode(fn=fn_ctx, waits_for=[_meta_node_a])
_meta_node_explicit: TaskNode[int] = TaskNode(fn=fn_a, node_id='custom-id')


class _MetaStampWorkflow(WorkflowDefinition[int]):
    """Test workflow defined purely for metaclass stamp verification."""

    name = 'meta_stamp_test'

    step_a = _meta_node_a
    step_b = _meta_node_b
    step_explicit = _meta_node_explicit


# Closure captures the module-level node — the fresh-worker scenario
def _meta_closure_condition(ctx: WorkflowContext) -> bool:
    result = ctx.result_for(_meta_node_a)
    return result.is_ok()


@pytest.mark.unit
class TestMetaclassStamping:
    """WorkflowDefinitionMeta.__new__ stamps node_id at class-definition time."""

    def test_metaclass_stamps_node_id_at_class_definition(self) -> None:
        """Class-level nodes get node_id == attr_name from the metaclass.

        No build() call is needed — the stamp happens at class definition time.
        This is the fix for the fresh-worker closure bug: condition closures
        always see a valid node_id on the captured original.
        """
        assert _meta_node_a.node_id == 'step_a'
        assert _meta_node_b.node_id == 'step_b'

        # _node_id_auto_derived is False because __setattr__ clears it
        assert _meta_node_a._node_id_auto_derived is False
        assert _meta_node_b._node_id_auto_derived is False

    def test_metaclass_does_not_overwrite_explicit_node_id(self) -> None:
        """User-provided node_id is preserved by the metaclass."""
        assert _meta_node_explicit.node_id == 'custom-id'

    def test_closure_works_without_build(self) -> None:
        """Condition closure resolves ctx.result_for(original_node) without build().

        This is the core regression test for the fresh-worker scenario:
        the metaclass stamps node_id at import time, so closures captured
        at module level always have a valid node_id for lookup.
        """
        from horsies.core.models.tasks import TaskResult, TaskError

        # Build a WorkflowContext keyed by the metaclass-stamped node_id
        node_id = _meta_node_a.node_id
        assert node_id is not None

        expected_result = TaskResult[int, TaskError](ok=42)
        ctx = WorkflowContext(
            workflow_id='test-wf-id',
            task_index=1,
            task_name='task_ctx',
            results_by_id={node_id: expected_result},
        )

        # The closure captures _meta_node_a (module-level original).
        # Because the metaclass stamped node_id='step_a', this resolves.
        assert _meta_closure_condition(ctx) is True


# =============================================================================
# Copy-on-build: build() must not mutate class-level nodes
# =============================================================================


class _MockApp:
    """Minimal app mock that exercises queue/priority enrichment."""

    def workflow(
        self,
        name: str,
        tasks: list[Any],
        **kwargs: Any,
    ) -> WorkflowSpec[Any]:
        # Simulate the enrichment app.workflow() does (app.py:939,943)
        for node in tasks:
            if isinstance(node, TaskNode):
                if node.queue is None:
                    node.queue = 'default'
                if node.priority is None:
                    node.priority = 5
        return WorkflowSpec(name=name, tasks=tasks, **kwargs)

    def get_broker(self) -> None:
        return None


@pytest.mark.unit
class TestCopyOnBuild:
    """build() must copy class-level nodes before passing to app.workflow()."""

    def test_class_level_nodes_not_enriched_by_build(self) -> None:
        """Queue/priority enrichment in app.workflow() must not reach class-level nodes."""

        class MyWorkflow(WorkflowDefinition[Any]):
            name = 'copy_on_build_test'
            fetch = TaskNode(fn=fn_a)
            process = TaskNode(fn=fn_b, waits_for=[fetch])

        # Snapshot pre-build state
        assert MyWorkflow.fetch.queue is None
        assert MyWorkflow.fetch.priority is None
        assert MyWorkflow.process.queue is None
        assert MyWorkflow.process.priority is None

        mock_app = _MockApp()
        _spec = MyWorkflow.build_with(mock_app)  # type: ignore[arg-type]

        # Class-level nodes must be untouched
        assert MyWorkflow.fetch.queue is None
        assert MyWorkflow.fetch.priority is None
        assert MyWorkflow.process.queue is None
        assert MyWorkflow.process.priority is None

    def test_build_twice_produces_independent_specs(self) -> None:
        """Two build() calls must not share node identity."""

        class MyWorkflow(WorkflowDefinition[Any]):
            name = 'double_build_test'
            step = TaskNode(fn=fn_a)

        mock_app = _MockApp()
        spec_1 = MyWorkflow.build_with(mock_app)  # type: ignore[arg-type]
        spec_2 = MyWorkflow.build_with(mock_app)  # type: ignore[arg-type]

        # Spec nodes are independent copies
        assert spec_1.tasks[0] is not spec_2.tasks[0]
        # Neither is the class-level original
        assert spec_1.tasks[0] is not MyWorkflow.step
        assert spec_2.tasks[0] is not MyWorkflow.step
