"""Unit tests for WorkflowSpec deep-freeze (Phase B).

Verifies that nodes and specs are immutable after construction:
_frozen guards, container conversions (MappingProxyType, tuple),
ContextVar plumbing, and preservation of user-facing DX.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any
from dataclasses import dataclass

import pytest

from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.task_send_types import TaskSendError, TaskSendResult
from horsies.core.task_decorator import TaskHandle, TaskFunction, NodeFactory
from horsies.core.types.result import Ok
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowSpec,
    WorkflowContext,
    WorkflowDefinition,
    OnError,
    SuccessPolicy,
    SuccessCase,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


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
fn_c = MockFnWithCtx(task_name='task_c')


def _make_spec(
    *,
    with_deps: bool = False,
    with_output: bool = False,
    with_success_policy: bool = False,
    with_kwargs: bool = False,
    with_workflow_ctx: bool = False,
) -> WorkflowSpec[Any]:
    """Build a WorkflowSpec with configurable complexity."""
    a = TaskNode(fn=fn_a, kwargs={'url': 'https://example.com'} if with_kwargs else {})
    deps: list[TaskNode[Any] | SubWorkflowNode[Any]] = [a] if with_deps else []
    args_from_map: dict[str, TaskNode[Any] | SubWorkflowNode[Any]] = (
        {'data': a} if with_deps else {}
    )
    ctx_from = [a] if with_workflow_ctx else None

    b_fn: TaskFunction[Any, Any] = fn_c if with_workflow_ctx else fn_b
    b = TaskNode(
        fn=b_fn,
        waits_for=deps,
        args_from=args_from_map,
        workflow_ctx_from=ctx_from,
    )

    tasks: list[TaskNode[Any] | SubWorkflowNode[Any]] = [a, b]
    output = b if with_output else None
    policy = SuccessPolicy(
        cases=[SuccessCase(required=[a])],
        optional=[b],
    ) if with_success_policy else None

    return WorkflowSpec(
        name='test_wf',
        tasks=tasks,
        output=output,
        success_policy=policy,
    )


# ===========================================================================
# Freeze enforcement — attribute write
# ===========================================================================


class TestFrozenNodeRejectsAttributeWrite:
    """Frozen nodes reject attribute writes via __setattr__."""

    def test_task_node_index(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen TaskNode'):
            spec.tasks[0].index = 99

    def test_task_node_node_id(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen TaskNode'):
            spec.tasks[0].node_id = 'overwrite'

    def test_task_node_kwargs(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen TaskNode'):
            spec.tasks[0].kwargs = {}  # type: ignore[assignment]

    def test_task_node_queue(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen TaskNode'):
            spec.tasks[0].queue = 'other'  # type: ignore[union-attr]


class TestFrozenNodeRejectsAttributeDelete:
    """Frozen nodes reject attribute deletes via __delattr__."""

    def test_task_node_del(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen TaskNode'):
            del spec.tasks[0].index


class TestFrozenSpecRejectsAttributeWrite:
    """Frozen WorkflowSpec rejects attribute writes."""

    def test_spec_name(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen WorkflowSpec'):
            spec.name = 'overwrite'

    def test_spec_tasks(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen WorkflowSpec'):
            spec.tasks = []  # type: ignore[assignment]

    def test_spec_workflow_def_module(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen WorkflowSpec'):
            spec.workflow_def_module = 'some.module'


class TestFrozenSpecRejectsAttributeDelete:
    """Frozen WorkflowSpec rejects attribute deletes."""

    def test_spec_del(self) -> None:
        spec = _make_spec()
        with pytest.raises(AttributeError, match='frozen WorkflowSpec'):
            del spec.name


# ===========================================================================
# Freeze enforcement — container immutability
# ===========================================================================


class TestFrozenContainers:
    """Container fields are converted to immutable forms after construction."""

    def test_kwargs_is_mapping_proxy(self) -> None:
        spec = _make_spec(with_kwargs=True)
        assert isinstance(spec.tasks[0].kwargs, MappingProxyType)

    def test_kwargs_rejects_write(self) -> None:
        spec = _make_spec(with_kwargs=True)
        with pytest.raises(TypeError):
            spec.tasks[0].kwargs['new_key'] = 'val'  # type: ignore[index]

    def test_kwargs_values_preserved(self) -> None:
        spec = _make_spec(with_kwargs=True)
        assert spec.tasks[0].kwargs['url'] == 'https://example.com'

    def test_waits_for_is_tuple(self) -> None:
        spec = _make_spec(with_deps=True)
        assert isinstance(spec.tasks[1].waits_for, tuple)

    def test_args_from_is_mapping_proxy(self) -> None:
        spec = _make_spec(with_deps=True)
        assert isinstance(spec.tasks[1].args_from, MappingProxyType)

    def test_args_from_rejects_write(self) -> None:
        spec = _make_spec(with_deps=True)
        with pytest.raises(TypeError):
            spec.tasks[1].args_from['new'] = spec.tasks[0]  # type: ignore[index]

    def test_workflow_ctx_from_is_tuple(self) -> None:
        spec = _make_spec(with_deps=True, with_workflow_ctx=True)
        assert spec.tasks[1].workflow_ctx_from is not None
        assert isinstance(spec.tasks[1].workflow_ctx_from, tuple)

    def test_tasks_is_tuple(self) -> None:
        spec = _make_spec()
        assert isinstance(spec.tasks, tuple)


class TestFrozenSuccessPolicy:
    """SuccessPolicy and SuccessCase containers are frozen."""

    def test_cases_is_tuple(self) -> None:
        spec = _make_spec(with_success_policy=True)
        assert spec.success_policy is not None
        assert isinstance(spec.success_policy.cases, tuple)

    def test_required_is_tuple(self) -> None:
        spec = _make_spec(with_success_policy=True)
        assert spec.success_policy is not None
        assert isinstance(spec.success_policy.cases[0].required, tuple)

    def test_optional_is_tuple(self) -> None:
        spec = _make_spec(with_success_policy=True)
        assert spec.success_policy is not None
        assert spec.success_policy.optional is not None
        assert isinstance(spec.success_policy.optional, tuple)

    def test_success_case_rejects_write(self) -> None:
        spec = _make_spec(with_success_policy=True)
        assert spec.success_policy is not None
        with pytest.raises(AttributeError, match='frozen SuccessCase'):
            spec.success_policy.cases[0].required = []  # type: ignore[assignment]

    def test_success_policy_rejects_write(self) -> None:
        spec = _make_spec(with_success_policy=True)
        assert spec.success_policy is not None
        with pytest.raises(AttributeError, match='frozen SuccessPolicy'):
            spec.success_policy.cases = []  # type: ignore[assignment]


# ===========================================================================
# Cross-reference remapping after freeze
# ===========================================================================


class TestCrossRefRemapping:
    """Internal cross-references point to spec-internal copies, not originals."""

    def test_waits_for_points_to_spec_copy(self) -> None:
        spec = _make_spec(with_deps=True)
        assert spec.tasks[1].waits_for[0] is spec.tasks[0]

    def test_args_from_points_to_spec_copy(self) -> None:
        spec = _make_spec(with_deps=True)
        assert spec.tasks[1].args_from['data'] is spec.tasks[0]

    def test_output_points_to_spec_copy(self) -> None:
        spec = _make_spec(with_deps=True, with_output=True)
        assert spec.output is spec.tasks[1]

    def test_success_policy_required_points_to_spec_copy(self) -> None:
        spec = _make_spec(with_success_policy=True)
        assert spec.success_policy is not None
        assert spec.success_policy.cases[0].required[0] is spec.tasks[0]

    def test_workflow_ctx_from_points_to_spec_copy(self) -> None:
        spec = _make_spec(with_deps=True, with_workflow_ctx=True)
        assert spec.tasks[1].workflow_ctx_from is not None
        assert spec.tasks[1].workflow_ctx_from[0] is spec.tasks[0]


# ===========================================================================
# Isolation — originals not mutated
# ===========================================================================


class TestOriginalsNotMutated:
    """Caller-supplied nodes are not mutated by _freeze_graph()."""

    def test_original_kwargs_stays_dict(self) -> None:
        a = TaskNode(fn=fn_a, kwargs={'url': 'https://example.com'})
        _ = WorkflowSpec(name='wf', tasks=[a])
        # Original should still be a plain dict, not MappingProxyType
        assert type(a.kwargs) is dict

    def test_original_not_frozen(self) -> None:
        a = TaskNode(fn=fn_a)
        _ = WorkflowSpec(name='wf', tasks=[a])
        # Original node should still be mutable
        a.queue = 'new_queue'
        assert a.queue == 'new_queue'


# ===========================================================================
# Pre-construction mutability preserved
# ===========================================================================


class TestPreConstructionMutability:
    """Nodes are mutable before being passed to WorkflowSpec."""

    def test_can_set_waits_for_before_construction(self) -> None:
        a = TaskNode(fn=fn_a)
        b = TaskNode(fn=fn_b)
        b.waits_for = [a]
        spec = WorkflowSpec(name='wf', tasks=[a, b])
        assert spec.tasks[1].waits_for[0] is spec.tasks[0]

    def test_can_set_kwargs_before_construction(self) -> None:
        a = TaskNode(fn=fn_a)
        a.kwargs = {'key': 'val'}
        spec = WorkflowSpec(name='wf', tasks=[a])
        assert spec.tasks[0].kwargs['key'] == 'val'


# ===========================================================================
# Shallow kwargs freeze (nested values shared)
# ===========================================================================


class TestShallowKwargsFreeze:
    """Nested mutable values in kwargs are shared by reference (documented)."""

    def test_nested_mutable_value_is_shared(self) -> None:
        inner_list: list[int] = [1, 2, 3]
        a = TaskNode(fn=fn_a, kwargs={'data': inner_list})
        spec = WorkflowSpec(name='wf', tasks=[a])
        # The list itself is the same object (shallow freeze)
        assert spec.tasks[0].kwargs['data'] is inner_list


# ===========================================================================
# Frozen-node reuse regression (Bug 1)
# ===========================================================================


class TestFrozenNodeReuse:
    """Constructing a new spec from an existing spec's frozen tasks must work."""

    def test_spec_from_frozen_tasks(self) -> None:
        """tasks=list(old_spec.tasks) must not raise AttributeError."""
        a = TaskNode(fn=fn_a)
        b = TaskNode(fn=fn_b, waits_for=[a])
        old_spec = WorkflowSpec(name='wf_old', tasks=[a, b])
        assert all(getattr(t, '_frozen', False) for t in old_spec.tasks)

        # This previously raised AttributeError during back-propagation.
        new_spec = WorkflowSpec(name='wf_new', tasks=list(old_spec.tasks))
        assert len(new_spec.tasks) == 2
        # New spec's copies are independent frozen nodes
        assert new_spec.tasks[0] is not old_spec.tasks[0]

    def test_frozen_originals_not_mutated_by_new_spec(self) -> None:
        """Back-propagation must skip frozen originals."""
        a = TaskNode(fn=fn_a, node_id='original_id')
        old_spec = WorkflowSpec(name='wf_old', tasks=[a])
        frozen_a = old_spec.tasks[0]
        old_node_id = frozen_a.node_id

        _ = WorkflowSpec(name='wf_new', tasks=[frozen_a])
        # Frozen original's node_id must not be overwritten
        assert frozen_a.node_id == old_node_id

    def test_reuse_frozen_tasks_preserves_closure_compatibility(self) -> None:
        """Closures capturing earlier nodes still resolve after frozen-task reuse."""
        a = TaskNode(fn=fn_a)
        b = TaskNode(
            fn=fn_c,
            waits_for=[a],
            workflow_ctx_from=[a],
            run_when=lambda ctx: ctx.result_for(a).is_ok(),
        )
        old_spec = WorkflowSpec(name='wf_old', tasks=[a, b])
        new_spec = WorkflowSpec(name='wf_new', tasks=list(old_spec.tasks))

        assert new_spec.tasks[1].run_when is not None
        ctx = WorkflowContext(
            workflow_id='wf',
            task_index=1,
            task_name='task_c',
            results_by_id={new_spec.tasks[0].node_id: TaskResult[int, TaskError](ok=1)},
        )
        assert new_spec.tasks[1].run_when(ctx) is True


# ===========================================================================
# Cached spec metadata regression (Bug 2)
# ===========================================================================


class TestCachedSpecMetadata:
    """build_with() returning a cached/prebuilt spec must still get metadata."""

    def test_cached_spec_gets_workflow_def_metadata(self) -> None:
        """Prebuilt spec returned from build_with() has metadata set."""
        # Prebuilt spec — constructed without ContextVar
        prebuilt = WorkflowSpec(name='cached', tasks=[TaskNode(fn=fn_a)])
        assert prebuilt.workflow_def_module is None

        class CachedWorkflow(WorkflowDefinition[Any]):
            name = 'cached'
            fetch = TaskNode(fn=fn_a)

            @classmethod
            def build_with(cls, app: Any, **params: Any) -> WorkflowSpec[Any]:
                _ = params
                return prebuilt  # returns cached spec

        # Simulate what the metaclass wrapper does
        from horsies.core.models.workflow.definition import _workflow_def_context
        token = _workflow_def_context.set(
            (CachedWorkflow.__module__, CachedWorkflow.__qualname__, CachedWorkflow),
        )
        try:
            # Call the wrapped build_with
            spec = CachedWorkflow.build_with(None)  # type: ignore[arg-type]
        finally:
            _workflow_def_context.reset(token)

        assert spec.workflow_def_module == CachedWorkflow.__module__
        assert spec.workflow_def_qualname == CachedWorkflow.__qualname__
        assert spec.workflow_def_cls is CachedWorkflow

    def test_shared_cached_spec_metadata_updates_per_workflow_class(self) -> None:
        """Shared cached spec must not keep stale metadata from prior class."""
        shared = WorkflowSpec(name='shared', tasks=[TaskNode(fn=fn_a)])

        class WorkflowA(WorkflowDefinition[Any]):
            name = 'a'
            fetch = TaskNode(fn=fn_a)

            @classmethod
            def build_with(cls, app: Any, **params: Any) -> WorkflowSpec[Any]:
                _ = app, params
                return shared

        class WorkflowB(WorkflowDefinition[Any]):
            name = 'b'
            fetch = TaskNode(fn=fn_a)

            @classmethod
            def build_with(cls, app: Any, **params: Any) -> WorkflowSpec[Any]:
                _ = app, params
                return shared

        spec_a = WorkflowA.build_with(None)  # type: ignore[arg-type]
        assert spec_a.workflow_def_cls is WorkflowA

        spec_b = WorkflowB.build_with(None)  # type: ignore[arg-type]
        assert spec_b.workflow_def_module == WorkflowB.__module__
        assert spec_b.workflow_def_qualname == WorkflowB.__qualname__
        assert spec_b.workflow_def_cls is WorkflowB
