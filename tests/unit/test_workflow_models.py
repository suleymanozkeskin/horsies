"""Unit tests for workflow models (no database required)."""

from __future__ import annotations

from typing import Any
from dataclasses import dataclass

import pytest

from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.task_decorator import TaskHandle, TaskFunction, NodeFactory
from horsies.core.models.workflow import (
    NODE_ID_PATTERN,
    NodeKey,
    TaskNode,
    SubWorkflowNode,
    SubWorkflowRetryMode,
    SubWorkflowSummary,
    WorkflowSpec,
    WorkflowContext,
    WorkflowMeta,
    WorkflowStatus,
    WorkflowTaskStatus,
    OnError,
    WorkflowValidationError,
    SuccessPolicy,
    SuccessCase,
    WorkflowDefinition,
    slugify,
)
from horsies.core.errors import ErrorCode, HorsiesError, MultipleValidationErrors


# =============================================================================
# Mock TaskWrapper for unit tests
# =============================================================================


@dataclass
class MockTaskWrapper(TaskFunction[Any, Any]):
    """Mock TaskWrapper for unit tests."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(self, *args: Any, **kwargs: Any) -> TaskResult[Any, TaskError]:
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def node(
        self,
        *,
        waits_for: Any = None,
        workflow_ctx_from: Any = None,
        args_from: Any = None,
        queue: Any = None,
        priority: Any = None,
        allow_failed_deps: bool = False,
        run_when: Any = None,
        skip_when: Any = None,
        join: Any = 'all',
        min_success: Any = None,
        good_until: Any = None,
        node_id: Any = None,
    ) -> NodeFactory[Any, Any]:
        return NodeFactory(
            fn=self,  # type: ignore[arg-type]
            waits_for=waits_for,
            workflow_ctx_from=workflow_ctx_from,
            args_from=args_from,
            queue=queue,
            priority=priority,
            allow_failed_deps=allow_failed_deps,
            run_when=run_when,
            skip_when=skip_when,
            join=join,
            min_success=min_success,
            good_until=good_until,
            node_id=node_id,
        )


@dataclass
class MockTaskWrapperWithCtx(TaskFunction[Any, Any]):
    """Mock TaskWrapper that accepts workflow_ctx."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(
        self,
        workflow_ctx: WorkflowContext | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> TaskResult[Any, TaskError]:
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def node(
        self,
        *,
        waits_for: Any = None,
        workflow_ctx_from: Any = None,
        args_from: Any = None,
        queue: Any = None,
        priority: Any = None,
        allow_failed_deps: bool = False,
        run_when: Any = None,
        skip_when: Any = None,
        join: Any = 'all',
        min_success: Any = None,
        good_until: Any = None,
        node_id: Any = None,
    ) -> NodeFactory[Any, Any]:
        return NodeFactory(
            fn=self,  # type: ignore[arg-type]
            waits_for=waits_for,
            workflow_ctx_from=workflow_ctx_from,
            args_from=args_from,
            queue=queue,
            priority=priority,
            allow_failed_deps=allow_failed_deps,
            run_when=run_when,
            skip_when=skip_when,
            join=join,
            min_success=min_success,
            good_until=good_until,
            node_id=node_id,
        )


@dataclass
class MockTaskWrapperWithParams(TaskFunction[Any, Any]):
    """Mock TaskWrapper with required parameters for signature validation."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(
        self,
        required: TaskResult[int, TaskError],
        *,
        flag: bool,
    ) -> TaskResult[Any, TaskError]:
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def node(
        self,
        *,
        waits_for: Any = None,
        workflow_ctx_from: Any = None,
        args_from: Any = None,
        queue: Any = None,
        priority: Any = None,
        allow_failed_deps: bool = False,
        run_when: Any = None,
        skip_when: Any = None,
        join: Any = 'all',
        min_success: Any = None,
        good_until: Any = None,
        node_id: Any = None,
    ) -> NodeFactory[Any, Any]:
        return NodeFactory(
            fn=self,  # type: ignore[arg-type]
            waits_for=waits_for,
            workflow_ctx_from=workflow_ctx_from,
            args_from=args_from,
            queue=queue,
            priority=priority,
            allow_failed_deps=allow_failed_deps,
            run_when=run_when,
            skip_when=skip_when,
            join=join,
            min_success=min_success,
            good_until=good_until,
            node_id=node_id,
        )


@dataclass
class MockTaskWrapperWithKwargs(TaskFunction[Any, Any]):
    """Mock TaskWrapper that accepts **kwargs (validation should allow any key)."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(self, **kwargs: Any) -> TaskResult[Any, TaskError]:
        _ = kwargs
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def node(
        self,
        *,
        waits_for: Any = None,
        workflow_ctx_from: Any = None,
        args_from: Any = None,
        queue: Any = None,
        priority: Any = None,
        allow_failed_deps: bool = False,
        run_when: Any = None,
        skip_when: Any = None,
        join: Any = 'all',
        min_success: Any = None,
        good_until: Any = None,
        node_id: Any = None,
    ) -> NodeFactory[Any, Any]:
        return NodeFactory(
            fn=self,  # type: ignore[arg-type]
            waits_for=waits_for,
            workflow_ctx_from=workflow_ctx_from,
            args_from=args_from,
            queue=queue,
            priority=priority,
            allow_failed_deps=allow_failed_deps,
            run_when=run_when,
            skip_when=skip_when,
            join=join,
            min_success=min_success,
            good_until=good_until,
            node_id=node_id,
        )


@dataclass
class MockTaskWrapperWithRequiredMeta(TaskFunction[Any, Any]):
    """Mock TaskWrapper with required workflow_meta for signature validation."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(
        self,
        workflow_meta: WorkflowMeta,
    ) -> TaskResult[Any, TaskError]:
        _ = workflow_meta
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        return TaskHandle('mock')

    def node(
        self,
        *,
        waits_for: Any = None,
        workflow_ctx_from: Any = None,
        args_from: Any = None,
        queue: Any = None,
        priority: Any = None,
        allow_failed_deps: bool = False,
        run_when: Any = None,
        skip_when: Any = None,
        join: Any = 'all',
        min_success: Any = None,
        good_until: Any = None,
        node_id: Any = None,
    ) -> NodeFactory[Any, Any]:
        return NodeFactory(
            fn=self,  # type: ignore[arg-type]
            waits_for=waits_for,
            workflow_ctx_from=workflow_ctx_from,
            args_from=args_from,
            queue=queue,
            priority=priority,
            allow_failed_deps=allow_failed_deps,
            run_when=run_when,
            skip_when=skip_when,
            join=join,
            min_success=min_success,
            good_until=good_until,
            node_id=node_id,
        )


@dataclass
class MockTaskWrapperInt(TaskFunction[Any, int]):
    """Mock producer wrapper with concrete ok-type int."""

    task_name: str
    task_ok_type: Any = int

    def __call__(self, *args: Any, **kwargs: Any) -> TaskResult[int, TaskError]:
        _ = args
        _ = kwargs
        return TaskResult(ok=1)

    def send(self, *args: Any, **kwargs: Any) -> TaskHandle[int]:
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskHandle[int]:
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskHandle[int]:
        _ = delay
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    def node(
        self,
        *,
        waits_for: Any = None,
        workflow_ctx_from: Any = None,
        args_from: Any = None,
        queue: Any = None,
        priority: Any = None,
        allow_failed_deps: bool = False,
        run_when: Any = None,
        skip_when: Any = None,
        join: Any = 'all',
        min_success: Any = None,
        good_until: Any = None,
        node_id: Any = None,
    ) -> NodeFactory[Any, int]:
        return NodeFactory(
            fn=self,  # type: ignore[arg-type]
            waits_for=waits_for,
            workflow_ctx_from=workflow_ctx_from,
            args_from=args_from,
            queue=queue,
            priority=priority,
            allow_failed_deps=allow_failed_deps,
            run_when=run_when,
            skip_when=skip_when,
            join=join,
            min_success=min_success,
            good_until=good_until,
            node_id=node_id,
        )


@dataclass
class MockTaskWrapperWithStringResultParam(TaskFunction[Any, Any]):
    """Mock consumer expecting TaskResult[str, TaskError] for args_from."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(
        self,
        required: TaskResult[str, TaskError],
        *,
        flag: bool,
    ) -> TaskResult[Any, TaskError]:
        _ = required
        _ = flag
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        _ = delay
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    def node(
        self,
        *,
        waits_for: Any = None,
        workflow_ctx_from: Any = None,
        args_from: Any = None,
        queue: Any = None,
        priority: Any = None,
        allow_failed_deps: bool = False,
        run_when: Any = None,
        skip_when: Any = None,
        join: Any = 'all',
        min_success: Any = None,
        good_until: Any = None,
        node_id: Any = None,
    ) -> NodeFactory[Any, Any]:
        return NodeFactory(
            fn=self,  # type: ignore[arg-type]
            waits_for=waits_for,
            workflow_ctx_from=workflow_ctx_from,
            args_from=args_from,
            queue=queue,
            priority=priority,
            allow_failed_deps=allow_failed_deps,
            run_when=run_when,
            skip_when=skip_when,
            join=join,
            min_success=min_success,
            good_until=good_until,
            node_id=node_id,
        )


@dataclass
class MockTaskWrapperWithRawParam(TaskFunction[Any, Any]):
    """Mock consumer with non-TaskResult annotation for args_from validation."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(
        self,
        required: int,
        *,
        flag: bool,
    ) -> TaskResult[Any, TaskError]:
        _ = required
        _ = flag
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        _ = delay
        _ = args
        _ = kwargs
        return TaskHandle('mock')

    def node(
        self,
        *,
        waits_for: Any = None,
        workflow_ctx_from: Any = None,
        args_from: Any = None,
        queue: Any = None,
        priority: Any = None,
        allow_failed_deps: bool = False,
        run_when: Any = None,
        skip_when: Any = None,
        join: Any = 'all',
        min_success: Any = None,
        good_until: Any = None,
        node_id: Any = None,
    ) -> NodeFactory[Any, Any]:
        return NodeFactory(
            fn=self,  # type: ignore[arg-type]
            waits_for=waits_for,
            workflow_ctx_from=workflow_ctx_from,
            args_from=args_from,
            queue=queue,
            priority=priority,
            allow_failed_deps=allow_failed_deps,
            run_when=run_when,
            skip_when=skip_when,
            join=join,
            min_success=min_success,
            good_until=good_until,
            node_id=node_id,
        )


# =============================================================================
# TaskNode Tests
# =============================================================================


@pytest.mark.unit
class TestTaskNode:
    """Tests for TaskNode dataclass."""

    def test_tasknode_basic_construction(self) -> None:
        """TaskNode can be constructed with fn, args, kwargs."""
        mock_fn = MockTaskWrapper(task_name='test_task')
        node = TaskNode(
            fn=mock_fn,
            args=(1, 2),
            kwargs={'key': 'value'},
        )

        assert node.fn == mock_fn
        assert node.args == (1, 2)
        assert node.kwargs == {'key': 'value'}

    def test_tasknode_name_property(self) -> None:
        """name property returns fn.task_name."""
        mock_fn = MockTaskWrapper(task_name='my_task_name')
        node = TaskNode(fn=mock_fn)

        assert node.name == 'my_task_name'

    def test_tasknode_defaults(self) -> None:
        """Default values for optional fields."""
        mock_fn = MockTaskWrapper(task_name='test')
        node = TaskNode(fn=mock_fn)

        assert node.args == ()
        assert node.kwargs == {}
        assert node.waits_for == []
        assert node.args_from == {}
        assert node.workflow_ctx_from is None
        assert node.queue is None
        assert node.priority is None
        assert node.index is None

    def test_tasknode_allow_failed_deps_default(self) -> None:
        """allow_failed_deps defaults to False."""
        mock_fn = MockTaskWrapper(task_name='test')
        node = TaskNode(fn=mock_fn)

        assert node.allow_failed_deps is False

    def test_tasknode_allow_failed_deps_true(self) -> None:
        """allow_failed_deps can be set to True."""
        mock_fn = MockTaskWrapper(task_name='test')
        node = TaskNode(fn=mock_fn, allow_failed_deps=True)

        assert node.allow_failed_deps is True


# =============================================================================
# WorkflowSpec Validation Tests
# =============================================================================


@pytest.mark.unit
class TestWorkflowSpecValidation:
    """Tests for WorkflowSpec DAG validation."""

    def test_valid_linear_dag(self) -> None:
        """A -> B -> C passes validation."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_c = MockTaskWrapperWithCtx(task_name='task_c')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b, waits_for=[node_a])
        node_c = TaskNode(fn=fn_c, waits_for=[node_b])

        # Should not raise
        spec = WorkflowSpec(name='linear', tasks=[node_a, node_b, node_c])
        assert len(spec.tasks) == 3

    def test_valid_diamond_dag(self) -> None:
        """A -> [B, C] -> D passes validation."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_c = MockTaskWrapperWithCtx(task_name='task_c')
        fn_d = MockTaskWrapper(task_name='task_d')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b, waits_for=[node_a])
        node_c = TaskNode(fn=fn_c, waits_for=[node_a])
        node_d = TaskNode(fn=fn_d, waits_for=[node_b, node_c])

        spec = WorkflowSpec(name='diamond', tasks=[node_a, node_b, node_c, node_d])
        assert len(spec.tasks) == 4

    def test_no_roots_raises(self) -> None:
        """All tasks have deps raises error with WORKFLOW_NO_ROOT_TASKS code."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        # Create circular dependency manually (both depend on each other)
        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b)

        # Manually set deps to create no-root scenario
        node_a.waits_for = [node_b]
        node_b.waits_for = [node_a]

        # Mutual deps trigger both no_roots and cycle → MultipleValidationErrors
        with pytest.raises(MultipleValidationErrors) as exc_info:
            WorkflowSpec(name='no_roots', tasks=[node_a, node_b])

        codes = [e.code for e in exc_info.value.report.errors]
        assert ErrorCode.WORKFLOW_NO_ROOT_TASKS in codes
        assert ErrorCode.WORKFLOW_CYCLE_DETECTED in codes

    def test_cycle_detection_simple(self) -> None:
        """A -> B -> A raises MultipleValidationErrors with cycle and no-root codes."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b, waits_for=[node_a])

        # Create cycle: A waits for B
        node_a.waits_for = [node_b]

        with pytest.raises(MultipleValidationErrors) as exc_info:
            WorkflowSpec(name='cycle', tasks=[node_a, node_b])

        codes = [e.code for e in exc_info.value.report.errors]
        assert ErrorCode.WORKFLOW_NO_ROOT_TASKS in codes
        assert ErrorCode.WORKFLOW_CYCLE_DETECTED in codes

    def test_cycle_detection_indirect(self) -> None:
        """A -> B -> C -> A raises MultipleValidationErrors with cycle and no-root codes."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_c = MockTaskWrapperWithCtx(task_name='task_c')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b, waits_for=[node_a])
        node_c = TaskNode(fn=fn_c, waits_for=[node_b])

        # Create cycle: A waits for C
        node_a.waits_for = [node_c]

        with pytest.raises(MultipleValidationErrors) as exc_info:
            WorkflowSpec(name='indirect_cycle', tasks=[node_a, node_b, node_c])

        codes = [e.code for e in exc_info.value.report.errors]
        assert ErrorCode.WORKFLOW_NO_ROOT_TASKS in codes
        assert ErrorCode.WORKFLOW_CYCLE_DETECTED in codes

    def test_invalid_dep_reference(self) -> None:
        """Dep not in tasks list raises WORKFLOW_INVALID_DEPENDENCY."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_external = MockTaskWrapper(task_name='external')

        node_a = TaskNode(fn=fn_a)
        node_external = TaskNode(fn=fn_external)
        node_b = TaskNode(fn=fn_b, waits_for=[node_external])

        # node_external is NOT in tasks list
        with pytest.raises(WorkflowValidationError, match='not in workflow') as exc_info:
            WorkflowSpec(name='invalid_dep', tasks=[node_a, node_b])

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_DEPENDENCY

    def test_args_from_not_in_waits_for(self) -> None:
        """args_from ref not in waits_for raises WORKFLOW_INVALID_ARGS_FROM."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_c = MockTaskWrapperWithCtx(task_name='task_c')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b)
        # node_c has args_from pointing to node_a, but only waits_for node_b
        node_c = TaskNode(
            fn=fn_c,
            waits_for=[node_b],
            args_from={'input': node_a},
        )

        with pytest.raises(WorkflowValidationError, match='not in waits_for') as exc_info:
            WorkflowSpec(name='bad_args_from', tasks=[node_a, node_b, node_c])

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_ARGS_FROM

    def test_workflow_ctx_from_not_in_waits_for(self) -> None:
        """ctx_from ref not in waits_for raises WORKFLOW_INVALID_CTX_FROM."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        # Use MockTaskWrapperWithCtx so only the waits_for check fires
        # (not also CTX_PARAM_MISSING for lacking workflow_ctx param)
        fn_c = MockTaskWrapperWithCtx(task_name='task_c')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b)
        # node_c has workflow_ctx_from pointing to node_a, but only waits_for node_b
        node_c = TaskNode(
            fn=fn_c,
            waits_for=[node_b],
            workflow_ctx_from=[node_a],
        )

        with pytest.raises(WorkflowValidationError, match='not in waits_for') as exc_info:
            WorkflowSpec(name='bad_ctx_from', tasks=[node_a, node_b, node_c])

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_CTX_FROM

    def test_workflow_ctx_from_requires_workflow_ctx_param(self) -> None:
        """workflow_ctx_from requires workflow_ctx param → WORKFLOW_CTX_PARAM_MISSING."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_with_ctx = MockTaskWrapperWithCtx(task_name='task_with_ctx')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_with_ctx,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )
        WorkflowSpec(name='ctx_param_ok', tasks=[node_a, node_b])

        node_c = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        with pytest.raises(
            WorkflowValidationError, match='missing workflow_ctx param',
        ) as exc_info:
            WorkflowSpec(name='ctx_param_missing', tasks=[node_a, node_c])

        assert exc_info.value.code == ErrorCode.WORKFLOW_CTX_PARAM_MISSING

    def test_args_with_args_from_rejected(self) -> None:
        """args cannot be used when args_from is set."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            args=(1,),
            waits_for=[node_a],
            args_from={'input': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='args_with_args_from', tasks=[node_a, node_b])

        assert exc.value.code == ErrorCode.WORKFLOW_ARGS_WITH_INJECTION

    def test_args_with_workflow_ctx_from_rejected(self) -> None:
        """args cannot be used when workflow_ctx_from is set."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_with_ctx = MockTaskWrapperWithCtx(task_name='task_with_ctx')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_with_ctx,
            args=(1,),
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='args_with_ctx_from', tasks=[node_a, node_b])

        assert exc.value.code == ErrorCode.WORKFLOW_ARGS_WITH_INJECTION

    def test_subworkflow_args_with_args_from_rejected(self) -> None:
        """SubWorkflowNode args cannot be used when args_from is set."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_args_with_args_from'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node_a = TaskNode(fn=fn_a)
        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            args=(1,),
            waits_for=[node_a],
            args_from={'value': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(
                name='subworkflow_args_with_args_from',
                tasks=[node_a, child_node],
            )

        assert exc.value.code == ErrorCode.WORKFLOW_ARGS_WITH_INJECTION

    def test_subworkflow_params_require_overridden_build_with(self) -> None:
        """Default build_with cannot be parameterized via kwargs."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_default_build_with'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            kwargs={'x': 1},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='subworkflow_default_build_with_kwargs', tasks=[child_node])

        assert exc.value.code == ErrorCode.WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH

    def test_subworkflow_inherited_default_build_with_requires_override(self) -> None:
        """Inherited default build_with must still reject runtime params (E022)."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class BaseChildWorkflow(WorkflowDefinition[int]):
            name = 'base_child_default_build_with'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        BaseChildWorkflow.Meta.output = BaseChildWorkflow.child

        class InheritedDefaultWorkflow(BaseChildWorkflow):
            name = 'inherited_default_build_with'
            child2 = TaskNode(fn=fn_a)

            class Meta:
                output = None

        InheritedDefaultWorkflow.Meta.output = InheritedDefaultWorkflow.child2

        child_node = SubWorkflowNode(
            workflow_def=InheritedDefaultWorkflow,
            kwargs={'x': 1},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(
                name='subworkflow_inherited_default_build_with_kwargs',
                tasks=[child_node],
            )

        assert exc.value.code == ErrorCode.WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH

    def test_subworkflow_default_build_with_no_params_allowed(self) -> None:
        """Default build_with remains valid when no params are passed."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_default_build_with_no_params'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        child_node = SubWorkflowNode(workflow_def=ChildWorkflow)

        spec = WorkflowSpec(name='subworkflow_default_build_with_no_params_ok', tasks=[child_node])
        assert len(spec.tasks) == 1

    def test_subworkflow_inherited_custom_build_with_missing_param_rejected(self) -> None:
        """Inherited custom build_with keeps required-param validation (E020)."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class BaseChildWorkflow(WorkflowDefinition[int]):
            name = 'base_child_custom_build_with_missing'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app: Any, value: int) -> WorkflowSpec:
                _ = value
                return cls.build(app)

        BaseChildWorkflow.Meta.output = BaseChildWorkflow.child

        class InheritedCustomWorkflow(BaseChildWorkflow):
            name = 'inherited_custom_build_with_missing'
            child2 = TaskNode(fn=fn_a)

            class Meta:
                output = None

        InheritedCustomWorkflow.Meta.output = InheritedCustomWorkflow.child2

        child_node = SubWorkflowNode(
            workflow_def=InheritedCustomWorkflow,
            kwargs={},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(
                name='subworkflow_inherited_custom_build_with_missing',
                tasks=[child_node],
            )

        assert exc.value.code == ErrorCode.WORKFLOW_MISSING_REQUIRED_PARAMS

    def test_subworkflow_inherited_custom_build_with_invalid_kwarg_rejected(self) -> None:
        """Inherited custom build_with keeps kwarg-key validation (E019)."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class BaseChildWorkflow(WorkflowDefinition[int]):
            name = 'base_child_custom_build_with_invalid_key'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app: Any, value: int) -> WorkflowSpec:
                _ = value
                return cls.build(app)

        BaseChildWorkflow.Meta.output = BaseChildWorkflow.child

        class InheritedCustomWorkflow(BaseChildWorkflow):
            name = 'inherited_custom_build_with_invalid_key'
            child2 = TaskNode(fn=fn_a)

            class Meta:
                output = None

        InheritedCustomWorkflow.Meta.output = InheritedCustomWorkflow.child2

        child_node = SubWorkflowNode(
            workflow_def=InheritedCustomWorkflow,
            kwargs={'value': 1, 'typo': 2},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(
                name='subworkflow_inherited_custom_build_with_invalid_key',
                tasks=[child_node],
            )

        assert exc.value.code == ErrorCode.WORKFLOW_INVALID_KWARG_KEY

    def test_subworkflow_build_with_duplicate_positional_and_kwarg_rejected(self) -> None:
        """Subworkflow build_with cannot receive same param via args and kwargs."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_build_with_duplicate_binding'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app: Any, value: int) -> WorkflowSpec:
                _ = value
                return cls.build(app)

        ChildWorkflow.Meta.output = ChildWorkflow.child

        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            args=(1,),
            kwargs={'value': 2},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='subworkflow_duplicate_binding', tasks=[child_node])

        assert exc.value.code == ErrorCode.WORKFLOW_SUBWORKFLOW_BUILD_WITH_BINDING

    def test_subworkflow_build_with_too_many_positional_rejected(self) -> None:
        """Subworkflow build_with rejects extra positional args."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_build_with_too_many_positional'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app: Any, value: int) -> WorkflowSpec:
                _ = value
                return cls.build(app)

        ChildWorkflow.Meta.output = ChildWorkflow.child

        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            args=(1, 2),
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='subworkflow_too_many_positional', tasks=[child_node])

        assert exc.value.code == ErrorCode.WORKFLOW_EXCESS_POSITIONAL_ARGS

    def test_invalid_args_from_key_rejected(self) -> None:
        """args_from key must match function parameter name."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapperWithParams(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            kwargs={'flag': True},
            args_from={'required': node_a, 'typo': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='invalid_args_from_key', tasks=[node_a, node_b])

        assert exc.value.code == ErrorCode.WORKFLOW_INVALID_KWARG_KEY

    def test_kwargs_args_from_overlap_rejected(self) -> None:
        """Overlapping kwargs and args_from keys raises E021."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapperWithParams(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            kwargs={'required': 42, 'flag': True},
            args_from={'required': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='overlap_rejected', tasks=[node_a, node_b])

        assert exc.value.code == ErrorCode.WORKFLOW_KWARGS_ARGS_FROM_OVERLAP

    def test_kwargs_args_from_disjoint_accepted(self) -> None:
        """Disjoint kwargs and args_from keys pass validation."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapperWithParams(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            kwargs={'flag': True},
            args_from={'required': node_a},
        )

        spec = WorkflowSpec(name='disjoint_ok', tasks=[node_a, node_b])
        assert len(spec.tasks) == 2

    def test_args_from_type_mismatch_rejected(self) -> None:
        """args_from source ok-type must match consumer TaskResult ok-type."""
        fn_a = MockTaskWrapperInt(task_name='task_a_int')
        fn_b = MockTaskWrapperWithStringResultParam(task_name='task_b_expects_str')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            kwargs={'flag': True},
            args_from={'required': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='args_from_type_mismatch', tasks=[node_a, node_b])

        assert exc.value.code == ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH

    def test_args_from_requires_taskresult_param_annotation(self) -> None:
        """args_from target param must be annotated as TaskResult[OkT, TaskError]."""
        fn_a = MockTaskWrapperInt(task_name='task_a_int')
        fn_b = MockTaskWrapperWithRawParam(task_name='task_b_raw_param')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            kwargs={'flag': True},
            args_from={'required': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='args_from_raw_param', tasks=[node_a, node_b])

        assert exc.value.code == ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH

    def test_args_from_type_match_accepted(self) -> None:
        """args_from passes when producer and consumer TaskResult ok-types match."""
        fn_a = MockTaskWrapperInt(task_name='task_a_int')
        fn_b = MockTaskWrapperWithParams(task_name='task_b_expects_int')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            kwargs={'flag': True},
            args_from={'required': node_a},
        )

        spec = WorkflowSpec(name='args_from_type_match', tasks=[node_a, node_b])
        assert len(spec.tasks) == 2

    def test_subworkflow_kwargs_args_from_overlap_rejected(self) -> None:
        """SubWorkflowNode with overlapping kwargs and args_from raises E021."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_overlap'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app: Any, *, value: int) -> WorkflowSpec:
                _ = value
                return cls.build(app)

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node_a = TaskNode(fn=fn_a)
        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            waits_for=[node_a],
            kwargs={'value': 10},
            args_from={'value': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(
                name='subworkflow_overlap',
                tasks=[node_a, child_node],
            )

        assert exc.value.code == ErrorCode.WORKFLOW_KWARGS_ARGS_FROM_OVERLAP

    def test_task_excess_positional_args_rejected(self) -> None:
        """TaskNode with more positional args than the function accepts raises E026."""
        fn_a = MockTaskWrapperWithParams(task_name='task_a')
        # MockTaskWrapperWithParams.__call__(self, required, *, flag) → 1 positional slot
        # Also provide flag so E020 doesn't fire alongside E026.
        node_a = TaskNode(fn=fn_a, args=(1, 2, 3), kwargs={'flag': True})

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='excess_positional', tasks=[node_a])

        assert exc.value.code == ErrorCode.WORKFLOW_EXCESS_POSITIONAL_ARGS
        assert "provides 3 positional arg(s)" in exc.value.notes[0]
        assert "accepts at most 1" in exc.value.notes[1]

    def test_task_exact_positional_args_accepted(self) -> None:
        """TaskNode with exactly the right number of positional args passes."""
        fn_a = MockTaskWrapperWithParams(task_name='task_a')
        # 1 positional (required) + 1 keyword (flag) → should pass
        node_a = TaskNode(fn=fn_a, args=(1,), kwargs={'flag': True})

        spec = WorkflowSpec(name='exact_positional', tasks=[node_a])
        assert len(spec.tasks) == 1

    def test_task_var_positional_allows_unlimited_args(self) -> None:
        """TaskNode targeting a function with *args accepts any positional count."""
        fn_a = MockTaskWrapper(task_name='task_a')
        # MockTaskWrapper.__call__(self, *args, **kwargs) → unlimited
        node_a = TaskNode(fn=fn_a, args=(1, 2, 3, 4, 5))

        spec = WorkflowSpec(name='var_positional', tasks=[node_a])
        assert len(spec.tasks) == 1

    def test_task_excess_positional_skipped_when_args_from_present(self) -> None:
        """E026 skips when args_from is set — E016 handles that case."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapperWithParams(task_name='task_b')
        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            args=(1, 2, 3),
            waits_for=[node_a],
            args_from={'required': node_a},
        )

        with pytest.raises(
            (WorkflowValidationError, MultipleValidationErrors)
        ) as exc:
            WorkflowSpec(name='excess_with_args_from', tasks=[node_a, node_b])

        if isinstance(exc.value, MultipleValidationErrors):
            codes = {error.code for error in exc.value.report.errors}
        else:
            codes = {exc.value.code}

        assert ErrorCode.WORKFLOW_ARGS_WITH_INJECTION in codes
        assert ErrorCode.WORKFLOW_EXCESS_POSITIONAL_ARGS not in codes

    def test_missing_required_params_rejected(self) -> None:
        """Missing required parameters raises validation error."""
        fn_a = MockTaskWrapperWithParams(task_name='task_a')
        node_a = TaskNode(fn=fn_a)

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='missing_required', tasks=[node_a])

        assert exc.value.code == ErrorCode.WORKFLOW_MISSING_REQUIRED_PARAMS

    def test_required_workflow_meta_treated_as_injected(self) -> None:
        """Required workflow_meta is treated as auto-injected by validator."""
        fn_a = MockTaskWrapperWithRequiredMeta(task_name='task_requires_meta')
        node_a = TaskNode(fn=fn_a)

        # Should not raise WORKFLOW_MISSING_REQUIRED_PARAMS
        spec = WorkflowSpec(name='required_workflow_meta_ok', tasks=[node_a])
        assert len(spec.tasks) == 1

    def test_subworkflow_missing_required_params_rejected(self) -> None:
        """SubWorkflowNode build_with required params must be provided."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_missing_required'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app, *, source: str) -> WorkflowSpec:
                _ = source
                return cls.build(app)

        ChildWorkflow.Meta.output = ChildWorkflow.child

        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='subworkflow_missing_required', tasks=[child_node])

        assert exc.value.code == ErrorCode.WORKFLOW_MISSING_REQUIRED_PARAMS

    def test_subworkflow_invalid_kwargs_key_rejected(self) -> None:
        """SubWorkflowNode kwargs keys must match build_with signature."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_invalid_kwargs'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app, *, source: str) -> WorkflowSpec:
                _ = source
                return cls.build(app)

        ChildWorkflow.Meta.output = ChildWorkflow.child

        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            kwargs={'source': 'ok', 'typo': 'bad'},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='subworkflow_invalid_kwargs', tasks=[child_node])

        assert exc.value.code == ErrorCode.WORKFLOW_INVALID_KWARG_KEY

    def test_subworkflow_invalid_args_from_key_rejected(self) -> None:
        """SubWorkflowNode args_from keys must match build_with signature."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_invalid_args_from'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app, *, source: str) -> WorkflowSpec:
                _ = source
                return cls.build(app)

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node_a = TaskNode(fn=fn_a)
        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            waits_for=[node_a],
            kwargs={'source': 'ok'},
            args_from={'typo': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='subworkflow_invalid_args_from', tasks=[node_a, child_node])

        assert exc.value.code == ErrorCode.WORKFLOW_INVALID_KWARG_KEY

    def test_subworkflow_args_from_type_mismatch_rejected(self) -> None:
        """SubWorkflowNode args_from must match build_with TaskResult ok-type annotations."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_type_mismatch'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(
                cls, app: Any, *, source: TaskResult[str, TaskError]
            ) -> WorkflowSpec:
                _ = source
                return cls.build(app)

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node_a = TaskNode(fn=MockTaskWrapperInt(task_name='producer_int'))
        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            waits_for=[node_a],
            args_from={'source': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            WorkflowSpec(name='subworkflow_args_from_type_mismatch', tasks=[node_a, child_node])

        assert exc.value.code == ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH

    def test_kwargs_validation_skipped_for_kwargs_task(self) -> None:
        """Tasks accepting **kwargs should skip key validation."""
        fn_a = MockTaskWrapperWithKwargs(task_name='task_a')
        node_a = TaskNode(
            fn=fn_a,
            kwargs={'unknown': 1, 'another': 2},
        )

        # Should not raise
        WorkflowSpec(name='kwargs_task_ok', tasks=[node_a])

    def test_subworkflow_kwargs_validation_skipped_for_kwargs_build_with(self) -> None:
        """Subworkflow build_with(**kwargs) should skip key validation."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_kwargs_build_with'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

            @classmethod
            def build_with(cls, app, **kwargs: Any) -> WorkflowSpec:
                _ = kwargs
                return cls.build(app)

        ChildWorkflow.Meta.output = ChildWorkflow.child

        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            kwargs={'unknown': 1},
            args_from={},
        )

        # Should not raise
        WorkflowSpec(name='subworkflow_kwargs_ok', tasks=[child_node])

    def test_output_not_in_workflow(self) -> None:
        """output task not in tasks raises E011."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_external = MockTaskWrapper(task_name='external')

        node_a = TaskNode(fn=fn_a)
        node_external = TaskNode(fn=fn_external)

        with pytest.raises(WorkflowValidationError, match='not in workflow') as exc_info:
            WorkflowSpec(name='bad_output', tasks=[node_a], output=node_external)

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_OUTPUT

    def test_index_assignment(self) -> None:
        """index assigned based on list position."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_c = MockTaskWrapper(task_name='task_c')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b, waits_for=[node_a])
        node_c = TaskNode(fn=fn_c, waits_for=[node_b])

        spec = WorkflowSpec(name='indexed', tasks=[node_a, node_b, node_c])

        assert spec.tasks[0].index == 0
        assert spec.tasks[1].index == 1
        assert spec.tasks[2].index == 2

    def test_node_id_auto_assigned(self) -> None:
        """node_id is auto-assigned when not provided."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b, waits_for=[node_a])

        spec = WorkflowSpec(name='node_id_auto', tasks=[node_a, node_b])
        assert spec.tasks[0].node_id == 'node_id_auto:0'
        assert spec.tasks[1].node_id == 'node_id_auto:1'

    def test_node_id_duplicate_raises(self) -> None:
        """Duplicate node_id raises WORKFLOW_DUPLICATE_NODE_ID."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a, node_id='dup')
        node_b = TaskNode(fn=fn_b, node_id='dup')

        with pytest.raises(WorkflowValidationError, match='duplicate node_id') as exc_info:
            WorkflowSpec(name='dup_ids', tasks=[node_a, node_b])

        assert exc_info.value.code == ErrorCode.WORKFLOW_DUPLICATE_NODE_ID

    def test_node_id_invalid_characters_raises(self) -> None:
        """Invalid node_id characters raise WORKFLOW_INVALID_NODE_ID."""
        fn_a = MockTaskWrapper(task_name='task_a')
        node_a = TaskNode(fn=fn_a, node_id='bad id')

        with pytest.raises(
            WorkflowValidationError, match='contains invalid characters',
        ) as exc_info:
            WorkflowSpec(name='bad_id', tasks=[node_a])

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_NODE_ID

    def test_success_policy_empty_cases_raises(self) -> None:
        """SuccessPolicy with empty cases list raises E012."""
        fn_a = MockTaskWrapper(task_name='task_a')
        node_a = TaskNode(fn=fn_a)

        policy = SuccessPolicy(cases=[])

        with pytest.raises(WorkflowValidationError, match='at least one SuccessCase') as exc_info:
            WorkflowSpec(name='empty_cases', tasks=[node_a], success_policy=policy)

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY

    def test_success_policy_empty_required_raises(self) -> None:
        """SuccessCase with empty required list raises E012."""
        fn_a = MockTaskWrapper(task_name='task_a')
        node_a = TaskNode(fn=fn_a)

        policy = SuccessPolicy(cases=[SuccessCase(required=[])])

        with pytest.raises(WorkflowValidationError, match='has no required tasks') as exc_info:
            WorkflowSpec(name='empty_required', tasks=[node_a], success_policy=policy)

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY

    def test_success_policy_invalid_task_raises(self) -> None:
        """SuccessCase referencing task not in workflow raises E012."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b)  # Not in tasks list

        policy = SuccessPolicy(cases=[SuccessCase(required=[node_b])])

        with pytest.raises(WorkflowValidationError, match='not in workflow') as exc_info:
            WorkflowSpec(
                name='invalid_policy_task', tasks=[node_a], success_policy=policy
            )

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY

    def test_success_policy_valid(self) -> None:
        """Valid success policy passes validation."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b)

        policy = SuccessPolicy(
            cases=[SuccessCase(required=[node_a])],
            optional=[node_b],
        )

        spec = WorkflowSpec(
            name='valid_policy',
            tasks=[node_a, node_b],
            success_policy=policy,
        )
        assert spec.success_policy is not None
        assert len(spec.success_policy.cases) == 1

    def test_quorum_missing_min_success_raises(self) -> None:
        """join='quorum' without min_success raises error."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            join='quorum',
            # min_success not set
        )

        with pytest.raises(WorkflowValidationError, match='min_success is not set'):
            WorkflowSpec(name='quorum_no_min', tasks=[node_a, node_b])

    def test_quorum_min_success_too_high_raises(self) -> None:
        """min_success exceeding dependency count raises error."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            join='quorum',
            min_success=5,  # Only 1 dep, but min_success is 5
        )

        with pytest.raises(WorkflowValidationError, match='exceeds dependency count'):
            WorkflowSpec(name='quorum_too_high', tasks=[node_a, node_b])

    def test_quorum_min_success_zero_raises(self) -> None:
        """min_success < 1 raises error."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            join='quorum',
            min_success=0,
        )

        with pytest.raises(WorkflowValidationError, match='must be >= 1'):
            WorkflowSpec(name='quorum_zero', tasks=[node_a, node_b])

    def test_all_join_with_min_success_raises(self) -> None:
        """min_success set with join='all' raises error."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            join='all',
            min_success=1,  # Invalid - min_success only for quorum
        )

        with pytest.raises(
            WorkflowValidationError, match="only used with join='quorum'"
        ):
            WorkflowSpec(name='all_with_min', tasks=[node_a, node_b])

    def test_any_join_with_min_success_raises(self) -> None:
        """min_success set with join='any' raises error."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(
            fn=fn_b,
            waits_for=[node_a],
            join='any',
            min_success=1,  # Invalid - min_success only for quorum
        )

        with pytest.raises(
            WorkflowValidationError, match="only used with join='quorum'"
        ):
            WorkflowSpec(name='any_with_min', tasks=[node_a, node_b])

    def test_valid_quorum_join(self) -> None:
        """Valid quorum configuration passes validation."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_c = MockTaskWrapper(task_name='task_c')
        fn_d = MockTaskWrapper(task_name='task_d')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b)
        node_c = TaskNode(fn=fn_c)
        node_d = TaskNode(
            fn=fn_d,
            waits_for=[node_a, node_b, node_c],
            join='quorum',
            min_success=2,  # 2 out of 3
        )

        spec = WorkflowSpec(name='valid_quorum', tasks=[node_a, node_b, node_c, node_d])
        assert spec.tasks[3].join == 'quorum'
        assert spec.tasks[3].min_success == 2

    def test_valid_any_join(self) -> None:
        """Valid any join configuration passes validation."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_c = MockTaskWrapper(task_name='task_c')

        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b)
        node_c = TaskNode(
            fn=fn_c,
            waits_for=[node_a, node_b],
            join='any',
        )

        spec = WorkflowSpec(name='valid_any', tasks=[node_a, node_b, node_c])
        assert spec.tasks[2].join == 'any'
        assert spec.tasks[2].min_success is None


# =============================================================================
# WorkflowContext Tests
# =============================================================================


@pytest.mark.unit
class TestWorkflowContext:
    """Tests for WorkflowContext with type-safe result_for(node) access."""

    def test_from_serialized_reconstruction(self) -> None:
        """from_serialized() creates valid WorkflowContext."""
        result_0: TaskResult[int, TaskError] = TaskResult(ok=42)
        results_by_id: dict[str, TaskResult[Any, TaskError]] = {'node-0': result_0}

        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=2,
            task_name='my_task',
            results_by_id=results_by_id,
        )

        assert ctx.workflow_id == 'wf-123'
        assert ctx.task_index == 2
        assert ctx.task_name == 'my_task'

    def test_result_for_returns_correct_result(self) -> None:
        """result_for(node) returns the correct TaskResult."""
        result_0: TaskResult[int, TaskError] = TaskResult(ok=42)
        results_by_id: dict[str, TaskResult[Any, TaskError]] = {'node-0': result_0}

        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id=results_by_id,
        )

        # Create a mock node with node_id set
        mock_task = MockTaskWrapper('producer')
        node: TaskNode[int] = TaskNode(fn=mock_task, args=(1,))
        node.node_id = 'node-0'  # Simulate WorkflowSpec having assigned node_id

        result = ctx.result_for(node)
        assert result.is_ok()
        assert result.unwrap() == 42

    def test_result_for_node_key(self) -> None:
        """result_for(NodeKey) returns the correct TaskResult."""
        result_0: TaskResult[int, TaskError] = TaskResult(ok=7)
        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={'node-0': result_0},
        )

        key = NodeKey[int]('node-0')
        result = ctx.result_for(key)
        assert result.is_ok()
        assert result.unwrap() == 7

    def test_result_for_raises_on_missing_id(self) -> None:
        """result_for(node) raises RuntimeError if node has no node_id."""
        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={},
        )

        mock_task = MockTaskWrapper('producer')
        node: TaskNode[int] = TaskNode(fn=mock_task, args=(1,))
        # node.node_id is None (not assigned)

        with pytest.raises(RuntimeError, match='TaskNode node_id is not set'):
            ctx.result_for(node)

    def test_result_for_raises_on_missing_result(self) -> None:
        """result_for(node) raises KeyError if result not in context."""
        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={},  # Empty
        )

        mock_task = MockTaskWrapper('producer')
        node: TaskNode[int] = TaskNode(fn=mock_task, args=(1,))
        node.node_id = 'node-0'

        with pytest.raises(KeyError, match='not in workflow context'):
            ctx.result_for(node)

    def test_has_result(self) -> None:
        """has_result(node) returns True if result exists."""
        result_0: TaskResult[int, TaskError] = TaskResult(ok=42)
        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={'node-0': result_0},
        )

        mock_task = MockTaskWrapper('producer')
        node_with_result: TaskNode[int] = TaskNode(fn=mock_task, args=(1,))
        node_with_result.node_id = 'node-0'

        node_without_result: TaskNode[int] = TaskNode(fn=mock_task, args=(2,))
        node_without_result.node_id = 'node-99'

        node_no_index: TaskNode[int] = TaskNode(fn=mock_task, args=(3,))
        # node_no_index.node_id is None

        assert ctx.has_result(node_with_result) is True
        assert ctx.has_result(node_without_result) is False
        assert ctx.has_result(node_no_index) is False


# =============================================================================
# WorkflowMeta Tests
# =============================================================================


@pytest.mark.unit
class TestWorkflowMeta:
    """Tests for WorkflowMeta dataclass."""

    def test_workflow_meta_creation(self) -> None:
        """WorkflowMeta can be created with all fields."""
        meta = WorkflowMeta(
            workflow_id='wf-123',
            task_index=5,
            task_name='my_task',
        )

        assert meta.workflow_id == 'wf-123'
        assert meta.task_index == 5
        assert meta.task_name == 'my_task'


# =============================================================================
# Enum Tests
# =============================================================================


@pytest.mark.unit
class TestEnums:
    """Tests for workflow enums."""

    def test_workflow_status_values(self) -> None:
        """All status values exist."""
        assert WorkflowStatus.PENDING == 'PENDING'
        assert WorkflowStatus.RUNNING == 'RUNNING'
        assert WorkflowStatus.COMPLETED == 'COMPLETED'
        assert WorkflowStatus.FAILED == 'FAILED'
        assert WorkflowStatus.PAUSED == 'PAUSED'
        assert WorkflowStatus.CANCELLED == 'CANCELLED'

    def test_workflow_task_status_values(self) -> None:
        """All task status values exist."""
        assert WorkflowTaskStatus.PENDING == 'PENDING'
        assert WorkflowTaskStatus.READY == 'READY'
        assert WorkflowTaskStatus.ENQUEUED == 'ENQUEUED'
        assert WorkflowTaskStatus.RUNNING == 'RUNNING'
        assert WorkflowTaskStatus.COMPLETED == 'COMPLETED'
        assert WorkflowTaskStatus.FAILED == 'FAILED'
        assert WorkflowTaskStatus.SKIPPED == 'SKIPPED'

    def test_on_error_values(self) -> None:
        """FAIL and PAUSE exist."""
        assert OnError.FAIL == 'fail'
        assert OnError.PAUSE == 'pause'


# =============================================================================
# WorkflowDefinition Tests
# =============================================================================


@pytest.mark.unit
class TestWorkflowDefinition:
    """Tests for WorkflowDefinition class-based workflow definition."""

    def test_basic_workflow_definition(self) -> None:
        """Basic class-based workflow definition builds WorkflowSpec."""
        from horsies.core.models.workflow import WorkflowDefinition
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        class SimpleWorkflow(WorkflowDefinition):
            name = 'simple'
            fetch = TaskNode(fn=fn_a, args=(1,))
            process = TaskNode(fn=fn_b, waits_for=[fetch], args_from={'data': fetch})

        # Create minimal app (no broker connection needed for build)
        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        spec = SimpleWorkflow.build(app)

        assert spec.name == 'simple'
        assert len(spec.tasks) == 2
        assert spec.on_error == OnError.FAIL

    def test_node_id_from_attribute_name(self) -> None:
        """node_id is auto-assigned from attribute name."""
        from horsies.core.models.workflow import WorkflowDefinition
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        class NodeIdWorkflow(WorkflowDefinition):
            name = 'node_id_test'
            fetch = TaskNode(fn=fn_a)
            parse = TaskNode(fn=fn_b, waits_for=[fetch])

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        spec = NodeIdWorkflow.build(app)

        # node_id should be attribute name, not workflow_name:index
        assert spec.tasks[0].node_id == 'fetch'
        assert spec.tasks[1].node_id == 'parse'

    def test_explicit_node_id_preserved(self) -> None:
        """Explicit node_id is not overwritten."""
        from horsies.core.models.workflow import WorkflowDefinition
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_a = MockTaskWrapper(task_name='task_a')

        class ExplicitIdWorkflow(WorkflowDefinition):
            name = 'explicit_id'
            fetch = TaskNode(fn=fn_a, node_id='my_custom_id')

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        spec = ExplicitIdWorkflow.build(app)

        assert spec.tasks[0].node_id == 'my_custom_id'

    def test_meta_output_configuration(self) -> None:
        """Meta.output sets workflow output."""
        from horsies.core.models.workflow import WorkflowDefinition
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')

        # Define nodes outside class so Meta can reference them
        fetch_node = TaskNode(fn=fn_a)
        process_node = TaskNode(fn=fn_b, waits_for=[fetch_node])

        class OutputWorkflow(WorkflowDefinition):
            name = 'output_test'
            fetch = fetch_node
            process = process_node

            class Meta:
                output = process_node

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        spec = OutputWorkflow.build(app)

        assert spec.output is not None
        assert spec.output.name == 'task_b'

    def test_meta_on_error_configuration(self) -> None:
        """Meta.on_error sets error policy."""
        from horsies.core.models.workflow import WorkflowDefinition
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_a = MockTaskWrapper(task_name='task_a')

        class PauseWorkflow(WorkflowDefinition):
            name = 'pause_test'
            fetch = TaskNode(fn=fn_a)

            class Meta:
                on_error = OnError.PAUSE

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        spec = PauseWorkflow.build(app)

        assert spec.on_error == OnError.PAUSE

    def test_missing_name_raises(self) -> None:
        """Missing name attribute raises WorkflowValidationError."""
        from horsies.core.models.workflow import WorkflowDefinition
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_a = MockTaskWrapper(task_name='task_a')

        class NoNameWorkflow(WorkflowDefinition):
            # name not defined
            fetch = TaskNode(fn=fn_a)

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        with pytest.raises(WorkflowValidationError, match="must define a 'name'"):
            NoNameWorkflow.build(app)

    def test_no_tasks_raises(self) -> None:
        """No TaskNode attributes raises WorkflowValidationError."""
        from horsies.core.models.workflow import WorkflowDefinition
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        class EmptyWorkflow(WorkflowDefinition):
            name = 'empty'
            # No TaskNode attributes

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        with pytest.raises(WorkflowValidationError, match='has no TaskNode'):
            EmptyWorkflow.build(app)

    def test_duplicate_task_function_allowed(self) -> None:
        """Same task function can be used multiple times."""
        from horsies.core.models.workflow import WorkflowDefinition
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_fetch = MockTaskWrapper(task_name='fetch')

        class MultiFetchWorkflow(WorkflowDefinition):
            name = 'multi_fetch'
            fetch_a = TaskNode(fn=fn_fetch, args=('url_a',))
            fetch_b = TaskNode(fn=fn_fetch, args=('url_b',))
            fetch_c = TaskNode(fn=fn_fetch, args=('url_c',))

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        spec = MultiFetchWorkflow.build(app)

        assert len(spec.tasks) == 3
        # Each gets unique node_id from attribute name
        assert spec.tasks[0].node_id == 'fetch_a'
        assert spec.tasks[1].node_id == 'fetch_b'
        assert spec.tasks[2].node_id == 'fetch_c'

    def test_workflow_ctx_from_supported(self) -> None:
        """workflow_ctx_from works in class-based definition."""
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        # fn_c must accept workflow_ctx since audit_node uses workflow_ctx_from
        fn_c = MockTaskWrapperWithCtx(task_name='task_c')

        # Define nodes outside so we can reference them in waits_for and workflow_ctx_from
        fetch_node = TaskNode(fn=fn_a)
        parse_node = TaskNode(fn=fn_b, waits_for=[fetch_node])
        # audit waits for both fetch and parse so workflow_ctx_from is valid
        audit_node = TaskNode(
            fn=fn_c,
            waits_for=[fetch_node, parse_node],
            workflow_ctx_from=[fetch_node, parse_node],
        )

        class CtxWorkflow(WorkflowDefinition):
            name = 'ctx_test'
            fetch = fetch_node
            parse = parse_node
            audit = audit_node

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        spec = CtxWorkflow.build(app)

        assert spec.tasks[2].workflow_ctx_from is not None
        assert len(spec.tasks[2].workflow_ctx_from) == 2

    def test_subworkflow_retry_mode_guard(self) -> None:
        """Unsupported SubWorkflowRetryMode raises validation error."""
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_retry_guard'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        child_node = SubWorkflowNode(
            workflow_def=ChildWorkflow,
            retry_mode=SubWorkflowRetryMode.RERUN_ALL,
        )

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test'
                ),
            )
        )

        with pytest.raises(WorkflowValidationError) as exc:
            app.workflow(
                name='parent_retry_guard',
                tasks=[child_node],
                output=child_node,
            )

        err = exc.value
        assert err.code == ErrorCode.WORKFLOW_INVALID_SUBWORKFLOW_RETRY_MODE


# =============================================================================
# Workflow Output Type Mismatch Tests (E025)
# =============================================================================


@pytest.mark.unit
class TestWorkflowOutputTypeMismatch:
    """Tests for _validate_workflow_generic_output_match (E025)."""

    @staticmethod
    def _make_app() -> Any:
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig

        return Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://test:test@localhost/test',
                ),
            ),
        )

    def test_matching_output_type_accepted(self) -> None:
        """WorkflowDefinition[int] with TaskNode producing int passes."""
        fn_int = MockTaskWrapperInt(task_name='producer_int')
        app = self._make_app()

        class IntWorkflow(WorkflowDefinition[int]):
            name = 'int_workflow'
            produce = TaskNode(fn=fn_int)

            class Meta:
                output = None

        IntWorkflow.Meta.output = IntWorkflow.produce  # type: ignore[assignment]

        # Should not raise
        spec = IntWorkflow.build(app)
        assert spec.output is not None

    def test_mismatched_output_type_rejected(self) -> None:
        """WorkflowDefinition[int] with TaskNode producing str raises E025."""
        fn_str = MockTaskWrapper(task_name='producer_str', task_ok_type=str)
        app = self._make_app()

        class MismatchWorkflow(WorkflowDefinition[int]):
            name = 'mismatch_workflow'
            produce = TaskNode(fn=fn_str)

            class Meta:
                output = None

        MismatchWorkflow.Meta.output = MismatchWorkflow.produce  # type: ignore[assignment]

        with pytest.raises(WorkflowValidationError) as exc:
            MismatchWorkflow.build(app)

        err = exc.value
        assert err.code == ErrorCode.WORKFLOW_OUTPUT_TYPE_MISMATCH
        assert 'int' in err.message
        assert 'str' in err.message

    def test_no_generic_parameter_skips_validation(self) -> None:
        """WorkflowDefinition without generic skips output type check."""
        fn_str = MockTaskWrapper(task_name='producer_str', task_ok_type=str)
        app = self._make_app()

        class UntypedWorkflow(WorkflowDefinition):
            name = 'untyped_workflow'
            produce = TaskNode(fn=fn_str)

            class Meta:
                output = None

        UntypedWorkflow.Meta.output = UntypedWorkflow.produce  # type: ignore[assignment]

        # Should not raise — no generic to compare against
        spec = UntypedWorkflow.build(app)
        assert spec.output is not None

    def test_any_output_type_skips_validation(self) -> None:
        """WorkflowDefinition[Any] with any output skips check."""
        fn_str = MockTaskWrapper(task_name='producer_str', task_ok_type=str)
        app = self._make_app()

        class AnyWorkflow(WorkflowDefinition[Any]):
            name = 'any_workflow'
            produce = TaskNode(fn=fn_str)

            class Meta:
                output = None

        AnyWorkflow.Meta.output = AnyWorkflow.produce  # type: ignore[assignment]

        # Should not raise — Any is compatible with everything
        spec = AnyWorkflow.build(app)
        assert spec.output is not None

    def test_no_output_skips_validation(self) -> None:
        """Workflow without Meta.output skips type check."""
        fn_a = MockTaskWrapper(task_name='task_a')
        app = self._make_app()

        class NoOutputWorkflow(WorkflowDefinition[int]):
            name = 'no_output_workflow'
            task_a = TaskNode(fn=fn_a)

        # Should not raise — no output to validate
        spec = NoOutputWorkflow.build(app)
        assert spec.output is None

    def test_subclass_compatible_output_accepted(self) -> None:
        """Subclass of declared type is compatible."""
        from pydantic import BaseModel

        class Base(BaseModel):
            value: int

        class Derived(Base):
            extra: str

        fn_derived = MockTaskWrapper(task_name='producer_derived', task_ok_type=Derived)
        app = self._make_app()

        class SubclassWorkflow(WorkflowDefinition[Base]):
            name = 'subclass_workflow'
            produce = TaskNode(fn=fn_derived)

            class Meta:
                output = None

        SubclassWorkflow.Meta.output = SubclassWorkflow.produce  # type: ignore[assignment]

        # Derived is subclass of Base — should pass
        spec = SubclassWorkflow.build(app)
        assert spec.output is not None


# =============================================================================
# Multi-Error Collection Tests
# =============================================================================


@pytest.mark.unit
class TestMultiErrorWorkflowCollection:
    """Tests for phase-gated error collection in WorkflowSpec."""

    def test_cycle_collects_no_roots_and_cycle(self) -> None:
        """Circular DAG collects both 'no roots' and 'cycle' errors."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b)
        node_a.waits_for = [node_b]
        node_b.waits_for = [node_a]

        with pytest.raises(MultipleValidationErrors) as exc_info:
            WorkflowSpec(name='cycle_test', tasks=[node_a, node_b])

        errors = exc_info.value.report.errors
        assert len(errors) == 2
        messages = [e.message for e in errors]
        assert 'no root tasks found' in messages
        assert 'cycle detected in workflow DAG' in messages

    def test_multiple_join_errors_collected(self) -> None:
        """Multiple tasks with bad join settings are all reported."""
        fn_root = MockTaskWrapper(task_name='root')
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        root = TaskNode(fn=fn_root)
        node_a = TaskNode(fn=fn_a, waits_for=[root], join='quorum')  # missing min_success
        node_b = TaskNode(fn=fn_b, waits_for=[root], join='all', min_success=3)  # min_success on 'all'

        with pytest.raises(MultipleValidationErrors) as exc_info:
            WorkflowSpec(name='bad_joins', tasks=[root, node_a, node_b])

        errors = exc_info.value.report.errors
        assert len(errors) == 2

    def test_node_id_errors_gate_remaining_validation(self) -> None:
        """Node ID errors prevent subsequent validation from running."""
        fn_a = MockTaskWrapper(task_name='task_a')
        # Explicitly set an invalid node_id
        node_a = TaskNode(fn=fn_a, node_id='invalid chars!')

        with pytest.raises(WorkflowValidationError, match='invalid characters'):
            WorkflowSpec(name='bad_id', tasks=[node_a])

    def test_duplicate_node_ids_collected(self) -> None:
        """Multiple duplicate node_id errors are collected."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        fn_c = MockTaskWrapper(task_name='task_c')
        node_a = TaskNode(fn=fn_a, node_id='shared')
        node_b = TaskNode(fn=fn_b, node_id='shared')
        node_c = TaskNode(fn=fn_c, node_id='shared')

        with pytest.raises(HorsiesError, match='duplicate node_id'):
            WorkflowSpec(name='dup_ids', tasks=[node_a, node_b, node_c])

    def test_single_error_preserves_original_type(self) -> None:
        """Single workflow validation error preserves original type."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_b = MockTaskWrapper(task_name='task_b')
        node_a = TaskNode(fn=fn_a)
        node_b = TaskNode(fn=fn_b, waits_for=[node_a])
        # args_from ref not in waits_for — single error
        fn_c = MockTaskWrapper(task_name='task_c')
        node_c = TaskNode(
            fn=fn_c,
            waits_for=[node_b],
            args_from={'input': node_a},
        )

        with pytest.raises(WorkflowValidationError, match='not in waits_for'):
            WorkflowSpec(name='single_err', tasks=[node_a, node_b, node_c])


# =============================================================================
# TaskNode.key() and SubWorkflowNode.key() Tests
# =============================================================================


@pytest.mark.unit
class TestNodeKey:
    """Tests for TaskNode.key() and SubWorkflowNode.key() methods."""

    def test_tasknode_key_returns_node_key(self) -> None:
        """TaskNode.key() returns NodeKey with correct node_id."""
        fn = MockTaskWrapper(task_name='task_a')
        node = TaskNode(fn=fn, node_id='my-node')

        key = node.key()

        assert isinstance(key, NodeKey)
        assert key.node_id == 'my-node'

    def test_tasknode_key_raises_when_node_id_none(self) -> None:
        """TaskNode.key() raises WorkflowValidationError if node_id is None."""
        fn = MockTaskWrapper(task_name='task_a')
        node = TaskNode(fn=fn)
        # node_id is None by default

        with pytest.raises(WorkflowValidationError, match='node_id is not set'):
            node.key()

    def test_subworkflow_node_key_returns_node_key(self) -> None:
        """SubWorkflowNode.key() returns NodeKey with correct node_id."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_key_test'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow, node_id='sub-node')

        key = node.key()

        assert isinstance(key, NodeKey)
        assert key.node_id == 'sub-node'

    def test_subworkflow_node_key_raises_when_node_id_none(self) -> None:
        """SubWorkflowNode.key() raises WorkflowValidationError if node_id is None."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_key_none'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow)
        # node_id is None by default

        with pytest.raises(WorkflowValidationError, match='node_id is not set'):
            node.key()


# =============================================================================
# SubWorkflowNode Construction Tests
# =============================================================================


@pytest.mark.unit
class TestSubWorkflowNode:
    """Tests for SubWorkflowNode dataclass."""

    def test_defaults(self) -> None:
        """Default values for optional fields."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_defaults'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow)

        assert node.args == ()
        assert node.kwargs == {}
        assert node.waits_for == []
        assert node.args_from == {}
        assert node.workflow_ctx_from is None
        assert node.join == 'all'
        assert node.min_success is None
        assert node.allow_failed_deps is False
        assert node.retry_mode == SubWorkflowRetryMode.RERUN_FAILED_ONLY
        assert node.index is None
        assert node.node_id is None

    def test_name_property(self) -> None:
        """name property returns workflow_def.name."""
        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'my_child_workflow'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow)

        assert node.name == 'my_child_workflow'


# =============================================================================
# WorkflowContext summary_for / has_summary Tests
# =============================================================================


@pytest.mark.unit
class TestWorkflowContextSummary:
    """Tests for WorkflowContext.summary_for() and has_summary()."""

    def test_summary_for_returns_correct_summary(self) -> None:
        """summary_for(node) returns the correct SubWorkflowSummary."""
        summary = SubWorkflowSummary(
            status=WorkflowStatus.COMPLETED,
            success_case=None,
            output=42,
            total_tasks=3,
            completed_tasks=3,
            failed_tasks=0,
            skipped_tasks=0,
        )

        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={},
            summaries_by_id={'sub-node': summary},
        )

        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_summary'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow, node_id='sub-node')

        result = ctx.summary_for(node)

        assert result.status == WorkflowStatus.COMPLETED
        assert result.output == 42
        assert result.total_tasks == 3

    def test_summary_for_raises_on_missing_node_id(self) -> None:
        """summary_for(node) raises RuntimeError if node_id is None."""
        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={},
        )

        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_no_id'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow)
        # node_id is None

        with pytest.raises(RuntimeError, match='node_id is not set'):
            ctx.summary_for(node)

    def test_summary_for_raises_on_missing_summary(self) -> None:
        """summary_for(node) raises KeyError if summary not in context."""
        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={},
            summaries_by_id={},
        )

        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_missing'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow, node_id='sub-node')

        with pytest.raises(KeyError, match='not in workflow context'):
            ctx.summary_for(node)

    def test_has_summary_true(self) -> None:
        """has_summary returns True when summary exists."""
        summary = SubWorkflowSummary(
            status=WorkflowStatus.COMPLETED,
            success_case=None,
            output=1,
            total_tasks=1,
            completed_tasks=1,
            failed_tasks=0,
            skipped_tasks=0,
        )

        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={},
            summaries_by_id={'sub-node': summary},
        )

        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_has_sum'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow, node_id='sub-node')

        assert ctx.has_summary(node) is True

    def test_has_summary_false_missing(self) -> None:
        """has_summary returns False when summary does not exist."""
        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={},
            summaries_by_id={},
        )

        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_no_sum'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow, node_id='sub-node')

        assert ctx.has_summary(node) is False

    def test_has_summary_false_no_node_id(self) -> None:
        """has_summary returns False when node_id is None."""
        ctx = WorkflowContext.from_serialized(
            workflow_id='wf-123',
            task_index=1,
            task_name='consumer',
            results_by_id={},
        )

        fn_a = MockTaskWrapper(task_name='task_a')

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'child_no_id2'
            child = TaskNode(fn=fn_a)

            class Meta:
                output = None

        ChildWorkflow.Meta.output = ChildWorkflow.child

        node = SubWorkflowNode(workflow_def=ChildWorkflow)

        assert ctx.has_summary(node) is False


# =============================================================================
# Enum is_terminal Property Tests
# =============================================================================


@pytest.mark.unit
class TestEnumIsTerminal:
    """Tests for is_terminal property on workflow enums."""

    def test_workflow_status_terminal_states(self) -> None:
        """COMPLETED, FAILED, CANCELLED are terminal."""
        assert WorkflowStatus.COMPLETED.is_terminal is True
        assert WorkflowStatus.FAILED.is_terminal is True
        assert WorkflowStatus.CANCELLED.is_terminal is True

    def test_workflow_status_non_terminal_states(self) -> None:
        """PENDING, RUNNING, PAUSED are not terminal."""
        assert WorkflowStatus.PENDING.is_terminal is False
        assert WorkflowStatus.RUNNING.is_terminal is False
        assert WorkflowStatus.PAUSED.is_terminal is False

    def test_workflow_task_status_terminal_states(self) -> None:
        """COMPLETED, FAILED, SKIPPED are terminal."""
        assert WorkflowTaskStatus.COMPLETED.is_terminal is True
        assert WorkflowTaskStatus.FAILED.is_terminal is True
        assert WorkflowTaskStatus.SKIPPED.is_terminal is True

    def test_workflow_task_status_non_terminal_states(self) -> None:
        """PENDING, READY, ENQUEUED, RUNNING are not terminal."""
        assert WorkflowTaskStatus.PENDING.is_terminal is False
        assert WorkflowTaskStatus.READY.is_terminal is False
        assert WorkflowTaskStatus.ENQUEUED.is_terminal is False
        assert WorkflowTaskStatus.RUNNING.is_terminal is False


# =============================================================================
# Node ID Length Validation Test
# =============================================================================


@pytest.mark.unit
class TestNodeIdLengthValidation:
    """Tests for node_id length limit (128 chars)."""

    def test_node_id_exceeds_128_chars_raises(self) -> None:
        """Explicit node_id > 128 characters raises WORKFLOW_INVALID_NODE_ID."""
        fn_a = MockTaskWrapper(task_name='task_a')
        long_id = 'a' * 129
        node_a = TaskNode(fn=fn_a, node_id=long_id)

        with pytest.raises(WorkflowValidationError) as exc_info:
            WorkflowSpec(name='long_id', tasks=[node_a])

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_NODE_ID


# =============================================================================
# Success Policy Optional Task Validation Test
# =============================================================================


@pytest.mark.unit
class TestSuccessPolicyOptionalValidation:
    """Tests for success_policy optional task validation."""

    def test_optional_task_not_in_workflow_raises(self) -> None:
        """success_policy.optional referencing task not in workflow raises E012."""
        fn_a = MockTaskWrapper(task_name='task_a')
        fn_external = MockTaskWrapper(task_name='external')

        node_a = TaskNode(fn=fn_a)
        node_external = TaskNode(fn=fn_external)

        policy = SuccessPolicy(
            cases=[SuccessCase(required=[node_a])],
            optional=[node_external],  # not in tasks
        )

        with pytest.raises(WorkflowValidationError, match='not in workflow') as exc_info:
            WorkflowSpec(
                name='bad_optional', tasks=[node_a], success_policy=policy,
            )

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY


# =============================================================================
# Slugify Tests
# =============================================================================


@pytest.mark.unit
class TestSlugify:
    """Tests for slugify() function and workflow name handling."""

    def test_replaces_spaces_with_underscores(self) -> None:
        assert slugify('Hello World') == 'Hello_World'
        assert slugify('My Workflow Name') == 'My_Workflow_Name'

    def test_preserves_valid_characters(self) -> None:
        assert slugify('task:v2.0') == 'task:v2.0'
        assert slugify('my-task-name') == 'my-task-name'
        assert slugify('prefix.suffix') == 'prefix.suffix'

    def test_removes_invalid_characters(self) -> None:
        assert slugify('Hello!World') == 'HelloWorld'
        assert slugify('test@#$%chars') == 'testchars'
        assert slugify('parentheses(removed)') == 'parenthesesremoved'

    def test_handles_mixed_input(self) -> None:
        assert slugify('My App: v2.0!') == 'My_App:_v2.0'
        assert slugify('Company Name (Inc.)') == 'Company_Name_Inc.'

    def test_returns_underscore_for_empty_result(self) -> None:
        assert slugify('!@#$%') == '_'
        assert slugify('') == '_'

    def test_output_matches_node_id_pattern(self) -> None:
        """Slugified output always matches NODE_ID_PATTERN."""
        test_inputs = [
            'Hello World',
            'My Company Name GmbH',
            'task:v2.0',
            'special!@#chars',
            'mixed Input-with_stuff.here',
        ]
        for input_str in test_inputs:
            result = slugify(input_str)
            assert NODE_ID_PATTERN.match(result) is not None, (
                f'slugify({input_str!r}) = {result!r} does not match NODE_ID_PATTERN'
            )


# =============================================================================
# NODE_ID_PATTERN Validation Tests
# =============================================================================


@pytest.mark.unit
class TestNodeIdPattern:
    """Tests for NODE_ID_PATTERN regex validation."""

    def test_valid_node_ids(self) -> None:
        valid_ids = [
            'simple',
            'with_underscore',
            'with-hyphen',
            'with:colon',
            'with.dot',
            'MixedCase123',
            'workflow_name:0',
            'my-task.v2:42',
        ]
        for node_id in valid_ids:
            assert NODE_ID_PATTERN.match(node_id) is not None, (
                f'{node_id!r} should be valid'
            )

    def test_invalid_node_ids(self) -> None:
        invalid_ids = [
            'with space',
            'with!exclaim',
            'with@at',
            'with#hash',
            'with$dollar',
            'with%percent',
            '(parentheses)',
            '',
        ]
        for node_id in invalid_ids:
            match = NODE_ID_PATTERN.match(node_id)
            assert match is None, f'{node_id!r} should be invalid'
