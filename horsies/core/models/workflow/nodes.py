"""DAG node types and success policy models."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Callable,
    Literal,
)

from horsies.core.errors import WorkflowValidationError

from .enums import (
    OkT_co,
    SubWorkflowRetryMode,
)

if TYPE_CHECKING:
    from horsies.core.task_decorator import TaskFunction
    from horsies.core.models.tasks import TaskResult, TaskError
    from .context import WorkflowContext
    from .definition import WorkflowDefinition


# =============================================================================
# NodeKey (typed, stable id)
# =============================================================================


@dataclass(frozen=True)
class NodeKey(Generic[OkT_co]):
    """Typed stable identifier for a TaskNode."""

    node_id: str


# =============================================================================
# TaskNode
# =============================================================================


@dataclass
class TaskNode(Generic[OkT_co]):
    """
    A node in the workflow DAG representing a single task execution.

    Generic parameter OkT represents the success type of the task's TaskResult.
    This enables type-safe access to results via WorkflowContext.result_for(node).

    Example:
        ```python
        fetch = TaskNode(fn=fetch_data, kwargs={"url": "https://example.com"})
        process = TaskNode(
            fn=process_data,
            waits_for=[fetch],           # wait for fetch to be terminal
            args_from={"raw": fetch},     # inject fetch's TaskResult as 'raw' kwarg
            allow_failed_deps=True,       # run even if fetch failed
        )
        ```
    """

    fn: TaskFunction[Any, OkT_co]
    args: tuple[Any, ...] = field(default_factory=tuple, init=False)
    """
    Internal compatibility slot for positional args.
    Workflow nodes are kwargs-only; this field is not accepted in constructors.
    """
    kwargs: dict[str, Any] = field(default_factory=lambda: {})
    waits_for: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] = field(
        default_factory=lambda: [],
    )
    """
    - List of TaskNodes or SubWorkflowNodes that this task waits for
    - The node with the dependencies will wait for all dependencies to be terminal (COMPLETED/FAILED/SKIPPED)
    """

    args_from: dict[str, TaskNode[Any] | SubWorkflowNode[Any]] = field(
        default_factory=lambda: {},
    )
    """
    - Data flow: inject dependency TaskResults as kwargs (keyword-only)
    - Example: args_from={"validated": validate_node, "transformed": transform_node}
    - Task receives: def my_task(validated: TaskResult[A, E], transformed: TaskResult[B, E])
    """

    workflow_ctx_from: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] | None = None
    """
    - Optional context: subset of dependencies to include in WorkflowContext
    - Only injected if task declares `workflow_ctx: WorkflowContext | None` parameter

    """
    # Queue/priority overrides (if None, use task decorator defaults)
    queue: str | None = None
    """
    - Queue overrides (if None, use task decorator defaults)
    """
    priority: int | None = None
    """
    - Priority overrides (if None, use task decorator defaults)
    """

    allow_failed_deps: bool = False
    """
    - If True, this task runs even if dependencies failed (receives failed TaskResults)
    - If False (default), task is SKIPPED when any dependency fails
    """

    run_when: Callable[[WorkflowContext], bool] | None = field(
        default=None, repr=False
    )
    """
    - Conditional execution: evaluated after deps are terminal, before enqueue
    - skip_when has priority over run_when
    - Callables receive WorkflowContext built from workflow_ctx_from
    """

    skip_when: Callable[[WorkflowContext], bool] | None = field(
        default=None, repr=False
    )
    """
    - Conditional execution: evaluated after deps are terminal, before enqueue
    - skip_when has priority over run_when
    - Callables receive WorkflowContext built from workflow_ctx_from
    """

    join: Literal['all', 'any', 'quorum'] = 'all'
    """
    - Dependency join semantics
    - "all": task runs when ALL dependencies are terminal (default)
    - "any": task runs when ANY dependency succeeds (COMPLETED)
    - "quorum": task runs when at least min_success dependencies succeed
    """
    min_success: int | None = None
    """
    - Required for join="quorum"
    - Minimum number of dependencies that must succeed
    """

    good_until: datetime | None = None
    """
    - Task expiry deadline (task skipped if not claimed by this time)
    """

    # Assigned during WorkflowSpec construction
    index: int | None = field(default=None, repr=False)
    """
    - Assigned during WorkflowSpec construction
    - Index of the task in the workflow
    """

    node_id: str | None = field(default=None, repr=False)
    """
    Optional stable identifier for this task within the workflow.
    If None, auto-assigned as '{slugify(workflow_name)}:{task_index}'.
    Must be unique within the workflow.
    Must match pattern: [A-Za-z0-9_\\-:.]+
    """

    @property
    def name(self) -> str:
        """Get the task name from the wrapped function."""
        return self.fn.task_name

    def key(self) -> NodeKey[OkT_co]:
        """Return a typed NodeKey for this task node."""
        if self.node_id is None:
            raise WorkflowValidationError(
                'TaskNode node_id is not set. Ensure WorkflowSpec assigns node_id '
                'or provide an explicit node_id.'
            )
        return NodeKey(self.node_id)


# =============================================================================
# SubWorkflowNode
# =============================================================================


@dataclass
class SubWorkflowNode(Generic[OkT_co]):
    """
    A node that runs a child workflow as a composite task.

    The generic parameter OkT represents the child workflow's output type,
    derived from WorkflowDefinition[OkT].

    When the child workflow completes:
    - COMPLETED: parent node receives TaskResult[OkT, TaskError] with child's output
    - FAILED: parent node receives TaskResult with SubWorkflowError containing SubWorkflowSummary

    Example:
        class DataPipeline(WorkflowDefinition[ProcessedData]):
            ...

        pipeline: SubWorkflowNode[ProcessedData] = SubWorkflowNode(
            workflow_def=DataPipeline,
            kwargs={"source_url": "https://..."},  # passed to build_with()
        )
        # Downstream: args_from={"data": pipeline} → TaskResult[ProcessedData, TaskError]
    """

    workflow_def: type[WorkflowDefinition[OkT_co]]
    """
    - The WorkflowDefinition subclass to run as a child workflow
    - Must implement build_with() for parameterization
    """

    args: tuple[Any, ...] = field(default_factory=tuple, init=False)
    """
    Internal compatibility slot for positional args.
    Workflow nodes are kwargs-only; this field is not accepted in constructors.
    """

    kwargs: dict[str, Any] = field(default_factory=lambda: {})
    """
    - Keyword arguments passed to workflow_def.build_with(app, **kwargs)
    - Use with args_from to inject upstream results as parameters
    """

    waits_for: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] = field(
        default_factory=lambda: [],
    )
    """
    - Nodes this subworkflow waits for before starting
    - Waits for all to be terminal (COMPLETED/FAILED/SKIPPED)
    - Same semantics as TaskNode.waits_for
    """

    args_from: dict[str, TaskNode[Any] | SubWorkflowNode[Any]] = field(
        default_factory=lambda: {},
    )
    """
    - Maps kwarg names to upstream nodes for data injection
    - Injected as TaskResult into build_with() kwargs
    - Example: args_from={"input_data": fetch_node} → kwargs["input_data"] = TaskResult[T, E]
    """

    workflow_ctx_from: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] | None = None
    """
    - Nodes whose results to include in WorkflowContext for run_when/skip_when
    - For SubWorkflowNodes: access via ctx.summary_for(node) → SubWorkflowSummary
    """

    join: Literal['all', 'any', 'quorum'] = 'all'
    """
    - "all": start when ALL dependencies are terminal (default)
    - "any": start when ANY dependency succeeds (COMPLETED)
    - "quorum": start when min_success dependencies succeed
    """

    min_success: int | None = None
    """
    - Required for join="quorum": minimum dependencies that must succeed
    """

    allow_failed_deps: bool = False
    """
    - False (default): SKIPPED if any dependency failed
    - True: starts regardless, failed deps passed as TaskResult(err=...) via args_from
    """

    run_when: Callable[[WorkflowContext], bool] | None = field(
        default=None, repr=False
    )
    """
    - Condition evaluated after deps terminal, before starting child workflow
    - If returns False: node is SKIPPED
    - skip_when takes priority over run_when
    """

    skip_when: Callable[[WorkflowContext], bool] | None = field(
        default=None, repr=False
    )
    """
    - Condition evaluated after deps terminal, before starting child workflow
    - If returns True: node is SKIPPED
    - skip_when takes priority over run_when
    """

    retry_mode: SubWorkflowRetryMode = SubWorkflowRetryMode.RERUN_FAILED_ONLY
    """
    - How to retry if child workflow fails (only RERUN_FAILED_ONLY supported)
    """

    index: int | None = field(default=None, repr=False)
    """
    - Auto-assigned during WorkflowSpec construction
    """

    node_id: str | None = field(default=None, repr=False)
    """
    Optional stable identifier for this subworkflow within the parent workflow.
    If None, auto-assigned as '{slugify(workflow_name)}:{task_index}'.
    Must be unique within the workflow.
    Must match pattern: [A-Za-z0-9_\\-:.]+
    """

    @property
    def name(self) -> str:
        """Get the subworkflow name."""
        return self.workflow_def.name

    def key(self) -> NodeKey[OkT_co]:
        """Return a typed NodeKey for this subworkflow node."""
        if self.node_id is None:
            raise WorkflowValidationError(
                'SubWorkflowNode node_id is not set. Ensure WorkflowSpec assigns node_id '
                'or provide an explicit node_id.'
            )
        return NodeKey(self.node_id)


AnyNode = TaskNode[Any] | SubWorkflowNode[Any]
"""
Type alias for any node type
"""


# =============================================================================
# Success Policy
# =============================================================================


@dataclass
class SuccessCase:
    """
    A single success scenario for a workflow.

    The case is satisfied when ALL required tasks are COMPLETED.

    Example:
        # Workflow succeeds if either (A and B) or (C) completes
        SuccessPolicy(cases=[
            SuccessCase(required=[task_a, task_b]),
            SuccessCase(required=[task_c]),
        ])
    """

    required: list[TaskNode[Any]]
    """
    - All tasks in this list must be COMPLETED for the case to be satisfied
    """


@dataclass
class SuccessPolicy:
    """
    Custom success criteria: workflow COMPLETED if ANY SuccessCase is satisfied.

    Without a success_policy, default behavior is: any task failure → workflow FAILED.
    With a success_policy, workflow is COMPLETED if at least one case has all
    its required tasks COMPLETED, regardless of other task failures.

    Example:
        # "Succeed if primary path completes, even if fallback fails"
        success_policy = SuccessPolicy(
            cases=[SuccessCase(required=[primary_task])],
            optional=[fallback_task],  # can fail without affecting success
        )
    """

    cases: list[SuccessCase]
    """
    - List of success scenarios
    - Workflow succeeds if ANY case is fully satisfied (all required COMPLETED)
    """

    optional: list[TaskNode[Any]] | None = None
    """
    - Tasks that may fail without affecting success evaluation
    - These failures don't block success cases from being satisfied
    """


WorkflowTerminalResults = dict[str, 'TaskResult[Any, TaskError] | None']
"""
Aggregated terminal results returned by workflow get()/get_async() when no explicit
output node is configured.
"""
