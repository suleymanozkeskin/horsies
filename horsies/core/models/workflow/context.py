"""Workflow context, metadata, and subworkflow summary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeGuard,
    cast,
)

from pydantic import BaseModel

from .enums import OkT, OkT_co, WorkflowStatus
from .nodes import NodeKey

if TYPE_CHECKING:
    from horsies.core.models.tasks import TaskResult, TaskError
    from .nodes import TaskNode, SubWorkflowNode


# =============================================================================
# SubWorkflowSummary
# =============================================================================


@dataclass
class SubWorkflowSummary(Generic[OkT_co]):
    """
    Summary of a child workflow's execution, available via WorkflowContext.summary_for().

    Provides visibility into child workflow health without exposing internal DAG.
    Useful for conditional logic based on partial success/failure.

    Example:
        def make_report(workflow_ctx: WorkflowContext | None) -> TaskResult[str, TaskError]:
            if workflow_ctx is None:
                return TaskResult(err=TaskError(error_code="NO_CTX"))
            summary = workflow_ctx.summary_for(data_pipeline_node)
            if summary.failed_tasks > 0:
                return TaskResult(ok=f"Partial: {summary.completed_tasks}/{summary.total_tasks}")
            return TaskResult(ok=f"Full: {summary.output}")
    """

    status: WorkflowStatus
    """Child workflow's final status (COMPLETED, FAILED, etc.)"""

    success_case: str | None
    """Which SuccessCase was satisfied (if success_policy used)"""

    output: OkT_co | None
    """Child's output value (typed via generic parameter)"""

    total_tasks: int
    """Total number of tasks in child workflow"""

    completed_tasks: int
    """Number of COMPLETED tasks"""

    failed_tasks: int
    """Number of FAILED tasks"""

    skipped_tasks: int
    """Number of SKIPPED tasks"""

    error_summary: str | None = None
    """Brief description of failure (if child failed)"""

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> SubWorkflowSummary[Any]:
        """Build a SubWorkflowSummary from a JSON-like dict safely."""
        def _is_str_key_dict(value: object) -> TypeGuard[dict[str, Any]]:
            if not isinstance(value, dict):
                return False
            return all(isinstance(k, str) for k in cast(dict[Any, Any], value))

        # Normalize possible serde dataclass envelope:
        # {"__dataclass__": true, "module": ..., "qualname": ..., "data": {...}}
        payload: dict[str, Any] = data
        raw_inner = data.get('data')
        if data.get('__dataclass__') and _is_str_key_dict(raw_inner):
            payload = raw_inner

        def _as_int(value: Any, default: int = 0) -> int:
            return int(value) if isinstance(value, (int, float)) else default

        status_val = payload.get('status')
        try:
            status = WorkflowStatus(str(status_val))
        except Exception:
            status = WorkflowStatus.FAILED

        success_case_val = payload.get('success_case')
        error_val = payload.get('error_summary')

        return cls(
            status=status,
            success_case=str(success_case_val) if success_case_val else None,
            output=payload.get('output'),
            total_tasks=_as_int(payload.get('total_tasks')),
            completed_tasks=_as_int(payload.get('completed_tasks')),
            failed_tasks=_as_int(payload.get('failed_tasks')),
            skipped_tasks=_as_int(payload.get('skipped_tasks')),
            error_summary=str(error_val) if error_val else None,
        )


# =============================================================================
# WorkflowMeta (metadata only, no result access)
# =============================================================================


@dataclass
class WorkflowMeta:
    """
    Workflow execution metadata.

    Auto-injected if task declares `workflow_meta: WorkflowMeta | None` parameter.
    Contains only metadata, no result access.

    Attributes:
        workflow_id: UUID of the workflow instance
        task_index: Index of the current task in the workflow
        task_name: Name of the current task
    """

    workflow_id: str
    task_index: int
    task_name: str


# =============================================================================
# Exceptions
# =============================================================================


class WorkflowContextMissingIdError(RuntimeError):
    """Raised when TaskNode node_id is missing for WorkflowContext.result_for()."""


class WorkflowHandleMissingIdError(RuntimeError):
    """Raised when TaskNode node_id is missing for WorkflowHandle.result_for()."""


# =============================================================================
# WorkflowContext (type-safe result access via result_for)
# =============================================================================


class WorkflowContext(BaseModel):
    """
    Context passed to workflow tasks with type-safe access to dependency results.

    Only injected if:
    1. TaskNode has workflow_ctx_from set, AND
    2. Task function declares `workflow_ctx: WorkflowContext | None` parameter

    Use result_for(node) to access results in a type-safe manner.
    Use summary_for(node) to access SubWorkflowSummary for SubWorkflowNodes.

    Attributes:
        workflow_id: UUID of the workflow instance
        task_index: Index of the current task in the workflow
        task_name: Name of the current task
    """

    model_config = {'arbitrary_types_allowed': True}

    workflow_id: str  # UUID as string for JSON serialization
    task_index: int
    task_name: str

    # Internal storage: results keyed by node_id
    _results_by_id: dict[str, TaskResult[Any, TaskError]] = {}
    # Internal storage: subworkflow summaries keyed by node_id
    _summaries_by_id: dict[str, SubWorkflowSummary[Any]] = {}

    def __init__(
        self,
        workflow_id: str,
        task_index: int,
        task_name: str,
        results_by_id: dict[str, TaskResult[Any, TaskError]] | None = None,
        summaries_by_id: dict[str, SubWorkflowSummary[Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            workflow_id=workflow_id,
            task_index=task_index,
            task_name=task_name,
            **kwargs,
        )
        # Store results internally (not exposed as Pydantic field)
        object.__setattr__(self, '_results_by_id', results_by_id or {})
        object.__setattr__(self, '_summaries_by_id', summaries_by_id or {})

    def result_for(
        self,
        node: TaskNode[OkT] | NodeKey[OkT],
    ) -> TaskResult[OkT, TaskError]:
        """
        Get the result for a specific TaskNode.

        Type-safe: returns TaskResult[T, TaskError] where T matches the node's type.

        Args:
            node: The TaskNode or NodeKey whose result to retrieve. Must have been
                  included in workflow_ctx_from and have a node_id assigned.

        Returns:
            The TaskResult from the completed task.

        Raises:
            KeyError: If the node's result is not in this context.
            RuntimeError: If the node has no node_id assigned.
        """

        node_id: str | None
        if isinstance(node, NodeKey):
            node_id = node.node_id
        else:
            node_id = node.node_id

        if node_id is None:
            raise WorkflowContextMissingIdError(
                'TaskNode node_id is not set. Ensure WorkflowSpec assigns node_id '
                'or provide an explicit node_id.'
            )

        if node_id not in self._results_by_id:
            raise KeyError(
                f"TaskNode id '{node_id}' not in workflow context. "
                'Ensure the node is included in workflow_ctx_from.'
            )

        # Cast is safe because the generic parameter ensures type correctness
        return cast('TaskResult[OkT, TaskError]', self._results_by_id[node_id])

    def has_result(self, node: TaskNode[Any] | SubWorkflowNode[Any]) -> bool:
        """Check if a result exists for the given node."""
        if node.node_id is None:
            return False
        return node.node_id in self._results_by_id

    def summary_for(
        self,
        node: SubWorkflowNode[OkT],
    ) -> SubWorkflowSummary[OkT]:
        """
        Get the SubWorkflowSummary for a completed SubWorkflowNode.

        Type-safe: returns SubWorkflowSummary[T] where T matches the node's output type.

        Args:
            node: The SubWorkflowNode whose summary to retrieve. Must have been
                  included in workflow_ctx_from and have a node_id assigned.

        Returns:
            The SubWorkflowSummary from the completed subworkflow.

        Raises:
            KeyError: If the node's summary is not in this context.
            RuntimeError: If the node has no node_id assigned.
        """
        node_id = node.node_id

        if node_id is None:
            raise WorkflowContextMissingIdError(
                'SubWorkflowNode node_id is not set. Ensure WorkflowSpec assigns node_id.'
            )

        if node_id not in self._summaries_by_id:
            raise KeyError(
                f"SubWorkflowNode id '{node_id}' not in workflow context summaries. "
                'Ensure the node is included in workflow_ctx_from.'
            )

        # Cast is safe because the generic parameter ensures type correctness
        return cast('SubWorkflowSummary[OkT]', self._summaries_by_id[node_id])

    def has_summary(self, node: SubWorkflowNode[Any]) -> bool:
        """Check if a summary exists for the given SubWorkflowNode."""
        if node.node_id is None:
            return False
        return node.node_id in self._summaries_by_id

    @classmethod
    def from_serialized(
        cls,
        workflow_id: str,
        task_index: int,
        task_name: str,
        results_by_id: dict[str, TaskResult[Any, TaskError]],
        summaries_by_id: dict[str, SubWorkflowSummary[Any]] | None = None,
    ) -> WorkflowContext:
        """Reconstruct WorkflowContext from serialized data."""
        return cls(
            workflow_id=workflow_id,
            task_index=task_index,
            task_name=task_name,
            results_by_id=results_by_id,
            summaries_by_id=summaries_by_id,
        )
