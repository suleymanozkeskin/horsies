"""Workflow context, metadata, and subworkflow summary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Literal,
    Self,
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
        # {"__h_dataclass__": true, "module": ..., "qualname": ..., "data": {...}}
        payload: dict[str, Any] = data
        raw_inner = data.get('data')
        if data.get('__h_dataclass__') and _is_str_key_dict(raw_inner):
            payload = raw_inner

        def _as_int(value: Any, default: int = 0) -> int:
            return int(value) if isinstance(value, (int, float)) else default

        status_val = payload.get('status')
        if isinstance(status_val, WorkflowStatus):
            status = status_val
        else:
            try:
                status = WorkflowStatus(str(status_val))
            except Exception:
                status = WorkflowStatus.FAILED

        error_val = payload.get('error_summary')

        return cls(
            status=status,
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

    @staticmethod
    def _dump_payload_map(
        payload: dict[str, Any],
        *,
        mode: Literal['json', 'python'] | str,
    ) -> dict[str, Any]:
        """Dump private context payloads in the same mode as model_dump()."""
        if mode != 'json':
            return dict(payload)

        from horsies.core.codec.serde import to_jsonable
        from horsies.core.types.result import is_err

        serialized: dict[str, Any] = {}
        for key, value in payload.items():
            result = to_jsonable(value)
            if is_err(result):
                raise ValueError(
                    f'Failed to serialize WorkflowContext payload {key!r}: '
                    f'{result.err_value}'
                )
            serialized[key] = result.ok_value
        return serialized

    @staticmethod
    def _should_dump_payload_field(
        field_name: str,
        *,
        include: Any,
        exclude: Any,
    ) -> bool:
        """Respect top-level include/exclude for synthetic payload fields."""
        if include is not None:
            if isinstance(include, (set, frozenset)) and field_name not in include:
                return False
            if isinstance(include, dict):
                include_map = cast(dict[Any, Any], include)
                include_value: Any = include_map.get(field_name)
                if include_value is None or include_value is False:
                    return False

        if exclude is not None:
            if isinstance(exclude, (set, frozenset)) and field_name in exclude:
                return False
            if isinstance(exclude, dict):
                exclude_map = cast(dict[Any, Any], exclude)
                if exclude_map.get(field_name) is True:
                    return False

        return True

    @classmethod
    def _coerce_results_by_id(
        cls,
        raw_results: Any,
    ) -> dict[str, TaskResult[Any, TaskError]]:
        from horsies.core.codec.serde import task_result_from_json
        from horsies.core.models.tasks import TaskResult
        from horsies.core.types.result import is_err

        if raw_results is None:
            return {}
        if not isinstance(raw_results, dict):
            raise ValueError('WorkflowContext results_by_id must be a dict')

        results: dict[str, TaskResult[Any, TaskError]] = {}
        raw_result_map = cast(dict[Any, Any], raw_results)
        for key, value in raw_result_map.items():
            if not isinstance(key, str):
                raise ValueError('WorkflowContext results_by_id keys must be strings')
            if isinstance(value, TaskResult):
                results[key] = value
                continue

            result = task_result_from_json(value)
            if is_err(result):
                raise ValueError(
                    f'Invalid WorkflowContext TaskResult for {key!r}: {result.err_value}'
                )
            results[key] = result.ok_value
        return results

    @classmethod
    def _coerce_summaries_by_id(
        cls,
        raw_summaries: Any,
    ) -> dict[str, SubWorkflowSummary[Any]]:
        if raw_summaries is None:
            return {}
        if not isinstance(raw_summaries, dict):
            raise ValueError('WorkflowContext summaries_by_id must be a dict')

        summaries: dict[str, SubWorkflowSummary[Any]] = {}
        raw_summary_map = cast(dict[Any, Any], raw_summaries)
        for key, value in raw_summary_map.items():
            if not isinstance(key, str):
                raise ValueError('WorkflowContext summaries_by_id keys must be strings')
            if isinstance(value, SubWorkflowSummary):
                summaries[key] = value
                continue
            if not isinstance(value, dict):
                raise ValueError(f'Invalid WorkflowContext SubWorkflowSummary for {key!r}')
            summaries[key] = SubWorkflowSummary.from_json(cast(dict[str, Any], value))
        return summaries

    def model_dump(
        self,
        *,
        mode: Literal['json', 'python'] | str = 'python',
        include: Any = None,
        exclude: Any = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        exclude_computed_fields: bool = False,
        round_trip: bool = False,
        warnings: bool | Literal['none', 'warn', 'error'] = True,
        fallback: Callable[[Any], Any] | None = None,
        serialize_as_any: bool = False,
        polymorphic_serialization: bool | None = None,
    ) -> dict[str, Any]:
        data = super().model_dump(
            mode=mode,
            include=include,
            exclude=exclude,
            context=context,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            exclude_computed_fields=exclude_computed_fields,
            round_trip=round_trip,
            warnings=warnings,
            fallback=fallback,
            serialize_as_any=serialize_as_any,
            polymorphic_serialization=polymorphic_serialization,
        )

        if self._should_dump_payload_field(
            'results_by_id',
            include=include,
            exclude=exclude,
        ):
            data['results_by_id'] = self._dump_payload_map(self._results_by_id, mode=mode)
        if self._should_dump_payload_field(
            'summaries_by_id',
            include=include,
            exclude=exclude,
        ):
            data['summaries_by_id'] = self._dump_payload_map(self._summaries_by_id, mode=mode)

        return data

    @classmethod
    def model_validate(
        cls,
        obj: Any,
        *,
        strict: bool | None = None,
        extra: Any | None = None,
        from_attributes: bool | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        if not isinstance(obj, dict):
            return super().model_validate(
                obj,
                strict=strict,
                extra=extra,
                from_attributes=from_attributes,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )

        data: dict[str, Any] = dict(cast(dict[str, Any], obj))
        raw_results: Any = data.pop('results_by_id', None)
        if raw_results is None and '_results_by_id' in data:
            raw_results = data.pop('_results_by_id')
        raw_summaries: Any = data.pop('summaries_by_id', None)
        if raw_summaries is None and '_summaries_by_id' in data:
            raw_summaries = data.pop('_summaries_by_id')

        ctx = super().model_validate(
            data,
            strict=strict,
            extra=extra,
            from_attributes=from_attributes,
            context=context,
            by_alias=by_alias,
            by_name=by_name,
        )
        object.__setattr__(
            ctx,
            '_results_by_id',
            cls._coerce_results_by_id(raw_results),
        )
        object.__setattr__(
            ctx,
            '_summaries_by_id',
            cls._coerce_summaries_by_id(raw_summaries),
        )
        return ctx

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
