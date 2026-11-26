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
    overload,
)

from pydantic import BaseModel

from .enums import CodeT, OkT, OkT_co, WorkflowStatus
from .nodes import NodeKey

if TYPE_CHECKING:
    from horsies.core.models.outcome import TaskOutcome
    from horsies.core.models.tasks import TaskError
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
        def make_report(workflow_ctx: WorkflowContext | None) -> Result[str, TaskError]:
            if workflow_ctx is None:
                return Err(TaskError(
                    error_code="NO_CTX",
                    message="make_report ran without a workflow context",
                ))
            summary = workflow_ctx.summary_for(data_pipeline_node)
            if summary.failed_nodes > 0:
                return Ok(f"Partial: {summary.completed_nodes}/{summary.total_nodes}")
            return Ok(f"Full: {summary.output}")
    """

    status: WorkflowStatus
    """Child workflow's final status (COMPLETED, FAILED, etc.)"""

    output: OkT_co | None
    """Child's output value (typed via generic parameter)"""

    total_nodes: int
    """Total number of nodes in the child workflow"""

    completed_nodes: int
    """Number of COMPLETED nodes"""

    failed_nodes: int
    """Number of FAILED nodes"""

    skipped_nodes: int
    """Number of SKIPPED nodes"""

    error_summary: str | None = None
    """Brief description of failure (if child failed)"""

    def to_json(self) -> dict[str, Any]:
        """Serialize to a plain JSON-native dict (inverse of from_json).

        Emits a bare, envelope-free dict — no serde class tags. ``output`` is
        passed through as-is: at the engine emit site it is already a decoded
        plain-JSON value. ``status`` is written as its string value.
        """
        return {
            'status': self.status.value,
            'output': self.output,
            'total_nodes': self.total_nodes,
            'completed_nodes': self.completed_nodes,
            'failed_nodes': self.failed_nodes,
            'skipped_nodes': self.skipped_nodes,
            'error_summary': self.error_summary,
        }

    @classmethod
    def from_json(cls, data: object) -> SubWorkflowSummary[Any]:
        """Build a SubWorkflowSummary from persisted JSON-like data.

        Fails closed: raises ValueError when the payload is not a string-keyed
        dict or the status is missing/unrecognized, so corrupt persisted data
        is surfaced instead of being silently coerced into a FAILED summary.
        """
        def _is_str_key_dict(value: object) -> TypeGuard[dict[str, Any]]:
            if not isinstance(value, dict):
                return False
            return all(isinstance(k, str) for k in cast(dict[Any, Any], value))

        if not _is_str_key_dict(data):
            raise ValueError(
                'SubWorkflowSummary payload must be a string-keyed dict, '
                f'got {type(data).__name__}'
            )

        payload: dict[str, Any] = data

        def _require_count(field_name: str, value: Any) -> int:
            # Counts are always written by the engine; missing or non-int
            # (incl. bool) or negative values mean corrupt persisted data.
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(
                    f'SubWorkflowSummary {field_name} must be a non-negative '
                    f'integer, got {value!r}'
                )
            return value

        status_val = payload.get('status')
        if isinstance(status_val, WorkflowStatus):
            status = status_val
        elif isinstance(status_val, str):
            try:
                status = WorkflowStatus(status_val)
            except ValueError:
                raise ValueError(
                    f'SubWorkflowSummary has unrecognized status {status_val!r}'
                ) from None
        else:
            raise ValueError(
                'SubWorkflowSummary status must be a status string, '
                f'got {type(status_val).__name__}'
            )

        error_val = payload.get('error_summary')
        if error_val is None:
            error_summary = None
        elif isinstance(error_val, str):
            error_summary = error_val
        else:
            raise ValueError(
                'SubWorkflowSummary error_summary must be a string or null, '
                f'got {type(error_val).__name__}'
            )

        return cls(
            status=status,
            output=payload.get('output'),
            total_nodes=_require_count('total_nodes', payload.get('total_nodes')),
            completed_nodes=_require_count('completed_nodes', payload.get('completed_nodes')),
            failed_nodes=_require_count('failed_nodes', payload.get('failed_nodes')),
            skipped_nodes=_require_count('skipped_nodes', payload.get('skipped_nodes')),
            error_summary=error_summary,
        )


# =============================================================================
# WorkflowMetadata (metadata only, no result access)
# =============================================================================


@dataclass
class WorkflowMetadata:
    """
    Workflow execution metadata.

    Auto-injected if task declares `workflow_meta: WorkflowMetadata | None` parameter.
    Contains only metadata, no result access.

    Attributes:
        workflow_id: UUID of the workflow instance
        node_index: Index of the current node in the workflow
        task_name: Name of the current task
    """

    workflow_id: str
    node_index: int
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
        node_index: Index of the current node in the workflow
        task_name: Name of the current task
    """

    model_config = {'arbitrary_types_allowed': True}

    workflow_id: str  # UUID as string for JSON serialization
    node_index: int
    task_name: str

    # Internal storage: results keyed by node_id
    _results_by_id: dict[str, TaskOutcome[Any, TaskError]] = {}
    # Internal storage: subworkflow summaries keyed by node_id
    _summaries_by_id: dict[str, SubWorkflowSummary[Any]] = {}

    def __init__(
        self,
        workflow_id: str,
        node_index: int,
        task_name: str,
        results_by_node_id: dict[str, TaskOutcome[Any, TaskError]] | None = None,
        summaries_by_node_id: dict[str, SubWorkflowSummary[Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            workflow_id=workflow_id,
            node_index=node_index,
            task_name=task_name,
            **kwargs,
        )
        # Store results internally (not exposed as Pydantic field)
        object.__setattr__(self, '_results_by_id', results_by_node_id or {})
        object.__setattr__(self, '_summaries_by_id', summaries_by_node_id or {})

    @staticmethod
    def _dump_payload_map(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Shallow-copy private context payloads for python-mode dump.

        Strict-serde phase 7: JSON-mode dump is no longer supported on
        ``WorkflowContext`` (see ``model_dump`` / ``model_dump_json``
        overrides). The legacy ``to_jsonable`` branch is gone — the only
        sanctioned reconstruction path is ``from_serialized`` with
        pre-typed ``TaskOutcome`` values.
        """
        return dict(payload)

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
    ) -> dict[str, TaskOutcome[Any, TaskError]]:
        """Strict-serde phase 6 (design §6 / plan §H).

        ``WorkflowContext`` no longer decodes raw envelopes inside this
        validator — decode happens at the engine/child-runner boundary
        where ``node_id → ok_type`` is known via the workflow spec
        and local task registry. Here we accept only pre-typed
        ``TaskOutcome`` values (the shape produced upstream) and
        reject raw dicts so a cross-version producer can't slip a
        wrong-shape value past the type system.
        """
        from horsies.core.models.outcome import Failed, Succeeded

        if raw_results is None:
            return {}
        if not isinstance(raw_results, dict):
            raise ValueError('WorkflowContext results_by_node_id must be a dict')

        results: dict[str, TaskOutcome[Any, TaskError]] = {}
        raw_result_map = cast(dict[Any, Any], raw_results)
        for key, value in raw_result_map.items():
            if not isinstance(key, str):
                raise ValueError('WorkflowContext results_by_node_id keys must be strings')
            if not isinstance(value, Succeeded | Failed):
                raise ValueError(
                    f'WorkflowContext results_by_node_id[{key!r}] must be a '
                    f'TaskOutcome (Succeeded or Failed); got {type(value).__name__}. '
                    f'Strict-serde decodes raw envelopes at the engine/'
                    f'worker boundary, not inside the model validator.'
                )
            results[key] = value
        return results

    @classmethod
    def _coerce_summaries_by_id(
        cls,
        raw_summaries: Any,
    ) -> dict[str, SubWorkflowSummary[Any]]:
        if raw_summaries is None:
            return {}
        if not isinstance(raw_summaries, dict):
            raise ValueError('WorkflowContext summaries_by_node_id must be a dict')

        summaries: dict[str, SubWorkflowSummary[Any]] = {}
        raw_summary_map = cast(dict[Any, Any], raw_summaries)
        for key, value in raw_summary_map.items():
            if not isinstance(key, str):
                raise ValueError('WorkflowContext summaries_by_node_id keys must be strings')
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
        if mode == 'json':
            raise NotImplementedError(
                "WorkflowContext.model_dump(mode='json') is not supported "
                "under strict-serde. WorkflowContext is not wire-encodable "
                "because its per-result ok_type information lives at the "
                "engine/worker boundary, not on the model. Use "
                "WorkflowContext.from_serialized(...) with pre-typed "
                "TaskOutcome values for in-process reconstruction."
            )
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
            'results_by_node_id',
            include=include,
            exclude=exclude,
        ):
            data['results_by_node_id'] = self._dump_payload_map(self._results_by_id)
        if self._should_dump_payload_field(
            'summaries_by_node_id',
            include=include,
            exclude=exclude,
        ):
            data['summaries_by_node_id'] = self._dump_payload_map(self._summaries_by_id)

        return data

    def model_dump_json(
        self,
        *,
        indent: int | None = None,
        ensure_ascii: bool = False,
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
    ) -> str:
        """Refuse wire serialization of WorkflowContext.

        Strict-serde phase 7: WorkflowContext can only be reconstructed
        via ``from_serialized`` with pre-typed ``TaskOutcome`` values;
        per-result ``ok_type`` information lives at the engine/worker
        boundary, not on the model, so a JSON form has no sanctioned
        decode path.
        """
        _ = (
            indent, ensure_ascii, include, exclude, context, by_alias,
            exclude_unset, exclude_defaults, exclude_none,
            exclude_computed_fields, round_trip, warnings, fallback,
            serialize_as_any, polymorphic_serialization,
        )
        raise NotImplementedError(
            'WorkflowContext.model_dump_json() is not supported under '
            'strict-serde. See model_dump(mode="json") for the rationale '
            'and migration guidance.'
        )

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
        raw_results: Any = data.pop('results_by_node_id', None)
        if raw_results is None and '_results_by_id' in data:
            raw_results = data.pop('_results_by_id')
        raw_summaries: Any = data.pop('summaries_by_node_id', None)
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

    @overload
    def result_for(
        self, node: TaskNode[OkT, CodeT] | NodeKey[OkT, CodeT],
    ) -> TaskOutcome[OkT, TaskError[CodeT]]: ...

    @overload
    def result_for(
        self, node: SubWorkflowNode[OkT],
    ) -> TaskOutcome[OkT, TaskError]: ...

    def result_for(
        self,
        node: TaskNode[OkT, Any] | SubWorkflowNode[OkT] | NodeKey[OkT, Any],
    ) -> TaskOutcome[OkT, TaskError[Any]]:
        """
        Get the outcome of a specific node.

        The custom codes of a failed outcome are members of the ``Enum`` of
        the node's task when the task returns ``TaskError[E]``, and text for
        a task that returns ``TaskError`` and for a ``SubWorkflowNode`` (a
        child workflow holds the codes of many tasks). A stored code that
        ``E`` does not name is a failed outcome with
        ``RESULT_DESERIALIZATION_ERROR``, as a node whose stored value does
        not read.

        Type-safe: returns TaskOutcome[T, TaskError] where T matches the node's type.

        Accepts SubWorkflowNode as well as TaskNode: a SubWorkflowNode's
        terminal outcome is stored alongside task outcomes (see
        ``has_result``, which also accepts both). Use ``summary_for`` when you
        need the richer SubWorkflowSummary instead.

        Args:
            node: The TaskNode, SubWorkflowNode, or NodeKey whose result to
                  retrieve. Must have been included in workflow_ctx_from and
                  have a node_id assigned.

        Returns:
            The outcome of the completed task: ``Succeeded(value)`` or
            ``Failed(task_error)``.

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
        return cast('TaskOutcome[OkT, TaskError[Any]]', self._results_by_id[node_id])

    def has_result(self, node: TaskNode[Any, Any] | SubWorkflowNode[Any]) -> bool:
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

        Type-safe: returns SubWorkflowSummary[T] where T matches the
        node's output type. The summary's ``output`` field is decoded
        against the child workflow's declared ``WorkflowDefinition[OkT]``
        generic when resolvable; raw-JSON-decoded values returned by the
        engine are run through ``decode_value(raw, OkT)`` so callers
        observe the typed value, not the raw dict.

        When the child workflow declares ``WorkflowDefinition[Any]`` (or
        the generic can't be resolved), the raw value is passed through
        unchanged — matching the previous behavior.

        Args:
            node: The SubWorkflowNode whose summary to retrieve. Must have been
                  included in workflow_ctx_from and have a node_id assigned.

        Returns:
            A fresh ``SubWorkflowSummary[OkT]`` with the typed ``output``.
            The stored raw summary is not mutated.

        Raises:
            KeyError: If the node's summary is not in this context.
            RuntimeError: If the node has no node_id assigned.
            pydantic.ValidationError: If the raw output payload doesn't
                satisfy the child workflow's declared output type.
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

        raw_summary = self._summaries_by_id[node_id]

        # Resolve the child workflow's declared output type. When
        # unresolvable (no generic parameter) or explicitly ``Any``, fall
        # through with the raw summary cast — matches legacy behavior.
        from horsies.core.codec.typed import decode_value
        from .typing_utils import _resolve_workflow_def_ok_type

        output_type = _resolve_workflow_def_ok_type(node.workflow_definition)
        if output_type is None or output_type is Any:
            return cast('SubWorkflowSummary[OkT]', raw_summary)

        # Typed decode of the output slot. ``None`` (failed / no-output
        # child) passes through unchanged; raw is what the engine stored.
        if raw_summary.output is None:
            decoded_output: Any = None
        else:
            decoded_output = decode_value(raw_summary.output, output_type)

        return SubWorkflowSummary(
            status=raw_summary.status,
            output=decoded_output,
            total_nodes=raw_summary.total_nodes,
            completed_nodes=raw_summary.completed_nodes,
            failed_nodes=raw_summary.failed_nodes,
            skipped_nodes=raw_summary.skipped_nodes,
            error_summary=raw_summary.error_summary,
        )

    def has_summary(self, node: SubWorkflowNode[Any]) -> bool:
        """Check if a summary exists for the given SubWorkflowNode."""
        if node.node_id is None:
            return False
        return node.node_id in self._summaries_by_id

    @classmethod
    def from_serialized(
        cls,
        workflow_id: str,
        node_index: int,
        task_name: str,
        results_by_node_id: dict[str, TaskOutcome[Any, TaskError]],
        summaries_by_node_id: dict[str, SubWorkflowSummary[Any]] | None = None,
    ) -> WorkflowContext:
        """Reconstruct WorkflowContext from serialized data."""
        return cls(
            workflow_id=workflow_id,
            node_index=node_index,
            task_name=task_name,
            results_by_node_id=results_by_node_id,
            summaries_by_node_id=summaries_by_node_id,
        )
