"""Core workflow models for DAG-based task orchestration."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
import re
from types import UnionType
from datetime import datetime
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    Generic,
    TypeGuard,
    cast,
    Callable,
    Literal,
    ClassVar,
    get_args,
    get_origin,
    get_type_hints,
    Union,
)
import inspect
import time
from horsies.core.utils.loop_runner import LoopRunner
from horsies.core.errors import (
    ErrorCode,
    SourceLocation,
    ValidationReport,
    raise_collected,
)
from pydantic import BaseModel
from sqlalchemy import text

if TYPE_CHECKING:
    from horsies.core.task_decorator import TaskFunction
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.tasks import TaskResult, TaskError
    from horsies.core.app import Horsies


# TypeVar for TaskNode generic parameter (the "ok" type of TaskResult)
OkT = TypeVar('OkT')
OkT_co = TypeVar('OkT_co', covariant=True)
OutT = TypeVar('OutT')


# =============================================================================
# Enums
# =============================================================================


class WorkflowStatus(str, Enum):
    """
    Status of a workflow instance.

    State machine:
        PENDING → RUNNING → COMPLETED
                         → FAILED (on task failure with on_error=FAIL)
                         → PAUSED (on task failure with on_error=PAUSE)
                         → CANCELLED (user requested)
    """

    PENDING = 'PENDING'
    """Created but not yet started"""

    RUNNING = 'RUNNING'
    """At least one task executing or ready"""

    COMPLETED = 'COMPLETED'
    """All tasks terminal and success criteria met"""

    FAILED = 'FAILED'
    """A task failed and on_error=FAIL (or no success case satisfied)"""

    PAUSED = 'PAUSED'
    """A task failed with on_error=PAUSE; awaiting resume() or cancel()"""

    CANCELLED = 'CANCELLED'
    """User cancelled via WorkflowHandle.cancel()"""

    @property
    def is_terminal(self) -> bool:
        """Whether this status represents a final state (no further transitions)."""
        return self in WORKFLOW_TERMINAL_STATES


WORKFLOW_TERMINAL_STATES: frozenset[WorkflowStatus] = frozenset(
    {
        WorkflowStatus.COMPLETED,
        WorkflowStatus.FAILED,
        WorkflowStatus.CANCELLED,
    }
)


class WorkflowTaskStatus(str, Enum):
    """
    Status of a single task/node within a workflow.

    State machine:
        PENDING → READY → ENQUEUED → RUNNING → COMPLETED
                                             → FAILED
                       → SKIPPED (deps failed and allow_failed_deps=False)
    """

    PENDING = 'PENDING'
    """Waiting for dependencies to become terminal"""

    READY = 'READY'
    """Dependencies satisfied, waiting to be enqueued"""

    ENQUEUED = 'ENQUEUED'
    """Task created in tasks table, waiting for worker"""

    RUNNING = 'RUNNING'
    """Worker is executing the task (or child workflow is running)"""

    COMPLETED = 'COMPLETED'
    """Task succeeded (TaskResult.is_ok())"""

    FAILED = 'FAILED'
    """Task failed (TaskResult.is_err())"""

    SKIPPED = 'SKIPPED'
    """Skipped due to: upstream failure, condition, or quorum impossible"""

    @property
    def is_terminal(self) -> bool:
        """Whether this status represents a final state (no further transitions)."""
        return self in WORKFLOW_TASK_TERMINAL_STATES


WORKFLOW_TASK_TERMINAL_STATES: frozenset[WorkflowTaskStatus] = frozenset(
    {
        WorkflowTaskStatus.COMPLETED,
        WorkflowTaskStatus.FAILED,
        WorkflowTaskStatus.SKIPPED,
    }
)


class OnError(str, Enum):
    """
    Error handling policy for workflows when a task fails.
    """

    FAIL = 'fail'
    """Continue DAG resolution but mark workflow as will-fail. Skip tasks without allow_failed_deps."""

    PAUSE = 'pause'
    """Pause workflow immediately. No new tasks enqueued until resume()."""


class SubWorkflowRetryMode(str, Enum):
    """
    Retry behavior for subworkflows (only RERUN_FAILED_ONLY currently supported).
    """

    RERUN_FAILED_ONLY = 'rerun_failed_only'
    """Re-run only failed/cancelled child tasks (default, only supported mode)"""

    RERUN_ALL = 'rerun_all'
    """Re-run entire child workflow from scratch (not yet implemented)"""

    NO_RERUN = 'no_rerun'
    """Re-evaluate success policy without re-running (not yet implemented)"""


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
    def from_json(cls, data: dict[str, Any]) -> 'SubWorkflowSummary[Any]':
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
# Exceptions
# =============================================================================

# Re-export from errors module for backward compatibility
from horsies.core.errors import WorkflowValidationError


def _get_inspect_target(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Return underlying function for introspection when wrappers expose _original_fn."""
    inspect_target: Callable[..., Any] = fn
    original = getattr(fn, '_original_fn', None)
    if callable(original):
        inspect_target = original
    return inspect_target


def _task_accepts_workflow_ctx(fn: Callable[..., Any]) -> bool:
    inspect_target: Callable[..., Any] = _get_inspect_target(fn)
    try:
        sig = inspect.signature(inspect_target)
    except (TypeError, ValueError):
        return False
    return 'workflow_ctx' in sig.parameters


def _get_signature(fn: Callable[..., Any]) -> inspect.Signature | None:
    """Return an inspectable signature for a callable, or None if unavailable."""
    inspect_target: Callable[..., Any] = _get_inspect_target(fn)
    try:
        return inspect.signature(inspect_target)
    except (TypeError, ValueError):
        return None


def _signature_accepts_kwargs(sig: inspect.Signature) -> bool:
    """Return True if the signature allows **kwargs."""
    return any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in sig.parameters.values()
    )


def _valid_kwarg_names(
    sig: inspect.Signature, *, exclude: set[str] | None = None
) -> set[str]:
    """
    Return names that are valid to pass by keyword.

    Excludes positional-only params and *args/**kwargs.
    """
    names = {
        param.name
        for param in sig.parameters.values()
        if param.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    if exclude:
        names -= exclude
    return names


def _resolved_type_hints(fn: Callable[..., Any]) -> dict[str, Any]:
    """Return resolved type hints for a callable, or {} when unavailable."""
    inspect_target: Callable[..., Any] = _get_inspect_target(fn)
    targets: list[Callable[..., Any]] = [inspect_target]
    call_attr = getattr(inspect_target, '__call__', None)
    if callable(call_attr):
        targets.append(call_attr)

    for target in targets:
        try:
            return get_type_hints(target)
        except Exception:
            continue
    return {}


def _extract_taskresult_ok_type(annotation: Any) -> Any | None:
    """Extract Ok type from TaskResult[Ok, Err], including Optional/Union wrappers."""
    if annotation is None:
        return None

    from horsies.core.models.tasks import TaskResult

    origin = get_origin(annotation)
    if origin is TaskResult:
        args = get_args(annotation)
        if len(args) == 2:
            return args[0]
        return None

    if origin in (Union, UnionType):
        extracted = [_extract_taskresult_ok_type(member) for member in get_args(annotation)]
        non_none = [value for value in extracted if value is not None]
        if len(non_none) == 1:
            return non_none[0]
        return None

    return None


def _normalize_resolved_ok_type(value: Any | None) -> Any | None:
    """Normalize unresolved generic/typevar outputs to None."""
    if value is None:
        return None
    if isinstance(value, TypeVar):
        return None
    return value


def _resolve_task_fn_ok_type(fn: Callable[..., Any]) -> Any | None:
    """
    Resolve TaskResult ok-type produced by a task function wrapper.

    Prefers explicit wrapper metadata (task_ok_type), then falls back to
    return-type annotations.
    """
    explicit_ok = getattr(fn, 'task_ok_type', None)
    normalized = _normalize_resolved_ok_type(explicit_ok)
    if normalized is not None:
        return normalized

    hints = _resolved_type_hints(fn)
    ok_from_hints = _extract_taskresult_ok_type(hints.get('return'))
    normalized = _normalize_resolved_ok_type(ok_from_hints)
    if normalized is not None:
        return normalized

    sig = _get_signature(fn)
    if sig is None or sig.return_annotation is inspect.Parameter.empty:
        return None
    ok_from_sig = _extract_taskresult_ok_type(sig.return_annotation)
    return _normalize_resolved_ok_type(ok_from_sig)


def _resolve_workflow_def_ok_type(
    workflow_def: type['WorkflowDefinition[Any]'],
) -> Any | None:
    """Resolve WorkflowDefinition[OkT] generic output type from class bases."""
    for cls in workflow_def.__mro__:
        for base in getattr(cls, '__orig_bases__', ()):
            origin = get_origin(base)
            if getattr(origin, '__name__', '') != 'WorkflowDefinition':
                continue
            args = get_args(base)
            if len(args) != 1:
                continue
            return _normalize_resolved_ok_type(args[0])
    return None


def _resolve_source_node_ok_type(node: 'TaskNode[Any] | SubWorkflowNode[Any]') -> Any | None:
    """Resolve output ok-type for TaskNode/SubWorkflowNode source used in args_from."""
    if isinstance(node, TaskNode):
        return _resolve_task_fn_ok_type(node.fn)
    return _resolve_workflow_def_ok_type(node.workflow_def)


def _format_type_name(annotation: Any | None) -> str:
    """Stable display for type annotations in validation notes."""
    if annotation is None:
        return 'unknown'
    if annotation is Any:
        return 'Any'
    return repr(annotation)


def _is_ok_type_compatible(source_ok: Any, expected_ok: Any) -> bool:
    """
    Return True if source TaskResult ok-type can be injected into expected type.

    Rules:
      - Any on either side is treated as compatible
      - Exact match is compatible
      - source subclass of expected is compatible for concrete classes
      - Union expected accepts if any branch is compatible
      - Union source is compatible only if all branches are compatible
    """
    if source_ok is Any or expected_ok is Any:
        return True
    if source_ok == expected_ok:
        return True

    expected_origin = get_origin(expected_ok)
    if expected_origin in (Union, UnionType):
        return any(
            _is_ok_type_compatible(source_ok, branch) for branch in get_args(expected_ok)
        )

    source_origin = get_origin(source_ok)
    if source_origin in (Union, UnionType):
        return all(
            _is_ok_type_compatible(branch, expected_ok) for branch in get_args(source_ok)
        )

    if isinstance(source_ok, type) and isinstance(expected_ok, type):
        try:
            return issubclass(source_ok, expected_ok)
        except TypeError:
            return False

    return False


NODE_ID_PATTERN = re.compile(r'^[A-Za-z0-9_\-:.]+$')


def slugify(value: str) -> str:
    """
    Convert a string to a valid node_id by replacing invalid characters.

    Spaces become underscores, other invalid characters are removed.
    Result matches NODE_ID_PATTERN: [A-Za-z0-9_\\-:.]+

    Example:
        slugify("My Workflow Name")  # "My_Workflow_Name"
        slugify("task:v2.0")         # "task:v2.0" (unchanged)
    """
    result = value.replace(' ', '_')
    result = re.sub(r'[^A-Za-z0-9_\-:.]', '', result)
    return result or '_'


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
        fetch = TaskNode(fn=fetch_data, args=("url",))
        process = TaskNode(
            fn=process_data,
            waits_for=[fetch],           # wait for fetch to be terminal
            args_from={"raw": fetch},     # inject fetch's TaskResult as 'raw' kwarg
            allow_failed_deps=True,       # run even if fetch failed
        )
        ```
    """

    fn: TaskFunction[Any, OkT_co]
    args: tuple[Any, ...] = ()
    """
    - Positional arguments passed to the task function
    - Not allowed when args_from or workflow_ctx_from are set; use kwargs instead
    """
    kwargs: dict[str, Any] = field(default_factory=lambda: {})
    waits_for: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] = field(
        default_factory=lambda: [],
    )
    """
    - List of TaskNodes or SubWorkflowNodes that this task waits for
    - The node with the dependencies will wait for all dependencies to be terminal (COMPLETED/FAILED/SKIPPED)
    """

    args_from: dict[str, 'TaskNode[Any] | SubWorkflowNode[Any]'] = field(
        default_factory=lambda: {},
    )
    """
    - Data flow: inject dependency TaskResults as kwargs (keyword-only)
    - Example: args_from={"validated": validate_node, "transformed": transform_node}
    - Task receives: def my_task(validated: TaskResult[A, E], transformed: TaskResult[B, E])
    """

    workflow_ctx_from: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None = None
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

    run_when: Callable[['WorkflowContext'], bool] | None = field(
        default=None, repr=False
    )
    """
    - Conditional execution: evaluated after deps are terminal, before enqueue
    - skip_when has priority over run_when
    - Callables receive WorkflowContext built from workflow_ctx_from
    """

    skip_when: Callable[['WorkflowContext'], bool] | None = field(
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

    def key(self) -> 'NodeKey[OkT_co]':
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

    workflow_def: type['WorkflowDefinition[OkT_co]']
    """
    - The WorkflowDefinition subclass to run as a child workflow
    - Must implement build_with() for parameterization
    """

    args: tuple[Any, ...] = ()
    """
    - Positional arguments passed to workflow_def.build_with(app, *args, **kwargs)
    - Not allowed when args_from or workflow_ctx_from are set; use kwargs instead
    """

    kwargs: dict[str, Any] = field(default_factory=lambda: {})
    """
    - Keyword arguments passed to workflow_def.build_with(app, *args, **kwargs)
    - Use with args_from to inject upstream results as parameters
    """

    waits_for: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] = field(
        default_factory=lambda: [],
    )
    """
    - Nodes this subworkflow waits for before starting
    - Waits for all to be terminal (COMPLETED/FAILED/SKIPPED)
    - Same semantics as TaskNode.waits_for
    """

    args_from: dict[str, 'TaskNode[Any] | SubWorkflowNode[Any]'] = field(
        default_factory=lambda: {},
    )
    """
    - Maps kwarg names to upstream nodes for data injection
    - Injected as TaskResult into build_with() kwargs
    - Example: args_from={"input_data": fetch_node} → kwargs["input_data"] = TaskResult[T, E]
    """

    workflow_ctx_from: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None = None
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

    run_when: Callable[['WorkflowContext'], bool] | None = field(
        default=None, repr=False
    )
    """
    - Condition evaluated after deps terminal, before starting child workflow
    - If returns False: node is SKIPPED
    - skip_when takes priority over run_when
    """

    skip_when: Callable[['WorkflowContext'], bool] | None = field(
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

    def key(self) -> 'NodeKey[OkT_co]':
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
# NodeKey (typed, stable id)
# =============================================================================


@dataclass(frozen=True)
class NodeKey(Generic[OkT_co]):
    """Typed stable identifier for a TaskNode."""

    node_id: str


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


# =============================================================================
# WorkflowSpec
# =============================================================================


@dataclass
class WorkflowSpec(Generic[OutT]):
    """
    Specification for a workflow DAG. Created via app.workflow() or WorkflowDefinition.build().

    Validates the DAG on construction (cycles, dependency refs, args_from, etc.)
    to catch configuration errors early, before execution.

    Example:
        spec = app.workflow(
            name="my_pipeline",
            tasks=[fetch, process, persist],
            output=persist,
            on_error=OnError.PAUSE,
        )
        handle = await spec.start_async()
    """

    name: str
    """
    Human-readable workflow name (used in logs, DB, registry).
    Can contain any characters including spaces. When auto-generating
    node_ids, the name is passed through slugify() to ensure validity.
    """

    tasks: list[TaskNode[Any] | SubWorkflowNode[Any]]
    """
    - All nodes in the DAG (order determines index assignment)
    - Root nodes (empty waits_for) start immediately
    """

    on_error: OnError = OnError.FAIL
    """
    - FAIL (default): on task failure, mark workflow as will-fail, skip dependent tasks
    - PAUSE: on task failure, pause workflow for manual intervention (resume/cancel)
    """

    output: TaskNode[OutT] | SubWorkflowNode[OutT] | None = None
    """
    - Explicit output node: WorkflowHandle.get() returns this node's result
    - If None: get() returns dict of all terminal node results keyed by node_id
    """

    success_policy: SuccessPolicy | None = None
    """
    - Custom success criteria: workflow COMPLETED if any SuccessCase is satisfied
    - If None (default): any task failure → workflow FAILED
    """

    workflow_def_module: str | None = None
    """
    - Module path of WorkflowDefinition class (for import fallback in workers)
    """

    workflow_def_qualname: str | None = None
    """
    - Qualified name of WorkflowDefinition class (for import fallback in workers)
    """

    broker: PostgresBroker | None = field(default=None, repr=False)
    """
    - Database broker for start()/start_async()
    - Set automatically by app.workflow()
    """

    def __post_init__(self) -> None:
        """Validate DAG structure on construction.

        Phase-gated: node_id errors gate the rest (downstream needs valid IDs).
        All other validations run and collect errors together.
        """
        self._assign_indices()

        # Gate 1: node_id assignment — if any ID errors, skip remaining validation
        node_id_errors = self._collect_node_id_errors()
        if node_id_errors:
            report = ValidationReport('workflow')
            for error in node_id_errors:
                report.add(error)
            raise_collected(report)
            return

        # Gate 2: collect all remaining validation errors
        report = ValidationReport('workflow')
        for error in self._collect_dag_errors():
            report.add(error)
        for error in self._collect_args_from_errors():
            report.add(error)
        for error in self._collect_workflow_ctx_from_errors():
            report.add(error)
        for error in self._collect_args_with_injection_errors():
            report.add(error)
        for error in self._collect_kwargs_args_from_overlap_errors():
            report.add(error)
        for error in self._collect_subworkflow_default_build_with_param_errors():
            report.add(error)
        for error in self._collect_subworkflow_build_with_binding_errors():
            report.add(error)
        for error in self._collect_invalid_kwargs_errors():
            report.add(error)
        for error in self._collect_args_from_type_errors():
            report.add(error)
        for error in self._collect_missing_required_param_errors():
            report.add(error)
        for error in self._collect_output_errors():
            report.add(error)
        for error in self._collect_success_policy_errors():
            report.add(error)
        for error in self._collect_join_semantics_errors():
            report.add(error)
        for error in self._collect_subworkflow_retry_mode_errors():
            report.add(error)
        for error in self._collect_subworkflow_cycle_errors():
            report.add(error)

        raise_collected(report)

        # Only if clean: conditions + registration
        self._validate_conditions()
        self._register_for_conditions()

    def _assign_indices(self) -> None:
        """Assign index to each TaskNode based on list position."""
        for i, task in enumerate(self.tasks):
            task.index = i

    def _collect_node_id_errors(self) -> list[WorkflowValidationError]:
        """Assign node_id to each TaskNode if missing and validate uniqueness.

        Returns all node_id errors instead of raising on first.

        node_id source:
        - User provides workflow NAME (e.g., "My Pipeline")
        - node_id is either:
          a) Derived from workflow name: slugify(name) + ":" + index
          b) Explicitly provided by user on TaskNode
        - Errors must distinguish between (a) and (b) so users know what to fix
        """
        errors: list[WorkflowValidationError] = []
        seen_ids: set[str] = set()
        for task in self.tasks:
            # Track whether node_id comes from workflow name or was explicitly set
            node_id_from_workflow_name = task.node_id is None
            if node_id_from_workflow_name:
                if task.index is None:
                    errors.append(
                        WorkflowValidationError(
                            message='TaskNode index is not set before assigning node_id',
                            code=ErrorCode.WORKFLOW_INVALID_NODE_ID,
                        )
                    )
                    continue
                task.node_id = f'{slugify(self.name)}:{task.index}'

            node_id = task.node_id
            if node_id is None or not node_id.strip():
                errors.append(
                    WorkflowValidationError(
                        message='TaskNode node_id must be a non-empty string',
                        code=ErrorCode.WORKFLOW_INVALID_NODE_ID,
                    )
                )
                continue
            if len(node_id) > 128:
                if node_id_from_workflow_name:
                    errors.append(
                        WorkflowValidationError(
                            message='workflow name too long',
                            code=ErrorCode.WORKFLOW_INVALID_NODE_ID,
                            notes=[
                                f"workflow name: '{self.name}'",
                                f'derived node_id would be {len(node_id)} characters (max 128)',
                            ],
                            help_text='use a shorter workflow name',
                        )
                    )
                else:
                    errors.append(
                        WorkflowValidationError(
                            message='TaskNode node_id exceeds 128 characters',
                            code=ErrorCode.WORKFLOW_INVALID_NODE_ID,
                            notes=[
                                f"node_id '{node_id}' has {len(node_id)} characters"
                            ],
                            help_text='use a shorter node_id (max 128 characters)',
                        )
                    )
                continue
            if NODE_ID_PATTERN.match(node_id) is None:
                if node_id_from_workflow_name:
                    # This should never happen since slugify() sanitizes the name
                    errors.append(
                        WorkflowValidationError(
                            message='workflow name produced invalid characters (internal error)',
                            code=ErrorCode.WORKFLOW_INVALID_NODE_ID,
                            notes=[
                                f"workflow name: '{self.name}'",
                                f"derived node_id: '{node_id}'",
                                'slugify() failed to sanitize the name',
                            ],
                            help_text='please report this bug',
                        )
                    )
                else:
                    errors.append(
                        WorkflowValidationError(
                            message='TaskNode node_id contains invalid characters',
                            code=ErrorCode.WORKFLOW_INVALID_NODE_ID,
                            notes=[f"node_id '{node_id}'"],
                            help_text='node_id must match pattern: [A-Za-z0-9_\\-:.]+',
                        )
                    )
                continue
            if node_id in seen_ids:
                errors.append(
                    WorkflowValidationError(
                        message=f"duplicate node_id '{node_id}'",
                        code=ErrorCode.WORKFLOW_DUPLICATE_NODE_ID,
                        help_text='each TaskNode must have a unique node_id within the workflow',
                    )
                )
            seen_ids.add(node_id)
        return errors

    def _collect_dag_errors(self) -> list[WorkflowValidationError]:
        """Validate DAG structure. Returns all errors found."""
        errors: list[WorkflowValidationError] = []

        # 1. Check for roots (tasks with no dependencies)
        roots = [t for t in self.tasks if not t.waits_for]
        if not roots:
            errors.append(
                WorkflowValidationError(
                    message='no root tasks found',
                    code=ErrorCode.WORKFLOW_NO_ROOT_TASKS,
                    notes=[
                        'all tasks have dependencies, creating an impossible start condition',
                    ],
                    help_text='at least one task must have empty waits_for list',
                )
            )

        # 2. Validate dependency references exist in workflow
        task_ids = set(id(t) for t in self.tasks)
        for task in self.tasks:
            for dep in task.waits_for:
                if id(dep) not in task_ids:
                    errors.append(
                        WorkflowValidationError(
                            message='dependency references task not in workflow',
                            code=ErrorCode.WORKFLOW_INVALID_DEPENDENCY,
                            notes=[
                                f"task '{task.name}' waits for a TaskNode not in this workflow",
                            ],
                            help_text='ensure all dependencies are included in the workflow tasks list',
                        )
                    )

        # 3. Cycle detection (Kahn's algorithm) over valid dependencies only
        in_degree: dict[int, int] = {}
        for task in self.tasks:
            idx = task.index
            if idx is None:
                continue
            in_degree[idx] = 0
            for dep in task.waits_for:
                if id(dep) in task_ids and dep.index is not None:
                    in_degree[idx] += 1

        queue = [
            t.index
            for t in self.tasks
            if t.index is not None and in_degree.get(t.index, 0) == 0
        ]
        visited = 0

        while queue:
            node_idx = queue.pop(0)
            visited += 1
            for task in self.tasks:
                dep_indices = [
                    d.index
                    for d in task.waits_for
                    if id(d) in task_ids and d.index is not None
                ]
                if node_idx in dep_indices:
                    task_idx = task.index
                    if task_idx is not None:
                        in_degree[task_idx] -= 1
                        if in_degree[task_idx] == 0:
                            queue.append(task_idx)

        if visited != len(self.tasks):
            errors.append(
                WorkflowValidationError(
                    message='cycle detected in workflow DAG',
                    code=ErrorCode.WORKFLOW_CYCLE_DETECTED,
                    notes=['workflows must be acyclic directed graphs (DAG)'],
                    help_text='remove circular dependencies between tasks',
                )
            )

        return errors

    def _collect_args_from_errors(self) -> list[WorkflowValidationError]:
        """Validate args_from references are valid dependencies. Returns all errors."""
        errors: list[WorkflowValidationError] = []
        for task in self.tasks:
            deps_ids = set(id(d) for d in task.waits_for)
            for kwarg_name, source_node in task.args_from.items():
                if id(source_node) not in deps_ids:
                    errors.append(
                        WorkflowValidationError(
                            message='args_from references task not in waits_for',
                            code=ErrorCode.WORKFLOW_INVALID_ARGS_FROM,
                            notes=[
                                f"task '{task.name}' args_from['{kwarg_name}'] references '{source_node.name}'",
                                f"'{source_node.name}' must be in waits_for to inject its result",
                            ],
                            help_text=f"add '{source_node.name}' to waits_for list",
                        )
                    )
        return errors

    def _collect_workflow_ctx_from_errors(self) -> list[WorkflowValidationError]:
        """Validate workflow_ctx_from references are valid dependencies. Returns all errors."""
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            if node.workflow_ctx_from is None:
                continue
            deps_ids = set(id(d) for d in node.waits_for)
            for ctx_node in node.workflow_ctx_from:
                if id(ctx_node) not in deps_ids:
                    errors.append(
                        WorkflowValidationError(
                            message='workflow_ctx_from references task not in waits_for',
                            code=ErrorCode.WORKFLOW_INVALID_CTX_FROM,
                            notes=[
                                f"node '{node.name}' references '{ctx_node.name}'",
                                f"'{ctx_node.name}' must be in waits_for to use in workflow_ctx_from",
                            ],
                            help_text=f"add '{ctx_node.name}' to waits_for list",
                        )
                    )

            # Only check function parameter for TaskNode (SubWorkflowNode has no fn)
            if isinstance(node, SubWorkflowNode):
                continue

            task = node
            if not _task_accepts_workflow_ctx(task.fn):
                # Get the original function for accurate source location
                original_fn = getattr(task.fn, '_original_fn', task.fn)
                fn_location = SourceLocation.from_function(original_fn)

                errors.append(
                    WorkflowValidationError(
                        message='workflow_ctx_from declared but function missing workflow_ctx param',
                        code=ErrorCode.WORKFLOW_CTX_PARAM_MISSING,
                        location=fn_location,  # May be None for non-function callables
                        notes=[
                            f"workflow '{self.name}'\n"
                            f"TaskNode '{task.name}' declares workflow_ctx_from=[...]\n"
                            f"but function '{task.name}' has no workflow_ctx parameter",
                        ],
                        help_text=(
                            'either:\n'
                            '  1. add `workflow_ctx: WorkflowContext | None` param to the function above if needs context\n'
                            '  2. remove `workflow_ctx_from` from the TaskNode definition if this was a mistake'
                        ),
                    )
                )
        return errors

    def _collect_args_with_injection_errors(self) -> list[WorkflowValidationError]:
        """Reject positional args when args_from/workflow_ctx_from are set."""
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            if not node.args:
                continue
            has_args_from = bool(node.args_from)
            has_ctx_from = node.workflow_ctx_from is not None
            if not has_args_from and not has_ctx_from:
                continue

            injected_sources: list[str] = []
            if has_args_from:
                injected_sources.append('args_from')
            if has_ctx_from:
                injected_sources.append('workflow_ctx_from')

            injected_str = ' and '.join(injected_sources)

            errors.append(
                WorkflowValidationError(
                    message=(
                        'positional args not allowed when using args_from or workflow_ctx_from'
                    ),
                    code=ErrorCode.WORKFLOW_ARGS_WITH_INJECTION,
                    notes=[
                        f"node '{node.name}' sets args=(...) and also {injected_str}",
                        'positional args are only supported when args_from/workflow_ctx_from are not used',
                    ],
                    help_text=(
                        'move static inputs into kwargs=... and reserve args_from/workflow_ctx_from '
                        'for injected values'
                    ),
                )
            )
        return errors

    def _collect_kwargs_args_from_overlap_errors(self) -> list[WorkflowValidationError]:
        """Reject nodes where kwargs and args_from share any key."""
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            overlap = set(node.kwargs.keys()) & set(node.args_from.keys())
            if not overlap:
                continue
            errors.append(
                WorkflowValidationError(
                    message='kwargs and args_from keys must be disjoint',
                    code=ErrorCode.WORKFLOW_KWARGS_ARGS_FROM_OVERLAP,
                    notes=[
                        f"node '{node.name}' has overlapping key(s): {sorted(overlap)}",
                        'args_from would silently overwrite the static kwarg value at runtime',
                    ],
                    help_text='remove the overlapping key from either kwargs or args_from',
                ),
            )
        return errors

    def _collect_subworkflow_default_build_with_param_errors(
        self,
    ) -> list[WorkflowValidationError]:
        """
        Reject parameterized SubWorkflowNode when build_with is not overridden.

        Default WorkflowDefinition.build_with(app, *args, **params) ignores all
        runtime parameters and just forwards to build(app), which is a silent
        data-loss foot-gun when users pass kwargs/args_from/args.
        """
        errors: list[WorkflowValidationError] = []

        default_build_with = getattr(
            WorkflowDefinition.build_with,
            '__func__',
            WorkflowDefinition.build_with,
        )
        for node in self.tasks:
            if not isinstance(node, SubWorkflowNode):
                continue
            if node.args and (node.args_from or node.workflow_ctx_from is not None):
                continue  # Covered by WORKFLOW_ARGS_WITH_INJECTION
            has_runtime_params = bool(node.args) or bool(node.kwargs) or bool(node.args_from)
            if not has_runtime_params:
                continue

            build_with_fn = getattr(
                node.workflow_def.build_with,
                '__func__',
                node.workflow_def.build_with,
            )
            if build_with_fn is not default_build_with:
                continue

            notes: list[str] = [
                f"subworkflow '{node.name}' provides runtime params but uses default build_with()",
            ]
            if node.args:
                notes.append(f'positional args count: {len(node.args)}')
            if node.kwargs:
                notes.append(f'kwargs keys: {sorted(node.kwargs.keys())}')
            if node.args_from:
                notes.append(f'args_from keys: {sorted(node.args_from.keys())}')

            errors.append(
                WorkflowValidationError(
                    message='subworkflow params require overriding build_with',
                    code=ErrorCode.WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH,
                    notes=notes,
                    help_text=(
                        f"override {node.workflow_def.__name__}.build_with(app, ...) "
                        'to explicitly accept and apply runtime parameters'
                    ),
                )
            )

        return errors

    def _collect_subworkflow_build_with_binding_errors(
        self,
    ) -> list[WorkflowValidationError]:
        """
        Validate subworkflow runtime parameters bind cleanly to build_with.

        This fills gaps not covered by key/missing checks:
        - too many positional arguments
        - same param supplied by positional args and kwargs/args_from
        """
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            if not isinstance(node, SubWorkflowNode):
                continue
            if node.args and (node.args_from or node.workflow_ctx_from is not None):
                continue  # Covered by WORKFLOW_ARGS_WITH_INJECTION

            # Skip to avoid duplicate noise; overlap has a dedicated validator.
            if set(node.kwargs.keys()) & set(node.args_from.keys()):
                continue

            sig = _get_signature(node.workflow_def.build_with)
            if sig is None:
                continue

            provided_positional = [object(), *node.args]
            provided_kwargs = set(node.kwargs.keys()) | set(node.args_from.keys())

            consumed = 0
            accepts_var_positional = False
            positional_bound_kw_names: set[str] = set()
            for param in sig.parameters.values():
                if param.kind == inspect.Parameter.VAR_POSITIONAL:
                    accepts_var_positional = True
                    break
                if param.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ):
                    if consumed < len(provided_positional):
                        consumed += 1
                        if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                            positional_bound_kw_names.add(param.name)

            if consumed < len(provided_positional) and not accepts_var_positional:
                errors.append(
                    WorkflowValidationError(
                        message='subworkflow build_with received too many positional args',
                        code=ErrorCode.WORKFLOW_SUBWORKFLOW_BUILD_WITH_BINDING,
                        notes=[
                            f"node '{node.name}' passed {len(node.args)} user positional arg(s)",
                            "build_with signature cannot accept that many positional arguments",
                        ],
                        help_text='remove extra positional args or add *args to build_with',
                    )
                )
                continue

            duplicate = sorted(provided_kwargs & positional_bound_kw_names)
            if duplicate:
                errors.append(
                    WorkflowValidationError(
                        message='subworkflow param provided by both args and kwargs/args_from',
                        code=ErrorCode.WORKFLOW_SUBWORKFLOW_BUILD_WITH_BINDING,
                        notes=[
                            f"node '{node.name}' duplicates parameter(s): {duplicate}",
                            'the same build_with parameter is bound positionally and by keyword',
                        ],
                        help_text='provide each build_with parameter exactly once',
                    )
                )

        return errors

    def _collect_invalid_kwargs_errors(self) -> list[WorkflowValidationError]:
        """Validate kwargs/args_from keys match function parameter names."""
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            if not node.kwargs and not node.args_from:
                continue

            # Get the function to inspect (TaskNode vs SubWorkflowNode)
            if isinstance(node, TaskNode):
                fn = node.fn
                exclude_names: set[str] = set()
            else:
                # SubWorkflowNode - validate against build_with signature
                fn = node.workflow_def.build_with
                exclude_names = {'app', 'cls'}

            sig = _get_signature(fn)
            if sig is None:
                continue  # Can't validate (uninspectable)

            if _signature_accepts_kwargs(sig):
                continue  # Accepts **kwargs, any key is valid

            valid_names = _valid_kwarg_names(sig, exclude=exclude_names)

            invalid_kwargs = set(node.kwargs.keys()) - valid_names
            invalid_args_from = set(node.args_from.keys()) - valid_names
            if not invalid_kwargs and not invalid_args_from:
                continue

            notes: list[str] = []
            if invalid_kwargs:
                notes.append(
                    f"node '{node.name}' has unknown kwargs: {sorted(invalid_kwargs)}"
                )
            if invalid_args_from:
                notes.append(
                    f"node '{node.name}' has unknown args_from keys: {sorted(invalid_args_from)}"
                )
            notes.append(f"valid parameters: {sorted(valid_names)}")

            errors.append(
                WorkflowValidationError(
                    message='invalid kwarg key(s) for callable signature',
                    code=ErrorCode.WORKFLOW_INVALID_KWARG_KEY,
                    notes=notes,
                    help_text='check for typos in kwarg or args_from keys',
                )
            )
        return errors

    def _collect_args_from_type_errors(self) -> list[WorkflowValidationError]:
        """
        Validate args_from source result types against callable parameter annotations.

        args_from always injects TaskResult objects, so target params should be
        annotated as TaskResult[OkT, TaskError] (or Optional/Union containing it).
        """
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            if not node.args_from:
                continue
            if node.args and (node.args_from or node.workflow_ctx_from is not None):
                continue  # Covered by WORKFLOW_ARGS_WITH_INJECTION.
            if set(node.kwargs.keys()) & set(node.args_from.keys()):
                continue  # Covered by WORKFLOW_KWARGS_ARGS_FROM_OVERLAP.

            deps_ids = {id(dep) for dep in node.waits_for}
            if any(id(source_node) not in deps_ids for source_node in node.args_from.values()):
                continue  # Covered by WORKFLOW_INVALID_ARGS_FROM.

            # Get the function to inspect (TaskNode vs SubWorkflowNode)
            if isinstance(node, TaskNode):
                fn = node.fn
                exclude_names: set[str] = set()
            else:
                fn = node.workflow_def.build_with
                exclude_names = {'app', 'cls'}

            sig = _get_signature(fn)
            if sig is None:
                continue

            hints = _resolved_type_hints(fn)
            accepts_kwargs = _signature_accepts_kwargs(sig)

            for kwarg_name, source_node in node.args_from.items():
                if kwarg_name in exclude_names:
                    continue

                param = sig.parameters.get(kwarg_name)
                if param is None:
                    if accepts_kwargs:
                        # No parameter-level annotation available for **kwargs.
                        continue
                    continue  # Covered by WORKFLOW_INVALID_KWARG_KEY.

                annotation = hints.get(kwarg_name)
                if annotation is None and param.annotation is not inspect.Parameter.empty:
                    annotation = param.annotation
                expected_ok = _extract_taskresult_ok_type(annotation)
                if expected_ok is None:
                    errors.append(
                        WorkflowValidationError(
                            message='args_from target parameter must be annotated as TaskResult[OkT, TaskError]',
                            code=ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH,
                            notes=[
                                f"node '{node.name}' args_from['{kwarg_name}'] targets parameter '{kwarg_name}'",
                                f"parameter annotation is {_format_type_name(annotation)}",
                                'args_from injects TaskResult values, not raw payloads',
                            ],
                            help_text=(
                                f"annotate '{kwarg_name}' as TaskResult[ExpectedType, TaskError]"
                            ),
                        )
                    )
                    continue

                source_ok = _resolve_source_node_ok_type(source_node)
                if source_ok is None:
                    errors.append(
                        WorkflowValidationError(
                            message='unable to resolve args_from source output type',
                            code=ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH,
                            notes=[
                                f"node '{node.name}' args_from['{kwarg_name}'] references source '{source_node.name}'",
                                "source output type could not be inferred from task return annotation or workflow generic",
                            ],
                            help_text=(
                                "ensure source task returns TaskResult[ConcreteType, TaskError] "
                                'or source subworkflow subclasses WorkflowDefinition[ConcreteType]'
                            ),
                        )
                    )
                    continue

                if _is_ok_type_compatible(source_ok, expected_ok):
                    continue

                errors.append(
                    WorkflowValidationError(
                        message='args_from source type does not match target parameter type',
                        code=ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH,
                        notes=[
                            f"node '{node.name}' args_from['{kwarg_name}'] receives from '{source_node.name}'",
                            f'source TaskResult ok-type: {_format_type_name(source_ok)}',
                            f"target expects TaskResult[{_format_type_name(expected_ok)}, TaskError]",
                        ],
                        help_text='fix args_from wiring or update the target parameter type annotation',
                    )
                )

        return errors

    def _collect_missing_required_param_errors(self) -> list[WorkflowValidationError]:
        """Validate required parameters are provided via args/kwargs/args_from."""
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            # Get the function to inspect (TaskNode vs SubWorkflowNode)
            if isinstance(node, TaskNode):
                fn = node.fn
                args_provided = list(node.args)
                injected_kwargs: set[str] = set()
            else:
                fn = node.workflow_def.build_with
                # build_with(app, *args, **kwargs) always provides the first positional
                args_provided = [object(), *node.args]
                injected_kwargs = set()

            sig = _get_signature(fn)
            if sig is None:
                continue  # Can't validate

            provided_kwargs = set(node.kwargs.keys()) | set(node.args_from.keys())

            # Auto-injected workflow context for TaskNode when workflow_ctx_from is set
            if isinstance(node, TaskNode):
                if node.workflow_ctx_from is not None and 'workflow_ctx' in sig.parameters:
                    injected_kwargs.add('workflow_ctx')
                # WorkflowMeta is auto-injected if declared
                if 'workflow_meta' in sig.parameters:
                    injected_kwargs.add('workflow_meta')

            provided_kwargs |= injected_kwargs

            missing: list[str] = []
            consumed_positional = 0

            for param in sig.parameters.values():
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue  # *args/**kwargs do not require values
                if param.default is not inspect.Parameter.empty:
                    continue  # optional

                if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                    if consumed_positional < len(args_provided):
                        consumed_positional += 1
                        continue
                    missing.append(param.name)
                elif param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                    if consumed_positional < len(args_provided):
                        consumed_positional += 1
                        continue
                    if param.name in provided_kwargs:
                        continue
                    missing.append(param.name)
                elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                    if param.name in provided_kwargs:
                        continue
                    missing.append(param.name)

            if not missing:
                continue

            target = 'task function' if isinstance(node, TaskNode) else 'build_with'
            errors.append(
                WorkflowValidationError(
                    message=f'missing required parameters for {target}',
                    code=ErrorCode.WORKFLOW_MISSING_REQUIRED_PARAMS,
                    notes=[
                        f"node '{node.name}' missing required parameter(s): {sorted(missing)}",
                    ],
                    help_text='provide required params via args=..., kwargs=..., or args_from',
                )
            )
        return errors

    def _collect_output_errors(self) -> list[WorkflowValidationError]:
        """Validate output task is in the workflow. Returns all errors."""
        errors: list[WorkflowValidationError] = []
        if self.output is None:
            return errors
        task_ids = set(id(t) for t in self.tasks)
        if id(self.output) not in task_ids:
            errors.append(
                WorkflowValidationError(
                    f"Output task '{self.output.name}' is not in workflow",
                )
            )
        return errors

    def _collect_success_policy_errors(self) -> list[WorkflowValidationError]:
        """Validate success policy references are valid workflow tasks. Returns all errors."""
        errors: list[WorkflowValidationError] = []
        if self.success_policy is None:
            return errors

        # Validate cases list is not empty
        if not self.success_policy.cases:
            errors.append(
                WorkflowValidationError(
                    'SuccessPolicy must have at least one SuccessCase',
                )
            )
            return errors

        task_ids = set(id(t) for t in self.tasks)

        # Validate each success case
        for i, case in enumerate(self.success_policy.cases):
            if not case.required:
                errors.append(
                    WorkflowValidationError(
                        f'SuccessCase[{i}] has no required tasks',
                    )
                )
            for task in case.required:
                if id(task) not in task_ids:
                    errors.append(
                        WorkflowValidationError(
                            f"SuccessCase[{i}] required task '{task.name}' is not in workflow",
                        )
                    )

        # Validate optional tasks
        if self.success_policy.optional:
            for task in self.success_policy.optional:
                if id(task) not in task_ids:
                    errors.append(
                        WorkflowValidationError(
                            f"SuccessPolicy optional task '{task.name}' is not in workflow",
                        )
                    )

        return errors

    def _collect_join_semantics_errors(self) -> list[WorkflowValidationError]:
        """Validate join and min_success settings. Returns all errors."""
        errors: list[WorkflowValidationError] = []
        for task in self.tasks:
            if task.join == 'quorum':
                if task.min_success is None:
                    errors.append(
                        WorkflowValidationError(
                            f"Task '{task.name}' has join='quorum' but min_success is not set",
                        )
                    )
                elif task.min_success < 1:
                    errors.append(
                        WorkflowValidationError(
                            f"Task '{task.name}' min_success must be >= 1, got {task.min_success}",
                        )
                    )
                else:
                    dep_count = len(task.waits_for)
                    if task.min_success > dep_count:
                        errors.append(
                            WorkflowValidationError(
                                f"Task '{task.name}' min_success ({task.min_success}) exceeds "
                                f'dependency count ({dep_count})',
                            )
                        )
            elif task.join in ('all', 'any'):
                if task.min_success is not None:
                    errors.append(
                        WorkflowValidationError(
                            f"Task '{task.name}' has min_success set but join='{task.join}' "
                            "(min_success is only used with join='quorum')",
                        )
                    )
        return errors

    def _validate_conditions(self) -> None:
        """Validate condition callables have required context dependencies."""
        for task in self.tasks:
            has_condition = task.run_when is not None or task.skip_when is not None
            if has_condition and not task.workflow_ctx_from:
                # Conditions require context, but no context sources specified
                # This is allowed (empty context), but may cause KeyError if
                # condition tries to access dependency results
                pass  # Allow - user may have conditions that don't use context

    def _collect_subworkflow_cycle_errors(self) -> list[WorkflowValidationError]:
        """Detect cycles in nested workflow definitions. Returns all errors.

        Prevents circular references like:
        - WorkflowA contains SubWorkflowNode(WorkflowB)
          and WorkflowB contains SubWorkflowNode(WorkflowA)

        Uses DFS with a recursion stack to detect back-edges.
        """
        errors: list[WorkflowValidationError] = []
        visited: set[str] = set()
        stack: set[str] = set()

        def visit(workflow_name: str, workflow_class: type[Any]) -> None:
            """DFS visit with cycle detection via recursion stack."""
            if workflow_name in stack:
                # Found a back-edge - this is a cycle
                errors.append(
                    WorkflowValidationError(
                        message='cycle detected in nested workflows',
                        code=ErrorCode.WORKFLOW_CYCLE_DETECTED,
                        notes=[
                            f"workflow '{workflow_name}' creates a circular reference",
                            'cycles in nested workflows are not allowed',
                        ],
                        help_text='remove the circular SubWorkflowNode reference',
                    )
                )
                return

            if workflow_name in visited:
                # Already fully explored this workflow, no cycle through here
                return

            visited.add(workflow_name)
            stack.add(workflow_name)

            # Check all SubWorkflowNodes in this workflow's definition
            nodes = workflow_class.get_workflow_nodes()
            if nodes:
                for _, wf_node in nodes:
                    if isinstance(wf_node, SubWorkflowNode):
                        wf_node_any = cast(SubWorkflowNode[Any], wf_node)
                        workflow_def = wf_node_any.workflow_def
                        child_name: str = workflow_def.name
                        visit(child_name, workflow_def)

            # Done exploring this workflow
            stack.remove(workflow_name)

        # Start DFS from each SubWorkflowNode in this workflow's tasks
        for node in self.tasks:
            if isinstance(node, SubWorkflowNode):
                visit(node.workflow_def.name, node.workflow_def)

        return errors

    def _collect_subworkflow_retry_mode_errors(self) -> list[WorkflowValidationError]:
        """Reject unsupported subworkflow retry modes. Returns all errors."""
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            if not isinstance(node, SubWorkflowNode):
                continue
            if node.retry_mode != SubWorkflowRetryMode.RERUN_FAILED_ONLY:
                errors.append(
                    WorkflowValidationError(
                        message='unsupported SubWorkflowRetryMode',
                        code=ErrorCode.WORKFLOW_INVALID_SUBWORKFLOW_RETRY_MODE,
                        notes=[
                            f"node '{node.name}' uses retry_mode='{node.retry_mode.value}'",
                            "only 'rerun_failed_only' is supported in this release",
                        ],
                        help_text='use SubWorkflowRetryMode.RERUN_FAILED_ONLY',
                    )
                )
        return errors

    def _register_for_conditions(self) -> None:
        """Register this spec for condition evaluation at runtime."""
        # Register if any task has conditions OR any SubWorkflowNode exists
        has_conditions = any(
            t.run_when is not None or t.skip_when is not None for t in self.tasks
        )
        has_subworkflow = any(isinstance(t, SubWorkflowNode) for t in self.tasks)
        if has_conditions or has_subworkflow:
            from horsies.core.workflows.registry import register_workflow_spec

            register_workflow_spec(self)

    def start(self, workflow_id: str | None = None) -> 'WorkflowHandle[OutT]':
        """
        Start workflow execution.

        Args:
            workflow_id: Optional custom workflow ID. Auto-generated if not provided.

        Returns:
            WorkflowHandle for tracking and retrieving results.

        Raises:
            RuntimeError: If broker is not configured.
        """
        if self.broker is None:
            raise RuntimeError(
                'WorkflowSpec requires a broker. Use app.workflow() or set broker.'
            )

        # Import here to avoid circular imports
        from horsies.core.workflows.engine import start_workflow

        return start_workflow(self, self.broker, workflow_id)

    async def start_async(
        self, workflow_id: str | None = None
    ) -> 'WorkflowHandle[OutT]':
        """
        Start workflow execution (async).

        Args:
            workflow_id: Optional custom workflow ID. Auto-generated if not provided.

        Returns:
            WorkflowHandle for tracking and retrieving results.

        Raises:
            RuntimeError: If broker is not configured.
        """
        if self.broker is None:
            raise RuntimeError(
                'WorkflowSpec requires a broker. Use app.workflow() or set broker.'
            )

        # Import here to avoid circular imports
        from horsies.core.workflows.engine import start_workflow_async

        return await start_workflow_async(self, self.broker, workflow_id)


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
# WorkflowContext (type-safe result access via result_for)
# =============================================================================


class WorkflowContextMissingIdError(RuntimeError):
    """Raised when TaskNode node_id is missing for WorkflowContext.result_for()."""


class WorkflowHandleMissingIdError(RuntimeError):
    """Raised when TaskNode node_id is missing for WorkflowHandle.result_for()."""


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
    _results_by_id: dict[str, 'TaskResult[Any, TaskError]'] = {}
    # Internal storage: subworkflow summaries keyed by node_id
    _summaries_by_id: dict[str, 'SubWorkflowSummary[Any]'] = {}

    def __init__(
        self,
        workflow_id: str,
        task_index: int,
        task_name: str,
        results_by_id: dict[str, 'TaskResult[Any, TaskError]'] | None = None,
        summaries_by_id: dict[str, 'SubWorkflowSummary[Any]'] | None = None,
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
    ) -> 'TaskResult[OkT, TaskError]':
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
        node: 'SubWorkflowNode[OkT]',
    ) -> 'SubWorkflowSummary[OkT]':
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

    def has_summary(self, node: 'SubWorkflowNode[Any]') -> bool:
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
        results_by_id: dict[str, 'TaskResult[Any, TaskError]'],
        summaries_by_id: dict[str, 'SubWorkflowSummary[Any]'] | None = None,
    ) -> 'WorkflowContext':
        """Reconstruct WorkflowContext from serialized data."""
        return cls(
            workflow_id=workflow_id,
            task_index=task_index,
            task_name=task_name,
            results_by_id=results_by_id,
            summaries_by_id=summaries_by_id,
        )


# =============================================================================
# SQL constants for WorkflowHandle
# =============================================================================

GET_WORKFLOW_STATUS_SQL = text("""
    SELECT status FROM horsies_workflows WHERE id = :wf_id
""")

GET_WORKFLOW_RESULT_SQL = text("""
    SELECT result FROM horsies_workflows WHERE id = :wf_id
""")

GET_WORKFLOW_ERROR_SQL = text("""
    SELECT error, status FROM horsies_workflows WHERE id = :wf_id
""")

GET_WORKFLOW_TASK_RESULTS_SQL = text("""
    SELECT node_id, result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND result IS NOT NULL
""")

GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL = text("""
    SELECT result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = :node_id
      AND result IS NOT NULL
""")

GET_WORKFLOW_TASKS_SQL = text("""
    SELECT node_id, task_index, task_name, status, result, started_at, completed_at
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
    ORDER BY task_index
""")

CANCEL_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'CANCELLED', updated_at = NOW()
    WHERE id = :wf_id AND status IN ('PENDING', 'RUNNING', 'PAUSED')
""")

SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND status IN ('PENDING', 'READY')
""")


# =============================================================================
# WorkflowHandle
# =============================================================================


@dataclass
class WorkflowTaskInfo:
    """Information about a task within a workflow."""

    node_id: str | None
    index: int
    name: str
    status: WorkflowTaskStatus
    result: TaskResult[Any, TaskError] | None
    started_at: datetime | None
    completed_at: datetime | None


@dataclass
class WorkflowHandle(Generic[OutT]):
    """
    Handle for tracking and retrieving workflow results.

    Provides methods to:
    - Check workflow status
    - Wait for and retrieve results
    - Inspect individual task states
    - Cancel the workflow
    """

    workflow_id: str
    broker: PostgresBroker

    def status(self) -> WorkflowStatus:
        """Get current workflow status."""

        runner = LoopRunner()
        try:
            return runner.call(self.status_async)
        finally:
            runner.stop()

    async def status_async(self) -> WorkflowStatus:
        """Async version of status()."""
        async with self.broker.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_STATUS_SQL,
                {'wf_id': self.workflow_id},
            )
            row = result.fetchone()
            if row is None:
                raise ValueError(f'Workflow {self.workflow_id} not found')
            return WorkflowStatus(row[0])

    def get(self, timeout_ms: int | None = None) -> TaskResult[OutT, TaskError]:
        """
        Block until workflow completes or timeout.

        Returns:
            If output task specified: that task's TaskResult
            Otherwise: TaskResult containing dict of terminal task results
        """

        runner = LoopRunner()
        try:
            return runner.call(self.get_async, timeout_ms)
        finally:
            runner.stop()

    async def get_async(
        self, timeout_ms: int | None = None
    ) -> TaskResult[OutT, TaskError]:
        """Async version of get()."""

        from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode

        start = time.monotonic()
        timeout_sec = timeout_ms / 1000 if timeout_ms else None

        while True:
            # Check current status
            status = await self.status_async()

            if status == WorkflowStatus.COMPLETED:
                return await self._get_result()

            if status in (WorkflowStatus.FAILED, WorkflowStatus.CANCELLED):
                return await self._get_error()

            if status == WorkflowStatus.PAUSED:
                return cast(
                    'TaskResult[OutT, TaskError]',
                    TaskResult(
                        err=TaskError(
                            error_code='WORKFLOW_PAUSED',
                            message='Workflow is paused awaiting intervention',
                        )
                    ),
                )

            # Check timeout
            elapsed = time.monotonic() - start
            if timeout_sec and elapsed >= timeout_sec:
                return cast(
                    'TaskResult[OutT, TaskError]',
                    TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.WAIT_TIMEOUT,
                            message=f'Workflow did not complete within {timeout_ms}ms',
                        )
                    ),
                )

            # Wait for notification or poll
            remaining = (timeout_sec - elapsed) if timeout_sec else 5.0
            await self._wait_for_completion(min(remaining, 5.0))

    async def _get_result(self) -> TaskResult[OutT, TaskError]:
        """Fetch completed workflow result."""
        from horsies.core.models.tasks import TaskResult
        from horsies.core.codec.serde import loads_json, task_result_from_json

        async with self.broker.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_RESULT_SQL,
                {'wf_id': self.workflow_id},
            )
            row = result.fetchone()
            if row and row[0]:
                return cast(
                    'TaskResult[OutT, TaskError]',
                    task_result_from_json(loads_json(row[0])),
                )
            return cast('TaskResult[OutT, TaskError]', TaskResult(ok=None))

    async def _get_error(self) -> TaskResult[OutT, TaskError]:
        """Fetch failed workflow error."""
        from horsies.core.models.tasks import TaskResult, TaskError
        from horsies.core.codec.serde import loads_json

        async with self.broker.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_ERROR_SQL,
                {'wf_id': self.workflow_id},
            )
            row = result.fetchone()
            if row and row[0]:
                error_data = loads_json(row[0])
                if isinstance(error_data, dict):
                    # Safely extract known TaskError fields with type narrowing
                    raw_code = error_data.get('error_code')
                    raw_msg = error_data.get('message')
                    return cast(
                        'TaskResult[OutT, TaskError]',
                        TaskResult(
                            err=TaskError(
                                error_code=str(raw_code) if raw_code is not None else None,
                                message=str(raw_msg) if raw_msg is not None else None,
                                data=error_data.get('data'),
                            )
                        ),
                    )
            status_str = row[1] if row else 'FAILED'
            return cast(
                'TaskResult[OutT, TaskError]',
                TaskResult(
                    err=TaskError(
                        error_code=f'WORKFLOW_{status_str}',
                        message=f'Workflow {status_str.lower()}',
                    )
                )
            )

    async def _wait_for_completion(self, timeout_sec: float) -> None:
        """Wait for workflow_done notification or poll interval."""
        import asyncio

        try:
            q = await self.broker.listener.listen('workflow_done')
            try:

                async def _wait_for_workflow() -> None:
                    while True:
                        note = await q.get()
                        if note.payload == self.workflow_id:
                            return

                await asyncio.wait_for(_wait_for_workflow(), timeout=timeout_sec)
            finally:
                await self.broker.listener.unsubscribe('workflow_done', q)
        except asyncio.TimeoutError:
            pass  # Polling fallback
        except RuntimeError:
            # Cross-loop access: listener dispatcher is on a different event loop
            # (e.g. sync handle.get() via LoopRunner). Fall back to sleep-based polling.
            await asyncio.sleep(min(timeout_sec, 1.0))

    def results(self) -> dict[str, TaskResult[Any, TaskError]]:
        """
        Get all task results keyed by unique identifier.

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".
        """
        from horsies.core.utils.loop_runner import LoopRunner

        runner = LoopRunner()
        try:
            return runner.call(self.results_async)
        finally:
            runner.stop()

    async def results_async(self) -> dict[str, TaskResult[Any, TaskError]]:
        """
        Async version of results().

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".
        """
        from horsies.core.codec.serde import loads_json, task_result_from_json

        async with self.broker.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_TASK_RESULTS_SQL,
                {'wf_id': self.workflow_id},
            )

            return {
                row[0]: task_result_from_json(loads_json(row[1]))
                for row in result.fetchall()
            }

    def result_for(
        self, node: TaskNode[OkT] | NodeKey[OkT]
    ) -> 'TaskResult[OkT, TaskError]':
        """
        Get the result for a specific TaskNode or NodeKey.

        Non-blocking: queries the database once and returns immediately.

        Args:
            node: The TaskNode or NodeKey whose result to retrieve.

        Returns:
            TaskResult[T, TaskError] where T matches the node's type.
            - If task completed: returns the task's result (success or error)
            - If task not completed: returns TaskResult with
              error_code=LibraryErrorCode.RESULT_NOT_READY

        Raises:
            WorkflowHandleMissingIdError: If node has no node_id assigned.

        Example:
            result = handle.result_for(node)
            if result.is_err() and result.err.error_code == LibraryErrorCode.RESULT_NOT_READY:
                # Task hasn't completed yet - wait or check later
                pass
        """
        from horsies.core.utils.loop_runner import LoopRunner

        runner = LoopRunner()
        try:
            return runner.call(self.result_for_async, node)
        finally:
            runner.stop()

    async def result_for_async(
        self, node: TaskNode[OkT] | NodeKey[OkT]
    ) -> 'TaskResult[OkT, TaskError]':
        """Async version of result_for(). See result_for() for full documentation."""
        from horsies.core.codec.serde import loads_json, task_result_from_json

        node_id: str | None
        if isinstance(node, NodeKey):
            node_id = node.node_id
        else:
            node_id = node.node_id

        if node_id is None:
            raise WorkflowHandleMissingIdError(
                'TaskNode node_id is not set. Ensure WorkflowSpec assigns node_id '
                'or provide an explicit node_id.'
            )

        async with self.broker.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL,
                {'wf_id': self.workflow_id, 'node_id': node_id},
            )
            row = result.fetchone()
            if row is None or row[0] is None:
                from horsies.core.models.tasks import (
                    TaskResult,
                    TaskError,
                    LibraryErrorCode,
                )

                return cast(
                    'TaskResult[OkT, TaskError]',
                    TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.RESULT_NOT_READY,
                            message=(
                                f"Task '{node_id}' has not completed yet "
                                f"in workflow '{self.workflow_id}'"
                            ),
                        )
                    ),
                )

            return cast(
                'TaskResult[OkT, TaskError]',
                task_result_from_json(loads_json(row[0])),
            )

    def tasks(self) -> list[WorkflowTaskInfo]:
        """Get status of all tasks in workflow."""
        from horsies.core.utils.loop_runner import LoopRunner

        runner = LoopRunner()
        try:
            return runner.call(self.tasks_async)
        finally:
            runner.stop()

    async def tasks_async(self) -> list[WorkflowTaskInfo]:
        """Async version of tasks()."""
        from horsies.core.codec.serde import loads_json, task_result_from_json

        async with self.broker.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_TASKS_SQL,
                {'wf_id': self.workflow_id},
            )

            return [
                WorkflowTaskInfo(
                    node_id=row[0],
                    index=row[1],
                    name=row[2],
                    status=WorkflowTaskStatus(row[3]),
                    result=task_result_from_json(loads_json(row[4]))
                    if row[4]
                    else None,
                    started_at=row[5],
                    completed_at=row[6],
                )
                for row in result.fetchall()
            ]

    def cancel(self) -> None:
        """Request workflow cancellation."""
        from horsies.core.utils.loop_runner import LoopRunner

        runner = LoopRunner()
        try:
            runner.call(self.cancel_async)
        finally:
            runner.stop()

    async def cancel_async(self) -> None:
        """Async version of cancel()."""
        async with self.broker.session_factory() as session:
            # Cancel workflow
            await session.execute(
                CANCEL_WORKFLOW_SQL,
                {'wf_id': self.workflow_id},
            )

            # Skip pending/ready tasks
            await session.execute(
                SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL,
                {'wf_id': self.workflow_id},
            )

            await session.commit()

    def pause(self) -> bool:
        """
        Pause a running workflow.

        Transitions workflow from RUNNING to PAUSED state. Already-running tasks
        will continue to completion, but no new tasks will be enqueued.

        Use resume() to continue execution.

        Returns:
            True if workflow was paused, False if not RUNNING (no-op)
        """
        from horsies.core.utils.loop_runner import LoopRunner

        runner = LoopRunner()
        try:
            return runner.call(self.pause_async)
        finally:
            runner.stop()

    async def pause_async(self) -> bool:
        """
        Async version of pause().

        Returns:
            True if workflow was paused, False if not RUNNING (no-op)
        """
        from horsies.core.workflows.engine import pause_workflow

        return await pause_workflow(self.broker, self.workflow_id)

    def resume(self) -> bool:
        """
        Resume a paused workflow.

        Re-evaluates all PENDING tasks (marks READY if deps are terminal) and
        enqueues all READY tasks. Only works if workflow is currently PAUSED.

        Returns:
            True if workflow was resumed, False if not PAUSED (no-op)
        """
        from horsies.core.utils.loop_runner import LoopRunner

        runner = LoopRunner()
        try:
            return runner.call(self.resume_async)
        finally:
            runner.stop()

    async def resume_async(self) -> bool:
        """
        Async version of resume().

        Returns:
            True if workflow was resumed, False if not PAUSED (no-op)
        """
        from horsies.core.workflows.engine import resume_workflow

        return await resume_workflow(self.broker, self.workflow_id)


# =============================================================================
# WorkflowDefinition (class-based workflow definition)
# =============================================================================


class WorkflowDefinitionMeta(type):
    """
    Metaclass for WorkflowDefinition that preserves attribute order.

    Collects TaskNode and SubWorkflowNode instances from class attributes
    in definition order.
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> 'WorkflowDefinitionMeta':
        cls = super().__new__(mcs, name, bases, namespace)

        # Skip processing for the base class itself
        if name == 'WorkflowDefinition':
            return cls

        # Collect TaskNode and SubWorkflowNode instances in definition order
        nodes: list[tuple[str, TaskNode[Any] | SubWorkflowNode[Any]]] = []
        for attr_name, attr_value in namespace.items():
            if isinstance(attr_value, (TaskNode, SubWorkflowNode)):
                nodes.append((attr_name, cast(AnyNode, attr_value)))

        # Store the collected nodes on the class
        cls._workflow_nodes = nodes  # type: ignore[attr-defined]

        return cls


class WorkflowDefinition(Generic[OkT_co], metaclass=WorkflowDefinitionMeta):
    """
    Base class for declarative workflow definitions.

    Generic parameter OkT represents the workflow's output type, derived from
    Meta.output task's return type.

    Provides a class-based alternative to app.workflow() for defining workflows.
    TaskNode and SubWorkflowNode instances defined as class attributes are
    automatically collected and used to build a WorkflowSpec.

    Example:
        class ScrapeWorkflow(WorkflowDefinition[PersistResult]):
            name = "scrape_pipeline"

            fetch = TaskNode(fn=fetch_listing, args=("url",))
            parse = TaskNode(fn=parse_listing, waits_for=[fetch], args_from={"raw": fetch})
            persist = TaskNode(fn=persist_listing, waits_for=[parse], args_from={"data": parse})

            class Meta:
                output = persist  # Output type is PersistResult
                on_error = OnError.FAIL

        spec = ScrapeWorkflow.build(app)

    Attributes:
        name: Required workflow name (class attribute).
        Meta: Optional inner class for workflow configuration.
            - output: TaskNode/SubWorkflowNode to use as workflow output (default: None)
            - on_error: Error handling policy (default: OnError.FAIL)
            - success_policy: Custom success policy (default: None)
    """

    # Class attributes to be defined by subclasses
    name: ClassVar[str]

    # Populated by metaclass
    _workflow_nodes: ClassVar[list[tuple[str, TaskNode[Any] | SubWorkflowNode[Any]]]]

    @classmethod
    def get_workflow_nodes(
        cls,
    ) -> list[tuple[str, TaskNode[Any] | SubWorkflowNode[Any]]]:
        """Return collected workflow nodes or an empty list if none were defined."""
        nodes = getattr(cls, '_workflow_nodes', None)
        if not isinstance(nodes, list):
            return []
        return cast(list[tuple[str, TaskNode[Any] | SubWorkflowNode[Any]]], nodes)

    @classmethod
    def build(cls, app: 'Horsies') -> WorkflowSpec[OkT_co]:
        """
        Build a WorkflowSpec from this workflow definition.

        Collects all TaskNode class attributes, assigns node_ids from attribute
        names, and creates a WorkflowSpec with the configured options.

        Args:
            app: Horsies application instance (provides broker).

        Returns:
            WorkflowSpec ready for execution.

        Raises:
            WorkflowValidationError: If workflow definition is invalid.
        """
        # Validate name is defined
        if not hasattr(cls, 'name') or not cls.name:
            raise WorkflowValidationError(
                f"WorkflowDefinition '{cls.__name__}' must define a 'name' class attribute"
            )

        # Get collected nodes from metaclass
        nodes = cls.get_workflow_nodes()
        if not nodes:
            raise WorkflowValidationError(
                f"WorkflowDefinition '{cls.__name__}' has no TaskNode attributes"
            )

        # Assign node_id from attribute name (if not already set)
        for attr_name, node in nodes:
            if node.node_id is None:
                node.node_id = attr_name

        # Extract task list (preserving definition order)
        tasks = [node for _, node in nodes]

        # Get Meta configuration
        output: TaskNode[OkT_co] | SubWorkflowNode[OkT_co] | None = None
        on_error: OnError = OnError.FAIL
        success_policy: SuccessPolicy | None = None

        meta: type[Any] | None = getattr(cls, 'Meta', None)
        if meta is not None:
            output = getattr(meta, 'output', None)
            on_error = getattr(meta, 'on_error', OnError.FAIL)
            success_policy = getattr(meta, 'success_policy', None)

        # Build WorkflowSpec
        spec = app.workflow(
            name=cls.name,
            tasks=tasks,
            output=output,
            on_error=on_error,
            success_policy=success_policy,
        )
        spec.workflow_def_module = cls.__module__
        spec.workflow_def_qualname = cls.__qualname__
        return cast('WorkflowSpec[OkT_co]', spec)

    @classmethod
    def build_with(
        cls,
        app: 'Horsies',
        *args: Any,
        **params: Any,
    ) -> WorkflowSpec[OkT_co]:
        """
        Build a WorkflowSpec with runtime parameters.

        Subclasses can override this to apply params to TaskNodes.
        Default implementation forwards to build().
        """
        _ = args
        _ = params
        spec = cls.build(app)
        spec.workflow_def_module = cls.__module__
        spec.workflow_def_qualname = cls.__qualname__
        return cast('WorkflowSpec[OkT_co]', spec)
