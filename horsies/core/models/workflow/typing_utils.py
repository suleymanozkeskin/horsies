"""Type inspection helpers for workflow validation."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import inspect
import re
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    Callable,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from horsies.core.errors import (
    ErrorCode,
    WorkflowValidationError,
)

from .nodes import TaskNode, SubWorkflowNode

if TYPE_CHECKING:
    from .definition import WorkflowDefinition
    from .spec import WorkflowSpec


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
    workflow_def: type[WorkflowDefinition[Any]],
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


def _resolve_source_node_ok_type(node: TaskNode[Any] | SubWorkflowNode[Any]) -> Any | None:
    """Resolve output ok-type for TaskNode/SubWorkflowNode source used in args_from."""
    if isinstance(node, TaskNode):
        return _resolve_task_fn_ok_type(node.fn)
    return _resolve_workflow_def_ok_type(node.workflow_def)


def validate_workflow_generic_output_match(
    workflow_cls: type[WorkflowDefinition[Any]],
    spec: WorkflowSpec[Any],
) -> None:
    """Validate that the declared WorkflowDefinition[OkT] matches the actual spec output type.

    Catches the 'lying generic' bug where a class declares e.g.
    WorkflowDefinition[AggregatedReport] but Meta.output points to
    a TaskNode[str].

    Raises WorkflowValidationError (E025) on mismatch.
    Skips validation when either side is unresolvable or Any.
    """
    if spec.output is None:
        return

    declared_ok = _resolve_workflow_def_ok_type(workflow_cls)
    if declared_ok is None or declared_ok is Any:
        # No generic parameter or explicitly Any — nothing to check
        return

    actual_ok = _resolve_source_node_ok_type(spec.output)
    if actual_ok is None or actual_ok is Any:
        # Output type unresolvable — can't validate
        return

    if not _is_ok_type_compatible(actual_ok, declared_ok):
        raise WorkflowValidationError(
            code=ErrorCode.WORKFLOW_OUTPUT_TYPE_MISMATCH,
            message=(
                f"workflow '{spec.name}' declares WorkflowDefinition"
                f'[{_format_type_name(declared_ok)}] but Meta.output produces '
                f'{_format_type_name(actual_ok)}'
            ),
            notes=[
                f'declared generic: {_format_type_name(declared_ok)}',
                f'actual output type: {_format_type_name(actual_ok)}',
            ],
            help_text='change the class generic to match the output type, or change Meta.output to a node that produces the declared type',
        )


def _format_type_name(annotation: Any | None) -> str:
    """Stable display for type annotations in validation notes."""
    if annotation is None:
        return 'unknown'
    if annotation is Any:
        return 'Any'
    # Plain classes: use qualname (e.g. 'AggregatedReport' not '<class ...>')
    if isinstance(annotation, type):
        return annotation.__qualname__
    # Generic aliases, unions, etc.: repr is fine
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
