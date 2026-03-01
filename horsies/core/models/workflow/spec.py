"""WorkflowSpec: validated DAG specification."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import inspect
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    cast,
)

from horsies.core.errors import (
    ErrorCode,
    SourceLocation,
    ValidationReport,
    WorkflowValidationError,
    raise_collected,
)

from .enums import OutT, OnError, SubWorkflowRetryMode
from .nodes import (
    TaskNode,
    SubWorkflowNode,
    SuccessPolicy,
)
from .typing_utils import (
    NODE_ID_PATTERN,
    slugify,
    _task_accepts_workflow_ctx,
    _get_signature,
    _signature_accepts_kwargs,
    _valid_kwarg_names,
    _resolved_type_hints,
    _extract_taskresult_ok_type,
    _resolve_source_node_ok_type,
    _is_ok_type_compatible,
    _format_type_name,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.workflow.handle import WorkflowHandle
    from horsies.core.workflows.start_types import WorkflowStartResult


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
        result = await spec.start_async()  # WorkflowStartResult
        handle = result.ok_value              # WorkflowHandle
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

    workflow_def_cls: type[Any] | None = field(default=None, repr=False)
    """
    - Reference to the WorkflowDefinition subclass that built this spec.
    - Used by start APIs for output type mismatch validation (E025).
    - None when spec is built directly via app.workflow() without a definition class.
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
        for error in self._collect_positional_args_not_supported_errors():
            report.add(error)
        for error in self._collect_from_node_marker_kwargs_errors():
            report.add(error)
        for error in self._collect_kwargs_args_from_overlap_errors():
            report.add(error)
        for error in self._collect_subworkflow_default_build_with_param_errors():
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
            cycle_nodes = [
                t.name
                for t in self.tasks
                if t.index is not None and in_degree.get(t.index, 0) > 0
            ]
            errors.append(
                WorkflowValidationError(
                    message='cycle detected in workflow DAG',
                    code=ErrorCode.WORKFLOW_CYCLE_DETECTED,
                    notes=[
                        f'nodes involved: {cycle_nodes}',
                        'workflows must be acyclic directed graphs (DAG)',
                    ],
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

    def _collect_positional_args_not_supported_errors(
        self,
    ) -> list[WorkflowValidationError]:
        """Reject positional args on workflow nodes."""
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            if not node.args:
                continue
            errors.append(
                WorkflowValidationError(
                    message='positional args are not supported for workflow nodes',
                    code=ErrorCode.WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED,
                    notes=[
                        f"node '{node.name}' sets args=(...)",
                        'workflow node inputs must be provided via kwargs and/or args_from',
                    ],
                    help_text='remove args=... and pass static values via kwargs=...',
                )
            )
        return errors

    def _collect_from_node_marker_kwargs_errors(
        self,
    ) -> list[WorkflowValidationError]:
        """Reject raw from_node() markers in kwargs.

        from_node() markers are intended for `.node()()` calls where they are
        translated into args_from mappings. If users place markers directly in
        TaskNode/SubWorkflowNode kwargs, that conversion never runs.
        """
        from horsies.core.task_decorator import FromNodeMarker

        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            marker_keys = [
                key
                for key, value in node.kwargs.items()
                if isinstance(value, FromNodeMarker)
            ]
            if not marker_keys:
                continue

            errors.append(
                WorkflowValidationError(
                    message='from_node() markers are not allowed in kwargs',
                    code=ErrorCode.WORKFLOW_INVALID_ARGS_FROM,
                    notes=[
                        f"node '{node.name}' has from_node() marker value(s) in kwargs: {sorted(marker_keys)}",
                        'from_node() markers must be translated to args_from mappings',
                    ],
                    help_text=(
                        'use .node()(..., key=from_node(upstream)) so conversion runs, '
                        'or use explicit args_from={...}'
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

        Default WorkflowDefinition.build_with(app, **params) ignores all
        runtime parameters and just forwards to build(app), which is a silent
        data-loss foot-gun when users pass kwargs/args_from.
        """
        # Deferred import to avoid circular dependency with definition.py
        from .definition import WorkflowDefinition

        errors: list[WorkflowValidationError] = []

        default_build_with = getattr(
            WorkflowDefinition.build_with,
            '__func__',
            WorkflowDefinition.build_with,
        )
        for node in self.tasks:
            if not isinstance(node, SubWorkflowNode):
                continue
            has_runtime_params = bool(node.kwargs) or bool(node.args_from)
            if not has_runtime_params:
                continue

            build_with_fn = getattr(
                node.workflow_def,
                '_original_build_with',
                node.workflow_def.build_with,
            )
            if build_with_fn is not default_build_with:
                continue

            notes: list[str] = [
                f"subworkflow '{node.name}' provides runtime params but uses default build_with()",
            ]
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
                fn = getattr(
                    node.workflow_def,
                    '_original_build_with',
                    node.workflow_def.build_with,
                )
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
                fn = getattr(
                    node.workflow_def,
                    '_original_build_with',
                    node.workflow_def.build_with,
                )
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
        """Validate required parameters are provided via kwargs/args_from."""
        errors: list[WorkflowValidationError] = []
        for node in self.tasks:
            # Get the function to inspect (TaskNode vs SubWorkflowNode)
            if isinstance(node, TaskNode):
                fn = node.fn
                args_provided: list[object] = []
                injected_kwargs: set[str] = set()
            else:
                fn = getattr(
                    node.workflow_def,
                    '_original_build_with',
                    node.workflow_def.build_with,
                )
                # _original_build_with is a raw function: (cls, app, **kwargs).
                # Two sentinels account for cls and app being provided automatically.
                args_provided = [object(), object()]
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
                    help_text='provide required params via kwargs=... or args_from',
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
                    code=ErrorCode.WORKFLOW_INVALID_OUTPUT,
                    notes=[
                        f"output task: '{self.output.name}'",
                        f"workflow tasks: {[t.name for t in self.tasks]}",
                    ],
                    help_text='add the output task to the tasks list, or change output to a task already in the workflow',
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
                    code=ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY,
                    help_text='add at least one SuccessCase to the cases list',
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
                        code=ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY,
                        notes=[f'case index: {i}'],
                        help_text='add at least one task to the required list',
                    )
                )
            for task in case.required:
                if id(task) not in task_ids:
                    errors.append(
                        WorkflowValidationError(
                            f"SuccessCase[{i}] required task '{task.name}' is not in workflow",
                            code=ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY,
                            notes=[
                                f'case index: {i}',
                                f"task: '{task.name}'",
                            ],
                            help_text='add the task to the workflow tasks list',
                        )
                    )

        # Validate optional tasks
        if self.success_policy.optional:
            for task in self.success_policy.optional:
                if id(task) not in task_ids:
                    errors.append(
                        WorkflowValidationError(
                            f"SuccessPolicy optional task '{task.name}' is not in workflow",
                            code=ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY,
                            notes=[f"task: '{task.name}'"],
                            help_text='add the task to the workflow tasks list',
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

    def start(self, workflow_id: str | None = None) -> WorkflowStartResult[WorkflowHandle[OutT]]:
        """Start workflow execution.

        Args:
            workflow_id: Optional custom workflow ID. Auto-generated if not provided.

        Returns:
            ``Ok(WorkflowHandle)`` on success,
            ``Err(WorkflowStartError)`` on failure.
        """
        if self.broker is None:
            import uuid as _uuid

            from horsies.core.workflows.start_types import (
                WorkflowStartError as _WSE,
                WorkflowStartErrorCode as _WSEC,
                WorkflowStartStage as _WSS,
            )
            from horsies.core.types.result import Err as _Err

            return _Err(_WSE(
                code=_WSEC.BROKER_NOT_CONFIGURED,
                message='WorkflowSpec requires a broker. Use app.workflow() or set broker.',
                retryable=False,
                stage=_WSS.PREVALIDATE,
                workflow_name=self.name,
                workflow_id=workflow_id or str(_uuid.uuid4()),
            ))

        # Import here to avoid circular imports
        from horsies.core.workflows.engine import start_workflow
        workflow_sent_at = datetime.now(timezone.utc)
        return start_workflow(self, self.broker, workflow_id, workflow_sent_at)

    async def start_async(
        self, workflow_id: str | None = None,
    ) -> WorkflowStartResult[WorkflowHandle[OutT]]:
        """Start workflow execution (async).

        Args:
            workflow_id: Optional custom workflow ID. Auto-generated if not provided.

        Returns:
            ``Ok(WorkflowHandle)`` on success,
            ``Err(WorkflowStartError)`` on failure.
        """
        if self.broker is None:
            import uuid as _uuid

            from horsies.core.workflows.start_types import (
                WorkflowStartError as _WSE,
                WorkflowStartErrorCode as _WSEC,
                WorkflowStartStage as _WSS,
            )
            from horsies.core.types.result import Err as _Err

            return _Err(_WSE(
                code=_WSEC.BROKER_NOT_CONFIGURED,
                message='WorkflowSpec requires a broker. Use app.workflow() or set broker.',
                retryable=False,
                stage=_WSS.PREVALIDATE,
                workflow_name=self.name,
                workflow_id=workflow_id or str(_uuid.uuid4()),
            ))

        # Import here to avoid circular imports
        from horsies.core.workflows.engine import start_workflow_async
        workflow_sent_at = datetime.now(timezone.utc)
        return await start_workflow_async(self, self.broker, workflow_id, workflow_sent_at)
