"""Class-based workflow definition."""

from __future__ import annotations

import copy as copy_module
import uuid as uuid_module
from contextvars import ContextVar
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    ClassVar,
    Callable,
    cast,
)

from horsies.core.errors import ErrorCode, WorkflowValidationError

from .enums import OkT_co, OnError
from .nodes import TaskNode, SubWorkflowNode, AnyNode, SuccessPolicy, SuccessCase
from .spec import WorkflowSpec
from .typing_utils import validate_workflow_generic_output_match

if TYPE_CHECKING:
    from horsies.core.app import Horsies


_workflow_def_context: ContextVar[tuple[str, type[Any]] | None] = ContextVar(
    '_workflow_def_context', default=None,
)

_build_call_token: ContextVar[str | None] = ContextVar(
    '_build_call_token', default=None,
)


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
    ) -> WorkflowDefinitionMeta:
        cls = super().__new__(mcs, name, bases, namespace)

        # Skip processing for the base class itself
        if name == 'WorkflowDefinition':
            return cls

        # Collect TaskNode and SubWorkflowNode instances in definition order.
        # Stamp node_id = attr_name on unstamped nodes so that condition
        # closures captured at module level always see a valid node_id —
        # even in fresh worker processes where build() is never called.
        nodes: list[tuple[str, TaskNode[Any] | SubWorkflowNode[Any]]] = []
        for attr_name, attr_value in namespace.items():
            if isinstance(attr_value, (TaskNode, SubWorkflowNode)):
                if attr_value.node_id is None:
                    attr_value.node_id = attr_name
                nodes.append((attr_name, cast(AnyNode, attr_value)))

        # Store the collected nodes on the class
        cls._workflow_nodes = nodes  # type: ignore[attr-defined]

        # Resolve the true (unwrapped) build_with implementation for this class.
        # Priority:
        #   1) class-local override in namespace
        #   2) inherited _original_build_with from parent workflow class
        #   3) fallback to cls.build_with unwrapped
        if 'build_with' in namespace:
            raw_build_with: Any = namespace['build_with']
            unwrapped = getattr(raw_build_with, '__func__', raw_build_with)
        else:
            inherited_original: Any | None = None
            for base in bases:
                inherited_original = getattr(base, '_original_build_with', None)
                if inherited_original is not None:
                    break
            if inherited_original is not None:
                unwrapped = inherited_original
            else:
                raw_build_with = cast(Any, cls.build_with)  # type: ignore[attr-defined]
                unwrapped = getattr(raw_build_with, '__func__', raw_build_with)

        # Store true original for signature/default-build_with checks (HRS-019-HRS-023).
        cls._original_build_with = unwrapped  # type: ignore[attr-defined]

        original_fn = cast(
            Callable[..., 'WorkflowSpec[Any]'],
            unwrapped,
        )

        @classmethod  # type: ignore[misc]
        def _wrapped_build_with(
            klass: type,
            app: Any,
            *bw_args: Any,
            **bw_params: Any,
        ) -> WorkflowSpec[Any]:
            call_token = str(uuid_module.uuid4())
            ctx_token = _workflow_def_context.set(
                (klass._require_definition_key(), klass),
            )
            call_token_reset = _build_call_token.set(call_token)
            try:
                spec = original_fn(klass, app, *bw_args, **bw_params)
            finally:
                _workflow_def_context.reset(ctx_token)
                _build_call_token.reset(call_token_reset)
            # Reject specs not constructed during THIS call. A fresh spec
            # picks up the call_token via ContextVar in __post_init__.
            # Cached/prebuilt specs (including same-class reuse) will have
            # a stale or missing token.
            spec_token: str | None = getattr(spec, '_build_call_token', None)
            if spec_token != call_token:
                raise WorkflowValidationError(
                    f"build_with() for '{klass.__qualname__}' returned a spec "
                    f"not constructed during this call. "
                    f"build_with() must return a fresh WorkflowSpec per call — "
                    f"cached or prebuilt spec reuse is not supported.",
                )
            validate_workflow_generic_output_match(klass, spec)
            return spec

        cls.build_with = _wrapped_build_with  # type: ignore[attr-defined]

        definition_key = getattr(cls, 'definition_key', None)
        if isinstance(definition_key, str) and definition_key.strip():
            from horsies.core.workflows.registry import register_workflow_definition

            register_workflow_definition(cast('type[WorkflowDefinition[Any]]', cls))

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

            fetch = TaskNode(fn=fetch_listing, kwargs={"url": "url"})
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
    definition_key: ClassVar[str]

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
    def build(cls, app: Horsies) -> WorkflowSpec[OkT_co]:
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
                f"WorkflowDefinition '{cls.__name__}' must define a 'name' class attribute",
                code=ErrorCode.WORKFLOW_NO_NAME,
            )

        # Get collected nodes from metaclass
        nodes = cls.get_workflow_nodes()
        if not nodes:
            raise WorkflowValidationError(
                f"WorkflowDefinition '{cls.__name__}' has no TaskNode attributes",
                code=ErrorCode.WORKFLOW_NO_NODES,
            )
        definition_key = cls._require_definition_key()

        # Copy-on-build: shallow-copy class-level nodes so app.workflow()
        # enrichment (queue/priority) never mutates class-level originals.
        old_to_new: dict[int, TaskNode[Any] | SubWorkflowNode[Any]] = {}
        tasks: list[TaskNode[Any] | SubWorkflowNode[Any]] = []
        for attr_name, node in nodes:
            node_copy = copy_module.copy(node)
            if node_copy.node_id is None:
                node_copy.node_id = attr_name
            old_to_new[id(node)] = node_copy
            tasks.append(node_copy)

        # Remap cross-references to point at copies, not class-level originals.
        for node_copy in tasks:
            node_copy.waits_for = [
                old_to_new.get(id(ref), ref) for ref in node_copy.waits_for
            ]
            node_copy.args_from = {
                k: old_to_new.get(id(v), v)
                for k, v in node_copy.args_from.items()
            }
            if node_copy.workflow_ctx_from is not None:
                node_copy.workflow_ctx_from = [
                    old_to_new.get(id(ref), ref)
                    for ref in node_copy.workflow_ctx_from
                ]

        # Get Meta configuration, remapping output/success_policy refs to copies.
        output: TaskNode[OkT_co] | SubWorkflowNode[OkT_co] | None = None
        on_error: OnError = OnError.FAIL
        success_policy: SuccessPolicy | None = None

        meta: type[Any] | None = getattr(cls, 'Meta', None)
        if meta is not None:
            raw_output: TaskNode[Any] | SubWorkflowNode[Any] | None = getattr(
                meta, 'output', None,
            )
            if raw_output is not None:
                output = cast(
                    'TaskNode[OkT_co] | SubWorkflowNode[OkT_co]',
                    old_to_new.get(id(raw_output), raw_output),
                )
            on_error = getattr(meta, 'on_error', OnError.FAIL)
            raw_policy: SuccessPolicy | None = getattr(
                meta, 'success_policy', None,
            )
            if raw_policy is not None:
                new_cases: list[SuccessCase] = []
                for case in raw_policy.cases:
                    new_cases.append(SuccessCase(
                        required=[
                            cast('TaskNode[Any]', old_to_new.get(id(t), t))
                            for t in case.required
                        ],
                    ))
                new_optional: list[TaskNode[Any]] | None = None
                if raw_policy.optional is not None:
                    new_optional = [
                        cast('TaskNode[Any]', old_to_new.get(id(t), t))
                        for t in raw_policy.optional
                    ]
                success_policy = SuccessPolicy(
                    cases=new_cases,
                    optional=new_optional,
                )

        # Build WorkflowSpec — split paths for correct type inference.
        # When output is set, overload resolves WorkflowSpec[OkT_co] directly.
        # When output is None, we cast since OkT_co is meaningless for
        # outputless workflows (they use results()/results_async() instead).
        if output is not None:
            spec = app.workflow(
                name=cls.name,
                tasks=tasks,
                output=output,
                on_error=on_error,
                success_policy=success_policy,
                definition_key=definition_key,
                workflow_def_cls=cls,
            )
        else:
            spec = cast(
                'WorkflowSpec[OkT_co]',
                app.workflow(
                    name=cls.name,
                    tasks=tasks,
                    on_error=on_error,
                    success_policy=success_policy,
                    definition_key=definition_key,
                    workflow_def_cls=cls,
                ),
            )
        validate_workflow_generic_output_match(cls, spec)
        return spec

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        **params: Any,
    ) -> WorkflowSpec[OkT_co]:
        """
        Build a WorkflowSpec with runtime parameters.

        Subclasses can override this to apply keyword params to TaskNodes.
        Default implementation forwards to build().
        """
        _ = params
        return cls.build(app)

    @classmethod
    def _require_definition_key(cls) -> str:
        definition_key = cls.__dict__.get('definition_key')
        if not isinstance(definition_key, str) or not definition_key.strip():
            raise WorkflowValidationError(
                (
                    f"WorkflowDefinition '{cls.__name__}' must define a non-empty "
                    "'definition_key' class attribute"
                ),
                code=ErrorCode.WORKFLOW_NO_DEFINITION_KEY,
            )
        return definition_key
