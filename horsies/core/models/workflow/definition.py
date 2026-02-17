"""Class-based workflow definition."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    ClassVar,
    Callable,
    cast,
)

from horsies.core.errors import WorkflowValidationError

from .enums import OkT_co, OnError
from .nodes import TaskNode, SubWorkflowNode, AnyNode, SuccessPolicy
from .spec import WorkflowSpec
from .typing_utils import validate_workflow_generic_output_match

if TYPE_CHECKING:
    from horsies.core.app import Horsies


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

        # Collect TaskNode and SubWorkflowNode instances in definition order
        nodes: list[tuple[str, TaskNode[Any] | SubWorkflowNode[Any]]] = []
        for attr_name, attr_value in namespace.items():
            if isinstance(attr_value, (TaskNode, SubWorkflowNode)):
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

        # Store true original for signature/default-build_with checks (E019-E023).
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
            spec = original_fn(klass, app, *bw_args, **bw_params)
            spec.workflow_def_module = klass.__module__
            spec.workflow_def_qualname = klass.__qualname__
            spec.workflow_def_cls = klass
            validate_workflow_generic_output_match(klass, spec)
            return spec

        cls.build_with = _wrapped_build_with  # type: ignore[attr-defined]

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
            )
        else:
            spec = cast(
                'WorkflowSpec[OkT_co]',
                app.workflow(
                    name=cls.name,
                    tasks=tasks,
                    on_error=on_error,
                    success_policy=success_policy,
                ),
            )
        spec.workflow_def_module = cls.__module__
        spec.workflow_def_qualname = cls.__qualname__
        spec.workflow_def_cls = cls
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
