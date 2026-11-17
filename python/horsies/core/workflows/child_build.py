"""The build of the child workflow of a sub-workflow node, with no database writes.

A workflow start builds the child of every sub-workflow root before it sends
its statement; a step that promotes a sub-workflow node builds its child
before it calls ``horsies_workflow_child_start``. Both take the same steps,
in the order of ``enqueue_subworkflow_task`` (``core/workflows/engine.py``):

1. the depth limit (``MAX_SUBWORKFLOW_DEPTH``);
2. the load of the child definition by its ``definition_key``;
3. the node input: the stored kwargs, decoded against the child's
   ``build_with`` signature;
4. the ``args_from`` members: each key must not name a static kwarg, and the
   result of the named dependency must fit the type that ``build_with``
   declares;
5. ``build_with``, then the check of the declared output type;
6. the node rows of the child: no positional args, and each task node's
   queue bound against the app's queue config.

A child that cannot be built is a ``ChildBuildFailed`` with the error that
the node stores; the caller fails the node, and its workflow's ``on_error``
applies. Task options that do not parse and values that do not serialize are
defects of values that the process built: they raise ``SerializationError``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, assert_never, cast

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.json_io import SerializationError, dumps_json, loads_json
from horsies.core.codec.json_value import StrictJsonError, escape_lone_surrogates
from horsies.core.codec.kwargs import (
    decode_subworkflow_kwargs,
    fill_build_with_defaults,
    subworkflow_target_ok_type,
)
from horsies.core.codec.typed import conform_to_target
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.errors import ConfigurationError, ErrorCode, WorkflowValidationError
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    validate_workflow_generic_output_match,
)
from horsies.core.models.workflow.limits import MAX_SUBWORKFLOW_DEPTH
from horsies.core.models.workflow.nodes import stored_kwargs
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.retention import queue_retention_class
from horsies.core.workflows.sql import SubWorkflowParams, TaskNodeFacts

if TYPE_CHECKING:
    from horsies.core.app import Horsies
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.workflow import WorkflowSpec

logger = get_logger('workflow.engine')

BUILD_FAILURE_TEXT_CHARS: Final = 200
"""Characters of an exception text that a node error keeps."""

SUBWORKFLOW_NODE_QUEUE: Final = 'default'
"""The queue column of a sub-workflow node row. The row is not a claimable
task: no claim reads it."""

SUBWORKFLOW_NODE_PRIORITY: Final = 100
"""The priority column of a sub-workflow node row. No claim reads it."""


class NodeOwner(Enum):
    """The workflow whose spec holds a node: it names the node in the text of
    a defect."""

    START = 'START'
    """The spec of a workflow start, which ``app.workflow()`` built."""

    CHILD = 'CHILD'
    """The spec that a child's ``build_with`` returned."""

    def options_label(self) -> str:
        match self:
            case NodeOwner.START:
                return 'node'
            case NodeOwner.CHILD:
                return 'child node'

    def kwargs_label(self, kind: str) -> str:
        match self:
            case NodeOwner.START:
                return 'node'
            case NodeOwner.CHILD:
                return f'child {kind}'


@dataclass(frozen=True, slots=True)
class TaskNodeKind:
    """A task node: the typed facts that its row and its task row store."""

    facts: TaskNodeFacts


@dataclass(frozen=True, slots=True)
class SubWorkflowNodeKind:
    """A sub-workflow node: the child it starts and the params choice."""

    sub_workflow_name: str
    sub_definition_key: str
    params: SubWorkflowParams


type NodeKind = TaskNodeKind | SubWorkflowNodeKind


@dataclass(frozen=True, slots=True)
class NodeRow:
    """One node row of a starting workflow, as ``horsies_workflow_start`` takes it."""

    task_index: int
    node_id: str | None
    task_name: str
    input_json: str
    queue_name: str
    priority: int
    dependencies: tuple[int, ...]
    args_from: tuple[tuple[str, int], ...]
    """``args_from`` keys and the index of the node each names, in spec order."""
    workflow_ctx_from: tuple[str, ...] | None
    """Node ids of the context nodes; ``None`` when the node takes no context."""
    allow_failed_deps: bool
    join_type: str
    min_success: int | None
    task_options: str | None
    kind: NodeKind


@dataclass(frozen=True, slots=True)
class ChildOfNode:
    """The sub-workflow node whose child is built.

    ``node_input`` is the stored kwargs text of the node. ``args_from`` is
    the node's ``args_from`` map (key to dependency index), as the row stores
    it: JSON text, a decoded object, or ``None`` when the node has none.
    """

    workflow_id: str
    workflow_name: str
    task_index: int
    parent_depth: int
    sub_definition_key: str | None
    node_input: str
    args_from: str | Mapping[str, object] | None


@dataclass(frozen=True, slots=True)
class ChildBuilt:
    """The child spec, its depth, and the rows of its nodes."""

    spec: WorkflowSpec[Any]
    depth: int
    nodes: tuple[NodeRow, ...]


@dataclass(frozen=True, slots=True)
class ChildBuildFailed:
    """The child cannot start. ``error`` is what the node stores."""

    error: TaskError


type ChildBuild = ChildBuilt | ChildBuildFailed


def node_error(
    workflow_id: str,
    task_index: int,
    message: str,
    error_code: OperationalErrorCode,
) -> TaskError:
    """The error of a node whose task or child cannot be built. Pure.

    ``message`` can hold text from user code, so a lone surrogate in it is
    escaped: the node result must encode.
    """
    return TaskError(
        error_code=error_code,
        message=escape_lone_surrogates(message),
        data={'workflow_id': workflow_id, 'task_index': task_index},
    )


def node_failure_result(error: TaskError) -> str:
    """The stored result of a node that fails with ``error``. Pure."""
    return serialize_error_payload(TaskResult(err=error))


def _failed(
    node: ChildOfNode, message: str, error_code: OperationalErrorCode,
) -> ChildBuildFailed:
    return ChildBuildFailed(
        error=node_error(node.workflow_id, node.task_index, message, error_code),
    )


def _load_failed(node: ChildOfNode) -> ChildBuildFailed:
    return ChildBuildFailed(
        error=TaskError(
            error_code=OperationalErrorCode.SUBWORKFLOW_LOAD_FAILED,
            message=(
                f'Failed to load subworkflow definition for '
                f'{node.workflow_name}:{node.task_index}'
            ),
            data={'definition_key': node.sub_definition_key},
        ),
    )


def _load_definition(
    sub_definition_key: str | None,
) -> type[WorkflowDefinition[Any]] | None:
    from horsies.core.workflows.registry import get_workflow_definition

    match sub_definition_key:
        case None | '':
            return None
        case key:
            return get_workflow_definition(key)


def _args_from_map(
    node: ChildOfNode,
) -> dict[str, int] | ChildBuildFailed:
    """The validated ``args_from`` map of ``node``; empty when it has none."""
    raw: object = None
    match node.args_from:
        case None | '':
            return {}
        case str() as text_value:
            match loads_json(text_value):
                case Ok(parsed):
                    raw = parsed
                case Err(parse_error):
                    logger.warning(
                        f'JSON parse failed (subworkflow args_from): {parse_error}',
                    )
                case _ as unreachable:
                    assert_never(unreachable)
            if raw is None:
                return _failed(
                    node,
                    f'Corrupt args_from JSON for subworkflow task {node.task_index}',
                    OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
        case mapping:
            if not mapping:
                return {}
            raw = dict(mapping)
    if not isinstance(raw, dict):
        return {}
    pairs = cast(dict[object, object], raw)
    validated: dict[str, int] = {}
    for key, value in pairs.items():
        if not isinstance(key, str) or not isinstance(value, int):
            return _failed(
                node,
                f'args_from has non-int values for subworkflow task {node.task_index}',
                OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
            )
        validated[key] = value
    return validated


def _input_kwargs(
    node: ChildOfNode,
    workflow_def: type[WorkflowDefinition[Any]],
    dependency_results: Mapping[int, TaskResult[Any, TaskError]],
) -> dict[str, Any] | ChildBuildFailed:
    """The kwargs of ``build_with``: the decoded node input and the args_from members."""
    raw_kwargs: object = None
    match loads_json(node.node_input):
        case Ok(parsed):
            raw_kwargs = parsed
        case Err(parse_error):
            logger.warning(f'JSON parse failed (subworkflow kwargs): {parse_error}')
        case _ as unreachable:
            assert_never(unreachable)
    if raw_kwargs is None and node.node_input:
        return _failed(
            node,
            f'Corrupt kwargs JSON for subworkflow task {node.task_index}',
            OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
        )
    raw_kwargs_dict: dict[str, Any] = (
        cast(dict[str, Any], raw_kwargs) if isinstance(raw_kwargs, dict) else {}
    )
    try:
        kwargs = decode_subworkflow_kwargs(workflow_def, raw_kwargs_dict)
    except (StrictJsonError, ValueValidationError) as exc:
        return _failed(
            node,
            f'Typed decode of subworkflow kwargs failed for task {node.task_index}: {exc}',
            OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
        )
    match _args_from_map(node):
        case ChildBuildFailed() as failed:
            return failed
        case args_from:
            pass
    for kwarg_name, dependency_index in args_from.items():
        if kwarg_name in kwargs:
            return _failed(
                node,
                (
                    f"args_from key '{kwarg_name}' conflicts with static kwarg "
                    f'(subworkflow task {node.task_index})'
                ),
                OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
            )
        dependency_result = dependency_results.get(dependency_index)
        if dependency_result is None:
            continue
        target_ok_type = subworkflow_target_ok_type(workflow_def, kwarg_name)
        if target_ok_type is None:
            kwargs[kwarg_name] = dependency_result
            continue
        match conform_to_target(dependency_result, target_ok_type):
            case Ok(ok_value=conformed):
                kwargs[kwarg_name] = conformed
            case Err(err_value=refusal):
                return _failed(
                    node,
                    (
                        f"args_from value for '{kwarg_name}' does not fit the type "
                        f'that build_with declares (subworkflow task {node.task_index}): '
                        f'{escape_lone_surrogates(str(refusal))[:BUILD_FAILURE_TEXT_CHARS]}'
                    ),
                    OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
            case _ as unreachable:
                assert_never(unreachable)
    return kwargs


def _built_spec(
    node: ChildOfNode,
    workflow_def: type[WorkflowDefinition[Any]],
    kwargs: dict[str, Any],
    app: Horsies | None,
) -> WorkflowSpec[Any] | ChildBuildFailed:
    """``build_with`` of the child and the check of its output type.

    An exception from user code fails the node; ``asyncio.CancelledError``
    is not an ``Exception`` and propagates.
    """
    try:
        if app is None:
            raise WorkflowValidationError(
                message='Broker missing app reference for subworkflow',
                code=ErrorCode.WORKFLOW_SUBWORKFLOW_APP_MISSING,
                notes=[
                    'Subworkflows require a Horsies app instance to build the child spec',
                    'Ensure broker.app is set (Horsies.get_broker() does this automatically)',
                ],
                help_text='use app.get_broker() or attach app to broker before starting workflows',
            )
        default_build_with = getattr(
            WorkflowDefinition.build_with, '__func__', WorkflowDefinition.build_with,
        )
        build_with_fn = getattr(
            workflow_def, '_original_build_with', workflow_def.build_with,
        )
        if kwargs and build_with_fn is default_build_with:
            raise RuntimeError(
                f"subworkflow '{workflow_def.name}' received runtime parameters "
                'but uses default build_with(); override build_with(app, ...) to accept them'
            )
        filled = fill_build_with_defaults(workflow_def, kwargs)
        child_spec = workflow_def.build_with(app, **filled)
        validate_workflow_generic_output_match(workflow_def, child_spec)
    except Exception as exc:
        logger.error(
            f'Subworkflow enqueue build failed for {node.workflow_name}:{node.task_index}: '
            f'{type(exc).__name__}: {exc}'
        )
        return _failed(
            node,
            (
                f'Subworkflow build failed: {type(exc).__name__}: '
                f'{str(exc)[:BUILD_FAILURE_TEXT_CHARS]} '
                f'(workflow {node.workflow_name}:{node.task_index})'
            ),
            OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
        )
    return child_spec


def build_child(
    broker: PostgresBroker,
    node: ChildOfNode,
    dependency_results: Mapping[int, TaskResult[Any, TaskError]],
) -> ChildBuild:
    """Build the child of ``node``. Does no I/O; it runs the child's ``build_with``.

    ``dependency_results`` holds the result of each terminal dependency of
    the node, by index; an ``args_from`` key whose dependency has no entry
    gets no member.

    Raises:
        SerializationError: the task options of a child task do not parse, or
            a value of a child node row does not serialize. These come from
            the task function and from a spec that ``build_with`` proved, so
            this is a defect.
    """
    child_depth = node.parent_depth + 1
    if child_depth > MAX_SUBWORKFLOW_DEPTH:
        return _failed(
            node,
            (
                f'Subworkflow not started: the child workflow would have depth '
                f'{child_depth}, and the limit is {MAX_SUBWORKFLOW_DEPTH} '
                f'(workflow {node.workflow_name}:{node.task_index})'
            ),
            OperationalErrorCode.SUBWORKFLOW_DEPTH_LIMIT_REACHED,
        )
    return build_child_within_depth(broker, node, dependency_results, child_depth)


def build_child_within_depth(
    broker: PostgresBroker,
    node: ChildOfNode,
    dependency_results: Mapping[int, TaskResult[Any, TaskError]],
    child_depth: int,
) -> ChildBuild:
    """``build_child`` after its depth check, for a child at ``child_depth``,
    which is within ``MAX_SUBWORKFLOW_DEPTH``: the load of the definition,
    the input, ``build_with``, then the node rows. Does no I/O; it runs the
    child's ``build_with``.

    Raises:
        SerializationError: as ``build_child``.
    """
    assert child_depth <= MAX_SUBWORKFLOW_DEPTH, (
        f'the child of {node.workflow_name}:{node.task_index} is at depth {child_depth}, '
        f'past the limit {MAX_SUBWORKFLOW_DEPTH}; the depth check comes first'
    )
    workflow_def = _load_definition(node.sub_definition_key)
    if workflow_def is None:
        logger.error(
            f'SubWorkflowNode load failed for {node.workflow_name}:{node.task_index}',
        )
        return _load_failed(node)
    match _input_kwargs(node, workflow_def, dependency_results):
        case ChildBuildFailed() as failed:
            return failed
        case kwargs:
            pass
    match _built_spec(node, workflow_def, kwargs, broker.app):
        case ChildBuildFailed() as failed:
            return failed
        case child_spec:
            pass
    match child_node_rows(broker, node, child_spec):
        case ChildBuildFailed() as failed:
            return failed
        case rows:
            return ChildBuilt(spec=child_spec, depth=child_depth, nodes=rows)


def _serialized(value: object, context: str) -> str:
    match dumps_json(value):
        case Ok(text_value):
            return text_value
        case Err(error):
            raise SerializationError(f'Failed to serialize {context}: {error}')
        case _ as unreachable:
            assert_never(unreachable)


def _index_of(node: TaskNode[Any] | SubWorkflowNode[Any]) -> int:
    assert node.index is not None, (
        f'node {node.name!r} has no index; a WorkflowSpec assigns one to every node'
    )
    return node.index


def _graph_columns(
    node: TaskNode[Any] | SubWorkflowNode[Any],
) -> tuple[tuple[int, ...], tuple[tuple[str, int], ...], tuple[str, ...] | None]:
    """Dependencies, args_from pairs and context node ids of a spec node. Pure."""
    dependencies = tuple(_index_of(dependency) for dependency in node.waits_for)
    args_from = tuple((key, _index_of(source)) for key, source in node.args_from.items())
    context: tuple[str, ...] | None = None
    if node.workflow_ctx_from:
        context = tuple(
            context_node.node_id
            for context_node in node.workflow_ctx_from
            if context_node.node_id is not None
        )
    return dependencies, args_from, context


def subworkflow_node_row(node: SubWorkflowNode[Any], owner: NodeOwner) -> NodeRow:
    """The row of a sub-workflow node of a spec. Pure apart from serialization.

    Raises:
        SerializationError: the node kwargs do not serialize (a defect).
    """
    from horsies.core.workflows.lifecycle import subworkflow_params_of

    dependencies, args_from, context = _graph_columns(node)
    return NodeRow(
        task_index=_index_of(node),
        node_id=node.node_id,
        task_name=node.name,
        input_json=_serialized(
            stored_kwargs(node), f"kwargs for {owner.kwargs_label('sub')} {node.name}",
        ),
        queue_name=SUBWORKFLOW_NODE_QUEUE,
        priority=SUBWORKFLOW_NODE_PRIORITY,
        dependencies=dependencies,
        args_from=args_from,
        workflow_ctx_from=context,
        allow_failed_deps=node.allow_failed_deps,
        join_type=node.join,
        min_success=node.min_success,
        task_options=None,
        kind=SubWorkflowNodeKind(
            sub_workflow_name=node.workflow_def.name,
            sub_definition_key=node.workflow_def._require_definition_key(),  # pyright: ignore[reportPrivateUsage]
            params=subworkflow_params_of(node),
        ),
    )


def task_options_with_good_until(node: TaskNode[Any], owner: NodeOwner) -> str | None:
    """The task options JSON of a task node, with its ``good_until`` set.

    The options are the task decorator's own serialization of a validated
    ``TaskOptions``, so options that do not parse to a JSON object are a
    defect.

    Raises:
        SerializationError: the options do not parse to a JSON object, or
            the merged options do not serialize.
    """
    options_json = node.fn.task_options_json
    if node.good_until is None:
        return options_json
    base_options: dict[str, Any] = {}
    if options_json:
        match loads_json(options_json):
            case Ok(dict() as parsed_options):
                base_options = cast(dict[str, Any], parsed_options)
            case Ok(not_an_object):
                raise SerializationError(
                    f'Task options for {owner.options_label()} {node.name} are not '
                    f'a JSON object: got {type(not_an_object).__name__}',
                )
            case Err(parse_error):
                raise SerializationError(
                    f'Corrupt task_options for {owner.options_label()} {node.name}: '
                    f'{parse_error}',
                )
            case _ as unreachable:
                assert_never(unreachable)
    base_options['good_until'] = node.good_until.isoformat()
    return _serialized(base_options, f'task_options for {owner.options_label()} {node.name}')


def task_node_row(
    broker: PostgresBroker,
    node: TaskNode[Any],
    owner: NodeOwner,
    queue_name: str,
    priority: int,
) -> NodeRow:
    """The row of a task node of a spec, on ``queue_name`` at ``priority``.

    Raises:
        SerializationError: the task options do not parse or validate, or the
            node kwargs do not serialize (a defect).
    """
    from horsies.core.workflows.lifecycle import start_task_node_facts

    task_options = task_options_with_good_until(node, owner)
    facts = start_task_node_facts(
        task_options, node.name, queue_retention_class(broker, queue_name),
    )
    dependencies, args_from, context = _graph_columns(node)
    return NodeRow(
        task_index=_index_of(node),
        node_id=node.node_id,
        task_name=node.name,
        input_json=_serialized(
            stored_kwargs(node), f"kwargs for {owner.kwargs_label('task')} {node.name}",
        ),
        queue_name=queue_name,
        priority=priority,
        dependencies=dependencies,
        args_from=args_from,
        workflow_ctx_from=context,
        allow_failed_deps=node.allow_failed_deps,
        join_type=node.join,
        min_success=node.min_success,
        task_options=task_options,
        kind=TaskNodeKind(facts=facts),
    )


def child_node_rows(
    broker: PostgresBroker,
    node: ChildOfNode,
    child_spec: WorkflowSpec[Any],
) -> tuple[NodeRow, ...] | ChildBuildFailed:
    """The rows of the nodes of a built child, in spec order.

    A node with positional args, and a task node whose queue the app's
    queue config refuses, fail the parent node. The child spec was built
    by ``build_with``, which may not pass through ``app.workflow()``, so
    each task node's queue and priority are bound here.

    Raises:
        SerializationError: as ``task_node_row``.
    """
    from horsies.core.task_decorator import bind_task_node
    from horsies.core.workflows.lifecycle import guard_no_positional_args

    rows: list[NodeRow] = []
    for child_node in child_spec.tasks:
        try:
            guard_no_positional_args(child_node.name, child_node.args)
        except ValueError as exc:
            kind_text = 'subworkflow' if isinstance(child_node, SubWorkflowNode) else 'task'
            return _failed(
                node,
                (
                    f'Invalid child {kind_text} node args for {child_node.name}: '
                    f'{str(exc)[:BUILD_FAILURE_TEXT_CHARS]}'
                ),
                OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
            )
        match child_node:
            case SubWorkflowNode():
                rows.append(subworkflow_node_row(child_node, NodeOwner.CHILD))
            case TaskNode():
                _ = task_options_with_good_until(child_node, NodeOwner.CHILD)
                _ = _serialized(
                    stored_kwargs(child_node), f'kwargs for child task {child_node.name}',
                )
                assert broker.app is not None, (
                    'a built child spec implies an app: build_with refuses a broker without one'
                )
                try:
                    bound = bind_task_node(broker.app, child_node)
                except ConfigurationError as exc:
                    return _failed(
                        node,
                        f'Invalid queue/priority for child task {child_node.name}: {exc}',
                        OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
                    )
                rows.append(
                    task_node_row(
                        broker, child_node, NodeOwner.CHILD, bound.queue, bound.priority,
                    ),
                )
    return tuple(rows)
