"""A built workflow spec as the data that the core takes.

The DSL builds and checks a ``WorkflowSpec``; the node rows of the spec
(``child_build.NodeRow``) hold what each node row stores, with the queue and
priority of each task node bound and its deadline merged into its task
options. ``workflow_spec_data`` turns the spec and its rows into
``WorkflowSpecData`` (``horsies._catalog.workflow_spec``), which crosses to
the core as JSON text, written by ``dumps_json`` as every stored form is.
Pure: no I/O.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, assert_never

from horsies._catalog.workflow_spec import (
    MANIFEST_VERSION,
    SpecDefinitionKey,
    SpecDefinitionKeyKeyed,
    SpecDefinitionKeyUnkeyed,
    SpecJoin,
    SpecJoinAll,
    SpecJoinAny,
    SpecJoinQuorum,
    SpecNode,
    SpecNodeBody,
    SpecNodeBodySubWorkflow,
    SpecNodeBodyTask,
    SpecOnError,
    SpecOutput,
    SpecOutputNode,
    SpecOutputNoOutput,
    SpecSuccessCase,
    SpecSuccessPolicy,
    SpecSuccessPolicyCases,
    SpecSuccessPolicyNoPolicy,
    SpecSubWorkflowParams,
    SpecTaskOptions,
    SpecTaskOptionsEncoded,
    SpecTaskOptionsNoOptions,
    WorkflowSpecData,
    dump_workflow_spec_data,
)
from horsies.core.codec.json_io import SerdeResult
from horsies.core.models.workflow.enums import OnError
from horsies.core.workflows.child_build import NodeRow, SubWorkflowNodeKind, TaskNodeKind
from horsies.core.workflows.sql import SubWorkflowParams

if TYPE_CHECKING:
    from horsies.core.models.workflow import WorkflowSpec


def _definition_key(spec: WorkflowSpec[Any]) -> SpecDefinitionKey:
    match spec.definition_key:
        case None:
            return SpecDefinitionKeyUnkeyed()
        case key:
            return SpecDefinitionKeyKeyed(key=key)


def _on_error(spec: WorkflowSpec[Any]) -> SpecOnError:
    match spec.on_error:
        case OnError.FAIL:
            return 'FAIL'
        case OnError.PAUSE:
            return 'PAUSE'
        case _ as unreachable:
            assert_never(unreachable)


def _output(spec: WorkflowSpec[Any]) -> SpecOutput:
    if spec.output is None or spec.output.index is None:
        return SpecOutputNoOutput()
    return SpecOutputNode(index=spec.output.index)


def _success_policy(spec: WorkflowSpec[Any]) -> SpecSuccessPolicy:
    """The policy in the index form that the workflow row stores
    (``start_tree.success_policy_text``)."""
    policy = spec.success_policy
    if policy is None:
        return SpecSuccessPolicyNoPolicy()
    return SpecSuccessPolicyCases(
        cases=tuple(
            SpecSuccessCase(
                required=tuple(node.index for node in case.required if node.index is not None),
            )
            for case in policy.cases
        ),
        optional=tuple(
            node.index for node in (policy.optional or ()) if node.index is not None
        ),
    )


def _join(row: NodeRow) -> SpecJoin:
    match (row.join_type, row.min_success):
        case ('all', None):
            return SpecJoinAll()
        case ('any', None):
            return SpecJoinAny()
        case ('quorum', int() as min_success):
            return SpecJoinQuorum(min_success=min_success)
        case (join_type, min_success):
            raise AssertionError(
                f'node {row.task_index} ({row.task_name}) reached the spec data with '
                f'join {join_type!r} and min_success {min_success!r}; spec validation refuses '
                'every other pair',
            )


def _task_options(row: NodeRow) -> SpecTaskOptions:
    match row.task_options:
        case None:
            return SpecTaskOptionsNoOptions()
        case text:
            return SpecTaskOptionsEncoded(json=text)


def _params(params: SubWorkflowParams) -> SpecSubWorkflowParams:
    match params:
        case SubWorkflowParams.GIVEN:
            return 'GIVEN'
        case SubWorkflowParams.NOT_GIVEN:
            return 'NOT_GIVEN'
        case _ as unreachable:
            assert_never(unreachable)


def _body(row: NodeRow) -> SpecNodeBody:
    match row.kind:
        case TaskNodeKind():
            return SpecNodeBodyTask(
                queue=row.queue_name,
                priority=row.priority,
                task_options=_task_options(row),
            )
        case SubWorkflowNodeKind(sub_definition_key=definition_key, params=params):
            return SpecNodeBodySubWorkflow(
                definition_key=definition_key,
                params=_params(params),
            )
        case _ as unreachable:
            assert_never(unreachable)


def _node(row: NodeRow) -> SpecNode:
    assert row.node_id is not None, (
        f'node {row.task_index} ({row.task_name}) reached the spec data without a node id; '
        'a WorkflowSpec assigns one to every node'
    )
    return SpecNode(
        index=row.task_index,
        node_id=row.node_id,
        task_name=row.task_name,
        input_json=row.input_json,
        dependencies=row.dependencies,
        args_from=dict(row.args_from),
        workflow_ctx_from=row.workflow_ctx_from or (),
        allow_failed_deps=row.allow_failed_deps,
        join=_join(row),
        body=_body(row),
    )


def _queue_classes(rows: Sequence[NodeRow]) -> dict[str, str]:
    """The stored retention class key of each queue of the task rows. Each
    task row carries the class of its queue, so two rows on one queue carry
    the same class."""
    classes: dict[str, str] = {}
    for row in rows:
        match row.kind:
            case TaskNodeKind(facts=facts):
                classes[row.queue_name] = facts.retention_class_key
            case SubWorkflowNodeKind():
                continue
            case _ as unreachable:
                assert_never(unreachable)
    return classes


def workflow_spec_data(spec: WorkflowSpec[Any], rows: Sequence[NodeRow]) -> WorkflowSpecData:
    """The spec data of ``spec`` with its node rows ``rows`` in index order.

    The rows come from ``start_tree.top_node_rows`` for a start and from
    ``child_build.child_node_rows`` for a built child. Pure.
    """
    return WorkflowSpecData(
        data_version=MANIFEST_VERSION,
        name=spec.name,
        definition_key=_definition_key(spec),
        on_error=_on_error(spec),
        output=_output(spec),
        success_policy=_success_policy(spec),
        queue_classes=_queue_classes(rows),
        nodes=tuple(_node(row) for row in rows),
    )


def workflow_spec_json(
    spec: WorkflowSpec[Any], rows: Sequence[NodeRow],
) -> SerdeResult[str]:
    """The JSON text of ``workflow_spec_data(spec, rows)``. Pure.

    Err (``SerializationError``): a text of the spec (its name, a key, a
    task name, a queue name) holds a lone surrogate, which UTF-8 cannot
    encode. Nothing else of the spec data can fail the encode.
    """
    return dump_workflow_spec_data(workflow_spec_data(spec, rows))
