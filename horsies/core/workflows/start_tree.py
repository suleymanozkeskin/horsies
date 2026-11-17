"""The workflow tree that ``horsies_workflow_start`` and ``horsies_workflow_child_start`` take.

A start writes a whole tree in one call: the top workflow, a pre-built
child for every sub-workflow root (and, in turn, for the roots of each
child, to the depth limit), every node row with its typed facts, and one
failed-node record for each sub-workflow root whose child cannot be built.
The tree is built in memory before the statement; the build runs the
``build_with`` of each pre-built child, which must have no side effects.
The child tree of an owed child start has the same form, with the child
row on top.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final, assert_never

from horsies.core.codec.json_io import SerializationError, dumps_json
from horsies.core.models.workflow import SubWorkflowNode, TaskNode
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.child_build import (
    ChildBuildFailed,
    ChildBuilt,
    ChildOfNode,
    NodeOwner,
    NodeRow,
    SubWorkflowNodeKind,
    TaskNodeKind,
    build_child,
    node_failure_result,
    subworkflow_node_row,
    task_node_row,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.workflow import WorkflowSpec


@dataclass(frozen=True, slots=True)
class TopLevel:
    """The workflow has no parent."""


@dataclass(frozen=True, slots=True)
class ChildOfWorkflow:
    """The workflow is the child of node ``parent_task_index`` of ``parent_workflow_id``."""

    parent_workflow_id: str
    parent_task_index: int


type WorkflowParent = TopLevel | ChildOfWorkflow


@dataclass(frozen=True, slots=True)
class WorkflowRow:
    """One workflow row of a tree."""

    workflow_id: str
    name: str
    definition_key: str | None
    on_error: str
    output_task_index: int | None
    success_policy: str | None
    """The index form of the success policy as JSON text; ``None`` without one."""
    parent: WorkflowParent
    depth: int


@dataclass(frozen=True, slots=True)
class TreeNode:
    """One node row of a tree and the workflow it belongs to."""

    workflow_id: str
    row: NodeRow


@dataclass(frozen=True, slots=True)
class FailedRecord:
    """A sub-workflow root of the tree whose child cannot be built.

    ``result`` is the error envelope that the node stores.
    """

    workflow_id: str
    task_index: int
    result: str


@dataclass(frozen=True, slots=True)
class StartTree:
    """The rows of one tree: the top workflow first, parents before children."""

    workflows: tuple[WorkflowRow, ...]
    nodes: tuple[TreeNode, ...]
    failed: tuple[FailedRecord, ...]

    @property
    def top_workflow_id(self) -> str:
        """The id of the top workflow."""
        return self.workflows[0].workflow_id

    def workflow_names(self) -> dict[str, str]:
        """The name of each workflow of the tree, by id."""
        return {workflow.workflow_id: workflow.name for workflow in self.workflows}

    @property
    def has_failed_records(self) -> bool:
        """Whether a sub-workflow root of the tree fails. Only then can the
        statement owe child starts."""
        return bool(self.failed)


@dataclass(frozen=True, slots=True)
class _PendingWorkflow:
    """A workflow of the tree whose rows are not pushed yet."""

    spec: WorkflowSpec[Any]
    workflow_id: str
    parent: WorkflowParent
    depth: int
    nodes: tuple[NodeRow, ...]


def _serialized(value: object, context: str) -> str:
    match dumps_json(value):
        case Ok(text_value):
            return text_value
        case Err(error):
            raise SerializationError(f'Failed to serialize {context}: {error}')
        case _ as unreachable:
            assert_never(unreachable)


def success_policy_text(spec: WorkflowSpec[Any]) -> str | None:
    """The success policy of ``spec`` in its stored index form; ``None`` without one.

    Raises:
        SerializationError: the policy does not serialize (a defect).
    """
    if spec.success_policy is None:
        return None
    policy: dict[str, Any] = {
        'cases': [
            {'required_indices': [node.index for node in case.required if node.index is not None]}
            for case in spec.success_policy.cases
        ],
    }
    if spec.success_policy.optional:
        policy['optional_indices'] = [
            node.index for node in spec.success_policy.optional if node.index is not None
        ]
    return _serialized(policy, f'success_policy for workflow {spec.name}')


def top_node_rows(
    broker: PostgresBroker, spec: WorkflowSpec[Any],
) -> tuple[NodeRow, ...]:
    """The node rows of a spec that ``app.workflow()`` built and the start checked.

    Each task node has its queue (``resolve_node_queue``) and priority set.

    Raises:
        SerializationError: as ``task_node_row`` (a defect).
    """
    from horsies.core.task_decorator import resolve_node_queue

    rows: list[NodeRow] = []
    for node in spec.tasks:
        assert not node.args, (
            f"{type(node).__name__} '{node.name}' reached the start with "
            f'{len(node.args)} positional args; spec construction refuses them'
        )
        match node:
            case SubWorkflowNode():
                rows.append(subworkflow_node_row(node, NodeOwner.START))
            case TaskNode():
                assert node.priority is not None, (
                    f"TaskNode '{node.name}' reached the start with priority=None; "
                    'start prevalidation refuses it'
                )
                rows.append(
                    task_node_row(
                        broker, node, NodeOwner.START, resolve_node_queue(node), node.priority,
                    ),
                )
    return tuple(rows)


def _workflow_row(pending: _PendingWorkflow) -> WorkflowRow:
    spec = pending.spec
    return WorkflowRow(
        workflow_id=pending.workflow_id,
        name=spec.name,
        definition_key=spec.definition_key,
        on_error=spec.on_error.value,
        output_task_index=spec.output.index if spec.output is not None else None,
        success_policy=success_policy_text(spec),
        parent=pending.parent,
        depth=pending.depth,
    )


def _args_from_text(row: NodeRow) -> str | None:
    match row.args_from:
        case ():
            return None
        case pairs:
            return _serialized(dict(pairs), f'args_from for node {row.task_name}')


def _tree(broker: PostgresBroker, top: _PendingWorkflow) -> StartTree:
    """The rows of the tree under ``top``, parents before children.

    Each workflow is pushed once, and a child is built only below the depth
    limit (``build_child``), so the work list ends.
    """
    workflows: list[WorkflowRow] = []
    nodes: list[TreeNode] = []
    failed: list[FailedRecord] = []
    pending: list[_PendingWorkflow] = [top]
    while pending:
        current = pending.pop()
        workflows.append(_workflow_row(current))
        nodes.extend(TreeNode(workflow_id=current.workflow_id, row=row) for row in current.nodes)
        children: list[_PendingWorkflow] = []
        for row in current.nodes:
            match row.kind:
                case TaskNodeKind():
                    continue
                case SubWorkflowNodeKind(sub_definition_key=definition_key):
                    pass
            if row.dependencies:
                continue
            built = build_child(
                broker,
                ChildOfNode(
                    workflow_id=current.workflow_id,
                    workflow_name=current.spec.name,
                    task_index=row.task_index,
                    parent_depth=current.depth,
                    sub_definition_key=definition_key,
                    node_input=row.input_json,
                    args_from=_args_from_text(row),
                ),
                {},
            )
            match built:
                case ChildBuilt(spec=child_spec, depth=depth, nodes=child_nodes):
                    children.append(
                        _PendingWorkflow(
                            spec=child_spec,
                            workflow_id=str(uuid.uuid4()),
                            parent=ChildOfWorkflow(
                                parent_workflow_id=current.workflow_id,
                                parent_task_index=row.task_index,
                            ),
                            depth=depth,
                            nodes=child_nodes,
                        ),
                    )
                case ChildBuildFailed(error=error):
                    failed.append(
                        FailedRecord(
                            workflow_id=current.workflow_id,
                            task_index=row.task_index,
                            result=node_failure_result(error),
                        ),
                    )
        pending.extend(reversed(children))
    return StartTree(workflows=tuple(workflows), nodes=tuple(nodes), failed=tuple(failed))


def start_tree(
    broker: PostgresBroker, spec: WorkflowSpec[Any], workflow_id: str,
) -> StartTree:
    """The tree of a top-level start of ``spec`` as ``workflow_id``.

    Does no I/O; it runs the ``build_with`` of each pre-built child.

    Raises:
        SerializationError: a value of the tree does not serialize, or task
            options do not parse (a defect of values the process built).
    """
    return _tree(
        broker,
        _PendingWorkflow(
            spec=spec,
            workflow_id=workflow_id,
            parent=TopLevel(),
            depth=0,
            nodes=top_node_rows(broker, spec),
        ),
    )


def child_tree(
    broker: PostgresBroker,
    built: ChildBuilt,
    parent_workflow_id: str,
    parent_task_index: int,
) -> StartTree:
    """The tree of an owed child start: the built child under its node, with a new id.

    Raises:
        SerializationError: as ``start_tree``.
    """
    return _tree(
        broker,
        _PendingWorkflow(
            spec=built.spec,
            workflow_id=str(uuid.uuid4()),
            parent=ChildOfWorkflow(
                parent_workflow_id=parent_workflow_id,
                parent_task_index=parent_task_index,
            ),
            depth=built.depth,
            nodes=built.nodes,
        ),
    )


_NO_TREE_PARAMETERS: Final = (
    'workflow_ids', 'names', 'definition_keys', 'on_errors', 'output_task_indexes',
    'success_policies', 'parent_workflow_ids', 'parent_task_indexes', 'depths',
    'node_workflow_ids', 'node_task_indexes', 'node_ids', 'node_task_names', 'node_inputs',
    'node_queue_names', 'node_priorities', 'node_dependencies', 'node_args_from',
    'node_workflow_ctx_from', 'node_allow_failed_deps', 'node_join_types',
    'node_min_successes', 'node_task_options', 'node_max_retries', 'node_executions',
    'node_good_untils', 'node_retention_class_keys', 'node_is_subworkflow',
    'node_sub_workflow_names', 'node_sub_definition_keys', 'node_sub_workflow_params',
    'failed_workflow_ids', 'failed_task_indexes', 'failed_results',
)
"""The array parameters of a tree, in the order of the function arguments."""


def no_tree_parameters() -> dict[str, object]:
    """Every array parameter of a tree as NULL: the FAILURE form of a child start."""
    return {name: None for name in _NO_TREE_PARAMETERS}


def _parent_columns(parent: WorkflowParent) -> tuple[str | None, int | None]:
    match parent:
        case TopLevel():
            return None, None
        case ChildOfWorkflow(parent_workflow_id=parent_id, parent_task_index=index):
            return parent_id, index


def _dependencies_text(row: NodeRow) -> str:
    return _serialized(list(row.dependencies), f'dependencies of node {row.task_name}')


def _context_text(row: NodeRow) -> str | None:
    match row.workflow_ctx_from:
        case None:
            return None
        case node_ids:
            return _serialized(list(node_ids), f'workflow_ctx_from of node {row.task_name}')


def _task_fact(row: NodeRow) -> tuple[int | None, str | None, str | None, str | None]:
    """max_retries, execution, good_until and retention class key of a task node."""
    match row.kind:
        case TaskNodeKind(facts=facts):
            return (
                facts.max_retries,
                facts.execution.value,
                facts.good_until,
                facts.retention_class_key,
            )
        case SubWorkflowNodeKind():
            return None, None, None, None


def _sub_columns(row: NodeRow) -> tuple[str | None, str | None, str | None]:
    """Child name, definition key and params choice of a sub-workflow node."""
    match row.kind:
        case TaskNodeKind():
            return None, None, None
        case SubWorkflowNodeKind(
            sub_workflow_name=name, sub_definition_key=key, params=params,
        ):
            return name, key, params.value


def _is_subworkflow(row: NodeRow) -> bool:
    match row.kind:
        case TaskNodeKind():
            return False
        case SubWorkflowNodeKind():
            return True


def tree_parameters(tree: StartTree) -> dict[str, object]:
    """The array parameters of ``tree``. Pure apart from serialization.

    Raises:
        SerializationError: a node's dependencies or context do not serialize
            (a defect).
    """
    parents = [_parent_columns(workflow.parent) for workflow in tree.workflows]
    rows = [node.row for node in tree.nodes]
    facts = [_task_fact(row) for row in rows]
    subs = [_sub_columns(row) for row in rows]
    return {
        'workflow_ids': [workflow.workflow_id for workflow in tree.workflows],
        'names': [workflow.name for workflow in tree.workflows],
        'definition_keys': [workflow.definition_key for workflow in tree.workflows],
        'on_errors': [workflow.on_error for workflow in tree.workflows],
        'output_task_indexes': [workflow.output_task_index for workflow in tree.workflows],
        'success_policies': [workflow.success_policy for workflow in tree.workflows],
        'parent_workflow_ids': [parent_id for parent_id, _ in parents],
        'parent_task_indexes': [index for _, index in parents],
        'depths': [workflow.depth for workflow in tree.workflows],
        'node_workflow_ids': [node.workflow_id for node in tree.nodes],
        'node_task_indexes': [row.task_index for row in rows],
        'node_ids': [row.node_id for row in rows],
        'node_task_names': [row.task_name for row in rows],
        'node_inputs': [row.input_json for row in rows],
        'node_queue_names': [row.queue_name for row in rows],
        'node_priorities': [row.priority for row in rows],
        'node_dependencies': [_dependencies_text(row) for row in rows],
        'node_args_from': [_args_from_text(row) for row in rows],
        'node_workflow_ctx_from': [_context_text(row) for row in rows],
        'node_allow_failed_deps': [row.allow_failed_deps for row in rows],
        'node_join_types': [row.join_type for row in rows],
        'node_min_successes': [row.min_success for row in rows],
        'node_task_options': [row.task_options for row in rows],
        'node_max_retries': [fact[0] for fact in facts],
        'node_executions': [fact[1] for fact in facts],
        'node_good_untils': [fact[2] for fact in facts],
        'node_retention_class_keys': [fact[3] for fact in facts],
        'node_is_subworkflow': [_is_subworkflow(row) for row in rows],
        'node_sub_workflow_names': [sub[0] for sub in subs],
        'node_sub_definition_keys': [sub[1] for sub in subs],
        'node_sub_workflow_params': [sub[2] for sub in subs],
        'failed_workflow_ids': [record.workflow_id for record in tree.failed],
        'failed_task_indexes': [record.task_index for record in tree.failed],
        'failed_results': [record.result for record in tree.failed],
    }


def start_parameters(tree: StartTree, sent_at: datetime) -> dict[str, object]:
    """The parameters of ``horsies_workflow_start`` for ``tree``, started at ``sent_at``."""
    return {**tree_parameters(tree), 'sent_at': sent_at}
