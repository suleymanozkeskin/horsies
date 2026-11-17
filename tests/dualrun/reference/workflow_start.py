"""Workflow start, before ``horsies_workflow_start``.

``start_workflow_async`` (``python/horsies/core/workflows/lifecycle.py``) sent, in
one session transaction: the top workflow row (``ON CONFLICT DO NOTHING``);
on a conflict, the read of the stored name; else every node row in one
bulk insert, the task-node roots ENQUEUED with their task ids; then, for
each sub-workflow root, ``enqueue_subworkflow_task`` (the engine's child
start, which runs the failure policy of a child that cannot be built); the
drain of the parent advances; the status read and the task rows of the
roots, or their revert to READY when a failure paused the workflow.

The engine functions that the start called stay in the package (workflow
recovery calls them); this module keeps the start's own statements and
the building of their parameters.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.workflow import SubWorkflowNode, TaskNode, WorkflowSpec
from horsies.core.models.workflow.nodes import stored_kwargs
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.retention import queue_retention_class
from horsies.core.workflows.sql import (
    INSERT_TASK_FOR_WORKFLOW_SQL,
    INSERT_WORKFLOW_TASKS_BULK_SQL,
    REVERT_PRELINKED_FAST_ROOTS_SQL,
    subworkflow_node_kind_params,
    task_node_kind_params,
    with_downstream_subworkflow,
    workflow_task_row_params,
)

INSERT_WORKFLOW_SQL: Final = text("""
    INSERT INTO horsies_workflows (id, name, status, on_error, output_task_index,
                             success_policy, definition_key,
                             depth, root_workflow_id,
                             sent_at, created_at, started_at, updated_at)
    VALUES (:id, :name, 'RUNNING', :on_error, :output_idx,
            :success_policy, :definition_key,
            0, :id,
            :sent_at, NOW(), NOW(), NOW())
    ON CONFLICT (id) DO NOTHING
    RETURNING id
""")

GET_WORKFLOW_NAME_SQL: Final = text("""SELECT name FROM horsies_workflows WHERE id = :wf_id""")

GET_WORKFLOW_STATUS_SQL: Final = text(
    """SELECT status FROM horsies_workflows WHERE id = :wf_id"""
)


@dataclass(frozen=True, slots=True)
class Started:
    """The tree was written."""


@dataclass(frozen=True, slots=True)
class Existing:
    """A workflow with the id and the same name exists; nothing was written."""


@dataclass(frozen=True, slots=True)
class IdConflict:
    """A workflow with the id and another name exists; nothing was written."""

    existing_name: str


type StartReport = Started | Existing | IdConflict


def _json(value: object) -> str:
    match dumps_json(value):
        case Ok(text_value):
            return text_value
        case Err(error):
            raise AssertionError(f'a seed value does not serialize: {error}')


def _success_policy(spec: WorkflowSpec[Any]) -> str | None:
    if spec.success_policy is None:
        return None
    policy: dict[str, Any] = {
        'cases': [
            {'required_indices': [t.index for t in case.required if t.index is not None]}
            for case in spec.success_policy.cases
        ],
    }
    if spec.success_policy.optional:
        policy['optional_indices'] = [
            t.index for t in spec.success_policy.optional if t.index is not None
        ]
    return _json(policy)


@dataclass(frozen=True, slots=True)
class _NodeParameters:
    nodes: list[dict[str, Any]]
    root_tasks: list[dict[str, Any]]
    root_indexes: list[int]
    subworkflow_roots: list[int]


def _node_parameters(
    broker: PostgresBroker, spec: WorkflowSpec[Any], workflow_id: str, sent_at: datetime,
) -> _NodeParameters:
    from horsies.core.history.identity.uuid7 import mint_task_id
    from horsies.core.task_decorator import resolve_node_queue
    from horsies.core.workflows.lifecycle import start_task_node_facts, subworkflow_params_of

    built = _NodeParameters(nodes=[], root_tasks=[], root_indexes=[], subworkflow_roots=[])
    for node in spec.tasks:
        assert node.index is not None
        dependencies = [d.index for d in node.waits_for if d.index is not None]
        args_from = {k: v.index for k, v in node.args_from.items() if v.index is not None}
        ctx_from = (
            [n.node_id for n in node.workflow_ctx_from if n.node_id is not None]
            if node.workflow_ctx_from
            else None
        )
        common: dict[str, Any] = {
            'id': str(uuid.uuid4()),
            'wf_id': workflow_id,
            'idx': node.index,
            'node_id': node.node_id,
            'name': node.name,
            'input': _json(stored_kwargs(node)),
            'deps': dependencies,
            'args_from': _json(args_from) if args_from else None,
            'ctx_from': ctx_from,
            'allow_failed': node.allow_failed_deps,
            'join_type': node.join,
            'min_success': node.min_success,
        }
        match node:
            case SubWorkflowNode():
                if not dependencies:
                    built.subworkflow_roots.append(node.index)
                built.nodes.append({
                    **common,
                    'queue': 'default',
                    'priority': 100,
                    'task_options': None,
                    **subworkflow_node_kind_params(subworkflow_params_of(node)),
                    'status': 'PENDING' if dependencies else 'READY',
                    'is_subworkflow': True,
                    'sub_wf_name': node.workflow_def.name,
                    'sub_def_key': node.workflow_def._require_definition_key(),  # pyright: ignore[reportPrivateUsage]
                    'task_id': None,
                    'is_enqueued': False,
                })
            case TaskNode():
                assert node.priority is not None
                options = node.fn.task_options_json
                queue = resolve_node_queue(node)
                facts = start_task_node_facts(
                    options, node.name, queue_retention_class(broker, queue),
                )
                task_id: str | None = None
                if not dependencies:
                    task_id = str(mint_task_id())
                    task_input = dict(stored_kwargs(node))
                    task_input['__h_workflow_meta__'] = {
                        'workflow_id': workflow_id,
                        'task_index': node.index,
                        'task_name': node.name,
                    }
                    built.root_indexes.append(node.index)
                    built.root_tasks.append(workflow_task_row_params(
                        task_id=task_id,
                        task_name=node.name,
                        queue_name=queue,
                        priority=node.priority,
                        input_json=_json(task_input),
                        sent_at=sent_at,
                        task_options_json=options,
                        facts=facts,
                    ))
                built.nodes.append({
                    **common,
                    'queue': queue,
                    'priority': node.priority,
                    'task_options': options,
                    **task_node_kind_params(facts),
                    'status': 'ENQUEUED' if task_id is not None else (
                        'PENDING' if dependencies else 'READY'
                    ),
                    'is_subworkflow': False,
                    'sub_wf_name': None,
                    'sub_def_key': None,
                    'task_id': task_id,
                    'is_enqueued': task_id is not None,
                })
    return built


async def _existing(session: AsyncSession, spec: WorkflowSpec[Any], workflow_id: str) -> StartReport:
    row = (await session.execute(GET_WORKFLOW_NAME_SQL, {'wf_id': workflow_id})).fetchone()
    existing_name = row.name if row is not None else None
    if existing_name is not None and existing_name != spec.name:
        return IdConflict(existing_name=existing_name)
    return Existing()


async def start(
    session_factory: async_sessionmaker[AsyncSession],
    broker: PostgresBroker,
    spec: WorkflowSpec[Any],
    workflow_id: str,
    sent_at: datetime,
) -> StartReport:
    """The start of ``spec`` as ``workflow_id``, in one transaction. Spec nodes
    with ``good_until`` are outside this copy (the seeds have none)."""
    from horsies.core.workflows.engine import (
        drain_parent_propagations_in_session,
        enqueue_subworkflow_task,
    )

    async with session_factory() as session:
        inserted = (
            await session.execute(
                INSERT_WORKFLOW_SQL,
                {
                    'id': workflow_id,
                    'name': spec.name,
                    'on_error': spec.on_error.value,
                    'output_idx': spec.output.index if spec.output else None,
                    'success_policy': _success_policy(spec),
                    'definition_key': spec.definition_key,
                    'sent_at': sent_at,
                },
            )
        ).scalar_one_or_none()
        if inserted is None:
            return await _existing(session, spec, workflow_id)
        built = _node_parameters(broker, spec, workflow_id, sent_at)
        await session.execute(INSERT_WORKFLOW_TASKS_BULK_SQL, with_downstream_subworkflow(built.nodes))
        for index in built.subworkflow_roots:
            await enqueue_subworkflow_task(
                session,
                broker,
                workflow_id,
                index,
                all_dep_results={},
                all_dep_task_names={},
                parent_depth=0,
                root_workflow_id=workflow_id,
                child_sent_at=sent_at,
            )
        if built.subworkflow_roots:
            await drain_parent_propagations_in_session(session, broker)
        if built.root_tasks:
            runnable = True
            if built.subworkflow_roots:
                status = (
                    await session.execute(GET_WORKFLOW_STATUS_SQL, {'wf_id': workflow_id})
                ).fetchone()
                runnable = status is not None and status.status == 'RUNNING'
            if runnable:
                await session.execute(INSERT_TASK_FOR_WORKFLOW_SQL, built.root_tasks)
            else:
                await session.execute(
                    REVERT_PRELINKED_FAST_ROOTS_SQL,
                    {'wf_id': workflow_id, 'idxs': built.root_indexes},
                )
        await session.commit()
    return Started()
