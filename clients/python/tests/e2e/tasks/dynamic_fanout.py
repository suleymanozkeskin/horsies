"""E2E tasks for the dynamic fan-out example in the subworkflows docs.

The child workflow ``ShardReport`` is built at run time by
``SubWorkflowNode.build_with`` from the result of ``list_shards``. It has one
count node per shard and a fan-in node that reads the shard results through
``workflow_context.result_for(NodeKey(...))``. The shapes follow the example in
``website/src/content/docs/concepts/workflows/subworkflows.md``
("Dynamic Fan-Out with ``args_from``").
"""

from __future__ import annotations

from typing import Any

from horsies import (
    Err,
    Failed,
    Horsies,
    NodeKey,
    Ok,
    Result,
    SubWorkflowNode,
    Succeeded,
    TaskError,
    TaskNode,
    TaskOutcome,
    WorkflowContext,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
    WorkflowSpec,
)

from tests.e2e.tasks.instance_dynamic_fanout import app

FANOUT_TENANT_ID = 'tenant-1'
FANOUT_SHARD_SUFFIXES = ('a', 'b', 'c')


@app.task(name='e2e_fanout_ready')
def ready_task() -> Result[str, TaskError]:
    """Readiness probe for the fan-out worker."""
    return Ok('ready')


@app.task(name='e2e_fanout_list_shards')
def list_shards(*, tenant_id: str) -> Result[list[str], TaskError]:
    return Ok([f'{tenant_id}-{suffix}' for suffix in FANOUT_SHARD_SUFFIXES])


@app.task(name='e2e_fanout_count_shard_rows')
def count_shard_rows(*, shard: str) -> Result[int, TaskError]:
    return Ok(len(shard))


@app.task(name='e2e_fanout_sum_shard_rows')
def sum_shard_rows(
    *,
    shard_count: int,
    workflow_context: WorkflowContext | None = None,
) -> Result[int, TaskError]:
    if workflow_context is None:
        return Err(
            TaskError(code='NO_CONTEXT', message='workflow_context is missing'),
        )
    total = 0
    for position in range(shard_count):
        match workflow_context.result_for(NodeKey[int](f'shard_{position}')):
            case Failed(error):
                return Err(error)
            case Succeeded(rows):
                total += rows
    return Ok(total)


class ShardReport(WorkflowDefinition[int]):
    """Child workflow. Its shape depends on the result of list_shards."""

    name = 'e2e_fanout_shard_report'
    definition_key = 'tests.e2e.dynamic_fanout.shard_report.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        shards: TaskOutcome[list[str], TaskError],
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[int]:
        match shards:
            case Failed():
                raise ValueError('shard_report needs the shard list')
            case Succeeded(shard_names):
                pass

        count_nodes: list[TaskNode[int]] = [
            count_shard_rows.node(node_id=f'shard_{position}')(shard=shard_name)
            for position, shard_name in enumerate(shard_names)
        ]
        total = sum_shard_rows.node(
            waits_for=list(count_nodes),
            workflow_context_from=list(count_nodes),
            node_id='total',
        )(shard_count=len(count_nodes))
        return app.workflow(
            name=cls.name,
            nodes=[*count_nodes, total],
            output=total,
        )


class TenantReport(WorkflowDefinition[int]):
    """Parent workflow: list the shards, then build the child from the list."""

    name = 'e2e_fanout_tenant_report'
    definition_key = 'tests.e2e.dynamic_fanout.tenant_report.v1'

    shards = list_shards.node(node_id='shards')(tenant_id=FANOUT_TENANT_ID)
    report = SubWorkflowNode(
        workflow_definition=ShardReport,
        waits_for=[shards],
        args_from={'shards': shards},
        node_id='report',
    )

    config = WorkflowDefinitionConfig(output=report)
