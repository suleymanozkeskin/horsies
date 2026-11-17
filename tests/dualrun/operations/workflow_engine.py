"""The operation table of the workflow engine switch.

One row per Python entry point of the workflow engine that calls the core.
Every operation of this switch runs in pool mode: the core commits its own
statements on its own pool. Run:
``pytest tests/integration --dual-run=workflow-engine``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import DualRunOperation, PoolMode

WORKFLOW_ENGINE_OPERATIONS: Final[tuple[DualRunOperation, ...]] = (
    DualRunOperation(
        name='workflow start',
        production_path='horsies.core.workflows.lifecycle:start_workflow_async',
        reference_path='tests.dualrun.reference.workflow_start_caller:start_workflow_async',
        mode=PoolMode(),
        core_methods=('start_workflow_async',),
    ),
    DualRunOperation(
        name='workflow resume',
        production_path='horsies.core.workflows.lifecycle:resume_workflow',
        reference_path='tests.dualrun.reference.workflow_resume_caller:resume_workflow',
        mode=PoolMode(),
        core_methods=('resume_workflow_async',),
    ),
    DualRunOperation(
        name='workflow phase 2',
        production_path='horsies.core.worker.finalize:FinalizeMixin._finalize_workflow_phase',
        reference_path='tests.dualrun.reference.workflow_phase2_caller:finalize_workflow_phase',
        mode=PoolMode(),
        core_methods=('finish_workflow_node_async',),
    ),
    DualRunOperation(
        name='workflow parent advance',
        production_path='horsies.core.worker.finalize:FinalizeMixin._drive_parent_propagations',
        reference_path='tests.dualrun.reference.workflow_phase2_caller:drive_parent_propagations',
        mode=PoolMode(),
        core_methods=('advance_workflow_parents_async',),
    ),
    DualRunOperation(
        name='workflow pause',
        production_path='horsies.core.workflows.lifecycle:pause_workflow',
        reference_path='tests.dualrun.reference.workflow_pause_caller:pause_workflow',
        mode=PoolMode(),
        core_methods=('pause_workflow_async',),
    ),
    DualRunOperation(
        name='workflow cancel',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.cancel_async',
        reference_path='tests.dualrun.reference.workflow_cancel_caller:cancel_async',
        mode=PoolMode(),
        core_methods=('cancel_workflow_async',),
    ),
    DualRunOperation(
        name='workflow retention delete',
        production_path='horsies.core.worker.reaper:ReaperMixin._delete_expired_workflows',
        reference_path='tests.dualrun.reference.workflow_retention_caller:delete_expired_workflows',
        mode=PoolMode(),
        core_methods=('delete_expired_workflows_async',),
    ),
    DualRunOperation(
        name='paused workflow expiry',
        production_path='horsies.core.worker.reaper:ReaperMixin._expire_paused_workflows',
        reference_path='tests.dualrun.reference.paused_expiry_caller:expire_paused_workflows',
        mode=PoolMode(),
        core_methods=('expire_paused_workflows_async',),
    ),
    DualRunOperation(
        name='workflow handle status',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.status_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:status_async',
        mode=PoolMode(),
        core_methods=('workflow_status_async',),
    ),
    DualRunOperation(
        name='workflow handle get',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.get_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:get_async',
        mode=PoolMode(),
        core_methods=('wait_workflow_result_async',),
    ),
    DualRunOperation(
        name='workflow handle result for',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.result_for_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:result_for_async',
        mode=PoolMode(),
        core_methods=('workflow_node_result_async',),
    ),
    DualRunOperation(
        name='workflow handle results',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.results_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:results_async',
        mode=PoolMode(),
        core_methods=('workflow_node_results_async',),
    ),
    DualRunOperation(
        name='workflow handle tasks',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.tasks_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:tasks_async',
        mode=PoolMode(),
        core_methods=('workflow_nodes_async',),
    ),
)
