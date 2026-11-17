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
)
