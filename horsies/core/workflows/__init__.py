"""Workflow execution engine for DAG-based task orchestration."""

from horsies.core.workflows.engine import (
    start_workflow,
    start_workflow_async,
    pause_workflow,
    pause_workflow_sync,
    resume_workflow,
    resume_workflow_sync,
    on_workflow_task_complete,
)
from horsies.core.workflows.recovery import recover_stuck_workflows
from horsies.core.workflows.lifecycle_types import (
    LifecycleErrorCode,
    LifecycleOperationError,
    LifecycleResult,
)
from horsies.core.workflows.start_types import (
    WorkflowStartError,
    WorkflowStartErrorCode,
    WorkflowStartResult,
    WorkflowStartStage,
)

__all__ = [
    'start_workflow',
    'start_workflow_async',
    'pause_workflow',
    'pause_workflow_sync',
    'resume_workflow',
    'resume_workflow_sync',
    'on_workflow_task_complete',
    'recover_stuck_workflows',
    # Workflow lifecycle result types
    'LifecycleErrorCode',
    'LifecycleOperationError',
    'LifecycleResult',
    # Workflow start result types
    'WorkflowStartError',
    'WorkflowStartErrorCode',
    'WorkflowStartResult',
    'WorkflowStartStage',
]
