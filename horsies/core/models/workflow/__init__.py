"""Core workflow models for DAG-based task orchestration."""

# pyright: reportPrivateUsage=false

# Re-export all public symbols for backward compatibility.
# External code can continue to use:
#   from horsies.core.models.workflow import TaskNode, WorkflowSpec, ...

from horsies.core.models.workflow.enums import (
    OkT,
    OkT_co,
    OutT,
    WorkflowStatus,
    WORKFLOW_TERMINAL_STATES,
    WorkflowTaskStatus,
    WORKFLOW_TASK_TERMINAL_STATES,
    WF_TASK_TERMINAL_VALUES,
    OnError,
    SubWorkflowRetryMode,
)
from horsies.core.models.workflow.nodes import (
    NodeKey,
    TaskNode,
    SubWorkflowNode,
    AnyNode,
    SuccessCase,
    SuccessPolicy,
    WorkflowTerminalResults,
)
from horsies.core.models.workflow.typing_utils import (
    NODE_ID_PATTERN,
    slugify,
    _get_inspect_target,
    _task_accepts_workflow_ctx,
    _get_signature,
    _signature_accepts_kwargs,
    _valid_kwarg_names,
    _resolved_type_hints,
    _extract_taskresult_ok_type,
    _normalize_resolved_ok_type,
    _resolve_task_fn_ok_type,
    _resolve_workflow_def_ok_type,
    _resolve_source_node_ok_type,
    validate_workflow_generic_output_match,
    _format_type_name,
    _is_ok_type_compatible,
)
from horsies.core.models.workflow.context import (
    SubWorkflowSummary,
    WorkflowMeta,
    WorkflowContextMissingIdError,
    WorkflowContext,
)
from horsies.core.models.workflow.handle import (
    GET_WORKFLOW_STATUS_SQL,
    GET_WORKFLOW_RESULT_SQL,
    GET_WORKFLOW_ERROR_SQL,
    GET_WORKFLOW_TASK_RESULTS_SQL,
    GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL,
    GET_WORKFLOW_TASKS_SQL,
    CANCEL_WORKFLOW_SQL,
    SYNC_RUNNING_ENQUEUED_WORKFLOW_TASKS_ON_CANCEL_SQL,
    MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL,
    SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL,
    SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL,
    WorkflowTaskInfo,
    WorkflowHandle,
)
from horsies.core.models.workflow.handle_types import (
    HandleErrorCode,
    HandleOperationError,
    HandleResult,
)
from horsies.core.models.workflow.spec import WorkflowSpec
from horsies.core.models.workflow.definition import (
    WorkflowDefinitionMeta,
    WorkflowDefinition,
)

# Backward compatibility: re-export WorkflowValidationError
from horsies.core.errors import WorkflowValidationError

__all__ = [
    # enums
    'OkT',
    'OkT_co',
    'OutT',
    'WorkflowStatus',
    'WORKFLOW_TERMINAL_STATES',
    'WorkflowTaskStatus',
    'WORKFLOW_TASK_TERMINAL_STATES',
    'WF_TASK_TERMINAL_VALUES',
    'OnError',
    'SubWorkflowRetryMode',
    # nodes
    'NodeKey',
    'TaskNode',
    'SubWorkflowNode',
    'AnyNode',
    'SuccessCase',
    'SuccessPolicy',
    'WorkflowTerminalResults',
    # typing utils
    'NODE_ID_PATTERN',
    'slugify',
    '_get_inspect_target',
    '_task_accepts_workflow_ctx',
    '_get_signature',
    '_signature_accepts_kwargs',
    '_valid_kwarg_names',
    '_resolved_type_hints',
    '_extract_taskresult_ok_type',
    '_normalize_resolved_ok_type',
    '_resolve_task_fn_ok_type',
    '_resolve_workflow_def_ok_type',
    '_resolve_source_node_ok_type',
    'validate_workflow_generic_output_match',
    '_format_type_name',
    '_is_ok_type_compatible',
    # context
    'SubWorkflowSummary',
    'WorkflowMeta',
    'WorkflowContextMissingIdError',
    'WorkflowContext',
    # handle
    'GET_WORKFLOW_STATUS_SQL',
    'GET_WORKFLOW_RESULT_SQL',
    'GET_WORKFLOW_ERROR_SQL',
    'GET_WORKFLOW_TASK_RESULTS_SQL',
    'GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL',
    'GET_WORKFLOW_TASKS_SQL',
    'CANCEL_WORKFLOW_SQL',
    'SYNC_RUNNING_ENQUEUED_WORKFLOW_TASKS_ON_CANCEL_SQL',
    'MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL',
    'SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL',
    'SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL',
    'WorkflowTaskInfo',
    'WorkflowHandle',
    # handle types
    'HandleErrorCode',
    'HandleOperationError',
    'HandleResult',
    # spec
    'WorkflowSpec',
    # definition
    'WorkflowDefinitionMeta',
    'WorkflowDefinition',
    # errors
    'WorkflowValidationError',
]
