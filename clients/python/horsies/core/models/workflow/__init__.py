"""Core workflow models for DAG-based task orchestration."""

# Re-export all public symbols for backward compatibility.
# External code can continue to use:
#   from horsies.core.models.workflow import TaskNode, WorkflowSpec, ...

from horsies.core.models.workflow.enums import (
    OkT as OkT,
    OkT_co as OkT_co,
    OutT as OutT,
    WorkflowStatus,
    WORKFLOW_TERMINAL_STATES,
    WorkflowNodeStatus,
    WORKFLOW_NODE_TERMINAL_STATES,
    OnError,
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
from horsies.core.models.workflow.no_output import NoWorkflowOutput
from horsies.core.models.workflow.typing_utils import (
    NODE_ID_PATTERN as NODE_ID_PATTERN,
    node_id_from_text,
    validate_workflow_generic_output_match,
)
from horsies.core.models.workflow.context import (
    SubWorkflowSummary,
    WorkflowMetadata,
    WorkflowContextMissingId,
    WorkflowContext,
)
from horsies.core.models.workflow.handle import (
    WorkflowNodeInfo,
    WorkflowHandle,
)
from horsies.core.models.workflow.handle_types import (
    WorkflowHandleAsyncContext,
    WorkflowHandleDatabaseOperationFailed,
    WorkflowHandleErrorCode,
    WorkflowHandleInvalidWorkflowId,
    WorkflowHandleError,
    HandleResult,
    WorkflowHandleResumeRecoveryFailed,
    WorkflowHandleStoredDataInvalid,
    WorkflowHandleNotFound,
)
from horsies.core.models.workflow.spec import WorkflowSpec
from horsies.core.models.workflow.definition import (
    WorkflowDefinitionMeta as WorkflowDefinitionMeta,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
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
    'WorkflowNodeStatus',
    'WORKFLOW_NODE_TERMINAL_STATES',
    'OnError',
    # nodes
    'NodeKey',
    'TaskNode',
    'SubWorkflowNode',
    'AnyNode',
    'SuccessCase',
    'SuccessPolicy',
    'WorkflowTerminalResults',
    'NoWorkflowOutput',
    # typing utils
    'NODE_ID_PATTERN',
    'node_id_from_text',
    'validate_workflow_generic_output_match',
    # context
    'SubWorkflowSummary',
    'WorkflowMetadata',
    'WorkflowContextMissingId',
    'WorkflowContext',
    # handle
    'WorkflowNodeInfo',
    'WorkflowHandle',
    # handle types
    'WorkflowHandleAsyncContext',
    'WorkflowHandleDatabaseOperationFailed',
    'WorkflowHandleErrorCode',
    'WorkflowHandleInvalidWorkflowId',
    'WorkflowHandleError',
    'HandleResult',
    'WorkflowHandleResumeRecoveryFailed',
    'WorkflowHandleStoredDataInvalid',
    'WorkflowHandleNotFound',
    # spec
    'WorkflowSpec',
    # definition
    'WorkflowDefinition',
    'WorkflowDefinitionConfig',
    'WorkflowDefinitionMeta',
    # errors
    'WorkflowValidationError',
]
