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
    WorkflowTaskStatus,
    WORKFLOW_TASK_TERMINAL_STATES,
    WF_TASK_TERMINAL_VALUES as WF_TASK_TERMINAL_VALUES,
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
    NODE_ID_PATTERN as NODE_ID_PATTERN,
    slugify,
    validate_workflow_generic_output_match,
)
from horsies.core.models.workflow.context import (
    SubWorkflowSummary,
    WorkflowMeta,
    WorkflowContextMissingIdError,
    WorkflowContext,
)
from horsies.core.models.workflow.handle import (
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
    WorkflowDefinitionMeta as WorkflowDefinitionMeta,
    WorkflowDefinition,
)

# Backward compatibility: re-export WorkflowValidationError
from horsies.core.errors import WorkflowValidationError

__all__ = [
    # enums
    'WorkflowStatus',
    'WORKFLOW_TERMINAL_STATES',
    'WorkflowTaskStatus',
    'WORKFLOW_TASK_TERMINAL_STATES',
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
    'slugify',
    'validate_workflow_generic_output_match',
    # context
    'SubWorkflowSummary',
    'WorkflowMeta',
    'WorkflowContextMissingIdError',
    'WorkflowContext',
    # handle
    'WorkflowTaskInfo',
    'WorkflowHandle',
    # handle types
    'HandleErrorCode',
    'HandleOperationError',
    'HandleResult',
    # spec
    'WorkflowSpec',
    # definition
    'WorkflowDefinition',
    # errors
    'WorkflowValidationError',
]
