"""The handle errors of the workflow operations that the client builds.

A workflow operation of the handle (pause, cancel, resume, and the reads)
runs through ``horsies._core``. The core answers with one record per
outcome; a failure is the generated ``HandleOperationError`` class, which
the core builds with its message, and the client returns it as it is. These
functions build the handle errors that never reach the core: a core
boundary error raised by the call, and a workflow id text that is not a
UUID. The calls raise ``ForkedAfterCoreStart`` and ``CoreStopped``
(``call_refusal_or_raise``): no handle error names them.
"""

from __future__ import annotations

import uuid
from enum import Enum
from typing import assert_never

from horsies.core.models.workflow.handle_types import HandleDbOperationFailed
from horsies.core.types.core_failure import CoreCallRefusal, core_error_retryable
from horsies.core.types.result import Err, Ok, Result


class WorkflowOperation(Enum):
    """A workflow operation of the handle that runs in the core. The value
    is the core's name of the operation (``horsies_core::HandleOperation``),
    the subject of the core's exception of a failure."""

    PAUSE = 'pause_workflow'
    CANCEL = 'cancel_workflow'
    STATUS = 'workflow_status'
    GET = 'wait_workflow_result'
    RESULT_FOR = 'workflow_node_result'
    RESULTS = 'workflow_node_results'
    TASKS = 'workflow_nodes'
    RESUME = 'resume_workflow'


def _label(operation: WorkflowOperation) -> str:
    """The word that starts the message of a failure, as the core's
    ``HandleOperation::failure_message`` starts the failures that it builds.
    The core's message of a failed ``result_for`` read names the node
    instead: ``DB query failed for result_for node {node_id} in workflow
    {id}: {cause}``. Pure."""
    match operation:
        case WorkflowOperation.PAUSE:
            return 'Pause'
        case WorkflowOperation.CANCEL:
            return 'Cancel'
        case WorkflowOperation.STATUS:
            return 'Status'
        case WorkflowOperation.GET:
            return 'Get'
        case WorkflowOperation.RESULT_FOR:
            return 'Result read'
        case WorkflowOperation.RESULTS:
            return 'Results'
        case WorkflowOperation.TASKS:
            return 'Tasks'
        case WorkflowOperation.RESUME:
            return 'Resume'
        case _ as unreachable:
            assert_never(unreachable)


def workflow_uuid(
    workflow_id: str,
    operation: WorkflowOperation,
) -> Result[uuid.UUID, HandleDbOperationFailed]:
    """The workflow id text as a UUID. Pure.

    Text that is not a UUID is ``DB_OPERATION_FAILED``, not retryable: the
    database refused it with SQLSTATE 22P02 when the statement took the text,
    and the public result stays the same.
    """
    try:
        return Ok(uuid.UUID(workflow_id))
    except ValueError as exc:
        return Err(HandleDbOperationFailed(
            message=(
                f'{_label(operation)} failed for workflow {workflow_id}: '
                f'the id is not a UUID: {exc}'
            ),
            retryable=False,
            workflow_id=workflow_id,
            exception=exc,
        ))


def core_call_error(
    exc: CoreCallRefusal,
    workflow_id: str,
    operation: WorkflowOperation,
) -> HandleDbOperationFailed:
    """The handle error of a core boundary error raised by the call: nothing
    ran (``TooManyPendingCalls``, ``CoreStartFailed``), or the outcome is
    unknown (``CoreCallTimedOut``). Retryable by ``core_error_retryable``. Pure."""
    return HandleDbOperationFailed(
        message=(
            f'{_label(operation)} failed for workflow {workflow_id}: '
            f'{type(exc).__name__}: {exc}'
        ),
        retryable=core_error_retryable(exc),
        workflow_id=workflow_id,
        exception=exc,
    )
