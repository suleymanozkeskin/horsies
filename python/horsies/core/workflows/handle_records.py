"""The handle errors of the workflow operations that run in the core.

A workflow operation of the handle (pause, cancel, and the reads) runs one
statement through ``horsies._core``. The core answers with one record per outcome; these
functions build the public ``HandleOperationError`` of a failure record, of a
core boundary error, and of a workflow id text that is not a UUID.
"""

from __future__ import annotations

import uuid
from enum import Enum
from typing import assert_never

import horsies._core as core
from horsies._core import CoreError
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleStoredDataInvalid,
)
from horsies.core.types.core_failure import CoreOperationFailed, core_error_retryable
from horsies.core.types.result import Err, Ok, Result


class WorkflowOperation(Enum):
    """A workflow operation of the handle that runs in the core."""

    PAUSE = 'pause_workflow'
    CANCEL = 'cancel_workflow'
    STATUS = 'workflow_status'
    GET = 'wait_workflow_result'
    RESULT_FOR = 'workflow_node_result'
    RESULTS = 'workflow_node_results'
    TASKS = 'workflow_nodes'
    RESUME = 'resume_workflow'


def _label(operation: WorkflowOperation) -> str:
    """The word that starts the message of a failure. Pure."""
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


def handle_failed_error(
    record: core.HandleFailed,
    workflow_id: str,
    operation: WorkflowOperation,
) -> HandleDbOperationFailed | HandleStoredDataInvalid:
    """The handle error of a ``HandleFailed`` record. Pure.

    ``exception`` holds a ``CoreOperationFailed`` whose subject is the
    operation, with the record's message and retry rule; the core exception
    classes replace it (#1363).
    """
    message = f'{_label(operation)} failed for workflow {workflow_id}: {record.message}'
    cause = CoreOperationFailed(operation.value, record.message, record.retryable)
    match record.code:
        case 'DB_OPERATION_FAILED':
            return HandleDbOperationFailed(
                message=message,
                retryable=record.retryable,
                workflow_id=workflow_id,
                exception=cause,
            )
        case 'STORED_DATA_INVALID':
            return HandleStoredDataInvalid(
                message=message,
                workflow_id=workflow_id,
                exception=cause,
            )
        case _ as unreachable:
            assert_never(unreachable)


def core_call_error(
    exc: CoreError,
    workflow_id: str,
    operation: WorkflowOperation,
) -> HandleDbOperationFailed:
    """The handle error of a core boundary error raised by the call: nothing
    ran (``TooManyPendingCalls``, ``CoreStopped``, ...), or the outcome is
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
