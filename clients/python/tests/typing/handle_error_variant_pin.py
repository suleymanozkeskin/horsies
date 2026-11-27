# pyright: reportPrivateUsage=false
"""Type pin: the workflow handle error variants, their cause fields, and the handle id.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a cause that
``raise ... from`` accepts without a match, a cause field that accepts
``NoCause()``, or an async status read that can return ``ASYNC_CONTEXT``
fails the check. A handle id is a ``UUID``: a text goes through
``Horsies.workflow_handle``, whose error is not a handle operation failure.
"""

from __future__ import annotations

import uuid

from horsies import (
    WorkflowHandleAsyncContext,
    WorkflowHandleDatabaseOperationFailed,
    WorkflowHandleInvalidWorkflowId,
    WorkflowHandleError,
    WorkflowHandleResumeRecoveryFailed,
    WorkflowHandleStoredDataInvalid,
    WorkflowHandleNotFound,
    Horsies,
    NoCause,
    WorkflowHandle,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import Err, Ok, is_err


def causes_are_exceptions() -> None:
    _ = WorkflowHandleDatabaseOperationFailed(
        message='read failed',
        retryable=True,
        workflow_id='id',
        cause=ConnectionError('lost'),
    )
    _ = WorkflowHandleDatabaseOperationFailed(
        message='read failed',
        retryable=True,
        workflow_id='id',
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = WorkflowHandleStoredDataInvalid(
        message='stored data invalid',
        workflow_id='id',
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = WorkflowHandleResumeRecoveryFailed(
        message='recovery failed',
        workflow_id='id',
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def chain_the_database_cause(failed: WorkflowHandleDatabaseOperationFailed) -> None:
    raise RuntimeError('read failed') from failed.cause


def chain_a_cause_without_a_match(error: WorkflowHandleError) -> None:
    raise RuntimeError('failed') from error.exception  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]


def chain_the_matched_cause(error: WorkflowHandleError) -> None:
    match error:
        case WorkflowHandleDatabaseOperationFailed(cause=cause):
            raise RuntimeError(error.message) from cause
        case WorkflowHandleStoredDataInvalid(cause=cause):
            raise RuntimeError(error.message) from cause
        case WorkflowHandleResumeRecoveryFailed(cause=cause):
            raise RuntimeError(error.message) from cause
        case WorkflowHandleNotFound() | WorkflowHandleAsyncContext():
            raise RuntimeError(error.message)


async def async_status_read_has_no_async_context(
    handle: WorkflowHandle[int],
) -> None:
    result = await handle._status()
    if is_err(result):
        match result.err_value:
            case WorkflowHandleAsyncContext():  # pyright: ignore[reportUnnecessaryComparison]
                pass
            case _:
                pass


def the_handle_id_is_a_uuid(broker: PostgresBroker) -> None:
    _ = WorkflowHandle[int](workflow_id=uuid.UUID(int=1), broker=broker)
    _ = WorkflowHandle[int](
        workflow_id='3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f',  # pyright: ignore[reportArgumentType]
        broker=broker,
    )


def a_workflow_id_text_goes_through_the_app(app: Horsies) -> uuid.UUID:
    match app.workflow_handle('3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f'):
        case Ok(ok_value=handle):
            return handle.workflow_id
        case Err(err_value=WorkflowHandleInvalidWorkflowId(cause=cause) as refused):
            raise RuntimeError(refused.message) from cause


def an_invalid_id_is_not_a_handle_operation_failure(error: WorkflowHandleError) -> None:
    match error:
        case WorkflowHandleInvalidWorkflowId():  # pyright: ignore[reportUnnecessaryComparison]
            pass
        case _:
            pass
