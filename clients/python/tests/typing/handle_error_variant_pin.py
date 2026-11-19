# pyright: reportPrivateUsage=false
"""Type pin: the workflow handle error variants, their cause fields, and the handle id.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a cause that
``raise ... from`` accepts without a match, a cause field that accepts
``NoCause()``, or an async status read that can return ``ASYNC_CONTEXT``
fails the check. A handle id is a ``UUID``: a text goes through
``WorkflowHandle.for_id``, whose error is not a handle operation failure.
"""

from __future__ import annotations

import uuid

from horsies import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleInvalidWorkflowId,
    HandleOperationError,
    HandleResumeRecoveryFailed,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
    NoCause,
    WorkflowHandle,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import Err, Ok, is_err


def causes_are_exceptions() -> None:
    _ = HandleDbOperationFailed(
        message='read failed',
        retryable=True,
        workflow_id='id',
        exception=ConnectionError('lost'),
    )
    _ = HandleDbOperationFailed(
        message='read failed',
        retryable=True,
        workflow_id='id',
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = HandleStoredDataInvalid(
        message='stored data invalid',
        workflow_id='id',
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = HandleResumeRecoveryFailed(
        message='recovery failed',
        workflow_id='id',
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def chain_the_database_cause(failed: HandleDbOperationFailed) -> None:
    raise RuntimeError('read failed') from failed.exception


def chain_a_cause_without_a_match(error: HandleOperationError) -> None:
    raise RuntimeError('failed') from error.exception  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]


def chain_the_matched_cause(error: HandleOperationError) -> None:
    match error:
        case HandleDbOperationFailed(exception=cause):
            raise RuntimeError(error.message) from cause
        case HandleStoredDataInvalid(exception=cause):
            raise RuntimeError(error.message) from cause
        case HandleResumeRecoveryFailed(exception=cause):
            raise RuntimeError(error.message) from cause
        case HandleWorkflowNotFound() | HandleAsyncContext():
            raise RuntimeError(error.message)


async def async_status_read_has_no_async_context(
    handle: WorkflowHandle[int],
) -> None:
    result = await handle._status()
    if is_err(result):
        match result.err_value:
            case HandleAsyncContext():  # pyright: ignore[reportUnnecessaryComparison]
                pass
            case _:
                pass


def the_handle_id_is_a_uuid(broker: PostgresBroker) -> None:
    _ = WorkflowHandle[int](workflow_id=uuid.UUID(int=1), broker=broker)
    _ = WorkflowHandle[int](
        workflow_id='3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f',  # pyright: ignore[reportArgumentType]
        broker=broker,
    )


def a_workflow_id_text_goes_through_for_id(broker: PostgresBroker) -> uuid.UUID:
    match WorkflowHandle.for_id('3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f', broker):
        case Ok(ok_value=handle):
            return handle.workflow_id
        case Err(err_value=HandleInvalidWorkflowId(exception=cause) as refused):
            raise RuntimeError(refused.message) from cause


def an_invalid_id_is_not_a_handle_operation_failure(error: HandleOperationError) -> None:
    match error:
        case HandleInvalidWorkflowId():  # pyright: ignore[reportUnnecessaryComparison]
            pass
        case _:
            pass
