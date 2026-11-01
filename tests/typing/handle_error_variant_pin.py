# pyright: reportPrivateUsage=false
"""Type pin: the workflow handle error variants and their cause fields.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a cause that
``raise ... from`` accepts without a match, a cause field that accepts
``NoCause()``, a field on the wrong variant, or a status read that can
return a loop limit fails the check.
"""

from __future__ import annotations

from horsies import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleEventLoopLimit,
    HandleLoopRunnerFailed,
    HandleOperationError,
    HandleResumeRecoveryFailed,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
    NoCause,
    WorkflowHandle,
)
from horsies.core.brokers.resources import BrokerResources
from horsies.core.types.result import is_err


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
    _ = HandleLoopRunnerFailed(
        message='runner failed',
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
        case HandleLoopRunnerFailed(exception=cause):
            raise RuntimeError(error.message) from cause
        case HandleWorkflowNotFound() | HandleEventLoopLimit() | HandleAsyncContext():
            raise RuntimeError(error.message)


def loop_limit_only_on_the_loop_limit(
    loop_limit: HandleEventLoopLimit,
    async_context: HandleAsyncContext,
) -> None:
    _ = loop_limit.loop_limit
    _ = async_context.loop_limit  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]


async def status_read_has_no_loop_limit(
    handle: WorkflowHandle[int],
    resources: BrokerResources,
) -> None:
    result = await handle._status(resources)
    if is_err(result):
        match result.err_value:
            case HandleEventLoopLimit():  # pyright: ignore[reportUnnecessaryComparison]
                pass
            case HandleAsyncContext():  # pyright: ignore[reportUnnecessaryComparison]
                pass
            case _:
                pass
