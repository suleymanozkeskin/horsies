"""Type pin: the workflow start error variants and the retry parameter.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a retry method that
accepts a refused variant, a cause that ``raise ... from`` accepts without a
match, or a field on the wrong variant fails the check.
"""

from __future__ import annotations

from horsies import (
    NoCause,
    WorkflowSpec,
    WorkflowStartAsyncContext,
    WorkflowStartBrokerNotConfigured,
    WorkflowStartCommandConflict,
    WorkflowStartEnqueueFailed,
    WorkflowStartIdConflict,
    WorkflowStartValidationFailed,
)


def sync_retries(
    spec: WorkflowSpec[int],
    enqueue_failed: WorkflowStartEnqueueFailed,
    async_context: WorkflowStartAsyncContext,
    broker_not_configured: WorkflowStartBrokerNotConfigured,
    validation_failed: WorkflowStartValidationFailed,
    id_conflict: WorkflowStartIdConflict,
    command_conflict: WorkflowStartCommandConflict,
) -> None:
    _ = spec.retry_start(enqueue_failed)
    _ = spec.retry_start(async_context)
    _ = spec.retry_start(broker_not_configured)  # pyright: ignore[reportArgumentType]
    _ = spec.retry_start(validation_failed)  # pyright: ignore[reportArgumentType]
    _ = spec.retry_start(id_conflict)  # pyright: ignore[reportArgumentType]
    _ = spec.retry_start(command_conflict)  # pyright: ignore[reportArgumentType]


async def async_retries(
    spec: WorkflowSpec[int],
    async_context: WorkflowStartAsyncContext,
    id_conflict: WorkflowStartIdConflict,
    command_conflict: WorkflowStartCommandConflict,
) -> None:
    _ = await spec.retry_start_async(async_context)
    _ = await spec.retry_start_async(id_conflict)  # pyright: ignore[reportArgumentType]
    _ = await spec.retry_start_async(command_conflict)  # pyright: ignore[reportArgumentType]


def enqueue_cause_is_an_exception() -> None:
    _ = WorkflowStartEnqueueFailed(
        message='start failed',
        retryable=True,
        workflow_name='wf',
        workflow_id='id',
        cause=ConnectionError('lost'),
    )
    _ = WorkflowStartEnqueueFailed(
        message='start failed',
        retryable=True,
        workflow_name='wf',
        workflow_id='id',
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def chain_the_enqueue_cause(enqueue_failed: WorkflowStartEnqueueFailed) -> None:
    raise RuntimeError('start failed') from enqueue_failed.cause


def chain_the_validation_cause(validation_failed: WorkflowStartValidationFailed) -> None:
    raise RuntimeError('refused') from validation_failed.cause  # pyright: ignore[reportGeneralTypeIssues]


def chain_the_matched_cause(validation_failed: WorkflowStartValidationFailed) -> None:
    match validation_failed.cause:
        case NoCause():
            raise RuntimeError(validation_failed.message)
        case cause:
            raise RuntimeError(validation_failed.message) from cause


def existing_name_only_on_the_conflict(
    conflict: WorkflowStartIdConflict,
    validation_failed: WorkflowStartValidationFailed,
    command_conflict: WorkflowStartCommandConflict,
) -> None:
    _ = conflict.existing_workflow_name
    _ = validation_failed.existing_workflow_name  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
    _ = command_conflict.existing_workflow_name  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
