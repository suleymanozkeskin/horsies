"""Type pin: the resend methods accept only the ``TaskSendResendable`` variants.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
call that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a resend method that
accepts a refused variant, or a cause field that ``raise ... from`` accepts
without a match, fails the check.
"""

from __future__ import annotations

from horsies import (
    NoCause,
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendIdempotencyKeyConflict,
    TaskSendPayloadMismatch,
    TaskSendPayloadTooLarge,
    TaskSendSuppressed,
    TaskSendValidationFailed,
)
from horsies.core.task_decorator import TaskFunction


def sync_resends(
    task: TaskFunction[..., int],
    suppressed: TaskSendSuppressed,
    async_context: TaskSendAsyncContext,
    validation_failed: TaskSendValidationFailed,
    enqueue_failed: TaskSendEnqueueFailed,
    payload_mismatch: TaskSendPayloadMismatch,
    payload_too_large: TaskSendPayloadTooLarge,
    key_conflict: TaskSendIdempotencyKeyConflict,
) -> None:
    _ = task.resend(enqueue_failed)
    _ = task.resend(async_context)
    _ = task.resend(key_conflict)  # pyright: ignore[reportArgumentType]
    _ = task.resend(suppressed)  # pyright: ignore[reportArgumentType]
    _ = task.resend(validation_failed)  # pyright: ignore[reportArgumentType]
    _ = task.resend(payload_mismatch)  # pyright: ignore[reportArgumentType]
    _ = task.resend(payload_too_large)  # pyright: ignore[reportArgumentType]


async def async_resends(
    task: TaskFunction[..., int],
    enqueue_failed: TaskSendEnqueueFailed,
    payload_mismatch: TaskSendPayloadMismatch,
    async_context: TaskSendAsyncContext,
    key_conflict: TaskSendIdempotencyKeyConflict,
) -> None:
    _ = await task.resend_async(enqueue_failed)
    _ = await task.resend_async(async_context)
    _ = await task.resend_async(key_conflict)  # pyright: ignore[reportArgumentType]
    _ = await task.resend_async(payload_mismatch)  # pyright: ignore[reportArgumentType]


def chain_the_cause(enqueue_failed: TaskSendEnqueueFailed) -> None:
    raise RuntimeError('send failed') from enqueue_failed.cause  # pyright: ignore[reportGeneralTypeIssues]


def chain_the_matched_cause(enqueue_failed: TaskSendEnqueueFailed) -> None:
    match enqueue_failed.cause:
        case NoCause():
            raise RuntimeError('send failed')
        case cause:
            raise RuntimeError('send failed') from cause
