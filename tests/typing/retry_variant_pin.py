"""Type pin: the retry methods accept only the ``TaskSendRetryable`` variants.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
call that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a retry method that
accepts a refused variant, or a cause field that ``raise ... from`` accepts
without a match, fails the check.
"""

from __future__ import annotations

from horsies import (
    NoCause,
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendEventLoopLimit,
    TaskSendIdempotencyKeyConflict,
    TaskSendLoopRunnerFailed,
    TaskSendPayloadMismatch,
    TaskSendPayloadTooLarge,
    TaskSendSuppressed,
    TaskSendValidationFailed,
)
from horsies.core.task_decorator import TaskFunction


def sync_retries(
    task: TaskFunction[..., int],
    suppressed: TaskSendSuppressed,
    async_context: TaskSendAsyncContext,
    validation_failed: TaskSendValidationFailed,
    enqueue_failed: TaskSendEnqueueFailed,
    payload_mismatch: TaskSendPayloadMismatch,
    payload_too_large: TaskSendPayloadTooLarge,
    key_conflict: TaskSendIdempotencyKeyConflict,
    loop_limit: TaskSendEventLoopLimit,
    loop_runner: TaskSendLoopRunnerFailed,
) -> None:
    _ = task.retry_send(enqueue_failed)
    _ = task.retry_send(async_context)
    _ = task.retry_send(key_conflict)
    _ = task.retry_send(loop_runner)
    _ = task.retry_send(suppressed)  # pyright: ignore[reportArgumentType]
    _ = task.retry_send(validation_failed)  # pyright: ignore[reportArgumentType]
    _ = task.retry_send(payload_mismatch)  # pyright: ignore[reportArgumentType]
    _ = task.retry_send(payload_too_large)  # pyright: ignore[reportArgumentType]
    _ = task.retry_send(loop_limit)  # pyright: ignore[reportArgumentType]
    _ = task.retry_schedule(enqueue_failed)
    _ = task.retry_schedule(payload_mismatch)  # pyright: ignore[reportArgumentType]


async def async_retries(
    task: TaskFunction[..., int],
    enqueue_failed: TaskSendEnqueueFailed,
    payload_mismatch: TaskSendPayloadMismatch,
    loop_runner: TaskSendLoopRunnerFailed,
) -> None:
    _ = await task.retry_send_async(enqueue_failed)
    _ = await task.retry_send_async(loop_runner)
    _ = await task.retry_schedule_async(loop_runner)
    _ = await task.retry_send_async(payload_mismatch)  # pyright: ignore[reportArgumentType]
    _ = await task.retry_schedule_async(enqueue_failed)
    _ = await task.retry_schedule_async(payload_mismatch)  # pyright: ignore[reportArgumentType]


def chain_the_cause(enqueue_failed: TaskSendEnqueueFailed) -> None:
    raise RuntimeError('send failed') from enqueue_failed.exception  # pyright: ignore[reportGeneralTypeIssues]


def chain_the_matched_cause(enqueue_failed: TaskSendEnqueueFailed) -> None:
    match enqueue_failed.exception:
        case NoCause():
            raise RuntimeError('send failed')
        case cause:
            raise RuntimeError('send failed') from cause


def chain_the_runner_cause(loop_runner: TaskSendLoopRunnerFailed) -> None:
    raise RuntimeError('send failed') from loop_runner.exception
