"""Type pin: the broker error variants and their cause fields.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a variant that accepts
another code, a cause that ``raise ... from`` accepts without a match, or a
field on the wrong variant fails the check.
"""

from __future__ import annotations

from typing import assert_never

from horsies import (
    BrokerEnqueueFailed,
    BrokerErrorCode,
    BrokerIdempotencyKeyConflict,
    BrokerInvalidInput,
    BrokerOperationError,
    BrokerOperationFailed,
    BrokerPayloadMismatch,
    NoCause,
    broker_error_cause,
)


def operation_codes() -> None:
    _ = BrokerOperationFailed(
        code=BrokerErrorCode.DB_PING_FAILED,
        message='ping failed',
        retryable=True,
        exception=ConnectionError('lost'),
    )
    _ = BrokerOperationFailed(
        code=BrokerErrorCode.ENQUEUE_FAILED,  # pyright: ignore[reportArgumentType]
        message='enqueue failed',
        retryable=True,
        exception=ConnectionError('lost'),
    )
    _ = BrokerOperationFailed(
        code=BrokerErrorCode.CLOSE_FAILED,
        message='close failed',
        retryable=False,
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def chain_the_operation_cause(operation: BrokerOperationFailed) -> None:
    raise RuntimeError('operation failed') from operation.exception


def chain_the_enqueue_cause(enqueue: BrokerEnqueueFailed) -> None:
    raise RuntimeError('enqueue failed') from enqueue.exception  # pyright: ignore[reportGeneralTypeIssues]


def chain_the_refusal_cause(refused: BrokerInvalidInput) -> None:
    raise RuntimeError('refused') from refused.exception  # pyright: ignore[reportGeneralTypeIssues]


def chain_the_matched_cause(error: BrokerOperationError) -> None:
    match broker_error_cause(error):
        case NoCause():
            raise RuntimeError(error.message)
        case cause:
            raise RuntimeError(error.message) from cause


def owner_only_on_the_conflict(
    conflict: BrokerIdempotencyKeyConflict,
    mismatch: BrokerPayloadMismatch,
) -> None:
    _ = conflict.owner_task_id
    _ = mismatch.owner_task_id  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]


def every_variant_is_matched(error: BrokerOperationError) -> str:
    match error:
        case BrokerOperationFailed() | BrokerEnqueueFailed():
            return 'operation'
        case BrokerPayloadMismatch() | BrokerIdempotencyKeyConflict():
            return 'conflict'
        case _ as unreachable:
            assert_never(unreachable)  # pyright: ignore[reportArgumentType]
