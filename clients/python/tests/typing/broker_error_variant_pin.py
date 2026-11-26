"""Type pin: the broker error variants and their cause fields.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a variant that accepts
another code, a cause that ``raise ... from`` accepts without a match, or a
field on the wrong variant fails the check.

The enqueue path and ``ensure_schema_initialized`` return a subset of
``DatabaseOperationError``. Each narrowed return type has an assignment to
its declared type, which fails when the type widens. The public methods
also have a ``DatabaseOperationFailed`` arm that must be reported as never
matched.

The mapping from a broker error to a send error takes only the enqueue
and sync-call errors. Passing an operation error to it must fail.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import assert_never
from uuid import UUID

from horsies import (
    DatabaseAsyncContext,
    DatabaseEnqueueError,
    DatabaseEnqueueFailed,
    DatabaseErrorCode,
    DatabaseIdempotencyKeyConflict,
    DatabaseInvalidInput,
    DatabaseInvalidTaskOptions,
    DatabaseOperationError,
    DatabaseOperationFailed,
    DatabasePayloadMismatch,
    DatabaseSyncCallError,
    Err,
    NoCause,
    Ok,
    TaskSendPayload,
    broker_error_cause,
)
from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.postgres import (
    PostgresBroker,
    _async_context_err,  # pyright: ignore[reportPrivateUsage]
    _parse_enqueue_input,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.task_decorator import (
    _map_broker_err,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.types.result import Result

TASK_ID = UUID('0198c0de-0000-7000-8000-000000000001')


def operation_codes() -> None:
    _ = DatabaseOperationFailed(
        code=DatabaseErrorCode.DATABASE_PING_FAILED,
        message='ping failed',
        retryable=True,
        exception=ConnectionError('lost'),
    )
    _ = DatabaseOperationFailed(
        code=DatabaseErrorCode.ENQUEUE_FAILED,  # pyright: ignore[reportArgumentType]
        message='enqueue failed',
        retryable=True,
        exception=ConnectionError('lost'),
    )
    _ = DatabaseOperationFailed(
        code=DatabaseErrorCode.CLOSE_FAILED,
        message='close failed',
        retryable=False,
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def chain_the_operation_cause(operation: DatabaseOperationFailed) -> None:
    raise RuntimeError('operation failed') from operation.exception


def chain_the_enqueue_cause(enqueue: DatabaseEnqueueFailed) -> None:
    raise RuntimeError('enqueue failed') from enqueue.exception  # pyright: ignore[reportGeneralTypeIssues]


def chain_the_refusal_cause(refused: DatabaseInvalidInput) -> None:
    raise RuntimeError('refused') from refused.exception  # pyright: ignore[reportGeneralTypeIssues]


def chain_the_matched_cause(error: DatabaseOperationError) -> None:
    match broker_error_cause(error):
        case NoCause():
            raise RuntimeError(error.message)
        case cause:
            raise RuntimeError(error.message) from cause


def owner_only_on_the_conflict(
    conflict: DatabaseIdempotencyKeyConflict,
    mismatch: DatabasePayloadMismatch,
) -> None:
    _ = conflict.owner_task_id
    _ = mismatch.owner_task_id  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]



async def enqueue_async_returns_only_enqueue_errors(broker: PostgresBroker) -> None:
    declared: Result[UUID, DatabaseEnqueueError] = await broker.enqueue_async(
        'report', task_id=TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc),
    )
    _ = declared
    match await broker.enqueue_async('report', task_id=TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc)):
        case Ok():
            pass
        case Err(error):
            match error:
                case DatabaseOperationFailed():  # pyright: ignore[reportUnnecessaryComparison]
                    pass
                case _:
                    pass


def enqueue_returns_only_enqueue_and_sync_call_errors(broker: PostgresBroker) -> None:
    declared: Result[UUID, DatabaseEnqueueError | DatabaseSyncCallError] = broker.enqueue(
        'report', task_id=TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc),
    )
    _ = declared
    match broker.enqueue('report', task_id=TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc)):
        case Ok():
            pass
        case Err(error):
            match error:
                case DatabaseOperationFailed():  # pyright: ignore[reportUnnecessaryComparison]
                    pass
                case _:
                    pass


async def schema_init_returns_only_the_operation_failure(
    broker: PostgresBroker,
) -> None:
    declared: Result[None, DatabaseOperationFailed] = (
        await broker.ensure_schema_initialized()
    )
    _ = declared
    match await broker.ensure_schema_initialized():
        case Ok():
            pass
        case Err(error):
            match error:
                case DatabaseEnqueueFailed():  # pyright: ignore[reportUnnecessaryComparison]
                    pass
                case _:
                    pass


def every_enqueue_variant_is_matched(
    error: DatabaseEnqueueError | DatabaseSyncCallError,
) -> str:
    match error:
        case DatabaseEnqueueFailed():
            return 'write failed or not confirmed'
        case DatabasePayloadMismatch():
            return 'payload mismatch'
        case DatabaseIdempotencyKeyConflict():
            return 'key conflict'
        case DatabaseInvalidTaskOptions():
            return 'task options refused'
        case DatabaseInvalidInput():
            return 'input refused'
        case DatabaseAsyncContext():
            return 'async context'
        case _ as unreachable:
            assert_never(unreachable)


def enqueue_helpers_return_their_subsets() -> None:
    stored_input: Result[str, DatabaseInvalidInput] = _parse_enqueue_input(
        'report', None,
    )
    async_context: Err[DatabaseAsyncContext] = _async_context_err(
        'PostgresBroker.enqueue()', 'broker.enqueue_async(...)',
    )
    _ = (stored_input, async_context)


def send_mapping_refuses_the_operation_error(
    enqueue_error: DatabaseEnqueueError,
    operation_error: DatabaseOperationFailed,
    payload: TaskSendPayload,
) -> None:
    _ = _map_broker_err(enqueue_error, TASK_ID, payload)
    _ = _map_broker_err(operation_error, TASK_ID, payload)  # pyright: ignore[reportArgumentType]
