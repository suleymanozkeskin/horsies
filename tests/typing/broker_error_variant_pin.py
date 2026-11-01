"""Type pin: the broker error variants and their cause fields.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a variant that accepts
another code, a cause that ``raise ... from`` accepts without a match, or a
field on the wrong variant fails the check.

The enqueue path and ``ensure_schema_initialized`` return a subset of
``BrokerOperationError``. Each narrowed return type has an assignment to
its declared type, which fails when the type widens. The public methods
also have a ``BrokerOperationFailed`` arm that must be reported as never
matched.

The mapping from a broker error to a send error takes only the enqueue
and sync-call errors. Passing an operation error to it must fail.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, assert_never

from sqlalchemy.ext.asyncio import AsyncSession

from horsies import (
    BrokerAsyncContext,
    BrokerEnqueueError,
    BrokerEnqueueFailed,
    BrokerErrorCode,
    BrokerEventLoopLimit,
    BrokerIdempotencyKeyConflict,
    BrokerInvalidInput,
    BrokerInvalidTaskOptions,
    BrokerLoopRunnerFailed,
    BrokerOperationError,
    BrokerOperationFailed,
    BrokerPayloadMismatch,
    BrokerSyncCallError,
    Err,
    NoCause,
    Ok,
    TaskSendPayload,
    broker_error_cause,
)
from horsies.core.brokers.postgres import (
    PostgresBroker,
    _async_context_err,  # pyright: ignore[reportPrivateUsage]
    _loop_runner_err,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.brokers.resources import BrokerResources
from horsies.core.models.schedule import TaskSchedule
from horsies.core.scheduler.service import Scheduler
from horsies.core.task_decorator import (
    _map_broker_err,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.types.result import Result
from horsies.core.utils.loop_runner import LoopRunnerError


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



async def enqueue_async_returns_only_enqueue_errors(broker: PostgresBroker) -> None:
    declared: Result[str, BrokerEnqueueError] = await broker.enqueue_async(
        'report', task_id='task-1', enqueue_sha='sha',
    )
    _ = declared
    match await broker.enqueue_async('report', task_id='task-1', enqueue_sha='sha'):
        case Ok():
            pass
        case Err(error):
            match error:
                case BrokerOperationFailed():  # pyright: ignore[reportUnnecessaryComparison]
                    pass
                case _:
                    pass


def enqueue_returns_only_enqueue_and_sync_call_errors(broker: PostgresBroker) -> None:
    declared: Result[str, BrokerEnqueueError | BrokerSyncCallError] = broker.enqueue(
        'report', task_id='task-1', enqueue_sha='sha',
    )
    _ = declared
    match broker.enqueue('report', task_id='task-1', enqueue_sha='sha'):
        case Ok():
            pass
        case Err(error):
            match error:
                case BrokerOperationFailed():  # pyright: ignore[reportUnnecessaryComparison]
                    pass
                case _:
                    pass


async def schema_init_returns_only_operation_and_loop_limit(
    broker: PostgresBroker,
) -> None:
    declared: Result[None, BrokerOperationFailed | BrokerEventLoopLimit] = (
        await broker.ensure_schema_initialized()
    )
    _ = declared
    match await broker.ensure_schema_initialized():
        case Ok():
            pass
        case Err(error):
            match error:
                case BrokerEnqueueFailed():  # pyright: ignore[reportUnnecessaryComparison]
                    pass
                case _:
                    pass


def every_enqueue_variant_is_matched(
    error: BrokerEnqueueError | BrokerSyncCallError,
) -> str:
    match error:
        case BrokerEnqueueFailed():
            return 'write failed or not confirmed'
        case BrokerPayloadMismatch():
            return 'payload mismatch'
        case BrokerIdempotencyKeyConflict():
            return 'key conflict'
        case BrokerInvalidTaskOptions():
            return 'task options refused'
        case BrokerInvalidInput():
            return 'input refused'
        case BrokerEventLoopLimit():
            return 'loop limit'
        case BrokerAsyncContext():
            return 'async context'
        case BrokerLoopRunnerFailed():
            return 'loop runner failed'
        case _ as unreachable:
            assert_never(unreachable)


async def enqueue_helpers_return_their_subsets(
    broker: PostgresBroker,
    resources: BrokerResources,
    session: AsyncSession,
    good_until: datetime,
) -> None:
    cutover_values = PostgresBroker._cutover_enqueue_values  # pyright: ignore[reportPrivateUsage]
    cutover: Result[dict[str, Any], BrokerInvalidInput] = cutover_values(
        task_name='report',
        queue_name='default',
        priority=100,
        args_json=None,
        kwargs_json=None,
        kwargs={},
        good_until=good_until,
        enqueue_delay_seconds=None,
        task_options=None,
        task_options_value=None,
        retention_class_key='default',
        retain_rerun_input=True,
    )
    bind_key = broker._bind_key_after_task_id_conflict  # pyright: ignore[reportPrivateUsage]
    bound: Result[str, BrokerEnqueueFailed | BrokerPayloadMismatch] = await bind_key(
        session,
        task_id='task-1',
        enqueue_sha='sha',
        task_name='report',
        key_digest=b'key',
        command_fingerprint=b'fingerprint',
    )
    verify = broker._verify_enqueue_conflict  # pyright: ignore[reportPrivateUsage]
    verified: Result[str, BrokerEnqueueFailed | BrokerPayloadMismatch] = await verify(
        resources, 'task-1', 'sha', 'report',
    )
    async_context: Err[BrokerAsyncContext] = _async_context_err(
        'PostgresBroker.enqueue()', 'broker.enqueue_async(...)',
    )
    loop_runner: Err[BrokerLoopRunnerFailed] = _loop_runner_err(
        'PostgresBroker.enqueue()', LoopRunnerError('stopped'),
    )
    _ = (cutover, bound, verified, async_context, loop_runner)


async def scheduled_enqueue_returns_only_enqueue_errors(
    scheduler: Scheduler,
    schedule: TaskSchedule,
    slot_time: datetime,
) -> None:
    enqueue_slot = scheduler._enqueue_scheduled_task  # pyright: ignore[reportPrivateUsage]
    enqueued: Result[str, BrokerEnqueueError] = await enqueue_slot(
        schedule, slot_time=slot_time,
    )
    _ = enqueued


def send_mapping_refuses_the_operation_error(
    enqueue_error: BrokerEnqueueError,
    operation_error: BrokerOperationFailed,
    payload: TaskSendPayload,
) -> None:
    _ = _map_broker_err(enqueue_error, 'task-id', payload)
    _ = _map_broker_err(operation_error, 'task-id', payload)  # pyright: ignore[reportArgumentType]
