# app/core/brokers/postgres.py
from __future__ import annotations
import asyncio, hashlib, contextlib, os, threading, time
from collections.abc import AsyncGenerator, Callable, Coroutine
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    ClassVar,
    Final,
    Literal,
    Optional,
    TYPE_CHECKING,
    assert_never,
)
from uuid import UUID
from datetime import datetime, timedelta, timezone
import psycopg
from horsies._catalog.manifest import dump_producer_manifest
from horsies._core import (
    ClassCoverageRecord,
    ClassReportRecord,
    CoreError,
    CoreFailure,
    TaskListComplete,
    TaskListTruncated,
    WorkerStateAbsent,
    WorkerPongsRecord,
    WorkerStateRecord,
    WorkerStatesRecord,
    CoreHandle,
    DatabasePingRecord,
    Enqueued,
    EnqueueRequest,
    SendEnqueueFailed,
    SendKeyConflict,
    SendPayloadMismatch,
    SendAsyncContext,
    SendPayloadTooLarge,
    SendRefusalByRule,
    SendRefusalByValueRule,
    SendRefusalUnreadableJson,
    SendSuppressed,
    SendValidationFailed,
    StartAfterDelay as CoreStartAfterDelay,
    StartAt as CoreStartAt,
    StartNow as CoreStartNow,
    SchemaInstalled,
    SchemaInstallNotRun,
    RawResultAbsent,
    RawResultRecordRecord,
    TaskAttemptRecord,
    TaskInfoNotFound,
    TaskInfoRecord,
    StaleClaimsBoundReached,
    StaleClaimsDrained,
    StaleClaimsRequeueFailed,
    OrphanAuditFailed,
    OrphanAuditLockBusy,
    OrphanPageAudited,
    PendingExpiryBoundReached,
    PendingExpiryDrained,
    PendingExpiryFailed,
    StaleRunningRecovered,
    StaleRunningScanFailed,
    StaleRunningScanFull,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.resources import (
    BrokerResources,
    BuildForLoop,
    EventLoopLimit,
    LimitReached,
    ListenerDisabled,
    ListenerEnabled,
    ListenerState,
    LoopResources,
    LoopRole,
    SessionEngine,
    SessionEngineSeparate,
    SessionEngineShared,
    UseExisting,
    decide_for_loop,
    split_closed_loops,
)
from horsies.core.brokers.enqueue_task import (
    EnqueueKey,
    EnqueueRow,
    EnqueueStart,
    FirstRun,
    Keyed,
    SendKind,
    StartAfterDelay,
    StartAt,
    StartNow,
    Unkeyed,
)
from horsies.core.brokers.worker_state_records import (
    class_coverage_of,
    class_report_of,
    worker_state_snapshot,
)
from horsies.core.brokers.result_types import (
    BrokerAsyncContext,
    BrokerEnqueueError,
    BrokerEnqueueFailed,
    BrokerErrorCode,
    BrokerEventLoopLimit,
    BrokerIdempotencyKeyConflict,
    BrokerInvalidInput,
    BrokerInvalidJsonPayload,
    BrokerInvalidTaskOptions,
    BrokerListenerDisabled,
    BrokerOperationFailed,
    BrokerOperationFailedCode,
    BrokerPayloadMismatch,
    BrokerResult,
    BrokerSyncCallError,
    BrokerWaitInsideTask,
    RawResultRecord,
)
from horsies.core.manifest import (
    install_coverage_of,
    schema_mode_of,
    to_producer_manifest,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.recovery import OrphanTaskAuditReport
from horsies.core.models.retention import RetentionConfig
from horsies.core.models.workflow_pg import (
    WorkflowModel as _WorkflowModel,
    WorkflowTaskModel as _WorkflowTaskModel,
)
from horsies.core.types.core_failure import (
    CoreCallRefusal,
    call_refusal_or_raise,
    core_error_retryable,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.status import (
    TaskAttemptOutcome,
    TaskStatus,
)
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.codec.json_io import Json, SerializationError, loads_json
from horsies.core.codec.task_options import (
    OPTIONS_INTEGER_OUT_OF_RANGE,
    options_integer_out_of_range,
    stored_execution,
    stored_max_retries,
)
from horsies.core.codec.validation_errors import without_validation_input
from horsies.core.models.tasks import Execution, TaskInfo, TaskAttemptInfo
from horsies.core.models.class_coverage import ClassCoverage, ClassReport
from horsies.core.models.health import (
    DatabasePing,
    WorkerPong,
    WorkerStateSnapshot,
)
from horsies.core.history.identity.fingerprint import (
    COMMAND_FINGERPRINT_VERSION,
    EnqueueCommandV1,
)
from horsies.core.history.periods import RetentionClassKey
from horsies.core.history.ddl.classes import (
    DEFAULT_RETENTION_CLASS_KEY,
    resolve_retention_class_key,
)
from horsies.core.history.maintenance.database import (
    PartitionMaintenanceDatabase,
)
from horsies.core.defaults import PRE_PING_AFTER_IDLE_S
from horsies.core.utils.db import (
    is_retryable_connection_error,
    SessionSettings,
    register_identity_text_reads,
    register_idle_ping,
    register_session_settings,
)
from horsies.core.process_role import (
    ProcessRole,
    process_role,
    wait_inside_task_message,
)
from horsies.core.utils.loop_runner import (
    LoopRunner,
    async_context_message,
    in_running_event_loop,
)
from horsies.core.utils.task_loop_registry import is_task_loop
from horsies.core.utils.url import sqlalchemy_url
from horsies.core.utils.wait_timeout import wait_timeout_seconds

if TYPE_CHECKING:
    from horsies.core.app import Horsies
from horsies.core.logging import enable_statement_echo, get_logger

# Ensure workflow tables are registered in SQLAlchemy metadata.
_ = (_WorkflowModel, _WorkflowTaskModel)


def _operation_failed(
    code: BrokerOperationFailedCode,
    message: str,
    exc: BaseException,
) -> Err[BrokerOperationFailed]:
    """Build a ``BrokerOperationFailed`` with retryable classification from the raw exception.

    A pydantic ``ValidationError`` cause is stored in its form without input
    values (``without_validation_input``).
    """
    return Err(
        BrokerOperationFailed(
            code=code,
            message=message,
            retryable=is_retryable_connection_error(exc),
            exception=without_validation_input(exc),
        )
    )


def _schema_init_failed(exc: CoreFailure | CoreCallRefusal) -> Err[BrokerOperationFailed]:
    """``SCHEMA_INIT_FAILED`` with the cause of an install that failed, or
    that the core of this process could not run. Pure."""
    return _operation_failed(
        BrokerErrorCode.SCHEMA_INIT_FAILED,
        f'Schema initialization failed: {exc}',
        exc,
    )


_DRIVER_ERRORS: Final = (SQLAlchemyError, psycopg.Error, OSError)
"""What a statement can raise when the database or the network fails."""


@dataclass(frozen=True, slots=True)
class SendCommand:
    """The stored values of one send command, checked. Only the database can refuse it now.

    The version-1 fingerprint leaves the task id and ``sent_at`` out, so one
    value serves every row of the command: each slot of a schedule fire.
    ``stored_input`` is the ``input`` text the row stores; ``execution`` and
    ``max_retries`` are read from ``task_options``.
    """

    task_name: str
    queue_name: str
    priority: int
    stored_input: str
    task_options: str | None
    execution: Execution
    max_retries: int
    retention_class_key: str
    retain_rerun_input: bool
    fingerprint_version: int
    fingerprint: bytes


def _send_command(
    *,
    task_name: str,
    queue_name: str,
    priority: int,
    input_json: str | None,
    good_until: datetime | None,
    enqueue_delay_seconds: int | None,
    task_options: str | None,
    retention_class_key: str,
    retain_rerun_input: bool,
) -> Result[SendCommand, BrokerInvalidInput | BrokerInvalidTaskOptions]:
    """Check the input and the options of one send and build its command. Pure.

    Refusals, in this order: the input text (``INVALID_INPUT``), the
    options text (``INVALID_TASK_OPTIONS``), a value the fingerprint
    refuses (``INVALID_INPUT``).
    """
    input_r = _parse_enqueue_input(task_name, input_json)
    if is_err(input_r):
        return input_r
    stored_input = input_r.ok_value
    options_r = _parse_enqueue_task_options(task_name, task_options)
    if is_err(options_r):
        return options_r
    parsed_options = options_r.ok_value
    fingerprint_r = _command_fingerprint(
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
        stored_input=stored_input,
        good_until=good_until,
        enqueue_delay_seconds=enqueue_delay_seconds,
        task_options=task_options,
        retention_class_key=retention_class_key,
        retain_rerun_input=retain_rerun_input,
    )
    if is_err(fingerprint_r):
        return fingerprint_r
    return Ok(SendCommand(
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
        stored_input=stored_input,
        task_options=task_options,
        execution=parsed_options.execution,
        max_retries=parsed_options.max_retries,
        retention_class_key=retention_class_key,
        retain_rerun_input=retain_rerun_input,
        fingerprint_version=COMMAND_FINGERPRINT_VERSION,
        fingerprint=fingerprint_r.ok_value,
    ))


@dataclass(frozen=True, slots=True)
class _PreparedEnqueue:
    """The checked input of one enqueue. Only the database can refuse it now.

    ``row.sent_at`` is the caller's call-site instant, which the row stores.
    """

    row: EnqueueRow


def _command_fingerprint(
    *,
    task_name: str,
    queue_name: str,
    priority: int,
    stored_input: str,
    good_until: datetime | None,
    enqueue_delay_seconds: int | None,
    task_options: str | None,
    retention_class_key: str,
    retain_rerun_input: bool,
) -> Result[bytes, BrokerInvalidInput]:
    """The version-1 command fingerprint of a send. Pure.

    The input travels in the fingerprint's ``input_json`` element as the
    stored ``input`` text, and the args element is always null: a Python send has
    no positional arguments. The fingerprint is therefore a function of the
    stored row. A value that the fingerprint refuses is ``INVALID_INPUT``.
    """
    try:
        command = EnqueueCommandV1(
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            positional_args_json=None,
            input_json=stored_input,
            good_until=good_until,
            enqueue_delay_seconds=enqueue_delay_seconds,
            task_options_json=task_options,
            retention_class_key=retention_class_key,
            retain_rerun_input=retain_rerun_input,
            rerun_of_task_id=None,
            rerun_root_task_id=None,
        )
    except ValueError as exc:
        return Err(BrokerInvalidInput(
            message=f'enqueue input refused for {task_name}: {exc}',
            exception=exc,
        ))
    return Ok(command.fingerprint)


_SENT_AT_FUTURE_TOLERANCE: Final = timedelta(seconds=5)
"""Absorbs trivial clock drift in the future ``sent_at`` guard."""


def _check_enqueue_timing(
    *,
    sent_at: datetime,
    enqueued_at: datetime | None,
    enqueue_delay_seconds: int | None,
) -> Result[None, BrokerInvalidInput]:
    """Refuse a call whose instants cannot mean what they say.

    A naive instant cannot be compared with now, and a naive ``enqueued_at``
    would be stored under the session time zone: a wrong instant. ``sent_at``
    is an immutable call-site timestamp, not a scheduling mechanism: a future
    one with no explicit schedule is almost certainly legacy ETA usage that
    would silently run immediately, because ``enqueued_at`` defaults to NOW().
    """
    if enqueued_at is not None and enqueue_delay_seconds is not None:
        return Err(BrokerInvalidInput(
            message='Cannot specify both enqueued_at and enqueue_delay_seconds',
            exception=NoCause(),
        ))
    for label, instant in (('sent_at', sent_at), ('enqueued_at', enqueued_at)):
        if instant is not None and instant.tzinfo is None:
            return Err(BrokerInvalidInput(
                message=f'{label} must be timezone-aware',
                exception=NoCause(),
            ))
    if (
        sent_at > datetime.now(timezone.utc) + _SENT_AT_FUTURE_TOLERANCE
        and enqueued_at is None
        and enqueue_delay_seconds is None
    ):
        return Err(BrokerInvalidInput(
            message=(
                'sent_at is in the future without enqueued_at or enqueue_delay_seconds; '
                'sent_at is a call-site timestamp, use enqueued_at or '
                'enqueue_delay_seconds to schedule deferred execution'
            ),
            exception=NoCause(),
        ))
    return Ok(None)


@dataclass(frozen=True, slots=True)
class _EnqueueOptions:
    """The columns read from the task options of one enqueue."""

    max_retries: int
    execution: Execution


def _parse_enqueue_task_options(
    task_name: str, task_options: str | None,
) -> Result[_EnqueueOptions, BrokerInvalidTaskOptions]:
    """Parse task options to their retry limit and execution class.

    ``serialize_task_options()`` always produces this text, so malformed JSON
    is a bug, not a runtime condition, and it must not be silently swallowed.
    No options (None) store no retries and ``ISOLATED``. An empty text is
    not JSON and is refused, as the core refuses it. Options that hold an int
    outside ``MIN_OPTIONS_INTEGER`` to ``MAX_OPTIONS_INTEGER`` (for example a
    ``max_delay_seconds`` of ``2**64``) are refused before the retry limit is
    read, as the core refuses them.
    """
    if task_options is None:
        return Ok(_EnqueueOptions(max_retries=0, execution=Execution.ISOLATED))
    opts_r = loads_json(task_options)
    if is_err(opts_r):
        return Err(BrokerInvalidTaskOptions(
            message=f'task_options JSON corrupt: {opts_r.err_value}',
            exception=opts_r.err_value,
        ))
    if options_integer_out_of_range(opts_r.ok_value):
        return Err(BrokerInvalidTaskOptions(
            message=OPTIONS_INTEGER_OUT_OF_RANGE,
            exception=NoCause(),
        ))
    retry_limit_r = stored_max_retries(opts_r.ok_value)
    if is_err(retry_limit_r):
        detail = retry_limit_r.err_value.detail
        return Err(BrokerInvalidTaskOptions(
            message=f'task_options refused for {task_name}: {detail}',
            exception=NoCause(),
        ))
    execution_r = stored_execution(opts_r.ok_value)
    if is_err(execution_r):
        detail = execution_r.err_value.detail
        return Err(BrokerInvalidTaskOptions(
            message=f'task_options refused for {task_name}: {detail}',
            exception=NoCause(),
        ))
    return Ok(_EnqueueOptions(
        max_retries=retry_limit_r.ok_value,
        execution=execution_r.ok_value,
    ))


def _parse_enqueue_json(
    label: str, task_name: str, text: str,
) -> Result[Json, BrokerInvalidInput]:
    """Parse one JSON input text.

    A text that does not read is refused, an empty text included
    (``EmptyJsonTextError``).
    """
    loaded = loads_json(text)
    if is_err(loaded):
        return Err(BrokerInvalidInput(
            message=f'{label} JSON corrupt for {task_name}: {loaded.err_value}',
            exception=loaded.err_value,
        ))
    return loaded


EMPTY_TASK_INPUT: Final = '{}'
"""The ``input`` of a send with no keyword arguments."""


def _parse_enqueue_input(
    task_name: str, input_json: str | None,
) -> Result[str, BrokerInvalidInput]:
    """Check the input text and give the ``input`` text that the row stores.

    A Python send's input is its keyword arguments as one JSON object. An
    object is stored exactly as sent, because the command fingerprint
    covers its bytes. No input and JSON null store ``EMPTY_TASK_INPUT``.
    Anything else is refused; the refusal text names the JSON type, not the
    value.
    """
    if input_json is None:
        return Ok(EMPTY_TASK_INPUT)
    parsed = _parse_enqueue_json('kwargs', task_name, input_json)
    if is_err(parsed):
        return parsed
    match parsed.ok_value:
        case None:
            return Ok(EMPTY_TASK_INPUT)
        case dict():
            return Ok(input_json)
        case other:
            return Err(BrokerInvalidInput(
                message=(
                    f'kwargs JSON for {task_name} must be an object or null, '
                    f'got {type(other).__name__}'
                ),
                exception=NoCause(),
            ))


def _enqueue_start(
    enqueued_at: datetime | None,
    enqueue_delay_seconds: int | None,
) -> EnqueueStart:
    """When the row is due: at ``enqueued_at``, after the delay, or now.

    ``_check_enqueue_timing`` refuses both together first; both here is a
    caller defect (``AssertionError``). Pure.
    """
    match (enqueued_at, enqueue_delay_seconds):
        case (None, None):
            return StartNow()
        case (datetime() as at, None):
            return StartAt(at=at)
        case (None, int() as delay_s):
            return StartAfterDelay(delay_s=delay_s)
        case _:
            raise AssertionError(
                'the timing check refuses enqueued_at with enqueue_delay_seconds',
            )


def _enqueue_key(
    task_name: str,
    idempotency_key: str | None,
    key_window: timedelta,
) -> Result[EnqueueKey, BrokerInvalidInput]:
    """The key of a send: none, or the digest of the key scoped to the task
    name with its reservation window, exact to the microsecond. A key that the
    scope refuses is ``INVALID_INPUT``. Pure."""
    if idempotency_key is None:
        return Ok(Unkeyed())
    from horsies.core.history.identity.keys import (
        IDEMPOTENCY_SCOPE_VERSION,
        ScopedIdempotencyKey,
    )

    try:
        digest = ScopedIdempotencyKey(task_name=task_name, key=idempotency_key).digest
    except ValueError as exc:
        return Err(BrokerInvalidInput(
            message=f'idempotency key refused for {task_name}: {exc}',
            exception=exc,
        ))
    return Ok(Keyed(
        digest=digest,
        scope_version=IDEMPOTENCY_SCOPE_VERSION,
        window=key_window,
    ))


def _monitoring_call_failed(
    operation: str, exc: CoreCallRefusal,
) -> Err[BrokerOperationFailed]:
    """A core boundary error of a monitoring read (the core did not start,
    the loop has too many calls in flight, or a sync call reached its
    bound): no statement ran, or the read was dropped. Nothing changed."""
    return Err(
        BrokerOperationFailed(
            code=BrokerErrorCode.MONITORING_QUERY_FAILED,
            message=f'{operation} failed: {exc}',
            retryable=core_error_retryable(exc),
            exception=exc,
        ),
    )


def _database_ping_call_failed(exc: CoreCallRefusal) -> Err[BrokerOperationFailed]:
    """The ping's error for a core boundary error: ``DB_PING_FAILED`` with
    the error, retryable by ``core_error_retryable``. Nothing changed."""
    return Err(
        BrokerOperationFailed(
            code=BrokerErrorCode.DB_PING_FAILED,
            message=f'ping_database failed: {exc}',
            retryable=core_error_retryable(exc),
            exception=exc,
        ),
    )


def _database_ping_answer(
    outcome: DatabasePingRecord | BrokerOperationFailed,
) -> BrokerResult[DatabasePing]:
    """The answer of the core's database ping. Pure.

    - ``DatabasePingRecord``: ``Ok(DatabasePing)`` with its latency.
    - ``BrokerOperationFailed`` with ``DB_PING_FAILED``: the core's class as
      it is, with Python's message (``ping_database failed: {error}``) and
      the core's exception of the cause.

    Another operation code contradicts the ping's catalog: ``AssertionError``.
    """
    match outcome:
        case DatabasePingRecord(latency_ms=latency_ms):
            return Ok(DatabasePing(latency_ms=latency_ms))
        case BrokerOperationFailed(code=BrokerErrorCode.DB_PING_FAILED):
            return Err(outcome)
        case BrokerOperationFailed(code=code):
            raise AssertionError(f'the core database ping answered the code {code}')
        case _ as unreachable:
            assert_never(unreachable)


def _worker_ping_call_failed(exc: CoreCallRefusal) -> Err[BrokerOperationFailed]:
    """The ping's error for a core boundary error: ``WORKER_PING_FAILED`` with
    the error, retryable by ``core_error_retryable``. The NOTIFY may have been
    sent."""
    return Err(
        BrokerOperationFailed(
            code=BrokerErrorCode.WORKER_PING_FAILED,
            message=f'ping_workers failed: {exc}',
            retryable=core_error_retryable(exc),
            exception=exc,
        ),
    )


def _worker_ping_answer(
    outcome: (
        WorkerPongsRecord
        | BrokerOperationFailed
        | BrokerInvalidInput
        | BrokerListenerDisabled
    ),
) -> BrokerResult[list[WorkerPong]]:
    """The answer of the core's worker ping. Pure.

    - ``WorkerPongsRecord``: one ``WorkerPong`` per reply, in reply order.
    - The core's class as it is: ``BrokerInvalidInput`` (``NoCause()``),
      ``BrokerListenerDisabled``, or ``BrokerOperationFailed`` with
      ``WORKER_PING_FAILED`` and the core's exception of the cause.

    Another operation code contradicts the ping's catalog: ``AssertionError``.
    """
    match outcome:
        case WorkerPongsRecord(pongs=pongs):
            return Ok(
                [
                    WorkerPong(
                        worker_id=pong.worker_id,
                        hostname=pong.hostname,
                        pid=pong.pid,
                        round_trip_ms=pong.round_trip_ms,
                    )
                    for pong in pongs
                ],
            )
        case BrokerInvalidInput() | BrokerListenerDisabled():
            return Err(outcome)
        case BrokerOperationFailed(code=BrokerErrorCode.WORKER_PING_FAILED):
            return Err(outcome)
        case BrokerOperationFailed(code=code):
            raise AssertionError(f'the core worker ping answered the code {code}')
        case _ as unreachable:
            assert_never(unreachable)


def _worker_states_answer(
    outcome: WorkerStatesRecord | BrokerOperationFailed | BrokerInvalidInput,
) -> BrokerResult[list[WorkerStateSnapshot]]:
    """The answer of a read of worker state snapshots (the latest per worker,
    or the history of one worker). A failure is the core's class as it is:
    ``BrokerInvalidInput`` for a refused history limit (nothing ran), or
    ``BrokerOperationFailed`` with ``MONITORING_QUERY_FAILED``. Pure."""
    match outcome:
        case WorkerStatesRecord(states=states):
            return Ok([worker_state_snapshot(state) for state in states])
        case BrokerInvalidInput():
            return Err(outcome)
        case BrokerOperationFailed(code=BrokerErrorCode.MONITORING_QUERY_FAILED):
            return Err(outcome)
        case BrokerOperationFailed(code=code):
            raise AssertionError(f'the core worker state read answered the code {code}')
        case _ as unreachable:
            assert_never(unreachable)


def _worker_state_answer(
    outcome: WorkerStateRecord | WorkerStateAbsent | BrokerOperationFailed,
) -> BrokerResult[WorkerStateSnapshot | None]:
    """The answer of the read of one worker's latest snapshot: ``Ok(None)``
    for a worker with no snapshot; a failure is the core's class as it is.
    Pure."""
    match outcome:
        case WorkerStateRecord() as state:
            return Ok(worker_state_snapshot(state))
        case WorkerStateAbsent():
            return Ok(None)
        case BrokerOperationFailed(code=BrokerErrorCode.MONITORING_QUERY_FAILED):
            return Err(outcome)
        case BrokerOperationFailed(code=code):
            raise AssertionError(f'the core monitoring read answered the code {code}')
        case _ as unreachable:
            assert_never(unreachable)


def _class_coverage_answer(
    outcome: ClassCoverageRecord | BrokerOperationFailed,
) -> BrokerResult[ClassCoverage]:
    """The answer of the fleet check; a failure is the core's class as it
    is. Pure."""
    match outcome:
        case ClassCoverageRecord() as coverage:
            return Ok(class_coverage_of(coverage))
        case BrokerOperationFailed(code=BrokerErrorCode.MONITORING_QUERY_FAILED):
            return Err(outcome)
        case BrokerOperationFailed(code=code):
            raise AssertionError(f'the core monitoring read answered the code {code}')
        case _ as unreachable:
            assert_never(unreachable)


def _class_report_answer(
    outcome: ClassReportRecord | BrokerOperationFailed,
) -> BrokerResult[ClassReport]:
    """The answer of the class report; a failure is the core's class as it
    is. Pure."""
    match outcome:
        case ClassReportRecord() as report:
            return Ok(class_report_of(report))
        case BrokerOperationFailed(code=BrokerErrorCode.MONITORING_QUERY_FAILED):
            return Err(outcome)
        case BrokerOperationFailed(code=code):
            raise AssertionError(f'the core monitoring read answered the code {code}')
        case _ as unreachable:
            assert_never(unreachable)


def _close_answer(errors: list[BaseException]) -> BrokerResult[None]:
    """The answer of a close for the failures of its steps. Pure.

    No failure: ``Ok(None)``. Otherwise ``CLOSE_FAILED`` with the count and
    the first failure, retryable by ``is_retryable_connection_error``.
    """
    match errors:
        case [first, *_]:
            return _operation_failed(
                BrokerErrorCode.CLOSE_FAILED,
                f'Close failed ({len(errors)} error(s)): {first}',
                first,
            )
        case _:
            return Ok(None)


def _async_context_err(
    sync_call: str, async_call: str,
) -> Err[BrokerAsyncContext]:
    """Build the broker error for a sync facade called inside a running event loop.

    Nothing ran, and the sync runner was not started.
    """
    return Err(
        BrokerAsyncContext(
            message=async_context_message(sync_call, async_call),
        )
    )


def _wait_inside_task_err(
    operation: str, task_id: UUID,
) -> Err[BrokerWaitInsideTask]:
    """Build the broker error for a result wait in a process that runs task bodies.

    Nothing ran, and nothing waited.
    """
    return Err(
        BrokerWaitInsideTask(
            message=wait_inside_task_message(f'{operation} for task {task_id}'),
        )
    )


def _cancel_requested() -> bool:
    """Whether the running task has a pending cancellation request."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


_SCHEMA_LOCK_POLL_INTERVAL_SECONDS = 0.025


async def _acquire_session_advisory_lock(
    connection: psycopg.AsyncConnection[Any],
    key: int,
) -> None:
    """Take a session advisory lock without a statement that waits for it.

    A blocked ``pg_advisory_lock`` statement holds a snapshot, and a
    ``CREATE INDEX CONCURRENTLY`` that the lock holder runs on another
    connection waits for that snapshot: the cycle passes through the
    holder's client process, so PostgreSQL does not detect it.
    Each ``pg_try_advisory_lock`` ends its statement before the next poll.
    The wait has no bound, as with the blocking form: it ends when the
    holder releases the lock or its session ends.
    """
    while True:
        cursor = await connection.execute(
            'SELECT pg_try_advisory_lock(%s)',
            (key,),
        )
        row = await cursor.fetchone()
        assert row is not None, 'pg_try_advisory_lock returns one row'
        if bool(row[0]):
            return
        await asyncio.sleep(_SCHEMA_LOCK_POLL_INTERVAL_SECONDS)

# ---- Monitoring queries ----


# ---- Cleanup queries ----


SIGNED_64_BIT_MIN: Final = -(2**63)
SIGNED_64_BIT_MAX: Final = 2**63 - 1
"""The range of an integer argument of a core operation. A value outside it
raises ``OverflowError`` at the call, so a facade refuses it first."""

type CorePoolProfile = Literal['PRODUCER', 'WORKER']
"""The pool profile of a broker's core handle. ``PRODUCER``: no reserve.
``WORKER``: the worker process; batch checkouts leave connections of the main
core pool free for the per-task writes."""

class SyncSendRoute(Enum):
    """Where a sync ``enqueue`` writes its row."""

    CALLER_THREAD = 'caller_thread'
    """On the calling thread, through the broker's sync send engine."""
    LOOP_RUNNER = 'loop_runner'
    """On the sync runner's event loop, through ``enqueue_async``."""


@dataclass(frozen=True, slots=True)
class _SyncSendEngineNotOpen:
    """No sync send ran yet, or the broker closed the engine."""


@dataclass(frozen=True, slots=True)
class _SyncSendEngineOpen:
    engine: Engine


@dataclass(frozen=True, slots=True)
class _PythonResources:
    """The Python-side resources that one close took from the broker: the
    resource sets of the event loops, and the engine of the sync sends that
    ran on the caller's thread."""

    loop_sets: tuple[LoopResources, ...]
    sync_send_engine: _SyncSendEngineNotOpen | _SyncSendEngineOpen


def _task_info_core_error(
    task_id: UUID, exc: CoreCallRefusal,
) -> Err[BrokerOperationFailed]:
    """The core of this process cannot run a task info read: it did not
    start, has too many calls in flight, or a blocking call reached its
    bound. Nothing changed. Pure."""
    return Err(
        BrokerOperationFailed(
            code=BrokerErrorCode.TASK_INFO_QUERY_FAILED,
            message=f'get_task_info failed for {task_id}: {exc}',
            retryable=core_error_retryable(exc),
            exception=exc,
        ),
    )


def _strict_result_envelope(
    task_id: UUID, result_json: str,
) -> Result[dict[str, Json] | None, BrokerInvalidJsonPayload]:
    """Read a stored result envelope with the strict JSON reader: a JSON
    object, or ``None`` for JSON null. Pure.

    A text that does not read (malformed, a non-finite number) or a value
    that is neither an object nor null is ``INVALID_JSON_PAYLOAD``, as the
    Python read of a stored result before the switch. An empty text does not
    read either (``EmptyJsonTextError``): a task with no result stores NULL,
    which the caller reads before this call.
    """
    loaded = loads_json(result_json)
    if is_err(loaded):
        return Err(BrokerInvalidJsonPayload(
            message=f'Result JSON parse failed for task {task_id}: {loaded.err_value}',
            exception=loaded.err_value,
        ))
    match loaded.ok_value:
        case dict() as envelope:
            return Ok(envelope)
        case None:
            return Ok(None)
        case other:
            return Err(BrokerInvalidJsonPayload(
                message=(
                    f'Result for task {task_id} is not a JSON object; '
                    f'got {type(other).__name__}'
                ),
                exception=NoCause(),
            ))


def _task_attempt_info(record: TaskAttemptRecord) -> TaskAttemptInfo:
    """The ``TaskAttemptInfo`` of one core attempt record. Pure."""
    return TaskAttemptInfo(
        task_id=record.task_id,
        attempt=record.attempt,
        outcome=TaskAttemptOutcome(record.outcome),
        will_retry=record.will_retry,
        started_at=record.started_at,
        finished_at=record.finished_at,
        error_code=record.error_code,
        error_message=record.error_message,
        failed_reason=record.failed_reason,
        worker_id=record.worker_id,
        worker_hostname=record.worker_hostname,
        worker_pid=record.worker_pid,
        worker_process_name=record.worker_process_name,
    )


def _task_info_of_record(record: TaskInfoRecord) -> BrokerResult[TaskInfo]:
    """The ``TaskInfo`` of one core task info record. Pure.

    ``decoded_result`` stays ``None``: the typed decode needs the task's
    type, which the caller knows. A stored result that does not read is
    ``INVALID_JSON_PAYLOAD``.
    """
    raw_result: dict[str, Json] | None
    match record.result_json:
        case None:
            raw_result = None
        case str() as result_json:
            match _strict_result_envelope(record.task_id, result_json):
                case Ok(envelope):
                    raw_result = envelope
                case Err() as refused:
                    return refused
    return Ok(TaskInfo(
        task_id=record.task_id,
        task_name=record.task_name,
        status=TaskStatus(record.status),
        queue_name=record.queue_name,
        priority=record.priority,
        retry_count=record.retry_count,
        max_retries=record.max_retries,
        next_retry_at=record.next_retry_at,
        sent_at=record.sent_at,
        enqueued_at=record.enqueued_at,
        claimed_at=record.claimed_at,
        started_at=record.started_at,
        completed_at=record.completed_at,
        failed_at=record.failed_at,
        worker_hostname=record.worker_hostname,
        worker_pid=record.worker_pid,
        worker_process_name=record.worker_process_name,
        error_code=record.error_code,
        raw_result=raw_result,
        decoded_result=None,
        result_decoded=False,
        failed_reason=record.failed_reason,
        attempts=(
            None
            if record.attempts is None
            else [_task_attempt_info(attempt) for attempt in record.attempts]
        ),
        execution=None if record.execution is None else Execution(record.execution),
    ))


def _task_info_result(
    outcome: (
        TaskInfoRecord | TaskInfoNotFound | BrokerOperationFailed | BrokerInvalidJsonPayload
    ),
) -> BrokerResult[TaskInfo | None]:
    """The result of one core task info read. Pure.

    - ``TaskInfoRecord``: ``Ok(TaskInfo)``, or ``INVALID_JSON_PAYLOAD`` for a
      stored result that does not read.
    - ``TaskInfoNotFound``: ``Ok(None)``.
    - The core's class as it is, with Python's message
      (``get_task_info failed for {task_id}: {error}``):
      ``BrokerOperationFailed`` with ``TASK_INFO_QUERY_FAILED`` (the connect,
      the install or a read failed; ``retryable`` is the core's
      classification), or ``BrokerInvalidJsonPayload`` (stored data that
      does not read, for example a history result whose digest differs; not
      retryable).

    Each failure's ``exception`` is the core's exception of the cause, or
    ``NoCause()`` for a check of the core. Another operation code contradicts
    the read's catalog: ``AssertionError``.
    """
    match outcome:
        case TaskInfoRecord() as record:
            return _task_info_of_record(record)
        case TaskInfoNotFound():
            return Ok(None)
        case BrokerInvalidJsonPayload():
            return Err(outcome)
        case BrokerOperationFailed(code=BrokerErrorCode.TASK_INFO_QUERY_FAILED):
            return Err(outcome)
        case BrokerOperationFailed(code=code):
            raise AssertionError(f'the core task info read answered the code {code}')
        case _ as unreachable:
            assert_never(unreachable)


SIGNED_32_BIT_MIN: Final = -(2**31)
SIGNED_32_BIT_MAX: Final = 2**31 - 1
"""The range of a 32-bit integer argument of a core operation (a priority)."""


type CoreEnqueueStart = CoreStartNow | CoreStartAfterDelay | CoreStartAt
type CoreSendOutcome = (
    Enqueued
    | SendSuppressed
    | SendAsyncContext
    | SendValidationFailed
    | SendEnqueueFailed
    | SendPayloadMismatch
    | SendPayloadTooLarge
    | SendKeyConflict
)
type CoreRefusalCause = (
    SendRefusalUnreadableJson | SendRefusalByRule | SendRefusalByValueRule
)
type RerunInputPolicy = Literal['APP_DEFAULT', 'KEEP', 'DROP']


def _invalid_input(message: str) -> Err[BrokerInvalidInput]:
    return Err(BrokerInvalidInput(message=message, exception=NoCause()))


def _command_rule_refused(task_name: str, rule: str) -> Err[BrokerInvalidInput]:
    """A rule of the version-1 command refused a value that the core's
    request cannot hold: ``INVALID_INPUT`` with the rule as a ``ValueError``,
    the refusal of Python's ``EnqueueCommandV1``. The core answers a value
    that its request holds with the same message and cause
    (``SendRefusalByValueRule``). Pure."""
    return Err(BrokerInvalidInput(
        message=f'enqueue input refused for {task_name}: {rule}',
        exception=ValueError(rule),
    ))


def _enqueue_start_of(
    *,
    task_name: str,
    sent_at: datetime,
    enqueued_at: datetime | None,
    enqueue_delay_seconds: int | None,
    good_until: datetime | None,
) -> Result[CoreEnqueueStart, BrokerInvalidInput]:
    """The start of a send, after the rules that the core's request cannot
    state: one of ``enqueued_at`` and ``enqueue_delay_seconds``, aware
    instants, and a delay that fits in 64 bits. Pure but for the
    construction of the start value; nothing is written on a refusal.

    A naive ``good_until`` and a delay below the 64-bit range break a rule of
    the version-1 command (``_command_rule_refused``); the other refusals
    have no cause. The core refuses a ``sent_at`` in the future of a send
    that starts now.
    """
    if enqueued_at is not None and enqueue_delay_seconds is not None:
        return _invalid_input('Cannot specify both enqueued_at and enqueue_delay_seconds')
    for label, instant in (('sent_at', sent_at), ('enqueued_at', enqueued_at)):
        if instant is not None and instant.tzinfo is None:
            return _invalid_input(f'{label} must be timezone-aware')
    if good_until is not None and good_until.tzinfo is None:
        return _command_rule_refused(task_name, 'good_until must be timezone-aware')
    match (enqueued_at, enqueue_delay_seconds):
        case (None, None):
            return Ok(CoreStartNow())
        case (datetime() as at, None):
            return Ok(CoreStartAt(at))
        case (None, int() as delay_s) if SIGNED_64_BIT_MIN <= delay_s <= SIGNED_64_BIT_MAX:
            return Ok(CoreStartAfterDelay(delay_s))
        case (None, int() as delay_s) if delay_s < SIGNED_64_BIT_MIN:
            return _command_rule_refused(
                task_name, 'enqueue_delay_seconds must be non-negative',
            )
        case (None, int() as delay_s):
            return _invalid_input(
                f'enqueue_delay_seconds {delay_s} is not a 64-bit integer',
            )
        case _:
            raise AssertionError('both start values are refused above')


def _check_key_encodes(
    task_name: str, idempotency_key: str | None,
) -> Result[None, BrokerInvalidInput]:
    """Refuse an idempotency key that UTF-8 cannot encode (a lone surrogate)
    before the core's request reads it as UTF-8 text. The refusal is the one
    of Python's ``ScopedIdempotencyKey``: ``INVALID_INPUT`` with the
    ``UnicodeEncodeError`` as the cause. The core checks the other rules of
    the key. Pure."""
    match idempotency_key:
        case None:
            return Ok(None)
        case str() as key:
            try:
                _ = key.encode('utf-8')
            except UnicodeEncodeError as exc:
                return Err(BrokerInvalidInput(
                    message=f'idempotency key refused for {task_name}: {exc}',
                    exception=exc,
                ))
            return Ok(None)
        case _ as unreachable:
            assert_never(unreachable)


def _rerun_input_policy(retain_rerun_input: bool | None) -> RerunInputPolicy:
    """The rerun-input policy of a send: the config's default, keep or drop. Pure."""
    match retain_rerun_input:
        case None:
            return 'APP_DEFAULT'
        case True:
            return 'KEEP'
        case False:
            return 'DROP'


def _enqueue_request(
    *,
    task_id: UUID,
    task_name: str,
    queue_name: str,
    priority: int,
    input_json: str | None,
    sent_at: datetime,
    enqueued_at: datetime | None,
    enqueue_delay_seconds: int | None,
    good_until: datetime | None,
    task_options: str | None,
    retention_class_key: str,
    retain_rerun_input: bool | None,
    idempotency_key: str | None,
    send_kind: SendKind,
) -> Result[EnqueueRequest, BrokerInvalidInput]:
    """The core's request of one send, after the client rules: the start
    (``_enqueue_start_of``), the input shape of a Python send
    (``_parse_enqueue_input``: an object, or ``{}`` for none or JSON null),
    a priority that fits in 32 bits (outside it, the command's priority
    rule refuses it) and an idempotency key that UTF-8 encodes
    (``_check_key_encodes``). Each refusal is ``INVALID_INPUT`` and writes
    nothing.

    The core checks the rest at the send: the strict JSON rule of the input
    and the options, the retry limit and the class of the options, the
    command fingerprint, the scope of the idempotency key, the retention
    class key, and a ``sent_at`` in the future of a send that starts now.
    """
    match _enqueue_start_of(
        task_name=task_name,
        sent_at=sent_at,
        enqueued_at=enqueued_at,
        enqueue_delay_seconds=enqueue_delay_seconds,
        good_until=good_until,
    ):
        case Err() as refused:
            return refused
        case Ok(ok_value=start):
            pass
    match _parse_enqueue_input(task_name, input_json):
        case Err() as refused:
            return refused
        case Ok(ok_value=stored_input):
            pass
    if not SIGNED_32_BIT_MIN <= priority <= SIGNED_32_BIT_MAX:
        return _command_rule_refused(task_name, 'priority must be between 1 and 100')
    match _check_key_encodes(task_name, idempotency_key):
        case Err() as refused:
            return refused
        case Ok():
            pass
    return Ok(EnqueueRequest(
        task_id=task_id,
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
        input_json=stored_input,
        sent_at=sent_at,
        start=start,
        good_until=good_until,
        task_options_json=task_options,
        idempotency_key=idempotency_key,
        retention_class_key=retention_class_key,
        rerun_input=_rerun_input_policy(retain_rerun_input),
        send_kind=send_kind.value,
    ))


def _enqueue_core_error(
    task_name: str, exc: CoreCallRefusal,
) -> Err[BrokerEnqueueFailed]:
    """The core of this process cannot run a send: it did not start, has
    too many calls in flight, or a blocking call reached its bound. The row
    may or may not exist after a timed-out call. Pure."""
    return Err(BrokerEnqueueFailed(
        message=f'Failed to enqueue task {task_name}: {exc}',
        retryable=core_error_retryable(exc),
        exception=exc,
    ))


def _refusal_cause(cause: CoreRefusalCause) -> SerializationError | ValueError | NoCause:
    """The exception of a core refusal, as Python's broker gave it: the JSON
    reader's error as a ``SerializationError``, the rule of the version-1
    command or of the scoped idempotency key as a ``ValueError``, and
    ``NoCause()`` when another rule refused a value that reads. Pure."""
    match cause:
        case SendRefusalUnreadableJson(reader_error=reader_error):
            return SerializationError(reader_error)
        case SendRefusalByValueRule(rule=rule):
            return ValueError(rule)
        case SendRefusalByRule():
            return NoCause()
        case _ as unreachable:
            assert_never(unreachable)


def _enqueue_result(outcome: CoreSendOutcome) -> Result[UUID, BrokerEnqueueError]:
    """The result of one core send. Pure.

    - ``Enqueued``: ``Ok`` with the sent id, or the id that holds the key
      for a key replay.
    - ``SendValidationFailed``: ``INVALID_INPUT`` or
      ``INVALID_TASK_OPTIONS``, with the cause of ``_refusal_cause``.
    - ``SendEnqueueFailed``: ``ENQUEUE_FAILED`` with the core's exception of
      the cause.
    - ``SendPayloadMismatch``: ``PAYLOAD_MISMATCH``.
    - ``SendKeyConflict``: ``IDEMPOTENCY_KEY_CONFLICT`` with the owner.

    The core's send never answers ``SendSuppressed``, ``SendAsyncContext``
    or ``SendPayloadTooLarge`` (the client checks each before the send):
    ``AssertionError``.
    """
    match outcome:
        case Enqueued(task_id=task_id):
            return Ok(task_id)
        case SendValidationFailed(
            code='INVALID_INPUT', message=message, cause=cause,
        ):
            return Err(BrokerInvalidInput(
                message=message, exception=_refusal_cause(cause),
            ))
        case SendValidationFailed(
            code='INVALID_TASK_OPTIONS', message=message, cause=cause,
        ):
            return Err(BrokerInvalidTaskOptions(
                message=message, exception=_refusal_cause(cause),
            ))
        case SendValidationFailed(code=code):
            raise AssertionError(f'the core send refused with the code {code}')
        case SendEnqueueFailed(
            message=message, retryable=retryable, exception=exception,
        ):
            return Err(BrokerEnqueueFailed(
                message=message,
                retryable=retryable,
                exception=exception,
            ))
        case SendPayloadMismatch(message=message):
            return Err(BrokerPayloadMismatch(message=message))
        case SendKeyConflict(message=message, owner_task_id=owner_task_id):
            return Err(BrokerIdempotencyKeyConflict(
                message=message, owner_task_id=owner_task_id,
            ))
        case SendSuppressed() | SendAsyncContext() | SendPayloadTooLarge():
            raise AssertionError(
                f'the core send answered {type(outcome).__name__}, which the '
                'client checks before the send',
            )
        case _ as unreachable:
            assert_never(unreachable)


# Most open event loops, other than the broker's sync runner loop and a worker
# child's task loop, that get their own resource set. pytest-asyncio keeps one event loop open per
# fixture scope (session, package, module, class, function), so one test
# run can hold five open loops on one thread. A loop beyond the limit gets
# EVENT_LOOP_LIMIT instead of one more connection pool.
MAX_CALLER_EVENT_LOOPS = 5

# Bound on the release of a resource set whose event loop closed. The
# release waits for no other party: it drops the closed loop's tasks without
# awaiting them, and closes at most pool_size + max_overflow idle
# connections per engine plus 2 listener connections. Each close is local
# work: psycopg AsyncConnection.close() calls PQfinish, which sends
# Terminate and returns. The bound fails closed on a close that is stuck.
DEAD_LOOP_RELEASE_TIMEOUT = timedelta(seconds=5)

# Bound on a close handed off to the open event loop that owns a resource
# set. It covers the delay before a busy owner loop runs the close, plus the
# cancel-and-await of the listener's dispatcher and health tasks. The
# connection closes inside it are local work, as above.
CLOSE_HANDOFF_TIMEOUT = timedelta(seconds=5)


class PostgresBroker:
    """
    PostgreSQL-based task broker with LISTEN/NOTIFY for real-time updates.

    Provides both async and sync APIs:
      - Async: enqueue_async(), get_raw_result_record_async()
      - Sync: enqueue(), get_raw_result_record() (blocking forms of the core,
        on the calling thread)

    Features:
      - Real-time notifications via PostgreSQL triggers
      - Automatic task status tracking
      - Connection pooling and health monitoring
      - Operational monitoring (stale tasks, worker stats)

    ``retention`` is the retention config of the app that sends through the
    broker (``Horsies.get_broker`` passes ``AppConfig.retention``). The
    schema install ends with the startup coverage pass of its classes and
    horizons; with ``None`` the pass covers the classes of the default
    ``RetentionConfig``.

    Every method that calls the core raises ``ForkedAfterCoreStart`` and
    ``CoreStopped``: they are states of the process, never a result
    (``call_refusal_or_raise``). The other core boundary errors
    (``CoreCallRefusal``) are the failure code of the method.
    """

    _CORE_POOL_PROFILE: ClassVar[CorePoolProfile] = 'PRODUCER'
    """The pool profile of the broker's core handle."""

    def __init__(
        self,
        config: PostgresConfig,
        *,
        assume_initialized: bool = False,
        run_schema_migrations: bool = True,
        idempotency_reservation_window: Optional[timedelta] = None,
        retention: RetentionConfig | None = None,
    ):
        self.config = config
        # The keyed-enqueue reservation window, snapshotted onto every
        # keyed request at enqueue. Broker-configured by contract:
        # default 24 hours, accepted range greater than zero through the
        # inclusive 30-day maximum; validation is typed and happens
        # here, at construction.
        from horsies.core.history.identity.keys import (
            IDEMPOTENCY_WINDOW_DEFAULT,
            validate_reservation_window,
        )

        self.idempotency_reservation_window = validate_reservation_window(
            idempotency_reservation_window
            if idempotency_reservation_window is not None
            else IDEMPOTENCY_WINDOW_DEFAULT
        )
        # False makes this broker incapable of executing DDL. Read-only
        # tooling that constructs its own broker sets it so that pointing
        # the tool at a database can never migrate that database. Owning
        # the schema stays the app's and the worker's job.
        self.run_schema_migrations = run_schema_migrations
        self.logger = get_logger('broker')
        self._app: Horsies | None = None  # Set by Horsies.get_broker()

        self._assume_initialized = assume_initialized
        # One resource set per open event loop (see select_resources). The
        # tuple is immutable: each change replaces it under the lock.
        self._loop_sets: tuple[LoopResources, ...] = ()
        self._loop_sets_lock = threading.Lock()

        self._initialized = assume_initialized
        # The engine of the sync sends on the caller's thread; close()
        # disposes it when one is open.
        self._sync_send_engine: _SyncSendEngineNotOpen | _SyncSendEngineOpen = (
            _SyncSendEngineNotOpen()
        )
        self._sync_send_lock = threading.Lock()
        # True once this broker saw the staged result function. Only a
        # positive answer is kept: until a worker publishes the readers,
        # each result probe checks again.
        self._staged_result_reader_published = False
        self._loop_runner = LoopRunner()  # for run_sync
        # The operations that run in the core use the pools of this handle.
        # It is built from the same config as the engines, so its pools have
        # their shape. Building it starts nothing: the first core operation
        # connects, and close_async closes its pools.
        # The schema install of the handle ends with the coverage pass of the
        # app's retention: the leaves of every class its sends can stamp.
        match retention:
            case None:
                install_retention = RetentionConfig()
            case RetentionConfig():
                install_retention = retention
        # The retention config whose classes the install covers.
        self._install_retention: RetentionConfig = install_retention
        self._core_handle = CoreHandle(
            dump_producer_manifest(
                to_producer_manifest(
                    config,
                    schema=schema_mode_of(
                        run_schema_migrations=run_schema_migrations,
                        assume_initialized=assume_initialized,
                    ),
                    idempotency_reservation_window=self.idempotency_reservation_window,
                ),
            ),
            pool_profile=self._CORE_POOL_PROFILE,
        ).with_install_coverage(install_coverage_of(install_retention))
        # The workflow operations of the core build each sub-workflow child
        # through this source, which runs the child's build_with.
        from horsies.core.workflows.child_spec_source import child_spec_source_of

        self._core_handle.register_child_spec_source(child_spec_source_of(self))

        # The core handle logs each statement on horsies.sql when echo is on;
        # show those records as SQLAlchemy's echo shows its own.
        match config.echo:
            case True:
                enable_statement_echo()
            case False:
                pass

        # Each worker child builds its own broker, so this fires on every
        # recycle (max_tasks_per_child); keep it at DEBUG in child processes.
        if os.getenv('HORSIES_CHILD_PROCESS') == '1':
            self.logger.debug('PostgresBroker initialized')
        else:
            self.logger.info('PostgresBroker initialized')

    def _build_resources(self) -> BrokerResources:
        """Build one set of loop-bound resources. No I/O: engines and the
        listener connect on first use.
        """
        engine = create_async_engine(
            sqlalchemy_url(self.config.database_url.get_secret_value()),
            **self._runtime_engine_config(),
        )
        register_identity_text_reads(engine)
        register_session_settings(engine, self._runtime_session_settings())
        self._register_pre_ping(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        session_url = self.config.effective_session_database_url
        session_engine: SessionEngine
        if session_url == self.config.database_url.get_secret_value():
            session_engine = SessionEngineShared()
            session_async_engine = engine
        else:
            session_async_engine = create_async_engine(
                sqlalchemy_url(session_url), **self._session_engine_config()
            )
            # The session URL must keep session state (LISTEN, session
            # advisory locks), so it keeps session settings too.
            register_session_settings(session_async_engine, SessionSettings.KEPT)
            self._register_pre_ping(session_async_engine)
            session_engine = SessionEngineSeparate(engine=session_async_engine)
        partition_maintenance = PartitionMaintenanceDatabase(
            session_async_engine,
            connection_capacity=self.config.pool_size + self.config.max_overflow,
        )

        listener: ListenerState
        match self._assume_initialized:
            case True:
                listener = ListenerDisabled()
            case False:
                listener = ListenerEnabled(
                    listener=PostgresListener(
                        session_url,
                        tcp_user_timeout_ms=self.config.tcp_user_timeout_ms,
                    ),
                )
        return BrokerResources(
            engine=engine,
            session_factory=session_factory,
            session_engine=session_engine,
            partition_maintenance=partition_maintenance,
            listener=listener,
        )

    async def select_resources(self) -> Result[BrokerResources, EventLoopLimit]:
        """Return the resource set for the running event loop.

        Call once at a public entry point and pass the value to helpers.
        Each open event loop gets its own set, built on its first call
        without I/O (engines and the listener connect on first use). The
        broker's sync runner loop, and in a worker child the task loop,
        always get a set. Other loops get one up to
        ``MAX_CALLER_EVENT_LOOPS``; a loop beyond that gets
        ``Err(EventLoopLimit)`` and nothing runs.

        Each call also releases the sets of loops that closed, each bounded
        by ``DEAD_LOOP_RELEASE_TIMEOUT``. A failed release is logged at
        WARNING and does not fail the call.
        """
        selected = self._resources_for_loop(
            asyncio.get_running_loop(), threading.current_thread().name,
        )
        closed_loop_sets = self._take_closed_loop_sets()
        if closed_loop_sets:
            await asyncio.gather(
                *(self._release_closed_loop_set(entry) for entry in closed_loop_sets),
            )
        return selected

    def _resources_for_loop(
        self, loop: asyncio.AbstractEventLoop, thread_name: str,
    ) -> Result[BrokerResources, EventLoopLimit]:
        """Find or build the set for ``loop``. No I/O; it never releases a set."""
        role = self._loop_role(loop)
        with self._loop_sets_lock:
            decision = decide_for_loop(
                self._loop_sets, loop, thread_name, role, MAX_CALLER_EVENT_LOOPS,
            )
            match decision:
                case UseExisting(resources=resources):
                    return Ok(resources)
                case LimitReached(loop_limit=loop_limit):
                    return Err(loop_limit)
                case BuildForLoop():
                    resources = self._build_resources()
                    entry = LoopResources(
                        loop=loop, thread_name=thread_name, role=role,
                        resources=resources,
                    )
                    self._loop_sets = (*self._loop_sets, entry)
                    return Ok(resources)

    def _loop_role(self, loop: asyncio.AbstractEventLoop) -> LoopRole:
        """The sync runner loop of this broker, a task loop, or a caller loop."""
        match (self._loop_runner.runs_on(loop), is_task_loop(loop)):
            case (True, _):
                return LoopRole.SYNC_RUNNER
            case (False, True):
                return LoopRole.TASK_LOOP
            case (False, False):
                return LoopRole.CALLER

    def _take_closed_loop_sets(self) -> tuple[LoopResources, ...]:
        """Remove and return the entries whose event loop is closed."""
        with self._loop_sets_lock:
            by_state = split_closed_loops(self._loop_sets)
            self._loop_sets = by_state.open_loops
        return by_state.closed_loops

    async def _release_closed_loop_set(self, entry: LoopResources) -> None:
        """Release the set of a closed event loop. Total: failures are logged."""
        for error in await self._release_with_bound(entry):
            self.logger.warning(
                'Release of the resource set of the closed event loop of '
                'thread %r failed: %s: %s',
                entry.thread_name, type(error).__name__, error,
            )

    async def _release_with_bound(self, entry: LoopResources) -> list[BaseException]:
        """Release a closed loop's set on the running loop, bounded by
        ``DEAD_LOOP_RELEASE_TIMEOUT``. Returns the failures; raises none.

        A connection that a task of the closed loop still held stays
        checked out; garbage collection closes it.
        """
        bound = DEAD_LOOP_RELEASE_TIMEOUT.total_seconds()
        try:
            return await asyncio.wait_for(
                self._release_resource_set(entry.resources), timeout=bound,
            )
        except TimeoutError:
            return [
                TimeoutError(
                    'release of the resource set of the closed event loop of '
                    f'thread {entry.thread_name!r} did not finish within {bound} s',
                ),
            ]

    async def _release_resource_set(
        self, resources: BrokerResources,
    ) -> list[BaseException]:
        """Release a set whose event loop is closed, on the running loop.

        Returns the failures; raises none.
        """
        errors: list[BaseException] = []
        match resources.listener:
            case ListenerEnabled(listener=listener):
                try:
                    await listener.release_after_owner_loop_closed()
                except Exception as exc:
                    errors.append(exc)
            case ListenerDisabled():
                pass
        errors.extend(await self._dispose_engines(resources))
        return errors

    async def _close_resource_set(
        self, resources: BrokerResources,
    ) -> list[BaseException]:
        """Close a set on its own event loop. Returns the failures; raises none."""
        errors: list[BaseException] = []
        match resources.listener:
            case ListenerEnabled(listener=listener):
                try:
                    await listener.close()
                except Exception as exc:
                    errors.append(exc)
            case ListenerDisabled():
                pass
        errors.extend(await self._dispose_engines(resources))
        return errors

    async def _dispose_engines(self, resources: BrokerResources) -> list[BaseException]:
        """Dispose the set's engines, which closes their idle connections.

        Returns the failures; raises none.
        """
        errors: list[BaseException] = []
        try:
            await resources.engine.dispose()
        except Exception as exc:
            errors.append(exc)
        match resources.session_engine:
            case SessionEngineSeparate(engine=session_engine):
                try:
                    await session_engine.dispose()
                except Exception as exc:
                    errors.append(exc)
            case SessionEngineShared():
                pass
        return errors

    async def _close_loop_set(
        self, entry: LoopResources, current_loop: asyncio.AbstractEventLoop,
    ) -> list[BaseException]:
        """Close one set by the rule for its loop. Returns the failures."""
        match entry.loop:
            case owner_loop if owner_loop is current_loop:
                return await self._close_resource_set(entry.resources)
            case owner_loop if owner_loop.is_closed():
                return await self._release_with_bound(entry)
            case _:
                return await self._hand_off_close(entry)

    async def _hand_off_close(self, entry: LoopResources) -> list[BaseException]:
        """Close a set on its own open event loop, from another loop.

        Waits at most ``CLOSE_HANDOFF_TIMEOUT``. On timeout the close stays
        scheduled on the owner loop and runs if that loop runs again; the
        timeout is returned as a failure that names the owner thread.
        Returns the failures; raises none except the caller's cancellation.
        """
        close_coroutine = self._close_resource_set(entry.resources)
        try:
            future = asyncio.run_coroutine_threadsafe(close_coroutine, entry.loop)
        except RuntimeError:
            # The owner loop closed after the close rule was chosen.
            close_coroutine.close()
            return await self._release_with_bound(entry)
        bound = CLOSE_HANDOFF_TIMEOUT.total_seconds()
        try:
            return await asyncio.wait_for(
                asyncio.shield(asyncio.wrap_future(future)), timeout=bound,
            )
        except TimeoutError:
            return [
                TimeoutError(
                    'close of the resource set of the event loop on thread '
                    f'{entry.thread_name!r} did not finish within {bound} s; '
                    'the close stays scheduled on that loop',
                ),
            ]
        except asyncio.CancelledError:
            if _cancel_requested():
                raise
            return [
                RuntimeError(
                    'close of the resource set of the event loop on thread '
                    f'{entry.thread_name!r} was cancelled on that loop',
                ),
            ]

    def run_sync[**P, T](
        self,
        coro_fn: Callable[P, Coroutine[Any, Any, T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Run ``coro_fn`` on this broker's sync runner and block until it returns.

        No sync facade of the package calls it: each one calls a blocking
        form of the core on the calling thread. It blocks the calling
        thread, so it must not run inside an event loop: a call from a thread
        that runs an event loop is a defect of the caller.

        Raises:
            AssertionError: the calling thread runs an event loop. Nothing
                ran, and the runner was not started.
            LoopRunnerError: the runner is stopped, its thread did not
                start, or scheduling onto its loop failed. Nothing ran.
            Exception: the coroutine's own exception, unchanged.
        """
        if in_running_event_loop():
            raise AssertionError(
                f'PostgresBroker.run_sync({coro_fn.__name__}) was called on '
                f'thread {threading.current_thread().name!r}, which runs an '
                'event loop; run_sync blocks its thread, so it must not run '
                'there',
            )
        return self._loop_runner.call(coro_fn, *args, **kwargs)

    def _runtime_session_settings(self) -> SessionSettings:
        """Whether runtime-pool connections keep session settings."""
        match self.config.pgbouncer_transaction_mode:
            case True:
                return SessionSettings.NOT_KEPT
            case False:
                return SessionSettings.KEPT

    def _base_engine_config(self) -> dict[str, Any]:
        return self.config.sqlalchemy_engine_kwargs()

    def _runtime_engine_config(self) -> dict[str, Any]:
        engine_cfg = self._base_engine_config()
        connect_args = self.config.pooled_connect_args
        if connect_args:
            # TCP keepalives, tcp_user_timeout, and the PgBouncer
            # prepared-statement knob, built from the typed fields on
            # PostgresConfig.
            engine_cfg['connect_args'] = connect_args
        return engine_cfg

    def _session_engine_config(self) -> dict[str, Any]:
        engine_cfg = self._base_engine_config()
        engine_cfg['connect_args'] = self.config.tcp_connect_args()
        return engine_cfg

    def create_single_connection_engine(self) -> AsyncEngine:
        """Create a private engine of at most one connection.

        It has the runtime engine's URL, options, and read hooks, and does
        not share the runtime pool. It opens its connection on first use.
        The caller owns the engine and disposes it.
        """
        engine_cfg = self._runtime_engine_config()
        engine_cfg['pool_size'] = 1
        engine_cfg['max_overflow'] = 0
        engine = create_async_engine(
            sqlalchemy_url(self.config.database_url.get_secret_value()),
            **engine_cfg,
        )
        register_identity_text_reads(engine)
        register_session_settings(engine, self._runtime_session_settings())
        self._register_pre_ping(engine)
        return engine

    def _register_pre_ping(self, engine: AsyncEngine | Engine) -> None:
        """With ``pool_pre_ping`` on, ping a pooled connection of ``engine``
        at checkout when it waited in the pool for longer than
        ``PRE_PING_AFTER_IDLE_S``. No I/O at registration."""
        match self.config.pool_pre_ping:
            case True:
                register_idle_ping(engine, PRE_PING_AFTER_IDLE_S)
            case False:
                pass

    @property
    def app(self) -> 'Horsies | None':
        """Get the attached Horsies app instance (if any)."""
        return self._app

    @app.setter
    def app(self, value: 'Horsies | None') -> None:
        """Set the Horsies app instance."""
        self._app = value

    def discard_inherited_resources(self) -> None:
        """Drop the pools a forked child inherited, without closing them.

        The parent process owns the inherited connections, so the child must
        not use or close them: each engine of each set is disposed with
        ``close=False``. Listener connections are dropped with the broker;
        psycopg closes a connection on garbage collection only in the
        process that opened it. It takes no lock: a lock that a parent
        thread held at fork time stays held in the child.
        """
        loop_sets = self._loop_sets
        self._loop_sets = ()
        for entry in loop_sets:
            entry.resources.engine.sync_engine.dispose(close=False)
            match entry.resources.session_engine:
                case SessionEngineSeparate(engine=session_engine):
                    session_engine.sync_engine.dispose(close=False)
                case SessionEngineShared():
                    pass

    def _schema_advisory_key(self) -> int:
        """
        Compute a stable 64-bit advisory lock key for schema initialization.

        PostgreSQL advisory locks are scoped to the current database, so this
        URL-independent key serializes Horsies schema initializers per database.
        """
        h = hashlib.sha256(b'horsies:schema:v1').digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    @contextlib.asynccontextmanager
    async def _schema_session_lock(
        self,
    ) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
        """Hold the schema lock on one direct physical connection, and yield it.

        The connection is in autocommit mode and holds the session advisory
        lock until the context ends. The wait for the lock has no bound.
        """
        connection = await psycopg.AsyncConnection.connect(
            self.config.effective_session_database_url,
            autocommit=True,
        )
        key = self._schema_advisory_key()
        held = False
        body_error: BaseException | None = None
        try:
            await _acquire_session_advisory_lock(connection, key)
            held = True
            yield connection
        except BaseException as exc:
            body_error = exc
            raise
        finally:
            unlock_failed = False
            if held:
                try:
                    cursor = await connection.execute(
                        'SELECT pg_advisory_unlock(%s)',
                        (key,),
                    )
                    row = await cursor.fetchone()
                    unlock_failed = row is None or not bool(row[0])
                except psycopg.Error:
                    unlock_failed = True
            await connection.close()
            if unlock_failed:
                message = 'schema advisory lock ownership was lost'
                if body_error is None:
                    raise RuntimeError(message)
                body_error.add_note(message)

    async def _ensure_initialized(self) -> None:
        """Install the schema once per broker through ``CoreHandle.install_schema_async``.

        The core installs the baseline into an empty database, or completes
        or checks an installed one, under the fleet schema lock: the
        migration with this client's family row, then the published history
        readers. The install ends with the startup coverage pass that the
        worker start runs, so the history leaves of the current UTC day and
        of the lookahead exist before any terminal write, with or without a
        worker. A handle with migrations off or ``assume_initialized`` runs
        no schema statement. After a success the broker does not call the
        core again; a broker built with ``assume_initialized=True`` never
        calls it.

        Raises:
            CoreFailure: the core answered ``BrokerOperationFailed`` with
                ``SCHEMA_INIT_FAILED``; the exception is the core's exception
                of the cause, with the
                core's classification as ``retryable`` where the cause may
                clear. The broker keeps nothing, so the next call installs
                again.
            CoreError: the core of this process cannot run the call.
        """
        if self._initialized:
            return
        match await self._core_handle.install_schema_async():
            case SchemaInstalled() | SchemaInstallNotRun():
                self._initialized = True
            case BrokerOperationFailed(
                code=BrokerErrorCode.SCHEMA_INIT_FAILED, exception=exception,
            ):
                raise exception
            case BrokerOperationFailed(code=code):
                raise AssertionError(f'the core schema install answered the code {code}')
            case _ as unreachable:
                assert_never(unreachable)

    async def ensure_schema_initialized(
        self,
    ) -> Result[None, BrokerOperationFailed | BrokerEventLoopLimit]:
        """Install the schema, or check it, once per broker. Writes.

        Safe to call many times and from many processes: the core guards the
        install with the fleet schema advisory lock, and waits for another
        installer's lock for at most the core's bound.

        Returns ``Ok(None)`` on success. On failure ``BrokerOperationFailed``
        with ``SCHEMA_INIT_FAILED`` and its cause: the core reported the
        install failed (``exception`` is the core's ``CoreFailure``), for
        example a database of the other client family or one this build
        cannot install into (not retryable), or the core of this process
        cannot run the call (``exception`` is the ``CoreCallRefusal``). The
        install runs on the core pools, so ``BrokerEventLoopLimit`` is no
        longer a failure exit. A cancel of the caller does not stop the
        install. Raises ``ForkedAfterCoreStart`` and ``CoreStopped``
        (``call_refusal_or_raise``).
        """
        try:
            await self._ensure_initialized()
        except CoreFailure as exc:
            return _schema_init_failed(exc)
        except CoreError as exc:
            return _schema_init_failed(call_refusal_or_raise(exc))
        return Ok(None)

    # ----------------- Async API -----------------

    @staticmethod
    def _prepare_enqueue(
        *,
        task_id: UUID,
        task_name: str,
        queue_name: str,
        priority: int,
        input_json: str | None,
        sent_at: datetime,
        enqueued_at: Optional[datetime],
        enqueue_delay_seconds: Optional[int],
        good_until: Optional[datetime],
        task_options: Optional[str],
        retention_class_key: str,
        retain_rerun_input: bool,
        idempotency_key: Optional[str],
        key_window: timedelta,
    ) -> Result[_PreparedEnqueue, BrokerInvalidInput | BrokerInvalidTaskOptions]:
        """Check one enqueue's input and build its row. Touches no database.

        Every refusal means nothing was written, so a refused input never
        looks like a write that may have happened. A lone surrogate in
        ``task_name`` or ``queue_name`` raises ``UnicodeEncodeError`` from the
        fingerprint: those are programmer constants, so it crashes loud.
        ``key_window`` is how long a key stays reserved after its task
        finishes; only a keyed send uses it.
        """
        timing_r = _check_enqueue_timing(
            sent_at=sent_at,
            enqueued_at=enqueued_at,
            enqueue_delay_seconds=enqueue_delay_seconds,
        )
        if is_err(timing_r):
            return timing_r
        command_r = _send_command(
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            input_json=input_json,
            good_until=good_until,
            enqueue_delay_seconds=enqueue_delay_seconds,
            task_options=task_options,
            retention_class_key=retention_class_key,
            retain_rerun_input=retain_rerun_input,
        )
        if is_err(command_r):
            return command_r
        command = command_r.ok_value

        key_r = _enqueue_key(task_name, idempotency_key, key_window)
        if is_err(key_r):
            return key_r
        return Ok(_PreparedEnqueue(
            row=EnqueueRow(
                task_id=task_id,
                task_name=task_name,
                queue_name=queue_name,
                priority=priority,
                stored_input=command.stored_input,
                task_options=task_options,
                execution=command.execution,
                retention_class_key=retention_class_key,
                retain_rerun_input=retain_rerun_input,
                max_retries=command.max_retries,
                good_until=good_until,
                sent_at=sent_at,
                start=_enqueue_start(enqueued_at, enqueue_delay_seconds),
                command_fingerprint=command.fingerprint,
                key=key_r.ok_value,
                lineage=FirstRun(),
            ),
        ))

    def prepare_send_command(
        self,
        *,
        task_name: str,
        queue_name: str,
        priority: int,
        input_json: str | None,
        task_options: str | None,
        retention_class_key: RetentionClassKey | None = DEFAULT_RETENTION_CLASS_KEY,
    ) -> Result[SendCommand, BrokerInvalidInput | BrokerInvalidTaskOptions]:
        """The stored values of an unkeyed send with no deadline and no delay, without a write.

        The values are those ``enqueue_async`` writes for the same arguments:
        the retention class resolves as there (an explicit None is
        ``forever``), and the rerun-input posture is the broker's default.
        Touches no database; the refusals are those of ``enqueue_async``
        before its write.
        """
        return _send_command(
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            input_json=input_json,
            good_until=None,
            enqueue_delay_seconds=None,
            task_options=task_options,
            retention_class_key=resolve_retention_class_key(retention_class_key),
            retain_rerun_input=self.config.retain_rerun_input_default,
        )

    async def enqueue_async(
        self,
        task_name: str,
        queue_name: str = 'default',
        *,
        task_id: UUID,
        send_kind: SendKind,
        input_json: str | None = None,
        priority: int = 100,
        sent_at: datetime,
        enqueued_at: Optional[datetime] = None,
        enqueue_delay_seconds: Optional[int] = None,
        good_until: Optional[datetime] = None,
        task_options: Optional[str] = None,
        retention_class_key: RetentionClassKey | None = DEFAULT_RETENTION_CLASS_KEY,
        retain_rerun_input: Optional[bool] = None,
        idempotency_key: Optional[str] = None,
    ) -> Result[UUID, BrokerEnqueueError]:
        """Write one PENDING task row, or answer the send that wrote it,
        through ``CoreHandle.enqueue_async``.

        The core installs the schema once per handle, checks the family of
        the database once per handle, and sends one ``horsies_enqueue_task``
        statement in autocommit. The row stores ``input_json`` as its
        ``input`` (``{}`` for none or JSON null), the version-1 command
        fingerprint of the send and ``sent_at``, the call time of the send.
        A send that repeats a task id is the same send only when the stored
        fingerprint and ``sent_at`` are equal; a resend passes the
        ``sent_at`` of the first send. ``send_kind`` is ``SendKind.REPEAT``
        for a send retry: a task that finished then answers with its id and
        does not run again. ``retention_class_key`` absent is the 30-day
        class, an explicit ``None`` is ``forever``; ``retain_rerun_input``
        ``None`` takes the config's default.

        Failures (``_enqueue_request``, ``_enqueue_result``):
        - ``INVALID_INPUT`` / ``INVALID_TASK_OPTIONS``: refused before any
          statement; nothing was written. A rule of the version-1 command
          (an empty retention class key included) or of the idempotency key
          has its ``ValueError`` as the cause, as Python's broker gave it.
        - ``ENQUEUE_FAILED``: the row may or may not exist; ``retryable`` is
          the core's classification. A database of the other client family
          and a retention class key that is not empty and that no class
          derives are not retryable.
        - ``PAYLOAD_MISMATCH``, ``IDEMPOTENCY_KEY_CONFLICT``: nothing was
          written.

        Raises ``ForkedAfterCoreStart`` (this process was forked without
        ``exec`` after the core started) and ``CoreStopped`` (a core
        background task panicked): states of the process, never a result.
        Nothing was written. Every other core error is ``ENQUEUE_FAILED``
        with it as ``exception``.

        A cancel of the caller does not stop the write: the core runs it to
        its end.
        """
        match _enqueue_request(
            task_id=task_id,
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            input_json=input_json,
            sent_at=sent_at,
            enqueued_at=enqueued_at,
            enqueue_delay_seconds=enqueue_delay_seconds,
            good_until=good_until,
            task_options=task_options,
            retention_class_key=resolve_retention_class_key(retention_class_key),
            retain_rerun_input=retain_rerun_input,
            idempotency_key=idempotency_key,
            send_kind=send_kind,
        ):
            case Err() as refused:
                return refused
            case Ok(ok_value=request):
                pass
        try:
            outcome = await self._core_handle.enqueue_async(request)
        except CoreError as exc:
            return _enqueue_core_error(task_name, call_refusal_or_raise(exc))
        return _enqueue_result(outcome)

    def _build_raw_result_record(
        self,
        task_id: UUID,
        task_name: str,
        status: TaskStatus,
        result_json: 'str | None',
    ) -> BrokerResult[RawResultRecord | None]:
        """Decode the result column into a ``RawResultRecord``.

        ``result_json`` None (no stored result) gives a record without a
        result. Returns ``Err(INVALID_JSON_PAYLOAD)`` when the stored JSON is
        malformed (parser failure, an empty text, NaN / Infinity rejected by
        the parse-constant guard) or when the top-level payload is neither
        ``None`` nor a JSON object (the envelope grammar requires a
        ``dict``). Otherwise wraps the loaded value into the record.
        """
        if result_json is None:
            return Ok(RawResultRecord(
                task_id=task_id,
                task_name=task_name,
                status=status,
                raw_result=None,
            ))
        _lr = loads_json(result_json)
        if is_err(_lr):
            return Err(BrokerInvalidJsonPayload(
                message=(
                    f'Result JSON parse failed for task {task_id}: '
                    f'{_lr.err_value}'
                ),
                exception=_lr.err_value,
            ))
        raw_value = _lr.ok_value
        if raw_value is not None and not isinstance(raw_value, dict):
            return Err(BrokerInvalidJsonPayload(
                message=(
                    f'Result for task {task_id} is not a JSON object; '
                    f'got {type(raw_value).__name__}'
                ),
                exception=NoCause(),
            ))
        return Ok(RawResultRecord(
            task_id=task_id,
            task_name=task_name,
            status=status,
            raw_result=raw_value,
        ))

    async def get_raw_result_record_async(
        self,
        task_id: UUID,
        timeout_ms: Optional[int] = None,
    ) -> BrokerResult[RawResultRecord | None]:
        """Raw broker fetch of a task's stored result envelope, through
        ``CoreHandle.raw_result_record_async``.

        The broker performs no typed decoding; callers at the handle/app
        layer derive ``ok_type`` from ``record.task_name`` and call
        ``decode_task_result(record.raw_result, ok_type)``.

        The core installs the schema once per handle first, then reads the
        live row or the history record. While the task has not finished it
        waits up to ``timeout_ms`` (no limit for ``None``) for a
        ``task_done`` notification on the process listener of the session
        URL, reading again at each wake and at the core's interval; a broker
        built with ``assume_initialized=True`` has no listener and reads
        again every 200 ms. Reads only: a cancel of the caller drops the
        wait.

        Semantics:

        - ``Ok(None)``: neither the live table nor the history holds the task.
        - ``Ok(RawResultRecord(status=COMPLETED|FAILED|EXPIRED, raw_result=<dict>))``:
          terminal task with a stored payload.
        - ``Ok(RawResultRecord(status=CANCELLED, raw_result=None))``:
          task was cancelled before completion.
        - ``Ok(RawResultRecord(status=<non-terminal>, raw_result=None))``:
          the limit passed before the task reached a terminal state.
        - ``Err(BrokerInvalidJsonPayload)``: the stored result does not read
          (malformed, non-finite number, not an object, a history digest
          that differs). The same read fails again.
        - ``Err(BrokerWaitInsideTask)``: the call runs in a process that
          runs task bodies (a worker child). This is the first check: it
          comes before the argument checks. Nothing ran.
        - ``Err(BrokerInvalidInput)``: ``timeout_ms`` is not None or an int
          from 0 to ``MAX_TASK_TIMEOUT_MS``. Nothing ran.
        - ``Err(BrokerOperationFailed(TASK_INFO_QUERY_FAILED))``: the
          connect, the install or a read failed (``retryable`` is the core's
          classification, ``exception`` the core's exception of the cause), or the
          core of this process cannot run the call (``exception`` is the
          ``CoreCallRefusal``).
        """
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return _wait_inside_task_err(
                'PostgresBroker.get_raw_result_record_async()', task_id,
            )
        match wait_timeout_seconds(timeout_ms):
            case Err(err_value=refused):
                return Err(BrokerInvalidInput(
                    message=f'raw result fetch of task {task_id}: {refused.message}',
                    exception=NoCause(),
                ))
            case Ok():
                pass
        try:
            outcome = await self._core_handle.raw_result_record_async(
                task_id, timeout_ms=timeout_ms,
            )
        except CoreError as exc:
            return self._raw_result_core_error(task_id, call_refusal_or_raise(exc))
        return self._raw_result_of_outcome(task_id, outcome)

    def get_raw_result_record(
        self,
        task_id: UUID,
        timeout_ms: Optional[int] = None,
    ) -> BrokerResult[RawResultRecord | None]:
        """The blocking form of ``get_raw_result_record_async``:
        ``CoreHandle.raw_result_record`` on the calling thread, with the GIL
        released and signals checked every 100 ms.

        In a process that runs task bodies it returns
        ``Err(BrokerWaitInsideTask)`` first. On a thread that runs an event
        loop it returns ``Err(ASYNC_CONTEXT)``. Neither does I/O. A wait
        with a limit is bounded by the limit plus the core's bound of a
        blocking call; a wait with no limit has no call bound, as before.
        """
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return _wait_inside_task_err(
                'PostgresBroker.get_raw_result_record()', task_id,
            )
        if in_running_event_loop():
            return _async_context_err(
                'PostgresBroker.get_raw_result_record()',
                'broker.get_raw_result_record_async(...)',
            )
        match wait_timeout_seconds(timeout_ms):
            case Err(err_value=refused):
                return Err(BrokerInvalidInput(
                    message=f'raw result fetch of task {task_id}: {refused.message}',
                    exception=NoCause(),
                ))
            case Ok():
                pass
        try:
            outcome = self._core_handle.raw_result_record(task_id, timeout_ms=timeout_ms)
        except CoreError as exc:
            return self._raw_result_core_error(task_id, call_refusal_or_raise(exc))
        return self._raw_result_of_outcome(task_id, outcome)

    def _raw_result_core_error(
        self, task_id: UUID, exc: CoreCallRefusal,
    ) -> Err[BrokerOperationFailed]:
        """The core of this process cannot run a result read: it did not
        start, has too many calls in flight, or a blocking call reached its
        bound. Logged on ``horsies.broker``; nothing changed."""
        self.logger.error(
            'Broker error while retrieving raw task result record for task %s: %s',
            task_id,
            exc,
        )
        return Err(
            BrokerOperationFailed(
                code=BrokerErrorCode.TASK_INFO_QUERY_FAILED,
                message=f'Broker error while retrieving task {task_id}: {exc}',
                retryable=core_error_retryable(exc),
                exception=exc,
            ),
        )

    def _raw_result_of_outcome(
        self,
        task_id: UUID,
        outcome: (
            RawResultRecordRecord
            | RawResultAbsent
            | BrokerOperationFailed
            | BrokerInvalidJsonPayload
            | BrokerInvalidInput
        ),
    ) -> BrokerResult[RawResultRecord | None]:
        """The result of one core result read.

        - ``RawResultRecordRecord``: ``Ok(RawResultRecord)``; ``result_json``
          is read with the strict JSON reader (``INVALID_JSON_PAYLOAD`` when
          it does not read as an object or null).
        - ``RawResultAbsent``: ``Ok(None)``.
        - ``BrokerInvalidJsonPayload``: the core's class as it is.
        - ``BrokerOperationFailed`` with ``TASK_INFO_QUERY_FAILED``: the
          core's class as it is, with Python's message (``Broker error while
          retrieving task {task_id}: {error}``), logged on
          ``horsies.broker``.

        Each failure's ``exception`` is the core's exception of the cause, or
        ``NoCause()`` for a check of the core. ``BrokerInvalidInput`` for a
        limit that the check above admitted contradicts that check:
        ``AssertionError``.
        """
        match outcome:
            case RawResultRecordRecord(
                task_name=task_name, status=status, result_json=result_json,
            ):
                match result_json:
                    case None:
                        raw_result = None
                    case str():
                        match _strict_result_envelope(task_id, result_json):
                            case Ok(envelope):
                                raw_result = envelope
                            case Err() as refused:
                                return refused
                return Ok(RawResultRecord(
                    task_id=task_id,
                    task_name=task_name,
                    status=TaskStatus(status),
                    raw_result=raw_result,
                ))
            case RawResultAbsent():
                return Ok(None)
            case BrokerInvalidJsonPayload():
                return Err(outcome)
            case BrokerOperationFailed(
                code=BrokerErrorCode.TASK_INFO_QUERY_FAILED, message=message,
            ):
                self.logger.error(
                    'Broker error while retrieving raw task result record for task %s: %s',
                    task_id,
                    message,
                )
                return Err(outcome)
            case BrokerOperationFailed(code=code):
                raise AssertionError(f'the core result read answered the code {code}')
            case BrokerInvalidInput():
                raise AssertionError(
                    'the core result read answered the code INVALID_INPUT for a limit that '
                    'the check admitted',
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def close_async(self) -> BrokerResult[None]:
        """Close every resource set and the core handle's pools, then stop
        the sync runner if it was started.

        Each set closes on its own event loop: the running loop's set
        directly, the set of a closed loop by release, and the set of
        another open loop by a hand-off bounded by ``CLOSE_HANDOFF_TIMEOUT``.
        The core handle's writes in flight end within the core's close
        bound; a handle that never connected closes without starting the
        core. The broker stays usable: a later async call builds a new set
        for its loop, and a later core operation opens new core pools.

        Raises ``ForkedAfterCoreStart`` and ``CoreStopped`` from the core
        handle's close, after the resource sets closed and the sync runner
        stopped.
        """
        errors = await self._close_python_resources(self._take_python_resources())
        try:
            _ = await self._core_handle.close_async()
        except CoreError as exc:
            # The core of this process cannot run the close: it did not
            # start, or the loop has too many calls in flight.
            errors.append(call_refusal_or_raise(exc))
        finally:
            errors.extend(await self._stop_loop_runner_off_its_thread())
        return _close_answer(errors)

    async def _stop_loop_runner_off_its_thread(self) -> list[BaseException]:
        """Stop the sync runner when sync APIs started it and this call does
        not run on the runner's own thread. Returns the stop's failure, or
        no failure.

        ``stop()`` joins the runner thread (up to 2 s), so it runs on a
        worker thread and this loop keeps running during the join.
        """
        loop_thread = self._loop_runner._thread  # pyright: ignore[reportPrivateUsage]
        if (
            self._loop_runner._started  # pyright: ignore[reportPrivateUsage]
            and loop_thread is not None
            and threading.current_thread() is not loop_thread
        ):
            try:
                await asyncio.to_thread(self._loop_runner.stop)
            except Exception as exc:
                return [exc]
        return []

    def _take_python_resources(self) -> _PythonResources:
        """Take the resource sets and the sync send engine from the broker, so
        that no later call uses them. No I/O."""
        with self._loop_sets_lock:
            loop_sets = self._loop_sets
            self._loop_sets = ()
        with self._sync_send_lock:
            sync_send_engine = self._sync_send_engine
            self._sync_send_engine = _SyncSendEngineNotOpen()
        return _PythonResources(loop_sets=loop_sets, sync_send_engine=sync_send_engine)

    async def _close_python_resources(self, taken: _PythonResources) -> list[BaseException]:
        """Close the taken resource sets, each by the rule for its loop, and
        dispose the taken sync send engine, on the running loop.

        Returns the failures; raises none except the caller's cancellation.
        """
        current_loop = asyncio.get_running_loop()
        per_set_errors = await asyncio.gather(
            *(self._close_loop_set(entry, current_loop) for entry in taken.loop_sets),
        )
        errors: list[BaseException] = [
            error for set_errors in per_set_errors for error in set_errors
        ]
        match taken.sync_send_engine:
            case _SyncSendEngineOpen(engine=engine):
                try:
                    await asyncio.to_thread(engine.dispose)
                except _DRIVER_ERRORS as exc:
                    errors.append(exc)
            case _SyncSendEngineNotOpen():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        return errors

    # ------------- Operational & Monitoring Methods -------------

    async def get_stale_tasks(
        self,
        stale_threshold_minutes: int = 2,
    ) -> BrokerResult[list[dict[str, Any]]]:
        """RUNNING tasks whose latest runner heartbeat, or their start when
        they have no heartbeat, is older than the threshold. Reads only, in
        one statement (``horsies_list_stale_tasks``) that the core runs on
        the broker's core handle.

        Rows come ordered by the latest heartbeat, rows with none first, then
        by id. Each row is a dict with the keys ``id`` (``uuid.UUID``),
        ``worker_hostname``, ``worker_pid``, ``worker_process_name``,
        ``last_heartbeat``, ``started_at`` and ``task_name``. At most
        ``TASK_LIST_ROW_BOUND`` rows come back; when more rows match, the
        first ``TASK_LIST_ROW_BOUND`` come back and a warning is logged.

        - ``Err(INVALID_INPUT)``: ``stale_threshold_minutes`` is not an int
          from 0 to ``MAX_STALE_THRESHOLD_MINUTES``. Nothing ran.
        - ``Err(MONITORING_QUERY_FAILED)``: the read failed, or the core of
          this process cannot run it (``CoreCallRefusal``).
        """
        try:
            outcome = await self._core_handle.stale_tasks_async(stale_threshold_minutes)
        except CoreError as exc:
            return _monitoring_call_failed('get_stale_tasks', call_refusal_or_raise(exc))
        return self._task_list_answer('get_stale_tasks', outcome)

    def _task_list_answer(
        self,
        operation: str,
        outcome: (
            TaskListComplete | TaskListTruncated | BrokerOperationFailed | BrokerInvalidInput
        ),
    ) -> BrokerResult[list[dict[str, Any]]]:
        """The answer of a task list from the core's record; a cut list logs a
        warning that names ``operation`` and the bound. A failure is the
        core's class as it is: ``BrokerInvalidInput`` for a refused threshold
        (nothing ran), or ``BrokerOperationFailed`` with
        ``MONITORING_QUERY_FAILED``."""
        match outcome:
            case TaskListComplete(rows=rows):
                return Ok(rows)
            case TaskListTruncated(rows=rows, bound=bound):
                self.logger.warning(
                    '%s returned the first %d rows; more rows match', operation, bound,
                )
                return Ok(rows)
            case BrokerInvalidInput():
                return Err(outcome)
            case BrokerOperationFailed(code=BrokerErrorCode.MONITORING_QUERY_FAILED):
                return Err(outcome)
            case BrokerOperationFailed(code=code):
                raise AssertionError(f'the core {operation} answered the code {code}')
            case _ as unreachable:
                assert_never(unreachable)

    async def ping_database_async(self) -> BrokerResult[DatabasePing]:
        """Probe Postgres reachability with one statement on the broker's main
        core pool, through ``CoreHandle.ping_database_async``.

        A success also confirms that a connection could be checked out of the
        pool. Returns the measured round-trip latency. Reads only; a cancel
        of the caller drops the ping.

        Failures, each ``DB_PING_FAILED``; nothing changed:
        - the connect or the statement failed; ``retryable`` is the core's
          classification of the cause, and ``exception`` the core's
          exception of the cause;
        - the core of this process cannot run the call (``CoreCallRefusal``:
          it did not start, has too many calls in flight, or a sync call
          reached its bound); the
          ``exception`` is that error, and ``retryable`` follows
          ``core_error_retryable``.
        """
        try:
            outcome = await self._core_handle.ping_database_async()
        except CoreError as exc:
            return _database_ping_call_failed(call_refusal_or_raise(exc))
        return _database_ping_answer(outcome)

    def _ping_database_blocking(self) -> BrokerResult[DatabasePing]:
        """The blocking form of ``ping_database_async``, for
        ``Horsies.ping_database``: ``CoreHandle.ping_database`` on the calling
        thread, with the GIL released. The same outcomes, plus
        ``DB_PING_FAILED`` with the ``CoreCallTimedOut`` (retryable) for a
        call that does not end within the core's bound of a blocking call.

        The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
        event loop.
        """
        try:
            outcome = self._core_handle.ping_database()
        except CoreError as exc:
            return _database_ping_call_failed(call_refusal_or_raise(exc))
        return _database_ping_answer(outcome)

    async def ping_workers_async(
        self,
        *,
        target_worker_id: str | None = None,
        timeout_seconds: float = 2.0,
        min_responses: int | None = None,
    ) -> BrokerResult[list[WorkerPong]]:
        """Active ping-pong: NOTIFY workers and collect replies within a window.

        Subscribes to a unique reply channel, broadcasts a ping on
        ``WORKER_PING_CHANNEL``, then collects pongs until a stop condition:

        - ``target_worker_id`` set: returns as soon as that worker replies.
        - ``min_responses`` set: returns as soon as that many distinct workers
          reply (fast fail-open liveness, e.g. ``min_responses=1`` for a
          ``/health`` gate — a healthy fleet answers in milliseconds; only a
          degraded fleet pays the full ``timeout_seconds``).
        - neither set: waits the full window and enumerates every responder.

        A pong proves the replying worker's event loop is responsive *and*
        that it can reach Postgres. Workers present in
        ``list_worker_states_async`` but absent here are non-responsive.

        The core runs the ping on the process listener of the broker's core
        handle; the window starts when the ping NOTIFY has committed. A
        reply that does not decode is logged on ``horsies.broker`` and left
        out. An infinite ``timeout_seconds`` waits with no deadline.

        - ``Err(INVALID_INPUT)``: ``timeout_seconds`` is 0 or less, or NaN;
          ``min_responses`` is below 1; ``target_worker_id`` is not a
          ``str``. Nothing ran.
        - ``Err(LISTENER_DISABLED)``: the broker was built with
          ``assume_initialized=True`` and has no listener. Nothing ran.
        - ``Err(WORKER_PING_FAILED)``: the reply subscribe, the NOTIFY or the
          wait failed, or the core of this process cannot run the ping
          (``CoreCallRefusal``). The NOTIFY may have been sent.
        """
        try:
            outcome = await self._core_handle.ping_workers_async(
                target_worker_id=target_worker_id,
                timeout_seconds=timeout_seconds,
                min_responses=min_responses,
            )
        except CoreError as exc:
            return _worker_ping_call_failed(call_refusal_or_raise(exc))
        return _worker_ping_answer(outcome)

    def _ping_workers_blocking(
        self,
        *,
        target_worker_id: str | None,
        timeout_seconds: float,
        min_responses: int | None,
    ) -> BrokerResult[list[WorkerPong]]:
        """The blocking form of ``ping_workers_async``, for
        ``Horsies.ping_workers``: ``CoreHandle.ping_workers`` on the calling
        thread, with the GIL released, and the same outcomes. The call is
        bounded by the window plus the core's bound of a blocking call, and
        not bounded for an infinite window; a call past its bound is
        ``WORKER_PING_FAILED`` with the ``CoreCallTimedOut``.

        The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
        event loop.
        """
        try:
            outcome = self._core_handle.ping_workers(
                target_worker_id=target_worker_id,
                timeout_seconds=timeout_seconds,
                min_responses=min_responses,
            )
        except CoreError as exc:
            return _worker_ping_call_failed(call_refusal_or_raise(exc))
        return _worker_ping_answer(outcome)

    async def list_worker_states_async(self) -> BrokerResult[list[WorkerStateSnapshot]]:
        """Latest state snapshot per worker, including idle workers.

        Unlike the retired ``get_worker_stats`` (RUNNING tasks only), this
        reads ``horsies_worker_states`` so every worker that has reported a
        snapshot appears, regardless of current load. Reads only, in one
        statement that the core runs on the broker's core handle.

        ``Err(MONITORING_QUERY_FAILED)``: the read failed, a stored snapshot
        has a value of another shape than the worker writes (not
        retryable), or the core of this process cannot run the read
        (``CoreCallRefusal``). Nothing changed.
        """
        try:
            outcome = await self._core_handle.worker_states_async()
        except CoreError as exc:
            return _monitoring_call_failed('list_worker_states', call_refusal_or_raise(exc))
        return _worker_states_answer(outcome)

    def _list_worker_states_blocking(self) -> BrokerResult[list[WorkerStateSnapshot]]:
        """The blocking form of ``list_worker_states_async``, for
        ``Horsies.list_worker_states``: ``CoreHandle.worker_states`` on the calling
        thread, with the GIL released, and the same outcomes. A call past the
        core's bound of a blocking call is ``MONITORING_QUERY_FAILED`` with
        the ``CoreCallTimedOut``.

        The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
        event loop.
        """
        try:
            outcome = self._core_handle.worker_states()
        except CoreError as exc:
            return _monitoring_call_failed('list_worker_states', call_refusal_or_raise(exc))
        return _worker_states_answer(outcome)

    async def get_worker_state_async(
        self,
        worker_id: str,
    ) -> BrokerResult[WorkerStateSnapshot | None]:
        """Latest state snapshot for one worker, or ``None`` if unknown.

        Reads only, in one statement that the core runs on the broker's core
        handle. ``Err(MONITORING_QUERY_FAILED)``: as
        ``list_worker_states_async``.
        """
        try:
            outcome = await self._core_handle.worker_state_async(worker_id)
        except CoreError as exc:
            return _monitoring_call_failed('get_worker_state', call_refusal_or_raise(exc))
        return _worker_state_answer(outcome)

    def _get_worker_state_blocking(
        self, worker_id: str,
    ) -> BrokerResult[WorkerStateSnapshot | None]:
        """The blocking form of ``get_worker_state_async``, for
        ``Horsies.get_worker_state``: ``CoreHandle.worker_state`` on the calling
        thread, with the GIL released, and the same outcomes. A call past the
        core's bound of a blocking call is ``MONITORING_QUERY_FAILED`` with
        the ``CoreCallTimedOut``.

        The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
        event loop.
        """
        try:
            outcome = self._core_handle.worker_state(worker_id)
        except CoreError as exc:
            return _monitoring_call_failed('get_worker_state', call_refusal_or_raise(exc))
        return _worker_state_answer(outcome)

    async def get_worker_state_history_async(
        self,
        worker_id: str,
        *,
        limit: int | None = None,
    ) -> BrokerResult[list[WorkerStateSnapshot]]:
        """Timeseries snapshots for one worker, newest first.

        ``limit`` of ``None`` returns all retained rows; pass an explicit cap
        to bound the fetch (the table grows ~1 row per worker per interval).
        Reads only, in one statement that the core runs on the broker's core
        handle.

        - ``Err(INVALID_INPUT)``: ``limit`` is 0 or less. Nothing ran.
        - ``Err(MONITORING_QUERY_FAILED)``: as ``list_worker_states_async``,
          or a ``limit`` above the PostgreSQL ``bigint`` range (not
          retryable; nothing ran).
        """
        try:
            outcome = await self._core_handle.worker_state_history_async(
                worker_id, limit=limit,
            )
        except CoreError as exc:
            return _monitoring_call_failed('get_worker_state_history', call_refusal_or_raise(exc))
        return _worker_states_answer(outcome)

    def _get_worker_state_history_blocking(
        self, worker_id: str, *, limit: int | None,
    ) -> BrokerResult[list[WorkerStateSnapshot]]:
        """The blocking form of ``get_worker_state_history_async``, for
        ``Horsies.get_worker_state_history``: ``CoreHandle.worker_state_history`` on the calling
        thread, with the GIL released, and the same outcomes. A call past the
        core's bound of a blocking call is ``MONITORING_QUERY_FAILED`` with
        the ``CoreCallTimedOut``.

        The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
        event loop.
        """
        try:
            outcome = self._core_handle.worker_state_history(
                worker_id, limit=limit,
            )
        except CoreError as exc:
            return _monitoring_call_failed('get_worker_state_history', call_refusal_or_raise(exc))
        return _worker_states_answer(outcome)

    async def get_class_coverage_async(self) -> BrokerResult[ClassCoverage]:
        """The fleet check: for each (queue, class) with PENDING rows, whether
        a live worker serves it.

        A worker is live while its latest snapshot is at most
        ``WORKER_STATE_LIVE_WINDOW_S`` old. It serves a (queue, class) when
        the queue is one of its queues and one of its budgets serves the
        class. Reads only, in one statement that the core runs on the
        broker's core handle.
        ``Err(MONITORING_QUERY_FAILED)``: a database or network error, a live
        snapshot whose ``class_capacity`` is unreadable, or a read that
        reached its bound (``retryable`` is ``False`` for the last two), or
        the core of this process cannot run the read (``CoreCallRefusal``).
        """
        try:
            outcome = await self._core_handle.class_coverage_async()
        except CoreError as exc:
            return _monitoring_call_failed('get_class_coverage', call_refusal_or_raise(exc))
        return _class_coverage_answer(outcome)

    def _get_class_coverage_blocking(self) -> BrokerResult[ClassCoverage]:
        """The blocking form of ``get_class_coverage_async``, for
        ``Horsies.get_class_coverage``: ``CoreHandle.class_coverage`` on the calling
        thread, with the GIL released, and the same outcomes. A call past the
        core's bound of a blocking call is ``MONITORING_QUERY_FAILED`` with
        the ``CoreCallTimedOut``.

        The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
        event loop.
        """
        try:
            outcome = self._core_handle.class_coverage()
        except CoreError as exc:
            return _monitoring_call_failed('get_class_coverage', call_refusal_or_raise(exc))
        return _class_coverage_answer(outcome)

    async def get_class_report_async(self) -> BrokerResult[ClassReport]:
        """Pending streams with the wait of their oldest due row and their
        coverage, and the budget load of each live worker.

        Reads only, in one statement that the core runs on the broker's core
        handle.
        ``Err(MONITORING_QUERY_FAILED)``: as ``get_class_coverage_async``, or
        a live worker holds rows of a class that its recorded budgets do not
        serve.
        """
        try:
            outcome = await self._core_handle.class_report_async()
        except CoreError as exc:
            return _monitoring_call_failed('get_class_report', call_refusal_or_raise(exc))
        return _class_report_answer(outcome)

    def _get_class_report_blocking(self) -> BrokerResult[ClassReport]:
        """The blocking form of ``get_class_report_async``, for
        ``Horsies.get_class_report``: ``CoreHandle.class_report`` on the calling
        thread, with the GIL released, and the same outcomes. A call past the
        core's bound of a blocking call is ``MONITORING_QUERY_FAILED`` with
        the ``CoreCallTimedOut``.

        The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
        event loop.
        """
        try:
            outcome = self._core_handle.class_report()
        except CoreError as exc:
            return _monitoring_call_failed('get_class_report', call_refusal_or_raise(exc))
        return _class_report_answer(outcome)

    async def get_expired_tasks(self) -> BrokerResult[list[dict[str, Any]]]:
        """PENDING tasks whose ``good_until`` passed before a worker took
        them. Reads only, in one statement (``horsies_list_expired_tasks``)
        that the core runs on the broker's core handle.

        Rows come ordered by ``good_until``, oldest first, then by id. Each
        row is a dict with the keys ``id`` (``uuid.UUID``), ``task_name``,
        ``queue_name``, ``priority``, ``sent_at``, ``enqueued_at``,
        ``good_until`` and ``expired_for`` (a ``timedelta``, on the database
        clock). At most ``TASK_LIST_ROW_BOUND`` rows come back; when more
        rows match, the first ``TASK_LIST_ROW_BOUND`` come back and a warning
        is logged.

        - ``Err(MONITORING_QUERY_FAILED)``: the read failed, or the core of
          this process cannot run it (``CoreCallRefusal``).
        """
        try:
            outcome = await self._core_handle.expired_tasks_async()
        except CoreError as exc:
            return _monitoring_call_failed('get_expired_tasks', call_refusal_or_raise(exc))
        return self._task_list_answer('get_expired_tasks', outcome)

    async def mark_stale_tasks_as_failed(
        self,
        stale_threshold_ms: int = 300_000,
        finalizing_stale_threshold_ms: int = 300_000,
    ) -> BrokerResult[int]:
        """Recover RUNNING tasks whose runner stopped beating: a crash retry
        when the task's policy allows it, else the failure. One core pass.

        The core reads at most one scan batch of candidates with
        ``horsies_stale_running_scan``, without a lock; a full batch leaves
        the rest to the next pass and a warning is logged. Each candidate is
        then one ``horsies_finish_stale_attempt`` statement that commits
        alone: it judges staleness again under the task advisory lock and the
        row lock and writes the attempt with the retry or the move, with the
        crash result that the core builds. A scheduled retry also wakes its
        queue (best effort). Returns the number of tasks retried or failed; a
        refused candidate is not counted, and a candidate whose statement
        failed is logged on ``horsies.broker`` and skipped.

        Failures, each ``CLEANUP_FAILED``; nothing was written:
        - the scan, the connect or the client family check failed;
          ``retryable`` is the core's classification of the cause;
        - a threshold below 0 or above 2,147,483,647 ms; not retryable;
        - the core of this process cannot run the pass (``CoreCallRefusal``);
          ``retryable`` follows ``core_error_retryable``.

        A cancel of the caller does not stop the pass: the core runs it to
        its end.
        """
        thresholds = (stale_threshold_ms, finalizing_stale_threshold_ms)
        for threshold_ms in thresholds:
            if not SIGNED_64_BIT_MIN <= threshold_ms <= SIGNED_64_BIT_MAX:
                refusal = f'the stale threshold {threshold_ms} ms is not a 64-bit integer'
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=f'mark_stale_tasks_as_failed failed: {refusal}',
                        retryable=False,
                        exception=OverflowError(refusal),
                    ),
                )
        try:
            outcome = await self._core_handle.mark_stale_running_async(
                stale_threshold_ms, finalizing_stale_threshold_ms,
            )
        except CoreError as exc:
            refusal = call_refusal_or_raise(exc)
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'mark_stale_tasks_as_failed failed: {refusal}',
                    retryable=core_error_retryable(refusal),
                    exception=refusal,
                ),
            )
        match outcome:
            case StaleRunningRecovered(recovered=recovered):
                return Ok(recovered)
            case StaleRunningScanFull(recovered=recovered, scan_batch=scan_batch):
                self.logger.warning(
                    'mark_stale_tasks_as_failed read a full scan batch of %s '
                    'candidates; the rest is recovered next pass',
                    scan_batch,
                )
                return Ok(recovered)
            case StaleRunningScanFailed(
                message=message, retryable=retryable, exception=exception,
            ):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=f'mark_stale_tasks_as_failed failed: {message}',
                        retryable=retryable,
                        exception=exception,
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def expire_pending_tasks(self) -> BrokerResult[int]:
        """Expire PENDING tasks whose ``good_until`` deadline passed: one core
        pass.

        The core sends ``horsies_expire_pending_tasks`` in batches of 500
        rows, earliest deadline first. Each batch is one transaction that
        commits alone, and only when every row it reports moved: the row goes
        to history as EXPIRED with a ``TASK_EXPIRED`` result. No attempt row
        is written (the task never started). Batches repeat until one is
        short, at most 200 times; past that the rest expires on the next pass
        and a warning is logged. Returns the number of tasks expired.

        Failures, each ``CLEANUP_FAILED``; the batches before the failure
        committed:
        - a database failure, or a batch outside the function's contract;
          ``retryable`` is the core's classification of the cause;
        - the core of this process cannot run the pass (``CoreCallRefusal``);
          ``retryable`` follows ``core_error_retryable``.

        A cancel of the caller does not stop the pass: the core runs it to
        its end.
        """
        try:
            outcome = await self._core_handle.expire_pending_tasks_async()
        except CoreError as exc:
            refusal = call_refusal_or_raise(exc)
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'expire_pending_tasks failed: {refusal}',
                    retryable=core_error_retryable(refusal),
                    exception=refusal,
                ),
            )
        match outcome:
            case PendingExpiryDrained(expired=expired):
                return Ok(expired)
            case PendingExpiryBoundReached(expired=expired):
                self.logger.warning(
                    'expire_pending_tasks reached the per-pass batch cap after '
                    'expiring %s task(s); the remainder expires next interval',
                    expired,
                )
                return Ok(expired)
            case PendingExpiryFailed(
                message=message, retryable=retryable, exception=exception,
            ):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=f'expire_pending_tasks failed: {message}',
                        retryable=retryable,
                        exception=exception,
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def requeue_stale_claimed(
        self,
        stale_threshold_ms: int = 120_000,
    ) -> BrokerResult[int]:
        """Requeue CLAIMED rows whose worker stopped beating: one core pass.

        The core sends ``horsies_requeue_stale_claims`` in batches of
        ``REQUEUE_STALE_CLAIMS_BATCH_SIZE``, each one statement that commits
        alone: a row with no claimer beat newer than the threshold, and
        claimed longer ago than the threshold, goes back to PENDING (a
        workflow task with no runnable node stays). Batches repeat until one
        is short, at most ``REQUEUE_STALE_CLAIMS_MAX_BATCHES_PER_PASS``
        times; past that the remainder waits for the next pass and a warning
        is logged. Returns the number of rows requeued.

        Failures, each ``CLEANUP_FAILED``:
        - a database failure: the batches before it committed, and the
          message counts their rows; ``retryable`` is the core's
          classification of the cause;
        - a threshold below 0 or above 2,147,483,647 ms: nothing ran; not
          retryable.

        - the core of this process cannot run the pass (``CoreCallRefusal``:
          it did not start, or has too many calls in flight); the
          ``exception`` is that error, and ``retryable`` follows
          ``core_error_retryable``.

        A cancel of the caller does not stop the pass: the core runs it to
        its end.
        """
        if not SIGNED_64_BIT_MIN <= stale_threshold_ms <= SIGNED_64_BIT_MAX:
            refusal = (
                f'the stale threshold {stale_threshold_ms} ms is not a 64-bit integer'
            )
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=(
                        f'requeue_stale_claimed failed after requeueing 0 task(s): '
                        f'{refusal}'
                    ),
                    retryable=False,
                    exception=OverflowError(refusal),
                ),
            )
        try:
            outcome = await self._core_handle.requeue_stale_claimed_async(
                stale_threshold_ms,
            )
        except CoreError as exc:
            refusal = call_refusal_or_raise(exc)
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'requeue_stale_claimed failed: {refusal}',
                    retryable=core_error_retryable(refusal),
                    exception=refusal,
                ),
            )
        match outcome:
            case StaleClaimsDrained(requeued=requeued):
                return Ok(requeued)
            case StaleClaimsBoundReached(requeued=requeued):
                self.logger.warning(
                    'requeue_stale_claimed reached the per-pass batch cap after '
                    'requeueing %s task(s); the remainder is requeued next pass',
                    requeued,
                )
                return Ok(requeued)
            case StaleClaimsRequeueFailed(
                requeued_before=requeued_before,
                message=message,
                retryable=retryable,
                exception=exception,
            ):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=(
                            f'requeue_stale_claimed failed after requeueing '
                            f'{requeued_before} task(s): {message}'
                        ),
                        retryable=retryable,
                        exception=exception,
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def audit_orphaned_workflow_tasks(
        self,
    ) -> BrokerResult[OrphanTaskAuditReport]:
        """Scan and cancel one bounded page of orphan workflow tasks: one core
        call.

        The core runs ``horsies_cancel_orphaned_tasks`` with a page of 500
        rows and reads the page's cursor counts, in one transaction: the
        scanned PENDING and CLAIMED workflow tasks that no runnable node links
        move to history as CANCELLED. The report carries the cursor counts
        and the time of the call.

        Failures, each ``CLEANUP_FAILED``; nothing committed (after a lost
        ``COMMIT`` reply, the outcome is unknown):
        - another sweep holds the cursor: the page is refused at once; the
          ``exception`` is ``CoreServerError``, whose ``sqlstate`` is
          ``'55P03'``;
        - a database failure, or a page outside the function's contract;
          ``retryable`` is the core's classification of the cause;
        - the core of this process cannot run the page (``CoreCallRefusal``);
          ``retryable`` follows ``core_error_retryable``.

        A cancel of the caller does not stop the page: the core runs it to
        its end.
        """
        started = time.monotonic()
        try:
            outcome = await self._core_handle.audit_orphaned_workflow_tasks_async()
        except CoreError as exc:
            refusal = call_refusal_or_raise(exc)
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'audit_orphaned_workflow_tasks failed: {refusal}',
                    retryable=core_error_retryable(refusal),
                    exception=refusal,
                ),
            )
        match outcome:
            case OrphanPageAudited(
                rows_selected=rows_selected,
                candidates_returned=candidates_returned,
                cancelled=cancelled,
            ):
                return Ok(OrphanTaskAuditReport(
                    rows_selected=rows_selected,
                    candidates_returned=candidates_returned,
                    cancelled=cancelled,
                    duration_ms=int((time.monotonic() - started) * 1000),
                ))
            case (
                OrphanAuditLockBusy(
                    message=message, retryable=retryable, exception=exception,
                )
                | OrphanAuditFailed(
                    message=message, retryable=retryable, exception=exception,
                )
            ):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=f'audit_orphaned_workflow_tasks failed: {message}',
                        retryable=retryable,
                        exception=exception,
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def terminate_orphaned_workflow_tasks(self) -> BrokerResult[int]:
        """Cancel one bounded page and return its applied outcome count."""
        match await self.audit_orphaned_workflow_tasks():
            case Ok(report):
                return Ok(report.cancelled)
            case Err(error):
                return Err(error)

    # ----------------- Sync API Facades -----------------

    def enqueue(
        self,
        task_name: str,
        queue_name: str = 'default',
        *,
        task_id: UUID,
        send_kind: SendKind,
        input_json: str | None = None,
        priority: int = 100,
        sent_at: datetime,
        enqueued_at: Optional[datetime] = None,
        enqueue_delay_seconds: Optional[int] = None,
        good_until: Optional[datetime] = None,
        task_options: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        retention_class_key: RetentionClassKey | None = DEFAULT_RETENTION_CLASS_KEY,
        retain_rerun_input: Optional[bool] = None,
    ) -> Result[UUID, BrokerEnqueueError | BrokerSyncCallError]:
        """The blocking form of ``enqueue_async``: ``CoreHandle.enqueue`` on
        the calling thread, with the GIL released, and the outcomes of
        ``enqueue_async``.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and writes nothing. A call that does not end within the core's bound
        of a blocking call is ``ENQUEUE_FAILED`` with the
        ``CoreCallTimedOut`` (retryable): the row may or may not exist.
        Raises ``ForkedAfterCoreStart`` and ``CoreStopped``, as
        ``enqueue_async`` does.
        """
        if in_running_event_loop():
            return _async_context_err(
                'PostgresBroker.enqueue()', 'broker.enqueue_async(...)',
            )
        match _enqueue_request(
            task_id=task_id,
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            input_json=input_json,
            sent_at=sent_at,
            enqueued_at=enqueued_at,
            enqueue_delay_seconds=enqueue_delay_seconds,
            good_until=good_until,
            task_options=task_options,
            retention_class_key=resolve_retention_class_key(retention_class_key),
            retain_rerun_input=retain_rerun_input,
            idempotency_key=idempotency_key,
            send_kind=send_kind,
        ):
            case Err() as refused:
                return refused
            case Ok(ok_value=request):
                pass
        try:
            outcome = self._core_handle.enqueue(request)
        except CoreError as exc:
            return _enqueue_core_error(task_name, call_refusal_or_raise(exc))
        return _enqueue_result(outcome)

    async def get_task_info_async(
        self,
        task_id: UUID,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
        include_attempts: bool = False,
    ) -> BrokerResult[TaskInfo | None]:
        """Fetch metadata for a task by ID, through ``CoreHandle.task_info_async``.

        The core installs the schema once per handle first (nothing with
        migrations off), then reads the live row, or the task's history
        record when the live row is gone; with ``include_attempts`` the
        attempts, newest first. A live task has no result, failed reason,
        error code or terminal time. Reads only; a cancel of the caller
        drops the read.

        Returns ``Ok(TaskInfo)``, or ``Ok(None)`` when neither the live
        table nor the history holds the task. ``raw_result`` is the stored
        envelope, read with the strict JSON reader; ``decoded_result`` is
        left to the caller (``Horsies.get_task_info_async``). Failures, see
        ``_task_info_result``.
        """
        try:
            outcome = await self._core_handle.task_info_async(
                task_id,
                include_result=include_result,
                include_failed_reason=include_failed_reason,
                include_attempts=include_attempts,
            )
        except CoreError as exc:
            return _task_info_core_error(task_id, call_refusal_or_raise(exc))
        return _task_info_result(outcome)

    def get_task_info(
        self,
        task_id: UUID,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
        include_attempts: bool = False,
    ) -> BrokerResult[TaskInfo | None]:
        """The blocking form of ``get_task_info_async``: ``CoreHandle.task_info``
        on the calling thread, with the GIL released.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O. A call that does not end within the core's bound of
        a blocking call is ``TASK_INFO_QUERY_FAILED`` with the
        ``CoreCallTimedOut`` (retryable); nothing changed.
        """
        if in_running_event_loop():
            return _async_context_err(
                'PostgresBroker.get_task_info()',
                'broker.get_task_info_async(...)',
            )
        try:
            outcome = self._core_handle.task_info(
                task_id,
                include_result=include_result,
                include_failed_reason=include_failed_reason,
                include_attempts=include_attempts,
            )
        except CoreError as exc:
            return _task_info_core_error(task_id, call_refusal_or_raise(exc))
        return _task_info_result(outcome)

    def close(self) -> BrokerResult[None]:
        """The blocking form of ``close_async``: the core handle's close
        (``CoreHandle.close``) on the calling thread, with the GIL released.
        Then it stops the sync runner, which does not start again.

        Resource sets and a sync send engine that earlier calls opened close
        by the rules of ``close_async``, on an event loop that this call runs
        for them; with none, no event loop runs. The core handle's writes in
        flight end within the core's close bound; a handle that never
        connected closes without starting the core. A raised signal handler
        (Ctrl+C) leaves the close; the core's close runs on to its end.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and closes nothing: no resource set closes and the sync runner keeps
        its state. Use ``await close_async()`` there.

        Raises ``ForkedAfterCoreStart`` and ``CoreStopped`` from the core
        handle's close, after the resource sets closed and the sync runner
        stopped.
        """
        if in_running_event_loop():
            return _async_context_err(
                'PostgresBroker.close()', 'broker.close_async()',
            )
        try:
            taken = self._take_python_resources()
            match taken:
                case _PythonResources(loop_sets=(), sync_send_engine=_SyncSendEngineNotOpen()):
                    errors: list[BaseException] = []
                case _PythonResources():
                    errors = asyncio.run(self._close_python_resources(taken))
            try:
                _ = self._core_handle.close()
            except CoreError as exc:
                # As in close_async: the core of this process cannot run the
                # close, or the close did not end within the core's bound.
                errors.append(call_refusal_or_raise(exc))
            return _close_answer(errors)
        finally:
            self._loop_runner.stop()


