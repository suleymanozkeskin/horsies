# app/core/brokers/postgres.py
from __future__ import annotations
import asyncio, hashlib, contextlib, os, random, threading, time, uuid
from collections.abc import AsyncGenerator, Callable, Coroutine, Sequence
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
    cast,
)
from uuid import UUID
from datetime import datetime, timedelta, timezone
import psycopg
from horsies._catalog.manifest import dump_producer_manifest
from horsies._core import (
    BrokerFailure,
    CoreError,
    CoreHandle,
    DatabasePingRecord,
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
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
)
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, RowMapping
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from pydantic import ValidationError
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
    ENQUEUE_TASK_DRIVER_SQL,
    ENQUEUE_TASK_SQL,
    EnqueueKey,
    EnqueueOutcome,
    EnqueueRow,
    EnqueueStart,
    FirstRun,
    Keyed,
    SendKind,
    StartAfterDelay,
    StartAt,
    StartNow,
    Unkeyed,
    enqueue_result,
    enqueue_task_params,
    read_enqueue_outcome,
)
from horsies.core.brokers.session_capability import probe_session_capability
from horsies.core.brokers.class_coverage_reads import (
    read_class_coverage,
    read_class_report,
)
from horsies.core.brokers.result_types import (
    BrokerAsyncContext,
    BrokerEnqueueError,
    BrokerEnqueueFailed,
    BrokerErrorCode,
    BrokerEventLoopLimit,
    BrokerInvalidInput,
    BrokerInvalidJsonPayload,
    BrokerInvalidTaskOptions,
    BrokerListenerDisabled,
    BrokerLoopRunnerFailed,
    BrokerOperationFailed,
    BrokerOperationFailedCode,
    BrokerResult,
    BrokerSyncCallError,
    BrokerWaitInsideTask,
    RawResultRecord,
)
from horsies.core.manifest import schema_mode_of, to_producer_manifest
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.recovery import OrphanTaskAuditReport
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.workflow_pg import (
    WorkflowModel as _WorkflowModel,
    WorkflowTaskModel as _WorkflowTaskModel,
)
from horsies.core.types.core_failure import (
    CoreLockNotAvailable,
    CoreOperationFailed,
    core_error_retryable,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.status import (
    TASK_TERMINAL_STATES,
    TaskAttemptOutcome,
    TaskStatus,
)
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.codec.json_io import Json, loads_json, dumps_json
from horsies.core.codec.task_options import stored_execution, stored_max_retries
from horsies.core.codec.validation_errors import (
    exception_text,
    validation_summary,
    without_validation_input,
)
from horsies.core.models.tasks import Execution, TaskInfo, TaskAttemptInfo
from horsies.core.models.class_coverage import (
    ClassCoverage,
    ClassReadRefused,
    ClassReport,
)
from horsies.core.models.health import (
    WORKER_PING_CHANNEL,
    DatabasePing,
    WorkerPong,
    WorkerPongPayload,
    WorkerPingRequest,
    WorkerStateSnapshot,
    parse_class_capacity,
    parse_slot_health,
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
from horsies.core.baseline.install import (
    CompleteInstall,
    InstallBaseline,
    RefuseInstall,
    SchemaReady,
    SchemaStatementFailed,
    decide_install,
    install_baseline,
    publish_history_readers,
    read_database_state,
    record_client_family,
)
from horsies.core.baseline.migration import (
    BaselineMigration,
    load_baseline_migration,
)
from horsies.core.defaults import PRE_PING_AFTER_IDLE_S
from horsies.core.utils.db import (
    autocommit_connection,
    is_retryable_connection_error,
    SessionSettings,
    register_identity_text_reads,
    uuid_column_value,
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
    LoopRunnerError,
    async_context_message,
    in_running_event_loop,
)
from horsies.core.utils.task_loop_registry import is_task_loop
from horsies.core.utils.url import sqlalchemy_url, to_psycopg_url
from horsies.core.utils.wait_timeout import wait_timeout_seconds

if TYPE_CHECKING:
    from horsies.core.app import Horsies
from horsies.core.logging import get_logger

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


def _enqueue_failed(message: str, exc: BaseException) -> Err[BrokerEnqueueFailed]:
    """Build a ``BrokerEnqueueFailed`` with retryable classification from the raw exception.

    The write may or may not have happened. A pydantic ``ValidationError``
    cause is stored in its form without input values.
    """
    return Err(
        BrokerEnqueueFailed(
            message=message,
            retryable=is_retryable_connection_error(exc),
            exception=without_validation_input(exc),
        )
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
    kwargs_json: str | None,
    good_until: datetime | None,
    enqueue_delay_seconds: int | None,
    task_options: str | None,
    retention_class_key: str,
    retain_rerun_input: bool,
) -> Result[SendCommand, BrokerInvalidInput | BrokerInvalidTaskOptions]:
    """Check the input and the options of one send and build its command. Pure.

    Refusals, in this order: the kwargs text (``INVALID_INPUT``), the
    options text (``INVALID_TASK_OPTIONS``), a value the fingerprint
    refuses (``INVALID_INPUT``).
    """
    input_r = _parse_enqueue_input(task_name, kwargs_json)
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

    The input travels in the fingerprint's kwargs element as the stored
    ``input`` text, and the args element is always null: a Python send has
    no positional arguments. The fingerprint is therefore a function of the
    stored row. A value that the fingerprint refuses is ``INVALID_INPUT``.
    """
    try:
        command = EnqueueCommandV1(
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            args_json=None,
            kwargs_json=stored_input,
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
    No options store no retries and ``ISOLATED``.
    """
    if not task_options:
        return Ok(_EnqueueOptions(max_retries=0, execution=Execution.ISOLATED))
    opts_r = loads_json(task_options)
    if is_err(opts_r):
        return Err(BrokerInvalidTaskOptions(
            message=f'task_options JSON corrupt: {opts_r.err_value}',
            exception=opts_r.err_value,
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

    Empty text is refused: it is not JSON, and ``loads_json`` reads it as null.
    """
    if not text:
        return Err(BrokerInvalidInput(
            message=f'{label} JSON corrupt for {task_name}: empty text',
            exception=NoCause(),
        ))
    loaded = loads_json(text)
    if is_err(loaded):
        return Err(BrokerInvalidInput(
            message=f'{label} JSON corrupt for {task_name}: {loaded.err_value}',
            exception=loaded.err_value,
        ))
    return loaded


def _check_enqueue_args(
    task_name: str, args_json: str | None,
) -> Result[None, BrokerInvalidInput]:
    """Refuse args text that no worker can run.

    The worker takes only a null or an empty array, so anything else is
    refused here and not at run time. The text names no input value.
    """
    if args_json is None:
        return Ok(None)
    parsed = _parse_enqueue_json('args', task_name, args_json)
    if is_err(parsed):
        return parsed
    match parsed.ok_value:
        case None | []:
            return Ok(None)
        case other:
            return Err(BrokerInvalidInput(
                message=(
                    f'args JSON for {task_name} must be null or an empty array, '
                    f'got {type(other).__name__}; positional args are not supported'
                ),
                exception=NoCause(),
            ))


EMPTY_TASK_INPUT: Final = '{}'
"""The ``input`` of a send with no keyword arguments."""


def _parse_enqueue_input(
    task_name: str, kwargs_json: str | None,
) -> Result[str, BrokerInvalidInput]:
    """Check the kwargs text and give the ``input`` text that the row stores.

    An object is stored exactly as sent, because the command fingerprint
    covers its bytes. Absent kwargs and JSON null store ``EMPTY_TASK_INPUT``:
    a Python send stores its keyword arguments as one JSON object. Anything
    else is refused; the refusal text names the JSON type, not the value.
    """
    if kwargs_json is None:
        return Ok(EMPTY_TASK_INPUT)
    parsed = _parse_enqueue_json('kwargs', task_name, kwargs_json)
    if is_err(parsed):
        return parsed
    match parsed.ok_value:
        case None:
            return Ok(EMPTY_TASK_INPUT)
        case dict():
            return Ok(kwargs_json)
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


def _loop_limit_err(
    operation: str, loop_limit: EventLoopLimit,
) -> Err[BrokerEventLoopLimit]:
    """Build the broker error for the event loop limit. Nothing ran."""
    return Err(
        BrokerEventLoopLimit(
            message=f'{operation}: {loop_limit.describe()}',
            loop_limit=loop_limit,
        )
    )


def _class_read_refused(
    operation: str,
    refused: ClassReadRefused,
) -> Err[BrokerOperationFailed]:
    """Stored data that a class read cannot decide on, or a bound it reached.

    Not retryable: the same rows give the same answer. Nothing changed.
    """
    message = f'{operation} refused: {refused.reason}'
    return _operation_failed(
        BrokerErrorCode.MONITORING_QUERY_FAILED,
        message,
        ValueError(message),
    )


def _loop_runner_err(
    sync_call: str, exc: LoopRunnerError,
) -> Err[BrokerLoopRunnerFailed]:
    """Build the broker error for a sync facade whose runner could not run the call.

    Nothing ran. Not retryable: a stopped runner does not start again.
    """
    return Err(
        BrokerLoopRunnerFailed(
            message=f'Loop runner failed for {sync_call}: {exc}',
            exception=exc,
        )
    )


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


def _dbapi_sqlstate(exc: BaseException) -> str | None:
    orig = getattr(exc, 'orig', exc)
    sqlstate = getattr(orig, 'sqlstate', None)
    if isinstance(sqlstate, str):
        return sqlstate
    pgcode = getattr(orig, 'pgcode', None)
    if isinstance(pgcode, str):
        return pgcode
    return None


_SCHEMA_INIT_MAX_ATTEMPTS = 5
_SCHEMA_INIT_DEADLOCK_SQLSTATE = '40P01'
_UNDEFINED_FUNCTION_SQLSTATE = '42883'
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

TASK_LIST_ROW_BOUND: Final = 10_000
"""The most rows that ``get_stale_tasks`` and ``get_expired_tasks`` return: the
row bound of ``horsies_list_stale_tasks`` and ``horsies_list_expired_tasks``."""

MAX_STALE_THRESHOLD_MINUTES: Final = 525_600
"""The largest ``stale_threshold_minutes`` of ``get_stale_tasks``: one year
of 365 days."""

# (dict key, result column) of each list row, in the order of the dict: the
# keys of the client statements that the list functions replaced.
_STALE_TASK_KEYS: Final = (
    ('id', 'task_id'),
    ('worker_hostname', 'worker_hostname'),
    ('worker_pid', 'worker_pid'),
    ('worker_process_name', 'worker_process_name'),
    ('last_heartbeat', 'last_heartbeat'),
    ('started_at', 'started_at'),
    ('task_name', 'task_name'),
)
_EXPIRED_TASK_KEYS: Final = (
    ('id', 'task_id'),
    ('task_name', 'task_name'),
    ('queue_name', 'queue_name'),
    ('priority', 'priority'),
    ('sent_at', 'sent_at'),
    ('enqueued_at', 'enqueued_at'),
    ('good_until', 'good_until'),
    ('expired_for', 'expired_for'),
)


@dataclass(frozen=True, slots=True)
class _CompleteTaskList:
    """Every matching row."""

    rows: list[dict[str, Any]]


@dataclass(frozen=True, slots=True)
class _TruncatedTaskList:
    """The first ``TASK_LIST_ROW_BOUND`` matching rows; more rows match."""

    rows: list[dict[str, Any]]


type _BoundedTaskList = _CompleteTaskList | _TruncatedTaskList


def _task_list_dict(
    item: RowMapping, keys: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    """One TASK row of a list function as the dict of the public API. Pure.

    The ``id`` value is the ``uuid.UUID`` of the public API.
    """
    entry: dict[str, Any] = {key: item[column] for key, column in keys}
    entry['id'] = uuid_column_value(entry['id'], column='task_id')
    return entry


def _function_task_list(
    rows: Sequence[RowMapping],
    keys: tuple[tuple[str, str], ...],
) -> _BoundedTaskList:
    """The TASK rows of a list function as dicts, and whether it cut the list. Pure.

    ``keys`` maps each dict key to its result column, in dict order. The
    last row is the LIST_END row: ``TRUNCATED`` means the function cut the
    list at ``TASK_LIST_ROW_BOUND`` rows.
    """
    match rows:
        case [*items, end]:
            pass
        case _:
            raise AssertionError('the list function answered no LIST_END row')
    assert end['row_kind'] == 'LIST_END', end['row_kind']
    assert end['row_bound'] == TASK_LIST_ROW_BOUND, end['row_bound']
    assert all(item['row_kind'] == 'TASK' for item in items), 'a row before LIST_END is not a TASK row'
    dicts = [_task_list_dict(item, keys) for item in items]
    match end['list_state']:
        case 'COMPLETE':
            return _CompleteTaskList(rows=dicts)
        case 'TRUNCATED':
            return _TruncatedTaskList(rows=dicts)
        case other:
            raise AssertionError(f'list state {other!r} is not COMPLETE or TRUNCATED')


def _stale_threshold_minutes(value: object) -> Result[int, BrokerInvalidInput]:
    """Check the stale threshold: whole minutes from 0 to
    ``MAX_STALE_THRESHOLD_MINUTES``. Pure.

    The refusal text holds no value, because an int with more than 4300
    digits cannot be converted to a string.
    """
    match value:
        case bool():
            cause = 'is a bool, not an int'
        case int() if value < 0:
            cause = 'is below 0'
        case int() if value > MAX_STALE_THRESHOLD_MINUTES:
            cause = f'is above {MAX_STALE_THRESHOLD_MINUTES} (one year)'
        case int():
            return Ok(value)
        case _:
            cause = f'is a {type(value).__name__}, not an int'
    return Err(BrokerInvalidInput(
        message=(
            f'stale_threshold_minutes {cause}; give whole minutes from 0 '
            f'to {MAX_STALE_THRESHOLD_MINUTES}'
        ),
        exception=NoCause(),
    ))


LIST_STALE_TASKS_SQL = text("""
    SELECT row_kind, task_id, worker_hostname, worker_pid, worker_process_name,
           last_heartbeat, started_at, task_name, list_state, row_bound
    FROM horsies_list_stale_tasks(CAST(:threshold_minutes AS integer))
    ORDER BY row_position NULLS LAST
""")

# Latest snapshot per worker (includes idle workers). The retired
# get_worker_stats only saw workers with RUNNING tasks; this reads the
# worker-states timeseries instead. DISTINCT ON keeps the newest row per
# worker_id.
_WORKER_STATE_COLUMNS = """
        worker_id,
        snapshot_at,
        hostname,
        pid,
        processes,
        max_claim_batch,
        max_claim_per_worker,
        cluster_wide_cap,
        queues,
        queue_priorities,
        queue_max_concurrency,
        recovery_config,
        tasks_running,
        tasks_claimed,
        memory_usage_mb,
        memory_percent,
        cpu_percent,
        children_memory_mb,
        worker_started_at,
        slots_down,
        class_capacity
"""

# Recursive skip-scan: one (worker_id, snapshot_at DESC) index probe per
# worker instead of a DISTINCT ON pass over the whole snapshot timeseries.
# The timeseries holds days of per-worker rows (5s-30s cadence), so the
# DISTINCT ON form reads every retained row to return one per worker —
# measured at 10s on a 118k-row production table.
LIST_WORKER_STATES_SQL = text(f"""
    WITH RECURSIVE latest AS (
        (
            SELECT
{_WORKER_STATE_COLUMNS}
            FROM horsies_worker_states
            ORDER BY worker_id, snapshot_at DESC
            LIMIT 1
        )
        UNION ALL
        SELECT nxt.* FROM latest l
        CROSS JOIN LATERAL (
            SELECT
{_WORKER_STATE_COLUMNS}
            FROM horsies_worker_states w
            WHERE w.worker_id > l.worker_id
            ORDER BY w.worker_id, w.snapshot_at DESC
            LIMIT 1
        ) nxt
    )
    SELECT * FROM latest
""")

GET_WORKER_STATE_LATEST_SQL = text(f"""
    SELECT
{_WORKER_STATE_COLUMNS}
    FROM horsies_worker_states
    WHERE worker_id = :worker_id
    ORDER BY snapshot_at DESC
    LIMIT 1
""")

# History for a single worker, newest first. ``:limit`` of NULL returns all
# retained rows; callers pass an explicit cap to bound the fetch.
GET_WORKER_STATE_HISTORY_SQL = text(f"""
    SELECT
{_WORKER_STATE_COLUMNS}
    FROM horsies_worker_states
    WHERE worker_id = :worker_id
    ORDER BY snapshot_at DESC
    LIMIT :limit
""")

LIST_EXPIRED_TASKS_SQL = text("""
    SELECT row_kind, task_id, task_name, queue_name, priority, sent_at, enqueued_at,
           good_until, expired_for, list_state, row_bound
    FROM horsies_list_expired_tasks()
    ORDER BY row_position NULLS LAST
""")

# ---- Cleanup queries ----

# The pending expiry and the orphan audit run in the core
# (`expire_pending_tasks`, `audit_orphaned_workflow_tasks`) with the same batch
# sizes and bound. The bounds and the cursor read stay here for the Python
# reference of the dual-run (tests/dualrun/reference/expiry_and_orphans_caller.py).
_EXPIRE_BATCH_SIZE = 500
# Backstop against an unbounded pass; the remainder expires next interval.
_EXPIRE_MAX_BATCHES_PER_PASS = 200
_ORPHAN_BATCH_SIZE = 500

_ORPHAN_AUDIT_SUBJECT: Final = 'PostgresBroker.audit_orphaned_workflow_tasks'

READ_ORPHAN_TASK_SCAN_STATS_SQL = text("""
SELECT last_scan_rows, last_candidate_rows
FROM horsies_recovery_scan_cursors
WHERE scan_name = 'orphan_workflow_tasks'
""")

SELECT_TASK_ATTEMPTS_BY_TASK_ID_SQL = text("""
    SELECT task_id, attempt, outcome, will_retry,
           started_at, finished_at,
           error_code, error_message, failed_reason,
           worker_id, worker_hostname, worker_pid, worker_process_name
    FROM horsies_task_attempts
    WHERE task_id = :task_id
    ORDER BY attempt DESC
""")

# Slim status probe for result-wait polling: the full row drags the
# (potentially TOASTed) input column along on every poll iteration;
# status+name is all the loop needs. A live row holds no result: the result
# is stored with the terminal record in history.
GET_TASK_STATUS_NAME_SQL = text("""
    SELECT status, task_name FROM horsies_tasks WHERE id = :id
""")

# Sentinel returned by _probe_result_row while the row is non-terminal.
_STILL_WAITING = object()

# One bounded batch of the stale CLAIMED requeue, one statement in autocommit.
# The function requeues a CLAIMED row with no claimer beat newer than the
# threshold that was claimed longer ago than the threshold, skips a workflow
# task with no runnable node and rows another transaction holds, and returns
# the number of rows it requeued. The core runs the pass with the same batch
# size and bound (`requeue_stale_claimed`); the statement and the two bounds
# stay here for the Python reference of the dual-run
# (tests/dualrun/reference/requeue_caller.py).
REQUEUE_STALE_CLAIMS_SQL = text("""
    SELECT horsies_requeue_stale_claims(
        CAST(:stale_after_ms AS INTEGER), CAST(:batch_size AS INTEGER)
    )
""")

REQUEUE_STALE_CLAIMS_BATCH_SIZE: Final = 500
REQUEUE_STALE_CLAIMS_MAX_BATCHES_PER_PASS: Final = 200
"""Backstop against an unbounded pass; the remainder is requeued next pass."""

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


def _task_info_core_error(task_id: UUID, exc: CoreError) -> Err[BrokerOperationFailed]:
    """The core of this process cannot run a task info read: it stopped, did
    not start, has too many calls in flight, or a blocking call reached its
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
    Python read of a stored result before the switch.
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
    task_id: UUID, outcome: TaskInfoRecord | TaskInfoNotFound | BrokerFailure,
) -> BrokerResult[TaskInfo | None]:
    """The result of one core task info read. Pure.

    - ``TaskInfoRecord``: ``Ok(TaskInfo)``, or ``INVALID_JSON_PAYLOAD`` for a
      stored result that does not read.
    - ``TaskInfoNotFound``: ``Ok(None)``.
    - ``BrokerFailure(TASK_INFO_QUERY_FAILED)``: the connect, the install
      or a read failed; ``retryable`` is the core's classification.
    - ``BrokerFailure(INVALID_JSON_PAYLOAD)``: the core found stored data
      that does not read, for example a history result whose digest
      differs; not retryable.

    Each failure's ``exception`` is a ``CoreOperationFailed``. Another code
    contradicts the core's catalog of the read: ``AssertionError``.
    """
    match outcome:
        case TaskInfoRecord() as record:
            return _task_info_of_record(record)
        case TaskInfoNotFound():
            return Ok(None)
        case BrokerFailure(
            code='TASK_INFO_QUERY_FAILED', message=message, retryable=retryable,
        ):
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.TASK_INFO_QUERY_FAILED,
                    message=f'get_task_info failed for {task_id}: {message}',
                    retryable=retryable,
                    exception=CoreOperationFailed(
                        'PostgresBroker.get_task_info_async', message, retryable,
                    ),
                ),
            )
        case BrokerFailure(code='INVALID_JSON_PAYLOAD', message=message):
            return Err(
                BrokerInvalidJsonPayload(
                    message=f'get_task_info failed for {task_id}: {message}',
                    exception=CoreOperationFailed(
                        'PostgresBroker.get_task_info_async', message, False,
                    ),
                ),
            )
        case BrokerFailure(code=code):
            raise AssertionError(f'the core task info read answered the code {code}')
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
      - Sync: enqueue(), get_raw_result_record() (run in background loop)

    Features:
      - Real-time notifications via PostgreSQL triggers
      - Automatic task status tracking
      - Connection pooling and health monitoring
      - Operational monitoring (stale tasks, worker stats)
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
        # The engine of the sync sends that run on the caller's thread
        # (send_route), opened by the first such send.
        self._sync_send_engine: _SyncSendEngineNotOpen | _SyncSendEngineOpen = (
            _SyncSendEngineNotOpen()
        )
        self._sync_send_lock = threading.Lock()
        # True once this broker saw the staged result function. Only a
        # positive answer is kept: until a worker publishes the readers,
        # each result probe checks again.
        self._staged_result_reader_published = False
        self._loop_runner = LoopRunner()  # for sync facades
        # The operations that run in the core use the pools of this handle.
        # It is built from the same config as the engines, so its pools have
        # their shape. Building it starts nothing: the first core operation
        # connects, and close_async closes its pools.
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
        )

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
                        to_psycopg_url(session_url),
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

        Every sync facade calls this, so a broker has one sync event loop.
        It blocks the calling thread, so it must not run inside an event loop.
        Every sync facade returns ``ASYNC_CONTEXT`` before it calls this on
        a thread that runs an event loop, so a call from such a thread is a
        defect: a facade without that check.

        Raises:
            AssertionError: the calling thread runs an event loop. Nothing
                ran, and the runner was not started.
            LoopRunnerError: the runner is stopped, its thread did not
                start, or scheduling onto its loop failed. Nothing ran.
                Every sync facade returns this as its ``LOOP_RUNNER_FAILED``
                error.
            Exception: the coroutine's own exception, unchanged. Every sync
                facade lets it propagate, as the async form does.
        """
        if in_running_event_loop():
            raise AssertionError(
                f'PostgresBroker.run_sync({coro_fn.__name__}) was called on '
                f'thread {threading.current_thread().name!r}, which runs an '
                'event loop; every sync facade must return ASYNC_CONTEXT '
                'before it calls run_sync',
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

    def _schema_engine_config(self) -> dict[str, Any]:
        return self._session_engine_config()

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

    async def validate_worker_database_paths(self) -> None:
        """Connect both worker paths and prove session capability."""
        match await self.select_resources():
            case Err(loop_limit):
                raise AssertionError(
                    f'worker broker resources: {loop_limit.describe()}'
                )
            case Ok(resources):
                pass
        async with resources.engine.connect() as connection:
            await connection.execute(text('SELECT 1'))
        async with resources.partition_maintenance.connect() as connection:
            await connection.execute(text('SELECT 1'))
        try:
            await probe_session_capability(
                to_psycopg_url(self.config.effective_session_database_url)
            )
        except (asyncio.TimeoutError, psycopg.Error) as exc:
            raise ConfigurationError(
                message='broker session capability check failed',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    'session_database_url appears to be transaction-pooled; '
                    'LISTEN notification was not delivered',
                ],
                help_text=(
                    'use a direct/session-capable Postgres URL for '
                    'session_database_url'
                ),
            ) from exc

    def _schema_advisory_key(self) -> int:
        """
        Compute a stable 64-bit advisory lock key for schema initialization.

        PostgreSQL advisory locks are scoped to the current database, so this
        URL-independent key serializes Horsies schema initializers per database.
        """
        h = hashlib.sha256(b'horsies:schema:v1').digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    async def _run_with_schema_engine(
        self, resources: BrokerResources, fn: Any,
    ) -> None:
        await fn(resources.session_async_engine())

    async def _maybe_run_schema_init(self, engine: AsyncEngine) -> None:
        """Install the baseline schema when this broker runs migrations.

        The Python install before the switch to the core: the broker installs
        through ``_ensure_initialized``, and only the dual-run reference
        (``tests/dualrun/reference/install_caller.py``) runs this path.

        ``engine`` is the session URL's engine: the reader publication runs
        on it. The state read, the lock and the migration use their own
        direct session connection.

        A broker with migrations off runs no schema statement. Read-only
        consumers such as the monitoring service inspect the schema with their
        own probe and never change it.
        """
        if self.run_schema_migrations:
            await self._run_fleet_schema_init(engine)

    async def _run_fleet_schema_init(self, engine: AsyncEngine) -> None:
        """Install the baseline into an empty database, or check an installed one.

        Returns after the published history readers exist.

        Raises:
            RuntimeError: the database holds something other than an empty
                schema or this build's baseline; the text names what it holds.
                Nothing was changed.
            psycopg.Error: a statement failed (after the deadlock retries).
        """
        migration = load_baseline_migration()
        for attempt in range(1, _SCHEMA_INIT_MAX_ATTEMPTS + 1):
            match await self._install_once(migration, engine):
                case Ok():
                    return
                case Err(failure):
                    pass
            match failure:
                case RefuseInstall(cause=refusal):
                    raise RuntimeError(refusal)
                case SchemaStatementFailed(step=step, cause=cause):
                    if (
                        cause.sqlstate != _SCHEMA_INIT_DEADLOCK_SQLSTATE
                        or attempt == _SCHEMA_INIT_MAX_ATTEMPTS
                    ):
                        cause.add_note(f'schema install step: {step}')
                        raise cause
                    backoff = 0.05 + random.uniform(0, 0.2) * (2 ** (attempt - 1))
                    self.logger.warning(
                        'schema init hit deadlock; retrying attempt=%s backoff_s=%.3f',
                        attempt,
                        backoff,
                    )
                    await asyncio.sleep(backoff)

    async def _install_once(
        self, migration: BaselineMigration, engine: AsyncEngine,
    ) -> Result[None, RefuseInstall | SchemaStatementFailed]:
        """One pass: read the state, and only when a write is due, lock and act.

        A database that already holds this build's baseline, its published
        readers and this client's family gets no lock and no write.
        """
        connection = await psycopg.AsyncConnection.connect(
            to_psycopg_url(self.config.effective_session_database_url),
            autocommit=True,
        )
        try:
            first = await read_database_state(connection, migration)
        finally:
            await connection.close()
        match first:
            case Err(failed):
                return Err(failed)
            case Ok(state):
                pass
        match decide_install(state):
            case SchemaReady():
                return Ok(None)
            case RefuseInstall() as refused:
                return Err(refused)
            case InstallBaseline() | CompleteInstall():
                pass
        async with self._schema_session_lock() as locked:
            return await self._install_under_lock(locked, migration, engine)

    async def _install_under_lock(
        self,
        connection: psycopg.AsyncConnection[Any],
        migration: BaselineMigration,
        engine: AsyncEngine,
    ) -> Result[None, RefuseInstall | SchemaStatementFailed]:
        """Read the state again under the schema lock and do what it needs.

        Another installer may have finished while this one waited for the
        lock; the second read sees its work.

        The migration (with the family row) and the reader publication are
        two transactions. An install that stops between them leaves a
        baseline with missing readers; the next install reads that state as
        ``CompleteInstall`` and publishes them.
        """
        match await read_database_state(connection, migration):
            case Err(failed):
                return Err(failed)
            case Ok(state):
                pass
        match decide_install(state):
            case SchemaReady():
                return Ok(None)
            case RefuseInstall() as refused:
                return Err(refused)
            case InstallBaseline():
                self.logger.info(
                    'Installing the baseline schema (migration %s)', migration.version,
                )
                match await install_baseline(connection, migration):
                    case Err(failed):
                        return Err(failed)
                    case Ok():
                        pass
                # The family row committed with the migration, and no other
                # installer writes while this one holds the lock.
                return await publish_history_readers(engine)
            case CompleteInstall():
                match await publish_history_readers(engine):
                    case Err(failed):
                        return Err(failed)
                    case Ok():
                        pass
                match await record_client_family(connection):
                    case Err(failed):
                        return Err(failed)
                    case Ok():
                        pass
        # The family insert may have lost to another client's row: read again.
        match await read_database_state(connection, migration):
            case Err(failed):
                return Err(failed)
            case Ok(after):
                pass
        match decide_install(after):
            case SchemaReady():
                return Ok(None)
            case RefuseInstall() as refused:
                return Err(refused)
            case InstallBaseline() | CompleteInstall():
                raise AssertionError(
                    f'schema state after completing the install: {after!r}'
                )

    @contextlib.asynccontextmanager
    async def _schema_session_lock(
        self,
    ) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
        """Hold the schema lock on one direct physical connection, and yield it.

        The connection is in autocommit mode and holds the session advisory
        lock until the context ends. The wait for the lock has no bound.
        """
        connection = await psycopg.AsyncConnection.connect(
            to_psycopg_url(self.config.effective_session_database_url),
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
        readers. A handle with migrations off or ``assume_initialized`` runs
        no schema statement. After a success the broker does not call the
        core again; a broker built with ``assume_initialized=True`` never
        calls it.

        The ``_maybe_run_schema_init`` path of this class is the Python
        install before the switch; the dual-run reference
        (``tests/dualrun/reference/install_caller.py``) runs it.

        Raises:
            CoreOperationFailed: the core reported ``SCHEMA_INIT_FAILED``;
                ``retryable`` is the core's classification of the cause. The
                broker keeps nothing, so the next call installs again.
            CoreError: the core of this process cannot run the call.
        """
        if self._initialized:
            return
        match await self._core_handle.install_schema_async():
            case SchemaInstalled() | SchemaInstallNotRun():
                self._initialized = True
            case BrokerFailure(code='SCHEMA_INIT_FAILED', message=message, retryable=retryable):
                raise CoreOperationFailed(
                    'PostgresBroker.ensure_schema_initialized', message, retryable,
                )
            case BrokerFailure(code=code):
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
        install failed (``exception`` is a ``CoreOperationFailed``), for
        example a database of the other client family or one this build
        cannot install into (not retryable), or the core of this process
        cannot run the call (``exception`` is the ``CoreError``). The install
        runs on the core pools, so ``BrokerEventLoopLimit`` is no longer a
        failure exit. A cancel of the caller does not stop the install.
        """
        try:
            await self._ensure_initialized()
        except (CoreOperationFailed, CoreError) as exc:
            return _operation_failed(
                BrokerErrorCode.SCHEMA_INIT_FAILED,
                f'Schema initialization failed: {exc}',
                exc,
            )
        return Ok(None)

    # ----------------- Async API -----------------

    @staticmethod
    def _prepare_enqueue(
        *,
        task_id: UUID,
        task_name: str,
        queue_name: str,
        priority: int,
        args_json: str | None,
        kwargs_json: str | None,
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
        args_r = _check_enqueue_args(task_name, args_json)
        if is_err(args_r):
            return args_r
        command_r = _send_command(
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            kwargs_json=kwargs_json,
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
        kwargs_json: str | None,
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
            kwargs_json=kwargs_json,
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
        enqueue_sha: str,
        send_kind: SendKind,
        args_json: str | None = None,
        kwargs_json: str | None = None,
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
        """Write one PENDING task row, or answer the send that wrote it.

        One ``horsies_enqueue_task`` statement in autocommit. The row stores
        ``kwargs_json`` as its ``input`` (``{}`` when there are none), the
        version-1 command fingerprint of the send and ``sent_at``, the call
        time of the send. A send that repeats a task id is the same send only
        when the stored fingerprint and ``sent_at`` are equal; a resend
        passes the ``sent_at`` of the first send. ``send_kind`` is
        ``SendKind.REPEAT`` for a send retry: a task that finished then
        answers with its id and does not run again. ``enqueue_sha`` is not
        stored and not compared.
        """
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('enqueue', loop_limit)
            case Ok(resources):
                pass
        # The ratified retention resolution: absent → the immutable
        # 30-day default class; forever only by EXPLICIT None.
        resolved_retention_class_key = resolve_retention_class_key(
            retention_class_key
        )
        # The ratified retention posture: an app-level default with a
        # per-task override, resolved here and snapshotted at enqueue.
        # None means inherit the deployment's standing policy.
        if retain_rerun_input is None:
            retain_rerun_input = self.config.retain_rerun_input_default
        prepared_r = self._prepare_enqueue(
            task_id=task_id,
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            args_json=args_json,
            kwargs_json=kwargs_json,
            sent_at=sent_at,
            enqueued_at=enqueued_at,
            enqueue_delay_seconds=enqueue_delay_seconds,
            good_until=good_until,
            task_options=task_options,
            retention_class_key=resolved_retention_class_key,
            retain_rerun_input=retain_rerun_input,
            idempotency_key=idempotency_key,
            key_window=self.idempotency_reservation_window,
        )
        if is_err(prepared_r):
            return prepared_r
        prepared = prepared_r.ok_value

        # Schema init keeps a broad catch, as ensure_schema_initialized does:
        # it raises RuntimeError for operator states such as an incomplete
        # cutover, and each of those is a failed enqueue, not a crash.
        try:
            await self._ensure_initialized()
        except Exception as exc:
            return _enqueue_failed(
                f'Failed to enqueue task {task_name}: {exc}',
                exc,
            )

        try:
            async with resources.engine.connect() as conn:
                autocommit_conn = await conn.execution_options(
                    isolation_level='AUTOCOMMIT',
                )
                result = await autocommit_conn.execute(
                    ENQUEUE_TASK_SQL,
                    enqueue_task_params(prepared.row, send_kind),
                )
                rows = result.fetchall()
        except _DRIVER_ERRORS as exc:
            return _enqueue_failed(
                f'Failed to enqueue task {task_name}: {exc}',
                exc,
            )
        return enqueue_result(
            read_enqueue_outcome(rows),
            task_id=task_id,
            task_name=task_name,
        )

    def _build_raw_result_record(
        self,
        task_id: UUID,
        task_name: str,
        status: TaskStatus,
        result_json: 'str | None',
    ) -> BrokerResult[RawResultRecord | None]:
        """Decode the result column into a ``RawResultRecord``.

        Returns ``Err(INVALID_JSON_PAYLOAD)`` when the stored JSON is
        malformed (parser failure, NaN / Infinity rejected by the
        parse-constant guard) or when the top-level payload is neither
        ``None`` nor a JSON object (the envelope grammar requires a
        ``dict``). Otherwise wraps the loaded value into the record.
        """
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

    async def _history_result_record(
        self,
        session: AsyncSession,
        task_id: UUID,
    ) -> 'BrokerResult[RawResultRecord | None] | object':
        """Resolve a result from the task-history side.

        Terminalization moves the finished row out of ``horsies_tasks``,
        so an absent (or vanished-between-statements) live row is the
        NORMAL terminal signal, not an error. Resolution rides the
        staged detail function — location then detail, one statement.
        Returns ``Ok(None)`` when history does not hold the task, and
        ``_STILL_WAITING`` when the row is reported live (it appeared
        after the live probe missed it). The install and every coverage
        pass publish the detail reader, so no publication probe runs; a
        reader removed out of band raises ``42883`` naming it.
        """
        from horsies.core.history.reads.detail import (
            HistoryTaskDetail,
            TaskDetailAbsent,
            read_task_detail,
        )

        connection = await session.connection()
        detail = await read_task_detail(connection, task_id=task_id)
        match detail:
            case TaskDetailAbsent():
                return Ok(None)
            case HistoryTaskDetail():
                pass
            case _:
                # Reported live: it appeared after the live probe
                # missed it — keep polling.
                return _STILL_WAITING
        return self._history_raw_result_record(
            task_id,
            detail.task_name,
            detail.status,
            detail.result_payload,
            detail.result_digest,
        )

    def _history_raw_result_record(
        self,
        task_id: UUID,
        task_name: str,
        status: TaskStatus,
        payload: bytes | None,
        digest: bytes | None,
    ) -> BrokerResult[RawResultRecord | None]:
        """Verify and decode a stored history result payload.

        No payload gives a record without a result. A digest that is
        absent or differs from the SHA-256 of the payload, or a payload
        that is not UTF-8, gives ``Err(INVALID_JSON_PAYLOAD)``.
        """
        if payload is None:
            return Ok(RawResultRecord(
                task_id=task_id,
                task_name=task_name,
                status=status,
                raw_result=None,
            ))
        # Digest over the exact stored bytes BEFORE parsing — the
        # envelope discipline; a mismatch is corruption, not a payload.
        if digest is None or hashlib.sha256(payload).digest() != digest:
            return Err(BrokerInvalidJsonPayload(
                message=(
                    f'History result digest mismatch for task {task_id}'
                ),
                exception=NoCause(),
            ))
        try:
            result_text = payload.decode('utf-8')
        except UnicodeDecodeError as decode_error:
            return Err(BrokerInvalidJsonPayload(
                message=(
                    f'History result payload for task {task_id} is not '
                    f'valid UTF-8: {decode_error}'
                ),
                exception=decode_error,
            ))
        return self._build_raw_result_record(
            task_id,
            task_name,
            status,
            result_text,
        )

    async def _probe_result_row(
        self,
        session: AsyncSession,
        task_id: UUID,
        *,
        non_terminal_snapshot: bool = False,
    ) -> 'BrokerResult[RawResultRecord | None] | object':
        """Result probe for the result-wait loop.

        One statement through the staged result function. While that
        function is not published, the live probe and the staged detail
        read run instead. An absence from the function is confirmed
        through the detail reader: a publisher of an earlier release can
        add a leaf that this function does not probe yet.

        Returns the loop's final ``BrokerResult`` when the wait is over,
        or the ``_STILL_WAITING`` sentinel to keep polling. With
        ``non_terminal_snapshot=True`` (timeout path) a non-terminal row is
        returned as a payload-less record instead of the sentinel.
        """
        from horsies.core.history.reads.result import (
            HistoryResultAbsent,
            HistoryResultStored,
            HistoryTaskResult,
            LiveTaskResult,
            TaskResultAbsent,
            read_task_result,
            staged_result_published,
        )

        connection = await session.connection()
        if not self._staged_result_reader_published:
            if not await staged_result_published(connection):
                return await self._probe_live_result_row(
                    session,
                    task_id,
                    non_terminal_snapshot=non_terminal_snapshot,
                )
            self._staged_result_reader_published = True
        try:
            read = await read_task_result(connection, task_id=task_id)
        except DBAPIError as error:
            if _dbapi_sqlstate(error) == _UNDEFINED_FUNCTION_SQLSTATE:
                # Dropped after this broker saw it: check again next time.
                self._staged_result_reader_published = False
            raise
        match read:
            case LiveTaskResult(task_name=task_name):
                status = TaskStatus(read.status)
                match status:
                    case (
                        TaskStatus.COMPLETED
                        | TaskStatus.FAILED
                        | TaskStatus.EXPIRED
                    ):
                        # A live row stores no result.
                        return self._build_raw_result_record(
                            task_id, task_name, status, None,
                        )
                    case _ if (
                        status is TaskStatus.CANCELLED or non_terminal_snapshot
                    ):
                        return Ok(RawResultRecord(
                            task_id=task_id,
                            task_name=task_name,
                            status=status,
                            raw_result=None,
                        ))
                    case _:
                        return _STILL_WAITING
            case HistoryTaskResult(task_name=task_name, payload=payload):
                status = TaskStatus(read.status)
                match payload:
                    case HistoryResultAbsent():
                        return self._history_raw_result_record(
                            task_id, task_name, status, None, None,
                        )
                    case HistoryResultStored(payload=stored, digest=digest):
                        return self._history_raw_result_record(
                            task_id, task_name, status, stored, digest,
                        )
                    case _:
                        assert_never(payload)
            case TaskResultAbsent():
                return await self._history_result_record(session, task_id)
            case _:
                assert_never(read)

    async def _probe_live_result_row(
        self,
        session: AsyncSession,
        task_id: UUID,
        *,
        non_terminal_snapshot: bool = False,
    ) -> 'BrokerResult[RawResultRecord | None] | object':
        """Slim status probe for the result-wait loop.

        Polls status+name only. A live row holds no result: the result is
        stored with the terminal record in history, so a live row that is
        absent falls through to the history side, because terminalization
        DELETES the live row as it records the terminal fact. A live row
        with a terminal status has no result to read and gives a
        payload-less record.

        Returns the loop's final ``BrokerResult`` when the wait is over,
        or the ``_STILL_WAITING`` sentinel to keep polling. With
        ``non_terminal_snapshot=True`` (timeout path) a non-terminal row is
        returned as a payload-less record instead of the sentinel.
        """
        probe = await session.execute(
            GET_TASK_STATUS_NAME_SQL, {'id': str(task_id)},
        )
        probe_row = probe.fetchone()
        if probe_row is None:
            return await self._history_result_record(session, task_id)
        status = TaskStatus(str(probe_row.status))
        if status in TASK_TERMINAL_STATES or non_terminal_snapshot:
            return Ok(RawResultRecord(
                task_id=task_id,
                task_name=str(probe_row.task_name),
                status=status,
                raw_result=None,
            ))
        return _STILL_WAITING

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
          classification, ``exception`` a ``CoreOperationFailed``), or the
          core of this process cannot run the call (``exception`` is the
          ``CoreError``).
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
            return self._raw_result_core_error(task_id, exc)
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
            return self._raw_result_core_error(task_id, exc)
        return self._raw_result_of_outcome(task_id, outcome)

    def _raw_result_core_error(
        self, task_id: UUID, exc: CoreError,
    ) -> Err[BrokerOperationFailed]:
        """The core of this process cannot run a result read: it stopped,
        did not start, has too many calls in flight, or a blocking call
        reached its bound. Logged on ``horsies.broker``; nothing changed."""
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
        outcome: RawResultRecordRecord | RawResultAbsent | BrokerFailure,
    ) -> BrokerResult[RawResultRecord | None]:
        """The result of one core result read.

        - ``RawResultRecordRecord``: ``Ok(RawResultRecord)``; ``result_json``
          is read with the strict JSON reader (``INVALID_JSON_PAYLOAD`` when
          it does not read as an object or null).
        - ``RawResultAbsent``: ``Ok(None)``.
        - ``BrokerFailure(INVALID_JSON_PAYLOAD)``: ``BrokerInvalidJsonPayload``.
        - ``BrokerFailure(TASK_INFO_QUERY_FAILED)``: ``BrokerOperationFailed``,
          logged on ``horsies.broker``.

        Each failure's ``exception`` is a ``CoreOperationFailed``. Another
        code contradicts the core's catalog of the read, for example
        ``INVALID_INPUT`` for a limit that the check above admitted:
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
            case BrokerFailure(code='INVALID_JSON_PAYLOAD', message=message):
                return Err(BrokerInvalidJsonPayload(
                    message=message,
                    exception=CoreOperationFailed(
                        'PostgresBroker.get_raw_result_record_async', message, False,
                    ),
                ))
            case BrokerFailure(
                code='TASK_INFO_QUERY_FAILED', message=message, retryable=retryable,
            ):
                self.logger.error(
                    'Broker error while retrieving raw task result record for task %s: %s',
                    task_id,
                    message,
                )
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.TASK_INFO_QUERY_FAILED,
                        message=f'Broker error while retrieving task {task_id}: {message}',
                        retryable=retryable,
                        exception=CoreOperationFailed(
                            'PostgresBroker.get_raw_result_record_async', message, retryable,
                        ),
                    ),
                )
            case BrokerFailure(code=code):
                raise AssertionError(f'the core result read answered the code {code}')
            case _ as unreachable:
                assert_never(unreachable)

    async def _unsubscribe_task_done_safely(
        self, listener: PostgresListener, task_id: UUID, q: asyncio.Queue[Any],
    ) -> None:
        """Ensure task_done unsubscribe completes even under repeated cancellation."""
        unsubscribe_task = asyncio.create_task(
            listener.unsubscribe_payload('task_done', str(task_id), q)
        )
        cancelled_during_cleanup = False
        while not unsubscribe_task.done():
            try:
                await asyncio.shield(unsubscribe_task)
            except asyncio.CancelledError:
                cancelled_during_cleanup = True
                continue

        await unsubscribe_task

        # Propagate cancellation only after cleanup has completed.
        if cancelled_during_cleanup:
            raise asyncio.CancelledError

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
        """
        current_loop = asyncio.get_running_loop()
        with self._loop_sets_lock:
            loop_sets = self._loop_sets
            self._loop_sets = ()
        per_set_errors = await asyncio.gather(
            *(self._close_loop_set(entry, current_loop) for entry in loop_sets),
        )
        errors: list[BaseException] = [
            error for set_errors in per_set_errors for error in set_errors
        ]
        with self._sync_send_lock:
            sync_send_engine = self._sync_send_engine
            self._sync_send_engine = _SyncSendEngineNotOpen()
        match sync_send_engine:
            case _SyncSendEngineOpen(engine=engine):
                try:
                    await asyncio.to_thread(engine.dispose)
                except _DRIVER_ERRORS as exc:
                    errors.append(exc)
            case _SyncSendEngineNotOpen():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        try:
            _ = await self._core_handle.close_async()
        except CoreError as exc:
            # The core of this process cannot run the close: it stopped,
            # failed to start, or this process is a fork of the one that
            # started it.
            errors.append(exc)
        # If sync APIs started the LoopRunner, close_async should also tear it down
        # when called from any thread except the loop-runner thread itself.
        # stop() joins the runner thread (up to 2 s), so it runs on a worker
        # thread and this loop keeps running during the join.
        loop_thread = self._loop_runner._thread  # pyright: ignore[reportPrivateUsage]
        if (
            self._loop_runner._started  # pyright: ignore[reportPrivateUsage]
            and loop_thread is not None
            and threading.current_thread() is not loop_thread
        ):
            try:
                await asyncio.to_thread(self._loop_runner.stop)
            except Exception as exc:
                errors.append(exc)
        if errors:
            return _operation_failed(
                BrokerErrorCode.CLOSE_FAILED,
                f'Close failed ({len(errors)} error(s)): {errors[0]}',
                errors[0],
            )
        return Ok(None)

    # ------------- Operational & Monitoring Methods -------------

    async def get_stale_tasks(
        self,
        stale_threshold_minutes: int = 2,
    ) -> BrokerResult[list[dict[str, Any]]]:
        """RUNNING tasks whose latest runner heartbeat, or their start when
        they have no heartbeat, is older than the threshold. Reads only.

        Rows come ordered by the latest heartbeat, rows with none first, then
        by id. At most ``TASK_LIST_ROW_BOUND`` rows come back; when more rows
        match, the first ``TASK_LIST_ROW_BOUND`` come back and a warning is
        logged.

        - ``Err(INVALID_INPUT)``: ``stale_threshold_minutes`` is not an int
          from 0 to ``MAX_STALE_THRESHOLD_MINUTES``. Nothing ran.
        - ``Err(MONITORING_QUERY_FAILED)``: the read failed.
        """
        match _stale_threshold_minutes(stale_threshold_minutes):
            case Err() as refused:
                return refused
            case Ok(ok_value=threshold):
                pass
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('get_stale_tasks', loop_limit)
            case Ok(resources):
                pass
        try:
            async with resources.session_factory() as session:
                connection = await autocommit_connection(session)
                result = await connection.execute(
                    LIST_STALE_TASKS_SQL, {'threshold_minutes': threshold},
                )
                rows = result.mappings().all()
        except Exception as exc:
            return _operation_failed(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_stale_tasks failed: {exc}',
                exc,
            )
        return Ok(self._rows_within_bound(
            'get_stale_tasks', _function_task_list(rows, _STALE_TASK_KEYS),
        ))

    def _rows_within_bound(
        self,
        operation: str,
        bounded: _BoundedTaskList,
    ) -> list[dict[str, Any]]:
        """The rows of a task list read; a cut list logs a warning that names
        ``operation`` and the bound."""
        match bounded:
            case _CompleteTaskList(rows=complete):
                return complete
            case _TruncatedTaskList(rows=first_rows):
                self.logger.warning(
                    '%s returned the first %d rows; more rows match',
                    operation,
                    TASK_LIST_ROW_BOUND,
                )
                return first_rows

    async def ping_database_async(self) -> BrokerResult[DatabasePing]:
        """Probe Postgres reachability with one statement on the broker's main
        core pool, through ``CoreHandle.ping_database_async``.

        A success also confirms that a connection could be checked out of the
        pool. Returns the measured round-trip latency. Reads only; a cancel
        of the caller drops the ping.

        Failures, each ``DB_PING_FAILED``; nothing changed:
        - the connect or the statement failed; ``retryable`` is the core's
          classification of the cause, and ``exception`` a
          ``CoreOperationFailed``;
        - the core of this process cannot run the call (``CoreError``: it
          stopped, did not start, or has too many calls in flight); the
          ``exception`` is that error, and ``retryable`` follows
          ``core_error_retryable``.
        """
        try:
            outcome = await self._core_handle.ping_database_async()
        except CoreError as exc:
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.DB_PING_FAILED,
                    message=f'ping_database failed: {exc}',
                    retryable=core_error_retryable(exc),
                    exception=exc,
                ),
            )
        match outcome:
            case DatabasePingRecord(latency_ms=latency_ms):
                return Ok(DatabasePing(latency_ms=latency_ms))
            case BrokerFailure(code='DB_PING_FAILED', message=message, retryable=retryable):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.DB_PING_FAILED,
                        message=f'ping_database failed: {message}',
                        retryable=retryable,
                        exception=CoreOperationFailed(
                            'PostgresBroker.ping_database_async', message, retryable,
                        ),
                    ),
                )
            case BrokerFailure(code=code):
                raise AssertionError(f'the core database ping answered the code {code}')
            case _ as unreachable:
                assert_never(unreachable)

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
        """
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('ping_workers', loop_limit)
            case Ok(resources):
                pass
        if timeout_seconds <= 0:
            return Err(
                BrokerInvalidInput(
                    message=f'timeout_seconds must be positive, got {timeout_seconds}',
                    exception=NoCause(),
                )
            )
        if min_responses is not None and min_responses < 1:
            return Err(
                BrokerInvalidInput(
                    message=f'min_responses must be >= 1 when set, got {min_responses}',
                    exception=NoCause(),
                )
            )

        correlation_id = uuid.uuid4().hex
        reply_channel = f'horsies_worker_pong_{correlation_id}'

        # A broker without a listener cannot receive replies: an Err, so the
        # Result contract holds on every path.
        match resources.listener:
            case ListenerEnabled(listener=listener):
                pass
            case ListenerDisabled():
                return Err(
                    BrokerListenerDisabled(
                        message=(
                            'ping_workers listener unavailable: this broker was '
                            'built with assume_initialized=True and has no listener'
                        ),
                    )
                )
        # A refused target_worker_id is refused input. A validated request
        # holds strings only, so its encode cannot fail; the encode runs
        # outside every catch, so a failure crashes as the defect it is.
        try:
            request = WorkerPingRequest(
                correlation_id=correlation_id,
                reply_channel=reply_channel,
                target_worker_id=target_worker_id,
            )
        except ValidationError as exc:
            return Err(BrokerInvalidInput(
                message=f'ping_workers request refused: {validation_summary(exc)}',
                exception=without_validation_input(exc),
            ))
        match dumps_json(request.model_dump()):
            case Ok(encoded):
                payload = encoded
            case Err(encode_error):
                raise AssertionError(
                    f'ping_workers request encode failed: {encode_error}',
                )

        listen_r = await listener.listen(reply_channel)
        if is_err(listen_r):
            err = listen_r.err_value
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.WORKER_PING_FAILED,
                    message=f'ping_workers reply subscribe failed: {err.message}',
                    retryable=err.retryable,
                    exception=err.exception,
                )
            )
        queue = listen_r.ok_value

        try:
            loop = asyncio.get_running_loop()
            async with resources.session_factory() as session:
                await session.execute(
                    text('SELECT pg_notify(:ch, :p)'),
                    {'ch': WORKER_PING_CHANNEL, 'p': payload},
                )
                await session.commit()
            sent_at = loop.time()

            pongs: list[WorkerPong] = []
            seen: set[str] = set()
            deadline = sent_at + timeout_seconds
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    notify = await asyncio.wait_for(queue.get(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                pong = self._decode_pong(
                    notify.payload, correlation_id, loop.time() - sent_at
                )
                if pong is None or pong.worker_id in seen:
                    continue  # malformed, mismatched, or duplicate worker
                seen.add(pong.worker_id)
                pongs.append(pong)
                if target_worker_id is not None and pong.worker_id == target_worker_id:
                    break
                if min_responses is not None and len(pongs) >= min_responses:
                    break
            return Ok(pongs)
        except Exception as exc:
            return _operation_failed(
                BrokerErrorCode.WORKER_PING_FAILED,
                f'ping_workers failed: {exception_text(exc)}',
                exc,
            )
        finally:
            await self._unsubscribe_ping_safely(listener, reply_channel, queue)

    async def _unsubscribe_ping_safely(
        self, listener: PostgresListener, channel: str, q: asyncio.Queue[Any],
    ) -> None:
        """Ensure reply-channel unsubscribe completes even under repeated cancellation.

        Mirrors ``_unsubscribe_task_done_safely``: shields the unsubscribe so a
        cancelled ``ping_workers_async`` does not leak the server-side LISTEN,
        and re-raises cancellation only after cleanup finishes.
        """
        unsubscribe_task = asyncio.create_task(listener.unsubscribe(channel, q))
        cancelled_during_cleanup = False
        while not unsubscribe_task.done():
            try:
                await asyncio.shield(unsubscribe_task)
            except asyncio.CancelledError:
                cancelled_during_cleanup = True
                continue
        await unsubscribe_task
        if cancelled_during_cleanup:
            raise asyncio.CancelledError

    def _decode_pong(
        self,
        raw_payload: str,
        correlation_id: str,
        elapsed_seconds: float,
    ) -> WorkerPong | None:
        """Decode a pong notification, discarding malformed or mismatched replies."""
        parsed = loads_json(raw_payload)
        if is_err(parsed):
            self.logger.warning(
                'Discarding unparseable pong payload: %s', parsed.err_value
            )
            return None
        body = parsed.ok_value
        if not isinstance(body, dict):
            return None
        try:
            payload = WorkerPongPayload.model_validate(body)
        except Exception as exc:
            self.logger.warning('Discarding invalid pong payload: %s', exception_text(exc))
            return None
        if payload.correlation_id != correlation_id:
            return None
        return WorkerPong(
            worker_id=payload.worker_id,
            hostname=payload.hostname,
            pid=payload.pid,
            round_trip_ms=elapsed_seconds * 1000.0,
        )

    @staticmethod
    def _row_to_worker_snapshot(row: Any) -> WorkerStateSnapshot:
        """Map a worker-states row to a typed snapshot.

        Raises:
            ValueError: the stored ``slots_down`` or ``class_capacity`` does
                not have the shape that the worker writes. The read methods
                catch it and return ``MONITORING_QUERY_FAILED``.
        """
        m = row._mapping
        match parse_slot_health(m['slots_down'], m['processes']):
            case Ok(slot_health):
                pass
            case Err(unreadable):
                raise ValueError(
                    f'worker {m["worker_id"]} snapshot at {m["snapshot_at"]}: '
                    f'slots_down is unreadable: {unreadable.reason}',
                )
        match parse_class_capacity(m['class_capacity']):
            case Ok(class_capacity):
                pass
            case Err(unreadable):
                raise ValueError(
                    f'worker {m["worker_id"]} snapshot at {m["snapshot_at"]}: '
                    f'class_capacity is unreadable: {unreadable.reason}',
                )
        return WorkerStateSnapshot(
            worker_id=m['worker_id'],
            snapshot_at=m['snapshot_at'],
            hostname=m['hostname'],
            pid=m['pid'],
            processes=m['processes'],
            max_claim_batch=m['max_claim_batch'],
            max_claim_per_worker=m['max_claim_per_worker'],
            cluster_wide_cap=m['cluster_wide_cap'],
            queues=list(m['queues']),
            queue_priorities=m['queue_priorities'],
            queue_max_concurrency=m['queue_max_concurrency'],
            recovery_config=m['recovery_config'],
            tasks_running=m['tasks_running'],
            tasks_claimed=m['tasks_claimed'],
            memory_usage_mb=m['memory_usage_mb'],
            memory_percent=m['memory_percent'],
            cpu_percent=m['cpu_percent'],
            children_memory_mb=m['children_memory_mb'],
            worker_started_at=m['worker_started_at'],
            slot_health=slot_health,
            class_capacity=class_capacity,
        )

    async def list_worker_states_async(self) -> BrokerResult[list[WorkerStateSnapshot]]:
        """Latest state snapshot per worker, including idle workers.

        Unlike the retired ``get_worker_stats`` (RUNNING tasks only), this
        reads ``horsies_worker_states`` so every worker that has reported a
        snapshot appears, regardless of current load.
        """
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('list_worker_states', loop_limit)
            case Ok(resources):
                pass
        try:
            async with resources.session_factory() as session:
                result = await session.execute(LIST_WORKER_STATES_SQL)
                return Ok(
                    [self._row_to_worker_snapshot(row) for row in result.fetchall()]
                )
        except Exception as exc:
            return _operation_failed(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'list_worker_states failed: {exc}',
                exc,
            )

    async def get_worker_state_async(
        self,
        worker_id: str,
    ) -> BrokerResult[WorkerStateSnapshot | None]:
        """Latest state snapshot for one worker, or ``None`` if unknown."""
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('get_worker_state', loop_limit)
            case Ok(resources):
                pass
        try:
            async with resources.session_factory() as session:
                result = await session.execute(
                    GET_WORKER_STATE_LATEST_SQL,
                    {'worker_id': worker_id},
                )
                row = result.fetchone()
                if row is None:
                    return Ok(None)
                return Ok(self._row_to_worker_snapshot(row))
        except Exception as exc:
            return _operation_failed(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_worker_state failed: {exc}',
                exc,
            )

    async def get_worker_state_history_async(
        self,
        worker_id: str,
        *,
        limit: int | None = None,
    ) -> BrokerResult[list[WorkerStateSnapshot]]:
        """Timeseries snapshots for one worker, newest first.

        ``limit`` of ``None`` returns all retained rows; pass an explicit cap
        to bound the fetch (the table grows ~1 row per worker per interval).
        """
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('get_worker_state_history', loop_limit)
            case Ok(resources):
                pass
        if limit is not None and limit <= 0:
            return Err(
                BrokerInvalidInput(
                    message=f'limit must be positive when set, got {limit}',
                    exception=NoCause(),
                )
            )
        try:
            async with resources.session_factory() as session:
                result = await session.execute(
                    GET_WORKER_STATE_HISTORY_SQL,
                    {'worker_id': worker_id, 'limit': limit},
                )
                return Ok(
                    [self._row_to_worker_snapshot(row) for row in result.fetchall()]
                )
        except Exception as exc:
            return _operation_failed(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_worker_state_history failed: {exc}',
                exc,
            )

    async def get_class_coverage_async(self) -> BrokerResult[ClassCoverage]:
        """The fleet check: for each (queue, class) with PENDING rows, whether
        a live worker serves it.

        A worker is live while its latest snapshot is at most
        ``WORKER_STATE_LIVE_WINDOW_S`` old. It serves a (queue, class) when
        the queue is one of its queues and one of its budgets serves the
        class. Reads only.
        ``Err(MONITORING_QUERY_FAILED)``: a database or network error, a live
        snapshot whose ``class_capacity`` is unreadable, or a read that
        reached its bound (``retryable`` is ``False`` for the last two).
        ``Err(EVENT_LOOP_LIMIT)``: nothing ran. A code defect raises.
        """
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('get_class_coverage', loop_limit)
            case Ok(resources):
                pass
        try:
            async with resources.session_factory() as session:
                read = await read_class_coverage(session)
        except _DRIVER_ERRORS as exc:
            return _operation_failed(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_class_coverage failed: {exc}',
                exc,
            )
        match read:
            case Ok(coverage):
                return Ok(coverage)
            case Err(refused):
                return _class_read_refused('get_class_coverage', refused)

    async def get_class_report_async(self) -> BrokerResult[ClassReport]:
        """Pending streams with the wait of their oldest due row and their
        coverage, and the budget load of each live worker.

        Reads only.
        ``Err(MONITORING_QUERY_FAILED)``: as ``get_class_coverage_async``, or
        a live worker holds rows of a class that its recorded budgets do not
        serve.
        """
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('get_class_report', loop_limit)
            case Ok(resources):
                pass
        try:
            async with resources.session_factory() as session:
                read = await read_class_report(session)
        except _DRIVER_ERRORS as exc:
            return _operation_failed(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_class_report failed: {exc}',
                exc,
            )
        match read:
            case Ok(report):
                return Ok(report)
            case Err(refused):
                return _class_read_refused('get_class_report', refused)

    async def get_expired_tasks(self) -> BrokerResult[list[dict[str, Any]]]:
        """PENDING tasks whose ``good_until`` passed before a worker took
        them. Reads only.

        Rows come ordered by ``good_until``, oldest first, then by id. At
        most ``TASK_LIST_ROW_BOUND`` rows come back; when more rows match,
        the first ``TASK_LIST_ROW_BOUND`` come back and a warning is logged.

        - ``Err(MONITORING_QUERY_FAILED)``: the read failed.
        """
        match await self.select_resources():
            case Err(loop_limit):
                return _loop_limit_err('get_expired_tasks', loop_limit)
            case Ok(resources):
                pass
        try:
            async with resources.session_factory() as session:
                connection = await autocommit_connection(session)
                result = await connection.execute(LIST_EXPIRED_TASKS_SQL)
                rows = result.mappings().all()
        except Exception as exc:
            return _operation_failed(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_expired_tasks failed: {exc}',
                exc,
            )
        return Ok(self._rows_within_bound(
            'get_expired_tasks', _function_task_list(rows, _EXPIRED_TASK_KEYS),
        ))

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
        - the core of this process cannot run the pass (``CoreError``);
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
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'mark_stale_tasks_as_failed failed: {exc}',
                    retryable=core_error_retryable(exc),
                    exception=exc,
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
            case StaleRunningScanFailed(message=message, retryable=retryable):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=f'mark_stale_tasks_as_failed failed: {message}',
                        retryable=retryable,
                        exception=CoreOperationFailed(
                            'PostgresBroker.mark_stale_tasks_as_failed', message, retryable,
                        ),
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
        - the core of this process cannot run the pass (``CoreError``);
          ``retryable`` follows ``core_error_retryable``.

        A cancel of the caller does not stop the pass: the core runs it to
        its end.
        """
        try:
            outcome = await self._core_handle.expire_pending_tasks_async()
        except CoreError as exc:
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'expire_pending_tasks failed: {exc}',
                    retryable=core_error_retryable(exc),
                    exception=exc,
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
            case PendingExpiryFailed(message=message, retryable=retryable):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=f'expire_pending_tasks failed: {message}',
                        retryable=retryable,
                        exception=CoreOperationFailed(
                            'PostgresBroker.expire_pending_tasks', message, retryable,
                        ),
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

        - the core of this process cannot run the pass (``CoreError``: it
          stopped, did not start, or has too many calls in flight); the
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
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'requeue_stale_claimed failed: {exc}',
                    retryable=core_error_retryable(exc),
                    exception=exc,
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
            ):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=(
                            f'requeue_stale_claimed failed after requeueing '
                            f'{requeued_before} task(s): {message}'
                        ),
                        retryable=retryable,
                        exception=CoreOperationFailed(
                            'PostgresBroker.requeue_stale_claimed', message, retryable,
                        ),
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
          ``exception`` is ``CoreLockNotAvailable``, whose ``sqlstate`` is
          ``'55P03'``;
        - a database failure, or a page outside the function's contract;
          ``retryable`` is the core's classification of the cause;
        - the core of this process cannot run the page (``CoreError``);
          ``retryable`` follows ``core_error_retryable``.

        A cancel of the caller does not stop the page: the core runs it to
        its end.
        """
        started = time.monotonic()
        try:
            outcome = await self._core_handle.audit_orphaned_workflow_tasks_async()
        except CoreError as exc:
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'audit_orphaned_workflow_tasks failed: {exc}',
                    retryable=core_error_retryable(exc),
                    exception=exc,
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
            case OrphanAuditLockBusy(message=message, retryable=retryable):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=f'audit_orphaned_workflow_tasks failed: {message}',
                        retryable=retryable,
                        exception=CoreLockNotAvailable(
                            _ORPHAN_AUDIT_SUBJECT, message, retryable,
                        ),
                    ),
                )
            case OrphanAuditFailed(message=message, retryable=retryable):
                return Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.CLEANUP_FAILED,
                        message=f'audit_orphaned_workflow_tasks failed: {message}',
                        retryable=retryable,
                        exception=CoreOperationFailed(
                            _ORPHAN_AUDIT_SUBJECT, message, retryable,
                        ),
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

    def send_route(self) -> SyncSendRoute:
        """Where a sync ``enqueue`` writes its row. Pure.

        ``CALLER_THREAD`` for a send of a process that runs no task bodies,
        once this broker's schema is initialized, with or without an
        idempotency key: both are one ``horsies_enqueue_task`` statement in
        autocommit. A send before the schema setup (which the first send may
        run) stays on the loop runner.
        Task code in a worker child sends through the child's
        ``task_send_pool``, which the worker profile counts exactly, so it
        stays on the loop runner too.
        """
        match (process_role(), self._initialized):
            case (ProcessRole.RUNS_NO_TASK_BODIES, True):
                return SyncSendRoute.CALLER_THREAD
            case _:
                return SyncSendRoute.LOOP_RUNNER

    def _sync_send_engine_open(self) -> Engine:
        """The engine of the sync sends, opened on first use: the runtime
        engine's URL, pool sizes, connect args and session settings, in
        autocommit, and the idle ping of ``pool_pre_ping``. Thread-safe. No
        I/O: connections open on first checkout."""
        with self._sync_send_lock:
            match self._sync_send_engine:
                case _SyncSendEngineOpen(engine=engine):
                    return engine
                case _SyncSendEngineNotOpen():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            engine = create_engine(
                sqlalchemy_url(self.config.database_url.get_secret_value()),
                **self._runtime_engine_config(),
                isolation_level='AUTOCOMMIT',
            )
            register_session_settings(engine, self._runtime_session_settings())
            self._register_pre_ping(engine)
            self._sync_send_engine = _SyncSendEngineOpen(engine)
            return engine

    def _enqueue_task_on_caller_thread(
        self,
        row: EnqueueRow,
        send_kind: SendKind,
    ) -> EnqueueOutcome:
        """Run ``horsies_enqueue_task`` on this thread, one statement in
        autocommit. Raises what the driver and SQLAlchemy raise: the caller
        classifies it, as on the async path."""
        engine = self._sync_send_engine_open()
        with engine.connect() as connection:
            rows = connection.exec_driver_sql(
                ENQUEUE_TASK_DRIVER_SQL, enqueue_task_params(row, send_kind),
            ).fetchall()
        return read_enqueue_outcome(rows)

    def _enqueue_on_caller_thread(
        self,
        task_name: str,
        queue_name: str,
        *,
        task_id: UUID,
        send_kind: SendKind,
        args_json: str | None,
        kwargs_json: str | None,
        priority: int,
        sent_at: datetime,
        enqueued_at: Optional[datetime],
        enqueue_delay_seconds: Optional[int],
        good_until: Optional[datetime],
        task_options: Optional[str],
        retention_class_key: RetentionClassKey | None,
        retain_rerun_input: Optional[bool],
        idempotency_key: Optional[str],
    ) -> Result[UUID, BrokerEnqueueError | BrokerSyncCallError]:
        """``enqueue_async`` on the calling thread: the same row (with the
        key of a keyed send), the same statement and the same error
        classification."""
        resolved_retention_class_key = resolve_retention_class_key(
            retention_class_key
        )
        if retain_rerun_input is None:
            retain_rerun_input = self.config.retain_rerun_input_default
        prepared_r = self._prepare_enqueue(
            task_id=task_id,
            task_name=task_name,
            queue_name=queue_name,
            priority=priority,
            args_json=args_json,
            kwargs_json=kwargs_json,
            sent_at=sent_at,
            enqueued_at=enqueued_at,
            enqueue_delay_seconds=enqueue_delay_seconds,
            good_until=good_until,
            task_options=task_options,
            retention_class_key=resolved_retention_class_key,
            retain_rerun_input=retain_rerun_input,
            idempotency_key=idempotency_key,
            key_window=self.idempotency_reservation_window,
        )
        if is_err(prepared_r):
            return prepared_r
        prepared = prepared_r.ok_value
        try:
            outcome = self._enqueue_task_on_caller_thread(prepared.row, send_kind)
        except _DRIVER_ERRORS as exc:
            return _enqueue_failed(
                f'Failed to enqueue task {task_name}: {exc}',
                exc,
            )
        return enqueue_result(outcome, task_id=task_id, task_name=task_name)

    def enqueue(
        self,
        task_name: str,
        queue_name: str = 'default',
        *,
        task_id: UUID,
        enqueue_sha: str,
        send_kind: SendKind,
        args_json: str | None = None,
        kwargs_json: str | None = None,
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
        """Synchronous task submission, with the outcomes of ``enqueue_async``.

        ``send_route`` decides where the row is written: on the calling
        thread, or on the sync runner's loop through ``enqueue_async``. On a
        thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)`` and
        writes nothing.
        """
        if in_running_event_loop():
            return _async_context_err(
                'PostgresBroker.enqueue()', 'broker.enqueue_async(...)',
            )
        match self.send_route():
            case SyncSendRoute.CALLER_THREAD:
                return self._enqueue_on_caller_thread(
                    task_name,
                    queue_name,
                    task_id=task_id,
                    send_kind=send_kind,
                    args_json=args_json,
                    kwargs_json=kwargs_json,
                    priority=priority,
                    sent_at=sent_at,
                    enqueued_at=enqueued_at,
                    enqueue_delay_seconds=enqueue_delay_seconds,
                    good_until=good_until,
                    task_options=task_options,
                    retention_class_key=retention_class_key,
                    retain_rerun_input=retain_rerun_input,
                    idempotency_key=idempotency_key,
                )
            case SyncSendRoute.LOOP_RUNNER:
                pass
        try:
            return self.run_sync(
                self.enqueue_async,
                task_name,
                queue_name,
                task_id=task_id,
                enqueue_sha=enqueue_sha,
                send_kind=send_kind,
                args_json=args_json,
                kwargs_json=kwargs_json,
                priority=priority,
                sent_at=sent_at,
                enqueued_at=enqueued_at,
                enqueue_delay_seconds=enqueue_delay_seconds,
                good_until=good_until,
                task_options=task_options,
                idempotency_key=idempotency_key,
                retention_class_key=retention_class_key,
                retain_rerun_input=retain_rerun_input,
            )
        except LoopRunnerError as exc:
            return _loop_runner_err(f'PostgresBroker.enqueue({task_name!r})', exc)



    async def _history_task_info(
        self,
        session: 'AsyncSession',
        task_id: UUID,
        *,
        include_result: bool,
        include_failed_reason: bool,
        include_attempts: bool,
    ) -> BrokerResult[TaskInfo | None]:
        """The history side of a task-info read, or Ok(None) if absent.

        The terminal instant lands in completed_at for COMPLETED and in
        failed_at otherwise; claim state is structurally absent on a
        history record, while the claim's timestamp survives as
        provenance. Attempts decode from the record's snapshot — their
        only home after the move — and are listed newest first.
        """
        from horsies.core.history.reads.detail import (
            HistoryTaskDetail,
            read_task_detail,
        )

        connection = await session.connection()
        detail = await read_task_detail(connection, task_id=task_id)
        match detail:
            case HistoryTaskDetail():
                pass
            case _:
                return Ok(None)

        status = detail.status
        completed_at = (
            detail.terminal_at if status is TaskStatus.COMPLETED else None
        )
        failed_at = (
            detail.terminal_at if status is not TaskStatus.COMPLETED else None
        )
        raw_result_value: dict[str, Any] | None = None
        if include_result and detail.result_payload is not None:
            from horsies.core.history.archive.results import (
                decode_result_envelope,
            )
            from horsies.core.history.archive.versions import (
                DecodedArchiveValue,
            )

            if detail.result_digest is None:
                return Err(BrokerInvalidJsonPayload(
                    message=(
                        f'History result digest is absent for task {task_id}'
                    ),
                    exception=NoCause(),
                ))
            decoded = decode_result_envelope(
                version=detail.result_envelope_version,
                payload=detail.result_payload,
                digest=detail.result_digest,
            )
            match decoded:
                case DecodedArchiveValue(value=loaded):
                    pass
                case failure:
                    return Err(BrokerInvalidJsonPayload(
                        message=(
                            f'History result envelope failed validation for '
                            f'task {task_id}: {failure!r}'
                        ),
                        exception=NoCause(),
                    ))
            if loaded is not None and not isinstance(loaded, dict):
                return Err(BrokerInvalidJsonPayload(
                    message=(
                        f'Result for task {task_id} is not a JSON '
                        f'object; got {type(loaded).__name__}'
                    ),
                    exception=NoCause(),
                ))
            raw_result_value = cast('dict[str, Any] | None', loaded)

        attempts: list[TaskAttemptInfo] | None = None
        if include_attempts:
            # The snapshot holds the attempts oldest first; TaskInfo lists
            # them newest first, as the live read does (ORDER BY attempt DESC).
            attempts = [
                TaskAttemptInfo(
                    task_id=task_id,
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
                for record in reversed(detail.attempts)
            ]

        return Ok(
            TaskInfo(
                task_id=detail.task_id,
                task_name=detail.task_name,
                status=status,
                queue_name=detail.queue_name,
                priority=detail.priority,
                retry_count=detail.retry_count,
                max_retries=detail.max_retries,
                next_retry_at=None,
                sent_at=detail.sent_at,
                enqueued_at=detail.enqueued_at,
                claimed_at=detail.claimed_at,
                started_at=detail.started_at,
                completed_at=completed_at,
                failed_at=failed_at,
                worker_hostname=detail.last_worker_hostname,
                worker_pid=detail.last_worker_pid,
                worker_process_name=None,
                error_code=detail.error_code,
                raw_result=raw_result_value,
                decoded_result=None,
                result_decoded=False,
                failed_reason=(
                    detail.final_failed_reason
                    if include_failed_reason
                    else None
                ),
                attempts=attempts,
            )
        )

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
            return _task_info_core_error(task_id, exc)
        return _task_info_result(task_id, outcome)

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
            return _task_info_core_error(task_id, exc)
        return _task_info_result(task_id, outcome)

    def close(self) -> BrokerResult[None]:
        """Synchronous cleanup (runs close_async in background loop).

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and closes nothing: no resource set closes and the sync runner keeps
        its state. Use ``await close_async()`` there.
        """
        if in_running_event_loop():
            return _async_context_err(
                'PostgresBroker.close()', 'broker.close_async()',
            )
        try:
            return self.run_sync(self.close_async)
        except LoopRunnerError as exc:
            return _loop_runner_err('PostgresBroker.close()', exc)
        finally:
            self._loop_runner.stop()


class WorkerProcessBroker(PostgresBroker):
    """The broker of a worker process, built from
    ``PostgresConfig.worker_process_config``.

    Its core handle connects with the ``WORKER`` pool profile: the batch
    checkouts of the worker process leave connections of the main core pool
    free for its per-task writes. Everything else is ``PostgresBroker``.
    """

    _CORE_POOL_PROFILE: ClassVar[CorePoolProfile] = 'WORKER'
