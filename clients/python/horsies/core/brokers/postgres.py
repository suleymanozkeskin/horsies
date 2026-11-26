# app/core/brokers/postgres.py
from __future__ import annotations
import asyncio, hashlib, contextlib, os, time
from collections.abc import AsyncGenerator
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
from datetime import datetime, timedelta
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
from horsies.core.brokers.enqueue_task import (
    SendKind,
)
from horsies.core.telemetry.send_telemetry import NO_TRACE_CARRIER, TraceArgument
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
from horsies.core.types.core_failure import (
    CoreCallRefusal,
    call_refusal_or_raise,
    core_error_retryable,
    core_exception_retryable,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.status import (
    TaskAttemptOutcome,
    TaskStatus,
)
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.codec.json_io import Json, loads_json
from horsies.core.models.tasks import Execution, TaskInfo, TaskAttemptInfo
from horsies.core.models.class_coverage import ClassCoverage, ClassReport
from horsies.core.models.health import (
    DatabasePing,
    WorkerPong,
    WorkerStateSnapshot,
)
from horsies.core.history.periods import RetentionClassKey
from horsies.core.history.ddl.classes import (
    DEFAULT_RETENTION_CLASS_KEY,
    resolve_retention_class_key,
)
from horsies.core.process_role import (
    ProcessRole,
    process_role,
    wait_inside_task_message,
)
from horsies.core.utils.loop_runner import (
    async_context_message,
    in_running_event_loop,
)
from horsies.core.utils.wait_timeout import wait_timeout_seconds

if TYPE_CHECKING:
    import logging

    from horsies.core.app import Horsies
from horsies.core.logging import enable_statement_echo, get_logger


def _operation_failed(
    code: BrokerOperationFailedCode,
    message: str,
    exc: CoreFailure | CoreCallRefusal,
) -> Err[BrokerOperationFailed]:
    """Build a ``BrokerOperationFailed`` for a core failure or a core
    boundary error, retryable by ``core_exception_retryable``. Pure."""
    return Err(
        BrokerOperationFailed(
            code=code,
            message=message,
            retryable=core_exception_retryable(exc),
            exception=exc,
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


def _close_answer(refusal: CoreCallRefusal | None) -> BrokerResult[None]:
    """The answer of a close. Pure.

    ``None`` (the core handle closed): ``Ok(None)``. A core boundary error
    of the close: ``CLOSE_FAILED`` with it, retryable by
    ``core_exception_retryable``.
    """
    match refusal:
        case None:
            return Ok(None)
        case _:
            return _operation_failed(
                BrokerErrorCode.CLOSE_FAILED,
                f'Close failed (1 error(s)): {refusal}',
                refusal,
            )


def _async_context_err(
    sync_call: str, async_call: str,
) -> Err[BrokerAsyncContext]:
    """Build the broker error for a sync facade called inside a running event loop.

    Nothing ran.
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
type CoreSendOutcome = Enqueued | BrokerEnqueueError
type RerunInputPolicy = Literal['APP_DEFAULT', 'KEEP', 'DROP']


def _invalid_input(message: str) -> Err[BrokerInvalidInput]:
    return Err(BrokerInvalidInput(message=message, exception=NoCause()))


def _command_rule_refused(task_name: str, rule: str) -> Err[BrokerInvalidInput]:
    """A rule of the version-1 command refused a value that the core's
    request cannot hold: ``INVALID_INPUT`` with the rule as a ``ValueError``,
    the refusal of Python's ``EnqueueCommandV1``. The core answers a value
    that its request holds with the same message and cause. Pure."""
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
    trace: TraceArgument,
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
    ``trace`` goes to the core as it is: the core checks the bounds of a
    carrier and stores none for a carrier that breaks one, with no refusal.
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
        trace=trace,
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


def _enqueue_result(outcome: CoreSendOutcome) -> Result[UUID, BrokerEnqueueError]:
    """The result of one core send. Pure.

    - ``Enqueued``: ``Ok`` with the sent id, or the id that holds the key
      for a key replay.
    - The class that the core built, as it is: ``BrokerInvalidInput`` or
      ``BrokerInvalidTaskOptions`` (refused before any statement; the
      ``exception`` is the ``ValueError`` of a rule, the
      ``SerializationError`` of a JSON text that does not read, or
      ``NoCause()``), ``BrokerEnqueueFailed`` (the core's exception of the
      cause, or ``NoCause()`` for a conflicting row that was gone),
      ``BrokerPayloadMismatch``, or ``BrokerIdempotencyKeyConflict`` with
      the owner. The core decides the code, the message and the cause.
    """
    match outcome:
        case Enqueued(task_id=task_id):
            return Ok(task_id)
        case (
            BrokerInvalidInput()
            | BrokerInvalidTaskOptions()
            | BrokerEnqueueFailed()
            | BrokerPayloadMismatch()
            | BrokerIdempotencyKeyConflict()
        ):
            return Err(outcome)
        case _ as unreachable:
            assert_never(unreachable)


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

        self.idempotency_reservation_window: timedelta = validate_reservation_window(
            idempotency_reservation_window
            if idempotency_reservation_window is not None
            else IDEMPOTENCY_WINDOW_DEFAULT
        )
        # False makes this broker incapable of executing DDL. Read-only
        # tooling that constructs its own broker sets it so that pointing
        # the tool at a database can never migrate that database. Owning
        # the schema stays the app's and the worker's job.
        self.run_schema_migrations = run_schema_migrations
        self.logger: logging.Logger = get_logger('broker')
        self._app: Horsies | None = None  # Set by Horsies.get_broker()

        self._initialized = assume_initialized
        # True once this broker saw the staged result function. Only a
        # positive answer is kept: until a worker publishes the readers,
        # each result probe checks again.
        self._staged_result_reader_published = False
        # The operations that run in the core use the pools of this handle.
        # Building it starts nothing: the first core operation connects, and
        # close_async closes its pools.
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

    @property
    def app(self) -> 'Horsies | None':
        """Get the attached Horsies app instance (if any)."""
        return self._app

    @app.setter
    def app(self, value: 'Horsies | None') -> None:
        """Set the Horsies app instance."""
        self._app = value

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
    ) -> Result[None, BrokerOperationFailed]:
        """Install the schema, or check it, once per broker. Writes.

        Safe to call many times and from many processes: the core guards the
        install with the fleet schema advisory lock, and waits for another
        installer's lock for at most the core's bound.

        Returns ``Ok(None)`` on success. On failure ``BrokerOperationFailed``
        with ``SCHEMA_INIT_FAILED`` and its cause: the core reported the
        install failed (``exception`` is the core's ``CoreFailure``), for
        example a database of the other client family or one this build
        cannot install into (not retryable), or the core of this process
        cannot run the call (``exception`` is the ``CoreCallRefusal``). A
        cancel of the caller does not stop the install. Raises ``ForkedAfterCoreStart`` and ``CoreStopped``
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
        trace: TraceArgument = NO_TRACE_CARRIER,
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
        ``None`` takes the config's default. ``trace`` is the carrier of the
        send's span (``TraceCarrier``) or ``NO_TRACE_CARRIER``; the row
        stores a carrier that passes the core's bounds, and a carrier that
        breaks one never fails the send.

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
            trace=trace,
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
        - ``Err(BrokerInvalidInput)``: ``timeout_ms`` is not None or an int
          from 0 to ``MAX_TASK_TIMEOUT_MS``. This is the first check, as in
          every client. Nothing ran.
        - ``Err(BrokerWaitInsideTask)``: the call runs in a process that
          runs task bodies (a worker child). Nothing ran.
        - ``Err(BrokerOperationFailed(TASK_INFO_QUERY_FAILED))``: the
          connect, the install or a read failed (``retryable`` is the core's
          classification, ``exception`` the core's exception of the cause), or the
          core of this process cannot run the call (``exception`` is the
          ``CoreCallRefusal``).
        """
        match wait_timeout_seconds(timeout_ms):
            case Err(err_value=refused):
                return Err(BrokerInvalidInput(
                    message=f'raw result fetch of task {task_id}: {refused.message}',
                    exception=NoCause(),
                ))
            case Ok():
                pass
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return _wait_inside_task_err(
                'PostgresBroker.get_raw_result_record_async()', task_id,
            )
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

        The checks before any I/O come in this order: the wait limit
        (``Err(BrokerInvalidInput)``), a process that runs task bodies
        (``Err(BrokerWaitInsideTask)``), and a thread that runs an event
        loop (``Err(ASYNC_CONTEXT)``). A wait with a limit is bounded by the
        limit plus the core's bound of a blocking call; a wait with no limit
        has no call bound, as before.
        """
        match wait_timeout_seconds(timeout_ms):
            case Err(err_value=refused):
                return Err(BrokerInvalidInput(
                    message=f'raw result fetch of task {task_id}: {refused.message}',
                    exception=NoCause(),
                ))
            case Ok():
                pass
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return _wait_inside_task_err(
                'PostgresBroker.get_raw_result_record()', task_id,
            )
        if in_running_event_loop():
            return _async_context_err(
                'PostgresBroker.get_raw_result_record()',
                'broker.get_raw_result_record_async(...)',
            )
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
        """Close the core handle's pools.

        The core handle's writes in flight end within the core's close
        bound; a handle that never connected closes without starting the
        core. The broker stays usable: a later core operation opens new core
        pools.

        Returns ``Ok(None)``, or ``CLOSE_FAILED`` with the ``CoreCallRefusal``
        when the core of this process cannot run the close (it did not
        start, or the loop has too many calls in flight). Raises
        ``ForkedAfterCoreStart`` and ``CoreStopped`` from the core handle's
        close.
        """
        try:
            _ = await self._core_handle.close_async()
        except CoreError as exc:
            return _close_answer(call_refusal_or_raise(exc))
        return _close_answer(None)

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

        The reaper of each worker runs the same pass when
        ``RecoveryConfig.auto_fail_stale_running`` is on. This method runs
        it on demand, also when that setting is off.

        The core reads at most one scan batch of 1,000 candidates with
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
        """Cancel one bounded page of orphan workflow tasks and return the
        number cancelled: one core call.

        The reaper of each worker runs the same page every
        ``RecoveryConfig.orphan_task_audit_interval_ms`` when
        ``RecoveryConfig.auto_terminate_orphaned_workflow_tasks`` is on. This
        method runs one page on demand, also when that setting is off.

        The page is the one of ``audit_orphaned_workflow_tasks``: the core
        runs ``horsies_cancel_orphaned_tasks`` with a page of 500 rows, in one
        transaction, and the scanned PENDING and CLAIMED workflow tasks that
        no runnable node links move to history as CANCELLED.

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
        try:
            outcome = await self._core_handle.audit_orphaned_workflow_tasks_async()
        except CoreError as exc:
            refusal = call_refusal_or_raise(exc)
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.CLEANUP_FAILED,
                    message=f'terminate_orphaned_workflow_tasks failed: {refusal}',
                    retryable=core_error_retryable(refusal),
                    exception=refusal,
                ),
            )
        match outcome:
            case OrphanPageAudited(cancelled=cancelled):
                return Ok(cancelled)
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
                        message=f'terminate_orphaned_workflow_tasks failed: {message}',
                        retryable=retryable,
                        exception=exception,
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)

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
        trace: TraceArgument = NO_TRACE_CARRIER,
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
            trace=trace,
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

        The core handle's writes in flight end within the core's close
        bound; a handle that never connected closes without starting the
        core. A raised signal handler (Ctrl+C) leaves the close; the core's
        close runs on to its end.

        Returns ``Ok(None)``, or ``CLOSE_FAILED`` with the ``CoreCallRefusal``
        when the core of this process cannot run the close, or the close did
        not end within the core's bound. On a thread that runs an event loop
        it returns ``Err(ASYNC_CONTEXT)`` and closes nothing; use ``await
        close_async()`` there.

        Raises ``ForkedAfterCoreStart`` and ``CoreStopped`` from the core
        handle's close.
        """
        if in_running_event_loop():
            return _async_context_err(
                'PostgresBroker.close()', 'broker.close_async()',
            )
        try:
            _ = self._core_handle.close()
        except CoreError as exc:
            return _close_answer(call_refusal_or_raise(exc))
        return _close_answer(None)
