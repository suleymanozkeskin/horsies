"""The rerun operation: one recorded request re-executed as a new one.

Resolution is one call of ``horsies_rerun_source_read``
(0006_history_functions.sql): the class check of the new request, the
published detail reader and the absence reason in one snapshot. A live
source is a typed refusal (a live task is retried by its own lifecycle,
never rerun), and an absence carries the reason of
``horsies_task_absence_reason``. Eligibility
precedes input, mirroring the ratified ladder: workflow backing tasks
and completed sources refuse before any envelope is touched. Input
reconstruction reads the envelope that the move built, with the digest
verified before parse; every non-INLINE disposition is typed
unavailable.

The new row is written strictly by the provenance table: replayed
fields from the source row or its carried envelope, the latest start time
(``good_until``) as the caller's explicit choice, policy resolved at
enqueue, identity and lifecycle state fresh, the lineage pair atomic. The
write is one call of ``horsies_enqueue_task`` (0004_lifecycle_functions.sql)
with the lineage ``RERUN``, in the caller's transaction: a keyed rerun claims
its key inside that call. Because the fingerprint covers rerun lineage, a
key still reserved by the source request conflicts by contract rather than
aliasing its rerun.

``rerun_task_in_async`` runs in the core: the core builds the read and the write
and decides the outcome (``horsies._core.rerun_read``,
``prepare_rerun_write``, ``decode_rerun_write``), and the caller's
connection runs both statements.

The new task stores the trace that the request carries. ``rerun_task_in_async``
passes none. ``Horsies.rerun_task_in`` and ``Horsies.rerun_task_in_async`` pass
the carrier of the caller's current context when the app traces
(``rerun_with_trace``, ``rerun_with_trace_async``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID

from typing import assert_never

from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies._core import (
    IdMintRefused,
    PreparedStatement,
    RerunClassNotRegistered,
    RerunClientFamilyMismatch,
    RerunCommandRecord,
    RerunEnqueuedRecord,
    RerunIdempotencyKeyRefused,
    RerunInputCorruptRecord,
    RerunInputUnavailableRecord,
    RerunKeyConflictRecord,
    RerunKeyReplayRecord,
    RerunNotEligibleRecord,
    RerunPolicyRecord,
    RerunSourceAbsentRecord,
    RerunSourceCorruptRecord,
    RerunSourceLiveRecord,
    RerunUnknownStoredFamily,
    RerunWriteRecord,
    decode_rerun_write,
    prepare_rerun_write,
)
from horsies._core import rerun_read as core_rerun_read
from horsies.core.telemetry.send_telemetry import NO_TRACE_CARRIER, TraceArgument

from ..ddl.tables import FOREVER_CLASS_KEY
from ..periods import parse_retention_period, period_class_key
from horsies.core.types.result import Err, Ok
from ..identity.keys import validate_reservation_window
from horsies._catalog.history import TaskAbsenceReason


@dataclass(frozen=True, slots=True)
class RerunCommand:
    """One rerun command. ``good_until``, the latest start time of the new
    task, is explicit: a value or the explicit no-limit choice, never a
    silent default."""

    source_task_id: UUID
    good_until: datetime | None
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class RerunEnqueuePolicy:
    """The NEW request's enqueue-time policy snapshot, supplied by the
    caller because policy lives in configuration, not in this module.

    ``retention`` is a finite period in whole days, or ``None`` to keep the
    record forever. The class is the one the period stands for.
    """

    retention: timedelta | None
    retain_rerun_input: bool
    reservation_window: timedelta
    retention_class_key: str = field(init=False)
    """The class key the new request is stamped with, from ``retention``."""

    def __post_init__(self) -> None:
        match self.retention:
            case None:
                class_key = FOREVER_CLASS_KEY
            case period:
                match parse_retention_period(period):
                    case Err(err_value=refused):
                        raise ValueError(f'rerun retention: {refused.reason}')
                    case Ok(ok_value=days):
                        class_key = period_class_key(days)
        validate_reservation_window(self.reservation_window)
        object.__setattr__(self, 'retention_class_key', class_key)


class NotEligibleReason(Enum):
    COMPLETED_SOURCE = 'COMPLETED_SOURCE'
    WORKFLOW_TASK = 'WORKFLOW_TASK'


@dataclass(frozen=True, slots=True)
class RerunEnqueued:
    """The new request exists; the source record is untouched."""

    new_task_id: UUID
    source_task_id: UUID
    rerun_root_task_id: UUID


@dataclass(frozen=True, slots=True)
class RerunSourceLive:
    """The source is live; its own lifecycle owns retry."""

    task_id: UUID


@dataclass(frozen=True, slots=True)
class RerunSourceAbsent:
    """No live or history record has the source id.

    ``reason`` says whether retention can explain the absence. Nothing is
    written."""

    task_id: UUID
    reason: TaskAbsenceReason


@dataclass(frozen=True, slots=True)
class RerunSourceCorrupt:
    """The source history row does not decode; fail closed.

    ``detail`` names why the source read does not decode: a stored status
    that is not a ``TaskStatus`` member, an attempt snapshot that does not
    decode, or a location that the staged read does not define. Nothing is
    written. A repeat gives the same result until the stored row or the
    server changes."""

    task_id: UUID
    detail: str


@dataclass(frozen=True, slots=True)
class RerunNotEligible:
    task_id: UUID
    reason: NotEligibleReason


@dataclass(frozen=True, slots=True)
class RerunInputUnavailable:
    """The library cannot reconstruct input: the source row stores no
    envelope. ``disposition`` is the stored reason, for example
    ``DECLINED_BY_POLICY`` or ``OVER_BOUND``."""

    task_id: UUID
    disposition: str


@dataclass(frozen=True, slots=True)
class RerunInputCorrupt:
    """Stored envelope fails integrity or shape; fail closed."""

    task_id: UUID
    detail: str


@dataclass(frozen=True, slots=True)
class RerunKeyConflict:
    """The idempotency key is reserved by a different canonical command —
    including the source request itself, whose fingerprint differs
    from its rerun's by lineage. ``owner_task_id`` is the task that holds
    the key. The contract working, not a defect."""

    task_id: UUID
    owner_task_id: UUID


@dataclass(frozen=True, slots=True)
class RerunKeyReplay:
    """The key already reserved this exact rerun command; the committed
    request is returned instead of a duplicate."""

    existing_task_id: UUID


type RerunOutcome = (
    RerunEnqueued
    | RerunSourceLive
    | RerunSourceAbsent
    | RerunSourceCorrupt
    | RerunNotEligible
    | RerunInputUnavailable
    | RerunInputCorrupt
    | RerunKeyConflict
    | RerunKeyReplay
)


async def _run_prepared(
    connection: AsyncConnection, statement: PreparedStatement
) -> tuple[object, ...]:
    """Run one core statement on the caller's connection and give its one row
    as a tuple in column order. A ``memoryview`` of a ``bytea`` column is
    passed as ``bytes``. A database error raises from the caller's driver."""
    row = (await connection.execute(text(statement.sql), statement.parameters)).one()
    return tuple(_cell(value) for value in row)


def _run_prepared_blocking(
    connection: Connection, statement: PreparedStatement
) -> tuple[object, ...]:
    """``_run_prepared`` on a blocking connection of the caller."""
    row = connection.execute(text(statement.sql), statement.parameters).one()
    return tuple(_cell(value) for value in row)


def _cell(value: object) -> object:
    """``value`` as the core reads it: a ``memoryview`` as ``bytes``. Pure."""
    match value:
        case memoryview():
            return value.tobytes()
        case _:
            return value


def _write_outcome(
    record: RerunEnqueuedRecord | RerunKeyReplayRecord | RerunKeyConflictRecord,
) -> RerunEnqueued | RerunKeyReplay | RerunKeyConflict:
    """The public outcome of a core write record. Pure."""
    match record:
        case RerunEnqueuedRecord(
            new_task_id=new_task_id,
            source_task_id=source_task_id,
            rerun_root_task_id=rerun_root_task_id,
        ):
            return RerunEnqueued(
                new_task_id=new_task_id,
                source_task_id=source_task_id,
                rerun_root_task_id=rerun_root_task_id,
            )
        case RerunKeyReplayRecord(existing_task_id=existing_task_id):
            return RerunKeyReplay(existing_task_id=existing_task_id)
        case RerunKeyConflictRecord(task_id=task_id, owner_task_id=owner_task_id):
            return RerunKeyConflict(task_id=task_id, owner_task_id=owner_task_id)
        case _ as unreachable:
            assert_never(unreachable)


def _class_not_registered(class_key: str) -> ValueError:
    # The policy's key routes the NEW request's eventual history row into a
    # LIST partition; an unregistered key has none, so the failure would
    # otherwise arrive at the rerun's completion as a partition-routing
    # error. A caller precondition raises before any write.
    return ValueError(
        f'retention class {class_key!r} is not '
        'registered: the new request would have no history partition. '
        'Add the period to the maintenance owner\'s queue_retention or '
        'retention_periods, and rerun after its next pass'
    )


@dataclass(frozen=True, slots=True)
class _CoreRequest:
    """The request and the policy of one rerun, as the core takes them."""

    command: RerunCommandRecord
    policy: RerunPolicyRecord


def _core_request(
    command: RerunCommand,
    policy: RerunEnqueuePolicy,
    trace: TraceArgument,
) -> _CoreRequest:
    """The core's records of ``command``, ``policy`` and ``trace``. Pure."""
    return _CoreRequest(
        command=RerunCommandRecord(
            source_task_id=command.source_task_id,
            good_until=command.good_until,
            idempotency_key=command.idempotency_key,
            trace=trace,
        ),
        policy=RerunPolicyRecord(
            retention_class_key=policy.retention_class_key,
            retain_rerun_input=policy.retain_rerun_input,
            reservation_window_us=policy.reservation_window // timedelta(microseconds=1),
        ),
    )


def _write_or_outcome(
    request: _CoreRequest,
    read: PreparedStatement,
    read_row: tuple[object, ...],
) -> RerunWriteRecord | RerunOutcome:
    """The core's decision on the read row: the write to run, or the outcome
    of a rerun that writes nothing. Does no I/O.

    Raises:
        ValueError, CorePreconditionFailed, CoreContractViolation: as
            ``rerun_task_in_async`` lists them. Nothing is written.
    """
    prepared = prepare_rerun_write(
        read,
        read_row,
        request.command,
        request.policy,
        sent_at=datetime.now(timezone.utc),
    )
    match prepared:
        case RerunWriteRecord():
            return prepared
        case RerunSourceLiveRecord(task_id=task_id):
            return RerunSourceLive(task_id=task_id)
        case RerunSourceAbsentRecord(task_id=task_id, reason=reason):
            return RerunSourceAbsent(task_id=task_id, reason=TaskAbsenceReason(reason))
        case RerunSourceCorruptRecord(task_id=task_id, detail=detail):
            return RerunSourceCorrupt(task_id=task_id, detail=detail)
        case RerunNotEligibleRecord(task_id=task_id, reason=reason):
            return RerunNotEligible(task_id=task_id, reason=NotEligibleReason(reason))
        case RerunInputUnavailableRecord(task_id=task_id, disposition=disposition):
            return RerunInputUnavailable(task_id=task_id, disposition=disposition)
        case RerunInputCorruptRecord(task_id=task_id, detail=detail):
            return RerunInputCorrupt(task_id=task_id, detail=detail)
        case RerunClassNotRegistered(class_key=class_key):
            raise _class_not_registered(class_key)
        case RerunClientFamilyMismatch(exception=exception):
            raise exception
        case RerunUnknownStoredFamily(exception=exception):
            raise exception
        case RerunIdempotencyKeyRefused(cause=cause):
            raise ValueError(cause)
        case IdMintRefused(cause=cause):
            raise ValueError(cause)
        case _ as unreachable:
            assert_never(unreachable)


async def rerun_task_in_async(
    connection: AsyncConnection,
    command: RerunCommand,
    policy: RerunEnqueuePolicy,
) -> RerunOutcome:
    """Execute one rerun inside the caller-owned transaction.

    The core builds each statement and decides the outcome
    (``horsies._core.rerun_read``, ``prepare_rerun_write``,
    ``decode_rerun_write``); ``connection`` runs the two statements (the
    source read ``horsies_rerun_source_read``, then for an eligible source
    one ``horsies_enqueue_task`` with the lineage ``RERUN``), so the rerun
    commits or rolls back with the caller's own writes. Every outcome but
    ``RerunEnqueued`` writes nothing.

    The new task stores no trace carrier, so each of its attempts is the
    root of a new trace. ``Horsies.rerun_task_in_async`` (and the blocking
    ``Horsies.rerun_task_in``) is the same rerun with the tracing setting of
    the app: with ``OtelTracing`` the new task stores the carrier of the
    caller's current context.

    Raises:
        ValueError: the new request's retention class is not registered, the
            idempotency key is not valid, or the task id source refused to
            mint. Nothing is written.
        CorePreconditionFailed: the database was installed by the other
            client family, or records none (subject ``rerun_task_in``, not
            retryable). Nothing is written.
        CoreContractViolation: the family row of the database holds a value
            that is not a client family: corrupt stored data (subject
            ``rerun_task_in``; the message names the stored value). Nothing is
            written.
        CoreError: the core of this process cannot run (forked after the
            core started, or stopped).
        A database error of a statement raises from the caller's driver.
    """
    return await rerun_with_trace_async(connection, command, policy, NO_TRACE_CARRIER)


async def rerun_with_trace_async(
    connection: AsyncConnection,
    command: RerunCommand,
    policy: RerunEnqueuePolicy,
    trace: TraceArgument,
) -> RerunOutcome:
    """``rerun_task_in_async`` whose new task stores ``trace``: ``NO_TRACE_CARRIER``,
    or the ``TraceCarrier`` of the caller's current context. The core checks
    the bounds of the carrier: a carrier that breaks one is not stored and
    never refuses the rerun. Raises as ``rerun_task_in_async``."""
    request = _core_request(command, policy, trace)
    read = core_rerun_read(request.command, request.policy)
    read_row = await _run_prepared(connection, read)
    match _write_or_outcome(request, read, read_row):
        case RerunWriteRecord() as write:
            written = await _run_prepared(connection, write.statement)
            return _write_outcome(decode_rerun_write(write, written))
        case outcome:
            return outcome


def rerun_with_trace(
    connection: Connection,
    command: RerunCommand,
    policy: RerunEnqueuePolicy,
    trace: TraceArgument,
) -> RerunOutcome:
    """``rerun_with_trace_async`` on a blocking connection of the caller:
    the same statements, outcomes and errors."""
    request = _core_request(command, policy, trace)
    read = core_rerun_read(request.command, request.policy)
    read_row = _run_prepared_blocking(connection, read)
    match _write_or_outcome(request, read, read_row):
        case RerunWriteRecord() as write:
            written = _run_prepared_blocking(connection, write.statement)
            return _write_outcome(decode_rerun_write(write, written))
        case outcome:
            return outcome
