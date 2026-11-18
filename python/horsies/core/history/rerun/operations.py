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
fields from the source row or its carried envelope, the deadline as the
caller's explicit choice, policy resolved at enqueue, identity and
lifecycle state fresh, the lineage pair atomic. The write is one call of
``horsies_enqueue_task`` (0004_lifecycle_functions.sql) with the lineage
``RERUN``, in the caller's transaction: a keyed rerun claims its key inside
that call. Because the fingerprint covers rerun lineage, a key still
reserved by the source request conflicts by contract rather than aliasing
its rerun.

``rerun_task`` runs in the core: the core builds the read and the write
and decides the outcome (``horsies._core.rerun_read``,
``prepare_rerun_write``, ``decode_rerun_write``), and the caller's
connection runs both statements.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID

from typing import assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies._core import (
    IdMintRefused,
    PreparedStatement,
    RerunCallerKeyRefused,
    RerunClassNotRegistered,
    RerunClientFamilyMismatch,
    RerunCommandRecord,
    RerunEnqueuedRecord,
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

from ..ddl.tables import FOREVER_CLASS_KEY
from ..errors import HistoryContractError
from ..periods import parse_retention_period, period_class_key
from horsies.core.types.result import Err, Ok
from ..identity.keys import validate_reservation_window
from ..reads.detail import (
    TaskAbsenceReason,
)


@dataclass(frozen=True, slots=True)
class RerunTask:
    """One rerun request. The deadline is explicit: a value or the
    explicit no-deadline choice, never a silent default."""

    source_task_id: UUID
    deadline: datetime | None
    caller_key: str | None = None


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

    ``exception`` is the ``HistoryContractError`` of the source read: a
    stored status that is not a ``TaskStatus`` member, an attempt snapshot
    that does not decode, or a location that the staged read does not
    define. Nothing is written. A repeat gives the same result until the
    stored row or the server changes."""

    task_id: UUID
    exception: HistoryContractError


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
    """The caller key is reserved by a different canonical command —
    including the source request itself, whose fingerprint differs
    from its rerun's by lineage. The contract working, not a defect."""

    task_id: UUID
    reserved_by_task_id: UUID


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
        case RerunKeyConflictRecord(task_id=task_id, reserved_by_task_id=reserved_by):
            return RerunKeyConflict(task_id=task_id, reserved_by_task_id=reserved_by)
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


async def rerun_task(
    connection: AsyncConnection,
    command: RerunTask,
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

    Raises:
        ValueError: the new request's retention class is not registered, the
            caller key is not a valid idempotency key, or the task id source
            refused to mint. Nothing is written.
        CorePreconditionFailed: the database was installed by the other
            client family, or records none (subject ``rerun_task``, not
            retryable). Nothing is written.
        CoreContractViolation: the family row of the database holds a value
            that is not a client family: corrupt stored data (subject
            ``rerun_task``; the message names the stored value). Nothing is
            written.
        CoreError: the core of this process cannot run (forked after the
            core started, or stopped).
        A database error of a statement raises from the caller's driver.
    """
    core_command = RerunCommandRecord(
        source_task_id=command.source_task_id,
        deadline=command.deadline,
        caller_key=command.caller_key,
    )
    core_policy = RerunPolicyRecord(
        retention_class_key=policy.retention_class_key,
        retain_rerun_input=policy.retain_rerun_input,
        reservation_window_us=policy.reservation_window // timedelta(microseconds=1),
    )
    read = core_rerun_read(core_command, core_policy)
    read_row = await _run_prepared(connection, read)
    prepared = prepare_rerun_write(
        read,
        read_row,
        core_command,
        core_policy,
        sent_at=datetime.now(timezone.utc),
    )
    match prepared:
        case RerunWriteRecord():
            written = await _run_prepared(connection, prepared.statement)
            return _write_outcome(decode_rerun_write(prepared, written))
        case RerunSourceLiveRecord(task_id=task_id):
            return RerunSourceLive(task_id=task_id)
        case RerunSourceAbsentRecord(task_id=task_id, reason=reason):
            return RerunSourceAbsent(task_id=task_id, reason=TaskAbsenceReason(reason))
        case RerunSourceCorruptRecord(task_id=task_id, detail=detail):
            return RerunSourceCorrupt(task_id=task_id, exception=HistoryContractError(detail))
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
        case RerunCallerKeyRefused(cause=cause):
            raise ValueError(cause)
        case IdMintRefused(cause=cause):
            raise ValueError(cause)
        case _ as unreachable:
            assert_never(unreachable)
