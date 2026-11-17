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
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID

from typing import Any, Final, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.brokers.enqueue_task import (
    ENQUEUE_TASK_SQL,
    ConflictUnverifiable,
    EnqueueKey,
    EnqueueOutcome,
    EnqueueRow,
    Inserted,
    KeyConflict,
    Keyed,
    KeyReplay,
    PayloadMismatch,
    RerunOf,
    SameCommand,
    SendKind,
    StartNow,
    Unkeyed,
    enqueue_task_params,
    read_enqueue_outcome,
)
from horsies.core.codec.task_options import stored_execution

from ..ddl.tables import FOREVER_CLASS_KEY
from ..errors import HistoryContractError
from ..periods import parse_retention_period, period_class_key
from horsies.core.types.result import Err, Ok
from ..identity.fingerprint import EnqueueCommandV1
from ..identity.keys import (
    IDEMPOTENCY_SCOPE_VERSION,
    ScopedIdempotencyKey,
    validate_reservation_window,
)
from ..identity.uuid7 import mint_task_id
from ...utils.db import uuid_column_value
from horsies.core.types.status import TaskStatus
from ..archive.attempts import decode_attempt_snapshot
from ..archive.versions import DecodedArchiveValue
from ..reads.detail import (
    TaskAbsenceReason,
    absence_reason_of,
    history_status,
)
from .input_envelope import (
    InputEnvelopeCorrupt,
    InputEnvelopeVersionUnknown,
    ReconstructedInput,
    StoredTaskOptions,
    decode_input_envelope,
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


@dataclass(frozen=True, slots=True)
class RerunClassUnregistered:
    """The new request's retention class has no class row. ``rerun_task``
    raises ``ValueError`` for it: a caller precondition."""

    class_key: str


@dataclass(frozen=True, slots=True)
class RerunSource:
    """The facts of a history source that the rerun decides on and copies.

    The stored status is a ``TaskStatus`` member and the attempt snapshot
    decoded; a source that fails either is ``RerunSourceCorrupt``.
    """

    task_id: UUID
    task_name: str
    queue_name: str
    priority: int
    max_retries: int
    status: TaskStatus
    is_workflow_task: bool
    rerun_root_task_id: UUID | None
    rerun_input_disposition: str
    rerun_input_version: int | None
    rerun_input_digest: bytes | None
    rerun_input_inline: bytes | None


type RerunSourceRead = (
    RerunClassUnregistered
    | RerunSourceLive
    | RerunSourceAbsent
    | RerunSourceCorrupt
    | RerunSource
)
"""The answer of ``read_rerun_source``."""

_RERUN_SOURCE_SQL: Final = text(
    'SELECT * FROM horsies_rerun_source_read(CAST(:task_id AS uuid), :class_key)'
)


def _optional_bytes(value: Any) -> bytes | None:
    return bytes(value) if value is not None else None


def _history_source(task_id: UUID, row: Any) -> RerunSource | RerunSourceCorrupt:
    """The history source of one rerun read row, or the corruption it carries."""
    try:
        status = history_status(task_id, row.status)
    except HistoryContractError as exc:
        return RerunSourceCorrupt(task_id=task_id, exception=exc)
    decoded = decode_attempt_snapshot(
        version=row.attempt_archive_version,
        payload=bytes(row.attempt_snapshot),
        digest=bytes(row.attempt_snapshot_digest),
    )
    match decoded:
        case DecodedArchiveValue():
            pass
        case _:
            return RerunSourceCorrupt(
                task_id=task_id,
                exception=HistoryContractError(
                    f'attempt snapshot failed to decode: {decoded!r}'
                ),
            )
    return RerunSource(
        task_id=uuid_column_value(row.task_id, column='task_id'),
        task_name=row.task_name,
        queue_name=row.queue_name,
        priority=row.priority,
        max_retries=row.max_retries,
        status=status,
        is_workflow_task=row.is_workflow_task,
        rerun_root_task_id=(
            uuid_column_value(row.rerun_root_task_id, column='rerun_root_task_id')
            if row.rerun_root_task_id is not None
            else None
        ),
        rerun_input_disposition=row.rerun_input_disposition,
        rerun_input_version=row.rerun_input_version,
        rerun_input_digest=_optional_bytes(row.rerun_input_digest),
        rerun_input_inline=_optional_bytes(row.rerun_input_inline),
    )


async def read_rerun_source(
    connection: AsyncConnection,
    source_task_id: UUID,
    class_key: str,
) -> RerunSourceRead:
    """Read the source of a rerun and the class of the new request: one statement.

    Calls ``horsies_rerun_source_read`` in the caller's transaction; the
    detail reader's leaf probe keeps ACCESS SHARE on the source leaf until
    the transaction ends. Outcomes: the class is unregistered (checked
    first), the source is live, absent (with the reason of
    ``horsies_task_absence_reason``), corrupt (a location, absence reason or
    status outside its set, or an attempt snapshot that does not decode), or
    a history source. A database error propagates. Writes nothing.
    """
    row = (
        await connection.execute(
            _RERUN_SOURCE_SQL,
            {'task_id': source_task_id, 'class_key': class_key},
        )
    ).one()
    if not row.class_registered:
        return RerunClassUnregistered(class_key=class_key)
    match row.location:
        case 'LIVE':
            return RerunSourceLive(task_id=source_task_id)
        case 'ABSENT':
            try:
                reason = absence_reason_of(source_task_id, row.absence_reason)
            except HistoryContractError as exc:
                return RerunSourceCorrupt(task_id=source_task_id, exception=exc)
            return RerunSourceAbsent(task_id=source_task_id, reason=reason)
        case 'HISTORY':
            return _history_source(source_task_id, row)
        case other:
            return RerunSourceCorrupt(
                task_id=source_task_id,
                exception=HistoryContractError(
                    f'staged detail returned unknown location {other!r}'
                ),
            )


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


async def rerun_task(
    connection: AsyncConnection,
    command: RerunTask,
    policy: RerunEnqueuePolicy,
) -> RerunOutcome:
    """Execute one rerun inside the caller-owned transaction."""
    read = await read_rerun_source(
        connection, command.source_task_id, policy.retention_class_key
    )
    match read:
        case RerunClassUnregistered():
            # The policy's key routes the NEW request's eventual history row
            # into a LIST partition; an unregistered key has none, so the
            # failure would otherwise arrive at the rerun's completion as a
            # partition-routing error. A caller precondition raises before
            # any write.
            raise ValueError(
                f'retention class {policy.retention_class_key!r} is not '
                'registered: the new request would have no history partition. '
                'Add the period to the maintenance owner\'s queue_retention or '
                'retention_periods, and rerun after its next pass'
            )
        case RerunSourceLive() | RerunSourceAbsent() | RerunSourceCorrupt():
            return read
        case RerunSource():
            source = read

    if source.is_workflow_task:
        return RerunNotEligible(
            task_id=command.source_task_id,
            reason=NotEligibleReason.WORKFLOW_TASK,
        )
    if source.status is TaskStatus.COMPLETED:
        return RerunNotEligible(
            task_id=command.source_task_id,
            reason=NotEligibleReason.COMPLETED_SOURCE,
        )

    if source.rerun_input_disposition != 'INLINE':
        return RerunInputUnavailable(
            task_id=command.source_task_id,
            disposition=source.rerun_input_disposition,
        )
    if (
        source.rerun_input_version is None
        or source.rerun_input_inline is None
        or source.rerun_input_digest is None
    ):
        return RerunInputCorrupt(
            task_id=command.source_task_id,
            detail='inline disposition with an incomplete envelope',
        )
    decoded = decode_input_envelope(
        version=source.rerun_input_version,
        payload=source.rerun_input_inline,
        digest=source.rerun_input_digest,
    )
    match decoded:
        case ReconstructedInput():
            pass
        case InputEnvelopeVersionUnknown(version=version):
            return RerunInputCorrupt(
                task_id=command.source_task_id,
                detail=f'unknown input-envelope version {version}',
            )
        case InputEnvelopeCorrupt(detail=corruption):
            return RerunInputCorrupt(
                task_id=command.source_task_id, detail=corruption
            )
    match decoded.options:
        case None:
            options_json = None
            options_value = None
        case StoredTaskOptions(text=stored_text, value=stored_value):
            options_json = stored_text
            options_value = stored_value
    # The rerun runs with the execution class of the stored request.
    match stored_execution(options_value):
        case Err(err_value=invalid_execution):
            return RerunInputCorrupt(
                task_id=command.source_task_id,
                detail=f'stored task options: {invalid_execution.detail}',
            )
        case Ok(ok_value=execution):
            pass

    new_task_id = mint_task_id()
    root_task_id = source.rerun_root_task_id or source.task_id
    # The one fingerprint over the stored input: the args element is
    # always null and the kwargs element is the input text.
    fingerprint = EnqueueCommandV1(
        task_name=source.task_name,
        queue_name=source.queue_name,
        priority=source.priority,
        args_json=None,
        kwargs_json=decoded.input_json,
        good_until=command.deadline,
        enqueue_delay_seconds=None,
        task_options_json=options_json,
        retention_class_key=policy.retention_class_key,
        retain_rerun_input=policy.retain_rerun_input,
        rerun_of_task_id=str(source.task_id),
        rerun_root_task_id=str(root_task_id),
    ).fingerprint

    key: EnqueueKey
    match command.caller_key:
        case None:
            key = Unkeyed()
        case caller_key:
            key = Keyed(
                digest=ScopedIdempotencyKey(
                    task_name=source.task_name, key=caller_key
                ).digest,
                scope_version=IDEMPOTENCY_SCOPE_VERSION,
                window=policy.reservation_window,
            )
    # The row stores no envelope: the move builds it from ``input`` and
    # ``task_options``. Both texts are the source's, unchanged, so when the
    # new row's policy retains its input, its envelope has the source
    # envelope's bytes and digest.
    row = EnqueueRow(
        task_id=new_task_id,
        task_name=source.task_name,
        queue_name=source.queue_name,
        priority=source.priority,
        stored_input=decoded.input_json,
        task_options=options_json,
        execution=execution,
        retention_class_key=policy.retention_class_key,
        retain_rerun_input=policy.retain_rerun_input,
        max_retries=source.max_retries,
        good_until=command.deadline,
        sent_at=datetime.now(timezone.utc),
        start=StartNow(),
        command_fingerprint=fingerprint,
        key=key,
        lineage=RerunOf(source_task_id=source.task_id, root_task_id=root_task_id),
    )
    rows = (
        await connection.execute(
            ENQUEUE_TASK_SQL, enqueue_task_params(row, SendKind.FIRST)
        )
    ).all()
    return _rerun_write_outcome(
        read_enqueue_outcome(rows),
        new_task_id=new_task_id,
        source_task_id=source.task_id,
        rerun_root_task_id=root_task_id,
    )


def _rerun_write_outcome(
    written: EnqueueOutcome,
    *,
    new_task_id: UUID,
    source_task_id: UUID,
    rerun_root_task_id: UUID,
) -> RerunEnqueued | RerunKeyReplay | RerunKeyConflict:
    """The rerun outcome of the write of ``new_task_id``. Pure.

    ``SAME_COMMAND`` is the write of this id that already ran in the caller's
    transaction: the rerun is enqueued. ``new_task_id`` was minted for this
    write, so another row cannot hold it: ``PAYLOAD_MISMATCH`` and
    ``CONFLICT_UNVERIFIABLE`` are defects of the id mint or of the function
    (``AssertionError`` with the id and the outcome).
    """
    match written:
        case Inserted() | SameCommand():
            return RerunEnqueued(
                new_task_id=new_task_id,
                source_task_id=source_task_id,
                rerun_root_task_id=rerun_root_task_id,
            )
        case KeyReplay(task_id=existing):
            return RerunKeyReplay(existing_task_id=existing)
        case KeyConflict(owner_task_id=owner):
            return RerunKeyConflict(
                task_id=source_task_id,
                reserved_by_task_id=owner,
            )
        case PayloadMismatch() | ConflictUnverifiable():
            raise AssertionError(
                f'the rerun write of the minted task id {new_task_id} answered '
                f'{written!r}: no other row can hold a minted id'
            )
        case _ as unreachable:
            assert_never(unreachable)
