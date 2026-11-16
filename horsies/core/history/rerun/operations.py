"""The rerun operation: one recorded request re-executed as a new one.

Resolution goes through the staged detail function; a live source is a
typed refusal (a live task is retried by its own lifecycle, never
rerun), and an absence carries the detail read's reason. Eligibility
precedes input, mirroring the ratified ladder: workflow backing tasks
and completed sources refuse before any envelope is touched. Input
reconstruction reads the envelope that the move built, with the digest
verified before parse; every non-INLINE disposition is typed
unavailable.

The new row is written strictly by the provenance table: replayed
fields from the source row or its carried envelope, the deadline as the
caller's explicit choice, policy resolved at enqueue, identity and
lifecycle state fresh, the lineage pair atomic. A keyed rerun rides the
reservation registry exactly as frozen; because the fingerprint covers
rerun lineage, a key still reserved by the source request conflicts by
contract rather than aliasing its rerun.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.codec.task_options import stored_execution

from ..ddl.tables import FOREVER_CLASS_KEY
from ..errors import HistoryContractError
from ..periods import parse_retention_period, period_class_key
from horsies.core.types.result import Err, Ok
from ..identity.fingerprint import EnqueueCommandV1
from ..names import RETENTION_CLASSES
from ..identity.keys import (
    IDEMPOTENCY_SCOPE_VERSION,
    ScopedIdempotencyKey,
    validate_reservation_window,
)
from ..identity.reservations import (
    ReservationApplied,
    ReservationConflict,
    ReservationReplay,
    claim_key_reservation,
)
from ..identity.uuid7 import mint_task_id
from ..names import LIVE_TASKS
from horsies.core.types.status import TaskStatus
from ..reads.detail import (
    HistoryTaskDetail,
    LiveTaskLocation,
    TaskAbsenceReason,
    TaskDetailAbsent,
    read_task_detail,
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
    registered = (
        await connection.execute(
            text(
                f'SELECT EXISTS (SELECT 1 FROM {RETENTION_CLASSES} '
                'WHERE class_key = :class_key)'
            ),
            {'class_key': policy.retention_class_key},
        )
    ).scalar_one()
    if not registered:
        # The policy's key routes the NEW request's eventual history row
        # into a LIST partition; an unregistered key has none, so the
        # failure would otherwise arrive at the rerun's completion as a
        # partition-routing error. A caller precondition raises before
        # any read or write.
        raise ValueError(
            f'retention class {policy.retention_class_key!r} is not '
            'registered: the new request would have no history partition. '
            'Add the period to the maintenance owner\'s queue_retention or '
            'retention_periods, and rerun after its next pass'
        )
    try:
        detail = await read_task_detail(
            connection, task_id=command.source_task_id
        )
    except HistoryContractError as exc:
        return RerunSourceCorrupt(
            task_id=command.source_task_id, exception=exc,
        )
    match detail:
        case LiveTaskLocation():
            return RerunSourceLive(task_id=command.source_task_id)
        case TaskDetailAbsent(reason=reason):
            return RerunSourceAbsent(
                task_id=command.source_task_id,
                reason=reason,
            )
        case HistoryTaskDetail():
            pass

    if detail.is_workflow_task:
        return RerunNotEligible(
            task_id=command.source_task_id,
            reason=NotEligibleReason.WORKFLOW_TASK,
        )
    if detail.status is TaskStatus.COMPLETED:
        return RerunNotEligible(
            task_id=command.source_task_id,
            reason=NotEligibleReason.COMPLETED_SOURCE,
        )

    if detail.rerun_input_disposition != 'INLINE':
        return RerunInputUnavailable(
            task_id=command.source_task_id,
            disposition=detail.rerun_input_disposition,
        )
    if (
        detail.rerun_input_version is None
        or detail.rerun_input_inline is None
        or detail.rerun_input_digest is None
    ):
        return RerunInputCorrupt(
            task_id=command.source_task_id,
            detail='inline disposition with an incomplete envelope',
        )
    decoded = decode_input_envelope(
        version=detail.rerun_input_version,
        payload=detail.rerun_input_inline,
        digest=detail.rerun_input_digest,
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
    root_task_id = detail.rerun_root_task_id or detail.task_id
    # The one fingerprint over the stored input: the args element is
    # always null and the kwargs element is the input text.
    fingerprint = EnqueueCommandV1(
        task_name=detail.task_name,
        queue_name=detail.queue_name,
        priority=detail.priority,
        args_json=None,
        kwargs_json=decoded.input_json,
        good_until=command.deadline,
        enqueue_delay_seconds=None,
        task_options_json=options_json,
        retention_class_key=policy.retention_class_key,
        retain_rerun_input=policy.retain_rerun_input,
        rerun_of_task_id=str(detail.task_id),
        rerun_root_task_id=str(root_task_id),
    ).fingerprint

    if command.caller_key is not None:
        scoped = ScopedIdempotencyKey(
            task_name=detail.task_name, key=command.caller_key
        )
        claim = await claim_key_reservation(
            connection,
            key_digest=scoped.digest,
            key_scope_version=IDEMPOTENCY_SCOPE_VERSION,
            reservation_window_seconds=int(
                policy.reservation_window.total_seconds()
            ),
            fingerprint_version=1,
            fingerprint=fingerprint,
            task_id=new_task_id,
        )
        match claim:
            case ReservationReplay(task_id=existing):
                return RerunKeyReplay(existing_task_id=existing)
            case ReservationConflict(task_id=owner):
                return RerunKeyConflict(
                    task_id=command.source_task_id,
                    reserved_by_task_id=owner,
                )
            case ReservationApplied():
                pass

    await connection.execute(
        _INSERT_RERUN_SQL,
        {
            'id': str(new_task_id),
            'task_name': detail.task_name,
            'queue_name': detail.queue_name,
            'priority': detail.priority,
            'input': decoded.input_json,
            'task_options': options_json,
            'execution': execution.value,
            'retention_class_key': policy.retention_class_key,
            'sent_at': datetime.now(timezone.utc),
            'fingerprint': fingerprint,
            'key_digest': (
                ScopedIdempotencyKey(
                    task_name=detail.task_name, key=command.caller_key
                ).digest
                if command.caller_key is not None
                else None
            ),
            'max_retries': detail.max_retries,
            'good_until': command.deadline,
            'retain': policy.retain_rerun_input,
            'rerun_of': str(detail.task_id),
            'rerun_root': str(root_task_id),
        },
    )
    return RerunEnqueued(
        new_task_id=new_task_id,
        source_task_id=detail.task_id,
        rerun_root_task_id=root_task_id,
    )


_INSERT_RERUN_SQL = text(
    f"""
    INSERT INTO {LIVE_TASKS} (
        id, task_name, queue_name, priority, input, task_options,
        execution, retention_class_key, sent_at, created_at,
        command_fingerprint_version, command_fingerprint,
        idempotency_key_digest, status, enqueued_at, retry_count,
        max_retries, good_until, is_workflow_task, updated_at,
        retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        CAST(:id AS uuid), :task_name, :queue_name, :priority, :input,
        :task_options,
        :execution, :retention_class_key, :sent_at, statement_timestamp(),
        1, :fingerprint,
        :key_digest, 'PENDING', statement_timestamp(), 0,
        :max_retries, :good_until, FALSE, now(),
        :retain, CAST(:rerun_of AS uuid), CAST(:rerun_root AS uuid)
    )
    """
)
"""The new PENDING row. The row stores no envelope: the move builds it
from ``input`` and ``task_options``. Both texts are the source's,
unchanged, so when the new row's policy retains its input, its envelope
has the source envelope's bytes and digest."""
