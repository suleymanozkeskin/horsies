"""The rerun operation: one recorded request re-executed as a new one.

Resolution goes through the staged detail function; a live source is a
typed refusal (a live task is retried by its own lifecycle, never
rerun), and absence classifies against the published birth floor.
Eligibility precedes input, mirroring the ratified ladder: workflow
backing tasks and completed sources refuse before any envelope is
touched. Input reconstruction is content v1 with the digest verified
before parse; every non-INLINE disposition is typed unavailable and
CARRIES the reference locator the caller wrote at enqueue — the input
is unavailable to the library, not unreachable to the caller.

The new row is written strictly by the provenance table: replayed
fields from the source row or its carried envelope, the deadline as the
caller's explicit choice, policy resolved at enqueue, identity and
lifecycle state fresh, the lineage pair atomic. A keyed rerun rides the
reservation registry exactly as frozen; because the fingerprint covers
rerun lineage, a key still reserved by the source request conflicts by
contract rather than aliasing its rerun.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from hashlib import sha256
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..identity.fingerprint import EnqueueCommandV1
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
from horsies.core.utils.fingerprint import enqueue_fingerprint
from ..reads.detail import (
    HistoryTaskDetail,
    LiveTaskLocation,
    TaskDetailAbsent,
    read_task_detail,
)
from .input_envelope import (
    INPUT_ENVELOPE_CODEC,
    INPUT_ENVELOPE_CONTENT_TYPE,
    INPUT_ENVELOPE_INLINE_MAX_BYTES,
    INPUT_ENVELOPE_VERSION,
    InputEnvelopeCorrupt,
    InputEnvelopeVersionUnknown,
    ReconstructedInput,
    decode_input_envelope,
    encode_input_envelope_v1,
)


@dataclass(frozen=True, slots=True)
class RerunTask:
    """One rerun request. The deadline is explicit: a value or the
    explicit no-deadline choice, never a silent default."""

    source_task_id: str
    deadline: datetime | None
    caller_key: str | None = None


@dataclass(frozen=True, slots=True)
class RerunEnqueuePolicy:
    """The NEW request's enqueue-time policy snapshot, supplied by the
    caller because policy lives in configuration, not in this module."""

    retention_class_key: str
    retain_rerun_input: bool
    reservation_window: timedelta

    def __post_init__(self) -> None:
        if not self.retention_class_key:
            raise ValueError('retention class key must be non-empty')
        validate_reservation_window(self.reservation_window)


class NotEligibleReason(Enum):
    COMPLETED_SOURCE = 'COMPLETED_SOURCE'
    WORKFLOW_TASK = 'WORKFLOW_TASK'


@dataclass(frozen=True, slots=True)
class RerunEnqueued:
    """The new request exists; the source record is untouched."""

    new_task_id: str
    source_task_id: str
    rerun_root_task_id: str


@dataclass(frozen=True, slots=True)
class RerunSourceLive:
    """The source is live; its own lifecycle owns retry."""

    task_id: str


@dataclass(frozen=True, slots=True)
class RerunSourceAbsent:
    """No retained record. True means the identifier predates all
    retained history; None means unclassifiable."""

    task_id: str
    predates_retained_floor: bool | None


@dataclass(frozen=True, slots=True)
class RerunNotEligible:
    task_id: str
    reason: NotEligibleReason


@dataclass(frozen=True, slots=True)
class RerunInputUnavailable:
    """The library cannot reconstruct input. The locator is the
    caller's own data, surfaced so an external fetch and a fresh
    enqueue remain possible: unavailable, not unreachable."""

    task_id: str
    disposition: str
    reference_locator: str | None


@dataclass(frozen=True, slots=True)
class RerunInputCorrupt:
    """Stored envelope fails integrity or shape; fail closed."""

    task_id: str
    detail: str


@dataclass(frozen=True, slots=True)
class RerunKeyConflict:
    """The caller key is reserved by a different canonical command —
    including the source request itself, whose fingerprint differs
    from its rerun's by lineage. The contract working, not a defect."""

    task_id: str
    reserved_by_task_id: str


@dataclass(frozen=True, slots=True)
class RerunKeyReplay:
    """The key already reserved this exact rerun command; the committed
    request is returned instead of a duplicate."""

    existing_task_id: str


type RerunOutcome = (
    RerunEnqueued
    | RerunSourceLive
    | RerunSourceAbsent
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
    detail = await read_task_detail(
        connection, task_id=command.source_task_id
    )
    match detail:
        case LiveTaskLocation():
            return RerunSourceLive(task_id=command.source_task_id)
        case TaskDetailAbsent(predates_retained_floor=predates):
            return RerunSourceAbsent(
                task_id=command.source_task_id,
                predates_retained_floor=predates,
            )
        case HistoryTaskDetail():
            pass

    if detail.is_workflow_task:
        return RerunNotEligible(
            task_id=command.source_task_id,
            reason=NotEligibleReason.WORKFLOW_TASK,
        )
    if detail.status == 'COMPLETED':
        return RerunNotEligible(
            task_id=command.source_task_id,
            reason=NotEligibleReason.COMPLETED_SOURCE,
        )

    if detail.rerun_input_disposition != 'INLINE':
        return RerunInputUnavailable(
            task_id=command.source_task_id,
            disposition=detail.rerun_input_disposition,
            reference_locator=detail.rerun_input_reference,
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

    new_task_id = mint_task_id()
    root_task_id = detail.rerun_root_task_id or detail.task_id
    args_json = _canonical_json(list(decoded.args))
    kwargs_json = _canonical_json(decoded.kwargs)
    options_json = (
        _canonical_json(decoded.options)
        if decoded.options is not None
        else None
    )
    fingerprint = EnqueueCommandV1(
        task_name=detail.task_name,
        queue_name=detail.queue_name,
        priority=detail.priority,
        args_json=args_json,
        kwargs_json=kwargs_json,
        good_until=command.deadline,
        enqueue_delay_seconds=None,
        task_options_json=options_json,
        retention_class_key=policy.retention_class_key,
        retain_rerun_input=policy.retain_rerun_input,
        rerun_of_task_id=detail.task_id,
        rerun_root_task_id=root_task_id,
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

    envelope = _prepare_envelope(decoded, retain=policy.retain_rerun_input)
    # enqueue_sha reconciliation: the same helper the send path uses,
    # computed over the rerun's OWN request values — it remains the
    # exact-ID resend comparator, never the key contract.
    rerun_sent_at = datetime.now(timezone.utc)
    rerun_enqueue_sha = enqueue_fingerprint(
        task_name=detail.task_name,
        queue_name=detail.queue_name,
        priority=detail.priority,
        args_json=args_json,
        kwargs_json=kwargs_json,
        sent_at=rerun_sent_at,
        good_until=command.deadline,
        enqueue_delay_seconds=None,
        task_options=options_json,
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {LIVE_TASKS} (
                id, task_name, queue_name, priority, args, kwargs,
                task_options, status, sent_at, enqueued_at, created_at,
                retry_count, max_retries, good_until, is_workflow_task,
                enqueue_sha,
                command_fingerprint_version, command_fingerprint,
                retention_class_key, input_digest,
                rerun_of_task_id, rerun_root_task_id,
                idempotency_key_digest,
                retain_rerun_input, prepared_rerun_input_disposition,
                prepared_rerun_input_version, prepared_rerun_input_codec,
                prepared_rerun_input_content_type,
                prepared_rerun_input_digest, prepared_rerun_input_inline,
                prepared_rerun_input_reference
            ) VALUES (
                CAST(:id AS uuid), :task_name, :queue_name, :priority,
                :args, :kwargs, :task_options, 'PENDING',
                :sent_at, statement_timestamp(), statement_timestamp(),
                0, :max_retries, :good_until, FALSE,
                :enqueue_sha,
                1, :fingerprint,
                :retention_class_key, :input_digest,
                CAST(:rerun_of AS uuid), CAST(:rerun_root AS uuid),
                :key_digest,
                :retain, :disposition,
                :env_version, :env_codec, :env_content_type,
                :env_digest, :env_inline, NULL
            )
            """
        ),
        {
            'id': new_task_id,
            'sent_at': rerun_sent_at,
            'enqueue_sha': rerun_enqueue_sha,
            'task_name': detail.task_name,
            'queue_name': detail.queue_name,
            'priority': detail.priority,
            'args': args_json,
            'kwargs': kwargs_json,
            'task_options': options_json,
            'max_retries': detail.max_retries,
            'good_until': command.deadline,
            'fingerprint': fingerprint,
            'retention_class_key': policy.retention_class_key,
            'input_digest': envelope['input_digest'],
            'rerun_of': detail.task_id,
            'rerun_root': root_task_id,
            'key_digest': (
                ScopedIdempotencyKey(
                    task_name=detail.task_name, key=command.caller_key
                ).digest
                if command.caller_key is not None
                else None
            ),
            'retain': policy.retain_rerun_input,
            'disposition': envelope['disposition'],
            'env_version': envelope['version'],
            'env_codec': envelope['codec'],
            'env_content_type': envelope['content_type'],
            'env_digest': envelope['digest'],
            'env_inline': envelope['inline'],
        },
    )
    return RerunEnqueued(
        new_task_id=new_task_id,
        source_task_id=detail.task_id,
        rerun_root_task_id=root_task_id,
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, separators=(',', ':'), sort_keys=True, ensure_ascii=False
    )


def _prepare_envelope(
    decoded: ReconstructedInput,
    *,
    retain: bool,
) -> dict[str, Any]:
    """Re-prepare the new request's envelope under current policy.

    The canonical serializer makes re-encoding byte-identical to the
    source envelope, so the digest carries over content identity; the
    disposition ladder mirrors enqueue: policy declines first, the
    inline bound judges the whole payload second.
    """
    payload = encode_input_envelope_v1(
        args=decoded.args, kwargs=decoded.kwargs, options=decoded.options
    )
    # input_digest identifies the input itself and carries on every
    # disposition; the envelope digest exists only when bytes are stored.
    digest = sha256(payload).digest()
    if not retain:
        return {
            'disposition': 'DECLINED_BY_POLICY',
            'version': None,
            'codec': None,
            'content_type': None,
            'digest': None,
            'inline': None,
            'input_digest': digest,
        }
    if len(payload) > INPUT_ENVELOPE_INLINE_MAX_BYTES:
        return {
            'disposition': 'OVER_BOUND',
            'version': None,
            'codec': None,
            'content_type': None,
            'digest': None,
            'inline': None,
            'input_digest': digest,
        }
    return {
        'disposition': 'INLINE',
        'version': INPUT_ENVELOPE_VERSION,
        'codec': INPUT_ENVELOPE_CODEC,
        'content_type': INPUT_ENVELOPE_CONTENT_TYPE,
        'digest': digest,
        'inline': payload,
        'input_digest': digest,
    }
