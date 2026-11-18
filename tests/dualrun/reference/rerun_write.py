"""The rerun write, before ``horsies_enqueue_task``.

``rerun_task`` (``python/horsies/core/history/rerun/operations.py``) wrote the
new task with two statements in the caller's transaction: for a keyed rerun,
the key claim (``claim_key_reservation``, ``horsies_key_reservation_claim``),
then one INSERT of the PENDING row (``INSERT_RERUN_SQL``) that stamped
``created_at`` and ``enqueued_at`` with ``statement_timestamp()`` and
``updated_at`` with ``now()``. The source read, the eligibility, the envelope
decode and the fingerprint did not change in the switch; this copy calls the
production functions for them.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.codec.task_options import stored_execution
from horsies.core.history.identity.fingerprint import EnqueueCommandV1
from horsies.core.history.identity.keys import (
    IDEMPOTENCY_SCOPE_VERSION,
    ScopedIdempotencyKey,
)
from horsies.core.history.identity.reservations import (
    ReservationApplied,
    ReservationConflict,
    ReservationReplay,
    claim_key_reservation,
)
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.history.names import LIVE_TASKS
from horsies.core.history.rerun.input_envelope import (
    InputEnvelopeCorrupt,
    InputEnvelopeVersionUnknown,
    ReconstructedInput,
    StoredTaskOptions,
    decode_input_envelope,
)
from horsies.core.history.rerun.operations import (
    NotEligibleReason,
    RerunClassUnregistered,
    RerunEnqueued,
    RerunEnqueuePolicy,
    RerunInputCorrupt,
    RerunInputUnavailable,
    RerunKeyConflict,
    RerunKeyReplay,
    RerunNotEligible,
    RerunOutcome,
    RerunSource,
    RerunSourceAbsent,
    RerunSourceCorrupt,
    RerunSourceLive,
    RerunTask,
    read_rerun_source,
)
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus

INSERT_RERUN_SQL: Final = text(
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
"""The new PENDING row, as ``rerun_task`` wrote it before the switch."""


async def rerun_task_before(
    connection: AsyncConnection,
    command: RerunTask,
    policy: RerunEnqueuePolicy,
) -> RerunOutcome:
    """``rerun_task`` with the key claim and the INSERT of before the switch."""
    read = await read_rerun_source(
        connection, command.source_task_id, policy.retention_class_key
    )
    match read:
        case RerunClassUnregistered():
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
            return RerunInputCorrupt(task_id=command.source_task_id, detail=corruption)
    match decoded.options:
        case None:
            options_json = None
            options_value = None
        case StoredTaskOptions(text=stored_text, value=stored_value):
            options_json = stored_text
            options_value = stored_value
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
    fingerprint = EnqueueCommandV1(
        task_name=source.task_name,
        queue_name=source.queue_name,
        priority=source.priority,
        positional_args_json=None,
        input_json=decoded.input_json,
        good_until=command.deadline,
        enqueue_delay_seconds=None,
        task_options_json=options_json,
        retention_class_key=policy.retention_class_key,
        retain_rerun_input=policy.retain_rerun_input,
        rerun_of_task_id=str(source.task_id),
        rerun_root_task_id=str(root_task_id),
    ).fingerprint

    if command.caller_key is not None:
        scoped = ScopedIdempotencyKey(task_name=source.task_name, key=command.caller_key)
        claim = await claim_key_reservation(
            connection,
            key_digest=scoped.digest,
            key_scope_version=IDEMPOTENCY_SCOPE_VERSION,
            reservation_window=policy.reservation_window,
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

    _ = await connection.execute(
        INSERT_RERUN_SQL,
        {
            'id': str(new_task_id),
            'task_name': source.task_name,
            'queue_name': source.queue_name,
            'priority': source.priority,
            'input': decoded.input_json,
            'task_options': options_json,
            'execution': execution.value,
            'retention_class_key': policy.retention_class_key,
            'sent_at': datetime.now(UTC),
            'fingerprint': fingerprint,
            'key_digest': (
                ScopedIdempotencyKey(task_name=source.task_name, key=command.caller_key).digest
                if command.caller_key is not None
                else None
            ),
            'max_retries': source.max_retries,
            'good_until': command.deadline,
            'retain': policy.retain_rerun_input,
            'rerun_of': str(source.task_id),
            'rerun_root': str(root_task_id),
        },
    )
    return RerunEnqueued(
        new_task_id=new_task_id,
        source_task_id=source.task_id,
        rerun_root_task_id=root_task_id,
    )
