# pyright: reportPrivateUsage=false
"""The Python ``rerun_task`` before it called the core.

``rerun_task`` (``python/horsies/core/history/rerun/operations.py``) read the
source with ``read_rerun_source``, decided the outcome in Python, and wrote
the new task with one ``horsies_enqueue_task`` call with the lineage
``RERUN``, in the caller's transaction (since #1428). The function below is
that body, with its write outcome; the dual-run rebinding puts it in the
place of the production ``rerun_task``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import assert_never
from uuid import UUID

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
from horsies.core.history.identity.fingerprint import EnqueueCommandV1
from horsies.core.history.identity.keys import (
    IDEMPOTENCY_SCOPE_VERSION,
    ScopedIdempotencyKey,
)
from horsies.core.history.identity.uuid7 import mint_task_id
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
