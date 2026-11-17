"""The rerun source read, before ``horsies_rerun_source_read``.

``rerun_task`` (``python/horsies/core/history/rerun/operations.py``) sent, in the
caller's transaction: the class check of the new task's retention class
(``CLASS_REGISTERED_SQL``), then ``read_task_detail``
(``python/horsies/core/history/reads/detail.py``), one statement over the published
detail reader (``DETAIL_SQL``). An absent source was ``NOT_CLASSIFIED``: the
detail read classified no absence. A stored status that is not a
``TaskStatus`` member, an attempt snapshot that does not decode, or a location
that the reader does not define was a corrupt source.

The answers are mapped onto the candidate's ``RerunSourceRead`` so that the
two sides compare as values.
"""

from __future__ import annotations

from typing import Final
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.archive.attempts import decode_attempt_snapshot
from horsies.core.history.archive.versions import DecodedArchiveValue
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.detail import TaskAbsenceReason
from horsies.core.history.rerun.operations import (
    RerunClassUnregistered,
    RerunSource,
    RerunSourceAbsent,
    RerunSourceCorrupt,
    RerunSourceLive,
    RerunSourceRead,
)
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import uuid_column_value

CLASS_REGISTERED_SQL: Final = text(
    'SELECT EXISTS (SELECT 1 FROM horsies_retention_classes WHERE class_key = :class_key)'
)

DETAIL_SQL: Final = text(
    'SELECT location, (detail.task_row).* '
    'FROM horsies_task_detail_staged(CAST(:task_id AS uuid)) AS detail'
)

_STATUS_BY_VALUE: Final = {status.value: status for status in TaskStatus}


def _corrupt(task_id: UUID, message: str) -> RerunSourceCorrupt:
    return RerunSourceCorrupt(task_id=task_id, exception=HistoryContractError(message))


async def read_rerun_source_before(
    connection: AsyncConnection, source_task_id: UUID, class_key: str
) -> RerunSourceRead:
    """The two statements and the decode of the rerun source before the switch."""
    registered = (
        await connection.execute(CLASS_REGISTERED_SQL, {'class_key': class_key})
    ).scalar_one()
    if not registered:
        return RerunClassUnregistered(class_key=class_key)
    row = (
        await connection.execute(DETAIL_SQL, {'task_id': source_task_id})
    ).one_or_none()
    if row is None:
        return RerunSourceAbsent(
            task_id=source_task_id, reason=TaskAbsenceReason.NOT_CLASSIFIED
        )
    match row.location:
        case 'LIVE':
            return RerunSourceLive(task_id=source_task_id)
        case 'HISTORY':
            pass
        case other:
            return _corrupt(
                source_task_id, f'staged detail returned unknown location {other!r}'
            )
    status = _STATUS_BY_VALUE.get(row.status)
    if status is None:
        return _corrupt(
            source_task_id,
            f'history row {row.task_id} has status {row.status!r}, which is not '
            'a task status',
        )
    decoded = decode_attempt_snapshot(
        version=row.attempt_archive_version,
        payload=bytes(row.attempt_snapshot),
        digest=bytes(row.attempt_snapshot_digest),
    )
    match decoded:
        case DecodedArchiveValue():
            pass
        case _:
            return _corrupt(
                source_task_id, f'attempt snapshot failed to decode: {decoded!r}'
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
        rerun_input_digest=(
            bytes(row.rerun_input_digest) if row.rerun_input_digest is not None else None
        ),
        rerun_input_inline=(
            bytes(row.rerun_input_inline) if row.rerun_input_inline is not None else None
        ),
    )
