"""Task-detail read primitive over the staged detail function.

One client statement resolves a task to LIVE, a full decoded history
detail, or a typed absence. The composite `task_row` expands in SQL —
`(task_row).*` — so every stored column arrives as a real column, not a
composite the driver would hand back as text. Attempts on a history hit
decode from the row's snapshot columns: the move deletes a terminal
task's attempt rows, so the snapshot is the only home of attempt history
and it purges with the row.

The stored status is parsed here, one time, into `TaskStatus`. The
history parent has no status CHECK, so a value that is not a member can
be stored out of band. The read raises `HistoryContractError` for it, as
it does for an attempt snapshot that does not decode. A reader of
`HistoryTaskDetail.status` gets a member.

An absence is classified by ``horsies_task_absence_reason``
(0006_history_functions.sql), the rule of every reader: NOT_CLASSIFIED for
an id that is not a UUIDv7, PREDATES_RETAINED_HISTORY for a birth before the
latest upper anchor of a detached PERIOD leaf plus 5 s,
WITHIN_RETAINED_HISTORY otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies._catalog.history import TaskAbsenceReason as TaskAbsenceReason

from ...types.status import TaskStatus
from ...utils.db import uuid_column_value
from ..archive.attempts import AttemptRecord, decode_attempt_snapshot
from ..archive.versions import DecodedArchiveValue
from ..errors import HistoryContractError
from ..names import TASK_DETAIL_FUNCTION

_STATUS_BY_VALUE: dict[str, TaskStatus] = {
    status.value: status for status in TaskStatus
}
_REASON_BY_VALUE: dict[str, TaskAbsenceReason] = {
    reason.value: reason for reason in TaskAbsenceReason
}
_ABSENCE_REASON_SQL: Final = text(
    'SELECT horsies_task_absence_reason(CAST(:task_id AS uuid))'
)


@dataclass(frozen=True, slots=True)
class LiveTaskLocation:
    """The task is live; the live read layer owns its detail."""

    task_id: UUID


@dataclass(frozen=True, slots=True)
class HistoryTaskDetail:
    """One terminal task's full history detail, attempts decoded."""

    task_id: UUID
    task_name: str
    queue_name: str
    priority: int
    status: TaskStatus
    terminalization_kind: str
    terminal_at: datetime
    retention_class_key: str
    enqueued_at: datetime
    created_at: datetime
    sent_at: datetime
    claimed_at: datetime | None
    started_at: datetime | None
    good_until: datetime | None
    retry_count: int
    max_retries: int
    last_claimed_worker_id: str | None
    last_worker_hostname: str | None
    last_worker_pid: int | None
    result_envelope_version: int
    result_payload: bytes | None
    result_digest: bytes | None
    error_code: str | None
    final_failed_reason: str | None
    rerun_of_task_id: UUID | None
    rerun_root_task_id: UUID | None
    workflow_id: str | None
    is_workflow_task: bool
    rerun_input_disposition: str
    rerun_input_version: int | None
    rerun_input_digest: bytes | None
    rerun_input_inline: bytes | None
    attempts: tuple[AttemptRecord, ...]


@dataclass(frozen=True, slots=True)
class TaskDetailAbsent:
    """No row anywhere. `reason` says whether retention can explain the
    absence (``horsies_task_absence_reason``)."""

    task_id: UUID
    reason: TaskAbsenceReason


type TaskDetailResult = LiveTaskLocation | HistoryTaskDetail | TaskDetailAbsent


async def read_task_detail(
    connection: AsyncConnection,
    *,
    task_id: UUID,
) -> TaskDetailResult:
    """Resolve one task through the staged detail function.

    An absent id costs a second statement, the absence reason.

    Raises:
        HistoryContractError: the staged function reports an unknown
            location, the history row does not decode (its status is not a
            ``TaskStatus`` member, or its attempt snapshot does not decode),
            or the absence reason is not a ``TaskAbsenceReason`` member.
    """
    row = (
        await connection.execute(
            text(
                f'SELECT location, (detail.task_row).* '
                f'FROM {TASK_DETAIL_FUNCTION}(CAST(:task_id AS uuid)) '
                'AS detail'
            ),
            {'task_id': str(task_id)},
        )
    ).one_or_none()
    if row is None:
        return TaskDetailAbsent(
            task_id=task_id,
            reason=await _classify_absence(
                connection, task_id
            ),
        )
    if row.location == 'LIVE':
        return LiveTaskLocation(task_id=task_id)
    if row.location != 'HISTORY':
        raise HistoryContractError(
            f'staged detail returned unknown location {row.location!r}'
        )
    return _decode_history_detail(row)


def history_status(task_id: UUID, value: str) -> TaskStatus:
    """The stored status of a history row as a ``TaskStatus`` member.

    Raises:
        HistoryContractError: the stored text is not a member.
    """
    match _STATUS_BY_VALUE.get(value):
        case TaskStatus() as status:
            return status
        case None:
            raise HistoryContractError(
                f'history row {task_id} has status {value!r}, which is not '
                'a task status'
            )


def _decode_history_detail(row: Any) -> HistoryTaskDetail:
    task_id = uuid_column_value(row.task_id, column='task_id')
    status = history_status(task_id, row.status)
    decoded = decode_attempt_snapshot(
        version=row.attempt_archive_version,
        payload=bytes(row.attempt_snapshot),
        digest=bytes(row.attempt_snapshot_digest),
    )
    match decoded:
        case DecodedArchiveValue():
            pass
        case _:
            raise HistoryContractError(
                f'attempt snapshot failed to decode: {decoded!r}'
            )
    return HistoryTaskDetail(
        task_id=task_id,
        task_name=row.task_name,
        queue_name=row.queue_name,
        priority=row.priority,
        status=status,
        terminalization_kind=row.terminalization_kind,
        terminal_at=row.terminal_at,
        retention_class_key=row.retention_class_key,
        enqueued_at=row.enqueued_at,
        created_at=row.created_at,
        sent_at=row.sent_at,
        claimed_at=row.claimed_at,
        started_at=row.started_at,
        good_until=row.good_until,
        retry_count=row.retry_count,
        max_retries=row.max_retries,
        last_claimed_worker_id=row.last_claimed_worker_id,
        last_worker_hostname=row.last_worker_hostname,
        last_worker_pid=row.last_worker_pid,
        result_envelope_version=row.result_envelope_version,
        result_payload=(
            bytes(row.result_payload)
            if row.result_payload is not None
            else None
        ),
        result_digest=(
            bytes(row.result_digest)
            if row.result_digest is not None
            else None
        ),
        error_code=row.error_code,
        final_failed_reason=row.final_failed_reason,
        rerun_of_task_id=(
            uuid_column_value(row.rerun_of_task_id, column='rerun_of_task_id')
            if row.rerun_of_task_id is not None
            else None
        ),
        rerun_root_task_id=(
            uuid_column_value(
                row.rerun_root_task_id, column='rerun_root_task_id',
            )
            if row.rerun_root_task_id is not None
            else None
        ),
        workflow_id=(
            str(row.workflow_id) if row.workflow_id is not None else None
        ),
        is_workflow_task=row.is_workflow_task,
        rerun_input_disposition=row.rerun_input_disposition,
        rerun_input_version=row.rerun_input_version,
        rerun_input_digest=(
            bytes(row.rerun_input_digest)
            if row.rerun_input_digest is not None
            else None
        ),
        rerun_input_inline=(
            bytes(row.rerun_input_inline)
            if row.rerun_input_inline is not None
            else None
        ),
        attempts=decoded.value,
    )


def absence_reason_of(task_id: UUID, value: object) -> TaskAbsenceReason:
    """The value of ``horsies_task_absence_reason`` as a ``TaskAbsenceReason``.

    Raises:
        HistoryContractError: the value is not a member.
    """
    match _REASON_BY_VALUE.get(value) if isinstance(value, str) else None:
        case TaskAbsenceReason() as reason:
            return reason
        case None:
            raise HistoryContractError(
                f'absence reason of {task_id} is {value!r}, which is not a '
                'task absence reason'
            )


async def _classify_absence(
    connection: AsyncConnection,
    task_id: UUID,
) -> TaskAbsenceReason:
    """The absence reason of a task id with no row anywhere: one statement,
    ``horsies_task_absence_reason``. Writes nothing.

    Raises:
        HistoryContractError: the function returns a value that is not a
            ``TaskAbsenceReason`` member.
    """
    value = (
        await connection.execute(_ABSENCE_REASON_SQL, {'task_id': task_id})
    ).scalar_one()
    return absence_reason_of(task_id, value)
