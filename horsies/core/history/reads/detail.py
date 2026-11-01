"""Task-detail read primitive over the staged detail function.

One client statement resolves a task to LIVE, a full decoded history
detail, or a typed absence. The composite `task_row` expands in SQL —
`(task_row).*` — so every frozen column arrives as a real column, not a
composite the driver would hand back as text. Attempts on a history hit
decode from the row's snapshot columns: post-cutover, a terminal task's
attempt rows are deleted by the move, so the snapshot is the only home
of attempt history and it purges with the row.

The stored status is parsed here, one time, into `TaskStatus`. Schema
v49 drops the status CHECK of the history parent, so a value that is
not a member can be stored out of band. The read raises
`HistoryContractError` for it, as it does for an attempt snapshot that
does not decode. A reader of `HistoryTaskDetail.status` gets a member.

Absence classifies through the same birth-floor mechanism the generated
functions use: a v7 birth before the retained-history floor means the
task, if it ever existed, lies before all retained history — the typed
expired presentation. Never-seen and purged are indistinguishable past
that point by construction, and the classification says exactly what is
known instead of guessing.

The floor comes from the attached leaf catalog, not from the published
probe manifest. The two differ by exactly the leaves whose relation was
dropped out of band, and those leaves DID retain history — someone
removed it. Reading the floor from the probe list would raise it over
them and report their tasks as predating retained history, which
presents an accidental drop as ordinary ageing. The catalog floor keeps
the claim true; unreadable is not the same as never kept.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ...types.status import TaskStatus
from ..archive.attempts import AttemptRecord, decode_attempt_snapshot
from ..archive.versions import DecodedArchiveValue
from ..errors import HistoryContractError
from ..identity.uuid7 import uuid7_birth_at
from ..names import HEARTBEAT_CLASS_KEY, TASK_DETAIL_FUNCTION
from ..partitions.catalog import read_attached_birth_floor

_STATUS_BY_VALUE: dict[str, TaskStatus] = {
    status.value: status for status in TaskStatus
}


@dataclass(frozen=True, slots=True)
class LiveTaskLocation:
    """The task is live; the live read layer owns its detail."""

    task_id: str


@dataclass(frozen=True, slots=True)
class HistoryTaskDetail:
    """One terminal task's full history detail, attempts decoded."""

    task_id: str
    task_name: str
    queue_name: str
    priority: int
    status: TaskStatus
    terminalization_kind: str
    terminal_at: datetime
    retention_class_key: str
    enqueued_at: datetime
    created_at: datetime
    sent_at: datetime | None
    claimed_at: datetime | None
    started_at: datetime | None
    good_until: datetime | None
    retry_count: int
    max_retries: int
    last_claimed_worker_id: str | None
    last_worker_hostname: str | None
    last_worker_pid: int | None
    result_envelope_version: int
    result_codec: str
    result_content_type: str
    result_payload: bytes | None
    prior_result_payload: bytes | None
    result_digest: bytes | None
    error_code: str | None
    final_failed_reason: str | None
    rerun_of_task_id: str | None
    rerun_root_task_id: str | None
    workflow_id: str | None
    is_workflow_task: bool
    rerun_input_disposition: str
    rerun_input_version: int | None
    rerun_input_codec: str | None
    rerun_input_content_type: str | None
    rerun_input_digest: bytes | None
    rerun_input_inline: bytes | None
    rerun_input_reference: str | None
    attempts: tuple[AttemptRecord, ...]


class TaskAbsenceReason(str, enum.Enum):
    """What a read knows about a task id that no table holds.

    The live table and the task history have no row for the id. The reason
    says whether retention can explain that.
    """

    # The id's UUIDv7 birth is before the oldest retained history, so
    # retention can have removed the task.
    PREDATES_RETAINED_HISTORY = 'PREDATES_RETAINED_HISTORY'
    # Retained history covers the id's birth and has no row: the task did
    # not exist, or its history partition was removed out of band.
    WITHIN_RETAINED_HISTORY = 'WITHIN_RETAINED_HISTORY'
    # The id is not a UUIDv7, no attached history partition has a birth, or
    # the database has no staged history read.
    NOT_CLASSIFIED = 'NOT_CLASSIFIED'


@dataclass(frozen=True, slots=True)
class TaskDetailAbsent:
    """No row anywhere. `reason` is PREDATES_RETAINED_HISTORY when the v7
    birth lies before the attached-catalog floor (the typed expired
    presentation), WITHIN_RETAINED_HISTORY when it lies within retained
    range, and NOT_CLASSIFIED when the identifier is not v7 or no
    attached leaf carries a birth.

    WITHIN_RETAINED_HISTORY is the answer for a task whose leaf was destroyed out of band:
    its history was retained and then removed, so it does not predate
    retained history. The row is gone either way, but the reason
    published here stays true — an operator asking why a rerun is
    impossible learns the partition is missing rather than that the task
    aged out."""

    task_id: str
    reason: TaskAbsenceReason


type TaskDetailResult = LiveTaskLocation | HistoryTaskDetail | TaskDetailAbsent


@dataclass(frozen=True, slots=True)
class HistoryTaskState:
    """The status and the workflow flag of a history row. Nothing else."""

    task_id: str
    status: TaskStatus
    is_workflow_task: bool


@dataclass(frozen=True, slots=True)
class TaskStateAbsent:
    """Neither the live table nor history holds the task."""

    task_id: str


type TaskStateResult = LiveTaskLocation | HistoryTaskState | TaskStateAbsent


async def read_task_state(
    connection: AsyncConnection,
    *,
    task_id: str,
) -> TaskStateResult:
    """Resolve one task to LIVE, its history status and workflow flag, or absent.

    It reads the same staged detail function as ``read_task_detail``, but
    it takes two columns of the history row. It does not decode the attempt
    snapshot and does not classify an absence against the retained-history
    floor. It does not write.

    Raises:
        HistoryContractError: the staged function reports an unknown
            location, or the stored status is not a ``TaskStatus`` member.
    """
    row = (
        await connection.execute(
            text(
                'SELECT location, (detail.task_row).status, '
                '(detail.task_row).is_workflow_task '
                f'FROM {TASK_DETAIL_FUNCTION}(CAST(:task_id AS uuid)) '
                'AS detail'
            ),
            {'task_id': task_id},
        )
    ).one_or_none()
    if row is None:
        return TaskStateAbsent(task_id=task_id)
    if row.location == 'LIVE':
        return LiveTaskLocation(task_id=task_id)
    if row.location != 'HISTORY':
        raise HistoryContractError(
            f'staged detail returned unknown location {row.location!r}'
        )
    return HistoryTaskState(
        task_id=task_id,
        status=_history_status(task_id, row.status),
        is_workflow_task=row.is_workflow_task,
    )


async def read_task_detail(
    connection: AsyncConnection,
    *,
    task_id: str,
) -> TaskDetailResult:
    """Resolve one task through the staged detail function.

    Raises:
        HistoryContractError: the staged function reports an unknown
            location, or the history row does not decode: its status is
            not a ``TaskStatus`` member, or its attempt snapshot does not
            decode.
    """
    row = (
        await connection.execute(
            text(
                f'SELECT location, (detail.task_row).* '
                f'FROM {TASK_DETAIL_FUNCTION}(CAST(:task_id AS uuid)) '
                'AS detail'
            ),
            {'task_id': task_id},
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


def _history_status(task_id: str, value: str) -> TaskStatus:
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
    status = _history_status(str(row.task_id), row.status)
    decoded = decode_attempt_snapshot(
        version=row.attempt_archive_version,
        codec=row.attempt_snapshot_codec,
        content_type=row.attempt_snapshot_content_type,
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
        task_id=str(row.task_id),
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
        result_codec=row.result_codec,
        result_content_type=row.result_content_type,
        result_payload=(
            bytes(row.result_payload)
            if row.result_payload is not None
            else None
        ),
        prior_result_payload=(
            bytes(row.prior_result_payload)
            if row.prior_result_payload is not None
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
            str(row.rerun_of_task_id)
            if row.rerun_of_task_id is not None
            else None
        ),
        rerun_root_task_id=(
            str(row.rerun_root_task_id)
            if row.rerun_root_task_id is not None
            else None
        ),
        workflow_id=(
            str(row.workflow_id) if row.workflow_id is not None else None
        ),
        is_workflow_task=row.is_workflow_task,
        rerun_input_disposition=row.rerun_input_disposition,
        rerun_input_version=row.rerun_input_version,
        rerun_input_codec=row.rerun_input_codec,
        rerun_input_content_type=row.rerun_input_content_type,
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
        rerun_input_reference=row.rerun_input_reference,
        attempts=decoded.value,
    )


async def _classify_absence(
    connection: AsyncConnection,
    task_id: str,
) -> TaskAbsenceReason:
    """Whether a v7 birth predates all retained history.

    The floor is read live from the attached leaf catalog rather than
    from the published probe manifest, so a leaf whose relation was
    dropped out of band still counts as retained history — see the
    module docstring for why the two sources stopped agreeing.

    A catalog carrying attached leaves before any reader is published
    now yields a real answer where the manifest-sourced floor yielded
    NOT_CLASSIFIED. That is intended: the leaves say where history begins whether
    or not a function has been generated over them, and answering from
    what is known beats declaring the question unclassifiable.
    """
    birth = uuid7_birth_at(task_id)
    if birth is None:
        return TaskAbsenceReason.NOT_CLASSIFIED
    floor = await read_attached_birth_floor(
        connection, excluded_class_key=HEARTBEAT_CLASS_KEY
    )
    if floor is None:
        return TaskAbsenceReason.NOT_CLASSIFIED
    if birth < floor:
        return TaskAbsenceReason.PREDATES_RETAINED_HISTORY
    return TaskAbsenceReason.WITHIN_RETAINED_HISTORY


async def staged_detail_published(connection: AsyncConnection) -> bool:
    """Whether the staged detail function exists on this database.

    Consumers that fall through to history must not error on a
    pre-coverage database — one that has no history to consult and no
    way to have moved a row. One owner for the probe; every fallback
    site consults it rather than carrying its own.
    """
    return bool(
        (
            await connection.execute(
                text('SELECT to_regprocedure(:name) IS NOT NULL'),
                {'name': f'{TASK_DETAIL_FUNCTION}(uuid)'},
            )
        ).scalar_one()
    )
