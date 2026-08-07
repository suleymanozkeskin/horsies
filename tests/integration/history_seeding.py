"""Shared history-row seeding for post-cutover integration suites.

The live table's status domain admits only live rows, so a fixture with
a terminal status is seeded as a `horsies_task_history` row — exactly
where production writes it. A fresh install performs no class
registration, leaf creation, or staged-reader publication at startup,
so `ensure_history_seedable` owns that publication sequence; every step
is idempotent (registration reports the class as already registered,
coverage reports leaves as already conformant, republication rewrites
the same manifest).

`route_rows` is the choke point suites call from their own persist
helpers: terminal-status `TaskModel` fixtures become history rows, any
same-call `TaskAttemptModel` rows for such a task fold into its attempt
snapshot, and a same-call `WorkflowTaskModel` naming the task supplies
its workflow linkage. Everything else persists through the ORM
unchanged.
"""

from __future__ import annotations

from datetime import timedelta
from hashlib import sha256
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from horsies.core.history.archive.attempts import (
    AttemptRecord,
    encode_attempt_snapshot,
)
from horsies.core.history.commands import EnsureLeafCoverage
from horsies.core.history.ddl.classes import (
    ClassAlreadyRegistered,
    ClassRegistered,
    DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DURATION,
    register_finite_retention_class,
)
from horsies.core.history.heartbeats.partitioning import (
    EnsureHeartbeatCoverage,
    ensure_heartbeat_coverage,
    register_heartbeat_class,
)
from horsies.core.history.outcomes import (
    LeafAlreadyConformant,
    LeafCreated,
    LeafIndexRepaired,
)
from horsies.core.history.partitions.manager import ensure_leaf_coverage
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from horsies.core.models.task_pg import TaskAttemptModel, TaskModel
from horsies.core.models.workflow_pg import WorkflowTaskModel
from horsies.core.types.status import TaskStatus

HISTORY_SEED_CLASS_KEY = DEFAULT_RETENTION_CLASS_KEY
"""Seeding uses the ratified default class, so fixture rows, stamped
live rows, and production-enqueued rows all share one registered class."""


async def ensure_history_seedable(connection: AsyncConnection) -> None:
    """Register the default class, cover leaves + heartbeats, publish.

    The test-side stand-in for the production maintenance owner (its
    groundwork proposal is pending): the same idempotent sequence a
    fleet needs before any terminalization or heartbeat write can land.
    """
    registration = await register_finite_retention_class(
        connection,
        class_key=DEFAULT_RETENTION_CLASS_KEY,
        duration=DEFAULT_RETENTION_DURATION,
    )
    match registration:
        case ClassRegistered() | ClassAlreadyRegistered():
            pass
        case _:
            raise AssertionError(f'retention class unusable: {registration!r}')
    publisher = StagedLoaderPublisher()
    creations = await ensure_leaf_coverage(
        connection,
        EnsureLeafCoverage(
            class_key=DEFAULT_RETENTION_CLASS_KEY, horizon_days=2
        ),
        publisher,
    )
    for creation in creations:
        match creation:
            case LeafCreated() | LeafAlreadyConformant() | LeafIndexRepaired():
                continue
            case _:
                raise AssertionError(f'leaf coverage refused: {creation!r}')
    await register_heartbeat_class(connection, horizon=timedelta(hours=3))
    heartbeat_creations = await ensure_heartbeat_coverage(
        connection,
        EnsureHeartbeatCoverage(horizon_hours=2),
    )
    for creation in heartbeat_creations:
        match creation:
            case LeafCreated() | LeafAlreadyConformant() | LeafIndexRepaired():
                continue
            case _:
                raise AssertionError(
                    f'heartbeat coverage refused: {creation!r}'
                )
    await publisher.republish(connection)


def _attempt_record(row: TaskAttemptModel) -> AttemptRecord:
    return AttemptRecord(
        attempt=row.attempt,
        outcome=row.outcome,
        will_retry=row.will_retry,
        started_at=row.started_at,
        finished_at=row.finished_at,
        error_code=row.error_code,
        error_message=row.error_message,
        failed_reason=row.failed_reason,
        worker_id=row.worker_id,
        worker_hostname=row.worker_hostname,
        worker_pid=row.worker_pid,
        worker_process_name=row.worker_process_name,
    )


INSERT_HISTORY_ROW_SQL = text(
    """
    INSERT INTO horsies_task_history (
        task_id, task_name, queue_name, priority,
        command_fingerprint_version, command_fingerprint,
        status, terminalization_kind, terminal_at, retention_anchor_at,
        retention_class_key, sent_at, enqueued_at, claimed_at, started_at,
        created_at, good_until, retry_count, max_retries,
        last_claimed_worker_id, last_worker_hostname,
        result_envelope_version, result_codec, result_content_type,
        result_payload, result_digest, error_code, final_failed_reason,
        workflow_id, is_workflow_task, history_schema_version,
        attempt_archive_version, attempt_snapshot_codec,
        attempt_snapshot_content_type, attempt_snapshot,
        attempt_snapshot_digest, rerun_input_disposition
    ) VALUES (
        CAST(:task_id AS uuid), :task_name, :queue_name, :priority,
        :fingerprint_version, :fingerprint,
        :status, :terminalization_kind, :terminal_at, :terminal_at,
        :retention_class_key, :sent_at, :enqueued_at, :claimed_at,
        :started_at, :created_at, :good_until, :retry_count, :max_retries,
        :last_claimed_worker_id, :last_worker_hostname,
        :result_envelope_version, :result_codec, :result_content_type,
        :result_payload, :result_digest, :error_code, :final_failed_reason,
        CAST(:workflow_id AS uuid), :is_workflow_task,
        :history_schema_version,
        :attempt_archive_version, :attempt_snapshot_codec,
        :attempt_snapshot_content_type, :attempt_snapshot,
        :attempt_snapshot_digest, 'NEVER_ELIGIBLE'
    )
    """
)


def history_row_params(
    task: TaskModel,
    attempts: tuple[TaskAttemptModel, ...],
    workflow_id: str | None,
) -> dict[str, object | None]:
    """Project a terminal live-shaped fixture onto the history row.

    ``LEGACY_TERMINAL`` is the one kind that admits all four terminal
    statuses without asserting which move operation produced the row —
    fixtures built from live-shaped models carry no such provenance.
    """
    terminal_at = task.terminal_at
    if terminal_at is None:
        raise AssertionError('terminal fixture rows must carry terminal_at')
    snapshot = encode_attempt_snapshot(
        tuple(
            _attempt_record(row)
            for row in sorted(attempts, key=lambda row: row.attempt)
        )
    )
    completed = task.status is TaskStatus.COMPLETED
    payload = b'{}' if completed else None
    return {
        'task_id': task.id,
        'task_name': task.task_name,
        'queue_name': task.queue_name,
        'priority': task.priority,
        'fingerprint_version': 1,
        'fingerprint': sha256(task.id.encode()).digest(),
        'status': task.status.value,
        'terminalization_kind': 'LEGACY_TERMINAL',
        'terminal_at': terminal_at,
        'retention_class_key': HISTORY_SEED_CLASS_KEY,
        'sent_at': task.sent_at,
        'enqueued_at': task.enqueued_at,
        'claimed_at': task.claimed_at,
        'started_at': task.started_at,
        'created_at': task.enqueued_at,
        'good_until': task.good_until,
        'retry_count': task.retry_count,
        'max_retries': task.max_retries,
        'last_claimed_worker_id': task.claimed_by_worker_id,
        'last_worker_hostname': task.worker_hostname,
        'result_envelope_version': 1,
        'result_codec': 'json-utf8',
        'result_content_type': 'application/json',
        'result_payload': payload,
        'result_digest': sha256(payload).digest() if payload else None,
        'error_code': task.error_code,
        'final_failed_reason': task.failed_reason,
        'workflow_id': workflow_id,
        'is_workflow_task': task.is_workflow_task,
        'history_schema_version': 1,
        'attempt_archive_version': snapshot.version,
        'attempt_snapshot_codec': snapshot.codec,
        'attempt_snapshot_content_type': snapshot.content_type,
        'attempt_snapshot': snapshot.payload,
        'attempt_snapshot_digest': snapshot.digest,
    }


async def route_rows(session: AsyncSession, rows: tuple[Any, ...]) -> None:
    """Persist fixture rows on their lifecycle side and commit."""
    task_rows = [row for row in rows if isinstance(row, TaskModel)]
    history_tasks = {
        row.id: row for row in task_rows if row.status.is_terminal
    }
    attempts_for_history: dict[str, list[TaskAttemptModel]] = {
        task_id: [] for task_id in history_tasks
    }
    workflow_for_task: dict[str, str] = {
        row.task_id: row.workflow_id
        for row in rows
        if isinstance(row, WorkflowTaskModel) and row.task_id is not None
    }
    live_rows: list[Any] = []
    for row in rows:
        if isinstance(row, TaskModel) and row.id in history_tasks:
            continue
        if (
            isinstance(row, TaskAttemptModel)
            and row.task_id in attempts_for_history
        ):
            attempts_for_history[row.task_id].append(row)
            continue
        live_rows.append(row)
    if task_rows:
        connection = await session.connection()
        await ensure_history_seedable(connection)
        # A live row must be terminalizable: the move routes its history
        # row by the task's retention class and copies its command
        # fingerprint, both stamped by the production enqueue path that
        # these fixtures bypass. Unstamped fixtures get the seed values.
        for task in task_rows:
            if task.retention_class_key is None:
                task.retention_class_key = HISTORY_SEED_CLASS_KEY
            if task.command_fingerprint_version is None:
                task.command_fingerprint_version = 1
                task.command_fingerprint = sha256(task.id.encode()).digest()
            if task.prepared_rerun_input_disposition is None:
                task.prepared_rerun_input_disposition = 'DECLINED_BY_POLICY'
            if task.retain_rerun_input is None:
                task.retain_rerun_input = False
        for task_id, task in history_tasks.items():
            await connection.execute(
                INSERT_HISTORY_ROW_SQL,
                history_row_params(
                    task,
                    tuple(attempts_for_history[task_id]),
                    workflow_for_task.get(task_id),
                ),
            )
    session.add_all(live_rows)
    await session.commit()
