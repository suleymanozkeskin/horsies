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
    decode_attempt_snapshot,
    encode_attempt_snapshot,
)
from horsies.core.history.archive.versions import DecodedArchiveValue
from horsies.core.history.commands import EnsureLeafCoverage
from horsies.core.history.names import WORKFLOW_PHASE2_PENDING
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
    await connection.execute(ITEST_TASK_ROWS_VIEW_DDL)


ITEST_TASK_ROWS_VIEW_DDL = text(
    """
    CREATE OR REPLACE VIEW itest_task_rows AS
    SELECT id, task_name, queue_name, priority, status, is_workflow_task,
           error_code, failed_reason, failed_at, completed_at, terminal_at,
           sent_at, started_at, enqueued_at, good_until, next_retry_at,
           result, claimed, claimed_at, claimed_by_worker_id,
           claimed_by_worker_id AS last_claimed_worker_id,
           claim_expires_at, finalizing_at, finalizing_by_worker_id,
           retry_count, max_retries, worker_pid, worker_hostname,
           worker_process_name, retention_class_key, terminalization_kind,
           created_at
    FROM horsies_tasks
    UNION ALL
    SELECT task_id AS id, task_name, queue_name, priority, status,
           is_workflow_task,
           error_code,
           final_failed_reason AS failed_reason,
           CASE WHEN status <> 'COMPLETED' THEN terminal_at END AS failed_at,
           CASE WHEN status = 'COMPLETED' THEN terminal_at END AS completed_at,
           terminal_at,
           sent_at, started_at, enqueued_at, good_until,
           NULL::timestamptz AS next_retry_at,
           convert_from(result_payload, 'UTF8') AS result,
           FALSE AS claimed, claimed_at,
           NULL AS claimed_by_worker_id,
           last_claimed_worker_id,
           NULL::timestamptz AS claim_expires_at,
           NULL::timestamptz AS finalizing_at,
           NULL AS finalizing_by_worker_id,
           retry_count, max_retries,
           last_worker_pid AS worker_pid,
           last_worker_hostname AS worker_hostname,
           last_worker_process_name AS worker_process_name,
           retention_class_key, terminalization_kind,
           created_at
    FROM horsies_task_history
    """
)
"""Test-only readback surface: one task per row regardless of which
lifecycle side holds it, presented with the live column names. The
terminal instant lands in completed_at/failed_at by status. The two
claimant columns say different things and are NOT interchangeable:
`claimed_by_worker_id` is the LEASE — who holds the task now — and is
structurally absent on a history row, while `last_claimed_worker_id` is
PROVENANCE — who ran it — and survives the move. A reader asking "who
ran this" must ask for the second; the first answering NULL after
terminalization is correct, not a gap."""


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


async def read_attempts(
    session: AsyncSession, task_id: str
) -> tuple[AttemptRecord, ...]:
    """Every recorded attempt, wherever it lives, attempt ascending.

    Live attempt rows while the task is live; after terminalization the
    move purges them and the history row's snapshot is their only home.
    One reader, because the two homes are one question and a caller
    asking it twice can only get them out of step.
    """
    live = (
        await session.execute(
            text(
                """
                SELECT attempt, outcome, will_retry, started_at,
                       finished_at, error_code, error_message,
                       failed_reason, worker_id, worker_hostname,
                       worker_pid, worker_process_name
                FROM horsies_task_attempts
                WHERE task_id = CAST(:id AS uuid)
                ORDER BY attempt
                """
            ),
            {'id': task_id},
        )
    ).all()
    if live:
        return tuple(
            AttemptRecord(
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
            for row in live
        )
    snapshot = (
        await session.execute(
            text(
                """
                SELECT attempt_archive_version, attempt_snapshot_codec,
                       attempt_snapshot_content_type, attempt_snapshot,
                       attempt_snapshot_digest
                FROM horsies_task_history
                WHERE task_id = CAST(:id AS uuid)
                """
            ),
            {'id': task_id},
        )
    ).first()
    if snapshot is None:
        return ()
    decoded = decode_attempt_snapshot(
        version=snapshot.attempt_archive_version,
        codec=snapshot.attempt_snapshot_codec,
        content_type=snapshot.attempt_snapshot_content_type,
        payload=bytes(snapshot.attempt_snapshot),
        digest=bytes(snapshot.attempt_snapshot_digest),
    )
    match decoded:
        case DecodedArchiveValue(value=records):
            return records
        case _:
            raise AssertionError(f'corrupt attempt snapshot: {decoded!r}')


async def read_attempt_history(
    session: AsyncSession, task_id: str
) -> list[tuple[int, str, str | None]]:
    """(attempt, outcome, failed_reason) tuples, wherever they live."""
    return [
        (record.attempt, record.outcome, record.failed_reason)
        for record in await read_attempts(session, task_id)
    ]


async def read_attempt_workers(
    session: AsyncSession, task_id: str
) -> set[str]:
    """The distinct workers that attempted a task, wherever they live."""
    return {
        record.worker_id
        for record in await read_attempts(session, task_id)
        if record.worker_id is not None
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


async def force_terminal(
    session: AsyncSession,
    task_id: str,
    *,
    status: str,
    result_json: str | None = None,
    error_code: str | None = None,
    failed_reason: str | None = None,
    kind: str | None = None,
    aged_seconds: float | None = None,
) -> None:
    """Put a live task where a real terminalization would have left it.

    Post-split that is a history row (LEGACY_TERMINAL when the forcing
    test names no operation), no live row, and no live attempt rows —
    the simulate-a-worker-finished shape the suites previously wrote as
    an in-place UPDATE. The stored result rides as the record's payload.
    """
    snapshot = encode_attempt_snapshot(())
    payload = result_json.encode() if result_json is not None else None
    await session.execute(
        text("""
            INSERT INTO horsies_task_history (
                task_id, task_name, queue_name, priority,
                command_fingerprint_version, command_fingerprint,
                status, terminalization_kind, terminal_at,
                retention_anchor_at, retention_class_key,
                sent_at, enqueued_at, claimed_at, started_at, created_at,
                retry_count, max_retries,
                last_claimed_worker_id, last_worker_hostname,
                result_envelope_version, result_codec, result_content_type,
                result_payload, result_digest,
                error_code, final_failed_reason,
                is_workflow_task, history_schema_version,
                attempt_archive_version, attempt_snapshot_codec,
                attempt_snapshot_content_type, attempt_snapshot,
                attempt_snapshot_digest, rerun_input_disposition
            )
            SELECT id, task_name, queue_name, priority,
                   COALESCE(command_fingerprint_version, 1),
                   COALESCE(
                       command_fingerprint,
                       sha256(convert_to(CAST(id AS text), 'UTF8'))
                   ),
                   :status, :kind,
                   NOW() - make_interval(
                       secs => CAST(COALESCE(:aged_seconds, 0)
                                    AS double precision)),
                   NOW() - make_interval(
                       secs => CAST(COALESCE(:aged_seconds, 0)
                                    AS double precision)),
                   COALESCE(retention_class_key, :default_class),
                   sent_at, enqueued_at, claimed_at, started_at, created_at,
                   retry_count, max_retries,
                   claimed_by_worker_id, worker_hostname,
                   1, 'json-utf8', 'application/json',
                   :payload,
                   CASE WHEN CAST(:payload AS bytea) IS NOT NULL
                        THEN sha256(CAST(:payload AS bytea)) END,
                   :error_code, :failed_reason,
                   is_workflow_task, 1,
                   :snapshot_version, :snapshot_codec,
                   :snapshot_content_type, :snapshot_payload,
                   :snapshot_digest, 'NEVER_ELIGIBLE'
            FROM horsies_tasks WHERE id = CAST(:id AS uuid)
        """),
        {
            'status': status,
            'kind': kind if kind is not None else 'LEGACY_TERMINAL',
            'id': task_id,
            'payload': payload,
            'error_code': error_code,
            'failed_reason': failed_reason,
            'aged_seconds': aged_seconds,
            'default_class': HISTORY_SEED_CLASS_KEY,
            'snapshot_version': snapshot.version,
            'snapshot_codec': snapshot.codec,
            'snapshot_content_type': snapshot.content_type,
            'snapshot_payload': snapshot.payload,
            'snapshot_digest': snapshot.digest,
        },
    )
    # A workflow-backing task's move also records the pending progression
    # in the outbox, which is where phase-2 recovery finds it. Simulating
    # the move without the outbox row would simulate a terminalization
    # that cannot be recovered from, which is not a state the wire
    # produces. The node whose progression is owed is the one still
    # holding this task and not yet terminal.
    await session.execute(
        text(f"""
            INSERT INTO {WORKFLOW_PHASE2_PENDING} (
                task_id, workflow_id, workflow_node_row_id,
                terminal_status, terminal_at, terminalization_kind,
                recovery_source, history_class, history_anchor,
                history_schema_version, result_digest,
                phase2_generation, created_at, attempt_count
            )
            SELECT h.task_id, wt.workflow_id, wt.id,
                   h.status, h.terminal_at, h.terminalization_kind,
                   'HISTORY', h.retention_class_key, h.retention_anchor_at,
                   h.history_schema_version,
                   sha256(h.result_payload),
                   gen_random_uuid(), h.terminal_at, 0
            FROM horsies_task_history h
            JOIN horsies_workflow_tasks wt ON wt.task_id = h.task_id
            WHERE h.task_id = CAST(:id AS uuid)
              AND wt.status NOT IN
                  ('COMPLETED', 'FAILED', 'CANCELLED', 'SKIPPED')
              -- The digest is the payload's, as the move writes it, and
              -- a deferred terminalization without a payload is refused
              -- at the source; so no payload means no evidence, rather
              -- than evidence carrying a digest of nothing.
              AND h.result_payload IS NOT NULL
            ON CONFLICT (task_id) DO NOTHING
        """),
        {'id': task_id},
    )
    await session.execute(
        text(
            'DELETE FROM horsies_task_attempts '
            'WHERE task_id = CAST(:id AS uuid)'
        ),
        {'id': task_id},
    )
    await session.execute(
        text('DELETE FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
        {'id': task_id},
    )


async def relax_cutover_columns(connection: AsyncConnection) -> None:
    """Return the cutover statements' effects to their pre-cutover shape.

    A fresh database is now born with these columns required and
    checked. The cutover battery tests the OTHER world — the one a real
    pre-cutover deployment brings, whose rows predate every one of these
    values — so it demotes explicitly. Rendered from the same structured
    authority the tightening renders from, so a column added there is
    relaxed here without anyone remembering to.
    """
    from horsies.core.history.terminalization.live_cutover import (
        CUTOVER_COLUMNS,
    )

    for column in CUTOVER_COLUMNS:
        if column.check is not None:
            await connection.execute(
                text(
                    'ALTER TABLE horsies_tasks DROP CONSTRAINT IF EXISTS '
                    f'horsies_tasks_{column.name}_cutover'
                )
            )
        if column.not_null:
            await connection.execute(
                text(
                    f'ALTER TABLE horsies_tasks ALTER COLUMN {column.name} '
                    'DROP NOT NULL'
                )
            )
    await connection.execute(
        text(
            'ALTER TABLE horsies_tasks DROP CONSTRAINT IF EXISTS '
            'horsies_tasks_rerun_lineage_pair'
        )
    )
    # The locator contract goes with them. It is a cutover statement
    # too, and it cannot survive the identity demotion: the node key it
    # references converts back to varchar while the outbox's columns
    # stay uuid, so a surviving constraint is one the tighten can no
    # longer re-implement.
    await connection.execute(
        text(
            'ALTER TABLE horsies_workflow_phase2_pending '
            'DROP CONSTRAINT IF EXISTS '
            'horsies_workflow_phase2_pending_node_fkey'
        )
    )
    await connection.execute(
        text(
            'ALTER TABLE horsies_workflow_tasks DROP CONSTRAINT IF EXISTS '
            'horsies_workflow_tasks_node_workflow_key'
        )
    )
