"""Shared history-row seeding for post-cutover integration suites.

The live table's status domain admits only live rows, so a fixture with
a terminal status is seeded as a `horsies_task_history` row — exactly
where production writes it. A fresh install performs no class
registration, leaf creation, or staged-reader publication at startup,
so `ensure_history_seedable` runs that sequence through the baseline
functions of the core's coverage pass; every step is idempotent
(registration answers VERIFIED, leaf creation ALREADY_CONFORMANT, and
republication rewrites the same manifest).

`force_terminal` moves a seeded row to history with a terminal status;
`read_attempts`, `read_attempt_history`, `read_attempt_workers` and
`stored_history_status` read what history holds.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from horsies.core.history.archive.attempts import (
    AttemptRecord,
    decode_attempt_snapshot,
)
from horsies.core.history.archive.versions import DecodedArchiveValue
from horsies.core.history.ddl.classes import (
    DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DAYS,
)
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from tests.attempt_snapshot_support import encode_attempt_snapshot

WORKFLOW_PHASE2_PENDING: Final = 'horsies_workflow_phase2_pending'
"""The phase-2 record table of the baseline."""

HISTORY_SEED_CLASS_KEY = DEFAULT_RETENTION_CLASS_KEY
"""Seeding uses the ratified default class, so fixture rows, stamped
live rows, and production-enqueued rows all share one registered class."""


def seed_terminalization_kind(status: str) -> str:
    """The kind a seeded history row carries when its test names none.

    Each kind is one a real operation writes with that status. FAILED,
    EXPIRED and CANCELLED take a kind of the reaper's families, so a worker
    that finalizes the row afterwards is told another operation won.
    COMPLETED takes the locked completion, the only family that writes it
    besides the fused one. Each FAILED, EXPIRED and COMPLETED kind is one
    the phase-2 outbox accepts.
    """
    match status:
        case 'COMPLETED':
            return 'COMPLETE_LOCKED'
        case 'FAILED':
            return 'FAIL_STALE'
        case 'EXPIRED':
            return 'EXPIRE_CLAIMED'
        case 'CANCELLED':
            return 'CANCEL_ORPHAN_SWEEP'
        case _:
            raise AssertionError(f'{status!r} is not a terminal status')


SEED_HISTORY_HORIZON_DAYS: Final = 2
"""Daily history leaves from today through this many days ahead."""

SEED_HEARTBEAT_HORIZON_HOURS: Final = 2
"""Hourly heartbeat leaves from this hour through this many hours ahead."""

SEED_HEARTBEAT_CLASS_HORIZON_HOURS: Final = 3
"""The horizon that the heartbeat class row stores."""

_REGISTER_CLASSES_SQL = text(
    'SELECT class_key, outcome FROM horsies_register_retention_classes('
    'CAST(:days AS smallint[]), CAST(:hours AS smallint))'
)
_CREATE_HISTORY_LEAF_SQL = text(
    'SELECT outcome, leaf_name, conflict_kind FROM horsies_create_history_leaf('
    'CAST(:class_key AS text), CAST(:lower_anchor AS timestamptz))'
)
_CREATE_HEARTBEAT_LEAF_SQL = text(
    'SELECT outcome, leaf_name, conflict_kind FROM horsies_create_heartbeat_leaf('
    'CAST(:lower_anchor AS timestamptz))'
)
_REGISTERED_OUTCOMES: Final = frozenset({'REGISTERED', 'VERIFIED', 'HORIZON_UPDATED'})
_COVERED_OUTCOMES: Final = frozenset({'CREATED', 'ALREADY_CONFORMANT', 'INDEX_REPAIRED'})


async def ensure_history_seedable(connection: AsyncConnection) -> None:
    """Register the default class, cover leaves + heartbeats, publish.

    The steps of the core's coverage pass, through its baseline functions,
    in the caller's transaction: the class rows
    (``horsies_register_retention_classes``), the daily leaves of the
    default class (``horsies_create_history_leaf``), the hourly heartbeat
    leaves (``horsies_create_heartbeat_leaf``), then the readers
    (``horsies_publish_history_readers``). Any other outcome fails the
    caller with an ``AssertionError``.
    """
    registered = (
        await connection.execute(
            _REGISTER_CLASSES_SQL,
            {'days': [DEFAULT_RETENTION_DAYS], 'hours': SEED_HEARTBEAT_CLASS_HORIZON_HOURS},
        )
    ).all()
    for row in registered:
        if row.outcome not in _REGISTERED_OUTCOMES:
            raise AssertionError(f'retention class {row.class_key} unusable: {row.outcome}')
    database_now = (await connection.execute(text('SELECT now()'))).scalar_one()
    assert isinstance(database_now, datetime), database_now
    now = database_now.astimezone(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    for day_offset in range(SEED_HISTORY_HORIZON_DAYS + 1):
        leaf = (
            await connection.execute(
                _CREATE_HISTORY_LEAF_SQL,
                {
                    'class_key': DEFAULT_RETENTION_CLASS_KEY,
                    'lower_anchor': today + timedelta(days=day_offset),
                },
            )
        ).one()
        if leaf.outcome not in _COVERED_OUTCOMES:
            raise AssertionError(
                f'leaf coverage refused: {leaf.outcome} {leaf.leaf_name} {leaf.conflict_kind}'
            )
    this_hour = now.replace(minute=0, second=0, microsecond=0)
    for hour_offset in range(SEED_HEARTBEAT_HORIZON_HOURS + 1):
        leaf = (
            await connection.execute(
                _CREATE_HEARTBEAT_LEAF_SQL,
                {'lower_anchor': this_hour + timedelta(hours=hour_offset)},
            )
        ).one()
        if leaf.outcome not in _COVERED_OUTCOMES:
            raise AssertionError(
                f'heartbeat coverage refused: {leaf.outcome} {leaf.leaf_name} '
                f'{leaf.conflict_kind}'
            )
    _ = await StagedLoaderPublisher().republish(connection)
    await connection.execute(ITEST_TASK_ROWS_VIEW_DDL)


ITEST_TASK_ROWS_VIEW_DDL = text(
    """
    CREATE OR REPLACE VIEW itest_task_rows AS
    SELECT id, task_name, queue_name, priority, status, is_workflow_task,
           NULL::text AS error_code, NULL::text AS failed_reason,
           NULL::timestamptz AS failed_at, NULL::timestamptz AS completed_at,
           NULL::timestamptz AS terminal_at,
           sent_at, started_at, enqueued_at, good_until, next_retry_at,
           NULL::text AS result, FALSE AS claimed, claimed_at,
           claimed_by_worker_id,
           claimed_by_worker_id AS last_claimed_worker_id,
           claim_expires_at, finalizing_at, finalizing_by_worker_id,
           retry_count, max_retries, worker_pid, worker_hostname,
           worker_process_name, retention_class_key,
           NULL::text AS terminalization_kind,
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
lifecycle side holds it, presented with the live column names. A live row
has no terminal columns: its error, failed reason, terminal times, result
and kind read NULL, and ``claimed`` reads FALSE. The
terminal instant lands in completed_at/failed_at by status. The two
claimant columns say different things and are NOT interchangeable:
`claimed_by_worker_id` is the LEASE — who holds the task now — and is
structurally absent on a history row, while `last_claimed_worker_id` is
PROVENANCE — who ran it — and survives the move. A reader asking "who
ran this" must ask for the second; the first answering NULL after
terminalization is correct, not a gap."""


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
                SELECT attempt_archive_version, attempt_snapshot,
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


@asynccontextmanager
async def stored_history_status(
    session: AsyncSession, task_id: str, value: str,
) -> AsyncGenerator[None, None]:
    """Store ``value`` as the history status of ``task_id`` for the block.

    Schema v49 drops the status CHECK of the history parent, so an
    out-of-band write can store a value that is not a ``TaskStatus``
    member. The block writes the previous status back, so later tests read
    a valid row.
    """
    previous = (
        await session.execute(
            text(
                'SELECT status FROM horsies_task_history '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    status_sql = text(
        'UPDATE horsies_task_history SET status = :status '
        'WHERE task_id = CAST(:task_id AS uuid)'
    )
    _ = await session.execute(status_sql, {'status': value, 'task_id': task_id})
    await session.commit()
    try:
        yield
    finally:
        _ = await session.execute(
            status_sql, {'status': previous, 'task_id': task_id},
        )
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

    Post-split that is a history row (``seed_terminalization_kind`` of the
    status when the forcing test names no operation), no live row, and no
    live attempt rows —
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
                retention_class_key,
                sent_at, enqueued_at, claimed_at, started_at, created_at,
                retry_count, max_retries,
                last_claimed_worker_id, last_worker_hostname,
                result_envelope_version,
                result_payload, result_digest,
                error_code, final_failed_reason,
                is_workflow_task, history_schema_version,
                attempt_archive_version, attempt_snapshot,
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
                   COALESCE(retention_class_key, :default_class),
                   sent_at, enqueued_at, claimed_at, started_at, created_at,
                   retry_count, max_retries,
                   claimed_by_worker_id, worker_hostname,
                   1,
                   :payload,
                   CASE WHEN CAST(:payload AS bytea) IS NOT NULL
                        THEN sha256(CAST(:payload AS bytea)) END,
                   :error_code, :failed_reason,
                   is_workflow_task, 1,
                   :snapshot_version, :snapshot_payload,
                   :snapshot_digest, 'NEVER_ELIGIBLE'
            FROM horsies_tasks WHERE id = CAST(:id AS uuid)
        """),
        {
            'status': status,
            'kind': (
                kind if kind is not None else seed_terminalization_kind(status)
            ),
            'id': task_id,
            'payload': payload,
            'error_code': error_code,
            'failed_reason': failed_reason,
            'aged_seconds': aged_seconds,
            'default_class': HISTORY_SEED_CLASS_KEY,
            'snapshot_version': snapshot.version,
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
                terminalization_kind,
                recovery_source, history_class, history_anchor,
                history_schema_version, result_digest,
                quarantine_task_id, created_at, attempt_count,
                last_attempt_at, last_failure_class
            )
            SELECT h.task_id, wt.workflow_id, wt.id,
                   h.terminalization_kind,
                   'HISTORY', h.retention_class_key, h.terminal_at,
                   h.history_schema_version,
                   sha256(h.result_payload),
                   NULL, h.terminal_at, 0,
                   NULL, NULL
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
