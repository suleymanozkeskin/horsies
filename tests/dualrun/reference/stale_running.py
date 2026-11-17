"""Stale RUNNING recovery, before ``horsies_stale_running_scan`` and ``horsies_finish_stale_attempt``.

``PostgresBroker.mark_stale_tasks_as_failed`` (``python/horsies/core/brokers/postgres.py``)
scanned the candidate ids with ``SELECT_STALE_RUNNING_TASKS_SQL`` (no row
bound; the transaction rolled back). Per candidate, in one transaction: the
task advisory lock, ``SELECT_STALE_TASK_FOR_UPDATE_SQL``, the retry decision
from the locked row, then either ``UPSERT_TASK_ATTEMPT_SQL`` and
``SCHEDULE_STALE_TASK_RETRY_SQL`` (commit, then the ``retry:<id>`` wake), or
``UPSERT_TASK_ATTEMPT_SQL`` and ``horsies_fail_stale_task`` (commit only when
applied). The baseline no longer defines ``horsies_fail_stale_task``:
``interim_terminal_writes.py`` installs its record on the reference database.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, Final, LiteralString

import psycopg
from psycopg.rows import dict_row

from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.models.tasks import OperationalErrorCode, TaskError, TaskResult
from horsies.core.types.result import Err, Ok
from tests.dualrun.capture import Connection
from tests.dualrun.reference.retry_rules import (
    calculate_retry_delay,
    check_retry_eligibility,
    parse_retry_policy,
)

SELECT_STALE_RUNNING_TASKS_SQL: Final[LiteralString] = """
    SELECT t2.id, t2.worker_pid, t2.worker_hostname, t2.claimed_by_worker_id,
           t2.started_at, hb.last_heartbeat,
           t2.retry_count, t2.worker_process_name,
           t2.max_retries, t2.task_options, t2.good_until,
           clock_timestamp() AS db_now
    FROM horsies_tasks t2
    LEFT JOIN LATERAL (
        SELECT sent_at AS last_heartbeat
        FROM horsies_heartbeats h
        WHERE h.task_id = t2.id AND h.role = 'runner'
        ORDER BY sent_at DESC
        LIMIT 1
    ) hb ON TRUE
    WHERE t2.status = 'RUNNING'
      AND t2.started_at IS NOT NULL
      AND (
          t2.finalizing_at IS NULL
          OR t2.finalizing_at < NOW() - CAST(%(finalizing_stale_threshold)s || ' seconds' AS INTERVAL)
      )
      AND COALESCE(hb.last_heartbeat, t2.started_at) < NOW() - CAST(%(stale_threshold)s || ' seconds' AS INTERVAL)
    FOR UPDATE OF t2 SKIP LOCKED
"""

LOCK_TASK_SQL: Final[LiteralString] = """
    SELECT pg_advisory_xact_lock(
        hashtextextended(CAST(CAST(%(task_id)s AS uuid) AS text), 731)
    )
"""

SELECT_STALE_TASK_FOR_UPDATE_SQL: Final[LiteralString] = """
    SELECT t.id, t.worker_pid, t.worker_hostname, t.claimed_by_worker_id,
           t.started_at, hb.last_heartbeat, t.retry_count, t.worker_process_name,
           t.max_retries, t.task_options, t.good_until, t.queue_name,
           clock_timestamp() AS db_now
    FROM horsies_tasks t
    LEFT JOIN LATERAL (
        SELECT sent_at AS last_heartbeat
        FROM horsies_heartbeats h
        WHERE h.task_id = t.id AND h.role = 'runner'
        ORDER BY sent_at DESC
        LIMIT 1
    ) hb ON TRUE
    WHERE t.id = %(id)s
      AND t.status = 'RUNNING'
      AND t.started_at IS NOT NULL
      AND (
          t.finalizing_at IS NULL
          OR t.finalizing_at < NOW() - CAST(%(finalizing_stale_threshold)s || ' seconds' AS INTERVAL)
      )
      AND COALESCE(hb.last_heartbeat, t.started_at) < NOW() - CAST(%(stale_threshold)s || ' seconds' AS INTERVAL)
    FOR UPDATE OF t
"""

UPSERT_TASK_ATTEMPT_SQL: Final[LiteralString] = """
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry,
        started_at, finished_at,
        error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name
    )
    VALUES (
        %(task_id)s, %(attempt)s, %(outcome)s, %(will_retry)s,
        %(started_at)s, %(finished_at)s,
        %(error_code)s, %(error_message)s, %(failed_reason)s,
        %(worker_id)s, %(worker_hostname)s, %(worker_pid)s, %(worker_process_name)s
    )
    ON CONFLICT (task_id, attempt) DO UPDATE SET
        outcome = EXCLUDED.outcome,
        will_retry = EXCLUDED.will_retry,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        error_code = EXCLUDED.error_code,
        error_message = EXCLUDED.error_message,
        failed_reason = EXCLUDED.failed_reason,
        worker_id = EXCLUDED.worker_id,
        worker_hostname = EXCLUDED.worker_hostname,
        worker_pid = EXCLUDED.worker_pid,
        worker_process_name = EXCLUDED.worker_process_name
"""

SCHEDULE_STALE_TASK_RETRY_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks AS t
    SET status = 'PENDING',
        retry_count = %(retry_count)s,
        next_retry_at = %(next_retry_at)s,
        enqueued_at = %(next_retry_at)s,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = now()
    WHERE t.id = %(id)s
      AND t.status = 'RUNNING'
      AND t.started_at IS NOT NULL
      AND (
          t.finalizing_at IS NULL
          OR t.finalizing_at < NOW() - CAST(%(finalizing_stale_threshold)s || ' seconds' AS INTERVAL)
      )
      AND COALESCE(
          (
              SELECT h.sent_at
              FROM horsies_heartbeats h
              WHERE h.task_id = t.id AND h.role = 'runner'
              ORDER BY h.sent_at DESC
              LIMIT 1
          ),
          t.started_at
      ) < NOW() - CAST(%(stale_threshold)s || ' seconds' AS INTERVAL)
      AND (t.good_until IS NULL OR %(next_retry_at)s < t.good_until)
    RETURNING t.id
"""

FAIL_STALE_TASK_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_fail_stale_task(
        CAST(%(task_id)s AS uuid),
        CAST(%(stale_after_ms)s AS INTEGER),
        CAST(%(finalizing_stale_after_ms)s AS INTEGER),
        %(result)s, %(error_code)s, %(failed_reason)s
    )
"""

NOTIFY_SQL: Final[LiteralString] = 'SELECT pg_notify(%(ch)s, %(p)s)'

_CRASHED: Final = OperationalErrorCode.WORKER_CRASHED


def _crash_result(row: Mapping[str, Any], stale_threshold_ms: int, reason: str, db_now: datetime) -> str:
    started_at = row['started_at']
    task_error = TaskError(
        error_code=_CRASHED,
        message=reason,
        data={
            'stale_threshold_ms': stale_threshold_ms,
            'stale_threshold_seconds': stale_threshold_ms / 1000.0,
            'worker_pid': row['worker_pid'],
            'worker_hostname': row['worker_hostname'],
            'worker_id': row['claimed_by_worker_id'],
            'started_at': started_at.isoformat() if started_at else None,
            'detected_at': db_now.isoformat(),
        },
    )
    task_result: TaskResult[None, TaskError] = TaskResult(err=task_error)
    match dumps_json(encode_task_result(task_result, type(None))):
        case Ok(text):
            return text
        case Err(error):
            raise AssertionError(f'the crash result does not serialize: {error}')


def _next_retry(row: Mapping[str, Any], db_now: datetime) -> tuple[int, datetime] | None:
    """The old retry decision: eligibility, the delay, and the deadline check."""
    retry_count = row['retry_count'] or 0
    good_until: datetime | None = row['good_until']
    if not check_retry_eligibility(
        retry_count=retry_count,
        max_retries=row['max_retries'] or 0,
        task_options_json=row['task_options'],
        error_code=_CRASHED,
        good_until=good_until,
        db_now=db_now,
    ):
        return None
    policy = parse_retry_policy(row['task_options']) or {}
    next_retry_at = db_now + timedelta(seconds=calculate_retry_delay(retry_count + 1, policy))
    if good_until is not None and next_retry_at >= good_until.astimezone(timezone.utc):
        return None
    return retry_count + 1, next_retry_at


def _attempt(row: Mapping[str, Any], will_retry: bool, reason: str, db_now: datetime) -> dict[str, object]:
    return {
        'task_id': row['id'],
        'attempt': (row['retry_count'] or 0) + 1,
        'outcome': 'FAILED',
        'will_retry': will_retry,
        'started_at': row['started_at'] or db_now,
        'finished_at': db_now,
        'error_code': _CRASHED.value,
        'error_message': reason,
        'failed_reason': reason,
        'worker_id': row['claimed_by_worker_id'],
        'worker_hostname': row['worker_hostname'],
        'worker_pid': row['worker_pid'],
        'worker_process_name': row['worker_process_name'],
    }


def _recover_one(
    connection: Connection,
    task_id: object,
    stale_ms: int,
    finalizing_ms: int,
) -> bool:
    """One candidate in one transaction. Whether its retry or failure committed."""
    thresholds = {
        'stale_threshold': stale_ms / 1000.0,
        'finalizing_stale_threshold': finalizing_ms / 1000.0,
    }
    reason = (
        f'Worker process crashed (no runner heartbeat for {stale_ms}ms = {stale_ms / 1000:.1f}s)'
    )
    with connection.transaction():
        cursor = connection.cursor(row_factory=dict_row)
        _ = cursor.execute(LOCK_TASK_SQL, {'task_id': task_id})
        row = cursor.execute(
            SELECT_STALE_TASK_FOR_UPDATE_SQL, {'id': task_id, **thresholds}
        ).fetchone()
        if row is None:
            raise psycopg.Rollback()
        db_now: datetime = row['db_now']
        retry = _next_retry(row, db_now)
        if retry is not None:
            retry_count, next_retry_at = retry
            _ = cursor.execute(UPSERT_TASK_ATTEMPT_SQL, _attempt(row, True, reason, db_now))
            scheduled = cursor.execute(
                SCHEDULE_STALE_TASK_RETRY_SQL,
                {
                    'id': task_id,
                    'retry_count': retry_count,
                    'next_retry_at': next_retry_at,
                    **thresholds,
                },
            ).fetchone()
            if scheduled is None:
                raise psycopg.Rollback()
        else:
            _ = cursor.execute(UPSERT_TASK_ATTEMPT_SQL, _attempt(row, False, reason, db_now))
            failed = cursor.execute(
                FAIL_STALE_TASK_SQL,
                {
                    'task_id': task_id,
                    'stale_after_ms': stale_ms,
                    'finalizing_stale_after_ms': finalizing_ms,
                    'result': _crash_result(row, stale_ms, reason, db_now),
                    'error_code': _CRASHED.value,
                    'failed_reason': reason,
                },
            ).fetchone()
            if failed is None or failed['outcome'] != 'APPLIED':
                raise psycopg.Rollback()
        return True
    return False


def mark_stale_tasks_as_failed(
    connection: Connection,
    stale_ms: int,
    finalizing_ms: int,
) -> int:
    """The old pass: the scan, then one transaction per candidate. The number recovered."""
    with connection.transaction(force_rollback=True):
        candidate_ids: list[object] = [
            record[0]
            for record in connection.execute(
                SELECT_STALE_RUNNING_TASKS_SQL,
                {
                    'stale_threshold': stale_ms / 1000.0,
                    'finalizing_stale_threshold': finalizing_ms / 1000.0,
                },
            ).fetchall()
        ]
    return sum(
        1 for task_id in candidate_ids if _recover_one(connection, task_id, stale_ms, finalizing_ms)
    )
