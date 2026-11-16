"""Stale CLAIMED requeue, before ``horsies_requeue_stale_claims``.

``PostgresBroker.requeue_stale_claimed`` (``horsies/core/brokers/postgres.py``)
sent ``REQUEUE_STALE_CLAIMED_SQL`` once per pass in a session transaction,
with the threshold in seconds, and committed. The statement had no row bound.
"""

from __future__ import annotations

from typing import Final, LiteralString

from tests.dualrun.capture import Connection

REQUEUE_STALE_CLAIMED_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks AS t
    SET status = 'PENDING',
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = NOW()
    FROM (
        SELECT t2.id
        FROM horsies_tasks t2
        LEFT JOIN LATERAL (
            SELECT sent_at AS last_heartbeat
            FROM horsies_heartbeats h
            WHERE h.task_id = t2.id AND h.role = 'claimer'
            ORDER BY sent_at DESC
            LIMIT 1
        ) hb ON TRUE
        WHERE t2.status = 'CLAIMED'
          AND (
            (hb.last_heartbeat IS NULL AND t2.claimed_at IS NOT NULL AND t2.claimed_at < NOW() - CAST(%(stale_threshold)s || ' seconds' AS INTERVAL))
            OR (hb.last_heartbeat IS NOT NULL AND hb.last_heartbeat < NOW() - CAST(%(stale_threshold)s || ' seconds' AS INTERVAL))
          )
          AND NOT (
              t2.is_workflow_task = TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM horsies_workflow_tasks wt
                  WHERE wt.task_id = t2.id
                    AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
              )
          )
        FOR UPDATE OF t2 SKIP LOCKED
    ) s
    WHERE t.id = s.id
"""


def requeue_stale_claimed(connection: Connection, stale_threshold_ms: int) -> int:
    """One pass in one transaction. The number of rows requeued."""
    with connection.transaction():
        cursor = connection.execute(
            REQUEUE_STALE_CLAIMED_SQL,
            {'stale_threshold': stale_threshold_ms / 1000.0},
        )
        return cursor.rowcount
