"""SQL constants for the Worker."""

from __future__ import annotations

from sqlalchemy import text

from horsies.core.models.workflow import WORKFLOW_TERMINAL_STATES
from horsies.core.types.status import TASK_TERMINAL_STATES

WORKFLOW_TERMINAL_VALUES: list[str] = [s.value for s in WORKFLOW_TERMINAL_STATES]
TASK_TERMINAL_VALUES: list[str] = [s.value for s in TASK_TERMINAL_STATES]


# ---------- Claim SQL (priority + sent_at) ----------
# All claimed tasks receive a bounded lease via claim_expires_at.
# Expired leases are reclaimable by any worker (crash recovery + soft-cap prefetch).

CLAIM_SQL = text("""
WITH next AS (
  SELECT id
  FROM horsies_tasks
  WHERE queue_name = :queue
    AND (
      -- Fresh pending tasks
      status = 'PENDING'
      -- OR expired claims (lease expired, reclaimable by any worker)
      OR (status = 'CLAIMED' AND claim_expires_at IS NOT NULL AND claim_expires_at < now())
    )
    AND sent_at <= now()
    AND (next_retry_at IS NULL OR next_retry_at <= now())
    AND (good_until IS NULL OR good_until > now())
  ORDER BY priority ASC, sent_at ASC, id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT :lim
)
UPDATE horsies_tasks t
SET status = 'CLAIMED',
    claimed = TRUE,
    claimed_at = now(),
    claimed_by_worker_id = :worker_id,
    claim_expires_at = :claim_expires_at,
    updated_at = now()
FROM next
WHERE t.id = next.id
RETURNING t.id, t.task_name, t.args, t.kwargs;
""")


# ---------- Worker SQL constants ----------

CLAIM_ADVISORY_LOCK_SQL = text("""
    SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))
""")

# Effective in-flight counts: expired CLAIMED tasks are excluded because they
# are reclaimable and must not consume cap budget.  The predicate for an
# "active" claim is: claim_expires_at IS NULL OR claim_expires_at > now().

COUNT_GLOBAL_IN_FLIGHT_SQL = text("""
    SELECT COUNT(*) FROM horsies_tasks
    WHERE status = 'RUNNING'
       OR (status = 'CLAIMED'
           AND (claim_expires_at IS NULL OR claim_expires_at > now()))
""")

COUNT_QUEUE_IN_FLIGHT_HARD_SQL = text("""
    SELECT COUNT(*) FROM horsies_tasks
    WHERE queue_name = :q
      AND (status = 'RUNNING'
           OR (status = 'CLAIMED'
               AND (claim_expires_at IS NULL OR claim_expires_at > now())))
""")

COUNT_QUEUE_IN_FLIGHT_SOFT_SQL = text("""
    SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = :q
""")

COUNT_CLAIMED_FOR_WORKER_SQL = text("""
    SELECT COUNT(*)
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND status = 'CLAIMED'
      AND (claim_expires_at IS NULL OR claim_expires_at > now())
""")

COUNT_RUNNING_FOR_WORKER_SQL = text("""
    SELECT COUNT(*)
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND status = 'RUNNING'
""")

COUNT_IN_FLIGHT_FOR_WORKER_SQL = text("""
    SELECT COUNT(*)
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND (status = 'RUNNING'
           OR (status = 'CLAIMED'
               AND (claim_expires_at IS NULL OR claim_expires_at > now())))
""")

COUNT_RUNNING_IN_QUEUE_SQL = text("""
    SELECT COUNT(*)
    FROM horsies_tasks
    WHERE status = 'RUNNING'
      AND queue_name = :q
""")

GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL = text("""
    SELECT t.id, w.status
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE t.id = ANY(:ids)
      AND w.status IN ('PAUSED', 'CANCELLED')
""")

UNCLAIM_PAUSED_TASKS_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        updated_at = NOW()
    WHERE id = ANY(:ids)
""")

UNCLAIM_CLAIMED_TASK_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        updated_at = NOW()
    WHERE id = :id
      AND status = 'CLAIMED'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
""")

RESET_PAUSED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY', task_id = NULL, started_at = NULL
    WHERE task_id = ANY(:ids)
""")

CANCEL_CANCELLED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'CANCELLED',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        updated_at = NOW()
    WHERE id = ANY(:ids)
      AND status IN ('CLAIMED', 'PENDING')
""")

SKIP_CANCELLED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED',
        completed_at = NOW()
    WHERE task_id = ANY(:ids)
      AND status IN ('PENDING', 'READY', 'ENQUEUED')
""")

MARK_TASK_FAILED_WORKER_SQL = text("""
    UPDATE horsies_tasks
    SET status='FAILED',
        failed_at = :now,
        failed_reason = :reason,
        updated_at = :now
    WHERE id = :id
      AND status = 'RUNNING'
    RETURNING id
""")

MARK_TASK_FAILED_SQL = text("""
    UPDATE horsies_tasks
    SET status='FAILED',
        failed_at = :now,
        result = :result_json,
        updated_at = :now
    WHERE id = :id
      AND status = 'RUNNING'
    RETURNING id
""")

MARK_TASK_COMPLETED_SQL = text("""
    UPDATE horsies_tasks
    SET status='COMPLETED',
        completed_at = :now,
        result = :result_json,
        updated_at = :now
    WHERE id = :id
      AND status = 'RUNNING'
    RETURNING id
""")

GET_TASK_QUEUE_NAME_SQL = text("""
    SELECT queue_name FROM horsies_tasks WHERE id = :id
""")

NOTIFY_TASK_NEW_SQL = text("""
    SELECT pg_notify(:c1, :p)
""")

NOTIFY_TASK_QUEUE_SQL = text("""
    SELECT pg_notify(:c2, :p)
""")

CHECK_WORKFLOW_TASK_EXISTS_SQL = text("""
    SELECT 1 FROM horsies_workflow_tasks WHERE task_id = :tid LIMIT 1
""")

GET_TASK_RETRY_INFO_SQL = text("""
    SELECT retry_count, max_retries, task_options, good_until, clock_timestamp() AS db_now
    FROM horsies_tasks
    WHERE id = :id
""")

GET_TASK_RETRY_CONFIG_SQL = text("""
    SELECT retry_count, task_options, good_until, clock_timestamp() AS db_now
    FROM horsies_tasks
    WHERE id = :id
""")

GET_TASK_RETRY_POSTCHECK_SQL = text("""
    SELECT status, good_until
    FROM horsies_tasks
    WHERE id = :id
""")

SCHEDULE_TASK_RETRY_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        retry_count = :retry_count,
        next_retry_at = :next_retry_at,
        sent_at = :next_retry_at,
        updated_at = now()
    WHERE id = :id
      AND status = 'RUNNING'
      AND (good_until IS NULL OR :next_retry_at < good_until)
    RETURNING id
""")

NOTIFY_DELAYED_SQL = text("""
    SELECT pg_notify(:channel, :payload)
""")

INSERT_CLAIMER_HEARTBEAT_SQL = text("""
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    SELECT id, CAST(:wid AS VARCHAR), 'claimer', NOW(), :host, :pid
    FROM horsies_tasks
    WHERE status = 'CLAIMED' AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
""")

RENEW_CLAIM_LEASE_SQL = text("""
    UPDATE horsies_tasks
    SET claim_expires_at = :new_expires_at,
        updated_at = NOW()
    WHERE status = 'CLAIMED'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND claimed_at >= NOW() - :max_claim_age_ms * INTERVAL '1 millisecond'
""")

INSERT_WORKER_STATE_SQL = text("""
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid,
        processes, max_claim_batch, max_claim_per_worker,
        cluster_wide_cap, queues, queue_priorities, queue_max_concurrency,
        recovery_config, tasks_running, tasks_claimed,
        memory_usage_mb, memory_percent, cpu_percent,
        worker_started_at
    )
    VALUES (
        :wid, NOW(), :host, :pid, :procs, :mcb, :mcpw, :cwc,
        :queues, :qp, :qmc, :recovery, :running, :claimed,
        :mem_mb, :mem_pct, :cpu_pct, :started
    )
""")

DELETE_EXPIRED_HEARTBEATS_SQL = text("""
    DELETE FROM horsies_heartbeats
    WHERE sent_at < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
""")

DELETE_EXPIRED_WORKER_STATES_SQL = text("""
    DELETE FROM horsies_worker_states
    WHERE snapshot_at < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
""")

DELETE_EXPIRED_WORKFLOW_TASKS_SQL = text("""
    DELETE FROM horsies_workflow_tasks wt
    USING horsies_workflows w
    WHERE wt.workflow_id = w.id
      AND w.status = ANY(:wf_terminal_states)
      AND COALESCE(w.completed_at, w.updated_at, w.created_at) < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
""")

DELETE_EXPIRED_WORKFLOWS_SQL = text("""
    DELETE FROM horsies_workflows
    WHERE status = ANY(:wf_terminal_states)
      AND COALESCE(completed_at, updated_at, created_at) < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
""")

DELETE_EXPIRED_TASKS_SQL = text("""
    DELETE FROM horsies_tasks t
    WHERE t.status = ANY(:task_terminal_states)
      AND COALESCE(t.completed_at, t.failed_at, t.updated_at, t.created_at) < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
      AND NOT EXISTS (
          SELECT 1
          FROM horsies_workflow_tasks wt
          JOIN horsies_workflows w ON w.id = wt.workflow_id
          WHERE wt.task_id = t.id
            AND NOT (w.status = ANY(:wf_terminal_states))
      )
""")

_RETENTION_CLEANUP_INTERVAL_S = 3600.0


_FINALIZER_DRAIN_TIMEOUT_S: float = 30.0
