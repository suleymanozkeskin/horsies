"""Frozen pre-consolidation SQL used only as performance controls.

These statements are not runtime writers. The performance harness retains
them so a future explicitly requested benchmark can compare the consolidated
operations with their former implementation. Correctness tests and production
code must not import this module.
"""

from sqlalchemy import text


MARK_TASK_FAILED_SQL = text("""
    UPDATE horsies_tasks
    SET status='FAILED',
        failed_at = NOW(),
        result = :result_json,
        error_code = :error_code,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = :id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
    RETURNING id
""")

MARK_TASK_COMPLETED_SQL = text("""
    UPDATE horsies_tasks
    SET status='COMPLETED',
        completed_at = NOW(),
        result = :result_json,
        error_code = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = :id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
    RETURNING id
""")

FINALIZE_TASK_COMPLETED_SQL = text("""
    WITH ctx AS (
        SELECT id, retry_count, started_at, claimed_by_worker_id,
               worker_hostname, worker_pid, worker_process_name,
               clock_timestamp() AS db_now
        FROM horsies_tasks
        WHERE id = :id
          AND status = 'RUNNING'
          AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
          AND (CAST(:claimed_at AS TIMESTAMPTZ) IS NULL
               OR claimed_at = CAST(:claimed_at AS TIMESTAMPTZ))
        FOR UPDATE
    ),
    attempt AS (
        INSERT INTO horsies_task_attempts (
            task_id, attempt, outcome, will_retry,
            started_at, finished_at,
            error_code, error_message, failed_reason,
            worker_id, worker_hostname, worker_pid, worker_process_name
        )
        SELECT ctx.id, COALESCE(ctx.retry_count, 0) + 1, 'COMPLETED', FALSE,
               COALESCE(ctx.started_at, ctx.db_now), ctx.db_now,
               NULL, NULL, NULL,
               ctx.claimed_by_worker_id, ctx.worker_hostname, ctx.worker_pid,
               ctx.worker_process_name
        FROM ctx
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
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'COMPLETED',
            completed_at = NOW(),
            result = :result_json,
            error_code = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            terminal_at = NOW(),
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
        RETURNING t.id
    )
    SELECT upd.id, pg_notify(:notify_channel, :notify_payload)
    FROM upd
""")

MARK_STALE_TASK_FAILED_SQL = text("""
    UPDATE horsies_tasks AS t
    SET status = 'FAILED',
        failed_at = NOW(),
        failed_reason = :failed_reason,
        result = :result,
        error_code = :error_code,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE t.id = :task_id
      AND t.status = 'RUNNING'
      AND t.started_at IS NOT NULL
      AND (
          t.finalizing_at IS NULL
          OR t.finalizing_at < NOW()
              - CAST(:finalizing_stale_threshold || ' seconds' AS INTERVAL)
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
      ) < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL)
    RETURNING t.id
""")

EXPIRE_PENDING_TASKS_SQL = text("""
    UPDATE horsies_tasks t
    SET status = 'EXPIRED',
        failed_at = NOW(),
        result = :result,
        error_code = :error_code,
        terminal_at = NOW(),
        updated_at = NOW()
    FROM (
        SELECT id FROM horsies_tasks
        WHERE status = 'PENDING'
          AND good_until IS NOT NULL
          AND good_until <= NOW()
        ORDER BY good_until ASC
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    ) s
    WHERE t.id = s.id
""")

EXPIRE_CLAIMED_TASK_BEFORE_START_SQL = """
    UPDATE horsies_tasks
    SET status = 'EXPIRED',
        claimed = FALSE,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        failed_at = NOW(),
        result = %s,
        error_code = %s,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = %s
      AND status = 'CLAIMED'
      AND claimed_by_worker_id = %s
      AND good_until IS NOT NULL
      AND good_until <= NOW()
    RETURNING id
"""
