-- Task counts grouped by worker (with rollup for totals)
-- Parameter $1: bool - when true, only include tasks with retry_count > 0
-- Return schema:
-- - worker_id (text)
-- - total_count (bigint)
-- - pending_count (bigint)
-- - claimed_count (bigint)
-- - running_count (bigint)
-- - completed_count (bigint)
-- - failed_count (bigint)
-- - cancelled_count (bigint)
-- - expired_count (bigint)
-- - retried_count (bigint)
-- - claimed_task_ids (text[], nullable)
-- - running_task_ids (text[], nullable)
-- - claimed_retry_counts (int[], nullable)
-- - running_retry_counts (int[], nullable)
WITH base AS (
  SELECT
    claimed_by_worker_id,
    status::text AS status,
    id,
    enqueued_at,
    started_at,
    retry_count
  FROM horsies_tasks
  WHERE (NOT $1::bool OR retry_count > 0)
),
agg AS (
  SELECT
    claimed_by_worker_id,
    GROUPING(claimed_by_worker_id) AS g,
    COUNT(*)                                        AS total_count,
    COUNT(*) FILTER (WHERE status = 'PENDING')      AS pending_count,
    COUNT(*) FILTER (WHERE status = 'CLAIMED')      AS claimed_count,
    COUNT(*) FILTER (WHERE status = 'RUNNING')      AS running_count,
    COUNT(*) FILTER (WHERE status = 'COMPLETED')    AS completed_count,
    COUNT(*) FILTER (WHERE status = 'FAILED')       AS failed_count,
    COUNT(*) FILTER (WHERE status = 'CANCELLED')   AS cancelled_count,
    COUNT(*) FILTER (WHERE status = 'EXPIRED')    AS expired_count,
    COUNT(*) FILTER (WHERE retry_count > 0)         AS retried_count,
    -- Collect all matching task IDs and retry counts for expand
    ARRAY_AGG(id ORDER BY enqueued_at)              AS task_ids,
    ARRAY_AGG(retry_count ORDER BY enqueued_at)     AS retry_counts
  FROM base
  GROUP BY ROLLUP (claimed_by_worker_id)
)
SELECT
  CASE WHEN g = 1 THEN 'TOTAL'
       ELSE COALESCE(claimed_by_worker_id, 'unknown')
  END                                               AS worker_id,
  total_count,
  pending_count,
  claimed_count,
  running_count,
  completed_count,
  failed_count,
  cancelled_count,
  expired_count,
  retried_count,
  CASE WHEN g = 1 THEN NULL ELSE task_ids END          AS claimed_task_ids,
  CASE WHEN g = 1 THEN NULL ELSE NULL::text[] END      AS running_task_ids,
  CASE WHEN g = 1 THEN NULL ELSE retry_counts END      AS claimed_retry_counts,
  CASE WHEN g = 1 THEN NULL ELSE NULL::int[] END       AS running_retry_counts

FROM agg
ORDER BY g, worker_id;
