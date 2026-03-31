-- Task counts grouped by worker, filtered by status array and optional retried filter
-- Parameter $1: text[] - array of status values to include (e.g., ARRAY['CLAIMED', 'RUNNING'])
-- Parameter $2: bool - when true, only include tasks with retry_count > 0
-- Returns same schema as aggregated-breakdown.sql but filtered
WITH base AS (
  SELECT
    claimed_by_worker_id,
    status::text AS status,
    id,
    enqueued_at,
    started_at,
    retry_count
  FROM horsies_tasks
  WHERE status::text = ANY($1)
    AND (NOT $2::bool OR retry_count > 0)
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
    -- Collect task IDs for all filtered statuses (not just claimed/running)
    ARRAY_AGG(id ORDER BY enqueued_at) AS task_ids,
    ARRAY_AGG(retry_count ORDER BY enqueued_at) AS retry_counts
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
  -- For backwards compat, map task_ids to claimed_task_ids (we'll use this for all filtered IDs)
  CASE WHEN g = 1 THEN NULL ELSE task_ids END           AS claimed_task_ids,
  CASE WHEN g = 1 THEN NULL ELSE NULL::text[] END       AS running_task_ids,
  CASE WHEN g = 1 THEN NULL ELSE retry_counts END       AS claimed_retry_counts,
  CASE WHEN g = 1 THEN NULL ELSE NULL::int[] END        AS running_retry_counts
FROM agg
ORDER BY g, worker_id;
