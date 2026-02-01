-- Task counts grouped by worker, filtered by status array
-- Parameter $1: text[] - array of status values to include (e.g., ARRAY['CLAIMED', 'RUNNING'])
-- Returns same schema as aggregated-breakdown.sql but filtered
WITH base AS (
  SELECT
    claimed_by_worker_id,
    status::text AS status,
    id,
    sent_at,
    started_at
  FROM horsies_tasks
  WHERE status::text = ANY($1)
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
    -- Collect task IDs for all filtered statuses (not just claimed/running)
    ARRAY_AGG(id ORDER BY sent_at) AS task_ids
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
  -- For backwards compat, map task_ids to claimed_task_ids (we'll use this for all filtered IDs)
  CASE WHEN g = 1 THEN NULL ELSE task_ids END       AS claimed_task_ids,
  CASE WHEN g = 1 THEN NULL ELSE NULL::text[] END   AS running_task_ids
FROM agg
ORDER BY g, worker_id;
