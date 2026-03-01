-- Task counts grouped by worker (with rollup for totals)
-- Return schema:
-- - worker_id (text)
-- - total_count (bigint)
-- - pending_count (bigint)
-- - claimed_count (bigint)
-- - running_count (bigint)
-- - completed_count (bigint)
-- - failed_count (bigint)
-- - claimed_task_ids (text[], nullable)
-- - running_task_ids (text[], nullable)
WITH base AS (
  SELECT
    claimed_by_worker_id,
    status::text AS status,
    id,
    enqueued_at,
    started_at
  FROM horsies_tasks
),
agg AS (
  SELECT
    claimed_by_worker_id,
    GROUPING(claimed_by_worker_id) AS g,
    COUNT(*)                                        AS total_count,
    COUNT(*) FILTER (WHERE status = 'PENDING')      AS pending_count,
    COUNT(*) FILTER (WHERE status = 'CLAIMED')      AS claimed_count,
    ARRAY_AGG(id ORDER BY enqueued_at)
      FILTER (WHERE status = 'CLAIMED')             AS claimed_task_ids,
    COUNT(*) FILTER (WHERE status = 'RUNNING')      AS running_count,
    ARRAY_AGG(id ORDER BY started_at NULLS LAST, enqueued_at)
      FILTER (WHERE status = 'RUNNING')             AS running_task_ids,
    COUNT(*) FILTER (WHERE status = 'COMPLETED')    AS completed_count,
    COUNT(*) FILTER (WHERE status = 'FAILED')       AS failed_count
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
  CASE WHEN g = 1 THEN NULL ELSE claimed_task_ids END   AS claimed_task_ids,
  CASE WHEN g = 1 THEN NULL ELSE running_task_ids END   AS running_task_ids

FROM agg
ORDER BY g, worker_id;
