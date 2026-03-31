-- Fetch task rows with composable filters.
-- Parameters:
--   $1: text    - worker_id (NULL = all workers)
--   $2: text[]  - status filter (empty array = all statuses)
--   $3: bool    - retried_only (true = only retry_count > 0)
--   $4: text[]  - task_name filter (empty array = all names)
--   $5: text[]  - queue_name filter (empty array = all queues)
--   $6: text[]  - error_code filter (empty array = all errors)
SELECT
    id,
    task_name,
    queue_name,
    status::text AS status,
    error_code,
    retry_count,
    max_retries,
    enqueued_at,
    started_at,
    completed_at,
    failed_at
FROM horsies_tasks
WHERE ($1::text IS NULL OR claimed_by_worker_id = $1)
  AND (CARDINALITY($2::text[]) = 0 OR status::text = ANY($2))
  AND (NOT $3::bool OR retry_count > 0)
  AND (CARDINALITY($4::text[]) = 0 OR task_name = ANY($4))
  AND (CARDINALITY($5::text[]) = 0 OR queue_name = ANY($5))
  AND (CARDINALITY($6::text[]) = 0 OR error_code = ANY($6))
ORDER BY enqueued_at DESC
LIMIT 500
