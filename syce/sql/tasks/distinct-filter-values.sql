-- Fetch distinct task_names, queue_names, and error_codes with counts.
-- Scoped to an optional worker and respects status/retried filters.
-- Parameters:
--   $1: text    - worker_id (NULL = all workers)
--   $2: text[]  - status filter (empty array = all statuses)
--   $3: bool    - retried_only
-- Returns three result sets via UNION ALL with a discriminator column.
WITH filtered AS (
  SELECT task_name, queue_name, error_code
  FROM horsies_tasks
  WHERE ($1::text IS NULL OR claimed_by_worker_id = $1)
    AND (CARDINALITY($2::text[]) = 0 OR status::text = ANY($2))
    AND (NOT $3::bool OR retry_count > 0)
),
names AS (
  SELECT 'task_name' AS kind, task_name AS value, COUNT(*) AS count
  FROM filtered
  GROUP BY task_name
  ORDER BY count DESC
  LIMIT 30
),
queues AS (
  SELECT 'queue' AS kind, queue_name AS value, COUNT(*) AS count
  FROM filtered
  GROUP BY queue_name
  ORDER BY count DESC
  LIMIT 30
),
errors AS (
  SELECT 'error' AS kind, error_code AS value, COUNT(*) AS count
  FROM filtered
  WHERE error_code IS NOT NULL
  GROUP BY error_code
  ORDER BY count DESC
  LIMIT 20
)
SELECT kind, value, count FROM names
UNION ALL
SELECT kind, value, count FROM queues
UNION ALL
SELECT kind, value, count FROM errors;
