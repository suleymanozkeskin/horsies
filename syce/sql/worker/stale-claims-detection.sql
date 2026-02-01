-- Workers with high claimed/running ratio (potential stale claims)
-- Alerts on workers where claimed tasks are 2x+ running tasks
-- Returns: worker_id, hostname, tasks_running, tasks_claimed,
--          claim_ratio, snapshot_at
-- Return schema:
-- - worker_id (text)
-- - hostname (text)
-- - tasks_running (integer)
-- - tasks_claimed (integer)
-- - claim_ratio (double precision, nullable)
-- - snapshot_at (timestamp with time zone)
SELECT DISTINCT ON (worker_id)
    worker_id,
    hostname,
    tasks_running,
    tasks_claimed,
    ROUND((tasks_claimed::float / NULLIF(tasks_running, 0))::numeric, 2)::double precision as claim_ratio,
    snapshot_at
FROM horsies_worker_states
WHERE snapshot_at > NOW() - INTERVAL '2 minutes'
  AND tasks_claimed > tasks_running * 2
ORDER BY worker_id, snapshot_at DESC;
