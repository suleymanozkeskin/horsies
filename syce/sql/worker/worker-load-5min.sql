-- Worker load over last 5 minutes (for line charts)
-- Returns: worker_id, snapshot_at, tasks_running, tasks_claimed,
--          memory_percent, cpu_percent
-- Return schema:
-- - worker_id (text)
-- - snapshot_at (timestamp with time zone)
-- - tasks_running (integer)
-- - tasks_claimed (integer)
-- - memory_percent (double precision, nullable)
-- - cpu_percent (double precision, nullable)
SELECT
    worker_id,
    snapshot_at,
    tasks_running,
    tasks_claimed,
    memory_percent,
    cpu_percent
FROM horsies_worker_states
WHERE snapshot_at > NOW() - INTERVAL '5 minutes'
ORDER BY worker_id, snapshot_at;
