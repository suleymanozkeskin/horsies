-- Active workers only (seen in last 2 minutes)
-- Returns: worker_id, hostname, pid, processes, tasks_running, tasks_claimed,
--          memory_percent, cpu_percent, snapshot_at
-- Return schema:
-- - worker_id (text)
-- - hostname (text)
-- - pid (integer)
-- - processes (integer)
-- - tasks_running (integer)
-- - tasks_claimed (integer)
-- - memory_percent (double precision, nullable)
-- - cpu_percent (double precision, nullable)
-- - snapshot_at (timestamp with time zone)
SELECT DISTINCT ON (worker_id)
    worker_id,
    hostname,
    pid,
    processes,
    tasks_running,
    tasks_claimed,
    memory_percent,
    cpu_percent,
    snapshot_at
FROM horsies_worker_states
WHERE snapshot_at > NOW() - INTERVAL '2 minutes'
ORDER BY worker_id, snapshot_at DESC;
