-- Overloaded workers (high memory or CPU)
-- Alerts on workers with memory > 85% or CPU > 80%
-- Returns: worker_id, hostname, memory_percent, cpu_percent,
--          tasks_running, processes, snapshot_at
-- Return schema:
-- - worker_id (text)
-- - hostname (text)
-- - memory_percent (double precision, nullable)
-- - cpu_percent (double precision, nullable)
-- - tasks_running (integer)
-- - processes (integer)
-- - snapshot_at (timestamp with time zone)
SELECT DISTINCT ON (worker_id)
    worker_id,
    hostname,
    memory_percent,
    cpu_percent,
    tasks_running,
    processes,
    snapshot_at
FROM horsies_worker_states
WHERE snapshot_at > NOW() - INTERVAL '5 minutes'
  AND (memory_percent > 85 OR cpu_percent > 80)
ORDER BY worker_id, snapshot_at DESC;
