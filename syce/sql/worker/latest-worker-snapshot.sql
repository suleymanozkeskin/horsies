-- Current state of all active workers (latest snapshot per worker)
-- Returns: worker_id, hostname, pid, capacity, tasks_running, tasks_claimed,
--          utilization_pct, memory_percent, cpu_percent, last_seen, offline_duration
-- Return schema:
-- - worker_id (text)
-- - hostname (text)
-- - pid (integer)
-- - capacity (integer)
-- - tasks_running (integer)
-- - tasks_claimed (integer)
-- - utilization_pct (double precision, nullable)
-- - memory_percent (double precision, nullable)
-- - cpu_percent (double precision, nullable)
-- - last_seen (timestamp with time zone)
-- - offline_duration (interval)
SELECT DISTINCT ON (worker_id)
    worker_id,
    hostname,
    pid,
    processes as capacity,
    tasks_running,
    tasks_claimed,
    ROUND((100.0 * tasks_running / NULLIF(processes, 0))::numeric, 1)::double precision as utilization_pct,
    memory_percent,
    cpu_percent,
    snapshot_at as last_seen,
    age(NOW(), snapshot_at) as offline_duration
FROM horsies_worker_states
ORDER BY worker_id, snapshot_at DESC;
