-- Per-worker average utilization over last hour
-- Returns: worker_id, hostname, avg_utilization_pct, avg_memory_pct,
--          avg_cpu_pct, peak_running, snapshot_count
-- Return schema:
-- - worker_id (text)
-- - hostname (text)
-- - avg_utilization_pct (double precision)
-- - avg_memory_pct (double precision, nullable)
-- - avg_cpu_pct (double precision, nullable)
-- - peak_running (integer)
-- - snapshot_count (bigint)
SELECT
    worker_id,
    hostname,
    AVG(tasks_running::float / NULLIF(processes, 0)) * 100 as avg_utilization_pct,
    AVG(memory_percent) as avg_memory_pct,
    AVG(cpu_percent) as avg_cpu_pct,
    MAX(tasks_running) as peak_running,
    COUNT(*) as snapshot_count
FROM horsies_worker_states
WHERE snapshot_at > NOW() - INTERVAL '1 hour'
GROUP BY worker_id, hostname
ORDER BY avg_utilization_pct DESC;
