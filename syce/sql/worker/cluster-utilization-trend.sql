-- Cluster-wide utilization trend (1-minute buckets, last hour)
-- Returns: minute, avg_utilization_pct, total_running, total_capacity
-- Return schema:
-- - minute (timestamp with time zone)
-- - avg_utilization_pct (double precision, nullable)
-- - total_running (bigint, nullable)
-- - total_capacity (bigint, nullable)
SELECT
    date_trunc('minute', snapshot_at) as minute,
    ROUND((AVG(tasks_running::float / NULLIF(processes, 0)) * 100)::numeric, 2)::double precision as avg_utilization_pct,
    SUM(tasks_running) as total_running,
    SUM(processes) as total_capacity
FROM horsies_worker_states
WHERE snapshot_at > NOW() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute;
