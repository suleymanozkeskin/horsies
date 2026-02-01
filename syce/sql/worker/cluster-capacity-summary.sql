-- Cluster capacity summary across all active workers
-- Returns: active_workers, total_capacity, total_running, total_claimed,
--          cluster_utilization_pct, avg_memory_pct, avg_cpu_pct
-- Return schema:
-- - active_workers (bigint)
-- - total_capacity (bigint, nullable)
-- - total_running (bigint, nullable)
-- - total_claimed (bigint, nullable)
-- - cluster_utilization_pct (double precision, nullable)
-- - avg_memory_pct (double precision, nullable)
-- - avg_cpu_pct (double precision, nullable)
SELECT
    COUNT(DISTINCT worker_id) as active_workers,
    SUM(processes) as total_capacity,
    SUM(tasks_running) as total_running,
    SUM(tasks_claimed) as total_claimed,
    ROUND((100.0 * SUM(tasks_running) / NULLIF(SUM(processes), 0))::numeric, 2)::double precision as cluster_utilization_pct,
    ROUND(AVG(memory_percent)::numeric, 1)::double precision as avg_memory_pct,
    ROUND(AVG(cpu_percent)::numeric, 1)::double precision as avg_cpu_pct
FROM (
    SELECT DISTINCT ON (worker_id) *
    FROM horsies_worker_states
    WHERE snapshot_at > NOW() - INTERVAL '2 minutes'
    ORDER BY worker_id, snapshot_at DESC
) latest;
