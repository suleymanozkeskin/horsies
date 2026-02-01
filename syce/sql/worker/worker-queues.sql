-- Queue-Specific Worker Monitoring
-- Shows which workers are serving which queues and their priorities
-- For use in multi-queue setups and queue-specific dashboards
-- Return schema:
-- - worker_id (text)
-- - hostname (text)
-- - queues (text[])
-- - queue_priorities (jsonb, nullable)
-- - queue_max_concurrency (jsonb, nullable)
-- - tasks_running (integer)
-- - tasks_claimed (integer)

-- Workers serving specific queues (for multi-queue setups)
SELECT DISTINCT ON (worker_id)
    worker_id,
    hostname,
    queues,
    queue_priorities,
    queue_max_concurrency,
    tasks_running,
    tasks_claimed
FROM horsies_worker_states
WHERE snapshot_at > NOW() - INTERVAL '2 minutes'
ORDER BY worker_id, snapshot_at DESC;
