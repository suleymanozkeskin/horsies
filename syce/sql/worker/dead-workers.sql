-- Dead Worker Detection
-- Identifies workers that have stopped sending snapshots
-- For use in alerting and cleanup operations
-- Return schema:
-- - worker_id (text)
-- - hostname (text)
-- - pid (integer)
-- - last_seen (timestamp with time zone)
-- - offline_duration (interval)
-- - tasks_at_death (integer)

-- Workers that haven't sent snapshots in 2+ minutes
SELECT
    worker_id,
    hostname,
    pid,
    MAX(snapshot_at) as last_seen,
    age(NOW(), MAX(snapshot_at)) as offline_duration,
    MAX(tasks_claimed) as tasks_at_death
FROM horsies_worker_states
GROUP BY worker_id, hostname, pid
HAVING MAX(snapshot_at) < NOW() - INTERVAL '2 minutes'
ORDER BY last_seen DESC;
