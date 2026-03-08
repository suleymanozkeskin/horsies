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
-- Uses DISTINCT ON to get the latest snapshot per worker (actual state at death)
SELECT
    worker_id,
    hostname,
    pid,
    snapshot_at as last_seen,
    age(NOW(), snapshot_at) as offline_duration,
    tasks_claimed as tasks_at_death
FROM (
    SELECT DISTINCT ON (worker_id)
        worker_id,
        hostname,
        pid,
        snapshot_at,
        tasks_claimed
    FROM horsies_worker_states
    ORDER BY worker_id, snapshot_at DESC
) latest
WHERE snapshot_at < NOW() - INTERVAL '2 minutes'
ORDER BY last_seen DESC;
