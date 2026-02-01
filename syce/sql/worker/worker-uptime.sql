-- Worker uptime (time since worker_started_at)
-- Returns: worker_id, hostname, worker_started_at, last_seen, uptime
-- Return schema:
-- - worker_id (text)
-- - hostname (text)
-- - worker_started_at (timestamp with time zone)
-- - last_seen (timestamp with time zone)
-- - uptime (interval)
SELECT DISTINCT ON (worker_id)
    worker_id,
    hostname,
    worker_started_at,
    snapshot_at as last_seen,
    age(snapshot_at, worker_started_at) as uptime
FROM horsies_worker_states
WHERE snapshot_at > NOW() - INTERVAL '5 minutes'
ORDER BY worker_id, snapshot_at DESC;
