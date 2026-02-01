-- Delete old snapshots (run periodically as maintenance job)
-- Removes snapshots older than 7 days to manage storage
-- Return schema:
-- - none (DELETE returns only command tag and rows affected count)
DELETE FROM horsies_worker_states
WHERE snapshot_at < NOW() - INTERVAL '7 days';
