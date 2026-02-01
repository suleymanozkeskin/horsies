-- Workflow status aggregation for the last 24 hours
SELECT
    status,
    COUNT(*) as count,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration_secs
FROM horsies_workflows
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY status
ORDER BY count DESC
