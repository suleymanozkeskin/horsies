-- Workflow status counts for dashboard summary
SELECT
    COUNT(*) FILTER (WHERE status = 'PENDING') as pending,
    COUNT(*) FILTER (WHERE status = 'RUNNING') as running,
    COUNT(*) FILTER (WHERE status = 'COMPLETED') as completed,
    COUNT(*) FILTER (WHERE status = 'FAILED') as failed,
    COUNT(*) FILTER (WHERE status = 'PAUSED') as paused,
    COUNT(*) FILTER (WHERE status = 'CANCELLED') as cancelled
FROM horsies_workflows
