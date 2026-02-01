-- Recent workflows filtered by status with task progress counts
-- Parameter: $1 = status array (e.g., ARRAY['RUNNING', 'FAILED'])
SELECT
    w.id,
    w.name,
    w.status,
    w.on_error,
    w.output_task_index,
    w.success_policy,
    w.result,
    w.error,
    w.created_at,
    w.started_at,
    w.completed_at,
    w.updated_at,
    COUNT(wt.id) as total_tasks,
    COUNT(*) FILTER (WHERE wt.status = 'COMPLETED') as completed_tasks,
    COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed_tasks,
    COUNT(*) FILTER (WHERE wt.status = 'RUNNING') as running_tasks,
    COUNT(*) FILTER (WHERE wt.status = 'PENDING' OR wt.status = 'READY') as pending_tasks
FROM horsies_workflows w
LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
WHERE w.status = ANY($1)
GROUP BY w.id
ORDER BY w.created_at DESC
LIMIT 100
