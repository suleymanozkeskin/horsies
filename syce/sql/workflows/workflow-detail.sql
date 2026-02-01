-- Single workflow's task details for the detail view
-- Parameter: $1 = workflow_id
SELECT
    wt.task_index,
    wt.node_id,
    wt.task_name,
    wt.queue_name,
    wt.priority,
    wt.status,
    wt.dependencies,
    wt.args_from,
    wt.workflow_ctx_from,
    wt.allow_failed_deps,
    wt.join_type,
    wt.min_success,
    wt.task_options,
    wt.task_id,
    wt.started_at,
    wt.completed_at,
    wt.result,
    wt.error
FROM horsies_workflow_tasks wt
WHERE wt.workflow_id = $1
ORDER BY wt.task_index
