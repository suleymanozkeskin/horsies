-- Fetch a single task by ID for the detail view
SELECT
    id,
    task_name,
    queue_name,
    priority,
    status::text as status,
    args,
    kwargs,
    result,
    failed_reason,
    retry_count,
    max_retries,
    worker_hostname,
    worker_pid,
    claimed_by_worker_id,
    sent_at,
    enqueued_at,
    claimed_at,
    started_at,
    completed_at,
    failed_at,
    created_at,
    updated_at
FROM horsies_tasks
WHERE id = $1
