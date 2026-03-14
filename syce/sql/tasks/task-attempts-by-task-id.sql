SELECT
    task_id,
    attempt,
    outcome,
    will_retry,
    started_at,
    finished_at,
    error_code,
    error_message,
    failed_reason,
    worker_id,
    worker_hostname,
    worker_pid,
    worker_process_name
FROM horsies_task_attempts
WHERE task_id = $1
ORDER BY attempt DESC;
