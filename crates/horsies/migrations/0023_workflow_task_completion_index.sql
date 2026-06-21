-- Composite index for first-failed-by-index lookups in the workflow completion
-- critical section (parity with horsies PR #117, schema v8).
--
-- FIRST_FAILED_TASK_ERROR_SQL / FIRST_FAILED_REQUIRED_TASK_ERROR_SQL run
-- `WHERE workflow_id = $1 AND status = 'FAILED' [AND task_index = ANY($2)]
--  ORDER BY task_index ASC LIMIT 1` under the completion lock. Without a
-- covering composite they did an O(N) ordered scan per FAILED completion; the
-- (workflow_id, status, task_index) index makes it an ordered index fetch.

CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_wf_status_index
    ON horsies_workflow_tasks (workflow_id, status, task_index);
