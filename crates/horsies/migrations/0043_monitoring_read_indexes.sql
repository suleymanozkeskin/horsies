-- Python schema v16 monitoring read indexes.
--
-- The default task list orders by enqueued_at and stops at a small LIMIT.
-- The task-name facet groups an immutable, heavily deduplicated column.
-- Neither index can serve the queue-leading, priority-leading claim plan.
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_enqueued_at
    ON horsies_tasks (enqueued_at);

CREATE INDEX IF NOT EXISTS idx_horsies_tasks_task_name
    ON horsies_tasks (task_name);
