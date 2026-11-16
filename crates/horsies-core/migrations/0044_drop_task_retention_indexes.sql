-- Terminal tasks move to partitioned history instead of aging in horsies_tasks.
DROP INDEX IF EXISTS idx_horsies_tasks_retention;
DROP INDEX IF EXISTS idx_horsies_tasks_queue_retention;
