-- Add condition flags to horsies_workflow_tasks.
-- These indicate whether a node had run_when/skip_when closures registered
-- at build time. The engine checks these flags to decide whether to look up
-- conditions in the WorkflowSpecRegistry before enqueuing.

ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS has_run_when BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_skip_when BOOLEAN NOT NULL DEFAULT FALSE;
