-- Task lifecycle columns (parity with horsies PR #96).
--
-- is_workflow_task: denormalized flag so the claim/finalize path can route
--   workflow tasks without a per-task JOIN to horsies_workflow_tasks.
-- finalizing_at / finalizing_by_worker_id: finalization handoff metadata so the
--   stale-RUNNING reaper can skip a task that is actively being finalized.

ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS is_workflow_task BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS finalizing_at TIMESTAMPTZ;
ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS finalizing_by_worker_id TEXT;

CREATE INDEX IF NOT EXISTS idx_horsies_tasks_is_workflow_task
    ON horsies_tasks (is_workflow_task);
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_finalizing_at
    ON horsies_tasks (finalizing_at);

-- One-time backfill: any task linked from a workflow_task is a workflow task.
UPDATE horsies_tasks t
SET is_workflow_task = TRUE
WHERE is_workflow_task = FALSE
  AND EXISTS (SELECT 1 FROM horsies_workflow_tasks wt WHERE wt.task_id = t.id);
