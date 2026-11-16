-- Replace workflow_def_module/workflow_def_qualname with definition_key.
-- Replace sub_workflow_module/sub_workflow_qualname with sub_definition_key.
-- Matches Python's current schema contract.

-- Add new identity columns.
ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS definition_key VARCHAR(255);

ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_definition_key VARCHAR(255);

-- Drop retired columns from horsies_workflows.
ALTER TABLE horsies_workflows
    DROP COLUMN IF EXISTS workflow_def_module,
    DROP COLUMN IF EXISTS workflow_def_qualname;

-- Drop retired columns from horsies_workflow_tasks.
ALTER TABLE horsies_workflow_tasks
    DROP COLUMN IF EXISTS sub_workflow_module,
    DROP COLUMN IF EXISTS sub_workflow_qualname,
    DROP COLUMN IF EXISTS sub_workflow_retry_mode;
