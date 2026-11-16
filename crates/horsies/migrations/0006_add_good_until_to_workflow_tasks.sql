-- Add good_until column to horsies_workflow_tasks for task expiry deadlines.
ALTER TABLE horsies_workflow_tasks
ADD COLUMN IF NOT EXISTS good_until TIMESTAMPTZ;
