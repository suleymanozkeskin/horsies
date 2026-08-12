-- Schema v31: EXPIRED workflow retention and UUID-safe notifications.
DROP INDEX IF EXISTS idx_horsies_workflows_retention;
CREATE INDEX IF NOT EXISTS idx_horsies_workflows_retention
    ON horsies_workflows (COALESCE(completed_at, updated_at, created_at))
    WHERE status IN ('CANCELLED', 'COMPLETED', 'EXPIRED', 'FAILED');;
-- Remove the pre-parity Rust monitoring objects before installing the
-- canonical Python v16-v31 trigger surface.
DROP TRIGGER IF EXISTS task_status_change_notify ON horsies_tasks;
DROP TRIGGER IF EXISTS task_status_change_insert_notify ON horsies_tasks;
DROP TRIGGER IF EXISTS task_status_change_update_notify ON horsies_tasks;
DROP TRIGGER IF EXISTS workflow_status_change_notify ON horsies_workflows;
DROP TRIGGER IF EXISTS workflow_status_change_insert_notify ON horsies_workflows;
DROP TRIGGER IF EXISTS workflow_status_change_update_notify ON horsies_workflows;
DROP TRIGGER IF EXISTS worker_state_insert_notify ON horsies_worker_states;
DROP FUNCTION IF EXISTS notify_task_status_change();
DROP FUNCTION IF EXISTS notify_workflow_status_change();
DROP FUNCTION IF EXISTS notify_worker_state_insert();
CREATE OR REPLACE FUNCTION horsies_notify_task_changes()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' AND NEW.status = 'PENDING' THEN
            -- New task notifications: wake up workers
            PERFORM pg_notify('task_new', NEW.id::text);  -- Global worker notification
            PERFORM pg_notify('task_queue_' || NEW.queue_name, NEW.id::text);  -- Queue-specific notification
        ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
            -- Task completion notifications: wake up result waiters
            IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
                PERFORM pg_notify('task_done', NEW.id::text);  -- Send task_id as payload
            END IF;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;;
DROP TRIGGER IF EXISTS horsies_task_notify_trigger ON horsies_tasks;
    DROP TRIGGER IF EXISTS horsies_task_notify_insert_trigger ON horsies_tasks;
    DROP TRIGGER IF EXISTS horsies_task_notify_update_trigger ON horsies_tasks;
    CREATE TRIGGER horsies_task_notify_insert_trigger
        AFTER INSERT ON horsies_tasks
        FOR EACH ROW
        WHEN (NEW.status = 'PENDING')
        EXECUTE FUNCTION horsies_notify_task_changes();
    CREATE TRIGGER horsies_task_notify_update_trigger
        AFTER UPDATE ON horsies_tasks
        FOR EACH ROW
        WHEN (OLD.status IS DISTINCT FROM NEW.status)
        EXECUTE FUNCTION horsies_notify_task_changes();;
CREATE OR REPLACE FUNCTION horsies_notify_task_status_change()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status != NEW.status) THEN
            PERFORM pg_notify('horsies_task_status', NEW.id::text);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;;
DROP TRIGGER IF EXISTS horsies_task_status_notify_trigger ON horsies_tasks;
    DROP TRIGGER IF EXISTS horsies_task_status_notify_insert_trigger ON horsies_tasks;
    DROP TRIGGER IF EXISTS horsies_task_status_notify_update_trigger ON horsies_tasks;
    CREATE TRIGGER horsies_task_status_notify_insert_trigger
        AFTER INSERT ON horsies_tasks
        FOR EACH ROW
        EXECUTE FUNCTION horsies_notify_task_status_change();
    CREATE TRIGGER horsies_task_status_notify_update_trigger
        AFTER UPDATE ON horsies_tasks
        FOR EACH ROW
        WHEN (OLD.status IS DISTINCT FROM NEW.status)
        EXECUTE FUNCTION horsies_notify_task_status_change();;
CREATE OR REPLACE FUNCTION horsies_notify_worker_state_change()
    RETURNS trigger AS $$
    BEGIN
        PERFORM pg_notify('horsies_worker_state', NEW.worker_id);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;;
DROP TRIGGER IF EXISTS horsies_worker_state_notify_trigger ON horsies_worker_states;
    CREATE TRIGGER horsies_worker_state_notify_trigger
        AFTER INSERT OR UPDATE ON horsies_worker_states
        FOR EACH ROW
        EXECUTE FUNCTION horsies_notify_worker_state_change();;
CREATE OR REPLACE FUNCTION horsies_notify_workflow_changes()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
            -- Workflow completion notifications
            IF NEW.status IN (
                'COMPLETED', 'FAILED', 'CANCELLED', 'PAUSED', 'EXPIRED'
            ) THEN
                PERFORM pg_notify('workflow_done', NEW.id::text);
            END IF;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;;
DROP TRIGGER IF EXISTS horsies_workflow_notify_trigger ON horsies_workflows;
    CREATE TRIGGER horsies_workflow_notify_trigger
        AFTER UPDATE ON horsies_workflows
        FOR EACH ROW
        WHEN (OLD.status IS DISTINCT FROM NEW.status)
        EXECUTE FUNCTION horsies_notify_workflow_changes();;
CREATE OR REPLACE FUNCTION horsies_notify_workflow_status_change()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status != NEW.status) THEN
            PERFORM pg_notify('horsies_workflow_status', NEW.id::text);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;;
DROP TRIGGER IF EXISTS horsies_workflow_status_notify_trigger ON horsies_workflows;
    DROP TRIGGER IF EXISTS horsies_workflow_status_notify_insert_trigger ON horsies_workflows;
    DROP TRIGGER IF EXISTS horsies_workflow_status_notify_update_trigger ON horsies_workflows;
    CREATE TRIGGER horsies_workflow_status_notify_insert_trigger
        AFTER INSERT ON horsies_workflows
        FOR EACH ROW
        EXECUTE FUNCTION horsies_notify_workflow_status_change();
    CREATE TRIGGER horsies_workflow_status_notify_update_trigger
        AFTER UPDATE ON horsies_workflows
        FOR EACH ROW
        WHEN (OLD.status IS DISTINCT FROM NEW.status)
        EXECUTE FUNCTION horsies_notify_workflow_status_change();;
