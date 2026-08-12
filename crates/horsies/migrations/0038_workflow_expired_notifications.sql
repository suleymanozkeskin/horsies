-- Schema v31: EXPIRED workflow retention and UUID-safe notifications.
DROP INDEX IF EXISTS idx_horsies_workflows_retention;
CREATE INDEX IF NOT EXISTS idx_horsies_workflows_retention
    ON horsies_workflows (COALESCE(completed_at, updated_at, created_at))
    WHERE status IN ('CANCELLED', 'COMPLETED', 'EXPIRED', 'FAILED');;
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
CREATE OR REPLACE FUNCTION notify_task_status_change()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status != NEW.status) THEN
            PERFORM pg_notify('horsies_task_status', NEW.id::text);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;;
CREATE OR REPLACE FUNCTION notify_workflow_status_change()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status != NEW.status) THEN
            PERFORM pg_notify('horsies_workflow_status', NEW.id::text);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;;
