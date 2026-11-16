-- Trigger: notify on task inserts (new pending) and status changes (done).
CREATE OR REPLACE FUNCTION horsies_notify_task_changes()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.status = 'PENDING' THEN
        PERFORM pg_notify('task_new', NEW.id);
        PERFORM pg_notify('task_queue_' || NEW.queue_name, NEW.id);
    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        IF NEW.status IN ('COMPLETED', 'FAILED') THEN
            PERFORM pg_notify('task_done', NEW.id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS horsies_task_notify_trigger ON horsies_tasks;
CREATE TRIGGER horsies_task_notify_trigger
    AFTER INSERT OR UPDATE ON horsies_tasks
    FOR EACH ROW
    EXECUTE FUNCTION horsies_notify_task_changes();

-- Trigger: notify on workflow terminal status changes.
CREATE OR REPLACE FUNCTION horsies_notify_workflow_changes()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'PAUSED') THEN
            PERFORM pg_notify('workflow_done', NEW.id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS horsies_workflow_notify_trigger ON horsies_workflows;
CREATE TRIGGER horsies_workflow_notify_trigger
    AFTER UPDATE ON horsies_workflows
    FOR EACH ROW
    EXECUTE FUNCTION horsies_notify_workflow_changes();
