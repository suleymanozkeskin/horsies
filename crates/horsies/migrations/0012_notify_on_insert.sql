-- Extend task/workflow status triggers to also fire on INSERT.
-- Previous migration (0011) only covered UPDATE, so new tasks and workflows
-- never sent a horsies_task_status / horsies_workflow_status notification.

-- Task: fire on INSERT or status-changing UPDATE
CREATE OR REPLACE FUNCTION notify_task_status_change() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status IS DISTINCT FROM NEW.status) THEN
        PERFORM pg_notify('horsies_task_status', NEW.id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS task_status_change_notify ON horsies_tasks;
CREATE TRIGGER task_status_change_notify
AFTER INSERT OR UPDATE ON horsies_tasks
FOR EACH ROW EXECUTE FUNCTION notify_task_status_change();

-- Workflow: fire on INSERT or status-changing UPDATE
CREATE OR REPLACE FUNCTION notify_workflow_status_change() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status IS DISTINCT FROM NEW.status) THEN
        PERFORM pg_notify('horsies_workflow_status', NEW.id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflow_status_change_notify ON horsies_workflows;
CREATE TRIGGER workflow_status_change_notify
AFTER INSERT OR UPDATE ON horsies_workflows
FOR EACH ROW EXECUTE FUNCTION notify_workflow_status_change();
