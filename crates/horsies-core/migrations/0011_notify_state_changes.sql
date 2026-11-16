-- Triggers to notify syce (and other listeners) when task/workflow states change.
-- Payload is minimal: just the entity type. Listeners should refetch as needed.

-- Task status change notification
CREATE OR REPLACE FUNCTION notify_task_status_change() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        PERFORM pg_notify('horsies_task_status', NEW.status);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS task_status_change_notify ON horsies_tasks;
CREATE TRIGGER task_status_change_notify
AFTER UPDATE ON horsies_tasks
FOR EACH ROW EXECUTE FUNCTION notify_task_status_change();

-- Workflow status change notification
CREATE OR REPLACE FUNCTION notify_workflow_status_change() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        PERFORM pg_notify('horsies_workflow_status', NEW.status);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflow_status_change_notify ON horsies_workflows;
CREATE TRIGGER workflow_status_change_notify
AFTER UPDATE ON horsies_workflows
FOR EACH ROW EXECUTE FUNCTION notify_workflow_status_change();

-- Worker state insert notification (timeseries inserts, not updates)
CREATE OR REPLACE FUNCTION notify_worker_state_insert() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('horsies_worker_state', NEW.worker_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS worker_state_insert_notify ON horsies_worker_states;
CREATE TRIGGER worker_state_insert_notify
AFTER INSERT ON horsies_worker_states
FOR EACH ROW EXECUTE FUNCTION notify_worker_state_insert();
