-- Gate notify triggers with WHEN clauses (parity with horsies PR #101,
-- e455141b / schema v6).
--
-- Each notify trigger fired AFTER INSERT OR UPDATE and re-checked TG_OP / status
-- inside plpgsql. Hot-path updates that do not change status (lease renewals,
-- finalizing markers, heartbeat-driven touches) paid a function call per row.
-- Splitting into INSERT/UPDATE triggers with WHEN clauses lets Postgres evaluate
-- the guard without invoking plpgsql at all on those updates. The trigger
-- functions are unchanged (their internal guards remain a harmless backstop, and
-- the INSERT path still emits task_new for external observers).

-- horsies task notify (new-task wakeups + task_done).
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
    EXECUTE FUNCTION horsies_notify_task_changes();

-- syce task status notify (horsies_task_status).
DROP TRIGGER IF EXISTS task_status_change_notify ON horsies_tasks;
DROP TRIGGER IF EXISTS task_status_change_insert_notify ON horsies_tasks;
DROP TRIGGER IF EXISTS task_status_change_update_notify ON horsies_tasks;
CREATE TRIGGER task_status_change_insert_notify
    AFTER INSERT ON horsies_tasks
    FOR EACH ROW
    EXECUTE FUNCTION notify_task_status_change();
CREATE TRIGGER task_status_change_update_notify
    AFTER UPDATE ON horsies_tasks
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION notify_task_status_change();

-- horsies workflow notify (workflow_done). Already UPDATE-only; add the WHEN gate.
DROP TRIGGER IF EXISTS horsies_workflow_notify_trigger ON horsies_workflows;
CREATE TRIGGER horsies_workflow_notify_trigger
    AFTER UPDATE ON horsies_workflows
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION horsies_notify_workflow_changes();

-- syce workflow status notify (horsies_workflow_status).
DROP TRIGGER IF EXISTS workflow_status_change_notify ON horsies_workflows;
DROP TRIGGER IF EXISTS workflow_status_change_insert_notify ON horsies_workflows;
DROP TRIGGER IF EXISTS workflow_status_change_update_notify ON horsies_workflows;
CREATE TRIGGER workflow_status_change_insert_notify
    AFTER INSERT ON horsies_workflows
    FOR EACH ROW
    EXECUTE FUNCTION notify_workflow_status_change();
CREATE TRIGGER workflow_status_change_update_notify
    AFTER UPDATE ON horsies_workflows
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION notify_workflow_status_change();
