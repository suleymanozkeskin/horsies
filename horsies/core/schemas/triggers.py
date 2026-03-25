"""PostgreSQL trigger definitions for LISTEN/NOTIFY."""

from __future__ import annotations

from sqlalchemy import text


# ---- Task notification triggers (worker wake-up) ----

CREATE_TASK_NOTIFY_FUNCTION_SQL = text("""
    CREATE OR REPLACE FUNCTION horsies_notify_task_changes()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' AND NEW.status = 'PENDING' THEN
            -- New task notifications: wake up workers
            PERFORM pg_notify('task_new', NEW.id);  -- Global worker notification
            PERFORM pg_notify('task_queue_' || NEW.queue_name, NEW.id);  -- Queue-specific notification
        ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
            -- Task completion notifications: wake up result waiters
            IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
                PERFORM pg_notify('task_done', NEW.id);  -- Send task_id as payload
            END IF;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
""")

CREATE_TASK_NOTIFY_TRIGGER_SQL = text("""
    DROP TRIGGER IF EXISTS horsies_task_notify_trigger ON horsies_tasks;
    CREATE TRIGGER horsies_task_notify_trigger
        AFTER INSERT OR UPDATE ON horsies_tasks
        FOR EACH ROW
        EXECUTE FUNCTION horsies_notify_task_changes();
""")


# ---- Task status triggers (TUI/monitoring) ----
# Fire on ANY status change so the TUI gets real-time updates.

CREATE_TASK_STATUS_NOTIFY_FUNCTION_SQL = text("""
    CREATE OR REPLACE FUNCTION horsies_notify_task_status_change()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status != NEW.status) THEN
            PERFORM pg_notify('horsies_task_status', NEW.id);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
""")

CREATE_TASK_STATUS_NOTIFY_TRIGGER_SQL = text("""
    DROP TRIGGER IF EXISTS horsies_task_status_notify_trigger ON horsies_tasks;
    CREATE TRIGGER horsies_task_status_notify_trigger
        AFTER INSERT OR UPDATE ON horsies_tasks
        FOR EACH ROW
        EXECUTE FUNCTION horsies_notify_task_status_change();
""")


# ---- Worker state triggers (TUI/monitoring) ----

CREATE_WORKER_STATE_NOTIFY_FUNCTION_SQL = text("""
    CREATE OR REPLACE FUNCTION horsies_notify_worker_state_change()
    RETURNS trigger AS $$
    BEGIN
        PERFORM pg_notify('horsies_worker_state', NEW.worker_id);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
""")

CREATE_WORKER_STATE_NOTIFY_TRIGGER_SQL = text("""
    DROP TRIGGER IF EXISTS horsies_worker_state_notify_trigger ON horsies_worker_states;
    CREATE TRIGGER horsies_worker_state_notify_trigger
        AFTER INSERT OR UPDATE ON horsies_worker_states
        FOR EACH ROW
        EXECUTE FUNCTION horsies_notify_worker_state_change();
""")


# ---- Workflow notification triggers (result waiters) ----

CREATE_WORKFLOW_NOTIFY_FUNCTION_SQL = text("""
    CREATE OR REPLACE FUNCTION horsies_notify_workflow_changes()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
            -- Workflow completion notifications
            IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'PAUSED') THEN
                PERFORM pg_notify('workflow_done', NEW.id);
            END IF;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
""")

CREATE_WORKFLOW_NOTIFY_TRIGGER_SQL = text("""
    DROP TRIGGER IF EXISTS horsies_workflow_notify_trigger ON horsies_workflows;
    CREATE TRIGGER horsies_workflow_notify_trigger
        AFTER UPDATE ON horsies_workflows
        FOR EACH ROW
        EXECUTE FUNCTION horsies_notify_workflow_changes();
""")


# ---- Workflow status triggers (TUI/monitoring) ----

CREATE_WORKFLOW_STATUS_NOTIFY_FUNCTION_SQL = text("""
    CREATE OR REPLACE FUNCTION horsies_notify_workflow_status_change()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status != NEW.status) THEN
            PERFORM pg_notify('horsies_workflow_status', NEW.id);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
""")

CREATE_WORKFLOW_STATUS_NOTIFY_TRIGGER_SQL = text("""
    DROP TRIGGER IF EXISTS horsies_workflow_status_notify_trigger ON horsies_workflows;
    CREATE TRIGGER horsies_workflow_status_notify_trigger
        AFTER INSERT OR UPDATE ON horsies_workflows
        FOR EACH ROW
        EXECUTE FUNCTION horsies_notify_workflow_status_change();
""")
