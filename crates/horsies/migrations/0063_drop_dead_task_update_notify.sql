-- horsies_task_notify_update_trigger ran horsies_notify_task_changes() on every
-- live status change. Its UPDATE branch sends task_done only for COMPLETED,
-- FAILED, CANCELLED and EXPIRED. horsies_tasks_live_status_only limits live
-- rows to PENDING, CLAIMED and RUNNING, and terminal tasks move to history, so
-- that branch cannot send. The terminalization functions send task_done.
DROP TRIGGER horsies_task_notify_update_trigger ON horsies_tasks;

CREATE OR REPLACE FUNCTION horsies_notify_task_changes()
    RETURNS trigger AS $$
    BEGIN
        IF TG_OP = 'INSERT' AND NEW.status = 'PENDING' THEN
            PERFORM pg_notify('task_new', NEW.id::text);
            PERFORM pg_notify('task_queue_' || NEW.queue_name, NEW.id::text);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
