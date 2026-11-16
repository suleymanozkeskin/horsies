-- Add EXPIRED to the task_done notification trigger.
-- EXPIRED tasks are terminal and should wake get_result() waiters.

CREATE OR REPLACE FUNCTION notify_task_done() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
        PERFORM pg_notify('task_done', NEW.id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
