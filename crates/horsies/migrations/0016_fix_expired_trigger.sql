-- Fix: include EXPIRED in the task_done notification path.
--
-- Migration 0013 recreated horsies_notify_task_changes() with only
-- COMPLETED, FAILED, CANCELLED.  Migration 0015 defined a standalone
-- notify_task_done() that handles EXPIRED but never wired it to a trigger.
--
-- This migration replaces horsies_notify_task_changes() with the complete
-- set of terminal statuses and drops the orphaned notify_task_done().

CREATE OR REPLACE FUNCTION horsies_notify_task_changes()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.status = 'PENDING' THEN
        PERFORM pg_notify('task_new', NEW.id);
        PERFORM pg_notify('task_queue_' || NEW.queue_name, NEW.id);
    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
            PERFORM pg_notify('task_done', NEW.id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop the orphaned function from migration 0015.
DROP FUNCTION IF EXISTS notify_task_done();
