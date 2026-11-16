-- Schema parity migration: align Rust port with Python horsies spec.

-- ============================================================================
-- 1. horsies_tasks: add missing columns
-- ============================================================================

-- enqueued_at: mutable dispatch timestamp (deferred execution, retry re-enqueue).
ALTER TABLE horsies_tasks ADD COLUMN IF NOT EXISTS enqueued_at TIMESTAMPTZ;
UPDATE horsies_tasks SET enqueued_at = COALESCE(sent_at, NOW()) WHERE enqueued_at IS NULL;
ALTER TABLE horsies_tasks ALTER COLUMN enqueued_at SET NOT NULL;
ALTER TABLE horsies_tasks ALTER COLUMN enqueued_at SET DEFAULT NOW();

-- enqueue_sha: SHA-256 digest for idempotent enqueue verification.
ALTER TABLE horsies_tasks ADD COLUMN IF NOT EXISTS enqueue_sha VARCHAR(64);
UPDATE horsies_tasks SET enqueue_sha = 'legacy-pre-sha' WHERE enqueue_sha IS NULL;
ALTER TABLE horsies_tasks ALTER COLUMN enqueue_sha SET NOT NULL;

-- error_code: queryable error category on task row.
ALTER TABLE horsies_tasks ADD COLUMN IF NOT EXISTS error_code TEXT;

-- Partial index on error_code for efficient filtering.
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_error_code
    ON horsies_tasks (error_code)
    WHERE error_code IS NOT NULL;

-- ============================================================================
-- 2. horsies_workflows: add sent_at
-- ============================================================================

ALTER TABLE horsies_workflows ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ;
UPDATE horsies_workflows SET sent_at = COALESCE(created_at, NOW()) WHERE sent_at IS NULL;
ALTER TABLE horsies_workflows ALTER COLUMN sent_at SET NOT NULL;
ALTER TABLE horsies_workflows ALTER COLUMN sent_at SET DEFAULT NOW();

-- ============================================================================
-- 3. horsies_task_attempts: create table
-- ============================================================================

CREATE TABLE IF NOT EXISTS horsies_task_attempts (
    id BIGSERIAL PRIMARY KEY,
    task_id VARCHAR(36) NOT NULL REFERENCES horsies_tasks(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    will_retry BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    error_code TEXT,
    error_message TEXT,
    failed_reason TEXT,
    worker_id VARCHAR(255),
    worker_hostname VARCHAR(255),
    worker_pid INTEGER,
    worker_process_name VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_horsies_task_attempts_task_attempt UNIQUE (task_id, attempt),
    CONSTRAINT ck_horsies_task_attempts_outcome
        CHECK (outcome IN ('COMPLETED', 'FAILED', 'WORKER_FAILURE'))
);

CREATE INDEX IF NOT EXISTS idx_horsies_task_attempts_error_code
    ON horsies_task_attempts (error_code)
    WHERE error_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_horsies_task_attempts_finished_at_desc
    ON horsies_task_attempts (finished_at DESC);

-- ============================================================================
-- 4. horsies_workflow_tasks: drop divergent columns
-- ============================================================================

ALTER TABLE horsies_workflow_tasks DROP COLUMN IF EXISTS sub_workflow_retry_mode;
ALTER TABLE horsies_workflow_tasks DROP COLUMN IF EXISTS good_until;
ALTER TABLE horsies_workflow_tasks DROP COLUMN IF EXISTS has_run_when;
ALTER TABLE horsies_workflow_tasks DROP COLUMN IF EXISTS has_skip_when;

-- ============================================================================
-- 5. Fix task notification trigger: add CANCELLED to terminal statuses
-- ============================================================================

CREATE OR REPLACE FUNCTION horsies_notify_task_changes()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.status = 'PENDING' THEN
        PERFORM pg_notify('task_new', NEW.id);
        PERFORM pg_notify('task_queue_' || NEW.queue_name, NEW.id);
    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
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

-- ============================================================================
-- 6. Heartbeat index: replace two separate indexes with optimal composite
-- ============================================================================

DROP INDEX IF EXISTS idx_horsies_heartbeats_task_role;
DROP INDEX IF EXISTS idx_horsies_heartbeats_sent_at;
CREATE INDEX IF NOT EXISTS idx_horsies_heartbeats_task_role_sent
    ON horsies_heartbeats (task_id, role, sent_at DESC);

-- ============================================================================
-- 7. horsies_schedule_state: drop extra columns not in Python spec
-- ============================================================================

ALTER TABLE horsies_schedule_state DROP COLUMN IF EXISTS task_name;
ALTER TABLE horsies_schedule_state DROP COLUMN IF EXISTS created_at;
