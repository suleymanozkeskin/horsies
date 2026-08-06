-- terminal_at completeness (parity with horsies PR #221 / schema v18):
-- backfill, then a CHECK tying a terminal status to a terminal instant, in
-- that order in one transaction so the constraint's precondition holds when
-- it is enforced.

-- Rows terminalized before the column existed carry no terminal instant. The
-- backfill takes the timestamp the writer did record; the handful of writers
-- that recorded neither fall back to the row's last update, which is the
-- closest bound the row still holds.
DO $$
BEGIN
    -- Once the constraint is validated, no undated terminal row can exist, so
    -- the scan can only find nothing. Every later schema release would still
    -- pay for it, which is why the proof is the guard.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'horsies_tasks'::regclass
          AND conname = 'ck_horsies_tasks_terminal_at_terminal_only'
          AND convalidated
    ) THEN
        UPDATE horsies_tasks
        SET terminal_at =
            COALESCE(completed_at, failed_at, updated_at, created_at)
        WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
          AND terminal_at IS NULL;
    END IF;
END
$$;

-- Terminal exactly when dated. Installed NOT VALID only when absent, and
-- validated separately: on first apply the scan runs under the migration
-- transaction's lock regardless, but a later release neither re-adds nor
-- rescans an already-valid constraint.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'horsies_tasks'::regclass
          AND conname = 'ck_horsies_tasks_terminal_at_terminal_only'
    ) THEN
        ALTER TABLE horsies_tasks
        ADD CONSTRAINT ck_horsies_tasks_terminal_at_terminal_only
        CHECK (
            (status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')) = (terminal_at IS NOT NULL)
        ) NOT VALID;
    END IF;
END
$$;

ALTER TABLE horsies_tasks
VALIDATE CONSTRAINT ck_horsies_tasks_terminal_at_terminal_only;
