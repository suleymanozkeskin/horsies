-- horsies schema migration 1005: a CLAIMED or RUNNING task row holds its claim generation.
--
-- The install applies this text after migration 1004 (core/migrations/1004_worker_profile_name.sql), in one READ
-- COMMITTED transaction with its bookkeeping row. A database that applied an older text of migration 1005 is refused
-- by the checksum.
--
-- Changes:
-- - horsies_tasks: the CHECK horsies_tasks_claim_generation_by_status refuses a CLAIMED or RUNNING row whose claim
--   generation (claimed_at) is NULL.
--
-- The writers of horsies_tasks keep the rule:
-- - horsies_insert_task_rows inserts PENDING rows with no claim (migration 1001).
-- - horsies_claim_candidates sets CLAIMED and claimed_at = now() in one statement (migration 1001).
-- - horsies_start_attempt starts only a CLAIMED row at the non-NULL claim generation p_claimed_at, and does not change
--   claimed_at (core/baseline/0004_lifecycle_functions.sql).
-- - Each writer that clears claimed_at sets PENDING in the same statement: horsies_finish_failed_attempt (release and
--   retry) and horsies_finish_stale_attempt (retry) of migration 1001, horsies_requeue_stale_claims and
--   horsies_release_claims of the baseline, and the requeue of the core (REQUEUE_SQL, core/src/broker/postgres.rs).
-- The attempt writers of migrations 1001 and 1003 copy claimed_at of the live row into the phase record of the attempt
-- row, which requires it (horsies_task_attempts_phase_record_shape). With this CHECK, a row with no claim generation is
-- refused at its own write, not later at the write of its attempt.
--
-- A database that holds a CLAIMED or RUNNING row with claimed_at NULL stops the migration. The migration raises
-- SQLSTATE 23514 with a message that names the table, the rule, the row count and the query that finds the rows. The
-- transaction rolls back, so the database stays at version 1004 and the process does not start. No horsies writer
-- makes such a row, so the migration does not guess the state of the row: the operator corrects or deletes each row,
-- then starts again.
--
-- Rows that keep the rule do not change, and the migration writes no row.
--
-- Rules of the file, as migration 1000: no database default, objects in the current schema, no transaction control.
--
-- Locks: the ALTER TABLE statements take ACCESS EXCLUSIVE on horsies_tasks until the migration commits. The first
-- ALTER TABLE takes the lock before the row check, so the row check and the new CHECK read the same rows. The new
-- CHECK reads the table once. The table holds live rows only.

------------------------------------------------------------------------------------------------------------------------
-- Claim generation by status (horsies_tasks)
------------------------------------------------------------------------------------------------------------------------

-- A constraint of this name with another predicate (one that was added by hand) does not stay: the migration drops it
-- and adds the rule of this file, so after the migration the CHECK has this predicate.
ALTER TABLE horsies_tasks DROP CONSTRAINT IF EXISTS horsies_tasks_claim_generation_by_status;

-- The rows that the new CHECK refuses stop the migration with a message that tells the operator how to find them.
-- Without this check, the ADD CONSTRAINT below fails with 23514 and a message that names no row.
DO $$
DECLARE
    v_rows bigint;
BEGIN
    SELECT count(*) INTO v_rows
    FROM horsies_tasks AS t
    WHERE t.status IN ('CLAIMED', 'RUNNING') AND t.claimed_at IS NULL;
    IF v_rows > 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = 'check_violation',
            MESSAGE = 'horsies schema migration 1005 stopped: horsies_tasks holds CLAIMED or RUNNING rows with no '
                || 'claim generation (claimed_at NULL), which the CHECK horsies_tasks_claim_generation_by_status '
                || 'refuses. Rows: ' || v_rows || '. The migration changed nothing. Find the rows with: '
                || 'SELECT id, status, claimed_by_worker_id FROM horsies_tasks '
                || 'WHERE status IN (''CLAIMED'', ''RUNNING'') AND claimed_at IS NULL. '
                || 'No horsies writer makes such a row. Correct or delete each row, then start again.';
    END IF;
END
$$;

-- claimed_at is the claim generation of the claim that holds the row. A CLAIMED or RUNNING row has it. The CHECK does
-- not read a PENDING row: every writer that sets PENDING clears claimed_at, and this rule does not pin that.
-- status is NOT NULL, so neither arm is NULL.
ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_claim_generation_by_status CHECK (
        status NOT IN ('CLAIMED', 'RUNNING') OR claimed_at IS NOT NULL
    );
