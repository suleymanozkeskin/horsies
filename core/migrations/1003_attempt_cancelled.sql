-- horsies schema migration 1003: the cancelled attempt of a running task, and the cancel notice to its worker.
--
-- The install applies this text after migration 1002 (core/migrations/1002_workflow_start_command_fingerprint.sql),
-- in one READ COMMITTED transaction with its bookkeeping row. A database that applied an older text of migration 1003
-- is refused by the checksum.
--
-- Changes:
-- - horsies_task_attempts: the attempt outcome CANCELLED (never with will_retry), and the phase record writer CANCEL.
-- - horsies_cancel_task: the cancel of a RUNNING task (include_running) writes the CANCELLED attempt row with its
--   phase record before the move, so the attempt archive of the history row holds the run, and sends the cancel
--   notice pg_notify('horsies_task_cancel', ...) to the worker that runs it. PENDING and CLAIMED cancels do not
--   change: no attempt row and no cancel notice.
-- - horsies_cancelled_runs: the read of a worker that may have missed cancel notices (a listener gap, a full payload
--   queue, a runner beat that did not land): which of the runs that it holds an operator cancelled.
--
-- Rows written before this migration do not change, and the migration writes no row.
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control. A function whose argument list changes is dropped and created again; a function
-- whose argument list stays is replaced in place. The comment above each function of this file is its contract from
-- this migration on; the comment in core/baseline/ or in an earlier migration is the contract before it.
--
-- Locks: the ALTER TABLE statements take ACCESS EXCLUSIVE on horsies_task_attempts until the migration commits; each
-- new CHECK reads the table once. The table holds the attempts of live rows only (a move deletes them), so the read is
-- short.

------------------------------------------------------------------------------------------------------------------------
-- Attempt outcome and phase record writer (horsies_task_attempts)
------------------------------------------------------------------------------------------------------------------------

-- outcome CANCELLED: an operator cancelled the task while the attempt ran (horsies_cancel_task with include_running).
-- The attempt ends with the task, so it never has will_retry.
ALTER TABLE horsies_task_attempts DROP CONSTRAINT horsies_task_attempts_outcome_known;
ALTER TABLE horsies_task_attempts
    ADD CONSTRAINT horsies_task_attempts_outcome_known CHECK (
        outcome IN ('FAILED', 'WORKER_FAILURE') OR (outcome IN ('COMPLETED', 'CANCELLED') AND NOT will_retry)
    );

-- recorded_by CANCEL: the cancel statement wrote the attempt row (horsies_cancel_task). As WORKER, it has no
-- last_seen_at; as RECOVERY, it observed no placement (placed_at NULL). The other rules of migration 1001 stay.
ALTER TABLE horsies_task_attempts DROP CONSTRAINT horsies_task_attempts_phase_record_shape;
ALTER TABLE horsies_task_attempts
    -- Each arm names IS NOT NULL: without it a NULL makes the arm NULL and the CHECK passes.
    ADD CONSTRAINT horsies_task_attempts_phase_record_shape CHECK (
        (
            phase_record_version IS NULL
            AND enqueued_at IS NULL AND claimed_at IS NULL AND attempt_claims IS NULL
            AND first_claimed_at IS NULL AND placed_at IS NULL AND finalizing_at IS NULL
            AND recorded_by IS NULL AND last_seen_at IS NULL
        )
        OR (
            phase_record_version IS NOT NULL AND phase_record_version = 1
            AND enqueued_at IS NOT NULL
            AND claimed_at IS NOT NULL
            AND (attempt_claims IS NULL) = (first_claimed_at IS NULL)
            AND (attempt_claims IS NULL OR attempt_claims >= 1)
            AND recorded_by IS NOT NULL
            AND recorded_by IN ('WORKER', 'RECOVERY', 'CANCEL')
            AND (recorded_by = 'RECOVERY') = (last_seen_at IS NOT NULL)
            AND (recorded_by = 'WORKER' OR placed_at IS NULL)
        )
    );

------------------------------------------------------------------------------------------------------------------------
-- Cancel from monitoring
------------------------------------------------------------------------------------------------------------------------

-- horsies_cancel_task: the administrative cancel of one plain task.
--
-- Guarantee:
-- - A plain task that is PENDING or CLAIMED (RUNNING only when p_include_running is TRUE) moves to history as
--   CANCELLED (CANCEL_ADMIN, error code TASK_CANCELLED, 'Cancelled via monitoring API'), with no result. task_done
--   and horsies_task_status fire (the move). A later finalize of the row is a conflict.
-- - PENDING and CLAIMED: no attempt row (no attempt ran: a CLAIMED child stops at its start write with CLAIM_LOST).
-- - RUNNING: before the move, one attempt row for attempt retry_count + 1: outcome CANCELLED, will_retry FALSE,
--   started_at of the row (the terminal time when the row has none), finished_at = the terminal time of the move,
--   error code TASK_CANCELLED, error message 'Cancelled via monitoring API', no failed reason, the worker columns of
--   the row, and the phase record (version 1) of the locked row: enqueued_at, claimed_at, attempt_claims,
--   first_claimed_at, finalizing_at; recorded_by CANCEL; placed_at NULL (the cancel observed no placement);
--   last_seen_at NULL. The move archives it (attempt archive version 2; the open attempt is null) and deletes it.
--   Then the cancel notice pg_notify('horsies_task_cancel', '<task id> <claimed_at>'), with claimed_at as epoch
--   microseconds (floor, decimal integer): the claim generation of the run. PostgreSQL delivers it at commit, so a
--   cancel that rolls back sends nothing.
-- - A workflow task is refused and stays as it is, live or finished: SOURCE_STATE_CONFLICT with guard WORKFLOW_TASK
--   and its status.
-- - A live plain task in a status the caller did not permit: SOURCE_STATE_CONFLICT with its status (guard NULL).
-- - A task absent from the live table is classified from history: an earlier admin cancel is ALREADY_APPLIED
--   (status CANCELLED); another end is SOURCE_STATE_CONFLICT with guard FOREIGN_TERMINALIZATION and its status; no
--   history row is TASK_ABSENT.
-- - Every refusal and every replay writes nothing and sends no cancel notice.
-- Refused (22023): a NULL task or running choice. The attempt insert raises 23505 (unique_violation) when the
-- running attempt has a row already, which no writer leaves on a RUNNING row: nothing is written then.
-- Locks: the task advisory lock, then the whole row by id (FOR UPDATE), the row that the move takes.
-- Returns one row of the shared outcome columns; observed_status is the status the cancel saw.
-- Python 0.7.0 (`v0.7.0` package): cancel_task (monitoring/task_actions.py:326-431) and _diagnose_live_miss (:290-323),
-- horsies_cancel_locked_task (core/history/terminalization/move.py:2046-2096).
CREATE OR REPLACE FUNCTION horsies_cancel_task(p_task_id uuid, p_include_running boolean) RETURNS TABLE(
    task_id uuid,
    ordinality bigint,
    outcome text,
    terminal_at timestamp with time zone,
    terminalization_kind text,
    observed_status text,
    observed_worker_id text,
    observed_claimed_at timestamp with time zone,
    guard_kind text,
    observed_guard jsonb
)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_task record;
    v_row horsies_tasks;
    v_provenance record;
    v_terminal_at timestamptz;
BEGIN
    IF num_nulls(p_task_id, p_include_running) <> 0 THEN
        RAISE EXCEPTION 'horsies_cancel_task: the task and the running choice are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at, t.is_workflow_task, t AS task
    INTO v_task
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF NOT FOUND THEN
        SELECT p.* INTO v_provenance FROM horsies_task_provenance_staged(p_task_id) AS p;
        IF NOT FOUND THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'TASK_ABSENT'::text, NULL::timestamptz, NULL::text,
                NULL::text, NULL::text, NULL::timestamptz, NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint,
            CASE
                WHEN NOT v_provenance.is_workflow_task AND v_provenance.terminalization_kind = 'CANCEL_ADMIN'
                THEN 'ALREADY_APPLIED'
                ELSE 'SOURCE_STATE_CONFLICT'
            END::text,
            v_provenance.terminal_at, v_provenance.terminalization_kind::text,
            v_provenance.status::text, NULL::text, NULL::timestamptz,
            CASE
                WHEN v_provenance.is_workflow_task THEN 'WORKFLOW_TASK'
                WHEN v_provenance.terminalization_kind = 'CANCEL_ADMIN' THEN NULL
                ELSE 'FOREIGN_TERMINALIZATION'
            END::text,
            NULL::jsonb;
        RETURN;
    END IF;

    IF v_task.is_workflow_task THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text, NULL::timestamptz, NULL::text,
            v_task.status::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, 'WORKFLOW_TASK'::text,
            NULL::jsonb;
        RETURN;
    END IF;
    IF NOT (v_task.status IN ('PENDING', 'CLAIMED') OR (p_include_running AND v_task.status = 'RUNNING')) THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text, NULL::timestamptz, NULL::text,
            v_task.status::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    v_terminal_at := NOW();
    v_row := v_task.task;
    IF v_row.status = 'RUNNING' THEN
        INSERT INTO horsies_task_attempts (
            task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
            worker_id, worker_hostname, worker_pid, worker_process_name,
            phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at, placed_at,
            finalizing_at, recorded_by, last_seen_at
        )
        VALUES (
            v_row.id, v_row.retry_count + 1, 'CANCELLED', FALSE, COALESCE(v_row.started_at, v_terminal_at),
            v_terminal_at, 'TASK_CANCELLED', 'Cancelled via monitoring API', NULL,
            v_row.claimed_by_worker_id, v_row.worker_hostname, v_row.worker_pid, v_row.worker_process_name,
            -- Phase record version 1, from the locked row. The cancel observed no placement and records no last
            -- sign of life: the attempt ended at the cancel.
            1, v_row.enqueued_at, v_row.claimed_at, v_row.attempt_claims, v_row.first_claimed_at, NULL,
            v_row.finalizing_at, 'CANCEL', NULL
        );
    END IF;
    PERFORM horsies_move_task_to_history(
        v_row, 'CANCELLED', 'CANCEL_ADMIN', v_terminal_at, NULL, 'TASK_CANCELLED', 'Cancelled via monitoring API'
    );
    IF v_row.status = 'RUNNING' THEN
        PERFORM pg_notify(
            'horsies_task_cancel',
            p_task_id::text || ' ' || floor(extract(epoch FROM v_row.claimed_at) * 1000000)::bigint::text
        );
    END IF;
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'CANCEL_ADMIN'::text,
        v_task.status::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, NULL::text, NULL::jsonb;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Cancelled runs of a worker
------------------------------------------------------------------------------------------------------------------------

-- horsies_cancelled_runs(p_task_ids uuid[], p_claimed_ats timestamptz[]): which of the runs that a worker holds an
-- operator cancelled. The pair at each position is one run: a task id and the claim generation (claimed_at) of the
-- claim that runs it.
--
-- Guarantee: returns each distinct input pair (task_id, claimed_at) whose task has no live row and whose history row
-- has the kind CANCEL_ADMIN and the claimed_at of the pair: the cancel statement moved the task while that claim ran
-- it. Not returned: a live task (it runs on, or it finalizes); a task in history at another claim generation, or
-- of another kind (it completed, failed, or another statement ended it); an id that neither table holds. A worker
-- calls it when cancel notices may have been lost (a listener gap, a full payload queue, a runner beat that did not
-- land), with the runs it holds.
-- Arguments: two arrays of the same length, at most 1000 pairs; a longer list is sent in parts. Refused (22023): a
-- NULL array, arrays of different lengths, a NULL element, more than 1000 pairs. Empty arrays return no row.
-- Reads: the live row by horsies_tasks_pkey; for a task with no live row, the published readers
-- horsies_task_provenance_staged (the kind) and, only for a CANCEL_ADMIN row, horsies_task_detail_staged (the claim
-- generation), each through its leaf task_id indexes. Locks nothing; writes nothing.
-- Returns: one row per cancelled pair, in input order.
CREATE FUNCTION horsies_cancelled_runs(
    p_task_ids uuid[],
    p_claimed_ats timestamp with time zone[]
) RETURNS TABLE(task_id uuid, claimed_at timestamp with time zone)
    LANGUAGE plpgsql STABLE
    AS $$
#variable_conflict use_column
DECLARE
    v_count integer;
    v_index integer;
    v_task_id uuid;
    v_claimed_at timestamptz;
    v_kind text;
    v_history_claimed_at timestamptz;
    v_seen text[] := ARRAY[]::text[];
    v_pair text;
BEGIN
    IF p_task_ids IS NULL OR p_claimed_ats IS NULL THEN
        RAISE EXCEPTION 'horsies_cancelled_runs: the task ids and the claim generations are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_count := COALESCE(cardinality(p_task_ids), 0);
    IF v_count <> COALESCE(cardinality(p_claimed_ats), 0) THEN
        RAISE EXCEPTION 'horsies_cancelled_runs: % task ids and % claim generations', v_count,
            COALESCE(cardinality(p_claimed_ats), 0)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_count > 1000 THEN
        RAISE EXCEPTION 'horsies_cancelled_runs: % pairs; at most 1000 in one call', v_count
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_task_ids, NULL) IS NOT NULL OR array_position(p_claimed_ats, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'horsies_cancelled_runs: a task id or a claim generation is NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    FOR v_index IN 1..v_count LOOP
        v_task_id := p_task_ids[v_index];
        v_claimed_at := p_claimed_ats[v_index];
        v_pair := v_task_id::text || ' ' || floor(extract(epoch FROM v_claimed_at) * 1000000)::bigint::text;
        CONTINUE WHEN v_pair = ANY (v_seen);
        v_seen := v_seen || v_pair;
        CONTINUE WHEN EXISTS (SELECT 1 FROM horsies_tasks AS t WHERE t.id = v_task_id);
        SELECT p.terminalization_kind INTO v_kind FROM horsies_task_provenance_staged(v_task_id) AS p;
        CONTINUE WHEN NOT FOUND OR v_kind IS DISTINCT FROM 'CANCEL_ADMIN';
        SELECT (d.task_row).claimed_at INTO v_history_claimed_at
        FROM horsies_task_detail_staged(v_task_id) AS d
        WHERE d.location = 'HISTORY';
        CONTINUE WHEN NOT FOUND OR v_history_claimed_at IS DISTINCT FROM v_claimed_at;
        RETURN QUERY SELECT v_task_id, v_claimed_at;
    END LOOP;
END
$$;
