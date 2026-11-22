-- horsies schema migration 1001: the lifecycle record of each attempt.
--
-- The install applies this text after migration 1000 (core/baseline/), in one READ COMMITTED transaction with its
-- bookkeeping row. A database that applied an older text of migration 1001 is refused by the checksum.
--
-- Changes:
-- - horsies_tasks: attempt_claims and first_claimed_at count the claims of the current attempt.
-- - horsies_task_attempts: nine columns keep the phase times of each recorded attempt (the phase record).
-- - The four attempt writers fill the phase record in the statement that writes the attempt row. The three writers
--   of a worker take the placement that the worker observed; the stale recovery writer records the last time the
--   attempt was known alive.
-- - The attempt archive goes to version 2 (horsies_encode_attempt_archive): in each history row that a move writes,
--   and in the live branch of horsies_monitoring_task_detail.
--
-- Rows written before this migration keep NULL in every new column, and the migration writes no value into them:
-- - horsies_tasks.attempt_claims NULL: the claims of the current attempt are not counted. A claim keeps it NULL; the
--   next retry sets it to 0.
-- - horsies_task_attempts.phase_record_version NULL: the attempt row has no phase record.
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control. A function whose argument list changes is dropped and created again; a function
-- whose argument list stays is replaced in place. The comment above each function of this file is its contract from
-- this migration on; the comment in core/baseline/ is the contract of migration 1000.
--
-- Locks: each ALTER TABLE takes ACCESS EXCLUSIVE on its table until the migration commits. The new columns are
-- nullable with no default, so no table is rewritten; each new CHECK reads its table once.

------------------------------------------------------------------------------------------------------------------------
-- Claims of the current attempt (horsies_tasks)
------------------------------------------------------------------------------------------------------------------------

-- attempt_claims: the claims taken for the current attempt (retry_count + 1). 0 before its first claim. A claim adds
-- 1 (horsies_claim_candidates); a release and a stale claim requeue keep it; a retry sets it to 0. NULL: the current
-- attempt began before this migration, so its claims are not counted.
-- first_claimed_at: the time of the first claim of the current attempt (the claimed_at of that claim). NULL when
-- attempt_claims is 0 (no claim yet) or NULL (not counted).
-- No index: the claim update changes indexed columns already.
ALTER TABLE horsies_tasks
    ADD COLUMN attempt_claims integer,
    ADD COLUMN first_claimed_at timestamp with time zone,
    -- Each arm names IS NOT NULL: without it a NULL makes the arm NULL and the CHECK passes.
    ADD CONSTRAINT horsies_tasks_attempt_claims_shape CHECK (
        (attempt_claims IS NULL AND first_claimed_at IS NULL)
        OR (
            attempt_claims IS NOT NULL AND attempt_claims = 0
            AND first_claimed_at IS NULL
            AND status = 'PENDING'
        )
        OR (
            attempt_claims IS NOT NULL AND attempt_claims >= 1
            AND first_claimed_at IS NOT NULL
        )
    );

------------------------------------------------------------------------------------------------------------------------
-- Phase record of an attempt (horsies_task_attempts)
------------------------------------------------------------------------------------------------------------------------

-- The phase record of one attempt, written by the statement that writes the attempt row. Every column is NULL on a
-- row written before this migration (phase_record_version NULL: no phase record).
-- - phase_record_version: 1, the form of this migration.
-- - enqueued_at: the due time of the attempt (the enqueued_at of the live row: the send's due time, or next_retry_at
--   after a retry).
-- - claimed_at: the claim generation that ran the attempt.
-- - attempt_claims, first_claimed_at: the claim count of the attempt (1 or more) and the time of its first claim.
--   Both NULL: the claims of the attempt are not counted (it began before this migration).
-- - placed_at: when a child of the worker took the task: claimed_at + the slot wait that the worker measured on its
--   monotonic clock. NULL: the placement was not observed (always for RECOVERY).
-- - finalizing_at: the finalizing marker of the claim. NULL: no marker.
-- - recorded_by: WORKER (the worker wrote the end) or RECOVERY (stale RUNNING recovery wrote it).
-- - last_seen_at: RECOVERY only: the last time the attempt was known alive (the greatest of started_at,
--   finalizing_at and the newest runner beat of the attempt). NULL for WORKER.
-- started_at and finished_at keep their meaning: finished_at is the time the attempt was recorded.
-- No CHECK orders the times: a step of the database clock can break the order, and a write is never refused for it.
-- No index: the table holds the attempts of live rows only.
ALTER TABLE horsies_task_attempts
    ADD COLUMN phase_record_version smallint,
    ADD COLUMN enqueued_at timestamp with time zone,
    ADD COLUMN claimed_at timestamp with time zone,
    ADD COLUMN attempt_claims integer,
    ADD COLUMN first_claimed_at timestamp with time zone,
    ADD COLUMN placed_at timestamp with time zone,
    ADD COLUMN finalizing_at timestamp with time zone,
    ADD COLUMN recorded_by character varying(8),
    ADD COLUMN last_seen_at timestamp with time zone,
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
            AND recorded_by IN ('WORKER', 'RECOVERY')
            AND (recorded_by = 'RECOVERY') = (last_seen_at IS NOT NULL)
            AND (recorded_by = 'WORKER' OR placed_at IS NULL)
        )
    );

------------------------------------------------------------------------------------------------------------------------
-- Placement of an attempt
------------------------------------------------------------------------------------------------------------------------

-- horsies_require_attempt_placement: internal. Checks the placement pair that a worker attempt writer takes.
--
-- The pair:
-- - PLACED with p_slot_wait_us: a child of the worker took the task p_slot_wait_us microseconds after the worker read
--   its claim result (the slot wait of the AttemptPlaced event). The writer stores placed_at = claimed_at + the wait.
-- - NOT_OBSERVED with a NULL wait: the writer has no placement; placed_at stays NULL.
-- Guarantee: returns for a valid pair. Raises 22023 (invalid_parameter_value), with p_function in the message, for a
-- NULL or unknown kind, a PLACED wait that is NULL or negative, a NOT_OBSERVED wait that is not NULL. Reads nothing;
-- writes nothing.
-- Callers: horsies_complete_task_fused, horsies_complete_tasks_fused (each pair, before the first row),
-- horsies_finish_workflow_success, horsies_finish_failed_attempt.
CREATE FUNCTION horsies_require_attempt_placement(
    p_function text,
    p_placement_kind text,
    p_slot_wait_us bigint
) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    CASE
        WHEN p_placement_kind = 'PLACED' AND p_slot_wait_us IS NOT NULL AND p_slot_wait_us >= 0 THEN
            RETURN;
        WHEN p_placement_kind = 'NOT_OBSERVED' AND p_slot_wait_us IS NULL THEN
            RETURN;
        ELSE
            RAISE EXCEPTION '%: the attempt placement is PLACED with a slot wait of 0 us or more, or NOT_OBSERVED with '
                'no slot wait; got % with slot wait %',
                p_function, COALESCE(p_placement_kind, 'NULL'), COALESCE(p_slot_wait_us::text, 'NULL')
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Attempt archive, version 2
------------------------------------------------------------------------------------------------------------------------

-- The version-1 encoder reads the attempt rows by task id only, so it cannot hold the open attempt of the row.
DROP FUNCTION horsies_encode_task_attempts(uuid);

-- horsies_encode_attempt_archive(p_task horsies_tasks): the attempt archive of one live task, version 2.
--
-- Guarantee: the UTF-8 bytes of one JSON array of two items, [attempts, open], with no space between tokens:
-- - attempts: one array of 21 values per attempt row of the task, in ascending attempt order. Positions 0-11 are the
--   12 values of version 1, in the same order and form: attempt, outcome, will_retry, started_at, finished_at,
--   error_code, error_message, failed_reason, worker_id, worker_hostname, worker_pid, worker_process_name. Then
--   12 phase_record_version, 13 enqueued_at, 14 claimed_at, 15 attempt_claims, 16 first_claimed_at, 17 placed_at,
--   18 finalizing_at, 19 recorded_by, 20 last_seen_at. Times are epoch microseconds (floor); NULL is JSON null.
-- - open: JSON null when the task has an attempt row for attempt retry_count + 1: the current attempt ended with its
--   row. Else [attempt_claims, first_claimed_at, finalizing_at] of p_task: the open attempt, which has no row (a
--   cancel, an expiry or a pause abandon ends it, or it still runs), with null for a NULL column.
-- The Rust codec of the archive (core/src/core/history/archive/attempts.rs) writes the same bytes for the same values.
-- p_task is the live row as the caller read it, so the open record holds the values that the move takes.
-- Reads: the attempt rows of the task (horsies_task_attempts_pkey). Locks nothing; writes nothing. A NULL row gives
-- NULL (STRICT).
-- Callers: horsies_move_task_to_history, horsies_move_tasks_to_history, horsies_monitoring_task_detail (live branch).
CREATE FUNCTION horsies_encode_attempt_archive(p_task horsies_tasks) RETURNS bytea
    LANGUAGE sql STABLE STRICT
    AS $$
    SELECT convert_to(
        '[[' || COALESCE(
            string_agg(
                '[' || to_jsonb(a.attempt)::text || ',' ||
                to_jsonb(a.outcome)::text || ',' ||
                to_jsonb(a.will_retry)::text || ',' ||
                to_jsonb(floor(extract(epoch FROM a.started_at) * 1000000)::bigint)::text || ',' ||
                to_jsonb(floor(extract(epoch FROM a.finished_at) * 1000000)::bigint)::text || ',' ||
                COALESCE(to_jsonb(a.error_code)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.error_message)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.failed_reason)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_id)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_hostname)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_pid)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_process_name)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.phase_record_version)::text, 'null') || ',' ||
                COALESCE(to_jsonb(floor(extract(epoch FROM a.enqueued_at) * 1000000)::bigint)::text, 'null') || ',' ||
                COALESCE(to_jsonb(floor(extract(epoch FROM a.claimed_at) * 1000000)::bigint)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.attempt_claims)::text, 'null') || ',' ||
                COALESCE(to_jsonb(floor(extract(epoch FROM a.first_claimed_at) * 1000000)::bigint)::text, 'null')
                || ',' ||
                COALESCE(to_jsonb(floor(extract(epoch FROM a.placed_at) * 1000000)::bigint)::text, 'null') || ',' ||
                COALESCE(to_jsonb(floor(extract(epoch FROM a.finalizing_at) * 1000000)::bigint)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.recorded_by)::text, 'null') || ',' ||
                COALESCE(to_jsonb(floor(extract(epoch FROM a.last_seen_at) * 1000000)::bigint)::text, 'null') || ']',
                ',' ORDER BY a.attempt
            ),
            ''
        ) || '],' ||
        CASE
            WHEN COALESCE(bool_or(a.attempt = p_task.retry_count + 1), FALSE) THEN 'null'
            ELSE '[' ||
                COALESCE(to_jsonb(p_task.attempt_claims)::text, 'null') || ',' ||
                COALESCE(to_jsonb(floor(extract(epoch FROM p_task.first_claimed_at) * 1000000)::bigint)::text, 'null')
                || ',' ||
                COALESCE(to_jsonb(floor(extract(epoch FROM p_task.finalizing_at) * 1000000)::bigint)::text, 'null')
                || ']'
        END || ']',
        'UTF8'
    )
    FROM horsies_task_attempts AS a
    WHERE a.task_id = p_task.id
$$;

------------------------------------------------------------------------------------------------------------------------
-- Attempt writers whose argument list changes
------------------------------------------------------------------------------------------------------------------------

DROP FUNCTION horsies_complete_tasks_fused(uuid[], timestamp with time zone[], text[], text, boolean);
DROP FUNCTION horsies_complete_task_fused(uuid, text, timestamp with time zone, text, boolean);
DROP FUNCTION horsies_finish_workflow_success(uuid, text, timestamp with time zone, text);
DROP FUNCTION horsies_finish_failed_attempt(
    uuid, text, timestamp with time zone, text, integer, double precision, text, text, text, text, text, text, text,
    text, boolean
);

-- horsies_complete_task_fused: the success of one plain task. Replaces the five-argument function of migration 1000.
--
-- Guarantee:
-- - In one statement: the fence (RUNNING, owner, claim generation), the COMPLETED attempt row (started_at of the
--   row, finished_at from clock_timestamp(), and the phase record of the attempt), the move to history
--   (COMPLETE_FUSED) and, when p_wake is TRUE, the capacity wake pg_notify('task_queue_' || queue_name, 'capacity')
--   with the queue of the locked row.
-- - A refusal writes nothing and is classified by horsies_terminalization_miss: ALREADY_APPLIED (a replay),
--   LOST_CLAIM, SOURCE_STATE_CONFLICT, TASK_ABSENT.
-- - The claim generation is required (22023 when NULL).
-- - The phase record (version 1): enqueued_at, claimed_at, attempt_claims, first_claimed_at and finalizing_at of
--   the locked row; recorded_by WORKER; placed_at = claimed_at + p_slot_wait_us microseconds when p_placement_kind
--   is PLACED, NULL when it is NOT_OBSERVED. Refused (22023, horsies_require_attempt_placement): another kind, a
--   PLACED wait that is NULL or negative, a NOT_OBSERVED wait that is not NULL.
--
-- Locks: the task advisory lock, then the row by id alone (FOR UPDATE; the primary key serves it), then the move.
-- A row that fails the fence stays locked for the miss classifier, which locks it by id too.
-- Returns one row of the shared outcome columns (ordinality NULL).
-- Python 0.7.0 (`v0.7.0` package): horsies_complete_task_fused (core/history/terminalization/move.py:678-763), its call
-- (core/lifecycle/persistence.py:123-133), the plain success (core/worker/finalize.py:1384-1436).
CREATE FUNCTION horsies_complete_task_fused(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone,
    p_result text,
    p_wake boolean,
    p_placement_kind text,
    p_slot_wait_us bigint
) RETURNS TABLE(
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
    v_ctx record;
    v_terminal_at timestamptz;
BEGIN
    IF num_nulls(p_task_id, p_worker_id, p_claimed_at, p_wake) <> 0 THEN
        RAISE EXCEPTION 'horsies_complete_task_fused: the task, worker, claim generation and wake are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM horsies_require_attempt_placement('horsies_complete_task_fused', p_placement_kind, p_slot_wait_us);
    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    -- The whole locked row (task): the move takes it as read here.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.queue_name,
           t.enqueued_at, t.attempt_claims, t.first_claimed_at, t.finalizing_at,
           clock_timestamp() AS db_now, t AS task
    INTO v_ctx
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;
    IF NOT FOUND OR (
        v_ctx.status = 'RUNNING'
        AND v_ctx.claimed_by_worker_id = p_worker_id
        AND v_ctx.claimed_at = p_claimed_at
    ) IS NOT TRUE THEN
        RETURN QUERY
        SELECT m.task_id, m.ordinality, m.outcome, m.terminal_at, m.terminalization_kind, m.observed_status,
               m.observed_worker_id::text, m.observed_claimed_at, m.guard_kind, m.observed_guard
        FROM horsies_terminalization_miss(
            p_task_id, 'COMPLETE_FUSED', 'OWNER_AND_GENERATION', p_worker_id, p_claimed_at
        ) AS m;
        RETURN;
    END IF;

    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name,
        phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at, placed_at, finalizing_at,
        recorded_by, last_seen_at
    )
    VALUES (
        v_ctx.id, v_ctx.retry_count + 1, 'COMPLETED', FALSE, COALESCE(v_ctx.started_at, v_ctx.db_now),
        v_ctx.db_now, NULL, NULL, NULL,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid, v_ctx.worker_process_name,
        -- Phase record version 1, from the locked row and the placement that the worker observed.
        1, v_ctx.enqueued_at, v_ctx.claimed_at, v_ctx.attempt_claims, v_ctx.first_claimed_at,
        CASE WHEN p_placement_kind = 'PLACED' THEN v_ctx.claimed_at + p_slot_wait_us * INTERVAL '1 microsecond' END,
        v_ctx.finalizing_at, 'WORKER', NULL
    )
    ON CONFLICT (task_id, attempt) DO UPDATE SET
        outcome = EXCLUDED.outcome,
        will_retry = EXCLUDED.will_retry,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        error_code = EXCLUDED.error_code,
        error_message = EXCLUDED.error_message,
        failed_reason = EXCLUDED.failed_reason,
        worker_id = EXCLUDED.worker_id,
        worker_hostname = EXCLUDED.worker_hostname,
        worker_pid = EXCLUDED.worker_pid,
        worker_process_name = EXCLUDED.worker_process_name,
        phase_record_version = EXCLUDED.phase_record_version,
        enqueued_at = EXCLUDED.enqueued_at,
        claimed_at = EXCLUDED.claimed_at,
        attempt_claims = EXCLUDED.attempt_claims,
        first_claimed_at = EXCLUDED.first_claimed_at,
        placed_at = EXCLUDED.placed_at,
        finalizing_at = EXCLUDED.finalizing_at,
        recorded_by = EXCLUDED.recorded_by,
        last_seen_at = EXCLUDED.last_seen_at;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        v_ctx.task, 'COMPLETED', 'COMPLETE_FUSED', v_terminal_at, p_result, NULL, NULL
    );
    IF p_wake THEN
        PERFORM pg_notify('task_queue_' || v_ctx.queue_name, 'capacity');
    END IF;
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'COMPLETE_FUSED'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb;
END
$$;

-- horsies_complete_tasks_fused: the plain successes of one worker, up to 64, in one statement. Replaces the
-- five-argument function of migration 1000.
--
-- Guarantee: each row keeps its own fence and outcome: the function calls horsies_complete_task_fused once per row,
-- in task id order. PostgreSQL delivers one notification per channel and payload in a transaction, so the rows of
-- one queue send one capacity wake. Refused (22023) before any row runs: a NULL array, an empty batch, more than 64
-- rows, arrays of different lengths, a NULL or repeated task id, a NULL claim generation, a NULL worker or wake, a
-- placement pair that horsies_require_attempt_placement refuses. Each row passes its own placement pair
-- (p_placement_kinds, p_slot_waits_us, at the position of the row) to horsies_complete_task_fused.
--
-- Locks: per row in task id order, the task advisory lock, then the row lock, then the move. A holder of another
-- row of the batch takes one task, takes rows in ascending id order, or never waits, so no lock cycle can form.
-- Returns one row per input row, with ordinality = its 1-based position in the input arrays.
-- Python 0.7.0 (`v0.7.0` package): horsies_complete_tasks_fused (core/history/terminalization/move.py:764-843), its
-- call (core/lifecycle/persistence.py:134-147, :627-671), the batcher (core/worker/finalize.py:1375-1436).
CREATE FUNCTION horsies_complete_tasks_fused(
    p_task_ids uuid[],
    p_claimed_ats timestamp with time zone[],
    p_results text[],
    p_worker_id text,
    p_wake boolean,
    p_placement_kinds text[],
    p_slot_waits_us bigint[]
) RETURNS TABLE(
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
    v_item record;
BEGIN
    IF num_nulls(p_task_ids, p_claimed_ats, p_results, p_worker_id, p_wake, p_placement_kinds, p_slot_waits_us) <> 0
    THEN
        RAISE EXCEPTION 'horsies_complete_tasks_fused: batch arrays, worker and wake must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_task_ids) = 0 OR cardinality(p_task_ids) > 64 THEN
        RAISE EXCEPTION 'horsies_complete_tasks_fused: a batch holds 1 to 64 rows'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_claimed_ats) <> cardinality(p_task_ids)
       OR cardinality(p_results) <> cardinality(p_task_ids)
       OR cardinality(p_placement_kinds) <> cardinality(p_task_ids)
       OR cardinality(p_slot_waits_us) <> cardinality(p_task_ids) THEN
        RAISE EXCEPTION 'horsies_complete_tasks_fused: batch array lengths differ'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- count(DISTINCT) skips NULL, so a NULL id fails this check too.
    IF cardinality(p_task_ids) <> (SELECT count(DISTINCT u.id) FROM unnest(p_task_ids) AS u(id)) THEN
        RAISE EXCEPTION 'horsies_complete_tasks_fused: batch task ids must be distinct and non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_claimed_ats, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'horsies_complete_tasks_fused: every row needs its claim generation'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- Every placement pair is checked before the first row runs.
    PERFORM horsies_require_attempt_placement('horsies_complete_tasks_fused', p.kind, p.wait_us)
    FROM unnest(p_placement_kinds, p_slot_waits_us) AS p(kind, wait_us);

    FOR v_item IN
        SELECT u.id, u.claimed_at, u.result, u.placement_kind, u.slot_wait_us, u.position
        FROM unnest(p_task_ids, p_claimed_ats, p_results, p_placement_kinds, p_slot_waits_us) WITH ORDINALITY
            AS u(id, claimed_at, result, placement_kind, slot_wait_us, position)
        ORDER BY u.id
    LOOP
        RETURN QUERY
        SELECT c.task_id, v_item.position, c.outcome, c.terminal_at, c.terminalization_kind, c.observed_status,
               c.observed_worker_id, c.observed_claimed_at, c.guard_kind, c.observed_guard
        FROM horsies_complete_task_fused(
            v_item.id, p_worker_id, v_item.claimed_at, v_item.result, p_wake, v_item.placement_kind,
            v_item.slot_wait_us
        ) AS c;
    END LOOP;
END
$$;

-- horsies_finish_workflow_success: the success of one workflow task (phase 1). Replaces the four-argument function
-- of migration 1000.
--
-- Guarantee: in one statement: the fence (RUNNING, owner, claim generation), the COMPLETED attempt row, the move
-- (COMPLETE_LOCKED), which records the phase-2 work, then node_has_dependents and downstream_subworkflow. No
-- capacity wake: phase 2 sends it.
-- A refusal writes nothing and is classified by horsies_terminalization_miss. Refused (22023): a NULL claim
-- generation, a plain task. A NULL result is refused with 23502 (not_null_violation), as the move requires one.
-- The attempt row carries the phase record of horsies_complete_task_fused: the same columns from the locked row,
-- recorded_by WORKER, and placed_at from p_placement_kind and p_slot_wait_us (refused as there, 22023).
--
-- Locks: the task advisory lock, then the row by id alone (FOR UPDATE), then the node row (in the move).
-- Returns one row: the shared outcome columns, queue_name and task_name (APPLIED only), node_has_dependents and
-- downstream_subworkflow (APPLIED only; NULL on every other outcome).
-- Python 0.7.0 (`v0.7.0` package): horsies_finish_workflow_success (core/history/terminalization/move.py:1285-1392),
-- its call (core/worker/finalize.py:1322-1372).
CREATE FUNCTION horsies_finish_workflow_success(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone,
    p_result text,
    p_placement_kind text,
    p_slot_wait_us bigint
) RETURNS TABLE(
    task_id uuid,
    ordinality bigint,
    outcome text,
    terminal_at timestamp with time zone,
    terminalization_kind text,
    observed_status text,
    observed_worker_id text,
    observed_claimed_at timestamp with time zone,
    guard_kind text,
    observed_guard jsonb,
    queue_name text,
    task_name text,
    node_has_dependents boolean,
    downstream_subworkflow text
)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_ctx record;
    v_terminal_at timestamptz;
BEGIN
    IF num_nulls(p_task_id, p_worker_id, p_claimed_at) <> 0 THEN
        RAISE EXCEPTION 'horsies_finish_workflow_success: the task, worker and claim generation are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_result IS NULL THEN
        RAISE EXCEPTION 'horsies_finish_workflow_success: a workflow task success needs a result payload'
            USING ERRCODE = 'not_null_violation';
    END IF;
    PERFORM horsies_require_attempt_placement('horsies_finish_workflow_success', p_placement_kind, p_slot_wait_us);
    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    -- The whole locked row (task): the move takes it as read here.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.is_workflow_task, t.queue_name, t.task_name,
           t.enqueued_at, t.attempt_claims, t.first_claimed_at, t.finalizing_at,
           clock_timestamp() AS db_now, t AS task
    INTO v_ctx
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;
    IF NOT FOUND OR (
        v_ctx.status = 'RUNNING'
        AND v_ctx.claimed_by_worker_id = p_worker_id
        AND v_ctx.claimed_at = p_claimed_at
    ) IS NOT TRUE THEN
        RETURN QUERY
        SELECT m.task_id, m.ordinality, m.outcome, m.terminal_at, m.terminalization_kind, m.observed_status,
               m.observed_worker_id::text, m.observed_claimed_at, m.guard_kind, m.observed_guard,
               NULL::text, NULL::text, NULL::boolean, NULL::text
        FROM horsies_terminalization_miss(
            p_task_id, 'COMPLETE_LOCKED', 'OWNER_AND_GENERATION', p_worker_id, p_claimed_at
        ) AS m;
        RETURN;
    END IF;
    IF NOT v_ctx.is_workflow_task THEN
        RAISE EXCEPTION
            'horsies_finish_workflow_success: the task is not a workflow task; its success is the fused completion'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name,
        phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at, placed_at, finalizing_at,
        recorded_by, last_seen_at
    )
    VALUES (
        v_ctx.id, v_ctx.retry_count + 1, 'COMPLETED', FALSE, COALESCE(v_ctx.started_at, v_ctx.db_now),
        v_ctx.db_now, NULL, NULL, NULL,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid, v_ctx.worker_process_name,
        -- Phase record version 1, from the locked row and the placement that the worker observed.
        1, v_ctx.enqueued_at, v_ctx.claimed_at, v_ctx.attempt_claims, v_ctx.first_claimed_at,
        CASE WHEN p_placement_kind = 'PLACED' THEN v_ctx.claimed_at + p_slot_wait_us * INTERVAL '1 microsecond' END,
        v_ctx.finalizing_at, 'WORKER', NULL
    )
    ON CONFLICT (task_id, attempt) DO UPDATE SET
        outcome = EXCLUDED.outcome,
        will_retry = EXCLUDED.will_retry,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        error_code = EXCLUDED.error_code,
        error_message = EXCLUDED.error_message,
        failed_reason = EXCLUDED.failed_reason,
        worker_id = EXCLUDED.worker_id,
        worker_hostname = EXCLUDED.worker_hostname,
        worker_pid = EXCLUDED.worker_pid,
        worker_process_name = EXCLUDED.worker_process_name,
        phase_record_version = EXCLUDED.phase_record_version,
        enqueued_at = EXCLUDED.enqueued_at,
        claimed_at = EXCLUDED.claimed_at,
        attempt_claims = EXCLUDED.attempt_claims,
        first_claimed_at = EXCLUDED.first_claimed_at,
        placed_at = EXCLUDED.placed_at,
        finalizing_at = EXCLUDED.finalizing_at,
        recorded_by = EXCLUDED.recorded_by,
        last_seen_at = EXCLUDED.last_seen_at;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        v_ctx.task, 'COMPLETED', 'COMPLETE_LOCKED', v_terminal_at, p_result, NULL, NULL
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'COMPLETE_LOCKED'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb,
        v_ctx.queue_name::text, v_ctx.task_name::text, horsies_node_has_dependents(p_task_id),
        horsies_node_downstream_subworkflow(p_task_id);
END
$$;

-- horsies_finish_failed_attempt: one failed attempt of a task this worker holds: retry, terminal failure, or the
-- release of a row whose child never started it. Replaces the fifteen-argument function of migration 1000.
--
-- Guarantee:
-- - No read before it: retry_count, max_retries and task_options cannot change inside one claim generation (only
--   the retry writers change retry_count, and both end the generation), so the caller computes the retry request
--   from the claimed row (horsies_claim returns them).
-- - p_claimed_row_rule RELEASE (the child was killed before it reported): a row that is still CLAIMED by this worker
--   at this claim generation goes back to PENDING with every claim and run field cleared and no attempt row
--   (RELEASED); the claim count of the attempt (attempt_claims, first_claimed_at) is kept, so the next claim counts
--   on the same attempt. REFUSE (the child reported a result): the row must be RUNNING.
-- - Fence: RUNNING, owner, claim generation, and, with a retry request, the retry count that the request used. A
--   refusal writes nothing and is classified by horsies_terminalization_miss (FAIL_RUNNING).
-- - The attempt row (p_attempt_outcome FAILED or WORKER_FAILURE) is written with will_retry and with the phase record
--   of horsies_complete_task_fused (placed_at from p_placement_kind and p_slot_wait_us; the release ignores both).
-- - Retry: next_retry_at = clock_timestamp() + the delay; refused when it would be at or after good_until, judged
--   under the lock (the failure is then terminal with retry_disposition PAST_DEADLINE). The row goes back to PENDING
--   with every claim and run field cleared, retry_count + 1, enqueued_at = next_retry_at, attempt_claims 0 and
--   first_claimed_at NULL (the next attempt has no claim yet), and execution ISOLATED when the placement is
--   ISOLATED. The delayed retry wake is the caller's, at next_retry_at.
-- - Terminal: the move (FAIL_RUNNING) and, for a plain task with p_wake TRUE, the capacity wake
--   pg_notify('task_queue_' || queue_name, 'capacity'), in the same statement. A workflow task gets
--   node_has_dependents and downstream_subworkflow; its phase 2 sends the wake.
--
-- Arguments: p_retry_kind NONE | RETRY (p_expected_retry_count, p_retry_delay_s finite and >= 0, p_retry_placement
-- SAME_CLASS | ISOLATED; NULL for NONE). p_result, the task's error code and failed reason are the terminal values;
-- p_attempt_message and p_attempt_reason go to the attempt row. Refused (22023): a NULL task, worker, claim
-- generation, rule or wake; a retry slot that does not fit its kind; an unknown placement, rule or attempt outcome;
-- an attempt placement pair that horsies_require_attempt_placement refuses.
-- A NULL result is refused with 23502 (not_null_violation).
--
-- Locks: the task advisory lock, then the row by id alone (FOR UPDATE), then (the move of a workflow task) the node.
-- Returns one row: the shared outcome columns (outcome APPLIED, RETRY_SCHEDULED, RELEASED, or a miss outcome),
-- retry_disposition (APPLIED: NOT_REQUESTED or PAST_DEADLINE), retry_count and next_retry_at (RETRY_SCHEDULED),
-- good_until (PAST_DEADLINE), is_workflow_task, queue_name, task_name (APPLIED; queue_name also RETRY_SCHEDULED and
-- RELEASED), node_has_dependents and downstream_subworkflow (APPLIED of a workflow task; else NULL).
-- Python 0.7.0 (`v0.7.0` package): horsies_finish_failed_attempt (core/history/terminalization/move.py:1045-1262),
-- READ_IN_FLIGHT_ATTEMPT_SQL (core/worker/sql.py:306-314) and its read (core/worker/failed_attempt.py:101-130), the
-- call (core/worker/failed_attempt.py:298-330, core/worker/finalize.py:1467-1523), the killed-child path
-- (core/worker/dispatch.py:357-410) with UNCLAIM_CLAIMED_TASK_SQL (core/worker/sql.py:237-255).
CREATE FUNCTION horsies_finish_failed_attempt(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone,
    p_retry_kind text,
    p_expected_retry_count integer,
    p_retry_delay_s double precision,
    p_retry_placement text,
    p_result text,
    p_error_code text,
    p_failed_reason text,
    p_attempt_outcome text,
    p_attempt_message text,
    p_attempt_reason text,
    p_claimed_row_rule text,
    p_wake boolean,
    p_placement_kind text,
    p_slot_wait_us bigint
) RETURNS TABLE(
    task_id uuid,
    ordinality bigint,
    outcome text,
    terminal_at timestamp with time zone,
    terminalization_kind text,
    observed_status text,
    observed_worker_id text,
    observed_claimed_at timestamp with time zone,
    guard_kind text,
    observed_guard jsonb,
    retry_disposition text,
    retry_count integer,
    next_retry_at timestamp with time zone,
    good_until timestamp with time zone,
    is_workflow_task boolean,
    queue_name text,
    task_name text,
    node_has_dependents boolean,
    downstream_subworkflow text
)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_ctx record;
    v_retry boolean;
    v_will_retry boolean := FALSE;
    v_retry_disposition text;
    v_next_retry_at timestamptz;
    v_good_until timestamptz;
    v_terminal_at timestamptz;
BEGIN
    IF num_nulls(p_task_id, p_worker_id, p_claimed_at, p_retry_kind, p_claimed_row_rule, p_wake) <> 0 THEN
        RAISE EXCEPTION
            'horsies_finish_failed_attempt: the task, worker, claim generation, retry kind, rule and wake are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    CASE p_retry_kind
        WHEN 'NONE' THEN
            IF num_nulls(p_expected_retry_count, p_retry_delay_s, p_retry_placement) <> 3 THEN
                RAISE EXCEPTION 'horsies_finish_failed_attempt: no retry takes no retry count, delay or placement'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_retry := FALSE;
        WHEN 'RETRY' THEN
            IF num_nulls(p_expected_retry_count, p_retry_delay_s, p_retry_placement) <> 0 THEN
                RAISE EXCEPTION
                    'horsies_finish_failed_attempt: a retry needs the retry count, the delay and the placement'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            IF NOT (p_retry_delay_s >= 0 AND p_retry_delay_s < 'Infinity'::double precision) THEN
                RAISE EXCEPTION 'horsies_finish_failed_attempt: the retry delay is not a finite, non-negative value'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            IF p_retry_placement NOT IN ('SAME_CLASS', 'ISOLATED') THEN
                RAISE EXCEPTION 'horsies_finish_failed_attempt: unknown retry placement'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_retry := TRUE;
        ELSE
            RAISE EXCEPTION 'horsies_finish_failed_attempt: unknown retry kind'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    IF p_attempt_outcome IS NULL OR p_attempt_outcome NOT IN ('FAILED', 'WORKER_FAILURE') THEN
        RAISE EXCEPTION 'horsies_finish_failed_attempt: the attempt outcome is not a failure'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_claimed_row_rule NOT IN ('REFUSE', 'RELEASE') THEN
        RAISE EXCEPTION 'horsies_finish_failed_attempt: unknown claimed-row rule'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_result IS NULL THEN
        RAISE EXCEPTION 'horsies_finish_failed_attempt: a failed attempt needs a result payload'
            USING ERRCODE = 'not_null_violation';
    END IF;
    PERFORM horsies_require_attempt_placement('horsies_finish_failed_attempt', p_placement_kind, p_slot_wait_us);

    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    -- The whole locked row (task): the move takes it as read here.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.good_until, t.is_workflow_task,
           t.queue_name, t.task_name, t.enqueued_at, t.attempt_claims, t.first_claimed_at, t.finalizing_at,
           clock_timestamp() AS db_now, t AS task
    INTO v_ctx
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    -- A killed child that never started the task: the row goes back to the queue with no attempt.
    IF FOUND
       AND p_claimed_row_rule = 'RELEASE'
       AND v_ctx.status = 'CLAIMED'
       AND v_ctx.claimed_by_worker_id = p_worker_id
       AND v_ctx.claimed_at = p_claimed_at THEN
        UPDATE horsies_tasks t
        SET status = 'PENDING',
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            started_at = NULL,
            worker_pid = NULL,
            worker_hostname = NULL,
            worker_process_name = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            updated_at = NOW()
        WHERE t.id = p_task_id;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'RELEASED'::text, NULL::timestamptz, NULL::text,
            'CLAIMED'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb,
            NULL::text, NULL::integer, NULL::timestamptz, NULL::timestamptz,
            NULL::boolean, v_ctx.queue_name::text, NULL::text, NULL::boolean, NULL::text;
        RETURN;
    END IF;

    IF NOT FOUND OR (
        v_ctx.status = 'RUNNING'
        AND v_ctx.claimed_by_worker_id = p_worker_id
        AND v_ctx.claimed_at = p_claimed_at
        AND (NOT v_retry OR v_ctx.retry_count = p_expected_retry_count)
    ) IS NOT TRUE THEN
        RETURN QUERY
        SELECT m.task_id, m.ordinality, m.outcome, m.terminal_at, m.terminalization_kind, m.observed_status,
               m.observed_worker_id::text, m.observed_claimed_at, m.guard_kind, m.observed_guard,
               NULL::text, NULL::integer, NULL::timestamptz, NULL::timestamptz,
               NULL::boolean, NULL::text, NULL::text, NULL::boolean, NULL::text
        FROM horsies_terminalization_miss(
            p_task_id, 'FAIL_RUNNING', 'OWNER_AND_GENERATION', p_worker_id, p_claimed_at
        ) AS m;
        RETURN;
    END IF;

    IF NOT v_retry THEN
        v_retry_disposition := 'NOT_REQUESTED';
    ELSE
        v_next_retry_at := v_ctx.db_now + make_interval(secs => p_retry_delay_s);
        IF v_ctx.good_until IS NULL OR v_next_retry_at < v_ctx.good_until THEN
            v_will_retry := TRUE;
        ELSE
            v_retry_disposition := 'PAST_DEADLINE';
            v_good_until := v_ctx.good_until;
        END IF;
    END IF;

    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name,
        phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at, placed_at, finalizing_at,
        recorded_by, last_seen_at
    )
    VALUES (
        v_ctx.id, v_ctx.retry_count + 1, p_attempt_outcome, v_will_retry, COALESCE(v_ctx.started_at, v_ctx.db_now),
        v_ctx.db_now, p_error_code, p_attempt_message, p_attempt_reason,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid, v_ctx.worker_process_name,
        -- Phase record version 1, from the locked row and the placement that the worker observed.
        1, v_ctx.enqueued_at, v_ctx.claimed_at, v_ctx.attempt_claims, v_ctx.first_claimed_at,
        CASE WHEN p_placement_kind = 'PLACED' THEN v_ctx.claimed_at + p_slot_wait_us * INTERVAL '1 microsecond' END,
        v_ctx.finalizing_at, 'WORKER', NULL
    )
    ON CONFLICT (task_id, attempt) DO UPDATE SET
        outcome = EXCLUDED.outcome,
        will_retry = EXCLUDED.will_retry,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        error_code = EXCLUDED.error_code,
        error_message = EXCLUDED.error_message,
        failed_reason = EXCLUDED.failed_reason,
        worker_id = EXCLUDED.worker_id,
        worker_hostname = EXCLUDED.worker_hostname,
        worker_pid = EXCLUDED.worker_pid,
        worker_process_name = EXCLUDED.worker_process_name,
        phase_record_version = EXCLUDED.phase_record_version,
        enqueued_at = EXCLUDED.enqueued_at,
        claimed_at = EXCLUDED.claimed_at,
        attempt_claims = EXCLUDED.attempt_claims,
        first_claimed_at = EXCLUDED.first_claimed_at,
        placed_at = EXCLUDED.placed_at,
        finalizing_at = EXCLUDED.finalizing_at,
        recorded_by = EXCLUDED.recorded_by,
        last_seen_at = EXCLUDED.last_seen_at;

    IF v_will_retry THEN
        UPDATE horsies_tasks t
        SET status = 'PENDING',
            execution = CASE WHEN p_retry_placement = 'ISOLATED' THEN 'ISOLATED' ELSE t.execution END,
            retry_count = v_ctx.retry_count + 1,
            next_retry_at = v_next_retry_at,
            enqueued_at = v_next_retry_at,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            started_at = NULL,
            worker_pid = NULL,
            worker_hostname = NULL,
            worker_process_name = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            attempt_claims = 0,
            first_claimed_at = NULL,
            updated_at = NOW()
        WHERE t.id = p_task_id;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'RETRY_SCHEDULED'::text, NULL::timestamptz, NULL::text,
            'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb,
            NULL::text, v_ctx.retry_count + 1, v_next_retry_at, NULL::timestamptz,
            NULL::boolean, v_ctx.queue_name::text, NULL::text, NULL::boolean, NULL::text;
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        v_ctx.task, 'FAILED', 'FAIL_RUNNING', v_terminal_at, p_result, p_error_code, p_failed_reason
    );
    IF NOT v_ctx.is_workflow_task AND p_wake THEN
        PERFORM pg_notify('task_queue_' || v_ctx.queue_name, 'capacity');
    END IF;
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'FAIL_RUNNING'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb,
        v_retry_disposition, NULL::integer, v_next_retry_at, v_good_until,
        v_ctx.is_workflow_task, v_ctx.queue_name::text, v_ctx.task_name::text,
        CASE WHEN v_ctx.is_workflow_task THEN horsies_node_has_dependents(p_task_id) END,
        CASE WHEN v_ctx.is_workflow_task THEN horsies_node_downstream_subworkflow(p_task_id) END;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Writers whose argument list stays: the row insert, the claim, the stale recovery
------------------------------------------------------------------------------------------------------------------------

-- horsies_insert_task_rows: internal. The one place that holds the column list of a new task row.
--
-- Guarantee: each row is inserted as PENDING with every column named (retry_count 0; created_at, updated_at
-- p_row_time; every claim and run field NULL; attempt_claims 0 and first_claimed_at NULL: the first attempt has no
-- claim yet), or it is not inserted and its conflict is classified. The conflict
-- read runs in a statement after the insert, never inside it: a lock taken inside the insert would wait for a move
-- that holds the row, and after the move deletes it PostgreSQL would retry the insert and write a second live row
-- with the same id. The read after the insert sees the row gone and answers CONFLICT_UNVERIFIABLE.
--
-- Exact-id check of a conflict: the stored command_fingerprint_version, command_fingerprint and sent_at equal the
-- requested ones. A row with no fingerprint (an engine-minted workflow task) has no command to compare, so its
-- conflict is never SAME_COMMAND. Callers that mint the ids (workflow start, node task insert) treat every outcome
-- other than INSERTED as a defect.
--
-- Arguments: one parallel array per column (built-in types, one element per row). p_trace_carriers and p_trace_ids
-- hold NULL for a row with no trace; the table CHECK horsies_tasks_trace_pair refuses a lone half of the pair and a
-- trace id that is not 16 bytes (23514). p_row_time: the created_at and updated_at of every row. The caller passes
-- the time that the Python writer of that row used: the time of the writing statement (statement_timestamp()) or the
-- start of its transaction (now()). p_conflict_lock: NONE (the conflict read takes no lock) or FOR_UPDATE (it locks
-- the conflicting rows in id order, for a caller that writes the row next). Refused (22023): a NULL array, arrays of
-- different lengths, a NULL element in a NOT NULL column, a repeated id, a NULL p_row_time, an unknown
-- p_conflict_lock.
--
-- Locks: the inserted rows; with FOR_UPDATE also the conflicting rows, in id order.
-- Returns one row per input row: its 1-based position, its id, and INSERTED, SAME_COMMAND, PAYLOAD_MISMATCH or
-- CONFLICT_UNVERIFIABLE.
-- Callers, with the row time each passes: horsies_enqueue_task (statement_timestamp(); FOR_UPDATE for a keyed send,
-- else NONE), the schedule fire (now()), and the node task insert (the time its caller passes; the workflow start
-- and each promotion pass now()).
-- Python: ENQUEUE_INSERT_SQL (core/brokers/postgres.py:906-935), the conflict read of _verify_enqueue_conflict
-- (:2861-2925) and of _bind_key_after_task_id_conflict (:2780-2859).
CREATE OR REPLACE FUNCTION horsies_insert_task_rows(
    p_ids uuid[],
    p_task_names text[],
    p_queue_names text[],
    p_priorities integer[],
    p_inputs text[],
    p_task_options text[],
    p_executions text[],
    p_retention_class_keys text[],
    p_retain_rerun_inputs boolean[],
    p_max_retries integer[],
    p_good_untils timestamp with time zone[],
    p_sent_ats timestamp with time zone[],
    p_enqueued_ats timestamp with time zone[],
    p_is_workflow_tasks boolean[],
    p_fingerprint_versions smallint[],
    p_fingerprints bytea[],
    p_key_digests bytea[],
    p_rerun_of_task_ids uuid[],
    p_rerun_root_task_ids uuid[],
    p_trace_carriers text[],
    p_trace_ids bytea[],
    p_row_time timestamp with time zone,
    p_conflict_lock text
) RETURNS TABLE(ordinality bigint, task_id uuid, outcome text)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_count integer;
    v_inserted uuid[];
    v_conflicts uuid[];
BEGIN
    IF p_conflict_lock IS NULL OR p_conflict_lock NOT IN ('NONE', 'FOR_UPDATE') THEN
        RAISE EXCEPTION 'horsies_insert_task_rows: unknown conflict lock'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_row_time IS NULL THEN
        RAISE EXCEPTION 'horsies_insert_task_rows: the row time must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF num_nulls(
        p_ids, p_task_names, p_queue_names, p_priorities, p_inputs, p_task_options, p_executions,
        p_retention_class_keys, p_retain_rerun_inputs, p_max_retries, p_good_untils, p_sent_ats, p_enqueued_ats,
        p_is_workflow_tasks, p_fingerprint_versions, p_fingerprints, p_key_digests, p_rerun_of_task_ids,
        p_rerun_root_task_ids, p_trace_carriers, p_trace_ids
    ) <> 0 THEN
        RAISE EXCEPTION 'horsies_insert_task_rows: row arrays must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_count := cardinality(p_ids);
    IF cardinality(p_task_names) <> v_count OR cardinality(p_queue_names) <> v_count
       OR cardinality(p_priorities) <> v_count OR cardinality(p_inputs) <> v_count
       OR cardinality(p_task_options) <> v_count OR cardinality(p_executions) <> v_count
       OR cardinality(p_retention_class_keys) <> v_count OR cardinality(p_retain_rerun_inputs) <> v_count
       OR cardinality(p_max_retries) <> v_count OR cardinality(p_good_untils) <> v_count
       OR cardinality(p_sent_ats) <> v_count OR cardinality(p_enqueued_ats) <> v_count
       OR cardinality(p_is_workflow_tasks) <> v_count OR cardinality(p_fingerprint_versions) <> v_count
       OR cardinality(p_fingerprints) <> v_count OR cardinality(p_key_digests) <> v_count
       OR cardinality(p_rerun_of_task_ids) <> v_count OR cardinality(p_rerun_root_task_ids) <> v_count
       OR cardinality(p_trace_carriers) <> v_count OR cardinality(p_trace_ids) <> v_count THEN
        RAISE EXCEPTION 'horsies_insert_task_rows: row array lengths differ'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_task_names, NULL) IS NOT NULL OR array_position(p_queue_names, NULL) IS NOT NULL
       OR array_position(p_priorities, NULL) IS NOT NULL OR array_position(p_inputs, NULL) IS NOT NULL
       OR array_position(p_executions, NULL) IS NOT NULL
       OR array_position(p_retention_class_keys, NULL) IS NOT NULL
       OR array_position(p_retain_rerun_inputs, NULL) IS NOT NULL
       OR array_position(p_max_retries, NULL) IS NOT NULL OR array_position(p_sent_ats, NULL) IS NOT NULL
       OR array_position(p_enqueued_ats, NULL) IS NOT NULL
       OR array_position(p_is_workflow_tasks, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'horsies_insert_task_rows: a required row value is NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- count(DISTINCT) skips NULL, so a NULL id fails this check too.
    IF v_count <> (SELECT count(DISTINCT u.id) FROM unnest(p_ids) AS u(id)) THEN
        RAISE EXCEPTION 'horsies_insert_task_rows: task ids must be distinct and non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_count = 0 THEN
        RETURN;
    END IF;

    WITH inserted AS (
        INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
            created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status,
            enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
            finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name, retry_count, max_retries,
            next_retry_at, good_until, is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
            rerun_root_task_id, trace_carrier, trace_id, attempt_claims, first_claimed_at
        )
        SELECT
            r.id, r.task_name, r.queue_name, r.priority, r.input, r.task_options, r.execution,
            r.retention_class_key, r.sent_at,
            p_row_time, r.fingerprint_version, r.fingerprint, r.key_digest, 'PENDING',
            r.enqueued_at, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 0, r.max_retries,
            NULL, r.good_until, r.is_workflow_task, p_row_time, r.retain_rerun_input, r.rerun_of_task_id,
            r.rerun_root_task_id, r.trace_carrier, r.trace_id, 0, NULL
        FROM unnest(
            p_ids, p_task_names, p_queue_names, p_priorities, p_inputs, p_task_options, p_executions,
            p_retention_class_keys, p_retain_rerun_inputs, p_max_retries, p_good_untils, p_sent_ats,
            p_enqueued_ats, p_is_workflow_tasks, p_fingerprint_versions, p_fingerprints, p_key_digests,
            p_rerun_of_task_ids, p_rerun_root_task_ids, p_trace_carriers, p_trace_ids
        ) AS r(
            id, task_name, queue_name, priority, input, task_options, execution, retention_class_key,
            retain_rerun_input, max_retries, good_until, sent_at, enqueued_at, is_workflow_task,
            fingerprint_version, fingerprint, key_digest, rerun_of_task_id, rerun_root_task_id, trace_carrier,
            trace_id
        )
        ORDER BY r.id
        ON CONFLICT (id) DO NOTHING
        RETURNING horsies_tasks.id
    )
    SELECT COALESCE(array_agg(i.id), ARRAY[]::uuid[]) INTO v_inserted FROM inserted i;

    IF cardinality(v_inserted) = v_count THEN
        RETURN QUERY
        SELECT u.position, u.id, 'INSERTED'::text
        FROM unnest(p_ids) WITH ORDINALITY AS u(id, position)
        ORDER BY u.position;
        RETURN;
    END IF;

    SELECT array_agg(u.id ORDER BY u.id) INTO v_conflicts
    FROM unnest(p_ids) AS u(id)
    WHERE u.id <> ALL (v_inserted);

    -- New statements, so their snapshots see a row whose insert committed while the insert above waited on it.
    -- FOR_UPDATE locks the conflicting rows in id order first; a row that a move deleted meanwhile is skipped by
    -- the lock and absent from the read that follows.
    IF p_conflict_lock = 'FOR_UPDATE' THEN
        PERFORM 1
        FROM horsies_tasks t
        WHERE t.id = ANY (v_conflicts)
        ORDER BY t.id
        FOR UPDATE;
    END IF;

    RETURN QUERY
    WITH requested AS (
        SELECT r.id, r.position, r.fingerprint_version, r.fingerprint, r.sent_at
        FROM unnest(p_ids, p_fingerprint_versions, p_fingerprints, p_sent_ats) WITH ORDINALITY
            AS r(id, fingerprint_version, fingerprint, sent_at, position)
    ),
    stored AS (
        SELECT t.id, t.command_fingerprint_version, t.command_fingerprint, t.sent_at
        FROM horsies_tasks t
        WHERE t.id = ANY (v_conflicts)
    )
    SELECT q.position, q.id,
        CASE
            WHEN q.id = ANY (v_inserted) THEN 'INSERTED'
            WHEN s.id IS NULL THEN 'CONFLICT_UNVERIFIABLE'
            WHEN q.fingerprint IS NULL OR s.command_fingerprint IS NULL THEN 'PAYLOAD_MISMATCH'
            WHEN s.command_fingerprint_version = q.fingerprint_version
                 AND s.command_fingerprint = q.fingerprint
                 AND s.sent_at = q.sent_at THEN 'SAME_COMMAND'
            ELSE 'PAYLOAD_MISMATCH'
        END::text
    FROM requested q
    LEFT JOIN stored s ON s.id = q.id
    ORDER BY q.position;
END
$$;

-- horsies_claim_candidates: internal to horsies_claim.
--
-- Guarantee: locks and claims the rows of the plan of horsies_claim_plan, per planned (queue, class, arm) stream,
-- with FOR UPDATE SKIP LOCKED, and updates them by id: status CLAIMED, claimed_at = now(), the owner, the lease
-- from now(), the finalizing marker cleared. Each claim also counts on the current attempt: attempt_claims + 1, and
-- first_claimed_at = now() on its first claim (attempt_claims was 0). A row whose attempt began before migration
-- 1001 keeps attempt_claims and first_claimed_at NULL (claims not counted).
-- Locks: the row locks of the taken rows only.
-- Returns the CLAIMED rows in claim order and one SHORT row when fewer rows were taken than planned.
-- Python 0.7.0 (`v0.7.0` package): horsies_claim_candidates (core/schemas/migrations.py:1005).
CREATE OR REPLACE FUNCTION horsies_claim_candidates(
    p_total integer,
    p_available jsonb,
    p_routes jsonb,
    p_queues jsonb,
    p_queue_priority jsonb,
    p_queue_max_concurrency jsonb,
    p_queue_counts jsonb,
    p_max_claim_batch integer,
    p_worker_id text,
    p_lease_ms bigint
) RETURNS TABLE(
    row_kind text,
    id uuid,
    task_name text,
    queue_name text,
    input text,
    task_options text,
    execution text,
    is_workflow_task boolean,
    claimed_at timestamp with time zone,
    retry_count integer,
    max_retries integer,
    priority integer,
    enqueued_at timestamp with time zone,
    trace_carrier text,
    trace_id bytea,
    short_by integer
)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
    SET jit TO 'off'
    AS $$
#variable_conflict use_column
DECLARE
    v_queue text[];
    v_class text[];
    v_arm text[];
    v_take int[];
    v_planned int;
    v_taken int;
BEGIN
    SELECT COALESCE(array_agg(p.queue_name), ARRAY[]::text[]),
           COALESCE(array_agg(p.row_class), ARRAY[]::text[]),
           COALESCE(array_agg(p.arm), ARRAY[]::text[]),
           COALESCE(array_agg(p.take), ARRAY[]::int[]),
           COALESCE(sum(p.take), 0)::int
    INTO v_queue, v_class, v_arm, v_take, v_planned
    FROM horsies_claim_plan(
        p_total, p_available, p_routes, p_queues, p_queue_priority,
        p_queue_max_concurrency, p_queue_counts, p_max_claim_batch
    ) AS p;
    IF v_planned = 0 THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH planned AS (
        SELECT p.queue_name, p.row_class, p.arm, p.take
        FROM unnest(v_queue, v_class, v_arm, v_take) AS p(queue_name, row_class, arm, take)
    ),
    -- FOR UPDATE is not allowed on UNION operands, so each arm locks in its own CTE.
    locked_pending AS (
        SELECT l.id
        FROM planned p
        CROSS JOIN LATERAL (
            SELECT t.id
            FROM horsies_tasks t
            WHERE t.queue_name = p.queue_name
              AND t.execution = p.row_class
              AND t.status = 'PENDING'
              AND t.enqueued_at <= now()
              AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
              AND (t.good_until IS NULL OR t.good_until > now())
            ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT p.take
        ) l
        WHERE p.arm = 'PENDING'
    ),
    locked_expired AS (
        SELECT l.id
        FROM planned p
        CROSS JOIN LATERAL (
            SELECT t.id
            FROM horsies_tasks t
            WHERE t.queue_name = p.queue_name
              AND t.execution = p.row_class
              AND t.status = 'CLAIMED'
              AND t.claim_expires_at < now()
              AND t.enqueued_at <= now()
              AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
              AND (t.good_until IS NULL OR t.good_until > now())
            ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT p.take
        ) l
        WHERE p.arm = 'EXPIRED'
    ),
    -- Targets are looked up by id; the selected batch never drives a scan of the live table.
    updated AS (
        UPDATE horsies_tasks t
        SET status = 'CLAIMED',
            claimed_at = now(),
            claimed_by_worker_id = p_worker_id,
            claim_expires_at = now() + p_lease_ms * INTERVAL '1 millisecond',
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            -- NULL + 1 stays NULL: a count that began before migration 1001 stays not counted.
            attempt_claims = t.attempt_claims + 1,
            first_claimed_at = CASE WHEN t.attempt_claims = 0 THEN now() ELSE t.first_claimed_at END,
            updated_at = now()
        WHERE t.id = ANY(ARRAY(
            SELECT locked_pending.id FROM locked_pending
            UNION ALL
            SELECT locked_expired.id FROM locked_expired
        ))
        RETURNING t.id, t.task_name, t.queue_name, t.input, t.task_options, t.execution,
                  t.is_workflow_task, t.claimed_at, t.retry_count, t.max_retries, t.priority, t.enqueued_at,
                  t.trace_carrier, t.trace_id
    )
    SELECT 'CLAIMED'::text, u.id, u.task_name::text, u.queue_name::text, u.input, u.task_options,
           u.execution, u.is_workflow_task, u.claimed_at, u.retry_count, u.max_retries, u.priority, u.enqueued_at,
           u.trace_carrier, u.trace_id, NULL::integer
    FROM updated u
    ORDER BY COALESCE((p_queue_priority ->> u.queue_name)::int, 100) ASC,
             u.priority ASC, u.enqueued_at ASC, u.id ASC;

    GET DIAGNOSTICS v_taken = ROW_COUNT;
    IF v_taken < v_planned THEN
        RETURN QUERY
        SELECT 'SHORT'::text, NULL::uuid, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
               NULL::boolean, NULL::timestamptz, NULL::integer, NULL::integer, NULL::integer, NULL::timestamptz,
               NULL::text, NULL::bytea, v_planned - v_taken;
    END IF;
END;
$$;

-- horsies_finish_stale_attempt: the stale RUNNING recovery of one task: crash retry by policy, or failure.
--
-- p_evidence names why the row is recovered, a closed set (the evidence of horsies_stale_running_scan):
-- - BEAT_AGE (both thresholds, >= 0; no owner and no claim generation): the runner stopped beating.
-- - OWNER_GONE (p_owner_worker_id and p_owner_claimed_at; no threshold): the owner is gone (its liveness lock was
--   free in two reaper reads far enough apart, at one generation, horsies_unheld_worker_owners), in place of the
--   beat age.
--
-- Guarantee:
-- - Judged under the task advisory lock and the row lock, on one capture of the row, its newest runner beat at or
--   after started_at, and NOW(). BEAT_AGE judges that beat only inside the recency bound of
--   horsies_stale_running_scan (the greater threshold); a beat before the bound counts as no beat.
-- - BEAT_AGE: RUNNING, started, no finalizing marker newer than p_finalizing_stale_after_ms, no runner beat of this
--   attempt newer than p_stale_after_ms (started_at when it has none). A fresh row is SOURCE_STATE_CONFLICT with
--   guard STALENESS and its evidence, whose last_heartbeat_at is NULL until the first runner beat of the attempt; a
--   row that is not RUNNING is classified by horsies_terminalization_miss (FAIL_STALE, fence NONE).
-- - OWNER_GONE: RUNNING and started under p_owner_worker_id at the claim generation p_owner_claimed_at, whatever its
--   beats and its finalizing marker. Any other row is classified by horsies_terminalization_miss (FAIL_STALE, fence
--   OWNER_AND_GENERATION): another owner or generation is LOST_CLAIM.
-- - A refusal writes nothing.
-- - A retry request carries the retry count it was computed from; a row whose count differs is
--   SOURCE_STATE_CONFLICT with guard RETRY_COUNT and writes nothing (the next pass decides again).
-- - One FAILED attempt with the caller's error code (WORKER_CRASHED) is written with will_retry and the phase record
--   (version 1) of the locked row: enqueued_at, claimed_at, attempt_claims, first_claimed_at, finalizing_at;
--   recorded_by RECOVERY; placed_at NULL (recovery observed no placement); last_seen_at = the greatest of
--   started_at, finalizing_at and the newest runner beat at or after started_at (both evidences), the last time the
--   attempt was known alive.
-- - Retry: next_retry_at = clock_timestamp() + the delay, refused at or after good_until under the lock (then the
--   failure with retry_disposition PAST_DEADLINE). The row goes back to PENDING with every claim and run field
--   cleared, retry_count + 1, enqueued_at = next_retry_at, attempt_claims 0 and first_claimed_at NULL; the execution
--   class is kept. The delayed retry wake is the caller's, at next_retry_at.
-- - Failure: the move (FAIL_STALE). No capacity wake, as Python. A workflow task gets node_has_dependents and
--   downstream_subworkflow; phase-2 recovery runs its phase 2.
--
-- Arguments: the evidence and its arguments (thresholds in ms, >= 0); p_retry_kind NONE | RETRY
-- (p_expected_retry_count, p_retry_delay_s finite and >= 0; NULL for NONE); the terminal result, error code and
-- failed reason; the attempt message and reason.
-- Refused (22023): a NULL task, evidence or retry kind, an evidence or retry slot that does not fit its kind, a
-- negative threshold, a NULL error code. A NULL result is refused with 23502.
-- Locks: the task advisory lock, then the row by id alone (FOR UPDATE), then (the move of a workflow task) the node.
-- Returns one row with the columns of horsies_finish_failed_attempt (outcome APPLIED, RETRY_SCHEDULED, or a
-- refusal; never RELEASED). observed_worker_id and observed_claimed_at are those of the silent claim.
-- Python 0.7.0 (`v0.7.0` package): mark_stale_tasks_as_failed (core/brokers/postgres.py:3932-4215) with
-- SELECT_STALE_TASK_FOR_UPDATE_SQL (:790-817), UPSERT_TASK_ATTEMPT_SQL (core/worker/sql.py:502-527),
-- SCHEDULE_STALE_TASK_RETRY_SQL (core/worker/sql.py:316-353) and horsies_fail_stale_task
-- (core/history/terminalization/move.py:901-1014), for BEAT_AGE. OWNER_GONE is a core addition.
CREATE OR REPLACE FUNCTION horsies_finish_stale_attempt(
    p_task_id uuid,
    p_evidence text,
    p_stale_after_ms integer,
    p_finalizing_stale_after_ms integer,
    p_owner_worker_id text,
    p_owner_claimed_at timestamp with time zone,
    p_retry_kind text,
    p_expected_retry_count integer,
    p_retry_delay_s double precision,
    p_result text,
    p_error_code text,
    p_failed_reason text,
    p_attempt_message text,
    p_attempt_reason text
) RETURNS TABLE(
    task_id uuid,
    ordinality bigint,
    outcome text,
    terminal_at timestamp with time zone,
    terminalization_kind text,
    observed_status text,
    observed_worker_id text,
    observed_claimed_at timestamp with time zone,
    guard_kind text,
    observed_guard jsonb,
    retry_disposition text,
    retry_count integer,
    next_retry_at timestamp with time zone,
    good_until timestamp with time zone,
    is_workflow_task boolean,
    queue_name text,
    task_name text,
    node_has_dependents boolean,
    downstream_subworkflow text
)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_ctx record;
    v_retry boolean;
    v_will_retry boolean := FALSE;
    v_retry_disposition text;
    v_next_retry_at timestamptz;
    v_good_until timestamptz;
    v_terminal_at timestamptz;
    v_last_heartbeat timestamptz;
BEGIN
    IF num_nulls(p_task_id, p_retry_kind, p_error_code) <> 0 THEN
        RAISE EXCEPTION 'horsies_finish_stale_attempt: the task, retry kind and error code are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    CASE p_evidence
        WHEN 'BEAT_AGE' THEN
            IF p_stale_after_ms IS NULL OR p_stale_after_ms < 0
               OR p_finalizing_stale_after_ms IS NULL OR p_finalizing_stale_after_ms < 0
               OR num_nulls(p_owner_worker_id, p_owner_claimed_at) <> 2 THEN
                RAISE EXCEPTION
                    'horsies_finish_stale_attempt: BEAT_AGE takes two thresholds of 0 ms or more and no owner'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'OWNER_GONE' THEN
            IF num_nulls(p_stale_after_ms, p_finalizing_stale_after_ms) <> 2
               OR num_nulls(p_owner_worker_id, p_owner_claimed_at) <> 0 THEN
                RAISE EXCEPTION
                    'horsies_finish_stale_attempt: OWNER_GONE takes an owner and its claim generation and no threshold'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        ELSE
            RAISE EXCEPTION 'horsies_finish_stale_attempt: unknown evidence'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    CASE p_retry_kind
        WHEN 'NONE' THEN
            IF num_nulls(p_expected_retry_count, p_retry_delay_s) <> 2 THEN
                RAISE EXCEPTION 'horsies_finish_stale_attempt: no retry takes no retry count and no delay'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_retry := FALSE;
        WHEN 'RETRY' THEN
            IF num_nulls(p_expected_retry_count, p_retry_delay_s) <> 0
               OR NOT (p_retry_delay_s >= 0 AND p_retry_delay_s < 'Infinity'::double precision) THEN
                RAISE EXCEPTION
                    'horsies_finish_stale_attempt: a retry needs the retry count and a finite delay of 0 s or more'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_retry := TRUE;
        ELSE
            RAISE EXCEPTION 'horsies_finish_stale_attempt: unknown retry kind'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    IF p_result IS NULL THEN
        RAISE EXCEPTION 'horsies_finish_stale_attempt: a stale attempt needs a result payload'
            USING ERRCODE = 'not_null_violation';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    -- One capture: the locked row, the newest runner beat read beside it, and the instant both thresholds are judged
    -- at. Nothing is read again after a refusal, so the evidence cannot show a beat that the judgement did not see.
    -- The whole locked row (task) is the row that the move takes.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.finalizing_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.good_until, t.is_workflow_task,
           t.queue_name, t.task_name, t.enqueued_at, t.attempt_claims, t.first_claimed_at,
           (
               SELECT h.sent_at
               FROM horsies_heartbeats h
               WHERE h.task_id = t.id
                 AND h.role = 'runner'
                 AND h.sent_at >= t.started_at
               ORDER BY h.sent_at DESC
               LIMIT 1
           ) AS newest_runner_beat,
           NOW() AS evaluated_at,
           clock_timestamp() AS db_now,
           t AS task
    INTO v_ctx
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE OF t;

    -- OWNER_GONE fences on the owner and the claim generation that the scan read; BEAT_AGE on no claim. A NOT FOUND
    -- capture leaves every field NULL, so the first test decides it.
    IF NOT FOUND OR v_ctx.status <> 'RUNNING' OR (
        p_evidence = 'OWNER_GONE' AND NOT (
            v_ctx.claimed_by_worker_id IS NOT DISTINCT FROM p_owner_worker_id::varchar
            AND v_ctx.claimed_at IS NOT DISTINCT FROM p_owner_claimed_at
            AND v_ctx.started_at IS NOT NULL
        )
    ) THEN
        RETURN QUERY
        SELECT m.task_id, m.ordinality, m.outcome, m.terminal_at, m.terminalization_kind, m.observed_status,
               m.observed_worker_id::text, m.observed_claimed_at, m.guard_kind, m.observed_guard,
               NULL::text, NULL::integer, NULL::timestamptz, NULL::timestamptz,
               NULL::boolean, NULL::text, NULL::text, NULL::boolean, NULL::text
        FROM horsies_terminalization_miss(
            p_task_id, 'FAIL_STALE',
            CASE p_evidence WHEN 'OWNER_GONE' THEN 'OWNER_AND_GENERATION' ELSE 'NONE' END,
            p_owner_worker_id, p_owner_claimed_at
        ) AS m;
        RETURN;
    END IF;

    -- The beat that BEAT_AGE judges: the newest runner beat when it is inside the recency bound of
    -- horsies_stale_running_scan, else NULL. Every other beat of the attempt is older than the newest one, so no beat
    -- inside the bound is left out.
    v_last_heartbeat := CASE
        WHEN p_evidence = 'BEAT_AGE'
             AND v_ctx.newest_runner_beat >= v_ctx.evaluated_at - make_interval(
                 secs => GREATEST(p_stale_after_ms, p_finalizing_stale_after_ms) / 1000.0
             )
        THEN v_ctx.newest_runner_beat
    END;

    IF p_evidence = 'BEAT_AGE' AND NOT (
        v_ctx.started_at IS NOT NULL
        AND (
            v_ctx.finalizing_at IS NULL
            OR v_ctx.finalizing_at < v_ctx.evaluated_at - make_interval(secs => p_finalizing_stale_after_ms / 1000.0)
        )
        AND COALESCE(v_last_heartbeat, v_ctx.started_at)
            < v_ctx.evaluated_at - make_interval(secs => p_stale_after_ms / 1000.0)
    ) THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text, NULL::timestamptz, NULL::text,
            v_ctx.status::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, 'STALENESS'::text,
            jsonb_build_object(
                'last_heartbeat_at', v_last_heartbeat,
                'started_at', v_ctx.started_at,
                'finalizing_at', v_ctx.finalizing_at,
                'stale_after_ms', p_stale_after_ms,
                'finalizing_stale_after_ms', p_finalizing_stale_after_ms,
                'evaluated_at', v_ctx.evaluated_at
            ),
            NULL::text, NULL::integer, NULL::timestamptz, NULL::timestamptz,
            NULL::boolean, NULL::text, NULL::text, NULL::boolean, NULL::text;
        RETURN;
    END IF;

    IF v_retry AND v_ctx.retry_count <> p_expected_retry_count THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text, NULL::timestamptz, NULL::text,
            v_ctx.status::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, 'RETRY_COUNT'::text,
            jsonb_build_object(
                'expected_retry_count', p_expected_retry_count,
                'retry_count', v_ctx.retry_count
            ),
            NULL::text, NULL::integer, NULL::timestamptz, NULL::timestamptz,
            NULL::boolean, NULL::text, NULL::text, NULL::boolean, NULL::text;
        RETURN;
    END IF;

    IF NOT v_retry THEN
        v_retry_disposition := 'NOT_REQUESTED';
    ELSE
        v_next_retry_at := v_ctx.db_now + make_interval(secs => p_retry_delay_s);
        IF v_ctx.good_until IS NULL OR v_next_retry_at < v_ctx.good_until THEN
            v_will_retry := TRUE;
        ELSE
            v_retry_disposition := 'PAST_DEADLINE';
            v_good_until := v_ctx.good_until;
        END IF;
    END IF;

    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name,
        phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at, placed_at, finalizing_at,
        recorded_by, last_seen_at
    )
    VALUES (
        v_ctx.id, v_ctx.retry_count + 1, 'FAILED', v_will_retry, v_ctx.started_at, v_ctx.db_now,
        p_error_code, p_attempt_message, p_attempt_reason,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid, v_ctx.worker_process_name,
        -- Phase record version 1, from the locked row. Recovery observed no placement; last_seen_at is the
        -- newest of the start, the finalizing marker and the newest runner beat of this attempt.
        1, v_ctx.enqueued_at, v_ctx.claimed_at, v_ctx.attempt_claims, v_ctx.first_claimed_at, NULL,
        v_ctx.finalizing_at, 'RECOVERY', GREATEST(v_ctx.started_at, v_ctx.finalizing_at, v_ctx.newest_runner_beat)
    )
    ON CONFLICT (task_id, attempt) DO UPDATE SET
        outcome = EXCLUDED.outcome,
        will_retry = EXCLUDED.will_retry,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        error_code = EXCLUDED.error_code,
        error_message = EXCLUDED.error_message,
        failed_reason = EXCLUDED.failed_reason,
        worker_id = EXCLUDED.worker_id,
        worker_hostname = EXCLUDED.worker_hostname,
        worker_pid = EXCLUDED.worker_pid,
        worker_process_name = EXCLUDED.worker_process_name,
        phase_record_version = EXCLUDED.phase_record_version,
        enqueued_at = EXCLUDED.enqueued_at,
        claimed_at = EXCLUDED.claimed_at,
        attempt_claims = EXCLUDED.attempt_claims,
        first_claimed_at = EXCLUDED.first_claimed_at,
        placed_at = EXCLUDED.placed_at,
        finalizing_at = EXCLUDED.finalizing_at,
        recorded_by = EXCLUDED.recorded_by,
        last_seen_at = EXCLUDED.last_seen_at;

    IF v_will_retry THEN
        UPDATE horsies_tasks t
        SET status = 'PENDING',
            retry_count = v_ctx.retry_count + 1,
            next_retry_at = v_next_retry_at,
            enqueued_at = v_next_retry_at,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            started_at = NULL,
            worker_pid = NULL,
            worker_hostname = NULL,
            worker_process_name = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            attempt_claims = 0,
            first_claimed_at = NULL,
            updated_at = NOW()
        WHERE t.id = p_task_id;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'RETRY_SCHEDULED'::text, NULL::timestamptz, NULL::text,
            'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb,
            NULL::text, v_ctx.retry_count + 1, v_next_retry_at, NULL::timestamptz,
            NULL::boolean, v_ctx.queue_name::text, NULL::text, NULL::boolean, NULL::text;
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        v_ctx.task, 'FAILED', 'FAIL_STALE', v_terminal_at, p_result, p_error_code, p_failed_reason
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'FAIL_STALE'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb,
        v_retry_disposition, NULL::integer, v_next_retry_at, v_good_until,
        v_ctx.is_workflow_task, v_ctx.queue_name::text, v_ctx.task_name::text,
        CASE WHEN v_ctx.is_workflow_task THEN horsies_node_has_dependents(p_task_id) END,
        CASE WHEN v_ctx.is_workflow_task THEN horsies_node_downstream_subworkflow(p_task_id) END;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Move to history: the attempt archive in version 2
------------------------------------------------------------------------------------------------------------------------

-- horsies_move_task_to_history(p_task horsies_tasks, ...): move one live task to history.
--
-- Guarantee: inside the caller's transaction, the live row p_task becomes exactly one history row, and every effect
-- of finishing happens together or not at all: the history row (with the rerun input envelope, the attempt
-- archive in version 2 (horsies_encode_attempt_archive of p_task) and the trace id of the live row), the clock
-- exception when the id's UUIDv7 birth is more than 5 s later than p_terminal_at, the phase-2 pending row (deferred
-- kinds of a workflow task), the TERMINAL window of the task's key reservation, the delete of the attempt rows and
-- of the live row, and the task_done and horsies_task_status notifications (sent at commit).
-- Precondition: the caller holds pg_advisory_xact_lock(hashtextextended(id::text, 731)), then read p_task with
-- SELECT * FROM horsies_tasks WHERE id = $1 FOR UPDATE, then checked its own fence. p_task is the stored row: a
-- caller that changes the live row before the move passes the row its UPDATE returned.
-- Rerun envelope: eligibility before policy. A workflow task or a COMPLETED status gives NEVER_ELIGIBLE;
-- retain_rerun_input off gives DECLINED_BY_POLICY; else the stored input and task_options are framed once by
-- horsies_encode_rerun_input (0003); more than 65,536 bytes gives OVER_BOUND, else INLINE with version 1 and the
-- sha256 digest.
-- Refusals (each raises; the caller's transaction rolls back with nothing changed):
-- - 22023 invalid_parameter_value: an unknown kind; COMPLETE_FUSED or CANCEL_ADMIN on a workflow task.
-- - P0001: a kind whose status disagrees; a NULL row or terminal time; a status that is not live; a row that is
--   not the stored live row (gone, or changed since the caller read it).
-- - XX001 data_corrupted: the duplicate identity guard finds the id in history or in the clock exceptions.
-- - 23502 not_null_violation: a deferred kind on a workflow task with no result.
-- - P0002 no_data_found: a deferred kind on a workflow task with no node row.
-- - 55H01 (history leaf missing): the class partition or the leaf of p_terminal_at does not exist, also when
--   retention dropped the leaf (horsies_require_history_leaf).
-- - 23514 check_violation: the leaf of p_terminal_at exists but takes no row (partition routing): retention detached
--   it or is detaching it.
-- Retention: before its first read of history the move takes pg_advisory_xact_lock_shared on
-- horsies_task_history_leaf_lock_key(retention_class_key, p_terminal_at), the key of the leaf that takes the row.
-- Moves share it; leaf creation, the detach and the drop take it exclusively with a try-lock. So maintenance answers
-- BUSY for a leaf while a move into it has not ended, and a move waits while maintenance holds its leaf; when it
-- continues into a leaf that is detached or detaching it raises 23514, and 55H01 for a dropped one. Without the
-- lock, a detach that began while a move was writing would wait for the move's commit and then detach the leaf with
-- the new row and the phase-2 pending row that points at it.
-- Lock order: task advisory lock and live row FOR UPDATE (both taken by the caller); the leaf lock (shared); the
-- node row FOR UPDATE; the history leaf (ROW EXCLUSIVE, by the insert); the clock exception row; the phase-2
-- pending row; the reservation row (UPDATE); the attempt rows and the live row (DELETE). Maintenance takes the
-- leaf lock first, never waits for it, and takes no task, node or pending row lock while it holds it. A move that
-- waits for the leaf lock holds no lock on history when every move of its transaction passes the same p_terminal_at
-- (the database clock), so DETACH ... CONCURRENTLY, which waits only for the lockers of the class parent, never
-- waits for it. DETACH ... FINALIZE also waits for every older snapshot, so a move that waits for the leaf lock of a
-- leaf in FINALIZE closes a cycle: PostgreSQL ends one of the two with 40P01, and nothing is written. Retention
-- detaches only leaves older than the class retention, so a p_terminal_at from the database clock never waits for
-- a detach or a FINALIZE.
-- Indexes: ix_horsies_workflow_tasks_task_id (node probe); the leaf task_id indexes and
-- horsies_task_history_clock_exceptions_pkey (guard, ON CONFLICT); horsies_task_attempts (task_id, attempt);
-- horsies_key_reservations_pkey; horsies_tasks_pkey (delete).
-- Returns: nothing.
-- Replaces (`v0.7.0` package): core/history/terminalization/move.py:423-620 (the move body), which reads the locked
-- live row again (:476) after a second advisory lock (:472), counts the node links, and reads the live row a third time
-- for the clock exception (core/history/terminalization/duplicate_guard.py:123-143).
CREATE OR REPLACE FUNCTION horsies_move_task_to_history(p_task horsies_tasks, p_terminal_status text, p_terminalization_kind text, p_terminal_at timestamp with time zone, p_result text, p_error_code text, p_failed_reason text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_requires_deferred_phase2 boolean;
    v_workflow_id uuid;
    v_workflow_node_row_id uuid;
    v_rerun_disposition varchar(32);
    v_rerun_version smallint;
    v_rerun_digest bytea;
    v_rerun_inline bytea;
    v_rerun_envelope bytea;
    v_attempt_snapshot bytea;
    v_result_payload bytea;
    v_result_digest bytea;
    v_history_rows bigint;
    v_deleted_rows bigint;
BEGIN
    CASE p_terminalization_kind
        WHEN 'COMPLETE_LOCKED' THEN
            IF p_terminal_status <> 'COMPLETED' THEN
                RAISE EXCEPTION 'completion-locked projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        WHEN 'COMPLETE_FUSED' THEN
            IF p_terminal_status <> 'COMPLETED' THEN
                RAISE EXCEPTION 'completion-fused projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'FAIL_RUNNING' THEN
            IF p_terminal_status <> 'FAILED' THEN
                RAISE EXCEPTION 'running-failure projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        WHEN 'FAIL_STALE' THEN
            IF p_terminal_status <> 'FAILED' THEN
                RAISE EXCEPTION 'stale-failure projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        WHEN 'EXPIRE_CLAIMED' THEN
            IF p_terminal_status <> 'EXPIRED' THEN
                RAISE EXCEPTION 'claimed-expiry projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        WHEN 'CANCEL_ADMIN' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'administrative-cancel projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'CANCEL_ORPHAN' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'orphan-cancel projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'PAUSE_ABANDON_CLAIM' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'pause-abandon projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'PAUSE_ABANDON_CLAIM_BATCH' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'pause-abandon-batch projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'PAUSE_ABANDON_WORKFLOW' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'paused-workflow-sweep projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'WORKFLOW_CANCEL_CLAIM' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'workflow-cancel projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'WORKFLOW_CANCEL_CLAIM_BATCH' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'workflow-cancel-batch projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'WORKFLOW_CANCEL_WORKFLOW' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'cancelled-workflow-sweep projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        ELSE
            RAISE EXCEPTION 'terminalization kind % has no single-row move', p_terminalization_kind
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;

    IF p_task.id IS NULL THEN
        RAISE EXCEPTION 'the move needs the live row that the caller locked; got a NULL row';
    END IF;
    IF p_task.status NOT IN ('PENDING', 'CLAIMED', 'RUNNING') THEN
        RAISE EXCEPTION 'live task % has non-live status %', p_task.id, p_task.status;
    END IF;
    IF p_terminal_at IS NULL THEN
        RAISE EXCEPTION 'terminal timestamp is required to move task %', p_task.id;
    END IF;
    -- The leaf lock of the row's leaf, shared, before the first read of history: see "Retention" above.
    PERFORM pg_advisory_xact_lock_shared(horsies_task_history_leaf_lock_key(p_task.retention_class_key, p_terminal_at));
    IF horsies_task_history_duplicate_staged(p_task.id) THEN
        RAISE EXCEPTION 'task identity % exists in multiple locations', p_task.id
            USING ERRCODE = 'data_corrupted';
    END IF;

    -- The node link: one probe of the unique task_id index. A second link cannot be stored.
    IF p_task.is_workflow_task THEN
        IF p_terminalization_kind IN ('COMPLETE_FUSED', 'CANCEL_ADMIN') THEN
            RAISE EXCEPTION 'operation % cannot terminalize workflow task %', p_terminalization_kind, p_task.id
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        IF v_requires_deferred_phase2 THEN
            -- A deferred kind needs the node row: STRICT raises no_data_found when it is gone.
            SELECT n.id, n.workflow_id
            INTO STRICT v_workflow_node_row_id, v_workflow_id
            FROM horsies_workflow_tasks AS n
            WHERE n.task_id = p_task.id
            FOR UPDATE;
            IF p_result IS NULL THEN
                RAISE EXCEPTION 'deferred workflow terminalization of task % requires a result payload', p_task.id
                    USING ERRCODE = 'not_null_violation';
            END IF;
        ELSE
            -- An orphan kind is defined by a missing node row, so zero rows is a valid answer.
            SELECT n.id, n.workflow_id
            INTO v_workflow_node_row_id, v_workflow_id
            FROM horsies_workflow_tasks AS n
            WHERE n.task_id = p_task.id
            FOR UPDATE;
        END IF;
    END IF;

    IF p_task.is_workflow_task OR p_terminal_status = 'COMPLETED' THEN
        v_rerun_disposition := 'NEVER_ELIGIBLE';
    ELSIF NOT p_task.retain_rerun_input THEN
        v_rerun_disposition := 'DECLINED_BY_POLICY';
    ELSE
        v_rerun_envelope := horsies_encode_rerun_input(p_task.input, p_task.task_options);
        IF octet_length(v_rerun_envelope) > 65536 THEN
            v_rerun_disposition := 'OVER_BOUND';
        ELSE
            v_rerun_disposition := 'INLINE';
            v_rerun_version := 1;
            v_rerun_digest := sha256(v_rerun_envelope);
            v_rerun_inline := v_rerun_envelope;
        END IF;
    END IF;

    v_attempt_snapshot := horsies_encode_attempt_archive(p_task);

    -- An administrative cancel stores no result. One digest serves the history row and the deferred record.
    IF p_terminalization_kind = 'CANCEL_ADMIN' OR p_result IS NULL THEN
        v_result_payload := NULL;
        v_result_digest := NULL;
    ELSE
        v_result_payload := convert_to(p_result, 'UTF8');
        v_result_digest := sha256(v_result_payload);
    END IF;

    -- The leaf that takes the row exists: else 55H01, which names the class and the missing range.
    PERFORM horsies_require_history_leaf(p_task.retention_class_key, p_terminal_at);
    INSERT INTO horsies_task_history (
        task_id, task_name, queue_name, priority, command_fingerprint_version, command_fingerprint,
        status, terminalization_kind, terminal_at, retention_class_key,
        sent_at, enqueued_at, claimed_at, started_at, created_at, good_until,
        result_envelope_version, result_payload, result_digest, error_code, final_failed_reason,
        retry_count, max_retries, last_claimed_worker_id, last_worker_hostname, last_worker_pid,
        last_worker_process_name, rerun_of_task_id, rerun_root_task_id, workflow_id, is_workflow_task,
        history_schema_version, attempt_archive_version, attempt_snapshot, attempt_snapshot_digest,
        rerun_input_disposition, rerun_input_version, rerun_input_digest, rerun_input_inline, trace_id
    ) VALUES (
        p_task.id, p_task.task_name, p_task.queue_name, p_task.priority, p_task.command_fingerprint_version,
        p_task.command_fingerprint,
        p_terminal_status, p_terminalization_kind, p_terminal_at, p_task.retention_class_key,
        p_task.sent_at, p_task.enqueued_at, p_task.claimed_at, p_task.started_at, p_task.created_at,
        p_task.good_until,
        1, v_result_payload, v_result_digest, p_error_code, p_failed_reason,
        p_task.retry_count, p_task.max_retries, p_task.claimed_by_worker_id, p_task.worker_hostname,
        p_task.worker_pid, p_task.worker_process_name, p_task.rerun_of_task_id, p_task.rerun_root_task_id,
        v_workflow_id, p_task.is_workflow_task,
        1, 2, v_attempt_snapshot, sha256(v_attempt_snapshot),
        v_rerun_disposition, v_rerun_version, v_rerun_digest, v_rerun_inline, p_task.trace_id
    );
    GET DIAGNOSTICS v_history_rows = ROW_COUNT;
    IF v_history_rows <> 1 THEN
        RAISE EXCEPTION 'history insert of task % affected % rows', p_task.id, v_history_rows;
    END IF;

    -- A producer clock more than 5 s fast places the id's birth after its terminal time; the guard then finds
    -- the id through this table, because the birth-pruned leaf probes skip the leaf that holds it.
    IF horsies_uuid7_birth(p_task.id) - interval '5 seconds' > p_terminal_at THEN
        INSERT INTO horsies_task_history_clock_exceptions (task_id, retention_class_key, terminal_at)
        VALUES (p_task.id, p_task.retention_class_key, p_terminal_at)
        ON CONFLICT (task_id) DO NOTHING;
    END IF;

    IF v_requires_deferred_phase2 AND p_task.is_workflow_task THEN
        INSERT INTO horsies_workflow_phase2_pending (
            task_id, workflow_id, workflow_node_row_id, terminalization_kind,
            recovery_source, history_class, history_anchor, history_schema_version, result_digest,
            quarantine_task_id, created_at, attempt_count, last_attempt_at, last_failure_class
        ) VALUES (
            p_task.id, v_workflow_id, v_workflow_node_row_id, p_terminalization_kind,
            'HISTORY', p_task.retention_class_key, p_terminal_at, 1, v_result_digest,
            NULL, statement_timestamp(), 0, NULL, NULL
        );
    END IF;

    IF p_task.idempotency_key_digest IS NOT NULL THEN
        -- Explicit discard: a keyed live row owns the LIVE reservation that its enqueue transaction wrote, and only
        -- this move ends it, so the update always applies; the boolean adds no information here.
        PERFORM horsies_key_reservation_terminalize(p_task.idempotency_key_digest, p_task.id, p_terminal_at);
    END IF;

    DELETE FROM horsies_task_attempts AS a WHERE a.task_id = p_task.id;
    -- The row comparison makes a caller that passed a row it did not lock fail closed.
    DELETE FROM horsies_tasks AS t WHERE t.id = p_task.id AND t IS NOT DISTINCT FROM p_task;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;
    IF v_deleted_rows <> 1 THEN
        RAISE EXCEPTION 'live task % is gone or differs from the row the caller locked', p_task.id;
    END IF;
    PERFORM pg_notify('task_done', p_task.id::text);
    PERFORM pg_notify('horsies_task_status', p_task.id::text);
END
$$;

-- horsies_move_tasks_to_history(p_task_ids uuid[], ...): the move stage that every batch shares. Internal: the
-- batch terminal writes call it after they select and lock their rows.
--
-- Guarantee: inside the caller's transaction, every listed live task becomes exactly one history row, or none
-- does. One terminal time and one result digest serve the whole batch; each attempt archive is encoded once, in
-- version 2 (horsies_encode_attempt_archive of the locked row); the rerun envelope is built only for rows that are
-- eligible and retained (the single move's ladder); each history row keeps the trace id of its live row; clock
-- exceptions, phase-2 pending rows (EXPIRE_PENDING workflow tasks) and
-- key reservation windows are written for every row that needs one; attempt rows and live rows are deleted; one
-- task_done and one horsies_task_status notification per task, in the order of p_task_ids. An empty array moves
-- nothing.
-- Precondition: the caller locked every listed row (discovery FOR UPDATE SKIP LOCKED, or FOR UPDATE in sorted id
-- order), passes NOW() as p_terminal_at, and reads its outcome rows from the locked live rows before this call.
-- Kinds: EXPIRE_PENDING (status EXPIRED, deferred phase 2); PAUSE_ABANDON_WORKFLOW, PAUSE_ABANDON_CLAIM_BATCH,
-- WORKFLOW_CANCEL_WORKFLOW, WORKFLOW_CANCEL_CLAIM_BATCH, CANCEL_ORPHAN_SWEEP (status CANCELLED).
-- Refusals (each raises; the caller's transaction rolls back with nothing changed):
-- - 22023 invalid_parameter_value: an unknown kind; a NULL array; an id listed twice.
-- - P0001: a kind whose status disagrees; a NULL terminal time; a listed id that is not a live row.
-- - 23502 not_null_violation: EXPIRE_PENDING of a workflow task with no result.
-- - 23503 foreign_key_violation: EXPIRE_PENDING of a workflow task with no node row.
-- - XX001 data_corrupted: the duplicate identity guard finds a listed id.
-- - 55H01 (history leaf missing): the class partition or the leaf at p_terminal_at of a class of the batch does not
--   exist (horsies_require_history_leaf).
-- - 23514 check_violation: the leaf exists but takes no row (detached or detaching).
-- Lock order: the caller's row locks; the shared leaf lock of each class of the batch at p_terminal_at, in key
-- order (as the single move, Retention); then as the single move (history leaves, clock exception rows, pending
-- rows, reservation rows, attempt and live row deletes). Node rows are read, not locked, as in Python.
-- Indexes: horsies_tasks_pkey (id = ANY); ix_horsies_workflow_tasks_task_id; the leaf task_id indexes and
-- horsies_task_history_clock_exceptions_pkey; horsies_task_attempts (task_id, attempt);
-- horsies_key_reservations_pkey.
-- Returns: nothing.
-- Replaces (`v0.7.0` package): core/history/terminalization/move.py:1582-1741 (the shared move stage) and :1493-1580
-- (the batch history insert), which 0.7.0 renders into six functions. The link check keeps only the refusal of a
-- deferred workflow task without its node row: the unique index ix_horsies_workflow_tasks_task_id makes a second link
-- impossible.
CREATE OR REPLACE FUNCTION horsies_move_tasks_to_history(p_task_ids uuid[], p_terminal_status text, p_terminalization_kind text, p_terminal_at timestamp with time zone, p_result text, p_error_code text, p_failed_reason text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_requires_deferred_phase2 boolean;
    v_result_payload bytea;
    v_result_digest bytea;
    v_moved bigint;
    v_deleted bigint;
BEGIN
    CASE p_terminalization_kind
        WHEN 'EXPIRE_PENDING' THEN
            IF p_terminal_status <> 'EXPIRED' THEN
                RAISE EXCEPTION 'pending-expiry projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        WHEN 'PAUSE_ABANDON_WORKFLOW' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'paused-workflow-sweep projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'PAUSE_ABANDON_CLAIM_BATCH' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'pause-abandon-batch projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'WORKFLOW_CANCEL_WORKFLOW' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'cancelled-workflow-sweep projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'WORKFLOW_CANCEL_CLAIM_BATCH' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'workflow-cancel-batch projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN 'CANCEL_ORPHAN_SWEEP' THEN
            IF p_terminal_status <> 'CANCELLED' THEN
                RAISE EXCEPTION 'orphan-sweep projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        ELSE
            RAISE EXCEPTION 'terminalization kind % has no batch move', p_terminalization_kind
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;

    IF p_task_ids IS NULL THEN
        RAISE EXCEPTION 'the batch move needs an id array; got NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_task_ids) = 0 THEN
        RETURN;
    END IF;
    IF (SELECT count(DISTINCT u.id) FROM unnest(p_task_ids) AS u(id)) <> cardinality(p_task_ids) THEN
        RAISE EXCEPTION 'the batch move lists a task id more than once'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_terminal_at IS NULL THEN
        RAISE EXCEPTION 'terminal timestamp is required for a batch move';
    END IF;

    -- The shared leaf lock of each class in the batch, in key order, before the first read of history (as the single
    -- move).
    PERFORM pg_advisory_xact_lock_shared(k.key)
    FROM (
        SELECT DISTINCT horsies_task_history_leaf_lock_key(t.retention_class_key, p_terminal_at) AS key
        FROM horsies_tasks AS t
        WHERE t.id = ANY(p_task_ids)
    ) AS k
    ORDER BY k.key;

    IF v_requires_deferred_phase2 THEN
        IF p_result IS NULL AND EXISTS (
            SELECT 1 FROM horsies_tasks AS t WHERE t.id = ANY(p_task_ids) AND t.is_workflow_task
        ) THEN
            RAISE EXCEPTION 'deferred workflow terminalization requires a result payload'
                USING ERRCODE = 'not_null_violation';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM horsies_tasks AS t
            WHERE t.id = ANY(p_task_ids)
              AND t.is_workflow_task
              AND NOT EXISTS (SELECT 1 FROM horsies_workflow_tasks AS n WHERE n.task_id = t.id)
        ) THEN
            RAISE EXCEPTION 'workflow-backing task lacks exactly one node row'
                USING ERRCODE = 'foreign_key_violation';
        END IF;
    END IF;

    IF EXISTS (SELECT 1 FROM unnest(p_task_ids) AS u(id) WHERE horsies_task_history_duplicate_staged(u.id)) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    -- One payload for the whole batch: hash it once for the history rows and the deferred records.
    IF p_result IS NULL THEN
        v_result_payload := NULL;
        v_result_digest := NULL;
    ELSE
        v_result_payload := convert_to(p_result, 'UTF8');
        v_result_digest := sha256(v_result_payload);
    END IF;

    -- The leaf of each class in the batch exists: else 55H01, which names a class that has none and its range.
    PERFORM horsies_require_history_leaf(c.class_key, p_terminal_at)
    FROM (
        SELECT DISTINCT t.retention_class_key::text AS class_key
        FROM horsies_tasks AS t
        WHERE t.id = ANY(p_task_ids)
    ) AS c
    ORDER BY c.class_key;

    WITH attempt_data AS MATERIALIZED (
        SELECT t.id, horsies_encode_attempt_archive(t) AS snapshot
        FROM horsies_tasks AS t
        WHERE t.id = ANY(p_task_ids)
    )
    INSERT INTO horsies_task_history (
        task_id, task_name, queue_name, priority, command_fingerprint_version, command_fingerprint,
        status, terminalization_kind, terminal_at, retention_class_key,
        sent_at, enqueued_at, claimed_at, started_at, created_at, good_until,
        result_envelope_version, result_payload, result_digest, error_code, final_failed_reason,
        retry_count, max_retries, last_claimed_worker_id, last_worker_hostname, last_worker_pid,
        last_worker_process_name, rerun_of_task_id, rerun_root_task_id, workflow_id, is_workflow_task,
        history_schema_version, attempt_archive_version, attempt_snapshot, attempt_snapshot_digest,
        rerun_input_disposition, rerun_input_version, rerun_input_digest, rerun_input_inline, trace_id
    )
    SELECT
        t.id, t.task_name, t.queue_name, t.priority, t.command_fingerprint_version, t.command_fingerprint,
        p_terminal_status, p_terminalization_kind, p_terminal_at, t.retention_class_key,
        t.sent_at, t.enqueued_at, t.claimed_at, t.started_at, t.created_at, t.good_until,
        1, v_result_payload, v_result_digest, p_error_code, p_failed_reason,
        t.retry_count, t.max_retries, t.claimed_by_worker_id, t.worker_hostname, t.worker_pid,
        t.worker_process_name, t.rerun_of_task_id, t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN n.workflow_id END, t.is_workflow_task,
        1, 2, attempt_data.snapshot, sha256(attempt_data.snapshot),
        d.disposition,
        CASE WHEN d.disposition = 'INLINE' THEN 1::smallint END,
        CASE WHEN d.disposition = 'INLINE' THEN sha256(e.envelope) END,
        CASE WHEN d.disposition = 'INLINE' THEN e.envelope END,
        t.trace_id
    FROM horsies_tasks AS t
    JOIN attempt_data ON attempt_data.id = t.id
    LEFT JOIN horsies_workflow_tasks AS n ON n.task_id = t.id
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR p_terminal_status = 'COMPLETED' THEN NULL::bytea
            WHEN NOT t.retain_rerun_input THEN NULL::bytea
            ELSE horsies_encode_rerun_input(t.input, t.task_options)
        END AS envelope
    ) AS e
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR p_terminal_status = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            WHEN octet_length(e.envelope) > 65536 THEN 'OVER_BOUND'
            ELSE 'INLINE'
        END AS disposition
    ) AS d
    WHERE t.id = ANY(p_task_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(p_task_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows: a listed id is not a live row',
            v_moved, cardinality(p_task_ids);
    END IF;

    INSERT INTO horsies_task_history_clock_exceptions (task_id, retention_class_key, terminal_at)
    SELECT t.id, t.retention_class_key, p_terminal_at
    FROM horsies_tasks AS t
    WHERE t.id = ANY(p_task_ids)
      AND horsies_uuid7_birth(t.id) - interval '5 seconds' > p_terminal_at
    ON CONFLICT (task_id) DO NOTHING;

    IF v_requires_deferred_phase2 THEN
        INSERT INTO horsies_workflow_phase2_pending (
            task_id, workflow_id, workflow_node_row_id, terminalization_kind,
            recovery_source, history_class, history_anchor, history_schema_version, result_digest,
            quarantine_task_id, created_at, attempt_count, last_attempt_at, last_failure_class
        )
        SELECT
            t.id, n.workflow_id, n.id, p_terminalization_kind,
            'HISTORY', t.retention_class_key, p_terminal_at, 1, v_result_digest,
            NULL, statement_timestamp(), 0, NULL, NULL
        FROM horsies_tasks AS t
        JOIN horsies_workflow_tasks AS n ON n.task_id = t.id
        WHERE t.id = ANY(p_task_ids) AND t.is_workflow_task;
    END IF;

    -- Explicit discard: every keyed row owns a LIVE reservation that only its move ends (see the single move).
    PERFORM horsies_key_reservation_terminalize_batch(
        keyed.digests, keyed.ids, p_terminal_at
    )
    FROM (
        SELECT COALESCE(array_agg(t.idempotency_key_digest ORDER BY t.id), '{}') AS digests,
               COALESCE(array_agg(t.id ORDER BY t.id), '{}') AS ids
        FROM horsies_tasks AS t
        WHERE t.id = ANY(p_task_ids) AND t.idempotency_key_digest IS NOT NULL
    ) AS keyed;

    DELETE FROM horsies_task_attempts AS a WHERE a.task_id = ANY(p_task_ids);
    DELETE FROM horsies_tasks AS t WHERE t.id = ANY(p_task_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(p_task_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows', v_deleted, cardinality(p_task_ids);
    END IF;

    PERFORM pg_notify('task_done', u.id::text)
    FROM unnest(p_task_ids) WITH ORDINALITY AS u(id, position)
    ORDER BY u.position;
    PERFORM pg_notify('horsies_task_status', u.id::text)
    FROM unnest(p_task_ids) WITH ORDINALITY AS u(id, position)
    ORDER BY u.position;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Monitoring: the live attempts in version 2
------------------------------------------------------------------------------------------------------------------------

-- horsies_monitoring_task_detail: one task, live or in history, with its attempts and its workflow link.
--
-- Guarantee: every part comes from one snapshot, with no REPEATABLE READ: the function is STABLE, so each of its
-- statements and the STABLE published reader horsies_task_detail_staged that it calls read the snapshot of the
-- statement that calls the function. A task that moves to history during the read is either LIVE with all its
-- attempts or HISTORY with its snapshot, never LIVE without attempts and never absent.
-- - LIVE: the live row. The live table has no terminal, error or result column. With p_with_attempts, the attempt
--   rows in ascending order with the open attempt of the live row, encoded by horsies_encode_attempt_archive: the
--   same version-2 bytes that a move of the row at this instant stores in history, so one decoder serves both
--   locations.
-- - HISTORY: after a live miss, the history row through horsies_task_detail_staged (which prunes its leaf probes by
--   the id's UUIDv7 birth). With p_with_result, the stored result envelope; with p_with_attempts, the stored attempt
--   snapshot and its digest.
-- - Both: the workflow link (the node whose task_id is this task) by ix_horsies_workflow_tasks_task_id.
-- - ABSENT: no row in either place. absence_reason from horsies_task_absence_reason (0006_history_functions.sql),
--   the one rule of absence that the rerun read also uses, read in the same snapshot: NOT_CLASSIFIED when the id is
--   not a UUIDv7; PREDATES_RETAINED_HISTORY when its birth is before the latest upper anchor of the detached leaves
--   of PERIOD classes plus 5 s, so retention can have removed it; WITHIN_RETAINED_HISTORY otherwise, also when
--   retention has detached no leaf.
-- - The caller decodes the stored status text and the attempt snapshot, and checks the digests.
-- Lock order: none (a plain read; no row lock, no advisory lock).
-- Returns: one row. Columns by location:
-- - every location: location ('LIVE', 'HISTORY', 'ABSENT'), task_id;
-- - LIVE and HISTORY: task_name, queue_name, priority, status, is_workflow_task, retry_count, max_retries, sent_at,
--   enqueued_at, claimed_at, started_at, good_until, worker_id, worker_hostname, worker_pid, worker_process_name
--   (LIVE: the current claim; HISTORY: the last claim), workflow_id and workflow_task_index (both NULL for a task
--   that backs no node);
-- - LIVE only: next_retry_at, execution;
-- - HISTORY only: terminal_at, terminalization_kind, error_code, final_failed_reason; with p_with_result:
--   result_envelope_version, result_payload, result_digest;
-- - with p_with_attempts: attempt_archive_version and attempt_snapshot; HISTORY also attempt_snapshot_digest;
-- - ABSENT only: absence_reason;
-- - LIVE and HISTORY: trace_id, the trace id of the producer span (NULL for a task sent with no trace).
-- Raises 22004 for a NULL argument, and data_corrupted (XX001) when the published reader reports the task live
-- after the live miss of the same snapshot, or answers a location it does not define.
-- Replaces: clients/python/horsies/monitoring/queries.py:1404-1499 (get_task_detail) and :1516-1548 (_history_detail), which read
-- in REPEATABLE READ with 5 or 6 round trips; clients/python/horsies/core/brokers/postgres.py:4768-4978 (get_task_info_async) and
-- :4625-4766 (_history_task_info), 4 or 5 round trips with a snapshot per statement.
CREATE OR REPLACE FUNCTION horsies_monitoring_task_detail(
    p_task_id uuid,
    p_with_result boolean,
    p_with_attempts boolean
) RETURNS TABLE (
    location text,
    task_id uuid,
    task_name character varying,
    queue_name character varying,
    priority integer,
    status text,
    is_workflow_task boolean,
    retry_count integer,
    max_retries integer,
    sent_at timestamp with time zone,
    enqueued_at timestamp with time zone,
    claimed_at timestamp with time zone,
    started_at timestamp with time zone,
    good_until timestamp with time zone,
    worker_id character varying,
    worker_hostname character varying,
    worker_pid integer,
    worker_process_name character varying,
    next_retry_at timestamp with time zone,
    execution text,
    terminal_at timestamp with time zone,
    terminalization_kind text,
    error_code text,
    final_failed_reason text,
    result_envelope_version smallint,
    result_payload bytea,
    result_digest bytea,
    attempt_archive_version smallint,
    attempt_snapshot bytea,
    attempt_snapshot_digest bytea,
    workflow_id uuid,
    workflow_task_index integer,
    absence_reason text,
    trace_id bytea
)
    LANGUAGE plpgsql STABLE
AS $$
#variable_conflict use_column
DECLARE
    v_detail record;
    v_location text;
    v_row horsies_task_history;
BEGIN
    IF p_task_id IS NULL OR p_with_result IS NULL OR p_with_attempts IS NULL THEN
        RAISE EXCEPTION 'horsies_monitoring_task_detail: task id and both part flags are required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;

    RETURN QUERY
    SELECT 'LIVE'::text, t.id, t.task_name, t.queue_name, t.priority, t.status::text, t.is_workflow_task,
           t.retry_count, t.max_retries, t.sent_at, t.enqueued_at, t.claimed_at, t.started_at, t.good_until,
           t.claimed_by_worker_id, t.worker_hostname, t.worker_pid, t.worker_process_name, t.next_retry_at,
           t.execution, NULL::timestamptz, NULL::text, NULL::text, NULL::text,
           NULL::smallint, NULL::bytea, NULL::bytea,
           CASE WHEN p_with_attempts THEN 2::smallint END,
           CASE WHEN p_with_attempts THEN horsies_encode_attempt_archive(t) END,
           NULL::bytea,
           link.workflow_id, link.task_index, NULL::text, t.trace_id
    FROM horsies_tasks AS t
    LEFT JOIN horsies_workflow_tasks AS link ON link.task_id = t.id
    WHERE t.id = p_task_id;
    IF FOUND THEN
        RETURN;
    END IF;

    SELECT d.location, d.task_row INTO v_detail
    FROM horsies_task_detail_staged(p_task_id) AS d;
    IF FOUND THEN
        v_location := v_detail.location;
        v_row := v_detail.task_row;
        IF v_location IS DISTINCT FROM 'HISTORY' THEN
            RAISE EXCEPTION 'horsies_monitoring_task_detail: the published reader reports task % at location % after '
                'a live miss in the same snapshot', p_task_id, v_location
                USING ERRCODE = 'data_corrupted';
        END IF;
        RETURN QUERY
        SELECT 'HISTORY'::text, v_row.task_id, v_row.task_name, v_row.queue_name, v_row.priority,
               v_row.status::text, v_row.is_workflow_task, v_row.retry_count, v_row.max_retries, v_row.sent_at,
               v_row.enqueued_at, v_row.claimed_at, v_row.started_at, v_row.good_until,
               v_row.last_claimed_worker_id, v_row.last_worker_hostname, v_row.last_worker_pid,
               v_row.last_worker_process_name, NULL::timestamptz, NULL::text,
               v_row.terminal_at, v_row.terminalization_kind::text, v_row.error_code, v_row.final_failed_reason,
               CASE WHEN p_with_result THEN v_row.result_envelope_version END,
               CASE WHEN p_with_result THEN v_row.result_payload END,
               CASE WHEN p_with_result THEN v_row.result_digest END,
               CASE WHEN p_with_attempts THEN v_row.attempt_archive_version END,
               CASE WHEN p_with_attempts THEN v_row.attempt_snapshot END,
               CASE WHEN p_with_attempts THEN v_row.attempt_snapshot_digest END,
               link.workflow_id, link.task_index, NULL::text, v_row.trace_id
        FROM (SELECT 1) AS one
        LEFT JOIN horsies_workflow_tasks AS link ON link.task_id = p_task_id;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT 'ABSENT'::text, p_task_id, NULL::character varying, NULL::character varying, NULL::integer, NULL::text,
           NULL::boolean, NULL::integer, NULL::integer, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz,
           NULL::timestamptz, NULL::timestamptz, NULL::character varying, NULL::character varying, NULL::integer,
           NULL::character varying, NULL::timestamptz, NULL::text, NULL::timestamptz, NULL::text, NULL::text,
           NULL::text, NULL::smallint, NULL::bytea, NULL::bytea, NULL::smallint, NULL::bytea, NULL::bytea,
           NULL::uuid, NULL::integer,
           horsies_task_absence_reason(p_task_id), NULL::bytea;
END;
$$;
