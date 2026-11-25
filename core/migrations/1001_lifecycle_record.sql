-- horsies schema migration 1001: the lifecycle record of each attempt and of each workflow run.
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
-- - horsies_workflow_events keeps the facts of a run that no task row or node row holds: a node that went back to
--   READY, a sub-workflow node that went ENQUEUED, a task of a node that moved to history (with its history
--   locator), a run that was paused or resumed. The two moves and eight workflow steps write the events in the
--   statements that change those states.
-- - horsies_monitoring_workflow_run returns, beside the run and its nodes, the capture start, the events of the run
--   and the task rows of its nodes (live, or in history at the locator of each TASK_MOVED event), from which the
--   core composes the phase timing of the run and of each node.
--
-- Rows written before this migration keep NULL in every new column, and the migration writes no value into them:
-- - horsies_tasks.attempt_claims NULL: the claims of the current attempt are not counted. A claim keeps it NULL; the
--   next retry sets it to 0.
-- - horsies_task_attempts.phase_record_version NULL: the attempt row has no phase record.
-- The migration writes no event: a run that began before it has the events of the steps after it only.
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control. A function whose argument list changes is dropped and created again; a function
-- whose argument list stays is replaced in place. The comment above each function of this file is its contract from
-- this migration on; the comment in core/baseline/ is the contract of migration 1000.
--
-- Locks: each ALTER TABLE takes ACCESS EXCLUSIVE on its table until the migration commits. The new columns are
-- nullable with no default, so no table is rewritten; each new CHECK reads its table once. The foreign keys of
-- horsies_workflow_events take SHARE ROW EXCLUSIVE on horsies_workflows and horsies_workflow_tasks until the
-- migration commits.

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
-- Events of a workflow run (horsies_workflow_events)
------------------------------------------------------------------------------------------------------------------------

-- The values of horsies_workflow_events.event_id. Each event takes the next value in the statement that writes it,
-- so the values order the events of one run. occurred_at alone cannot order them: one statement writes several
-- events at one now(). No value is used twice.
CREATE SEQUENCE horsies_workflow_event_id AS bigint NO CYCLE;

-- One row per event of a run. The statement that changes the state writes the event; no trigger writes one.
-- event_kind:
-- - WORKFLOW_PAUSED, WORKFLOW_RESUMED: the run went PAUSED or RUNNING (horsies_workflow_pause,
--   horsies_workflow_record_failure, horsies_workflow_resume): one event for the workflow and one for each
--   descendant that moved with it. A pause that a cancel or a paused expiry ends has no end event: the
--   completed_at of the run ends it.
-- - NODE_READY: a node went back to READY with no work (the pause gate of horsies_workflow_promote, a child start
--   that found its workflow not RUNNING, the abandon of a claimed task of a paused run).
-- - NODE_ENQUEUED: a sub-workflow node went ENQUEUED and waits for its child start. A task node that goes ENQUEUED
--   has no event: its task row, written in the same call, holds the time (created_at).
-- - TASK_MOVED: a task of a node moved to history. occurred_at is the terminal_at of the history row; with
--   history_class and task_id it is the locator of that row, as the locator of a phase-2 pending row.
--   terminalization_kind is the kind of the move.
-- NULL: workflow_node_row_id is NULL on a run event (WORKFLOW_*) only; task_id, history_class and
-- terminalization_kind are NULL on every kind but TASK_MOVED. The CHECK refuses every other combination.
-- run_workflow_id is generated (no insert names it; it is not a default, and every other column takes no default):
-- the workflow id of a run event, NULL on a node event. A run event refers to its
-- workflow through it; a node event refers to its node only (the node key holds the workflow id). So the move of a
-- task, which holds the task row, takes no lock on the workflow row: the lock order of 0005_workflow_functions.sql
-- takes workflow rows before task rows, and a workflow cancel holds the workflow row while it waits for task rows.
-- Every writer of a run event holds its workflow row already.
-- Retention: the workflow retention statement deletes the events of each workflow it deletes; the two foreign keys
-- delete the events of a workflow or a node that any other statement deletes.
-- Indexes: the primary key serves the read of one run (workflow_id, then event_id order) and the retention delete;
-- the two partial indexes serve the foreign keys.
CREATE TABLE horsies_workflow_events (
    workflow_id uuid NOT NULL,
    event_id bigint NOT NULL,
    event_kind character varying(16) NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    workflow_node_row_id uuid,
    task_id uuid,
    history_class character varying(64),
    terminalization_kind character varying(32),
    run_workflow_id uuid GENERATED ALWAYS AS (
        CASE WHEN event_kind IN ('WORKFLOW_PAUSED', 'WORKFLOW_RESUMED') THEN workflow_id END
    ) STORED,
    CONSTRAINT horsies_workflow_events_pkey PRIMARY KEY (workflow_id, event_id),
    -- Each arm names IS NOT NULL: without it a NULL makes the arm NULL and the CHECK passes.
    CONSTRAINT horsies_workflow_events_kind_shape CHECK (
        (
            event_kind IN ('WORKFLOW_PAUSED', 'WORKFLOW_RESUMED')
            AND workflow_node_row_id IS NULL AND task_id IS NULL
            AND history_class IS NULL AND terminalization_kind IS NULL
        )
        OR (
            event_kind IN ('NODE_READY', 'NODE_ENQUEUED')
            AND workflow_node_row_id IS NOT NULL AND task_id IS NULL
            AND history_class IS NULL AND terminalization_kind IS NULL
        )
        OR (
            event_kind = 'TASK_MOVED'
            AND workflow_node_row_id IS NOT NULL AND task_id IS NOT NULL
            AND history_class IS NOT NULL
            AND (
                history_class = 'forever'
                OR history_class ~ '^standard_([1-9][0-9]{0,2}|1[0-7][0-9]{2}|18[01][0-9]|182[0-5])d$'
            )
            -- The kinds of horsies_task_history less COMPLETE_FUSED and CANCEL_ADMIN, which the move refuses for a
            -- workflow task.
            AND terminalization_kind IS NOT NULL
            AND terminalization_kind IN (
                'COMPLETE_LOCKED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING',
                'CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH',
                'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH',
                'WORKFLOW_CANCEL_WORKFLOW'
            )
        )
    ),
    CONSTRAINT horsies_workflow_events_run_fkey FOREIGN KEY (run_workflow_id)
        REFERENCES horsies_workflows (id) ON DELETE CASCADE,
    CONSTRAINT horsies_workflow_events_node_fkey FOREIGN KEY (workflow_node_row_id, workflow_id)
        REFERENCES horsies_workflow_tasks (id, workflow_id) ON DELETE CASCADE
);

-- The node foreign key: the delete of a node finds its events.
CREATE INDEX horsies_workflow_events_node_idx
    ON horsies_workflow_events USING btree (workflow_node_row_id)
    WHERE workflow_node_row_id IS NOT NULL;

-- The run foreign key: the delete of a workflow finds its run events.
CREATE INDEX horsies_workflow_events_run_idx
    ON horsies_workflow_events USING btree (run_workflow_id)
    WHERE run_workflow_id IS NOT NULL;

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
-- Move to history: the attempt archive in version 2, the TASK_MOVED event
------------------------------------------------------------------------------------------------------------------------

-- horsies_move_task_to_history(p_task horsies_tasks, ...): move one live task to history.
--
-- Guarantee: inside the caller's transaction, the live row p_task becomes exactly one history row, and every effect
-- of finishing happens together or not at all: the history row (with the rerun input envelope, the attempt
-- archive in version 2 (horsies_encode_attempt_archive of p_task) and the trace id of the live row), the clock
-- exception when the id's UUIDv7 birth is more than 5 s later than p_terminal_at, the phase-2 pending row (deferred
-- kinds of a workflow task), the TASK_MOVED event of a task that a node links (horsies_workflow_events: the node,
-- the task id, the retention class of the row and p_terminal_at, the locator of the history row, and the kind), the
-- TERMINAL window of the task's key reservation, the delete of the attempt rows and of the live row, and the
-- task_done and horsies_task_status notifications (sent at commit). A plain task, and a workflow task that no node
-- links (an orphan), has no event.
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
-- pending row; the event row (its foreign key reads the node row that the move holds, and no workflow row); the
-- reservation row (UPDATE); the attempt rows and the live row (DELETE). Maintenance takes the
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

    -- The node probe above found the node that links the task: its TASK_MOVED event, with the locator of the
    -- history row (class, terminal time, task id).
    IF v_workflow_node_row_id IS NOT NULL THEN
        INSERT INTO horsies_workflow_events (
            workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
            terminalization_kind
        ) VALUES (
            v_workflow_id, nextval('horsies_workflow_event_id'), 'TASK_MOVED', p_terminal_at, v_workflow_node_row_id,
            p_task.id, p_task.retention_class_key, p_terminalization_kind
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
-- exceptions, phase-2 pending rows (EXPIRE_PENDING workflow tasks),
-- TASK_MOVED events (horsies_workflow_events, one per moved task that a node links, in task id order, with the
-- locator of its history row: its retention class, p_terminal_at and its id) and
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
-- rows, event rows, reservation rows, attempt and live row deletes). Node rows are read, not locked FOR UPDATE, as
-- in Python; the foreign keys of the pending rows and of the events take a key share lock on them. No workflow row
-- is locked.
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

    -- The TASK_MOVED event of each moved task that a node links, with the locator of its history row.
    INSERT INTO horsies_workflow_events (
        workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
        terminalization_kind
    )
    SELECT n.workflow_id, nextval('horsies_workflow_event_id'), 'TASK_MOVED', p_terminal_at, n.id, t.id,
           t.retention_class_key, p_terminalization_kind
    FROM horsies_tasks AS t
    JOIN horsies_workflow_tasks AS n ON n.task_id = t.id
    WHERE t.id = ANY(p_task_ids) AND t.is_workflow_task
    ORDER BY t.id;

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

------------------------------------------------------------------------------------------------------------------------
-- Workflow steps that write workflow events
------------------------------------------------------------------------------------------------------------------------

-- Each function below keeps its argument list and its result of migration 1000 (0005_workflow_functions.sql) and is
-- replaced in place. The change of each is the event insert into horsies_workflow_events in the statement that
-- changes the state; the rest of each body is the body of migration 1000.

-- The failure policy of a workflow after one of its nodes went FAILED. p_error is the error of this failure, as the
-- bare error object text (NULL: no readable error): the err slot of the node's stored result, or for a node whose
-- child workflow ended FAILED, CANCELLED or EXPIRED the error of horsies_workflow_child_node_result. Returns TRUE when
-- progression goes on (on_error fail, or a workflow that is not RUNNING with fail), FALSE when it stops (on_error
-- pause).
-- - fail: the workflow error becomes the err slot of the first FAILED node by index, only while RUNNING; a node with
--   no readable error keeps the stored error. p_error is not used.
-- - pause: RUNNING to PAUSED with p_error (NULL keeps the stored error; a second failure keeps the first error: the
--   write needs RUNNING), and every RUNNING descendant to PAUSED. Claimed tasks of the paused tree are abandoned
--   later, at their start or claim-time check.
-- Events (horsies_workflow_events, at now()): with pause, one WORKFLOW_PAUSED for the workflow, then one for each
-- descendant that it paused, in (depth, id) order. A workflow that is not RUNNING gets none.
-- Precondition: the caller holds the workflow row FOR UPDATE and wrote the node FAILED.
-- Python: _handle_workflow_task_failure (core/workflows/engine.py:4002-4100), which stores the error it is given.
-- Locks: the workflow row (held), then the RUNNING descendant rows level by level in (depth, id) order, then the
-- event rows (their foreign key reads the workflow rows that this call holds).
CREATE OR REPLACE FUNCTION horsies_workflow_record_failure(p_workflow_id uuid, p_error text) RETURNS boolean
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_on_error text;
    v_error text;
    v_paused uuid[];
BEGIN
    SELECT w.on_error INTO v_on_error FROM horsies_workflows w WHERE w.id = p_workflow_id;
    CASE v_on_error
        WHEN 'fail' THEN
            SELECT horsies_workflow_err_slot(n.result) INTO v_error
            FROM horsies_workflow_tasks n
            WHERE n.workflow_id = p_workflow_id AND n.status = 'FAILED'
            ORDER BY n.task_index
            LIMIT 1;
            UPDATE horsies_workflows
            SET error = coalesce(v_error, error), updated_at = now()
            WHERE id = p_workflow_id AND status = 'RUNNING';
            RETURN TRUE;
        WHEN 'pause' THEN
            UPDATE horsies_workflows
            SET status = 'PAUSED', error = coalesce(p_error, error), updated_at = now()
            WHERE id = p_workflow_id AND status = 'RUNNING';
            IF FOUND THEN
                v_paused := ARRAY[p_workflow_id]
                    || horsies_workflow_move_descendants(p_workflow_id, 'RUNNING', 'PAUSED');
                INSERT INTO horsies_workflow_events (
                    workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
                    terminalization_kind
                )
                SELECT p.id, nextval('horsies_workflow_event_id'), 'WORKFLOW_PAUSED', now(), NULL, NULL, NULL, NULL
                FROM unnest(v_paused) WITH ORDINALITY AS p(id, position)
                ORDER BY p.position;
            END IF;
            RETURN FALSE;
    END CASE;
END;
$$;

-- One level of promotion: the join verdict of every PENDING node of p_workflow_id that depends on a node of
-- p_sources or is in p_candidates, written in two updates. Returns the indexes that went SKIPPED (an impossible
-- join, or join all with a FAILED or SKIPPED dependency and no allow_failed_deps) and those that went ENQUEUED (a met
-- join). A node with workflow_ctx_from waits until its context nodes are terminal. A candidate with no dependencies
-- is judged like any other: 0 of 0 dependencies are terminal, so join all is met and the node is ENQUEUED (it
-- recovers a root that a start left PENDING); it is never a dependent of p_sources. The verdict table is Python's
-- _evaluate_join and _plan_level (core/workflows/engine.py:2223-2344); the ctx gate is
-- try_make_ready_and_enqueue's (core/workflows/engine.py:2671-2683). Python's try_make_ready_and_enqueue leaves a
-- node with no dependencies PENDING; the core's rule (enqueue it) is the decided one.
-- Events (horsies_workflow_events, at now()): one NODE_ENQUEUED for each sub-workflow node that goes ENQUEUED, in
-- index order: the node waits for its child start. A task node that goes ENQUEUED has no event (its task row,
-- written in the same promotion, holds the time), and a node that goes SKIPPED has none (completed_at holds it).
-- Precondition: the caller holds the workflow row FOR UPDATE and the workflow is RUNNING.
-- Locks: the node rows that the two updates write (the workflow row is held by the caller), then the event rows.
CREATE OR REPLACE FUNCTION horsies_workflow_promote_level(
    p_workflow_id uuid, p_sources integer[], p_candidates integer[],
    OUT skipped integer[], OUT enqueued integer[]
)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
BEGIN
    WITH evaluated AS (
        SELECT d.id,
               CASE d.join_type
                   WHEN 'all' THEN CASE WHEN c.terminal = c.total THEN 'MET' ELSE 'WAIT' END
                   WHEN 'any' THEN CASE
                       WHEN c.completed >= 1 THEN 'MET'
                       WHEN c.terminal = c.total THEN 'IMPOSSIBLE'
                       ELSE 'WAIT'
                   END
                   WHEN 'quorum' THEN CASE
                       WHEN c.completed >= d.min_success THEN 'MET'
                       WHEN c.completed + (c.total - c.terminal) < d.min_success THEN 'IMPOSSIBLE'
                       ELSE 'WAIT'
                   END
               END AS join_verdict,
               d.join_type = 'all' AND c.failed_or_skipped > 0 AND NOT d.allow_failed_deps AS skip_after_ready,
               d.workflow_ctx_from IS NOT NULL
                   AND c.ctx_terminal < cardinality(d.workflow_ctx_from) AS ctx_waiting
        FROM horsies_workflow_tasks d
        CROSS JOIN LATERAL (
            SELECT cardinality(d.dependencies) AS total,
                   count(*) FILTER (WHERE s.status IN ('COMPLETED', 'FAILED', 'SKIPPED')) AS terminal,
                   count(*) FILTER (WHERE s.status = 'COMPLETED') AS completed,
                   count(*) FILTER (WHERE s.status IN ('FAILED', 'SKIPPED')) AS failed_or_skipped,
                   count(*) FILTER (
                       WHERE s.status IN ('COMPLETED', 'FAILED', 'SKIPPED')
                         AND s.node_id = ANY(d.workflow_ctx_from)
                   ) AS ctx_terminal
            FROM horsies_workflow_tasks s
            WHERE s.workflow_id = d.workflow_id
              AND s.task_index = ANY(d.dependencies)
        ) c
        WHERE d.workflow_id = p_workflow_id
          AND d.status = 'PENDING'
          AND (d.dependencies && p_sources OR d.task_index = ANY(p_candidates))
    ),
    skip_write AS (
        UPDATE horsies_workflow_tasks n
        SET status = 'SKIPPED', completed_at = now()
        FROM evaluated e
        WHERE n.id = e.id
          AND n.status = 'PENDING'
          AND (e.join_verdict = 'IMPOSSIBLE' OR (e.join_verdict = 'MET' AND NOT e.ctx_waiting AND e.skip_after_ready))
        RETURNING n.task_index
    ),
    enqueue_write AS (
        UPDATE horsies_workflow_tasks n
        SET status = 'ENQUEUED', started_at = now()
        FROM evaluated e
        WHERE n.id = e.id
          AND n.status = 'PENDING'
          AND e.join_verdict = 'MET' AND NOT e.ctx_waiting AND NOT e.skip_after_ready
        RETURNING n.id, n.task_index, n.is_subworkflow
    ),
    enqueue_event AS (
        INSERT INTO horsies_workflow_events (
            workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
            terminalization_kind
        )
        SELECT p_workflow_id, nextval('horsies_workflow_event_id'), 'NODE_ENQUEUED', now(), q.id, NULL, NULL, NULL
        FROM enqueue_write q
        WHERE q.is_subworkflow
        ORDER BY q.task_index
    )
    SELECT (SELECT coalesce(array_agg(s.task_index ORDER BY s.task_index), ARRAY[]::integer[]) FROM skip_write s),
           (SELECT coalesce(array_agg(q.task_index ORDER BY q.task_index), ARRAY[]::integer[]) FROM enqueue_write q)
    INTO skipped, enqueued;
END;
$$;

-- Promotion inside one held workflow lock, with the whole skip cascade. Inputs: p_ready_indexes are READY nodes to
-- enqueue as they are (resume, start roots); p_candidate_indexes are PENDING nodes to judge (resume); p_source_indexes
-- are nodes that just went terminal, whose PENDING dependents are judged (phase 2, child completion). Steps:
-- 1. READY nodes of p_ready_indexes to ENQUEUED (started_at = now()), with one NODE_ENQUEUED event for each
--    sub-workflow node among them.
-- 2. Levels of horsies_workflow_promote_level; the nodes that a level skipped, and the nodes whose build fails
--    (horsies_workflow_unbuildable_nodes), are the sources of the next level. A build failure writes the node FAILED
--    with its engine error and runs the failure policy: with on_error fail every failure of the level goes FAILED;
--    with on_error pause only the lowest index goes FAILED and the workflow pauses. The levels end when a level
--    makes no node terminal; each level makes at least one node terminal, so there are at most node count + 1.
-- 3. A pause in step 2: every node that this call moved to ENQUEUED and that has no task and no child goes back to
--    READY with started_at NULL and one NODE_READY event; nothing is minted and no child start is owed (the pause
--    gate).
-- 4. Otherwise: the task rows of the ENQUEUED task nodes (horsies_workflow_node_tasks_insert, dated now(): the start
--    of the transaction, as started_at in step 1 and Python's workflow task insert), and one CHILD_START row (with
--    its SOURCE rows) per ENQUEUED sub-workflow node.
-- Events (horsies_workflow_events, at now(), in the order of the steps): NODE_ENQUEUED of step 1 and of each level
-- (horsies_workflow_promote_level), WORKFLOW_PAUSED of a pause (horsies_workflow_record_failure), NODE_READY of
-- step 3. A task node that goes ENQUEUED has no event: its task row holds the time.
-- Does nothing for a workflow that is not RUNNING.
-- Precondition: the caller holds the workflow row FOR UPDATE.
-- Lock order: workflow row (held), node rows of each level, event rows, new task rows.
-- Python: _process_dependents, _promote_dependents_level, _write_promotions, _enqueue_promoted,
-- try_make_ready_and_enqueue, enqueue_workflow_task (core/workflows/engine.py:634-710, 2062-2738).
-- Returns: the CHILD_START and SOURCE rows of the owed child starts (no OUTCOME row).
CREATE OR REPLACE FUNCTION horsies_workflow_promote(
    p_workflow_id uuid,
    p_source_indexes integer[],
    p_candidate_indexes integer[],
    p_ready_indexes integer[]
) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    c_max_levels CONSTANT integer := 10002;
    v_wf record;
    v_sources integer[] := coalesce(p_source_indexes, ARRAY[]::integer[]);
    v_candidates integer[] := coalesce(p_candidate_indexes, ARRAY[]::integer[]);
    v_new integer[] := ARRAY[]::integer[];
    v_enqueued integer[] := ARRAY[]::integer[];
    v_failed integer[];
    v_level record;
    v_levels integer := 0;
BEGIN
    SELECT w.status, w.on_error
    INTO v_wf
    FROM horsies_workflows w
    WHERE w.id = p_workflow_id;
    IF NOT FOUND OR v_wf.status <> 'RUNNING' THEN
        RETURN;
    END IF;

    IF cardinality(p_ready_indexes) > 0 THEN
        WITH moved AS (
            UPDATE horsies_workflow_tasks n
            SET status = 'ENQUEUED', started_at = now()
            WHERE n.workflow_id = p_workflow_id AND n.task_index = ANY(p_ready_indexes) AND n.status = 'READY'
            RETURNING n.id, n.task_index, n.is_subworkflow
        ),
        moved_event AS (
            INSERT INTO horsies_workflow_events (
                workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
                terminalization_kind
            )
            SELECT p_workflow_id, nextval('horsies_workflow_event_id'), 'NODE_ENQUEUED', now(), moved.id, NULL,
                   NULL, NULL
            FROM moved
            WHERE moved.is_subworkflow
            ORDER BY moved.task_index
        )
        SELECT coalesce(array_agg(moved.task_index ORDER BY moved.task_index), ARRAY[]::integer[])
        INTO v_new FROM moved;
    END IF;

    LOOP
        v_levels := v_levels + 1;
        IF v_levels > c_max_levels THEN
            RAISE EXCEPTION 'workflow % promotion passed % levels', p_workflow_id, c_max_levels
                USING ERRCODE = 'program_limit_exceeded';
        END IF;
        IF cardinality(v_new) > 0 THEN
            v_enqueued := v_enqueued || v_new;
            v_failed := horsies_workflow_fail_unbuildable(p_workflow_id, v_new, v_wf.on_error);
            IF cardinality(v_failed) > 0 THEN
                IF NOT horsies_workflow_record_failure(
                    p_workflow_id,
                    (SELECT horsies_workflow_err_slot(n.result) FROM horsies_workflow_tasks n
                     WHERE n.workflow_id = p_workflow_id AND n.task_index = v_failed[1])
                ) THEN
                    WITH back_to_ready AS (
                        UPDATE horsies_workflow_tasks n
                        SET status = 'READY', started_at = NULL
                        WHERE n.workflow_id = p_workflow_id
                          AND n.task_index = ANY(v_enqueued)
                          AND n.status = 'ENQUEUED'
                          AND n.task_id IS NULL
                          AND n.sub_workflow_id IS NULL
                        RETURNING n.id, n.task_index
                    )
                    INSERT INTO horsies_workflow_events (
                        workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
                        terminalization_kind
                    )
                    SELECT p_workflow_id, nextval('horsies_workflow_event_id'), 'NODE_READY', now(),
                           back_to_ready.id, NULL, NULL, NULL
                    FROM back_to_ready
                    ORDER BY back_to_ready.task_index;
                    RETURN;
                END IF;
                v_sources := v_sources || v_failed;
            END IF;
        END IF;
        EXIT WHEN cardinality(v_sources) = 0 AND cardinality(v_candidates) = 0;
        SELECT * INTO v_level FROM horsies_workflow_promote_level(p_workflow_id, v_sources, v_candidates);
        v_new := v_level.enqueued;
        v_sources := v_level.skipped;
        v_candidates := ARRAY[]::integer[];
    END LOOP;

    PERFORM horsies_workflow_node_tasks_insert(p_workflow_id, v_enqueued, now());
    RETURN QUERY SELECT * FROM horsies_workflow_child_start_rows(p_workflow_id, v_enqueued);
END;
$$;

-- Pauses a RUNNING workflow and its RUNNING descendants, in one statement in autocommit. Steps: RUNNING to PAUSED
-- for the workflow (its row lock); the RUNNING descendants, level by level, locked in (depth, id) order and paused
-- (horsies_workflow_move_descendants); every CLAIMED task behind an ENQUEUED or RUNNING node of the paused tree is
-- abandoned (horsies_abandon_nodes_of_paused_workflows: PAUSE_ABANDON_WORKFLOW, task rows with SKIP LOCKED); the
-- nodes of the abandoned tasks go back to READY with no task. The status trigger sends one workflow_done per paused
-- workflow at commit; this function sends none.
-- Events (horsies_workflow_events, at now()): one WORKFLOW_PAUSED per paused workflow, the root first, then the
-- TASK_MOVED of each abandoned task (written by the move), then one NODE_READY per node that went back to READY.
-- NOT_RUNNING and NOT_FOUND write none.
-- Outcomes: PAUSED (paused_workflow_ids, the root first; abandoned_task_ids); NOT_RUNNING (observed_status; nothing
-- written); NOT_FOUND.
-- Lock order: root row, descendant rows in (depth, id) order, task rows (SKIP LOCKED), node rows, event rows.
-- Python: pause_workflow (core/workflows/lifecycle.py:886-994) with cascade_pause_to_children (:996-1030).
-- Returns: one row (outcome, observed_status, paused_workflow_ids, abandoned_task_ids).
CREATE OR REPLACE FUNCTION horsies_workflow_pause(p_workflow_id uuid)
    RETURNS TABLE (outcome text, observed_status text, paused_workflow_ids uuid[], abandoned_task_ids uuid[])
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_paused uuid[];
    v_abandoned uuid[];
    v_status text;
BEGIN
    PERFORM horsies_workflow_require(p_workflow_id IS NOT NULL, 'workflow id must not be NULL');
    UPDATE horsies_workflows w SET status = 'PAUSED', updated_at = now()
    WHERE w.id = p_workflow_id AND w.status = 'RUNNING';
    IF NOT FOUND THEN
        SELECT w.status INTO v_status FROM horsies_workflows w WHERE w.id = p_workflow_id;
        RETURN QUERY SELECT CASE WHEN v_status IS NULL THEN 'NOT_FOUND' ELSE 'NOT_RUNNING' END,
            v_status, NULL::uuid[], NULL::uuid[];
        RETURN;
    END IF;
    v_paused := ARRAY[p_workflow_id] || horsies_workflow_move_descendants(p_workflow_id, 'RUNNING', 'PAUSED');
    INSERT INTO horsies_workflow_events (
        workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
        terminalization_kind
    )
    SELECT p.id, nextval('horsies_workflow_event_id'), 'WORKFLOW_PAUSED', now(), NULL, NULL, NULL, NULL
    FROM unnest(v_paused) WITH ORDINALITY AS p(id, position)
    ORDER BY p.position;
    SELECT coalesce(array_agg(o.task_id ORDER BY o.task_id), ARRAY[]::uuid[]) INTO v_abandoned
    FROM horsies_abandon_nodes_of_paused_workflows(v_paused) AS o
    WHERE o.outcome = 'APPLIED';
    WITH back_to_ready AS (
        UPDATE horsies_workflow_tasks n
        SET status = 'READY', task_id = NULL, started_at = NULL
        WHERE n.task_id = ANY(v_abandoned) AND n.status IN ('ENQUEUED', 'RUNNING')
        RETURNING n.id, n.workflow_id
    )
    INSERT INTO horsies_workflow_events (
        workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
        terminalization_kind
    )
    SELECT back_to_ready.workflow_id, nextval('horsies_workflow_event_id'), 'NODE_READY', now(), back_to_ready.id,
           NULL, NULL, NULL
    FROM back_to_ready
    ORDER BY back_to_ready.id;
    RETURN QUERY SELECT 'PAUSED'::text, 'PAUSED'::text, v_paused, v_abandoned;
END;
$$;

-- Resumes a PAUSED workflow and its PAUSED descendants, in the caller's transaction (BEGIN; this; owed child
-- starts; COMMIT). Steps: PAUSED to RUNNING for the workflow and, level by level in (depth, id) order, its PAUSED
-- descendants; for each resumed workflow, in (depth, id) order, promotion with its PENDING nodes as candidates and
-- its READY nodes enqueued as they are (task rows minted; child starts owed for sub-workflow nodes); then the
-- completion check of each resumed workflow, deepest first; a resumed workflow that ends advances its parent node
-- in this statement when the parent is resumed too (horsies_workflow_child_finish), else the OUTCOME row names it.
-- Events (horsies_workflow_events, at now()): one WORKFLOW_RESUMED per resumed workflow, the root first, then the
-- events of the promotions (horsies_workflow_promote). NOT_PAUSED and NOT_FOUND write none.
-- Dispositions: RESUMED; NOT_PAUSED (detail: the observed status; nothing written); NOT_FOUND.
-- The tree recovery pass after the commit is the caller's (RESUME_RECOVERY_FAILED is reported there).
-- Lock order: root row, descendant rows in (depth, id) order, event rows, node rows, new task rows.
-- Python: resume_workflow (core/workflows/lifecycle.py:1084-1263) with cascade_resume_to_children (:1265-1358).
-- Returns: the OUTCOME row (advance_child_workflow_id when the resumed workflow ended and has a parent) and the
-- owed CHILD_START and SOURCE rows.
CREATE OR REPLACE FUNCTION horsies_workflow_resume(p_workflow_id uuid) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_resumed uuid[];
    v_status text;
    v_workflow record;
BEGIN
    PERFORM horsies_workflow_require(p_workflow_id IS NOT NULL, 'workflow id must not be NULL');
    UPDATE horsies_workflows w SET status = 'RUNNING', updated_at = now()
    WHERE w.id = p_workflow_id AND w.status = 'PAUSED';
    IF NOT FOUND THEN
        SELECT w.status INTO v_status FROM horsies_workflows w WHERE w.id = p_workflow_id;
        RETURN NEXT horsies_workflow_outcome_row(
            CASE WHEN v_status IS NULL THEN 'NOT_FOUND' ELSE 'NOT_PAUSED' END, p_workflow_id, FALSE, v_status
        );
        RETURN;
    END IF;
    v_resumed := ARRAY[p_workflow_id] || horsies_workflow_move_descendants(p_workflow_id, 'PAUSED', 'RUNNING');
    INSERT INTO horsies_workflow_events (
        workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
        terminalization_kind
    )
    SELECT p.id, nextval('horsies_workflow_event_id'), 'WORKFLOW_RESUMED', now(), NULL, NULL, NULL, NULL
    FROM unnest(v_resumed) WITH ORDINALITY AS p(id, position)
    ORDER BY p.position;

    FOR v_workflow IN
        SELECT w.id FROM horsies_workflows w WHERE w.id = ANY(v_resumed) ORDER BY w.depth, w.id
    LOOP
        RETURN QUERY SELECT * FROM horsies_workflow_promote(
            v_workflow.id,
            NULL,
            ARRAY(SELECT n.task_index FROM horsies_workflow_tasks n
                  WHERE n.workflow_id = v_workflow.id AND n.status = 'PENDING' ORDER BY n.task_index),
            ARRAY(SELECT n.task_index FROM horsies_workflow_tasks n
                  WHERE n.workflow_id = v_workflow.id AND n.status = 'READY' ORDER BY n.task_index)
        );
    END LOOP;

    -- Deepest first. A workflow of the set that is COMPLETED or FAILED here ended in this statement (each was PAUSED
    -- before it), by its own check or by a deeper child's advance: its parent node is advanced.
    FOR v_workflow IN
        SELECT w.id FROM horsies_workflows w
        WHERE w.id = ANY(v_resumed) AND w.id <> p_workflow_id
        ORDER BY w.depth DESC, w.id
    LOOP
        PERFORM horsies_workflow_try_complete(v_workflow.id);
        IF EXISTS (
            SELECT 1 FROM horsies_workflows w WHERE w.id = v_workflow.id AND w.status IN ('COMPLETED', 'FAILED')
        ) THEN
            RETURN QUERY SELECT * FROM horsies_workflow_child_finish(v_workflow.id) AS r WHERE r.row_kind <> 'OUTCOME';
        END IF;
    END LOOP;
    PERFORM horsies_workflow_try_complete(p_workflow_id);
    RETURN NEXT horsies_workflow_outcome_row('RESUMED', p_workflow_id, TRUE, NULL);
END;
$$;


-- Step function: the owed child start of one sub-workflow node, in the transaction of the step that owes it (BEGIN;
-- the step function; the host build callback and this call for each CHILD_START row; COMMIT). The parent node, the
-- child rows and the link commit together.
-- p_outcome_kind (a kind text plus the arguments of each kind; each kind refuses the arguments of the other, 22023):
-- - TREE: the child tree that the core built, in the arrays of horsies_workflow_start: the child row first, with the
--   parent pair (p_parent_workflow_id, p_parent_task_index) and the depth of the parent workflow plus one, then the
--   pre-built children of its sub-workflow roots; every node of the tree; the failed-node records (a sub-workflow
--   root of the tree whose own child could not be built). p_sent_at is the call time of the child start. Every
--   workflow row of the child tree copies the trace pair (trace_carrier, trace_id) of the parent workflow row, so
--   the tasks of a sub-workflow at any depth carry the trace of the top-level start.
-- - FAILURE: p_failure_result, the error envelope of a child that could not be built (a definition that does not
--   load, a failing build callback, an args_from key or value that the callback refuses, the depth limit). It must
--   be an error envelope (22023 otherwise).
-- Steps:
-- 1. Lock the parent workflow row FOR UPDATE (the step function of this transaction holds it already), then the
--    node row.
-- 2. The node is not an ENQUEUED sub-workflow node with no child: STALE, nothing written.
-- 3. The parent workflow is not RUNNING (a failure earlier in the same step paused it): the node goes back to READY
--    with started_at NULL and one NODE_READY event (horsies_workflow_events, at now()), so the resume or recovery
--    case 1.5 starts it later. WORKFLOW_NOT_RUNNING (detail: the status). Without this the node would stay
--    ENQUEUED with no child, which no recovery case selects.
-- 4. TREE: insert the child row (RUNNING, the root id of the parent's tree; horsies_workflow_start_top_row), link
--    the node (RUNNING, sub_workflow_id), then write the rest of the child tree as a start does
--    (horsies_workflow_start_tree: depth first, the roots of each workflow in index order while it is RUNNING, the
--    parent advance of a workflow that ends, then the task rows of the READY nodes of a workflow that is still
--    RUNNING). A child that is COMPLETED or FAILED now advances the node in this statement
--    (horsies_workflow_child_finish), as Python's in-session drain does. STARTED.
-- 5. FAILURE: the node goes FAILED with the envelope and completed_at, then horsies_workflow_progress for it
--    (failure policy, promotion of its dependents, completion). NODE_FAILED.
-- A refused argument raises 22023; a child id that exists already raises unique_violation (23505): the core mints
-- it, so a conflict is a defect. Nothing is written in either case.
-- Returns the OUTCOME row (workflow_id: the parent workflow; workflow_status: its status after the call;
-- advance_child_workflow_id: the parent workflow when this call ended it and it has a parent, for the advance after
-- the commit) and the CHILD_START and SOURCE rows that this call owes in turn.
-- Lock order: parent workflow row, node row, the new child rows, then the progression locks (node rows; under
-- on_error pause the RUNNING descendant rows in (depth, id) order).
-- Python: enqueue_subworkflow_task after its READY to ENQUEUED write (core/workflows/engine.py:1068-1627, with
-- INSERT_CHILD_WORKFLOW_SQL and LINK_SUB_WORKFLOW_SQL, core/workflows/sql.py:260-276), and _fail_enqueued_task
-- (core/workflows/engine.py:472-555).
CREATE OR REPLACE FUNCTION horsies_workflow_child_start(
    p_parent_workflow_id uuid,
    p_parent_task_index integer,
    p_outcome_kind text,
    p_failure_result text,
    p_sent_at timestamp with time zone,
    p_workflow_ids uuid[],
    p_names text[],
    p_definition_keys text[],
    p_on_errors text[],
    p_output_task_indexes integer[],
    p_success_policies text[],
    p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[],
    p_depths integer[],
    p_node_workflow_ids uuid[],
    p_node_task_indexes integer[],
    p_node_ids text[],
    p_node_task_names text[],
    p_node_inputs text[],
    p_node_queue_names text[],
    p_node_priorities integer[],
    p_node_dependencies text[],
    p_node_args_from text[],
    p_node_workflow_ctx_from text[],
    p_node_allow_failed_deps boolean[],
    p_node_join_types text[],
    p_node_min_successes integer[],
    p_node_task_options text[],
    p_node_max_retries integer[],
    p_node_executions text[],
    p_node_good_untils timestamp with time zone[],
    p_node_retention_class_keys text[],
    p_node_is_subworkflow boolean[],
    p_node_sub_workflow_names text[],
    p_node_sub_definition_keys text[],
    p_node_sub_workflow_params text[],
    p_failed_workflow_ids uuid[],
    p_failed_task_indexes integer[],
    p_failed_results text[]
) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_parent record;
    v_node record;
    v_child uuid := p_workflow_ids[1];
BEGIN
    PERFORM horsies_workflow_require(
        p_parent_workflow_id IS NOT NULL AND p_parent_task_index IS NOT NULL,
        'the parent workflow id and task index must not be NULL'
    );
    PERFORM horsies_workflow_require(
        p_outcome_kind IN ('TREE', 'FAILURE'), 'the outcome kind must be TREE or FAILURE'
    );
    CASE p_outcome_kind
        WHEN 'TREE' THEN
            PERFORM horsies_workflow_require(
                p_failure_result IS NULL AND p_sent_at IS NOT NULL,
                'a TREE child start takes sent_at and no failure result'
            );
            PERFORM horsies_workflow_tree_check(
                p_parent_workflow_id, p_parent_task_index, p_depths[1],
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_node_workflow_ids, p_node_task_indexes,
                p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names, p_node_priorities,
                p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from, p_node_allow_failed_deps,
                p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries, p_node_executions,
                p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids, p_failed_task_indexes,
                p_failed_results
            );
        WHEN 'FAILURE' THEN
            PERFORM horsies_workflow_require(
                p_sent_at IS NULL AND num_nonnulls(
                    p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                    p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_node_workflow_ids, p_node_task_indexes,
                    p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names, p_node_priorities,
                    p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from, p_node_allow_failed_deps,
                    p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries, p_node_executions,
                    p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                    p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids,
                    p_failed_task_indexes, p_failed_results
                ) = 0,
                'a FAILURE child start takes the failure result alone'
            );
            PERFORM horsies_workflow_require(
                horsies_workflow_result_kind(p_failure_result) = 'ERROR',
                'the failure result of a child start must be an error envelope'
            );
    END CASE;

    SELECT w.status, w.depth, coalesce(w.root_workflow_id, w.id) AS root_workflow_id, w.trace_carrier, w.trace_id
    INTO v_parent
    FROM horsies_workflows w
    WHERE w.id = p_parent_workflow_id
    FOR UPDATE;
    SELECT n.id, n.status, n.is_subworkflow, n.sub_workflow_id INTO v_node
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_parent_workflow_id AND n.task_index = p_parent_task_index
    FOR UPDATE;
    IF v_node.id IS NULL OR NOT v_node.is_subworkflow OR v_node.status <> 'ENQUEUED'
       OR v_node.sub_workflow_id IS NOT NULL THEN
        RETURN NEXT horsies_workflow_outcome_row('STALE', p_parent_workflow_id, FALSE, v_node.status::text);
        RETURN;
    END IF;
    IF v_parent.status <> 'RUNNING' THEN
        UPDATE horsies_workflow_tasks n SET status = 'READY', started_at = NULL WHERE n.id = v_node.id;
        INSERT INTO horsies_workflow_events (
            workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
            terminalization_kind
        )
        VALUES (
            p_parent_workflow_id, nextval('horsies_workflow_event_id'), 'NODE_READY', now(), v_node.id, NULL, NULL,
            NULL
        );
        RETURN NEXT horsies_workflow_outcome_row(
            'WORKFLOW_NOT_RUNNING', p_parent_workflow_id, FALSE, v_parent.status::text
        );
        RETURN;
    END IF;

    CASE p_outcome_kind
        WHEN 'TREE' THEN
            PERFORM horsies_workflow_require(
                p_depths[1] = v_parent.depth + 1,
                'the child row must have the depth of its parent workflow plus one'
            );
            IF NOT horsies_workflow_start_top_row(
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_parent.root_workflow_id,
                v_parent.trace_carrier, v_parent.trace_id
            ) THEN
                RAISE EXCEPTION 'child workflow % of workflow % node % exists already', v_child, p_parent_workflow_id,
                    p_parent_task_index USING ERRCODE = 'unique_violation';
            END IF;
            UPDATE horsies_workflow_tasks n SET status = 'RUNNING', sub_workflow_id = v_child WHERE n.id = v_node.id;
            RETURN QUERY SELECT * FROM horsies_workflow_start_tree(
                v_child, 1, v_child,
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_parent.root_workflow_id,
                v_parent.trace_carrier, v_parent.trace_id,
                p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs,
                p_node_queue_names, p_node_priorities, p_node_dependencies, p_node_args_from,
                p_node_workflow_ctx_from, p_node_allow_failed_deps, p_node_join_types, p_node_min_successes,
                p_node_task_options, p_node_max_retries, p_node_executions, p_node_good_untils,
                p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids, p_failed_task_indexes,
                p_failed_results
            );
            IF EXISTS (SELECT 1 FROM horsies_workflows w WHERE w.id = v_child AND w.status IN ('COMPLETED', 'FAILED'))
            THEN
                RETURN QUERY SELECT * FROM horsies_workflow_child_finish(v_child) AS r WHERE r.row_kind <> 'OUTCOME';
            END IF;
            RETURN NEXT horsies_workflow_outcome_row('STARTED', p_parent_workflow_id, TRUE, NULL);
        WHEN 'FAILURE' THEN
            UPDATE horsies_workflow_tasks n
            SET status = 'FAILED', result = p_failure_result, completed_at = now()
            WHERE n.id = v_node.id;
            RETURN QUERY SELECT * FROM horsies_workflow_progress(p_parent_workflow_id, p_parent_task_index, 'READ');
            RETURN NEXT horsies_workflow_outcome_row('NODE_FAILED', p_parent_workflow_id, TRUE, NULL);
    END CASE;
END;
$$;

-- The claim-time workflow check of one claim pass, in one statement in autocommit after the claim committed.
-- Arguments: the claimed task ids, the claim generation of each (claimed_at), the worker. Refused (22023): a NULL
-- array, element or worker id, arrays of different lengths, a repeated id, more than 1000 ids (the bound of one
-- claim pass, as horsies_release_claims).
-- Steps:
-- 1. The lookup: the workflow of each claimed task (the node by task_id, the workflow by id), unlocked. Two index
--    probes per task, bounded by the claim pass. A task with no node, or whose workflow is neither PAUSED nor
--    CANCELLED, is dispatched; when there is none of the other kind, the statement ends here.
-- 2. The PAUSED or CANCELLED workflows of the batch, locked FOR SHARE in (depth, id) order, and their status read
--    again under the lock: a resume or a pause cannot commit meanwhile. A workflow that a resume made RUNNING since
--    the lookup has its tasks dispatched.
-- 3. The task rows of the blocked tasks, locked FOR UPDATE in id order in one pass (the batch rule).
-- 4. Tasks of a PAUSED workflow: abandoned (horsies_abandon_owned_nodes, PAUSE_ABANDON_CLAIM_BATCH, fenced to this
--    worker and each claim generation; the move writes the TASK_MOVED event of each); the node of each abandoned
--    task goes back to READY with no task, with one NODE_READY event (horsies_workflow_events, at now()).
-- 5. Tasks of a CANCELLED workflow: cancelled (horsies_cancel_owned_nodes, WORKFLOW_CANCEL_CLAIM_BATCH, same fence);
--    the node of each cancelled task goes SKIPPED with completed_at.
-- A blocked task is never dispatched, whether its move applied or found the row moved on (a lost claim, a task
-- already terminal): that row belongs to its current owner.
-- The guard of v61 (no workflow PAUSED or CANCELLED at all) is not used: it needs an index on the workflow status,
-- which the baseline does not have, and any CANCELLED workflow kept by retention makes it pass.
-- Returns one row per input id, in input order: DISPATCH or BLOCKED.
-- Lock order: workflow rows FOR SHARE in (depth, id) order, task rows in id order (no task advisory lock), node rows,
-- event rows.
-- Python 0.7.0 (`v0.7.0` package): _filter_nonrunnable_workflow_tasks (core/worker/claiming.py:503-620) with
-- horsies_find_non_runnable_workflow_tasks, horsies_abandon_owned_nodes, RESET_PAUSED_WORKFLOW_TASKS_SQL,
-- horsies_cancel_owned_nodes and SKIP_CANCELLED_WORKFLOW_TASKS_SQL (core/worker/sql.py:228-233, :284-297).
CREATE OR REPLACE FUNCTION horsies_workflow_claimed_tasks_settle(
    p_task_ids uuid[], p_claimed_ats timestamp with time zone[], p_worker_id text
) RETURNS TABLE (task_id uuid, disposition text)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
#variable_conflict use_column
DECLARE
    c_max_tasks CONSTANT integer := 1000;
    v_lookup_workflows uuid[];
    v_paused_workflows uuid[];
    v_cancelled_workflows uuid[];
    v_paused_ids uuid[];
    v_paused_ats timestamp with time zone[];
    v_cancelled_ids uuid[];
    v_cancelled_ats timestamp with time zone[];
    v_abandoned uuid[];
    v_cancelled uuid[];
BEGIN
    PERFORM horsies_workflow_require(
        p_task_ids IS NOT NULL AND p_claimed_ats IS NOT NULL AND p_worker_id IS NOT NULL,
        'the task ids, claim generations and worker id must not be NULL'
    );
    PERFORM horsies_workflow_require(
        cardinality(p_task_ids) = cardinality(p_claimed_ats)
        AND array_position(p_task_ids, NULL) IS NULL AND array_position(p_claimed_ats, NULL) IS NULL,
        'the task id and claim generation arrays differ in length or hold NULL'
    );
    PERFORM horsies_workflow_require(cardinality(p_task_ids) <= c_max_tasks, 'at most 1000 task ids per call');
    PERFORM horsies_workflow_require(
        cardinality(p_task_ids) = (SELECT count(DISTINCT u.id) FROM unnest(p_task_ids) AS u(id)),
        'a task id repeats'
    );

    -- One probe of the node by task_id and one of its workflow by primary key per task: the LATERAL with OFFSET 0
    -- keeps each workflow read tied to its node, so no plan reads the workflows by status.
    SELECT array_agg(DISTINCT w.id) INTO v_lookup_workflows
    FROM horsies_workflow_tasks n
    CROSS JOIN LATERAL (
        SELECT workflow.id, workflow.status
        FROM horsies_workflows workflow
        WHERE workflow.id = n.workflow_id
        OFFSET 0
    ) AS w
    WHERE n.task_id = ANY(p_task_ids)
      AND w.status IN ('PAUSED', 'CANCELLED');
    IF v_lookup_workflows IS NULL THEN
        RETURN QUERY
        SELECT u.id, 'DISPATCH'::text FROM unnest(p_task_ids) WITH ORDINALITY AS u(id, position) ORDER BY u.position;
        RETURN;
    END IF;

    WITH locked AS (
        SELECT w.id, w.status
        FROM horsies_workflows w
        WHERE w.id = ANY(v_lookup_workflows)
        ORDER BY w.depth, w.id
        FOR SHARE
    )
    SELECT array_agg(l.id) FILTER (WHERE l.status = 'PAUSED'),
           array_agg(l.id) FILTER (WHERE l.status = 'CANCELLED')
    INTO v_paused_workflows, v_cancelled_workflows
    FROM locked l;

    SELECT array_agg(u.id ORDER BY u.id) FILTER (WHERE n.workflow_id = ANY(v_paused_workflows)),
           array_agg(u.claimed_at ORDER BY u.id) FILTER (WHERE n.workflow_id = ANY(v_paused_workflows)),
           array_agg(u.id ORDER BY u.id) FILTER (WHERE n.workflow_id = ANY(v_cancelled_workflows)),
           array_agg(u.claimed_at ORDER BY u.id) FILTER (WHERE n.workflow_id = ANY(v_cancelled_workflows))
    INTO v_paused_ids, v_paused_ats, v_cancelled_ids, v_cancelled_ats
    FROM unnest(p_task_ids, p_claimed_ats) AS u(id, claimed_at)
    JOIN horsies_workflow_tasks n ON n.task_id = u.id;

    PERFORM 1 FROM horsies_tasks t
    WHERE t.id = ANY(coalesce(v_paused_ids, ARRAY[]::uuid[]) || coalesce(v_cancelled_ids, ARRAY[]::uuid[]))
    ORDER BY t.id
    FOR UPDATE;

    IF v_paused_ids IS NOT NULL THEN
        SELECT coalesce(array_agg(o.task_id), ARRAY[]::uuid[]) INTO v_abandoned
        FROM horsies_abandon_owned_nodes(v_paused_ids, v_paused_ats, p_worker_id) AS o
        WHERE o.outcome = 'APPLIED';
        WITH back_to_ready AS (
            UPDATE horsies_workflow_tasks n
            SET status = 'READY', task_id = NULL, started_at = NULL
            WHERE n.task_id = ANY(v_abandoned) AND n.status IN ('ENQUEUED', 'RUNNING')
            RETURNING n.id, n.workflow_id
        )
        INSERT INTO horsies_workflow_events (
            workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
            terminalization_kind
        )
        SELECT back_to_ready.workflow_id, nextval('horsies_workflow_event_id'), 'NODE_READY', now(),
               back_to_ready.id, NULL, NULL, NULL
        FROM back_to_ready
        ORDER BY back_to_ready.id;
    END IF;
    IF v_cancelled_ids IS NOT NULL THEN
        SELECT coalesce(array_agg(o.task_id), ARRAY[]::uuid[]) INTO v_cancelled
        FROM horsies_cancel_owned_nodes(v_cancelled_ids, v_cancelled_ats, p_worker_id) AS o
        WHERE o.outcome = 'APPLIED';
        UPDATE horsies_workflow_tasks n
        SET status = 'SKIPPED', completed_at = now()
        WHERE n.task_id = ANY(v_cancelled) AND n.status IN ('PENDING', 'READY', 'ENQUEUED');
    END IF;

    RETURN QUERY
    SELECT u.id,
           CASE
               WHEN u.id = ANY(coalesce(v_paused_ids, ARRAY[]::uuid[]) || coalesce(v_cancelled_ids, ARRAY[]::uuid[]))
                   THEN 'BLOCKED'
               ELSE 'DISPATCH'
           END::text
    FROM unnest(p_task_ids) WITH ORDINALITY AS u(id, position)
    ORDER BY u.position;
END;
$$;

-- The stop of a workflow task whose start (horsies_start_attempt) missed, in one statement in autocommit sent after
-- the start statement ended, so no row lock of the start is held. The reason of the start's miss only chooses that
-- this call runs: the function judges again under its own locks, in Python's order.
-- Arguments: the task, the worker, the claim generation of the start, p_expiry_result (the TASK_EXPIRED envelope
-- built by the core) and p_orphan_self_heal. Every argument is required (22023).
-- Steps:
-- 1. The workflow row of the task's node (the node by task_id) locked FOR SHARE: a resume, a pause or a cancel of
--    that workflow cannot commit until this statement ends.
-- 2. The deadline (horsies_expire_owned_claim: the task advisory lock, the row lock, the owner fence, good_until
--    passed): when the expiry applies, or was applied already, EXPIRED. Phase 2 of the node is owed, in its long
--    form.
-- 3. The node of the task, locked, and the workflow status read under the lock of step 1:
--    - PAUSED: ABANDONED. The abandon runs (horsies_abandon_owned_node, PAUSE_ABANDON_CLAIM, fenced to the claim
--      generation); when it applies or was applied, the node goes back to READY with no task and no started_at,
--      with one NODE_READY event (horsies_workflow_events, at now()) when this call moves the node.
--    - CANCELLED: CANCELLED. The cancel runs (horsies_cancel_owned_node, WORKFLOW_CANCEL_CLAIM, with the carve-out
--      for a row requeued to PENDING); when it applies or was applied, the node goes SKIPPED with completed_at.
--    - the claim is still startable (CLAIMED by this worker at this generation, lease and deadline not passed) and
--      the node is linked live (ENQUEUED, READY, PENDING or RUNNING) in a RUNNING workflow: RESUMED, nothing
--      written. A resume committed after the start read PAUSED; the worker runs the start once more.
-- 4. The claim is held by this worker at this generation and no node links the task live:
--    - p_orphan_self_heal: ORPHAN_CANCELLED. The orphan cancel runs (horsies_cancel_owned_orphan, CANCEL_ORPHAN).
--    - else NODE_NOT_LINKED, nothing written (the worker reports WORKFLOW_CHECK_FAILED).
-- 5. Anything else: NOT_HELD, with the classification of horsies_terminalization_miss.
-- Returns one row: outcome (the step that ran), and move_outcome, the outcome of the move it ran (APPLIED,
-- ALREADY_APPLIED, LOST_CLAIM, SOURCE_STATE_CONFLICT, TASK_ABSENT), NULL for RESUMED and NODE_NOT_LINKED.
-- Lock order: workflow row FOR SHARE, task advisory lock, task row, node row, event row.
-- Python 0.7.0 (`v0.7.0` package): the miss path of _confirm_ownership_and_set_running
-- (core/worker/child_runner.py:1195-1235) with _expire_claimed_task_before_start (:1063-1096) and
-- _handle_workflow_stop_before_start (:963-1060), and the orphan cancel of the finalize classification
-- (core/lifecycle/classify.py:281-297).
CREATE OR REPLACE FUNCTION horsies_workflow_task_stop_before_start(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone,
    p_expiry_result text,
    p_orphan_self_heal boolean
) RETURNS TABLE (outcome text, move_outcome text)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
#variable_conflict use_column
DECLARE
    v_workflow_id uuid;
    v_workflow_status text;
    v_move text;
    v_task record;
    v_node record;
    v_held boolean;
BEGIN
    PERFORM horsies_workflow_require(
        num_nulls(p_task_id, p_worker_id, p_claimed_at, p_expiry_result, p_orphan_self_heal) = 0,
        'every argument of the stop before start is required'
    );
    SELECT n.workflow_id INTO v_workflow_id FROM horsies_workflow_tasks n WHERE n.task_id = p_task_id;
    IF v_workflow_id IS NOT NULL THEN
        SELECT w.status INTO v_workflow_status FROM horsies_workflows w WHERE w.id = v_workflow_id FOR SHARE;
    END IF;

    SELECT o.outcome INTO v_move
    FROM horsies_expire_owned_claim(p_task_id, p_worker_id, p_expiry_result, 'TASK_EXPIRED') AS o;
    IF v_move IN ('APPLIED', 'ALREADY_APPLIED') THEN
        RETURN QUERY SELECT 'EXPIRED'::text, v_move;
        RETURN;
    END IF;

    -- The task row is locked by the expiry call (its fence or its miss classifier); the node row last.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at, t.claim_expires_at, t.good_until INTO v_task
    FROM horsies_tasks t WHERE t.id = p_task_id;
    SELECT n.id, n.workflow_id, n.status INTO v_node
    FROM horsies_workflow_tasks n WHERE n.task_id = p_task_id
    FOR UPDATE;
    v_held := v_task.status = 'CLAIMED' AND v_task.claimed_by_worker_id = p_worker_id
              AND v_task.claimed_at = p_claimed_at;

    IF v_node.workflow_id IS NOT DISTINCT FROM v_workflow_id AND v_workflow_status = 'PAUSED' THEN
        SELECT o.outcome INTO v_move FROM horsies_abandon_owned_node(p_task_id, p_worker_id, p_claimed_at) AS o;
        IF v_move IN ('APPLIED', 'ALREADY_APPLIED') THEN
            WITH back_to_ready AS (
                UPDATE horsies_workflow_tasks n
                SET status = 'READY', task_id = NULL, started_at = NULL
                WHERE n.task_id = p_task_id AND n.status IN ('ENQUEUED', 'RUNNING')
                RETURNING n.id, n.workflow_id
            )
            INSERT INTO horsies_workflow_events (
                workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
                terminalization_kind
            )
            SELECT back_to_ready.workflow_id, nextval('horsies_workflow_event_id'), 'NODE_READY', now(),
                   back_to_ready.id, NULL, NULL, NULL
            FROM back_to_ready;
        END IF;
        RETURN QUERY SELECT 'ABANDONED'::text, v_move;
        RETURN;
    END IF;
    IF v_node.workflow_id IS NOT DISTINCT FROM v_workflow_id AND v_workflow_status = 'CANCELLED' THEN
        SELECT o.outcome INTO v_move
        FROM horsies_cancel_owned_node(p_task_id, p_worker_id, p_claimed_at, TRUE) AS o;
        IF v_move IN ('APPLIED', 'ALREADY_APPLIED') THEN
            UPDATE horsies_workflow_tasks n
            SET status = 'SKIPPED', completed_at = now()
            WHERE n.task_id = p_task_id AND n.status IN ('ENQUEUED', 'READY');
        END IF;
        RETURN QUERY SELECT 'CANCELLED'::text, v_move;
        RETURN;
    END IF;
    IF v_held
       AND v_task.claim_expires_at > now()
       AND (v_task.good_until IS NULL OR v_task.good_until > now())
       AND v_workflow_status = 'RUNNING'
       AND v_node.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING') THEN
        RETURN QUERY SELECT 'RESUMED'::text, NULL::text;
        RETURN;
    END IF;
    IF v_held AND v_node.status IS DISTINCT FROM 'ENQUEUED' AND v_node.status IS DISTINCT FROM 'READY'
       AND v_node.status IS DISTINCT FROM 'PENDING' AND v_node.status IS DISTINCT FROM 'RUNNING' THEN
        IF NOT p_orphan_self_heal THEN
            RETURN QUERY SELECT 'NODE_NOT_LINKED'::text, NULL::text;
            RETURN;
        END IF;
        SELECT o.outcome INTO v_move FROM horsies_cancel_owned_orphan(p_task_id, p_worker_id, p_claimed_at) AS o;
        RETURN QUERY SELECT 'ORPHAN_CANCELLED'::text, v_move;
        RETURN;
    END IF;
    -- The classifier takes one kind. EXPIRE_CLAIMED answers as an empty class would: a history row of the expire
    -- family ended this call at the expiry above (ALREADY_APPLIED there), and the task advisory lock that the expiry
    -- took keeps history unchanged until the transaction ends. Every other history row is FOREIGN_TERMINALIZATION.
    SELECT o.outcome INTO v_move
    FROM horsies_terminalization_miss(
        p_task_id, 'EXPIRE_CLAIMED', 'OWNER_AND_GENERATION', p_worker_id, p_claimed_at
    ) AS o;
    RETURN QUERY SELECT 'NOT_HELD'::text, v_move;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Monitoring read of a run with its timing records
------------------------------------------------------------------------------------------------------------------------

-- The result columns change, so the function of migration 1000 is dropped and created again.
DROP FUNCTION horsies_monitoring_workflow_run(uuid);

-- horsies_monitoring_workflow_run: one workflow run with its nodes, and the records that compose the phase timing of
-- the run and of each node (core/src/monitoring/timing.rs composes them).
--
-- Guarantee: for any run id, root or child, the rows below, read in one snapshot (the function is STABLE, so every
-- statement reads the snapshot of the calling statement). It changes nothing. The number of statements does not
-- depend on the number of nodes, events or tasks: no statement runs per node or per task.
-- Returns, in row_position order:
-- - No run: one row_kind 'ABSENT' row with workflow_id.
-- - row_kind 'RUN' (row_position 0): workflow_id, workflow_name, definition_key, status, created_at, completed_at,
--   trace_id (the trace id of the start, NULL for a start with no trace), and:
--   - as_of: clock_timestamp() after the snapshot was taken. Every time that a visible row holds was written before it.
--   - captured_since: installed_on of the bookkeeping row of migration 1001, the start of the lifecycle record.
--   - timing_rows: BEFORE_CAPTURE (created_at is before captured_since: the run began before the record, so no
--     EVENT or TASK row follows), BOUND_EXCEEDED (the run has more than c_max_events events: no EVENT or TASK row
--     follows, and no history row is read), or INCLUDED (the EVENT and TASK rows follow).
-- - row_kind 'NODE' (row_position 1..n), one per node in task_index order: status, started_at, completed_at,
--   task_index, node_id, task_name, dependencies, allow_failed_deps, is_subworkflow, sub_workflow_id, child_total,
--   child_failed (as migration 1000: both count the nodes of the child run of a sub-workflow node and the FAILED ones
--   among them; NULL for a task node, for a node with no child run, and for a child run with no node), and:
--   - node_created_at: the created_at of the node; node_task_id: the task that the node links (NULL for none).
--   - child_created_at, child_completed_at: the created_at and completed_at of the child run that sub_workflow_id
--     names; both NULL when the node names no child or no row holds the child.
-- - Only with timing_rows INCLUDED:
--   - row_kind 'EVENT', one per row of horsies_workflow_events of the run, in event_id order: event_id, event_kind,
--     occurred_at; task_index of the node of a node event (NULL for a run event); task_id and terminalization_kind of
--     a TASK_MOVED event (NULL for the other kinds).
--   - row_kind 'TASK', one per task of a node:
--     - location LIVE: the live task row that a node links (node_task_id): task_id, status, created_at, enqueued_at,
--       claimed_at, started_at, attempt_archive_version 2 and attempt_snapshot (horsies_encode_attempt_archive of
--       the live row, which holds the open attempt); attempt_snapshot_digest NULL.
--     - location HISTORY: the history row of a TASK_MOVED event, read at its locator (history_class, occurred_at,
--       task_id), so the probe prunes to one leaf, as the phase-2 source read does: task_id, created_at,
--       enqueued_at, claimed_at, started_at, attempt_archive_version, attempt_snapshot, attempt_snapshot_digest;
--       status NULL. Its terminal_at is the occurred_at of the event (the locator).
--     - location PRUNED: a TASK_MOVED event whose history row retention removed: task_id only.
-- Bounds: at most c_max_nodes (10,000) nodes, the bound of the workflow start; more nodes break that invariant and
-- raise data_corrupted (XX001). At most c_max_events (100,000) events are read; above it, BOUND_EXCEEDED. Each task
-- holds at most 21 attempts (horsies_task_attempts_attempt_range).
-- Raises 22004 for a NULL id; XX001 when no bookkeeping row of migration 1001 exists for an existing run (the capture
-- start is part of the answer, and an install writes the row in the transaction of the migration).
-- Lock order: none (a plain read).
CREATE FUNCTION horsies_monitoring_workflow_run(p_workflow_id uuid)
RETURNS TABLE (
    row_kind text,
    row_position integer,
    workflow_id uuid,
    workflow_name character varying,
    definition_key character varying,
    status text,
    created_at timestamp with time zone,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    task_index integer,
    node_id character varying,
    task_name character varying,
    dependencies integer[],
    allow_failed_deps boolean,
    is_subworkflow boolean,
    sub_workflow_id uuid,
    child_total bigint,
    child_failed bigint,
    trace_id bytea,
    as_of timestamp with time zone,
    captured_since timestamp with time zone,
    timing_rows text,
    node_created_at timestamp with time zone,
    node_task_id uuid,
    child_created_at timestamp with time zone,
    child_completed_at timestamp with time zone,
    event_id bigint,
    event_kind text,
    occurred_at timestamp with time zone,
    terminalization_kind text,
    location text,
    task_id uuid,
    enqueued_at timestamp with time zone,
    claimed_at timestamp with time zone,
    attempt_archive_version smallint,
    attempt_snapshot bytea,
    attempt_snapshot_digest bytea
)
    LANGUAGE plpgsql STABLE
AS $$
#variable_conflict use_column
DECLARE
    c_max_nodes CONSTANT integer := 10000;
    c_max_events CONSTANT integer := 100000;
    c_capture_version CONSTANT bigint := 1001;
    v_as_of timestamp with time zone;
    v_created_at timestamp with time zone;
    v_captured_since timestamp with time zone;
    v_event_rows bigint;
    v_timing_rows text;
    v_node_rows bigint;
BEGIN
    IF p_workflow_id IS NULL THEN
        RAISE EXCEPTION 'horsies_monitoring_workflow_run: the workflow id is required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    v_as_of := clock_timestamp();
    SELECT w.created_at INTO v_created_at FROM horsies_workflows AS w WHERE w.id = p_workflow_id;
    IF NOT FOUND THEN
        RETURN QUERY
        SELECT 'ABSENT'::text, NULL::integer, p_workflow_id, NULL::character varying, NULL::character varying,
               NULL::text, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::integer,
               NULL::character varying, NULL::character varying, NULL::integer[], NULL::boolean, NULL::boolean,
               NULL::uuid, NULL::bigint, NULL::bigint, NULL::bytea,
               NULL::timestamptz, NULL::timestamptz, NULL::text, NULL::timestamptz, NULL::uuid, NULL::timestamptz,
               NULL::timestamptz, NULL::bigint, NULL::text, NULL::timestamptz, NULL::text, NULL::text, NULL::uuid,
               NULL::timestamptz, NULL::timestamptz, NULL::smallint, NULL::bytea, NULL::bytea;
        RETURN;
    END IF;
    SELECT m.installed_on INTO v_captured_since FROM horsies_migrations AS m WHERE m.version = c_capture_version;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'horsies_monitoring_workflow_run: no bookkeeping row of migration % records the start of '
            'the lifecycle record', c_capture_version
            USING ERRCODE = 'data_corrupted';
    END IF;
    IF v_created_at < v_captured_since THEN
        v_timing_rows := 'BEFORE_CAPTURE';
    ELSE
        SELECT count(*) INTO v_event_rows
        FROM (
            SELECT 1 FROM horsies_workflow_events AS e WHERE e.workflow_id = p_workflow_id LIMIT c_max_events + 1
        ) AS bounded;
        v_timing_rows := CASE WHEN v_event_rows > c_max_events THEN 'BOUND_EXCEEDED' ELSE 'INCLUDED' END;
    END IF;

    RETURN QUERY
    SELECT 'RUN'::text, 0, w.id, w.name, w.definition_key, w.status::text, w.created_at, NULL::timestamptz,
           w.completed_at, NULL::integer, NULL::character varying, NULL::character varying, NULL::integer[],
           NULL::boolean, NULL::boolean, NULL::uuid, NULL::bigint, NULL::bigint, w.trace_id,
           v_as_of, v_captured_since, v_timing_rows, NULL::timestamptz, NULL::uuid, NULL::timestamptz,
           NULL::timestamptz, NULL::bigint, NULL::text, NULL::timestamptz, NULL::text, NULL::text, NULL::uuid,
           NULL::timestamptz, NULL::timestamptz, NULL::smallint, NULL::bytea, NULL::bytea
    FROM horsies_workflows AS w
    WHERE w.id = p_workflow_id;

    RETURN QUERY
    SELECT 'NODE'::text, (row_number() OVER (ORDER BY n.task_index))::integer, NULL::uuid,
           NULL::character varying, NULL::character varying, n.status::text, NULL::timestamptz, n.started_at,
           n.completed_at, n.task_index, n.node_id, n.task_name, n.dependencies, n.allow_failed_deps,
           n.is_subworkflow, n.sub_workflow_id, child.node_total, child.failed_total, NULL::bytea,
           NULL::timestamptz, NULL::timestamptz, NULL::text, n.created_at, n.task_id, child_run.created_at,
           child_run.completed_at, NULL::bigint, NULL::text, NULL::timestamptz, NULL::text, NULL::text, NULL::uuid,
           NULL::timestamptz, NULL::timestamptz, NULL::smallint, NULL::bytea, NULL::bytea
    FROM horsies_workflow_tasks AS n
    LEFT JOIN LATERAL (
        SELECT count(*) AS node_total, count(*) FILTER (WHERE c.status = 'FAILED') AS failed_total
        FROM horsies_workflow_tasks AS c
        WHERE n.is_subworkflow AND c.workflow_id = n.sub_workflow_id
        HAVING count(*) > 0
    ) AS child ON TRUE
    LEFT JOIN horsies_workflows AS child_run ON child_run.id = n.sub_workflow_id
    WHERE n.workflow_id = p_workflow_id
    ORDER BY n.task_index
    LIMIT c_max_nodes + 1;
    GET DIAGNOSTICS v_node_rows = ROW_COUNT;
    IF v_node_rows > c_max_nodes THEN
        RAISE EXCEPTION 'horsies_monitoring_workflow_run: run % has more than % nodes', p_workflow_id, c_max_nodes
            USING ERRCODE = 'data_corrupted';
    END IF;
    IF v_timing_rows <> 'INCLUDED' THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT 'EVENT'::text, (v_node_rows + row_number() OVER (ORDER BY e.event_id))::integer, NULL::uuid,
           NULL::character varying, NULL::character varying, NULL::text, NULL::timestamptz, NULL::timestamptz,
           NULL::timestamptz, event_node.task_index, NULL::character varying, NULL::character varying, NULL::integer[],
           NULL::boolean, NULL::boolean, NULL::uuid, NULL::bigint, NULL::bigint, NULL::bytea,
           NULL::timestamptz, NULL::timestamptz, NULL::text, NULL::timestamptz, NULL::uuid, NULL::timestamptz,
           NULL::timestamptz, e.event_id, e.event_kind::text, e.occurred_at, e.terminalization_kind::text,
           NULL::text, e.task_id, NULL::timestamptz, NULL::timestamptz, NULL::smallint, NULL::bytea, NULL::bytea
    FROM horsies_workflow_events AS e
    LEFT JOIN horsies_workflow_tasks AS event_node ON event_node.id = e.workflow_node_row_id
    WHERE e.workflow_id = p_workflow_id
    ORDER BY e.event_id;
    GET DIAGNOSTICS v_event_rows = ROW_COUNT;

    -- The live tasks in node order, then the moved tasks in event order. The live row and its attempt rows are read
    -- in this snapshot, so a task is LIVE here or has its TASK_MOVED event, never both.
    RETURN QUERY
    SELECT 'TASK'::text,
           (v_node_rows + v_event_rows + row_number() OVER (ORDER BY t.source_order, t.item_order))::integer,
           NULL::uuid, NULL::character varying, NULL::character varying, t.task_status, t.task_created_at,
           t.task_started_at, NULL::timestamptz, NULL::integer, NULL::character varying, NULL::character varying,
           NULL::integer[], NULL::boolean, NULL::boolean, NULL::uuid, NULL::bigint, NULL::bigint, NULL::bytea,
           NULL::timestamptz, NULL::timestamptz, NULL::text, NULL::timestamptz, NULL::uuid, NULL::timestamptz,
           NULL::timestamptz, NULL::bigint, NULL::text, NULL::timestamptz, NULL::text, t.task_location, t.item_task_id,
           t.task_enqueued_at, t.task_claimed_at, t.archive_version, t.archive_payload, t.archive_digest
    FROM (
        SELECT 1 AS source_order, n.task_index::bigint AS item_order, 'LIVE'::text AS task_location,
               live.id AS item_task_id, live.status::text AS task_status, live.created_at AS task_created_at,
               live.enqueued_at AS task_enqueued_at, live.claimed_at AS task_claimed_at,
               live.started_at AS task_started_at, 2::smallint AS archive_version,
               horsies_encode_attempt_archive(live) AS archive_payload, NULL::bytea AS archive_digest
        FROM horsies_workflow_tasks AS n
        JOIN horsies_tasks AS live ON live.id = n.task_id
        WHERE n.workflow_id = p_workflow_id
        UNION ALL
        SELECT 2, moved.event_id, CASE WHEN h.task_id IS NULL THEN 'PRUNED' ELSE 'HISTORY' END, moved.task_id,
               NULL::text, h.created_at, h.enqueued_at, h.claimed_at, h.started_at, h.attempt_archive_version,
               h.attempt_snapshot, h.attempt_snapshot_digest
        FROM horsies_workflow_events AS moved
        LEFT JOIN LATERAL (
            SELECT hr.task_id, hr.created_at, hr.enqueued_at, hr.claimed_at, hr.started_at,
                   hr.attempt_archive_version, hr.attempt_snapshot, hr.attempt_snapshot_digest
            FROM horsies_task_history AS hr
            WHERE hr.retention_class_key = moved.history_class
              AND hr.terminal_at = moved.occurred_at
              AND hr.task_id = moved.task_id
        ) AS h ON TRUE
        WHERE moved.workflow_id = p_workflow_id AND moved.event_kind = 'TASK_MOVED'
    ) AS t
    ORDER BY t.source_order, t.item_order;
END;
$$;
