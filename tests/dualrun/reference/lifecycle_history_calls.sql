-- The reference side of the 0004 history calls in the dual-run of test_history_moves.py: the functions of
-- 0004_lifecycle_functions.sql that call history, before they moved onto the row-taking move
-- (0006_history_functions.sql), the miss classifier of 0008_history_maintenance_functions.sql and the one-argument
-- provenance reader; the array overload of horsies_terminalization_miss with its fence kind, which they called
-- then; a two-argument provenance stand-in that answers "not found", as the function-call script of that commit
-- defined it; and the insert helper horsies_insert_task_rows with the 20 arguments that horsies_enqueue_task of
-- that commit calls, next to the baseline helper, which also takes the row time.
--
-- Each definition is the text of program/0.8.0 at commit cc5a40aa (0004_lifecycle_functions.sql,
-- 0003_interim_functions.sql, tests/integration/baseline_function_calls.sql), unchanged except CREATE OR REPLACE
-- for the six 0004 functions and no DEFAULT on the stand-in (a DEFAULT would make a one-argument call ambiguous next
-- to the stand-in of the script). The file runs after interim_miss_classifier.sql (the composite type
-- horsies_task_provenance) and interim_moves.sql (the uuid move). Do not edit a body: the file is the record of the
-- statements before the switch.

CREATE FUNCTION horsies_task_provenance_staged(p_task_id uuid, p_include_live boolean)
RETURNS horsies_task_provenance LANGUAGE sql AS $$
    SELECT ROW(FALSE, NULL, NULL, NULL, NULL, NULL)::horsies_task_provenance
$$;

CREATE FUNCTION horsies_terminalization_miss(p_task_id uuid, p_equivalent_kinds text[], p_fence_kind text, p_worker_id text, p_claimed_at timestamp with time zone) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_row horsies_tasks%ROWTYPE;
    v_provenance record;
    v_held boolean;
BEGIN
    CASE p_fence_kind
        WHEN 'NONE' THEN
            IF num_nulls(p_worker_id, p_claimed_at) <> 2 THEN
                RAISE EXCEPTION 'fence NONE takes no worker and no claim generation'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'OWNER' THEN
            IF p_worker_id IS NULL OR p_claimed_at IS NOT NULL THEN
                RAISE EXCEPTION 'fence OWNER takes a worker and no claim generation'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'OWNER_AND_GENERATION' THEN
            IF num_nulls(p_worker_id, p_claimed_at) <> 0 THEN
                RAISE EXCEPTION 'fence OWNER_AND_GENERATION takes a worker and a claim generation'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        ELSE
            RAISE EXCEPTION 'unknown fence kind'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;

    SELECT * INTO v_row
    FROM horsies_tasks
    WHERE id = p_task_id
    FOR UPDATE;

    IF NOT FOUND THEN
        SELECT * INTO v_provenance
        FROM horsies_task_provenance_staged(p_task_id, FALSE);
        IF NOT v_provenance.found THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'TASK_ABSENT'::text,
                NULL::timestamptz, NULL::text,
                NULL::text, NULL::varchar, NULL::timestamptz,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        IF v_provenance.location <> 'HISTORY' THEN
            RAISE EXCEPTION
                'provenance reported % after a live miss', v_provenance.location
                USING ERRCODE = 'data_corrupted';
        END IF;
        IF v_provenance.terminalization_kind = ANY(p_equivalent_kinds) THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'ALREADY_APPLIED'::text,
                v_provenance.terminal_at, v_provenance.terminalization_kind,
                v_provenance.status, NULL::varchar, NULL::timestamptz,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            v_provenance.terminal_at, v_provenance.terminalization_kind,
            v_provenance.status, NULL::varchar, NULL::timestamptz,
            'FOREIGN_TERMINALIZATION'::text, NULL::jsonb;
        RETURN;
    END IF;

    v_held := CASE p_fence_kind
        WHEN 'NONE' THEN TRUE
        WHEN 'OWNER' THEN v_row.claimed_by_worker_id IS NOT DISTINCT FROM CAST(p_worker_id AS VARCHAR)
        WHEN 'OWNER_AND_GENERATION' THEN
            v_row.claimed_by_worker_id IS NOT DISTINCT FROM CAST(p_worker_id AS VARCHAR)
            AND v_row.claimed_at IS NOT DISTINCT FROM p_claimed_at
    END;
    IF NOT v_held THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'LOST_CLAIM'::text,
            NULL::timestamptz, NULL::text,
            v_row.status::text, v_row.claimed_by_worker_id, v_row.claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
        NULL::timestamptz, NULL::text,
        v_row.status::text, v_row.claimed_by_worker_id, v_row.claimed_at,
        NULL::text, NULL::jsonb;
END
$$;

CREATE FUNCTION horsies_insert_task_rows(
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
    IF num_nulls(
        p_ids, p_task_names, p_queue_names, p_priorities, p_inputs, p_task_options, p_executions,
        p_retention_class_keys, p_retain_rerun_inputs, p_max_retries, p_good_untils, p_sent_ats, p_enqueued_ats,
        p_is_workflow_tasks, p_fingerprint_versions, p_fingerprints, p_key_digests, p_rerun_of_task_ids,
        p_rerun_root_task_ids
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
       OR cardinality(p_rerun_of_task_ids) <> v_count OR cardinality(p_rerun_root_task_ids) <> v_count THEN
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
            rerun_root_task_id
        )
        SELECT
            r.id, r.task_name, r.queue_name, r.priority, r.input, r.task_options, r.execution,
            r.retention_class_key, r.sent_at,
            NOW(), r.fingerprint_version, r.fingerprint, r.key_digest, 'PENDING',
            r.enqueued_at, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 0, r.max_retries,
            NULL, r.good_until, r.is_workflow_task, NOW(), r.retain_rerun_input, r.rerun_of_task_id,
            r.rerun_root_task_id
        FROM unnest(
            p_ids, p_task_names, p_queue_names, p_priorities, p_inputs, p_task_options, p_executions,
            p_retention_class_keys, p_retain_rerun_inputs, p_max_retries, p_good_untils, p_sent_ats,
            p_enqueued_ats, p_is_workflow_tasks, p_fingerprint_versions, p_fingerprints, p_key_digests,
            p_rerun_of_task_ids, p_rerun_root_task_ids
        ) AS r(
            id, task_name, queue_name, priority, input, task_options, execution, retention_class_key,
            retain_rerun_input, max_retries, good_until, sent_at, enqueued_at, is_workflow_task,
            fingerprint_version, fingerprint, key_digest, rerun_of_task_id, rerun_root_task_id
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

CREATE OR REPLACE FUNCTION horsies_enqueue_task(
    p_task_id uuid,
    p_task_name text,
    p_queue_name text,
    p_priority integer,
    p_input text,
    p_task_options text,
    p_execution text,
    p_retention_class_key text,
    p_retain_rerun_input boolean,
    p_max_retries integer,
    p_good_until timestamp with time zone,
    p_sent_at timestamp with time zone,
    p_start_kind text,
    p_start_at timestamp with time zone,
    p_start_delay_s integer,
    p_fingerprint_version smallint,
    p_fingerprint bytea,
    p_lineage_kind text,
    p_rerun_of_task_id uuid,
    p_rerun_root_task_id uuid,
    p_key_kind text,
    p_key_digest bytea,
    p_key_scope_version smallint,
    p_key_window_s integer,
    p_send_kind text
) RETURNS TABLE(outcome text, task_id uuid)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_enqueued_at timestamptz;
    v_keyed boolean;
    v_claim horsies_key_reservation_outcome;
    v_provenance record;
    v_history record;
    v_outcome text;
    v_bound_digest bytea;
BEGIN
    IF num_nulls(
        p_task_id, p_task_name, p_queue_name, p_priority, p_input, p_execution, p_retention_class_key,
        p_retain_rerun_input, p_max_retries, p_sent_at, p_start_kind, p_fingerprint_version, p_fingerprint,
        p_lineage_kind, p_key_kind, p_send_kind
    ) <> 0 THEN
        RAISE EXCEPTION 'horsies_enqueue_task: a required argument is NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF octet_length(p_fingerprint) <> 32 OR p_fingerprint_version <= 0 THEN
        RAISE EXCEPTION 'horsies_enqueue_task: the fingerprint must be 32 bytes with a positive version'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    CASE p_start_kind
        WHEN 'NOW' THEN
            IF num_nulls(p_start_at, p_start_delay_s) <> 2 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: start NOW takes no time and no delay'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_enqueued_at := NOW();
        WHEN 'AT' THEN
            IF p_start_at IS NULL OR p_start_delay_s IS NOT NULL THEN
                RAISE EXCEPTION 'horsies_enqueue_task: start AT takes a time and no delay'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_enqueued_at := p_start_at;
        WHEN 'AFTER_DELAY' THEN
            IF p_start_delay_s IS NULL OR p_start_at IS NOT NULL OR p_start_delay_s < 0 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: start AFTER_DELAY takes a delay of 0 s or more and no time'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_enqueued_at := NOW() + make_interval(secs => p_start_delay_s);
        ELSE
            RAISE EXCEPTION 'horsies_enqueue_task: unknown start kind'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    CASE p_lineage_kind
        WHEN 'FIRST_RUN' THEN
            IF num_nulls(p_rerun_of_task_id, p_rerun_root_task_id) <> 2 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: a first run has no lineage'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'RERUN' THEN
            IF num_nulls(p_rerun_of_task_id, p_rerun_root_task_id) <> 0 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: a rerun names its source and its root'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        ELSE
            RAISE EXCEPTION 'horsies_enqueue_task: unknown lineage kind'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    CASE p_key_kind
        WHEN 'UNKEYED' THEN
            IF num_nulls(p_key_digest, p_key_scope_version, p_key_window_s) <> 3 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: an unkeyed send has no key values'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_keyed := FALSE;
        WHEN 'KEYED' THEN
            IF num_nulls(p_key_digest, p_key_scope_version, p_key_window_s) <> 0 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: a keyed send names its digest, scope version and window'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_keyed := TRUE;
        ELSE
            RAISE EXCEPTION 'horsies_enqueue_task: unknown key kind'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    IF p_send_kind NOT IN ('FIRST', 'REPEAT') THEN
        RAISE EXCEPTION 'horsies_enqueue_task: unknown send kind'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- 1. The key reservation. The claim refuses a digest that is not 32 bytes and a window outside 0..30 days.
    IF v_keyed THEN
        SELECT * INTO STRICT v_claim
        FROM horsies_key_reservation_claim(
            p_key_digest, p_key_scope_version, make_interval(secs => p_key_window_s),
            p_fingerprint_version, p_fingerprint, p_task_id
        );
        CASE v_claim.outcome
            WHEN 'REPLAY' THEN
                RETURN QUERY SELECT 'KEY_REPLAY'::text, v_claim.task_id;
                RETURN;
            WHEN 'CONFLICT' THEN
                RETURN QUERY SELECT 'KEY_CONFLICT'::text, v_claim.task_id;
                RETURN;
            WHEN 'APPLIED' THEN
                NULL;
        END CASE;
    END IF;

    -- 2. A repeat probes history under the task advisory lock.
    IF p_send_kind = 'REPEAT' THEN
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
        SELECT * INTO v_provenance FROM horsies_task_provenance_staged(p_task_id, FALSE);
        IF v_provenance.found THEN
            IF v_provenance.location <> 'HISTORY' THEN
                RAISE EXCEPTION 'horsies_enqueue_task: provenance reported a location other than history'
                    USING ERRCODE = 'data_corrupted';
            END IF;
            SELECT h.command_fingerprint_version, h.command_fingerprint, h.sent_at
            INTO v_history
            FROM horsies_task_history h
            WHERE h.task_id = p_task_id
              AND h.terminal_at = v_provenance.terminal_at;
            v_outcome := CASE
                WHEN NOT FOUND THEN 'CONFLICT_UNVERIFIABLE'
                WHEN v_history.command_fingerprint_version = p_fingerprint_version
                     AND v_history.command_fingerprint = p_fingerprint
                     AND v_history.sent_at = p_sent_at THEN 'SAME_COMMAND'
                ELSE 'PAYLOAD_MISMATCH'
            END;
            IF v_keyed THEN
                DELETE FROM horsies_key_reservations r
                WHERE r.idempotency_key_digest = p_key_digest
                  AND r.task_id = p_task_id
                  AND r.disposition = 'LIVE';
            END IF;
            RETURN QUERY SELECT v_outcome, p_task_id;
            RETURN;
        END IF;
    END IF;

    -- 3. The insert, or the classified conflict.
    SELECT i.outcome INTO STRICT v_outcome
    FROM horsies_insert_task_rows(
        ARRAY[p_task_id], ARRAY[p_task_name], ARRAY[p_queue_name], ARRAY[p_priority], ARRAY[p_input],
        ARRAY[p_task_options], ARRAY[p_execution], ARRAY[p_retention_class_key], ARRAY[p_retain_rerun_input],
        ARRAY[p_max_retries], ARRAY[p_good_until], ARRAY[p_sent_at], ARRAY[v_enqueued_at], ARRAY[FALSE],
        ARRAY[p_fingerprint_version], ARRAY[p_fingerprint],
        ARRAY[CASE WHEN v_keyed THEN p_key_digest END], ARRAY[p_rerun_of_task_id], ARRAY[p_rerun_root_task_id],
        CASE WHEN v_keyed THEN 'FOR_UPDATE' ELSE 'NONE' END
    ) AS i;

    -- 4. A keyed send on a live row of the same command binds its key; the row is locked by the read in step 3.
    IF v_keyed AND v_outcome = 'SAME_COMMAND' THEN
        SELECT t.idempotency_key_digest INTO STRICT v_bound_digest
        FROM horsies_tasks t
        WHERE t.id = p_task_id;
        IF v_bound_digest IS NULL THEN
            UPDATE horsies_tasks t
            SET idempotency_key_digest = p_key_digest
            WHERE t.id = p_task_id;
        ELSIF v_bound_digest <> p_key_digest THEN
            v_outcome := 'PAYLOAD_MISMATCH';
        END IF;
    END IF;

    -- 5. A keyed send that wrote no row and bound no key leaves no reservation.
    IF v_keyed AND v_outcome NOT IN ('INSERTED', 'SAME_COMMAND') THEN
        DELETE FROM horsies_key_reservations r
        WHERE r.idempotency_key_digest = p_key_digest
          AND r.task_id = p_task_id
          AND r.disposition = 'LIVE';
    END IF;

    RETURN QUERY SELECT v_outcome, p_task_id;
END
$$;

CREATE OR REPLACE FUNCTION horsies_complete_task_fused(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone,
    p_result text,
    p_wake boolean
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
    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.queue_name,
           clock_timestamp() AS db_now
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
            p_task_id, ARRAY['COMPLETE_FUSED', 'COMPLETE_LOCKED']::text[], 'OWNER_AND_GENERATION', p_worker_id,
            p_claimed_at
        ) AS m;
        RETURN;
    END IF;

    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name
    )
    VALUES (
        v_ctx.id, v_ctx.retry_count + 1, 'COMPLETED', FALSE, COALESCE(v_ctx.started_at, v_ctx.db_now),
        v_ctx.db_now, NULL, NULL, NULL,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid, v_ctx.worker_process_name
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
        worker_process_name = EXCLUDED.worker_process_name;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        p_task_id, 'COMPLETED', 'COMPLETE_FUSED', v_terminal_at, p_result, NULL, NULL
    );
    IF p_wake THEN
        PERFORM pg_notify('task_queue_' || v_ctx.queue_name, 'capacity');
    END IF;
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'COMPLETE_FUSED'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb;
END
$$;

CREATE OR REPLACE FUNCTION horsies_finish_workflow_success(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone,
    p_result text
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
    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.is_workflow_task, t.queue_name, t.task_name,
           clock_timestamp() AS db_now
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
            p_task_id, ARRAY['COMPLETE_FUSED', 'COMPLETE_LOCKED']::text[], 'OWNER_AND_GENERATION', p_worker_id,
            p_claimed_at
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
        worker_id, worker_hostname, worker_pid, worker_process_name
    )
    VALUES (
        v_ctx.id, v_ctx.retry_count + 1, 'COMPLETED', FALSE, COALESCE(v_ctx.started_at, v_ctx.db_now),
        v_ctx.db_now, NULL, NULL, NULL,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid, v_ctx.worker_process_name
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
        worker_process_name = EXCLUDED.worker_process_name;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        p_task_id, 'COMPLETED', 'COMPLETE_LOCKED', v_terminal_at, p_result, NULL, NULL
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'COMPLETE_LOCKED'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb,
        v_ctx.queue_name::text, v_ctx.task_name::text, horsies_node_has_dependents(p_task_id),
        horsies_node_downstream_subworkflow(p_task_id);
END
$$;

CREATE OR REPLACE FUNCTION horsies_finish_failed_attempt(
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
    p_wake boolean
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

    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.good_until, t.is_workflow_task,
           t.queue_name, t.task_name, clock_timestamp() AS db_now
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
            p_task_id, ARRAY['FAIL_RUNNING']::text[], 'OWNER_AND_GENERATION', p_worker_id, p_claimed_at
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
        worker_id, worker_hostname, worker_pid, worker_process_name
    )
    VALUES (
        v_ctx.id, v_ctx.retry_count + 1, p_attempt_outcome, v_will_retry, COALESCE(v_ctx.started_at, v_ctx.db_now),
        v_ctx.db_now, p_error_code, p_attempt_message, p_attempt_reason,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid, v_ctx.worker_process_name
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
        worker_process_name = EXCLUDED.worker_process_name;

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
        p_task_id, 'FAILED', 'FAIL_RUNNING', v_terminal_at, p_result, p_error_code, p_failed_reason
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

CREATE OR REPLACE FUNCTION horsies_finish_stale_attempt(
    p_task_id uuid,
    p_stale_after_ms integer,
    p_finalizing_stale_after_ms integer,
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
BEGIN
    IF num_nulls(p_task_id, p_stale_after_ms, p_finalizing_stale_after_ms, p_retry_kind, p_error_code) <> 0
       OR p_stale_after_ms < 0 OR p_finalizing_stale_after_ms < 0 THEN
        RAISE EXCEPTION
            'horsies_finish_stale_attempt: the task, thresholds (0 ms or more), retry kind and error code are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
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
    -- One capture: the locked row, the beat read beside it, and the instant both thresholds are judged at. Nothing
    -- is read again after a refusal, so the evidence cannot show a beat that the judgement did not see.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.finalizing_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.good_until, t.is_workflow_task,
           t.queue_name, t.task_name,
           (
               SELECT h.sent_at
               FROM horsies_heartbeats h
               WHERE h.task_id = t.id
                 AND h.role = 'runner'
                 AND h.sent_at >= NOW() - make_interval(
                     secs => GREATEST(p_stale_after_ms, p_finalizing_stale_after_ms) / 1000.0
                 )
               ORDER BY h.sent_at DESC
               LIMIT 1
           ) AS last_heartbeat,
           NOW() AS evaluated_at,
           clock_timestamp() AS db_now
    INTO v_ctx
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE OF t;

    IF NOT FOUND OR v_ctx.status <> 'RUNNING' THEN
        RETURN QUERY
        SELECT m.task_id, m.ordinality, m.outcome, m.terminal_at, m.terminalization_kind, m.observed_status,
               m.observed_worker_id::text, m.observed_claimed_at, m.guard_kind, m.observed_guard,
               NULL::text, NULL::integer, NULL::timestamptz, NULL::timestamptz,
               NULL::boolean, NULL::text, NULL::text, NULL::boolean, NULL::text
        FROM horsies_terminalization_miss(
            p_task_id, ARRAY['FAIL_STALE']::text[], 'NONE', NULL::text, NULL::timestamptz
        ) AS m;
        RETURN;
    END IF;

    IF NOT (
        v_ctx.started_at IS NOT NULL
        AND (
            v_ctx.finalizing_at IS NULL
            OR v_ctx.finalizing_at < v_ctx.evaluated_at - make_interval(secs => p_finalizing_stale_after_ms / 1000.0)
        )
        AND COALESCE(v_ctx.last_heartbeat, v_ctx.started_at)
            < v_ctx.evaluated_at - make_interval(secs => p_stale_after_ms / 1000.0)
    ) THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text, NULL::timestamptz, NULL::text,
            v_ctx.status::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, 'STALENESS'::text,
            jsonb_build_object(
                'last_heartbeat_at', v_ctx.last_heartbeat,
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
        worker_id, worker_hostname, worker_pid, worker_process_name
    )
    VALUES (
        v_ctx.id, v_ctx.retry_count + 1, 'FAILED', v_will_retry, v_ctx.started_at, v_ctx.db_now,
        p_error_code, p_attempt_message, p_attempt_reason,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid, v_ctx.worker_process_name
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
        worker_process_name = EXCLUDED.worker_process_name;

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
        p_task_id, 'FAILED', 'FAIL_STALE', v_terminal_at, p_result, p_error_code, p_failed_reason
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
    v_provenance record;
    v_history_is_workflow boolean;
    v_terminal_at timestamptz;
BEGIN
    IF num_nulls(p_task_id, p_include_running) <> 0 THEN
        RAISE EXCEPTION 'horsies_cancel_task: the task and the running choice are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at, t.is_workflow_task
    INTO v_task
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF NOT FOUND THEN
        SELECT * INTO v_provenance FROM horsies_task_provenance_staged(p_task_id, FALSE);
        IF NOT v_provenance.found THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'TASK_ABSENT'::text, NULL::timestamptz, NULL::text,
                NULL::text, NULL::text, NULL::timestamptz, NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        IF v_provenance.location <> 'HISTORY' THEN
            RAISE EXCEPTION 'horsies_cancel_task: provenance reported a location other than history after a live miss'
                USING ERRCODE = 'data_corrupted';
        END IF;
        SELECT h.is_workflow_task INTO v_history_is_workflow
        FROM horsies_task_history h
        WHERE h.task_id = p_task_id
          AND h.terminal_at = v_provenance.terminal_at;
        IF NOT FOUND THEN
            -- The leaf that held the row was dropped between the two reads.
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'TASK_ABSENT'::text, NULL::timestamptz, NULL::text,
                NULL::text, NULL::text, NULL::timestamptz, NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint,
            CASE
                WHEN NOT v_history_is_workflow AND v_provenance.terminalization_kind = 'CANCEL_ADMIN'
                THEN 'ALREADY_APPLIED'
                ELSE 'SOURCE_STATE_CONFLICT'
            END::text,
            v_provenance.terminal_at, v_provenance.terminalization_kind::text,
            v_provenance.status::text, NULL::text, NULL::timestamptz,
            CASE
                WHEN v_history_is_workflow THEN 'WORKFLOW_TASK'
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
    PERFORM horsies_move_task_to_history(
        p_task_id, 'CANCELLED', 'CANCEL_ADMIN', v_terminal_at, NULL, 'TASK_CANCELLED', 'Cancelled via monitoring API'
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'CANCEL_ADMIN'::text,
        v_task.status::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, NULL::text, NULL::jsonb;
END
$$;
