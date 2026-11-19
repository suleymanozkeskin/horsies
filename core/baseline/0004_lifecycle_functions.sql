-- horsies baseline schema, part 4: the task lifecycle functions.
--
-- One function per lifecycle operation. Each runs as one statement in autocommit: one statement is one
-- transaction, and no operation keeps a transaction open across a client round trip.
--
-- Rules that hold for every function of this file:
-- - Parameters and result columns have built-in types only, so the core's statement layer can bind and read them
--   under PgBouncer transaction mode with no prepared-statement tracking. A function is called as
--   SELECT * FROM fn(...). A variant argument is a kind text plus the built-in columns of each kind. The evidence of
--   a refusal (observed_guard) is jsonb, as in every function that returns the shared outcome columns.
-- - A malformed argument is refused with SQLSTATE 22023 (invalid_parameter_value) before any lock. An error message
--   names no argument value, so a statement error carries no bound values.
-- - Times that decide behaviour come from the database clock.
-- - Lock order: workflow row, then the task advisory lock (hashtextextended(task_id::text, 731)), then the task row,
--   then node rows, then the phase-2 pending row. A function that locks many task rows takes the row locks in id
--   order or with SKIP LOCKED, and no advisory lock; the batched success takes, per row in id order, the advisory
--   lock and then the row lock.
-- - Python references name the code that each function replaces, at main ec7267ac (paths under horsies/).
--
-- Functions of 0003_interim_functions.sql that this file replaces by the same name (the interim body is removed
-- there): horsies_claim, horsies_claim_candidates, horsies_claim_in_flight, horsies_complete_task_fused,
-- horsies_complete_tasks_fused, horsies_finish_workflow_success, horsies_finish_failed_attempt,
-- horsies_expire_owned_claim, horsies_expire_pending_tasks, horsies_cancel_owned_orphan,
-- horsies_cancel_orphaned_tasks.
--
-- History calls: each terminal write passes the row it locked to horsies_move_task_to_history(horsies_tasks, ...)
-- (0006_history_functions.sql); each miss passes the writer's kind and fence kind to horsies_terminalization_miss
-- (0008_history_maintenance_functions.sql); history is probed through the published reader
-- horsies_task_provenance_staged(uuid), which returns no row for an id that history does not hold.
--
-- Part of migration 1000: the install concatenates the files of this folder in name order and computes one
-- checksum over the result.

------------------------------------------------------------------------------------------------------------------------
-- Enqueue
------------------------------------------------------------------------------------------------------------------------

-- horsies_insert_task_rows: internal. The one place that holds the column list of a new task row.
--
-- Guarantee: each row is inserted as PENDING with every column named (retry_count 0; created_at, updated_at
-- p_row_time; every claim and run field NULL), or it is not inserted and its conflict is classified. The conflict
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
            rerun_root_task_id, trace_carrier, trace_id
        )
        SELECT
            r.id, r.task_name, r.queue_name, r.priority, r.input, r.task_options, r.execution,
            r.retention_class_key, r.sent_at,
            p_row_time, r.fingerprint_version, r.fingerprint, r.key_digest, 'PENDING',
            r.enqueued_at, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 0, r.max_retries,
            NULL, r.good_until, r.is_workflow_task, p_row_time, r.retain_rerun_input, r.rerun_of_task_id,
            r.rerun_root_task_id, r.trace_carrier, r.trace_id
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

-- horsies_enqueue_task: one send (plain, keyed, a repeat of a send, a rerun) in one statement.
--
-- Guarantee:
-- - The send writes one PENDING row that names every value, or it writes nothing. Its created_at and updated_at
--   are statement_timestamp(), the time of the send's statement: a rerun in its caller's transaction gets the
--   created_at of Python's rerun insert, not the start of that transaction. In autocommit, statement_timestamp()
--   = now().
-- - Keyed: the key reservation is claimed first (horsies_key_reservation_claim). A key that a live or recent task
--   holds with the same command is KEY_REPLAY with that task's id; with another command it is KEY_CONFLICT with the
--   owner's id. Nothing is written for either.
-- - An id that is already live is SAME_COMMAND when the stored fingerprint version, fingerprint and sent_at equal
--   the sent ones, else PAYLOAD_MISMATCH. A keyed send on a live row bound to another key is PAYLOAD_MISMATCH; on an
--   unbound row it binds its key digest to the row. A row that moved to history between the conflict and the read
--   is CONFLICT_UNVERIFIABLE.
-- - A REPEAT send (the core's send retry sends the same id again) probes history before the insert, under the task
--   advisory lock, so no move can finish between the probe and the insert: a history row answers SAME_COMMAND or
--   PAYLOAD_MISMATCH by the same comparison, never a second run. The provenance reader returns the row's
--   fingerprint version, fingerprint and sent_at from its one leaf probe, so the probe is one read. A FIRST send pays
--   nothing for this.
-- - Every outcome other than INSERTED and a SAME_COMMAND that bound the key to a live row deletes the reservation
--   that the key claim inserted, so a refusal leaves no reservation of this send. (The key claim also deletes an
--   expired TERMINAL reservation of the same key; an expired reservation already counts as absent.)
-- - The insert trigger sends task_new, the queue wake (payload 'new') and horsies_task_status.
--
-- Arguments: every value of the row; four variants, each a kind text plus its columns:
-- - p_start_kind NOW | AT (p_start_at) | AFTER_DELAY (p_start_delay_s, whole seconds >= 0): enqueued_at is
--   statement_timestamp(), the given time, or statement_timestamp() plus the delay. A slot of another kind must be
--   NULL. A send in autocommit has statement_timestamp() = NOW(); a rerun in its caller's transaction is dated by
--   its own statement, not by the start of that transaction (Python's rerun insert).
-- - p_lineage_kind FIRST_RUN | RERUN (p_rerun_of_task_id and p_rerun_root_task_id).
-- - p_key_kind UNKEYED | KEYED (p_key_digest, p_key_scope_version, p_key_window: the reservation window, exact to the
--   microsecond, more than 0 and at most 30 days; the key claim refuses another window with 22023).
-- - p_trace_kind NO_CARRIER | CARRIER (p_trace_carrier, the canonical JSON text of the core's carrier encoder, and
--   p_trace_id, 16 bytes). The core checks the carrier before the call, and sends a carrier that breaks a bound as
--   NO_CARRIER. The pair is not part of the fingerprint: a send that answers SAME_COMMAND, KEY_REPLAY or from history
--   keeps the pair of the row it found.
-- p_send_kind FIRST | REPEAT. The fingerprint version and the 32-byte fingerprint are required: a plain send always
-- carries one. Column shapes (queue name length, retention class key, priority and retry ranges, execution class)
-- are the table's CHECKs.
--
-- Locks: the key reservation row (keyed), then the task advisory lock (REPEAT only), then the task row (only when
-- the id is taken, and FOR UPDATE only for a keyed send). A move of the existing task touches only the reservation
-- of that task's own key, and that key is never the one the key claim just inserted (a task bound to that key makes
-- the claim answer KEY_REPLAY or KEY_CONFLICT before any other lock), so no cycle can form.
-- Returns one row: outcome (INSERTED, SAME_COMMAND, KEY_REPLAY, KEY_CONFLICT, PAYLOAD_MISMATCH,
-- CONFLICT_UNVERIFIABLE) and task_id (the sent id; for KEY_REPLAY and KEY_CONFLICT the id that holds the key).
-- Python: PostgresBroker.enqueue_async (core/brokers/postgres.py:2587-2675), _insert_unkeyed_enqueue (:2677-2709),
-- _insert_keyed_enqueue (:2711-2778), _bind_key_after_task_id_conflict (:2780-2859), _verify_enqueue_conflict
-- (:2861-2925), ENQUEUE_INSERT_SQL (:906-935).
CREATE FUNCTION horsies_enqueue_task(
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
    p_key_window interval,
    p_send_kind text,
    p_trace_kind text,
    p_trace_carrier text,
    p_trace_id bytea
) RETURNS TABLE(outcome text, task_id uuid)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_enqueued_at timestamptz;
    v_keyed boolean;
    v_claim horsies_key_reservation_outcome;
    v_provenance record;
    v_outcome text;
    v_bound_digest bytea;
BEGIN
    IF num_nulls(
        p_task_id, p_task_name, p_queue_name, p_priority, p_input, p_execution, p_retention_class_key,
        p_retain_rerun_input, p_max_retries, p_sent_at, p_start_kind, p_fingerprint_version, p_fingerprint,
        p_lineage_kind, p_key_kind, p_send_kind, p_trace_kind
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
            v_enqueued_at := statement_timestamp();
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
            v_enqueued_at := statement_timestamp() + make_interval(secs => p_start_delay_s);
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
            IF num_nulls(p_key_digest, p_key_scope_version, p_key_window) <> 3 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: an unkeyed send has no key values'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_keyed := FALSE;
        WHEN 'KEYED' THEN
            IF num_nulls(p_key_digest, p_key_scope_version, p_key_window) <> 0 THEN
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
    CASE p_trace_kind
        WHEN 'NO_CARRIER' THEN
            IF num_nulls(p_trace_carrier, p_trace_id) <> 2 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: a send with no carrier has no trace values'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'CARRIER' THEN
            IF num_nulls(p_trace_carrier, p_trace_id) <> 0 OR octet_length(p_trace_id) <> 16 THEN
                RAISE EXCEPTION 'horsies_enqueue_task: a send with a carrier names its text and its 16-byte trace id'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        ELSE
            RAISE EXCEPTION 'horsies_enqueue_task: unknown trace kind'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;

    -- 1. The key reservation. The claim refuses a digest that is not 32 bytes and a window outside 0..30 days.
    IF v_keyed THEN
        SELECT * INTO STRICT v_claim
        FROM horsies_key_reservation_claim(
            p_key_digest, p_key_scope_version, p_key_window,
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
        SELECT p.* INTO v_provenance FROM horsies_task_provenance_staged(p_task_id) AS p;
        IF FOUND THEN
            v_outcome := CASE
                WHEN v_provenance.command_fingerprint_version = p_fingerprint_version
                     AND v_provenance.command_fingerprint = p_fingerprint
                     AND v_provenance.sent_at = p_sent_at THEN 'SAME_COMMAND'
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
        ARRAY[p_trace_carrier], ARRAY[p_trace_id], statement_timestamp(),
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

------------------------------------------------------------------------------------------------------------------------
-- Claim
------------------------------------------------------------------------------------------------------------------------

-- horsies_claim: one claim pass of one worker. Replaces the interim horsies_claim.
--
-- Guarantee:
-- - A pass takes at most what the worker's class budgets, its per-worker limit, the queue caps and the cluster cap
--   allow. The counts run under the cap locks (horsies_claim_in_flight). Expired claims do not count.
-- - Rows are taken in queue priority, then task priority, then enqueued_at, then id; each (queue, class) stream reads
--   only its own index range (horsies_claim_plan, horsies_claim_candidates).
-- - Only due PENDING rows and CLAIMED rows whose lease expired are taken; locked rows are skipped.
-- - Every taken row gets a new claim generation (claimed_at = now()) and a lease of p_lease_ms from the database
--   clock. A lease that is NULL or not above 0 is refused (22023): a claim always expires.
-- - The cap locks last for the one statement. A pass that took less than its plan returns one SHORT row.
--
-- Locks: the advisory cap locks in key order (p_lock_keys), then the row locks of the taken rows (FOR UPDATE SKIP
-- LOCKED). No task advisory lock; the claim never waits on a row.
-- Returns rows of kind CLAIMED (one per taken row: id, task_name, queue_name, input, task_options, execution,
-- is_workflow_task, claimed_at, retry_count, max_retries, priority, enqueued_at, trace_carrier, trace_id) and at most
-- one row of kind SHORT (short_by). retry_count and max_retries let a failed attempt be written in one statement with
-- no read. enqueued_at is the due time of the row (a retry and a delayed send set it), so claimed_at - enqueued_at is
-- the wait in the queue on the database clock. trace_carrier and trace_id are the stored trace pair (NULL for none).
-- Python 0.7.0 (`v0.7.0` package): horsies_claim (core/schemas/migrations.py:713-1003), HORSIES_CLAIM_SQL
-- (core/worker/sql.py:102-120), the claim pass (core/worker/claiming.py:282-296).
CREATE FUNCTION horsies_claim(
    p_worker_id text,
    p_queues jsonb,
    p_queue_priority jsonb,
    p_queue_max_concurrency jsonb,
    p_hard_cap_mode boolean,
    p_budgets jsonb,
    p_routes jsonb,
    p_prefetch_buffer integer,
    p_max_claim_per_worker integer,
    p_max_claim_batch integer,
    p_cluster_wide_cap integer,
    p_lease_ms bigint,
    p_lock_keys jsonb
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
    SET jit TO 'off'
    AS $$
#variable_conflict use_column
DECLARE
    v_key bigint;
    v_capped text[];
    v_my_claimed int;
    v_my_in_flight int;
    v_global_in_flight int;
    v_queue_counts jsonb;
    v_budget_counts jsonb;
    v_capacity int;
    v_extra int;
    v_max_claimed int;
    v_local_available int;
    v_global_remaining int;
    v_total int;
    v_available jsonb;
BEGIN
    -- 0. A claim always carries a lease: the status CHECK requires one on every CLAIMED row, and a claim with no
    --    lease would never return to the queue.
    IF p_lease_ms IS NULL OR p_lease_ms <= 0 THEN
        RAISE EXCEPTION 'horsies_claim: the lease must be greater than 0 ms'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- The worker builds both maps from one claim plan, so a route to a budget that p_budgets does not hold is a
    -- defect of the caller.
    IF EXISTS (
        SELECT 1
        FROM jsonb_each_text(p_routes) AS r(row_class, budget_class)
        WHERE NOT (p_budgets ? r.budget_class)
    ) THEN
        RAISE EXCEPTION
            'horsies_claim: p_routes names a budget class that p_budgets does not hold'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- 1. The advisory cap locks, in ascending key order.
    FOR v_key IN
        SELECT elem::bigint AS k
        FROM jsonb_array_elements_text(p_lock_keys) AS elem
        ORDER BY 1
    LOOP
        PERFORM pg_advisory_xact_lock(v_key);
    END LOOP;

    SELECT COALESCE(array_agg(key), ARRAY[]::text[])
    INTO v_capped
    FROM jsonb_object_keys(p_queue_max_concurrency) AS key;

    -- 2. Counts under the cap locks. Per queue: hard mode counts RUNNING and CLAIMED, soft mode RUNNING. Per budget:
    --    this worker's in-flight rows, by the budget that their class routes to.
    SELECT c.my_claimed, c.my_in_flight, c.global_in_flight, c.queue_counts, c.budget_counts
    INTO v_my_claimed, v_my_in_flight, v_global_in_flight, v_queue_counts, v_budget_counts
    FROM horsies_claim_in_flight(p_worker_id, v_capped, p_hard_cap_mode, p_routes) AS c;

    -- 3. Budget math. The capacity of the worker is the sum over budgets.
    SELECT COALESCE(SUM(b.capacity::int), 0)
    INTO v_capacity
    FROM jsonb_each_text(p_budgets) AS b(budget_class, capacity);
    v_extra := CASE WHEN p_hard_cap_mode THEN 0 ELSE p_prefetch_buffer END;
    v_max_claimed := CASE
        WHEN p_max_claim_per_worker > 0 THEN p_max_claim_per_worker
        WHEN p_prefetch_buffer > 0 THEN v_capacity + p_prefetch_buffer
        ELSE v_capacity END;
    IF v_my_claimed >= v_max_claimed THEN
        RETURN;
    END IF;

    -- A worker with no capacity in service gets no prefetch.
    v_local_available := CASE
        WHEN v_capacity = 0 THEN 0
        ELSE GREATEST(0, v_capacity + v_extra - v_my_in_flight) END;

    IF p_cluster_wide_cap IS NOT NULL THEN
        v_global_remaining := GREATEST(0, p_cluster_wide_cap - v_global_in_flight);
    ELSE
        v_global_remaining := 2147483647;
    END IF;

    v_total := LEAST(v_local_available, v_max_claimed - v_my_claimed, v_global_remaining);
    IF v_total <= 0 THEN
        RETURN;
    END IF;

    -- A budget with no capacity in service gets no prefetch.
    SELECT COALESCE(jsonb_object_agg(
               b.budget_class,
               CASE WHEN b.capacity::int = 0 THEN 0
                   ELSE GREATEST(0, b.capacity::int + v_extra
                       - COALESCE((v_budget_counts ->> b.budget_class)::int, 0))
                   END
           ), '{}'::jsonb)
    INTO v_available
    FROM jsonb_each_text(p_budgets) AS b(budget_class, capacity);

    -- 4. Candidate selection runs in horsies_claim_candidates, which reuses one generic plan; the accounting above
    --    keeps parameter-specific plans. Everything stays in this transaction.
    RETURN QUERY
    SELECT * FROM horsies_claim_candidates(
        v_total, v_available, p_routes, p_queues, p_queue_priority,
        p_queue_max_concurrency, v_queue_counts, p_max_claim_batch,
        p_worker_id, p_lease_ms
    );
END;
$$;

-- horsies_claim_candidates: internal to horsies_claim. Replaces the interim function of the same name.
--
-- Guarantee: locks and claims the rows of the plan of horsies_claim_plan, per planned (queue, class, arm) stream,
-- with FOR UPDATE SKIP LOCKED, and updates them by id: status CLAIMED, claimed_at = now(), the owner, the lease
-- from now(), the finalizing marker cleared.
-- Locks: the row locks of the taken rows only.
-- Returns the CLAIMED rows in claim order and one SHORT row when fewer rows were taken than planned.
-- Python 0.7.0 (`v0.7.0` package): horsies_claim_candidates (core/schemas/migrations.py:1005).
CREATE FUNCTION horsies_claim_candidates(
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

-- horsies_claim_in_flight: internal to horsies_claim. Replaces the interim function of the same name.
--
-- Guarantee: counts the in-flight rows under the cap locks: RUNNING rows and CLAIMED rows whose lease has not
-- expired. Every CLAIMED row has a lease (CHECK horsies_tasks_status_live) and no RUNNING row has one, so the lease
-- test has no NULL arm. The IN form with bitmap and sequential scans off keeps the plain scan of horsies_tasks_status,
-- which marks dead index entries.
-- Locks: none.
-- Returns one row: this worker's CLAIMED count, this worker's in-flight count, the cluster in-flight count, the
-- per-queue counts of the capped queues (JSON object), the per-budget counts of this worker (JSON object).
-- Python 0.7.0 (`v0.7.0` package): horsies_claim_in_flight (core/schemas/migrations.py:629).
CREATE FUNCTION horsies_claim_in_flight(
    p_worker_id text,
    p_capped text[],
    p_hard_cap_mode boolean,
    p_routes jsonb
) RETURNS TABLE(my_claimed bigint, my_in_flight bigint, global_in_flight bigint, queue_counts jsonb,
                budget_counts jsonb)
    LANGUAGE plpgsql STABLE
    SET jit TO 'off'
    SET enable_bitmapscan TO 'off'
    SET enable_seqscan TO 'off'
    AS $$
#variable_conflict use_column
BEGIN
    RETURN QUERY
    WITH in_flight AS (
        SELECT t.queue_name, t.status, t.claimed_by_worker_id, t.execution
        FROM horsies_tasks t
        WHERE t.status IN ('RUNNING', 'CLAIMED')
          AND (t.status = 'RUNNING' OR t.claim_expires_at > now())
    )
    SELECT
        COUNT(*) FILTER (WHERE f.claimed_by_worker_id = p_worker_id AND f.status = 'CLAIMED'),
        COUNT(*) FILTER (WHERE f.claimed_by_worker_id = p_worker_id),
        COUNT(*),
        COALESCE((
            SELECT jsonb_object_agg(g.queue_name, g.cnt) FROM (
                SELECT q.queue_name, COUNT(*) AS cnt
                FROM in_flight q
                WHERE q.queue_name = ANY(p_capped)
                  AND (p_hard_cap_mode OR q.status = 'RUNNING')
                GROUP BY q.queue_name
            ) g
        ), '{}'::jsonb),
        COALESCE((
            SELECT jsonb_object_agg(g.budget_class, g.cnt) FROM (
                SELECT p_routes ->> m.execution AS budget_class, COUNT(*) AS cnt
                FROM in_flight m
                WHERE m.claimed_by_worker_id = p_worker_id
                  AND p_routes ? m.execution
                GROUP BY 1
            ) g
        ), '{}'::jsonb)
    FROM in_flight f;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Start
------------------------------------------------------------------------------------------------------------------------

-- horsies_start_attempt: the RUNNING write of one claimed task, plain or workflow.
--
-- Guarantee:
-- - The row becomes RUNNING only when it is CLAIMED by p_worker_id at the claim generation p_claimed_at, its lease
--   has not expired, its good_until has not passed, and, for a workflow task, its workflow is not PAUSED or
--   CANCELLED and its node is linked (node status ENQUEUED, READY, PENDING or RUNNING). is_workflow_task comes from
--   the locked row.
-- - For a workflow task, the node's RUNNING write commits with the start. A node that is RUNNING already (a retried
--   attempt) keeps its first started_at. A start that does not apply writes nothing.
-- - The start writes no runner beat. started_at is the start of this attempt: the stale RUNNING probes
--   (horsies_stale_running_scan, horsies_finish_stale_attempt) read only the runner beats sent at or after it, so a
--   beat of an earlier attempt of the task never counts for this attempt, and an attempt with no such beat is judged
--   from started_at.
-- - The start sends no status event (the update trigger skips RUNNING).
-- - A miss ends its row lock with the statement, so the miss path that follows (expiry, or the stop of a workflow
--   task) never runs while this lock is held.
-- - The miss reason is the first that holds, in this order: DEADLINE_PASSED (CLAIMED by this worker at any claim
--   generation, good_until passed: the expiry applies), NOT_HELD (absent, or the status, owner or claim generation
--   differs), LEASE_EXPIRED, WORKFLOW_PAUSED, WORKFLOW_CANCELLED, NODE_NOT_LINKED.
--
-- Arguments: every argument is required (22023): the task, the worker, the claim generation and the process facts
-- of the row (pid, host, process name).
-- Locks: the task row (FOR NO KEY UPDATE, read by id alone so only the primary key serves it), then the linked node
-- row (FOR UPDATE), the order that the move uses. No task advisory lock and no workflow lock: nothing moves.
-- Returns one row: outcome STARTED with started_at, or MISSED with miss_reason. A STARTED workflow task also returns
-- the workflow id and the task index of its node (read with the node row the start locks); both are NULL for a plain
-- task and for a miss.
-- Python 0.7.0 (`v0.7.0` package): the RUNNING write (core/worker/child_runner.py:711-761, set_running_params
-- :899-925), the workflow guard (:740-748), the node write (:763-781), _confirm_ownership_and_set_running
-- (:1141-1257). Its RUNNING write also inserted a runner beat (first_beat, :753-759).
CREATE FUNCTION horsies_start_attempt(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone,
    p_worker_pid integer,
    p_worker_hostname text,
    p_worker_process_name text
) RETURNS TABLE(
    outcome text,
    miss_reason text,
    started_at timestamp with time zone,
    workflow_id uuid,
    workflow_task_index integer
)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_task record;
    v_link record;
    v_workflow_id uuid;
    v_workflow_task_index integer;
BEGIN
    IF num_nulls(
        p_task_id, p_worker_id, p_claimed_at, p_worker_pid, p_worker_hostname, p_worker_process_name
    ) <> 0 THEN
        RAISE EXCEPTION 'horsies_start_attempt: every argument is required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT t.status, t.claimed_by_worker_id, t.claimed_at, t.claim_expires_at, t.good_until, t.is_workflow_task
    INTO v_task
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR NO KEY UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT 'MISSED'::text, 'NOT_HELD'::text, NULL::timestamptz, NULL::uuid, NULL::integer;
        RETURN;
    END IF;
    IF v_task.status = 'CLAIMED'
       AND v_task.claimed_by_worker_id = p_worker_id
       AND v_task.good_until <= NOW() THEN
        RETURN QUERY SELECT 'MISSED'::text, 'DEADLINE_PASSED'::text, NULL::timestamptz, NULL::uuid, NULL::integer;
        RETURN;
    END IF;
    IF (
        v_task.status = 'CLAIMED'
        AND v_task.claimed_by_worker_id = p_worker_id
        AND v_task.claimed_at = p_claimed_at
    ) IS NOT TRUE THEN
        RETURN QUERY SELECT 'MISSED'::text, 'NOT_HELD'::text, NULL::timestamptz, NULL::uuid, NULL::integer;
        RETURN;
    END IF;
    IF v_task.claim_expires_at <= NOW() THEN
        RETURN QUERY SELECT 'MISSED'::text, 'LEASE_EXPIRED'::text, NULL::timestamptz, NULL::uuid, NULL::integer;
        RETURN;
    END IF;

    IF v_task.is_workflow_task THEN
        -- The node by task_id (ix_horsies_workflow_tasks_task_id), its workflow by primary key, unlocked.
        SELECT n.id AS node_id, n.status AS node_status, n.workflow_id, n.task_index,
               (SELECT w.status FROM horsies_workflows w WHERE w.id = n.workflow_id) AS workflow_status
        INTO v_link
        FROM horsies_workflow_tasks n
        WHERE n.task_id = p_task_id
        FOR UPDATE;
        IF FOUND AND v_link.workflow_status = 'PAUSED' THEN
            RETURN QUERY SELECT 'MISSED'::text, 'WORKFLOW_PAUSED'::text, NULL::timestamptz, NULL::uuid, NULL::integer;
            RETURN;
        END IF;
        IF FOUND AND v_link.workflow_status = 'CANCELLED' THEN
            RETURN QUERY
            SELECT 'MISSED'::text, 'WORKFLOW_CANCELLED'::text, NULL::timestamptz, NULL::uuid, NULL::integer;
            RETURN;
        END IF;
        IF NOT FOUND OR v_link.node_status NOT IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING') THEN
            RETURN QUERY SELECT 'MISSED'::text, 'NODE_NOT_LINKED'::text, NULL::timestamptz, NULL::uuid, NULL::integer;
            RETURN;
        END IF;
        v_workflow_id := v_link.workflow_id;
        v_workflow_task_index := v_link.task_index;
    END IF;

    UPDATE horsies_tasks t
    SET status = 'RUNNING',
        claim_expires_at = NULL,
        started_at = NOW(),
        worker_pid = p_worker_pid,
        worker_hostname = p_worker_hostname,
        worker_process_name = p_worker_process_name,
        updated_at = NOW()
    WHERE t.id = p_task_id;

    IF v_task.is_workflow_task THEN
        IF v_link.node_status <> 'RUNNING' THEN
            UPDATE horsies_workflow_tasks n
            SET status = 'RUNNING',
                started_at = NOW()
            WHERE n.id = v_link.node_id;
        END IF;
    END IF;

    RETURN QUERY SELECT 'STARTED'::text, NULL::text, NOW(), v_workflow_id, v_workflow_task_index;
END
$$;

-- horsies_mark_finalizing: the finalizing marker of a run whose body ended, written by the child before the parent
-- finalizes the attempt.
--
-- Guarantee:
-- - The row gets finalizing_at = NOW(), finalizing_by_worker_id = p_worker_id and updated_at = NOW() only when it is
--   RUNNING under p_worker_id at the claim generation p_claimed_at. The generation comparison has no NULL arm.
-- - A stale attempt cannot refresh the marker of a later claim of the same task: the fence is read on the locked
--   row, so a claim that changed while this call waited for the lock is NOT_HELD.
-- - The marker sends no status event (the status does not change) and moves nothing.
--
-- Arguments: every argument is required (22023): the task, the worker, the claim generation.
-- Locks: the task row (FOR NO KEY UPDATE, read by id alone so only the primary key serves it). No task advisory lock:
-- nothing moves.
-- Returns one row: outcome MARKED, or NOT_HELD (absent, not RUNNING, or another owner or claim generation; nothing
-- is written).
-- Python: MARK_FINALIZING_SQL (core/worker/child_runner.py:787-806), _mark_task_finalizing (:1257-1299),
-- mark_task_finalizing (core/worker/shared_lifecycle.py:470-500).
CREATE FUNCTION horsies_mark_finalizing(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone
) RETURNS TABLE(outcome text)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_held boolean;
BEGIN
    IF num_nulls(p_task_id, p_worker_id, p_claimed_at) <> 0 THEN
        RAISE EXCEPTION 'horsies_mark_finalizing: every argument is required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT (
        t.status = 'RUNNING'
        AND t.claimed_by_worker_id = p_worker_id
        AND t.claimed_at = p_claimed_at
    ) IS TRUE
    INTO v_held
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR NO KEY UPDATE;

    -- An absent row leaves v_held NULL.
    IF v_held IS NOT TRUE THEN
        RETURN QUERY SELECT 'NOT_HELD'::text;
        RETURN;
    END IF;

    UPDATE horsies_tasks t
    SET finalizing_at = NOW(),
        finalizing_by_worker_id = p_worker_id,
        updated_at = NOW()
    WHERE t.id = p_task_id;

    RETURN QUERY SELECT 'MARKED'::text;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Attempt finish: success
------------------------------------------------------------------------------------------------------------------------

-- horsies_node_has_dependents: internal. Whether a node of the same workflow lists the node of this task in its
-- dependencies. Dependencies never change after the workflow start, so the answer read after the move holds for
-- phase 2. Reads the node by task_id (ix_horsies_workflow_tasks_task_id) and the nodes of its workflow
-- (uq_horsies_workflow_task_index). FALSE when no node links the task.
-- Callers: horsies_finish_workflow_success, horsies_finish_failed_attempt, horsies_finish_stale_attempt, after the
-- move of a workflow task, so node_has_dependents has one meaning in every function that returns it.
-- Python: no counterpart in phase 1; Python's phase 2 learns the dependents when it reads the workflow's nodes.
CREATE FUNCTION horsies_node_has_dependents(p_task_id uuid) RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
    SELECT EXISTS (
        SELECT 1
        FROM horsies_workflow_tasks n
        JOIN horsies_workflow_tasks d ON d.workflow_id = n.workflow_id
        WHERE n.task_id = p_task_id
          AND n.task_index = ANY (d.dependencies)
    )
$$;

-- horsies_node_downstream_subworkflow: internal. The downstream_subworkflow fact of the node of this task (REACHABLE
-- or NONE), written at workflow start from the static graph; NULL when no node links the task. Phase 2 of a
-- REACHABLE node runs in a transaction, because its promotion can owe a child start. Reads the node by task_id
-- (ix_horsies_workflow_tasks_task_id); takes no lock.
-- Callers: horsies_finish_workflow_success, horsies_finish_failed_attempt, horsies_finish_stale_attempt, next to
-- node_has_dependents.
-- Python: no counterpart; Python's phase 2 always runs in one transaction.
CREATE FUNCTION horsies_node_downstream_subworkflow(p_task_id uuid) RETURNS text
    LANGUAGE sql STABLE
    AS $$
    SELECT n.downstream_subworkflow::text
    FROM horsies_workflow_tasks n
    WHERE n.task_id = p_task_id
$$;

-- horsies_complete_task_fused: the success of one plain task. Replaces the interim function of the same name.
--
-- Guarantee:
-- - In one statement: the fence (RUNNING, owner, claim generation), the COMPLETED attempt row (started_at of the
--   row, finished_at from clock_timestamp()), the move to history (COMPLETE_FUSED) and, when p_wake is TRUE, the
--   capacity wake pg_notify('task_queue_' || queue_name, 'capacity') with the queue of the locked row.
-- - A refusal writes nothing and is classified by horsies_terminalization_miss: ALREADY_APPLIED (a replay),
--   LOST_CLAIM, SOURCE_STATE_CONFLICT, TASK_ABSENT.
-- - The claim generation is required (22023 when NULL).
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
    -- The whole locked row (task): the move takes it as read here.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.queue_name,
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
-- interim function of the same name.
--
-- Guarantee: each row keeps its own fence and outcome: the function calls horsies_complete_task_fused once per row,
-- in task id order. PostgreSQL delivers one notification per channel and payload in a transaction, so the rows of
-- one queue send one capacity wake. Refused (22023) before any row runs: a NULL array, an empty batch, more than 64
-- rows, arrays of different lengths, a NULL or repeated task id, a NULL claim generation, a NULL worker or wake.
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
    v_item record;
BEGIN
    IF num_nulls(p_task_ids, p_claimed_ats, p_results, p_worker_id, p_wake) <> 0 THEN
        RAISE EXCEPTION 'horsies_complete_tasks_fused: batch arrays, worker and wake must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_task_ids) = 0 OR cardinality(p_task_ids) > 64 THEN
        RAISE EXCEPTION 'horsies_complete_tasks_fused: a batch holds 1 to 64 rows'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_claimed_ats) <> cardinality(p_task_ids)
       OR cardinality(p_results) <> cardinality(p_task_ids) THEN
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

    FOR v_item IN
        SELECT u.id, u.claimed_at, u.result, u.position
        FROM unnest(p_task_ids, p_claimed_ats, p_results) WITH ORDINALITY AS u(id, claimed_at, result, position)
        ORDER BY u.id
    LOOP
        RETURN QUERY
        SELECT c.task_id, v_item.position, c.outcome, c.terminal_at, c.terminalization_kind, c.observed_status,
               c.observed_worker_id, c.observed_claimed_at, c.guard_kind, c.observed_guard
        FROM horsies_complete_task_fused(v_item.id, p_worker_id, v_item.claimed_at, v_item.result, p_wake) AS c;
    END LOOP;
END
$$;

-- horsies_finish_workflow_success: the success of one workflow task (phase 1). Replaces the interim function of
-- the same name.
--
-- Guarantee: in one statement: the fence (RUNNING, owner, claim generation), the COMPLETED attempt row, the move
-- (COMPLETE_LOCKED), which records the phase-2 work, then node_has_dependents and downstream_subworkflow. No
-- capacity wake: phase 2 sends it.
-- A refusal writes nothing and is classified by horsies_terminalization_miss. Refused (22023): a NULL claim
-- generation, a plain task. A NULL result is refused with 23502 (not_null_violation), as the move requires one.
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
    -- The whole locked row (task): the move takes it as read here.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.is_workflow_task, t.queue_name, t.task_name,
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
        v_ctx.task, 'COMPLETED', 'COMPLETE_LOCKED', v_terminal_at, p_result, NULL, NULL
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'COMPLETE_LOCKED'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id::text, v_ctx.claimed_at, NULL::text, NULL::jsonb,
        v_ctx.queue_name::text, v_ctx.task_name::text, horsies_node_has_dependents(p_task_id),
        horsies_node_downstream_subworkflow(p_task_id);
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Attempt finish: failure and retry
------------------------------------------------------------------------------------------------------------------------

-- horsies_finish_failed_attempt: one failed attempt of a task this worker holds: retry, terminal failure, or the
-- release of a row whose child never started it. Replaces the interim function of the same name.
--
-- Guarantee:
-- - No read before it: retry_count, max_retries and task_options cannot change inside one claim generation (only
--   the retry writers change retry_count, and both end the generation), so the caller computes the retry request
--   from the claimed row (horsies_claim returns them).
-- - p_claimed_row_rule RELEASE (the child was killed before it reported): a row that is still CLAIMED by this worker
--   at this claim generation goes back to PENDING with every claim and run field cleared and no attempt row
--   (RELEASED). REFUSE (the child reported a result): the row must be RUNNING.
-- - Fence: RUNNING, owner, claim generation, and, with a retry request, the retry count that the request used. A
--   refusal writes nothing and is classified by horsies_terminalization_miss (FAIL_RUNNING).
-- - The attempt row (p_attempt_outcome FAILED or WORKER_FAILURE) is written with will_retry.
-- - Retry: next_retry_at = clock_timestamp() + the delay; refused when it would be at or after good_until, judged
--   under the lock (the failure is then terminal with retry_disposition PAST_DEADLINE). The row goes back to PENDING
--   with every claim and run field cleared, retry_count + 1, enqueued_at = next_retry_at, and execution ISOLATED when
--   the placement is ISOLATED. The delayed retry wake is the caller's, at next_retry_at.
-- - Terminal: the move (FAIL_RUNNING) and, for a plain task with p_wake TRUE, the capacity wake
--   pg_notify('task_queue_' || queue_name, 'capacity'), in the same statement. A workflow task gets
--   node_has_dependents and downstream_subworkflow; its phase 2 sends the wake.
--
-- Arguments: p_retry_kind NONE | RETRY (p_expected_retry_count, p_retry_delay_s finite and >= 0, p_retry_placement
-- SAME_CLASS | ISOLATED; NULL for NONE). p_result, the task's error code and failed reason are the terminal values;
-- p_attempt_message and p_attempt_reason go to the attempt row. Refused (22023): a NULL task, worker, claim
-- generation, rule or wake; a retry slot that does not fit its kind; an unknown placement, rule or attempt outcome.
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
    -- The whole locked row (task): the move takes it as read here.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.good_until, t.is_workflow_task,
           t.queue_name, t.task_name, clock_timestamp() AS db_now, t AS task
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
-- Worker liveness
------------------------------------------------------------------------------------------------------------------------

-- horsies_worker_liveness_key: the key of the session advisory lock that a worker process holds while it lives.
--
-- Guarantee: hashtextextended('horsies-worker-liveness:' || p_worker_id, 0); one key for one worker id. A worker id
-- is a new UUIDv4 at each process start, so a restarted worker never takes the key of its earlier process. A NULL id
-- gives NULL (STRICT).
-- Locks: none. Reads nothing.
CREATE FUNCTION horsies_worker_liveness_key(p_worker_id text) RETURNS bigint
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$ SELECT hashtextextended('horsies-worker-liveness:' || p_worker_id, 0) $$;

-- horsies_take_worker_liveness: one acquisition of the liveness lock of p_worker_id by this session, with its
-- generation.
--
-- Guarantee:
-- - It tries the session advisory lock on horsies_worker_liveness_key(p_worker_id), with no wait. Taken: it writes a
--   new generation for p_worker_id (the next value of horsies_worker_liveness_generation) into
--   horsies_worker_liveness and returns true, so the generation commits with the caller's transaction. Held by
--   another session: it writes nothing and returns false.
-- - The lock is a session lock, and a rollback keeps it: a session whose call failed, or whose transaction did not
--   commit, can hold the lock with no new generation. The caller ends that session.
-- - A worker counts an acquisition only after this call committed. Each hold that a live worker counts then has a
--   generation that commits while the hold lasts, so a regain between two reaper reads changes the generation that
--   the later reads see.
-- Arguments: p_worker_id not NULL (else 22023).
-- Locks: the session advisory lock of the key, until the session ends; the row of p_worker_id in
--   horsies_worker_liveness, until the transaction ends. It waits for a concurrent write of that row.
-- It does not commit.
-- Python: none (a core addition).
CREATE FUNCTION horsies_take_worker_liveness(p_worker_id text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF p_worker_id IS NULL THEN
        RAISE EXCEPTION 'horsies_take_worker_liveness: the worker id must not be NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF NOT pg_try_advisory_lock(horsies_worker_liveness_key(p_worker_id)) THEN
        RETURN false;
    END IF;
    INSERT INTO horsies_worker_liveness (worker_id, generation)
    VALUES (p_worker_id, nextval('horsies_worker_liveness_generation'))
    ON CONFLICT (worker_id) DO UPDATE SET generation = EXCLUDED.generation;
    RETURN true;
END
$$;

-- horsies_unheld_worker_owners: the liveness read of one reaper pass. The owners of CLAIMED and RUNNING rows whose
-- liveness lock no session of this database holds, with the generation of their latest acquisition.
--
-- Guarantee:
-- - Each returned owner owns at least one CLAIMED or RUNNING row, and the lock table (one read of pg_locks for the
--   whole statement) shows no granted advisory lock on its key (horsies_worker_liveness_key) in this database.
-- - generation is the owner's row of horsies_worker_liveness in the snapshot of this statement, which is taken
--   before the lock table read. It is NULL when the owner has no row: no acquisition is recorded, or the row was
--   deleted (horsies_forget_ended_worker_liveness).
-- - A worker takes its lock with a generation (horsies_take_worker_liveness) before its first claim, and holds it on
--   its own session until it ends; after a lost session it takes the lock again, with a new generation, within its
--   regain bound, or it fails closed. So a returned owner has ended, or is inside that bound, or took the lock again
--   since a read that saw an earlier generation.
-- - The caller judges an owner gone when two reads at least the regain bound plus a margin apart return it at one
--   generation, no read between them left it out or returned another generation, and a read of the generations
--   after the later one (horsies_worker_liveness_generations) still shows that generation. It then recovers the
--   owner's RUNNING and CLAIMED rows with the evidence OWNER_GONE (horsies_stale_running_scan,
--   horsies_finish_stale_attempt, horsies_requeue_stale_claims). An owner with a NULL generation is never judged.
-- - At most p_batch_size owners, in worker id order (C collation), so a cut list is cut at the same place by the
--   next read.
-- Arguments: p_batch_size 1..10000 (else 22023).
-- Locks: none. It takes no lock and waits for none.
-- Returns one row per owner: worker_id, generation, and db_now (clock_timestamp() after the lock table read).
-- Python: none (a core addition; Python recovers the rows of an ended worker by the beat age only).
CREATE FUNCTION horsies_unheld_worker_owners(p_batch_size integer)
RETURNS TABLE(worker_id text, generation bigint, db_now timestamp with time zone)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
BEGIN
    IF p_batch_size IS NULL OR p_batch_size < 1 OR p_batch_size > 10000 THEN
        RAISE EXCEPTION 'horsies_unheld_worker_owners: the batch size must be 1 to 10000'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    RETURN QUERY
    WITH held AS MATERIALIZED (
        -- A bigint advisory key is (classid << 32) | objid, with objsubid 1.
        SELECT (l.classid::bigint << 32) | l.objid::bigint AS lock_key
        FROM pg_locks l
        WHERE l.locktype = 'advisory'
          AND l.objsubid = 1
          AND l.granted
          AND l.database = (SELECT d.oid FROM pg_database d WHERE d.datname = current_database())
    ),
    owners AS (
        SELECT DISTINCT t.claimed_by_worker_id::text AS owner_id
        FROM horsies_tasks t
        WHERE t.status IN ('CLAIMED', 'RUNNING')
          AND t.claimed_by_worker_id IS NOT NULL
    )
    SELECT o.owner_id, r.generation, clock_timestamp()
    FROM owners o
    LEFT JOIN horsies_worker_liveness r ON r.worker_id = o.owner_id
    WHERE NOT EXISTS (
        SELECT 1 FROM held h WHERE h.lock_key = horsies_worker_liveness_key(o.owner_id)
    )
    ORDER BY o.owner_id COLLATE "C"
    LIMIT p_batch_size;
END
$$;

-- horsies_worker_liveness_generations: the current generation of each given worker.
--
-- Guarantee: one row for each distinct worker id of p_worker_ids that has a row in horsies_worker_liveness, with its
-- generation in the snapshot of this statement. A worker with no row is not returned. A reaper runs it as a
-- statement of its own after a liveness read (horsies_unheld_worker_owners): its snapshot then follows that read's
-- lock table read, so it shows each generation that committed before that read found the lock free.
-- Arguments: p_worker_ids not NULL, with at most 10000 elements (else 22023).
-- Locks: none. It takes no lock and waits for none.
-- Returns one row per worker: worker_id, generation, in worker id order (C collation).
-- Python: none (a core addition).
CREATE FUNCTION horsies_worker_liveness_generations(p_worker_ids text[])
RETURNS TABLE(worker_id text, generation bigint)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
BEGIN
    IF p_worker_ids IS NULL OR cardinality(p_worker_ids) > 10000 THEN
        RAISE EXCEPTION 'horsies_worker_liveness_generations: the worker ids must be an array of at most 10000'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    RETURN QUERY
    SELECT r.worker_id, r.generation
    FROM horsies_worker_liveness r
    WHERE r.worker_id = ANY (p_worker_ids)
    ORDER BY r.worker_id COLLATE "C";
END
$$;

-- horsies_forget_ended_worker_liveness: delete the liveness rows of ended workers.
--
-- Guarantee:
-- - It deletes at most p_batch_size rows of horsies_worker_liveness, in worker id order (C collation): rows of
--   workers that own no CLAIMED or RUNNING row and whose liveness lock no session of this database holds (one read
--   of pg_locks for the whole statement).
-- - A row whose generation changed after the snapshot of this statement is kept: the delete checks the generation
--   again on the row version that it locks, so a new acquisition stays recorded.
-- - A deleted row makes no reaper judge an owner gone: an owner with no row has a NULL generation in
--   horsies_unheld_worker_owners. A worker that takes its lock again writes a new row with a new generation.
-- Arguments: p_batch_size 1..10000 (else 22023).
-- Locks: the rows it deletes, until the transaction ends. It waits for a concurrent write of such a row.
-- Returns the number of deleted rows. It does not commit.
-- Python: none (a core addition).
CREATE FUNCTION horsies_forget_ended_worker_liveness(p_batch_size integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_deleted integer;
BEGIN
    IF p_batch_size IS NULL OR p_batch_size < 1 OR p_batch_size > 10000 THEN
        RAISE EXCEPTION 'horsies_forget_ended_worker_liveness: the batch size must be 1 to 10000'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    WITH held AS MATERIALIZED (
        -- A bigint advisory key is (classid << 32) | objid, with objsubid 1.
        SELECT (l.classid::bigint << 32) | l.objid::bigint AS lock_key
        FROM pg_locks l
        WHERE l.locktype = 'advisory'
          AND l.objsubid = 1
          AND l.granted
          AND l.database = (SELECT d.oid FROM pg_database d WHERE d.datname = current_database())
    ),
    owners AS MATERIALIZED (
        SELECT DISTINCT t.claimed_by_worker_id::text AS owner_id
        FROM horsies_tasks t
        WHERE t.status IN ('CLAIMED', 'RUNNING')
          AND t.claimed_by_worker_id IS NOT NULL
    ),
    ended AS (
        SELECT r.worker_id, r.generation
        FROM horsies_worker_liveness r
        WHERE NOT EXISTS (SELECT 1 FROM owners o WHERE o.owner_id = r.worker_id)
          AND NOT EXISTS (
              SELECT 1 FROM held h WHERE h.lock_key = horsies_worker_liveness_key(r.worker_id)
          )
        ORDER BY r.worker_id COLLATE "C"
        LIMIT p_batch_size
    )
    DELETE FROM horsies_worker_liveness r
    USING ended e
    WHERE r.worker_id = e.worker_id
      AND r.generation = e.generation;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Stale RUNNING recovery
------------------------------------------------------------------------------------------------------------------------

-- horsies_stale_running_scan: the candidates of one stale RUNNING pass. No locks.
--
-- p_evidence names what makes a RUNNING row a candidate, a closed set:
-- - BEAT_AGE (both thresholds, >= 0; no owner and no claim bound): a row with a start, no finalizing marker newer
--   than p_finalizing_stale_after_ms, and no runner beat of this attempt newer than p_stale_after_ms (its latest
--   beat sent at or after started_at, or started_at when there is none). A beat sent before started_at belongs to
--   an earlier attempt of the task: the start writes no beat, so such a beat would otherwise stand for a new attempt
--   that has not beaten yet. The beat probe also reads only beats newer than the larger threshold: an older beat
--   satisfies the staleness test for every value, so the bound cannot change the verdict, and it keeps the probe on
--   the current hourly leaves.
-- - OWNER_GONE (p_owner_worker_id and p_claimed_before; no threshold): a row with a start that p_owner_worker_id
--   claimed before p_claimed_before. The caller judged the owner gone: horsies_unheld_worker_owners returned it in
--   two reads far enough apart, at one generation. A gone owner writes no beat and finalizes nothing, so the beat
--   age and the finalizing marker are not read. The claim bound is the database time of the first of those reads:
--   a row claimed after it is not judged by that evidence.
-- Guarantee: at most p_batch_size rows. Each candidate is judged again under its locks by
-- horsies_finish_stale_attempt with the same evidence; the scan is only a hint.
-- Arguments: an evidence of the set with exactly its arguments, thresholds of 0 ms or more, p_batch_size 1..10000;
-- anything else is refused with 22023.
-- Locks: none.
-- Returns one row per candidate: the task name, the queue and the stored trace id (the caller's records and
-- recovery events), the facts the caller needs for the retry decision (retry_count, max_retries, task_options,
-- good_until), for the crash result (claimed_by_worker_id, worker_pid, worker_hostname, worker_process_name,
-- started_at) and for the fence of OWNER_GONE (claimed_at), and db_now (clock_timestamp()).
-- Python 0.7.0 (`v0.7.0` package): SELECT_STALE_RUNNING_TASKS_SQL (core/brokers/postgres.py:766-788) and its session
-- (core/brokers/postgres.py:3972-3984), for BEAT_AGE. OWNER_GONE is a core addition.
CREATE FUNCTION horsies_stale_running_scan(
    p_evidence text,
    p_stale_after_ms integer,
    p_finalizing_stale_after_ms integer,
    p_owner_worker_id text,
    p_claimed_before timestamp with time zone,
    p_batch_size integer
) RETURNS TABLE(
    task_id uuid,
    task_name text,
    queue_name text,
    trace_id bytea,
    retry_count integer,
    max_retries integer,
    task_options text,
    good_until timestamp with time zone,
    claimed_by_worker_id text,
    claimed_at timestamp with time zone,
    worker_pid integer,
    worker_hostname text,
    worker_process_name text,
    started_at timestamp with time zone,
    db_now timestamp with time zone
)
    LANGUAGE plpgsql STABLE
    AS $$
#variable_conflict use_column
BEGIN
    CASE p_evidence
        WHEN 'BEAT_AGE' THEN
            IF p_stale_after_ms IS NULL OR p_stale_after_ms < 0
               OR p_finalizing_stale_after_ms IS NULL OR p_finalizing_stale_after_ms < 0
               OR num_nulls(p_owner_worker_id, p_claimed_before) <> 2 THEN
                RAISE EXCEPTION
                    'horsies_stale_running_scan: BEAT_AGE takes two thresholds of 0 ms or more and no owner'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'OWNER_GONE' THEN
            IF num_nulls(p_stale_after_ms, p_finalizing_stale_after_ms) <> 2
               OR num_nulls(p_owner_worker_id, p_claimed_before) <> 0 THEN
                RAISE EXCEPTION
                    'horsies_stale_running_scan: OWNER_GONE takes an owner and a claim bound and no threshold'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        ELSE
            RAISE EXCEPTION 'horsies_stale_running_scan: unknown evidence'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    IF p_batch_size IS NULL OR p_batch_size < 1 OR p_batch_size > 10000 THEN
        RAISE EXCEPTION 'horsies_stale_running_scan: the batch size must be 1 to 10000'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    CASE p_evidence
        WHEN 'BEAT_AGE' THEN
            RETURN QUERY
            SELECT t.id, t.task_name::text, t.queue_name::text, t.trace_id, t.retry_count, t.max_retries,
                   t.task_options, t.good_until, t.claimed_by_worker_id::text, t.claimed_at, t.worker_pid,
                   t.worker_hostname::text, t.worker_process_name::text, t.started_at, clock_timestamp()
            FROM horsies_tasks t
            WHERE t.status = 'RUNNING'
              AND t.started_at IS NOT NULL
              AND (
                  t.finalizing_at IS NULL
                  OR t.finalizing_at < NOW() - make_interval(secs => p_finalizing_stale_after_ms / 1000.0)
              )
              AND COALESCE(
                  (
                      SELECT h.sent_at
                      FROM horsies_heartbeats h
                      WHERE h.task_id = t.id
                        AND h.role = 'runner'
                        AND h.sent_at >= NOW() - make_interval(
                            secs => GREATEST(p_stale_after_ms, p_finalizing_stale_after_ms) / 1000.0
                        )
                        AND h.sent_at >= t.started_at
                      ORDER BY h.sent_at DESC
                      LIMIT 1
                  ),
                  t.started_at
              ) < NOW() - make_interval(secs => p_stale_after_ms / 1000.0)
            LIMIT p_batch_size;
        WHEN 'OWNER_GONE' THEN
            RETURN QUERY
            SELECT t.id, t.task_name::text, t.queue_name::text, t.trace_id, t.retry_count, t.max_retries,
                   t.task_options, t.good_until, t.claimed_by_worker_id::text, t.claimed_at, t.worker_pid,
                   t.worker_hostname::text, t.worker_process_name::text, t.started_at, clock_timestamp()
            FROM horsies_tasks t
            WHERE t.status = 'RUNNING'
              AND t.started_at IS NOT NULL
              AND t.claimed_by_worker_id = p_owner_worker_id
              AND t.claimed_at < p_claimed_before
            LIMIT p_batch_size;
    END CASE;
END
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
-- - Judged under the task advisory lock and the row lock, on one capture of the row, its latest runner beat (the
--   bounds of horsies_stale_running_scan: sent at or after started_at, inside the recency bound; BEAT_AGE only) and
--   NOW().
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
-- - One FAILED attempt with the caller's error code (WORKER_CRASHED) is written with will_retry.
-- - Retry: next_retry_at = clock_timestamp() + the delay, refused at or after good_until under the lock (then the
--   failure with retry_disposition PAST_DEADLINE). The row goes back to PENDING with every claim and run field
--   cleared, retry_count + 1, enqueued_at = next_retry_at; the execution class is kept. The delayed retry wake is the
--   caller's, at next_retry_at.
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
CREATE FUNCTION horsies_finish_stale_attempt(
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
    -- One capture: the locked row, the beat read beside it (BEAT_AGE), and the instant both thresholds are judged
    -- at. Nothing is read again after a refusal, so the evidence cannot show a beat that the judgement did not see.
    -- The whole locked row (task) is the row that the move takes.
    SELECT t.id, t.status, t.retry_count, t.started_at, t.finalizing_at, t.claimed_by_worker_id, t.claimed_at,
           t.worker_hostname, t.worker_pid, t.worker_process_name, t.good_until, t.is_workflow_task,
           t.queue_name, t.task_name,
           CASE p_evidence WHEN 'BEAT_AGE' THEN (
               SELECT h.sent_at
               FROM horsies_heartbeats h
               WHERE h.task_id = t.id
                 AND h.role = 'runner'
                 AND h.sent_at >= NOW() - make_interval(
                     secs => GREATEST(p_stale_after_ms, p_finalizing_stale_after_ms) / 1000.0
                 )
                 AND h.sent_at >= t.started_at
               ORDER BY h.sent_at DESC
               LIMIT 1
           ) END AS last_heartbeat,
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

    IF p_evidence = 'BEAT_AGE' AND NOT (
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
-- Requeue: stale claims and released claims
------------------------------------------------------------------------------------------------------------------------

-- horsies_requeue_stale_claims: one bounded batch of the stale CLAIMED requeue.
--
-- p_evidence names what makes a CLAIMED row stale, a closed set:
-- - BEAT_AGE (p_stale_after_ms >= 0; no owner and no claim bound): no claimer beat newer than p_stale_after_ms, and
--   claimed longer ago than that. The claimed_at test keeps a fresh claim of a row whose earlier claim generation
--   left old beats; Python's "latest beat older than the threshold" requeued such a row.
-- - OWNER_GONE (p_owner_worker_id and p_claimed_before; no threshold): claimed by p_owner_worker_id before
--   p_claimed_before. The caller judged the owner gone (horsies_unheld_worker_owners, as for
--   horsies_stale_running_scan); the claim bound is the database time of the first read that found its lock free.
-- Guarantee:
-- - A stale row goes back to PENDING with every claim and run field cleared.
-- - A workflow task with no node in a runnable status (ENQUEUED, READY, PENDING, RUNNING) is never requeued: it
--   can only fail its start again; the orphan sweep cancels it.
-- - The test is judged again under the row lock: a row whose status, owner or claim changed before its lock was
--   taken is judged on its new version.
-- - At most p_batch_size rows; rows that another transaction holds are skipped. The caller repeats the statement
--   until a batch is short.
-- - The status change sends horsies_task_status (update trigger). No queue wake, as Python.
-- Arguments: an evidence of the set with exactly its arguments, a threshold of 0 ms or more, p_batch_size
-- 1..10000; anything else is refused with 22023.
-- Locks: the candidate rows only (FOR UPDATE SKIP LOCKED); no advisory lock, never waits.
-- Returns one row per requeued row: its id, task name and queue (the caller's count and recovery events), at most
-- p_batch_size rows.
-- Python: REQUEUE_STALE_CLAIMED_SQL (core/brokers/postgres.py:852-903) and requeue_stale_claimed (:4308-4333), for
-- BEAT_AGE. OWNER_GONE is a core addition.
CREATE FUNCTION horsies_requeue_stale_claims(
    p_evidence text,
    p_stale_after_ms integer,
    p_owner_worker_id text,
    p_claimed_before timestamp with time zone,
    p_batch_size integer
) RETURNS TABLE(
    task_id uuid,
    task_name text,
    queue_name text
)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_stale uuid[];
BEGIN
    CASE p_evidence
        WHEN 'BEAT_AGE' THEN
            IF p_stale_after_ms IS NULL OR p_stale_after_ms < 0
               OR num_nulls(p_owner_worker_id, p_claimed_before) <> 2 THEN
                RAISE EXCEPTION 'horsies_requeue_stale_claims: BEAT_AGE takes a threshold of 0 ms or more and no owner'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'OWNER_GONE' THEN
            IF p_stale_after_ms IS NOT NULL OR num_nulls(p_owner_worker_id, p_claimed_before) <> 0 THEN
                RAISE EXCEPTION
                    'horsies_requeue_stale_claims: OWNER_GONE takes an owner and a claim bound and no threshold'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        ELSE
            RAISE EXCEPTION 'horsies_requeue_stale_claims: unknown evidence'
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    IF p_batch_size IS NULL OR p_batch_size < 1 OR p_batch_size > 10000 THEN
        RAISE EXCEPTION 'horsies_requeue_stale_claims: the batch size must be 1 to 10000'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- The probes are scalar subqueries, so each candidate reads the heartbeat leaf index and the node index once,
    -- whatever the size of those tables; the update finds its targets by id.
    CASE p_evidence
        WHEN 'BEAT_AGE' THEN
            v_stale := ARRAY(
                SELECT t.id
                FROM horsies_tasks t
                WHERE t.status = 'CLAIMED'
                  AND t.claimed_at < NOW() - make_interval(secs => p_stale_after_ms / 1000.0)
                  AND (
                      SELECT h.sent_at
                      FROM horsies_heartbeats h
                      WHERE h.task_id = t.id
                        AND h.role = 'claimer'
                        AND h.sent_at >= NOW() - make_interval(secs => p_stale_after_ms / 1000.0)
                      ORDER BY h.sent_at DESC
                      LIMIT 1
                  ) IS NULL
                  AND (
                      NOT t.is_workflow_task
                      OR (
                          SELECT wt.status
                          FROM horsies_workflow_tasks wt
                          WHERE wt.task_id = t.id
                      ) IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                  )
                LIMIT p_batch_size
                FOR UPDATE OF t SKIP LOCKED
            );
        WHEN 'OWNER_GONE' THEN
            v_stale := ARRAY(
                SELECT t.id
                FROM horsies_tasks t
                WHERE t.status = 'CLAIMED'
                  AND t.claimed_by_worker_id = p_owner_worker_id
                  AND t.claimed_at < p_claimed_before
                  AND (
                      NOT t.is_workflow_task
                      OR (
                          SELECT wt.status
                          FROM horsies_workflow_tasks wt
                          WHERE wt.task_id = t.id
                      ) IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                  )
                LIMIT p_batch_size
                FOR UPDATE OF t SKIP LOCKED
            );
    END CASE;
    RETURN QUERY
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
    WHERE t.id = ANY (v_stale)
    RETURNING t.id, t.task_name::text, t.queue_name::text;
END
$$;

-- horsies_release_claims: return claimed rows that no child started to the queue.
--
-- Guarantee:
-- - A row that is CLAIMED by p_worker_id at its given claim generation goes back to PENDING with every claim and
--   run field cleared (RELEASED). Any other row, a re-claimed row included, is not touched (NOT_HELD). No attempt
--   row is written.
-- - p_placement ISOLATED also writes execution ISOLATED and wakes the queue of each released row
--   (pg_notify('task_queue_' || queue_name, 'class'); one notification per queue per transaction). SAME_CLASS sends
--   no wake, as Python.
-- - Refused (22023): a NULL argument or element, arrays of different lengths, a repeated id, more than 1000 rows (a
--   worker holds far fewer; a caller with more splits them), an unknown placement.
-- Locks: the rows, read by id in task id order (FOR NO KEY UPDATE), the order of the batched success, so a release
-- cannot form a lock cycle with a batch that holds a re-claimed row. Nothing moves, so no advisory lock.
-- Returns one row per input row: its 1-based position, its id, RELEASED or NOT_HELD.
-- Python 0.7.0 (`v0.7.0` package): UNCLAIM_CLAIMED_TASK_SQL (core/worker/sql.py:237-255, called per row at
-- core/worker/dispatch.py:254-280), RETURN_CLAIMED_TASK_AS_ISOLATED_SQL (core/worker/sql.py:263-282) with its wake
-- (core/worker/claiming.py:459-484).
CREATE FUNCTION horsies_release_claims(
    p_task_ids uuid[],
    p_claimed_ats timestamp with time zone[],
    p_worker_id text,
    p_placement text
) RETURNS TABLE(ordinality bigint, task_id uuid, outcome text)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_released uuid[];
    v_queues text[];
    v_queue text;
BEGIN
    IF num_nulls(p_task_ids, p_claimed_ats, p_worker_id, p_placement) <> 0 THEN
        RAISE EXCEPTION 'horsies_release_claims: every argument is required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_placement NOT IN ('SAME_CLASS', 'ISOLATED') THEN
        RAISE EXCEPTION 'horsies_release_claims: unknown placement'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_claimed_ats) <> cardinality(p_task_ids) THEN
        RAISE EXCEPTION 'horsies_release_claims: array lengths differ'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_task_ids) > 1000 THEN
        RAISE EXCEPTION 'horsies_release_claims: at most 1000 rows per call'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- count(DISTINCT) skips NULL, so a NULL id fails this check too.
    IF cardinality(p_task_ids) <> (SELECT count(DISTINCT u.id) FROM unnest(p_task_ids) AS u(id))
       OR array_position(p_claimed_ats, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'horsies_release_claims: task ids must be distinct, and no element may be NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    WITH target AS MATERIALIZED (
        SELECT t.id,
               (
                   t.status = 'CLAIMED'
                   AND t.claimed_by_worker_id = p_worker_id
                   AND t.claimed_at = r.claimed_at
               ) IS TRUE AS held
        FROM unnest(p_task_ids, p_claimed_ats) AS r(id, claimed_at)
        JOIN horsies_tasks t ON t.id = r.id
        ORDER BY t.id
        FOR NO KEY UPDATE OF t
    ),
    released AS (
        UPDATE horsies_tasks t
        SET status = 'PENDING',
            execution = CASE WHEN p_placement = 'ISOLATED' THEN 'ISOLATED' ELSE t.execution END,
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
        FROM target g
        WHERE t.id = g.id
          AND g.held
        RETURNING t.id, t.queue_name::text AS queue_name
    )
    SELECT COALESCE(array_agg(x.id), ARRAY[]::uuid[]),
           COALESCE(array_agg(DISTINCT x.queue_name), ARRAY[]::text[])
    INTO v_released, v_queues
    FROM released x;

    IF p_placement = 'ISOLATED' THEN
        FOREACH v_queue IN ARRAY v_queues LOOP
            PERFORM pg_notify('task_queue_' || v_queue, 'class');
        END LOOP;
    END IF;

    RETURN QUERY
    SELECT u.position, u.id,
           CASE WHEN u.id = ANY (v_released) THEN 'RELEASED' ELSE 'NOT_HELD' END::text
    FROM unnest(p_task_ids) WITH ORDINALITY AS u(id, position)
    ORDER BY u.position;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Cancel from monitoring
------------------------------------------------------------------------------------------------------------------------

-- horsies_cancel_task: the administrative cancel of one plain task. Replaces horsies_cancel_locked_task and the
-- caller's lock statements.
--
-- Guarantee:
-- - A plain task that is PENDING or CLAIMED (RUNNING only when p_include_running is TRUE) moves to history as
--   CANCELLED (CANCEL_ADMIN, error code TASK_CANCELLED, 'Cancelled via monitoring API'), with no attempt and no
--   result. task_done and horsies_task_status fire (the move). A later finalize of the row is a conflict.
-- - A workflow task is refused and stays as it is, live or finished: SOURCE_STATE_CONFLICT with guard WORKFLOW_TASK
--   and its status.
-- - A live plain task in a status the caller did not permit: SOURCE_STATE_CONFLICT with its status (guard NULL).
-- - A task absent from the live table is classified from history: an earlier admin cancel is ALREADY_APPLIED
--   (status CANCELLED); another end is SOURCE_STATE_CONFLICT with guard FOREIGN_TERMINALIZATION and its status; no
--   history row is TASK_ABSENT.
-- Locks: the task advisory lock, then the whole row by id (FOR UPDATE), the row that the move takes.
-- Returns one row of the shared outcome columns; observed_status is the status the cancel saw.
-- Python 0.7.0 (`v0.7.0` package): cancel_task (monitoring/task_actions.py:326-431) and _diagnose_live_miss (:290-323),
-- horsies_cancel_locked_task (core/history/terminalization/move.py:2046-2096).
CREATE FUNCTION horsies_cancel_task(p_task_id uuid, p_include_running boolean) RETURNS TABLE(
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
    PERFORM horsies_move_task_to_history(
        v_task.task, 'CANCELLED', 'CANCEL_ADMIN', v_terminal_at, NULL, 'TASK_CANCELLED', 'Cancelled via monitoring API'
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'CANCEL_ADMIN'::text,
        v_task.status::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, NULL::text, NULL::jsonb;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Expiry and orphan cancel
------------------------------------------------------------------------------------------------------------------------

-- horsies_expire_owned_claim: the expiry of one CLAIMED task whose deadline passed before its start. Replaces the
-- interim function of the same name.
--
-- Guarantee:
-- - A row CLAIMED by p_worker_id (any claim generation: the start check of every generation of this worker finds the
--   same deadline) whose good_until has passed on the database clock moves to history as EXPIRED (EXPIRE_CLAIMED)
--   with p_result and p_error_code; task_done and horsies_task_status fire (the move).
-- - A row CLAIMED by p_worker_id whose deadline has not passed: SOURCE_STATE_CONFLICT with guard DEADLINE and the
--   evidence {good_until, evaluated_at}; nothing is written.
-- - Every other row is classified by horsies_terminalization_miss (fence OWNER): ALREADY_APPLIED, LOST_CLAIM,
--   SOURCE_STATE_CONFLICT, TASK_ABSENT; nothing is written.
-- - p_result may be NULL; the move refuses a workflow task without one (23502), because phase 2 needs it.
--
-- Arguments: the task, the worker and the error code are required (22023).
-- Locks: the task advisory lock, then the whole row by id (FOR UPDATE), the row that the move takes. Every writer of
-- good_until locks this row, so the deadline read under the lock holds until the move.
-- Returns one row of the shared outcome columns (ordinality NULL).
-- Python 0.7.0 (`v0.7.0` package): horsies_expire_owned_claim (core/schemas/terminalization.py:670-782), its call
-- (core/lifecycle/persistence.py:167-171), the expiry of the child miss path.
CREATE FUNCTION horsies_expire_owned_claim(
    p_task_id uuid,
    p_worker_id text,
    p_result text,
    p_error_code text
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
    v_capture record;
    v_task horsies_tasks;
    v_terminal_at timestamptz;
BEGIN
    IF num_nulls(p_task_id, p_worker_id, p_error_code) <> 0 THEN
        RAISE EXCEPTION 'horsies_expire_owned_claim: the task, the worker and the error code are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    -- One locked capture judges the deadline; the move takes the row as read here.
    SELECT t AS task, NOW() AS evaluated_at
    INTO v_capture
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF FOUND THEN
        v_task := v_capture.task;
    END IF;
    IF FOUND
       AND v_task.status = 'CLAIMED'
       AND v_task.claimed_by_worker_id = p_worker_id THEN
        IF v_task.good_until IS NOT NULL
           AND v_task.good_until <= v_capture.evaluated_at THEN
            v_terminal_at := NOW();
            PERFORM horsies_move_task_to_history(
                v_task, 'EXPIRED', 'EXPIRE_CLAIMED', v_terminal_at, p_result, p_error_code, NULL
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'EXPIRE_CLAIMED'::text,
                'CLAIMED'::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text, NULL::timestamptz, NULL::text,
            v_task.status::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, 'DEADLINE'::text,
            jsonb_build_object('good_until', v_task.good_until, 'evaluated_at', v_capture.evaluated_at);
        RETURN;
    END IF;

    RETURN QUERY
    SELECT m.task_id, m.ordinality, m.outcome, m.terminal_at, m.terminalization_kind, m.observed_status,
           m.observed_worker_id::text, m.observed_claimed_at, m.guard_kind, m.observed_guard
    FROM horsies_terminalization_miss(p_task_id, 'EXPIRE_CLAIMED', 'OWNER', p_worker_id, NULL::timestamptz) AS m;
END
$$;

-- horsies_expire_pending_tasks: one batch of the expiry of PENDING tasks whose deadline passed. Replaces the interim
-- function of the same name.
--
-- Guarantee:
-- - Takes at most p_batch_size PENDING rows with good_until on or before NOW(), earliest deadline first, skipping
--   rows that another session holds; they are taken by a later batch.
-- - Each taken row moves to history as EXPIRED (EXPIRE_PENDING) with p_result and p_error_code, at one terminal
--   time, through the batch move (horsies_move_tasks_to_history); nothing else is written.
-- - p_result may be NULL; the batch move refuses a workflow task without one (23502) before any row moves.
--
-- Arguments: p_batch_size positive, p_error_code required (22023).
-- Locks: the taken rows (FOR UPDATE SKIP LOCKED), with no advisory lock, so a batch never waits on a row lock and
-- joins no deadlock cycle.
-- Returns one row of the shared outcome columns per moved row (APPLIED, ordinality NULL), earliest deadline first;
-- no row when nothing is due.
-- Python 0.7.0 (`v0.7.0` package): horsies_expire_pending_tasks (core/schemas/terminalization.py:797-844), its call
-- (core/lifecycle/persistence.py:173-177), PostgresBroker.expire_pending_tasks.
CREATE FUNCTION horsies_expire_pending_tasks(
    p_batch_size integer,
    p_result text,
    p_error_code text
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
    v_ids uuid[];
    v_terminal_at timestamptz;
BEGIN
    IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION 'horsies_expire_pending_tasks: p_batch_size must be a positive integer'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_error_code IS NULL THEN
        RAISE EXCEPTION 'horsies_expire_pending_tasks: the error code is required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT array_agg(s.id) INTO v_ids
    FROM (
        SELECT id FROM horsies_tasks
        WHERE status = 'PENDING'
          AND good_until IS NOT NULL
          AND good_until <= NOW()
        ORDER BY good_until ASC
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
    ) s;
    IF v_ids IS NULL THEN
        RETURN;
    END IF;

    v_terminal_at := NOW();
    -- The outcome rows come from the locked live rows before the batch move deletes them. RETURN QUERY materializes
    -- at once; it does not end the function.
    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'EXPIRE_PENDING'::text,
        t.status::text, t.claimed_by_worker_id::text, t.claimed_at, NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids)
    ORDER BY t.good_until ASC;

    PERFORM horsies_move_tasks_to_history(
        v_ids, 'EXPIRED', 'EXPIRE_PENDING', v_terminal_at,
        p_result, p_error_code, NULL
    );
END
$$;

-- horsies_cancel_owned_orphan: the cancel of one CLAIMED workflow task that its claim holds and whose node no longer
-- links it. Replaces the interim function of the same name.
--
-- Guarantee:
-- - A workflow task CLAIMED by p_worker_id at the claim generation p_claimed_at with no runnable node link (no node
--   with this task_id in ENQUEUED, READY, PENDING or RUNNING) moves to history as CANCELLED (CANCEL_ORPHAN, error
--   code WORKFLOW_CHECK_FAILED); task_done and horsies_task_status fire (the move).
-- - The same task with a runnable node link: SOURCE_STATE_CONFLICT with guard WORKFLOW_LINK_STATE and the evidence
--   {node_status}; nothing is written. The fence is classified before the guard.
-- - Every other row is classified by horsies_terminalization_miss (fence OWNER_AND_GENERATION); nothing is written.
--
-- Arguments: every argument is required (22023).
-- Locks: the task advisory lock, then the whole row by id (FOR UPDATE), the row that the move takes; the node link is
-- read beside it, unlocked.
-- Returns one row of the shared outcome columns (ordinality NULL).
-- Python 0.7.0 (`v0.7.0` package): horsies_cancel_owned_orphan (core/schemas/terminalization.py:950), its call
-- (core/lifecycle/persistence.py:186-191), the orphan self-heal of the worker's finalize.
CREATE FUNCTION horsies_cancel_owned_orphan(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone
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
    v_capture record;
    v_task horsies_tasks;
    v_terminal_at timestamptz;
BEGIN
    IF num_nulls(p_task_id, p_worker_id, p_claimed_at) <> 0 THEN
        RAISE EXCEPTION 'horsies_cancel_owned_orphan: the task, the worker and the claim generation are required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id::text, 731));
    -- One capture: the whole row locked (the move takes it as read here), and the runnable node link read beside
    -- it. A node in a final status does not count as runnable and leaves the task orphaned.
    SELECT t AS task,
           (
               SELECT wt.status
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id
                 AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
               ORDER BY wt.id
               LIMIT 1
           ) AS node_status
    INTO v_capture
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF FOUND THEN
        v_task := v_capture.task;
    END IF;
    IF FOUND
       AND v_task.status = 'CLAIMED'
       AND v_task.claimed_by_worker_id = p_worker_id
       AND v_task.claimed_at = p_claimed_at
       AND v_task.is_workflow_task THEN
        IF v_capture.node_status IS NULL THEN
            v_terminal_at := NOW();
            PERFORM horsies_move_task_to_history(
                v_task, 'CANCELLED', 'CANCEL_ORPHAN', v_terminal_at,
                NULL, 'WORKFLOW_CHECK_FAILED',
                'Workflow task orphaned: no live workflow_task linkage'
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'CANCEL_ORPHAN'::text,
                v_task.status::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text, NULL::timestamptz, NULL::text,
            v_task.status::text, v_task.claimed_by_worker_id::text, v_task.claimed_at, 'WORKFLOW_LINK_STATE'::text,
            jsonb_build_object('node_status', v_capture.node_status);
        RETURN;
    END IF;

    RETURN QUERY
    SELECT m.task_id, m.ordinality, m.outcome, m.terminal_at, m.terminalization_kind, m.observed_status,
           m.observed_worker_id::text, m.observed_claimed_at, m.guard_kind, m.observed_guard
    FROM horsies_terminalization_miss(
        p_task_id, 'CANCEL_ORPHAN', 'OWNER_AND_GENERATION', p_worker_id, p_claimed_at
    ) AS m;
END
$$;

-- horsies_cancel_orphaned_tasks: one page of the orphan sweep: the cancel of PENDING and CLAIMED workflow tasks that
-- no node links. Replaces the interim function of the same name.
--
-- Guarantee:
-- - Scans at most p_batch_size PENDING or CLAIMED workflow tasks in (created_at, id) order from the cursor
--   orphan_workflow_tasks of horsies_recovery_scan_cursors, up to the upper bound that the cycle fixed at its first
--   page; a page shorter than the batch, or one that reaches the bound, completes the cycle and resets the cursor.
-- - Each scanned row with no runnable node link (no node with this task_id in ENQUEUED, READY, PENDING or RUNNING)
--   that no other session holds moves to history as CANCELLED (CANCEL_ORPHAN_SWEEP, error code
--   WORKFLOW_CHECK_FAILED) at one terminal time, through the batch move. A held row is taken by a later cycle.
-- - The cursor row records the page: its position, the cycle bound, completed_cycles, last_scan_rows,
--   last_candidate_rows and last_scan_at, in the same statement.
--
-- Arguments: p_batch_size positive (22023). An absent cursor row is refused with data_corrupted (XX001).
-- Locks: the cursor row (FOR UPDATE NOWAIT: a second sweep fails at once with 55P03), then the candidate rows (FOR
-- UPDATE SKIP LOCKED), with no advisory lock.
-- Returns one row of the shared outcome columns per moved row (APPLIED, ordinality NULL); no row when the page has
-- no orphan.
-- Python 0.7.0 (`v0.7.0` package): horsies_cancel_orphaned_tasks (core/schemas/terminalization.py), its call
-- (core/lifecycle/persistence.py:193-197), PostgresBroker.terminate_orphaned_workflow_tasks.
CREATE FUNCTION horsies_cancel_orphaned_tasks(p_batch_size integer) RETURNS TABLE(
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
    v_cursor_created_at timestamptz;
    v_cursor_id uuid;
    v_upper_created_at timestamptz;
    v_upper_id uuid;
    v_scan_created_at timestamptz[];
    v_scan_ids uuid[];
    v_ids uuid[];
    v_scan_count integer;
    v_cycle_complete boolean;
    v_terminal_at timestamptz;
BEGIN
    IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION 'horsies_cancel_orphaned_tasks: p_batch_size must be a positive integer'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT c.last_created_at, c.last_id, c.cycle_upper_created_at, c.cycle_upper_id
    INTO v_cursor_created_at, v_cursor_id, v_upper_created_at, v_upper_id
    FROM horsies_recovery_scan_cursors c
    WHERE c.scan_name = 'orphan_workflow_tasks'
    FOR UPDATE NOWAIT;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'orphan workflow-task scan cursor is absent'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF v_upper_id IS NULL THEN
        SELECT t.created_at, t.id
        INTO v_upper_created_at, v_upper_id
        FROM horsies_tasks t
        WHERE t.is_workflow_task = TRUE
          AND t.status IN ('CLAIMED', 'PENDING')
        ORDER BY t.created_at DESC, t.id DESC
        LIMIT 1;
    END IF;

    IF v_cursor_id IS NULL THEN
        SELECT array_agg(s.created_at ORDER BY s.created_at, s.id),
               array_agg(s.id ORDER BY s.created_at, s.id)
        INTO v_scan_created_at, v_scan_ids
        FROM (
            SELECT t.created_at, t.id
            FROM horsies_tasks t
            WHERE t.is_workflow_task = TRUE
              AND t.status IN ('CLAIMED', 'PENDING')
              AND v_upper_id IS NOT NULL
              AND (t.created_at, t.id) <= (v_upper_created_at, v_upper_id)
            ORDER BY t.created_at, t.id
            LIMIT p_batch_size
        ) s;
    ELSE
        SELECT array_agg(s.created_at ORDER BY s.created_at, s.id),
               array_agg(s.id ORDER BY s.created_at, s.id)
        INTO v_scan_created_at, v_scan_ids
        FROM (
            SELECT t.created_at, t.id
            FROM horsies_tasks t
            WHERE t.is_workflow_task = TRUE
              AND t.status IN ('CLAIMED', 'PENDING')
              AND v_upper_id IS NOT NULL
              AND (t.created_at, t.id) > (v_cursor_created_at, v_cursor_id)
              AND (t.created_at, t.id) <= (v_upper_created_at, v_upper_id)
            ORDER BY t.created_at, t.id
            LIMIT p_batch_size
        ) s;
    END IF;
    v_scan_count := COALESCE(cardinality(v_scan_ids), 0);
    v_cycle_complete := v_scan_count < p_batch_size
        OR (
            v_scan_count > 0
            AND (v_scan_created_at[v_scan_count], v_scan_ids[v_scan_count]) = (v_upper_created_at, v_upper_id)
        );

    SELECT array_agg(s.id ORDER BY s.id) INTO v_ids
    FROM (
        SELECT candidate.id
        FROM unnest(COALESCE(v_scan_ids, '{}'::uuid[])) AS scanned(id)
        CROSS JOIN LATERAL (
            SELECT t.id
            FROM horsies_tasks t
            LEFT JOIN LATERAL (
                SELECT TRUE AS found
                FROM horsies_workflow_tasks wt
                WHERE wt.task_id = t.id
                  AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                LIMIT 1
            ) runnable_link ON TRUE
            WHERE t.id = scanned.id
              AND t.is_workflow_task = TRUE
              AND t.status IN ('CLAIMED', 'PENDING')
              AND runnable_link.found IS NULL
            LIMIT 1
            FOR UPDATE OF t SKIP LOCKED
        ) candidate
    ) s;

    UPDATE horsies_recovery_scan_cursors c
    SET last_created_at = CASE WHEN v_cycle_complete THEN NULL ELSE v_scan_created_at[v_scan_count] END,
        last_id = CASE WHEN v_cycle_complete THEN NULL ELSE v_scan_ids[v_scan_count] END,
        cycle_upper_created_at = CASE WHEN v_cycle_complete THEN NULL ELSE v_upper_created_at END,
        cycle_upper_id = CASE WHEN v_cycle_complete THEN NULL ELSE v_upper_id END,
        completed_cycles = c.completed_cycles + CASE WHEN v_cycle_complete THEN 1 ELSE 0 END,
        last_scan_rows = v_scan_count,
        last_candidate_rows = COALESCE(cardinality(v_ids), 0),
        last_scan_at = statement_timestamp()
    WHERE c.scan_name = 'orphan_workflow_tasks';

    IF v_ids IS NULL THEN
        RETURN;
    END IF;

    v_terminal_at := NOW();
    -- The outcome rows come from the locked live rows before the batch move deletes them. RETURN QUERY materializes
    -- at once; it does not end the function.
    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text, v_terminal_at, 'CANCEL_ORPHAN_SWEEP'::text,
        t.status::text, t.claimed_by_worker_id::text, t.claimed_at, NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids);

    PERFORM horsies_move_tasks_to_history(
        v_ids, 'CANCELLED', 'CANCEL_ORPHAN_SWEEP', v_terminal_at,
        NULL, 'WORKFLOW_CHECK_FAILED',
        'Workflow task orphaned: no live workflow_task linkage'
    );
END
$$;
