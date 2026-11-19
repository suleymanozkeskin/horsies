-- horsies baseline schema, part 6: the history and retention functions of the core design.
--
-- Each function is the one statement of a history operation in the core design: the move to history of one locked
-- row and of a batch of locked rows (with the leaf check that both run before their insert), the absence reason of a
-- task id, the rerun read, and the transactional parts of pruning. The terminal writes of 0003_interim_functions.sql
-- and 0004_lifecycle_functions.sql call the two moves;
-- horsies_monitoring_task_detail (0007_scheduler_monitoring_functions.sql) calls horsies_task_absence_reason. No
-- client calls the other functions yet. Each function names the Python code it replaces (paths at the 0.7.0 tag,
-- under horsies/core/).
--
-- Results are plain rows of built-in types. Closed sets of values are text columns; each function lists its values.
--
-- Part of migration 1000: the install concatenates the files of this folder in name order and computes one
-- checksum over the result.

------------------------------------------------------------------------------------------------------------------------
-- Move to history: the leaf check
------------------------------------------------------------------------------------------------------------------------

-- horsies_require_history_leaf(p_class_key text, p_terminal_at timestamptz): refuse a move whose row no history
-- leaf can take, before the row is written, with SQLSTATE 55H01 (history leaf missing).
--
-- The leaf that takes a row: under the class partition horsies_task_history_<class key>, the static leaf
-- horsies_task_history_forever_open for the forever class (0001_tables.sql), else the daily leaf
-- horsies_history_leaf_name(<class key>, <UTC day of p_terminal_at>) (0008_history_maintenance_functions.sql).
-- The check reads the two relation names only (to_regclass, the system catalog cache): no table, no lock.
-- Refusal: 55H01 when the class partition or the leaf does not exist. The message names the class and the missing
-- range, the UTC day of p_terminal_at as [lower, upper) in ISO 8601 UTC; DETAIL names the missing relation; HINT
-- names what creates it. The class is in SQLSTATE class 55 (object not in prerequisite state), so both clients
-- classify the refusal as retryable: the next coverage pass creates the leaf, and the same move then succeeds.
-- A leaf that exists but is detached or being detached still fails the insert with 23514: retention detaches only
-- leaves older than the class retention, so a p_terminal_at from the database clock never meets one.
-- Callers: horsies_move_task_to_history and horsies_move_tasks_to_history, after their other refusals and right
-- before their insert, where partition routing refused the row with 23514 before this check.
-- Returns: nothing.
CREATE FUNCTION horsies_require_history_leaf(p_class_key text, p_terminal_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
    v_day timestamp with time zone;
    v_partition text;
    v_leaf text;
    v_missing text;
BEGIN
    v_day := date_trunc('day', p_terminal_at, 'UTC');
    v_partition := 'horsies_task_history_' || p_class_key;
    IF p_class_key = 'forever' THEN
        v_leaf := 'horsies_task_history_forever_open';
    ELSE
        v_leaf := horsies_history_leaf_name(p_class_key, v_day);
    END IF;
    IF to_regclass(v_partition) IS NULL THEN
        v_missing := format('the class partition %s does not exist: the class is not registered', v_partition);
    ELSIF to_regclass(v_leaf) IS NULL THEN
        v_missing := format('the leaf %s does not exist', v_leaf);
    ELSE
        RETURN;
    END IF;
    RAISE EXCEPTION 'no task-history leaf of class % covers [%, %)',
        p_class_key,
        to_char(v_day AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        to_char((v_day + interval '1 day') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        USING ERRCODE = '55H01',
              DETAIL = v_missing,
              HINT = 'The schema install, the worker start and the reaper create the leaves of the current UTC day '
                     'and of the lookahead for the classes of their retention config; run one of them for this '
                     'class, then repeat the write.';
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Move to history: one locked row
------------------------------------------------------------------------------------------------------------------------

-- horsies_move_task_to_history(p_task horsies_tasks, ...): move one live task to history.
--
-- Guarantee: inside the caller's transaction, the live row p_task becomes exactly one history row, and every effect
-- of finishing happens together or not at all: the history row (with the rerun input envelope, the attempt
-- snapshot and the trace id of the live row), the clock exception when the id's UUIDv7 birth is more than 5 s later
-- than p_terminal_at, the phase-2 pending row (deferred kinds of a workflow task), the TERMINAL window of the task's
-- key reservation, the delete of the attempt rows and of the live row, and the task_done and horsies_task_status
-- notifications (sent at commit).
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
CREATE FUNCTION horsies_move_task_to_history(p_task horsies_tasks, p_terminal_status text, p_terminalization_kind text, p_terminal_at timestamp with time zone, p_result text, p_error_code text, p_failed_reason text) RETURNS void
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

    v_attempt_snapshot := horsies_encode_task_attempts(p_task.id);

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
        1, 1, v_attempt_snapshot, sha256(v_attempt_snapshot),
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

------------------------------------------------------------------------------------------------------------------------
-- Move to history: a batch of locked rows
------------------------------------------------------------------------------------------------------------------------

-- horsies_move_tasks_to_history(p_task_ids uuid[], ...): the move stage that every batch shares. Internal: the
-- batch terminal writes call it after they select and lock their rows.
--
-- Guarantee: inside the caller's transaction, every listed live task becomes exactly one history row, or none
-- does. One terminal time and one result digest serve the whole batch; each attempt snapshot is encoded once; the
-- rerun envelope is built only for rows that are eligible and retained (the single move's ladder); each history row
-- keeps the trace id of its live row; clock exceptions, phase-2 pending rows (EXPIRE_PENDING workflow tasks) and
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
CREATE FUNCTION horsies_move_tasks_to_history(p_task_ids uuid[], p_terminal_status text, p_terminalization_kind text, p_terminal_at timestamp with time zone, p_result text, p_error_code text, p_failed_reason text) RETURNS void
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
        SELECT u.id, horsies_encode_task_attempts(u.id) AS snapshot
        FROM unnest(p_task_ids) AS u(id)
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
        1, 1, attempt_data.snapshot, sha256(attempt_data.snapshot),
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
-- Absence reason of a task id
------------------------------------------------------------------------------------------------------------------------

-- horsies_task_absence_reason(p_task_id uuid): why a task id that no live row and no history row holds is absent.
--
-- Guarantee: one rule for every reader (the monitoring task detail and the rerun read call this function).
-- - NOT_CLASSIFIED: the id is not a UUIDv7, so it carries no birth time.
-- - PREDATES_RETAINED_HISTORY: the birth is earlier than the latest upper_anchor of any detached PERIOD-class leaf
--   plus 5 s. Retention removed history up to that anchor, and a moved task's birth is at most 5 s after its
--   terminal time (a later birth is a recorded clock exception), so retention can explain the absence.
-- - WITHIN_RETAINED_HISTORY: otherwise. No leaf that retention detached can have held the task: it never existed,
--   or its leaf was removed out of band (such a leaf is not detached in the catalog).
-- Heartbeat leaves and the forever leaf never count. Limit: a task whose id came from a producer clock more than
-- 5 s fast, and whose leaf retention removed, answers WITHIN_RETAINED_HISTORY.
-- Precondition: the caller found the id absent in the same snapshot. The function does not check it.
-- Reads: the PERIOD class rows, then per class the detached catalog row with the latest lower_anchor, through
-- horsies_task_history_leaf_catalog_interval_unique read backward (leaves of one class are daily and never
-- overlap, so the latest lower anchor has the latest upper anchor). Locks: none.
-- Returns: one of the three values above; NULL for a NULL id.
-- Replaces (`v0.7.0` package): core/history/reads/detail.py:353-380 (_classify_absence) and
-- core/history/partitions/catalog.py:510-548 (read_attached_birth_floor), which read min_birth_at, a column that no
-- writer sets on a fresh database.
CREATE FUNCTION horsies_task_absence_reason(p_task_id uuid) RETURNS text
    LANGUAGE sql STABLE STRICT
    AS $$
    SELECT CASE
        WHEN horsies_uuid7_birth(p_task_id) IS NULL THEN 'NOT_CLASSIFIED'
        WHEN horsies_uuid7_birth(p_task_id) < (
            SELECT max(latest.upper_anchor)
            FROM horsies_retention_classes AS r
            CROSS JOIN LATERAL (
                SELECT c.upper_anchor
                FROM horsies_task_history_leaf_catalog AS c
                WHERE c.class_key = r.class_key
                  AND c.detached_at IS NOT NULL
                ORDER BY c.lower_anchor DESC, c.upper_anchor DESC
                LIMIT 1
            ) AS latest
            WHERE r.class_kind = 'PERIOD'
        ) + interval '5 seconds' THEN 'PREDATES_RETAINED_HISTORY'
        ELSE 'WITHIN_RETAINED_HISTORY'
    END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Rerun read
------------------------------------------------------------------------------------------------------------------------

-- horsies_rerun_source_read(p_source_task_id uuid, p_retention_class_key text): the one read of a rerun.
--
-- Guarantee: one statement, one snapshot, one row with every fact the rerun decides on:
-- - class_registered: whether p_retention_class_key (the class of the new task) has a class row. The core refuses
--   an unregistered class before it looks at any other column.
-- - client_family: the recorded client family (PYTHON or RUST), or NULL when the install recorded none.
-- - location: LIVE, HISTORY, or ABSENT; another value is what the published detail reader returned, and the core
--   treats it as a corrupt source.
-- - absence_reason: horsies_task_absence_reason of the id for ABSENT; NULL otherwise.
-- - the source columns, for HISTORY only: identity, command, status, lineage, the attempt snapshot (so that a
--   snapshot that does not decode is a corrupt source, as in Python) and the rerun input envelope. The result
--   payload is not read.
-- Runs in the caller's transaction. The detail reader's leaf probe takes ACCESS SHARE on the source leaf until
-- that transaction ends, so retention cannot detach or drop the leaf before the rerun write.
-- Precondition: the history readers are published (the detail reader is called, not probed). Refusals: none of
-- its own; a missing reader raises 42883 undefined_function.
-- Locks: ACCESS SHARE on the relations it reads. Indexes: horsies_retention_classes_pkey,
-- horsies_client_family_pkey, horsies_tasks_pkey and the leaf task_id indexes (detail reader), and the absence
-- reads.
-- Returns: one row.
-- Replaces (`v0.7.0` package): core/history/rerun/operations.py:224-261 (the class check, the detail read) and
-- core/history/reads/detail.py:209-245 with :353-380 (the detail read and the absence read): 2 to 3 statements.
CREATE FUNCTION horsies_rerun_source_read(p_source_task_id uuid, p_retention_class_key text) RETURNS TABLE(class_registered boolean, client_family text, location text, absence_reason text, task_id uuid, task_name text, queue_name text, priority integer, max_retries integer, status text, terminalization_kind text, terminal_at timestamp with time zone, is_workflow_task boolean, rerun_root_task_id uuid, attempt_archive_version smallint, attempt_snapshot bytea, attempt_snapshot_digest bytea, rerun_input_disposition text, rerun_input_version smallint, rerun_input_digest bytea, rerun_input_inline bytea)
    LANGUAGE plpgsql STABLE
    AS $$
#variable_conflict use_column
DECLARE
    v_registered boolean;
    v_family text;
    v_detail record;
    v_location text;
    v_row horsies_task_history;
BEGIN
    IF p_source_task_id IS NULL OR p_retention_class_key IS NULL THEN
        RAISE EXCEPTION 'the rerun read needs a source task id and a retention class key'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_registered := EXISTS (
        SELECT 1 FROM horsies_retention_classes AS r WHERE r.class_key = p_retention_class_key
    );
    SELECT f.family INTO v_family FROM horsies_client_family AS f WHERE f.only_row;

    SELECT d.* INTO v_detail FROM horsies_task_detail_staged(p_source_task_id) AS d;
    IF NOT FOUND THEN
        RETURN QUERY SELECT
            v_registered, v_family, 'ABSENT'::text, horsies_task_absence_reason(p_source_task_id),
            NULL::uuid, NULL::text, NULL::text, NULL::integer, NULL::integer, NULL::text, NULL::text,
            NULL::timestamptz, NULL::boolean, NULL::uuid, NULL::smallint, NULL::bytea, NULL::bytea, NULL::text,
            NULL::smallint, NULL::bytea, NULL::bytea;
        RETURN;
    END IF;
    v_location := v_detail.location;
    IF v_location IS DISTINCT FROM 'HISTORY' THEN
        RETURN QUERY SELECT
            v_registered, v_family, v_location, NULL::text,
            NULL::uuid, NULL::text, NULL::text, NULL::integer, NULL::integer, NULL::text, NULL::text,
            NULL::timestamptz, NULL::boolean, NULL::uuid, NULL::smallint, NULL::bytea, NULL::bytea, NULL::text,
            NULL::smallint, NULL::bytea, NULL::bytea;
        RETURN;
    END IF;
    v_row := v_detail.task_row;
    RETURN QUERY SELECT
        v_registered, v_family, v_location, NULL::text,
        v_row.task_id, v_row.task_name::text, v_row.queue_name::text, v_row.priority, v_row.max_retries,
        v_row.status::text, v_row.terminalization_kind::text, v_row.terminal_at, v_row.is_workflow_task,
        v_row.rerun_root_task_id, v_row.attempt_archive_version, v_row.attempt_snapshot,
        v_row.attempt_snapshot_digest, v_row.rerun_input_disposition::text, v_row.rerun_input_version,
        v_row.rerun_input_digest, v_row.rerun_input_inline;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Pruning: candidates, inspection, detach, drop
------------------------------------------------------------------------------------------------------------------------

-- horsies_prune_candidates(p_limit integer): the expired leaves of one pruning pass.
--
-- Guarantee: one read returns the oldest expired PERIOD and HEARTBEAT leaves that the catalog has not recorded as
-- dropped, at most p_limit, ordered by lower_anchor then leaf_name. A PERIOD leaf expires when upper_anchor plus
-- retention_days has passed, a HEARTBEAT leaf when upper_anchor plus heartbeat_horizon_hours has passed (database
-- clock, days of 24 hours). Forever leaves are never candidates. The interrupted detaches that must be finalized
-- first come from the same rows (attachment DETACH_PENDING).
-- The caller passes its per-pass bound plus 1; more rows than the bound means the pass is truncated.
-- Columns: attachment = ATTACHED | DETACH_PENDING | DETACHED (the relation has no parent) | MISSING (no relation);
-- detach_recorded = the catalog's detached_at is set.
-- Refusals: 22023 invalid_parameter_value for a NULL or non-positive p_limit.
-- Indexes: horsies_retention_classes_pkey; per class, horsies_task_history_leaf_catalog_active
-- (class_key, upper_anchor) WHERE dropped_at IS NULL; pg_inherits by inhrelid. Locks: none.
-- Returns: zero or more rows.
-- Replaces (`v0.7.0` package): core/history/maintenance/pruning.py:95-127 (two candidate queries) and the inspection
-- per candidate that finds the interrupted detaches (:283-333).
CREATE FUNCTION horsies_prune_candidates(p_limit integer) RETURNS TABLE(leaf_name text, class_key text, class_kind text, lower_anchor timestamp with time zone, upper_anchor timestamp with time zone, expires_at timestamp with time zone, attachment text, detach_recorded boolean)
    LANGUAGE plpgsql STABLE
    SET "TimeZone" TO 'UTC'
    AS $$
#variable_conflict use_column
BEGIN
    IF p_limit IS NULL OR p_limit <= 0 THEN
        RAISE EXCEPTION 'p_limit must be a positive integer, got %', p_limit
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    RETURN QUERY
    SELECT
        c.leaf_name,
        c.class_key::text,
        r.class_kind::text,
        c.lower_anchor,
        c.upper_anchor,
        c.upper_anchor + r.retention,
        CASE
            WHEN to_regclass(quote_ident(c.leaf_name)) IS NULL THEN 'MISSING'
            WHEN i.inhrelid IS NULL THEN 'DETACHED'
            WHEN i.inhdetachpending THEN 'DETACH_PENDING'
            ELSE 'ATTACHED'
        END,
        c.detached_at IS NOT NULL
    FROM (
        SELECT
            rc.class_key,
            rc.class_kind,
            CASE rc.class_kind
                WHEN 'PERIOD' THEN make_interval(days => rc.retention_days)
                WHEN 'HEARTBEAT' THEN make_interval(hours => rc.heartbeat_horizon_hours)
            END AS retention
        FROM horsies_retention_classes AS rc
        WHERE rc.class_kind IN ('PERIOD', 'HEARTBEAT')
    ) AS r
    CROSS JOIN LATERAL (
        SELECT cat.leaf_name, cat.class_key, cat.lower_anchor, cat.upper_anchor, cat.detached_at
        FROM horsies_task_history_leaf_catalog AS cat
        WHERE cat.class_key = r.class_key
          AND cat.dropped_at IS NULL
          AND cat.lower_bound_kind = 'AT'
          AND cat.upper_anchor <= statement_timestamp() - r.retention
        ORDER BY cat.upper_anchor
        LIMIT p_limit
    ) AS c
    LEFT JOIN pg_inherits AS i ON i.inhrelid = to_regclass(quote_ident(c.leaf_name))
    ORDER BY c.lower_anchor, c.leaf_name
    LIMIT p_limit;
END
$$;

-- horsies_inspect_history_leaf(p_leaf_name text): the state of one PERIOD or HEARTBEAT leaf. Internal: the detach
-- and the drop call it.
--
-- Guarantee: one classification from the catalog row, the class row, pg_class, pg_inherits, the leaf's task index
-- and the phase-2 pending locators, read in one snapshot. It changes nothing. States, in the order they are
-- decided:
-- - NOT_CATALOGED: no catalog row and no relation.
-- - CATALOG_CONFLICT with conflict_kind: RELATION_WITHOUT_CATALOG (a relation and no catalog row), OPEN_LOWER_BOUND
--   (a finite leaf with an open lower bound), DROPPED_RELATION_EXISTS (the catalog recorded a drop and the relation
--   exists), FOREIGN_PARENT (attached to another parent), DETACH_RECORDED_WHILE_ATTACHED (the catalog recorded a
--   detach and the leaf is attached), PHYSICAL_NONCONFORMANT (attached or pending, and its partition bound differs
--   from the catalog, or its id index, <leaf>_task_idx of a PERIOD leaf or <leaf>_probe_idx of a HEARTBEAT leaf, is
--   missing or not conformant by horsies_history_leaf_indexes (0008_history_maintenance_functions.sql), the one rule
--   of leaf index conformance).
-- - ALREADY_DROPPED: the catalog recorded the drop and the relation is gone.
-- - MISSING: cataloged, not dropped, and the relation is gone (removed out of band).
-- - PENDING_BLOCKED with blocker_count and attachment: phase-2 pending locators point into the leaf's interval.
-- - DETACHED: the relation exists and has no parent.
-- - DETACH_INTERRUPTED: a concurrent detach is pending finalization.
-- - NOT_EXPIRED / DETACHABLE: attached; expires_at (upper_anchor plus the class retention) is later than, or not
--   later than, the statement time.
-- Refusals: a forever leaf, an unknown class kind and a missing class parent raise (defects: the candidate read
-- never names a forever leaf, and registration creates the class row and its parent together).
-- Indexes: horsies_task_history_leaf_catalog_pkey, horsies_retention_classes_pkey,
-- horsies_workflow_phase2_pending_locator_idx (blocker count). Locks: none.
-- Returns: one row.
-- Replaces (`v0.7.0` package): core/history/partitions/manager.py:147-269 (inspect_leaf), :1235-1260
-- (_pending_blocker_count), core/history/partitions/catalog.py:259-297 (read_retention_class), :344-388
-- (read_leaf_catalog_row) and :658-840 (read_leaf_physical_state): five to six statements per inspection.
CREATE FUNCTION horsies_inspect_history_leaf(p_leaf_name text) RETURNS TABLE(state text, class_key text, class_kind text, parent_name text, lower_anchor timestamp with time zone, upper_anchor timestamp with time zone, expires_at timestamp with time zone, blocker_count bigint, attachment text, conflict_kind text)
    LANGUAGE plpgsql STABLE
    SET "TimeZone" TO 'UTC'
    AS $$
#variable_conflict use_column
DECLARE
    v_catalog horsies_task_history_leaf_catalog;
    v_class horsies_retention_classes;
    v_parent_name text;
    v_expires_at timestamptz;
    v_leaf_oid oid;
    v_parent_oid oid;
    v_attached_parent oid;
    v_detach_pending boolean;
    v_attachment text;
    v_index_kind text;
    v_index_name text;
    v_conformant boolean;
    v_blockers bigint;
BEGIN
    SELECT * INTO v_catalog FROM horsies_task_history_leaf_catalog AS c WHERE c.leaf_name = p_leaf_name;
    v_leaf_oid := to_regclass(quote_ident(p_leaf_name));
    IF v_catalog.leaf_name IS NULL THEN
        IF v_leaf_oid IS NULL THEN
            RETURN QUERY SELECT 'NOT_CATALOGED'::text, NULL::text, NULL::text, NULL::text, NULL::timestamptz,
                NULL::timestamptz, NULL::timestamptz, NULL::bigint, NULL::text, NULL::text;
        ELSE
            RETURN QUERY SELECT 'CATALOG_CONFLICT'::text, NULL::text, NULL::text, NULL::text, NULL::timestamptz,
                NULL::timestamptz, NULL::timestamptz, NULL::bigint, NULL::text, 'RELATION_WITHOUT_CATALOG'::text;
        END IF;
        RETURN;
    END IF;

    -- The catalog's foreign key to the classes makes the class row exist.
    SELECT * INTO STRICT v_class FROM horsies_retention_classes AS r WHERE r.class_key = v_catalog.class_key;
    CASE v_class.class_kind
        WHEN 'PERIOD' THEN
            v_parent_name := 'horsies_task_history_' || v_class.class_key;
            v_index_kind := 'HISTORY';
            v_index_name := p_leaf_name || '_task_idx';
            v_expires_at := v_catalog.upper_anchor + make_interval(days => v_class.retention_days);
        WHEN 'HEARTBEAT' THEN
            v_parent_name := 'horsies_heartbeats';
            v_index_kind := 'HEARTBEAT';
            v_index_name := p_leaf_name || '_probe_idx';
            v_expires_at := v_catalog.upper_anchor + make_interval(hours => v_class.heartbeat_horizon_hours);
        WHEN 'FOREVER' THEN
            RAISE EXCEPTION 'leaf % belongs to the forever class, which retention never detaches', p_leaf_name
                USING ERRCODE = 'invalid_parameter_value';
        ELSE
            RAISE EXCEPTION 'class % of leaf % has unknown kind %', v_class.class_key, p_leaf_name,
                v_class.class_kind;
    END CASE;

    IF v_catalog.lower_bound_kind <> 'AT' THEN
        RETURN QUERY SELECT 'CATALOG_CONFLICT'::text, v_class.class_key::text, v_class.class_kind::text,
            v_parent_name, v_catalog.lower_anchor, v_catalog.upper_anchor, v_expires_at, NULL::bigint,
            NULL::text, 'OPEN_LOWER_BOUND'::text;
        RETURN;
    END IF;

    IF v_leaf_oid IS NULL THEN
        RETURN QUERY SELECT
            CASE WHEN v_catalog.dropped_at IS NOT NULL THEN 'ALREADY_DROPPED' ELSE 'MISSING' END,
            v_class.class_key::text, v_class.class_kind::text, v_parent_name, v_catalog.lower_anchor,
            v_catalog.upper_anchor, v_expires_at, NULL::bigint, NULL::text, NULL::text;
        RETURN;
    END IF;
    IF v_catalog.dropped_at IS NOT NULL THEN
        RETURN QUERY SELECT 'CATALOG_CONFLICT'::text, v_class.class_key::text, v_class.class_kind::text,
            v_parent_name, v_catalog.lower_anchor, v_catalog.upper_anchor, v_expires_at, NULL::bigint,
            NULL::text, 'DROPPED_RELATION_EXISTS'::text;
        RETURN;
    END IF;

    v_parent_oid := to_regclass(quote_ident(v_parent_name));
    IF v_parent_oid IS NULL THEN
        RAISE EXCEPTION 'parent % of leaf % does not exist', v_parent_name, p_leaf_name;
    END IF;
    SELECT i.inhparent, i.inhdetachpending INTO v_attached_parent, v_detach_pending
    FROM pg_inherits AS i
    WHERE i.inhrelid = v_leaf_oid;
    IF NOT FOUND THEN
        v_attachment := 'DETACHED';
    ELSIF v_attached_parent <> v_parent_oid THEN
        RETURN QUERY SELECT 'CATALOG_CONFLICT'::text, v_class.class_key::text, v_class.class_kind::text,
            v_parent_name, v_catalog.lower_anchor, v_catalog.upper_anchor, v_expires_at, NULL::bigint,
            NULL::text, 'FOREIGN_PARENT'::text;
        RETURN;
    ELSIF v_detach_pending THEN
        v_attachment := 'DETACH_PENDING';
    ELSE
        v_attachment := 'ATTACHED';
    END IF;

    IF v_attachment = 'ATTACHED' AND v_catalog.detached_at IS NOT NULL THEN
        RETURN QUERY SELECT 'CATALOG_CONFLICT'::text, v_class.class_key::text, v_class.class_kind::text,
            v_parent_name, v_catalog.lower_anchor, v_catalog.upper_anchor, v_expires_at, NULL::bigint,
            NULL::text, 'DETACH_RECORDED_WHILE_ATTACHED'::text;
        RETURN;
    END IF;

    IF v_attachment <> 'DETACHED' THEN
        -- The bound renders under TimeZone UTC (set on this function), as pg_get_expr renders it.
        SELECT
            pg_get_expr(leaf.relpartbound, leaf.oid) = format(
                'FOR VALUES FROM (%L) TO (%L)', v_catalog.lower_anchor, v_catalog.upper_anchor
            )
            AND EXISTS (
                SELECT 1
                FROM horsies_history_leaf_indexes(v_index_kind, p_leaf_name) AS i
                WHERE i.index_name = v_index_name AND i.conformant
            )
        INTO v_conformant
        FROM pg_class AS leaf
        WHERE leaf.oid = v_leaf_oid;
        IF v_conformant IS NOT TRUE THEN
            RETURN QUERY SELECT 'CATALOG_CONFLICT'::text, v_class.class_key::text, v_class.class_kind::text,
                v_parent_name, v_catalog.lower_anchor, v_catalog.upper_anchor, v_expires_at, NULL::bigint,
                NULL::text, 'PHYSICAL_NONCONFORMANT'::text;
            RETURN;
        END IF;
    END IF;

    IF v_class.class_kind = 'PERIOD' THEN
        SELECT count(*) INTO v_blockers
        FROM horsies_workflow_phase2_pending AS p
        WHERE p.recovery_source = 'HISTORY'
          AND p.history_class = v_class.class_key
          AND p.history_anchor >= v_catalog.lower_anchor
          AND p.history_anchor < v_catalog.upper_anchor;
    ELSE
        v_blockers := 0;
    END IF;
    IF v_blockers > 0 THEN
        RETURN QUERY SELECT 'PENDING_BLOCKED'::text, v_class.class_key::text, v_class.class_kind::text,
            v_parent_name, v_catalog.lower_anchor, v_catalog.upper_anchor, v_expires_at, v_blockers,
            v_attachment, NULL::text;
        RETURN;
    END IF;

    RETURN QUERY SELECT
        CASE v_attachment
            WHEN 'DETACHED' THEN 'DETACHED'
            WHEN 'DETACH_PENDING' THEN 'DETACH_INTERRUPTED'
            ELSE CASE WHEN v_expires_at > statement_timestamp() THEN 'NOT_EXPIRED' ELSE 'DETACHABLE' END
        END,
        v_class.class_key::text, v_class.class_kind::text, v_parent_name, v_catalog.lower_anchor,
        v_catalog.upper_anchor, v_expires_at, NULL::bigint, NULL::text, NULL::text;
END
$$;

-- horsies_begin_leaf_detach(p_leaf_name text): the session leaf lock and the inspection before a detach or a
-- finalize, in one call.
--
-- Guarantee: the leaf is inspected (horsies_inspect_history_leaf); only DETACHABLE and DETACH_INTERRUPTED take the
-- session-scoped advisory lock horsies_task_history_leaf_lock_key(class_key, lower_anchor) with a try-lock, and
-- the leaf is inspected again under the lock. After the call the session holds the lock exactly when the outcome
-- is DETACHABLE or DETACH_INTERRUPTED; every other outcome leaves no lock behind.
-- A DETACHED leaf (it has no parent and nothing pins it, for example after a detach out of band) gets its detach
-- recorded in the caller's transaction, without the leaf lock: horsies_record_leaf_detached and, when the published
-- manifest names the leaf, the reader publication (horsies_publish_history_readers). The leaf is then inspected
-- again, and that state is the outcome. So the catalog records every detach that a pruning pass finds, the readers
-- no longer name the leaf, and horsies_drop_detached_leaf is not refused with REFERENCED_BY_MANIFEST.
-- Outcomes: BUSY (another session holds the leaf lock: maintenance, or a move into the leaf that has not ended), or a
-- state of horsies_inspect_history_leaf.
-- The caller then runs, alone on the same connection in autocommit:
-- - DETACHABLE: SET statement_timeout and SET lock_timeout = '2s' (one simple query); ALTER TABLE <parent_name>
--   DETACH PARTITION <leaf> CONCURRENTLY (it cannot run in a function, a transaction block or a multi-statement
--   query; lock_not_available means BUSY);
-- - DETACH_INTERRUPTED: the same with ALTER TABLE <parent_name> DETACH PARTITION <leaf> FINALIZE;
-- - then horsies_record_leaf_detached in a transaction (with the reader publication when it says so), then RESET
--   of both timeouts and pg_advisory_unlock of the key in one simple query. A cancel, or a failure whose lock state
--   is not clear, closes the connection instead: a session lock does not end with a transaction.
-- Refusals: as horsies_inspect_history_leaf; for a DETACHED leaf that the manifest names, 25000
-- invalid_transaction_state from the reader publication under another isolation than READ COMMITTED (nothing is
-- recorded). A raise after the lock was taken releases it before it propagates.
-- Lock order: the catalog row, then the publication lock (the record of a DETACHED leaf); the session leaf lock,
-- then (the caller's statement) the relation locks of the detach.
-- Returns: one row: the outcome and the inspection columns.
-- Replaces (`v0.7.0` package): core/history/partitions/manager.py:763-876 (detach_expired_leaf up to the DETACH) and
-- :969-1044 (finalize_interrupted_detach up to the FINALIZE): two inspections and the lock, about 12 statements.
CREATE FUNCTION horsies_begin_leaf_detach(p_leaf_name text) RETURNS TABLE(outcome text, class_key text, class_kind text, parent_name text, lower_anchor timestamp with time zone, upper_anchor timestamp with time zone, expires_at timestamp with time zone, blocker_count bigint, attachment text, conflict_kind text)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_inspection record;
    v_recorded record;
    v_key bigint;
BEGIN
    SELECT * INTO STRICT v_inspection FROM horsies_inspect_history_leaf(p_leaf_name);
    IF v_inspection.state = 'DETACHED' THEN
        -- A detach out of band has no record: the manifest would keep naming the leaf, and its drop be refused.
        SELECT * INTO STRICT v_recorded FROM horsies_record_leaf_detached(p_leaf_name);
        IF v_recorded.manifest_names_leaf THEN
            PERFORM 1 FROM horsies_publish_history_readers();
        END IF;
        SELECT * INTO STRICT v_inspection FROM horsies_inspect_history_leaf(p_leaf_name);
    END IF;
    IF v_inspection.state NOT IN ('DETACHABLE', 'DETACH_INTERRUPTED') THEN
        RETURN QUERY SELECT v_inspection.state, v_inspection.class_key, v_inspection.class_kind,
            v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
            v_inspection.expires_at, v_inspection.blocker_count, v_inspection.attachment,
            v_inspection.conflict_kind;
        RETURN;
    END IF;

    v_key := horsies_task_history_leaf_lock_key(v_inspection.class_key, v_inspection.lower_anchor);
    IF NOT pg_try_advisory_lock(v_key) THEN
        RETURN QUERY SELECT 'BUSY'::text, v_inspection.class_key, v_inspection.class_kind,
            v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
            v_inspection.expires_at, NULL::bigint, NULL::text, NULL::text;
        RETURN;
    END IF;

    BEGIN
        SELECT * INTO STRICT v_inspection FROM horsies_inspect_history_leaf(p_leaf_name);
    EXCEPTION WHEN OTHERS THEN
        -- Cleanup only: the error propagates unchanged after the lock is released.
        IF NOT pg_advisory_unlock(v_key) THEN
            RAISE EXCEPTION 'leaf lock of % was not held after it was taken', p_leaf_name;
        END IF;
        RAISE;
    END;
    IF v_inspection.state NOT IN ('DETACHABLE', 'DETACH_INTERRUPTED') THEN
        IF NOT pg_advisory_unlock(v_key) THEN
            RAISE EXCEPTION 'leaf lock of % was not held after it was taken', p_leaf_name;
        END IF;
    END IF;
    RETURN QUERY SELECT v_inspection.state, v_inspection.class_key, v_inspection.class_kind,
        v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
        v_inspection.expires_at, v_inspection.blocker_count, v_inspection.attachment,
        v_inspection.conflict_kind;
END
$$;

-- horsies_record_leaf_detached(p_leaf_name text): record in the catalog that a leaf left its parent.
--
-- Guarantee: detached_at is set once, from the database clock, and only for a cataloged leaf whose relation exists
-- and has no parent. Outcomes:
-- - RECORDED: this call set detached_at.
-- - ALREADY_RECORDED: detached_at (or dropped_at) was already set; nothing changes.
-- - STILL_ATTACHED: the relation still has a parent, including a pending detach; nothing changes.
-- - MISSING: the relation is gone and the catalog recorded no drop; nothing changes (a leaf removed out of band is
--   not a retention detach, so the absence reason keeps calling its tasks WITHIN_RETAINED_HISTORY).
-- - NOT_CATALOGED: no catalog row; nothing changes.
-- manifest_names_leaf: the published reader manifest still names the leaf, so the reader publication must run in
-- the same transaction. Heartbeat leaves are never in the manifest.
-- Lock order: the catalog row (FOR UPDATE). Indexes: horsies_task_history_leaf_catalog_pkey,
-- horsies_task_lookup_manifest_pkey.
-- Returns: one row.
-- Replaces (`v0.7.0` package): core/history/partitions/manager.py:1222-1233 (_record_detached) and the freshness check
-- before the publication (:1208-1220, _republish_if_stale).
CREATE FUNCTION horsies_record_leaf_detached(p_leaf_name text) RETURNS TABLE(outcome text, manifest_names_leaf boolean)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_catalog horsies_task_history_leaf_catalog;
    v_leaf_oid oid;
    v_outcome text;
    v_updated bigint;
BEGIN
    SELECT * INTO v_catalog
    FROM horsies_task_history_leaf_catalog AS c
    WHERE c.leaf_name = p_leaf_name
    FOR UPDATE;
    v_leaf_oid := to_regclass(quote_ident(p_leaf_name));
    IF v_catalog.leaf_name IS NULL THEN
        v_outcome := 'NOT_CATALOGED';
    ELSIF v_catalog.detached_at IS NOT NULL OR v_catalog.dropped_at IS NOT NULL THEN
        v_outcome := 'ALREADY_RECORDED';
    ELSIF v_leaf_oid IS NULL THEN
        v_outcome := 'MISSING';
    ELSIF EXISTS (SELECT 1 FROM pg_inherits AS i WHERE i.inhrelid = v_leaf_oid) THEN
        v_outcome := 'STILL_ATTACHED';
    ELSE
        UPDATE horsies_task_history_leaf_catalog AS c
        SET detached_at = statement_timestamp()
        WHERE c.leaf_name = p_leaf_name AND c.detached_at IS NULL;
        GET DIAGNOSTICS v_updated = ROW_COUNT;
        IF v_updated <> 1 THEN
            RAISE EXCEPTION 'detach record of leaf % updated % rows', p_leaf_name, v_updated;
        END IF;
        v_outcome := 'RECORDED';
    END IF;
    RETURN QUERY SELECT v_outcome, EXISTS (
        SELECT 1 FROM horsies_task_lookup_manifest AS m WHERE m.leaf_name = p_leaf_name
    );
END
$$;

-- horsies_drop_detached_leaf(p_leaf_name text): drop one detached leaf inside the caller's transaction.
--
-- Guarantee: a leaf is dropped only when it is DETACHED (horsies_inspect_history_leaf), the transaction holds its
-- leaf lock, the published reader manifest does not name it, and its ACCESS EXCLUSIVE lock is free. The drop, the
-- delete of the clock exceptions of its class and interval (PERIOD leaves), and the catalog update (dropped_at, and
-- detached_at when not yet set) commit together. The catalog keeps the row.
-- Outcomes:
-- - DROPPED: this call dropped the leaf.
-- - BUSY: another transaction holds the leaf lock (maintenance, or a move into the leaf that has not ended), or a
--   reader holds a lock on the relation.
-- - REFERENCED_BY_MANIFEST: the published readers still probe the leaf; publish first.
-- - a state of horsies_inspect_history_leaf other than DETACHED, unchanged (for example ALREADY_DROPPED,
--   PENDING_BLOCKED, DETACHABLE).
-- Refusals: as horsies_inspect_history_leaf.
-- Lock order: the transaction leaf lock (try); the relation (ACCESS EXCLUSIVE NOWAIT); the clock exception rows;
-- the catalog row. Indexes: horsies_task_history_leaf_catalog_pkey, horsies_task_lookup_manifest_pkey,
-- horsies_task_history_clock_exceptions_leaf_idx.
-- Returns: one row: the outcome and the inspection columns.
-- Replaces (`v0.7.0` package): core/history/partitions/manager.py:1046-1104 (drop_detached_leaf) and :1181-1206
-- (_delete_leaf_clock_exceptions): about 10 statements.
CREATE FUNCTION horsies_drop_detached_leaf(p_leaf_name text) RETURNS TABLE(outcome text, class_key text, class_kind text, parent_name text, lower_anchor timestamp with time zone, upper_anchor timestamp with time zone, expires_at timestamp with time zone, blocker_count bigint, attachment text, conflict_kind text)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_inspection record;
    v_updated bigint;
BEGIN
    SELECT * INTO STRICT v_inspection FROM horsies_inspect_history_leaf(p_leaf_name);
    IF v_inspection.state <> 'DETACHED' THEN
        RETURN QUERY SELECT v_inspection.state, v_inspection.class_key, v_inspection.class_kind,
            v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
            v_inspection.expires_at, v_inspection.blocker_count, v_inspection.attachment,
            v_inspection.conflict_kind;
        RETURN;
    END IF;
    IF NOT pg_try_advisory_xact_lock(
        horsies_task_history_leaf_lock_key(v_inspection.class_key, v_inspection.lower_anchor)
    ) THEN
        RETURN QUERY SELECT 'BUSY'::text, v_inspection.class_key, v_inspection.class_kind,
            v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
            v_inspection.expires_at, NULL::bigint, NULL::text, NULL::text;
        RETURN;
    END IF;
    SELECT * INTO STRICT v_inspection FROM horsies_inspect_history_leaf(p_leaf_name);
    IF v_inspection.state <> 'DETACHED' THEN
        RETURN QUERY SELECT v_inspection.state, v_inspection.class_key, v_inspection.class_kind,
            v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
            v_inspection.expires_at, v_inspection.blocker_count, v_inspection.attachment,
            v_inspection.conflict_kind;
        RETURN;
    END IF;
    IF EXISTS (SELECT 1 FROM horsies_task_lookup_manifest AS m WHERE m.leaf_name = p_leaf_name) THEN
        RETURN QUERY SELECT 'REFERENCED_BY_MANIFEST'::text, v_inspection.class_key, v_inspection.class_kind,
            v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
            v_inspection.expires_at, NULL::bigint, NULL::text, NULL::text;
        RETURN;
    END IF;

    BEGIN
        EXECUTE format('LOCK TABLE %I IN ACCESS EXCLUSIVE MODE NOWAIT', p_leaf_name);
    EXCEPTION WHEN lock_not_available THEN
        RETURN QUERY SELECT 'BUSY'::text, v_inspection.class_key, v_inspection.class_kind,
            v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
            v_inspection.expires_at, NULL::bigint, NULL::text, NULL::text;
        RETURN;
    END;
    EXECUTE format('DROP TABLE %I', p_leaf_name);
    IF v_inspection.class_kind = 'PERIOD' THEN
        DELETE FROM horsies_task_history_clock_exceptions AS e
        WHERE e.retention_class_key = v_inspection.class_key
          AND e.terminal_at >= v_inspection.lower_anchor
          AND e.terminal_at < v_inspection.upper_anchor;
    END IF;
    UPDATE horsies_task_history_leaf_catalog AS c
    SET detached_at = COALESCE(c.detached_at, statement_timestamp()),
        dropped_at = statement_timestamp()
    WHERE c.leaf_name = p_leaf_name;
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN
        RAISE EXCEPTION 'drop record of leaf % updated % rows', p_leaf_name, v_updated;
    END IF;
    RETURN QUERY SELECT 'DROPPED'::text, v_inspection.class_key, v_inspection.class_kind,
        v_inspection.parent_name, v_inspection.lower_anchor, v_inspection.upper_anchor,
        v_inspection.expires_at, NULL::bigint, NULL::text, NULL::text;
END
$$;
