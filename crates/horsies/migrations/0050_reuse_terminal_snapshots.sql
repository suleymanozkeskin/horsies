CREATE OR REPLACE FUNCTION public.horsies_move_task_to_history(p_task_id uuid, p_terminal_status text, p_terminalization_kind text, p_terminal_at timestamp with time zone, p_result text, p_error_code text, p_failed_reason text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_result_digest bytea;
    v_task horsies_tasks%ROWTYPE;
    v_attempt_snapshot bytea;
    v_result_payload bytea;
    v_prior_result_payload bytea;
    v_workflow_id uuid;
    v_workflow_node_row_id uuid;
    v_link_count integer;
    v_distinct_workflows integer;
    v_history_rows bigint;
    v_deleted_rows bigint;
    v_requires_deferred_phase2 boolean;
    v_rerun_disposition varchar(32);
    v_rerun_version smallint;
    v_rerun_codec varchar(64);
    v_rerun_content_type varchar(255);
    v_rerun_digest bytea;
    v_rerun_inline bytea;
    v_rerun_reference varchar(2048);
BEGIN
    PERFORM horsies_assert_archive_available();

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
            RAISE EXCEPTION
                'terminalization kind % has no move family yet',
                p_terminalization_kind
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );

    SELECT * INTO STRICT v_task
    FROM horsies_tasks
    WHERE id = p_task_id
    FOR UPDATE;
    IF v_task.status NOT IN ('PENDING', 'CLAIMED', 'RUNNING') THEN
        RAISE EXCEPTION 'live task has non-live status %', v_task.status;
    END IF;
    IF p_terminal_at IS NULL THEN
        RAISE EXCEPTION 'terminal timestamp is required';
    END IF;
    IF (SELECT prov.found
        FROM horsies_task_provenance_staged(p_task_id, FALSE) AS prov) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF v_task.is_workflow_task THEN
        IF p_terminalization_kind IN ('COMPLETE_FUSED', 'CANCEL_ADMIN') THEN
            RAISE EXCEPTION
                'operation cannot terminalize a workflow task'
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        IF v_requires_deferred_phase2 THEN
            SELECT n.id, n.workflow_id
            INTO STRICT v_workflow_node_row_id, v_workflow_id
            FROM horsies_workflow_tasks AS n
            WHERE n.task_id = p_task_id
            FOR UPDATE;
            IF p_result IS NULL THEN
                RAISE EXCEPTION
                    'deferred workflow terminalization requires a result payload'
                    USING ERRCODE = 'not_null_violation';
            END IF;
        ELSE
            SELECT count(*), count(DISTINCT n.workflow_id)
            INTO v_link_count, v_distinct_workflows
            FROM horsies_workflow_tasks AS n
            WHERE n.task_id = p_task_id;
            IF v_distinct_workflows > 1 THEN
                RAISE EXCEPTION
                    'task links to multiple workflows'
                    USING ERRCODE = 'data_corrupted';
            END IF;
            IF v_link_count > 0 THEN
                SELECT n.id, n.workflow_id
                INTO v_workflow_node_row_id, v_workflow_id
                FROM horsies_workflow_tasks AS n
                WHERE n.task_id = p_task_id
                ORDER BY n.id
                LIMIT 1
                FOR UPDATE;
            END IF;
        END IF;
    END IF;

    IF v_task.is_workflow_task OR p_terminal_status = 'COMPLETED' THEN
        v_rerun_disposition := 'NEVER_ELIGIBLE';
    ELSIF NOT v_task.retain_rerun_input THEN
        v_rerun_disposition := 'DECLINED_BY_POLICY';
    ELSE
        v_rerun_disposition := v_task.prepared_rerun_input_disposition;
    END IF;
    IF v_rerun_disposition IN ('INLINE', 'REFERENCE') THEN
        v_rerun_version := v_task.prepared_rerun_input_version;
        v_rerun_codec := v_task.prepared_rerun_input_codec;
        v_rerun_content_type := v_task.prepared_rerun_input_content_type;
        v_rerun_digest := v_task.prepared_rerun_input_digest;
        v_rerun_inline := v_task.prepared_rerun_input_inline;
        v_rerun_reference := v_task.prepared_rerun_input_reference;
    END IF;

    v_attempt_snapshot := horsies_encode_task_attempts(p_task_id);

    IF p_terminalization_kind = 'CANCEL_ADMIN' THEN
        v_result_payload := NULL;
        v_prior_result_payload := CASE
            WHEN v_task.result IS NULL THEN NULL
            ELSE convert_to(v_task.result, 'UTF8')
        END;
    ELSE
        v_result_payload := CASE
            WHEN p_result IS NULL THEN NULL
            ELSE convert_to(p_result, 'UTF8')
        END;
        v_prior_result_payload := NULL;
    END IF;

    v_result_digest := CASE WHEN v_result_payload IS NOT NULL
                 THEN sha256(v_result_payload)
             WHEN v_prior_result_payload IS NOT NULL
                 THEN sha256(v_prior_result_payload)
             ELSE NULL END;

    INSERT INTO horsies_task_history (
        task_id,
        task_name,
        queue_name,
        priority,
        command_fingerprint_version,
        command_fingerprint,
        status,
        terminalization_kind,
        terminal_at,
        retention_anchor_at,
        retention_class_key,
        sent_at,
        enqueued_at,
        claimed_at,
        started_at,
        created_at,
        good_until,
        result_envelope_version,
        result_codec,
        result_content_type,
        result_payload,
        prior_result_payload,
        result_digest,
        error_code,
        final_failed_reason,
        retry_count,
        max_retries,
        last_claimed_worker_id,
        last_worker_hostname,
        last_worker_pid,
        last_worker_process_name,
        input_digest,
        rerun_of_task_id,
        rerun_root_task_id,
        workflow_id,
        is_workflow_task,
        history_schema_version,
        attempt_archive_version,
        attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition,
        rerun_input_version,
        rerun_input_codec,
        rerun_input_content_type,
        rerun_input_digest,
        rerun_input_inline,
        rerun_input_reference
    ) VALUES (
        v_task.id,
        v_task.task_name,
        v_task.queue_name,
        v_task.priority,
        v_task.command_fingerprint_version,
        v_task.command_fingerprint,
        p_terminal_status,
        p_terminalization_kind,
        p_terminal_at,
        p_terminal_at,
        v_task.retention_class_key,
        v_task.sent_at,
        v_task.enqueued_at,
        v_task.claimed_at,
        v_task.started_at,
        v_task.created_at,
        v_task.good_until,
        1,
        'json-utf8',
        'application/json',
        v_result_payload,
        v_prior_result_payload,
        v_result_digest,
        p_error_code,
        p_failed_reason,
        v_task.retry_count,
        v_task.max_retries,
        v_task.claimed_by_worker_id,
        v_task.worker_hostname,
        v_task.worker_pid,
        v_task.worker_process_name,
        v_task.input_digest,
        v_task.rerun_of_task_id,
        v_task.rerun_root_task_id,
        v_workflow_id,
        v_task.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        v_attempt_snapshot,
        sha256(v_attempt_snapshot),
        v_rerun_disposition,
        v_rerun_version,
        v_rerun_codec,
        v_rerun_content_type,
        v_rerun_digest,
        v_rerun_inline,
        v_rerun_reference
    );
    GET DIAGNOSTICS v_history_rows = ROW_COUNT;
    IF v_history_rows <> 1 THEN
        RAISE EXCEPTION 'terminal history insert did not affect one row';
    END IF;

    IF v_requires_deferred_phase2 AND v_task.is_workflow_task THEN
        INSERT INTO horsies_workflow_phase2_pending (
            task_id, workflow_id, workflow_node_row_id,
            terminal_status, terminal_at, terminalization_kind,
            recovery_source, history_class, history_anchor,
            history_schema_version, result_digest,
            phase2_generation, created_at, attempt_count
        ) VALUES (
            v_task.id, v_workflow_id, v_workflow_node_row_id,
            p_terminal_status, p_terminal_at, p_terminalization_kind,
            'HISTORY', v_task.retention_class_key, p_terminal_at,
            1, v_result_digest,
            gen_random_uuid(), statement_timestamp(), 0
        );
    END IF;

    IF v_task.idempotency_key_digest IS NOT NULL THEN
        PERFORM horsies_key_reservation_terminalize(
            v_task.idempotency_key_digest, p_task_id, p_terminal_at
        );
    END IF;

    DELETE FROM horsies_task_attempts WHERE task_id = p_task_id;
    DELETE FROM horsies_tasks WHERE id = p_task_id;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;
    IF v_deleted_rows <> 1 THEN
        RAISE EXCEPTION 'live task delete did not affect one row';
    END IF;
    PERFORM pg_notify('task_done', p_task_id::text);
END
$function$;

CREATE OR REPLACE FUNCTION public.horsies_abandon_nodes_of_paused_workflows(p_workflow_ids uuid[])
 RETURNS SETOF horsies_terminalization_outcome
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_ids uuid[];
    v_terminal_at timestamptz;
    v_moved bigint;
    v_deleted bigint;
    v_result_payload bytea;
BEGIN
    PERFORM horsies_assert_archive_available();

    SELECT array_agg(s.id) INTO v_ids
    FROM (
        SELECT t2.id FROM horsies_tasks t2
        WHERE t2.status = 'CLAIMED'
          AND EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t2.id
                AND wt.workflow_id = ANY(p_workflow_ids)
                AND w.status = 'PAUSED'
                AND wt.status IN ('ENQUEUED', 'RUNNING')
          )
        FOR UPDATE OF t2 SKIP LOCKED
    ) s;
    IF v_ids IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM horsies_tasks t
        WHERE t.id = ANY(v_ids)
          AND t.is_workflow_task
          AND (SELECT count(DISTINCT wt.workflow_id)
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id) > 1
    ) THEN
        RAISE EXCEPTION 'task links to multiple workflows'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(v_ids) AS u(tid)
        WHERE (SELECT prov.found
               FROM horsies_task_provenance_staged(u.tid, FALSE) AS prov)
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN (NULL::text) IS NULL THEN NULL
        ELSE convert_to((NULL::text), 'UTF8')
    END;

    WITH attempt_data AS MATERIALIZED (
        SELECT id, horsies_encode_task_attempts(id) AS snapshot
        FROM unnest(v_ids) AS u(id)
    )
    INSERT INTO horsies_task_history (
        task_id,
        task_name,
        queue_name,
        priority,
        command_fingerprint_version,
        command_fingerprint,
        status,
        terminalization_kind,
        terminal_at,
        retention_anchor_at,
        retention_class_key,
        sent_at,
        enqueued_at,
        claimed_at,
        started_at,
        created_at,
        good_until,
        result_envelope_version,
        result_codec,
        result_content_type,
        result_payload,
        prior_result_payload,
        result_digest,
        error_code,
        final_failed_reason,
        retry_count,
        max_retries,
        last_claimed_worker_id,
        last_worker_hostname,
        last_worker_pid,
        last_worker_process_name,
        input_digest,
        rerun_of_task_id,
        rerun_root_task_id,
        workflow_id,
        is_workflow_task,
        history_schema_version,
        attempt_archive_version,
        attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition,
        rerun_input_version,
        rerun_input_codec,
        rerun_input_content_type,
        rerun_input_digest,
        rerun_input_inline,
        rerun_input_reference
    )
    SELECT
        t.id,
        t.task_name,
        t.queue_name,
        t.priority,
        t.command_fingerprint_version,
        t.command_fingerprint,
        'CANCELLED',
        'PAUSE_ABANDON_WORKFLOW',
        v_terminal_at,
        v_terminal_at,
        t.retention_class_key,
        t.sent_at,
        t.enqueued_at,
        t.claimed_at,
        t.started_at,
        t.created_at,
        t.good_until,
        1,
        'json-utf8',
        'application/json',
        v_result_payload,
        NULL,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        'TASK_CANCELLED',
        'Workflow paused before task start',
        t.retry_count,
        t.max_retries,
        t.claimed_by_worker_id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        t.input_digest,
        t.rerun_of_task_id,
        t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN n.workflow_id END,
        t.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        attempt_data.snapshot,
        sha256(attempt_data.snapshot),
        d.disposition,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_version END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_codec END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_content_type END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_digest END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_inline END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_reference END
    FROM horsies_tasks t
    JOIN attempt_data ON attempt_data.id = t.id
    LEFT JOIN LATERAL (
        SELECT wt.workflow_id
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = t.id
        ORDER BY wt.id
        LIMIT 1
    ) n ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR 'CANCELLED' = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            ELSE t.prepared_rerun_input_disposition
        END AS disposition
    ) d
    WHERE t.id = ANY(v_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality(v_ids);
    END IF;

    PERFORM horsies_key_reservation_terminalize_batch(
        (SELECT COALESCE(array_agg(t.idempotency_key_digest), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        (SELECT COALESCE(array_agg(t.id), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        v_terminal_at
    );

    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'PAUSE_ABANDON_WORKFLOW'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids);

    DELETE FROM horsies_task_attempts WHERE task_id = ANY(v_ids);
    DELETE FROM horsies_tasks WHERE id = ANY(v_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality(v_ids);
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest(v_ids) AS u(tid);
END
$function$;

CREATE OR REPLACE FUNCTION public.horsies_abandon_owned_nodes(p_ids uuid[], p_claimed_ats timestamp with time zone[], p_worker_id text)
 RETURNS SETOF horsies_terminalization_outcome
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_applied_ids uuid[];
    v_terminal_at timestamptz;
    v_moved bigint;
    v_deleted bigint;
    v_result_payload bytea;
BEGIN
    IF p_ids IS NULL OR p_claimed_ats IS NULL THEN
        RAISE EXCEPTION 'batch arrays must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_ids) <> cardinality(p_claimed_ats) THEN
        RAISE EXCEPTION
            'batch array lengths differ: ids=%, claimed_ats=%',
            cardinality(p_ids), cardinality(p_claimed_ats)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_ids, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'batch task ids must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_ids) <> (
        SELECT COUNT(DISTINCT item.id)
        FROM unnest(p_ids) AS item(id)
    ) THEN
        RAISE EXCEPTION 'batch task ids must be distinct'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM horsies_assert_archive_available();

    PERFORM 1 FROM (
        SELECT t.id FROM horsies_tasks t
        WHERE t.id = ANY(p_ids)
        ORDER BY t.id
        FOR UPDATE
    ) locked;

    SELECT COALESCE(array_agg(t.id), '{}') INTO v_applied_ids
    FROM unnest(p_ids, p_claimed_ats)
        AS input(task_id, expected_claimed_at)
    JOIN horsies_tasks t ON t.id = input.task_id
    WHERE t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND (
          input.expected_claimed_at IS NULL
          OR t.claimed_at = input.expected_claimed_at
      );

    IF cardinality(v_applied_ids) > 0 THEN

    IF EXISTS (
        SELECT 1 FROM horsies_tasks t
        WHERE t.id = ANY(v_applied_ids)
          AND t.is_workflow_task
          AND (SELECT count(DISTINCT wt.workflow_id)
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id) > 1
    ) THEN
        RAISE EXCEPTION 'task links to multiple workflows'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(v_applied_ids) AS u(tid)
        WHERE (SELECT prov.found
               FROM horsies_task_provenance_staged(u.tid, FALSE) AS prov)
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN (NULL::text) IS NULL THEN NULL
        ELSE convert_to((NULL::text), 'UTF8')
    END;

    WITH attempt_data AS MATERIALIZED (
        SELECT id, horsies_encode_task_attempts(id) AS snapshot
        FROM unnest(v_applied_ids) AS u(id)
    )
    INSERT INTO horsies_task_history (
        task_id,
        task_name,
        queue_name,
        priority,
        command_fingerprint_version,
        command_fingerprint,
        status,
        terminalization_kind,
        terminal_at,
        retention_anchor_at,
        retention_class_key,
        sent_at,
        enqueued_at,
        claimed_at,
        started_at,
        created_at,
        good_until,
        result_envelope_version,
        result_codec,
        result_content_type,
        result_payload,
        prior_result_payload,
        result_digest,
        error_code,
        final_failed_reason,
        retry_count,
        max_retries,
        last_claimed_worker_id,
        last_worker_hostname,
        last_worker_pid,
        last_worker_process_name,
        input_digest,
        rerun_of_task_id,
        rerun_root_task_id,
        workflow_id,
        is_workflow_task,
        history_schema_version,
        attempt_archive_version,
        attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition,
        rerun_input_version,
        rerun_input_codec,
        rerun_input_content_type,
        rerun_input_digest,
        rerun_input_inline,
        rerun_input_reference
    )
    SELECT
        t.id,
        t.task_name,
        t.queue_name,
        t.priority,
        t.command_fingerprint_version,
        t.command_fingerprint,
        'CANCELLED',
        'PAUSE_ABANDON_CLAIM_BATCH',
        v_terminal_at,
        v_terminal_at,
        t.retention_class_key,
        t.sent_at,
        t.enqueued_at,
        t.claimed_at,
        t.started_at,
        t.created_at,
        t.good_until,
        1,
        'json-utf8',
        'application/json',
        v_result_payload,
        NULL,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        'TASK_CANCELLED',
        'Workflow paused before task start',
        t.retry_count,
        t.max_retries,
        t.claimed_by_worker_id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        t.input_digest,
        t.rerun_of_task_id,
        t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN n.workflow_id END,
        t.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        attempt_data.snapshot,
        sha256(attempt_data.snapshot),
        d.disposition,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_version END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_codec END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_content_type END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_digest END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_inline END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_reference END
    FROM horsies_tasks t
    JOIN attempt_data ON attempt_data.id = t.id
    LEFT JOIN LATERAL (
        SELECT wt.workflow_id
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = t.id
        ORDER BY wt.id
        LIMIT 1
    ) n ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR 'CANCELLED' = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            ELSE t.prepared_rerun_input_disposition
        END AS disposition
    ) d
    WHERE t.id = ANY(v_applied_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(v_applied_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality(v_applied_ids);
    END IF;

    PERFORM horsies_key_reservation_terminalize_batch(
        (SELECT COALESCE(array_agg(t.idempotency_key_digest), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_applied_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        (SELECT COALESCE(array_agg(t.id), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_applied_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        v_terminal_at
    );

    RETURN QUERY SELECT
        t.id, input.ordinality, 'APPLIED'::text,
        v_terminal_at, 'PAUSE_ABANDON_CLAIM_BATCH'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    JOIN unnest(p_ids) WITH ORDINALITY AS input(task_id, ordinality)
        ON input.task_id = t.id
    WHERE t.id = ANY(v_applied_ids);

    DELETE FROM horsies_task_attempts WHERE task_id = ANY(v_applied_ids);
    DELETE FROM horsies_tasks WHERE id = ANY(v_applied_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(v_applied_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality(v_applied_ids);
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest(v_applied_ids) AS u(tid);
    END IF;

    RETURN QUERY
    SELECT input.task_id, input.ordinality,
           m.outcome, m.terminal_at, m.terminalization_kind,
           m.observed_status, m.observed_worker_id, m.observed_claimed_at,
           m.guard_kind, m.observed_guard
    FROM unnest(p_ids, p_claimed_ats) WITH ORDINALITY
        AS input(task_id, expected_claimed_at, ordinality)
    CROSS JOIN LATERAL horsies_terminalization_miss(
        input.task_id, ARRAY['PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW']::text[],
        p_worker_id, input.expected_claimed_at
    ) m
    WHERE NOT (input.task_id = ANY(v_applied_ids));
END
$function$;

CREATE OR REPLACE FUNCTION public.horsies_cancel_nodes_of_cancelled_workflow(p_workflow_ids uuid[])
 RETURNS SETOF horsies_terminalization_outcome
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_ids uuid[];
    v_terminal_at timestamptz;
    v_moved bigint;
    v_deleted bigint;
    v_result_payload bytea;
BEGIN
    PERFORM horsies_assert_archive_available();

    SELECT array_agg(s.id) INTO v_ids
    FROM (
        SELECT t2.id FROM horsies_tasks t2
        WHERE t2.status IN ('PENDING', 'CLAIMED', 'RUNNING')
          AND EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t2.id
                AND wt.workflow_id = ANY(p_workflow_ids)
                AND w.status IN ('CANCELLED', 'EXPIRED')
                AND wt.status = 'ENQUEUED'
          )
        FOR UPDATE OF t2 SKIP LOCKED
    ) s;
    IF v_ids IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM horsies_tasks t
        WHERE t.id = ANY(v_ids)
          AND t.is_workflow_task
          AND (SELECT count(DISTINCT wt.workflow_id)
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id) > 1
    ) THEN
        RAISE EXCEPTION 'task links to multiple workflows'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(v_ids) AS u(tid)
        WHERE (SELECT prov.found
               FROM horsies_task_provenance_staged(u.tid, FALSE) AS prov)
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN (NULL::text) IS NULL THEN NULL
        ELSE convert_to((NULL::text), 'UTF8')
    END;

    WITH attempt_data AS MATERIALIZED (
        SELECT id, horsies_encode_task_attempts(id) AS snapshot
        FROM unnest(v_ids) AS u(id)
    )
    INSERT INTO horsies_task_history (
        task_id,
        task_name,
        queue_name,
        priority,
        command_fingerprint_version,
        command_fingerprint,
        status,
        terminalization_kind,
        terminal_at,
        retention_anchor_at,
        retention_class_key,
        sent_at,
        enqueued_at,
        claimed_at,
        started_at,
        created_at,
        good_until,
        result_envelope_version,
        result_codec,
        result_content_type,
        result_payload,
        prior_result_payload,
        result_digest,
        error_code,
        final_failed_reason,
        retry_count,
        max_retries,
        last_claimed_worker_id,
        last_worker_hostname,
        last_worker_pid,
        last_worker_process_name,
        input_digest,
        rerun_of_task_id,
        rerun_root_task_id,
        workflow_id,
        is_workflow_task,
        history_schema_version,
        attempt_archive_version,
        attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition,
        rerun_input_version,
        rerun_input_codec,
        rerun_input_content_type,
        rerun_input_digest,
        rerun_input_inline,
        rerun_input_reference
    )
    SELECT
        t.id,
        t.task_name,
        t.queue_name,
        t.priority,
        t.command_fingerprint_version,
        t.command_fingerprint,
        'CANCELLED',
        'WORKFLOW_CANCEL_WORKFLOW',
        v_terminal_at,
        v_terminal_at,
        t.retention_class_key,
        t.sent_at,
        t.enqueued_at,
        t.claimed_at,
        t.started_at,
        t.created_at,
        t.good_until,
        1,
        'json-utf8',
        'application/json',
        v_result_payload,
        NULL,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        NULL,
        NULL,
        t.retry_count,
        t.max_retries,
        t.claimed_by_worker_id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        t.input_digest,
        t.rerun_of_task_id,
        t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN n.workflow_id END,
        t.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        attempt_data.snapshot,
        sha256(attempt_data.snapshot),
        d.disposition,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_version END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_codec END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_content_type END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_digest END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_inline END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_reference END
    FROM horsies_tasks t
    JOIN attempt_data ON attempt_data.id = t.id
    LEFT JOIN LATERAL (
        SELECT wt.workflow_id
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = t.id
        ORDER BY wt.id
        LIMIT 1
    ) n ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR 'CANCELLED' = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            ELSE t.prepared_rerun_input_disposition
        END AS disposition
    ) d
    WHERE t.id = ANY(v_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality(v_ids);
    END IF;

    PERFORM horsies_key_reservation_terminalize_batch(
        (SELECT COALESCE(array_agg(t.idempotency_key_digest), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        (SELECT COALESCE(array_agg(t.id), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        v_terminal_at
    );

    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'WORKFLOW_CANCEL_WORKFLOW'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids);

    DELETE FROM horsies_task_attempts WHERE task_id = ANY(v_ids);
    DELETE FROM horsies_tasks WHERE id = ANY(v_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality(v_ids);
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest(v_ids) AS u(tid);
END
$function$;

CREATE OR REPLACE FUNCTION public.horsies_cancel_orphaned_tasks(p_batch_size integer)
 RETURNS SETOF horsies_terminalization_outcome
 LANGUAGE plpgsql
AS $function$
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
    v_moved bigint;
    v_deleted bigint;
    v_result_payload bytea;
BEGIN
    IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION
            'p_batch_size must be a positive integer, got %', p_batch_size
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM horsies_assert_archive_available();

    SELECT c.last_created_at, c.last_id,
           c.cycle_upper_created_at, c.cycle_upper_id
    INTO v_cursor_created_at, v_cursor_id,
         v_upper_created_at, v_upper_id
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
            AND (v_scan_created_at[v_scan_count], v_scan_ids[v_scan_count])
                = (v_upper_created_at, v_upper_id)
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

    UPDATE horsies_recovery_scan_cursors
    SET last_created_at = CASE WHEN v_cycle_complete THEN NULL
                               ELSE v_scan_created_at[v_scan_count] END,
        last_id = CASE WHEN v_cycle_complete THEN NULL
                       ELSE v_scan_ids[v_scan_count] END,
        cycle_upper_created_at = CASE WHEN v_cycle_complete THEN NULL
                                      ELSE v_upper_created_at END,
        cycle_upper_id = CASE WHEN v_cycle_complete THEN NULL ELSE v_upper_id END,
        completed_cycles = completed_cycles + CASE WHEN v_cycle_complete THEN 1 ELSE 0 END,
        last_scan_rows = v_scan_count,
        last_candidate_rows = COALESCE(cardinality(v_ids), 0),
        last_scan_at = statement_timestamp()
    WHERE scan_name = 'orphan_workflow_tasks';

    IF v_ids IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM horsies_tasks t
        WHERE t.id = ANY(v_ids)
          AND t.is_workflow_task
          AND (SELECT count(DISTINCT wt.workflow_id)
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id) > 1
    ) THEN
        RAISE EXCEPTION 'task links to multiple workflows'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(v_ids) AS u(tid)
        WHERE (SELECT prov.found
               FROM horsies_task_provenance_staged(u.tid, FALSE) AS prov)
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN (NULL::text) IS NULL THEN NULL
        ELSE convert_to((NULL::text), 'UTF8')
    END;

    WITH attempt_data AS MATERIALIZED (
        SELECT id, horsies_encode_task_attempts(id) AS snapshot
        FROM unnest(v_ids) AS u(id)
    )
    INSERT INTO horsies_task_history (
        task_id,
        task_name,
        queue_name,
        priority,
        command_fingerprint_version,
        command_fingerprint,
        status,
        terminalization_kind,
        terminal_at,
        retention_anchor_at,
        retention_class_key,
        sent_at,
        enqueued_at,
        claimed_at,
        started_at,
        created_at,
        good_until,
        result_envelope_version,
        result_codec,
        result_content_type,
        result_payload,
        prior_result_payload,
        result_digest,
        error_code,
        final_failed_reason,
        retry_count,
        max_retries,
        last_claimed_worker_id,
        last_worker_hostname,
        last_worker_pid,
        last_worker_process_name,
        input_digest,
        rerun_of_task_id,
        rerun_root_task_id,
        workflow_id,
        is_workflow_task,
        history_schema_version,
        attempt_archive_version,
        attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition,
        rerun_input_version,
        rerun_input_codec,
        rerun_input_content_type,
        rerun_input_digest,
        rerun_input_inline,
        rerun_input_reference
    )
    SELECT
        t.id,
        t.task_name,
        t.queue_name,
        t.priority,
        t.command_fingerprint_version,
        t.command_fingerprint,
        'CANCELLED',
        'CANCEL_ORPHAN_SWEEP',
        v_terminal_at,
        v_terminal_at,
        t.retention_class_key,
        t.sent_at,
        t.enqueued_at,
        t.claimed_at,
        t.started_at,
        t.created_at,
        t.good_until,
        1,
        'json-utf8',
        'application/json',
        v_result_payload,
        NULL,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        'WORKFLOW_CHECK_FAILED',
        'Workflow task orphaned: no live workflow_task linkage',
        t.retry_count,
        t.max_retries,
        t.claimed_by_worker_id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        t.input_digest,
        t.rerun_of_task_id,
        t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN n.workflow_id END,
        t.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        attempt_data.snapshot,
        sha256(attempt_data.snapshot),
        d.disposition,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_version END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_codec END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_content_type END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_digest END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_inline END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_reference END
    FROM horsies_tasks t
    JOIN attempt_data ON attempt_data.id = t.id
    LEFT JOIN LATERAL (
        SELECT wt.workflow_id
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = t.id
        ORDER BY wt.id
        LIMIT 1
    ) n ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR 'CANCELLED' = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            ELSE t.prepared_rerun_input_disposition
        END AS disposition
    ) d
    WHERE t.id = ANY(v_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality(v_ids);
    END IF;

    PERFORM horsies_key_reservation_terminalize_batch(
        (SELECT COALESCE(array_agg(t.idempotency_key_digest), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        (SELECT COALESCE(array_agg(t.id), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        v_terminal_at
    );

    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'CANCEL_ORPHAN_SWEEP'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids);

    DELETE FROM horsies_task_attempts WHERE task_id = ANY(v_ids);
    DELETE FROM horsies_tasks WHERE id = ANY(v_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality(v_ids);
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest(v_ids) AS u(tid);
END;
$function$;

CREATE OR REPLACE FUNCTION public.horsies_cancel_owned_nodes(p_ids uuid[], p_claimed_ats timestamp with time zone[], p_worker_id text)
 RETURNS SETOF horsies_terminalization_outcome
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_applied_ids uuid[];
    v_terminal_at timestamptz;
    v_moved bigint;
    v_deleted bigint;
    v_result_payload bytea;
BEGIN
    IF p_ids IS NULL OR p_claimed_ats IS NULL THEN
        RAISE EXCEPTION 'batch arrays must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_ids) <> cardinality(p_claimed_ats) THEN
        RAISE EXCEPTION
            'batch array lengths differ: ids=%, claimed_ats=%',
            cardinality(p_ids), cardinality(p_claimed_ats)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_ids, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'batch task ids must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_ids) <> (
        SELECT COUNT(DISTINCT item.id)
        FROM unnest(p_ids) AS item(id)
    ) THEN
        RAISE EXCEPTION 'batch task ids must be distinct'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM horsies_assert_archive_available();

    PERFORM 1 FROM (
        SELECT t.id FROM horsies_tasks t
        WHERE t.id = ANY(p_ids)
        ORDER BY t.id
        FOR UPDATE
    ) locked;

    SELECT COALESCE(array_agg(t.id), '{}') INTO v_applied_ids
    FROM unnest(p_ids, p_claimed_ats)
        AS input(task_id, expected_claimed_at)
    JOIN horsies_tasks t ON t.id = input.task_id
    WHERE t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND (
          input.expected_claimed_at IS NULL
          OR t.claimed_at = input.expected_claimed_at
      );

    IF cardinality(v_applied_ids) > 0 THEN

    IF EXISTS (
        SELECT 1 FROM horsies_tasks t
        WHERE t.id = ANY(v_applied_ids)
          AND t.is_workflow_task
          AND (SELECT count(DISTINCT wt.workflow_id)
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id) > 1
    ) THEN
        RAISE EXCEPTION 'task links to multiple workflows'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(v_applied_ids) AS u(tid)
        WHERE (SELECT prov.found
               FROM horsies_task_provenance_staged(u.tid, FALSE) AS prov)
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN (NULL::text) IS NULL THEN NULL
        ELSE convert_to((NULL::text), 'UTF8')
    END;

    WITH attempt_data AS MATERIALIZED (
        SELECT id, horsies_encode_task_attempts(id) AS snapshot
        FROM unnest(v_applied_ids) AS u(id)
    )
    INSERT INTO horsies_task_history (
        task_id,
        task_name,
        queue_name,
        priority,
        command_fingerprint_version,
        command_fingerprint,
        status,
        terminalization_kind,
        terminal_at,
        retention_anchor_at,
        retention_class_key,
        sent_at,
        enqueued_at,
        claimed_at,
        started_at,
        created_at,
        good_until,
        result_envelope_version,
        result_codec,
        result_content_type,
        result_payload,
        prior_result_payload,
        result_digest,
        error_code,
        final_failed_reason,
        retry_count,
        max_retries,
        last_claimed_worker_id,
        last_worker_hostname,
        last_worker_pid,
        last_worker_process_name,
        input_digest,
        rerun_of_task_id,
        rerun_root_task_id,
        workflow_id,
        is_workflow_task,
        history_schema_version,
        attempt_archive_version,
        attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition,
        rerun_input_version,
        rerun_input_codec,
        rerun_input_content_type,
        rerun_input_digest,
        rerun_input_inline,
        rerun_input_reference
    )
    SELECT
        t.id,
        t.task_name,
        t.queue_name,
        t.priority,
        t.command_fingerprint_version,
        t.command_fingerprint,
        'CANCELLED',
        'WORKFLOW_CANCEL_CLAIM_BATCH',
        v_terminal_at,
        v_terminal_at,
        t.retention_class_key,
        t.sent_at,
        t.enqueued_at,
        t.claimed_at,
        t.started_at,
        t.created_at,
        t.good_until,
        1,
        'json-utf8',
        'application/json',
        v_result_payload,
        NULL,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        NULL,
        NULL,
        t.retry_count,
        t.max_retries,
        t.claimed_by_worker_id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        t.input_digest,
        t.rerun_of_task_id,
        t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN n.workflow_id END,
        t.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        attempt_data.snapshot,
        sha256(attempt_data.snapshot),
        d.disposition,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_version END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_codec END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_content_type END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_digest END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_inline END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_reference END
    FROM horsies_tasks t
    JOIN attempt_data ON attempt_data.id = t.id
    LEFT JOIN LATERAL (
        SELECT wt.workflow_id
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = t.id
        ORDER BY wt.id
        LIMIT 1
    ) n ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR 'CANCELLED' = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            ELSE t.prepared_rerun_input_disposition
        END AS disposition
    ) d
    WHERE t.id = ANY(v_applied_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(v_applied_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality(v_applied_ids);
    END IF;

    PERFORM horsies_key_reservation_terminalize_batch(
        (SELECT COALESCE(array_agg(t.idempotency_key_digest), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_applied_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        (SELECT COALESCE(array_agg(t.id), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_applied_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        v_terminal_at
    );

    RETURN QUERY SELECT
        t.id, input.ordinality, 'APPLIED'::text,
        v_terminal_at, 'WORKFLOW_CANCEL_CLAIM_BATCH'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    JOIN unnest(p_ids) WITH ORDINALITY AS input(task_id, ordinality)
        ON input.task_id = t.id
    WHERE t.id = ANY(v_applied_ids);

    DELETE FROM horsies_task_attempts WHERE task_id = ANY(v_applied_ids);
    DELETE FROM horsies_tasks WHERE id = ANY(v_applied_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(v_applied_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality(v_applied_ids);
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest(v_applied_ids) AS u(tid);
    END IF;

    RETURN QUERY
    SELECT input.task_id, input.ordinality,
           m.outcome, m.terminal_at, m.terminalization_kind,
           m.observed_status, m.observed_worker_id, m.observed_claimed_at,
           m.guard_kind, m.observed_guard
    FROM unnest(p_ids, p_claimed_ats) WITH ORDINALITY
        AS input(task_id, expected_claimed_at, ordinality)
    CROSS JOIN LATERAL horsies_terminalization_miss(
        input.task_id, ARRAY['WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW']::text[],
        p_worker_id, input.expected_claimed_at
    ) m
    WHERE NOT (input.task_id = ANY(v_applied_ids));
END
$function$;

CREATE OR REPLACE FUNCTION public.horsies_expire_pending_tasks(p_batch_size integer, p_result text, p_error_code text)
 RETURNS SETOF horsies_terminalization_outcome
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_ids uuid[];
    v_terminal_at timestamptz;
    v_moved bigint;
    v_deleted bigint;
    v_result_payload bytea;
BEGIN
    IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION
            'p_batch_size must be a positive integer, got %', p_batch_size
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM horsies_assert_archive_available();

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

    IF p_result IS NULL AND EXISTS (
        SELECT 1 FROM horsies_tasks t
        WHERE t.id = ANY(v_ids) AND t.is_workflow_task
    ) THEN
        RAISE EXCEPTION
            'deferred workflow terminalization requires a result payload'
            USING ERRCODE = 'not_null_violation';
    END IF;

    IF EXISTS (
        SELECT 1 FROM horsies_tasks t
        WHERE t.id = ANY(v_ids)
          AND t.is_workflow_task
          AND (SELECT count(*) FROM horsies_workflow_tasks n
               WHERE n.task_id = t.id) <> 1
    ) THEN
        RAISE EXCEPTION
            'workflow-backing task lacks exactly one node row'
            USING ERRCODE = 'foreign_key_violation';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(v_ids) AS u(tid)
        WHERE (SELECT prov.found
               FROM horsies_task_provenance_staged(u.tid, FALSE) AS prov)
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN (p_result) IS NULL THEN NULL
        ELSE convert_to((p_result), 'UTF8')
    END;

    WITH attempt_data AS MATERIALIZED (
        SELECT id, horsies_encode_task_attempts(id) AS snapshot
        FROM unnest(v_ids) AS u(id)
    )
    INSERT INTO horsies_task_history (
        task_id,
        task_name,
        queue_name,
        priority,
        command_fingerprint_version,
        command_fingerprint,
        status,
        terminalization_kind,
        terminal_at,
        retention_anchor_at,
        retention_class_key,
        sent_at,
        enqueued_at,
        claimed_at,
        started_at,
        created_at,
        good_until,
        result_envelope_version,
        result_codec,
        result_content_type,
        result_payload,
        prior_result_payload,
        result_digest,
        error_code,
        final_failed_reason,
        retry_count,
        max_retries,
        last_claimed_worker_id,
        last_worker_hostname,
        last_worker_pid,
        last_worker_process_name,
        input_digest,
        rerun_of_task_id,
        rerun_root_task_id,
        workflow_id,
        is_workflow_task,
        history_schema_version,
        attempt_archive_version,
        attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition,
        rerun_input_version,
        rerun_input_codec,
        rerun_input_content_type,
        rerun_input_digest,
        rerun_input_inline,
        rerun_input_reference
    )
    SELECT
        t.id,
        t.task_name,
        t.queue_name,
        t.priority,
        t.command_fingerprint_version,
        t.command_fingerprint,
        'EXPIRED',
        'EXPIRE_PENDING',
        v_terminal_at,
        v_terminal_at,
        t.retention_class_key,
        t.sent_at,
        t.enqueued_at,
        t.claimed_at,
        t.started_at,
        t.created_at,
        t.good_until,
        1,
        'json-utf8',
        'application/json',
        v_result_payload,
        NULL,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        p_error_code,
        NULL,
        t.retry_count,
        t.max_retries,
        t.claimed_by_worker_id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        t.input_digest,
        t.rerun_of_task_id,
        t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN n.workflow_id END,
        t.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        attempt_data.snapshot,
        sha256(attempt_data.snapshot),
        d.disposition,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_version END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_codec END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_content_type END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_digest END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_inline END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_reference END
    FROM horsies_tasks t
    JOIN attempt_data ON attempt_data.id = t.id
    LEFT JOIN horsies_workflow_tasks n ON n.task_id = t.id
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR 'EXPIRED' = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            ELSE t.prepared_rerun_input_disposition
        END AS disposition
    ) d
    WHERE t.id = ANY(v_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality(v_ids);
    END IF;

    INSERT INTO horsies_workflow_phase2_pending (
        task_id, workflow_id, workflow_node_row_id,
        terminal_status, terminal_at, terminalization_kind,
        recovery_source, history_class, history_anchor,
        history_schema_version, result_digest,
        phase2_generation, created_at, attempt_count
    )
    SELECT
        t.id, n.workflow_id, n.id,
        'EXPIRED', v_terminal_at, 'EXPIRE_PENDING',
        'HISTORY', t.retention_class_key, v_terminal_at,
        1, sha256(v_result_payload),
        gen_random_uuid(), statement_timestamp(), 0
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks n ON n.task_id = t.id
    WHERE t.id = ANY(v_ids) AND t.is_workflow_task;

    PERFORM horsies_key_reservation_terminalize_batch(
        (SELECT COALESCE(array_agg(t.idempotency_key_digest), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        (SELECT COALESCE(array_agg(t.id), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        v_terminal_at
    );

    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'EXPIRE_PENDING'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids);

    DELETE FROM horsies_task_attempts WHERE task_id = ANY(v_ids);
    DELETE FROM horsies_tasks WHERE id = ANY(v_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality(v_ids);
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest(v_ids) AS u(tid);
END
$function$;
