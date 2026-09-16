DO $migration$
BEGIN
    IF to_regprocedure(
        'horsies_move_task_to_history(uuid,text,text,timestamp with time zone,text,text,text)'
    ) IS NULL THEN
        RETURN;
    END IF;

    EXECUTE $terminal_program$CREATE OR REPLACE FUNCTION horsies_expire_pending_tasks(p_batch_size integer, p_result text, p_error_code text)
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
    WHERE t.id = ANY(v_ids)
    ORDER BY t.good_until ASC;

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
$function$$terminal_program$;
END
$migration$;
