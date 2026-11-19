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
        CAST(t.id AS uuid),
        t.task_name,
        t.queue_name,
        t.priority,
        t.command_fingerprint_version,
        t.command_fingerprint,
        t.status,
        COALESCE(t.terminalization_kind, 'LEGACY_TERMINAL'),
        t.terminal_at,
        t.terminal_at,
        COALESCE(t.retention_class_key, 'forever'),
        t.sent_at,
        t.enqueued_at,
        t.claimed_at,
        t.started_at,
        t.created_at,
        t.good_until,
        1,
        'json-utf8',
        'application/json',
        CASE WHEN t.terminalization_kind = 'CANCEL_ADMIN' THEN NULL ELSE (CASE WHEN t.result IS NULL THEN NULL ELSE convert_to(t.result, 'UTF8') END) END,
        CASE WHEN t.terminalization_kind = 'CANCEL_ADMIN' THEN (CASE WHEN t.result IS NULL THEN NULL ELSE convert_to(t.result, 'UTF8') END) END,
        CASE WHEN t.terminalization_kind = 'CANCEL_ADMIN' THEN CASE WHEN t.result IS NULL THEN NULL ELSE sha256(convert_to(t.result, 'UTF8')) END WHEN t.result IS NULL THEN NULL ELSE sha256(convert_to(t.result, 'UTF8')) END,
        t.error_code,
        CASE WHEN t.status IN ('FAILED', 'EXPIRED') THEN last_attempt.failed_reason END,
        t.retry_count,
        t.max_retries,
        t.claimed_by_worker_id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        t.input_digest,
        t.rerun_of_task_id,
        t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN CAST(node.workflow_id AS uuid) END,
        t.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        horsies_encode_task_attempts(CAST(t.id AS uuid)),
        sha256(horsies_encode_task_attempts(CAST(t.id AS uuid))),
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
    LEFT JOIN LATERAL (
        SELECT wt.workflow_id
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = t.id
        ORDER BY wt.id
        LIMIT 1
    ) node ON TRUE
    LEFT JOIN LATERAL (
        SELECT a.failed_reason
        FROM horsies_task_attempts a
        WHERE a.task_id = CAST(t.id AS uuid)
        ORDER BY a.attempt DESC
        LIMIT 1
    ) last_attempt ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR t.status = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            ELSE t.prepared_rerun_input_disposition
        END AS disposition
    ) d
    WHERE t.id::text = ANY(CAST(:task_ids AS text[]))
