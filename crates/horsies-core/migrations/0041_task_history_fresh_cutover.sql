-- Schema v35: fresh databases are born at the validated cutover posture.
DO $migration$
DECLARE
    v_fresh boolean;
    v_relation text;
    v_column text;
    v_constraint text;
    v_terminal_rows bigint;
    v_history_rows bigint;
    v_ledger_rows bigint;
    v_uncataloged bigint;
    v_violations text[] := ARRAY[]::text[];
BEGIN
    LOCK TABLE horsies_tasks, horsies_task_attempts, horsies_workflows,
        horsies_workflow_tasks IN SHARE ROW EXCLUSIVE MODE;
    SELECT NOT EXISTS (SELECT 1 FROM horsies_tasks)
       AND NOT EXISTS (SELECT 1 FROM horsies_task_attempts)
       AND NOT EXISTS (SELECT 1 FROM horsies_workflows)
       AND NOT EXISTS (SELECT 1 FROM horsies_workflow_tasks)
    INTO v_fresh;

    IF NOT v_fresh THEN
        RETURN;
    END IF;

    ALTER TABLE horsies_task_attempts
        DROP CONSTRAINT IF EXISTS horsies_task_attempts_task_id_fkey;
    ALTER TABLE horsies_workflows
        DROP CONSTRAINT IF EXISTS horsies_workflows_parent_workflow_id_fkey;
    ALTER TABLE horsies_workflow_tasks
        DROP CONSTRAINT IF EXISTS horsies_workflow_tasks_workflow_id_fkey;
    ALTER TABLE horsies_workflow_tasks
        DROP CONSTRAINT IF EXISTS horsies_workflow_tasks_sub_workflow_id_fkey;

    ALTER TABLE horsies_tasks
        ALTER COLUMN id TYPE uuid USING id::uuid;
    ALTER TABLE horsies_task_attempts
        ALTER COLUMN task_id TYPE uuid USING task_id::uuid;
    ALTER TABLE horsies_workflows
        ALTER COLUMN id TYPE uuid USING id::uuid,
        ALTER COLUMN parent_workflow_id TYPE uuid USING parent_workflow_id::uuid,
        ALTER COLUMN root_workflow_id TYPE uuid USING root_workflow_id::uuid;
    ALTER TABLE horsies_workflow_tasks
        ALTER COLUMN id TYPE uuid USING id::uuid,
        ALTER COLUMN workflow_id TYPE uuid USING workflow_id::uuid,
        ALTER COLUMN task_id TYPE uuid USING task_id::uuid,
        ALTER COLUMN sub_workflow_id TYPE uuid USING sub_workflow_id::uuid;

    ALTER TABLE horsies_task_attempts
        ADD CONSTRAINT horsies_task_attempts_task_id_fkey
        FOREIGN KEY (task_id) REFERENCES horsies_tasks(id) ON DELETE CASCADE;
    ALTER TABLE horsies_workflows
        ADD CONSTRAINT horsies_workflows_parent_workflow_id_fkey
        FOREIGN KEY (parent_workflow_id) REFERENCES horsies_workflows(id)
        ON DELETE CASCADE;
    ALTER TABLE horsies_workflow_tasks
        ADD CONSTRAINT horsies_workflow_tasks_workflow_id_fkey
        FOREIGN KEY (workflow_id) REFERENCES horsies_workflows(id)
        ON DELETE CASCADE;
    ALTER TABLE horsies_workflow_tasks
        ADD CONSTRAINT horsies_workflow_tasks_sub_workflow_id_fkey
        FOREIGN KEY (sub_workflow_id) REFERENCES horsies_workflows(id)
        ON DELETE SET NULL;

    FOR v_constraint IN
        SELECT con.conname
        FROM pg_constraint AS con
        WHERE con.conrelid = 'horsies_tasks'::regclass
          AND con.contype = 'c'
          AND (
              SELECT att.attnum
              FROM pg_attribute AS att
              WHERE att.attrelid = con.conrelid
                AND att.attname = 'status'
          ) = ANY(con.conkey)
          AND con.conname <> 'horsies_tasks_live_status_only'
    LOOP
        EXECUTE format(
            'ALTER TABLE horsies_tasks DROP CONSTRAINT %I',
            v_constraint
        );
    END LOOP;

        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_live_status_only
    CHECK (status IN ('PENDING', 'CLAIMED', 'RUNNING'))$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP TYPE IF EXISTS horsies_terminalization_outcome CASCADE$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP FUNCTION IF EXISTS horsies_move_task_to_history(uuid, text, text, timestamptz, text, text, text)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP FUNCTION IF EXISTS horsies_encode_task_attempts(uuid)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP TYPE IF EXISTS horsies_phase2_disposition CASCADE$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP TYPE IF EXISTS horsies_phase2_quarantine_verdict CASCADE$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP FUNCTION IF EXISTS horsies_phase2_consume CASCADE$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP FUNCTION IF EXISTS horsies_phase2_quarantine_one CASCADE$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP TABLE IF EXISTS horsies_archive_replacement_batches$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP TABLE IF EXISTS horsies_archive_replacement_relations$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP TABLE IF EXISTS horsies_archive_replacement_jobs$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP FUNCTION IF EXISTS horsies_archive_replacement_note_mutation() CASCADE$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$DROP TABLE IF EXISTS horsies_cutover_relocation_ledger$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE TYPE horsies_terminalization_outcome AS (
    task_id uuid,
    ordinality bigint,
    outcome text,
    terminal_at timestamptz,
    terminalization_kind text,
    observed_status text,
    observed_worker_id varchar,
    observed_claimed_at timestamptz,
    guard_kind text,
    observed_guard jsonb
)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_terminalization_miss(
    p_task_id uuid,
    p_equivalent_kinds text[],
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_row horsies_tasks%ROWTYPE;
    v_provenance record;
BEGIN
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

    IF p_worker_id IS NOT NULL AND (
        v_row.claimed_by_worker_id IS DISTINCT FROM CAST(p_worker_id AS VARCHAR)
        OR (p_claimed_at IS NOT NULL
            AND v_row.claimed_at IS DISTINCT FROM p_claimed_at)
    ) THEN
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
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_encode_task_attempts(p_task_id uuid)
RETURNS bytea
LANGUAGE sql
STABLE
STRICT
AS $function$
    SELECT convert_to(
        '[' || COALESCE(
            string_agg(
                '[' || to_jsonb(a.attempt)::text || ',' ||
                to_jsonb(a.outcome)::text || ',' ||
                to_jsonb(a.will_retry)::text || ',' ||
                to_jsonb(
                    floor(
                        extract(epoch FROM a.started_at) * 1000000
                    )::bigint
                )::text || ',' ||
                to_jsonb(
                    floor(
                        extract(epoch FROM a.finished_at) * 1000000
                    )::bigint
                )::text || ',' ||
                COALESCE(to_jsonb(a.error_code)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.error_message)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.failed_reason)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_id)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_hostname)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_pid)::text, 'null') || ',' ||
                COALESCE(
                    to_jsonb(a.worker_process_name)::text,
                    'null'
                ) || ']',
                ',' ORDER BY a.attempt
            ),
            ''
        ) || ']',
        'UTF8'
    )
    FROM horsies_task_attempts AS a
    WHERE a.task_id = p_task_id
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_move_task_to_history(
    p_task_id uuid,
    p_terminal_status text,
    p_terminalization_kind text,
    p_terminal_at timestamptz,
    p_result text,
    p_error_code text,
    p_failed_reason text
) RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
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
        -- Linkage lookup shape derives from the SAME classification that
        -- set deferral: the CP9 STRICT strengthening's premise (a live
        -- workflow's node row cannot be gone) holds exactly for deferred
        -- kinds; orphan kinds are defined by its absence.
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

    -- Rerun-input carriage: eligibility before policy, bytes copied and
    -- never re-encoded, the digest copied and never recomputed. This block
    -- is the whole envelope decision, rendered from the shared ladder.
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

    -- Attempt capture: the complete ordered sequence as canonical
    -- version-1 bytes. This block is the whole attempt decision.
    v_attempt_snapshot := horsies_encode_task_attempts(p_task_id);

    -- Result block, including the ratified administrative-cancel swap:
    -- canonical result is null and the pre-cancellation output is
    -- retained only as the separately named prior payload, copied from
    -- the locked live row and never re-encoded.
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
        CASE WHEN v_result_payload IS NOT NULL
                 THEN sha256(v_result_payload)
             WHEN v_prior_result_payload IS NOT NULL
                 THEN sha256(v_prior_result_payload)
             ELSE NULL END,
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
            1, sha256(v_result_payload),
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
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_complete_locked_task(
    p_task_id uuid,
    p_worker_id text,
    p_result text
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    SELECT claimed_at INTO v_claimed_at
    FROM horsies_tasks
    WHERE id = p_task_id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, ARRAY['COMPLETE_FUSED', 'COMPLETE_LOCKED']::text[],
            p_worker_id, NULL::timestamptz
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        p_task_id, 'COMPLETED', 'COMPLETE_LOCKED', v_terminal_at,
        p_result, NULL, NULL
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'COMPLETE_LOCKED'::text,
        'RUNNING'::text, CAST(p_worker_id AS VARCHAR), v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_complete_task_fused(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamptz,
    p_result text,
    p_notify_channel text,
    p_notify_payload text
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_ctx record;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    SELECT id, retry_count, started_at, claimed_by_worker_id, claimed_at,
           worker_hostname, worker_pid, worker_process_name,
           clock_timestamp() AS db_now
    INTO v_ctx
    FROM horsies_tasks
    WHERE id = p_task_id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND (p_claimed_at IS NULL OR claimed_at = p_claimed_at)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, ARRAY['COMPLETE_FUSED', 'COMPLETE_LOCKED']::text[],
            p_worker_id, p_claimed_at
        );
        RETURN;
    END IF;

    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry,
        started_at, finished_at,
        error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name
    )
    VALUES (
        v_ctx.id, COALESCE(v_ctx.retry_count, 0) + 1, 'COMPLETED', FALSE,
        COALESCE(v_ctx.started_at, v_ctx.db_now), v_ctx.db_now,
        NULL, NULL, NULL,
        v_ctx.claimed_by_worker_id, v_ctx.worker_hostname, v_ctx.worker_pid,
        v_ctx.worker_process_name
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
        p_task_id, 'COMPLETED', 'COMPLETE_FUSED', v_terminal_at,
        p_result, NULL, NULL
    );
    PERFORM pg_notify(p_notify_channel, p_notify_payload);
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'COMPLETE_FUSED'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id, v_ctx.claimed_at,
        NULL::text, NULL::jsonb;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_fail_locked_task(
    p_task_id uuid,
    p_worker_id text,
    p_result text,
    p_error_code text,
    p_failed_reason text
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    SELECT claimed_at INTO v_claimed_at
    FROM horsies_tasks
    WHERE id = p_task_id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, ARRAY['FAIL_RUNNING']::text[],
            p_worker_id, NULL::timestamptz
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        p_task_id, 'FAILED', 'FAIL_RUNNING', v_terminal_at,
        p_result, p_error_code, p_failed_reason
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'FAIL_RUNNING'::text,
        'RUNNING'::text, CAST(p_worker_id AS VARCHAR), v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_fail_stale_task(
    p_task_id uuid,
    p_stale_after_ms integer,
    p_finalizing_stale_after_ms integer,
    p_result text,
    p_error_code text,
    p_failed_reason text
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_started_at timestamptz;
    v_finalizing_at timestamptz;
    v_last_heartbeat timestamptz;
    v_evaluated_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- One capture: the row locked, the heartbeat read beside it, and the
    -- instant both arms are judged at. Nothing is reread after a refusal,
    -- so the evidence cannot show a heartbeat the guard never saw.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at,
           t.started_at, t.finalizing_at,
           (
               -- Recency bound, derived not constant: a heartbeat older
               -- than stale_after satisfies the staleness comparison for
               -- every possible value, so excluding it cannot change the
               -- verdict; the conservative floor is the larger threshold.
               -- On partitioned heartbeat storage the bound prunes the
               -- probe to current leaves.
               SELECT h.sent_at
               FROM horsies_heartbeats h
               WHERE h.task_id = t.id AND h.role = 'runner'
                 AND h.sent_at >= NOW() - make_interval(
                     secs => GREATEST(
                         p_stale_after_ms, p_finalizing_stale_after_ms
                     )::double precision / 1000.0
                 )
               ORDER BY h.sent_at DESC
               LIMIT 1
           ),
           NOW()
    INTO v_status, v_worker, v_claimed_at, v_started_at, v_finalizing_at,
         v_last_heartbeat, v_evaluated_at
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF FOUND AND v_status = 'RUNNING' THEN
        IF v_started_at IS NOT NULL
           AND (
               v_finalizing_at IS NULL
               OR v_finalizing_at
                  < v_evaluated_at
                    - make_interval(
                        secs => p_finalizing_stale_after_ms::double precision
                            / 1000.0
                    )
           )
           AND COALESCE(v_last_heartbeat, v_started_at)
               < v_evaluated_at
                    - make_interval(
                        secs => p_stale_after_ms::double precision / 1000.0
                    )
        THEN
            v_terminal_at := NOW();
            PERFORM horsies_move_task_to_history(
                p_task_id, 'FAILED', 'FAIL_STALE', v_terminal_at,
                p_result, p_error_code, p_failed_reason
            );
            -- Cross-worker by design: the observed claim is whichever
            -- worker's silence the guard just judged, from the capture.
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, 'FAIL_STALE'::text,
                'RUNNING'::text, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            NULL::timestamptz, NULL::text,
            v_status, v_worker, v_claimed_at,
            'STALENESS'::text,
            jsonb_build_object(
                'last_heartbeat_at', v_last_heartbeat,
                'started_at', v_started_at,
                'finalizing_at', v_finalizing_at,
                'stale_after_ms', p_stale_after_ms,
                'finalizing_stale_after_ms', p_finalizing_stale_after_ms,
                'evaluated_at', v_evaluated_at
            );
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id, ARRAY['FAIL_STALE']::text[],
        NULL::text, NULL::timestamptz
    );
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_expire_owned_claim(
    p_task_id uuid,
    p_worker_id text,
    p_result text,
    p_error_code text
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_good_until timestamptz;
    v_evaluated_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- One locked capture judges the deadline. Every good_until writer
    -- mutates this row and therefore needs the lock this transaction now
    -- holds, so the production retry-under-lock loop has no race to serve
    -- and is deliberately not ported.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at,
           t.good_until, NOW()
    INTO v_status, v_worker, v_claimed_at, v_good_until, v_evaluated_at
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF FOUND
       AND v_status = 'CLAIMED'
       AND v_worker = CAST(p_worker_id AS VARCHAR) THEN
        IF v_good_until IS NOT NULL AND v_good_until <= v_evaluated_at THEN
            v_terminal_at := NOW();
            PERFORM horsies_move_task_to_history(
                p_task_id, 'EXPIRED', 'EXPIRE_CLAIMED', v_terminal_at,
                p_result, p_error_code, NULL
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, 'EXPIRE_CLAIMED'::text,
                'CLAIMED'::text, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            NULL::timestamptz, NULL::text,
            v_status, v_worker, v_claimed_at,
            'DEADLINE'::text,
            jsonb_build_object(
                'good_until', v_good_until,
                'evaluated_at', v_evaluated_at
            );
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id, ARRAY['EXPIRE_CLAIMED', 'EXPIRE_PENDING']::text[],
        p_worker_id, NULL::timestamptz
    );
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_expire_pending_tasks(
    p_batch_size integer,
    p_result text,
    p_error_code text
)
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

    -- Discovery under the batch locking rule: SKIP LOCKED never waits on
    -- a row lock, so this batch cannot join any deadlock cycle; rows held
    -- by advisory-first singles are skipped and caught next sweep.
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

    -- Per-row uniqueness guard through the staged mechanism.
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
        horsies_encode_task_attempts(t.id),
        sha256(horsies_encode_task_attempts(t.id)),
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

    -- The reservation transition has ONE owner: the registry module.
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

    -- Outcome rows stream from the still-locked live rows BEFORE the
    -- deletes: reading them back through the partitioned parent by
    -- task id would be the rejected fan-out mechanism. RETURN QUERY
    -- materializes immediately; it does not end the function.
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
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_cancel_locked_task(
    p_task_id uuid,
    p_permitted_source_statuses text[]
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- The caller chooses the permitted live source set; the database owns
    -- the fixed part: plain tasks only, live statuses only, and the
    -- operation's literals are not caller-supplied.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at
    INTO v_status, v_worker, v_claimed_at
    FROM horsies_tasks t
    WHERE t.id = p_task_id
      AND t.is_workflow_task = FALSE
      AND t.status IN ('PENDING', 'CLAIMED', 'RUNNING')
      AND t.status = ANY(p_permitted_source_statuses)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, ARRAY['CANCEL_ADMIN']::text[],
            NULL::text, NULL::timestamptz
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        p_task_id, 'CANCELLED', 'CANCEL_ADMIN', v_terminal_at,
        NULL, 'TASK_CANCELLED', 'Cancelled via monitoring API'
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'CANCEL_ADMIN'::text,
        v_status, v_worker, v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_cancel_owned_orphan(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_is_workflow_task boolean;
    v_node_status text;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- One capture: the row locked, the runnable-link observation read
    -- beside it. A terminal link does not count as runnable and still
    -- leaves the backing task orphaned.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at,
           t.is_workflow_task,
           (
               SELECT wt.status
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id
                 AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
               ORDER BY wt.id
               LIMIT 1
           )
    INTO v_status, v_worker, v_claimed_at, v_is_workflow_task,
         v_node_status
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF FOUND
       AND v_status = 'CLAIMED'
       AND v_worker = CAST(p_worker_id AS VARCHAR)
       AND (p_claimed_at IS NULL OR v_claimed_at = p_claimed_at)
       AND v_is_workflow_task THEN
        IF v_node_status IS NULL THEN
            v_terminal_at := NOW();
            PERFORM horsies_move_task_to_history(
                p_task_id, 'CANCELLED', 'CANCEL_ORPHAN', v_terminal_at,
                NULL, 'WORKFLOW_CHECK_FAILED',
                'Workflow task orphaned: no live workflow_task linkage'
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, 'CANCEL_ORPHAN'::text,
                v_status, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        -- Fence classified before guard: only a live CLAIMED workflow task
        -- still held by this generation can truthfully say a runnable
        -- link, rather than ownership, refused the operation.
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            NULL::timestamptz, NULL::text,
            v_status, v_worker, v_claimed_at,
            'WORKFLOW_LINK_STATE'::text,
            jsonb_build_object('node_status', v_node_status);
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id, ARRAY['CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP']::text[],
        p_worker_id, p_claimed_at
    );
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_cancel_orphaned_tasks(
    p_batch_size integer
)
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

    -- Discovery under the batch locking rule: SKIP LOCKED never waits on
    -- a row lock, so this batch cannot join any deadlock cycle; rows held
    -- by advisory-first singles are skipped and caught next sweep.
    SELECT array_agg(s.id) INTO v_ids
    FROM (
        SELECT t2.id FROM horsies_tasks t2
        WHERE t2.is_workflow_task = TRUE
          AND t2.status IN ('CLAIMED', 'PENDING')
          AND NOT EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              WHERE wt.task_id = t2.id
                AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
          )
        LIMIT p_batch_size
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

    -- Per-row uniqueness guard through the staged mechanism.
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
        horsies_encode_task_attempts(t.id),
        sha256(horsies_encode_task_attempts(t.id)),
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

    -- The reservation transition has ONE owner: the registry module.
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

    -- Outcome rows stream from the still-locked live rows BEFORE the
    -- deletes: reading them back through the partitioned parent by
    -- task id would be the rejected fan-out mechanism. RETURN QUERY
    -- materializes immediately; it does not end the function.
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
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_abandon_owned_node(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at
    INTO v_status, v_worker, v_claimed_at
    FROM horsies_tasks t
    WHERE t.id = p_task_id
      AND t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND (p_claimed_at IS NULL OR t.claimed_at = p_claimed_at)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, ARRAY['PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW']::text[],
            p_worker_id, p_claimed_at
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        p_task_id, 'CANCELLED', 'PAUSE_ABANDON_CLAIM', v_terminal_at,
        NULL, 'TASK_CANCELLED', 'Workflow paused before task start'
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'PAUSE_ABANDON_CLAIM'::text,
        v_status, v_worker, v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_cancel_owned_node(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamptz,
    p_accepts_requeued_pending boolean
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- Full ownership fence, with the explicit carve-out for a task this
    -- same child observed as requeued to PENDING, where no claim remains
    -- to fence; the carve-out is a property of this variant only.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at
    INTO v_status, v_worker, v_claimed_at
    FROM horsies_tasks t
    WHERE t.id = p_task_id
      AND (
          (
              t.status = 'CLAIMED'
              AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
              AND (p_claimed_at IS NULL OR t.claimed_at = p_claimed_at)
          )
          OR (p_accepts_requeued_pending AND t.status = 'PENDING')
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, ARRAY['WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW']::text[],
            p_worker_id, p_claimed_at
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        p_task_id, 'CANCELLED', 'WORKFLOW_CANCEL_CLAIM', v_terminal_at,
        NULL, NULL, NULL
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'WORKFLOW_CANCEL_CLAIM'::text,
        v_status, v_worker, v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_abandon_owned_nodes(
    p_ids uuid[],
    p_claimed_ats timestamptz[],
    p_worker_id text
)
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

    -- Waits-in-global-order: lock every present input row in sorted id
    -- order before anything is judged.
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

    -- Per-row uniqueness guard through the staged mechanism.
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
        horsies_encode_task_attempts(t.id),
        sha256(horsies_encode_task_attempts(t.id)),
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

    -- The reservation transition has ONE owner: the registry module.
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

    -- Applied outcomes stream from the still-locked live rows at their
    -- input ordinality, BEFORE the deletes.
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

    -- Every non-applied input gets its answer through the ONE miss
    -- classifier, at its own ordinality.
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
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_cancel_owned_nodes(
    p_ids uuid[],
    p_claimed_ats timestamptz[],
    p_worker_id text
)
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

    -- Waits-in-global-order: lock every present input row in sorted id
    -- order before anything is judged.
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

    -- Per-row uniqueness guard through the staged mechanism.
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
        horsies_encode_task_attempts(t.id),
        sha256(horsies_encode_task_attempts(t.id)),
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

    -- The reservation transition has ONE owner: the registry module.
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

    -- Applied outcomes stream from the still-locked live rows at their
    -- input ordinality, BEFORE the deletes.
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

    -- Every non-applied input gets its answer through the ONE miss
    -- classifier, at its own ordinality.
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
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_abandon_nodes_of_paused_workflows(
    p_workflow_ids uuid[]
)
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

    -- Discovery under the batch locking rule: SKIP LOCKED never waits on
    -- a row lock, so this batch cannot join any deadlock cycle; rows held
    -- by advisory-first singles are skipped and caught next sweep.
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

    -- Per-row uniqueness guard through the staged mechanism.
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
        horsies_encode_task_attempts(t.id),
        sha256(horsies_encode_task_attempts(t.id)),
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

    -- The reservation transition has ONE owner: the registry module.
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

    -- Outcome rows stream from the still-locked live rows BEFORE the
    -- deletes: reading them back through the partitioned parent by
    -- task id would be the rejected fan-out mechanism. RETURN QUERY
    -- materializes immediately; it does not end the function.
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
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_cancel_nodes_of_cancelled_workflow(
    p_workflow_ids uuid[]
)
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

    -- Discovery under the batch locking rule: SKIP LOCKED never waits on
    -- a row lock, so this batch cannot join any deadlock cycle; rows held
    -- by advisory-first singles are skipped and caught next sweep.
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
                -- EXPIRED propagates exactly as CANCELLED: one batch
                -- serves both, and the backing row carries the
                -- workflow-cancel kind either way.
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

    -- Per-row uniqueness guard through the staged mechanism.
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
        horsies_encode_task_attempts(t.id),
        sha256(horsies_encode_task_attempts(t.id)),
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

    -- The reservation transition has ONE owner: the registry module.
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

    -- Outcome rows stream from the still-locked live rows BEFORE the
    -- deletes: reading them back through the partitioned parent by
    -- task id would be the rejected fan-out mechanism. RETURN QUERY
    -- materializes immediately; it does not end the function.
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
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE TYPE horsies_phase2_disposition AS (
    disposition text,
    workflow_id uuid,
    node_row_id uuid,
    task_index integer,
    workflow_status text,
    workflow_depth integer,
    root_workflow_id uuid,
    on_error text,
    node_status text,
    terminal_status text,
    detail text
)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_phase2_consume(
    p_task_id uuid,
    p_terminal_node_status text
) RETURNS horsies_phase2_disposition
LANGUAGE plpgsql
AS $function$
DECLARE
    v_pending horsies_workflow_phase2_pending%ROWTYPE;
    v_wf record;
    v_node record;
    v_payload bytea;
    v_digest bytea;
    v_version smallint;
    v_source_task uuid;
    v_cas_won boolean;
BEGIN
    IF p_terminal_node_status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION
            'terminal node status must be COMPLETED, FAILED, or CANCELLED'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_pending
    FROM horsies_workflow_phase2_pending
    WHERE task_id = p_task_id;

    IF NOT FOUND THEN
        -- Idempotent replay after an uncertain commit: the first commit
        -- deleted pending. Classify from the node the task backs.
        SELECT wt.id, wt.workflow_id, wt.task_index, wt.status
        INTO v_node
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = p_task_id
        ORDER BY wt.id
        LIMIT 1;
        IF NOT FOUND THEN
            RETURN ROW('PENDING_ABSENT', NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL,
                       'no pending row and no node linkage')
                ::horsies_phase2_disposition;
        END IF;
        SELECT w.status, w.depth, w.root_workflow_id, w.on_error
        INTO v_wf
        FROM horsies_workflows w
        WHERE w.id = v_node.workflow_id;
        IF v_node.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'SKIPPED')
        THEN
            RETURN ROW('ALREADY_APPLIED', v_node.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error,
                       v_node.status, NULL, NULL)
                ::horsies_phase2_disposition;
        END IF;
        RETURN ROW('PENDING_ABSENT', v_node.workflow_id, v_node.id,
                   v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error,
                   v_node.status, NULL,
                   'no pending row; node not terminal')
            ::horsies_phase2_disposition;
    END IF;

    -- N6 order: workflow row first, node row second, pending third.
    SELECT w.status, w.depth, w.root_workflow_id, w.on_error
    INTO v_wf
    FROM horsies_workflows w
    WHERE w.id = v_pending.workflow_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_pending.workflow_node_row_id, NULL, NULL, NULL,
                   NULL, NULL, NULL, v_pending.terminal_status,
                   'workflow row absent while pending exists')
            ::horsies_phase2_disposition;
    END IF;

    SELECT wt.id, wt.workflow_id, wt.task_index, wt.status
    INTO v_node
    FROM horsies_workflow_tasks wt
    WHERE wt.id = v_pending.workflow_node_row_id
      AND wt.workflow_id = v_pending.workflow_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_pending.workflow_node_row_id, NULL, v_wf.status,
                   v_wf.depth, v_wf.root_workflow_id, v_wf.on_error,
                   NULL, v_pending.terminal_status,
                   'node row absent while pending exists')
            ::horsies_phase2_disposition;
    END IF;

    PERFORM 1 FROM horsies_workflow_phase2_pending
    WHERE task_id = p_task_id
    FOR UPDATE;

    IF v_wf.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
        DELETE FROM horsies_workflow_phase2_pending WHERE task_id = p_task_id;
        IF v_pending.recovery_source = 'QUARANTINE' THEN
            DELETE FROM horsies_workflow_phase2_quarantine
            WHERE task_id = v_pending.quarantine_task_id;
        END IF;
        RETURN ROW('SUPERSEDED_BY_WORKFLOW_TERMINAL',
                   v_pending.workflow_id, v_node.id, v_node.task_index,
                   v_wf.status, v_wf.depth, v_wf.root_workflow_id,
                   v_wf.on_error, v_node.status,
                   v_pending.terminal_status, NULL)
            ::horsies_phase2_disposition;
    END IF;

    IF v_pending.recovery_source = 'HISTORY' THEN
        -- One-leaf parent probe, NOT the rejected fan-out: the locator
        -- supplies both partition keys, so LIST (class) and RANGE
        -- (anchor) prune to exactly one leaf at plan time. The rejected
        -- mechanism carried a task-id predicate alone and planned every
        -- leaf; this read exists because the locator makes pruning
        -- possible.
        SELECT h.task_id, h.result_payload, h.result_digest,
               h.history_schema_version
        INTO v_source_task, v_payload, v_digest, v_version
        FROM horsies_task_history h
        WHERE h.retention_class_key = v_pending.history_class
          AND h.retention_anchor_at = v_pending.history_anchor
          AND h.task_id = p_task_id;
        IF NOT FOUND THEN
            RETURN ROW('SOURCE_ABSENT', v_pending.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                       v_pending.terminal_status,
                       'history row absent at locator')
                ::horsies_phase2_disposition;
        END IF;
    ELSE
        SELECT q.task_id, q.result_payload, q.result_digest,
               q.history_schema_version
        INTO v_source_task, v_payload, v_digest, v_version
        FROM horsies_workflow_phase2_quarantine q
        WHERE q.task_id = v_pending.quarantine_task_id;
        IF NOT FOUND THEN
            RETURN ROW('SOURCE_ABSENT', v_pending.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                       v_pending.terminal_status,
                       'quarantine row absent at locator')
                ::horsies_phase2_disposition;
        END IF;
    END IF;

    IF v_source_task <> p_task_id THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'source row carries a different task identity')
            ::horsies_phase2_disposition;
    END IF;
    IF v_version IS DISTINCT FROM v_pending.history_schema_version
       OR v_version <> 1 THEN
        RETURN ROW('SOURCE_VERSION_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'source schema version disagrees with locator')
            ::horsies_phase2_disposition;
    END IF;
    IF v_digest IS DISTINCT FROM v_pending.result_digest
       OR v_payload IS NULL
       OR sha256(v_payload) <> v_pending.result_digest THEN
        RETURN ROW('SOURCE_DIGEST_MISMATCH', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'result digest disagrees with locator or payload')
            ::horsies_phase2_disposition;
    END IF;

    UPDATE horsies_workflow_tasks wt
    SET status = p_terminal_node_status,
        result = convert_from(v_payload, 'UTF8'),
        completed_at = NOW()
    WHERE wt.id = v_node.id
      AND wt.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'SKIPPED');
    v_cas_won := FOUND;

    DELETE FROM horsies_workflow_phase2_pending WHERE task_id = p_task_id;
    IF v_pending.recovery_source = 'QUARANTINE' THEN
        DELETE FROM horsies_workflow_phase2_quarantine
        WHERE task_id = v_pending.quarantine_task_id;
    END IF;

    IF v_cas_won THEN
        RETURN ROW('APPLIED_TO_NODE', v_pending.workflow_id, v_node.id,
                   v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error,
                   p_terminal_node_status, v_pending.terminal_status, NULL)
            ::horsies_phase2_disposition;
    END IF;
    RETURN ROW('ALREADY_APPLIED', v_pending.workflow_id, v_node.id,
               v_node.task_index, v_wf.status, v_wf.depth,
               v_wf.root_workflow_id, v_wf.on_error, v_node.status,
               v_pending.terminal_status, NULL)
        ::horsies_phase2_disposition;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE TYPE horsies_phase2_quarantine_verdict AS (
    verdict text,
    detail text
)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_phase2_quarantine_one(
    p_task_id uuid,
    p_reason text
) RETURNS horsies_phase2_quarantine_verdict
LANGUAGE plpgsql
AS $function$
DECLARE
    v_pending horsies_workflow_phase2_pending%ROWTYPE;
    v_node_id text;
    v_hist record;
    v_copy record;
BEGIN
    -- The only lock this function takes: the pending row. Single-tier;
    -- consumption acquiring workflow -> node -> pending can wait on it
    -- but no cycle can form because nothing else is held here.
    SELECT * INTO v_pending
    FROM horsies_workflow_phase2_pending
    WHERE task_id = p_task_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('PENDING_GONE', 'no pending row; locator drained')
            ::horsies_phase2_quarantine_verdict;
    END IF;
    IF v_pending.recovery_source <> 'HISTORY' THEN
        RETURN ROW('ALREADY_QUARANTINED',
                   'pending already repointed at quarantine')
            ::horsies_phase2_quarantine_verdict;
    END IF;

    SELECT wt.node_id INTO v_node_id
    FROM horsies_workflow_tasks wt
    WHERE wt.id = v_pending.workflow_node_row_id
      AND wt.workflow_id = v_pending.workflow_id;
    IF NOT FOUND THEN
        RETURN ROW('NODE_ROW_ABSENT',
                   'node row absent while pending exists')
            ::horsies_phase2_quarantine_verdict;
    END IF;
    IF v_node_id IS NULL THEN
        -- The quarantine projection requires a node identity and the
        -- node row carries none; refusing retains the history locator.
        RETURN ROW('NODE_IDENTITY_ABSENT',
                   'node row carries no node_id')
            ::horsies_phase2_quarantine_verdict;
    END IF;

    -- One-leaf parent probe, NOT the rejected fan-out: the locator
    -- supplies both partition keys, so LIST (class) and RANGE (anchor)
    -- prune to exactly one leaf at plan time.
    SELECT h.task_id, h.task_name, h.status, h.terminalization_kind,
           h.terminal_at, h.history_schema_version,
           h.result_envelope_version, h.result_codec,
           h.result_content_type, h.result_payload, h.result_digest
    INTO v_hist
    FROM horsies_task_history h
    WHERE h.retention_class_key = v_pending.history_class
      AND h.retention_anchor_at = v_pending.history_anchor
      AND h.task_id = p_task_id;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_ABSENT', 'history row absent at locator')
            ::horsies_phase2_quarantine_verdict;
    END IF;

    BEGIN
        INSERT INTO horsies_workflow_phase2_quarantine (
            task_id, workflow_id, workflow_node_row_id, node_id,
            task_name, terminal_status, terminalization_kind, terminal_at,
            history_schema_version, result_envelope_version,
            result_codec, result_content_type,
            result_payload, result_digest,
            source_history_class, source_history_anchor,
            quarantine_reason, quarantined_at
        ) VALUES (
            v_hist.task_id, v_pending.workflow_id,
            v_pending.workflow_node_row_id, v_node_id,
            v_hist.task_name, v_hist.status, v_hist.terminalization_kind,
            v_hist.terminal_at,
            v_hist.history_schema_version, v_hist.result_envelope_version,
            v_hist.result_codec, v_hist.result_content_type,
            v_hist.result_payload, v_hist.result_digest,
            v_pending.history_class, v_pending.history_anchor,
            p_reason, statement_timestamp()
        );

        -- Verification on the copy itself: read the row back and hold
        -- it against the pending locator, so a projection defect is a
        -- refusal, not a corrupt quarantine row.
        SELECT q.task_id, q.result_payload, q.result_digest,
               q.history_schema_version
        INTO STRICT v_copy
        FROM horsies_workflow_phase2_quarantine q
        WHERE q.task_id = p_task_id;
        IF v_copy.task_id IS DISTINCT FROM p_task_id
           OR v_copy.result_digest IS DISTINCT FROM v_pending.result_digest
           OR v_copy.result_payload IS NULL
           OR sha256(v_copy.result_payload) <> v_copy.result_digest
           OR v_copy.history_schema_version
              IS DISTINCT FROM v_pending.history_schema_version
        THEN
            RAISE EXCEPTION 'quarantine copy disagrees with locator'
                USING ERRCODE = 'HQ001';
        END IF;
    EXCEPTION
        WHEN SQLSTATE 'HQ001' OR unique_violation OR not_null_violation
             OR check_violation THEN
            -- The sub-transaction rolls the copy back; pending keeps its
            -- history locator and the leaf stays pinned.
            RETURN ROW('COPY_VERIFICATION_FAILED', SQLERRM)
                ::horsies_phase2_quarantine_verdict;
    END;

    UPDATE horsies_workflow_phase2_pending
    SET recovery_source = 'QUARANTINE',
        quarantine_task_id = p_task_id,
        history_class = NULL,
        history_anchor = NULL
    WHERE task_id = p_task_id;

    RETURN ROW('REPOINTED', NULL)::horsies_phase2_quarantine_verdict;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE TABLE horsies_archive_replacement_jobs (
    job_id uuid PRIMARY KEY,
    maintenance_session_id uuid NOT NULL
        REFERENCES horsies_archive_maintenance_sessions(session_id),
    component text NOT NULL CHECK (
        component IN ('HISTORY_ROW', 'RESULT', 'ATTEMPTS', 'RERUN_INPUT')
    ),
    source_version smallint NOT NULL,
    target_version smallint NOT NULL,
    source_codec text NOT NULL,
    target_codec text NOT NULL,
    state text NOT NULL CHECK (
        state IN (
            'PLANNED', 'COPYING', 'COPIED',
            'VERIFIED', 'SWAPPED', 'COMPLETE'
        )
    ),
    transformed_rows bigint NOT NULL CHECK (transformed_rows >= 0),
    copied_rows_total bigint NOT NULL CHECK (copied_rows_total >= 0),
    copied_rows_completed bigint NOT NULL CHECK (
        copied_rows_completed >= 0
        AND copied_rows_completed <= copied_rows_total
    ),
    payload_rows bigint NOT NULL CHECK (payload_rows >= 0),
    payload_bytes_before bigint NOT NULL CHECK (payload_bytes_before >= 0),
    projected_payload_bytes bigint NOT NULL CHECK (
        projected_payload_bytes >= 0
    ),
    affected_relation_bytes bigint NOT NULL CHECK (
        affected_relation_bytes >= 0
    ),
    started_at timestamptz NOT NULL,
    last_batch_at timestamptz,
    copied_at timestamptz,
    verified_at timestamptz,
    swapped_at timestamptz,
    completed_at timestamptz,
    start_lsn pg_lsn NOT NULL,
    wal_bytes bigint CHECK (wal_bytes IS NULL OR wal_bytes >= 0),
    CHECK ((state = 'COMPLETE') = (completed_at IS NOT NULL)),
    CHECK ((state = 'COMPLETE') = (wal_bytes IS NOT NULL))
)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE UNIQUE INDEX horsies_archive_replacement_jobs_single_active_idx
    ON horsies_archive_replacement_jobs ((1))
    WHERE state <> 'COMPLETE'$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE TABLE horsies_archive_replacement_relations (
    job_id uuid NOT NULL REFERENCES horsies_archive_replacement_jobs(job_id),
    relation_ordinal integer NOT NULL CHECK (relation_ordinal > 0),
    source_relation_oid bigint NOT NULL,
    source_relation_name text NOT NULL,
    parent_relation_oid bigint NOT NULL,
    parent_relation_name text NOT NULL,
    partition_bound text NOT NULL,
    partition_constraint text NOT NULL,
    replacement_relation_name text NOT NULL,
    replacement_relation_oid bigint,
    backup_relation_name text NOT NULL,
    state text NOT NULL CHECK (
        state IN (
            'PLANNED', 'COPYING', 'COPIED',
            'VERIFIED', 'SWAPPED', 'COMPLETE'
        )
    ),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    transformed_rows bigint NOT NULL CHECK (transformed_rows >= 0),
    rows_copied bigint NOT NULL CHECK (
        rows_copied >= 0 AND rows_copied <= row_count
    ),
    relation_bytes bigint NOT NULL CHECK (relation_bytes >= 0),
    last_source_ctid tid,
    source_mutation_generation bigint NOT NULL DEFAULT 0
        CHECK (source_mutation_generation >= 0),
    replacement_mutation_generation bigint NOT NULL DEFAULT 0
        CHECK (replacement_mutation_generation >= 0),
    verified_source_generation bigint CHECK (
        verified_source_generation IS NULL
        OR verified_source_generation >= 0
    ),
    verified_replacement_generation bigint CHECK (
        verified_replacement_generation IS NULL
        OR verified_replacement_generation >= 0
    ),
    verified_source_filenode bigint,
    verified_replacement_filenode bigint,
    verified_source_schema_signature text,
    verified_replacement_schema_signature text,
    prepared_at timestamptz,
    copied_at timestamptz,
    verified_at timestamptz,
    swapped_at timestamptz,
    completed_at timestamptz,
    PRIMARY KEY (job_id, relation_ordinal),
    UNIQUE (job_id, source_relation_name),
    UNIQUE (job_id, replacement_relation_name),
    UNIQUE (job_id, backup_relation_name)
)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE TABLE horsies_archive_replacement_batches (
    job_id uuid NOT NULL REFERENCES horsies_archive_replacement_jobs(job_id),
    batch_number integer NOT NULL CHECK (batch_number > 0),
    relation_ordinal integer NOT NULL,
    rows_copied integer NOT NULL CHECK (rows_copied > 0),
    committed_at timestamptz NOT NULL,
    PRIMARY KEY (job_id, batch_number),
    FOREIGN KEY (job_id, relation_ordinal)
        REFERENCES horsies_archive_replacement_relations(job_id, relation_ordinal)
)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE FUNCTION horsies_archive_replacement_note_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    changed_rows integer;
BEGIN
    UPDATE horsies_archive_replacement_relations
    SET source_mutation_generation =
            source_mutation_generation
            + CASE WHEN source_relation_oid = TG_RELID
                   THEN 1 ELSE 0 END,
        replacement_mutation_generation =
            replacement_mutation_generation
            + CASE WHEN replacement_relation_oid = TG_RELID
                   THEN 1 ELSE 0 END
    WHERE state <> 'COMPLETE'
      AND (
            source_relation_oid = TG_RELID
            OR replacement_relation_oid = TG_RELID
          );
    GET DIAGNOSTICS changed_rows = ROW_COUNT;
    IF changed_rows <> 1 THEN
        RAISE EXCEPTION
            'archive replacement mutation guard has % owners for %',
            changed_rows, TG_RELID;
    END IF;
    RETURN NULL;
END
$function$$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$CREATE TABLE IF NOT EXISTS horsies_cutover_relocation_ledger (
    batch_number bigint PRIMARY KEY,
    task_ids uuid[] NOT NULL,
    rows_relocated integer NOT NULL,
    legacy_kind_rows integer NOT NULL,
    committed_at timestamptz NOT NULL
)$horsies_p1_sql$;
    DROP TABLE horsies_heartbeats;
        EXECUTE $horsies_p1_sql$CREATE TABLE horsies_heartbeats (
    id bigint GENERATED BY DEFAULT AS IDENTITY,
    task_id uuid NOT NULL,
    sender_id varchar(255) NOT NULL,
    role varchar(20) NOT NULL,
    sent_at timestamptz NOT NULL,
    hostname varchar(255),
    pid integer
) PARTITION BY RANGE (sent_at)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ALTER COLUMN command_fingerprint_version SET NOT NULL,
    ALTER COLUMN command_fingerprint SET NOT NULL,
    ALTER COLUMN retention_class_key SET NOT NULL,
    ALTER COLUMN retain_rerun_input SET NOT NULL,
    ALTER COLUMN prepared_rerun_input_disposition SET NOT NULL$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_command_fingerprint_version_cutover
    CHECK (command_fingerprint_version > 0)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_command_fingerprint_cutover
    CHECK (octet_length(command_fingerprint) = 32)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_input_digest_cutover
    CHECK (input_digest IS NULL OR octet_length(input_digest) = 32)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_idempotency_key_digest_cutover
    CHECK (idempotency_key_digest IS NULL
                OR octet_length(idempotency_key_digest) = 32)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_prepared_rerun_input_disposition_cutover
    CHECK (prepared_rerun_input_disposition IN (
                    'INLINE', 'REFERENCE', 'DECLINED_BY_POLICY',
                    'OVER_BOUND', 'NEVER_ELIGIBLE'
                ))$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_prepared_rerun_input_inline_cutover
    CHECK (prepared_rerun_input_inline IS NULL
                OR octet_length(prepared_rerun_input_inline) <= 65536)$horsies_p1_sql$;
        EXECUTE $horsies_p1_sql$ALTER TABLE horsies_tasks
    ADD CONSTRAINT horsies_tasks_rerun_lineage_pair
    CHECK ((rerun_of_task_id IS NULL AND rerun_root_task_id IS NULL)
            OR (rerun_of_task_id IS NOT NULL
                AND rerun_root_task_id IS NOT NULL))$horsies_p1_sql$;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'horsies_workflow_phase2_pending_node_fkey'
          AND confdeltype = 'c'
    ) THEN
        ALTER TABLE horsies_workflow_phase2_pending
            DROP CONSTRAINT IF EXISTS
                horsies_workflow_phase2_pending_node_fkey;
        ALTER TABLE horsies_workflow_tasks
            DROP CONSTRAINT IF EXISTS
                horsies_workflow_tasks_node_workflow_key;
            EXECUTE $horsies_p1_sql$ALTER TABLE horsies_workflow_tasks
            ADD CONSTRAINT horsies_workflow_tasks_node_workflow_key
            UNIQUE (id, workflow_id)$horsies_p1_sql$;
            EXECUTE $horsies_p1_sql$ALTER TABLE horsies_workflow_phase2_pending
            ADD CONSTRAINT horsies_workflow_phase2_pending_node_fkey
            FOREIGN KEY (workflow_node_row_id, workflow_id)
            REFERENCES horsies_workflow_tasks (id, workflow_id)
            ON DELETE CASCADE$horsies_p1_sql$;
    END IF;

    SELECT count(*) INTO v_terminal_rows
    FROM horsies_tasks
    WHERE status NOT IN ('PENDING', 'CLAIMED', 'RUNNING');
    IF v_terminal_rows <> 0 THEN
        v_violations := array_append(
            v_violations, v_terminal_rows || ' terminal rows remain live'
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'horsies_tasks'::regclass
          AND conname = 'horsies_tasks_live_status_only'
    ) THEN
        v_violations := array_append(
            v_violations, 'the live-only status domain is absent'
        );
    END IF;

    FOR v_column IN SELECT column_name FROM (VALUES
            ('command_fingerprint_version'),
            ('command_fingerprint'),
            ('retention_class_key'),
            ('retain_rerun_input'),
            ('prepared_rerun_input_disposition')
        ) AS required(column_name)
    LOOP
        IF NOT COALESCE((
            SELECT attnotnull
            FROM pg_attribute
            WHERE attrelid = 'horsies_tasks'::regclass
              AND attname = v_column
        ), FALSE) THEN
            v_violations := array_append(
                v_violations,
                'declared not-null column ' || v_column || ' is nullable'
            );
        END IF;
    END LOOP;

    FOR v_relation, v_column IN SELECT relation_name, column_name FROM (VALUES
            ('horsies_tasks', 'id'),
            ('horsies_task_attempts', 'task_id'),
            ('horsies_workflows', 'id'),
            ('horsies_workflows', 'parent_workflow_id'),
            ('horsies_workflows', 'root_workflow_id'),
            ('horsies_workflow_tasks', 'id'),
            ('horsies_workflow_tasks', 'workflow_id'),
            ('horsies_workflow_tasks', 'task_id'),
            ('horsies_heartbeats', 'task_id')
        ) AS required(relation_name, column_name)
    LOOP
        IF NOT COALESCE((
            SELECT atttypid = 'uuid'::regtype
            FROM pg_attribute
            WHERE attrelid = v_relation::regclass
              AND attname = v_column
        ), FALSE) THEN
            v_violations := array_append(
                v_violations, v_relation || '.' || v_column || ' is not uuid'
            );
        END IF;
    END LOOP;

    IF NOT COALESCE((
        SELECT relkind = 'p' FROM pg_class
        WHERE oid = 'horsies_heartbeats'::regclass
    ), FALSE) THEN
        v_violations := array_append(
            v_violations, 'the heartbeat shape is not partitioned'
        );
    END IF;

    IF NOT COALESCE((
        SELECT relkind = 'p' FROM pg_class
        WHERE oid = to_regclass('horsies_task_history_forever')
    ), FALSE) THEN
        v_violations := array_append(
            v_violations, 'the forever history class is not RANGE-partitioned'
        );
    ELSE
        SELECT count(*) INTO v_uncataloged
        FROM pg_partition_tree(
            'horsies_task_history_forever'::regclass
        ) AS tree
        JOIN pg_class AS child ON child.oid = tree.relid
        LEFT JOIN horsies_task_history_leaf_catalog AS catalog
          ON catalog.leaf_name = child.relname
         AND catalog.detached_at IS NULL
         AND catalog.dropped_at IS NULL
        WHERE tree.isleaf
          AND catalog.leaf_name IS NULL;
        IF v_uncataloged <> 0 THEN
            v_violations := array_append(
                v_violations,
                v_uncataloged ||
                    ' forever history leaves are absent from the leaf catalog'
            );
        END IF;
    END IF;

    SELECT count(*) INTO v_history_rows FROM horsies_task_history;
    SELECT COALESCE(sum(rows_relocated), 0)
    INTO v_ledger_rows
    FROM horsies_cutover_relocation_ledger;
    IF v_history_rows < v_ledger_rows THEN
        v_violations := array_append(
            v_violations,
            'history holds ' || v_history_rows || ' rows but the ledger recorded ' ||
                v_ledger_rows || ' relocations'
        );
    END IF;

    IF cardinality(v_violations) <> 0 THEN
        RAISE EXCEPTION 'native-uuid schema failed cutover validation: %',
            array_to_string(v_violations, '; ');
    END IF;
        EXECUTE $horsies_p1_sql$INSERT INTO horsies_cutover_state (cutover_name)
VALUES ('task_history_v1_validated_v1')
ON CONFLICT (cutover_name) DO NOTHING$horsies_p1_sql$;
END
$migration$;
