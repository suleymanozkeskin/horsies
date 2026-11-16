-- Schema v29: one atomic, final-shape history foundation.
CREATE TABLE horsies_retention_classes (
    class_key varchar(64) PRIMARY KEY,
    duration interval,
    partition_interval interval,
    finite_parent_name text,
    created_at timestamptz NOT NULL,
    CHECK (octet_length(class_key) BETWEEN 1 AND 64),
    CHECK (
        (duration IS NULL
            AND partition_interval IS NULL
            AND finite_parent_name IS NULL)
        OR (duration > interval '0'
            AND partition_interval > interval '0'
            AND finite_parent_name IS NOT NULL)
    )
);
INSERT INTO horsies_retention_classes
    (class_key, duration, partition_interval, finite_parent_name, created_at)
VALUES ('forever', NULL, NULL, NULL, statement_timestamp());
CREATE TABLE horsies_task_history (
    task_id uuid NOT NULL,
    task_name varchar(255) NOT NULL,
    queue_name varchar(100) NOT NULL,
    priority integer NOT NULL CHECK (priority BETWEEN 1 AND 100),
    command_fingerprint_version smallint NOT NULL
        CHECK (command_fingerprint_version > 0),
    command_fingerprint bytea NOT NULL
        CHECK (octet_length(command_fingerprint) = 32),
    status text NOT NULL CHECK (
        status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ),
    terminalization_kind varchar(32) NOT NULL CHECK (
        terminalization_kind IN ('COMPLETE_LOCKED', 'COMPLETE_FUSED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING', 'CANCEL_ADMIN', 'CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW', 'LEGACY_TERMINAL')
    ),
    terminal_at timestamptz NOT NULL,
    retention_anchor_at timestamptz NOT NULL,
    retention_class_key varchar(64) NOT NULL
        REFERENCES horsies_retention_classes(class_key),
    sent_at timestamptz,
    enqueued_at timestamptz NOT NULL,
    claimed_at timestamptz,
    started_at timestamptz,
    created_at timestamptz NOT NULL,
    good_until timestamptz,
    retry_count integer NOT NULL CHECK (retry_count >= 0),
    max_retries integer NOT NULL CHECK (max_retries >= 0),
    last_claimed_worker_id varchar(255),
    last_worker_hostname varchar(255),
    last_worker_pid integer,
    last_worker_process_name varchar(255),
    result_envelope_version smallint NOT NULL
        CHECK (result_envelope_version > 0),
    result_codec varchar(64) NOT NULL,
    result_content_type varchar(255) NOT NULL,
    result_payload bytea,
    prior_result_payload bytea,
    result_digest bytea,
    error_code text,
    final_failed_reason text,
    input_digest bytea,
    rerun_of_task_id uuid,
    rerun_root_task_id uuid,
    workflow_id uuid,
    is_workflow_task boolean NOT NULL,
    history_schema_version smallint NOT NULL
        CHECK (history_schema_version > 0),
    CHECK (retention_anchor_at = terminal_at),
    CHECK (octet_length(result_codec) BETWEEN 1 AND 64),
    CHECK (octet_length(result_content_type) BETWEEN 1 AND 255),
    CHECK (result_digest IS NULL OR octet_length(result_digest) = 32),
    CHECK (input_digest IS NULL OR octet_length(input_digest) = 32),
    CHECK (
        (rerun_of_task_id IS NULL AND rerun_root_task_id IS NULL)
        OR (rerun_of_task_id IS NOT NULL AND rerun_root_task_id IS NOT NULL)
    ),
    CHECK (
        terminalization_kind <> 'CANCEL_ADMIN'
        OR result_payload IS NULL
    ),
    CHECK (
        prior_result_payload IS NULL
        OR terminalization_kind = 'CANCEL_ADMIN'
    ),
    CHECK (result_payload IS NULL OR prior_result_payload IS NULL),
    CHECK (prior_result_payload IS NULL OR result_digest IS NOT NULL)
) PARTITION BY LIST (retention_class_key);
CREATE TABLE horsies_task_history_forever
    PARTITION OF horsies_task_history
    FOR VALUES IN ('forever')
    PARTITION BY RANGE (retention_anchor_at);
CREATE TABLE horsies_task_history_leaf_catalog (
    leaf_name text PRIMARY KEY,
    parent_name text NOT NULL,
    class_key text NOT NULL
        REFERENCES horsies_retention_classes(class_key),
    lower_anchor timestamptz NOT NULL,
    upper_anchor timestamptz NOT NULL,
    index_schema_version smallint NOT NULL,
    id_index_name text NOT NULL,
    partition_bound text NOT NULL,
    min_birth_at timestamptz,
    min_birth_verified boolean NOT NULL,
    created_at timestamptz NOT NULL,
    detached_at timestamptz,
    dropped_at timestamptz,
    CHECK (lower_anchor < upper_anchor),
    CHECK (dropped_at IS NULL OR detached_at IS NOT NULL),
    UNIQUE (class_key, lower_anchor, upper_anchor)
);
CREATE FUNCTION horsies_task_history_leaf_lock_key(
    p_class_key text,
    p_anchor timestamptz
) RETURNS bigint
LANGUAGE sql
STABLE
STRICT
AS $function$
    SELECT hashtextextended(
        p_class_key || chr(31) ||
        extract(epoch FROM date_trunc('day', p_anchor, 'UTC'))
            ::bigint::text,
        1601
    )
$function$;
CREATE TYPE horsies_task_lookup AS (
    found boolean,
    location text,
    task_id uuid,
    fingerprint_version smallint,
    command_fingerprint bytea
);
CREATE TYPE horsies_task_provenance AS (
    found boolean,
    location text,
    task_id uuid,
    status text,
    terminal_at timestamptz,
    terminalization_kind text
);
CREATE TABLE horsies_task_lookup_manifest (
    leaf_name text PRIMARY KEY,
    probe_position integer NOT NULL UNIQUE,
    lower_anchor timestamptz NOT NULL,
    upper_anchor timestamptz NOT NULL,
    min_birth_at timestamptz,
    published_at timestamptz NOT NULL,
    CHECK (probe_position >= 0),
    CHECK (lower_anchor < upper_anchor)
);
CREATE TABLE horsies_workflow_phase2_quarantine (
    task_id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL,
    workflow_node_row_id uuid NOT NULL,
    node_id text NOT NULL,
    task_name varchar(255) NOT NULL,
    terminal_status text NOT NULL CHECK (
        terminal_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ),
    terminalization_kind varchar(32) NOT NULL CHECK (
        terminalization_kind IN ('COMPLETE_LOCKED', 'COMPLETE_FUSED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING', 'CANCEL_ADMIN', 'CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW', 'LEGACY_TERMINAL')
    ),
    terminal_at timestamptz NOT NULL,
    history_schema_version smallint NOT NULL
        CHECK (history_schema_version > 0),
    result_envelope_version smallint NOT NULL
        CHECK (result_envelope_version > 0),
    result_codec varchar(64) NOT NULL,
    result_content_type varchar(255) NOT NULL,
    result_payload bytea NOT NULL,
    result_digest bytea NOT NULL,
    source_history_class varchar(64) NOT NULL,
    source_history_anchor timestamptz NOT NULL,
    quarantine_reason text NOT NULL,
    quarantined_at timestamptz NOT NULL,
    CHECK (octet_length(result_codec) BETWEEN 1 AND 64),
    CHECK (octet_length(result_content_type) BETWEEN 1 AND 255),
    CHECK (octet_length(result_digest) = 32),
    CHECK (octet_length(source_history_class) BETWEEN 1 AND 64)
);
CREATE TABLE horsies_workflow_phase2_pending (
    task_id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL,
    workflow_node_row_id uuid NOT NULL,
    terminal_status text NOT NULL CHECK (
        terminal_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ),
    terminal_at timestamptz NOT NULL,
    terminalization_kind varchar(32) NOT NULL CHECK (
        terminalization_kind IN ('COMPLETE_LOCKED', 'COMPLETE_FUSED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING', 'CANCEL_ADMIN', 'CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW', 'LEGACY_TERMINAL')
    ),
    recovery_source text NOT NULL CHECK (
        recovery_source IN ('HISTORY', 'QUARANTINE')
    ),
    history_class varchar(64)
        CHECK (
            history_class IS NULL
            OR octet_length(history_class) BETWEEN 1 AND 64
        ),
    history_anchor timestamptz,
    history_schema_version smallint NOT NULL
        CHECK (history_schema_version > 0),
    result_digest bytea NOT NULL CHECK (octet_length(result_digest) = 32),
    quarantine_task_id uuid
        REFERENCES horsies_workflow_phase2_quarantine(task_id),
    phase2_generation uuid NOT NULL,
    created_at timestamptz NOT NULL,
    attempt_count integer NOT NULL CHECK (attempt_count >= 0),
    last_attempt_at timestamptz,
    last_failure_class varchar(64)
        CHECK (
            last_failure_class IS NULL
            OR octet_length(last_failure_class) BETWEEN 1 AND 64
        ),
    CHECK (
        (recovery_source = 'HISTORY'
            AND history_class IS NOT NULL
            AND history_anchor IS NOT NULL
            AND quarantine_task_id IS NULL)
        OR (recovery_source = 'QUARANTINE'
            AND quarantine_task_id IS NOT NULL)
    )
);
CREATE INDEX horsies_workflow_phase2_pending_age_idx
        ON horsies_workflow_phase2_pending (created_at, task_id);
CREATE INDEX horsies_workflow_phase2_pending_node_idx
        ON horsies_workflow_phase2_pending (workflow_node_row_id);
CREATE INDEX horsies_workflow_phase2_pending_locator_idx
        ON horsies_workflow_phase2_pending (history_class, history_anchor, task_id)
        WHERE recovery_source = 'HISTORY';
CREATE INDEX horsies_workflow_phase2_pending_failure_idx
        ON horsies_workflow_phase2_pending (last_failure_class)
        WHERE last_failure_class IS NOT NULL;
CREATE TABLE horsies_archive_access_gate (
    singleton boolean PRIMARY KEY,
    CHECK (singleton IS TRUE)
);
INSERT INTO horsies_archive_access_gate (singleton) VALUES (TRUE);
CREATE TABLE horsies_archive_maintenance_sessions (
    session_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);
CREATE FUNCTION horsies_assert_archive_available()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM singleton
    FROM horsies_archive_access_gate
    WHERE singleton IS TRUE
    FOR SHARE;
    IF EXISTS (
        SELECT 1
        FROM horsies_archive_maintenance_sessions
        WHERE ended_at IS NULL
    ) THEN
        RAISE EXCEPTION 'archive maintenance is active'
            USING ERRCODE = 'object_in_use';
    END IF;
END
$function$;
ALTER TABLE horsies_task_history
        ADD COLUMN attempt_archive_version smallint NOT NULL
            CHECK (attempt_archive_version > 0),
        ADD COLUMN attempt_snapshot_codec varchar(64) NOT NULL
            CHECK (octet_length(attempt_snapshot_codec) BETWEEN 1 AND 64),
        ADD COLUMN attempt_snapshot_content_type varchar(255) NOT NULL
            CHECK (
                octet_length(attempt_snapshot_content_type) BETWEEN 1 AND 255
            ),
        ADD COLUMN attempt_snapshot bytea NOT NULL,
        ADD COLUMN attempt_snapshot_digest bytea NOT NULL
            CHECK (octet_length(attempt_snapshot_digest) = 32);
ALTER TABLE horsies_task_history
        ADD COLUMN rerun_input_disposition varchar(32) NOT NULL
            CHECK (
                rerun_input_disposition IN (
                    'INLINE', 'REFERENCE', 'DECLINED_BY_POLICY',
                    'OVER_BOUND', 'NEVER_ELIGIBLE'
                )
            ),
        ADD COLUMN rerun_input_version smallint
            CHECK (rerun_input_version IS NULL OR rerun_input_version > 0),
        ADD COLUMN rerun_input_codec varchar(64)
            CHECK (
                rerun_input_codec IS NULL
                OR octet_length(rerun_input_codec) BETWEEN 1 AND 64
            ),
        ADD COLUMN rerun_input_content_type varchar(255)
            CHECK (
                rerun_input_content_type IS NULL
                OR octet_length(rerun_input_content_type) BETWEEN 1 AND 255
            ),
        ADD COLUMN rerun_input_digest bytea
            CHECK (
                rerun_input_digest IS NULL
                OR octet_length(rerun_input_digest) = 32
            ),
        ADD COLUMN rerun_input_inline bytea
            CHECK (
                rerun_input_inline IS NULL
                OR octet_length(rerun_input_inline) <= 65536
            ),
        ADD COLUMN rerun_input_reference varchar(2048)
            CHECK (
                rerun_input_reference IS NULL
                OR octet_length(rerun_input_reference) BETWEEN 1 AND 2048
            );
ALTER TABLE horsies_task_history
        ADD CONSTRAINT horsies_task_history_rerun_input_shape CHECK (
            (rerun_input_disposition = 'INLINE'
                AND rerun_input_version IS NOT NULL
                AND rerun_input_codec IS NOT NULL
                AND rerun_input_content_type IS NOT NULL
                AND rerun_input_digest IS NOT NULL
                AND rerun_input_inline IS NOT NULL
                AND rerun_input_reference IS NULL)
            OR (rerun_input_disposition = 'REFERENCE'
                AND rerun_input_version IS NOT NULL
                AND rerun_input_codec IS NOT NULL
                AND rerun_input_content_type IS NOT NULL
                AND rerun_input_digest IS NOT NULL
                AND rerun_input_inline IS NULL
                AND rerun_input_reference IS NOT NULL)
            OR (rerun_input_disposition IN (
                    'DECLINED_BY_POLICY', 'OVER_BOUND', 'NEVER_ELIGIBLE'
                )
                AND rerun_input_version IS NULL
                AND rerun_input_codec IS NULL
                AND rerun_input_content_type IS NULL
                AND rerun_input_digest IS NULL
                AND rerun_input_inline IS NULL
                AND rerun_input_reference IS NULL)
        ),
        ADD CONSTRAINT horsies_task_history_rerun_input_eligibility CHECK (
            (status <> 'COMPLETED' AND NOT is_workflow_task)
            OR rerun_input_disposition = 'NEVER_ELIGIBLE'
        );
