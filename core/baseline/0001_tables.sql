-- horsies baseline schema, part 1: tables, constraints, a sequence, seed rows, helper functions, triggers.
--
-- Part 2 (0002_indexes.sql) holds the indexes that are not created by a PRIMARY KEY or UNIQUE constraint here.
-- Part 3 (0003_interim_functions.sql) holds the SQL functions that the clients call, and their composite types.
--
-- The files of this folder are one migration, version 1000: the install concatenates them in name order and
-- computes one checksum over the result. They stay separate files for review.
--
-- Rules that hold for the whole file:
-- - No database defaults: every insert names every value.
-- - Objects are created in the current schema (no schema qualifier).
-- - The file is plain DDL with no transaction control. The install applies the migration in one transaction,
--   with its bookkeeping row.
-- - The published history readers are rendered at run time.

------------------------------------------------------------------------------------------------------------------------
-- Install bookkeeping
------------------------------------------------------------------------------------------------------------------------

-- Migration bookkeeping in the sqlx form. The baseline is migration version 1000.
-- installed_on has no default: the install names every value of its bookkeeping row.
CREATE TABLE horsies_migrations (
    version bigint NOT NULL,
    description text NOT NULL,
    installed_on timestamp with time zone NOT NULL,
    success boolean NOT NULL,
    checksum bytea NOT NULL,
    execution_time bigint NOT NULL,
    CONSTRAINT horsies_migrations_pkey PRIMARY KEY (version)
);

-- 0.x guard. A 0.x Python process reads COALESCE(MAX(version), 0) from
-- this table and stops with "database schema is newer than this build supports" when the value is above its own
-- (61 at 0.7.0). The table keeps nothing old working.
CREATE TABLE horsies_schema_version (
    version integer NOT NULL,
    applied_at timestamp with time zone NOT NULL,
    CONSTRAINT horsies_schema_version_pkey PRIMARY KEY (version)
);

INSERT INTO horsies_schema_version (version, applied_at)
VALUES (1000, statement_timestamp());

-- Client family record: the client library that installed the database. One row at most.
-- The install step writes the row; this file writes none.
CREATE TABLE horsies_client_family (
    only_row boolean NOT NULL,
    family text NOT NULL,
    recorded_at timestamp with time zone NOT NULL,
    CONSTRAINT horsies_client_family_pkey PRIMARY KEY (only_row),
    CONSTRAINT horsies_client_family_only_row CHECK (only_row),
    CONSTRAINT horsies_client_family_family_known CHECK (family IN ('PYTHON', 'RUST'))
);

------------------------------------------------------------------------------------------------------------------------
-- Live tasks
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE horsies_tasks (
    -- Identity and command, written once at enqueue.
    id uuid NOT NULL,
    task_name character varying(255) NOT NULL,
    queue_name character varying(100) NOT NULL,
    priority integer NOT NULL,
    input text NOT NULL,
    task_options text,
    execution text NOT NULL,
    retention_class_key character varying(64) NOT NULL,
    -- Every writer sets it: send = call time, schedule = slot time, workflow task = the workflow's start call time,
    -- rerun = rerun call time. The exact-id resend check compares it with no NULL arm.
    sent_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    -- Fingerprints and keys.
    command_fingerprint_version smallint,
    command_fingerprint bytea,
    idempotency_key_digest bytea,
    -- Lifecycle.
    status character varying(9) NOT NULL,
    enqueued_at timestamp with time zone NOT NULL,
    claimed_at timestamp with time zone,
    claimed_by_worker_id character varying(255),
    claim_expires_at timestamp with time zone,
    started_at timestamp with time zone,
    finalizing_at timestamp with time zone,
    finalizing_by_worker_id text,
    worker_pid integer,
    worker_hostname character varying(255),
    worker_process_name character varying(255),
    retry_count integer NOT NULL,
    max_retries integer NOT NULL,
    next_retry_at timestamp with time zone,
    good_until timestamp with time zone,
    is_workflow_task boolean NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    -- Rerun.
    retain_rerun_input boolean NOT NULL,
    rerun_of_task_id uuid,
    rerun_root_task_id uuid,
    -- Trace, written once at enqueue: the carrier of the producer's propagator as the core's canonical JSON text, and
    -- the 16-byte trace id of the producer span. Both NULL when the send carried none, or when the carrier broke a
    -- bound of the core's check. A schedule fire writes none; a workflow task copies its workflow's pair. Not part of
    -- the command fingerprint: a send that answers from an existing row keeps the pair of that row.
    trace_carrier text,
    trace_id bytea,
    CONSTRAINT horsies_tasks_pkey PRIMARY KEY (id),
    CONSTRAINT horsies_tasks_status_live CHECK (
        status IN ('PENDING', 'CLAIMED', 'RUNNING')
        AND (status = 'CLAIMED') = (claim_expires_at IS NOT NULL)
    ),
    CONSTRAINT horsies_tasks_execution_known CHECK (
        execution IN ('ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD')
    ),
    -- The channel task_queue_<name> must stay within PostgreSQL's 63-byte limit.
    CONSTRAINT horsies_tasks_queue_name_length CHECK (octet_length(queue_name) <= 52),
    CONSTRAINT horsies_tasks_priority_range CHECK (priority BETWEEN 1 AND 100),
    CONSTRAINT horsies_tasks_retry_range CHECK (retry_count >= 0 AND max_retries BETWEEN 0 AND 20),
    -- forever, or standard_<days>d with days exactly 1..1825 and no leading zero, as
    -- horsies_retention_classes_kind_shape. The same form is in three more CHECKs below.
    CONSTRAINT horsies_tasks_retention_class_key_form CHECK (
        retention_class_key = 'forever'
        OR retention_class_key ~ '^standard_([1-9][0-9]{0,2}|1[0-7][0-9]{2}|18[01][0-9]|182[0-5])d$'
    ),
    -- Each arm names IS NOT NULL: without it a NULL makes the arm NULL and the CHECK passes.
    CONSTRAINT horsies_tasks_command_fingerprint_by_origin CHECK (
        (
            is_workflow_task
            AND command_fingerprint IS NULL
            AND command_fingerprint_version IS NULL
        )
        OR (
            NOT is_workflow_task
            AND command_fingerprint IS NOT NULL
            AND command_fingerprint_version IS NOT NULL
            AND octet_length(command_fingerprint) = 32
            AND command_fingerprint_version > 0
        )
    ),
    CONSTRAINT horsies_tasks_idempotency_key_digest_length CHECK (
        idempotency_key_digest IS NULL OR octet_length(idempotency_key_digest) = 32
    ),
    CONSTRAINT horsies_tasks_rerun_lineage_pair CHECK (
        (rerun_of_task_id IS NULL AND rerun_root_task_id IS NULL)
        OR (rerun_of_task_id IS NOT NULL AND rerun_root_task_id IS NOT NULL)
    ),
    CONSTRAINT horsies_tasks_trace_pair CHECK (
        (trace_carrier IS NULL AND trace_id IS NULL)
        OR (trace_carrier IS NOT NULL AND trace_id IS NOT NULL AND octet_length(trace_id) = 16)
    )
);

------------------------------------------------------------------------------------------------------------------------
-- Attempts, heartbeats, worker states
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE horsies_task_attempts (
    task_id uuid NOT NULL,
    attempt integer NOT NULL,
    outcome character varying(14) NOT NULL,
    will_retry boolean NOT NULL,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone NOT NULL,
    error_code text,
    error_message text,
    failed_reason text,
    worker_id character varying(255) NOT NULL,
    worker_hostname character varying(255) NOT NULL,
    worker_pid integer NOT NULL,
    worker_process_name character varying(255) NOT NULL,
    CONSTRAINT horsies_task_attempts_pkey PRIMARY KEY (task_id, attempt),
    CONSTRAINT horsies_task_attempts_outcome_known CHECK (
        outcome IN ('FAILED', 'WORKER_FAILURE') OR (outcome = 'COMPLETED' AND NOT will_retry)
    ),
    CONSTRAINT horsies_task_attempts_attempt_range CHECK (attempt BETWEEN 1 AND 21),
    -- NO ACTION: only the move deletes a live task row, and it deletes the attempt rows first. A writer that
    -- forgot that delete fails loud instead of losing attempts in silence.
    CONSTRAINT horsies_task_attempts_task_fkey FOREIGN KEY (task_id) REFERENCES horsies_tasks (id)
);

-- Partitioned by hour at run time. The partition manager creates each hourly leaf, its probe index
-- (task_id, role, sent_at DESC) and its catalog row. No default partition: a beat whose hour has no leaf fails.
CREATE TABLE horsies_heartbeats (
    task_id uuid NOT NULL,
    role character varying(7) NOT NULL,
    sent_at timestamp with time zone NOT NULL,
    sender_id character varying(255) NOT NULL,
    hostname character varying(255) NOT NULL,
    pid integer NOT NULL,
    CONSTRAINT horsies_heartbeats_role_known CHECK (role IN ('claimer', 'runner'))
)
PARTITION BY RANGE (sent_at);

-- No primary key constraint: the unique index horsies_worker_states_identity (0002_indexes.sql) is the identity.
CREATE TABLE horsies_worker_states (
    worker_id character varying(255) NOT NULL,
    snapshot_at timestamp with time zone NOT NULL,
    hostname character varying(255) NOT NULL,
    pid integer NOT NULL,
    processes integer NOT NULL,
    max_claim_batch integer NOT NULL,
    max_claim_per_worker integer NOT NULL,
    cluster_wide_cap integer,
    queues character varying(100)[] NOT NULL,
    queue_priorities jsonb NOT NULL,
    queue_max_concurrency jsonb NOT NULL,
    recovery_config jsonb NOT NULL,
    tasks_running integer NOT NULL,
    tasks_claimed integer NOT NULL,
    memory_usage_mb double precision,
    memory_percent double precision,
    cpu_percent double precision,
    children_memory_mb double precision,
    worker_started_at timestamp with time zone NOT NULL,
    slots_down jsonb NOT NULL,
    class_capacity jsonb NOT NULL,
    -- The telemetry of the worker: {"endpoint": {"state": "off" | "binding" | "serving" | "down", ...}}, the state of
    -- its metrics endpoint (core/src/worker/metrics_endpoint.rs, telemetry_json).
    telemetry jsonb NOT NULL,
    CONSTRAINT horsies_worker_states_counts_valid CHECK (
        processes >= 0
        AND max_claim_batch >= 0
        AND max_claim_per_worker >= 0
        AND tasks_running >= 0
        AND tasks_claimed >= 0
        AND (cluster_wide_cap IS NULL OR cluster_wide_cap > 0)
    )
);

-- The generation of the latest acquisition of each worker's liveness lock: one row per worker id, written by
-- horsies_take_worker_liveness in the transaction of each acquisition. A reaper judges an owner gone only when its
-- reads see one generation (horsies_unheld_worker_owners, horsies_worker_liveness_generations).
-- horsies_forget_ended_worker_liveness deletes the rows of ended workers.
CREATE TABLE horsies_worker_liveness (
    worker_id text NOT NULL,
    generation bigint NOT NULL,
    CONSTRAINT horsies_worker_liveness_pkey PRIMARY KEY (worker_id)
);

-- The generations of horsies_worker_liveness. Each acquisition takes a new value, and no value is used twice, so a
-- row that is deleted and written again never shows a generation that a reaper read before.
CREATE SEQUENCE horsies_worker_liveness_generation AS bigint NO CYCLE;

------------------------------------------------------------------------------------------------------------------------
-- Workflows
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE horsies_workflows (
    id uuid NOT NULL,
    name character varying(255) NOT NULL,
    definition_key character varying(255),
    status character varying(9) NOT NULL,
    on_error character varying(5) NOT NULL,
    output_task_index integer,
    success_policy jsonb,
    result text,
    error text,
    parent_workflow_id uuid,
    parent_task_index integer,
    root_workflow_id uuid,
    depth integer NOT NULL,
    sent_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    updated_at timestamp with time zone NOT NULL,
    -- Trace, written once at the start: the carrier and the trace id of the start call's span (the forms of
    -- horsies_tasks.trace_carrier and trace_id). Every workflow row of a started tree holds the pair of the start; a
    -- deferred child start copies its parent's pair; the task rows of the nodes copy the pair of their workflow.
    trace_carrier text,
    trace_id bytea,
    CONSTRAINT horsies_workflows_pkey PRIMARY KEY (id),
    CONSTRAINT horsies_workflows_status_known CHECK (
        status IN ('PENDING', 'RUNNING', 'PAUSED', 'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ),
    CONSTRAINT horsies_workflows_on_error_known CHECK (on_error IN ('fail', 'pause')),
    CONSTRAINT horsies_workflows_depth_range CHECK (depth BETWEEN 0 AND 32),
    CONSTRAINT horsies_workflows_parent_pair CHECK (
        (parent_workflow_id IS NULL) = (parent_task_index IS NULL)
    ),
    CONSTRAINT horsies_workflows_root_depth CHECK ((depth = 0) = (parent_workflow_id IS NULL)),
    CONSTRAINT horsies_workflows_trace_pair CHECK (
        (trace_carrier IS NULL AND trace_id IS NULL)
        OR (trace_carrier IS NOT NULL AND trace_id IS NOT NULL AND octet_length(trace_id) = 16)
    ),
    CONSTRAINT horsies_workflows_parent_workflow_id_fkey FOREIGN KEY (parent_workflow_id)
        REFERENCES horsies_workflows (id) ON DELETE CASCADE
);

CREATE TABLE horsies_workflow_tasks (
    id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    task_index integer NOT NULL,
    node_id character varying(128),
    task_name character varying(255) NOT NULL,
    input text NOT NULL,
    queue_name character varying(100) NOT NULL,
    priority integer NOT NULL,
    dependencies integer[] NOT NULL,
    args_from jsonb,
    workflow_ctx_from character varying(128)[],
    allow_failed_deps boolean NOT NULL,
    join_type character varying(6) NOT NULL,
    min_success integer,
    task_options text,
    -- Typed facts parsed at workflow start, so SQL can mint the task row of a task node. Set exactly on task
    -- nodes; see horsies_workflow_tasks_task_facts_by_kind. A task row
    -- that SQL mints takes max_retries, execution, good_until and retention_class_key from its node.
    max_retries integer,
    execution text,
    good_until timestamp with time zone,
    retention_class_key character varying(64),
    -- Typed fact of the static graph, written at workflow start: REACHABLE when a sub-workflow node is reachable
    -- through the dependency edges below this node (the node is an ancestor of one), NONE otherwise. Phase 1
    -- returns it, and phase 2 of a REACHABLE node runs in a transaction, because its promotion can owe a child
    -- start that must commit with the step. Conservative: a REACHABLE node can still promote no sub-workflow node.
    downstream_subworkflow character varying(9) NOT NULL,
    status character varying(9) NOT NULL,
    task_id uuid,
    is_subworkflow boolean NOT NULL,
    sub_workflow_id uuid,
    sub_workflow_name character varying(255),
    sub_definition_key character varying(255),
    -- The client's choice for a sub-workflow node's params: GIVEN when the node definition carries params for
    -- the child's build, NOT_GIVEN when it carries none. Set exactly on sub-workflow nodes; see
    -- horsies_workflow_tasks_sub_workflow_params_by_kind.
    sub_workflow_params character varying(9),
    sub_workflow_summary text,
    result text,
    error text,
    created_at timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    CONSTRAINT horsies_workflow_tasks_pkey PRIMARY KEY (id),
    CONSTRAINT horsies_workflow_tasks_node_workflow_key UNIQUE (id, workflow_id),
    CONSTRAINT uq_horsies_workflow_task_index UNIQUE (workflow_id, task_index),
    CONSTRAINT horsies_workflow_tasks_status_known CHECK (
        status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED')
    ),
    CONSTRAINT horsies_workflow_tasks_queue_name_length CHECK (octet_length(queue_name) <= 52),
    CONSTRAINT horsies_workflow_tasks_join_known CHECK (join_type IN ('all', 'any', 'quorum')),
    CONSTRAINT horsies_workflow_tasks_quorum_pair CHECK (
        (join_type = 'quorum' AND min_success IS NOT NULL AND min_success >= 1)
        OR (join_type <> 'quorum' AND min_success IS NULL)
    ),
    CONSTRAINT horsies_workflow_tasks_priority_range CHECK (priority BETWEEN 1 AND 100),
    CONSTRAINT horsies_workflow_tasks_task_node_fields CHECK (
        is_subworkflow
        OR (
            sub_workflow_id IS NULL
            AND sub_workflow_name IS NULL
            AND sub_definition_key IS NULL
            AND sub_workflow_summary IS NULL
        )
    ),
    CONSTRAINT horsies_workflow_tasks_subworkflow_fields CHECK (NOT is_subworkflow OR task_id IS NULL),
    CONSTRAINT horsies_workflow_tasks_downstream_subworkflow_known CHECK (
        downstream_subworkflow IN ('NONE', 'REACHABLE')
    ),
    CONSTRAINT horsies_workflow_tasks_sub_workflow_params_by_kind CHECK (
        (
            is_subworkflow
            AND sub_workflow_params IS NOT NULL
            AND sub_workflow_params IN ('NOT_GIVEN', 'GIVEN')
        )
        OR (
            NOT is_subworkflow
            AND sub_workflow_params IS NULL
        )
    ),
    -- The bounds are those of horsies_tasks.
    CONSTRAINT horsies_workflow_tasks_task_facts_by_kind CHECK (
        (
            NOT is_subworkflow
            AND max_retries IS NOT NULL
            AND max_retries BETWEEN 0 AND 20
            AND execution IS NOT NULL
            AND execution IN ('ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD')
            AND retention_class_key IS NOT NULL
            AND (
                retention_class_key = 'forever'
                OR retention_class_key ~ '^standard_([1-9][0-9]{0,2}|1[0-7][0-9]{2}|18[01][0-9]|182[0-5])d$'
            )
        )
        OR (
            is_subworkflow
            AND max_retries IS NULL
            AND execution IS NULL
            AND good_until IS NULL
            AND retention_class_key IS NULL
        )
    ),
    CONSTRAINT horsies_workflow_tasks_workflow_id_fkey FOREIGN KEY (workflow_id)
        REFERENCES horsies_workflows (id) ON DELETE CASCADE,
    CONSTRAINT horsies_workflow_tasks_sub_workflow_id_fkey FOREIGN KEY (sub_workflow_id)
        REFERENCES horsies_workflows (id) ON DELETE SET NULL
);

------------------------------------------------------------------------------------------------------------------------
-- Recovery and phase 2
------------------------------------------------------------------------------------------------------------------------

-- Created before the pending table, which refers to it.
CREATE TABLE horsies_workflow_phase2_quarantine (
    task_id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    workflow_node_row_id uuid NOT NULL,
    terminalization_kind character varying(16) NOT NULL,
    terminal_at timestamp with time zone NOT NULL,
    retention_class_key character varying(64) NOT NULL,
    history_schema_version smallint NOT NULL,
    result_envelope_version smallint NOT NULL,
    result_payload bytea NOT NULL,
    result_digest bytea NOT NULL,
    quarantined_at timestamp with time zone NOT NULL,
    CONSTRAINT horsies_workflow_phase2_quarantine_pkey PRIMARY KEY (task_id),
    CONSTRAINT horsies_workflow_phase2_quarantine_kind_known CHECK (
        terminalization_kind IN (
            'COMPLETE_LOCKED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING'
        )
    ),
    CONSTRAINT horsies_workflow_phase2_quarantine_class_form CHECK (
        retention_class_key = 'forever'
        OR retention_class_key ~ '^standard_([1-9][0-9]{0,2}|1[0-7][0-9]{2}|18[01][0-9]|182[0-5])d$'
    ),
    CONSTRAINT horsies_workflow_phase2_quarantine_versions_positive CHECK (
        history_schema_version > 0 AND result_envelope_version > 0
    ),
    CONSTRAINT horsies_workflow_phase2_quarantine_digest_matches CHECK (
        result_digest = sha256(result_payload)
    ),
    CONSTRAINT horsies_workflow_phase2_quarantine_node_fkey FOREIGN KEY (workflow_node_row_id, workflow_id)
        REFERENCES horsies_workflow_tasks (id, workflow_id) ON DELETE CASCADE
);

CREATE TABLE horsies_workflow_phase2_pending (
    task_id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    workflow_node_row_id uuid NOT NULL,
    terminalization_kind character varying(16) NOT NULL,
    recovery_source character varying(10) NOT NULL,
    history_class character varying(64),
    history_anchor timestamp with time zone,
    history_schema_version smallint NOT NULL,
    result_digest bytea NOT NULL,
    quarantine_task_id uuid,
    created_at timestamp with time zone NOT NULL,
    attempt_count integer NOT NULL,
    last_attempt_at timestamp with time zone,
    last_failure_class character varying(24),
    CONSTRAINT horsies_workflow_phase2_pending_pkey PRIMARY KEY (task_id),
    CONSTRAINT horsies_workflow_phase2_pending_kind_known CHECK (
        terminalization_kind IN (
            'COMPLETE_LOCKED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING'
        )
    ),
    -- The two arms also close the set of recovery_source values (no separate CHECK). Each arm names IS NOT NULL.
    CONSTRAINT horsies_workflow_phase2_pending_locator CHECK (
        (
            recovery_source = 'HISTORY'
            AND history_class IS NOT NULL
            AND history_anchor IS NOT NULL
            AND quarantine_task_id IS NULL
        )
        OR (
            recovery_source = 'QUARANTINE'
            AND history_class IS NULL
            AND history_anchor IS NULL
            AND quarantine_task_id IS NOT NULL
            AND quarantine_task_id = task_id
        )
    ),
    CONSTRAINT horsies_workflow_phase2_pending_history_class_form CHECK (
        history_class IS NULL
        OR history_class = 'forever'
        OR history_class ~ '^standard_([1-9][0-9]{0,2}|1[0-7][0-9]{2}|18[01][0-9]|182[0-5])d$'
    ),
    CONSTRAINT horsies_workflow_phase2_pending_history_version_positive CHECK (history_schema_version > 0),
    CONSTRAINT horsies_workflow_phase2_pending_digest_length CHECK (octet_length(result_digest) = 32),
    CONSTRAINT horsies_workflow_phase2_pending_attempts CHECK (
        attempt_count BETWEEN 0 AND 1000
        AND (attempt_count = 0) = (last_attempt_at IS NULL)
        AND (attempt_count = 0) = (last_failure_class IS NULL)
    ),
    CONSTRAINT horsies_workflow_phase2_pending_failure_known CHECK (
        last_failure_class IS NULL
        OR last_failure_class IN (
            'SOURCE_STATE_CONFLICT', 'SOURCE_ABSENT', 'SOURCE_VERSION_CONFLICT', 'SOURCE_DIGEST_MISMATCH', 'RAISED'
        )
    ),
    CONSTRAINT horsies_workflow_phase2_pending_node_fkey FOREIGN KEY (workflow_node_row_id, workflow_id)
        REFERENCES horsies_workflow_tasks (id, workflow_id) ON DELETE CASCADE,
    CONSTRAINT horsies_workflow_phase2_pending_quarantine_fkey FOREIGN KEY (quarantine_task_id)
        REFERENCES horsies_workflow_phase2_quarantine (task_id)
);

CREATE TABLE horsies_workflow_recovery_failures (
    workflow_id uuid NOT NULL,
    failed_passes integer NOT NULL,
    last_pass_id uuid NOT NULL,
    first_failed_at timestamp with time zone NOT NULL,
    last_failed_at timestamp with time zone NOT NULL,
    last_case character varying(40) NOT NULL,
    last_task_index integer,
    last_error_type character varying(255) NOT NULL,
    CONSTRAINT horsies_workflow_recovery_failures_pkey PRIMARY KEY (workflow_id),
    CONSTRAINT horsies_workflow_recovery_failures_passes_positive CHECK (failed_passes >= 1),
    CONSTRAINT horsies_workflow_recovery_failures_time_order CHECK (first_failed_at <= last_failed_at),
    CONSTRAINT horsies_workflow_recovery_failures_case_known CHECK (
        last_case IN (
            'pending_terminal_deps',
            'ready_not_enqueued',
            'ready_subworkflow_not_started',
            'completed_child_parent_not_updated',
            'terminal_workflow_running'
        )
    ),
    CONSTRAINT horsies_workflow_recovery_failures_case_node CHECK (
        (last_case = 'terminal_workflow_running') = (last_task_index IS NULL)
        AND (last_task_index IS NULL OR last_task_index >= 0)
    ),
    CONSTRAINT horsies_workflow_recovery_failures_workflow_fkey FOREIGN KEY (workflow_id)
        REFERENCES horsies_workflows (id) ON DELETE CASCADE
);

CREATE TABLE horsies_recovery_scan_cursors (
    scan_name character varying(32) NOT NULL,
    last_created_at timestamp with time zone,
    last_id uuid,
    cycle_upper_created_at timestamp with time zone,
    cycle_upper_id uuid,
    claim_token uuid,
    claim_expires_at timestamp with time zone,
    completed_cycles bigint NOT NULL,
    last_scan_rows integer NOT NULL,
    last_candidate_rows integer NOT NULL,
    last_scan_at timestamp with time zone,
    CONSTRAINT horsies_recovery_scan_cursors_pkey PRIMARY KEY (scan_name),
    CONSTRAINT horsies_recovery_cursor_scan_known CHECK (
        scan_name IN ('running_workflows', 'orphan_workflow_tasks')
    ),
    CONSTRAINT horsies_recovery_cursor_last_pair CHECK ((last_created_at IS NULL) = (last_id IS NULL)),
    CONSTRAINT horsies_recovery_cursor_upper_pair CHECK (
        (cycle_upper_created_at IS NULL) = (cycle_upper_id IS NULL)
    ),
    CONSTRAINT horsies_recovery_cursor_last_has_upper CHECK (last_id IS NULL OR cycle_upper_id IS NOT NULL),
    CONSTRAINT horsies_recovery_cursor_claim_pair CHECK ((claim_token IS NULL) = (claim_expires_at IS NULL)),
    CONSTRAINT horsies_recovery_cursor_claim_on_audit_only CHECK (
        claim_token IS NULL OR scan_name = 'running_workflows'
    ),
    CONSTRAINT horsies_recovery_cursor_counters CHECK (
        completed_cycles >= 0
        AND last_scan_rows BETWEEN 0 AND 1000
        AND last_candidate_rows BETWEEN 0 AND last_scan_rows
    ),
    CONSTRAINT horsies_recovery_cursor_never_scanned CHECK (
        last_scan_at IS NOT NULL
        OR (
            completed_cycles = 0
            AND last_scan_rows = 0
            AND last_candidate_rows = 0
            AND last_id IS NULL
            AND cycle_upper_id IS NULL
        )
    )
);

-- The two cursor rows. No other statement inserts or deletes a cursor row.
INSERT INTO horsies_recovery_scan_cursors (
    scan_name, last_created_at, last_id, cycle_upper_created_at, cycle_upper_id, claim_token,
    claim_expires_at, completed_cycles, last_scan_rows, last_candidate_rows, last_scan_at
)
VALUES
    ('running_workflows', NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, NULL),
    ('orphan_workflow_tasks', NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, NULL);

------------------------------------------------------------------------------------------------------------------------
-- Task history and retention
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE horsies_retention_classes (
    class_key character varying(64) NOT NULL,
    class_kind character varying(9) NOT NULL,
    retention_days smallint,
    heartbeat_horizon_hours smallint,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT horsies_retention_classes_pkey PRIMARY KEY (class_key),
    -- Each arm names IS NOT NULL: without it a PERIOD row with a NULL period would make its arm NULL and pass.
    CONSTRAINT horsies_retention_classes_kind_shape CHECK (
        (
            class_kind = 'FOREVER'
            AND class_key = 'forever'
            AND retention_days IS NULL
            AND heartbeat_horizon_hours IS NULL
        )
        OR (
            class_kind = 'PERIOD'
            AND retention_days IS NOT NULL
            AND retention_days BETWEEN 1 AND 1825
            AND class_key = 'standard_' || retention_days || 'd'
            AND heartbeat_horizon_hours IS NULL
        )
        OR (
            class_kind = 'HEARTBEAT'
            AND class_key = 'heartbeats'
            AND heartbeat_horizon_hours IS NOT NULL
            AND heartbeat_horizon_hours BETWEEN 2 AND 48
            AND retention_days IS NULL
        )
    )
);

-- The forever class. heartbeats and standard_30d come from the first coverage pass.
INSERT INTO horsies_retention_classes (class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at)
VALUES ('forever', 'FOREVER', NULL, NULL, statement_timestamp());

-- History parent. No primary key and no unique key on task_id: a unique index on a partitioned table must contain
-- the partition keys. The duplicate identity guard holds uniqueness at the move. One single-column CHECK, on the
-- terminalization kind, so no writer can store a kind outside the vocabulary; no other: the move functions are the
-- only writers, and each CHECK costs every history insert.
CREATE TABLE horsies_task_history (
    -- Identity and command, copied from the locked live row.
    task_id uuid NOT NULL,
    task_name character varying(255) NOT NULL,
    queue_name character varying(100) NOT NULL,
    priority integer NOT NULL,
    command_fingerprint_version smallint,
    command_fingerprint bytea,
    retention_class_key character varying(64) NOT NULL,
    is_workflow_task boolean NOT NULL,
    workflow_id uuid,
    rerun_of_task_id uuid,
    rerun_root_task_id uuid,
    -- Times. sent_at is copied from the live row, where it is NOT NULL.
    sent_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    enqueued_at timestamp with time zone NOT NULL,
    claimed_at timestamp with time zone,
    started_at timestamp with time zone,
    good_until timestamp with time zone,
    terminal_at timestamp with time zone NOT NULL,
    -- How the task ended.
    status character varying(9) NOT NULL,
    terminalization_kind character varying(32) NOT NULL,
    retry_count integer NOT NULL,
    max_retries integer NOT NULL,
    last_claimed_worker_id character varying(255),
    last_worker_hostname character varying(255),
    last_worker_pid integer,
    last_worker_process_name character varying(255),
    error_code text,
    final_failed_reason text,
    -- Result envelope (format version 1 implies json-utf8 and application/json).
    result_envelope_version smallint NOT NULL,
    result_payload bytea,
    result_digest bytea,
    -- Attempt archive.
    history_schema_version smallint NOT NULL,
    attempt_archive_version smallint NOT NULL,
    attempt_snapshot bytea NOT NULL,
    attempt_snapshot_digest bytea NOT NULL,
    -- Rerun input, built at the move.
    rerun_input_disposition character varying(32) NOT NULL,
    rerun_input_version smallint,
    rerun_input_digest bytea,
    rerun_input_inline bytea,
    -- Trace id of the producer span, copied from the live row (NULL when the live row had none). The carrier is not
    -- kept: no attempt runs after the move.
    trace_id bytea,
    CONSTRAINT horsies_task_history_result_domain CHECK (
        terminalization_kind <> 'CANCEL_ADMIN' OR (result_payload IS NULL AND result_digest IS NULL)
    ),
    -- The 15 terminalization kinds. Every leaf is a PARTITION OF this table and inherits the CHECK.
    CONSTRAINT horsies_task_history_kind_known CHECK (
        terminalization_kind IN (
            'COMPLETE_LOCKED', 'COMPLETE_FUSED', 'FAIL_RUNNING', 'FAIL_STALE',
            'EXPIRE_CLAIMED', 'EXPIRE_PENDING', 'CANCEL_ADMIN', 'CANCEL_ORPHAN',
            'CANCEL_ORPHAN_SWEEP', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH',
            'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH',
            'WORKFLOW_CANCEL_WORKFLOW'
        )
    ),
    CONSTRAINT horsies_task_history_rerun_input_shape CHECK (
        (
            rerun_input_disposition = 'INLINE'
            AND status <> 'COMPLETED'
            AND NOT is_workflow_task
            AND rerun_input_version IS NOT NULL
            AND rerun_input_digest IS NOT NULL
            AND rerun_input_inline IS NOT NULL
        )
        OR (
            rerun_input_disposition IN ('OVER_BOUND', 'DECLINED_BY_POLICY')
            AND status <> 'COMPLETED'
            AND NOT is_workflow_task
            AND rerun_input_version IS NULL
            AND rerun_input_digest IS NULL
            AND rerun_input_inline IS NULL
        )
        OR (
            rerun_input_disposition = 'NEVER_ELIGIBLE'
            AND rerun_input_version IS NULL
            AND rerun_input_digest IS NULL
            AND rerun_input_inline IS NULL
        )
    )
)
PARTITION BY LIST (retention_class_key);

-- The forever class parent. The RANGE level stays although one leaf covers every time.
CREATE TABLE horsies_task_history_forever PARTITION OF horsies_task_history
    FOR VALUES IN ('forever')
    PARTITION BY RANGE (terminal_at);

-- The one static forever leaf. Its two indexes are in 0002_indexes.sql.
CREATE TABLE horsies_task_history_forever_open PARTITION OF horsies_task_history_forever
    FOR VALUES FROM (MINVALUE) TO ('9999-01-01 00:00:00+00');

-- The lower bound is typed: OPEN (the partition starts at MINVALUE, no anchor) or AT (lower_anchor holds it).
-- An open bound is never stored as a sentinel time.
CREATE TABLE horsies_task_history_leaf_catalog (
    leaf_name text NOT NULL,
    class_key character varying(64) NOT NULL,
    lower_bound_kind character varying(4) NOT NULL,
    lower_anchor timestamp with time zone,
    upper_anchor timestamp with time zone NOT NULL,
    index_schema_version smallint NOT NULL,
    created_at timestamp with time zone NOT NULL,
    detached_at timestamp with time zone,
    dropped_at timestamp with time zone,
    CONSTRAINT horsies_task_history_leaf_catalog_pkey PRIMARY KEY (leaf_name),
    CONSTRAINT horsies_task_history_leaf_catalog_interval_unique UNIQUE (class_key, lower_anchor, upper_anchor),
    -- Each arm names IS NULL or IS NOT NULL: the anchor is set exactly when the kind is AT.
    CONSTRAINT horsies_task_history_leaf_catalog_lower_bound_pair CHECK (
        (lower_bound_kind = 'OPEN' AND lower_anchor IS NULL)
        OR (lower_bound_kind = 'AT' AND lower_anchor IS NOT NULL)
    ),
    CONSTRAINT horsies_task_history_leaf_catalog_bounds_ordered CHECK (
        lower_anchor IS NULL OR lower_anchor < upper_anchor
    ),
    CONSTRAINT horsies_task_history_leaf_catalog_drop_after_detach CHECK (
        dropped_at IS NULL OR detached_at IS NOT NULL
    ),
    CONSTRAINT horsies_task_history_leaf_catalog_class_fkey FOREIGN KEY (class_key)
        REFERENCES horsies_retention_classes (class_key)
);

-- Catalog row of the forever leaf. index_schema_version 1 is the leaf index shape of 0002_indexes.sql, the version that
-- horsies_create_partition_leaf writes and horsies_history_leaf_state checks.
INSERT INTO horsies_task_history_leaf_catalog (
    leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, index_schema_version, created_at,
    detached_at, dropped_at
)
VALUES (
    'horsies_task_history_forever_open',
    'forever',
    'OPEN',
    NULL,
    '9999-01-01 00:00:00+00',
    1,
    statement_timestamp(),
    NULL,
    NULL
);

-- The lower bound is typed as in the leaf catalog.
CREATE TABLE horsies_task_lookup_manifest (
    leaf_name text NOT NULL,
    probe_position integer NOT NULL,
    lower_bound_kind character varying(4) NOT NULL,
    lower_anchor timestamp with time zone,
    upper_anchor timestamp with time zone NOT NULL,
    published_at timestamp with time zone NOT NULL,
    CONSTRAINT horsies_task_lookup_manifest_pkey PRIMARY KEY (leaf_name),
    CONSTRAINT horsies_task_lookup_manifest_position_unique UNIQUE (probe_position),
    CONSTRAINT horsies_task_lookup_manifest_position_valid CHECK (probe_position >= 0),
    CONSTRAINT horsies_task_lookup_manifest_lower_bound_pair CHECK (
        (lower_bound_kind = 'OPEN' AND lower_anchor IS NULL)
        OR (lower_bound_kind = 'AT' AND lower_anchor IS NOT NULL)
    ),
    CONSTRAINT horsies_task_lookup_manifest_bounds_ordered CHECK (
        lower_anchor IS NULL OR lower_anchor < upper_anchor
    )
);

-- The single-column length and positivity checks of v61 live in horsies_key_reservation_claim, the only inserter.
CREATE TABLE horsies_key_reservations (
    idempotency_key_digest bytea NOT NULL,
    key_scope_version smallint NOT NULL,
    fingerprint_version smallint NOT NULL,
    command_fingerprint bytea NOT NULL,
    task_id uuid NOT NULL,
    disposition character varying(8) NOT NULL,
    reservation_window interval NOT NULL,
    expires_at timestamp with time zone,
    CONSTRAINT horsies_key_reservations_pkey PRIMARY KEY (idempotency_key_digest),
    CONSTRAINT horsies_key_reservations_disposition_shape CHECK (
        (disposition = 'LIVE' AND expires_at IS NULL)
        OR (disposition = 'TERMINAL' AND expires_at IS NOT NULL)
    ),
    CONSTRAINT horsies_key_reservations_window_range CHECK (
        reservation_window > interval '0' AND reservation_window <= interval '30 days'
    )
);

CREATE TABLE horsies_task_history_clock_exceptions (
    task_id uuid NOT NULL,
    retention_class_key character varying(64) NOT NULL,
    terminal_at timestamp with time zone NOT NULL,
    CONSTRAINT horsies_task_history_clock_exceptions_pkey PRIMARY KEY (task_id)
);

------------------------------------------------------------------------------------------------------------------------
-- Scheduler
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE horsies_schedule_state (
    schedule_name character varying(255) NOT NULL,
    next_run_at timestamp with time zone NOT NULL,
    last_run_at timestamp with time zone,
    last_task_id uuid,
    run_count integer NOT NULL,
    config_hash bytea NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT horsies_schedule_state_pkey PRIMARY KEY (schedule_name),
    CONSTRAINT horsies_schedule_state_run_count_nonnegative CHECK (run_count >= 0),
    CONSTRAINT horsies_schedule_state_last_run_fields CHECK (
        (last_run_at IS NULL) = (last_task_id IS NULL)
        AND (last_run_at IS NULL) = (run_count = 0)
    ),
    CONSTRAINT horsies_schedule_state_config_hash_length CHECK (octet_length(config_hash) = 32)
);

------------------------------------------------------------------------------------------------------------------------
-- Helper function that the workflow tables need
------------------------------------------------------------------------------------------------------------------------

-- A UUID version 7 from the database clock, laid out as PostgreSQL 18's built-in uuidv7():
-- - 48 bits: Unix time in milliseconds (clock_timestamp());
-- - 4 bits: version 7;
-- - 12 bits (rand_a): the sub-millisecond fraction, floor(microseconds within the millisecond * 4096 / 1000);
-- - 2 bits: variant 10, and 62 random bits from gen_random_uuid() (PostgreSQL 13 and later).
-- Inside one transaction the ids strictly increase in mint order: when the clock gives a stamp (milliseconds and
-- fraction, 60 bits) that is not above the last stamp of the transaction, the stamp is the last one plus 1. The last
-- stamp is a transaction-local setting, so no state outlives the transaction (safe under PgBouncer transaction
-- mode). Across transactions the order follows the database clock at microsecond resolution.
CREATE FUNCTION horsies_uuidv7() RETURNS uuid
    LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_now_us bigint := floor(extract(epoch FROM clock_timestamp()) * 1000000)::bigint;
    v_stamp bigint := ((v_now_us / 1000) << 12) | (((v_now_us % 1000) * 4096) / 1000);
    v_last text := current_setting('horsies.uuidv7_last_stamp', true);
BEGIN
    IF v_last IS NOT NULL AND v_last <> '' AND v_stamp <= v_last::bigint THEN
        v_stamp := v_last::bigint + 1;
    END IF;
    PERFORM set_config('horsies.uuidv7_last_stamp', v_stamp::text, true);
    RETURN encode(
        overlay(
            overlay(
                uuid_send(gen_random_uuid())
                PLACING substring(int8send(v_stamp >> 12) FROM 3)
                FROM 1 FOR 6
            )
            PLACING int2send((28672 + (v_stamp & 4095))::smallint)
            FROM 7 FOR 2
        ),
        'hex'
    )::uuid;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Notification triggers
------------------------------------------------------------------------------------------------------------------------

-- One function for every task insert. Order as in v61, where the queue trigger fired before the status trigger:
-- task_new (id, for outside observers) and task_queue_<queue> (constant payload 'new', one wake per kind) only for
-- a PENDING row, as v61's WHEN (NEW.status = 'PENDING'); horsies_task_status (id, for the web app) for every row.
CREATE FUNCTION horsies_notify_task_changes() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'PENDING' THEN
        PERFORM pg_notify('task_new', NEW.id::text);
        PERFORM pg_notify('task_queue_' || NEW.queue_name, 'new');
    END IF;
    PERFORM pg_notify('horsies_task_status', NEW.id::text);
    RETURN NULL;
END;
$$;

-- Update trigger only. The trigger's WHEN clause holds the condition, so the body is one notify.
CREATE FUNCTION horsies_notify_task_status_change() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('horsies_task_status', NEW.id::text);
    RETURN NULL;
END;
$$;

CREATE FUNCTION horsies_notify_worker_state_change() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('horsies_worker_state', NEW.worker_id);
    RETURN NULL;
END;
$$;

-- Insert and status update of a workflow. On a status update to COMPLETED, FAILED, CANCELLED, PAUSED or EXPIRED it
-- also sends workflow_done, first, as the merged v61 trigger did (horsies_workflow_notify_trigger fired before
-- horsies_workflow_status_notify_update_trigger).
CREATE FUNCTION horsies_notify_workflow_status_change() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'PAUSED', 'EXPIRED') THEN
        PERFORM pg_notify('workflow_done', NEW.id::text);
    END IF;
    PERFORM pg_notify('horsies_workflow_status', NEW.id::text);
    RETURN NULL;
END;
$$;

-- No WHEN: every insert sends the status event; the function sends the wake for a PENDING row only.
CREATE TRIGGER horsies_tasks_insert_notify
    AFTER INSERT ON horsies_tasks
    FOR EACH ROW EXECUTE FUNCTION horsies_notify_task_changes();

-- No event at task start (RUNNING); a terminal move deletes the row and sends its own events.
CREATE TRIGGER horsies_task_status_notify_update_trigger
    AFTER UPDATE ON horsies_tasks
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status AND NEW.status <> 'RUNNING')
    EXECUTE FUNCTION horsies_notify_task_status_change();

-- INSERT only: no statement updates a snapshot.
CREATE TRIGGER horsies_worker_state_notify_trigger
    AFTER INSERT ON horsies_worker_states
    FOR EACH ROW EXECUTE FUNCTION horsies_notify_worker_state_change();

CREATE TRIGGER horsies_workflow_status_notify_insert_trigger
    AFTER INSERT ON horsies_workflows
    FOR EACH ROW EXECUTE FUNCTION horsies_notify_workflow_status_change();

CREATE TRIGGER horsies_workflow_status_notify_update_trigger
    AFTER UPDATE ON horsies_workflows
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION horsies_notify_workflow_status_change();
