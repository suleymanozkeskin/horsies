"""Direct live-to-history terminalization in disposable schemas."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.schemas.terminalization import OUTCOME_COLUMNS
from tests.task_history_prototypes.schema import PrototypeSchema


async def install_history_terminalization_prototype(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    for statement in _terminalization_manifest(schema):
        await connection.execute(text(statement))


def _terminalization_manifest(schema: PrototypeSchema) -> tuple[str, ...]:
    namespace = schema.sql
    outcome_columns = ',\n            '.join(
        f'{name} {kind}' for name, kind in OUTCOME_COLUMNS
    )
    return (
        f"""
        CREATE TABLE {namespace}.live_tasks (
            LIKE horsies_tasks
                INCLUDING DEFAULTS
                INCLUDING GENERATED
                INCLUDING IDENTITY
                INCLUDING STORAGE
        )
        """,
        f'ALTER TABLE {namespace}.live_tasks ADD PRIMARY KEY (id)',
        f"""
        ALTER TABLE {namespace}.live_tasks
            ADD COLUMN retention_class_key text NOT NULL
                DEFAULT 'finite_30d_v1'
                REFERENCES {namespace}.retention_classes(class_key),
            ADD COLUMN rerun_of_task_id varchar(36),
            ADD COLUMN rerun_root_task_id varchar(36),
            ADD COLUMN input_digest bytea,
            ADD CHECK (
                input_digest IS NULL OR octet_length(input_digest) = 32
            )
        """,
        f"""
        CREATE TABLE {namespace}.live_attempts (
            LIKE horsies_task_attempts
                INCLUDING DEFAULTS
                INCLUDING GENERATED
                INCLUDING IDENTITY
                INCLUDING STORAGE
        )
        """,
        f'ALTER TABLE {namespace}.live_attempts ADD PRIMARY KEY (id)',
        f"""
        CREATE UNIQUE INDEX live_attempts_task_attempt_idx
            ON {namespace}.live_attempts (task_id, attempt)
        """,
        f"""
        CREATE TABLE {namespace}.live_heartbeats (
            LIKE horsies_heartbeats
                INCLUDING DEFAULTS
                INCLUDING GENERATED
                INCLUDING IDENTITY
                INCLUDING STORAGE
        )
        """,
        f"""
        CREATE TYPE {namespace}.terminalization_outcome AS (
            {outcome_columns}
        )
        """,
        _archive_availability_function(namespace),
        _task_notification_function(namespace),
        _attempt_snapshot_function(namespace),
        _move_function(namespace),
        _miss_function(namespace),
        _complete_locked_function(namespace),
        _complete_fused_function(namespace),
        _fail_locked_function(namespace),
        _fail_stale_function(namespace),
        _expire_owned_function(namespace),
        _expire_pending_function(namespace),
        _cancel_admin_function(namespace),
        _cancel_owned_orphan_function(namespace),
        _cancel_orphaned_batch_function(namespace),
        _abandon_owned_node_function(namespace),
        _cancel_owned_node_function(namespace),
        _owned_node_batch_function(namespace, pause=True),
        _owned_node_batch_function(namespace, pause=False),
        _workflow_scoped_batch_function(namespace, pause=True),
        _workflow_scoped_batch_function(namespace, pause=False),
    )


def _archive_availability_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.assert_archive_available()
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        PERFORM singleton
        FROM {namespace}.archive_access_gate
        WHERE singleton IS TRUE
        FOR SHARE;
        IF EXISTS (
            SELECT 1
            FROM {namespace}.archive_maintenance_sessions
            WHERE ended_at IS NULL
        ) THEN
            RAISE EXCEPTION 'archive maintenance is active'
                USING ERRCODE = 'object_in_use';
        END IF;
    END
    $function$
    """


def _task_notification_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.emit_task_done(p_task_id varchar)
    RETURNS void
    LANGUAGE sql
    STRICT
    AS $function$
        SELECT pg_notify('task_done', p_task_id)
    $function$
    """


def _attempt_snapshot_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.encode_live_attempts(p_task_id varchar)
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
        FROM {namespace}.live_attempts AS a
        WHERE a.task_id = p_task_id
    $function$
    """


def _move_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.move_locked_task_to_history(
        p_task_id varchar,
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
        v_task {namespace}.live_tasks%ROWTYPE;
        v_attempt_snapshot bytea;
        v_result_payload bytea;
        v_prior_result_payload bytea;
        v_workflow_id varchar(36);
        v_workflow_node_row_id bigint;
        v_history_rows bigint;
        v_deleted_rows bigint;
        v_requires_deferred_phase2 boolean;
    BEGIN
        PERFORM {namespace}.assert_archive_available();

        CASE p_terminalization_kind
            WHEN '{TerminalizationKind.COMPLETE_LOCKED.value}' THEN
                IF p_terminal_status <> 'COMPLETED' THEN
                    RAISE EXCEPTION 'completion-locked projection disagrees';
                END IF;
                v_requires_deferred_phase2 := TRUE;
            WHEN '{TerminalizationKind.COMPLETE_FUSED.value}' THEN
                IF p_terminal_status <> 'COMPLETED' THEN
                    RAISE EXCEPTION 'completion-fused projection disagrees';
                END IF;
                v_requires_deferred_phase2 := FALSE;
            WHEN '{TerminalizationKind.FAIL_RUNNING.value}' THEN
                IF p_terminal_status <> 'FAILED' THEN
                    RAISE EXCEPTION 'running-failure projection disagrees';
                END IF;
                v_requires_deferred_phase2 := TRUE;
            WHEN '{TerminalizationKind.FAIL_STALE.value}' THEN
                IF p_terminal_status <> 'FAILED' THEN
                    RAISE EXCEPTION 'stale-failure projection disagrees';
                END IF;
                v_requires_deferred_phase2 := TRUE;
            WHEN '{TerminalizationKind.EXPIRE_CLAIMED.value}' THEN
                IF p_terminal_status <> 'EXPIRED' THEN
                    RAISE EXCEPTION 'claimed-expiry projection disagrees';
                END IF;
                v_requires_deferred_phase2 := TRUE;
            WHEN '{TerminalizationKind.CANCEL_ADMIN.value}' THEN
                IF p_terminal_status <> 'CANCELLED'
                   OR p_error_code <> 'TASK_CANCELLED'
                   OR p_failed_reason <> 'Cancelled via monitoring API' THEN
                    RAISE EXCEPTION 'administrative-cancel projection disagrees';
                END IF;
                v_requires_deferred_phase2 := FALSE;
            WHEN '{TerminalizationKind.CANCEL_ORPHAN.value}' THEN
                IF p_terminal_status <> 'CANCELLED'
                   OR p_error_code <> 'WORKFLOW_CHECK_FAILED'
                   OR p_failed_reason <>
                        'Workflow task orphaned: no live workflow_task linkage' THEN
                    RAISE EXCEPTION 'orphan-cancel projection disagrees';
                END IF;
                v_requires_deferred_phase2 := FALSE;
            WHEN '{TerminalizationKind.PAUSE_ABANDON_CLAIM.value}' THEN
                IF p_terminal_status <> 'CANCELLED'
                   OR p_error_code <> 'TASK_CANCELLED'
                   OR p_failed_reason <> 'Workflow paused before task start' THEN
                    RAISE EXCEPTION 'pause-abandon projection disagrees';
                END IF;
                v_requires_deferred_phase2 := FALSE;
            WHEN '{TerminalizationKind.WORKFLOW_CANCEL_CLAIM.value}' THEN
                IF p_terminal_status <> 'CANCELLED' THEN
                    RAISE EXCEPTION 'workflow-cancel projection disagrees';
                END IF;
                v_requires_deferred_phase2 := FALSE;
            ELSE
                RAISE EXCEPTION 'unsupported prototype terminalization kind %',
                    p_terminalization_kind
                    USING ERRCODE = 'invalid_parameter_value';
        END CASE;

        SELECT * INTO STRICT v_task
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
        FOR UPDATE;
        IF v_task.status NOT IN ('PENDING', 'CLAIMED', 'RUNNING') THEN
            RAISE EXCEPTION 'live task has non-live status %', v_task.status;
        END IF;
        IF p_terminal_at IS NULL THEN
            RAISE EXCEPTION 'terminal timestamp is required';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM {namespace}.history_aggregate
            WHERE task_id = p_task_id
        ) THEN
            RAISE EXCEPTION 'task identity exists in multiple locations';
        END IF;

        IF v_task.is_workflow_task THEN
            IF p_terminalization_kind IN (
                '{TerminalizationKind.COMPLETE_FUSED.value}',
                '{TerminalizationKind.CANCEL_ADMIN.value}'
            ) THEN
                RAISE EXCEPTION
                    'operation cannot terminalize a workflow task'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            SELECT n.id, n.workflow_id
            INTO v_workflow_node_row_id, v_workflow_id
            FROM {namespace}.phase2_nodes AS n
            WHERE n.task_id = p_task_id
            FOR UPDATE;
            IF v_requires_deferred_phase2 AND NOT FOUND THEN
                RAISE EXCEPTION
                    'deferred workflow terminalization has no node linkage'
                    USING ERRCODE = 'foreign_key_violation';
            END IF;
            IF v_requires_deferred_phase2 AND p_result IS NULL THEN
                RAISE EXCEPTION
                    'deferred workflow completion requires a result payload'
                    USING ERRCODE = 'not_null_violation';
            END IF;
        END IF;

        v_attempt_snapshot := {namespace}.encode_live_attempts(p_task_id);
        IF p_terminalization_kind = '{TerminalizationKind.CANCEL_ADMIN.value}' THEN
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

        INSERT INTO {namespace}.history_aggregate (
            task_id, task_name, queue_name, priority, status,
            terminalization_kind, terminal_at, retention_anchor_at,
            retention_class_key, sent_at, enqueued_at, claimed_at,
            started_at, created_at,
            result_envelope_version, result_codec, result_payload,
            result_digest, error_code, final_failed_reason,
            prior_result_payload,
            retry_count, rerun_of_task_id, rerun_root_task_id,
            input_digest, workflow_id, is_workflow_task,
            history_schema_version, attempt_archive_version,
            attempt_snapshot_codec, attempt_snapshot,
            attempt_snapshot_digest
        ) VALUES (
            v_task.id, v_task.task_name, v_task.queue_name, v_task.priority,
            p_terminal_status, p_terminalization_kind,
            p_terminal_at, p_terminal_at, v_task.retention_class_key,
            v_task.sent_at, v_task.enqueued_at, v_task.claimed_at,
            v_task.started_at, v_task.created_at,
            1, 'json-utf8', v_result_payload,
            CASE WHEN v_result_payload IS NULL
                 THEN CASE WHEN v_prior_result_payload IS NULL
                           THEN NULL ELSE sha256(v_prior_result_payload) END
                 ELSE sha256(v_result_payload) END,
            p_error_code, p_failed_reason,
            v_prior_result_payload,
            v_task.retry_count, v_task.rerun_of_task_id,
            v_task.rerun_root_task_id, v_task.input_digest,
            v_workflow_id, v_task.is_workflow_task,
            1, 1, 'json-utf8', v_attempt_snapshot,
            sha256(v_attempt_snapshot)
        );
        GET DIAGNOSTICS v_history_rows = ROW_COUNT;
        IF v_history_rows <> 1 THEN
            RAISE EXCEPTION 'terminal history insert did not affect one row';
        END IF;

        IF v_requires_deferred_phase2 AND v_task.is_workflow_task THEN
            INSERT INTO {namespace}.workflow_phase2_pending (
                task_id, workflow_id, workflow_node_row_id,
                terminal_status, terminal_at, terminalization_kind,
                recovery_source, history_class, history_anchor,
                history_schema_version, result_digest,
                phase2_generation, created_at
            ) VALUES (
                v_task.id, v_workflow_id, v_workflow_node_row_id,
                p_terminal_status, p_terminal_at, p_terminalization_kind,
                'HISTORY', v_task.retention_class_key, p_terminal_at,
                1, sha256(v_result_payload),
                gen_random_uuid()::text, statement_timestamp()
            );
        END IF;

        DELETE FROM {namespace}.live_attempts WHERE task_id = p_task_id;
        DELETE FROM {namespace}.live_tasks WHERE id = p_task_id;
        GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;
        IF v_deleted_rows <> 1 THEN
            RAISE EXCEPTION 'live task delete did not affect one row';
        END IF;
        PERFORM {namespace}.emit_task_done(p_task_id);
    END
    $function$
    """


def _miss_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.terminalization_miss(
        p_task_id varchar,
        p_equivalent_kinds text[],
        p_worker_id text,
        p_claimed_at timestamptz
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_live {namespace}.live_tasks%ROWTYPE;
        v_history {namespace}.history_aggregate%ROWTYPE;
        v_history_count bigint;
        v_live_found boolean;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT * INTO v_live
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
        FOR UPDATE;
        v_live_found := FOUND;

        SELECT count(*) INTO v_history_count
        FROM {namespace}.history_aggregate
        WHERE task_id = p_task_id;
        IF v_history_count > 1 OR (v_live_found AND v_history_count > 0) THEN
            RAISE EXCEPTION 'task identity exists in multiple locations';
        END IF;
        IF v_history_count = 1 THEN
            SELECT * INTO STRICT v_history
            FROM {namespace}.history_aggregate
            WHERE task_id = p_task_id;
            IF v_history.terminalization_kind = ANY(p_equivalent_kinds) THEN
                RETURN QUERY SELECT
                    p_task_id, NULL::bigint, 'ALREADY_APPLIED'::text,
                    v_history.terminal_at, v_history.terminalization_kind,
                    v_history.status, NULL::varchar, v_history.claimed_at,
                    NULL::text, NULL::jsonb;
            ELSE
                RETURN QUERY SELECT
                    p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
                    v_history.terminal_at, v_history.terminalization_kind,
                    v_history.status, NULL::varchar, v_history.claimed_at,
                    'FOREIGN_TERMINALIZATION'::text, NULL::jsonb;
            END IF;
            RETURN;
        END IF;

        IF v_live.id IS NULL THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'TASK_ABSENT'::text,
                NULL::timestamptz, NULL::text,
                NULL::text, NULL::varchar, NULL::timestamptz,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        IF p_worker_id IS NOT NULL AND (
            v_live.claimed_by_worker_id
                IS DISTINCT FROM CAST(p_worker_id AS varchar)
            OR (p_claimed_at IS NOT NULL
                AND v_live.claimed_at IS DISTINCT FROM p_claimed_at)
        ) THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'LOST_CLAIM'::text,
                NULL::timestamptz, NULL::text,
                v_live.status::text, v_live.claimed_by_worker_id,
                v_live.claimed_at, NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            NULL::timestamptz, NULL::text,
            v_live.status::text, v_live.claimed_by_worker_id,
            v_live.claimed_at, NULL::text, NULL::jsonb;
    END
    $function$
    """


def _complete_locked_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_complete_locked_task(
        p_task_id varchar,
        p_worker_id text,
        p_result text
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_claimed_at timestamptz;
        v_terminal_at timestamptz;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT claimed_at INTO v_claimed_at
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
          AND status = 'RUNNING'
          AND claimed_by_worker_id = CAST(p_worker_id AS varchar)
        FOR UPDATE;
        IF FOUND THEN
            v_terminal_at := NOW();
            PERFORM {namespace}.move_locked_task_to_history(
                p_task_id, 'COMPLETED',
                '{TerminalizationKind.COMPLETE_LOCKED.value}',
                v_terminal_at, p_result, NULL, NULL
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at,
                '{TerminalizationKind.COMPLETE_LOCKED.value}'::text,
                'RUNNING'::text, CAST(p_worker_id AS varchar), v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY[
                '{TerminalizationKind.COMPLETE_FUSED.value}',
                '{TerminalizationKind.COMPLETE_LOCKED.value}'
            ]::text[],
            p_worker_id, NULL::timestamptz
        );
    END
    $function$
    """


def _complete_fused_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_complete_task_fused(
        p_task_id varchar,
        p_worker_id text,
        p_claimed_at timestamptz,
        p_result text,
        p_notify_channel text,
        p_notify_payload text
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_task {namespace}.live_tasks%ROWTYPE;
        v_terminal_at timestamptz;
        v_finished_at timestamptz;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT * INTO v_task
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
          AND status = 'RUNNING'
          AND claimed_by_worker_id = CAST(p_worker_id AS varchar)
          AND (p_claimed_at IS NULL OR claimed_at = p_claimed_at)
        FOR UPDATE;
        IF FOUND THEN
            v_terminal_at := NOW();
            v_finished_at := clock_timestamp();
            INSERT INTO {namespace}.live_attempts (
                task_id, attempt, outcome, will_retry,
                started_at, finished_at,
                error_code, error_message, failed_reason,
                worker_id, worker_hostname, worker_pid,
                worker_process_name
            ) VALUES (
                v_task.id, COALESCE(v_task.retry_count, 0) + 1,
                'COMPLETED', FALSE,
                COALESCE(v_task.started_at, v_finished_at), v_finished_at,
                NULL, NULL, NULL,
                v_task.claimed_by_worker_id, v_task.worker_hostname,
                v_task.worker_pid, v_task.worker_process_name
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
            PERFORM {namespace}.move_locked_task_to_history(
                p_task_id, 'COMPLETED',
                '{TerminalizationKind.COMPLETE_FUSED.value}',
                v_terminal_at, p_result, NULL, NULL
            );
            PERFORM pg_notify(p_notify_channel, p_notify_payload);
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at,
                '{TerminalizationKind.COMPLETE_FUSED.value}'::text,
                'RUNNING'::text, v_task.claimed_by_worker_id,
                v_task.claimed_at, NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY[
                '{TerminalizationKind.COMPLETE_FUSED.value}',
                '{TerminalizationKind.COMPLETE_LOCKED.value}'
            ]::text[],
            p_worker_id, p_claimed_at
        );
    END
    $function$
    """


def _fail_locked_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_fail_locked_task(
        p_task_id varchar,
        p_worker_id text,
        p_result text,
        p_error_code text,
        p_failed_reason text
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_claimed_at timestamptz;
        v_terminal_at timestamptz;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT claimed_at INTO v_claimed_at
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
          AND status = 'RUNNING'
          AND claimed_by_worker_id = CAST(p_worker_id AS varchar)
        FOR UPDATE;
        IF FOUND THEN
            v_terminal_at := NOW();
            PERFORM {namespace}.move_locked_task_to_history(
                p_task_id, 'FAILED',
                '{TerminalizationKind.FAIL_RUNNING.value}',
                v_terminal_at, p_result, p_error_code, p_failed_reason
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at,
                '{TerminalizationKind.FAIL_RUNNING.value}'::text,
                'RUNNING'::text, CAST(p_worker_id AS varchar), v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY['{TerminalizationKind.FAIL_RUNNING.value}']::text[],
            p_worker_id, NULL::timestamptz
        );
    END
    $function$
    """


def _fail_stale_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_fail_stale_task(
        p_task_id varchar,
        p_stale_after_ms integer,
        p_finalizing_stale_after_ms integer,
        p_result text,
        p_error_code text,
        p_failed_reason text
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_terminal_at timestamptz;
        v_status text;
        v_worker varchar;
        v_claimed_at timestamptz;
        v_started_at timestamptz;
        v_finalizing_at timestamptz;
        v_last_heartbeat timestamptz;
        v_evaluated_at timestamptz;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT t.status::text, t.claimed_by_worker_id, t.claimed_at,
               t.started_at, t.finalizing_at,
               (
                   SELECT h.sent_at
                   FROM {namespace}.live_heartbeats AS h
                   WHERE h.task_id = t.id AND h.role = 'runner'
                   ORDER BY h.sent_at DESC
                   LIMIT 1
               ),
               NOW()
        INTO v_status, v_worker, v_claimed_at, v_started_at, v_finalizing_at,
             v_last_heartbeat, v_evaluated_at
        FROM {namespace}.live_tasks AS t
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
                v_terminal_at := v_evaluated_at;
                PERFORM {namespace}.move_locked_task_to_history(
                    p_task_id, 'FAILED',
                    '{TerminalizationKind.FAIL_STALE.value}',
                    v_terminal_at, p_result, p_error_code, p_failed_reason
                );
                RETURN QUERY SELECT
                    p_task_id, NULL::bigint, 'APPLIED'::text,
                    v_terminal_at,
                    '{TerminalizationKind.FAIL_STALE.value}'::text,
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
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY['{TerminalizationKind.FAIL_STALE.value}']::text[],
            NULL::text, NULL::timestamptz
        );
    END
    $function$
    """


def _expire_owned_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_expire_owned_claim(
        p_task_id varchar,
        p_worker_id text,
        p_result text,
        p_error_code text
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_status text;
        v_worker varchar;
        v_claimed_at timestamptz;
        v_good_until timestamptz;
        v_evaluated_at timestamptz;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT status::text, claimed_by_worker_id, claimed_at,
               good_until, NOW()
        INTO v_status, v_worker, v_claimed_at,
             v_good_until, v_evaluated_at
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
        FOR UPDATE;
        IF FOUND
           AND v_status = 'CLAIMED'
           AND v_worker = CAST(p_worker_id AS varchar)
           AND v_good_until IS NOT NULL
           AND v_good_until <= v_evaluated_at THEN
            PERFORM {namespace}.move_locked_task_to_history(
                p_task_id, 'EXPIRED',
                '{TerminalizationKind.EXPIRE_CLAIMED.value}',
                v_evaluated_at, p_result, p_error_code, NULL
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_evaluated_at,
                '{TerminalizationKind.EXPIRE_CLAIMED.value}'::text,
                v_status, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        IF FOUND
           AND v_status = 'CLAIMED'
           AND v_worker = CAST(p_worker_id AS varchar) THEN
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
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY[
                '{TerminalizationKind.EXPIRE_CLAIMED.value}',
                '{TerminalizationKind.EXPIRE_PENDING.value}'
            ]::text[],
            p_worker_id, NULL::timestamptz
        );
    END
    $function$
    """


def _expire_pending_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_expire_pending_tasks(
        p_batch_size integer,
        p_result text,
        p_error_code text
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
            RAISE EXCEPTION
                'p_batch_size must be a positive integer, got %', p_batch_size
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        PERFORM {namespace}.assert_archive_available();
        PERFORM duplicate.id
        FROM {namespace}.live_tasks AS duplicate
        WHERE duplicate.status = 'PENDING'
          AND duplicate.good_until IS NOT NULL
          AND duplicate.good_until <= NOW()
          AND EXISTS (
              SELECT 1 FROM {namespace}.history_aggregate AS history
              WHERE history.task_id = duplicate.id
          )
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
        IF FOUND THEN
            RAISE EXCEPTION 'task identity exists in multiple locations';
        END IF;
        PERFORM invalid.id
        FROM {namespace}.live_tasks AS invalid
        WHERE invalid.status = 'PENDING'
          AND invalid.good_until IS NOT NULL
          AND invalid.good_until <= NOW()
          AND invalid.is_workflow_task
          AND NOT EXISTS (
              SELECT 1 FROM {namespace}.phase2_nodes AS node
              WHERE node.task_id = invalid.id
          )
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
        IF FOUND THEN
            RAISE EXCEPTION
                'deferred workflow terminalization has no node linkage'
                USING ERRCODE = 'foreign_key_violation';
        END IF;

        RETURN QUERY
        WITH targets AS MATERIALIZED (
            SELECT task.*, node.id AS workflow_node_row_id,
                   node.workflow_id,
                   NOW() AS assigned_terminal_at,
                   {namespace}.encode_live_attempts(task.id)
                       AS encoded_attempts,
                   CASE WHEN p_result IS NULL THEN NULL::bytea
                        ELSE convert_to(p_result, 'UTF8') END
                       AS encoded_result
            FROM {namespace}.live_tasks AS task
            LEFT JOIN {namespace}.phase2_nodes AS node
                ON node.task_id = task.id
            WHERE task.status = 'PENDING'
              AND task.good_until IS NOT NULL
              AND task.good_until <= NOW()
            ORDER BY task.good_until, task.id
            LIMIT p_batch_size
            FOR UPDATE OF task SKIP LOCKED
        ),
        history_rows AS (
            INSERT INTO {namespace}.history_aggregate (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, sent_at, enqueued_at, claimed_at,
                started_at, created_at,
                result_envelope_version, result_codec, result_payload,
                result_digest, error_code, final_failed_reason,
                retry_count, rerun_of_task_id, rerun_root_task_id,
                input_digest, workflow_id, is_workflow_task,
                history_schema_version, attempt_archive_version,
                attempt_snapshot_codec, attempt_snapshot,
                attempt_snapshot_digest
            )
            SELECT target.id, target.task_name, target.queue_name,
                   target.priority, 'EXPIRED',
                   '{TerminalizationKind.EXPIRE_PENDING.value}',
                   target.assigned_terminal_at, target.assigned_terminal_at,
                   target.retention_class_key, target.sent_at,
                   target.enqueued_at, target.claimed_at, target.started_at,
                   target.created_at, 1, 'json-utf8', target.encoded_result,
                   CASE WHEN target.encoded_result IS NULL THEN NULL
                        ELSE sha256(target.encoded_result) END,
                   p_error_code, NULL,
                   target.retry_count, target.rerun_of_task_id,
                   target.rerun_root_task_id, target.input_digest,
                   target.workflow_id, target.is_workflow_task,
                   1, 1, 'json-utf8', target.encoded_attempts,
                   sha256(target.encoded_attempts)
            FROM targets AS target
            RETURNING task_id, terminal_at, terminalization_kind
        ),
        pending_rows AS (
            INSERT INTO {namespace}.workflow_phase2_pending (
                task_id, workflow_id, workflow_node_row_id,
                terminal_status, terminal_at, terminalization_kind,
                recovery_source, history_class, history_anchor,
                history_schema_version, result_digest,
                phase2_generation, created_at
            )
            SELECT target.id, target.workflow_id, target.workflow_node_row_id,
                   'EXPIRED', history.terminal_at,
                   history.terminalization_kind, 'HISTORY',
                   target.retention_class_key, history.terminal_at,
                   1, sha256(target.encoded_result),
                   gen_random_uuid()::text, statement_timestamp()
            FROM targets AS target
            JOIN history_rows AS history ON history.task_id = target.id
            WHERE target.is_workflow_task
            RETURNING task_id
        ),
        ready AS MATERIALIZED (
            SELECT history.task_id
            FROM history_rows AS history
            JOIN targets AS target ON target.id = history.task_id
            WHERE NOT target.is_workflow_task
               OR EXISTS (
                   SELECT 1 FROM pending_rows AS pending
                   WHERE pending.task_id = history.task_id
               )
        ),
        purged_attempts AS (
            DELETE FROM {namespace}.live_attempts AS attempt
            WHERE attempt.task_id IN (SELECT task_id FROM ready)
            RETURNING attempt.task_id
        ),
        deleted_tasks AS (
            DELETE FROM {namespace}.live_tasks AS task
            WHERE task.id IN (SELECT task_id FROM ready)
              AND (SELECT count(*) FROM purged_attempts) >= 0
            RETURNING task.id
        ),
        notifications AS MATERIALIZED (
            SELECT ready.task_id,
                   {namespace}.emit_task_done(ready.task_id) AS emitted
            FROM ready
            JOIN deleted_tasks AS deleted ON deleted.id = ready.task_id
        )
        SELECT history.task_id, NULL::bigint, 'APPLIED'::text,
               history.terminal_at, history.terminalization_kind,
               'PENDING'::text, target.claimed_by_worker_id,
               target.claimed_at, NULL::text, NULL::jsonb
        FROM history_rows AS history
        JOIN targets AS target ON target.id = history.task_id
        JOIN deleted_tasks AS deleted ON deleted.id = history.task_id
        JOIN notifications AS notification
            ON notification.task_id = history.task_id
        ORDER BY target.good_until, target.id;
    END
    $function$
    """


def _cancel_admin_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_cancel_locked_task(
        p_task_id varchar,
        p_permitted_source_statuses text[]
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_status text;
        v_worker varchar;
        v_claimed_at timestamptz;
        v_terminal_at timestamptz;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT status::text, claimed_by_worker_id, claimed_at
        INTO v_status, v_worker, v_claimed_at
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
          AND NOT is_workflow_task
          AND status::text IN ('PENDING', 'CLAIMED', 'RUNNING')
          AND status::text = ANY(p_permitted_source_statuses)
        FOR UPDATE;
        IF FOUND THEN
            v_terminal_at := NOW();
            PERFORM {namespace}.move_locked_task_to_history(
                p_task_id, 'CANCELLED',
                '{TerminalizationKind.CANCEL_ADMIN.value}',
                v_terminal_at, NULL,
                'TASK_CANCELLED', 'Cancelled via monitoring API'
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at,
                '{TerminalizationKind.CANCEL_ADMIN.value}'::text,
                v_status, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY['{TerminalizationKind.CANCEL_ADMIN.value}']::text[],
            NULL::text, NULL::timestamptz
        );
    END
    $function$
    """


def _cancel_owned_orphan_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_cancel_owned_orphan(
        p_task_id varchar,
        p_worker_id text,
        p_claimed_at timestamptz
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_status text;
        v_worker varchar;
        v_claimed_at timestamptz;
        v_is_workflow_task boolean;
        v_node_status text;
        v_result text;
        v_terminal_at timestamptz;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT task.status::text, task.claimed_by_worker_id,
               task.claimed_at, task.is_workflow_task, task.result,
               (
                   SELECT node.status
                   FROM {namespace}.phase2_nodes AS node
                   WHERE node.task_id = task.id
                     AND node.status IN (
                         'ENQUEUED', 'READY', 'PENDING', 'RUNNING'
                     )
                   ORDER BY node.id
                   LIMIT 1
               )
        INTO v_status, v_worker, v_claimed_at,
             v_is_workflow_task, v_result, v_node_status
        FROM {namespace}.live_tasks AS task
        WHERE task.id = p_task_id
        FOR UPDATE;
        IF FOUND
           AND v_status = 'CLAIMED'
           AND v_worker = CAST(p_worker_id AS varchar)
           AND (p_claimed_at IS NULL OR v_claimed_at = p_claimed_at)
           AND v_is_workflow_task
           AND v_node_status IS NULL THEN
            v_terminal_at := NOW();
            PERFORM {namespace}.move_locked_task_to_history(
                p_task_id, 'CANCELLED',
                '{TerminalizationKind.CANCEL_ORPHAN.value}',
                v_terminal_at, v_result,
                'WORKFLOW_CHECK_FAILED',
                'Workflow task orphaned: no live workflow_task linkage'
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at,
                '{TerminalizationKind.CANCEL_ORPHAN.value}'::text,
                v_status, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        IF FOUND
           AND v_status = 'CLAIMED'
           AND v_worker = CAST(p_worker_id AS varchar)
           AND (p_claimed_at IS NULL OR v_claimed_at = p_claimed_at)
           AND v_is_workflow_task
           AND v_node_status IS NOT NULL THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
                NULL::timestamptz, NULL::text,
                v_status, v_worker, v_claimed_at,
                'WORKFLOW_LINK_STATE'::text,
                jsonb_build_object('node_status', v_node_status);
            RETURN;
        END IF;
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY[
                '{TerminalizationKind.CANCEL_ORPHAN.value}',
                '{TerminalizationKind.CANCEL_ORPHAN_SWEEP.value}'
            ]::text[],
            p_worker_id, p_claimed_at
        );
    END
    $function$
    """


def _cancel_orphaned_batch_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_cancel_orphaned_tasks(
        p_batch_size integer
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
            RAISE EXCEPTION
                'p_batch_size must be a positive integer, got %', p_batch_size
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        PERFORM {namespace}.assert_archive_available();
        PERFORM duplicate.id
        FROM {namespace}.live_tasks AS duplicate
        WHERE duplicate.is_workflow_task
          AND duplicate.status::text IN ('CLAIMED', 'PENDING')
          AND NOT EXISTS (
              SELECT 1 FROM {namespace}.phase2_nodes AS runnable
              WHERE runnable.task_id = duplicate.id
                AND runnable.status IN (
                    'ENQUEUED', 'READY', 'PENDING', 'RUNNING'
                )
          )
          AND EXISTS (
              SELECT 1 FROM {namespace}.history_aggregate AS history
              WHERE history.task_id = duplicate.id
          )
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
        IF FOUND THEN
            RAISE EXCEPTION 'task identity exists in multiple locations';
        END IF;

        RETURN QUERY
        WITH targets AS MATERIALIZED (
            SELECT task.*,
                   (
                       SELECT node.workflow_id
                       FROM {namespace}.phase2_nodes AS node
                       WHERE node.task_id = task.id
                       ORDER BY node.id
                       LIMIT 1
                   ) AS workflow_id,
                   NOW() AS assigned_terminal_at,
                   {namespace}.encode_live_attempts(task.id)
                       AS encoded_attempts,
                   CASE WHEN task.result IS NULL THEN NULL::bytea
                        ELSE convert_to(task.result, 'UTF8') END
                       AS encoded_result
            FROM {namespace}.live_tasks AS task
            WHERE task.is_workflow_task
              AND task.status::text IN ('CLAIMED', 'PENDING')
              AND NOT EXISTS (
                  SELECT 1 FROM {namespace}.phase2_nodes AS runnable
                  WHERE runnable.task_id = task.id
                    AND runnable.status IN (
                        'ENQUEUED', 'READY', 'PENDING', 'RUNNING'
                    )
              )
            LIMIT p_batch_size
            FOR UPDATE OF task SKIP LOCKED
        ),
        history_rows AS (
            INSERT INTO {namespace}.history_aggregate (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, sent_at, enqueued_at, claimed_at,
                started_at, created_at,
                result_envelope_version, result_codec, result_payload,
                result_digest, error_code, final_failed_reason,
                retry_count, rerun_of_task_id, rerun_root_task_id,
                input_digest, workflow_id, is_workflow_task,
                history_schema_version, attempt_archive_version,
                attempt_snapshot_codec, attempt_snapshot,
                attempt_snapshot_digest
            )
            SELECT target.id, target.task_name, target.queue_name,
                   target.priority, 'CANCELLED',
                   '{TerminalizationKind.CANCEL_ORPHAN_SWEEP.value}',
                   target.assigned_terminal_at, target.assigned_terminal_at,
                   target.retention_class_key, target.sent_at,
                   target.enqueued_at, target.claimed_at, target.started_at,
                   target.created_at, 1, 'json-utf8', target.encoded_result,
                   CASE WHEN target.encoded_result IS NULL THEN NULL
                        ELSE sha256(target.encoded_result) END,
                   'WORKFLOW_CHECK_FAILED',
                   'Workflow task orphaned: no live workflow_task linkage',
                   target.retry_count, target.rerun_of_task_id,
                   target.rerun_root_task_id, target.input_digest,
                   target.workflow_id, TRUE,
                   1, 1, 'json-utf8', target.encoded_attempts,
                   sha256(target.encoded_attempts)
            FROM targets AS target
            RETURNING task_id, terminal_at, terminalization_kind
        ),
        purged_attempts AS (
            DELETE FROM {namespace}.live_attempts AS attempt
            WHERE attempt.task_id IN (
                SELECT task_id FROM history_rows
            )
            RETURNING attempt.task_id
        ),
        deleted_tasks AS (
            DELETE FROM {namespace}.live_tasks AS task
            WHERE task.id IN (SELECT task_id FROM history_rows)
              AND (SELECT count(*) FROM purged_attempts) >= 0
            RETURNING task.id
        ),
        notifications AS MATERIALIZED (
            SELECT history.task_id,
                   {namespace}.emit_task_done(history.task_id) AS emitted
            FROM history_rows AS history
            JOIN deleted_tasks AS deleted ON deleted.id = history.task_id
        )
        SELECT history.task_id, NULL::bigint, 'APPLIED'::text,
               history.terminal_at, history.terminalization_kind,
               target.status::text, target.claimed_by_worker_id,
               target.claimed_at, NULL::text, NULL::jsonb
        FROM history_rows AS history
        JOIN targets AS target ON target.id = history.task_id
        JOIN deleted_tasks AS deleted ON deleted.id = history.task_id
        JOIN notifications AS notification
            ON notification.task_id = history.task_id;
    END
    $function$
    """


def _abandon_owned_node_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_abandon_owned_node(
        p_task_id varchar,
        p_worker_id text,
        p_claimed_at timestamptz
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_status text;
        v_worker varchar;
        v_claimed_at timestamptz;
        v_result text;
        v_terminal_at timestamptz;
        v_node_rows bigint;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT status::text, claimed_by_worker_id, claimed_at, result
        INTO v_status, v_worker, v_claimed_at, v_result
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
        FOR UPDATE;
        IF FOUND
           AND v_status = 'CLAIMED'
           AND v_worker = CAST(p_worker_id AS varchar)
           AND (p_claimed_at IS NULL OR v_claimed_at = p_claimed_at) THEN
            v_terminal_at := NOW();
            PERFORM {namespace}.move_locked_task_to_history(
                p_task_id, 'CANCELLED',
                '{TerminalizationKind.PAUSE_ABANDON_CLAIM.value}',
                v_terminal_at, v_result,
                'TASK_CANCELLED', 'Workflow paused before task start'
            );
            UPDATE {namespace}.phase2_nodes
            SET status = 'READY', task_id = NULL
            WHERE task_id = p_task_id
              AND status IN ('ENQUEUED', 'RUNNING');
            GET DIAGNOSTICS v_node_rows = ROW_COUNT;
            IF v_node_rows <> 1 THEN
                RAISE EXCEPTION
                    'pause node disposition did not affect one row';
            END IF;
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at,
                '{TerminalizationKind.PAUSE_ABANDON_CLAIM.value}'::text,
                v_status, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY[
                '{TerminalizationKind.PAUSE_ABANDON_CLAIM.value}',
                '{TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH.value}',
                '{TerminalizationKind.PAUSE_ABANDON_WORKFLOW.value}'
            ]::text[],
            p_worker_id, p_claimed_at
        );
    END
    $function$
    """


def _cancel_owned_node_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.horsies_cancel_owned_node(
        p_task_id varchar,
        p_worker_id text,
        p_claimed_at timestamptz,
        p_accepts_requeued_pending boolean
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_status text;
        v_worker varchar;
        v_claimed_at timestamptz;
        v_result text;
        v_error_code text;
        v_failed_reason text;
        v_terminal_at timestamptz;
        v_node_rows bigint;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));
        SELECT status::text, claimed_by_worker_id, claimed_at,
               result, error_code, failed_reason
        INTO v_status, v_worker, v_claimed_at,
             v_result, v_error_code, v_failed_reason
        FROM {namespace}.live_tasks
        WHERE id = p_task_id
        FOR UPDATE;
        IF FOUND AND (
            (
                v_status = 'CLAIMED'
                AND v_worker = CAST(p_worker_id AS varchar)
                AND (p_claimed_at IS NULL OR v_claimed_at = p_claimed_at)
            )
            OR (p_accepts_requeued_pending AND v_status = 'PENDING')
        ) THEN
            v_terminal_at := NOW();
            PERFORM {namespace}.move_locked_task_to_history(
                p_task_id, 'CANCELLED',
                '{TerminalizationKind.WORKFLOW_CANCEL_CLAIM.value}',
                v_terminal_at, v_result, v_error_code, v_failed_reason
            );
            UPDATE {namespace}.phase2_nodes
            SET status = 'SKIPPED'
            WHERE task_id = p_task_id
              AND status IN ('PENDING', 'READY', 'ENQUEUED');
            GET DIAGNOSTICS v_node_rows = ROW_COUNT;
            IF v_node_rows <> 1 THEN
                RAISE EXCEPTION
                    'cancel node disposition did not affect one row';
            END IF;
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at,
                '{TerminalizationKind.WORKFLOW_CANCEL_CLAIM.value}'::text,
                v_status, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT * FROM {namespace}.terminalization_miss(
            p_task_id,
            ARRAY[
                '{TerminalizationKind.WORKFLOW_CANCEL_CLAIM.value}',
                '{TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH.value}',
                '{TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW.value}'
            ]::text[],
            p_worker_id, p_claimed_at
        );
    END
    $function$
    """


def _owned_node_batch_function(namespace: str, *, pause: bool) -> str:
    if pause:
        function_name = 'horsies_abandon_owned_nodes'
        kind = TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH.value
        equivalent_kinds = (
            TerminalizationKind.PAUSE_ABANDON_CLAIM.value,
            TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH.value,
            TerminalizationKind.PAUSE_ABANDON_WORKFLOW.value,
        )
        node_statuses = "('ENQUEUED', 'RUNNING')"
        node_assignment = "status = 'READY', task_id = NULL"
        error_projection = "'TASK_CANCELLED'"
        reason_projection = "'Workflow paused before task start'"
    else:
        function_name = 'horsies_cancel_owned_nodes'
        kind = TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH.value
        equivalent_kinds = (
            TerminalizationKind.WORKFLOW_CANCEL_CLAIM.value,
            TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH.value,
            TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW.value,
        )
        node_statuses = "('PENDING', 'READY', 'ENQUEUED')"
        node_assignment = "status = 'SKIPPED'"
        error_projection = 'target.error_code'
        reason_projection = 'target.failed_reason'
    equivalent_array = ', '.join(f"'{item}'" for item in equivalent_kinds)
    return f"""
    CREATE FUNCTION {namespace}.{function_name}(
        p_ids varchar[],
        p_claimed_ats timestamptz[],
        p_worker_id text
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
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
            SELECT count(DISTINCT item.id)
            FROM unnest(p_ids) AS item(id)
        ) THEN
            RAISE EXCEPTION 'batch task ids must be distinct'
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        PERFORM {namespace}.assert_archive_available();
        IF EXISTS (
            SELECT 1
            FROM unnest(p_ids) AS requested(task_id)
            LEFT JOIN {namespace}.live_tasks AS live
                ON live.id = requested.task_id
            CROSS JOIN LATERAL (
                SELECT count(*) AS locations
                FROM {namespace}.history_aggregate AS history
                WHERE history.task_id = requested.task_id
            ) AS retained
            WHERE retained.locations > 1
               OR (live.id IS NOT NULL AND retained.locations > 0)
        ) THEN
            RAISE EXCEPTION 'task identity exists in multiple locations';
        END IF;

        RETURN QUERY
        WITH input AS MATERIALIZED (
            SELECT item.task_id, item.claimed_at, item.ordinality
            FROM unnest(p_ids, p_claimed_ats) WITH ORDINALITY
                AS item(task_id, claimed_at, ordinality)
        ),
        live_context AS MATERIALIZED (
            SELECT input.task_id, input.claimed_at AS expected_claimed_at,
                   input.ordinality, task.*,
                   {namespace}.encode_live_attempts(task.id)
                       AS encoded_attempts,
                   CASE WHEN task.result IS NULL THEN NULL::bytea
                        ELSE convert_to(task.result, 'UTF8') END
                       AS encoded_result
            FROM input
            JOIN {namespace}.live_tasks AS task ON task.id = input.task_id
            FOR UPDATE OF task
        ),
        history_context AS MATERIALIZED (
            SELECT input.ordinality, history.*
            FROM input
            JOIN {namespace}.history_aggregate AS history
                ON history.task_id = input.task_id
        ),
        targets AS MATERIALIZED (
            SELECT live.*, node.workflow_id, node.node_id,
                   NOW() AS assigned_terminal_at
            FROM live_context AS live
            JOIN {namespace}.phase2_nodes AS node
                ON node.task_id = live.task_id
            WHERE live.status = 'CLAIMED'
              AND live.claimed_by_worker_id = CAST(p_worker_id AS varchar)
              AND (
                  live.expected_claimed_at IS NULL
                  OR live.claimed_at = live.expected_claimed_at
              )
              AND node.status IN {node_statuses}
            FOR UPDATE OF node
        ),
        history_rows AS (
            INSERT INTO {namespace}.history_aggregate (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, sent_at, enqueued_at, claimed_at,
                started_at, created_at,
                result_envelope_version, result_codec, result_payload,
                result_digest, error_code, final_failed_reason,
                retry_count, rerun_of_task_id, rerun_root_task_id,
                input_digest, workflow_id, is_workflow_task,
                history_schema_version, attempt_archive_version,
                attempt_snapshot_codec, attempt_snapshot,
                attempt_snapshot_digest
            )
            SELECT target.id, target.task_name, target.queue_name,
                   target.priority, 'CANCELLED', '{kind}',
                   target.assigned_terminal_at, target.assigned_terminal_at,
                   target.retention_class_key, target.sent_at,
                   target.enqueued_at, target.claimed_at, target.started_at,
                   target.created_at, 1, 'json-utf8', target.encoded_result,
                   CASE WHEN target.encoded_result IS NULL THEN NULL
                        ELSE sha256(target.encoded_result) END,
                   {error_projection}, {reason_projection},
                   target.retry_count, target.rerun_of_task_id,
                   target.rerun_root_task_id, target.input_digest,
                   target.workflow_id, TRUE,
                   1, 1, 'json-utf8', target.encoded_attempts,
                   sha256(target.encoded_attempts)
            FROM targets AS target
            RETURNING task_id, terminal_at, terminalization_kind
        ),
        node_updates AS (
            UPDATE {namespace}.phase2_nodes AS node
            SET {node_assignment}
            FROM targets AS target
            JOIN history_rows AS history ON history.task_id = target.id
            WHERE node.workflow_id = target.workflow_id
              AND node.node_id = target.node_id
              AND node.status IN {node_statuses}
            RETURNING target.id AS task_id
        ),
        ready AS MATERIALIZED (
            SELECT history.task_id
            FROM history_rows AS history
            JOIN node_updates AS node ON node.task_id = history.task_id
        ),
        purged_attempts AS (
            DELETE FROM {namespace}.live_attempts AS attempt
            WHERE attempt.task_id IN (SELECT task_id FROM ready)
            RETURNING attempt.task_id
        ),
        deleted_tasks AS (
            DELETE FROM {namespace}.live_tasks AS task
            WHERE task.id IN (SELECT task_id FROM ready)
              AND (SELECT count(*) FROM purged_attempts) >= 0
            RETURNING task.id
        ),
        notifications AS MATERIALIZED (
            SELECT ready.task_id,
                   {namespace}.emit_task_done(ready.task_id) AS emitted
            FROM ready
            JOIN deleted_tasks AS deleted ON deleted.id = ready.task_id
        )
        SELECT input.task_id, input.ordinality,
               CASE
                   WHEN deleted.id IS NOT NULL THEN 'APPLIED'
                   WHEN live.task_id IS NULL AND retained.task_id IS NULL
                       THEN 'TASK_ABSENT'
                   WHEN retained.task_id IS NOT NULL
                        AND retained.terminalization_kind = ANY(
                            ARRAY[{equivalent_array}]::text[]
                        ) THEN 'ALREADY_APPLIED'
                   WHEN retained.task_id IS NOT NULL
                       THEN 'SOURCE_STATE_CONFLICT'
                   WHEN live.claimed_by_worker_id
                            IS DISTINCT FROM CAST(p_worker_id AS varchar)
                        OR (
                            input.claimed_at IS NOT NULL
                            AND live.claimed_at
                                IS DISTINCT FROM input.claimed_at
                        ) THEN 'LOST_CLAIM'
                   ELSE 'SOURCE_STATE_CONFLICT'
               END::text,
               CASE WHEN deleted.id IS NOT NULL THEN inserted.terminal_at
                    ELSE retained.terminal_at END,
               CASE WHEN deleted.id IS NOT NULL
                        THEN inserted.terminalization_kind
                    ELSE retained.terminalization_kind END,
               COALESCE(live.status::text, retained.status),
               live.claimed_by_worker_id,
               COALESCE(live.claimed_at, retained.claimed_at),
               CASE WHEN retained.task_id IS NOT NULL
                          AND NOT ((
                              retained.terminalization_kind = ANY(
                                  ARRAY[{equivalent_array}]::text[]
                              )
                          ) IS TRUE)
                    THEN 'FOREIGN_TERMINALIZATION'::text
                    ELSE NULL::text END,
               NULL::jsonb
        FROM input
        LEFT JOIN live_context AS live
            ON live.ordinality = input.ordinality
        LEFT JOIN history_context AS retained
            ON retained.ordinality = input.ordinality
        LEFT JOIN history_rows AS inserted
            ON inserted.task_id = input.task_id
        LEFT JOIN deleted_tasks AS deleted ON deleted.id = input.task_id
        LEFT JOIN notifications AS notification
            ON notification.task_id = input.task_id
        ORDER BY input.ordinality;
    END
    $function$
    """


def _workflow_scoped_batch_function(namespace: str, *, pause: bool) -> str:
    if pause:
        function_name = 'horsies_abandon_nodes_of_paused_workflows'
        kind = TerminalizationKind.PAUSE_ABANDON_WORKFLOW.value
        workflow_status = 'PAUSED'
        task_statuses = "('CLAIMED')"
        node_statuses = "('ENQUEUED', 'RUNNING')"
        node_assignment = "status = 'READY', task_id = NULL"
        error_projection = "'TASK_CANCELLED'"
        reason_projection = "'Workflow paused before task start'"
    else:
        function_name = 'horsies_cancel_nodes_of_cancelled_workflow'
        kind = TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW.value
        workflow_status = 'CANCELLED'
        task_statuses = "('PENDING', 'CLAIMED', 'RUNNING')"
        node_statuses = "('ENQUEUED')"
        node_assignment = "status = 'SKIPPED'"
        error_projection = 'target.error_code'
        reason_projection = 'target.failed_reason'
    return f"""
    CREATE FUNCTION {namespace}.{function_name}(
        p_workflow_ids varchar[]
    ) RETURNS SETOF {namespace}.terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        IF EXISTS (
            SELECT 1
            FROM {namespace}.live_tasks AS task
            JOIN {namespace}.phase2_nodes AS node ON node.task_id = task.id
            JOIN {namespace}.phase2_workflows AS workflow
                ON workflow.workflow_id = node.workflow_id
            WHERE node.workflow_id = ANY(p_workflow_ids)
              AND workflow.status = '{workflow_status}'
              AND task.status::text IN {task_statuses}
              AND node.status IN {node_statuses}
              AND EXISTS (
                  SELECT 1 FROM {namespace}.history_aggregate AS history
                  WHERE history.task_id = task.id
              )
        ) THEN
            RAISE EXCEPTION 'task identity exists in multiple locations';
        END IF;

        RETURN QUERY
        WITH targets AS MATERIALIZED (
            SELECT task.*, node.workflow_id, node.node_id,
                   NOW() AS assigned_terminal_at,
                   {namespace}.encode_live_attempts(task.id)
                       AS encoded_attempts,
                   CASE WHEN task.result IS NULL THEN NULL::bytea
                        ELSE convert_to(task.result, 'UTF8') END
                       AS encoded_result
            FROM {namespace}.live_tasks AS task
            JOIN {namespace}.phase2_nodes AS node ON node.task_id = task.id
            JOIN {namespace}.phase2_workflows AS workflow
                ON workflow.workflow_id = node.workflow_id
            WHERE node.workflow_id = ANY(p_workflow_ids)
              AND workflow.status = '{workflow_status}'
              AND task.status::text IN {task_statuses}
              AND node.status IN {node_statuses}
            FOR UPDATE OF task, node
        ),
        history_rows AS (
            INSERT INTO {namespace}.history_aggregate (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, sent_at, enqueued_at, claimed_at,
                started_at, created_at,
                result_envelope_version, result_codec, result_payload,
                result_digest, error_code, final_failed_reason,
                retry_count, rerun_of_task_id, rerun_root_task_id,
                input_digest, workflow_id, is_workflow_task,
                history_schema_version, attempt_archive_version,
                attempt_snapshot_codec, attempt_snapshot,
                attempt_snapshot_digest
            )
            SELECT target.id, target.task_name, target.queue_name,
                   target.priority, 'CANCELLED', '{kind}',
                   target.assigned_terminal_at, target.assigned_terminal_at,
                   target.retention_class_key, target.sent_at,
                   target.enqueued_at, target.claimed_at, target.started_at,
                   target.created_at, 1, 'json-utf8', target.encoded_result,
                   CASE WHEN target.encoded_result IS NULL THEN NULL
                        ELSE sha256(target.encoded_result) END,
                   {error_projection}, {reason_projection},
                   target.retry_count, target.rerun_of_task_id,
                   target.rerun_root_task_id, target.input_digest,
                   target.workflow_id, TRUE,
                   1, 1, 'json-utf8', target.encoded_attempts,
                   sha256(target.encoded_attempts)
            FROM targets AS target
            RETURNING task_id, terminal_at, terminalization_kind
        ),
        node_updates AS (
            UPDATE {namespace}.phase2_nodes AS node
            SET {node_assignment}
            FROM targets AS target
            JOIN history_rows AS history ON history.task_id = target.id
            WHERE node.workflow_id = target.workflow_id
              AND node.node_id = target.node_id
              AND node.status IN {node_statuses}
            RETURNING target.id AS task_id
        ),
        ready AS MATERIALIZED (
            SELECT history.task_id
            FROM history_rows AS history
            JOIN node_updates AS node ON node.task_id = history.task_id
        ),
        purged_attempts AS (
            DELETE FROM {namespace}.live_attempts AS attempt
            WHERE attempt.task_id IN (SELECT task_id FROM ready)
            RETURNING attempt.task_id
        ),
        deleted_tasks AS (
            DELETE FROM {namespace}.live_tasks AS task
            WHERE task.id IN (SELECT task_id FROM ready)
              AND (SELECT count(*) FROM purged_attempts) >= 0
            RETURNING task.id
        ),
        notifications AS MATERIALIZED (
            SELECT ready.task_id,
                   {namespace}.emit_task_done(ready.task_id) AS emitted
            FROM ready
            JOIN deleted_tasks AS deleted ON deleted.id = ready.task_id
        )
        SELECT history.task_id, NULL::bigint, 'APPLIED'::text,
               history.terminal_at, history.terminalization_kind,
               target.status::text, target.claimed_by_worker_id,
               target.claimed_at, NULL::text, NULL::jsonb
        FROM history_rows AS history
        JOIN targets AS target ON target.id = history.task_id
        JOIN deleted_tasks AS deleted ON deleted.id = history.task_id
        JOIN notifications AS notification
            ON notification.task_id = history.task_id;
    END
    $function$
    """
