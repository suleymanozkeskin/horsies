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
        _attempt_snapshot_function(namespace),
        _move_function(namespace),
        _miss_function(namespace),
        _complete_locked_function(namespace),
        _complete_fused_function(namespace),
        _fail_locked_function(namespace),
        _fail_stale_function(namespace),
        _expire_owned_function(namespace),
        _expire_pending_function(namespace),
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


def _attempt_snapshot_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.encode_live_attempts(p_task_id varchar)
    RETURNS bytea
    LANGUAGE sql
    STABLE
    STRICT
    AS $function$
        SELECT convert_to(
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'attempt', a.attempt,
                        'outcome', a.outcome,
                        'will_retry', a.will_retry,
                        'started_at', a.started_at,
                        'finished_at', a.finished_at,
                        'error_code', a.error_code,
                        'error_message', a.error_message,
                        'failed_reason', a.failed_reason,
                        'worker_id', a.worker_id,
                        'worker_hostname', a.worker_hostname,
                        'worker_pid', a.worker_pid,
                        'worker_process_name', a.worker_process_name
                    ) ORDER BY a.attempt
                ),
                '[]'::jsonb
            )::text,
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
        v_workflow_id varchar(36);
        v_node_id text;
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
            IF NOT v_requires_deferred_phase2 THEN
                RAISE EXCEPTION
                    'fused completion cannot terminalize a workflow task'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            SELECT n.workflow_id, n.node_id
            INTO v_workflow_id, v_node_id
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
        v_result_payload := CASE
            WHEN p_result IS NULL THEN NULL
            ELSE convert_to(p_result, 'UTF8')
        END;

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
        ) VALUES (
            v_task.id, v_task.task_name, v_task.queue_name, v_task.priority,
            p_terminal_status, p_terminalization_kind,
            p_terminal_at, p_terminal_at, v_task.retention_class_key,
            v_task.sent_at, v_task.enqueued_at, v_task.claimed_at,
            v_task.started_at, v_task.created_at,
            1, 'json-utf8', v_result_payload,
            CASE WHEN v_result_payload IS NULL
                 THEN NULL ELSE sha256(v_result_payload) END,
            p_error_code, p_failed_reason,
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
                task_id, workflow_id, node_id, task_name,
                terminal_status, terminal_at, terminalization_kind,
                recovery_source, history_class, history_anchor,
                history_schema_version, result_digest,
                phase2_generation, created_at
            ) VALUES (
                v_task.id, v_workflow_id, v_node_id, v_task.task_name,
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
        PERFORM pg_notify('task_done', p_task_id);
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
            SELECT task.*, node.workflow_id, node.node_id,
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
                task_id, workflow_id, node_id, task_name,
                terminal_status, terminal_at, terminalization_kind,
                recovery_source, history_class, history_anchor,
                history_schema_version, result_digest,
                phase2_generation, created_at
            )
            SELECT target.id, target.workflow_id, target.node_id,
                   target.task_name, 'EXPIRED', history.terminal_at,
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
                   pg_notify('task_done', ready.task_id) AS emitted
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
