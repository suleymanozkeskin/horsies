"""The direct live-to-history move and the completion-family operations.

One shared move function performs the reconciled transaction: archive
availability probe, task-ID advisory lock, one strict locked live-row
snapshot, liveness and identity-uniqueness guards, eligibility-before-
policy classification with envelope carriage in its own block, the
canonical attempt snapshot in its own block, exactly one asserted history
insert, variant-derived phase-2 pending, the keyed-reservation window
start, asserted live deletes, and the transactional raw-ID notification.
Every family extends the projection-agreement CASE; the completion family
is implemented here and any other kind raises as unsupported until its
family lands.

The attempt encoder concatenates per-element `to_jsonb` renderings with
explicit separators — never a jsonb array's own text form, which inserts
spaces — so the bytes match the Python codec exactly; the unit fixtures
pinned in the archive suite are the parity authority.

All functions target the post-cutover shape: native uuid identifiers on
live, attempts, and history alike.

Global lock-order invariant, binding on every family: the task advisory
lock is acquired BEFORE any row lock on that task. Both wire functions
take the advisory lock as their first statement, ahead of their guard
select; the move re-acquires it, which is free — advisory transaction
locks are reentrant in-session. A family that row-locks first would
deadlock against any advisory-first caller on the same task.
"""

from __future__ import annotations

from typing import Final

from ...lifecycle.operations import TerminalizationKind
from ..maintenance.gate import ARCHIVE_AVAILABILITY_FUNCTION
from ..names import (
    LIVE_TASKS,
    TASK_HISTORY_PARENT,
    TASK_PROVENANCE_FUNCTION,
    WORKFLOW_PHASE2_PENDING,
)
from .outcome import MISS_CLASSIFIER_FUNCTION, OUTCOME_TYPE

LIVE_ATTEMPTS: Final = 'horsies_task_attempts'
ATTEMPT_ENCODER_FUNCTION: Final = 'horsies_encode_task_attempts'
MOVE_FUNCTION: Final = 'horsies_move_task_to_history'
COMPLETE_LOCKED_FUNCTION: Final = 'horsies_complete_locked_task'
COMPLETE_FUSED_FUNCTION: Final = 'horsies_complete_task_fused'
FAIL_LOCKED_FUNCTION: Final = 'horsies_fail_locked_task'
FAIL_STALE_FUNCTION: Final = 'horsies_fail_stale_task'

EXPIRE_OWNED_FUNCTION: Final = 'horsies_expire_owned_claim'
EXPIRE_PENDING_FUNCTION: Final = 'horsies_expire_pending_tasks'

_TASK_LOCK_SEED: Final = 731

# The disposition ladder, once. Both the single-row IF chain and the batch
# CASE expression render from these rungs; a second hand-written ladder is
# the drift class this program keeps catching, closed here at generation.
_LADDER_RUNGS: Final[tuple[tuple[str, str], ...]] = (
    ("{row}.is_workflow_task OR {status} = 'COMPLETED'", 'NEVER_ELIGIBLE'),
    ('NOT {row}.retain_rerun_input', 'DECLINED_BY_POLICY'),
)
_LADDER_FALLBACK: Final = '{row}.prepared_rerun_input_disposition'


def disposition_if_chain(row: str, status: str) -> str:
    """The ladder as a PL/pgSQL IF chain assigning v_rerun_disposition."""
    lines: list[str] = []
    keyword = 'IF'
    for condition, result in _LADDER_RUNGS:
        rendered = condition.format(row=row, status=status)
        lines.append(f"    {keyword} {rendered} THEN")
        lines.append(f"        v_rerun_disposition := '{result}';")
        keyword = 'ELSIF'
    fallback = _LADDER_FALLBACK.format(row=row, status=status)
    lines.append('    ELSE')
    lines.append(f'        v_rerun_disposition := {fallback};')
    lines.append('    END IF;')
    return '\n'.join(lines)


def disposition_case_expression(row: str, status: str) -> str:
    """The same ladder as one SQL CASE expression."""
    arms = '\n'.join(
        f"            WHEN {condition.format(row=row, status=status)} "
        f"THEN '{result}'"
        for condition, result in _LADDER_RUNGS
    )
    fallback = _LADDER_FALLBACK.format(row=row, status=status)
    return f"""CASE
{arms}
            ELSE {fallback}
        END"""


ATTEMPT_ENCODER_DDL = f"""
CREATE FUNCTION {ATTEMPT_ENCODER_FUNCTION}(p_task_id uuid)
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
    FROM {LIVE_ATTEMPTS} AS a
    WHERE a.task_id = p_task_id
$function$
"""


def _move_ddl() -> str:
    complete_locked = TerminalizationKind.COMPLETE_LOCKED.value
    complete_fused = TerminalizationKind.COMPLETE_FUSED.value
    fail_running = TerminalizationKind.FAIL_RUNNING.value
    fail_stale = TerminalizationKind.FAIL_STALE.value
    expire_claimed = TerminalizationKind.EXPIRE_CLAIMED.value
    return f"""
CREATE FUNCTION {MOVE_FUNCTION}(
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
    v_task {LIVE_TASKS}%ROWTYPE;
    v_attempt_snapshot bytea;
    v_result_payload bytea;
    v_prior_result_payload bytea;
    v_workflow_id uuid;
    v_workflow_node_row_id uuid;
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
    PERFORM {ARCHIVE_AVAILABILITY_FUNCTION}();

    CASE p_terminalization_kind
        WHEN '{complete_locked}' THEN
            IF p_terminal_status <> 'COMPLETED' THEN
                RAISE EXCEPTION 'completion-locked projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        WHEN '{complete_fused}' THEN
            IF p_terminal_status <> 'COMPLETED' THEN
                RAISE EXCEPTION 'completion-fused projection disagrees';
            END IF;
            v_requires_deferred_phase2 := FALSE;
        WHEN '{fail_running}' THEN
            IF p_terminal_status <> 'FAILED' THEN
                RAISE EXCEPTION 'running-failure projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        WHEN '{fail_stale}' THEN
            IF p_terminal_status <> 'FAILED' THEN
                RAISE EXCEPTION 'stale-failure projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        WHEN '{expire_claimed}' THEN
            IF p_terminal_status <> 'EXPIRED' THEN
                RAISE EXCEPTION 'claimed-expiry projection disagrees';
            END IF;
            v_requires_deferred_phase2 := TRUE;
        ELSE
            RAISE EXCEPTION
                'terminalization kind % has no move family yet',
                p_terminalization_kind
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );

    SELECT * INTO STRICT v_task
    FROM {LIVE_TASKS}
    WHERE id = p_task_id
    FOR UPDATE;
    IF v_task.status NOT IN ('PENDING', 'CLAIMED', 'RUNNING') THEN
        RAISE EXCEPTION 'live task has non-live status %', v_task.status;
    END IF;
    IF p_terminal_at IS NULL THEN
        RAISE EXCEPTION 'terminal timestamp is required';
    END IF;
    IF (SELECT found FROM {TASK_PROVENANCE_FUNCTION}(p_task_id, FALSE)) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF v_task.is_workflow_task THEN
        IF p_terminalization_kind = '{complete_fused}' THEN
            RAISE EXCEPTION
                'operation cannot terminalize a workflow task'
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        SELECT n.id, n.workflow_id
        INTO STRICT v_workflow_node_row_id, v_workflow_id
        FROM horsies_workflow_tasks AS n
        WHERE n.task_id = p_task_id
        FOR UPDATE;
        IF v_requires_deferred_phase2 AND p_result IS NULL THEN
            RAISE EXCEPTION
                'deferred workflow terminalization requires a result payload'
                USING ERRCODE = 'not_null_violation';
        END IF;
    END IF;

    -- Rerun-input carriage: eligibility before policy, bytes copied and
    -- never re-encoded, the digest copied and never recomputed. This block
    -- is the whole envelope decision, rendered from the shared ladder.
{disposition_if_chain('v_task', 'p_terminal_status')}
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
    v_attempt_snapshot := {ATTEMPT_ENCODER_FUNCTION}(p_task_id);

    v_result_payload := CASE
        WHEN p_result IS NULL THEN NULL
        ELSE convert_to(p_result, 'UTF8')
    END;
    v_prior_result_payload := NULL;

    INSERT INTO {TASK_HISTORY_PARENT} (
        task_id, task_name, queue_name, priority,
        command_fingerprint_version, command_fingerprint, status,
        terminalization_kind, terminal_at, retention_anchor_at,
        retention_class_key, sent_at, enqueued_at, claimed_at,
        started_at, created_at, good_until,
        result_envelope_version, result_codec, result_content_type,
        result_payload, prior_result_payload, result_digest,
        error_code, final_failed_reason,
        retry_count, max_retries,
        last_claimed_worker_id, last_worker_hostname,
        last_worker_pid, last_worker_process_name,
        input_digest, rerun_of_task_id, rerun_root_task_id,
        workflow_id, is_workflow_task, history_schema_version,
        attempt_archive_version, attempt_snapshot_codec,
        attempt_snapshot_content_type, attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition, rerun_input_version, rerun_input_codec,
        rerun_input_content_type, rerun_input_digest,
        rerun_input_inline, rerun_input_reference
    ) VALUES (
        v_task.id, v_task.task_name, v_task.queue_name, v_task.priority,
        v_task.command_fingerprint_version, v_task.command_fingerprint,
        p_terminal_status, p_terminalization_kind,
        p_terminal_at, p_terminal_at, v_task.retention_class_key,
        v_task.sent_at, v_task.enqueued_at, v_task.claimed_at,
        v_task.started_at, v_task.created_at, v_task.good_until,
        1, 'json-utf8', 'application/json',
        v_result_payload, v_prior_result_payload,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        p_error_code, p_failed_reason,
        v_task.retry_count, v_task.max_retries,
        v_task.claimed_by_worker_id, v_task.worker_hostname,
        v_task.worker_pid, v_task.worker_process_name,
        v_task.input_digest, v_task.rerun_of_task_id,
        v_task.rerun_root_task_id,
        v_workflow_id, v_task.is_workflow_task, 1,
        1, 'json-utf8', 'application/json', v_attempt_snapshot,
        sha256(v_attempt_snapshot),
        v_rerun_disposition, v_rerun_version, v_rerun_codec,
        v_rerun_content_type, v_rerun_digest,
        v_rerun_inline, v_rerun_reference
    );
    GET DIAGNOSTICS v_history_rows = ROW_COUNT;
    IF v_history_rows <> 1 THEN
        RAISE EXCEPTION 'terminal history insert did not affect one row';
    END IF;

    IF v_requires_deferred_phase2 AND v_task.is_workflow_task THEN
        INSERT INTO {WORKFLOW_PHASE2_PENDING} (
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

    DELETE FROM {LIVE_ATTEMPTS} WHERE task_id = p_task_id;
    DELETE FROM {LIVE_TASKS} WHERE id = p_task_id;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;
    IF v_deleted_rows <> 1 THEN
        RAISE EXCEPTION 'live task delete did not affect one row';
    END IF;
    PERFORM pg_notify('task_done', p_task_id::text);
END
$function$
"""


def _complete_locked_ddl() -> str:
    kind = TerminalizationKind.COMPLETE_LOCKED.value
    return f"""
CREATE FUNCTION {COMPLETE_LOCKED_FUNCTION}(
    p_task_id uuid,
    p_worker_id text,
    p_result text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );
    SELECT claimed_at INTO v_claimed_at
    FROM {LIVE_TASKS}
    WHERE id = p_task_id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
            p_task_id, ARRAY['{kind}']::text[],
            p_worker_id, NULL::timestamptz
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM {MOVE_FUNCTION}(
        p_task_id, 'COMPLETED', '{kind}', v_terminal_at,
        p_result, NULL, NULL
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, '{kind}'::text,
        'RUNNING'::text, CAST(p_worker_id AS VARCHAR), v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$
"""


def _complete_fused_ddl() -> str:
    kind = TerminalizationKind.COMPLETE_FUSED.value
    return f"""
CREATE FUNCTION {COMPLETE_FUSED_FUNCTION}(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamptz,
    p_result text,
    p_notify_channel text,
    p_notify_payload text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_ctx record;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );
    SELECT id, retry_count, started_at, claimed_by_worker_id, claimed_at,
           worker_hostname, worker_pid, worker_process_name,
           clock_timestamp() AS db_now
    INTO v_ctx
    FROM {LIVE_TASKS}
    WHERE id = p_task_id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND (p_claimed_at IS NULL OR claimed_at = p_claimed_at)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
            p_task_id, ARRAY['{kind}']::text[],
            p_worker_id, p_claimed_at
        );
        RETURN;
    END IF;

    INSERT INTO {LIVE_ATTEMPTS} (
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
    PERFORM {MOVE_FUNCTION}(
        p_task_id, 'COMPLETED', '{kind}', v_terminal_at,
        p_result, NULL, NULL
    );
    PERFORM pg_notify(p_notify_channel, p_notify_payload);
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, '{kind}'::text,
        'RUNNING'::text, v_ctx.claimed_by_worker_id, v_ctx.claimed_at,
        NULL::text, NULL::jsonb;
END
$function$
"""


def _fail_locked_ddl() -> str:
    kind = TerminalizationKind.FAIL_RUNNING.value
    return f"""
CREATE FUNCTION {FAIL_LOCKED_FUNCTION}(
    p_task_id uuid,
    p_worker_id text,
    p_result text,
    p_error_code text,
    p_failed_reason text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );
    SELECT claimed_at INTO v_claimed_at
    FROM {LIVE_TASKS}
    WHERE id = p_task_id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
            p_task_id, ARRAY['{kind}']::text[],
            p_worker_id, NULL::timestamptz
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM {MOVE_FUNCTION}(
        p_task_id, 'FAILED', '{kind}', v_terminal_at,
        p_result, p_error_code, p_failed_reason
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, '{kind}'::text,
        'RUNNING'::text, CAST(p_worker_id AS VARCHAR), v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$
"""


def _fail_stale_ddl() -> str:
    kind = TerminalizationKind.FAIL_STALE.value
    return f"""
CREATE FUNCTION {FAIL_STALE_FUNCTION}(
    p_task_id uuid,
    p_stale_after_ms integer,
    p_finalizing_stale_after_ms integer,
    p_result text,
    p_error_code text,
    p_failed_reason text
)
RETURNS SETOF {OUTCOME_TYPE}
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
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );
    -- One capture: the row locked, the heartbeat read beside it, and the
    -- instant both arms are judged at. Nothing is reread after a refusal,
    -- so the evidence cannot show a heartbeat the guard never saw.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at,
           t.started_at, t.finalizing_at,
           (
               SELECT h.sent_at
               FROM horsies_heartbeats h
               WHERE h.task_id = t.id AND h.role = 'runner'
               ORDER BY h.sent_at DESC
               LIMIT 1
           ),
           NOW()
    INTO v_status, v_worker, v_claimed_at, v_started_at, v_finalizing_at,
         v_last_heartbeat, v_evaluated_at
    FROM {LIVE_TASKS} t
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
            PERFORM {MOVE_FUNCTION}(
                p_task_id, 'FAILED', '{kind}', v_terminal_at,
                p_result, p_error_code, p_failed_reason
            );
            -- Cross-worker by design: the observed claim is whichever
            -- worker's silence the guard just judged, from the capture.
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, '{kind}'::text,
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

    RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
        p_task_id, ARRAY['{kind}']::text[],
        NULL::text, NULL::timestamptz
    );
END
$function$
"""


def _expire_owned_ddl() -> str:
    kind = TerminalizationKind.EXPIRE_CLAIMED.value
    return f"""
CREATE FUNCTION {EXPIRE_OWNED_FUNCTION}(
    p_task_id uuid,
    p_worker_id text,
    p_result text,
    p_error_code text
)
RETURNS SETOF {OUTCOME_TYPE}
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
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );
    -- One locked capture judges the deadline. Every good_until writer
    -- mutates this row and therefore needs the lock this transaction now
    -- holds, so the production retry-under-lock loop has no race to serve
    -- and is deliberately not ported.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at,
           t.good_until, NOW()
    INTO v_status, v_worker, v_claimed_at, v_good_until, v_evaluated_at
    FROM {LIVE_TASKS} t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF FOUND
       AND v_status = 'CLAIMED'
       AND v_worker = CAST(p_worker_id AS VARCHAR) THEN
        IF v_good_until IS NOT NULL AND v_good_until <= v_evaluated_at THEN
            v_terminal_at := NOW();
            PERFORM {MOVE_FUNCTION}(
                p_task_id, 'EXPIRED', '{kind}', v_terminal_at,
                p_result, p_error_code, NULL
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, '{kind}'::text,
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

    RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
        p_task_id, ARRAY['{kind}']::text[],
        p_worker_id, NULL::timestamptz
    );
END
$function$
"""


def _expire_pending_ddl() -> str:
    kind = TerminalizationKind.EXPIRE_PENDING.value
    disposition_case = disposition_case_expression('t', "'EXPIRED'")
    return f"""
CREATE FUNCTION {EXPIRE_PENDING_FUNCTION}(
    p_batch_size integer,
    p_result text,
    p_error_code text
)
RETURNS SETOF {OUTCOME_TYPE}
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
    PERFORM {ARCHIVE_AVAILABILITY_FUNCTION}();

    -- Discovery under the batch locking rule: SKIP LOCKED never waits on
    -- a row lock, so this batch cannot join any deadlock cycle; rows held
    -- by advisory-first singles are skipped and caught next sweep.
    -- good_until order keeps the sweep oldest-first.
    SELECT array_agg(s.id) INTO v_ids
    FROM (
        SELECT id FROM {LIVE_TASKS}
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

    -- Set-wise deferred-result fence: no selected workflow-backing row may
    -- reach sha256(NULL).
    IF p_result IS NULL AND EXISTS (
        SELECT 1 FROM {LIVE_TASKS} t
        WHERE t.id = ANY(v_ids) AND t.is_workflow_task
    ) THEN
        RAISE EXCEPTION
            'deferred workflow terminalization requires a result payload'
            USING ERRCODE = 'not_null_violation';
    END IF;
    -- One node row per workflow-backing task, set-wise.
    IF EXISTS (
        SELECT 1 FROM {LIVE_TASKS} t
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
        WHERE (SELECT found FROM {TASK_PROVENANCE_FUNCTION}(u.tid, FALSE))
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN p_result IS NULL THEN NULL
        ELSE convert_to(p_result, 'UTF8')
    END;

    INSERT INTO {TASK_HISTORY_PARENT} (
        task_id, task_name, queue_name, priority,
        command_fingerprint_version, command_fingerprint, status,
        terminalization_kind, terminal_at, retention_anchor_at,
        retention_class_key, sent_at, enqueued_at, claimed_at,
        started_at, created_at, good_until,
        result_envelope_version, result_codec, result_content_type,
        result_payload, prior_result_payload, result_digest,
        error_code, final_failed_reason,
        retry_count, max_retries,
        last_claimed_worker_id, last_worker_hostname,
        last_worker_pid, last_worker_process_name,
        input_digest, rerun_of_task_id, rerun_root_task_id,
        workflow_id, is_workflow_task, history_schema_version,
        attempt_archive_version, attempt_snapshot_codec,
        attempt_snapshot_content_type, attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition, rerun_input_version, rerun_input_codec,
        rerun_input_content_type, rerun_input_digest,
        rerun_input_inline, rerun_input_reference
    )
    SELECT
        t.id, t.task_name, t.queue_name, t.priority,
        t.command_fingerprint_version, t.command_fingerprint,
        'EXPIRED', '{kind}',
        v_terminal_at, v_terminal_at, t.retention_class_key,
        t.sent_at, t.enqueued_at, t.claimed_at,
        t.started_at, t.created_at, t.good_until,
        1, 'json-utf8', 'application/json',
        v_result_payload, NULL,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        p_error_code, NULL,
        t.retry_count, t.max_retries,
        t.claimed_by_worker_id, t.worker_hostname,
        t.worker_pid, t.worker_process_name,
        t.input_digest, t.rerun_of_task_id, t.rerun_root_task_id,
        n.workflow_id, t.is_workflow_task, 1,
        1, 'json-utf8', 'application/json',
        {ATTEMPT_ENCODER_FUNCTION}(t.id),
        sha256({ATTEMPT_ENCODER_FUNCTION}(t.id)),
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
    FROM {LIVE_TASKS} t
    LEFT JOIN horsies_workflow_tasks n ON n.task_id = t.id
    CROSS JOIN LATERAL (
        SELECT {disposition_case} AS disposition
    ) d
    WHERE t.id = ANY(v_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality(v_ids);
    END IF;

    INSERT INTO {WORKFLOW_PHASE2_PENDING} (
        task_id, workflow_id, workflow_node_row_id,
        terminal_status, terminal_at, terminalization_kind,
        recovery_source, history_class, history_anchor,
        history_schema_version, result_digest,
        phase2_generation, created_at, attempt_count
    )
    SELECT
        t.id, n.workflow_id, n.id,
        'EXPIRED', v_terminal_at, '{kind}',
        'HISTORY', t.retention_class_key, v_terminal_at,
        1, sha256(v_result_payload),
        gen_random_uuid(), statement_timestamp(), 0
    FROM {LIVE_TASKS} t
    JOIN horsies_workflow_tasks n ON n.task_id = t.id
    WHERE t.id = ANY(v_ids) AND t.is_workflow_task;

    UPDATE horsies_key_reservations r
    SET disposition = 'TERMINAL',
        expires_at = v_terminal_at + r.reservation_window
    FROM {LIVE_TASKS} t
    WHERE t.id = ANY(v_ids)
      AND t.idempotency_key_digest IS NOT NULL
      AND r.idempotency_key_digest = t.idempotency_key_digest
      AND r.task_id = t.id
      AND r.disposition = 'LIVE';

    -- Outcome rows stream from the still-locked live rows BEFORE the
    -- deletes: reading them back through the partitioned parent by
    -- task id would be the rejected fan-out mechanism. RETURN QUERY
    -- materializes immediately; it does not end the function.
    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, '{kind}'::text,
        'PENDING'::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM {LIVE_TASKS} t
    WHERE t.id = ANY(v_ids);

    DELETE FROM {LIVE_ATTEMPTS} WHERE task_id = ANY(v_ids);
    DELETE FROM {LIVE_TASKS} WHERE id = ANY(v_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality(v_ids);
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest(v_ids) AS u(tid);
END
$function$
"""


def expiry_family_fragments() -> tuple[str, ...]:
    """The expiry-family wire functions, in installation order."""
    return (_expire_owned_ddl(), _expire_pending_ddl())


def failure_family_fragments() -> tuple[str, ...]:
    """The failure-family wire functions, in installation order."""
    return (_fail_locked_ddl(), _fail_stale_ddl())


def completion_family_fragments() -> tuple[str, ...]:
    """Encoder, move, and completion operations, in installation order."""
    return (
        ATTEMPT_ENCODER_DDL,
        _move_ddl(),
        _complete_locked_ddl(),
        _complete_fused_ddl(),
    )
