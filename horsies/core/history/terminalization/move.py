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
CANCEL_LOCKED_FUNCTION: Final = 'horsies_cancel_locked_task'
CANCEL_ORPHAN_SWEEP_FUNCTION: Final = 'horsies_cancel_orphaned_tasks'
ABANDON_OWNED_NODE_FUNCTION: Final = 'horsies_abandon_owned_node'
ABANDON_OWNED_NODES_FUNCTION: Final = 'horsies_abandon_owned_nodes'
ABANDON_PAUSED_SWEEP_FUNCTION: Final = (
    'horsies_abandon_nodes_of_paused_workflows'
)
CANCEL_OWNED_NODE_FUNCTION: Final = 'horsies_cancel_owned_node'
CANCEL_OWNED_NODES_FUNCTION: Final = 'horsies_cancel_owned_nodes'
CANCEL_CANCELLED_SWEEP_FUNCTION: Final = (
    'horsies_cancel_nodes_of_cancelled_workflow'
)
CANCEL_ORPHAN_FUNCTION: Final = 'horsies_cancel_owned_orphan'
COMPLETE_LOCKED_FUNCTION: Final = 'horsies_complete_locked_task'
COMPLETE_FUSED_FUNCTION: Final = 'horsies_complete_task_fused'
FAIL_LOCKED_FUNCTION: Final = 'horsies_fail_locked_task'
FAIL_STALE_FUNCTION: Final = 'horsies_fail_stale_task'

EXPIRE_OWNED_FUNCTION: Final = 'horsies_expire_owned_claim'
EXPIRE_PENDING_FUNCTION: Final = 'horsies_expire_pending_tasks'

_TASK_LOCK_SEED: Final = 731

# The kind classification, once: terminal status, projection-agreement
# message, deferred-phase-2 ownership, and — derived from deferral — the
# node-linkage lookup shape. The move's CASE renders from this table, so a
# future family chooses its classification explicitly here rather than
# inheriting a lookup shape by copy-paste.
KIND_PROJECTIONS: Final[
    tuple[tuple[TerminalizationKind, str, str, bool], ...]
] = (
    (
        TerminalizationKind.COMPLETE_LOCKED,
        'COMPLETED',
        'completion-locked projection disagrees',
        True,
    ),
    (
        TerminalizationKind.COMPLETE_FUSED,
        'COMPLETED',
        'completion-fused projection disagrees',
        False,
    ),
    (
        TerminalizationKind.FAIL_RUNNING,
        'FAILED',
        'running-failure projection disagrees',
        True,
    ),
    (
        TerminalizationKind.FAIL_STALE,
        'FAILED',
        'stale-failure projection disagrees',
        True,
    ),
    (
        TerminalizationKind.EXPIRE_CLAIMED,
        'EXPIRED',
        'claimed-expiry projection disagrees',
        True,
    ),
    (
        TerminalizationKind.CANCEL_ADMIN,
        'CANCELLED',
        'administrative-cancel projection disagrees',
        False,
    ),
    (
        TerminalizationKind.CANCEL_ORPHAN,
        'CANCELLED',
        'orphan-cancel projection disagrees',
        False,
    ),
    (
        TerminalizationKind.PAUSE_ABANDON_CLAIM,
        'CANCELLED',
        'pause-abandon projection disagrees',
        False,
    ),
    (
        TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH,
        'CANCELLED',
        'pause-abandon-batch projection disagrees',
        False,
    ),
    (
        TerminalizationKind.PAUSE_ABANDON_WORKFLOW,
        'CANCELLED',
        'paused-workflow-sweep projection disagrees',
        False,
    ),
    (
        TerminalizationKind.WORKFLOW_CANCEL_CLAIM,
        'CANCELLED',
        'workflow-cancel projection disagrees',
        False,
    ),
    (
        TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH,
        'CANCELLED',
        'workflow-cancel-batch projection disagrees',
        False,
    ),
    (
        TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW,
        'CANCELLED',
        'cancelled-workflow-sweep projection disagrees',
        False,
    ),
)
# Kinds whose wire contract forbids workflow-backing tasks entirely.
_WORKFLOW_REJECTING_KINDS: Final[tuple[TerminalizationKind, ...]] = (
    TerminalizationKind.COMPLETE_FUSED,
    TerminalizationKind.CANCEL_ADMIN,
)


HISTORY_PROJECTION_COLUMNS: Final[tuple[str, ...]] = (
    'task_id', 'task_name', 'queue_name', 'priority',
    'command_fingerprint_version', 'command_fingerprint', 'status',
    'terminalization_kind', 'terminal_at', 'retention_anchor_at',
    'retention_class_key', 'sent_at', 'enqueued_at', 'claimed_at',
    'started_at', 'created_at', 'good_until',
    'result_envelope_version', 'result_codec', 'result_content_type',
    'result_payload', 'prior_result_payload', 'result_digest',
    'error_code', 'final_failed_reason',
    'retry_count', 'max_retries',
    'last_claimed_worker_id', 'last_worker_hostname',
    'last_worker_pid', 'last_worker_process_name',
    'input_digest', 'rerun_of_task_id', 'rerun_root_task_id',
    'workflow_id', 'is_workflow_task', 'history_schema_version',
    'attempt_archive_version', 'attempt_snapshot_codec',
    'attempt_snapshot_content_type', 'attempt_snapshot',
    'attempt_snapshot_digest',
    'rerun_input_disposition', 'rerun_input_version', 'rerun_input_codec',
    'rerun_input_content_type', 'rerun_input_digest',
    'rerun_input_inline', 'rerun_input_reference',
)
"""The history-insert projection, once. Every writer of a history row —
the single-row move, the batch families, and the cutover relocation —
renders its INSERT from this list, so a column can never be added to
one writer and silently dropped from another."""


def history_insert_sql(
    values: dict[str, str],
    *,
    select_tail: str | None = None,
) -> str:
    """Render the history INSERT from the single projection authority.

    `values` must supply exactly one SQL expression per projection
    column — a missing or extra key raises at render (import) time,
    which is where a drifted writer should fail. With `select_tail`
    the statement is the set-based INSERT ... SELECT ... {tail};
    without it, the single-row INSERT ... VALUES form.
    """
    missing = [c for c in HISTORY_PROJECTION_COLUMNS if c not in values]
    extra = [c for c in values if c not in HISTORY_PROJECTION_COLUMNS]
    if missing or extra:
        raise ValueError(
            f'history projection mismatch: missing={missing} extra={extra}'
        )
    columns = ',\n        '.join(HISTORY_PROJECTION_COLUMNS)
    expressions = ',\n        '.join(
        values[column] for column in HISTORY_PROJECTION_COLUMNS
    )
    if select_tail is None:
        return (
            f'INSERT INTO {TASK_HISTORY_PARENT} (\n'
            f'        {columns}\n'
            f'    ) VALUES (\n'
            f'        {expressions}\n'
            f'    )'
        )
    return (
        f'INSERT INTO {TASK_HISTORY_PARENT} (\n'
        f'        {columns}\n'
        f'    )\n'
        f'    SELECT\n'
        f'        {expressions}\n'
        f'    {select_tail}'
    )


def _projection_case() -> str:
    arms: list[str] = []
    for kind, status, message, deferred in KIND_PROJECTIONS:
        arms.append(f"        WHEN '{kind.value}' THEN")
        arms.append(f"            IF p_terminal_status <> '{status}' THEN")
        arms.append(f"                RAISE EXCEPTION '{message}';")
        arms.append('            END IF;')
        arms.append(
            f'            v_requires_deferred_phase2 := '
            f'{"TRUE" if deferred else "FALSE"};'
        )
    return '\n'.join(arms)


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


def _single_row_history_insert() -> str:
    """The single-row move's projection, over its declared variables."""
    return history_insert_sql({
        'task_id': 'v_task.id',
        'task_name': 'v_task.task_name',
        'queue_name': 'v_task.queue_name',
        'priority': 'v_task.priority',
        'command_fingerprint_version': 'v_task.command_fingerprint_version',
        'command_fingerprint': 'v_task.command_fingerprint',
        'status': 'p_terminal_status',
        'terminalization_kind': 'p_terminalization_kind',
        'terminal_at': 'p_terminal_at',
        'retention_anchor_at': 'p_terminal_at',
        'retention_class_key': 'v_task.retention_class_key',
        'sent_at': 'v_task.sent_at',
        'enqueued_at': 'v_task.enqueued_at',
        'claimed_at': 'v_task.claimed_at',
        'started_at': 'v_task.started_at',
        'created_at': 'v_task.created_at',
        'good_until': 'v_task.good_until',
        'result_envelope_version': '1',
        'result_codec': "'json-utf8'",
        'result_content_type': "'application/json'",
        'result_payload': 'v_result_payload',
        'prior_result_payload': 'v_prior_result_payload',
        'result_digest': (
            'CASE WHEN v_result_payload IS NOT NULL\n'
            '                 THEN sha256(v_result_payload)\n'
            '             WHEN v_prior_result_payload IS NOT NULL\n'
            '                 THEN sha256(v_prior_result_payload)\n'
            '             ELSE NULL END'
        ),
        'error_code': 'p_error_code',
        'final_failed_reason': 'p_failed_reason',
        'retry_count': 'v_task.retry_count',
        'max_retries': 'v_task.max_retries',
        'last_claimed_worker_id': 'v_task.claimed_by_worker_id',
        'last_worker_hostname': 'v_task.worker_hostname',
        'last_worker_pid': 'v_task.worker_pid',
        'last_worker_process_name': 'v_task.worker_process_name',
        'input_digest': 'v_task.input_digest',
        'rerun_of_task_id': 'v_task.rerun_of_task_id',
        'rerun_root_task_id': 'v_task.rerun_root_task_id',
        'workflow_id': 'v_workflow_id',
        'is_workflow_task': 'v_task.is_workflow_task',
        'history_schema_version': '1',
        'attempt_archive_version': '1',
        'attempt_snapshot_codec': "'json-utf8'",
        'attempt_snapshot_content_type': "'application/json'",
        'attempt_snapshot': 'v_attempt_snapshot',
        'attempt_snapshot_digest': 'sha256(v_attempt_snapshot)',
        'rerun_input_disposition': 'v_rerun_disposition',
        'rerun_input_version': 'v_rerun_version',
        'rerun_input_codec': 'v_rerun_codec',
        'rerun_input_content_type': 'v_rerun_content_type',
        'rerun_input_digest': 'v_rerun_digest',
        'rerun_input_inline': 'v_rerun_inline',
        'rerun_input_reference': 'v_rerun_reference',
    })


def _move_ddl() -> str:
    cancel_admin = TerminalizationKind.CANCEL_ADMIN.value
    rejecting = ', '.join(
        f"'{kind.value}'" for kind in _WORKFLOW_REJECTING_KINDS
    )
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
    PERFORM {ARCHIVE_AVAILABILITY_FUNCTION}();

    CASE p_terminalization_kind
{_projection_case()}
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
    IF (SELECT prov.found
        FROM {TASK_PROVENANCE_FUNCTION}(p_task_id, FALSE) AS prov) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF v_task.is_workflow_task THEN
        IF p_terminalization_kind IN ({rejecting}) THEN
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

    -- Result block, including the ratified administrative-cancel swap:
    -- canonical result is null and the pre-cancellation output is
    -- retained only as the separately named prior payload, copied from
    -- the locked live row and never re-encoded.
    IF p_terminalization_kind = '{cancel_admin}' THEN
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

    {_single_row_history_insert()};
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


def rerun_carriage_expression(column: str) -> str:
    """One envelope-carriage column, gated on the shared ladder's verdict."""
    return (
        "CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')\n"
        f'             THEN t.prepared_rerun_input_{column} END'
    )


def _batch_history_insert(
    *,
    status: str,
    kind: TerminalizationKind,
    error_expression: str,
    reason_expression: str,
    linkage_join: str,
    disposition_case: str,
    ids_var: str,
) -> str:
    """The batch families' projection, over the shared column authority."""
    return history_insert_sql(
        {
            'task_id': 't.id',
            'task_name': 't.task_name',
            'queue_name': 't.queue_name',
            'priority': 't.priority',
            'command_fingerprint_version': 't.command_fingerprint_version',
            'command_fingerprint': 't.command_fingerprint',
            'status': f"'{status}'",
            'terminalization_kind': f"'{kind.value}'",
            'terminal_at': 'v_terminal_at',
            'retention_anchor_at': 'v_terminal_at',
            'retention_class_key': 't.retention_class_key',
            'sent_at': 't.sent_at',
            'enqueued_at': 't.enqueued_at',
            'claimed_at': 't.claimed_at',
            'started_at': 't.started_at',
            'created_at': 't.created_at',
            'good_until': 't.good_until',
            'result_envelope_version': '1',
            'result_codec': "'json-utf8'",
            'result_content_type': "'application/json'",
            'result_payload': 'v_result_payload',
            'prior_result_payload': 'NULL',
            'result_digest': (
                'CASE WHEN v_result_payload IS NULL THEN NULL\n'
                '             ELSE sha256(v_result_payload) END'
            ),
            'error_code': error_expression,
            'final_failed_reason': reason_expression,
            'retry_count': 't.retry_count',
            'max_retries': 't.max_retries',
            'last_claimed_worker_id': 't.claimed_by_worker_id',
            'last_worker_hostname': 't.worker_hostname',
            'last_worker_pid': 't.worker_pid',
            'last_worker_process_name': 't.worker_process_name',
            'input_digest': 't.input_digest',
            'rerun_of_task_id': 't.rerun_of_task_id',
            'rerun_root_task_id': 't.rerun_root_task_id',
            'workflow_id': (
                'CASE WHEN t.is_workflow_task THEN n.workflow_id END'
            ),
            'is_workflow_task': 't.is_workflow_task',
            'history_schema_version': '1',
            'attempt_archive_version': '1',
            'attempt_snapshot_codec': "'json-utf8'",
            'attempt_snapshot_content_type': "'application/json'",
            'attempt_snapshot': f'{ATTEMPT_ENCODER_FUNCTION}(t.id)',
            'attempt_snapshot_digest': (
                f'sha256({ATTEMPT_ENCODER_FUNCTION}(t.id))'
            ),
            'rerun_input_disposition': 'd.disposition',
            'rerun_input_version': rerun_carriage_expression('version'),
            'rerun_input_codec': rerun_carriage_expression('codec'),
            'rerun_input_content_type': rerun_carriage_expression('content_type'),
            'rerun_input_digest': rerun_carriage_expression('digest'),
            'rerun_input_inline': rerun_carriage_expression('inline'),
            'rerun_input_reference': rerun_carriage_expression('reference'),
        },
        select_tail=(
            f'FROM {LIVE_TASKS} t\n'
            f'    {linkage_join}\n'
            f'    CROSS JOIN LATERAL (\n'
            f'        SELECT {disposition_case} AS disposition\n'
            f'    ) d\n'
            f'    WHERE t.id = ANY({ids_var})'
        ),
    )


def _move_stage_core(
    *,
    kind: TerminalizationKind,
    status: str,
    result_expression: str,
    error_expression: str,
    reason_expression: str,
    deferred: bool,
    ids_var: str,
    outcome_sql: str,
) -> str:
    """The move stages every batch shares, rendered once.

    From the deferred fence through the per-row notify: both the
    discovery builder and the id-keyed builder compose this rendering
    around their own selection semantics, so the set-wise move has one
    implementation regardless of how a batch chooses its rows.
    """
    disposition_case = disposition_case_expression('t', f"'{status}'")
    fence_stage = ''
    if deferred:
        fence_stage = f"""
    IF p_result IS NULL AND EXISTS (
        SELECT 1 FROM {LIVE_TASKS} t
        WHERE t.id = ANY({ids_var}) AND t.is_workflow_task
    ) THEN
        RAISE EXCEPTION
            'deferred workflow terminalization requires a result payload'
            USING ERRCODE = 'not_null_violation';
    END IF;
"""
    if deferred:
        linkage_guard = f"""
    IF EXISTS (
        SELECT 1 FROM {LIVE_TASKS} t
        WHERE t.id = ANY({ids_var})
          AND t.is_workflow_task
          AND (SELECT count(*) FROM horsies_workflow_tasks n
               WHERE n.task_id = t.id) <> 1
    ) THEN
        RAISE EXCEPTION
            'workflow-backing task lacks exactly one node row'
            USING ERRCODE = 'foreign_key_violation';
    END IF;
"""
        linkage_join = (
            'LEFT JOIN horsies_workflow_tasks n ON n.task_id = t.id'
        )
    else:
        linkage_guard = f"""
    IF EXISTS (
        SELECT 1 FROM {LIVE_TASKS} t
        WHERE t.id = ANY({ids_var})
          AND t.is_workflow_task
          AND (SELECT count(DISTINCT wt.workflow_id)
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id) > 1
    ) THEN
        RAISE EXCEPTION 'task links to multiple workflows'
            USING ERRCODE = 'data_corrupted';
    END IF;
"""
        linkage_join = (
            'LEFT JOIN LATERAL (\n'
            '        SELECT wt.workflow_id\n'
            '        FROM horsies_workflow_tasks wt\n'
            '        WHERE wt.task_id = t.id\n'
            '        ORDER BY wt.id\n'
            '        LIMIT 1\n'
            '    ) n ON TRUE'
        )
    pending_stage = ''
    if deferred:
        pending_stage = f"""
    INSERT INTO {WORKFLOW_PHASE2_PENDING} (
        task_id, workflow_id, workflow_node_row_id,
        terminal_status, terminal_at, terminalization_kind,
        recovery_source, history_class, history_anchor,
        history_schema_version, result_digest,
        phase2_generation, created_at, attempt_count
    )
    SELECT
        t.id, n.workflow_id, n.id,
        '{status}', v_terminal_at, '{kind.value}',
        'HISTORY', t.retention_class_key, v_terminal_at,
        1, sha256(v_result_payload),
        gen_random_uuid(), statement_timestamp(), 0
    FROM {LIVE_TASKS} t
    JOIN horsies_workflow_tasks n ON n.task_id = t.id
    WHERE t.id = ANY({ids_var}) AND t.is_workflow_task;
"""
    return f"""{fence_stage}{linkage_guard}
    -- Per-row uniqueness guard through the staged mechanism.
    IF EXISTS (
        SELECT 1 FROM unnest({ids_var}) AS u(tid)
        WHERE (SELECT prov.found
               FROM {TASK_PROVENANCE_FUNCTION}(u.tid, FALSE) AS prov)
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN ({result_expression}) IS NULL THEN NULL
        ELSE convert_to(({result_expression}), 'UTF8')
    END;

    {_batch_history_insert(
        status=status,
        kind=kind,
        error_expression=error_expression,
        reason_expression=reason_expression,
        linkage_join=linkage_join,
        disposition_case=disposition_case,
        ids_var=ids_var,
    )};
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality({ids_var}) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality({ids_var});
    END IF;
{pending_stage}
    -- The reservation transition has ONE owner: the registry module.
    PERFORM horsies_key_reservation_terminalize_batch(
        (SELECT COALESCE(array_agg(t.idempotency_key_digest), '{{}}')
         FROM {LIVE_TASKS} t
         WHERE t.id = ANY({ids_var})
           AND t.idempotency_key_digest IS NOT NULL),
        (SELECT COALESCE(array_agg(t.id), '{{}}')
         FROM {LIVE_TASKS} t
         WHERE t.id = ANY({ids_var})
           AND t.idempotency_key_digest IS NOT NULL),
        v_terminal_at
    );

{outcome_sql}
    DELETE FROM {LIVE_ATTEMPTS} WHERE task_id = ANY({ids_var});
    DELETE FROM {LIVE_TASKS} WHERE id = ANY({ids_var});
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality({ids_var}) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality({ids_var});
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest({ids_var}) AS u(tid);
"""


def _discovery_outcome_sql(kind: TerminalizationKind, ids_var: str) -> str:
    return f"""    -- Outcome rows stream from the still-locked live rows BEFORE the
    -- deletes: reading them back through the partitioned parent by
    -- task id would be the rejected fan-out mechanism. RETURN QUERY
    -- materializes immediately; it does not end the function.
    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, '{kind.value}'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM {LIVE_TASKS} t
    WHERE t.id = ANY({ids_var});
"""


def _batch_move_ddl(
    *,
    function_name: str,
    parameters_sql: str,
    kind: TerminalizationKind,
    status: str,
    discovery_sql: str,
    result_expression: str,
    error_expression: str,
    reason_expression: str,
    deferred: bool,
    has_batch_size: bool = True,
) -> str:
    """The discovery-batch wrapper around the shared move-stage core.

    Never-waits posture: SKIP LOCKED discovery cannot join any deadlock
    cycle; rows held by advisory-first singles are skipped and caught by
    the next sweep.
    """
    core = _move_stage_core(
        kind=kind,
        status=status,
        result_expression=result_expression,
        error_expression=error_expression,
        reason_expression=reason_expression,
        deferred=deferred,
        ids_var='v_ids',
        outcome_sql=_discovery_outcome_sql(kind, 'v_ids'),
    )
    # Workflow-scoped sweeps are SCOPE-bounded by the caller's workflow
    # array, not size-bounded; production takes no LIMIT for them.
    size_stage = ''
    if has_batch_size:
        size_stage = """    IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION
            'p_batch_size must be a positive integer, got %', p_batch_size
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
"""
    return f"""
CREATE FUNCTION {function_name}(
{parameters_sql}
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
{size_stage}    PERFORM {ARCHIVE_AVAILABILITY_FUNCTION}();

    -- Discovery under the batch locking rule: SKIP LOCKED never waits on
    -- a row lock, so this batch cannot join any deadlock cycle; rows held
    -- by advisory-first singles are skipped and caught next sweep.
    SELECT array_agg(s.id) INTO v_ids
    FROM (
{discovery_sql}
    ) s;
    IF v_ids IS NULL THEN
        RETURN;
    END IF;
{core}END
$function$
"""


def _expire_pending_ddl() -> str:
    return _batch_move_ddl(
        function_name=EXPIRE_PENDING_FUNCTION,
        parameters_sql=(
            '    p_batch_size integer,\n'
            '    p_result text,\n'
            '    p_error_code text'
        ),
        kind=TerminalizationKind.EXPIRE_PENDING,
        status='EXPIRED',
        discovery_sql=f"""        SELECT id FROM {LIVE_TASKS}
        WHERE status = 'PENDING'
          AND good_until IS NOT NULL
          AND good_until <= NOW()
        ORDER BY good_until ASC
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED""",
        result_expression='p_result',
        error_expression='p_error_code',
        reason_expression='NULL',
        deferred=True,
    )


def _cancel_orphan_sweep_ddl() -> str:
    return _batch_move_ddl(
        function_name=CANCEL_ORPHAN_SWEEP_FUNCTION,
        parameters_sql='    p_batch_size integer',
        kind=TerminalizationKind.CANCEL_ORPHAN_SWEEP,
        status='CANCELLED',
        discovery_sql=f"""        SELECT t2.id FROM {LIVE_TASKS} t2
        WHERE t2.is_workflow_task = TRUE
          AND t2.status IN ('CLAIMED', 'PENDING')
          AND NOT EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              WHERE wt.task_id = t2.id
                AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
          )
        LIMIT p_batch_size
        FOR UPDATE OF t2 SKIP LOCKED""",
        result_expression='NULL::text',
        error_expression="'WORKFLOW_CHECK_FAILED'",
        reason_expression=(
            "'Workflow task orphaned: no live workflow_task linkage'"
        ),
        deferred=False,
    )


def _cancel_locked_ddl() -> str:
    kind = TerminalizationKind.CANCEL_ADMIN.value
    return f"""
CREATE FUNCTION {CANCEL_LOCKED_FUNCTION}(
    p_task_id uuid,
    p_permitted_source_statuses text[]
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );
    -- The caller chooses the permitted live source set; the database owns
    -- the fixed part: plain tasks only, live statuses only, and the
    -- operation's literals are not caller-supplied.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at
    INTO v_status, v_worker, v_claimed_at
    FROM {LIVE_TASKS} t
    WHERE t.id = p_task_id
      AND t.is_workflow_task = FALSE
      AND t.status IN ('PENDING', 'CLAIMED', 'RUNNING')
      AND t.status = ANY(p_permitted_source_statuses)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
            p_task_id, ARRAY['{kind}']::text[],
            NULL::text, NULL::timestamptz
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM {MOVE_FUNCTION}(
        p_task_id, 'CANCELLED', '{kind}', v_terminal_at,
        NULL, 'TASK_CANCELLED', 'Cancelled via monitoring API'
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, '{kind}'::text,
        v_status, v_worker, v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$
"""


def _cancel_orphan_ddl() -> str:
    kind = TerminalizationKind.CANCEL_ORPHAN.value
    return f"""
CREATE FUNCTION {CANCEL_ORPHAN_FUNCTION}(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF {OUTCOME_TYPE}
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
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
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
    FROM {LIVE_TASKS} t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF FOUND
       AND v_status = 'CLAIMED'
       AND v_worker = CAST(p_worker_id AS VARCHAR)
       AND (p_claimed_at IS NULL OR v_claimed_at = p_claimed_at)
       AND v_is_workflow_task THEN
        IF v_node_status IS NULL THEN
            v_terminal_at := NOW();
            PERFORM {MOVE_FUNCTION}(
                p_task_id, 'CANCELLED', '{kind}', v_terminal_at,
                NULL, 'WORKFLOW_CHECK_FAILED',
                'Workflow task orphaned: no live workflow_task linkage'
            );
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, '{kind}'::text,
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

    RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
        p_task_id, ARRAY['{kind}']::text[],
        p_worker_id, p_claimed_at
    );
END
$function$
"""


def _id_keyed_batch_ddl(
    *,
    function_name: str,
    kind: TerminalizationKind,
    error_expression: str,
    reason_expression: str,
) -> str:
    """The id-keyed pairwise wrapper around the shared move-stage core.

    Waits-in-global-order posture: an id-keyed batch cannot skip — the
    caller demands an answer per input id — so it does wait on row
    locks, and therefore locks every present input row in SORTED id
    order regardless of input order. Two id-keyed batches acquire
    identically (no batch-batch cycle); a single holding advisory+row
    completes independently (no batch-single cycle). Outcomes are
    emitted at input ordinality; the client decoder reconstructs by
    ordinality value and never trusts result order. Every non-applied
    input gets its answer through the ONE miss classifier.
    """
    core = _move_stage_core(
        kind=kind,
        status='CANCELLED',
        result_expression='NULL::text',
        error_expression=error_expression,
        reason_expression=reason_expression,
        deferred=False,
        ids_var='v_applied_ids',
        outcome_sql=f"""    -- Applied outcomes stream from the still-locked live rows at their
    -- input ordinality, BEFORE the deletes.
    RETURN QUERY SELECT
        t.id, input.ordinality, 'APPLIED'::text,
        v_terminal_at, '{kind.value}'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM {LIVE_TASKS} t
    JOIN unnest(p_ids) WITH ORDINALITY AS input(task_id, ordinality)
        ON input.task_id = t.id
    WHERE t.id = ANY(v_applied_ids);
""",
    )
    return f"""
CREATE FUNCTION {function_name}(
    p_ids uuid[],
    p_claimed_ats timestamptz[],
    p_worker_id text
)
RETURNS SETOF {OUTCOME_TYPE}
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
    PERFORM {ARCHIVE_AVAILABILITY_FUNCTION}();

    -- Waits-in-global-order: lock every present input row in sorted id
    -- order before anything is judged.
    PERFORM 1 FROM (
        SELECT t.id FROM {LIVE_TASKS} t
        WHERE t.id = ANY(p_ids)
        ORDER BY t.id
        FOR UPDATE
    ) locked;

    SELECT COALESCE(array_agg(t.id), '{{}}') INTO v_applied_ids
    FROM unnest(p_ids, p_claimed_ats)
        AS input(task_id, expected_claimed_at)
    JOIN {LIVE_TASKS} t ON t.id = input.task_id
    WHERE t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND (
          input.expected_claimed_at IS NULL
          OR t.claimed_at = input.expected_claimed_at
      );

    IF cardinality(v_applied_ids) > 0 THEN
{core}    END IF;

    -- Every non-applied input gets its answer through the ONE miss
    -- classifier, at its own ordinality.
    RETURN QUERY
    SELECT input.task_id, input.ordinality,
           m.outcome, m.terminal_at, m.terminalization_kind,
           m.observed_status, m.observed_worker_id, m.observed_claimed_at,
           m.guard_kind, m.observed_guard
    FROM unnest(p_ids, p_claimed_ats) WITH ORDINALITY
        AS input(task_id, expected_claimed_at, ordinality)
    CROSS JOIN LATERAL {MISS_CLASSIFIER_FUNCTION}(
        input.task_id, ARRAY['{kind.value}']::text[],
        p_worker_id, input.expected_claimed_at
    ) m
    WHERE NOT (input.task_id = ANY(v_applied_ids));
END
$function$
"""


def _abandon_owned_node_ddl() -> str:
    kind = TerminalizationKind.PAUSE_ABANDON_CLAIM.value
    return f"""
CREATE FUNCTION {ABANDON_OWNED_NODE_FUNCTION}(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at
    INTO v_status, v_worker, v_claimed_at
    FROM {LIVE_TASKS} t
    WHERE t.id = p_task_id
      AND t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND (p_claimed_at IS NULL OR t.claimed_at = p_claimed_at)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
            p_task_id, ARRAY['{kind}']::text[],
            p_worker_id, p_claimed_at
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM {MOVE_FUNCTION}(
        p_task_id, 'CANCELLED', '{kind}', v_terminal_at,
        NULL, 'TASK_CANCELLED', 'Workflow paused before task start'
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, '{kind}'::text,
        v_status, v_worker, v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$
"""


def _cancel_owned_node_ddl() -> str:
    kind = TerminalizationKind.WORKFLOW_CANCEL_CLAIM.value
    return f"""
CREATE FUNCTION {CANCEL_OWNED_NODE_FUNCTION}(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamptz,
    p_accepts_requeued_pending boolean
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, {_TASK_LOCK_SEED})
    );
    -- Full ownership fence, with the explicit carve-out for a task this
    -- same child observed as requeued to PENDING, where no claim remains
    -- to fence; the carve-out is a property of this variant only.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at
    INTO v_status, v_worker, v_claimed_at
    FROM {LIVE_TASKS} t
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
        RETURN QUERY SELECT * FROM {MISS_CLASSIFIER_FUNCTION}(
            p_task_id, ARRAY['{kind}']::text[],
            p_worker_id, p_claimed_at
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM {MOVE_FUNCTION}(
        p_task_id, 'CANCELLED', '{kind}', v_terminal_at,
        NULL, NULL, NULL
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, '{kind}'::text,
        v_status, v_worker, v_claimed_at,
        NULL::text, NULL::jsonb;
END
$function$
"""


def _abandon_owned_nodes_ddl() -> str:
    return _id_keyed_batch_ddl(
        function_name=ABANDON_OWNED_NODES_FUNCTION,
        kind=TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH,
        error_expression="'TASK_CANCELLED'",
        reason_expression="'Workflow paused before task start'",
    )


def _cancel_owned_nodes_ddl() -> str:
    return _id_keyed_batch_ddl(
        function_name=CANCEL_OWNED_NODES_FUNCTION,
        kind=TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH,
        error_expression='NULL',
        reason_expression='NULL',
    )


def _abandon_paused_sweep_ddl() -> str:
    return _batch_move_ddl(
        function_name=ABANDON_PAUSED_SWEEP_FUNCTION,
        parameters_sql='    p_workflow_ids uuid[]',
        kind=TerminalizationKind.PAUSE_ABANDON_WORKFLOW,
        status='CANCELLED',
        discovery_sql=f"""        SELECT t2.id FROM {LIVE_TASKS} t2
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
        FOR UPDATE OF t2 SKIP LOCKED""",
        result_expression='NULL::text',
        error_expression="'TASK_CANCELLED'",
        reason_expression="'Workflow paused before task start'",
        deferred=False,
        has_batch_size=False,
    )


def _cancel_cancelled_sweep_ddl() -> str:
    return _batch_move_ddl(
        function_name=CANCEL_CANCELLED_SWEEP_FUNCTION,
        parameters_sql='    p_workflow_ids uuid[]',
        kind=TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW,
        status='CANCELLED',
        discovery_sql=f"""        SELECT t2.id FROM {LIVE_TASKS} t2
        WHERE t2.status IN ('PENDING', 'CLAIMED', 'RUNNING')
          AND EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t2.id
                AND wt.workflow_id = ANY(p_workflow_ids)
                AND w.status = 'CANCELLED'
                AND wt.status = 'ENQUEUED'
          )
        FOR UPDATE OF t2 SKIP LOCKED""",
        result_expression='NULL::text',
        error_expression='NULL',
        reason_expression='NULL',
        deferred=False,
        has_batch_size=False,
    )


def workflow_node_family_fragments() -> tuple[str, ...]:
    """The workflow-node family wire functions, in install order."""
    return (
        _abandon_owned_node_ddl(),
        _cancel_owned_node_ddl(),
        _abandon_owned_nodes_ddl(),
        _cancel_owned_nodes_ddl(),
        _abandon_paused_sweep_ddl(),
        _cancel_cancelled_sweep_ddl(),
    )


def cancellation_family_fragments() -> tuple[str, ...]:
    """The cancellation-family wire functions, in install order."""
    return (
        _cancel_locked_ddl(),
        _cancel_orphan_ddl(),
        _cancel_orphan_sweep_ddl(),
    )


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
