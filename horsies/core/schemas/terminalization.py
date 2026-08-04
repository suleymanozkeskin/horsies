"""Database-owned terminal transitions, generated from the typed vocabulary.

Each operation is a function that owns its guard, stamps its own provenance,
and reports what it did as data. The alternative — the same guard spelled into
sixteen statements across five modules — is how a fence came to be missing from
six of them without anyone noticing.

The bodies are generated rather than written out because three things have to
agree and cannot be allowed to drift: the kind a function stamps, the kinds it
will accept as equivalent when it finds the work already done, and the row
shape every function returns for one decoder to read. All three come from
`horsies/core/lifecycle/`, so the vocabulary is the single source and the SQL
is its projection.

Functions are installed by DROP then CREATE on every migration apply, after
the `horsies_claim` precedent: no per-function version tracking, and both
drivers necessarily call one definition. A signature change must update the
drop list — PostgreSQL overloads by signature, so a stale drop leaves an old
overload behind that callers can still bind to.
"""

from __future__ import annotations

from sqlalchemy import TextClause, text

from ..lifecycle.operations import TerminalizationKind, equivalence_class_of

# The one row shape, per the wire contract. Every function returns exactly
# this, so the drivers share a decoder rather than each interpreting columns.
OUTCOME_COLUMNS: tuple[tuple[str, str], ...] = (
    ('task_id', 'varchar'),
    ('ordinality', 'bigint'),
    ('outcome', 'text'),
    ('terminal_at', 'timestamptz'),
    ('terminalization_kind', 'text'),
    ('observed_status', 'text'),
    ('observed_worker_id', 'varchar'),
    ('observed_claimed_at', 'timestamptz'),
    ('guard_kind', 'text'),
    ('observed_guard', 'jsonb'),
)

_TYPE_COLUMNS = ',\n    '.join(f'{name} {kind}' for name, kind in OUTCOME_COLUMNS)

# The shape is a composite type rather than a per-function RETURNS TABLE, and
# the reason is mechanical rather than aesthetic: PL/pgSQL treats RETURNS TABLE
# column names as variables inside the body, and several of these deliberately
# match real column names. An `ON CONFLICT (task_id, ...)` clause then refuses
# to resolve — index inference does not honour `#variable_conflict`, so the
# fused path's attempt upsert cannot be written at all. Naming the shape once
# removes the collision and makes "one row shape" a fact the database enforces.
OUTCOME_TYPE = 'horsies_terminalization_outcome'

CREATE_OUTCOME_TYPE_SQL = text(f'''
CREATE TYPE {OUTCOME_TYPE} AS (
    {_TYPE_COLUMNS}
)
''')

DROP_OUTCOME_TYPE_SQL = text(f'DROP TYPE IF EXISTS {OUTCOME_TYPE}')

TERMINAL_STATUSES_SQL = "('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')"


def _kind_array(kind: TerminalizationKind) -> str:
    """The kinds this operation accepts as its own work, already committed."""
    members = sorted(k.value for k in equivalence_class_of(kind))
    return 'ARRAY[' + ', '.join(f"'{value}'" for value in members) + ']::text[]'


def _kind_domain() -> str:
    values = sorted(kind.value for kind in TerminalizationKind)
    return ', '.join(f"'{value}'" for value in values)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

ADD_TERMINALIZATION_KIND_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS terminalization_kind TEXT
""")

# The value domain only. A stronger constraint tying a non-NULL kind to a
# terminal status cannot ship in the same release: during its rolling window an
# un-upgraded worker's manual-retry statement, which predates the column and
# cannot clear it, revives a row a new function terminalized — and the status
# arm would fail that worker's retry. NULL passes, which is what makes the
# value arm rolling-safe: legacy writers never supply the column at all.
DROP_TERMINALIZATION_KIND_CHECK_SQL = text("""
    ALTER TABLE horsies_tasks
    DROP CONSTRAINT IF EXISTS ck_horsies_tasks_terminalization_kind
""")

ADD_TERMINALIZATION_KIND_CHECK_SQL = text(f"""
    ALTER TABLE horsies_tasks
    ADD CONSTRAINT ck_horsies_tasks_terminalization_kind
    CHECK (
        terminalization_kind IS NULL
        OR terminalization_kind IN ({_kind_domain()})
    )
""")


# ---------------------------------------------------------------------------
# Shared miss-path classification
# ---------------------------------------------------------------------------

# Called when a guarded transition matched nothing. The order is the contract's
# and is not each function's to choose: absent, then already-applied, then lost
# claim, then conflict. Terminal is tested before the fence deliberately — a row
# this very operation already terminalized carries no claim, and the reverse
# order would report it as a lost claim.
CREATE_MISS_CLASSIFIER_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_terminalization_miss(
    p_task_id varchar,
    p_equivalent_kinds text[],
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_row horsies_tasks%ROWTYPE;
BEGIN
    SELECT * INTO v_row
    FROM horsies_tasks
    WHERE id = p_task_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'TASK_ABSENT'::text,
            NULL::timestamptz, NULL::text,
            NULL::text, NULL::varchar, NULL::timestamptz,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    IF v_row.status IN {TERMINAL_STATUSES_SQL} THEN
        IF v_row.terminalization_kind = ANY(p_equivalent_kinds) THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'ALREADY_APPLIED'::text,
                v_row.terminal_at, v_row.terminalization_kind,
                v_row.status::text, v_row.claimed_by_worker_id,
                v_row.claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        -- Terminal under another operation's kind, or under no kind at all:
        -- a row written before the column existed proves nothing about who
        -- won, so its provenance is reported rather than assumed.
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            v_row.terminal_at, v_row.terminalization_kind,
            v_row.status::text, v_row.claimed_by_worker_id, v_row.claimed_at,
            'FOREIGN_TERMINALIZATION'::text, NULL::jsonb;
        RETURN;
    END IF;

    -- Live, and this caller's fence cannot reach it: a different worker, a
    -- different generation, or a requeue that cleared the claim entirely.
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
END;
$$
""")

DROP_MISS_CLASSIFIER_SQL = text("""
DROP FUNCTION IF EXISTS horsies_terminalization_miss(
    varchar, text[], text, timestamptz
)
""")


# ---------------------------------------------------------------------------
# COMPLETED
# ---------------------------------------------------------------------------

# The guard is the caller's worker id only. The claim generation was fenced by
# the locking read this caller already performed, which is a property of the
# two-statement shape rather than a weaker guard.
CREATE_COMPLETE_LOCKED_TASK_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_complete_locked_task(
    p_task_id varchar,
    p_worker_id text,
    p_result text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_kind text;
    v_claimed_at timestamptz;
BEGIN
    UPDATE horsies_tasks t
    SET status = 'COMPLETED',
        completed_at = NOW(),
        result = p_result,
        error_code = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        terminalization_kind = '{TerminalizationKind.COMPLETE_LOCKED.value}',
        updated_at = NOW()
    WHERE t.id = p_task_id
      AND t.status = 'RUNNING'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
    RETURNING t.terminal_at, t.terminalization_kind, t.claimed_at
    INTO v_terminal_at, v_kind, v_claimed_at;

    IF FOUND THEN
        -- The pre-transition image: status and worker are what the guard
        -- matched, and this transition leaves the claim columns alone, so the
        -- returned claim is the one the update found.
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'APPLIED'::text,
            v_terminal_at, v_kind,
            'RUNNING'::text, CAST(p_worker_id AS VARCHAR), v_claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id,
        {_kind_array(TerminalizationKind.COMPLETE_LOCKED)},
        p_worker_id,
        NULL::timestamptz
    );
END;
$$
""")

DROP_COMPLETE_LOCKED_TASK_SQL = text("""
DROP FUNCTION IF EXISTS horsies_complete_locked_task(varchar, text, text)
""")

# The fused path keeps its CTE. Locking, writing the attempt from the locked
# row's own context, transitioning and waking queue capacity stay one
# statement: restating the guard as a separate update predicate would preserve
# the semantics and lose the property the fusion exists for.
CREATE_COMPLETE_TASK_FUSED_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_complete_task_fused(
    p_task_id varchar,
    p_worker_id text,
    p_claimed_at timestamptz,
    p_result text,
    p_notify_channel text,
    p_notify_payload text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_kind text;
    v_observed_worker varchar;
    v_observed_claimed_at timestamptz;
BEGIN
    WITH ctx AS (
        SELECT id, retry_count, started_at, claimed_by_worker_id, claimed_at,
               worker_hostname, worker_pid, worker_process_name,
               clock_timestamp() AS db_now
        FROM horsies_tasks
        WHERE id = p_task_id
          AND status = 'RUNNING'
          AND claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
          AND (p_claimed_at IS NULL OR claimed_at = p_claimed_at)
        FOR UPDATE
    ),
    attempt AS (
        INSERT INTO horsies_task_attempts (
            task_id, attempt, outcome, will_retry,
            started_at, finished_at,
            error_code, error_message, failed_reason,
            worker_id, worker_hostname, worker_pid, worker_process_name
        )
        SELECT ctx.id, COALESCE(ctx.retry_count, 0) + 1, 'COMPLETED', FALSE,
               COALESCE(ctx.started_at, ctx.db_now), ctx.db_now,
               NULL, NULL, NULL,
               ctx.claimed_by_worker_id, ctx.worker_hostname, ctx.worker_pid,
               ctx.worker_process_name
        FROM ctx
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
            worker_process_name = EXCLUDED.worker_process_name
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'COMPLETED',
            completed_at = NOW(),
            result = p_result,
            error_code = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.COMPLETE_FUSED.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
        RETURNING t.terminal_at, t.terminalization_kind
    )
    SELECT upd.terminal_at, upd.terminalization_kind,
           ctx.claimed_by_worker_id, ctx.claimed_at
    INTO v_terminal_at, v_kind, v_observed_worker, v_observed_claimed_at
    FROM upd, ctx;

    IF FOUND THEN
        -- The wake fires only for a transition that happened, and inside the
        -- same transaction, so delivery is unchanged: notifications are
        -- released at commit either way.
        PERFORM pg_notify(p_notify_channel, p_notify_payload);
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'APPLIED'::text,
            v_terminal_at, v_kind,
            'RUNNING'::text, v_observed_worker, v_observed_claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id,
        {_kind_array(TerminalizationKind.COMPLETE_FUSED)},
        p_worker_id,
        p_claimed_at
    );
END;
$$
""")

DROP_COMPLETE_TASK_FUSED_SQL = text("""
DROP FUNCTION IF EXISTS horsies_complete_task_fused(
    varchar, text, timestamptz, text, text, text
)
""")


# Drops precede creates on every apply, and the drop list names exact
# signatures: PostgreSQL overloads by signature, so a changed argument list
# without a matching drop leaves the old overload installed and callable.
DROP_TERMINALIZATION_FUNCTIONS_SQL: tuple[TextClause, ...] = (
    DROP_COMPLETE_LOCKED_TASK_SQL,
    DROP_COMPLETE_TASK_FUSED_SQL,
    DROP_MISS_CLASSIFIER_SQL,
)

# The type is dropped after the functions and recreated before them. A stale
# drop list announces itself here rather than silently: the type drop fails
# while a function still returns it, which names the omission instead of
# leaving an orphan overload installed.
CREATE_TERMINALIZATION_FUNCTIONS_SQL: tuple[TextClause, ...] = (
    CREATE_MISS_CLASSIFIER_SQL,
    CREATE_COMPLETE_LOCKED_TASK_SQL,
    CREATE_COMPLETE_TASK_FUSED_SQL,
)
