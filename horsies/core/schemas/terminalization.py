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

# Created once and then left alone. Every later schema release re-runs the
# full desired state, and dropping the type each time would churn its
# PostgreSQL identity underneath dependent functions, prepared statements and
# rolling workers for no gain. A change to the shape is a versioned migration
# of its own — a new type name, or an explicit ALTER — not a silent recreate.
CREATE_OUTCOME_TYPE_SQL = text(f'''
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = '{OUTCOME_TYPE}'
          AND n.nspname = current_schema()
    ) THEN
        CREATE TYPE {OUTCOME_TYPE} AS (
            {_TYPE_COLUMNS}
        );
    END IF;
END
$$''')

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
ADD_TERMINALIZATION_KIND_CHECK_SQL = text(f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'horsies_tasks'::regclass
          AND conname = 'ck_horsies_tasks_terminalization_kind'
    ) THEN
        ALTER TABLE horsies_tasks
        ADD CONSTRAINT ck_horsies_tasks_terminalization_kind
        CHECK (
            terminalization_kind IS NULL
            OR terminalization_kind IN ({_kind_domain()})
        ) NOT VALID;
    END IF;
END
$$""")

# ADD and VALIDATE share the migration transaction, so on first apply the
# validation scan runs under the lock the ADD takes either way. The split is
# for idempotency: the ADD runs only when the constraint is absent, and
# validating an already-valid constraint is a no-op, so a later release
# neither re-adds nor rescans.
VALIDATE_TERMINALIZATION_KIND_CHECK_SQL = text("""
    ALTER TABLE horsies_tasks
    VALIDATE CONSTRAINT ck_horsies_tasks_terminalization_kind
""")


# ---------------------------------------------------------------------------
# terminal_at completeness
# ---------------------------------------------------------------------------

# Rows terminalized before the column existed carry no terminal instant. The
# backfill takes the timestamp the writer did record; the handful of writers
# that recorded neither fall back to the row's last update, which is the
# closest bound the row still holds.
BACKFILL_TERMINAL_AT_SQL = text(f"""
DO $$
BEGIN
    -- Once the constraint is validated, no undated terminal row can exist, so
    -- the scan can only find nothing. Every later schema release would still
    -- pay for it, which is why the proof is the guard.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'horsies_tasks'::regclass
          AND conname = 'ck_horsies_tasks_terminal_at_terminal_only'
          AND convalidated
    ) THEN
        UPDATE horsies_tasks
        SET terminal_at =
            COALESCE(completed_at, failed_at, updated_at, created_at)
        WHERE status IN {TERMINAL_STATUSES_SQL}
          AND terminal_at IS NULL;
    END IF;
END
$$""")

# Terminal exactly when dated. Installed NOT VALID only when absent, and
# validated separately: on first apply the scan runs under the migration
# transaction's lock regardless, but a later release neither re-adds nor
# rescans an already-valid constraint.
ADD_TERMINAL_AT_CHECK_SQL = text(f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'horsies_tasks'::regclass
          AND conname = 'ck_horsies_tasks_terminal_at_terminal_only'
    ) THEN
        ALTER TABLE horsies_tasks
        ADD CONSTRAINT ck_horsies_tasks_terminal_at_terminal_only
        CHECK (
            (status IN {TERMINAL_STATUSES_SQL}) = (terminal_at IS NOT NULL)
        ) NOT VALID;
    END IF;
END
$$""")

VALIDATE_TERMINAL_AT_CHECK_SQL = text("""
    ALTER TABLE horsies_tasks
    VALIDATE CONSTRAINT ck_horsies_tasks_terminal_at_terminal_only
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


# ---------------------------------------------------------------------------
# FAILED
# ---------------------------------------------------------------------------

# One function for the two locked-failure writers: application failure and
# worker-level failure differ only in whether a failure reason is carried,
# and the COALESCE below is the single asymmetric assignment that difference
# requires. A NULL p_failed_reason means "leave the column as it is", not
# "clear it" — no requeue path clears failed_reason, so a row can carry one
# from an earlier attempt that this transition never owned.
CREATE_FAIL_LOCKED_TASK_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_fail_locked_task(
    p_task_id varchar,
    p_worker_id text,
    p_result text,
    p_error_code text,
    p_failed_reason text
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
    SET status = 'FAILED',
        failed_at = NOW(),
        result = p_result,
        error_code = p_error_code,
        failed_reason = COALESCE(p_failed_reason, failed_reason),
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        terminalization_kind = '{TerminalizationKind.FAIL_RUNNING.value}',
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
        {_kind_array(TerminalizationKind.FAIL_RUNNING)},
        p_worker_id,
        NULL::timestamptz
    );
END;
$$
""")

DROP_FAIL_LOCKED_TASK_SQL = text("""
DROP FUNCTION IF EXISTS horsies_fail_locked_task(
    varchar, text, text, text, text
)
""")

# Guard family, not fence family: the worker that held this task is by
# hypothesis not answering, so the guard is staleness rather than ownership.
# The row is locked and every observable the two arms compare — including
# the heartbeat, which has no locking relationship with the task row — is
# captured in that one snapshot together with the instant they are judged
# at. The decision and the evidence come from the capture; nothing is reread
# after a refusal, so the evidence cannot show a heartbeat the guard never
# saw. Absent, terminal and wrong-source-state rows delegate to the shared
# classifier with NULL claim parameters, which its arms are built to accept.
CREATE_FAIL_STALE_TASK_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_fail_stale_task(
    p_task_id varchar,
    p_stale_after_ms integer,
    p_finalizing_stale_after_ms integer,
    p_result text,
    p_error_code text,
    p_failed_reason text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_kind text;
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_started_at timestamptz;
    v_finalizing_at timestamptz;
    v_last_heartbeat timestamptz;
    v_evaluated_at timestamptz;
BEGIN
    -- One snapshot: the row locked, the heartbeat read beside it, and the
    -- instant both arms are judged at. NOW() is the transaction timestamp,
    -- so the instant captured here is the same one the transition below
    -- stamps into terminal_at.
    SELECT t.status::text, t.claimed_by_worker_id, t.claimed_at,
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
            UPDATE horsies_tasks t
            SET status = 'FAILED',
                failed_at = NOW(),
                failed_reason = p_failed_reason,
                result = p_result,
                error_code = p_error_code,
                finalizing_at = NULL,
                finalizing_by_worker_id = NULL,
                terminal_at = NOW(),
                terminalization_kind =
                    '{TerminalizationKind.FAIL_STALE.value}',
                updated_at = NOW()
            WHERE t.id = p_task_id
            RETURNING t.terminal_at, t.terminalization_kind
            INTO v_terminal_at, v_kind;

            -- Cross-worker by design: the observed claim is whichever
            -- worker's silence the guard just judged, from the capture.
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, v_kind,
                'RUNNING'::text, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        -- The refusal reports the capture itself — every value the two
        -- arms compared, and the instant they were compared at.
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

    -- Absent, terminal, or live outside the source state: the shared
    -- classifier's arms are exactly right, and with no fence to check the
    -- claim parameters are NULL. The row lock taken above is held for the
    -- rest of this transaction, so the classifier re-reads a settled image.
    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id,
        {_kind_array(TerminalizationKind.FAIL_STALE)},
        NULL::text,
        NULL::timestamptz
    );
END;
$$
""")

DROP_FAIL_STALE_TASK_SQL = text("""
DROP FUNCTION IF EXISTS horsies_fail_stale_task(
    varchar, integer, integer, text, text, text
)
""")


# ---------------------------------------------------------------------------
# EXPIRED
# ---------------------------------------------------------------------------

# Fenced on worker ownership but deliberately not on claim generation: once
# the deadline has passed, expiry is the correct outcome for whichever
# generation holds the row. The claim bookkeeping columns are cleared along
# with the transition — claimed and claim_expires_at — while claimed_at is
# left alone, which is what makes the returned claim the pre-image.
CREATE_EXPIRE_OWNED_CLAIM_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_expire_owned_claim(
    p_task_id varchar,
    p_worker_id text,
    p_result text,
    p_error_code text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_kind text;
    v_claimed_at timestamptz;
    v_status text;
    v_worker varchar;
    v_good_until timestamptz;
    v_evaluated_at timestamptz;
    v_retry_under_lock boolean := FALSE;
BEGIN
    LOOP
        -- The common apply path is one guarded UPDATE. A preceding SELECT FOR
        -- UPDATE would emit an extra tuple-lock WAL record for every expiry;
        -- the update already owns the lock and can return the pre-image fields
        -- that remain unchanged by this transition.
        UPDATE horsies_tasks t
        SET status = 'EXPIRED',
            claimed = FALSE,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            failed_at = NOW(),
            result = p_result,
            error_code = p_error_code,
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.EXPIRE_CLAIMED.value}',
            updated_at = NOW()
        WHERE t.id = p_task_id
          AND t.status = 'CLAIMED'
          AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
          AND t.good_until IS NOT NULL
          AND t.good_until <= NOW()
        RETURNING t.terminal_at, t.terminalization_kind,
                  t.claimed_by_worker_id, t.claimed_at
        INTO v_terminal_at, v_kind, v_worker, v_claimed_at;

        IF FOUND THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, v_kind,
                'CLAIMED'::text, v_worker, v_claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        IF v_retry_under_lock THEN
            RAISE EXCEPTION
                'owned expiry lost an eligible row while holding its lock'
                USING ERRCODE = 'serialization_failure';
        END IF;

        -- The apply attempt matched nothing. Lock and capture the row once,
        -- then judge every refusal from that capture. A concurrent update may
        -- have made the deadline eligible between statements; in that case
        -- retry the guarded UPDATE while this transaction owns the row lock.
        SELECT t.status::text, t.claimed_by_worker_id, t.claimed_at,
               t.good_until, NOW()
        INTO v_status, v_worker, v_claimed_at, v_good_until, v_evaluated_at
        FROM horsies_tasks t
        WHERE t.id = p_task_id
        FOR UPDATE;

        IF FOUND
           AND v_status = 'CLAIMED'
           AND v_worker = CAST(p_worker_id AS VARCHAR) THEN
            IF v_good_until IS NOT NULL AND v_good_until <= v_evaluated_at THEN
                v_retry_under_lock := TRUE;
                CONTINUE;
            END IF;

            -- Under this caller's claim and in the source state, so it was
            -- the deadline guard that refused: not yet passed, or absent.
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

        -- Absent, terminal, another worker's claim, or live outside the source
        -- state: the shared classifier's arms are exactly right, and the
        -- worker parameter keeps a foreign claim reported as the lost claim
        -- it is. A present row remains locked, so the classifier observes the
        -- same pre-image captured above.
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id,
            {_kind_array(TerminalizationKind.EXPIRE_CLAIMED)},
            p_worker_id,
            NULL::timestamptz
        );
        RETURN;
    END LOOP;
END;
$$
""")

DROP_EXPIRE_OWNED_CLAIM_SQL = text("""
DROP FUNCTION IF EXISTS horsies_expire_owned_claim(
    varchar, text, text, text
)
""")

# Discovery batch: the function selects its own targets under the deadline
# predicate, so there are no caller-keyed inputs to report against — only
# transitioned rows come back, with no ordinality, and an empty set is a
# valid answer. Batched rather than one statement because a mass expiry
# commits two notifications per row at once and overflows listener queues;
# the batch size is that bound. SKIP LOCKED steps around rows a concurrent
# claim pass holds — the claim re-checks the deadline itself, so the race
# resolves the same way either way.
CREATE_EXPIRE_PENDING_TASKS_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_expire_pending_tasks(
    p_batch_size integer,
    p_result text,
    p_error_code text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
BEGIN
    -- The bound is load-bearing: LIMIT NULL means no limit at all, and a
    -- non-positive value is a caller error, not a smaller batch. Raising is
    -- the contracted shape for a batch precondition violation — this is not
    -- an outcome of any row's transition, so no row can carry it.
    IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION
            'p_batch_size must be a positive integer, got %', p_batch_size
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    RETURN QUERY
    UPDATE horsies_tasks t
    SET status = 'EXPIRED',
        failed_at = NOW(),
        result = p_result,
        error_code = p_error_code,
        terminal_at = NOW(),
        terminalization_kind = '{TerminalizationKind.EXPIRE_PENDING.value}',
        updated_at = NOW()
    FROM (
        SELECT id FROM horsies_tasks
        WHERE status = 'PENDING'
          AND good_until IS NOT NULL
          AND good_until <= NOW()
        ORDER BY good_until ASC
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
    ) s
    WHERE t.id = s.id
    RETURNING t.id, NULL::bigint, 'APPLIED'::text,
              t.terminal_at, t.terminalization_kind,
              'PENDING'::text, t.claimed_by_worker_id, t.claimed_at,
              NULL::text, NULL::jsonb;
END;
$$
""")

DROP_EXPIRE_PENDING_TASKS_SQL = text("""
DROP FUNCTION IF EXISTS horsies_expire_pending_tasks(
    integer, text, text
)
""")


# ---------------------------------------------------------------------------
# CANCELLED — administrative
# ---------------------------------------------------------------------------

# The caller already holds this row's lock and decides whether RUNNING is in
# the permitted source set. The database still owns the fixed part of the
# transition: only a plain live task can be cancelled, terminal rows are never
# overwritten even if a malformed status array names one, and the operation's
# literals and provenance are not caller-supplied.
CREATE_CANCEL_LOCKED_TASK_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_cancel_locked_task(
    p_task_id varchar,
    p_permitted_source_statuses text[]
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_kind text;
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
BEGIN
    WITH ctx AS MATERIALIZED (
        SELECT t.id, t.status::text AS status,
               t.claimed_by_worker_id, t.claimed_at
        FROM horsies_tasks t
        WHERE t.id = p_task_id
          AND t.is_workflow_task = FALSE
          AND t.status::text IN ('PENDING', 'CLAIMED', 'RUNNING')
          AND t.status::text = ANY(p_permitted_source_statuses)
        FOR UPDATE
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'CANCELLED',
            error_code = 'TASK_CANCELLED',
            failed_reason = 'Cancelled via monitoring API',
            failed_at = NOW(),
            claimed = FALSE,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.CANCEL_ADMIN.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
        RETURNING t.terminal_at, t.terminalization_kind
    )
    SELECT upd.terminal_at, upd.terminalization_kind,
           ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at
    INTO v_terminal_at, v_kind, v_status, v_worker, v_claimed_at
    FROM upd, ctx;

    IF FOUND THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'APPLIED'::text,
            v_terminal_at, v_kind,
            v_status, v_worker, v_claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    -- CallerHoldsRowLock carries no ownership predicate. A miss is therefore
    -- absent, already terminalized, or a source-state conflict; the shared
    -- classifier's NULL claim parameters express exactly that ordering.
    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id,
        {_kind_array(TerminalizationKind.CANCEL_ADMIN)},
        NULL::text,
        NULL::timestamptz
    );
END;
$$
""")

DROP_CANCEL_LOCKED_TASK_SQL = text("""
DROP FUNCTION IF EXISTS horsies_cancel_locked_task(varchar, text[])
""")


# ---------------------------------------------------------------------------
# CANCELLED — orphan cleanup
# ---------------------------------------------------------------------------

_RUNNABLE_WORKFLOW_TASK_STATUSES_SQL = "('ENQUEUED', 'READY', 'PENDING', 'RUNNING')"

# The row, its runnable-link observation and the update share one PostgreSQL
# statement and therefore one snapshot. The task lock prevents another task
# transition from interleaving; the link has no locking relationship with the
# task row, so its status is captured beside the row and used both to decide
# and, on refusal, to explain the decision. A terminal workflow-task link does
# not count as runnable and therefore still leaves the backing task orphaned.
CREATE_CANCEL_OWNED_ORPHAN_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_cancel_owned_orphan(
    p_task_id varchar,
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_kind text;
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_is_workflow_task boolean;
    v_node_status text;
    v_applied boolean;
BEGIN
    WITH ctx AS MATERIALIZED (
        SELECT t.id, t.status::text AS status,
               t.claimed_by_worker_id, t.claimed_at,
               t.is_workflow_task,
               (
                   SELECT wt.status
                   FROM horsies_workflow_tasks wt
                   WHERE wt.task_id = t.id
                     AND wt.status IN {_RUNNABLE_WORKFLOW_TASK_STATUSES_SQL}
                   ORDER BY wt.id
                   LIMIT 1
               ) AS node_status
        FROM horsies_tasks t
        WHERE t.id = p_task_id
        FOR UPDATE
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'CANCELLED',
            claimed = FALSE,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            error_code = 'WORKFLOW_CHECK_FAILED',
            failed_reason =
                'Workflow task orphaned: no live workflow_task linkage',
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.CANCEL_ORPHAN.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
          AND ctx.status = 'CLAIMED'
          AND ctx.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
          AND (
              p_claimed_at IS NULL
              OR ctx.claimed_at = p_claimed_at
          )
          AND ctx.is_workflow_task = TRUE
          AND ctx.node_status IS NULL
        RETURNING t.terminal_at, t.terminalization_kind
    )
    SELECT ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           ctx.is_workflow_task, ctx.node_status,
           upd.terminal_at, upd.terminalization_kind,
           upd.terminal_at IS NOT NULL
    INTO v_status, v_worker, v_claimed_at, v_is_workflow_task,
         v_node_status, v_terminal_at, v_kind, v_applied
    FROM ctx
    LEFT JOIN upd ON TRUE;

    IF FOUND AND v_applied THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'APPLIED'::text,
            v_terminal_at, v_kind,
            v_status, v_worker, v_claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    -- Classification order puts the fence ahead of the guard. Only a live
    -- CLAIMED workflow task still held by this generation can truthfully say
    -- that a runnable link, rather than ownership, refused the operation.
    IF FOUND
       AND v_status = 'CLAIMED'
       AND v_worker = CAST(p_worker_id AS VARCHAR)
       AND (p_claimed_at IS NULL OR v_claimed_at = p_claimed_at)
       AND v_is_workflow_task
       AND v_node_status IS NOT NULL
    THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            NULL::timestamptz, NULL::text,
            v_status, v_worker, v_claimed_at,
            'WORKFLOW_LINK_STATE'::text,
            jsonb_build_object('node_status', v_node_status);
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id,
        {_kind_array(TerminalizationKind.CANCEL_ORPHAN)},
        p_worker_id,
        p_claimed_at
    );
END;
$$
""")

DROP_CANCEL_OWNED_ORPHAN_SQL = text("""
DROP FUNCTION IF EXISTS horsies_cancel_owned_orphan(
    varchar, text, timestamptz
)
""")

# Discovery batch: the orphan predicate is identical to the single-row
# operation's. It selects only valid task statuses, locks and bounds its own
# target set, and reports only rows it actually transitioned. Candidate order
# is deliberately unspecified: no supporting age index exists, so ordering
# before LIMIT would sort the whole orphan set and defeat the bound's purpose.
CREATE_CANCEL_ORPHANED_TASKS_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_cancel_orphaned_tasks(
    p_batch_size integer
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION
            'p_batch_size must be a positive integer, got %', p_batch_size
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    RETURN QUERY
    UPDATE horsies_tasks t
    SET status = 'CANCELLED',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        error_code = 'WORKFLOW_CHECK_FAILED',
        failed_reason =
            'Workflow task orphaned: no live workflow_task linkage',
        terminal_at = NOW(),
        terminalization_kind =
            '{TerminalizationKind.CANCEL_ORPHAN_SWEEP.value}',
        updated_at = NOW()
    FROM (
        SELECT t2.id, t2.status::text AS observed_status,
               t2.claimed_by_worker_id AS observed_worker_id,
               t2.claimed_at AS observed_claimed_at
        FROM horsies_tasks t2
        WHERE t2.is_workflow_task = TRUE
          AND t2.status::text IN ('CLAIMED', 'PENDING')
          AND NOT EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              WHERE wt.task_id = t2.id
                AND wt.status IN {_RUNNABLE_WORKFLOW_TASK_STATUSES_SQL}
          )
        LIMIT p_batch_size
        FOR UPDATE OF t2 SKIP LOCKED
    ) s
    WHERE t.id = s.id
    RETURNING t.id, NULL::bigint, 'APPLIED'::text,
              t.terminal_at, t.terminalization_kind,
              s.observed_status, s.observed_worker_id,
              s.observed_claimed_at,
              NULL::text, NULL::jsonb;
END;
$$
""")

DROP_CANCEL_ORPHANED_TASKS_SQL = text("""
DROP FUNCTION IF EXISTS horsies_cancel_orphaned_tasks(integer)
""")


# ---------------------------------------------------------------------------
# CANCELLED — workflow pause
# ---------------------------------------------------------------------------

# Single claimed node, fenced to the child process's claim generation. The
# pause literals are operation-owned, and the locked CTE preserves the claim
# pre-image before the update clears it.
CREATE_ABANDON_OWNED_NODE_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_abandon_owned_node(
    p_task_id varchar,
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_kind text;
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_applied boolean;
BEGIN
    WITH ctx AS MATERIALIZED (
        SELECT t.id, t.status::text AS status,
               t.claimed_by_worker_id, t.claimed_at
        FROM horsies_tasks t
        WHERE t.id = p_task_id
        FOR UPDATE
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'CANCELLED',
            claimed = FALSE,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            error_code = 'TASK_CANCELLED',
            failed_reason = 'Workflow paused before task start',
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.PAUSE_ABANDON_CLAIM.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
          AND ctx.status = 'CLAIMED'
          AND ctx.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
          AND (p_claimed_at IS NULL OR ctx.claimed_at = p_claimed_at)
        RETURNING t.terminal_at, t.terminalization_kind
    )
    SELECT ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           upd.terminal_at, upd.terminalization_kind,
           upd.terminal_at IS NOT NULL
    INTO v_status, v_worker, v_claimed_at,
         v_terminal_at, v_kind, v_applied
    FROM ctx
    LEFT JOIN upd ON TRUE;

    IF FOUND AND v_applied THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'APPLIED'::text,
            v_terminal_at, v_kind,
            v_status, v_worker, v_claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id,
        {_kind_array(TerminalizationKind.PAUSE_ABANDON_CLAIM)},
        p_worker_id,
        p_claimed_at
    );
END;
$$
""")

DROP_ABANDON_OWNED_NODE_SQL = text("""
DROP FUNCTION IF EXISTS horsies_abandon_owned_node(
    varchar, text, timestamptz
)
""")

# Id-keyed pairwise batch. Every input has exactly one output, keyed by its
# explicit ordinality. Preconditions are checked before the data-modifying CTE
# starts; PostgreSQL otherwise pads unequal arrays with NULL and an UPDATE FROM
# cannot define per-occurrence semantics for duplicate ids.
CREATE_ABANDON_OWNED_NODES_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_abandon_owned_nodes(
    p_ids varchar[],
    p_claimed_ats timestamptz[],
    p_worker_id text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
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

    RETURN QUERY
    WITH input AS MATERIALIZED (
        SELECT g.task_id, g.claimed_at, g.ordinality
        FROM unnest(p_ids, p_claimed_ats) WITH ORDINALITY
            AS g(task_id, claimed_at, ordinality)
    ),
    ctx AS MATERIALIZED (
        SELECT input.task_id, input.claimed_at AS expected_claimed_at,
               input.ordinality,
               t.status::text AS status,
               t.claimed_by_worker_id, t.claimed_at,
               t.terminal_at, t.terminalization_kind
        FROM input
        JOIN horsies_tasks t ON t.id = input.task_id
        FOR UPDATE OF t
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'CANCELLED',
            claimed = FALSE,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            error_code = 'TASK_CANCELLED',
            failed_reason = 'Workflow paused before task start',
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.task_id
          AND ctx.status = 'CLAIMED'
          AND ctx.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
          AND (
              ctx.expected_claimed_at IS NULL
              OR ctx.claimed_at = ctx.expected_claimed_at
          )
        RETURNING t.id, t.terminal_at, t.terminalization_kind
    )
    SELECT input.task_id,
           input.ordinality,
           CASE
               WHEN upd.id IS NOT NULL THEN 'APPLIED'
               WHEN ctx.task_id IS NULL THEN 'TASK_ABSENT'
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    AND ctx.terminalization_kind = ANY(
                        {_kind_array(TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH)}
                    ) THEN 'ALREADY_APPLIED'
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    THEN 'SOURCE_STATE_CONFLICT'
               WHEN ctx.claimed_by_worker_id
                        IS DISTINCT FROM CAST(p_worker_id AS VARCHAR)
                    OR (
                        input.claimed_at IS NOT NULL
                        AND ctx.claimed_at IS DISTINCT FROM input.claimed_at
                    ) THEN 'LOST_CLAIM'
               ELSE 'SOURCE_STATE_CONFLICT'
           END::text,
           CASE
               WHEN upd.id IS NOT NULL THEN upd.terminal_at
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    THEN ctx.terminal_at
               ELSE NULL::timestamptz
           END,
           CASE
               WHEN upd.id IS NOT NULL THEN upd.terminalization_kind
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    THEN ctx.terminalization_kind
               ELSE NULL::text
           END,
           ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           CASE
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    AND NOT ((
                        ctx.terminalization_kind = ANY(
                            {_kind_array(TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH)}
                        )
                    ) IS TRUE)
                    THEN 'FOREIGN_TERMINALIZATION'::text
               ELSE NULL::text
           END,
           NULL::jsonb
    FROM input
    LEFT JOIN ctx ON ctx.ordinality = input.ordinality
    LEFT JOIN upd ON upd.id = input.task_id
    ORDER BY input.ordinality;
END;
$$
""")

DROP_ABANDON_OWNED_NODES_SQL = text("""
DROP FUNCTION IF EXISTS horsies_abandon_owned_nodes(
    varchar[], timestamptz[], text
)
""")

# Workflow-scoped batch: the caller owns the workflow-task disposition in the
# same transaction. This function owns only the terminal task transition and
# reports only rows it changed; workflows whose guards match nothing produce
# no synthetic outcome.
CREATE_ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_abandon_nodes_of_paused_workflows(
    p_workflow_ids varchar[]
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH ctx AS MATERIALIZED (
        SELECT t.id, t.status::text AS status,
               t.claimed_by_worker_id, t.claimed_at
        FROM horsies_tasks t
        WHERE t.status = 'CLAIMED'
          AND EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t.id
                AND wt.workflow_id = ANY(p_workflow_ids)
                AND w.status = 'PAUSED'
                AND wt.status IN ('ENQUEUED', 'RUNNING')
          )
        FOR UPDATE OF t
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'CANCELLED',
            claimed = FALSE,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            error_code = 'TASK_CANCELLED',
            failed_reason = 'Workflow paused before task start',
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.PAUSE_ABANDON_WORKFLOW.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
        RETURNING t.id, t.terminal_at, t.terminalization_kind
    )
    SELECT upd.id, NULL::bigint, 'APPLIED'::text,
           upd.terminal_at, upd.terminalization_kind,
           ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           NULL::text, NULL::jsonb
    FROM upd
    JOIN ctx ON ctx.id = upd.id;
END;
$$
""")

DROP_ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL = text("""
DROP FUNCTION IF EXISTS horsies_abandon_nodes_of_paused_workflows(varchar[])
""")


# ---------------------------------------------------------------------------
# CANCELLED — workflow cancellation
# ---------------------------------------------------------------------------

# A claimed task uses the full ownership fence. The explicit carve-out admits
# a task this same child observed as requeued to PENDING, where no claim remains
# to fence; it is a property of this variant and cannot be supplied to another.
CREATE_CANCEL_OWNED_NODE_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_cancel_owned_node(
    p_task_id varchar,
    p_worker_id text,
    p_claimed_at timestamptz,
    p_accepts_requeued_pending boolean
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_kind text;
    v_status text;
    v_worker varchar;
    v_claimed_at timestamptz;
    v_applied boolean;
BEGIN
    WITH ctx AS MATERIALIZED (
        SELECT t.id, t.status::text AS status,
               t.claimed_by_worker_id, t.claimed_at
        FROM horsies_tasks t
        WHERE t.id = p_task_id
        FOR UPDATE
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'CANCELLED',
            claimed = FALSE,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.WORKFLOW_CANCEL_CLAIM.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
          AND (
              (
                  ctx.status = 'CLAIMED'
                  AND ctx.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
                  AND (
                      p_claimed_at IS NULL
                      OR ctx.claimed_at = p_claimed_at
                  )
              )
              OR (
                  p_accepts_requeued_pending
                  AND ctx.status = 'PENDING'
              )
          )
        RETURNING t.terminal_at, t.terminalization_kind
    )
    SELECT ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           upd.terminal_at, upd.terminalization_kind,
           upd.terminal_at IS NOT NULL
    INTO v_status, v_worker, v_claimed_at,
         v_terminal_at, v_kind, v_applied
    FROM ctx
    LEFT JOIN upd ON TRUE;

    IF FOUND AND v_applied THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'APPLIED'::text,
            v_terminal_at, v_kind,
            v_status, v_worker, v_claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id,
        {_kind_array(TerminalizationKind.WORKFLOW_CANCEL_CLAIM)},
        p_worker_id,
        p_claimed_at
    );
END;
$$
""")

DROP_CANCEL_OWNED_NODE_SQL = text("""
DROP FUNCTION IF EXISTS horsies_cancel_owned_node(
    varchar, text, timestamptz, boolean
)
""")

CREATE_CANCEL_OWNED_NODES_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_cancel_owned_nodes(
    p_ids varchar[],
    p_claimed_ats timestamptz[],
    p_worker_id text
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
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

    RETURN QUERY
    WITH input AS MATERIALIZED (
        SELECT g.task_id, g.claimed_at, g.ordinality
        FROM unnest(p_ids, p_claimed_ats) WITH ORDINALITY
            AS g(task_id, claimed_at, ordinality)
    ),
    ctx AS MATERIALIZED (
        SELECT input.task_id, input.claimed_at AS expected_claimed_at,
               input.ordinality,
               t.status::text AS status,
               t.claimed_by_worker_id, t.claimed_at,
               t.terminal_at, t.terminalization_kind
        FROM input
        JOIN horsies_tasks t ON t.id = input.task_id
        FOR UPDATE OF t
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'CANCELLED',
            claimed = FALSE,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.task_id
          AND ctx.status = 'CLAIMED'
          AND ctx.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
          AND (
              ctx.expected_claimed_at IS NULL
              OR ctx.claimed_at = ctx.expected_claimed_at
          )
        RETURNING t.id, t.terminal_at, t.terminalization_kind
    )
    SELECT input.task_id,
           input.ordinality,
           CASE
               WHEN upd.id IS NOT NULL THEN 'APPLIED'
               WHEN ctx.task_id IS NULL THEN 'TASK_ABSENT'
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    AND ctx.terminalization_kind = ANY(
                        {_kind_array(TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH)}
                    ) THEN 'ALREADY_APPLIED'
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    THEN 'SOURCE_STATE_CONFLICT'
               WHEN ctx.claimed_by_worker_id
                        IS DISTINCT FROM CAST(p_worker_id AS VARCHAR)
                    OR (
                        input.claimed_at IS NOT NULL
                        AND ctx.claimed_at IS DISTINCT FROM input.claimed_at
                    ) THEN 'LOST_CLAIM'
               ELSE 'SOURCE_STATE_CONFLICT'
           END::text,
           CASE
               WHEN upd.id IS NOT NULL THEN upd.terminal_at
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    THEN ctx.terminal_at
               ELSE NULL::timestamptz
           END,
           CASE
               WHEN upd.id IS NOT NULL THEN upd.terminalization_kind
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    THEN ctx.terminalization_kind
               ELSE NULL::text
           END,
           ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           CASE
               WHEN ctx.status IN {TERMINAL_STATUSES_SQL}
                    AND NOT ((
                        ctx.terminalization_kind = ANY(
                            {_kind_array(TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH)}
                        )
                    ) IS TRUE)
                    THEN 'FOREIGN_TERMINALIZATION'::text
               ELSE NULL::text
           END,
           NULL::jsonb
    FROM input
    LEFT JOIN ctx ON ctx.ordinality = input.ordinality
    LEFT JOIN upd ON upd.id = input.task_id
    ORDER BY input.ordinality;
END;
$$
""")

DROP_CANCEL_OWNED_NODES_SQL = text("""
DROP FUNCTION IF EXISTS horsies_cancel_owned_nodes(
    varchar[], timestamptz[], text
)
""")

CREATE_CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL = text(f"""
CREATE OR REPLACE FUNCTION horsies_cancel_nodes_of_cancelled_workflow(
    p_workflow_ids varchar[]
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH ctx AS MATERIALIZED (
        SELECT t.id, t.status::text AS status,
               t.claimed_by_worker_id, t.claimed_at
        FROM horsies_tasks t
        WHERE t.status IN ('PENDING', 'CLAIMED', 'RUNNING')
          AND EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t.id
                AND wt.workflow_id = ANY(p_workflow_ids)
                AND w.status = 'CANCELLED'
                AND wt.status = 'ENQUEUED'
          )
        FOR UPDATE OF t
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'CANCELLED',
            claimed = FALSE,
            claimed_at = NULL,
            claimed_by_worker_id = NULL,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            terminal_at = NOW(),
            terminalization_kind =
                '{TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW.value}',
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
        RETURNING t.id, t.terminal_at, t.terminalization_kind
    )
    SELECT upd.id, NULL::bigint, 'APPLIED'::text,
           upd.terminal_at, upd.terminalization_kind,
           ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           NULL::text, NULL::jsonb
    FROM upd
    JOIN ctx ON ctx.id = upd.id;
END;
$$
""")

DROP_CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL = text("""
DROP FUNCTION IF EXISTS horsies_cancel_nodes_of_cancelled_workflow(varchar[])
""")


# Drops precede creates on every apply, and the drop list names exact
# signatures: PostgreSQL overloads by signature, so a changed argument list
# without a matching drop leaves the old overload installed and callable.
DROP_TERMINALIZATION_FUNCTIONS_SQL: tuple[TextClause, ...] = (
    DROP_COMPLETE_LOCKED_TASK_SQL,
    DROP_COMPLETE_TASK_FUSED_SQL,
    DROP_FAIL_LOCKED_TASK_SQL,
    DROP_FAIL_STALE_TASK_SQL,
    DROP_EXPIRE_OWNED_CLAIM_SQL,
    DROP_EXPIRE_PENDING_TASKS_SQL,
    DROP_CANCEL_LOCKED_TASK_SQL,
    DROP_CANCEL_OWNED_ORPHAN_SQL,
    DROP_CANCEL_ORPHANED_TASKS_SQL,
    DROP_ABANDON_OWNED_NODE_SQL,
    DROP_ABANDON_OWNED_NODES_SQL,
    DROP_ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL,
    DROP_CANCEL_OWNED_NODE_SQL,
    DROP_CANCEL_OWNED_NODES_SQL,
    DROP_CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL,
    DROP_MISS_CLASSIFIER_SQL,
)

# The type is created once and never dropped, so it cannot announce a stale
# drop list. Catalog conformance does that instead: the installed signature set
# is asserted against the expected one, and an overload left behind by a
# changed argument list appears there as a difference.
CREATE_TERMINALIZATION_FUNCTIONS_SQL: tuple[TextClause, ...] = (
    CREATE_MISS_CLASSIFIER_SQL,
    CREATE_COMPLETE_LOCKED_TASK_SQL,
    CREATE_COMPLETE_TASK_FUSED_SQL,
    CREATE_FAIL_LOCKED_TASK_SQL,
    CREATE_FAIL_STALE_TASK_SQL,
    CREATE_EXPIRE_OWNED_CLAIM_SQL,
    CREATE_EXPIRE_PENDING_TASKS_SQL,
    CREATE_CANCEL_LOCKED_TASK_SQL,
    CREATE_CANCEL_OWNED_ORPHAN_SQL,
    CREATE_CANCEL_ORPHANED_TASKS_SQL,
    CREATE_ABANDON_OWNED_NODE_SQL,
    CREATE_ABANDON_OWNED_NODES_SQL,
    CREATE_ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL,
    CREATE_CANCEL_OWNED_NODE_SQL,
    CREATE_CANCEL_OWNED_NODES_SQL,
    CREATE_CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL,
)
