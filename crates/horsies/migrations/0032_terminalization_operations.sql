-- Database-owned terminalization operations (parity with horsies PRs
-- #226/#227 through #240, schema v19-v26 end state).
--
-- terminalization_kind: provenance column, never caller-supplied — each
-- operation function hardcodes its own kind. Value-domain CHECK only; NULL
-- means the row was terminalized before the column existed and provenance is
-- never inferred. The composite type horsies_terminalization_outcome is
-- created once and never dropped (a shape change is a new type name or an
-- explicit ALTER, not a silent recreate). Functions are installed by DROP
-- then CREATE with exact signatures: PostgreSQL overloads by signature, so a
-- changed argument list without a matching drop leaves a stale overload
-- callable. Bodies are transcribed verbatim from the Python port's rendered
-- SQL (horsies/core/schemas/terminalization.py at SCHEMA_VERSION 26);
-- future Python-side body changes reach Rust as a new migration re-running
-- this DROP+CREATE set (the 0027 horsies_claim precedent).
--
-- Nothing calls these functions yet; call sites cut over per operation in
-- later commits.

ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS terminalization_kind TEXT;

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
            OR terminalization_kind IN ('CANCEL_ADMIN', 'CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP', 'COMPLETE_FUSED', 'COMPLETE_LOCKED', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING', 'FAIL_RUNNING', 'FAIL_STALE', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW')
        ) NOT VALID;
    END IF;
END
$$;

ALTER TABLE horsies_tasks
    VALIDATE CONSTRAINT ck_horsies_tasks_terminalization_kind;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'horsies_terminalization_outcome'
          AND n.nspname = current_schema()
    ) THEN
        CREATE TYPE horsies_terminalization_outcome AS (
            task_id varchar,
    ordinality bigint,
    outcome text,
    terminal_at timestamptz,
    terminalization_kind text,
    observed_status text,
    observed_worker_id varchar,
    observed_claimed_at timestamptz,
    guard_kind text,
    observed_guard jsonb
        );
    END IF;
END
$$;

DROP FUNCTION IF EXISTS horsies_complete_locked_task(varchar, text, text);

DROP FUNCTION IF EXISTS horsies_complete_task_fused(
    varchar, text, timestamptz, text, text, text
);

DROP FUNCTION IF EXISTS horsies_fail_locked_task(
    varchar, text, text, text, text
);

DROP FUNCTION IF EXISTS horsies_fail_stale_task(
    varchar, integer, integer, text, text, text
);

DROP FUNCTION IF EXISTS horsies_expire_owned_claim(
    varchar, text, text, text
);

DROP FUNCTION IF EXISTS horsies_expire_pending_tasks(
    integer, text, text
);

DROP FUNCTION IF EXISTS horsies_cancel_locked_task(varchar, text[]);

DROP FUNCTION IF EXISTS horsies_cancel_owned_orphan(
    varchar, text, timestamptz
);

DROP FUNCTION IF EXISTS horsies_cancel_orphaned_tasks(integer);

DROP FUNCTION IF EXISTS horsies_abandon_owned_node(
    varchar, text, timestamptz
);

DROP FUNCTION IF EXISTS horsies_abandon_owned_nodes(
    varchar[], timestamptz[], text
);

DROP FUNCTION IF EXISTS horsies_abandon_nodes_of_paused_workflows(varchar[]);

DROP FUNCTION IF EXISTS horsies_cancel_owned_node(
    varchar, text, timestamptz, boolean
);

DROP FUNCTION IF EXISTS horsies_cancel_owned_nodes(
    varchar[], timestamptz[], text
);

DROP FUNCTION IF EXISTS horsies_cancel_nodes_of_cancelled_workflow(varchar[]);

DROP FUNCTION IF EXISTS horsies_terminalization_miss(
    varchar, text[], text, timestamptz
);

CREATE OR REPLACE FUNCTION horsies_terminalization_miss(
    p_task_id varchar,
    p_equivalent_kinds text[],
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF horsies_terminalization_outcome
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

    IF v_row.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
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
$$;

CREATE OR REPLACE FUNCTION horsies_complete_locked_task(
    p_task_id varchar,
    p_worker_id text,
    p_result text
)
RETURNS SETOF horsies_terminalization_outcome
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
        failed_reason = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        terminalization_kind = 'COMPLETE_LOCKED',
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
        ARRAY['COMPLETE_FUSED', 'COMPLETE_LOCKED']::text[],
        p_worker_id,
        NULL::timestamptz
    );
END;
$$;

CREATE OR REPLACE FUNCTION horsies_complete_task_fused(
    p_task_id varchar,
    p_worker_id text,
    p_claimed_at timestamptz,
    p_result text,
    p_notify_channel text,
    p_notify_payload text
)
RETURNS SETOF horsies_terminalization_outcome
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
            failed_reason = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            terminal_at = NOW(),
            terminalization_kind =
                'COMPLETE_FUSED',
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
        ARRAY['COMPLETE_FUSED', 'COMPLETE_LOCKED']::text[],
        p_worker_id,
        p_claimed_at
    );
END;
$$;

CREATE OR REPLACE FUNCTION horsies_fail_locked_task(
    p_task_id varchar,
    p_worker_id text,
    p_result text,
    p_error_code text,
    p_failed_reason text
)
RETURNS SETOF horsies_terminalization_outcome
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
        failed_reason = p_failed_reason,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        terminalization_kind = 'FAIL_RUNNING',
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
        ARRAY['FAIL_RUNNING']::text[],
        p_worker_id,
        NULL::timestamptz
    );
END;
$$;

CREATE OR REPLACE FUNCTION horsies_fail_stale_task(
    p_task_id varchar,
    p_stale_after_ms integer,
    p_finalizing_stale_after_ms integer,
    p_result text,
    p_error_code text,
    p_failed_reason text
)
RETURNS SETOF horsies_terminalization_outcome
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
                    'FAIL_STALE',
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
        ARRAY['FAIL_STALE']::text[],
        NULL::text,
        NULL::timestamptz
    );
END;
$$;

CREATE OR REPLACE FUNCTION horsies_expire_owned_claim(
    p_task_id varchar,
    p_worker_id text,
    p_result text,
    p_error_code text
)
RETURNS SETOF horsies_terminalization_outcome
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
            failed_reason = NULL,
            terminal_at = NOW(),
            terminalization_kind =
                'EXPIRE_CLAIMED',
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
            ARRAY['EXPIRE_CLAIMED', 'EXPIRE_PENDING']::text[],
            p_worker_id,
            NULL::timestamptz
        );
        RETURN;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION horsies_expire_pending_tasks(
    p_batch_size integer,
    p_result text,
    p_error_code text
)
RETURNS SETOF horsies_terminalization_outcome
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
        failed_reason = NULL,
        terminal_at = NOW(),
        terminalization_kind = 'EXPIRE_PENDING',
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
$$;

CREATE OR REPLACE FUNCTION horsies_cancel_locked_task(
    p_task_id varchar,
    p_permitted_source_statuses text[]
)
RETURNS SETOF horsies_terminalization_outcome
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
                'CANCEL_ADMIN',
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
        ARRAY['CANCEL_ADMIN']::text[],
        NULL::text,
        NULL::timestamptz
    );
END;
$$;

CREATE OR REPLACE FUNCTION horsies_cancel_owned_orphan(
    p_task_id varchar,
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF horsies_terminalization_outcome
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
                     AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
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
                'CANCEL_ORPHAN',
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
        ARRAY['CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP']::text[],
        p_worker_id,
        p_claimed_at
    );
END;
$$;

CREATE OR REPLACE FUNCTION horsies_cancel_orphaned_tasks(
    p_batch_size integer
)
RETURNS SETOF horsies_terminalization_outcome
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
            'CANCEL_ORPHAN_SWEEP',
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
                AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
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
$$;

CREATE OR REPLACE FUNCTION horsies_abandon_owned_node(
    p_task_id varchar,
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF horsies_terminalization_outcome
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
                'PAUSE_ABANDON_CLAIM',
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
        ARRAY['PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW']::text[],
        p_worker_id,
        p_claimed_at
    );
END;
$$;

CREATE OR REPLACE FUNCTION horsies_abandon_owned_nodes(
    p_ids varchar[],
    p_claimed_ats timestamptz[],
    p_worker_id text
)
RETURNS SETOF horsies_terminalization_outcome
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
                'PAUSE_ABANDON_CLAIM_BATCH',
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
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                    AND ctx.terminalization_kind = ANY(
                        ARRAY['PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW']::text[]
                    ) THEN 'ALREADY_APPLIED'
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
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
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                    THEN ctx.terminal_at
               ELSE NULL::timestamptz
           END,
           CASE
               WHEN upd.id IS NOT NULL THEN upd.terminalization_kind
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                    THEN ctx.terminalization_kind
               ELSE NULL::text
           END,
           ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           CASE
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                    AND NOT ((
                        ctx.terminalization_kind = ANY(
                            ARRAY['PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH', 'PAUSE_ABANDON_WORKFLOW']::text[]
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
$$;

CREATE OR REPLACE FUNCTION horsies_abandon_nodes_of_paused_workflows(
    p_workflow_ids varchar[]
)
RETURNS SETOF horsies_terminalization_outcome
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
                'PAUSE_ABANDON_WORKFLOW',
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
$$;

CREATE OR REPLACE FUNCTION horsies_cancel_owned_node(
    p_task_id varchar,
    p_worker_id text,
    p_claimed_at timestamptz,
    p_accepts_requeued_pending boolean
)
RETURNS SETOF horsies_terminalization_outcome
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
                'WORKFLOW_CANCEL_CLAIM',
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
        ARRAY['WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW']::text[],
        p_worker_id,
        p_claimed_at
    );
END;
$$;

CREATE OR REPLACE FUNCTION horsies_cancel_owned_nodes(
    p_ids varchar[],
    p_claimed_ats timestamptz[],
    p_worker_id text
)
RETURNS SETOF horsies_terminalization_outcome
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
                'WORKFLOW_CANCEL_CLAIM_BATCH',
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
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                    AND ctx.terminalization_kind = ANY(
                        ARRAY['WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW']::text[]
                    ) THEN 'ALREADY_APPLIED'
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
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
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                    THEN ctx.terminal_at
               ELSE NULL::timestamptz
           END,
           CASE
               WHEN upd.id IS NOT NULL THEN upd.terminalization_kind
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                    THEN ctx.terminalization_kind
               ELSE NULL::text
           END,
           ctx.status, ctx.claimed_by_worker_id, ctx.claimed_at,
           CASE
               WHEN ctx.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                    AND NOT ((
                        ctx.terminalization_kind = ANY(
                            ARRAY['WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW']::text[]
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
$$;

CREATE OR REPLACE FUNCTION horsies_cancel_nodes_of_cancelled_workflow(
    p_workflow_ids varchar[]
)
RETURNS SETOF horsies_terminalization_outcome
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
                'WORKFLOW_CANCEL_WORKFLOW',
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
$$;

