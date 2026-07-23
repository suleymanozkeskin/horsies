"""PostgreSQL schema migrations — idempotent ALTER TABLE statements."""

from __future__ import annotations

from sqlalchemy import text


# ---- Schema infrastructure ----
#
# v3: claim-path indexes (idx_horsies_tasks_claim_pending,
#     idx_horsies_tasks_claim_expired, idx_horsies_tasks_worker_status,
#     idx_horsies_worker_states_worker_snapshot).
# v4: widen horsies_heartbeats.id / horsies_worker_states.id to BIGINT
#     (int4 sequences exhaust within months at heartbeat insert rates).
# v5: drop write-amplifying single-column indexes on horsies_tasks whose
#     queries are served by the v3 partial composites (or that no query
#     uses); replace full good_until index with a partial one.
# v6: split notify triggers into INSERT/UPDATE pairs with WHEN clauses so
#     non-status updates (lease renewals etc.) never invoke plpgsql.
# v7: ordered partial index for the expired claim arm
#     (idx_horsies_tasks_claim_expired_ordered) — the v3 expiry-filter
#     index cannot serve the arm's ORDER BY, so deep expired backlogs
#     paid a full sort under SKIP LOCKED.
# v8: composite (workflow_id, status, task_index) on horsies_workflow_tasks
#     (idx_horsies_workflow_tasks_wf_status_idx) — first-failed lookups ran
#     an O(N) ordered scan per FAILED completion inside the workflow
#     completion lock.
# v9: add horsies_worker_states.children_memory_mb — summed RSS of executor
#     children. The existing memory_usage_mb is parent-only, so per-child
#     memory growth (the dyno-quota driver) was invisible in worker telemetry.

# v10: horsies_claim(...) plpgsql function — collapses the claim critical
#      section (advisory-lock acquisition + cap counts + windowed claim) into
#      ONE server-side statement so the xact-scoped advisory lock is never held
#      across a client round trip. Removes the client-stall-while-holding-lock
#      freeze. Cap semantics preserved (never over-claims; may under-claim under
#      SKIP-LOCKED contention, deferring work to the next pass).
#
# v11: retention eligibility indexes. idx_horsies_tasks_retention — partial
#      expression index on COALESCE(completed_at, failed_at, updated_at,
#      created_at) over terminal statuses; idx_horsies_worker_states_snapshot_at.
#      The hourly retention deletes seq-scanned both heaps on every pass,
#      including passes with zero eligible rows.
#
# v12: horsies_claim returns claimed_at — the claim-generation fence (C10).
#      Finalize CASes fenced on (status, worker_id) alone let a stale finalize
#      from a reaper-requeued attempt clobber a live attempt the SAME worker
#      re-claimed (worker_id matches, status matches). claimed_at identifies
#      the claim generation: set by the claim, cleared by every requeue.
#      The return-type change requires DROP + CREATE (CREATE OR REPLACE
#      cannot change OUT columns).
#      Also idx_horsies_heartbeats_sent_at: heartbeat retention deletes filter
#      sent_at < cutoff, but the composite (task_id, role, sent_at DESC) index
#      cannot serve a leading-column sent_at range, so every hourly pass
#      scanned the heartbeats heap — the v11 retention indexes covered tasks
#      and worker_states and omitted heartbeats.
#
# v13: idx_horsies_workflows_retention — partial expression index on
#      COALESCE(completed_at, updated_at, created_at) over terminal workflow
#      statuses. The workflow and workflow_tasks retention deletes both
#      filter horsies_workflows on this predicate; with no supporting index
#      and no statistics on the expression, the planner overestimated
#      eligibility and chose a stop-early pkey walk whose LIMIT never
#      filled — a full-table walk per statement, twice per hourly pass,
#      serial under FOR UPDATE, regardless of how few rows were eligible.
#      Completes the v11/v12 retention-index set (tasks, worker_states,
#      heartbeats, now workflows).

SCHEMA_VERSION = 13

SCHEMA_ADVISORY_LOCK_SQL = text("""
    SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))
""")

# Collapsed claim. The advisory lock is the FIRST imperative statement (a
# plpgsql function executes sequentially), guaranteeing acquisition before any
# read with predictable ordering — a raw CTE cannot guarantee this. Held until
# the caller COMMITs (xact-scoped); the only remaining client gap while held is
# the commit round trip.
#
# NOTE: the return type deliberately diverges from horsies-rust — its
# 0024_claim_function.sql has no claimed_at OUT column (Rust fences finalize
# on started_at, viable in-process; this repo's process boundary requires the
# claim-time marker, C10 / v12). Shared-database interoperability is BLOCKED
# on this: Rust's 0024 cannot apply against a v12 schema (return-type
# change), and a drop-first apply would remove the column this fence reads.
# Align the return shapes on both sides before any shared-DB deployment.
#
# Claim ordering (global rank: qprio, priority, enqueued_at, id):
#   - Distinct queue priorities: queue priority dominates — identical to the
#     prior per-queue loop.
#   - EQUAL queue priorities: an intentional semantic change. The prior loop
#     broke ties by configured queue order (arbitrary, can starve a queue);
#     this pools the equal-priority band and orders by task priority then FIFO.
#     So equal-importance tasks across such queues are FIFO, while an explicit
#     task/workflow-node priority still preempts within the band. Pinned by
#     tests/integration/test_claim_function.py.
CREATE_CLAIM_FUNCTION_SQL = text("""
CREATE OR REPLACE FUNCTION horsies_claim(
    p_worker_id text,
    p_queues jsonb,
    p_queue_priority jsonb,
    p_queue_max_concurrency jsonb,
    p_hard_cap_mode boolean,
    p_processes int,
    p_prefetch_buffer int,
    p_max_claim_per_worker int,
    p_max_claim_batch int,
    p_cluster_wide_cap int,
    p_lease_ms bigint,
    p_lock_keys jsonb
)
RETURNS TABLE(
    id varchar,
    task_name varchar,
    args text,
    kwargs text,
    queue_name varchar,
    is_workflow_task boolean,
    task_options text,
    claimed_at timestamptz
)
LANGUAGE plpgsql
AS $func$
#variable_conflict use_column
DECLARE
    v_key bigint;
    v_capped text[];
    v_my_claimed int;
    v_my_running int;
    v_my_in_flight int;
    v_global_in_flight int;
    v_queue_counts jsonb;
    v_max_claimed int;
    v_local_available int;
    v_global_remaining int;
    v_total int;
BEGIN
    -- 1. Acquire advisory locks first, in deadlock-safe ascending order.
    FOR v_key IN
        SELECT elem::bigint AS k
        FROM jsonb_array_elements_text(p_lock_keys) AS elem
        ORDER BY 1
    LOOP
        PERFORM pg_advisory_xact_lock(v_key);
    END LOOP;

    SELECT COALESCE(array_agg(key), ARRAY[]::text[])
    INTO v_capped
    FROM jsonb_object_keys(p_queue_max_concurrency) AS key;

    -- 2. Cap accounting under the lock snapshot (mirrors CLAIM_PASS_COUNTS_SQL).
    --    Per-queue counts: hard mode counts RUNNING+CLAIMED, soft counts RUNNING.
    WITH in_flight AS (
        SELECT queue_name, status, claimed_by_worker_id
        FROM horsies_tasks
        WHERE status = 'RUNNING'
           OR (status = 'CLAIMED'
               AND (claim_expires_at IS NULL OR claim_expires_at > now()))
    )
    SELECT
        COUNT(*) FILTER (
            WHERE claimed_by_worker_id = p_worker_id AND status = 'CLAIMED'),
        COUNT(*) FILTER (
            WHERE claimed_by_worker_id = p_worker_id AND status = 'RUNNING'),
        COUNT(*) FILTER (WHERE claimed_by_worker_id = p_worker_id),
        COUNT(*),
        COALESCE((
            SELECT jsonb_object_agg(g.queue_name, g.cnt) FROM (
                SELECT queue_name, COUNT(*) AS cnt
                FROM in_flight
                WHERE queue_name = ANY(v_capped)
                  AND (p_hard_cap_mode OR status = 'RUNNING')
                GROUP BY queue_name
            ) g
        ), '{}'::jsonb)
    INTO v_my_claimed, v_my_running, v_my_in_flight,
         v_global_in_flight, v_queue_counts
    FROM in_flight;

    -- 3. Budget math (mirrors claiming.py worker-local / global budget).
    v_max_claimed := CASE
        WHEN p_max_claim_per_worker > 0 THEN p_max_claim_per_worker
        WHEN p_prefetch_buffer > 0 THEN p_processes + p_prefetch_buffer
        ELSE p_processes END;
    IF v_my_claimed >= v_max_claimed THEN
        RETURN;
    END IF;

    IF p_hard_cap_mode THEN
        v_local_available := p_processes - v_my_in_flight;
    ELSE
        v_local_available :=
            (p_processes + p_prefetch_buffer) - v_my_running - v_my_claimed;
    END IF;
    IF v_local_available < 0 THEN
        v_local_available := 0;
    END IF;

    IF p_cluster_wide_cap IS NOT NULL THEN
        v_global_remaining := GREATEST(0, p_cluster_wide_cap - v_global_in_flight);
    ELSE
        v_global_remaining := 2147483647;
    END IF;

    v_total := LEAST(
        v_local_available, v_max_claimed - v_my_claimed, v_global_remaining);
    IF v_total <= 0 THEN
        RETURN;
    END IF;

    -- 4. Windowed claim: two-arm eligibility per queue (bounded lock fan-out),
    --    per-queue cap filter, global priority rank, global budget trim.
    RETURN QUERY
    WITH cand AS (
        SELECT c.id, qn.name AS queue_name, c.priority, c.enqueued_at,
               COALESCE((p_queue_priority ->> qn.name)::int, 100) AS qprio
        FROM jsonb_array_elements_text(p_queues) AS qn(name)
        CROSS JOIN LATERAL (
            -- Two eligibility arms as separate CTEs (FOR UPDATE is not allowed
            -- on UNION operands, so lock per-arm then union the references —
            -- same shape as the standalone CLAIM_SQL).
            WITH pend AS (
                SELECT t.id, t.priority, t.enqueued_at
                FROM horsies_tasks t
                WHERE t.queue_name = qn.name
                  AND t.status = 'PENDING'
                  AND t.enqueued_at <= now()
                  AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
                  AND (t.good_until IS NULL OR t.good_until > now())
                ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT v_total
            ),
            expired AS (
                SELECT t.id, t.priority, t.enqueued_at
                FROM horsies_tasks t
                WHERE t.queue_name = qn.name
                  AND t.status = 'CLAIMED'
                  AND t.claim_expires_at IS NOT NULL
                  AND t.claim_expires_at < now()
                  AND t.enqueued_at <= now()
                  AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
                  AND (t.good_until IS NULL OR t.good_until > now())
                ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT v_total
            )
            SELECT m.id, m.priority, m.enqueued_at
            FROM (SELECT * FROM pend UNION ALL SELECT * FROM expired) m
            ORDER BY m.priority ASC, m.enqueued_at ASC, m.id ASC
            LIMIT v_total
        ) c
    ),
    per_queue AS (
        SELECT cand.id, cand.queue_name, cand.priority, cand.enqueued_at,
               cand.qprio,
               row_number() OVER (
                   PARTITION BY cand.queue_name
                   ORDER BY cand.priority ASC, cand.enqueued_at ASC, cand.id ASC
               ) AS rn_q
        FROM cand
    ),
    capped AS (
        SELECT per_queue.id, per_queue.qprio, per_queue.priority,
               per_queue.enqueued_at
        FROM per_queue
        WHERE per_queue.rn_q <= LEAST(
            CASE WHEN p_queue_max_concurrency ? per_queue.queue_name
                THEN GREATEST(0,
                    (p_queue_max_concurrency ->> per_queue.queue_name)::int
                    - COALESCE((v_queue_counts ->> per_queue.queue_name)::int, 0))
                ELSE v_total END,
            CASE WHEN p_max_claim_batch > 0 THEN p_max_claim_batch ELSE v_total END
        )
    ),
    ranked AS (
        SELECT capped.id,
               row_number() OVER (
                   ORDER BY capped.qprio ASC, capped.priority ASC,
                            capped.enqueued_at ASC, capped.id ASC
               ) AS rn
        FROM capped
    ),
    pick AS (
        SELECT ranked.id FROM ranked WHERE ranked.rn <= v_total
    )
    UPDATE horsies_tasks t
    SET status = 'CLAIMED',
        claimed = TRUE,
        claimed_at = now(),
        claimed_by_worker_id = p_worker_id,
        claim_expires_at = now() + p_lease_ms * INTERVAL '1 millisecond',
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = now()
    FROM pick
    WHERE t.id = pick.id
    RETURNING t.id, t.task_name, t.args, t.kwargs, t.queue_name,
              t.is_workflow_task, t.task_options, t.claimed_at;
END;
$func$;
""")

# v12: the RETURNS TABLE column set changed (claimed_at added), and
# CREATE OR REPLACE cannot change a function's OUT columns — drop first.
# The exact signature pins the drop to the v10/v11 definition.
DROP_CLAIM_FUNCTION_SQL = text("""
DROP FUNCTION IF EXISTS horsies_claim(
    text, jsonb, jsonb, jsonb, boolean, int, int, int, int, int, bigint, jsonb
)
""")

CREATE_SCHEMA_VERSION_TABLE_SQL = text("""
    CREATE TABLE IF NOT EXISTS horsies_schema_version (
        version INTEGER PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
""")

SCHEMA_VERSION_TABLE_EXISTS_SQL = text("""
    SELECT to_regclass('horsies_schema_version') IS NOT NULL
""")

READ_SCHEMA_VERSION_SQL = text("""
    SELECT COALESCE(MAX(version), 0) FROM horsies_schema_version
""")

INSERT_SCHEMA_VERSION_SQL = text("""
    INSERT INTO horsies_schema_version (version)
    VALUES (:version)
    ON CONFLICT (version) DO NOTHING
""")


# ---- Task table migrations ----

SET_TASK_COLUMN_DEFAULTS_SQL = text("""
    ALTER TABLE horsies_tasks
    ALTER COLUMN claimed SET DEFAULT FALSE,
    ALTER COLUMN retry_count SET DEFAULT 0,
    ALTER COLUMN max_retries SET DEFAULT 0,
    ALTER COLUMN priority SET DEFAULT 100,
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at SET DEFAULT NOW();
""")

# Migration: add enqueued_at column and backfill from sent_at for existing rows.
# Column is added WITHOUT a default so existing rows stay NULL, allowing the
# backfill to copy historical sent_at values.  The default and NOT NULL
# constraint are applied after backfill.
ADD_ENQUEUED_AT_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS enqueued_at TIMESTAMPTZ;
""")
BACKFILL_ENQUEUED_AT_SQL = text("""
    UPDATE horsies_tasks
    SET enqueued_at = COALESCE(sent_at, NOW())
    WHERE enqueued_at IS NULL;
""")
SET_ENQUEUED_AT_NOT_NULL_SQL = text("""
    ALTER TABLE horsies_tasks
    ALTER COLUMN enqueued_at SET NOT NULL;
""")
SET_ENQUEUED_AT_DEFAULT_SQL = text("""
    ALTER TABLE horsies_tasks
    ALTER COLUMN enqueued_at SET DEFAULT NOW();
""")

# Migration: add enqueue_sha column for idempotent enqueue verification.
# 3-step: add nullable column, backfill NULLs, enforce NOT NULL.
ADD_ENQUEUE_SHA_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS enqueue_sha VARCHAR(64);
""")
BACKFILL_ENQUEUE_SHA_SQL = text("""
    UPDATE horsies_tasks SET enqueue_sha = 'legacy-pre-sha'
    WHERE enqueue_sha IS NULL;
""")
SET_ENQUEUE_SHA_NOT_NULL_SQL = text("""
    ALTER TABLE horsies_tasks
    ALTER COLUMN enqueue_sha SET NOT NULL;
""")


# ---- Workflow table migrations ----

ADD_TASK_OPTIONS_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS task_options TEXT;
""")

ADD_SUCCESS_POLICY_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS success_policy JSONB;
""")

ADD_JOIN_TYPE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS join_type VARCHAR(10) NOT NULL DEFAULT 'all';
""")

ADD_MIN_SUCCESS_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS min_success INTEGER;
""")

ADD_NODE_ID_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS node_id VARCHAR(128);
""")


ADD_PARENT_WORKFLOW_ID_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS parent_workflow_id VARCHAR(36);
""")

ADD_PARENT_TASK_INDEX_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS parent_task_index INTEGER;
""")

ADD_DEPTH_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS depth INTEGER NOT NULL DEFAULT 0;
""")

ADD_ROOT_WORKFLOW_ID_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS root_workflow_id VARCHAR(36);
""")

ADD_DEFINITION_KEY_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS definition_key VARCHAR(255);
""")

ADD_IS_SUBWORKFLOW_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS is_subworkflow BOOLEAN NOT NULL DEFAULT FALSE;
""")

ADD_SUB_WORKFLOW_ID_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_workflow_id VARCHAR(36);
""")

ADD_SUB_WORKFLOW_NAME_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_workflow_name VARCHAR(255);
""")

DROP_SUB_WORKFLOW_RETRY_MODE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    DROP COLUMN IF EXISTS sub_workflow_retry_mode;
""")

ADD_SUB_WORKFLOW_SUMMARY_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_workflow_summary TEXT;
""")

ADD_SUB_DEFINITION_KEY_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_definition_key VARCHAR(255);
""")

DROP_WORKFLOW_DEF_MODULE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    DROP COLUMN IF EXISTS workflow_def_module;
""")

DROP_WORKFLOW_DEF_QUALNAME_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    DROP COLUMN IF EXISTS workflow_def_qualname;
""")

DROP_SUB_WORKFLOW_MODULE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    DROP COLUMN IF EXISTS sub_workflow_module;
""")

DROP_SUB_WORKFLOW_QUALNAME_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    DROP COLUMN IF EXISTS sub_workflow_qualname;
""")

CREATE_TASK_ATTEMPTS_TABLE_SQL = text("""
    CREATE TABLE IF NOT EXISTS horsies_task_attempts (
        id BIGSERIAL PRIMARY KEY,
        task_id VARCHAR(36) NOT NULL REFERENCES horsies_tasks(id) ON DELETE CASCADE,
        attempt INTEGER NOT NULL,
        outcome VARCHAR(32) NOT NULL,
        will_retry BOOLEAN NOT NULL DEFAULT FALSE,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL,
        error_code TEXT,
        error_message TEXT,
        failed_reason TEXT,
        worker_id VARCHAR(255),
        worker_hostname VARCHAR(255),
        worker_pid INTEGER,
        worker_process_name VARCHAR(255),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_horsies_task_attempts_task_attempt UNIQUE (task_id, attempt),
        CONSTRAINT ck_horsies_task_attempts_outcome
            CHECK (outcome IN ('COMPLETED', 'FAILED', 'WORKER_FAILURE'))
    )
""")

ADD_ERROR_CODE_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS error_code TEXT;
""")

ADD_TASK_IS_WORKFLOW_TASK_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS is_workflow_task BOOLEAN NOT NULL DEFAULT FALSE;
""")

BACKFILL_TASK_IS_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_tasks t
    SET is_workflow_task = TRUE
    WHERE is_workflow_task = FALSE
      AND EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt WHERE wt.task_id = t.id
      );
""")

ADD_TASK_FINALIZING_COLUMNS_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS finalizing_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS finalizing_by_worker_id TEXT;
""")

# Migration (v4): widen timeseries PKs to BIGINT. Both tables are
# retention-bounded (hourly/periodic cleanup), so the type rewrite is cheap.
WIDEN_HEARTBEATS_ID_TO_BIGINT_SQL = text("""
    ALTER TABLE horsies_heartbeats
    ALTER COLUMN id TYPE BIGINT;
""")

WIDEN_WORKER_STATES_ID_TO_BIGINT_SQL = text("""
    ALTER TABLE horsies_worker_states
    ALTER COLUMN id TYPE BIGINT;
""")

# Migration (v5): every horsies_tasks lifecycle UPDATE touches an indexed
# column, making updates non-HOT — each one writes new entries into ALL
# indexes. Indexes that serve no query (or whose queries are covered by the
# v3 partial composites) are pure churn on the hottest table.
DROP_REDUNDANT_TASK_INDEXES_SQL = text("""
    DROP INDEX IF EXISTS ix_horsies_tasks_claimed;
    DROP INDEX IF EXISTS ix_horsies_tasks_claim_expires_at;
    DROP INDEX IF EXISTS ix_horsies_tasks_is_workflow_task;
    DROP INDEX IF EXISTS ix_horsies_tasks_finalizing_at;
    DROP INDEX IF EXISTS ix_horsies_tasks_good_until;
    DROP INDEX IF EXISTS ix_horsies_tasks_next_retry_at;
    DROP INDEX IF EXISTS ix_horsies_worker_states_worker_id;
""")

# good_until is immutable per row and mostly NULL; the expiry scans
# (status='PENDING' AND good_until <= NOW() ORDER BY good_until) only ever
# touch rows where it is set.
CREATE_TASKS_GOOD_UNTIL_PARTIAL_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_good_until_set
    ON horsies_tasks (good_until)
    WHERE good_until IS NOT NULL;
""")

ADD_WORKFLOW_SENT_AT_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ;
""")
BACKFILL_WORKFLOW_SENT_AT_SQL = text("""
    UPDATE horsies_workflows
    SET sent_at = COALESCE(created_at, NOW())
    WHERE sent_at IS NULL;
""")
SET_WORKFLOW_SENT_AT_NOT_NULL_SQL = text("""
    ALTER TABLE horsies_workflows
    ALTER COLUMN sent_at SET NOT NULL;
""")
SET_WORKFLOW_SENT_AT_DEFAULT_SQL = text("""
    ALTER TABLE horsies_workflows
    ALTER COLUMN sent_at SET DEFAULT NOW();
""")

# Migration (v9): expose executor-child memory in worker telemetry. Nullable,
# no backfill — pre-existing snapshots predate the metric and stay NULL.
ADD_WORKER_STATES_CHILDREN_MEMORY_COLUMN_SQL = text("""
    ALTER TABLE horsies_worker_states
    ADD COLUMN IF NOT EXISTS children_memory_mb DOUBLE PRECISION;
""")
