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

SCHEMA_VERSION = 8

SCHEMA_ADVISORY_LOCK_SQL = text("""
    SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))
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
