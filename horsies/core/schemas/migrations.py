"""PostgreSQL schema migrations — idempotent ALTER TABLE statements."""

from __future__ import annotations

from sqlalchemy import text


# ---- Schema infrastructure ----

SCHEMA_ADVISORY_LOCK_SQL = text("""
    SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))
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

ADD_WORKFLOW_DEF_MODULE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS workflow_def_module VARCHAR(512);
""")

ADD_WORKFLOW_DEF_QUALNAME_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS workflow_def_qualname VARCHAR(512);
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

ADD_SUB_WORKFLOW_MODULE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_workflow_module VARCHAR(512);
""")

ADD_SUB_WORKFLOW_QUALNAME_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_workflow_qualname VARCHAR(512);
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
