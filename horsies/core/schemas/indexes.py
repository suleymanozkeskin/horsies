"""PostgreSQL indexes that require raw SQL (cannot be expressed via ORM)."""

from __future__ import annotations

from sqlalchemy import text


# GIN index on workflow_tasks.dependencies for efficient dependency array lookups.
# Cannot use ORM __table_args__ because SQLAlchemy does not support GIN indexes directly.
CREATE_WORKFLOW_TASKS_DEPS_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_deps
    ON horsies_workflow_tasks USING GIN(dependencies);
""")

# Composite index for latest-heartbeat lookups.
# Also defined in TaskHeartbeatModel.__table_args__ for fresh installs via create_all;
# the raw SQL here is the upgrade path for existing databases missing the index.
CREATE_TASK_ATTEMPTS_ERROR_CODE_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_task_attempts_error_code
    ON horsies_task_attempts (error_code)
    WHERE error_code IS NOT NULL;
""")

CREATE_TASK_ATTEMPTS_FINISHED_AT_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_task_attempts_finished_at_desc
    ON horsies_task_attempts (finished_at DESC);
""")

CREATE_TASKS_ERROR_CODE_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_error_code
    ON horsies_tasks (error_code)
    WHERE error_code IS NOT NULL;
""")

CREATE_HEARTBEATS_TASK_ROLE_SENT_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_heartbeats_task_role_sent
    ON horsies_heartbeats (task_id, role, sent_at DESC);
""")

# Claim-path indexes (schema v3 + v7). CLAIM_SQL filters one queue for
# PENDING rows or expired CLAIMED leases, ordered by (priority,
# enqueued_at, id); without these, the planner falls back to the
# single-column queue_name / status indexes — poisoned by retained
# terminal rows — and sorts the whole eligible backlog on every claim
# pass.
#
# The pending arm has one ordered composite. The expired arm needs TWO
# complementary indexes because its filter (claim_expires_at range) and
# its ORDER BY use different columns, so no single index serves both:
#   - claim_expired (v3): expiry-filter index. Optimal in steady state
#     (few/zero expired among many active leases) — finds the expired
#     subset directly, then top-N sorts the small set.
#   - claim_expired_ordered (v7): ordered walk over CLAIMED rows.
#     Optimal for deep expired backlogs (mass crash recovery), where
#     the filter index would feed tens of thousands of rows into a
#     full sort (SKIP LOCKED's LockRows sits above the Sort node, so
#     the sort cannot stop at the LIMIT).
# The planner picks per data distribution; measured (50k rows): deep
# backlog 30.7ms -> 0.11ms with the ordered index, steady state 0.11ms
# via the filter index (planner correctly ignores the ordered one).
CREATE_TASKS_CLAIM_PENDING_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claim_pending
    ON horsies_tasks (queue_name, priority, enqueued_at, id)
    WHERE status = 'PENDING';
""")

CREATE_TASKS_CLAIM_EXPIRED_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claim_expired
    ON horsies_tasks (queue_name, claim_expires_at)
    WHERE status = 'CLAIMED';
""")

CREATE_TASKS_CLAIM_EXPIRED_ORDERED_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claim_expired_ordered
    ON horsies_tasks (queue_name, priority, enqueued_at, id)
    WHERE status = 'CLAIMED';
""")

# Per-worker in-flight counts (claim budget) and lease renewal filter on
# (claimed_by_worker_id, status); without an index they ride the low-
# selectivity status index. Partial: rows never claimed are irrelevant.
CREATE_TASKS_WORKER_STATUS_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_worker_status
    ON horsies_tasks (claimed_by_worker_id, status)
    WHERE claimed_by_worker_id IS NOT NULL;
""")

# Latest-snapshot-per-worker reads use DISTINCT ON (worker_id) ORDER BY
# worker_id, snapshot_at DESC over the snapshot timeseries.
CREATE_WORKER_STATES_WORKER_SNAPSHOT_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_worker_states_worker_snapshot
    ON horsies_worker_states (worker_id, snapshot_at DESC);
""")
