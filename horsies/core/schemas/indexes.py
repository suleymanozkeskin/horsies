"""PostgreSQL indexes that require raw SQL (cannot be expressed via ORM)."""

from __future__ import annotations

from sqlalchemy import text

from horsies.core.models.workflow.enums import WORKFLOW_TERMINAL_STATES
from horsies.core.types.status import TASK_TERMINAL_STATES

# Terminal task statuses rendered as SQL literals, sorted for deterministic
# statement text. A partial index serves a statement only while the planner
# can prove the statement's status predicate implies the index predicate,
# which requires literals (a bound array under a generic plan cannot be
# proven) rendered from the same set on both sides.
TASK_TERMINAL_STATUS_SQL_LITERALS: str = ', '.join(
    sorted(f"'{s.value}'" for s in TASK_TERMINAL_STATES),
)

# Terminal workflow statuses, same contract as above: rendered here so the
# workflow retention index and the workflow retention DELETEs in
# worker/sql.py share one literal set.
WORKFLOW_TERMINAL_STATUS_SQL_LITERALS: str = ', '.join(
    sorted(f"'{s.value}'" for s in WORKFLOW_TERMINAL_STATES),
)

# Editing either constant is a schema migration. Both feed partial-index
# predicates (three, below) as well as the DELETEs' IN-lists, and those
# indexes are created with CREATE INDEX IF NOT EXISTS — which does not
# rebuild an index that already exists under the old predicate. A changed
# set therefore requires an explicit versioned DROP INDEX + CREATE.
# Nothing catches this downstream: CI migrates a fresh database, gets the
# new predicate, and every EXPLAIN plan test passes, while deployed
# databases keep the stale index whose predicate no longer implies the new
# IN-list, and the planner falls back to the heap walk the index removed.


# v8: serves the first-failed lookups (GET_FIRST_FAILED_TASK_RESULT_SQL and
# the required-task variant), which filter workflow_id + status and order by
# task_index. Without it, every FAILED completion paid an O(N) ordered scan
# of the workflow's rows inside the completion lock.
CREATE_WORKFLOW_TASKS_WF_STATUS_IDX_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_wf_status_idx
    ON horsies_workflow_tasks (workflow_id, status, task_index);
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

# Claim-path index (schema v3). The expired arm of the claim reads CLAIMED
# rows whose lease ended. In steady state (few or no expired leases among
# many live ones) this expiry-filter index finds the expired subset directly
# and the claim sorts that small set. Since v55 each claim stream is one
# (queue, execution class) range: the pending arm reads
# idx_horsies_tasks_claim_pending_class and a deep expired backlog
# idx_horsies_tasks_claim_expired_class (schemas/recovery.py, CLAIM_INDEXES).
# v55 retires idx_horsies_tasks_claim_pending (v3) and
# idx_horsies_tasks_claim_expired_ordered (v7): their key has no class, so a
# stream of a rare or absent class filtered the whole queue.
CREATE_TASKS_CLAIM_EXPIRED_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claim_expired
    ON horsies_tasks (queue_name, claim_expires_at)
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

# v11: serves DELETE_EXPIRED_WORKER_STATES_SQL (snapshot_at < cutoff). The
# composite (worker_id, snapshot_at DESC) index leads with worker_id, so a
# bare snapshot_at range fell back to a seq scan of the timeseries heap
# (157 MB at a 6-worker week of snapshots). Insert-only table with
# monotonically increasing snapshot_at: maintenance is rightmost-leaf
# appends.
CREATE_WORKER_STATES_SNAPSHOT_AT_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_worker_states_snapshot_at
    ON horsies_worker_states (snapshot_at);
""")

# v12: serves DELETE_EXPIRED_HEARTBEATS_SQL (sent_at < cutoff). The composite
# (task_id, role, sent_at DESC) index leads with task_id and cannot serve a
# leading-column sent_at range, so every hourly retention pass scanned the
# heartbeats heap — the highest-insert-rate table in the schema (one row per
# running task per interval, plus claimer beats). v11 added the analogous
# indexes for tasks and worker_states and omitted heartbeats. Insert-order
# appends keep maintenance to rightmost-leaf inserts, same as the
# worker_states index above.
CREATE_HEARTBEATS_SENT_AT_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_heartbeats_sent_at
    ON horsies_heartbeats (sent_at);
""")

# v16: monitoring list/facet indexes. The task list's default sort
# (ORDER BY enqueued_at DESC LIMIT n) and the task-name facet
# (GROUP BY task_name) otherwise seq-scan the whole heap per dashboard
# render; at 1M rows that is ~167k buffer reads per query against ~53
# (sort via backward index scan) and ~1.2k (facet via index-only scan).
# Neither column appears in any worker-path predicate or leading sort key:
# the claim ORDER BY leads with priority, so the planner cannot route any
# hot-path query through these (asserted by the claim plan test). Both are
# cheap to maintain — enqueued_at is near-monotonic (rightmost-leaf
# appends, mutated only on retry), task_name is immutable and
# deduplicates heavily.
CREATE_TASKS_ENQUEUED_AT_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_enqueued_at
    ON horsies_tasks (enqueued_at);
""")

CREATE_TASKS_TASK_NAME_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_task_name
    ON horsies_tasks (task_name);
""")

# v13: retention eligibility for DELETE_EXPIRED_WORKFLOWS_SQL (which since
# the workflow-batched rewrite also purges node rows), filtering
# horsies_workflows on terminal status +
# COALESCE(completed_at, updated_at, created_at) < cutoff.
# Without it every hourly retention pass walks the whole workflows table:
# the planner has no statistics on the COALESCE expression, overestimates
# eligibility, and picks a stop-early pkey walk whose LIMIT never fills
# (36k retained workflows ≈ 4-5s per statement serially under FOR UPDATE,
# twice per pass, even with zero eligible rows). Partial on
# terminal statuses: a row enters the index once, on its terminal
# transition; updates during a workflow's running life never maintain it.
# The planner matches the parsed expression, not the SQL text: keep the
# COALESCE column list and order identical to the deletes' (column
# qualification is irrelevant).
CREATE_WORKFLOWS_RETENTION_INDEX_SQL = text(f"""
    CREATE INDEX IF NOT EXISTS idx_horsies_workflows_retention
    ON horsies_workflows (COALESCE(completed_at, updated_at, created_at))
    WHERE status IN ({WORKFLOW_TERMINAL_STATUS_SQL_LITERALS});
""")
