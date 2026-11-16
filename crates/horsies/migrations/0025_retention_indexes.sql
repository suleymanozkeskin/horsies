-- Retention eligibility indexes (parity with horsies PR #172 / schema v11).

-- Serves DELETE_EXPIRED_TASKS_SQL (worker/recovery.rs), which filters terminal
-- rows by COALESCE(completed_at, failed_at, updated_at, created_at) < cutoff.
-- Without it every hourly retention pass seq-scans the whole heap even with
-- zero eligible rows. Partial on terminal statuses: a row enters the index
-- once, at its finalize transition — claim/lease-renewal updates produce
-- non-terminal row versions and never maintain it. (Indexing updated_at
-- directly was rejected: it changes on every update and would drop the
-- HOT-update ratio to zero.) The expression and status list must stay
-- textually aligned with the DELETE: the planner can only use the partial
-- index while it can prove the DELETE's status predicate implies the index
-- predicate, which requires literals on both sides.
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_retention
    ON horsies_tasks (COALESCE(completed_at, failed_at, updated_at, created_at))
    WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED');

-- Serves DELETE_EXPIRED_WORKER_STATES_SQL (snapshot_at < cutoff). The 0018
-- composite (worker_id, snapshot_at DESC) leads with worker_id, so a bare
-- snapshot_at range fell back to a seq scan of the snapshot timeseries.
-- Insert-only table with monotonically increasing snapshot_at: maintenance
-- is rightmost-leaf appends.
CREATE INDEX IF NOT EXISTS idx_horsies_worker_states_snapshot_at
    ON horsies_worker_states (snapshot_at);
