-- Drop write-amplifying single-column indexes (parity with horsies PR #101,
-- 61a7f40b / schema v5).
--
-- These single-column indexes are superseded by the composite/partial claim
-- indexes added in 0018 (which cover the hot claim, per-worker accounting, and
-- expired-lease paths). Each row write to horsies_tasks updated all of them;
-- dropping them removes that amplification. good_until is replaced by a partial
-- index that only covers rows with a deadline (the only rows expiry scans).
--
-- Ordering: 0018 composites are applied before this drop, so the planner never
-- loses claim-path coverage.

DROP INDEX IF EXISTS idx_horsies_tasks_claimed;
DROP INDEX IF EXISTS idx_horsies_tasks_claim_expires_at;
DROP INDEX IF EXISTS idx_horsies_tasks_is_workflow_task;
DROP INDEX IF EXISTS idx_horsies_tasks_finalizing_at;
DROP INDEX IF EXISTS idx_horsies_tasks_good_until;
DROP INDEX IF EXISTS idx_horsies_tasks_next_retry_at;
DROP INDEX IF EXISTS idx_worker_states_worker_id;

CREATE INDEX IF NOT EXISTS idx_horsies_tasks_good_until_set
    ON horsies_tasks (good_until)
    WHERE good_until IS NOT NULL;
