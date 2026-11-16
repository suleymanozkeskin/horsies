-- Claim-path indexes (parity with horsies PR #101, 31e40a2f / schema v3).
--
-- Composite/partial indexes that back the two split-arm eligibility scans in
-- CLAIM_SQL (PENDING and expired-CLAIMED), the per-worker claim-count gate, and
-- the latest-snapshot worker-state read. Each is ordered to match the claim
-- ORDER BY (priority, enqueued_at, id) so the planner serves the LIMIT from the
-- index without a sort.

-- PENDING arm: ordered candidates per queue.
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claim_pending
    ON horsies_tasks (queue_name, priority, enqueued_at, id)
    WHERE status = 'PENDING';

-- Expired-CLAIMED arm: locate reclaimable leases per queue by expiry.
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claim_expired
    ON horsies_tasks (queue_name, claim_expires_at)
    WHERE status = 'CLAIMED';

-- Per-worker claim accounting (COUNT_CLAIMED_FOR_WORKER, ownership lookups).
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_worker_status
    ON horsies_tasks (claimed_by_worker_id, status)
    WHERE claimed_by_worker_id IS NOT NULL;

-- Latest worker-state snapshot per worker.
CREATE INDEX IF NOT EXISTS idx_horsies_worker_states_worker_snapshot
    ON horsies_worker_states (worker_id, snapshot_at DESC);
