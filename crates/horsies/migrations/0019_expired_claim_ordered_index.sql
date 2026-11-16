-- Ordered partial index for the expired claim arm (parity with horsies PR #101,
-- d77ffa46 / schema v7).
--
-- Complements idx_horsies_tasks_claim_expired (0018): that filter index suits
-- the steady-state few-expired case, while this ordered index lets the planner
-- serve the expired arm's LIMIT directly from the index for a deep expired
-- backlog (avoiding a full sort above the LockRows node).

CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claim_expired_ordered
    ON horsies_tasks (queue_name, priority, enqueued_at, id)
    WHERE status = 'CLAIMED';
