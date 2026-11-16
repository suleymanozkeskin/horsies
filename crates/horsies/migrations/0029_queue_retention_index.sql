-- Per-queue retention eligibility index (parity with horsies PR #207 /
-- schema v15).

-- Serves DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL (worker/recovery.rs), the
-- per-queue override deletes (queue_terminal_record_retention_hours), which
-- filter on queue_name plus the same COALESCE expression. The 0025 expression
-- index keeps serving the global remainder delete (its range is bounded by
-- the global cutoff); an override delete's range extends to a much more
-- recent cutoff, where every other queue's retained terminal rows would be
-- heap-filter misses proportional to (longest - shortest window) x the other
-- queues' volume. Partial on terminal statuses: maintained once per task
-- lifetime, on the finalize transition — claim/lease-renewal updates never
-- touch it (the 0025 mechanism). Keep the COALESCE column list and order
-- identical to the delete's; the planner matches the parsed expression.
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_queue_retention
    ON horsies_tasks (queue_name, COALESCE(completed_at, failed_at, updated_at, created_at))
    WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED');
