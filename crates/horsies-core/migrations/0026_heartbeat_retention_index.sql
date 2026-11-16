-- Heartbeat retention eligibility index (companion to 0025).

-- Serves DELETE_EXPIRED_HEARTBEATS_SQL (worker/recovery.rs), whose inner SELECT
-- filters `sent_at < cutoff`. Migration 0013 dropped the bare `sent_at` index in
-- favor of the composite (task_id, role, sent_at DESC), which leads with task_id
-- and cannot serve a leading-column sent_at range — so every hourly retention
-- pass seq-scanned the heartbeats heap, the highest-insert-rate table in the
-- schema (one row per running task per interval, plus claimer beats). 0025 added
-- the analogous index for tasks and worker_states but omitted heartbeats (P4).
-- Insert-order appends keep maintenance to rightmost-leaf inserts, the same
-- justification as 0025's worker_states index.
CREATE INDEX IF NOT EXISTS idx_horsies_heartbeats_sent_at
    ON horsies_heartbeats (sent_at);
