-- Widen high-insert-rate PKs to BIGINT (parity with horsies PR #101, 27122ab8 /
-- schema v4).
--
-- horsies_heartbeats and horsies_worker_states accumulate rows at worker
-- heartbeat / snapshot rates; an int4 SERIAL id sequence exhausts within months
-- at scale. Widen the columns to BIGINT (the underlying sequences are already
-- int8).

ALTER TABLE horsies_heartbeats ALTER COLUMN id TYPE BIGINT;
ALTER TABLE horsies_worker_states ALTER COLUMN id TYPE BIGINT;
