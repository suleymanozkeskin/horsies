-- Migration: Full schema parity with Python's horsies_worker_states table.
-- Changes data model from snapshot (overwrite) to timeseries (historical).

DROP TABLE IF EXISTS horsies_worker_states;

CREATE TABLE horsies_worker_states (
    id SERIAL PRIMARY KEY,
    worker_id VARCHAR(255) NOT NULL,
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    hostname VARCHAR(255) NOT NULL,
    pid INTEGER NOT NULL,
    processes INTEGER NOT NULL,
    max_claim_batch INTEGER NOT NULL,
    max_claim_per_worker INTEGER NOT NULL,
    cluster_wide_cap INTEGER,
    queues TEXT[] NOT NULL,
    queue_priorities JSONB,
    queue_max_concurrency JSONB,
    recovery_config JSONB,
    tasks_running INTEGER NOT NULL DEFAULT 0,
    tasks_claimed INTEGER NOT NULL DEFAULT 0,
    memory_usage_mb DOUBLE PRECISION,
    memory_percent DOUBLE PRECISION,
    cpu_percent DOUBLE PRECISION,
    worker_started_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_worker_states_worker_id ON horsies_worker_states(worker_id);
CREATE INDEX idx_worker_states_snapshot_at ON horsies_worker_states(snapshot_at);
CREATE INDEX idx_worker_states_tasks_running ON horsies_worker_states(tasks_running);
CREATE INDEX idx_worker_states_tasks_claimed ON horsies_worker_states(tasks_claimed);
