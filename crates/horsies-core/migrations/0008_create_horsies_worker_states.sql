CREATE TABLE IF NOT EXISTS horsies_worker_states (
    worker_id VARCHAR(255) PRIMARY KEY,
    hostname VARCHAR(255) NOT NULL,
    pid INTEGER NOT NULL,
    queues JSONB NOT NULL DEFAULT '[]',
    concurrency INTEGER NOT NULL DEFAULT 1,
    active_tasks INTEGER NOT NULL DEFAULT 0,
    snapshot JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worker_states_updated_at ON horsies_worker_states(updated_at);
