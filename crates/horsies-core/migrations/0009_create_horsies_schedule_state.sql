CREATE TABLE IF NOT EXISTS horsies_schedule_state (
    schedule_name VARCHAR(255) PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    last_run_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    last_task_id VARCHAR(36),
    run_count INTEGER NOT NULL DEFAULT 0,
    config_hash VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schedule_state_next_run ON horsies_schedule_state(next_run_at);
