-- Task queue table: stores all enqueued, running, and completed tasks.
CREATE TABLE IF NOT EXISTS horsies_tasks (
    id                      VARCHAR(36)     NOT NULL PRIMARY KEY,
    task_name               VARCHAR(255)    NOT NULL,
    queue_name              VARCHAR(100)    NOT NULL,
    priority                INTEGER         NOT NULL DEFAULT 100,
    args                    TEXT,
    kwargs                  TEXT,
    status                  VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    sent_at                 TIMESTAMPTZ,
    claimed_at              TIMESTAMPTZ,
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    failed_at               TIMESTAMPTZ,
    result                  TEXT,
    failed_reason           TEXT,
    claimed                 BOOLEAN         NOT NULL DEFAULT FALSE,
    claimed_by_worker_id    VARCHAR(255),
    claim_expires_at        TIMESTAMPTZ,
    good_until              TIMESTAMPTZ,
    retry_count             INTEGER         NOT NULL DEFAULT 0,
    max_retries             INTEGER         NOT NULL DEFAULT 0,
    next_retry_at           TIMESTAMPTZ,
    task_options            TEXT,
    worker_pid              INTEGER,
    worker_hostname         VARCHAR(255),
    worker_process_name     VARCHAR(255),
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_horsies_tasks_status ON horsies_tasks (status);
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_queue_name ON horsies_tasks (queue_name);
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claimed ON horsies_tasks (claimed);
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_claim_expires_at ON horsies_tasks (claim_expires_at);
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_good_until ON horsies_tasks (good_until);
CREATE INDEX IF NOT EXISTS idx_horsies_tasks_next_retry_at ON horsies_tasks (next_retry_at);
