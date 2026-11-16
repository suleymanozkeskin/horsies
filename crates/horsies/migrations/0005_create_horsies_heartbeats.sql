-- Heartbeat tracking for stale task detection.
-- Workers send periodic heartbeats while tasks are CLAIMED or RUNNING.
CREATE TABLE IF NOT EXISTS horsies_heartbeats (
    id          SERIAL          PRIMARY KEY,
    task_id     VARCHAR(36)     NOT NULL,
    sender_id   VARCHAR(255)    NOT NULL,
    role        VARCHAR(20)     NOT NULL,
    sent_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    hostname    VARCHAR(255),
    pid         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_horsies_heartbeats_task_role ON horsies_heartbeats (task_id, role);
CREATE INDEX IF NOT EXISTS idx_horsies_heartbeats_sent_at ON horsies_heartbeats (sent_at);
