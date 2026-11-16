CREATE TABLE IF NOT EXISTS horsies_recovery_scan_cursors (
    scan_name varchar(64) PRIMARY KEY,
    last_created_at timestamptz,
    last_id uuid,
    cycle_upper_created_at timestamptz,
    cycle_upper_id uuid,
    claim_token uuid,
    claim_expires_at timestamptz,
    completed_cycles bigint NOT NULL DEFAULT 0,
    last_scan_rows integer NOT NULL DEFAULT 0,
    last_candidate_rows integer NOT NULL DEFAULT 0,
    last_scan_at timestamptz,
    CONSTRAINT horsies_recovery_cursor_last_pair CHECK (
        (last_created_at IS NULL) = (last_id IS NULL)
    ),
    CONSTRAINT horsies_recovery_cursor_upper_pair CHECK (
        (cycle_upper_created_at IS NULL) = (cycle_upper_id IS NULL)
    ),
    CONSTRAINT horsies_recovery_cursor_claim_pair CHECK (
        (claim_token IS NULL) = (claim_expires_at IS NULL)
    ),
    CONSTRAINT horsies_recovery_cursor_last_has_upper CHECK (
        last_id IS NULL OR cycle_upper_id IS NOT NULL
    )
);

INSERT INTO horsies_recovery_scan_cursors (scan_name)
VALUES ('running_workflows'), ('orphan_workflow_tasks')
ON CONFLICT (scan_name) DO NOTHING;
