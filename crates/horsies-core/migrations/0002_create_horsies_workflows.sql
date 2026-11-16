-- Workflow metadata table: tracks workflow lifecycle and hierarchy.
CREATE TABLE IF NOT EXISTS horsies_workflows (
    id                      VARCHAR(36)     NOT NULL PRIMARY KEY,
    name                    VARCHAR(255)    NOT NULL,
    status                  VARCHAR(50)     NOT NULL DEFAULT 'PENDING',
    on_error                VARCHAR(50)     NOT NULL DEFAULT 'fail',
    output_task_index       INTEGER,
    success_policy          JSONB,
    result                  TEXT,
    error                   TEXT,
    workflow_def_module      VARCHAR(512),
    workflow_def_qualname    VARCHAR(512),
    parent_workflow_id      VARCHAR(36)     REFERENCES horsies_workflows(id) ON DELETE CASCADE,
    parent_task_index       INTEGER,
    depth                   INTEGER         NOT NULL DEFAULT 0,
    root_workflow_id        VARCHAR(36),
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_horsies_workflows_status ON horsies_workflows (status);
CREATE INDEX IF NOT EXISTS idx_horsies_workflows_parent ON horsies_workflows (parent_workflow_id);
CREATE INDEX IF NOT EXISTS idx_horsies_workflows_root ON horsies_workflows (root_workflow_id);
CREATE INDEX IF NOT EXISTS idx_horsies_workflows_updated ON horsies_workflows (updated_at);
