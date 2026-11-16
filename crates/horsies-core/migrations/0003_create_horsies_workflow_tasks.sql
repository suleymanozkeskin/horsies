-- Workflow DAG nodes: each row is a task within a workflow.
CREATE TABLE IF NOT EXISTS horsies_workflow_tasks (
    id                          VARCHAR(36)     NOT NULL PRIMARY KEY,
    workflow_id                 VARCHAR(36)     NOT NULL REFERENCES horsies_workflows(id) ON DELETE CASCADE,
    task_index                  INTEGER         NOT NULL,
    node_id                     VARCHAR(128),
    task_name                   VARCHAR(255)    NOT NULL,
    task_args                   TEXT,
    task_kwargs                 TEXT,
    queue_name                  VARCHAR(100)    NOT NULL DEFAULT 'default',
    priority                    INTEGER         NOT NULL DEFAULT 100,
    dependencies                INTEGER[]       NOT NULL DEFAULT '{}',
    args_from                   JSONB,
    workflow_ctx_from           VARCHAR(128)[],
    allow_failed_deps           BOOLEAN         NOT NULL DEFAULT FALSE,
    join_type                   VARCHAR(10)     NOT NULL DEFAULT 'all',
    min_success                 INTEGER,
    task_options                TEXT,
    status                      VARCHAR(50)     NOT NULL DEFAULT 'PENDING',
    task_id                     VARCHAR(36),
    is_subworkflow              BOOLEAN         NOT NULL DEFAULT FALSE,
    sub_workflow_id             VARCHAR(36)     REFERENCES horsies_workflows(id) ON DELETE SET NULL,
    sub_workflow_name           VARCHAR(255),
    sub_workflow_module         VARCHAR(512),
    sub_workflow_qualname       VARCHAR(512),
    sub_workflow_retry_mode     VARCHAR(50),
    sub_workflow_summary        TEXT,
    result                      TEXT,
    error                       TEXT,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    started_at                  TIMESTAMPTZ,
    completed_at                TIMESTAMPTZ,

    CONSTRAINT uq_horsies_workflow_task_index UNIQUE (workflow_id, task_index)
);

CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_workflow ON horsies_workflow_tasks (workflow_id);
CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_node ON horsies_workflow_tasks (node_id);
CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_status ON horsies_workflow_tasks (status);
CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_task ON horsies_workflow_tasks (task_id);
CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_subworkflow ON horsies_workflow_tasks (sub_workflow_id);
CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_deps ON horsies_workflow_tasks USING GIN (dependencies);
