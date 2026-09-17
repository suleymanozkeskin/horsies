-- no-transaction
CREATE INDEX CONCURRENTLY idx_horsies_workflow_tasks_nonterminal
    ON horsies_workflow_tasks (workflow_id)
    WHERE status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING');
