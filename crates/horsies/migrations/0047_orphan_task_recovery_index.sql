-- no-transaction
CREATE INDEX CONCURRENTLY idx_horsies_tasks_orphan_recovery_scan
    ON horsies_tasks (created_at, id)
    WHERE is_workflow_task = TRUE
      AND status IN ('CLAIMED', 'PENDING');
