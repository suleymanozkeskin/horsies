-- no-transaction
CREATE INDEX CONCURRENTLY idx_horsies_history_catalog_active
    ON horsies_task_history_leaf_catalog (class_key, upper_anchor)
    WHERE dropped_at IS NULL;
