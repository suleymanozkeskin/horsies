-- Workflow retention planning (parity with horsies PR #185 + #189 /
-- schema v13 + v14).
--
-- DELETE_EXPIRED_WORKFLOWS_SQL and DELETE_EXPIRED_WORKFLOW_TASKS_SQL
-- (worker/recovery.rs) both filter horsies_workflows on terminal status +
-- COALESCE(completed_at, updated_at, created_at) < cutoff. Without an index
-- every hourly retention pass walks the whole workflows table, serial under
-- FOR UPDATE SKIP LOCKED (row locking disables parallel workers), even with
-- zero eligible rows. Partial on terminal statuses: a row enters the index
-- once, at its terminal transition — updates during a workflow's running
-- life never maintain it. The planner matches the parsed expression, not
-- the SQL text: keep the COALESCE column list and order identical to the
-- deletes' (column qualification is irrelevant).
CREATE INDEX IF NOT EXISTS idx_horsies_workflows_retention
    ON horsies_workflows (COALESCE(completed_at, updated_at, created_at))
    WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED');

-- The index alone is not enough: the planner never uses statistics gathered
-- on a PARTIAL index for whole-table selectivity (they describe only rows
-- satisfying the index predicate), so the retention COALESCE cutoffs are
-- costed at the default 1/3 selectivity and index-vs-walk becomes a
-- function of table size alone. At 1M retained tasks the heap walk is
-- expensive enough that the 0025 index wins regardless; at 36k retained
-- workflows the planner keeps a full-table walk (estimate 12k vs 13
-- actual, 4-5s per statement per hourly pass), and the same misestimate
-- degrades the NOT EXISTS guard into a seq scan of the tasks table.
-- CREATE STATISTICS ON (expression) — PostgreSQL 14+, the supported
-- floor — builds whole-table expression statistics the planner does use.
-- Expressions must stay structurally identical to the index and DELETE
-- expressions.
CREATE STATISTICS IF NOT EXISTS stx_horsies_tasks_retention
    ON (COALESCE(completed_at, failed_at, updated_at, created_at))
    FROM horsies_tasks;

CREATE STATISTICS IF NOT EXISTS stx_horsies_workflows_retention
    ON (COALESCE(completed_at, updated_at, created_at))
    FROM horsies_workflows;

-- Extended statistics are empty until the table is analyzed after their
-- creation. ANALYZE is sampled (bounded by default_statistics_target
-- regardless of table size), takes SHARE UPDATE EXCLUSIVE (never blocks
-- reads or writes; a conflicting autovacuum worker yields), and is legal
-- inside the migration transaction — the statistics commit with it.
ANALYZE horsies_tasks, horsies_workflows;
