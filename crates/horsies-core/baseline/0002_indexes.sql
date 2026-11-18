-- horsies baseline schema, part 2: indexes. Applied after 0001_tables.sql, in the same transaction, as part of
-- migration 1000.
--
-- Each index has a comment that names the statements that read it.
--
-- Indexes that a constraint in 0001_tables.sql creates, with their readers:
-- - horsies_tasks_pkey (id): enqueue conflict arbiter and resend read, start, finalizing marker, runner beat,
--   success, failed attempt, release, expiry before start, stale RUNNING attempt, cancel, every move, task detail,
--   result wait, schedule fire conflict check.
-- - horsies_task_attempts_pkey (task_id, attempt): attempt inserts of success, failed and stale attempt; the
--   attempt encoder and the attempt delete of the move; live task detail.
-- - horsies_workflows_pkey (id): every read and lock by workflow id; workflow start conflict and name read.
-- - horsies_workflow_tasks_pkey (id): phase 2 (node by row id).
-- - horsies_workflow_tasks_node_workflow_key (id, workflow_id): the foreign keys of the pending and quarantine
--   tables.
-- - uq_horsies_workflow_task_index (workflow_id, task_index): workflow start inserts, phase 1 node_has_dependents
--   read, promotion source rows, completion, child completion, handle reads, run detail, retention node purge.
-- - horsies_workflow_phase2_pending_pkey (task_id): phase 2, phase-2 recovery, quarantine.
-- - horsies_workflow_phase2_quarantine_pkey (task_id): phase 2, phase-2 recovery (copy by task).
-- - horsies_workflow_recovery_failures_pkey (workflow_id): recovery failure bound upsert; cascade from a deleted
--   workflow.
-- - horsies_recovery_scan_cursors_pkey (scan_name): recovery audit page, claim renewal and release, orphan sweep,
--   stats read.
-- - horsies_retention_classes_pkey (class_key): class registration, coverage probe, pruning join, rerun class check.
-- - horsies_task_history_leaf_catalog_pkey (leaf_name): leaf create, inspect, record, drop; reader publication.
-- - horsies_task_history_leaf_catalog_interval_unique (class_key, lower_anchor, upper_anchor): one leaf per class
--   and interval.
-- - horsies_task_lookup_manifest_pkey (leaf_name): drop refusal while the manifest names the leaf.
-- - horsies_task_lookup_manifest_position_unique (probe_position): one leaf per probe position.
-- - horsies_key_reservations_pkey (idempotency_key_digest): key claim, terminalize.
-- - horsies_task_history_clock_exceptions_pkey (task_id): duplicate guard absence probe; ON CONFLICT at the move.
-- - horsies_schedule_state_pkey (schedule_name): schedule fire row lock, scheduler start, existence heal.
-- - horsies_worker_liveness_pkey (worker_id): the generation write of horsies_take_worker_liveness, the generation
--   reads of horsies_unheld_worker_owners and horsies_worker_liveness_generations, the delete of
--   horsies_forget_ended_worker_liveness.
-- - horsies_migrations_pkey (version), horsies_client_family_pkey (only_row): schema install state read.
--
-- Indexes created at run time, not here: each hourly heartbeat leaf gets (task_id, role, sent_at DESC), read by
-- the stale RUNNING scan and attempt, the stale-claims requeue and the monitoring stale list; each daily history leaf
-- of a period class gets <leaf>_task_idx and <leaf>_enqueued_idx, as the forever leaf below.

------------------------------------------------------------------------------------------------------------------------
-- horsies_tasks
------------------------------------------------------------------------------------------------------------------------

-- Claim: the pending arm of horsies_claim_plan and horsies_claim_candidates. Also the class report pending heads
-- (monitoring, skip scan) and the class coverage stream reads.
CREATE INDEX horsies_tasks_claim_pending
    ON horsies_tasks USING btree (queue_name, execution, priority, enqueued_at, id)
    WHERE status = 'PENDING';

-- Claim: the expired-lease arm under a deep expired backlog.
CREATE INDEX horsies_tasks_claim_expired_order
    ON horsies_tasks USING btree (queue_name, execution, priority, enqueued_at, id)
    WHERE status = 'CLAIMED';

-- Claim: the expired-lease arm when few claims expired (finds the expired subset, then sorts it).
CREATE INDEX horsies_tasks_claim_expired_at
    ON horsies_tasks USING btree (queue_name, claim_expires_at)
    WHERE status = 'CLAIMED';

-- horsies_claim_in_flight count; claimer heartbeat and lease renewal; stale-claims requeue; stale-running scan;
-- worker-snapshot statement counts; class coverage in-flight read; monitoring status filter; stale list (RUNNING
-- range).
CREATE INDEX horsies_tasks_status
    ON horsies_tasks USING btree (status);

-- horsies_expire_pending_tasks discovery in deadline order; the expired task list.
CREATE INDEX horsies_tasks_pending_deadline
    ON horsies_tasks USING btree (good_until)
    WHERE status = 'PENDING' AND good_until IS NOT NULL;

-- Monitoring task list, live side, default sort in both directions.
CREATE INDEX idx_horsies_tasks_enqueued_at
    ON horsies_tasks USING btree (enqueued_at);

-- horsies_cancel_orphaned_tasks (the orphan workflow task sweep).
CREATE INDEX idx_horsies_tasks_orphan_recovery_scan
    ON horsies_tasks USING btree (created_at, id)
    WHERE is_workflow_task = true AND status IN ('CLAIMED', 'PENDING');

------------------------------------------------------------------------------------------------------------------------
-- horsies_worker_states
------------------------------------------------------------------------------------------------------------------------

-- The row identity. Latest snapshot per worker (recursive skip scan), latest of one worker, history of one worker,
-- and the worker-state retention statement's delete by key. A unique index, not a constraint, because a
-- constraint index cannot hold the DESC order that the skip scan reads.
CREATE UNIQUE INDEX horsies_worker_states_identity
    ON horsies_worker_states USING btree (worker_id, snapshot_at DESC);

-- Worker-state retention statement (the pick of snapshot_at < cutoff); class report live window.
CREATE INDEX horsies_worker_states_snapshot_at
    ON horsies_worker_states USING btree (snapshot_at);

------------------------------------------------------------------------------------------------------------------------
-- horsies_workflows
------------------------------------------------------------------------------------------------------------------------

-- horsies_workflow_recovery_audit_page.
CREATE INDEX idx_horsies_workflows_running_recovery_scan
    ON horsies_workflows USING btree (created_at, id) INCLUDE (name)
    WHERE status = 'RUNNING';

-- Workflow retention delete.
CREATE INDEX idx_horsies_workflows_retention
    ON horsies_workflows USING btree ((COALESCE(completed_at, updated_at, created_at)))
    WHERE status IN ('CANCELLED', 'COMPLETED', 'EXPIRED', 'FAILED');

-- Statistics on the retention expression over the whole table, for the workflow retention delete. Kept from v61
-- The expression must stay the same as in the index above.
CREATE STATISTICS stx_horsies_workflows_retention
    ON (COALESCE(completed_at, updated_at, created_at))
    FROM horsies_workflows;

-- horsies_workflow_pause, horsies_workflow_resume, horsies_workflow_cancel (descendants); retention (the parent
-- cascade); monitoring workflow names (root filter).
CREATE INDEX idx_horsies_workflows_parent
    ON horsies_workflows USING btree (parent_workflow_id);

-- horsies_workflow_expire_paused (candidate read).
CREATE INDEX idx_horsies_workflows_paused_age
    ON horsies_workflows USING btree (updated_at, id)
    WHERE status = 'PAUSED';

-- Monitoring workflow runs list (root runs, created_at DESC with a limit).
CREATE INDEX idx_horsies_workflows_root_runs
    ON horsies_workflows USING btree (created_at)
    WHERE parent_workflow_id IS NULL;

------------------------------------------------------------------------------------------------------------------------
-- horsies_workflow_tasks
------------------------------------------------------------------------------------------------------------------------

-- Promotion (PENDING dependents), phase 2 (first FAILED by index), resume (PENDING and READY nodes), failure bound
-- (live nodes).
CREATE INDEX idx_horsies_workflow_tasks_workflow_status
    ON horsies_workflow_tasks USING btree (workflow_id, status, task_index);

-- Recovery audit page (live-node probe); recovery cases 0, 1, 1.5, 1.6 (global live-node scans); pause; cancel.
CREATE INDEX idx_horsies_workflow_tasks_nonterminal
    ON horsies_workflow_tasks USING btree (workflow_id)
    WHERE status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING');

-- Unique: a task backs at most one node. Phase 1 (node lock and node_has_dependents), phase 2 replay check, start
-- handoff, claim-time settle, cancel, orphan sweep, retention live join, monitoring task detail workflow link.
CREATE UNIQUE INDEX ix_horsies_workflow_tasks_task_id
    ON horsies_workflow_tasks USING btree (task_id);

-- Retention: the ON DELETE SET NULL action when a child workflow is deleted.
CREATE INDEX idx_horsies_workflow_tasks_sub_workflow
    ON horsies_workflow_tasks USING btree (sub_workflow_id);

------------------------------------------------------------------------------------------------------------------------
-- Phase-2 pending and quarantine
------------------------------------------------------------------------------------------------------------------------

-- horsies_workflow_phase2_recover discovery.
CREATE INDEX horsies_workflow_phase2_pending_age_idx
    ON horsies_workflow_phase2_pending USING btree (created_at, task_id);

-- Retention: the cascade from a deleted node.
CREATE INDEX horsies_workflow_phase2_pending_node_idx
    ON horsies_workflow_phase2_pending USING btree (workflow_node_row_id);

-- Leaf maintenance blocker count (horsies_begin_leaf_detach: a leaf pinned by a pending locator is not detached).
CREATE INDEX horsies_workflow_phase2_pending_locator_idx
    ON horsies_workflow_phase2_pending USING btree (history_class, history_anchor, task_id)
    WHERE recovery_source = 'HISTORY';

-- The foreign key horsies_workflow_phase2_pending_quarantine_fkey: the check that runs when a quarantine copy is
-- deleted (horsies_workflow_node_finish deletes the copy; the node cascade deletes it) finds the pending rows that
-- point at it through this index instead of a scan of the pending table.
CREATE INDEX horsies_workflow_phase2_pending_quarantine_idx
    ON horsies_workflow_phase2_pending USING btree (quarantine_task_id)
    WHERE quarantine_task_id IS NOT NULL;

-- Retention: the cascade of the quarantine node foreign key.
CREATE INDEX horsies_workflow_phase2_quarantine_node_idx
    ON horsies_workflow_phase2_quarantine USING btree (workflow_node_row_id);

------------------------------------------------------------------------------------------------------------------------
-- History
------------------------------------------------------------------------------------------------------------------------

-- Pruning candidates; the coverage probe's forever upper anchor.
-- One leaf per class and upper anchor among leaves with an OPEN lower bound. The unique constraint on
-- (class_key, lower_anchor, upper_anchor) treats NULL anchors as distinct, so it does not cover them.
CREATE UNIQUE INDEX horsies_task_history_leaf_catalog_open_interval_unique
    ON horsies_task_history_leaf_catalog USING btree (class_key, upper_anchor)
    WHERE lower_bound_kind = 'OPEN';

CREATE INDEX horsies_task_history_leaf_catalog_active
    ON horsies_task_history_leaf_catalog USING btree (class_key, upper_anchor)
    WHERE dropped_at IS NULL;

-- horsies_key_reservation_cleanup.
CREATE INDEX horsies_key_reservations_expiry_idx
    ON horsies_key_reservations USING btree (expires_at)
    WHERE disposition = 'TERMINAL';

-- horsies_drop_detached_leaf (delete of the clock exceptions of a class and day).
CREATE INDEX horsies_task_history_clock_exceptions_leaf_idx
    ON horsies_task_history_clock_exceptions USING btree (retention_class_key, terminal_at);

-- The forever leaf's two indexes (index shape version 1). <leaf>_task_idx: the published readers (duplicate guard,
-- provenance, detail, result), phase-2 node finish and quarantine after partition pruning. <leaf>_enqueued_idx:
-- the monitoring task list, history side, default sort.
CREATE INDEX horsies_task_history_forever_open_task_idx
    ON horsies_task_history_forever_open USING btree (task_id);

CREATE INDEX horsies_task_history_forever_open_enqueued_idx
    ON horsies_task_history_forever_open USING btree (enqueued_at);

------------------------------------------------------------------------------------------------------------------------
-- horsies_schedule_state
------------------------------------------------------------------------------------------------------------------------

-- Due schedule read (next_run_at <= now); schedule list (order by next_run_at).
CREATE INDEX ix_horsies_schedule_state_next_run_at
    ON horsies_schedule_state USING btree (next_run_at);
