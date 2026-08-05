"""SQL constants for the workflow engine."""

from __future__ import annotations

from sqlalchemy import text

# -- SQL constants for start_workflow_async --

CHECK_WORKFLOW_EXISTS_SQL = text(
    """SELECT id FROM horsies_workflows WHERE id = :wf_id"""
)
INSERT_WORKFLOW_SQL = text("""
    INSERT INTO horsies_workflows (id, name, status, on_error, output_task_index,
                             success_policy, definition_key,
                             depth, root_workflow_id,
                             sent_at, created_at, started_at, updated_at)
    VALUES (:id, :name, 'RUNNING', :on_error, :output_idx,
            :success_policy, :definition_key,
            0, :id,
            :sent_at, NOW(), NOW(), NOW())
    ON CONFLICT (id) DO NOTHING
    RETURNING id
""")
# Unified bulk node insert for workflow start (executemany over all nodes
# in one pipelined batch). Differences from the per-row inserts above:
# - covers BOTH node kinds (sub_* fields NULL + is_subworkflow FALSE for
#   TaskNodes);
# - fast-path roots are inserted directly as ENQUEUED with task_id set and
#   started_at = NOW() — mirroring ENQUEUE_WORKFLOW_TASK_SQL's column
#   effects. The READY->ENQUEUED CAS that statement performs protects the
#   cross-transaction promotion path; at start time these rows are created
#   in the same uncommitted transaction, so the CAS is vacuous.
# - :is_enqueued is a separate boolean param (not derived from :status) to
#   avoid psycopg AmbiguousParameter on a twice-used parameter.
INSERT_WORKFLOW_TASKS_BULK_SQL = text("""
    INSERT INTO horsies_workflow_tasks
    (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
     queue_name, priority, dependencies, args_from, workflow_ctx_from,
     allow_failed_deps, join_type, min_success, task_options, status,
     is_subworkflow, sub_workflow_name, sub_definition_key, task_id,
     started_at, created_at)
    VALUES (:id, :wf_id, :idx, :node_id, :name, :args, :kwargs, :queue, :priority,
            :deps, :args_from, :ctx_from, :allow_failed, :join_type, :min_success,
            :task_options, :status, :is_subworkflow, :sub_wf_name, :sub_def_key,
            :task_id, CASE WHEN :is_enqueued THEN NOW() END, NOW())
""")
# -- SQL constants for pause_workflow --

PAUSE_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'PAUSED', updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
    RETURNING id
""")
NOTIFY_WORKFLOW_DONE_SQL = text("""SELECT pg_notify('workflow_done', :wf_id)""")
# -- SQL constants for _cascade_pause_to_children --

GET_RUNNING_CHILD_WORKFLOWS_SQL = text("""
    SELECT id FROM horsies_workflows
    WHERE parent_workflow_id = :wf_id AND status = 'RUNNING'
""")
PAUSE_CHILD_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'PAUSED', updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
""")
RESET_ABANDONED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY',
        task_id = NULL,
        started_at = NULL
    WHERE task_id = ANY(:task_ids)
      AND status IN ('ENQUEUED', 'RUNNING')
""")
# -- SQL constants for resume_workflow --

RESUME_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'RUNNING', updated_at = NOW()
    WHERE id = :wf_id AND status = 'PAUSED'
    RETURNING id, depth, root_workflow_id
""")
GET_PENDING_WORKFLOW_TASKS_SQL = text("""
    SELECT task_index FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'PENDING'
""")
GET_READY_WORKFLOW_TASKS_SQL = text("""
    SELECT task_index, dependencies, is_subworkflow FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'READY'
""")
# -- SQL constants for _cascade_resume_to_children --

GET_PAUSED_CHILD_WORKFLOWS_SQL = text("""
    SELECT id, depth, root_workflow_id FROM horsies_workflows
    WHERE parent_workflow_id = :wf_id AND status = 'PAUSED'
""")
RESUME_CHILD_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'RUNNING', updated_at = NOW()
    WHERE id = :wf_id AND status = 'PAUSED'
""")
# -- SQL constants for enqueue_workflow_task --

ENQUEUE_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = 'ENQUEUED', started_at = NOW()
    FROM horsies_workflows w
    WHERE wt.workflow_id = :wf_id
      AND wt.task_index = :idx
      AND wt.status = 'READY'
      AND w.id = wt.workflow_id
      AND w.status = 'RUNNING'
    RETURNING wt.id, wt.task_name, wt.task_args, wt.task_kwargs, wt.queue_name, wt.priority,
              wt.args_from, wt.workflow_ctx_from, wt.task_options
""")
INSERT_TASK_FOR_WORKFLOW_SQL = text("""
    INSERT INTO horsies_tasks (id, task_name, queue_name, priority, args, kwargs, status,
                       sent_at, enqueued_at, created_at, updated_at, claimed, retry_count, max_retries,
                       task_options, good_until, enqueue_sha, is_workflow_task)
    VALUES (:id, :name, :queue, :priority, :args, :kwargs, 'PENDING',
            :sent_at, NOW(), NOW(), NOW(), FALSE, 0, :max_retries, :task_options, :good_until,
            :enqueue_sha, TRUE)
""")
LINK_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks SET task_id = :tid WHERE workflow_id = :wf_id AND task_index = :idx
""")
# Pause-mid-level revert: when a payload-build failure's on_error=PAUSE
# handler pauses the workflow, the level's already-CAS'd-but-not-inserted
# siblings go back to READY (with started_at cleared, matching the
# pre-enqueue shape). READY-with-no-task is the recoverable state by
# design (resume step 3 enqueues READY nodes; recovery case 1 covers
# READY not enqueued) — leaving them ENQUEUED without task rows would be
# the stranded no-recovery state. Status predicate skips rows another
# path already advanced (e.g. the FAILED node itself).
REVERT_ENQUEUED_TO_READY_BATCH_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY', started_at = NULL
    WHERE workflow_id = :wf_id AND task_index = ANY(:idxs)
      AND status = 'ENQUEUED' AND task_id IS NULL
    RETURNING task_index
""")
# Pause-mid-start revert for prelinked fast roots: when a SLOW root's
# failure pauses the (just-created) workflow before the fast roots' task
# rows were inserted, the fast roots go back to READY with task_id and
# started_at cleared — a paused workflow gains no runnable task rows, and
# READY-with-no-task is the recovery-covered shape resume re-enqueues.
# Unlike REVERT_ENQUEUED_TO_READY_BATCH_SQL (promotion path, task_id was
# never set), these rows were bulk-inserted with task_id preset, so the
# revert clears it. Safe: the workflow row was created in this same
# transaction, so its status can only have been changed by this start's
# own failure handler.
REVERT_PRELINKED_FAST_ROOTS_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY', task_id = NULL, started_at = NULL
    WHERE workflow_id = :wf_id AND task_index = ANY(:idxs)
      AND status = 'ENQUEUED'
    RETURNING task_index
""")
# -- SQL constants for enqueue_subworkflow_task --

ENQUEUE_SUBWORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = 'ENQUEUED', started_at = NOW()
    FROM horsies_workflows w
    WHERE wt.workflow_id = :wf_id
      AND wt.task_index = :idx
      AND wt.status = 'READY'
      AND wt.is_subworkflow = TRUE
      AND w.id = wt.workflow_id
      AND w.status = 'RUNNING'
    RETURNING wt.id, wt.sub_workflow_name, wt.task_args, wt.task_kwargs,
              wt.args_from, wt.node_id, wt.sub_definition_key,
              w.name AS parent_workflow_name
""")
GET_WORKFLOW_NAME_SQL = text("""SELECT name FROM horsies_workflows WHERE id = :wf_id""")
MARK_WORKFLOW_TASK_FAILED_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'FAILED', result = :result, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx
      AND NOT (status = ANY(:terminal_states))
    RETURNING task_index
""")
INSERT_CHILD_WORKFLOW_SQL = text("""
    INSERT INTO horsies_workflows
    (id, name, status, on_error, output_task_index, success_policy,
     definition_key,
     parent_workflow_id, parent_task_index, depth, root_workflow_id,
     sent_at, created_at, started_at, updated_at)
    VALUES (:id, :name, 'RUNNING', :on_error, :output_idx, :success_policy,
            :definition_key,
            :parent_wf_id, :parent_idx, :depth, :root_wf_id,
            :sent_at, NOW(), NOW(), NOW())
""")
LINK_SUB_WORKFLOW_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET sub_workflow_id = :child_id, status = 'RUNNING'
    WHERE workflow_id = :wf_id AND task_index = :idx
""")
# -- SQL constants for _build_workflow_context_data --

GET_SUBWORKFLOW_SUMMARIES_SQL = text("""
    SELECT node_id, sub_workflow_summary
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = ANY(:node_ids)
      AND is_subworkflow = TRUE
      AND sub_workflow_summary IS NOT NULL
""")
# -- SQL constants for on_workflow_task_complete --

# Single round trip replacing the old locate -> lock -> CAS-update triple
# (each statement cost one RTT under the workflow lock; see the RTT
# hot-path audit). One statement does all three:
#   - `found` locates the workflow task by its backing task id and takes
#     the workflow row's FOR UPDATE lock (serializes completion handling
#     per workflow — same lock discipline as before).
#   - `upd` CAS-updates the node to its terminal status (no-op if a
#     concurrent path already made it terminal).
#   - The outer SELECT returns the progression context (wf_status, depth,
#     root_workflow_id, on_error) so no follow-up reads are needed: the
#     held lock freezes the workflow row for the rest of the transaction
#     (pause/cancel are UPDATEs on the same row and block on it), and
#     data-modifying CTEs always execute exactly once, so the CAS runs
#     even though the outer query only reads `found`.
# Zero rows = not a workflow task, or its workflow row is missing; the
# caller treats both as a no-op (matches the old early returns).
#
# Lock-order invariant (N6): this statement locks the workflow row (`found`'s
# FOR UPDATE OF w) before the workflow_task row (`upd`'s UPDATE). Every
# transaction that locks both horsies_workflows and horsies_workflow_tasks
# rows must take them in that order — workflows before workflow_tasks — or it
# can deadlock against this path (Postgres aborts one side, SQLSTATE 40P01).
# The cancel transaction (LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL in
# models/workflow/handle.py) is held to the same order.
COMPLETE_WORKFLOW_TASK_SQL = text("""
    WITH found AS (
        SELECT wt.workflow_id, wt.task_index,
               w.status AS wf_status, w.depth, w.root_workflow_id, w.on_error
        FROM horsies_workflow_tasks wt
        JOIN horsies_workflows w ON w.id = wt.workflow_id
        WHERE wt.task_id = :tid
        FOR UPDATE OF w
    ),
    upd AS (
        UPDATE horsies_workflow_tasks wt
        SET status = :status, result = :result, completed_at = NOW()
        FROM found
        WHERE wt.workflow_id = found.workflow_id
          AND wt.task_index = found.task_index
          AND NOT (wt.status = ANY(:terminal_states))
        RETURNING wt.task_index
    )
    SELECT found.workflow_id, found.task_index, found.wf_status,
           found.depth, found.root_workflow_id, found.on_error,
           EXISTS (SELECT 1 FROM upd) AS cas_won
    FROM found
""")
GET_WORKFLOW_STATUS_SQL = text(
    """SELECT status FROM horsies_workflows WHERE id = :wf_id"""
)
# -- SQL constants for _process_dependents --

# Batched dependent evaluation: one row per PENDING dependent of ANY of the
# source indexes (the just-terminal nodes of this cascade level), carrying
# the node config plus its dependency-status counts as jsonb. Replaces the
# old per-dependent GET_TASK_CONFIG + GET_DEP_STATUS_COUNTS pair (2 RTTs
# per dependent under the workflow lock; see the RTT hot-path audit).
# The w.status = 'RUNNING' filter is the old Python-side wf_status guard
# folded into the query: zero rows when paused/cancelled, same outcome.
GET_DEPENDENTS_EVAL_SQL = text("""
    SELECT d.task_index, d.dependencies, d.allow_failed_deps, d.join_type,
           d.min_success, d.workflow_ctx_from, d.is_subworkflow,
           (SELECT jsonb_object_agg(s.status, s.cnt)
            FROM (SELECT src.status, COUNT(*) AS cnt
                  FROM horsies_workflow_tasks src
                  WHERE src.workflow_id = d.workflow_id
                    AND src.task_index = ANY(d.dependencies)
                  GROUP BY src.status) s) AS dep_status_counts
    FROM horsies_workflow_tasks d
    JOIN horsies_workflows w ON w.id = d.workflow_id
    WHERE d.workflow_id = :wf_id
      AND d.dependencies && CAST(:source_idxs AS INTEGER[])
      AND d.status = 'PENDING'
      AND w.status = 'RUNNING'
    ORDER BY d.task_index
""")
# Batch variants of the per-node promotion CAS statements. Each preserves
# the per-row CAS verbatim (the status predicate re-evaluates per row), so
# rows another path already advanced simply drop out of RETURNING.
MARK_TASKS_READY_BATCH_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY'
    WHERE workflow_id = :wf_id AND task_index = ANY(:idxs) AND status = 'PENDING'
    RETURNING task_index
""")
SKIP_PENDING_TASKS_BATCH_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND task_index = ANY(:idxs) AND status = 'PENDING'
    RETURNING task_index
""")
SKIP_READY_TASKS_BATCH_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND task_index = ANY(:idxs) AND status = 'READY'
    RETURNING task_index
""")
# Batched READY -> ENQUEUED CAS. Identical jobs as the single-row
# ENQUEUE_WORKFLOW_TASK_SQL: per-row CAS, pause guard (workflows join on
# RUNNING), and payload source (RETURNING feeds the bulk task INSERT).
# Rows must be CAS'd BEFORE any horsies_tasks insert — the returned set is
# the only set inserted, so a missed guard can never orphan a task row.
ENQUEUE_WORKFLOW_TASKS_BATCH_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = 'ENQUEUED', started_at = NOW()
    FROM horsies_workflows w
    WHERE wt.workflow_id = :wf_id
      AND wt.task_index = ANY(:idxs)
      AND wt.status = 'READY'
      AND w.id = wt.workflow_id
      AND w.status = 'RUNNING'
    RETURNING wt.task_index, wt.task_name, wt.task_args, wt.task_kwargs,
              wt.queue_name, wt.priority, wt.args_from, wt.workflow_ctx_from,
              wt.task_options
""")
GET_WORKFLOW_DEPTH_SQL = text(
    """SELECT depth, root_workflow_id FROM horsies_workflows WHERE id = :wf_id"""
)
# -- SQL constants for try_make_ready_and_enqueue --

GET_TASK_CONFIG_SQL = text("""
    SELECT wt.status, wt.dependencies, wt.allow_failed_deps,
           wt.join_type, wt.min_success, wt.workflow_ctx_from,
           wt.is_subworkflow,
           w.status as wf_status
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.workflow_id = :wf_id AND wt.task_index = :idx
""")
GET_DEP_STATUS_COUNTS_SQL = text("""
    SELECT status, COUNT(*) as cnt
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND task_index = ANY(:deps)
    GROUP BY status
""")
SKIP_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND task_index = :idx AND status = 'PENDING'
""")
COUNT_CTX_TERMINAL_DEPS_SQL = text("""
    SELECT COUNT(*) as cnt
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = ANY(:node_ids)
      AND status = ANY(:wf_task_terminal_states)
""")
MARK_TASK_READY_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY'
    WHERE workflow_id = :wf_id AND task_index = :idx AND status = 'PENDING'
    RETURNING task_index
""")
SKIP_READY_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND task_index = :idx AND status = 'READY'
""")
SET_READY_WORKFLOW_TASK_TERMINAL_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = :status, result = :result, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx AND status = 'READY'
    RETURNING task_index
""")
# -- SQL constants for get_dependency_results --

GET_DEPENDENCY_RESULTS_SQL = text("""
    SELECT task_index, task_name, status, result, sub_definition_key
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND task_index = ANY(:indices)
      AND status = ANY(:wf_task_terminal_states)
""")
# -- SQL constants for get_dependency_results_with_names --

GET_DEPENDENCY_RESULTS_WITH_NAMES_SQL = text("""
    SELECT task_index, task_name, node_id, status, result, sub_definition_key
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = ANY(:node_ids)
      AND status = ANY(:wf_task_terminal_states)
""")
# -- SQL constants for check_workflow_completion --

LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL = text("""
    SELECT id
    FROM horsies_workflows
    WHERE id = :wf_id
    FOR UPDATE
""")
GET_WORKFLOW_COMPLETION_STATUS_SQL = text("""
    SELECT
        w.status,
        w.completed_at,
        w.error,
        w.success_policy,
        w.name,
        COUNT(wt.id) FILTER (WHERE NOT (wt.status = ANY(:wf_task_terminal_states))) as incomplete,
        COUNT(wt.id) FILTER (WHERE wt.status = 'FAILED') as failed,
        COUNT(wt.id) FILTER (WHERE wt.status = 'COMPLETED') as completed,
        COUNT(wt.id) as total
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.id = :wf_id
    GROUP BY w.id, w.status, w.completed_at, w.error, w.success_policy, w.name
""")
MARK_WORKFLOW_COMPLETED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'COMPLETED', result = :result, completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING' AND completed_at IS NULL
""")
MARK_WORKFLOW_FAILED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'FAILED', result = :result,
        error = COALESCE(:error, error),
        completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING' AND completed_at IS NULL
""")
GET_PARENT_WORKFLOW_INFO_SQL = text("""
    SELECT parent_workflow_id, parent_task_index
    FROM horsies_workflows WHERE id = :wf_id
""")
# -- SQL constants for on_subworkflow_complete --

# Single pass with COUNT FILTER instead of four correlated subqueries each
# scanning the child's task set. total uses COUNT(wt.id), not COUNT(*):
# the LEFT JOIN's null row must not count as 1 for a task-less workflow
# (builder-unreachable, but free defense; FILTER counts are NULL-immune).
GET_CHILD_WORKFLOW_INFO_SQL = text("""
    SELECT w.status, w.result, w.error, w.parent_workflow_id, w.parent_task_index,
           COUNT(wt.id) as total,
           COUNT(wt.id) FILTER (WHERE wt.status = 'COMPLETED') as completed,
           COUNT(wt.id) FILTER (WHERE wt.status = 'FAILED') as failed,
           COUNT(wt.id) FILTER (WHERE wt.status = 'SKIPPED') as skipped
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.id = :child_id
    GROUP BY w.id, w.status, w.result, w.error, w.parent_workflow_id,
             w.parent_task_index
""")
UPDATE_PARENT_NODE_RESULT_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = :status, result = :result, sub_workflow_summary = :summary, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx
      AND NOT (status = ANY(:terminal_states))
    RETURNING task_index
""")
# -- SQL constants for evaluate_workflow_success --

GET_TASK_STATUSES_SQL = text("""
    SELECT task_index, status
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
""")
# -- SQL constants for get_workflow_failure_error --

GET_FIRST_FAILED_TASK_RESULT_SQL = text("""
    SELECT task_name, result FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'FAILED'
    ORDER BY task_index ASC LIMIT 1
""")
GET_FIRST_FAILED_REQUIRED_TASK_SQL = text("""
    SELECT task_name, result FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND status = 'FAILED'
      AND task_index = ANY(:required)
    ORDER BY task_index ASC LIMIT 1
""")
# -- SQL constants for get_workflow_final_result --

GET_WORKFLOW_OUTPUT_INDEX_SQL = text(
    """SELECT output_task_index FROM horsies_workflows WHERE id = :wf_id"""
)
GET_OUTPUT_TASK_RESULT_SQL = text("""
    SELECT task_name, result FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND task_index = :idx
""")
# Terminal-node resolution is two-step (payload-free edge read, then a
# fetch of exactly the terminal rows) instead of a single anti-join: the
# correlated form ran one GIN probe per row (162ms at N=1000 inside the
# workflow completion lock), and a SQL-level anti-join rewrite is
# plan-unstable — per-workflow row estimates come from per-table stats, so
# the planner can flip to a nested-loop that re-runs the edge subquery per
# row (measured 146ms). Computing the set difference in Python is O(N+E)
# with no planner freedom.
GET_WORKFLOW_DAG_EDGES_SQL = text("""
    SELECT task_index, dependencies
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
""")

GET_TASK_RESULTS_BY_INDEXES_SQL = text("""
    SELECT node_id, task_index, task_name, result, sub_definition_key
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND task_index = ANY(:idxs)
""")
# -- SQL constants for _handle_workflow_task_failure --

GET_WORKFLOW_ON_ERROR_SQL = text(
    """SELECT on_error FROM horsies_workflows WHERE id = :wf_id"""
)
SET_WORKFLOW_ERROR_SQL = text("""
    UPDATE horsies_workflows
    SET error = :error, updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
""")
PAUSE_WORKFLOW_ON_ERROR_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'PAUSED', error = :error, updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
    RETURNING id
""")
