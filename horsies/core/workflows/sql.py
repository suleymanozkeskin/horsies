"""SQL constants for the workflow engine."""

from __future__ import annotations

from sqlalchemy import text

# -- SQL constants for start_workflow_async --

CHECK_WORKFLOW_EXISTS_SQL = text(
    """SELECT id FROM horsies_workflows WHERE id = :wf_id"""
)
INSERT_WORKFLOW_SQL = text("""
    INSERT INTO horsies_workflows (id, name, status, on_error, output_task_index,
                             success_policy, workflow_def_module, workflow_def_qualname,
                             depth, root_workflow_id,
                             created_at, started_at, updated_at)
    VALUES (:id, :name, 'RUNNING', :on_error, :output_idx,
            :success_policy, :wf_module, :wf_qualname,
            0, :id,
            NOW(), NOW(), NOW())
""")
INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL = text("""
    INSERT INTO horsies_workflow_tasks
    (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
     queue_name, priority, dependencies, args_from, workflow_ctx_from,
     allow_failed_deps, join_type, min_success, task_options, status,
     is_subworkflow, sub_workflow_name, sub_workflow_retry_mode,
     sub_workflow_module, sub_workflow_qualname, created_at)
    VALUES (:id, :wf_id, :idx, :node_id, :name, :args, :kwargs, :queue, :priority,
            :deps, :args_from, :ctx_from, :allow_failed, :join_type, :min_success,
            :task_options, :status, TRUE, :sub_wf_name, :sub_wf_retry_mode,
            :sub_wf_module, :sub_wf_qualname, NOW())
""")
INSERT_WORKFLOW_TASK_SQL = text("""
    INSERT INTO horsies_workflow_tasks
    (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
     queue_name, priority, dependencies, args_from, workflow_ctx_from,
     allow_failed_deps, join_type, min_success, task_options, status,
     is_subworkflow, created_at)
    VALUES (:id, :wf_id, :idx, :node_id, :name, :args, :kwargs, :queue, :priority,
            :deps, :args_from, :ctx_from, :allow_failed, :join_type, :min_success,
            :task_options, :status, FALSE, NOW())
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
                       sent_at, created_at, updated_at, claimed, retry_count, max_retries,
                       task_options, good_until)
    VALUES (:id, :name, :queue, :priority, :args, :kwargs, 'PENDING',
            NOW(), NOW(), NOW(), FALSE, 0, :max_retries, :task_options, :good_until)
""")
LINK_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks SET task_id = :tid WHERE workflow_id = :wf_id AND task_index = :idx
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
              wt.args_from, wt.node_id, wt.sub_workflow_module,
              wt.sub_workflow_qualname, wt.sub_workflow_retry_mode
""")
GET_WORKFLOW_NAME_SQL = text("""SELECT name FROM horsies_workflows WHERE id = :wf_id""")
MARK_WORKFLOW_TASK_FAILED_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'FAILED', result = :result, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx
""")
INSERT_CHILD_WORKFLOW_SQL = text("""
    INSERT INTO horsies_workflows
    (id, name, status, on_error, output_task_index, success_policy,
     workflow_def_module, workflow_def_qualname,
     parent_workflow_id, parent_task_index, depth, root_workflow_id,
     created_at, started_at, updated_at)
    VALUES (:id, :name, 'RUNNING', :on_error, :output_idx, :success_policy,
            :wf_module, :wf_qualname,
            :parent_wf_id, :parent_idx, :depth, :root_wf_id,
            NOW(), NOW(), NOW())
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

GET_WORKFLOW_TASK_BY_TASK_ID_SQL = text("""
    SELECT workflow_id, task_index
    FROM horsies_workflow_tasks
    WHERE task_id = :tid
""")
UPDATE_WORKFLOW_TASK_RESULT_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = :status, result = :result, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx
      AND NOT (status = ANY(:terminal_states))
    RETURNING task_index
""")
GET_WORKFLOW_STATUS_SQL = text(
    """SELECT status FROM horsies_workflows WHERE id = :wf_id"""
)
# -- SQL constants for _process_dependents --

GET_DEPENDENT_TASKS_SQL = text("""
    SELECT task_index FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND :completed_idx = ANY(dependencies)
      AND status = 'PENDING'
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
# -- SQL constants for _evaluate_conditions --

GET_WORKFLOW_DEF_PATH_SQL = text("""
    SELECT workflow_def_module, workflow_def_qualname
    FROM horsies_workflows
    WHERE id = :wf_id
""")
# -- SQL constants for get_dependency_results --

GET_DEPENDENCY_RESULTS_SQL = text("""
    SELECT task_index, status, result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND task_index = ANY(:indices)
      AND status = ANY(:wf_task_terminal_states)
""")
# -- SQL constants for get_dependency_results_with_names --

GET_DEPENDENCY_RESULTS_WITH_NAMES_SQL = text("""
    SELECT task_index, task_name, node_id, status, result
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
        COUNT(*) FILTER (WHERE NOT (wt.status = ANY(:wf_task_terminal_states))) as incomplete,
        COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed,
        COUNT(*) FILTER (WHERE wt.status = 'COMPLETED') as completed,
        COUNT(*) as total
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.id = :wf_id
    GROUP BY w.id, w.status, w.completed_at, w.error, w.success_policy, w.name
""")
MARK_WORKFLOW_COMPLETED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'COMPLETED', result = :result, completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND completed_at IS NULL
""")
MARK_WORKFLOW_FAILED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'FAILED', result = :result,
        error = COALESCE(:error, error),
        completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND completed_at IS NULL
""")
GET_PARENT_WORKFLOW_INFO_SQL = text("""
    SELECT parent_workflow_id, parent_task_index
    FROM horsies_workflows WHERE id = :wf_id
""")
# -- SQL constants for on_subworkflow_complete --

GET_CHILD_WORKFLOW_INFO_SQL = text("""
    SELECT w.status, w.result, w.error, w.parent_workflow_id, w.parent_task_index,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id) as total,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'COMPLETED') as completed,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'FAILED') as failed,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'SKIPPED') as skipped
    FROM horsies_workflows w
    WHERE w.id = :child_id
""")
UPDATE_PARENT_NODE_RESULT_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = :status, result = :result, sub_workflow_summary = :summary, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx
""")
# -- SQL constants for evaluate_workflow_success --

GET_TASK_STATUSES_SQL = text("""
    SELECT task_index, status
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
""")
# -- SQL constants for get_workflow_failure_error --

GET_FIRST_FAILED_TASK_RESULT_SQL = text("""
    SELECT result FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'FAILED'
    ORDER BY task_index ASC LIMIT 1
""")
GET_FIRST_FAILED_REQUIRED_TASK_SQL = text("""
    SELECT result FROM horsies_workflow_tasks
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
    SELECT result FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND task_index = :idx
""")
GET_TERMINAL_TASK_RESULTS_SQL = text("""
    SELECT wt.node_id, wt.task_index, wt.result
    FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = :wf_id
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks other
          WHERE other.workflow_id = wt.workflow_id
            AND wt.task_index = ANY(other.dependencies)
      )
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
""")
