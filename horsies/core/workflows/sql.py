"""SQL constants for the workflow engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import text

from ..history.ddl.classes import resolve_retention_class_key
from ..history.periods import RetentionClassKey
from ..models.tasks import Execution

# -- SQL constants for the engine's child workflow start --

# Bulk node insert of the engine's child workflow start
# (enqueue_subworkflow_task; executemany over all nodes in one pipelined
# batch). Differences from the per-row inserts above:
# - covers BOTH node kinds (sub_* fields NULL + is_subworkflow FALSE for
#   TaskNodes);
# - fast-path roots are inserted directly as ENQUEUED with task_id set and
#   started_at = NOW() — mirroring ENQUEUE_WORKFLOW_TASK_SQL's column
#   effects. The READY->ENQUEUED CAS that statement performs protects the
#   cross-transaction promotion path; at start time these rows are created
#   in the same uncommitted transaction, so the CAS is vacuous.
# - :is_enqueued is a separate boolean param (not derived from :status) to
#   avoid psycopg AmbiguousParameter on a twice-used parameter.
# - the node-kind parameters (the typed task facts :max_retries, :execution,
#   :good_until, :retention_class_key, and :sub_workflow_params) come from
#   task_node_kind_params for a TaskNode and from
#   subworkflow_node_kind_params for a SubWorkflowNode;
# - :downstream_subworkflow is the fact of the static graph that
#   with_downstream_subworkflow adds to the parameters of one workflow.
INSERT_WORKFLOW_TASKS_BULK_SQL = text("""
    INSERT INTO horsies_workflow_tasks
    (id, workflow_id, task_index, node_id, task_name, input,
     queue_name, priority, dependencies, args_from, workflow_ctx_from,
     allow_failed_deps, join_type, min_success, task_options,
     max_retries, execution, good_until, retention_class_key, status,
     is_subworkflow, sub_workflow_name, sub_definition_key,
     sub_workflow_params, downstream_subworkflow, task_id, started_at,
     created_at)
    VALUES (:id, :wf_id, :idx, :node_id, :name, :input, :queue, :priority,
            :deps, :args_from, :ctx_from, :allow_failed, :join_type, :min_success,
            :task_options, :max_retries, :execution, :good_until,
            :retention_class_key, :status, :is_subworkflow, :sub_wf_name,
            :sub_def_key, :sub_workflow_params, :downstream_subworkflow, :task_id,
            CASE WHEN :is_enqueued THEN NOW() END, NOW())
""")
# -- SQL constants for pause_workflow --

PAUSE_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'PAUSED', updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
    RETURNING id
""")
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
    RETURNING wt.id, wt.task_name, wt.input, wt.queue_name, wt.priority,
              wt.args_from, wt.workflow_ctx_from, wt.task_options,
              w.sent_at AS workflow_sent_at
""")
# A task row that the engine mints for a workflow node. It carries no command
# fingerprint (horsies_tasks_command_fingerprint_by_origin) and does not keep
# its input for a rerun: the rerun surface refuses workflow tasks, and the
# engine builds their input again from the node.
_INSERT_TASK_FOR_WORKFLOW = """
    INSERT INTO horsies_tasks (id, task_name, queue_name, priority, input, status,
                       sent_at, enqueued_at, created_at, updated_at, retry_count, max_retries,
                       task_options, execution, good_until, is_workflow_task,
                       retention_class_key, command_fingerprint_version, command_fingerprint,
                       retain_rerun_input)
    VALUES (:id, :name, :queue, :priority, :input, 'PENDING',
            :sent_at, NOW(), NOW(), NOW(), 0, :max_retries, :task_options, :execution,
            :good_until, TRUE,
            :retention_class_key, NULL, NULL,
            FALSE)
"""
INSERT_TASK_FOR_WORKFLOW_SQL = text(_INSERT_TASK_FOR_WORKFLOW)


@dataclass(frozen=True, slots=True)
class TaskNodeFacts:
    """The typed facts of one task node, parsed from its task options.

    A workflow start writes them on the node row
    (``horsies_workflow_tasks_task_facts_by_kind``), and a task row minted
    for the node stores the same values.

    Attributes:
        max_retries: The retry limit of the node's retry policy; 0 without one.
        execution: The execution class of the node's task.
        good_until: The ``good_until`` text of the task options, or ``None``.
            The database parses it into the timestamp column, on the node row
            and on the task row alike.
        retention_class_key: The stored retention class key of the node's
            queue: ``forever`` or ``standard_<days>d``.
    """

    max_retries: int
    execution: Execution
    good_until: str | None
    retention_class_key: str


def task_node_facts(
    *,
    max_retries: int,
    execution: Execution,
    good_until: str | None,
    queue_retention_class: RetentionClassKey | None,
) -> TaskNodeFacts:
    """The facts of one task node, with its queue's class key resolved.

    ``queue_retention_class`` is the class that the node's queue resolves
    to; ``None`` is the explicit forever choice and stores ``forever``, as
    on the send path. The caller reads the class from app configuration,
    because this module builds statements. Does no I/O.
    """
    return TaskNodeFacts(
        max_retries=max_retries,
        execution=execution,
        good_until=good_until,
        retention_class_key=resolve_retention_class_key(queue_retention_class),
    )


def task_node_fact_params(facts: TaskNodeFacts) -> dict[str, Any]:
    """The fact parameters of INSERT_WORKFLOW_TASKS_BULK_SQL and of the task
    row insert, for one task node."""
    return {
        'max_retries': facts.max_retries,
        'execution': facts.execution.value,
        'good_until': facts.good_until,
        'retention_class_key': facts.retention_class_key,
    }


class SubWorkflowParams(Enum):
    """The client's choice for the params of a sub-workflow node.

    Stored in ``horsies_workflow_tasks.sub_workflow_params`` on every
    sub-workflow node row, and NULL on task node rows
    (``horsies_workflow_tasks_sub_workflow_params_by_kind``).
    """

    NOT_GIVEN = 'NOT_GIVEN'
    """The node definition carries no params for the child's build."""

    GIVEN = 'GIVEN'
    """The node definition carries params for the child's build."""


class DownstreamSubworkflow(Enum):
    """Whether a sub-workflow node is reachable below a node along the
    dependency edges of its workflow.

    Stored in ``horsies_workflow_tasks.downstream_subworkflow``
    (``horsies_workflow_tasks_downstream_subworkflow_known``). The core's
    phase 2 of a REACHABLE node runs in a transaction, because its promotion
    can owe a child start.
    """

    NONE = 'NONE'
    """No sub-workflow node depends on this node, directly or through others."""

    REACHABLE = 'REACHABLE'
    """The node is an ancestor of a sub-workflow node."""


def with_downstream_subworkflow(
    node_params: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """The node parameters of one workflow with ``downstream_subworkflow``
    added to each, from the static graph: ``idx``, ``deps`` and
    ``is_subworkflow`` of the given parameters.

    The walk goes up from each sub-workflow node through the dependencies and
    visits each index once, so it ends after at most the node count of steps.
    Does no I/O.
    """
    dependencies: dict[int, list[int]] = {
        params['idx']: list(params['deps']) for params in node_params
    }
    pending: list[int] = [
        dependency
        for params in node_params
        if params['is_subworkflow']
        for dependency in params['deps']
    ]
    reachable: set[int] = set()
    while pending:
        index = pending.pop()
        if index in reachable:
            continue
        reachable.add(index)
        pending.extend(dependencies.get(index, []))
    return [
        {
            **params,
            'downstream_subworkflow': (
                DownstreamSubworkflow.REACHABLE
                if params['idx'] in reachable
                else DownstreamSubworkflow.NONE
            ).value,
        }
        for params in node_params
    ]


def task_node_kind_params(facts: TaskNodeFacts) -> dict[str, Any]:
    """The node-kind parameters of INSERT_WORKFLOW_TASKS_BULK_SQL for a
    TaskNode: its task facts, and no sub-workflow params choice."""
    return {**task_node_fact_params(facts), 'sub_workflow_params': None}


def subworkflow_node_kind_params(params: SubWorkflowParams) -> dict[str, Any]:
    """The node-kind parameters of INSERT_WORKFLOW_TASKS_BULK_SQL for a
    SubWorkflowNode: no task facts, and its params choice."""
    return {
        'max_retries': None,
        'execution': None,
        'good_until': None,
        'retention_class_key': None,
        'sub_workflow_params': params.value,
    }


def workflow_task_row_params(
    *,
    task_id: str,
    task_name: str,
    queue_name: str,
    priority: int,
    input_json: str,
    sent_at: datetime,
    task_options_json: str | None,
    facts: TaskNodeFacts,
) -> dict[str, Any]:
    """The parameters of INSERT_TASK_FOR_WORKFLOW_SQL for one node's task row.

    ``input_json`` is the JSON object of the task's keyword arguments,
    including the values that the engine injects. ``sent_at`` is the
    ``sent_at`` of the node's workflow row: the call time of the start of
    that workflow, as a send stores its call time. Does no I/O.
    """
    return {
        'id': task_id,
        'name': task_name,
        'queue': queue_name,
        'priority': priority,
        'input': input_json,
        'sent_at': sent_at,
        'task_options': task_options_json,
        **task_node_fact_params(facts),
    }
# The insert of a node's task and the link of the node to it, in one
# statement: the parameters of INSERT_TASK_FOR_WORKFLOW_SQL, plus :wf_id and
# :idx of the node. The insert runs whether or not the node matches, as when
# the two ran as two statements. Run with executemany, one execution per
# node, in one round trip.
INSERT_AND_LINK_WORKFLOW_TASK_SQL = text(f"""
    WITH inserted AS (
        {_INSERT_TASK_FOR_WORKFLOW}
        RETURNING id
    )
    UPDATE horsies_workflow_tasks wt
    SET task_id = inserted.id
    FROM inserted
    WHERE wt.workflow_id = :wf_id AND wt.task_index = :idx
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
    RETURNING wt.id, wt.sub_workflow_name, wt.input,
              wt.args_from, wt.node_id, wt.sub_definition_key,
              w.name AS parent_workflow_name
""")
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
SKIP_PENDING_TASKS_BATCH_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED', completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = ANY(:idxs) AND status = 'PENDING'
    RETURNING task_index
""")
SKIP_READY_TASKS_BATCH_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED', completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = ANY(:idxs) AND status = 'READY'
    RETURNING task_index
""")
# The promotion write of a level: one update over the plain task dependents
# whose join is met. A row of :idxs that is still PENDING goes to ENQUEUED,
# with started_at, when it is in :enqueue_idxs and the workflow is RUNNING,
# and to READY otherwise; a row that is no longer PENDING drops out of
# RETURNING. The rows of :idxs that are not in :enqueue_idxs are the
# skip-on-ready nodes, which the caller then moves to SKIPPED.
#
# This leaves the rows as the READY write (no workflow guard) followed by
# the READY -> ENQUEUED write (a RUNNING guard) left them, and NOW() is the
# start of the transaction in either statement. The READY row that the two
# writes made in between was seen by no other transaction, and no trigger is
# on this table. A caller that holds the workflow row lock sees the workflow
# RUNNING here, since its dependents read selects RUNNING workflows only.
# The skip cascade of the recovery does not hold the lock: a pause that
# commits after its dependents read leaves the row READY, as before.
#
# One statement locks every row of the level, as the READY write did. The
# rows move before any horsies_tasks insert, and only the ENQUEUED rows get a
# task row, so a node that did not move never gets one. RETURNING feeds the
# payload of that insert.
PROMOTE_DEPENDENTS_BATCH_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = CASE
            WHEN w.status = 'RUNNING'
                 AND wt.task_index = ANY(CAST(:enqueue_idxs AS INTEGER[]))
            THEN 'ENQUEUED'
            ELSE 'READY'
        END,
        started_at = CASE
            WHEN w.status = 'RUNNING'
                 AND wt.task_index = ANY(CAST(:enqueue_idxs AS INTEGER[]))
            THEN NOW()
            ELSE wt.started_at
        END
    FROM horsies_workflows w
    WHERE wt.workflow_id = :wf_id
      AND wt.task_index = ANY(:idxs)
      AND wt.status = 'PENDING'
      AND w.id = wt.workflow_id
    RETURNING wt.task_index, wt.status, wt.task_name, wt.input,
              wt.queue_name, wt.priority, wt.args_from,
              wt.workflow_ctx_from, wt.task_options,
              w.sent_at AS workflow_sent_at
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
    SET status = 'SKIPPED', completed_at = NOW()
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
    SET status = 'SKIPPED', completed_at = NOW()
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
    SELECT task_index, task_name, node_id, status, result, sub_definition_key
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
        w.output_task_index,
        w.parent_workflow_id,
        COUNT(wt.id) FILTER (WHERE NOT (wt.status = ANY(:wf_task_terminal_states))) as incomplete,
        COUNT(wt.id) FILTER (WHERE wt.status = 'FAILED') as failed,
        COUNT(wt.id) FILTER (WHERE wt.status = 'COMPLETED') as completed,
        COUNT(wt.id) as total
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.id = :wf_id
    GROUP BY w.id, w.status, w.completed_at, w.error, w.success_policy, w.name,
             w.output_task_index, w.parent_workflow_id
""")
MARK_WORKFLOW_COMPLETED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'COMPLETED', result = :result, completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING' AND completed_at IS NULL
""")
MARK_WORKFLOW_FAILED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'FAILED', result = NULL,
        error = COALESCE(:error, error),
        completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING' AND completed_at IS NULL
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

GET_OUTPUT_TASK_RESULT_SQL = text("""
    SELECT node_id, task_name, status, result FROM horsies_workflow_tasks
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
    SELECT node_id, task_index, task_name, status, result, sub_definition_key
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
