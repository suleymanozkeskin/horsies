"""Workflow recovery logic.

This module handles recovery of stuck workflows:
- PENDING tasks with all dependencies terminal (race condition during parallel completion)
- READY tasks that weren't enqueued (crash after READY, before INSERT into tasks)
- READY SubWorkflowNodes that weren't started (sub_workflow_id is NULL)
- Child workflows completed but parent node not updated
- RUNNING workflows with no active tasks (all tasks done but workflow not updated)
- Stale RUNNING workflows (no progress for threshold period)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import text

from horsies.core.codec.serde import loads_json, task_result_from_json
from horsies.core.logging import get_logger
from horsies.core.types.result import is_err
from horsies.core.models.workflow import WF_TASK_TERMINAL_VALUES

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.tasks import TaskResult, TaskError

logger = get_logger('workflow.recovery')


GET_PENDING_WITH_TERMINAL_DEPS_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, w.depth, w.root_workflow_id
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'PENDING'
      AND w.status = 'RUNNING'
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks dep
          WHERE dep.workflow_id = wt.workflow_id
            AND dep.task_index = ANY(wt.dependencies)
            AND NOT (dep.status = ANY(:wf_task_terminal_states))
      )
""")


GET_READY_NOT_ENQUEUED_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.dependencies
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.task_id IS NULL
      AND wt.is_subworkflow = FALSE
      AND w.status = 'RUNNING'
""")

GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.dependencies, w.depth, w.root_workflow_id
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.is_subworkflow = TRUE
      AND wt.sub_workflow_id IS NULL
      AND w.status = 'RUNNING'
""")

RESET_SUBWORKFLOW_TO_PENDING_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'PENDING'
    WHERE workflow_id = :wf_id AND task_index = :idx AND status = 'READY'
""")

GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL = text("""
    SELECT child.id, child.parent_workflow_id, child.parent_task_index, child.status
    FROM horsies_workflows child
    JOIN horsies_workflows parent ON parent.id = child.parent_workflow_id
    JOIN horsies_workflow_tasks wt ON wt.workflow_id = parent.id AND wt.task_index = child.parent_task_index
    WHERE child.status IN ('COMPLETED', 'FAILED')
      AND wt.status = 'RUNNING'
      AND parent.status = 'RUNNING'
""")

GET_CRASHED_WORKER_TASKS_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.task_id,
           UPPER(t.status) as task_status, t.result as task_result
    FROM horsies_workflow_tasks wt
    JOIN horsies_tasks t ON t.id = wt.task_id
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE NOT (wt.status = ANY(:wf_task_terminal_states))
      AND wt.task_id IS NOT NULL
      AND wt.is_subworkflow = FALSE
      AND w.status = 'RUNNING'
      AND UPPER(t.status) IN ('COMPLETED', 'FAILED', 'CANCELLED')
""")

GET_TERMINAL_WORKFLOW_CANDIDATES_SQL = text("""
    SELECT w.id, w.error, w.success_policy,
           COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed_count
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.status = 'RUNNING'
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt2
          WHERE wt2.workflow_id = w.id
            AND NOT (wt2.status = ANY(:wf_task_terminal_states))
      )
    GROUP BY w.id, w.error, w.success_policy
""")

MARK_WORKFLOW_COMPLETED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'COMPLETED', result = :result, completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
""")

MARK_WORKFLOW_FAILED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'FAILED', result = :result, error = :error,
        completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
""")

NOTIFY_WORKFLOW_DONE_SQL = text("""
    SELECT pg_notify('workflow_done', :wf_id)
""")


async def recover_stuck_workflows(
    session: 'AsyncSession',
    broker: 'PostgresBroker | None' = None,
) -> int:
    """
    Find and recover workflows in inconsistent states.

    Recovery cases:
    0. PENDING tasks with all deps terminal - race condition during parallel completion
    1. READY tasks that weren't enqueued (task_id is NULL) - crash after READY, before INSERT
    2. RUNNING workflows with all tasks complete - workflow status not updated
    3. Workflows stuck in RUNNING with no progress

    Args:
        session: Database session (caller manages commit)

    Returns:
        Count of recovered workflow tasks.
    """
    recovered = 0

    from horsies.core.workflows.engine import get_dependency_results, try_make_ready_and_enqueue

    # Case 0: PENDING tasks with all dependencies terminal (race condition during parallel completion)
    # This happens when multiple dependencies complete concurrently and the PENDING→READY
    # transition is missed due to timing.
    # Delegates to try_make_ready_and_enqueue which handles all readiness logic:
    # join types (all/any/quorum), ctx_from gates, run_when/skip_when conditions,
    # allow_failed_deps, subworkflow routing, and dependent cascade.
    pending_ready = await session.execute(
        GET_PENDING_WITH_TERMINAL_DEPS_SQL,
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES},
    )

    for row in pending_ready.fetchall():
        workflow_id = row[0]
        task_index = row[1]
        depth = row[2] or 0
        root_wf_id = row[3] or workflow_id

        await try_make_ready_and_enqueue(
            session, broker, workflow_id, task_index, depth, root_wf_id,
        )
        logger.info(
            f'Recovery evaluated stuck PENDING task: '
            f'workflow={workflow_id}, task_index={task_index}'
        )
        recovered += 1

    # Case 1: READY tasks not enqueued (task_id is NULL but status is READY)
    # This happens if worker crashed after marking READY but before creating task
    # Excludes SubWorkflowNodes (handled separately)
    ready_not_enqueued = await session.execute(GET_READY_NOT_ENQUEUED_SQL)

    for row in ready_not_enqueued.fetchall():
        workflow_id = row[0]
        task_index = row[1]
        raw_deps = row[2]
        dependencies: list[int] = (
            cast(list[int], raw_deps) if isinstance(raw_deps, list) else []
        )

        # Fetch dependency results and re-enqueue
        dep_results: dict[
            int, 'TaskResult[Any, TaskError]'
        ] = await get_dependency_results(session, workflow_id, dependencies)

        from horsies.core.workflows.engine import enqueue_workflow_task

        task_id = await enqueue_workflow_task(
            session, workflow_id, task_index, dep_results, broker
        )
        if task_id:
            logger.info(
                f'Recovered stuck READY task: workflow={workflow_id}, '
                f'task_index={task_index}, new_task_id={task_id}'
            )
            recovered += 1

    # Case 1.5: READY SubWorkflowNodes not started (sub_workflow_id is NULL)
    # This happens if worker crashed after marking READY but before starting child workflow
    # NOTE: This requires broker to start the child workflow, so we just mark them for retry
    ready_subworkflows = await session.execute(GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL)

    for row in ready_subworkflows.fetchall():
        workflow_id = row[0]
        task_index = row[1]
        dependencies = row[2]
        depth = row[3] or 0
        root_wf_id = row[4] or workflow_id

        if broker is not None:
            from horsies.core.workflows.engine import (
                enqueue_subworkflow_task,
                get_dependency_results,
            )

            dep_indices: list[int] = (
                cast(list[int], dependencies) if isinstance(dependencies, list) else []
            )
            dep_results = await get_dependency_results(
                session, workflow_id, dep_indices
            )
            await enqueue_subworkflow_task(
                session, broker, workflow_id, task_index, dep_results, depth, root_wf_id
            )
            logger.info(
                f'Recovered stuck READY subworkflow (started): '
                f'workflow={workflow_id}, task_index={task_index}'
            )
        else:
            # Reset to PENDING so a future evaluation can start it
            await session.execute(
                RESET_SUBWORKFLOW_TO_PENDING_SQL,
                {'wf_id': workflow_id, 'idx': task_index},
            )
            logger.info(
                f'Recovered stuck READY subworkflow (reset to PENDING): '
                f'workflow={workflow_id}, task_index={task_index}'
            )
        recovered += 1

    # Case 1.6: Child workflows completed but parent node not updated
    # This happens if the on_subworkflow_complete callback failed or was interrupted
    completed_children = await session.execute(GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL)

    for row in completed_children.fetchall():
        child_id = row[0]
        parent_wf_id = row[1]
        parent_task_idx = row[2]
        child_status = row[3]

        # Re-trigger the subworkflow completion callback
        from horsies.core.workflows.engine import on_subworkflow_complete

        await on_subworkflow_complete(session, child_id, broker)
        logger.info(
            f'Recovered stuck child workflow completion: child={child_id}, '
            f'parent={parent_wf_id}:{parent_task_idx}, child_status={child_status}'
        )
        recovered += 1

    # Case 1.7: workflow_tasks stuck non-terminal but underlying task is already terminal.
    # This happens when a worker crashes mid-execution:
    # - Reaper marks tasks.status = FAILED (WORKER_CRASHED)
    # - on_workflow_task_complete() was never called (worker died)
    # - workflow_tasks row stays RUNNING/ENQUEUED indefinitely
    crashed_worker_tasks = await session.execute(
        GET_CRASHED_WORKER_TASKS_SQL,
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES},
    )

    for row in crashed_worker_tasks.fetchall():
        workflow_id = row[0]
        task_index = row[1]
        task_id = row[2]
        task_status = row[3]  # uppercase: COMPLETED, FAILED, or CANCELLED
        raw_task_result = row[4]

        from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode

        # Deserialize TaskResult from tasks.result, or build a synthetic one
        if raw_task_result is not None:
            _loads_r = loads_json(raw_task_result)
            _tr_r = task_result_from_json(_loads_r.ok_value) if not is_err(_loads_r) else None
            if _tr_r is not None and not is_err(_tr_r):
                result: TaskResult[Any, TaskError] = _tr_r.ok_value
            else:
                # Stored result is corrupt — treat as a crash with no usable result
                logger.warning(
                    f'Recovery: task {task_id} has corrupt result JSON, building synthetic error',
                )
                result = TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.WORKER_CRASHED,
                        message='Stored task result is corrupt and could not be deserialized',
                        data={'task_id': task_id, 'task_status': task_status, 'recovery': 'case_1_7'},
                    ),
                )
        else:
            # No result stored (e.g. crash before result, DB issue, or cancellation)
            if task_status == 'CANCELLED':
                error_code = LibraryErrorCode.TASK_CANCELLED
                message = 'Task was cancelled before producing a result'
            elif task_status == 'COMPLETED':
                error_code = LibraryErrorCode.RESULT_NOT_AVAILABLE
                message = 'Task completed but result is missing'
            else:
                error_code = LibraryErrorCode.WORKER_CRASHED
                message = (
                    'Worker crashed during task execution '
                    f'(task_status={task_status}, no result stored)'
                )

            result = TaskResult(
                err=TaskError(
                    error_code=error_code,
                    message=message,
                    data={
                        'task_id': task_id,
                        'task_status': task_status,
                        'recovery': 'case_1_7',
                    },
                ),
            )

        # Reuse the existing completion handler to update workflow_tasks,
        # apply on_error policy, process dependents, and check workflow completion
        from horsies.core.workflows.engine import on_workflow_task_complete

        await on_workflow_task_complete(session, task_id, result, broker)
        logger.info(
            f'Recovered crashed worker workflow task: workflow={workflow_id}, '
            f'task_index={task_index}, task_id={task_id}, task_status={task_status}'
        )
        recovered += 1

    # Case 2+3: Workflows with all tasks terminal but workflow still RUNNING
    # This handles both completed and failed workflows, respecting success_policy.
    # This happens if worker crashed after completing last task but before updating workflow
    terminal_candidates = await session.execute(
        GET_TERMINAL_WORKFLOW_CANDIDATES_SQL,
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES},
    )

    for row in terminal_candidates.fetchall():
        workflow_id = row[0]
        existing_error = row[1]
        success_policy_data = row[2]
        failed_count = row[3] or 0

        # Compute final result
        from horsies.core.workflows.engine import (
            get_workflow_final_result,
            evaluate_workflow_success,
            get_workflow_failure_error,
        )

        final_result = await get_workflow_final_result(session, workflow_id)

        # Evaluate success using success_policy (or default behavior)
        has_error = existing_error is not None
        workflow_succeeded = await evaluate_workflow_success(
            session, workflow_id, success_policy_data, has_error, failed_count
        )

        if workflow_succeeded:
            await session.execute(
                MARK_WORKFLOW_COMPLETED_SQL,
                {'wf_id': workflow_id, 'result': final_result},
            )
            logger.info(f'Recovered stuck COMPLETED workflow: {workflow_id}')
        else:
            # Compute error if not already set
            error_payload = existing_error
            if error_payload is None:
                error_payload = await get_workflow_failure_error(
                    session, workflow_id, success_policy_data
                )

            await session.execute(
                MARK_WORKFLOW_FAILED_SQL,
                {'wf_id': workflow_id, 'result': final_result, 'error': error_payload},
            )
            logger.info(f'Recovered stuck FAILED workflow: {workflow_id}')

        # Send NOTIFY for workflow completion
        await session.execute(
            NOTIFY_WORKFLOW_DONE_SQL,
            {'wf_id': workflow_id},
        )
        recovered += 1

    return recovered
