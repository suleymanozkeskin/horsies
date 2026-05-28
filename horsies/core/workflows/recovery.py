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

from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.serde import loads_json
from horsies.core.codec.typed import (
    decode_task_error,
    decode_task_result,
    validate_task_result_envelope,
)
from horsies.core.logging import get_logger
from horsies.core.types.result import is_err
from horsies.core.models.workflow import WF_TASK_TERMINAL_VALUES
from pydantic import ValidationError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from horsies.core.app import Horsies
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.tasks import TaskResult, TaskError

logger = get_logger('workflow.recovery')


def _decode_recovered_task_result(
    raw_task_result: str,
    *,
    app: 'Horsies | None',
    task_name: str | None,
    task_id: str,
    task_status: str,
) -> 'TaskResult[Any, TaskError]':
    """Decode a stored TaskResult during workflow recovery.

    Strict-serde phase 6: typed decode via ``decode_task_result`` using
    the source task's ``task_ok_type``. An err-only envelope is decoded
    via ``decode_task_error`` *without* requiring ``task_ok_type`` —
    ``TaskError`` is a fixed-schema internal type, so the real failure
    survives even if the task isn't registered in the recovery process.
    Only an ok-slot envelope without a registered ``task_ok_type`` (or
    any decode failure) falls back to the synthetic ``WORKER_CRASHED``
    error so the recovery flow keeps progressing.
    """
    from horsies.core.models.tasks import (
        TaskResult,
        TaskError,
        OperationalErrorCode,
    )

    synthetic: 'TaskResult[Any, TaskError]' = TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.WORKER_CRASHED,
            message='Stored task result could not be decoded during recovery',
            data={
                'task_id': task_id,
                'task_status': task_status,
                'recovery': 'case_1_7',
            },
        ),
    )

    loaded_r = loads_json(raw_task_result)
    if is_err(loaded_r):
        logger.warning(
            f'Recovery: task {task_id} result JSON corrupt: '
            f'{loaded_r.err_value}'
        )
        return synthetic

    # Err-fast-path: TaskError is fixed-schema; an err-only payload is
    # decodable without the source task's ok_type. Skips the
    # registration check that would otherwise discard the real err.
    try:
        envelope = validate_task_result_envelope(loaded_r.ok_value)
    except StrictJsonError as exc:
        logger.warning(
            f'Recovery: task {task_id} envelope invalid: {exc}'
        )
        return synthetic
    err_slot = envelope.get('err')
    if err_slot is not None:
        try:
            return TaskResult(err=decode_task_error(err_slot))
        except (StrictJsonError, ValidationError) as exc:
            logger.warning(
                f'Recovery: task {task_id} decode_task_error failed: {exc}'
            )
            return synthetic

    source_task = (
        app.tasks.get(task_name)
        if (app is not None and isinstance(task_name, str))
        else None
    )
    source_ok_type = (
        getattr(source_task, 'task_ok_type', None)
        if source_task is not None
        else None
    )
    if source_ok_type is None:
        logger.warning(
            f'Recovery: task {task_id} ({task_name!r}) not registered or '
            f'missing task_ok_type; using synthetic error'
        )
        return synthetic
    try:
        return decode_task_result(loaded_r.ok_value, source_ok_type)
    except (StrictJsonError, ValidationError) as exc:
        logger.warning(
            f'Recovery: task {task_id} decode_task_result failed: {exc}'
        )
        return synthetic


# Each candidate query carries a uniform scope filter:
#   AND (CAST(:scope_ids AS varchar[]) IS NULL OR <wf_col> = ANY(CAST(:scope_ids AS varchar[])))
# When ``:scope_ids`` is NULL (global reaper), the OR short-circuits and the
# query behaves exactly as before. When a list of workflow ids is bound
# (resume-time race closure), candidates are restricted to that tree. The
# explicit ``::varchar[]`` cast avoids "could not determine data type" on the
# NULL bind (workflow ids are VARCHAR(36) columns).

GET_PENDING_WITH_TERMINAL_DEPS_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, w.depth, w.root_workflow_id
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'PENDING'
      AND w.status = 'RUNNING'
      AND (CAST(:scope_ids AS varchar[]) IS NULL OR wt.workflow_id = ANY(CAST(:scope_ids AS varchar[])))
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
      AND (CAST(:scope_ids AS varchar[]) IS NULL OR wt.workflow_id = ANY(CAST(:scope_ids AS varchar[])))
""")

GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.dependencies, w.depth, w.root_workflow_id
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.is_subworkflow = TRUE
      AND wt.sub_workflow_id IS NULL
      AND w.status = 'RUNNING'
      AND (CAST(:scope_ids AS varchar[]) IS NULL OR wt.workflow_id = ANY(CAST(:scope_ids AS varchar[])))
""")

GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL = text("""
    SELECT child.id, child.parent_workflow_id, child.parent_task_index, child.status
    FROM horsies_workflows child
    JOIN horsies_workflows parent ON parent.id = child.parent_workflow_id
    JOIN horsies_workflow_tasks wt ON wt.workflow_id = parent.id AND wt.task_index = child.parent_task_index
    WHERE child.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
      AND wt.status = 'RUNNING'
      AND parent.status = 'RUNNING'
      AND (CAST(:scope_ids AS varchar[]) IS NULL OR child.id = ANY(CAST(:scope_ids AS varchar[])))
""")

GET_CRASHED_WORKER_TASKS_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.task_id, wt.task_name,
           UPPER(t.status) as task_status, t.result as task_result
    FROM horsies_workflow_tasks wt
    JOIN horsies_tasks t ON t.id = wt.task_id
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE NOT (wt.status = ANY(:wf_task_terminal_states))
      AND wt.task_id IS NOT NULL
      AND wt.is_subworkflow = FALSE
      AND w.status = 'RUNNING'
      AND UPPER(t.status) IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      AND (CAST(:scope_ids AS varchar[]) IS NULL OR wt.workflow_id = ANY(CAST(:scope_ids AS varchar[])))
""")

GET_TERMINAL_WORKFLOW_CANDIDATES_SQL = text("""
    SELECT w.id, w.error, w.success_policy,
           COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed_count
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.status = 'RUNNING'
      AND (CAST(:scope_ids AS varchar[]) IS NULL OR w.id = ANY(CAST(:scope_ids AS varchar[])))
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt2
          WHERE wt2.workflow_id = w.id
            AND NOT (wt2.status = ANY(:wf_task_terminal_states))
      )
    GROUP BY w.id, w.error, w.success_policy
""")

# Resolve the descendant tree (self + all transitive children) for a resumed
# workflow, so the resume-time recovery pass is scoped to that tree only.
GET_WORKFLOW_TREE_IDS_SQL = text("""
    WITH RECURSIVE tree AS (
        SELECT id FROM horsies_workflows WHERE id = :wf_id
        UNION ALL
        SELECT child.id
        FROM horsies_workflows child
        JOIN tree parent ON child.parent_workflow_id = parent.id
    )
    SELECT id FROM tree
""")

async def recover_stuck_workflows(
    session: 'AsyncSession',
    broker: 'PostgresBroker | None' = None,
    scope_workflow_ids: list[str] | None = None,
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
        broker: Optional broker for subworkflow/task re-enqueue.
        scope_workflow_ids: When provided, restrict every candidate query
            to these workflow ids. ``None`` (default, the periodic reaper)
            scans all workflows globally. Resume passes the resumed
            workflow's tree so the pause-resume race is closed without a
            full-DB sweep.

    Returns:
        Count of recovered workflow tasks.
    """
    recovered = 0
    scope_ids = scope_workflow_ids

    from horsies.core.workflows.engine import get_dependency_results, try_make_ready_and_enqueue

    # Case 0: PENDING tasks with all dependencies terminal (race condition during parallel completion)
    # This happens when multiple dependencies complete concurrently and the PENDING→READY
    # transition is missed due to timing.
    # Delegates to try_make_ready_and_enqueue which handles all readiness logic:
    # join types (all/any/quorum), ctx_from gates, allow_failed_deps,
    # subworkflow routing, and dependent cascade.
    pending_ready = await session.execute(
        GET_PENDING_WITH_TERMINAL_DEPS_SQL,
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES, 'scope_ids': scope_ids},
    )

    for row in pending_ready.fetchall():
        workflow_id = row.workflow_id
        task_index = row.task_index
        depth = row.depth or 0
        root_wf_id = row.root_workflow_id or workflow_id

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
    ready_not_enqueued = await session.execute(
        GET_READY_NOT_ENQUEUED_SQL,
        {'scope_ids': scope_ids},
    )

    for row in ready_not_enqueued.fetchall():
        workflow_id = row.workflow_id
        task_index = row.task_index
        raw_deps = row.dependencies
        dependencies: list[int] = (
            cast(list[int], raw_deps) if isinstance(raw_deps, list) else []
        )

        # Fetch dependency results and re-enqueue. Strict-serde phase 6
        # changed ``get_dependency_results`` to return a tuple of
        # (results_by_index, task_names_by_index) so the engine can
        # encode args_from envelopes with source-task metadata.
        recovery_app = broker.app if broker is not None else None
        dep_results, dep_task_names = await get_dependency_results(
            session, workflow_id, dependencies, app=recovery_app,
        )

        from horsies.core.workflows.engine import enqueue_workflow_task

        task_id = await enqueue_workflow_task(
            session,
            workflow_id,
            task_index,
            dep_results,
            dep_task_names,
            broker,
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
    ready_subworkflows = await session.execute(
        GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL,
        {'scope_ids': scope_ids},
    )

    for row in ready_subworkflows.fetchall():
        workflow_id = row.workflow_id
        task_index = row.task_index
        dependencies = row.dependencies
        depth = row.depth or 0
        root_wf_id = row.root_workflow_id or workflow_id

        if broker is None:
            logger.warning(
                f'Recovery found stuck READY subworkflow but no broker was '
                f'provided to start it: workflow={workflow_id}, '
                f'task_index={task_index}'
            )
            continue

        from horsies.core.workflows.engine import (
            enqueue_subworkflow_task,
            get_dependency_results,
        )

        dep_indices: list[int] = (
            cast(list[int], dependencies) if isinstance(dependencies, list) else []
        )
        dep_results, dep_task_names = await get_dependency_results(
            session, workflow_id, dep_indices, app=broker.app,
        )
        await enqueue_subworkflow_task(
            session,
            broker,
            workflow_id,
            task_index,
            dep_results,
            dep_task_names,
            depth,
            root_wf_id,
        )
        logger.info(
            f'Recovered stuck READY subworkflow (started): '
            f'workflow={workflow_id}, task_index={task_index}'
        )
        recovered += 1

    # Case 1.6: Child workflows completed but parent node not updated
    # This happens if the on_subworkflow_complete callback failed or was interrupted
    completed_children = await session.execute(
        GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL,
        {'scope_ids': scope_ids},
    )

    for row in completed_children.fetchall():
        child_id = row.id
        parent_wf_id = row.parent_workflow_id
        parent_task_idx = row.parent_task_index
        child_status = row.status

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
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES, 'scope_ids': scope_ids},
    )

    for row in crashed_worker_tasks.fetchall():
        workflow_id = row.workflow_id
        task_index = row.task_index
        task_id = row.task_id
        task_name = row.task_name
        task_status = row.task_status  # uppercase: COMPLETED, FAILED, CANCELLED, or EXPIRED
        raw_task_result = row.task_result

        from horsies.core.models.tasks import TaskResult, TaskError, OperationalErrorCode, RetrievalCode, OutcomeCode

        # Strict-serde phase 6: typed decode against the source task's
        # ``task_ok_type``. Recovery has no direct app reference; resolve
        # via the broker that owns this recovery cycle.
        recovery_app = broker.app if broker is not None else None
        if raw_task_result is not None:
            result = _decode_recovered_task_result(
                raw_task_result,
                app=recovery_app,
                task_name=task_name,
                task_id=task_id,
                task_status=task_status,
            )
        else:
            # No result stored (e.g. crash before result, DB issue, or cancellation)
            if task_status == 'CANCELLED':
                error_code = OutcomeCode.TASK_CANCELLED
                message = 'Task was cancelled before producing a result'
            elif task_status == 'EXPIRED':
                error_code = OutcomeCode.TASK_EXPIRED
                message = 'Task expired before execution started (good_until passed)'
            elif task_status == 'COMPLETED':
                error_code = RetrievalCode.RESULT_NOT_AVAILABLE
                message = 'Task completed but result is missing'
            else:
                error_code = OperationalErrorCode.WORKER_CRASHED
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
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES, 'scope_ids': scope_ids},
    )

    for row in terminal_candidates.fetchall():
        workflow_id = row.id
        # Delegate to the canonical completion path so recovery inherits locking,
        # parent propagation, and finalization semantics from engine.py.
        from horsies.core.workflows.engine import check_workflow_completion

        await check_workflow_completion(
            session,
            workflow_id,
            broker,
        )
        logger.info(f'Recovered terminal workflow via completion check: {workflow_id}')
        recovered += 1

    return recovered
