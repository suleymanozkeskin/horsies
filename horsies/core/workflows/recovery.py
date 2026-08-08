"""Workflow recovery logic.

This module handles recovery of stuck workflows:
- PENDING tasks with all dependencies terminal (race condition during parallel completion)
- READY tasks that weren't enqueued (crash after READY, before INSERT into tasks)
- READY SubWorkflowNodes that weren't started (sub_workflow_id is NULL)
- Child workflows completed but parent node not updated
- RUNNING workflows with no active tasks (all tasks done but workflow not updated)
- Stale RUNNING workflows (no progress for threshold period)

The crashed-worker case — a task terminal, its node not advanced — is
NOT here: terminalization records that progression as it moves the task
off the live table, and `phase2_recovery` consumes those records.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession as _RuntimeAsyncSession

from horsies.core.logging import get_logger
from horsies.core.models.workflow import WF_TASK_TERMINAL_VALUES

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from horsies.core.brokers.postgres import PostgresBroker

logger = get_logger('workflow.recovery')


async def _run_recovery_candidate(
    session: 'AsyncSession',
    *,
    case: str,
    workflow_id: str | None = None,
    task_index: int | None = None,
    child_id: str | None = None,
    task_id: str | None = None,
    action: Callable[[], Awaitable[bool]],
) -> bool:
    """Run one recovery candidate without letting it poison the full pass.

    The candidate's DB writes run in a SAVEPOINT so a failure rolls back its
    partial state without aborting the surrounding recovery transaction. The
    in-memory parent-propagation queue (``session.info``) is reverted in
    lockstep, since a SAVEPOINT rollback does not touch Python state. A
    SAVEPOINT requires a real ``AsyncSession``; mock-driven unit tests fall
    back to running the action directly under the same try/except guard.
    """
    from horsies.core.workflows.engine import (
        restore_pending_parent_propagations,
        snapshot_pending_parent_propagations,
    )

    pending_snapshot = snapshot_pending_parent_propagations(session)
    try:
        if isinstance(session, _RuntimeAsyncSession):
            async with session.begin_nested():
                return await action()
        return await action()
    except Exception as exc:
        restore_pending_parent_propagations(session, pending_snapshot)
        logger.error(
            'Workflow recovery candidate failed: case=%s workflow_id=%s '
            'task_index=%s child_id=%s task_id=%s exception_type=%s '
            'exception=%s',
            case,
            workflow_id,
            task_index,
            child_id,
            task_id,
            type(exc).__name__,
            str(exc)[:500],
        )
        return False


GLOBAL_SCAN_ROW_CAP = 200

GET_PENDING_WITH_TERMINAL_DEPS_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, w.depth, w.root_workflow_id
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'PENDING'
      AND w.status = 'RUNNING'
      AND (CAST(:scope_ids AS uuid[]) IS NULL OR wt.workflow_id = ANY(CAST(:scope_ids AS uuid[])))
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks dep
          WHERE dep.workflow_id = wt.workflow_id
            AND wt.dependencies @> ARRAY[dep.task_index]
            AND NOT (dep.status = ANY(:wf_task_terminal_states))
      )
    LIMIT CAST(:max_rows AS bigint)
""")


GET_READY_NOT_ENQUEUED_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.dependencies
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.task_id IS NULL
      AND wt.is_subworkflow = FALSE
      AND w.status = 'RUNNING'
      AND (CAST(:scope_ids AS uuid[]) IS NULL OR wt.workflow_id = ANY(CAST(:scope_ids AS uuid[])))
    LIMIT CAST(:max_rows AS bigint)
""")

GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.dependencies, w.depth, w.root_workflow_id
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.is_subworkflow = TRUE
      AND wt.sub_workflow_id IS NULL
      AND w.status = 'RUNNING'
      AND (CAST(:scope_ids AS uuid[]) IS NULL OR wt.workflow_id = ANY(CAST(:scope_ids AS uuid[])))
    LIMIT CAST(:max_rows AS bigint)
""")

GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL = text("""
    SELECT child.id, child.parent_workflow_id, child.parent_task_index, child.status
    FROM horsies_workflows child
    JOIN horsies_workflows parent ON parent.id = child.parent_workflow_id
    JOIN horsies_workflow_tasks wt ON wt.workflow_id = parent.id AND wt.task_index = child.parent_task_index
    WHERE child.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      AND wt.status = 'RUNNING'
      AND parent.status = 'RUNNING'
      AND (CAST(:scope_ids AS uuid[]) IS NULL OR child.id = ANY(CAST(:scope_ids AS uuid[])))
    LIMIT CAST(:max_rows AS bigint)
""")

GET_TERMINAL_WORKFLOW_CANDIDATES_SQL = text("""
    SELECT w.id, w.error, w.success_policy,
           COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed_count
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.status = 'RUNNING'
      AND (CAST(:scope_ids AS uuid[]) IS NULL OR w.id = ANY(CAST(:scope_ids AS uuid[])))
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt2
          WHERE wt2.workflow_id = w.id
            AND NOT (wt2.status = ANY(:wf_task_terminal_states))
      )
    GROUP BY w.id, w.error, w.success_policy
    LIMIT CAST(:max_rows AS bigint)
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
    # Global scans are capped per pass; scoped resume passes are not (see
    # the comment above the candidate queries).
    max_rows = GLOBAL_SCAN_ROW_CAP if scope_ids is None else None

    from horsies.core.workflows.engine import get_dependency_results, try_make_ready_and_enqueue

    # Case 0: PENDING tasks with all dependencies terminal (race condition during parallel completion)
    # This happens when multiple dependencies complete concurrently and the PENDING→READY
    # transition is missed due to timing.
    # Delegates to try_make_ready_and_enqueue which handles all readiness logic:
    # join types (all/any/quorum), ctx_from gates, allow_failed_deps,
    # subworkflow routing, and dependent cascade.
    pending_ready = await session.execute(
        GET_PENDING_WITH_TERMINAL_DEPS_SQL,
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES, 'scope_ids': scope_ids, 'max_rows': max_rows},
    )

    for row in pending_ready.fetchall():
        workflow_id = row.workflow_id
        task_index = row.task_index
        depth = row.depth or 0
        root_wf_id = row.root_workflow_id or workflow_id

        async def _recover_pending_ready() -> bool:
            await try_make_ready_and_enqueue(
                session, broker, workflow_id, task_index, depth, root_wf_id,
            )
            logger.info(
                f'Recovery evaluated stuck PENDING task: '
                f'workflow={workflow_id}, task_index={task_index}'
            )
            return True

        if await _run_recovery_candidate(
            session,
            case='pending_terminal_deps',
            workflow_id=workflow_id,
            task_index=task_index,
            action=_recover_pending_ready,
        ):
            recovered += 1

    # Case 1: READY tasks not enqueued (task_id is NULL but status is READY)
    # This happens if worker crashed after marking READY but before creating task
    # Excludes SubWorkflowNodes (handled separately)
    ready_not_enqueued = await session.execute(
        GET_READY_NOT_ENQUEUED_SQL,
        {'scope_ids': scope_ids, 'max_rows': max_rows},
    )

    for row in ready_not_enqueued.fetchall():
        workflow_id = row.workflow_id
        task_index = row.task_index
        raw_deps = row.dependencies
        dependencies: list[int] = (
            cast(list[int], raw_deps) if isinstance(raw_deps, list) else []
        )

        async def _recover_ready_not_enqueued() -> bool:
            # Fetch dependency results and re-enqueue. Strict-serde phase 6
            # changed ``get_dependency_results`` to return a tuple of
            # (results_by_index, task_names_by_index) so the engine can
            # encode args_from envelopes with source-task metadata.
            recovery_app = broker.app if broker is not None else None
            dep_results, dep_task_names, dep_definition_keys = await get_dependency_results(
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
                all_dep_definition_keys=dep_definition_keys,
            )
            if task_id:
                logger.info(
                    f'Recovered stuck READY task: workflow={workflow_id}, '
                    f'task_index={task_index}, new_task_id={task_id}'
                )
                return True
            return False

        if await _run_recovery_candidate(
            session,
            case='ready_not_enqueued',
            workflow_id=workflow_id,
            task_index=task_index,
            action=_recover_ready_not_enqueued,
        ):
            recovered += 1

    # Case 1.5: READY SubWorkflowNodes not started (sub_workflow_id is NULL)
    # This happens if worker crashed after marking READY but before starting child workflow
    # NOTE: This requires broker to start the child workflow, so we just mark them for retry
    ready_subworkflows = await session.execute(
        GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL,
        {'scope_ids': scope_ids, 'max_rows': max_rows},
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

        async def _recover_ready_subworkflow() -> bool:
            from horsies.core.workflows.engine import (
                enqueue_subworkflow_task,
                get_dependency_results,
            )

            dep_indices: list[int] = (
                dependencies if isinstance(dependencies, list) else []
            )
            dep_results, dep_task_names, _dep_definition_keys = await get_dependency_results(
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
            return True

        if await _run_recovery_candidate(
            session,
            case='ready_subworkflow_not_started',
            workflow_id=workflow_id,
            task_index=task_index,
            action=_recover_ready_subworkflow,
        ):
            recovered += 1

    # Case 1.6: Child workflows completed but parent node not updated
    # This happens if the on_subworkflow_complete callback failed or was interrupted
    completed_children = await session.execute(
        GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL,
        {'scope_ids': scope_ids, 'max_rows': max_rows},
    )

    for row in completed_children.fetchall():
        child_id = row.id
        parent_wf_id = row.parent_workflow_id
        parent_task_idx = row.parent_task_index
        child_status = row.status

        async def _recover_completed_child() -> bool:
            # Re-trigger the subworkflow completion callback.
            from horsies.core.workflows.engine import on_subworkflow_complete

            await on_subworkflow_complete(session, child_id, broker)
            logger.info(
                f'Recovered stuck child workflow completion: child={child_id}, '
                f'parent={parent_wf_id}:{parent_task_idx}, '
                f'child_status={child_status}'
            )
            return True

        if await _run_recovery_candidate(
            session,
            case='completed_child_parent_not_updated',
            workflow_id=parent_wf_id,
            task_index=parent_task_idx,
            child_id=child_id,
            action=_recover_completed_child,
        ):
            recovered += 1

    # Case 2+3: Workflows with all tasks terminal but workflow still RUNNING
    # This handles both completed and failed workflows, respecting success_policy.
    # This happens if worker crashed after completing last task but before updating workflow
    terminal_candidates = await session.execute(
        GET_TERMINAL_WORKFLOW_CANDIDATES_SQL,
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES, 'scope_ids': scope_ids, 'max_rows': max_rows},
    )

    for row in terminal_candidates.fetchall():
        workflow_id = row.id
        async def _recover_terminal_workflow() -> bool:
            # Delegate to the canonical completion path so recovery inherits locking,
            # parent propagation, and finalization semantics from engine.py.
            from horsies.core.workflows.engine import check_workflow_completion

            await check_workflow_completion(
                session,
                workflow_id,
                broker,
            )
            logger.info(
                f'Recovered terminal workflow via completion check: {workflow_id}'
            )
            return True

        if await _run_recovery_candidate(
            session,
            case='terminal_workflow_running',
            workflow_id=workflow_id,
            action=_recover_terminal_workflow,
        ):
            recovered += 1

    # Drain parent propagations queued by any completion check above. Reuse the
    # engine's drain but isolate each child in its own SAVEPOINT, so one poison
    # child cannot abort the rest of the recovery pass.
    from horsies.core.workflows.engine import (
        drain_parent_propagations_in_session,
        on_subworkflow_complete,
    )

    async def _drain_queued_propagation(child_id: str) -> None:
        async def _propagate() -> bool:
            await on_subworkflow_complete(session, child_id, broker)
            return True

        await _run_recovery_candidate(
            session,
            case='queued_parent_propagation',
            child_id=child_id,
            action=_propagate,
        )

    await drain_parent_propagations_in_session(
        session,
        broker,
        run_item=_drain_queued_propagation,
    )

    return recovered
