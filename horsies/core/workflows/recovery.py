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

import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, assert_never, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession as _RuntimeAsyncSession

from horsies.core.logging import get_logger
from horsies.core.models.workflow import WF_TASK_TERMINAL_VALUES

if TYPE_CHECKING:
    from sqlalchemy import TextClause
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
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
    metrics: 'RecoveryCaseMetrics | None' = None,
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
        if isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            session,
            _RuntimeAsyncSession,
        ):
            async with session.begin_nested():
                return await action()
        return await action()
    except Exception as exc:
        restore_pending_parent_propagations(session, pending_snapshot)
        if metrics is not None:
            metrics.errors += 1
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
      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks dep
          WHERE dep.workflow_id = wt.workflow_id
            AND wt.dependencies @> ARRAY[dep.task_index]
            AND NOT (dep.status = ANY(:wf_task_terminal_states))
      )
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_PENDING_WITH_TERMINAL_DEPS_SQL = text(
    GET_PENDING_WITH_TERMINAL_DEPS_SQL.text.replace(
        '      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))\n',
        '',
    )
)


GET_READY_NOT_ENQUEUED_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.dependencies
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.task_id IS NULL
      AND wt.is_subworkflow = FALSE
      AND w.status = 'RUNNING'
      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_READY_NOT_ENQUEUED_SQL = text(
    GET_READY_NOT_ENQUEUED_SQL.text.replace(
        '      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))\n',
        '',
    )
)

GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL = text("""
    SELECT wt.workflow_id, wt.task_index, wt.dependencies, w.depth, w.root_workflow_id
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.is_subworkflow = TRUE
      AND wt.sub_workflow_id IS NULL
      AND w.status = 'RUNNING'
      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL = text(
    GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL.text.replace(
        '      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))\n',
        '',
    )
)

GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL = text("""
    SELECT child.id, child.parent_workflow_id, child.parent_task_index, child.status
    FROM horsies_workflows child
    JOIN horsies_workflows parent ON parent.id = child.parent_workflow_id
    JOIN horsies_workflow_tasks wt ON wt.workflow_id = parent.id AND wt.task_index = child.parent_task_index
    WHERE child.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      AND wt.status = 'RUNNING'
      AND parent.status = 'RUNNING'
      AND child.id = ANY(CAST(:scope_ids AS uuid[]))
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL = text(
    GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL.text.replace(
        '      AND child.id = ANY(CAST(:scope_ids AS uuid[]))\n',
        '',
    )
)

GET_TERMINAL_WORKFLOW_CANDIDATES_SQL = text("""
    SELECT w.id, w.error, w.success_policy,
           COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed_count
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.status = 'RUNNING'
      AND w.id = ANY(CAST(:scope_ids AS uuid[]))
      AND EXISTS (
          SELECT 1 FROM horsies_workflow_tasks present
          WHERE present.workflow_id = w.id
      )
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt2
          WHERE wt2.workflow_id = w.id
            AND NOT (wt2.status = ANY(:wf_task_terminal_states))
      )
    GROUP BY w.id, w.error, w.success_policy
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_TERMINAL_WORKFLOW_CANDIDATES_SQL = text(
    GET_TERMINAL_WORKFLOW_CANDIDATES_SQL.text.replace(
        '      AND w.id = ANY(CAST(:scope_ids AS uuid[]))\n',
        '',
    )
)

GET_ORPHANED_WORKFLOW_CANDIDATES_SQL = text("""
    SELECT w.id, w.name
    FROM horsies_workflows w
    WHERE w.status = 'RUNNING'
      AND w.id = ANY(CAST(:scope_ids AS uuid[]))
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt
          WHERE wt.workflow_id = w.id
      )
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_ORPHANED_WORKFLOW_CANDIDATES_SQL = text(
    GET_ORPHANED_WORKFLOW_CANDIDATES_SQL.text.replace(
        '      AND w.id = ANY(CAST(:scope_ids AS uuid[]))\n',
        '',
    )
)

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

GLOBAL_WORKFLOW_AUDIT_SQL = text("""
WITH cursor_row AS MATERIALIZED (
    SELECT last_created_at, last_id,
           cycle_upper_created_at, cycle_upper_id
    FROM horsies_recovery_scan_cursors
    WHERE scan_name = 'running_workflows'
      AND (claim_token IS NULL OR claim_expires_at <= statement_timestamp())
    FOR UPDATE SKIP LOCKED
),
upper_bound AS MATERIALIZED (
    SELECT COALESCE(c.cycle_upper_created_at, latest.created_at) AS created_at,
           COALESCE(c.cycle_upper_id, latest.id) AS id
    FROM cursor_row c
    LEFT JOIN LATERAL (
        SELECT w.created_at, w.id
        FROM horsies_workflows w
        WHERE c.cycle_upper_id IS NULL
          AND w.status = 'RUNNING'
        ORDER BY w.created_at DESC, w.id DESC
        LIMIT 1
    ) latest ON TRUE
),
scanned AS MATERIALIZED (
    SELECT page.created_at, page.id, page.name
    FROM cursor_row c
    CROSS JOIN upper_bound u
    CROSS JOIN LATERAL (
        SELECT bounded.created_at, bounded.id, bounded.name
        FROM (
            (
                SELECT w.created_at, w.id, w.name
                FROM horsies_workflows w
                WHERE c.last_id IS NULL
                  AND w.status = 'RUNNING'
                  AND u.id IS NOT NULL
                  AND (w.created_at, w.id) <= (u.created_at, u.id)
                ORDER BY w.created_at, w.id
                LIMIT CAST(:max_rows AS bigint)
            )
            UNION ALL
            (
                SELECT w.created_at, w.id, w.name
                FROM horsies_workflows w
                WHERE c.last_id IS NOT NULL
                  AND w.status = 'RUNNING'
                  AND u.id IS NOT NULL
                  AND (w.created_at, w.id)
                      > (c.last_created_at, c.last_id)
                  AND (w.created_at, w.id) <= (u.created_at, u.id)
                ORDER BY w.created_at, w.id
                LIMIT CAST(:max_rows AS bigint)
            )
        ) bounded
        ORDER BY bounded.created_at, bounded.id
        LIMIT CAST(:max_rows AS bigint)
    ) page
),
classified AS MATERIALIZED (
    SELECT s.created_at, s.id, s.name,
           any_task.found IS NOT NULL AS has_tasks,
           nonterminal_task.found IS NULL AS all_tasks_terminal
    FROM scanned s
    LEFT JOIN LATERAL (
        SELECT TRUE AS found
        FROM horsies_workflow_tasks wt
        WHERE wt.workflow_id = s.id
        LIMIT 1
    ) any_task ON TRUE
    LEFT JOIN LATERAL (
        SELECT TRUE AS found
        FROM horsies_workflow_tasks wt
        WHERE wt.workflow_id = s.id
          AND NOT (wt.status = ANY(:wf_task_terminal_states))
        LIMIT 1
    ) nonterminal_task ON TRUE
),
summary AS MATERIALIZED (
    SELECT count(*)::bigint AS scanned_count,
           COALESCE(
               array_agg(id ORDER BY created_at, id)
                   FILTER (WHERE has_tasks AND all_tasks_terminal),
               '{}'::uuid[]
           ) AS completion_ids,
           COALESCE(
               array_agg(id ORDER BY created_at, id)
                   FILTER (WHERE NOT has_tasks),
               '{}'::uuid[]
           ) AS orphan_ids,
           COALESCE(
               array_agg(name ORDER BY created_at, id)
                   FILTER (WHERE NOT has_tasks),
               '{}'::text[]
           ) AS orphan_names
    FROM classified
),
progress AS MATERIALIZED (
    SELECT s.scanned_count,
           last_row.created_at AS last_created_at,
           last_row.id AS last_id,
           s.scanned_count < CAST(:max_rows AS bigint)
               OR (last_row.created_at, last_row.id) = (u.created_at, u.id)
               AS cycle_complete
    FROM summary s
    CROSS JOIN upper_bound u
    LEFT JOIN LATERAL (
        SELECT scanned.created_at, scanned.id
        FROM scanned
        ORDER BY scanned.created_at DESC, scanned.id DESC
        LIMIT 1
    ) last_row ON TRUE
),
advance AS (
    UPDATE horsies_recovery_scan_cursors c
    SET last_created_at = CASE WHEN progress.cycle_complete THEN NULL
                               ELSE progress.last_created_at END,
        last_id = CASE WHEN progress.cycle_complete THEN NULL
                       ELSE progress.last_id END,
        cycle_upper_created_at = CASE WHEN progress.cycle_complete THEN NULL
                                      ELSE upper_bound.created_at END,
        cycle_upper_id = CASE WHEN progress.cycle_complete THEN NULL
                              ELSE upper_bound.id END,
        claim_token = CASE
            WHEN cardinality(summary.completion_ids)
               + cardinality(summary.orphan_ids) > 0
            THEN CAST(:claim_token AS uuid) ELSE NULL
        END,
        claim_expires_at = CASE
            WHEN cardinality(summary.completion_ids)
               + cardinality(summary.orphan_ids) > 0
            THEN statement_timestamp()
                + CAST(:claim_ttl_ms AS bigint) * interval '1 millisecond'
            ELSE NULL
        END,
        completed_cycles = completed_cycles
            + CASE WHEN progress.cycle_complete THEN 1 ELSE 0 END,
        last_scan_rows = summary.scanned_count::integer,
        last_candidate_rows = cardinality(summary.completion_ids)
            + cardinality(summary.orphan_ids),
        last_scan_at = statement_timestamp()
    FROM summary, progress, upper_bound
    WHERE c.scan_name = 'running_workflows'
      AND EXISTS (SELECT 1 FROM cursor_row)
    RETURNING c.claim_token
)
SELECT summary.scanned_count, summary.completion_ids,
       summary.orphan_ids, summary.orphan_names,
       (SELECT claim_token FROM advance) AS claim_token
FROM summary
WHERE EXISTS (SELECT 1 FROM advance)
""")

RENEW_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL = text("""
UPDATE horsies_recovery_scan_cursors
SET claim_expires_at = statement_timestamp()
        + CAST(:claim_ttl_ms AS bigint) * interval '1 millisecond'
WHERE scan_name = 'running_workflows'
  AND claim_token = CAST(:claim_token AS uuid)
  AND claim_expires_at > statement_timestamp()
RETURNING TRUE
""")

LOCK_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL = text("""
SELECT TRUE
FROM horsies_recovery_scan_cursors
WHERE scan_name = 'running_workflows'
  AND claim_token = CAST(:claim_token AS uuid)
  AND claim_expires_at > statement_timestamp()
FOR SHARE
""")

RELEASE_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL = text("""
UPDATE horsies_recovery_scan_cursors
SET claim_token = NULL, claim_expires_at = NULL
WHERE scan_name = 'running_workflows'
  AND claim_token = CAST(:claim_token AS uuid)
""")

FAIL_ORPHANED_WORKFLOW_SQL = text("""
UPDATE horsies_workflows
SET status = 'FAILED',
    error = :error,
    completed_at = NOW(),
    updated_at = NOW()
WHERE id = CAST(:workflow_id AS uuid)
  AND status = 'RUNNING'
""")

GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS = 300_000


@dataclass
class RecoveryCaseMetrics:
    """Bounded work and outcome counts for one recovery case."""

    rows_selected: int = 0
    candidates_returned: int = 0
    duration_ms: int = 0
    refusals: int = 0
    errors: int = 0


@dataclass
class RecoveryReport:
    """Health result for one global recovery pass."""

    recovered: int = 0
    errors: int = 0
    metrics: dict[str, RecoveryCaseMetrics] = field(default_factory=lambda: {
        name: RecoveryCaseMetrics()
        for name in ('case_0', 'case_1', 'case_1_5', 'case_1_6', 'case_2_3', 'case_4')
    })

    def health(self) -> dict[str, object]:
        return {
            'recovered': self.recovered,
            'errors': self.errors,
            'cases': {
                name: asdict(metrics)
                for name, metrics in self.metrics.items()
            },
        }


class RecoveryPassFailure(Exception):
    """A failed global pass with its partial health report."""

    def __init__(self, report: RecoveryReport, error: Exception) -> None:
        super().__init__(str(error))
        self.report = report
        self.error = error

    def health(self) -> dict[str, object]:
        snapshot = self.report.health()
        snapshot['state'] = 'error'
        snapshot['error'] = str(self.error)
        return snapshot


class _GlobalCandidateOutcome(Enum):
    APPLIED = 'applied'
    STATE_REFUSED = 'state_refused'
    CLAIM_LOST = 'claim_lost'
    ERROR = 'error'


async def _execute_candidate_query(
    session: 'AsyncSession',
    statement: 'TextClause',
    parameters: dict[str, object],
    metrics: RecoveryCaseMetrics | None,
    *,
    started: float,
) -> list[Any]:
    try:
        return list((await session.execute(statement, parameters)).fetchall())
    except Exception:
        if metrics is not None:
            metrics.duration_ms = int((time.monotonic() - started) * 1000)
            metrics.errors += 1
        raise

async def recover_stuck_workflows(
    session: 'AsyncSession',
    broker: 'PostgresBroker | None' = None,
    scope_workflow_ids: list[str] | None = None,
    *,
    _include_workflow_end_states: bool = True,
    _metrics: dict[str, RecoveryCaseMetrics] | None = None,
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
    global_scope = scope_ids is None

    from horsies.core.workflows.engine import get_dependency_results, try_make_ready_and_enqueue

    # Case 0: PENDING tasks with all dependencies terminal (race condition during parallel completion)
    # This happens when multiple dependencies complete concurrently and the PENDING→READY
    # transition is missed due to timing.
    # Delegates to try_make_ready_and_enqueue which handles all readiness logic:
    # join types (all/any/quorum), ctx_from gates, allow_failed_deps,
    # subworkflow routing, and dependent cascade.
    started = time.monotonic()
    pending_rows = await _execute_candidate_query(
        session,
        GLOBAL_GET_PENDING_WITH_TERMINAL_DEPS_SQL
        if global_scope
        else GET_PENDING_WITH_TERMINAL_DEPS_SQL,
        {'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES, 'scope_ids': scope_ids, 'max_rows': max_rows},
        _metrics['case_0'] if _metrics is not None else None,
        started=started,
    )
    if _metrics is not None:
        _metrics['case_0'].rows_selected = len(pending_rows)
        _metrics['case_0'].candidates_returned = len(pending_rows)

    for row in pending_rows:
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
            metrics=_metrics['case_0'] if _metrics is not None else None,
        ):
            recovered += 1
    if _metrics is not None:
        _metrics['case_0'].duration_ms = int(
            (time.monotonic() - started) * 1000
        )

    # Case 1: READY tasks not enqueued (task_id is NULL but status is READY)
    # This happens if worker crashed after marking READY but before creating task
    # Excludes SubWorkflowNodes (handled separately)
    started = time.monotonic()
    ready_rows = await _execute_candidate_query(
        session,
        GLOBAL_GET_READY_NOT_ENQUEUED_SQL
        if global_scope
        else GET_READY_NOT_ENQUEUED_SQL,
        {'scope_ids': scope_ids, 'max_rows': max_rows},
        _metrics['case_1'] if _metrics is not None else None,
        started=started,
    )
    if _metrics is not None:
        _metrics['case_1'].rows_selected = len(ready_rows)
        _metrics['case_1'].candidates_returned = len(ready_rows)

    for row in ready_rows:
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

        case_1_metrics = _metrics['case_1'] if _metrics is not None else None
        errors_before = (
            case_1_metrics.errors if case_1_metrics is not None else 0
        )
        if await _run_recovery_candidate(
            session,
            case='ready_not_enqueued',
            workflow_id=workflow_id,
            task_index=task_index,
            action=_recover_ready_not_enqueued,
            metrics=case_1_metrics,
        ):
            recovered += 1
        elif case_1_metrics is not None and case_1_metrics.errors == errors_before:
            case_1_metrics.refusals += 1
    if _metrics is not None:
        _metrics['case_1'].duration_ms = int(
            (time.monotonic() - started) * 1000
        )

    # Case 1.5: READY SubWorkflowNodes not started (sub_workflow_id is NULL)
    # This happens if worker crashed after marking READY but before starting child workflow
    # NOTE: This requires broker to start the child workflow, so we just mark them for retry
    started = time.monotonic()
    subworkflow_rows = await _execute_candidate_query(
        session,
        GLOBAL_GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL
        if global_scope
        else GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL,
        {'scope_ids': scope_ids, 'max_rows': max_rows},
        _metrics['case_1_5'] if _metrics is not None else None,
        started=started,
    )
    if _metrics is not None:
        _metrics['case_1_5'].rows_selected = len(subworkflow_rows)
        _metrics['case_1_5'].candidates_returned = len(subworkflow_rows)

    for row in subworkflow_rows:
        workflow_id = row.workflow_id
        task_index = row.task_index
        dependencies = row.dependencies
        depth = row.depth or 0
        root_wf_id = row.root_workflow_id or workflow_id

        if broker is None:
            if _metrics is not None:
                _metrics['case_1_5'].refusals += 1
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

            dep_indices: list[int] = dependencies
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
            metrics=_metrics['case_1_5'] if _metrics is not None else None,
        ):
            recovered += 1
    if _metrics is not None:
        _metrics['case_1_5'].duration_ms = int(
            (time.monotonic() - started) * 1000
        )

    # Case 1.6: Child workflows completed but parent node not updated
    # This happens if the on_subworkflow_complete callback failed or was interrupted
    started = time.monotonic()
    child_rows = await _execute_candidate_query(
        session,
        GLOBAL_GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL
        if global_scope
        else GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL,
        {'scope_ids': scope_ids, 'max_rows': max_rows},
        _metrics['case_1_6'] if _metrics is not None else None,
        started=started,
    )
    if _metrics is not None:
        _metrics['case_1_6'].rows_selected = len(child_rows)
        _metrics['case_1_6'].candidates_returned = len(child_rows)

    for row in child_rows:
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
            metrics=_metrics['case_1_6'] if _metrics is not None else None,
        ):
            recovered += 1
    if _metrics is not None:
        _metrics['case_1_6'].duration_ms = int(
            (time.monotonic() - started) * 1000
        )

    # Case 2+3: Workflows with all tasks terminal but workflow still RUNNING
    # This handles both completed and failed workflows, respecting success_policy.
    # This happens if worker crashed after completing last task but before updating workflow
    if _include_workflow_end_states:
        terminal_candidates = await session.execute(
            GLOBAL_GET_TERMINAL_WORKFLOW_CANDIDATES_SQL
            if global_scope
            else GET_TERMINAL_WORKFLOW_CANDIDATES_SQL,
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

        orphan_candidates = await session.execute(
            GLOBAL_GET_ORPHANED_WORKFLOW_CANDIDATES_SQL
            if global_scope
            else GET_ORPHANED_WORKFLOW_CANDIDATES_SQL,
            {'scope_ids': scope_ids, 'max_rows': max_rows},
        )
        for row in orphan_candidates.fetchall():
            workflow_id = row.id
            error = json.dumps({
                'error_code': 'E400',
                'message': (
                    f"Orphaned workflow '{row.name}': no workflow_tasks found. "
                    'Workflow task insertion did not complete.'
                ),
                'recovery': 'case_4',
            })
            result = await session.execute(
                FAIL_ORPHANED_WORKFLOW_SQL,
                {'workflow_id': workflow_id, 'error': error},
            )
            if int(getattr(result, 'rowcount', 0)) == 1:
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


async def recover_stuck_workflow_tree(
    session: 'AsyncSession',
    broker: 'PostgresBroker | None',
    workflow_ids: list[str],
) -> int:
    """Recover one explicit workflow tree in the caller transaction."""
    return await recover_stuck_workflows(
        session,
        broker,
        scope_workflow_ids=workflow_ids,
    )


async def _renew_global_claim(
    session_factory: 'async_sessionmaker[AsyncSession]',
    claim_token: str,
) -> bool:
    async with session_factory() as session:
        result = await session.execute(
            RENEW_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL,
            {
                'claim_token': claim_token,
                'claim_ttl_ms': GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS,
            },
        )
        renewed = result.scalar_one_or_none() is True
        await session.commit()
        return renewed


async def _release_global_claim(
    session_factory: 'async_sessionmaker[AsyncSession]',
    claim_token: str,
) -> None:
    async with session_factory() as session:
        await session.execute(
            RELEASE_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL,
            {'claim_token': claim_token},
        )
        await session.commit()


async def _lock_global_claim(
    session: 'AsyncSession',
    claim_token: str,
) -> bool:
    result = await session.execute(
        LOCK_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL,
        {'claim_token': claim_token},
    )
    return result.scalar_one_or_none() is True


async def _recover_global_completion_candidate(
    session_factory: 'async_sessionmaker[AsyncSession]',
    broker: 'PostgresBroker | None',
    workflow_id: str,
    claim_token: str,
) -> _GlobalCandidateOutcome:
    try:
        async with session_factory() as session:
            if not await _lock_global_claim(session, claim_token):
                await session.rollback()
                return _GlobalCandidateOutcome.CLAIM_LOST

            async def _complete() -> bool:
                from horsies.core.workflows.engine import (
                    check_workflow_completion,
                )

                await check_workflow_completion(session, workflow_id, broker)
                return True

            applied = await _run_recovery_candidate(
                session,
                case='terminal_workflow_running',
                workflow_id=workflow_id,
                action=_complete,
            )
            if not applied:
                await session.rollback()
                return _GlobalCandidateOutcome.ERROR

            from horsies.core.workflows.engine import (
                drain_parent_propagations_in_session,
                on_subworkflow_complete,
            )

            async def _propagate(child_id: str) -> None:
                await on_subworkflow_complete(session, child_id, broker)

            await drain_parent_propagations_in_session(
                session,
                broker,
                run_item=_propagate,
            )
            await session.commit()
            return _GlobalCandidateOutcome.APPLIED
    except Exception as error:
        logger.error(
            'Global workflow completion recovery failed: '
            f'workflow_id={workflow_id} error={error}'
        )
        return _GlobalCandidateOutcome.ERROR


async def _recover_global_orphan_candidate(
    session_factory: 'async_sessionmaker[AsyncSession]',
    workflow_id: str,
    workflow_name: str,
    claim_token: str,
) -> _GlobalCandidateOutcome:
    error = json.dumps({
        'error_code': 'E400',
        'message': (
            f"Orphaned workflow '{workflow_name}': no workflow_tasks found. "
            'Workflow task insertion did not complete.'
        ),
        'recovery': 'case_4',
    })
    try:
        async with session_factory() as session:
            if not await _lock_global_claim(session, claim_token):
                await session.rollback()
                return _GlobalCandidateOutcome.CLAIM_LOST
            result = await session.execute(
                FAIL_ORPHANED_WORKFLOW_SQL,
                {'workflow_id': workflow_id, 'error': error},
            )
            await session.commit()
            if int(getattr(result, 'rowcount', 0)) == 1:
                return _GlobalCandidateOutcome.APPLIED
            return _GlobalCandidateOutcome.STATE_REFUSED
    except Exception as failure:
        logger.error(
            'Global orphan workflow recovery failed: '
            f'workflow_id={workflow_id} error={failure}'
        )
        return _GlobalCandidateOutcome.ERROR


async def _recover_stuck_workflows_global(
    session_factory: 'async_sessionmaker[AsyncSession]',
    broker: 'PostgresBroker | None',
    report: RecoveryReport,
) -> RecoveryReport:
    async with session_factory() as session:
        report.recovered += await recover_stuck_workflows(
            session,
            broker,
            _include_workflow_end_states=False,
            _metrics=report.metrics,
        )
        await session.commit()

    started = time.monotonic()
    requested_token = str(uuid.uuid4())
    try:
        async with session_factory() as session:
            result = await session.execute(
                GLOBAL_WORKFLOW_AUDIT_SQL,
                {
                    'max_rows': GLOBAL_SCAN_ROW_CAP,
                    'claim_token': requested_token,
                    'claim_ttl_ms': GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS,
                    'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
                },
            )
            audit = result.one_or_none()
            await session.commit()
    except Exception:
        duration_ms = int((time.monotonic() - started) * 1000)
        report.metrics['case_2_3'].duration_ms = duration_ms
        report.metrics['case_2_3'].errors += 1
        report.metrics['case_4'].duration_ms = duration_ms
        report.metrics['case_4'].errors += 1
        raise

    duration_ms = int((time.monotonic() - started) * 1000)
    case_2_3 = report.metrics['case_2_3']
    case_4 = report.metrics['case_4']
    case_2_3.duration_ms = duration_ms
    case_4.duration_ms = duration_ms
    if audit is None:
        case_2_3.refusals += 1
        case_4.refusals += 1
        return report

    completion_ids = [str(value) for value in audit.completion_ids]
    orphan_ids = [str(value) for value in audit.orphan_ids]
    orphan_names = [str(value) for value in audit.orphan_names]
    scanned_count = int(audit.scanned_count)
    case_2_3.rows_selected = scanned_count
    case_2_3.candidates_returned = len(completion_ids)
    case_4.rows_selected = scanned_count
    case_4.candidates_returned = len(orphan_ids)
    claim_token = str(audit.claim_token) if audit.claim_token is not None else None
    if claim_token is None:
        if completion_ids:
            case_2_3.refusals += 1
        if orphan_ids:
            case_4.refusals += 1
        return report

    owns_claim = True
    try:
        for workflow_id in completion_ids:
            try:
                renewed = await _renew_global_claim(
                    session_factory,
                    claim_token,
                )
            except Exception as error:
                logger.error(f'Workflow audit claim renewal failed: {error}')
                report.errors += 1
                case_2_3.errors += 1
                owns_claim = False
                break
            if not renewed:
                case_2_3.refusals += 1
                owns_claim = False
                break
            outcome = await _recover_global_completion_candidate(
                session_factory,
                broker,
                workflow_id,
                claim_token,
            )
            match outcome:
                case _GlobalCandidateOutcome.APPLIED:
                    report.recovered += 1
                case _GlobalCandidateOutcome.STATE_REFUSED:
                    case_2_3.refusals += 1
                case _GlobalCandidateOutcome.CLAIM_LOST:
                    case_2_3.refusals += 1
                    owns_claim = False
                    break
                case _GlobalCandidateOutcome.ERROR:
                    case_2_3.errors += 1
                    report.errors += 1
                case _ as unreachable:
                    assert_never(unreachable)

        if owns_claim:
            for workflow_id, workflow_name in zip(
                orphan_ids,
                orphan_names,
                strict=True,
            ):
                try:
                    renewed = await _renew_global_claim(
                        session_factory,
                        claim_token,
                    )
                except Exception as error:
                    logger.error(
                        f'Workflow audit claim renewal failed: {error}'
                    )
                    report.errors += 1
                    case_4.errors += 1
                    owns_claim = False
                    break
                if not renewed:
                    case_4.refusals += 1
                    owns_claim = False
                    break
                outcome = await _recover_global_orphan_candidate(
                    session_factory,
                    workflow_id,
                    workflow_name,
                    claim_token,
                )
                match outcome:
                    case _GlobalCandidateOutcome.APPLIED:
                        report.recovered += 1
                    case _GlobalCandidateOutcome.STATE_REFUSED:
                        case_4.refusals += 1
                    case _GlobalCandidateOutcome.CLAIM_LOST:
                        case_4.refusals += 1
                        owns_claim = False
                        break
                    case _GlobalCandidateOutcome.ERROR:
                        case_4.errors += 1
                        report.errors += 1
                    case _ as unreachable:
                        assert_never(unreachable)
    finally:
        if owns_claim:
            try:
                await _release_global_claim(session_factory, claim_token)
            except Exception as error:
                logger.error(f'Workflow audit claim release failed: {error}')
                report.errors += 1
                case_2_3.errors += 1
                case_4.errors += 1

    duration_ms = int((time.monotonic() - started) * 1000)
    case_2_3.duration_ms = duration_ms
    case_4.duration_ms = duration_ms
    return report


async def recover_stuck_workflows_global(
    session_factory: 'async_sessionmaker[AsyncSession]',
    broker: 'PostgresBroker | None' = None,
) -> RecoveryReport:
    """Run one bounded global pass and preserve partial failure metrics."""
    report = RecoveryReport()
    try:
        return await _recover_stuck_workflows_global(
            session_factory,
            broker,
            report,
        )
    except Exception as error:
        report.errors += 1
        raise RecoveryPassFailure(report, error) from error
