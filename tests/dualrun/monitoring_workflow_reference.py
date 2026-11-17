# pyright: reportPrivateUsage=false
"""Reference of the dual-run of the workflow and schedule reads: the client statements.

``list_workflow_names``, ``list_workflow_runs``, ``get_workflow_run`` and
``list_schedules`` as they ran before they called
``horsies_monitoring_workflow_names``, ``horsies_monitoring_workflow_runs``,
``horsies_monitoring_workflow_run`` and ``horsies_monitoring_schedules``:
each in a session, with the child counts of a run as a second statement.
The decided changes of these reads are applied (runs and schedules of one
time ordered by id and by name, the 10,000-row bound of the names and
schedules lists, the last task id of a schedule as ``uuid.UUID``), so the
reference and the functions answer the same.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import ColumnElement, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import InstrumentedAttribute, load_only

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_pg import ScheduleStateModel
from horsies.core.models.workflow_pg import WorkflowModel, WorkflowTaskModel
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import uuid_column_value
from horsies.monitoring.errors import MonitoringResult
from horsies.monitoring.models import (
    ScheduleStateInfo,
    WorkflowEdge,
    WorkflowNodeInfo,
    WorkflowRunDetail,
    WorkflowRunSummary,
)
from horsies.monitoring.pages import WorkflowRunsLimit
from tests.dualrun.reference.monitoring_queries import (
    LIST_ROW_BOUND,
    _db_err,
    _is_uuid_text,
    _loop_limit_err,
    _node_exec_s,
    elapsed_s,
    logger,
)


def _run_summary(run: WorkflowModel) -> WorkflowRunSummary:
    """Map a workflow row to its summary shape."""
    return WorkflowRunSummary(
        id=run.id,
        name=run.name,
        definition_key=run.definition_key,
        status=run.status,
        created_at=run.created_at,
        completed_at=run.completed_at,
        wall_s=elapsed_s(run.created_at, run.completed_at),
    )


# The node columns each read maps. The payload columns (input, result) stay
# unfetched; ``raiseload=True`` turns an access to any other column into an
# immediate error.
_RUN_NODE_COLUMNS: tuple[InstrumentedAttribute[Any], ...] = (
    WorkflowTaskModel.id,
    WorkflowTaskModel.task_index,
    WorkflowTaskModel.node_id,
    WorkflowTaskModel.task_name,
    WorkflowTaskModel.status,
    WorkflowTaskModel.dependencies,
    WorkflowTaskModel.allow_failed_deps,
    WorkflowTaskModel.is_subworkflow,
    WorkflowTaskModel.sub_workflow_id,
    WorkflowTaskModel.started_at,
    WorkflowTaskModel.completed_at,
)


async def list_workflow_names(
    broker: PostgresBroker,
) -> MonitoringResult[list[str]]:
    """Distinct names of root workflow runs, for the run-picker filter.

    At most ``LIST_ROW_BOUND`` names, in the database collation; a longer
    list is cut, and the cut is logged as a warning.
    """
    stmt = (
        select(WorkflowModel.name)
        .where(WorkflowModel.parent_workflow_id.is_(None))
        .distinct()
        .order_by(WorkflowModel.name)
        .limit(LIST_ROW_BOUND + 1)
    )

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('workflow names query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            names = (await session.execute(stmt)).scalars().all()
    except SQLAlchemyError as exc:
        return _db_err('workflow names query', exc)

    if len(names) > LIST_ROW_BOUND:
        _log_cut_list('workflow names', 'root workflow names')
    return Ok(list(names[:LIST_ROW_BOUND]))


def _log_cut_list(read: str, rows: str) -> None:
    """Warn that a list read returned ``LIST_ROW_BOUND`` rows and more exist."""
    logger.warning(
        '%s: the list is cut at %d %s; more exist and are not returned',
        read,
        LIST_ROW_BOUND,
        rows,
    )


async def list_workflow_runs(
    broker: PostgresBroker,
    *,
    name: str | None,
    status: str | None,
    limit: WorkflowRunsLimit,
) -> MonitoringResult[list[WorkflowRunSummary]]:
    """Recent root workflow runs, newest first.

    Runs created at one time come by id, highest first, so a page does not
    reorder between polls. ``name`` and ``status`` are independent
    exact-match filters combined with AND; a status no run carries yields an
    empty list.
    """
    conditions: list[ColumnElement[bool]] = [WorkflowModel.parent_workflow_id.is_(None)]
    if name is not None:
        conditions.append(WorkflowModel.name == name)
    if status is not None:
        conditions.append(WorkflowModel.status == status)
    stmt = (
        select(WorkflowModel)
        .where(*conditions)
        .order_by(WorkflowModel.created_at.desc(), WorkflowModel.id.desc())
        .limit(limit.value)
    )

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('workflow runs query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            runs = (await session.execute(stmt)).scalars().all()
    except SQLAlchemyError as exc:
        return _db_err('workflow runs query', exc)

    return Ok([_run_summary(run) for run in runs])


async def get_workflow_run(
    broker: PostgresBroker,
    workflow_id: str,
) -> MonitoringResult[WorkflowRunDetail | None]:
    """A run's DAG — metadata, nodes, and dependency edges — or None if absent.

    Works for any run id, root or subworkflow, so a subworkflow node is
    drilled into by calling this again with that node's ``sub_workflow_id``.
    Edges pointing at an index the run does not have are dropped.
    """
    if not _is_uuid_text(workflow_id):
        return Ok(None)
    run_stmt = select(WorkflowModel).where(WorkflowModel.id == workflow_id)
    nodes_stmt = (
        select(WorkflowTaskModel)
        .options(load_only(*_RUN_NODE_COLUMNS, raiseload=True))
        .where(WorkflowTaskModel.workflow_id == workflow_id)
        .order_by(WorkflowTaskModel.task_index)
    )

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('workflow run detail query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            run = (await session.execute(run_stmt)).scalar_one_or_none()
            if run is None:
                return Ok(None)
            node_rows = (await session.execute(nodes_stmt)).scalars().all()

            # Direct-child status rollup for subworkflow nodes: one grouped
            # count over the child runs' node rows, keyed by child run id.
            sub_ids = [
                row.sub_workflow_id
                for row in node_rows
                if row.is_subworkflow and row.sub_workflow_id is not None
            ]
            child_rollup: dict[str, tuple[int, int]] = {}
            if sub_ids:
                rollup_stmt = (
                    select(
                        WorkflowTaskModel.workflow_id,
                        func.count(),
                        func.count().filter(WorkflowTaskModel.status == 'FAILED'),
                    )
                    .where(WorkflowTaskModel.workflow_id.in_(sub_ids))
                    .group_by(WorkflowTaskModel.workflow_id)
                )
                child_rollup = {
                    child_id: (total, failed)
                    for child_id, total, failed in (
                        (await session.execute(rollup_stmt)).tuples().all()
                    )
                }
    except SQLAlchemyError as exc:
        return _db_err('workflow run detail query', exc)

    known_indices = {row.task_index for row in node_rows}
    nodes: list[WorkflowNodeInfo] = []
    edges: list[WorkflowEdge] = []
    for row in node_rows:
        counts = (
            child_rollup.get(row.sub_workflow_id)
            if row.is_subworkflow and row.sub_workflow_id is not None
            else None
        )
        child_total, child_failed = counts if counts is not None else (None, None)
        nodes.append(
            WorkflowNodeInfo(
                task_index=row.task_index,
                node_id=row.node_id,
                task_name=row.task_name,
                node_status=row.status,
                is_subworkflow=row.is_subworkflow,
                sub_workflow_id=row.sub_workflow_id,
                allow_failed_deps=row.allow_failed_deps,
                started_at=row.started_at,
                completed_at=row.completed_at,
                exec_s=_node_exec_s(row.status, row.started_at, row.completed_at),
                child_total=child_total,
                child_failed=child_failed,
            )
        )
        for dep_index in row.dependencies:
            if dep_index in known_indices:
                edges.append(
                    WorkflowEdge(from_index=dep_index, to_index=row.task_index)
                )

    failed_indices = [node.task_index for node in nodes if node.node_status == 'FAILED']
    return Ok(
        WorkflowRunDetail(
            run=_run_summary(run),
            nodes=nodes,
            edges=edges,
            failed_count=len(failed_indices),
            failed_indices=failed_indices,
        )
    )


async def list_schedules(
    broker: PostgresBroker,
) -> MonitoringResult[list[ScheduleStateInfo]]:
    """Recurring schedule states, soonest next-run first, then by name.

    Schedules due at one time come by name, so the list does not reorder
    between polls. At most ``LIST_ROW_BOUND`` schedules; a longer list is
    cut, and the cut is logged as a warning.
    """
    stmt = (
        select(ScheduleStateModel)
        .order_by(
            ScheduleStateModel.next_run_at.asc(), ScheduleStateModel.schedule_name.asc()
        )
        .limit(LIST_ROW_BOUND + 1)
    )

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('schedule state query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            rows = (await session.execute(stmt)).scalars().all()
    except SQLAlchemyError as exc:
        return _db_err('schedule state query', exc)

    if len(rows) > LIST_ROW_BOUND:
        _log_cut_list('schedule state', 'schedules')
    return Ok(
        [
            ScheduleStateInfo(
                schedule_name=row.schedule_name,
                last_run_at=row.last_run_at,
                next_run_at=row.next_run_at,
                last_task_id=(
                    uuid_column_value(
                        row.last_task_id,
                        column='horsies_schedule_state.last_task_id',
                    )
                    if row.last_task_id is not None
                    else None
                ),
                run_count=row.run_count,
                updated_at=row.updated_at,
            )
            for row in rows[:LIST_ROW_BOUND]
        ]
    )

