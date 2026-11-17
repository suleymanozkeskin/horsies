# pyright: reportPrivateUsage=false
"""Reference of the dual-run of the detail reads: the client statements.

``get_task_detail`` and ``get_workflow_node`` as they ran before they
called ``horsies_monitoring_task_detail`` and
``horsies_monitoring_workflow_node``: a REPEATABLE READ session with the
live read, the staged history read, the absence rule, the attempt rows and
the workflow link. The decided changes of these reads are applied (the
absence reason from the detached leaves, the node error from the stored
result, the task id as ``uuid.UUID``), so the reference and the functions
answer the same.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, assert_never

from sqlalchemy import Select, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute, load_only

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.archive.attempts import (
    AttemptRecord as ArchiveAttemptRecord,
)
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.detail import (
    HistoryTaskDetail,
    LiveTaskLocation,
    TaskDetailAbsent,
    read_task_detail,
)
from horsies.core.models.task_pg import TaskAttemptModel, TaskModel
from horsies.core.models.tasks import Execution
from horsies.core.models.workflow_pg import WorkflowTaskModel
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import uuid_column_value
from horsies.monitoring.errors import MonitoringResult
from horsies.monitoring.models import (
    LeafTaskInfo,
    TaskAbsenceReason,
    TaskAttemptInfo,
    TaskDetail,
    TaskDetailNotFound,
    WorkflowTaskDetail,
)
from tests.dualrun.reference.monitoring_queries import (
    _category_value,
    _contract_err,
    _db_err,
    _is_uuid_text,
    _loop_limit_err,
    _node_error,
    nz,
    span_s,
)
from tests.dualrun.monitoring_list_reference import _task_spans


# The exact column set ``_leaf_task`` reads (``_SUMMARY_COLUMNS`` of the list reference).
_LEAF_COLUMNS: tuple[InstrumentedAttribute[Any], ...] = (
    TaskModel.id,
    TaskModel.task_name,
    TaskModel.queue_name,
    TaskModel.priority,
    TaskModel.status,
    TaskModel.retry_count,
    TaskModel.max_retries,
    TaskModel.is_workflow_task,
    TaskModel.worker_hostname,
    TaskModel.enqueued_at,
    TaskModel.started_at,
    TaskModel.good_until,
    TaskModel.execution,
)


def _live_task_statement(task_id: uuid.UUID) -> Select[tuple[TaskModel]]:
    """The live row of one task, with the leaf columns only."""
    return (
        select(TaskModel)
        .options(load_only(*_LEAF_COLUMNS, raiseload=True))
        .where(TaskModel.id == str(task_id))
    )


def _leaf_task(task: TaskModel) -> LeafTaskInfo:
    """Map a live task row to the detail shape shared by tasks and workflow nodes.

    A live row has no error code, failed reason or end time, so these read
    None.
    """
    queue_s, exec_s = _task_spans(task)
    return LeafTaskInfo(
        task_id=uuid_column_value(task.id, column='horsies_tasks.id'),
        status=task.status.value,
        error_code=None,
        failed_reason=None,
        retry_count=task.retry_count,
        max_retries=task.max_retries,
        enqueued_at=task.enqueued_at,
        started_at=task.started_at,
        completed_at=None,
        failed_at=None,
        queue_s=queue_s,
        exec_s=exec_s,
        worker_hostname=task.worker_hostname,
        good_until=task.good_until,
        execution=Execution(task.execution),
    )


type _AttemptRow = tuple[
    int,
    str,
    bool,
    str | None,
    str | None,
    str | None,
    str,
    datetime,
    datetime,
]
"""``attempt, outcome, will_retry, error_code, error_message, failed_reason,
worker_hostname, started_at, finished_at`` of one attempt row."""


def _attempts_statement(task_id: uuid.UUID) -> Select[_AttemptRow]:
    """The attempt rows of one task, oldest first, with the read columns only."""
    return (
        select(
            TaskAttemptModel.attempt,
            TaskAttemptModel.outcome,
            TaskAttemptModel.will_retry,
            TaskAttemptModel.error_code,
            TaskAttemptModel.error_message,
            TaskAttemptModel.failed_reason,
            TaskAttemptModel.worker_hostname,
            TaskAttemptModel.started_at,
            TaskAttemptModel.finished_at,
        )
        .where(TaskAttemptModel.task_id == str(task_id))
        .order_by(TaskAttemptModel.attempt)
    )


def _attempt(row: _AttemptRow) -> TaskAttemptInfo:
    """Map an attempt row to its response shape."""
    (
        attempt,
        outcome,
        will_retry,
        error_code,
        error_message,
        failed_reason,
        worker_hostname,
        started_at,
        finished_at,
    ) = row
    return TaskAttemptInfo(
        attempt=attempt,
        outcome=outcome,
        will_retry=will_retry,
        error_code=nz(error_code),
        error_message=nz(error_message),
        failed_reason=nz(failed_reason),
        worker_hostname=worker_hostname,
        started_at=started_at,
        finished_at=finished_at,
    )


def _history_attempt(record: 'ArchiveAttemptRecord') -> TaskAttemptInfo:
    """Map one snapshot-preserved attempt to the same response shape."""
    return TaskAttemptInfo(
        attempt=record.attempt,
        outcome=record.outcome,
        will_retry=record.will_retry,
        error_code=nz(record.error_code),
        error_message=nz(record.error_message),
        failed_reason=nz(record.failed_reason),
        worker_hostname=record.worker_hostname,
        started_at=record.started_at,
        finished_at=record.finished_at,
    )


def _history_leaf(detail: 'HistoryTaskDetail') -> LeafTaskInfo:
    """Map a history detail to the leaf shape the routes already serve.

    The terminal instant is authoritative: it lands in ``completed_at``
    for COMPLETED and in ``failed_at`` for every other terminal status,
    which is strictly more end-stamp information than the pre-history
    live rows carried for cancelled and expired tasks.
    """
    completed_at = (
        detail.terminal_at if detail.status is TaskStatus.COMPLETED else None
    )
    failed_at = (
        detail.terminal_at
        if detail.status is not TaskStatus.COMPLETED
        else None
    )
    end = detail.terminal_at
    queue_s = span_s(
        detail.enqueued_at, detail.started_at or end, live=False
    )
    exec_s = span_s(detail.started_at, end, live=False)
    return LeafTaskInfo(
        task_id=detail.task_id,
        status=detail.status.value,
        error_code=nz(detail.error_code),
        failed_reason=nz(detail.final_failed_reason),
        retry_count=detail.retry_count,
        max_retries=detail.max_retries,
        enqueued_at=detail.enqueued_at,
        started_at=detail.started_at,
        completed_at=completed_at,
        failed_at=failed_at,
        queue_s=queue_s,
        exec_s=exec_s,
        worker_hostname=detail.last_worker_hostname,
        good_until=detail.good_until,
        execution=None,
    )


async def get_task_detail(
    broker: PostgresBroker,
    task_id: uuid.UUID,
) -> MonitoringResult[TaskDetail | TaskDetailNotFound]:
    """A single task with its attempt history.

    The attempt history is where a task's cause lives: ``error_message``
    carries the unhandled exception per try, including retries.

    Outcomes:
        - ``Ok(TaskDetail)``: the live row, or the history row when the live
          table has no row.
        - ``Ok(TaskDetailNotFound)``: no table holds the id. ``reason`` says
          whether retention can explain it.
        - ``Err(MonitoringQueryHistoryContractViolation)``: the history row
          does not decode, or the staged read reports the task live in the
          snapshot where the live read found no row.

    Every read runs in one REPEATABLE READ snapshot.
    """
    task_stmt = _live_task_statement(task_id)
    attempts_stmt = _attempts_statement(task_id)
    # One indexed hop to the node this task backs, if any. Kept as its own
    # statement rather than a join onto the task row: a standalone task is
    # the common case and this returns nothing for it.
    node_stmt = select(
        WorkflowTaskModel.workflow_id, WorkflowTaskModel.task_index
    ).where(WorkflowTaskModel.task_id == str(task_id))

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('task detail query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            await _begin_one_snapshot(session)
            task = (await session.execute(task_stmt)).scalar_one_or_none()
            if task is None:
                match await _history_detail(session, task_id):
                    case TaskAbsenceReason() as reason:
                        return Ok(TaskDetailNotFound(
                            task_id=task_id, reason=reason,
                        ))
                    case HistoryTaskDetail() as history:
                        pass
                node = (await session.execute(node_stmt)).tuples().first()
                workflow_id, workflow_task_index = (
                    node if node is not None else (None, None)
                )
                return Ok(
                    TaskDetail(
                        leaf=_history_leaf(history),
                        task_name=history.task_name,
                        queue_name=history.queue_name,
                        priority=history.priority,
                        is_workflow_task=history.is_workflow_task,
                        error_category=_category_value(history.error_code),
                        attempts=[
                            _history_attempt(record)
                            for record in history.attempts
                        ],
                        workflow_id=workflow_id,
                        workflow_task_index=workflow_task_index,
                    )
                )
            attempt_rows = (await session.execute(attempts_stmt)).tuples().all()
            node = (await session.execute(node_stmt)).tuples().first()
    except SQLAlchemyError as exc:
        return _db_err('task detail query', exc)
    except HistoryContractError as exc:
        return _contract_err('task detail history read', exc)

    workflow_id, workflow_task_index = node if node is not None else (None, None)
    return Ok(
        TaskDetail(
            leaf=_leaf_task(task),
            task_name=task.task_name,
            queue_name=task.queue_name,
            priority=task.priority,
            is_workflow_task=task.is_workflow_task,
            # A live row has no error code.
            error_category=None,
            attempts=[_attempt(row) for row in attempt_rows],
            workflow_id=workflow_id,
            workflow_task_index=workflow_task_index,
        )
    )


async def _begin_one_snapshot(session: AsyncSession) -> None:
    """Begin the session's transaction in REPEATABLE READ.

    Call it before the first statement. Every later statement of the
    session sees the snapshot of the first one, so the live read and the
    staged history read agree on where a task row is. A read-only
    REPEATABLE READ transaction gets no serialization failure. The
    isolation level resets when the connection goes back to the pool.
    """
    _ = await session.connection(
        execution_options={'isolation_level': 'REPEATABLE READ'},
    )


# The one rule of absence (#668), in the snapshot of the detail read: an id
# that is not a UUIDv7 is not classified; an id born before the latest upper
# anchor of the detached leaves of PERIOD classes, plus the 5 s clock
# allowance of the history readers, predates retained history (retention can
# have removed it); any other id is within retained history.
_ABSENCE_REASON_SQL = text("""
    SELECT CASE
               WHEN birth.at IS NULL THEN 'NOT_CLASSIFIED'
               WHEN detached.upper_anchor IS NOT NULL
                    AND birth.at < detached.upper_anchor + interval '5 seconds'
                   THEN 'PREDATES_RETAINED_HISTORY'
               ELSE 'WITHIN_RETAINED_HISTORY'
           END
    FROM (SELECT horsies_uuid7_birth(CAST(:task_id AS uuid)) AS at) AS birth
    CROSS JOIN (
        SELECT max(leaf.upper_anchor) AS upper_anchor
        FROM horsies_task_history_leaf_catalog AS leaf
        JOIN horsies_retention_classes AS class ON class.class_key = leaf.class_key
        WHERE class.class_kind = 'PERIOD' AND leaf.detached_at IS NOT NULL
    ) AS detached
""")


async def _history_detail(
    session: AsyncSession, task_id: uuid.UUID
) -> HistoryTaskDetail | TaskAbsenceReason:
    """The history side of a detail read, after the live read found no row.

    Terminalization moves finished rows out of the live table, so an
    absent live row is the normal terminal case, not an error. A task in
    neither place gets its reason from the detached leaves
    (``_ABSENCE_REASON_SQL``). The session reads in one snapshot
    (``_begin_one_snapshot``), so the staged read cannot find the task live.

    Raises:
        HistoryContractError: the history row does not decode, or the
            staged read reports the task live in the snapshot where the
            live read found no row.
    """
    connection = await session.connection()
    detail = await read_task_detail(connection, task_id=task_id)
    match detail:
        case HistoryTaskDetail():
            return detail
        case TaskDetailAbsent():
            reason = (
                await connection.execute(
                    _ABSENCE_REASON_SQL, {'task_id': str(task_id)},
                )
            ).scalar_one()
            return TaskAbsenceReason(reason)
        case LiveTaskLocation():
            raise HistoryContractError(
                f'staged detail reports task {task_id} live in the snapshot '
                'where the live read found no row'
            )
        case _ as unreachable:
            assert_never(unreachable)


_NODE_DETAIL_COLUMNS: tuple[InstrumentedAttribute[Any], ...] = (
    WorkflowTaskModel.id,
    WorkflowTaskModel.task_index,
    WorkflowTaskModel.node_id,
    WorkflowTaskModel.task_name,
    WorkflowTaskModel.status,
    WorkflowTaskModel.is_subworkflow,
    WorkflowTaskModel.task_id,
    WorkflowTaskModel.result,
)


async def get_workflow_node(
    broker: PostgresBroker,
    workflow_id: str,
    task_index: int,
) -> MonitoringResult[WorkflowTaskDetail | None]:
    """Detail for one node: node error, backing task, and attempt history.

    ``node_error`` is the message of the error that the node's stored
    result holds; a stored result that does not decode returns
    ``MonitoringQueryHistoryContractViolation``.

    Returns None when the run has no node at that index. Subworkflow nodes
    have no backing task; drill into the child run instead. ``leaf`` is also
    None when the backing task row has been removed by retention, while its
    attempt rows are still reported if present. A history row of the
    backing task that does not decode returns
    ``MonitoringQueryHistoryContractViolation``. Every read runs in one
    REPEATABLE READ snapshot.
    """
    if not _is_uuid_text(workflow_id):
        return Ok(None)
    node_stmt = (
        select(WorkflowTaskModel)
        .options(load_only(*_NODE_DETAIL_COLUMNS, raiseload=True))
        .where(
            WorkflowTaskModel.workflow_id == workflow_id,
            WorkflowTaskModel.task_index == task_index,
        )
    )

    match await broker.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('workflow node detail query', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            await _begin_one_snapshot(session)
            node = (await session.execute(node_stmt)).scalar_one_or_none()
            if node is None:
                return Ok(None)
            node_error = _node_error(node.task_index, node.result)

            task: TaskModel | None = None
            history: HistoryTaskDetail | None = None
            attempt_rows: list[_AttemptRow] = []
            if node.task_id is not None:
                node_task_id = uuid_column_value(
                    node.task_id, column='horsies_workflow_tasks.task_id',
                )
                task = (
                    await session.execute(_live_task_statement(node_task_id))
                ).scalar_one_or_none()
                if task is None:
                    match await _history_detail(session, node_task_id):
                        case HistoryTaskDetail() as found:
                            history = found
                        case TaskAbsenceReason():
                            history = None
                attempt_rows = list(
                    (
                        await session.execute(_attempts_statement(node_task_id))
                    )
                    .tuples()
                    .all()
                )
    except SQLAlchemyError as exc:
        return _db_err('workflow node detail query', exc)
    except HistoryContractError as exc:
        return _contract_err('workflow node history read', exc)

    if history is not None:
        leaf = _history_leaf(history)
        attempts = [_history_attempt(record) for record in history.attempts]
    else:
        leaf = _leaf_task(task) if task is not None else None
        attempts = [_attempt(row) for row in attempt_rows]
    return Ok(
        WorkflowTaskDetail(
            task_index=node.task_index,
            node_id=node.node_id,
            task_name=node.task_name,
            node_status=node.status,
            is_subworkflow=node.is_subworkflow,
            node_error=node_error,
            leaf=leaf,
            attempts=attempts,
        )
    )

