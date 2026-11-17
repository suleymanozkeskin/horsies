# pyright: reportPrivateUsage=false, reportUnnecessaryCast=false
"""The workflow handle reads, before they called the core.

``WorkflowHandle.status_async``, ``get_async``, ``result_for_async``,
``results_async`` and ``tasks_async``
(``python/horsies/core/models/workflow/handle.py``) read the workflow and
node rows on the broker's SQLAlchemy resources: ``get_async`` read the
status, then the result or the error (two statements), and waited on the
broker's ``workflow_done`` listener. The functions below are those bodies.
Each takes the handle as its first parameter, so the dual-run rebinding can
put it in the place of the method.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from typing import Any, Literal, assert_never, cast

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.resources import (
    BrokerResources,
    EventLoopLimit,
    ListenerDisabled,
    ListenerEnabled,
)
from horsies.core.codec.json_io import loads_json
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    BuiltInTaskCode,
    ContractCode,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow.context import SubWorkflowSummary
from horsies.core.models.workflow.enums import WorkflowStatus, stored_node_status
from horsies.core.models.workflow.handle import (
    WorkflowHandle,
    WorkflowTaskInfo,
    _broker_task_error,
    _completed_without_stored_result,
    _decode_per_task_envelope,
    _decode_stored_workflow_error,
    _decode_workflow_envelope,
    _empty_stored_error,
    _terminal_outcome_code,
    _TerminalErrorStatus,
    _wait_inside_task_result,
)
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleResult,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)
from horsies.core.models.workflow.nodes import NodeKey, TaskNode
from horsies.core.process_role import ProcessRole, process_role
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.utils.wait_timeout import wait_timeout_seconds

logger = get_logger('workflow.handle')


def _event_loop_limit_task_result(
    workflow_id: str, loop_limit: EventLoopLimit,
) -> TaskResult[Any, TaskError]:
    """Build a TaskResult with EVENT_LOOP_LIMIT for fold-strategy methods."""
    return TaskResult(
        err=TaskError(
            error_code=RetrievalCode.EVENT_LOOP_LIMIT,
            message=f'workflow {workflow_id}: {loop_limit.describe()}',
        ),
    )

GET_WORKFLOW_STATUS_SQL = text("""
    SELECT status FROM horsies_workflows WHERE id = :wf_id
""")

GET_WORKFLOW_RESULT_SQL = text("""
    SELECT result FROM horsies_workflows WHERE id = :wf_id
""")

GET_WORKFLOW_ERROR_SQL = text("""
    SELECT error FROM horsies_workflows WHERE id = :wf_id
""")

# The node reads start from the workflow row, so one statement tells "no
# workflow" (no row) from "no matching node" (one row whose
# workflow_task_id is NULL). See ``_workflow_node_rows``.
GET_WORKFLOW_TASK_RESULTS_SQL = text("""
    SELECT wt.id AS workflow_task_id, wt.node_id, wt.task_name, wt.result,
           wt.is_subworkflow, wt.sub_definition_key
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt
           ON wt.workflow_id = w.id
          AND wt.result IS NOT NULL
    WHERE w.id = :wf_id
""")

GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL = text("""
    SELECT task_name, result, is_subworkflow, sub_definition_key
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = :node_id
      AND result IS NOT NULL
""")

GET_WORKFLOW_TASKS_SQL = text("""
    SELECT wt.id AS workflow_task_id, wt.node_id, wt.task_index, wt.task_name,
           wt.status, wt.result, wt.started_at, wt.completed_at,
           wt.sub_workflow_id, wt.sub_workflow_summary,
           wt.is_subworkflow, wt.sub_definition_key
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.id = :wf_id
    ORDER BY wt.task_index
""")


def _parse_stored_workflow_status(
    workflow_id: str, stored_status: str,
) -> Result[WorkflowStatus, HandleStoredDataInvalid]:
    """Parse a stored ``horsies_workflows.status`` value.

    The column has no CHECK constraint, so a value that is not a
    ``WorkflowStatus`` is stored data that does not parse.
    """
    try:
        return Ok(WorkflowStatus(stored_status))
    except ValueError as exc:
        return Err(HandleStoredDataInvalid(
            message=(
                f'Workflow {workflow_id} has stored status {stored_status!r}, '
                'which is not a WorkflowStatus'
            ),
            workflow_id=workflow_id,
            exception=exc,
        ))


def _removed_after_status_read(
    workflow_id: str,
    status: Literal[WorkflowStatus.COMPLETED] | _TerminalErrorStatus,
) -> TaskResult[Any, TaskError]:
    """``WORKFLOW_NOT_FOUND`` for a workflow row that is gone at the second
    read of ``get()``: retention deleted it after the status read."""
    return TaskResult(
        err=TaskError(
            error_code=RetrievalCode.WORKFLOW_NOT_FOUND,
            message=(
                f'Workflow {workflow_id} not found: the row was removed '
                f'after its {status.value} status was read'
            ),
        ),
    )


async def status_async(self: WorkflowHandle[Any]) -> HandleResult[WorkflowStatus]:
    """Async version of status()."""
    match await self.broker.select_resources():
        case Err(loop_limit):
            return self._event_loop_limit(loop_limit, 'status')
        case Ok(resources):
            pass
    return await _status(self, resources)


async def _status(
    self: WorkflowHandle[Any], resources: BrokerResources,
) -> Result[
    WorkflowStatus,
    HandleWorkflowNotFound | HandleDbOperationFailed | HandleStoredDataInvalid,
]:
    """Read the workflow status with already-selected resources.

    It takes selected resources and is async, so it returns neither
    ``HandleEventLoopLimit`` nor ``HandleAsyncContext``.
    """
    try:
        async with resources.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_STATUS_SQL,
                {'wf_id': self.workflow_id},
            )
            row = result.fetchone()
            if row is None:
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=self.workflow_id,
                ))
            return _parse_stored_workflow_status(self.workflow_id, row.status)
    except SQLAlchemyError as exc:
        return Err(HandleDbOperationFailed(
            message=f'DB query failed for workflow {self.workflow_id} status: {exc}',
            retryable=is_retryable_connection_error(exc),
            workflow_id=self.workflow_id,
            exception=exc,
        ))

# ─── get ─────────────────────────────────────────────────────────


async def get_async(
    self: WorkflowHandle[Any], timeout_ms: int | None = None,
) -> TaskResult[Any, TaskError]:
    """Async version of get().

    In a process that runs task bodies it returns
    ``RetrievalCode.WAIT_INSIDE_TASK`` at once, as its first check, with
    no wait. Elsewhere, a timeout_ms that is not None or an int from 0 to
    MAX_TASK_TIMEOUT_MS returns ``RetrievalCode.INVALID_INPUT`` before any
    read.
    """
    if process_role() is ProcessRole.RUNS_TASK_BODIES:
        return cast(
            'TaskResult[Any, TaskError]',
            _wait_inside_task_result(self.workflow_id, 'get_async'),
        )

    start = time.monotonic()
    # timeout_ms=0 means "0ms budget": return the result if already
    # available, else WAIT_TIMEOUT immediately. Only None disables the
    # timeout. Matches PostgresBroker.get_result_async.
    match wait_timeout_seconds(timeout_ms):
        case Err(err_value=refused):
            return cast(
                'TaskResult[Any, TaskError]',
                TaskResult(
                    err=TaskError(
                        error_code=RetrievalCode.INVALID_INPUT,
                        message=(
                            f'WorkflowHandle.get() for workflow '
                            f'{self.workflow_id}: {refused.message}'
                        ),
                        data={'workflow_id': self.workflow_id},
                    ),
                ),
            )
        case Ok(ok_value=timeout_sec):
            pass

    match await self.broker.select_resources():
        case Err(loop_limit):
            return cast(
                'TaskResult[Any, TaskError]',
                _event_loop_limit_task_result(self.workflow_id, loop_limit),
            )
        case Ok(resources):
            pass

    # Subscribe to workflow_done once before the loop. A broker without a
    # listener or an infrastructure Err falls back to sleep-based polling.
    subscription: tuple[PostgresListener, asyncio.Queue[Any]] | None = None
    match resources.listener:
        case ListenerEnabled(listener=listener):
            listen_r = await listener.listen('workflow_done')
            match listen_r:
                case Ok(queue):
                    subscription = (listener, queue)
                case Err(listen_err):
                    logger.debug(
                        'LISTEN unavailable; falling back to polling for workflow_done. '
                        'If using PgBouncer transaction pooling, configure '
                        'PostgresConfig.session_database_url with a direct/session-capable '
                        'Postgres URL. Original error: %s',
                        listen_err.message,
                    )
        case ListenerDisabled():
            pass

    try:
        while True:
            # Check current status (now returns HandleResult)
            status_r = await _status(self, resources)

            if is_err(status_r):
                status_err = status_r.err_value
                match status_err:
                    case HandleWorkflowNotFound():
                        error_code: BuiltInTaskCode = RetrievalCode.WORKFLOW_NOT_FOUND
                    case HandleDbOperationFailed():
                        error_code = OperationalErrorCode.BROKER_ERROR
                    case HandleStoredDataInvalid():
                        error_code = OperationalErrorCode.BROKER_ERROR
                    case _ as unreachable:
                        assert_never(unreachable)
                return cast(
                    'TaskResult[Any, TaskError]',
                    TaskResult(
                        err=TaskError(
                            error_code=error_code,
                            message=status_err.message,
                        ),
                    ),
                )

            status = status_r.ok_value

            match status:
                case WorkflowStatus.COMPLETED:
                    return await _get_result(self, resources)
                case (
                    WorkflowStatus.FAILED
                    | WorkflowStatus.CANCELLED
                    | WorkflowStatus.EXPIRED
                ):
                    return await _get_error(self, resources, status)
                case WorkflowStatus.PAUSED:
                    return cast(
                        'TaskResult[Any, TaskError]',
                        TaskResult(
                            err=TaskError(
                                error_code=OutcomeCode.WORKFLOW_PAUSED,
                                message=(
                                    'Workflow is paused awaiting intervention'
                                ),
                            )
                        ),
                    )
                case WorkflowStatus.PENDING | WorkflowStatus.RUNNING:
                    pass
                case _ as unreachable:
                    assert_never(unreachable)

            # Check timeout
            elapsed = time.monotonic() - start
            if timeout_sec is not None and elapsed >= timeout_sec:
                return cast(
                    'TaskResult[Any, TaskError]',
                    TaskResult(
                        err=TaskError(
                            error_code=RetrievalCode.WAIT_TIMEOUT,
                            message=f'Workflow did not complete within {timeout_ms}ms',
                        )
                    ),
                )

            # Wait for notification or poll
            remaining = (timeout_sec - elapsed) if timeout_sec is not None else 5.0
            wait_time = min(remaining, 5.0)

            match subscription:
                case (_, queue):
                    # Drain queue looking for our workflow_id
                    try:
                        await asyncio.wait_for(
                            _drain_queue_for_workflow(self, queue), timeout=wait_time,
                        )
                    except asyncio.TimeoutError:
                        pass
                case None:
                    await asyncio.sleep(min(wait_time, 1.0))
    finally:
        match subscription:
            case (listener, queue):
                await _unsubscribe_workflow_done_safely(self, listener, queue)
            case None:
                pass


async def _unsubscribe_workflow_done_safely(
    self: WorkflowHandle[Any], listener: PostgresListener, q: asyncio.Queue[Any],
) -> None:
    """Ensure unsubscribe cleanup completes even under repeated cancellation."""
    unsubscribe_task = asyncio.create_task(
        listener.unsubscribe('workflow_done', q)
    )
    cancelled_during_cleanup = False
    while not unsubscribe_task.done():
        try:
            await asyncio.shield(unsubscribe_task)
        except asyncio.CancelledError:
            cancelled_during_cleanup = True
            continue

    # Propagate unsubscribe failures (if any).
    await unsubscribe_task

    # Propagate cancellation only after cleanup has completed.
    if cancelled_during_cleanup:
        raise asyncio.CancelledError


async def _drain_queue_for_workflow(
    self: WorkflowHandle[Any], q: Any) -> None:
    """Drain notifications until one matches this workflow."""
    while True:
        note = await q.get()
        if note.payload == self.workflow_id:
            return


async def _get_result(
    self: WorkflowHandle[Any], resources: BrokerResources) -> TaskResult[Any, TaskError]:
    """Read the stored result of a workflow whose COMPLETED status was read.

    A terminal status does not change, so this read does not read it
    again.

    Outcomes:
        - No row (retention deleted the workflow after the status
          read): ``RetrievalCode.WORKFLOW_NOT_FOUND``.
        - A NULL or empty stored result:
          ``RESULT_DESERIALIZATION_ERROR``. The library always stores a
          result with ``COMPLETED``.
        - A stored result that is not JSON:
          ``RESULT_DESERIALIZATION_ERROR``.
        - A stored result that is JSON: the outcome of
          ``_decode_workflow_envelope``.
        - A database error: ``BROKER_ERROR``.
    """
    try:
        async with resources.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_RESULT_SQL,
                {'wf_id': self.workflow_id},
            )
            row = result.fetchone()
            if row is None:
                return cast(
                    'TaskResult[Any, TaskError]',
                    _removed_after_status_read(
                        self.workflow_id, WorkflowStatus.COMPLETED,
                    ),
                )
            match row.result:
                case None:
                    return cast(
                        'TaskResult[Any, TaskError]',
                        _completed_without_stored_result(
                            self.workflow_id, 'NULL',
                        ),
                    )
                case '':
                    return cast(
                        'TaskResult[Any, TaskError]',
                        _completed_without_stored_result(
                            self.workflow_id, 'empty',
                        ),
                    )
                case stored_result:
                    loads_r = loads_json(stored_result)
                    if is_err(loads_r):
                        return cast('TaskResult[Any, TaskError]', TaskResult(
                            err=TaskError(
                                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=f'Workflow result JSON corrupt: {loads_r.err_value}',
                            ),
                        ))
                    decoded = _decode_workflow_envelope(
                        loads_r.ok_value,
                        self.out_type,
                        app=self.broker.app,
                        workflow_id=self.workflow_id,
                    )
                    return cast('TaskResult[Any, TaskError]', decoded)
    except SQLAlchemyError as exc:
        return cast(
            'TaskResult[Any, TaskError]',
            _broker_task_error(
                f'DB query failed fetching result for workflow {self.workflow_id}: {exc}',
            ),
        )


async def _get_error(
    self: WorkflowHandle[Any], resources: BrokerResources, status: _TerminalErrorStatus,
) -> TaskResult[Any, TaskError]:
    """Read the stored error of a workflow whose terminal status was read.

    ``status`` is the status that the status read parsed. A terminal
    status does not change, so this read does not read it again.

    Outcomes:
        - No row (retention deleted the workflow after the status
          read): ``RetrievalCode.WORKFLOW_NOT_FOUND``.
        - A NULL stored error (no stored error): the ``OutcomeCode`` of
          ``status``.
        - An empty stored error: ``RESULT_DESERIALIZATION_ERROR``.
        - Any other stored error: the outcome of
          ``_decode_stored_workflow_error``.
        - A database error: ``BROKER_ERROR``.

    Raises:
        Exception: a defect in the decode. Only ``StrictJsonError`` and
            ``ValueValidationError``, the refusals of
            ``decode_task_error``, are folded.
    """
    try:
        async with resources.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_ERROR_SQL,
                {'wf_id': self.workflow_id},
            )
            row = result.fetchone()
            if row is None:
                return cast(
                    'TaskResult[Any, TaskError]',
                    _removed_after_status_read(self.workflow_id, status),
                )
            match row.error:
                case None:
                    return cast(
                        'TaskResult[Any, TaskError]',
                        TaskResult(
                            err=TaskError(
                                error_code=_terminal_outcome_code(status),
                                message=f'Workflow {status.value.lower()}',
                            ),
                        ),
                    )
                case '':
                    return cast(
                        'TaskResult[Any, TaskError]',
                        _empty_stored_error(self.workflow_id),
                    )
                case stored_error:
                    return cast(
                        'TaskResult[Any, TaskError]',
                        _decode_stored_workflow_error(
                            self.workflow_id, status, stored_error,
                        ),
                    )
    except SQLAlchemyError as exc:
        return cast(
            'TaskResult[Any, TaskError]',
            _broker_task_error(
                f'DB query failed fetching error for workflow {self.workflow_id}: {exc}',
            ),
        )

# ─── result_for ──────────────────────────────────────────────────


async def result_for_async(
    self: WorkflowHandle[Any], node: TaskNode[Any] | NodeKey[Any],
) -> TaskResult[Any, TaskError]:
    """Async version of result_for(). See result_for() for full documentation."""
    node_id: str | None
    if isinstance(node, NodeKey):
        node_id = node.node_id
    else:
        node_id = node.node_id

    if node_id is None:
        return cast(
            'TaskResult[Any, TaskError]',
            TaskResult(
                err=TaskError(
                    error_code=ContractCode.WORKFLOW_CTX_MISSING_ID,
                    message=(
                        'TaskNode node_id is not set. Ensure WorkflowSpec assigns '
                        'node_id or provide an explicit node_id.'
                    ),
                ),
            ),
        )

    match await self.broker.select_resources():
        case Err(loop_limit):
            return cast(
                'TaskResult[Any, TaskError]',
                _event_loop_limit_task_result(self.workflow_id, loop_limit),
            )
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL,
                {'wf_id': self.workflow_id, 'node_id': node_id},
            )
            row = result.fetchone()
            if row is None or row.result is None:
                return cast(
                    'TaskResult[Any, TaskError]',
                    TaskResult(
                        err=TaskError(
                            error_code=RetrievalCode.RESULT_NOT_READY,
                            message=(
                                f"Task '{node_id}' has not completed yet "
                                f"in workflow '{self.workflow_id}'"
                            ),
                        )
                    ),
                )

            loads_r = loads_json(row.result)
            if is_err(loads_r):
                return cast('TaskResult[Any, TaskError]', TaskResult(
                    err=TaskError(
                        error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=f'Result JSON corrupt for node {node_id}: {loads_r.err_value}',
                    ),
                ))
            decoded = _decode_per_task_envelope(
                loads_r.ok_value,
                app=self.broker.app,
                task_name=row.task_name,
                node_id=node_id,
                sub_definition_key=(
                    row.sub_definition_key if row.is_subworkflow else None
                ),
            )
            return cast('TaskResult[Any, TaskError]', decoded)
    except SQLAlchemyError as exc:
        return cast(
            'TaskResult[Any, TaskError]',
            _broker_task_error(
                f'DB query failed for result_for node {node_id} '
                f'in workflow {self.workflow_id}: {exc}',
            ),
        )

# ─── results ─────────────────────────────────────────────────────


def _workflow_node_rows(
    self: WorkflowHandle[Any], rows: Sequence[Any],
) -> Result[Sequence[Any], HandleWorkflowNotFound]:
    """Split the rows of the workflow row LEFT JOIN its nodes.

    No row: no workflow has this id. One row whose ``workflow_task_id``
    is NULL: the workflow exists and no node matched, so ``Ok([])``.
    Otherwise every row is a node.
    """
    match rows:
        case []:
            return Err(HandleWorkflowNotFound(
                message=f'Workflow {self.workflow_id} not found',
                workflow_id=self.workflow_id,
            ))
        case [only] if only.workflow_task_id is None:
            return Ok([])
        case _:
            return Ok(rows)


async def results_async(self: WorkflowHandle[Any]) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]:
    """
    Async version of results().

    Keys are `node_id` values. If a TaskNode did not specify a node_id,
    WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".
    """
    match await self.broker.select_resources():
        case Err(loop_limit):
            return self._event_loop_limit(loop_limit, 'results')
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_TASK_RESULTS_SQL,
                {'wf_id': self.workflow_id},
            )
            match _workflow_node_rows(self, result.fetchall()):
                case Err() as not_found:
                    return not_found
                case Ok(node_rows):
                    pass

            out: dict[str, TaskResult[Any, TaskError]] = {}
            for row in node_rows:
                loads_r = loads_json(row.result)
                if is_err(loads_r):
                    out[row.node_id] = TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                            message=f'Result JSON corrupt for node {row.node_id}: {loads_r.err_value}',
                        ),
                    )
                    continue
                out[row.node_id] = _decode_per_task_envelope(
                    loads_r.ok_value,
                    app=self.broker.app,
                    task_name=row.task_name,
                    node_id=row.node_id,
                    sub_definition_key=(
                        row.sub_definition_key if row.is_subworkflow else None
                    ),
                )
            return Ok(out)
    except SQLAlchemyError as exc:
        return Err(HandleDbOperationFailed(
            message=f'DB query failed for workflow {self.workflow_id} results: {exc}',
            retryable=is_retryable_connection_error(exc),
            workflow_id=self.workflow_id,
            exception=exc,
        ))

# ─── tasks ───────────────────────────────────────────────────────


async def tasks_async(self: WorkflowHandle[Any]) -> HandleResult[list[WorkflowTaskInfo]]:
    """Async version of tasks()."""
    match await self.broker.select_resources():
        case Err(loop_limit):
            return self._event_loop_limit(loop_limit, 'tasks')
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_TASKS_SQL,
                {'wf_id': self.workflow_id},
            )
            match _workflow_node_rows(self, result.fetchall()):
                case Err() as not_found:
                    return not_found
                case Ok(node_rows):
                    pass

            out: list[WorkflowTaskInfo] = []
            for row in node_rows:
                task_result_value: TaskResult[Any, TaskError] | None = None
                if row.result:
                    loads_r = loads_json(row.result)
                    if is_err(loads_r):
                        task_result_value = TaskResult(
                            err=TaskError(
                                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=f'Result JSON corrupt: {loads_r.err_value}',
                            ),
                        )
                    else:
                        task_result_value = _decode_per_task_envelope(
                            loads_r.ok_value,
                            app=self.broker.app,
                            task_name=row.task_name,
                            node_id=row.node_id,
                            sub_definition_key=(
                                row.sub_definition_key
                                if row.is_subworkflow else None
                            ),
                        )

                summary: SubWorkflowSummary[Any] | None = None
                if row.sub_workflow_summary:
                    summary_loads_r = loads_json(row.sub_workflow_summary)
                    if is_err(summary_loads_r):
                        return Err(HandleStoredDataInvalid(
                            message=(
                                f'sub_workflow_summary JSON corrupt for node '
                                f'{row.node_id}: {summary_loads_r.err_value}'
                            ),
                            workflow_id=self.workflow_id,
                            exception=summary_loads_r.err_value,
                        ))
                    try:
                        summary = SubWorkflowSummary.from_json(
                            summary_loads_r.ok_value
                        )
                    except ValueError as summary_exc:
                        return Err(HandleStoredDataInvalid(
                            message=(
                                f'sub_workflow_summary invalid for node '
                                f'{row.node_id}: {summary_exc}'
                            ),
                            workflow_id=self.workflow_id,
                            exception=summary_exc,
                        ))

                out.append(WorkflowTaskInfo(
                    node_id=row.node_id,
                    index=row.task_index,
                    name=row.task_name,
                    status=stored_node_status(
                        workflow_id=self.workflow_id,
                        node_id=row.node_id,
                        stored=row.status,
                    ),
                    result=task_result_value,
                    started_at=row.started_at,
                    completed_at=row.completed_at,
                    sub_workflow_id=row.sub_workflow_id,
                    sub_workflow_summary=summary,
                ))
            return Ok(out)
    except SQLAlchemyError as exc:
        return Err(HandleDbOperationFailed(
            message=f'DB query failed for workflow {self.workflow_id} tasks: {exc}',
            retryable=is_retryable_connection_error(exc),
            workflow_id=self.workflow_id,
            exception=exc,
        ))
