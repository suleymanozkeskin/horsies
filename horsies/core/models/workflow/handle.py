"""Workflow handle for tracking and retrieving results."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Generic,
    TypeVar,
    cast,
)

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from horsies.core.logging import get_logger
from horsies.core.utils.loop_runner import get_shared_runner, LoopRunnerError
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.codec.serde import loads_json, task_result_from_json
from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.types.result import Ok, Err, is_err

from .enums import OkT, OutT, WorkflowStatus, WorkflowTaskStatus
from .context import WorkflowHandleMissingIdError
from .handle_types import HandleErrorCode, HandleOperationError, HandleResult
from .nodes import NodeKey

if TYPE_CHECKING:
    from horsies.core.workflows.lifecycle_types import (
        LifecycleOperationError,
    )

logger = get_logger('workflow.handle')


def _lifecycle_err_to_handle_err(
    err: 'LifecycleOperationError',
    operation: str,
    workflow_id: str,
) -> HandleOperationError:
    """Map a lifecycle-layer error to a handle-layer error."""
    from horsies.core.workflows.lifecycle_types import LifecycleErrorCode

    code_map: dict[LifecycleErrorCode, HandleErrorCode] = {
        LifecycleErrorCode.WORKFLOW_NOT_FOUND: HandleErrorCode.WORKFLOW_NOT_FOUND,
        LifecycleErrorCode.DB_OPERATION_FAILED: HandleErrorCode.DB_OPERATION_FAILED,
        LifecycleErrorCode.LOOP_RUNNER_FAILED: HandleErrorCode.LOOP_RUNNER_FAILED,
        LifecycleErrorCode.INTERNAL_FAILED: HandleErrorCode.INTERNAL_FAILED,
    }
    return HandleOperationError(
        code=code_map.get(err.code, HandleErrorCode.INTERNAL_FAILED),
        message=err.message,
        retryable=err.retryable,
        operation=operation,
        stage=err.stage,
        workflow_id=workflow_id,
        exception=err.exception,
    )

_T = TypeVar('_T')

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker
    from .nodes import TaskNode


# =============================================================================
# SQL constants for WorkflowHandle
# =============================================================================

GET_WORKFLOW_STATUS_SQL = text("""
    SELECT status FROM horsies_workflows WHERE id = :wf_id
""")

GET_WORKFLOW_RESULT_SQL = text("""
    SELECT result FROM horsies_workflows WHERE id = :wf_id
""")

GET_WORKFLOW_ERROR_SQL = text("""
    SELECT error, status FROM horsies_workflows WHERE id = :wf_id
""")

GET_WORKFLOW_TASK_RESULTS_SQL = text("""
    SELECT node_id, result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND result IS NOT NULL
""")

GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL = text("""
    SELECT result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = :node_id
      AND result IS NOT NULL
""")

GET_WORKFLOW_TASKS_SQL = text("""
    SELECT node_id, task_index, task_name, status, result, started_at, completed_at
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
    ORDER BY task_index
""")

CANCEL_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'CANCELLED', updated_at = NOW()
    WHERE id = :wf_id AND status IN ('PENDING', 'RUNNING', 'PAUSED')
""")

SYNC_RUNNING_ENQUEUED_WORKFLOW_TASKS_ON_CANCEL_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = 'RUNNING',
        started_at = COALESCE(wt.started_at, NOW())
    FROM horsies_tasks t
    WHERE wt.workflow_id = :wf_id
      AND wt.task_id = t.id
      AND wt.status = 'ENQUEUED'
      AND t.status = 'RUNNING'
""")

MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL = text("""
    UPDATE horsies_tasks t
    SET status = 'CANCELLED',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        updated_at = NOW()
    FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = :wf_id
      AND wt.task_id = t.id
      AND wt.status = 'ENQUEUED'
      AND t.status IN ('PENDING', 'CLAIMED')
""")

SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND status IN ('PENDING', 'READY')
""")

SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = 'SKIPPED',
        completed_at = NOW()
    FROM horsies_tasks t
    WHERE wt.workflow_id = :wf_id
      AND wt.task_id = t.id
      AND wt.status = 'ENQUEUED'
      AND t.status = 'CANCELLED'
""")


# =============================================================================
# WorkflowHandle
# =============================================================================


@dataclass
class WorkflowTaskInfo:
    """Information about a task within a workflow."""

    node_id: str | None
    index: int
    name: str
    status: WorkflowTaskStatus
    result: TaskResult[Any, TaskError] | None
    started_at: datetime | None
    completed_at: datetime | None


def _broker_task_error(message: str) -> TaskResult[Any, TaskError]:
    """Build a TaskResult with BROKER_ERROR for fold-strategy methods."""
    return TaskResult(
        err=TaskError(
            error_code=LibraryErrorCode.BROKER_ERROR,
            message=message,
        ),
    )


@dataclass
class WorkflowHandle(Generic[OutT]):
    """
    Handle for tracking and retrieving workflow results.

    Provides methods to:
    - Check workflow status
    - Wait for and retrieve results
    - Inspect individual task states
    - Cancel the workflow

    Error handling follows two strategies:

    **Wrap strategy** (status, cancel, pause, resume, results, tasks):
    Returns ``HandleResult[T]``. Infrastructure errors are
    ``Err(HandleOperationError)``.

    **Fold strategy** (get, result_for):
    Returns ``TaskResult[T, TaskError]``. Infrastructure errors fold into
    ``TaskResult(err=TaskError(BROKER_ERROR, ...))``.
    """

    workflow_id: str
    broker: PostgresBroker

    # ─── wrap-strategy sync helpers ──────────────────────────────────

    def _sync_call(
        self,
        coro_fn: Callable[..., Awaitable[HandleResult[_T]]],
        operation: str,
        *args: Any,
    ) -> HandleResult[_T]:
        """Sync bridge for wrap-strategy async methods."""
        try:
            return get_shared_runner().call(coro_fn, *args)
        except asyncio.CancelledError:
            raise
        except LoopRunnerError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.LOOP_RUNNER_FAILED,
                message=f'Loop runner failed for {operation}: {exc}',
                retryable=False,
                operation=operation,
                stage='loop_runner',
                workflow_id=self.workflow_id,
                exception=exc,
            ))
        except Exception as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.INTERNAL_FAILED,
                message=f'Unexpected error in sync bridge for {operation}: {exc}',
                retryable=False,
                operation=operation,
                stage='loop_runner',
                workflow_id=self.workflow_id,
                exception=exc,
            ))

    # ─── fold-strategy sync helpers ──────────────────────────────────

    def _sync_task_result_call(
        self,
        coro_fn: Callable[..., Awaitable[TaskResult[_T, TaskError]]],
        *args: Any,
    ) -> TaskResult[_T, TaskError]:
        """Sync bridge for fold-strategy async methods."""
        try:
            return get_shared_runner().call(coro_fn, *args)
        except asyncio.CancelledError:
            raise
        except LoopRunnerError as exc:
            return cast(
                'TaskResult[_T, TaskError]',
                _broker_task_error(f'Loop runner failed: {exc}'),
            )
        except Exception as exc:
            return cast(
                'TaskResult[_T, TaskError]',
                _broker_task_error(f'Unexpected error in sync bridge: {exc}'),
            )

    # ─── status ──────────────────────────────────────────────────────

    def status(self) -> HandleResult[WorkflowStatus]:
        """Get current workflow status."""
        return self._sync_call(self.status_async, 'status')

    async def status_async(self) -> HandleResult[WorkflowStatus]:
        """Async version of status()."""
        try:
            async with self.broker.session_factory() as session:
                result = await session.execute(
                    GET_WORKFLOW_STATUS_SQL,
                    {'wf_id': self.workflow_id},
                )
                row = result.fetchone()
                if row is None:
                    return Err(HandleOperationError(
                        code=HandleErrorCode.WORKFLOW_NOT_FOUND,
                        message=f'Workflow {self.workflow_id} not found',
                        retryable=False,
                        operation='status',
                        stage='status_lookup',
                        workflow_id=self.workflow_id,
                    ))
                return Ok(WorkflowStatus(row[0]))
        except SQLAlchemyError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.DB_OPERATION_FAILED,
                message=f'DB query failed for workflow {self.workflow_id} status: {exc}',
                retryable=is_retryable_connection_error(exc),
                operation='status',
                stage='query',
                workflow_id=self.workflow_id,
                exception=exc,
            ))

    # ─── get ─────────────────────────────────────────────────────────

    def get(self, timeout_ms: int | None = None) -> TaskResult[OutT, TaskError]:
        """
        Block until workflow completes or timeout.

        Returns:
            If output task specified: that task's TaskResult
            Otherwise: TaskResult containing dict of terminal task results
        """
        return self._sync_task_result_call(self.get_async, timeout_ms)

    async def get_async(
        self, timeout_ms: int | None = None,
    ) -> TaskResult[OutT, TaskError]:
        """Async version of get()."""

        start = time.monotonic()
        timeout_sec = timeout_ms / 1000 if timeout_ms else None

        # Subscribe to workflow_done once before the loop.
        # Cross-loop RuntimeError (programming error) or infrastructure Err
        # both fall back to sleep-based polling.
        q: asyncio.Queue[Any] | None = None
        try:
            listen_r = await self.broker.listener.listen('workflow_done')
        except RuntimeError:
            # Cross-loop access (sync handle.get() via LoopRunner).
            pass
        else:
            match listen_r:
                case Ok(queue):
                    q = queue
                case Err(listen_err):
                    logger.debug(
                        'Listener subscribe failed for workflow_done; falling back to polling: %s',
                        listen_err.message,
                    )

        try:
            while True:
                # Check current status (now returns HandleResult)
                status_r = await self.status_async()

                if is_err(status_r):
                    handle_err = status_r.err_value
                    match handle_err.code:
                        case HandleErrorCode.WORKFLOW_NOT_FOUND:
                            error_code: LibraryErrorCode = LibraryErrorCode.WORKFLOW_NOT_FOUND
                        case _:
                            error_code = LibraryErrorCode.BROKER_ERROR
                    return cast(
                        'TaskResult[OutT, TaskError]',
                        TaskResult(
                            err=TaskError(
                                error_code=error_code,
                                message=handle_err.message,
                            ),
                        ),
                    )

                status = status_r.ok_value

                if status == WorkflowStatus.COMPLETED:
                    return await self._get_result()

                if status in (WorkflowStatus.FAILED, WorkflowStatus.CANCELLED):
                    return await self._get_error()

                if status == WorkflowStatus.PAUSED:
                    return cast(
                        'TaskResult[OutT, TaskError]',
                        TaskResult(
                            err=TaskError(
                                error_code='WORKFLOW_PAUSED',
                                message='Workflow is paused awaiting intervention',
                            )
                        ),
                    )

                # Check timeout
                elapsed = time.monotonic() - start
                if timeout_sec and elapsed >= timeout_sec:
                    return cast(
                        'TaskResult[OutT, TaskError]',
                        TaskResult(
                            err=TaskError(
                                error_code=LibraryErrorCode.WAIT_TIMEOUT,
                                message=f'Workflow did not complete within {timeout_ms}ms',
                            )
                        ),
                    )

                # Wait for notification or poll
                remaining = (timeout_sec - elapsed) if timeout_sec else 5.0
                wait_time = min(remaining, 5.0)

                if q is not None:
                    # Drain queue looking for our workflow_id
                    try:
                        await asyncio.wait_for(
                            self._drain_queue_for_workflow(q), timeout=wait_time,
                        )
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(min(wait_time, 1.0))
        finally:
            if q is not None:
                await self.broker.listener.unsubscribe('workflow_done', q)

    async def _drain_queue_for_workflow(self, q: Any) -> None:
        """Drain notifications until one matches this workflow."""
        while True:
            note = await q.get()
            if note.payload == self.workflow_id:
                return

    async def _get_result(self) -> TaskResult[OutT, TaskError]:
        """Fetch completed workflow result."""
        try:
            async with self.broker.session_factory() as session:
                result = await session.execute(
                    GET_WORKFLOW_RESULT_SQL,
                    {'wf_id': self.workflow_id},
                )
                row = result.fetchone()
                if row and row[0]:
                    loads_r = loads_json(row[0])
                    if is_err(loads_r):
                        return cast('TaskResult[OutT, TaskError]', TaskResult(
                            err=TaskError(
                                error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=f'Workflow result JSON corrupt: {loads_r.err_value}',
                            ),
                        ))
                    tr_r = task_result_from_json(loads_r.ok_value)
                    if is_err(tr_r):
                        return cast('TaskResult[OutT, TaskError]', TaskResult(
                            err=TaskError(
                                error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=f'Workflow result deser failed: {tr_r.err_value}',
                            ),
                        ))
                    return cast('TaskResult[OutT, TaskError]', tr_r.ok_value)
                return cast('TaskResult[OutT, TaskError]', TaskResult(ok=None))
        except SQLAlchemyError as exc:
            return cast(
                'TaskResult[OutT, TaskError]',
                _broker_task_error(
                    f'DB query failed fetching result for workflow {self.workflow_id}: {exc}',
                ),
            )

    async def _get_error(self) -> TaskResult[OutT, TaskError]:
        """Fetch failed workflow error."""
        try:
            async with self.broker.session_factory() as session:
                result = await session.execute(
                    GET_WORKFLOW_ERROR_SQL,
                    {'wf_id': self.workflow_id},
                )
                row = result.fetchone()
                if row and row[0]:
                    loads_r = loads_json(row[0])
                    if is_err(loads_r):
                        logger.warning(
                            'Workflow %s error payload corrupt: %s',
                            self.workflow_id,
                            loads_r.err_value,
                        )
                        return cast(
                            'TaskResult[OutT, TaskError]',
                            TaskResult(
                                err=TaskError(
                                    error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                                    message=f'Workflow error payload corrupt: {loads_r.err_value}',
                                    data={'workflow_id': self.workflow_id},
                                )
                            ),
                        )
                    error_data = loads_r.ok_value
                    if isinstance(error_data, dict):
                        # Safely extract known TaskError fields with type narrowing
                        raw_code = error_data.get('error_code')
                        raw_msg = error_data.get('message')
                        return cast(
                            'TaskResult[OutT, TaskError]',
                            TaskResult(
                                err=TaskError(
                                    error_code=str(raw_code) if raw_code is not None else None,
                                    message=str(raw_msg) if raw_msg is not None else None,
                                    data=error_data.get('data'),
                                )
                            ),
                        )
                status_str = row[1] if row else 'FAILED'
                return cast(
                    'TaskResult[OutT, TaskError]',
                    TaskResult(
                        err=TaskError(
                            error_code=f'WORKFLOW_{status_str}',
                            message=f'Workflow {status_str.lower()}',
                        )
                    ),
                )
        except SQLAlchemyError as exc:
            return cast(
                'TaskResult[OutT, TaskError]',
                _broker_task_error(
                    f'DB query failed fetching error for workflow {self.workflow_id}: {exc}',
                ),
            )

    # ─── result_for ──────────────────────────────────────────────────

    def result_for(
        self, node: TaskNode[OkT] | NodeKey[OkT],
    ) -> TaskResult[OkT, TaskError]:
        """
        Get the result for a specific TaskNode or NodeKey.

        Non-blocking: queries the database once and returns immediately.

        Args:
            node: The TaskNode or NodeKey whose result to retrieve.

        Returns:
            TaskResult[T, TaskError] where T matches the node's type.
            - If task completed: returns the task's result (success or error)
            - If task not completed: returns TaskResult with
              error_code=LibraryErrorCode.RESULT_NOT_READY

        Raises:
            WorkflowHandleMissingIdError: If node has no node_id assigned.

        Example:
            result = handle.result_for(node)
            if result.is_err() and result.err.error_code == LibraryErrorCode.RESULT_NOT_READY:
                # Task hasn't completed yet - wait or check later
                pass
        """
        return self._sync_task_result_call(self.result_for_async, node)

    async def result_for_async(
        self, node: TaskNode[OkT] | NodeKey[OkT],
    ) -> TaskResult[OkT, TaskError]:
        """Async version of result_for(). See result_for() for full documentation."""
        node_id: str | None
        if isinstance(node, NodeKey):
            node_id = node.node_id
        else:
            node_id = node.node_id

        if node_id is None:
            raise WorkflowHandleMissingIdError(
                'TaskNode node_id is not set. Ensure WorkflowSpec assigns node_id '
                'or provide an explicit node_id.'
            )

        try:
            async with self.broker.session_factory() as session:
                result = await session.execute(
                    GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL,
                    {'wf_id': self.workflow_id, 'node_id': node_id},
                )
                row = result.fetchone()
                if row is None or row[0] is None:
                    return cast(
                        'TaskResult[OkT, TaskError]',
                        TaskResult(
                            err=TaskError(
                                error_code=LibraryErrorCode.RESULT_NOT_READY,
                                message=(
                                    f"Task '{node_id}' has not completed yet "
                                    f"in workflow '{self.workflow_id}'"
                                ),
                            )
                        ),
                    )

                loads_r = loads_json(row[0])
                if is_err(loads_r):
                    return cast('TaskResult[OkT, TaskError]', TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                            message=f'Result JSON corrupt for node {node_id}: {loads_r.err_value}',
                        ),
                    ))
                tr_r = task_result_from_json(loads_r.ok_value)
                if is_err(tr_r):
                    return cast('TaskResult[OkT, TaskError]', TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                            message=f'Result deser failed for node {node_id}: {tr_r.err_value}',
                        ),
                    ))
                return cast('TaskResult[OkT, TaskError]', tr_r.ok_value)
        except SQLAlchemyError as exc:
            return cast(
                'TaskResult[OkT, TaskError]',
                _broker_task_error(
                    f'DB query failed for result_for node {node_id} '
                    f'in workflow {self.workflow_id}: {exc}',
                ),
            )

    # ─── results ─────────────────────────────────────────────────────

    def results(self) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]:
        """
        Get all task results keyed by unique identifier.

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".
        """
        return self._sync_call(self.results_async, 'results')

    async def results_async(self) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]:
        """
        Async version of results().

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".
        """
        try:
            async with self.broker.session_factory() as session:
                result = await session.execute(
                    GET_WORKFLOW_TASK_RESULTS_SQL,
                    {'wf_id': self.workflow_id},
                )

                out: dict[str, TaskResult[Any, TaskError]] = {}
                for row in result.fetchall():
                    loads_r = loads_json(row[1])
                    if is_err(loads_r):
                        out[row[0]] = TaskResult(
                            err=TaskError(
                                error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=f'Result JSON corrupt for node {row[0]}: {loads_r.err_value}',
                            ),
                        )
                        continue
                    tr_r = task_result_from_json(loads_r.ok_value)
                    if is_err(tr_r):
                        out[row[0]] = TaskResult(
                            err=TaskError(
                                error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=f'Result deser failed for node {row[0]}: {tr_r.err_value}',
                            ),
                        )
                        continue
                    out[row[0]] = tr_r.ok_value
                return Ok(out)
        except SQLAlchemyError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.DB_OPERATION_FAILED,
                message=f'DB query failed for workflow {self.workflow_id} results: {exc}',
                retryable=is_retryable_connection_error(exc),
                operation='results',
                stage='query',
                workflow_id=self.workflow_id,
                exception=exc,
            ))

    # ─── tasks ───────────────────────────────────────────────────────

    def tasks(self) -> HandleResult[list[WorkflowTaskInfo]]:
        """Get status of all tasks in workflow."""
        return self._sync_call(self.tasks_async, 'tasks')

    async def tasks_async(self) -> HandleResult[list[WorkflowTaskInfo]]:
        """Async version of tasks()."""
        try:
            async with self.broker.session_factory() as session:
                result = await session.execute(
                    GET_WORKFLOW_TASKS_SQL,
                    {'wf_id': self.workflow_id},
                )

                out: list[WorkflowTaskInfo] = []
                for row in result.fetchall():
                    task_result_value: TaskResult[Any, TaskError] | None = None
                    if row[4]:
                        loads_r = loads_json(row[4])
                        if is_err(loads_r):
                            task_result_value = TaskResult(
                                err=TaskError(
                                    error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                                    message=f'Result JSON corrupt: {loads_r.err_value}',
                                ),
                            )
                        else:
                            tr_r = task_result_from_json(loads_r.ok_value)
                            if is_err(tr_r):
                                task_result_value = TaskResult(
                                    err=TaskError(
                                        error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                                        message=f'Result deser failed: {tr_r.err_value}',
                                    ),
                                )
                            else:
                                task_result_value = tr_r.ok_value

                    out.append(WorkflowTaskInfo(
                        node_id=row[0],
                        index=row[1],
                        name=row[2],
                        status=WorkflowTaskStatus(row[3]),
                        result=task_result_value,
                        started_at=row[5],
                        completed_at=row[6],
                    ))
                return Ok(out)
        except SQLAlchemyError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.DB_OPERATION_FAILED,
                message=f'DB query failed for workflow {self.workflow_id} tasks: {exc}',
                retryable=is_retryable_connection_error(exc),
                operation='tasks',
                stage='query',
                workflow_id=self.workflow_id,
                exception=exc,
            ))

    # ─── cancel ──────────────────────────────────────────────────────

    def cancel(self) -> HandleResult[None]:
        """Request workflow cancellation."""
        return self._sync_call(self.cancel_async, 'cancel')

    async def cancel_async(self) -> HandleResult[None]:
        """Async version of cancel()."""
        stage = 'query'
        try:
            async with self.broker.session_factory() as session:
                # Cancel workflow (UPDATE is a no-op if not found or already terminal)
                await session.execute(
                    CANCEL_WORKFLOW_SQL,
                    {'wf_id': self.workflow_id},
                )

                # Verify workflow exists — UPDATE is a no-op for both
                # nonexistent workflows and those in non-cancellable states.
                exists = await session.execute(
                    GET_WORKFLOW_STATUS_SQL,
                    {'wf_id': self.workflow_id},
                )
                exists_row = exists.fetchone()
                if exists_row is None:
                    return Err(HandleOperationError(
                        code=HandleErrorCode.WORKFLOW_NOT_FOUND,
                        message=f'Workflow {self.workflow_id} not found',
                        retryable=False,
                        operation='cancel',
                        stage='existence_check',
                        workflow_id=self.workflow_id,
                    ))

                status_val = WorkflowStatus(exists_row[0])
                if status_val != WorkflowStatus.CANCELLED:
                    # Non-cancellable state (COMPLETED, FAILED) → no-op
                    return Ok(None)

                # CANCELLED (either by our UPDATE or already) → idempotent cleanup
                # If a task has already started but workflow_task still says ENQUEUED,
                # normalize it to RUNNING so cancellation doesn't leave stale ENQUEUED rows.
                await session.execute(
                    SYNC_RUNNING_ENQUEUED_WORKFLOW_TASKS_ON_CANCEL_SQL,
                    {'wf_id': self.workflow_id},
                )

                # Cancel ENQUEUED tasks that have not started execution yet.
                # This guarantees they are no longer claimable by workers.
                await session.execute(
                    MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL,
                    {'wf_id': self.workflow_id},
                )

                # Skip pending/ready tasks
                await session.execute(
                    SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL,
                    {'wf_id': self.workflow_id},
                )

                # Skip ENQUEUED workflow tasks whose backing task was cancelled above.
                await session.execute(
                    SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL,
                    {'wf_id': self.workflow_id},
                )

                stage = 'commit'
                await session.commit()
            return Ok(None)
        except SQLAlchemyError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.DB_OPERATION_FAILED,
                message=f'Cancel failed for workflow {self.workflow_id}: {exc}',
                retryable=is_retryable_connection_error(exc),
                operation='cancel',
                stage=stage,
                workflow_id=self.workflow_id,
                exception=exc,
            ))

    # ─── pause ───────────────────────────────────────────────────────

    def pause(self) -> HandleResult[bool]:
        """
        Pause a running workflow.

        Transitions workflow from RUNNING to PAUSED state. Already-running tasks
        will continue to completion, but no new tasks will be enqueued.

        Use resume() to continue execution.

        Returns:
            Ok(True) if workflow was paused, Ok(False) if not RUNNING (no-op).
            Err(HandleOperationError) if workflow not found or infrastructure failure.
        """
        return self._sync_call(self.pause_async, 'pause')

    async def pause_async(self) -> HandleResult[bool]:
        """Async version of pause().

        Returns:
            Ok(True) if workflow was paused, Ok(False) if not RUNNING (no-op).
            Err(HandleOperationError) if workflow not found or infrastructure failure.
        """
        from horsies.core.workflows.engine import pause_workflow

        result = await pause_workflow(self.broker, self.workflow_id)
        match result:
            case Ok(value):
                return Ok(value)
            case Err(err):
                return Err(_lifecycle_err_to_handle_err(err, 'pause', self.workflow_id))

    # ─── resume ──────────────────────────────────────────────────────

    def resume(self) -> HandleResult[bool]:
        """
        Resume a paused workflow.

        Re-evaluates all PENDING tasks (marks READY if deps are terminal) and
        enqueues all READY tasks. Only works if workflow is currently PAUSED.

        Returns:
            Ok(True) if workflow was resumed, Ok(False) if not PAUSED (no-op).
            Err(HandleOperationError) if workflow not found or infrastructure failure.
        """
        return self._sync_call(self.resume_async, 'resume')

    async def resume_async(self) -> HandleResult[bool]:
        """Async version of resume().

        Returns:
            Ok(True) if workflow was resumed, Ok(False) if not PAUSED (no-op).
            Err(HandleOperationError) if workflow not found or infrastructure failure.
        """
        from horsies.core.workflows.engine import resume_workflow

        result = await resume_workflow(self.broker, self.workflow_id)
        match result:
            case Ok(value):
                return Ok(value)
            case Err(err):
                return Err(_lifecycle_err_to_handle_err(err, 'resume', self.workflow_id))
