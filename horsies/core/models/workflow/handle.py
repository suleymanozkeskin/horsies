"""Workflow handle for tracking and retrieving results."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    cast,
)

from sqlalchemy import text
import asyncio
from horsies.core.logging import get_logger
from horsies.core.utils.loop_runner import get_shared_runner
from horsies.core.codec.serde import loads_json, task_result_from_json
from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.types.result import is_err

logger = get_logger('workflow.handle')

from .enums import OkT, OutT, WorkflowStatus, WorkflowTaskStatus
from .context import WorkflowHandleMissingIdError
from .nodes import NodeKey

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

SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND status IN ('PENDING', 'READY')
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


@dataclass
class WorkflowHandle(Generic[OutT]):
    """
    Handle for tracking and retrieving workflow results.

    Provides methods to:
    - Check workflow status
    - Wait for and retrieve results
    - Inspect individual task states
    - Cancel the workflow
    """

    workflow_id: str
    broker: PostgresBroker

    def status(self) -> WorkflowStatus:
        """Get current workflow status."""
        return get_shared_runner().call(self.status_async)

    async def status_async(self) -> WorkflowStatus:
        """Async version of status()."""
        async with self.broker.session_factory() as session:
            result = await session.execute(
                GET_WORKFLOW_STATUS_SQL,
                {'wf_id': self.workflow_id},
            )
            row = result.fetchone()
            if row is None:
                raise ValueError(f'Workflow {self.workflow_id} not found')
            return WorkflowStatus(row[0])

    def get(self, timeout_ms: int | None = None) -> TaskResult[OutT, TaskError]:
        """
        Block until workflow completes or timeout.

        Returns:
            If output task specified: that task's TaskResult
            Otherwise: TaskResult containing dict of terminal task results
        """

        return get_shared_runner().call(self.get_async, timeout_ms)

    async def get_async(
        self, timeout_ms: int | None = None,
    ) -> TaskResult[OutT, TaskError]:
        """Async version of get()."""

        start = time.monotonic()
        timeout_sec = timeout_ms / 1000 if timeout_ms else None

        # Subscribe to workflow_done once before the loop
        q: asyncio.Queue[Any] | None = None
        try:
            q = await self.broker.listener.listen('workflow_done')
        except RuntimeError:
            # Cross-loop access (sync handle.get() via LoopRunner) —
            # fall back to sleep-based polling.
            pass

        try:
            while True:
                # Check current status
                status = await self.status_async()

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

    async def _get_error(self) -> TaskResult[OutT, TaskError]:
        """Fetch failed workflow error."""
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
                )
            )

    def results(self) -> dict[str, TaskResult[Any, TaskError]]:
        """
        Get all task results keyed by unique identifier.

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".
        """
        return get_shared_runner().call(self.results_async)

    async def results_async(self) -> dict[str, TaskResult[Any, TaskError]]:
        """
        Async version of results().

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".
        """
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
            return out

    def result_for(
        self, node: TaskNode[OkT] | NodeKey[OkT]
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
        return get_shared_runner().call(self.result_for_async, node)

    async def result_for_async(
        self, node: TaskNode[OkT] | NodeKey[OkT]
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

    def tasks(self) -> list[WorkflowTaskInfo]:
        """Get status of all tasks in workflow."""
        return get_shared_runner().call(self.tasks_async)

    async def tasks_async(self) -> list[WorkflowTaskInfo]:
        """Async version of tasks()."""
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
            return out

    def cancel(self) -> None:
        """Request workflow cancellation."""
        get_shared_runner().call(self.cancel_async)

    async def cancel_async(self) -> None:
        """Async version of cancel()."""
        async with self.broker.session_factory() as session:
            # Cancel workflow
            await session.execute(
                CANCEL_WORKFLOW_SQL,
                {'wf_id': self.workflow_id},
            )

            # Skip pending/ready tasks
            await session.execute(
                SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL,
                {'wf_id': self.workflow_id},
            )

            await session.commit()

    def pause(self) -> bool:
        """
        Pause a running workflow.

        Transitions workflow from RUNNING to PAUSED state. Already-running tasks
        will continue to completion, but no new tasks will be enqueued.

        Use resume() to continue execution.

        Returns:
            True if workflow was paused, False if not RUNNING (no-op)
        """
        return get_shared_runner().call(self.pause_async)

    async def pause_async(self) -> bool:
        """
        Async version of pause().

        Returns:
            True if workflow was paused, False if not RUNNING (no-op)
        """
        from horsies.core.workflows.engine import pause_workflow

        return await pause_workflow(self.broker, self.workflow_id)

    def resume(self) -> bool:
        """
        Resume a paused workflow.

        Re-evaluates all PENDING tasks (marks READY if deps are terminal) and
        enqueues all READY tasks. Only works if workflow is currently PAUSED.

        Returns:
            True if workflow was resumed, False if not PAUSED (no-op)
        """
        return get_shared_runner().call(self.resume_async)

    async def resume_async(self) -> bool:
        """
        Async version of resume().

        Returns:
            True if workflow was resumed, False if not PAUSED (no-op)
        """
        from horsies.core.workflows.engine import resume_workflow

        return await resume_workflow(self.broker, self.workflow_id)
