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
from pydantic import ValidationError
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.serde import loads_json
from horsies.core.codec.typed import (
    Json,
    TypeAnnotation,
    decode_task_error,
    decode_task_result,
)
from horsies.core.models.tasks import (
    TaskResult,
    TaskError,
    BuiltInTaskCode,
    OperationalErrorCode,
    ContractCode,
    RetrievalCode,
    OutcomeCode,
)
from horsies.core.types.result import Ok, Err, is_err

from .context import SubWorkflowSummary
from .enums import OkT, OutT, WorkflowStatus, WorkflowTaskStatus
from .handle_types import HandleErrorCode, HandleOperationError, HandleResult
from .nodes import NodeKey

logger = get_logger('workflow.handle')

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
    SELECT node_id, task_name, result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND result IS NOT NULL
""")

GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL = text("""
    SELECT task_name, result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = :node_id
      AND result IS NOT NULL
""")

GET_WORKFLOW_TASKS_SQL = text("""
    SELECT node_id, task_index, task_name, status, result,
           started_at, completed_at, sub_workflow_id, sub_workflow_summary
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
    sub_workflow_id: str | None = None
    sub_workflow_summary: SubWorkflowSummary[Any] | None = None


def _broker_task_error(message: str) -> TaskResult[Any, TaskError]:
    """Build a TaskResult with BROKER_ERROR for fold-strategy methods."""
    return TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.BROKER_ERROR,
            message=message,
        ),
    )


class _OutputlessTerminals:
    """Sentinel ``out_type`` for workflows without an explicit output node.

    Started handles set ``out_type = _OUTPUTLESS_TERMINALS`` when the
    workflow spec's ``output`` is ``None``. ``WorkflowHandle.get()``
    dispatches to a dedicated decode path that walks the per-node
    ``results_by_id`` map embedded in the outer envelope.
    """

    __slots__ = ()


_OUTPUTLESS_TERMINALS: Any = _OutputlessTerminals()


def _decode_per_task_envelope(
    loaded: Json,
    *,
    app: Any | None,
    task_name: str | None,
    node_id: str,
) -> TaskResult[Any, TaskError]:
    """Decode a single workflow_task row's envelope using the source
    task's registered ``task_ok_type``.

    Used by ``result_for``, ``results``, and ``tasks`` — each returns
    per-node ``TaskResult`` values. Failure folds into a
    ``RESULT_DESERIALIZATION_ERROR`` sentinel so the response stays
    typed.
    """
    if not isinstance(task_name, str):
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Workflow task node {node_id!r} is missing task_name'
                ),
                data={'node_id': node_id},
            ),
        )
    from .typing_utils import resolve_source_ok_type

    source_ok_type = resolve_source_ok_type(app, task_name)
    if source_ok_type is None:
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Source {task_name!r} not registered (no task or '
                    f'workflow definition with this name) (node {node_id!r})'
                ),
                data={'node_id': node_id, 'task_name': task_name},
            ),
        )
    try:
        return decode_task_result(loaded, source_ok_type)
    except (StrictJsonError, ValidationError) as exc:
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'decode_task_result failed for node {node_id!r}: '
                    f'{exc}'
                ),
                data={'node_id': node_id, 'task_name': task_name},
            ),
        )


def _decode_workflow_envelope(
    raw: Json,
    out_type: TypeAnnotation | None,
    *,
    app: Any | None,
    workflow_id: str,
) -> TaskResult[Any, TaskError]:
    """Decode the workflow's stored result envelope into a TaskResult.

    Routes:
    - Err slot populated → decode TaskError (no out_type needed).
    - out_type is ``_OUTPUTLESS_TERMINALS`` → per-node decode using
      ``task_name_by_id`` embedded in the envelope and the local task
      registry on ``app``.
    - out_type is set → ``decode_task_result(raw, out_type)``.
    - out_type is None and ok slot populated → NO_TYPE_AVAILABLE.
    """
    from horsies.core.models.tasks import ContractCode

    if not isinstance(raw, dict):
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Workflow {workflow_id} result envelope is not a '
                    f'dict; got {type(raw).__name__}'
                ),
            ),
        )
    envelope = cast('dict[str, Any]', raw)
    if envelope.get('__h_task_result__') is not True:
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Workflow {workflow_id} result missing '
                    f'__h_task_result__ envelope marker'
                ),
            ),
        )

    # Strict-serde phase 6: outputless wire-vs-type must agree before
    # any per-slot decoding. A mismatch (typed out_type with
    # outputless payload, or outputless handle reading a typed
    # payload) is a contract violation — the workflow definition
    # changed between produce and consume, or the caller wired the
    # wrong handle. Fail closed instead of silently coercing to the
    # wrong shape. Checking this before the err-fast-path ensures a
    # smuggled outputless flag on a typed envelope can't slip through
    # via the err route either.
    wire_outputless = envelope.get('__h_outputless_terminals__') is True
    handle_outputless = out_type is _OUTPUTLESS_TERMINALS
    if wire_outputless != handle_outputless:
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Workflow {workflow_id} outputless/typed envelope '
                    f'mismatch: wire_outputless={wire_outputless}, '
                    f'handle_outputless={handle_outputless}. Workflow '
                    f'definition likely changed between produce and '
                    f'consume.'
                ),
                data={
                    'workflow_id': workflow_id,
                    'wire_outputless': wire_outputless,
                    'handle_outputless': handle_outputless,
                },
            ),
        )

    err_slot = envelope.get('err')
    if err_slot is not None:
        # Polymorphic decode preserves SubWorkflowError fields
        # (``sub_workflow_id`` / ``sub_workflow_summary``) emitted by
        # the engine for failed parent nodes; plain TaskError decode
        # would drop them.
        try:
            err = decode_task_error(err_slot)
        except (StrictJsonError, ValidationError) as exc:
            return TaskResult(
                err=TaskError(
                    error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                    message=f'TaskError decode failed: {exc}',
                ),
            )
        return TaskResult(err=err)

    # Outputless workflow path.
    if handle_outputless:
        ok_slot = envelope.get('ok')
        if not isinstance(ok_slot, dict):
            return TaskResult(
                err=TaskError(
                    error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                    message='Outputless workflow envelope ok slot malformed',
                ),
            )
        results_raw = ok_slot.get('results_by_id', {})
        task_names = ok_slot.get('task_name_by_id', {})
        if not isinstance(results_raw, dict) or not isinstance(task_names, dict):
            return TaskResult(
                err=TaskError(
                    error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                    message=(
                        'Outputless workflow envelope missing or malformed '
                        'results_by_id / task_name_by_id'
                    ),
                ),
            )
        decoded_results: dict[str, TaskResult[Any, TaskError]] = {}
        for node_id, per_node_envelope in cast(
            'dict[str, Any]', results_raw,
        ).items():
            source_task_name = task_names.get(node_id)
            if per_node_envelope is None or not isinstance(source_task_name, str):
                decoded_results[node_id] = TaskResult(
                    err=TaskError(
                        error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=(
                            f'Outputless workflow {workflow_id} node '
                            f'{node_id!r} missing payload or task name'
                        ),
                        data={'node_id': node_id},
                    ),
                )
                continue
            from .typing_utils import resolve_source_ok_type as _resolve_src_ok_type

            source_ok_type = _resolve_src_ok_type(app, source_task_name)
            if source_ok_type is None:
                decoded_results[node_id] = TaskResult(
                    err=TaskError(
                        error_code=ContractCode.NO_TYPE_AVAILABLE,
                        message=(
                            f'Outputless workflow {workflow_id} node '
                            f'{node_id!r}: source '
                            f'{source_task_name!r} is not a known task or '
                            f'workflow definition'
                        ),
                        data={
                            'node_id': node_id,
                            'source_task_name': source_task_name,
                        },
                    ),
                )
                continue
            try:
                decoded_results[node_id] = decode_task_result(
                    per_node_envelope, source_ok_type,
                )
            except (StrictJsonError, ValidationError) as exc:
                decoded_results[node_id] = TaskResult(
                    err=TaskError(
                        error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=(
                            f'decode_task_result failed for node '
                            f'{node_id!r}: {exc}'
                        ),
                        data={'node_id': node_id},
                    ),
                )
        return TaskResult(ok=decoded_results)

    # Normal output workflow path.
    if out_type is None:
        return TaskResult(
            err=TaskError(
                error_code=ContractCode.NO_TYPE_AVAILABLE,
                message=(
                    f'Workflow {workflow_id} ok-result decode requires '
                    f'a declared OutT; handle has none. Use '
                    f'raw_result() instead.'
                ),
            ),
        )
    try:
        return decode_task_result(raw, out_type)
    except (StrictJsonError, ValidationError) as exc:
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=f'decode_task_result failed: {exc}',
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
    # Strict-serde phase 6: declared OutT for typed result decode.
    # Started handles populate this from the workflow spec (see
    # ``_resolve_source_node_ok_type(spec.output)``). For outputless
    # workflows, the sentinel ``_OUTPUTLESS_TERMINALS`` routes through
    # a dedicated per-node decode path. By-id reconstruction without a
    # registered spec leaves this ``None`` and ``get()`` folds
    # NO_TYPE_AVAILABLE into the err slot when the ok slot is present.
    out_type: TypeAnnotation | None = None

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
                workflow_id=self.workflow_id,
                exception=exc,
            ))
        except Exception as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.INTERNAL_FAILED,
                message=f'Unexpected error in sync bridge for {operation}: {exc}',
                retryable=False,
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
                        workflow_id=self.workflow_id,
                    ))
                return Ok(WorkflowStatus(row.status))
        except SQLAlchemyError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.DB_OPERATION_FAILED,
                message=f'DB query failed for workflow {self.workflow_id} status: {exc}',
                retryable=is_retryable_connection_error(exc),
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
                        'LISTEN unavailable; falling back to polling for workflow_done. '
                        'If using PgBouncer transaction pooling, configure '
                        'PostgresConfig.session_database_url with a direct/session-capable '
                        'Postgres URL. Original error: %s',
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
                            error_code: BuiltInTaskCode = RetrievalCode.WORKFLOW_NOT_FOUND
                        case _:
                            error_code = OperationalErrorCode.BROKER_ERROR
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
                                error_code=OutcomeCode.WORKFLOW_PAUSED,
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
                                error_code=RetrievalCode.WAIT_TIMEOUT,
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
                await self._unsubscribe_workflow_done_safely(q)

    async def _unsubscribe_workflow_done_safely(self, q: asyncio.Queue[Any]) -> None:
        """Ensure unsubscribe cleanup completes even under repeated cancellation."""
        unsubscribe_task = asyncio.create_task(
            self.broker.listener.unsubscribe('workflow_done', q)
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
                if row and row.result:
                    loads_r = loads_json(row.result)
                    if is_err(loads_r):
                        return cast('TaskResult[OutT, TaskError]', TaskResult(
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
                    return cast('TaskResult[OutT, TaskError]', decoded)
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
                if row and row.error:
                    loads_r = loads_json(row.error)
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
                                    error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                                    message=f'Workflow error payload corrupt: {loads_r.err_value}',
                                    data={'workflow_id': self.workflow_id},
                                )
                            ),
                        )
                    error_data = loads_r.ok_value
                    if isinstance(error_data, dict):
                        try:
                            # Polymorphic decode preserves SubWorkflowError
                            # subclass fields (sub_workflow_id /
                            # sub_workflow_summary); TaskError.model_validate
                            # would silently downcast.
                            validated_err = decode_task_error(error_data)
                        except Exception as exc:
                            logger.warning(
                                'Workflow %s error payload validation failed: %s',
                                self.workflow_id,
                                exc,
                            )
                            return cast(
                                'TaskResult[OutT, TaskError]',
                                TaskResult(
                                    err=TaskError(
                                        error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                                        message=f'Workflow error payload validation failed: {exc}',
                                        data={'workflow_id': self.workflow_id},
                                    ),
                                ),
                            )
                        return cast(
                            'TaskResult[OutT, TaskError]',
                            TaskResult(err=validated_err),
                        )
                status_str = row.status if row else 'FAILED'
                _TERMINAL_STATUS_CODES: dict[str, OutcomeCode] = {
                    'FAILED': OutcomeCode.WORKFLOW_FAILED,
                    'CANCELLED': OutcomeCode.WORKFLOW_CANCELLED,
                }
                fallback_code: OutcomeCode | str = _TERMINAL_STATUS_CODES.get(
                    status_str, f'WORKFLOW_{status_str}',
                )
                return cast(
                    'TaskResult[OutT, TaskError]',
                    TaskResult(
                        err=TaskError(
                            error_code=fallback_code,
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
              error_code=RetrievalCode.RESULT_NOT_READY
            - If node has no node_id: returns TaskResult with
              error_code=ContractCode.WORKFLOW_CTX_MISSING_ID

        Example:
            result = handle.result_for(node)
            if result.is_err() and result.err.error_code == RetrievalCode.RESULT_NOT_READY:
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
            return cast(
                'TaskResult[OkT, TaskError]',
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

        try:
            async with self.broker.session_factory() as session:
                result = await session.execute(
                    GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL,
                    {'wf_id': self.workflow_id, 'node_id': node_id},
                )
                row = result.fetchone()
                if row is None or row.result is None:
                    return cast(
                        'TaskResult[OkT, TaskError]',
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
                    return cast('TaskResult[OkT, TaskError]', TaskResult(
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
                )
                return cast('TaskResult[OkT, TaskError]', decoded)
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
                    )
                return Ok(out)
        except SQLAlchemyError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.DB_OPERATION_FAILED,
                message=f'DB query failed for workflow {self.workflow_id} results: {exc}',
                retryable=is_retryable_connection_error(exc),
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
                            )

                    summary: SubWorkflowSummary[Any] | None = None
                    if row.sub_workflow_summary:
                        summary_loads_r = loads_json(row.sub_workflow_summary)
                        if is_err(summary_loads_r):
                            return Err(HandleOperationError(
                                code=HandleErrorCode.DB_OPERATION_FAILED,
                                message=(
                                    f'sub_workflow_summary JSON corrupt for node '
                                    f'{row.node_id}: {summary_loads_r.err_value}'
                                ),
                                retryable=False,
                                workflow_id=self.workflow_id,
                            ))
                        summary = SubWorkflowSummary.from_json(summary_loads_r.ok_value)

                    out.append(WorkflowTaskInfo(
                        node_id=row.node_id,
                        index=row.task_index,
                        name=row.task_name,
                        status=WorkflowTaskStatus(row.status),
                        result=task_result_value,
                        started_at=row.started_at,
                        completed_at=row.completed_at,
                        sub_workflow_id=row.sub_workflow_id,
                        sub_workflow_summary=summary,
                    ))
                return Ok(out)
        except SQLAlchemyError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.DB_OPERATION_FAILED,
                message=f'DB query failed for workflow {self.workflow_id} tasks: {exc}',
                retryable=is_retryable_connection_error(exc),
                workflow_id=self.workflow_id,
                exception=exc,
            ))

    # ─── cancel ──────────────────────────────────────────────────────

    def cancel(self) -> HandleResult[None]:
        """Request workflow cancellation."""
        return self._sync_call(self.cancel_async, 'cancel')

    async def cancel_async(self) -> HandleResult[None]:
        """Async version of cancel()."""
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
                        workflow_id=self.workflow_id,
                    ))

                status_val = WorkflowStatus(exists_row.status)
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

                await session.commit()
            return Ok(None)
        except SQLAlchemyError as exc:
            return Err(HandleOperationError(
                code=HandleErrorCode.DB_OPERATION_FAILED,
                message=f'Cancel failed for workflow {self.workflow_id}: {exc}',
                retryable=is_retryable_connection_error(exc),
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

        return await pause_workflow(self.broker, self.workflow_id)

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

        return await resume_workflow(self.broker, self.workflow_id)
