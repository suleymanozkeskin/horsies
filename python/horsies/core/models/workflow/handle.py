"""Workflow handle for tracking and retrieving results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    assert_never,
    cast,
)


from horsies.core.logging import get_logger
from horsies.core.utils.loop_runner import (
    async_context_message,
    in_running_event_loop,
)
from horsies.core.utils.wait_timeout import wait_timeout_seconds
from horsies.core.process_role import (
    ProcessRole,
    process_role,
    wait_inside_task_message,
)
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import (
    Json,
    TypeAnnotation,
    decode_task_error,
    decode_task_result,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.models.tasks import (
    TaskResult,
    TaskError,
    OperationalErrorCode,
    ContractCode,
    RetrievalCode,
    OutcomeCode,
)
from horsies.core.types.core_failure import CoreCallRefusal, call_refusal_or_raise
from horsies.core.types.result import Ok, Err, Result, is_err

from .context import SubWorkflowSummary
from .enums import (
    OkT,
    OutT,
    WorkflowStatus,
    WorkflowTaskStatus,
    stored_node_status,
)
from .handle_types import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleResult,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)
from .nodes import NodeKey

logger = get_logger('workflow.handle')


if TYPE_CHECKING:
    from uuid import UUID

    from horsies._core import (
        HandleFailed,
        NodeResultNotStored,
        StoredNodeResultRecord,
        WaitTimedOut,
        WorkflowAbsent,
        WorkflowCancelled,
        WorkflowCompletedRead,
        WorkflowEndedRead,
        WorkflowNodeResultsRead,
        WorkflowNodesRead,
        WorkflowNotCancellable,
        WorkflowPausedRead,
        WorkflowStatusRead,
    )
    from horsies.core.brokers.postgres import PostgresBroker
    from .nodes import TaskNode


# =============================================================================
# SQL constants for WorkflowHandle
# =============================================================================


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


def _wait_inside_task_result(workflow_id: str, operation: str) -> TaskResult[Any, TaskError]:
    """Build a TaskResult with WAIT_INSIDE_TASK for a workflow result wait.

    Nothing ran, and nothing waited. The result is not cached anywhere.
    """
    return TaskResult(
        err=TaskError(
            error_code=RetrievalCode.WAIT_INSIDE_TASK,
            message=wait_inside_task_message(
                f'WorkflowHandle.{operation}() for workflow {workflow_id}',
            ),
            data={'workflow_id': workflow_id},
        ),
    )


def _async_context_task_result(operation: str) -> TaskResult[Any, TaskError]:
    """Build a TaskResult with ASYNC_CONTEXT for a sync fold-strategy call.

    Nothing ran. The result is not cached anywhere: the ``*_async`` form
    reads the value.
    """
    return TaskResult(
        err=TaskError(
            error_code=RetrievalCode.ASYNC_CONTEXT,
            message=async_context_message(
                f'WorkflowHandle.{operation}()', f'handle.{operation}_async(...)',
            ),
        ),
    )


type _TerminalErrorStatus = Literal[
    WorkflowStatus.FAILED,
    WorkflowStatus.CANCELLED,
    WorkflowStatus.EXPIRED,
]
"""A terminal workflow status that ``get()`` reports as an error."""


def _terminal_outcome_code(status: _TerminalErrorStatus) -> OutcomeCode:
    """The ``OutcomeCode`` of a terminal error status without a stored error."""
    match status:
        case WorkflowStatus.FAILED:
            return OutcomeCode.WORKFLOW_FAILED
        case WorkflowStatus.CANCELLED:
            return OutcomeCode.WORKFLOW_CANCELLED
        case WorkflowStatus.EXPIRED:
            return OutcomeCode.WORKFLOW_EXPIRED
        case _ as unreachable:
            assert_never(unreachable)


def _completed_without_stored_result(workflow_id: str) -> TaskResult[Any, TaskError]:
    """``RESULT_DESERIALIZATION_ERROR`` for a ``COMPLETED`` workflow row whose
    result column is NULL.

    The library writes a result in the same statement that sets
    ``COMPLETED``: the output node's stored result, an error envelope for an
    output node with no result, the outputless envelope, or the JSON text
    ``null`` when that envelope does not serialize. So a NULL result is
    stored data that does not parse, not a success.
    """
    return TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
            message=(
                f'Workflow {workflow_id} is COMPLETED and its stored result '
                'is NULL'
            ),
            data={'workflow_id': workflow_id},
        ),
    )


def _decode_stored_workflow_error(
    workflow_id: str, status: _TerminalErrorStatus, stored_error: str,
) -> TaskResult[Any, TaskError]:
    """Decode a stored workflow error that is not NULL.

    Outcomes:
        - An error object that decodes: that ``TaskError``.
        - The plain expiry text that 0.5.0 and 0.5.1 stored for an
          ``EXPIRED`` workflow: ``OutcomeCode.WORKFLOW_EXPIRED``.
        - Text that is not JSON (an empty text included: no library writer
          stores one), JSON that is not an object, or an object that does
          not decode: ``RESULT_DESERIALIZATION_ERROR``, with the cause in the
          message.

    Raises:
        Exception: a defect in the decode. Only ``StrictJsonError`` and
            ``ValueValidationError``, the refusals of ``decode_task_error``,
            are folded.
    """
    loads_r = loads_json(stored_error)
    if is_err(loads_r):
        if (
            status is WorkflowStatus.EXPIRED
            and stored_error.startswith('paused_workflow_auto_cancel_after:')
        ):
            # 0.5.0/0.5.1 stored this policy outcome as plain text. Preserve
            # readability of already-expired workflows without treating
            # arbitrary corrupt JSON as a valid terminal error.
            return TaskResult(
                err=TaskError(
                    error_code=OutcomeCode.WORKFLOW_EXPIRED,
                    message=stored_error,
                ),
            )
        logger.warning(
            'Workflow %s error payload corrupt: %s',
            workflow_id,
            loads_r.err_value,
        )
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=f'Workflow error payload corrupt: {loads_r.err_value}',
                data={'workflow_id': workflow_id},
            ),
        )
    match loads_r.ok_value:
        case dict() as error_object:
            try:
                # Polymorphic decode preserves SubWorkflowError subclass
                # fields (sub_workflow_id / sub_workflow_summary);
                # TaskError.model_validate would silently downcast.
                validated_err = decode_task_error(error_object)
            except (StrictJsonError, ValueValidationError) as exc:
                # The two refusals decode_task_error raises for an object
                # that is not a TaskError. Any other exception is a defect
                # and propagates.
                logger.warning(
                    'Workflow %s error payload validation failed: %s',
                    workflow_id,
                    exc,
                )
                return TaskResult(
                    err=TaskError(
                        error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=f'Workflow error payload validation failed: {exc}',
                        data={'workflow_id': workflow_id},
                    ),
                )
            return TaskResult(err=validated_err)
        case not_an_object:
            logger.warning(
                'Workflow %s error payload is not a JSON object: %s',
                workflow_id,
                type(not_an_object).__name__,
            )
            return TaskResult(
                err=TaskError(
                    error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                    message=(
                        'Workflow error payload is not a JSON '
                        f'object: got {type(not_an_object).__name__}'
                    ),
                    data={'workflow_id': workflow_id},
                ),
            )


def _terminal_error_status(
    status: Literal['FAILED', 'CANCELLED', 'EXPIRED'],
) -> _TerminalErrorStatus:
    """The workflow status of an ended wait record. Pure."""
    match status:
        case 'FAILED':
            return WorkflowStatus.FAILED
        case 'CANCELLED':
            return WorkflowStatus.CANCELLED
        case 'EXPIRED':
            return WorkflowStatus.EXPIRED
        case _ as unreachable:
            assert_never(unreachable)


def _ended_workflow_error(
    workflow_id: str, status: _TerminalErrorStatus, stored_error: str | None,
) -> TaskResult[Any, TaskError]:
    """The answer of ``get()`` for a FAILED, CANCELLED or EXPIRED workflow and
    its stored error.

    Outcomes:
        - A NULL stored error (no stored error): the ``OutcomeCode`` of
          ``status``. A cancel does not write the error, and the failure
          finalize keeps the earlier error, which can be NULL.
        - Any other stored error, an empty text included: the outcome of
          ``_decode_stored_workflow_error``.
    """
    match stored_error:
        case None:
            return TaskResult(
                err=TaskError(
                    error_code=_terminal_outcome_code(status),
                    message=f'Workflow {status.value.lower()}',
                ),
            )
        case stored:
            return _decode_stored_workflow_error(workflow_id, status, stored)


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
    sub_definition_key: str | None = None,
) -> TaskResult[Any, TaskError]:
    """Decode a single workflow_task row's envelope using the source's OkT.

    Used by ``result_for``, ``results``, and ``tasks`` — each returns
    per-node ``TaskResult`` values. Failure folds into a
    ``RESULT_DESERIALIZATION_ERROR`` sentinel so the response stays
    typed.

    ``sub_definition_key`` (from the row's ``sub_definition_key`` column)
    is passed for SubWorkflowNode rows so OkT resolves via the unique
    workflow ``definition_key`` rather than the ambiguous ``task_name``.
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

    source_ok_type = resolve_source_ok_type(
        app, task_name, sub_definition_key=sub_definition_key,
    )
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
    except (StrictJsonError, ValueValidationError) as exc:
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


def _stored_node_result(
    stored: str | None,
    *,
    app: Any | None,
    task_name: str,
    node_id: str,
    sub_definition_key: str | None,
) -> TaskResult[Any, TaskError] | None:
    """The ``result`` of one node of ``tasks()``, from its stored result
    text. Pure apart from the type lookups of the decode.

    Outcomes:
        - NULL: ``None``. A node with no result stores NULL.
        - A text that is not JSON, an empty text included (no writer stores
          one): ``RESULT_DESERIALIZATION_ERROR`` naming the node and the
          cause.
        - JSON: the outcome of ``_decode_per_task_envelope``.
    """
    match stored:
        case None:
            return None
        case text:
            loads_r = loads_json(text)
            if is_err(loads_r):
                return TaskResult(
                    err=TaskError(
                        error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=f'Result JSON corrupt for node {node_id}: {loads_r.err_value}',
                    ),
                )
            return _decode_per_task_envelope(
                loads_r.ok_value,
                app=app,
                task_name=task_name,
                node_id=node_id,
                sub_definition_key=sub_definition_key,
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
        except (StrictJsonError, ValueValidationError) as exc:
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
        definition_keys = ok_slot.get('definition_key_by_id', {})
        if (
            not isinstance(results_raw, dict)
            or not isinstance(task_names, dict)
            or not isinstance(definition_keys, dict)
        ):
            return TaskResult(
                err=TaskError(
                    error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                    message=(
                        'Outputless workflow envelope missing or malformed '
                        'results_by_id / task_name_by_id / definition_key_by_id'
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

            sub_definition_key = definition_keys.get(node_id)
            source_ok_type = _resolve_src_ok_type(
                app, source_task_name, sub_definition_key=sub_definition_key,
            )
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
            except (StrictJsonError, ValueValidationError) as exc:
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
    except (StrictJsonError, ValueValidationError) as exc:
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

    Every sync method called on a thread that runs an event loop returns
    ``ASYNC_CONTEXT`` (``HandleAsyncContext`` for the wrap strategy,
    ``RetrievalCode`` for the fold strategy) and runs nothing. Use the
    ``*_async`` form there.

    Every method that calls the core, in both strategies, raises
    ``ForkedAfterCoreStart`` and ``CoreStopped``: they are states of the
    process, never a result (``call_refusal_or_raise``).
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

    # ─── sync refusal ────────────────────────────────────────────────

    def _async_context_err(self, operation: str) -> Err[HandleAsyncContext]:
        """The refusal of a wrap-strategy sync method called on a thread that
        runs an event loop. Nothing ran."""
        return Err(HandleAsyncContext(
            message=async_context_message(
                f'WorkflowHandle.{operation}()',
                f'handle.{operation}_async(...)',
            ),
            workflow_id=self.workflow_id,
        ))

    # ─── status ──────────────────────────────────────────────────────

    def status(self) -> HandleResult[WorkflowStatus]:
        """Get current workflow status: one read in the core
        (``CoreHandle.workflow_status``) on the calling thread, with the GIL
        released; the outcomes of ``status_async``.

        On a thread that runs an event loop it returns
        ``Err(HandleAsyncContext)`` and runs nothing.
        """
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
            workflow_uuid,
        )

        if in_running_event_loop():
            return self._async_context_err('status')
        match workflow_uuid(self.workflow_id, WorkflowOperation.STATUS):
            case Err(not_an_id):
                return Err(not_an_id)
            case Ok(workflow_key):
                pass
        try:
            record = self.broker._core_handle.workflow_status(  # pyright: ignore[reportPrivateUsage]
                workflow_key,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.STATUS,
            ))
        return self._status_answer(record)

    async def status_async(self) -> HandleResult[WorkflowStatus]:
        """Async version of status(): one read in the core
        (``CoreHandle.workflow_status_async``)."""
        return await self._status()

    async def _status(
        self,
    ) -> Result[
        WorkflowStatus,
        HandleWorkflowNotFound | HandleDbOperationFailed | HandleStoredDataInvalid,
    ]:
        """Read the workflow status in the core.

        ``HandleWorkflowNotFound`` when no workflow has the id;
        ``HandleDbOperationFailed`` for a failed read, a core boundary error,
        or an id that is not a UUID (not retryable); ``HandleStoredDataInvalid``
        for a stored status that is not a ``WorkflowStatus``.
        """
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
            workflow_uuid,
        )

        match workflow_uuid(self.workflow_id, WorkflowOperation.STATUS):
            case Err(not_an_id):
                return Err(not_an_id)
            case Ok(workflow_key):
                pass
        try:
            record = await self.broker._core_handle.workflow_status_async(  # pyright: ignore[reportPrivateUsage]
                workflow_key,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.STATUS,
            ))
        return self._status_answer(record)

    def _status_answer(
        self,
        record: WorkflowStatusRead | WorkflowAbsent | HandleFailed,
    ) -> Result[
        WorkflowStatus,
        HandleWorkflowNotFound | HandleDbOperationFailed | HandleStoredDataInvalid,
    ]:
        """The answer of ``status`` and ``status_async`` for the core's
        record. Pure."""
        from horsies._core import HandleFailed, WorkflowAbsent, WorkflowStatusRead
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            handle_failed_error,
        )

        match record:
            case WorkflowStatusRead(status=status):
                return Ok(WorkflowStatus(status))
            case WorkflowAbsent():
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=self.workflow_id,
                ))
            case HandleFailed():
                return Err(handle_failed_error(
                    record, self.workflow_id, WorkflowOperation.STATUS,
                ))
            case _ as unreachable:
                assert_never(unreachable)

    # ─── get ─────────────────────────────────────────────────────────

    def get(self, timeout_ms: int | None = None) -> TaskResult[OutT, TaskError]:
        """
        Block until workflow completes or timeout.

        The wait runs in the core (``CoreHandle.wait_workflow_result``) on the
        calling thread, with the GIL released and signals checked every
        100 ms: a raised signal handler (Ctrl+C) ends the wait with its
        exception, and the wait is dropped. A wait with a limit is bounded by
        the limit plus the core's bound of a blocking call; a wait with no
        limit has no call bound. The outcomes are those of ``get_async``.

        Returns:
            If output task specified: that task's TaskResult. A COMPLETED
            workflow whose output task is SKIPPED returns
            ``OutcomeCode.WORKFLOW_OUTPUT_SKIPPED``.
            Otherwise: TaskResult containing dict of terminal task results. A
            SKIPPED terminal task has an entry with that same code.
            In a process that runs task bodies: ``RetrievalCode.WAIT_INSIDE_TASK``
            at once, as the first check, with no wait.
            On a thread that runs an event loop: ``RetrievalCode.ASYNC_CONTEXT``,
            with no wait.
        """
        from horsies._core import CoreError

        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return cast(
                'TaskResult[OutT, TaskError]',
                _wait_inside_task_result(self.workflow_id, 'get'),
            )
        if in_running_event_loop():
            return cast('TaskResult[OutT, TaskError]', _async_context_task_result('get'))
        match self._wait_key(timeout_ms):
            case Err(err_value=refused):
                return cast('TaskResult[OutT, TaskError]', refused)
            case Ok(ok_value=workflow_key):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        try:
            record = self.broker._core_handle.wait_workflow_result(  # pyright: ignore[reportPrivateUsage]
                workflow_key, timeout_ms=timeout_ms,
            )
        except CoreError as exc:
            return cast('TaskResult[OutT, TaskError]', self._get_call_failed(call_refusal_or_raise(exc)))
        return cast('TaskResult[OutT, TaskError]', self._get_answer(record, timeout_ms))

    async def get_async(
        self, timeout_ms: int | None = None,
    ) -> TaskResult[OutT, TaskError]:
        """Async version of get().

        In a process that runs task bodies it returns
        ``RetrievalCode.WAIT_INSIDE_TASK`` at once, as its first check, with
        no wait. Elsewhere, a timeout_ms that is not None or an int from 0 to
        MAX_TASK_TIMEOUT_MS returns ``RetrievalCode.INVALID_INPUT`` before any
        read. The wait runs in the core (``CoreHandle.wait_workflow_result_async``):
        a finished workflow is one read; a running one is read again on its
        ``workflow_done`` wake, and at least every 5 s. The handle decodes the
        stored result with its ``out_type``.
        """
        from horsies._core import CoreError

        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return cast(
                'TaskResult[OutT, TaskError]',
                _wait_inside_task_result(self.workflow_id, 'get_async'),
            )
        match self._wait_key(timeout_ms):
            case Err(err_value=refused):
                return cast('TaskResult[OutT, TaskError]', refused)
            case Ok(ok_value=workflow_key):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        try:
            record = await self.broker._core_handle.wait_workflow_result_async(  # pyright: ignore[reportPrivateUsage]
                workflow_key, timeout_ms=timeout_ms,
            )
        except CoreError as exc:
            return cast('TaskResult[OutT, TaskError]', self._get_call_failed(call_refusal_or_raise(exc)))
        return cast('TaskResult[OutT, TaskError]', self._get_answer(record, timeout_ms))

    def _wait_key(
        self, timeout_ms: int | None,
    ) -> Result[UUID, TaskResult[Any, TaskError]]:
        """The workflow id of a wait as a UUID, or the answer that refuses the
        wait before any read. Pure.

        - A timeout_ms that is not None or an int from 0 to
          MAX_TASK_TIMEOUT_MS: ``RetrievalCode.INVALID_INPUT``.
        - An id that is not a UUID: ``BROKER_ERROR``.
        """
        from horsies.core.workflows.handle_records import WorkflowOperation, workflow_uuid

        # timeout_ms=0 means "0ms budget": return the result if already
        # available, else WAIT_TIMEOUT immediately. Only None disables the
        # timeout. Matches PostgresBroker.get_result_async.
        match wait_timeout_seconds(timeout_ms):
            case Err(err_value=refused):
                return Err(TaskResult(
                    err=TaskError(
                        error_code=RetrievalCode.INVALID_INPUT,
                        message=(
                            f'WorkflowHandle.get() for workflow '
                            f'{self.workflow_id}: {refused.message}'
                        ),
                        data={'workflow_id': self.workflow_id},
                    ),
                ))
            case Ok():
                pass
        match workflow_uuid(self.workflow_id, WorkflowOperation.GET):
            case Err(not_an_id):
                return Err(_broker_task_error(not_an_id.message))
            case Ok(workflow_key):
                return Ok(workflow_key)

    def _get_call_failed(self, exc: CoreCallRefusal) -> TaskResult[Any, TaskError]:
        """The answer of a wait whose core call raised a core boundary error:
        ``BROKER_ERROR`` with its type and text. Pure."""
        return _broker_task_error(
            f'Get failed for workflow {self.workflow_id}: {type(exc).__name__}: {exc}',
        )

    def _get_answer(
        self,
        record: (
            WorkflowCompletedRead
            | WorkflowEndedRead
            | WorkflowPausedRead
            | WaitTimedOut
            | WorkflowAbsent
            | HandleFailed
        ),
        timeout_ms: int | None,
    ) -> TaskResult[Any, TaskError]:
        """The answer of ``get`` and ``get_async`` for the core's record. Pure
        apart from the type lookups of the decode."""
        from horsies._core import (
            HandleFailed,
            WaitTimedOut,
            WorkflowAbsent,
            WorkflowCompletedRead,
            WorkflowEndedRead,
            WorkflowPausedRead,
        )

        match record:
            case WorkflowCompletedRead(result_json=stored_result):
                return self._completed_result(stored_result)
            case WorkflowEndedRead(status=ended, error_json=stored_error):
                return _ended_workflow_error(
                    self.workflow_id, _terminal_error_status(ended), stored_error,
                )
            case WorkflowPausedRead():
                return TaskResult(
                    err=TaskError(
                        error_code=OutcomeCode.WORKFLOW_PAUSED,
                        message='Workflow is paused awaiting intervention',
                    ),
                )
            case WaitTimedOut():
                return TaskResult(
                    err=TaskError(
                        error_code=RetrievalCode.WAIT_TIMEOUT,
                        message=f'Workflow did not complete within {timeout_ms}ms',
                    ),
                )
            case WorkflowAbsent():
                return TaskResult(
                    err=TaskError(
                        error_code=RetrievalCode.WORKFLOW_NOT_FOUND,
                        message=f'Workflow {self.workflow_id} not found',
                    ),
                )
            case HandleFailed(message=message):
                return _broker_task_error(
                    f'Get failed for workflow {self.workflow_id}: {message}',
                )
            case _ as unreachable:
                assert_never(unreachable)

    def _completed_result(self, stored_result: str | None) -> TaskResult[Any, TaskError]:
        """The answer of ``get()`` for a COMPLETED workflow and its stored result. Pure
        apart from the type lookups of the decode.

        Outcomes:
            - A NULL stored result: ``RESULT_DESERIALIZATION_ERROR``. The
              library always stores a result with ``COMPLETED``.
            - A stored result that is not JSON, an empty text included:
              ``RESULT_DESERIALIZATION_ERROR``.
            - A stored result that is JSON: the outcome of
              ``_decode_workflow_envelope``.
        """
        match stored_result:
            case None:
                return _completed_without_stored_result(self.workflow_id)
            case stored:
                loads_r = loads_json(stored)
                if is_err(loads_r):
                    return TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                            message=f'Workflow result JSON corrupt: {loads_r.err_value}',
                        ),
                    )
                return _decode_workflow_envelope(
                    loads_r.ok_value,
                    self.out_type,
                    app=self.broker.app,
                    workflow_id=self.workflow_id,
                )

    # ─── result_for ──────────────────────────────────────────────────

    def result_for(
        self, node: TaskNode[OkT] | NodeKey[OkT],
    ) -> TaskResult[OkT, TaskError]:
        """
        Get the result for a specific TaskNode or NodeKey.

        Non-blocking: queries the database once and returns immediately. The
        read runs in the core (``CoreHandle.workflow_node_result``) on the
        calling thread, with the GIL released; the outcomes are those of
        ``result_for_async``. On a thread that runs an event loop it returns
        ``RetrievalCode.ASYNC_CONTEXT`` and reads nothing.

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
        from horsies._core import CoreError

        if in_running_event_loop():
            return cast('TaskResult[OkT, TaskError]', _async_context_task_result('result_for'))
        match self._node_result_key(node):
            case Err(err_value=refused):
                return cast('TaskResult[OkT, TaskError]', refused)
            case Ok(ok_value=(workflow_key, node_id)):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        try:
            record = self.broker._core_handle.workflow_node_result(  # pyright: ignore[reportPrivateUsage]
                workflow_key, node_id,
            )
        except CoreError as exc:
            return cast(
                'TaskResult[OkT, TaskError]', self._node_result_call_failed(node_id, call_refusal_or_raise(exc)),
            )
        return cast('TaskResult[OkT, TaskError]', self._node_result_answer(node_id, record))

    async def result_for_async(
        self, node: TaskNode[OkT] | NodeKey[OkT],
    ) -> TaskResult[OkT, TaskError]:
        """Async version of result_for(). See result_for() for full documentation.

        One read in the core (``CoreHandle.workflow_node_result_async``); the
        handle decodes the stored result.
        """
        from horsies._core import CoreError

        match self._node_result_key(node):
            case Err(err_value=refused):
                return cast('TaskResult[OkT, TaskError]', refused)
            case Ok(ok_value=(workflow_key, node_id)):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        try:
            record = await self.broker._core_handle.workflow_node_result_async(  # pyright: ignore[reportPrivateUsage]
                workflow_key, node_id,
            )
        except CoreError as exc:
            return cast(
                'TaskResult[OkT, TaskError]', self._node_result_call_failed(node_id, call_refusal_or_raise(exc)),
            )
        return cast('TaskResult[OkT, TaskError]', self._node_result_answer(node_id, record))

    def _node_result_key(
        self, node: TaskNode[Any] | NodeKey[Any],
    ) -> Result[tuple[UUID, str], TaskResult[Any, TaskError]]:
        """The workflow id as a UUID and the node id of a result read, or the
        answer that refuses the read before any read. Pure.

        - A node with no node_id: ``ContractCode.WORKFLOW_CTX_MISSING_ID``.
        - A workflow id that is not a UUID: ``BROKER_ERROR``.
        """
        from horsies.core.workflows.handle_records import WorkflowOperation, workflow_uuid

        node_id: str | None
        if isinstance(node, NodeKey):
            node_id = node.node_id
        else:
            node_id = node.node_id

        if node_id is None:
            return Err(TaskResult(
                err=TaskError(
                    error_code=ContractCode.WORKFLOW_CTX_MISSING_ID,
                    message=(
                        'TaskNode node_id is not set. Ensure WorkflowSpec assigns '
                        'node_id or provide an explicit node_id.'
                    ),
                ),
            ))
        match workflow_uuid(self.workflow_id, WorkflowOperation.RESULT_FOR):
            case Err(not_an_id):
                return Err(_broker_task_error(not_an_id.message))
            case Ok(workflow_key):
                return Ok((workflow_key, node_id))

    def _node_result_call_failed(
        self, node_id: str, exc: CoreCallRefusal,
    ) -> TaskResult[Any, TaskError]:
        """The answer of a result read whose core call raised a core boundary
        error: ``BROKER_ERROR`` with its type and text. Pure."""
        return _broker_task_error(
            f'DB query failed for result_for node {node_id} in workflow '
            f'{self.workflow_id}: {type(exc).__name__}: {exc}',
        )

    def _node_result_answer(
        self,
        node_id: str,
        record: StoredNodeResultRecord | NodeResultNotStored | HandleFailed,
    ) -> TaskResult[Any, TaskError]:
        """The answer of ``result_for`` and ``result_for_async`` for the core's
        record. Pure apart from the type lookups of the decode."""
        from horsies._core import (
            HandleFailed,
            NodeResultNotStored,
            StoredNodeResultRecord,
        )

        failure_subject = f'result_for node {node_id} in workflow {self.workflow_id}'
        match record:
            case NodeResultNotStored():
                return TaskResult(
                    err=TaskError(
                        error_code=RetrievalCode.RESULT_NOT_READY,
                        message=(
                            f"Task '{node_id}' has not completed yet "
                            f"in workflow '{self.workflow_id}'"
                        ),
                    )
                )
            case HandleFailed(message=message):
                return _broker_task_error(f'DB query failed for {failure_subject}: {message}')
            case StoredNodeResultRecord():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        loads_r = loads_json(record.result_json)
        if is_err(loads_r):
            return TaskResult(
                err=TaskError(
                    error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                    message=f'Result JSON corrupt for node {node_id}: {loads_r.err_value}',
                ),
            )
        return _decode_per_task_envelope(
            loads_r.ok_value,
            app=self.broker.app,
            task_name=record.task_name,
            node_id=node_id,
            sub_definition_key=(
                record.sub_definition_key if record.is_subworkflow else None
            ),
        )

    # ─── results ─────────────────────────────────────────────────────

    def results(self) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]:
        """
        Get all task results keyed by unique identifier.

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".

        ``Ok({})``: the workflow exists and no node has a stored result yet.
        ``Err(HandleWorkflowNotFound)``: no workflow has this id.

        One read in the core (``CoreHandle.workflow_node_results``) on the
        calling thread, with the GIL released; the outcomes of
        ``results_async``. On a thread that runs an event loop it returns
        ``Err(HandleAsyncContext)`` and runs nothing.
        """
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
            workflow_uuid,
        )

        if in_running_event_loop():
            return self._async_context_err('results')
        match workflow_uuid(self.workflow_id, WorkflowOperation.RESULTS):
            case Err(not_an_id):
                return Err(not_an_id)
            case Ok(workflow_key):
                pass
        try:
            record = self.broker._core_handle.workflow_node_results(  # pyright: ignore[reportPrivateUsage]
                workflow_key,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.RESULTS,
            ))
        return self._results_answer(record)

    async def results_async(self) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]:
        """
        Async version of results(): one read in the core
        (``CoreHandle.workflow_node_results_async``); the handle decodes each
        stored result.

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".
        """
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
            workflow_uuid,
        )

        match workflow_uuid(self.workflow_id, WorkflowOperation.RESULTS):
            case Err(not_an_id):
                return Err(not_an_id)
            case Ok(workflow_key):
                pass
        try:
            record = await self.broker._core_handle.workflow_node_results_async(  # pyright: ignore[reportPrivateUsage]
                workflow_key,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.RESULTS,
            ))
        return self._results_answer(record)

    def _results_answer(
        self,
        record: WorkflowNodeResultsRead | WorkflowAbsent | HandleFailed,
    ) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]:
        """The answer of ``results`` and ``results_async`` for the core's record.
        Pure apart from the type lookups of the decode."""
        from horsies._core import HandleFailed, WorkflowAbsent, WorkflowNodeResultsRead
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            handle_failed_error,
        )

        match record:
            case WorkflowAbsent():
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=self.workflow_id,
                ))
            case HandleFailed():
                return Err(handle_failed_error(
                    record, self.workflow_id, WorkflowOperation.RESULTS,
                ))
            case WorkflowNodeResultsRead(nodes=nodes):
                pass
            case _ as unreachable:
                assert_never(unreachable)

        out: dict[str, TaskResult[Any, TaskError]] = {}
        for node in nodes:
            # The column is nullable; every spec assigns a node id, and the
            # read before the switch used the stored value as it is.
            node_id = cast(str, node.node_id)
            loads_r = loads_json(node.result_json)
            if is_err(loads_r):
                out[node_id] = TaskResult(
                    err=TaskError(
                        error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=f'Result JSON corrupt for node {node.node_id}: {loads_r.err_value}',
                    ),
                )
                continue
            out[node_id] = _decode_per_task_envelope(
                loads_r.ok_value,
                app=self.broker.app,
                task_name=node.task_name,
                node_id=node_id,
                sub_definition_key=(
                    node.sub_definition_key if node.is_subworkflow else None
                ),
            )
        return Ok(out)

    # ─── tasks ───────────────────────────────────────────────────────

    def tasks(self) -> HandleResult[list[WorkflowTaskInfo]]:
        """Get status of all tasks in workflow.

        ``Ok([])``: the workflow exists and has no node.
        ``Err(HandleWorkflowNotFound)``: no workflow has this id.
        ``Err(HandleStoredDataInvalid)``: a stored sub-workflow summary does
        not read, an empty text included. A node without a summary stores
        NULL and has ``sub_workflow_summary=None``.

        A node without a result stores NULL and has ``result=None``. A stored
        result that does not read, an empty text included, gives that node a
        ``result`` with ``RESULT_DESERIALIZATION_ERROR``.

        One read in the core (``CoreHandle.workflow_nodes``) on the calling
        thread, with the GIL released; the outcomes of ``tasks_async``. On a
        thread that runs an event loop it returns ``Err(HandleAsyncContext)``
        and runs nothing.
        """
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
            workflow_uuid,
        )

        if in_running_event_loop():
            return self._async_context_err('tasks')
        match workflow_uuid(self.workflow_id, WorkflowOperation.TASKS):
            case Err(not_an_id):
                return Err(not_an_id)
            case Ok(workflow_key):
                pass
        try:
            record = self.broker._core_handle.workflow_nodes(  # pyright: ignore[reportPrivateUsage]
                workflow_key,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.TASKS,
            ))
        return self._tasks_answer(record)

    async def tasks_async(self) -> HandleResult[list[WorkflowTaskInfo]]:
        """Async version of tasks(): one read in the core
        (``CoreHandle.workflow_nodes_async``); the handle decodes each stored
        result and summary."""
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
            workflow_uuid,
        )

        match workflow_uuid(self.workflow_id, WorkflowOperation.TASKS):
            case Err(not_an_id):
                return Err(not_an_id)
            case Ok(workflow_key):
                pass
        try:
            record = await self.broker._core_handle.workflow_nodes_async(  # pyright: ignore[reportPrivateUsage]
                workflow_key,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.TASKS,
            ))
        return self._tasks_answer(record)

    def _tasks_answer(
        self,
        record: WorkflowNodesRead | WorkflowAbsent | HandleFailed,
    ) -> HandleResult[list[WorkflowTaskInfo]]:
        """The answer of ``tasks`` and ``tasks_async`` for the core's record.
        Pure apart from the type lookups of the decode."""
        from horsies._core import HandleFailed, WorkflowAbsent, WorkflowNodesRead
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            handle_failed_error,
        )

        match record:
            case WorkflowAbsent():
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=self.workflow_id,
                ))
            case HandleFailed():
                return Err(handle_failed_error(
                    record, self.workflow_id, WorkflowOperation.TASKS,
                ))
            case WorkflowNodesRead(nodes=nodes):
                pass
            case _ as unreachable:
                assert_never(unreachable)

        out: list[WorkflowTaskInfo] = []
        for row in nodes:
            # The column is nullable; every spec assigns a node id, and the
            # read before the switch used the stored value as it is.
            node_id = cast(str, row.node_id)
            task_result_value = _stored_node_result(
                row.result_json,
                app=self.broker.app,
                task_name=row.task_name,
                node_id=node_id,
                sub_definition_key=(
                    row.sub_definition_key if row.is_subworkflow else None
                ),
            )

            summary: SubWorkflowSummary[Any] | None
            match row.sub_workflow_summary_json:
                case None:
                    summary = None
                case summary_json:
                    summary_loads_r = loads_json(summary_json)
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
                        summary = SubWorkflowSummary.from_json(summary_loads_r.ok_value)
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
                    node_id=node_id,
                    stored=row.status,
                ),
                result=task_result_value,
                started_at=row.started_at,
                completed_at=row.completed_at,
                sub_workflow_id=(
                    None if row.sub_workflow_id is None else str(row.sub_workflow_id)
                ),
                sub_workflow_summary=summary,
            ))
        return Ok(out)

    # ─── cancel ──────────────────────────────────────────────────────

    def cancel(self) -> HandleResult[None]:
        """Request workflow cancellation.

        One statement in the core (``CoreHandle.cancel_workflow``) on the
        calling thread, with the GIL released; the outcomes of
        ``cancel_async``. A raised signal handler (Ctrl+C) leaves the call;
        the cancel runs on to its end. On a thread that runs an event loop it
        returns ``Err(HandleAsyncContext)`` and runs nothing.
        """
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
            workflow_uuid,
        )

        if in_running_event_loop():
            return self._async_context_err('cancel')
        match workflow_uuid(self.workflow_id, WorkflowOperation.CANCEL):
            case Err(not_an_id):
                return Err(not_an_id)
            case Ok(workflow_key):
                pass
        try:
            record = self.broker._core_handle.cancel_workflow(  # pyright: ignore[reportPrivateUsage]
                workflow_key,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.CANCEL,
            ))
        return self._cancel_answer(record)

    async def cancel_async(self) -> HandleResult[None]:
        """Async version of cancel().

        One statement in autocommit (``horsies_workflow_cancel``) that the
        core runs on the broker's core handle
        (``CoreHandle.cancel_workflow_async``); a cancel of the caller does
        not stop it. The workflow and every PENDING, RUNNING or PAUSED
        descendant become CANCELLED with ``completed_at``; the tasks behind
        ENQUEUED nodes are cancelled; PENDING and READY nodes, and the
        ENQUEUED nodes of the cancelled tasks, become SKIPPED. A node whose
        task already started stays RUNNING until the task ends.

        Returns:
            ``Ok(None)`` when the tree is cancelled, or when the workflow is
            already terminal (nothing written).
            ``Err(HandleWorkflowNotFound)`` when no workflow has the id.
            ``Err(HandleDbOperationFailed)`` on a database error or a core
            boundary error: the statement wrote nothing, or its commit
            outcome is unknown. An id that is not a UUID is not retryable.
            ``Err(HandleStoredDataInvalid)`` when a stored value does not
            parse.

        Raises:
            PanicException: the function returned a row outside its contract
                (a defect of the core).
        """
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
            workflow_uuid,
        )

        match workflow_uuid(self.workflow_id, WorkflowOperation.CANCEL):
            case Err(not_an_id):
                return Err(not_an_id)
            case Ok(workflow_key):
                pass
        try:
            record = await self.broker._core_handle.cancel_workflow_async(  # pyright: ignore[reportPrivateUsage]
                workflow_key,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.CANCEL,
            ))
        return self._cancel_answer(record)

    def _cancel_answer(
        self,
        record: WorkflowCancelled | WorkflowNotCancellable | WorkflowAbsent | HandleFailed,
    ) -> HandleResult[None]:
        """The answer of ``cancel`` and ``cancel_async`` for the core's record.
        Pure apart from the type lookups of the decode."""
        from horsies._core import HandleFailed, WorkflowAbsent, WorkflowCancelled, WorkflowNotCancellable
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            handle_failed_error,
        )

        # The CANCELLED status write fires horsies_workflow_notify_trigger
        # per workflow row, so no workflow_done notify is sent here.
        match record:
            case WorkflowCancelled() | WorkflowNotCancellable():
                return Ok(None)
            case WorkflowAbsent():
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=self.workflow_id,
                ))
            case HandleFailed():
                return Err(handle_failed_error(
                    record, self.workflow_id, WorkflowOperation.CANCEL,
                ))
            case _ as unreachable:
                assert_never(unreachable)

    # ─── pause ───────────────────────────────────────────────────────

    def pause(self) -> HandleResult[bool]:
        """
        Pause a running workflow.

        Transitions workflow from RUNNING to PAUSED state. Already-running tasks
        will continue to completion, but no new tasks will be enqueued.

        Use resume() to continue execution.

        One statement in the core (``CoreHandle.pause_workflow``) on the
        calling thread, with the GIL released; the outcomes of
        ``pause_async``. A raised signal handler (Ctrl+C) leaves the call; the
        pause runs on to its end. On a thread that runs an event loop it
        returns ``Err(HandleAsyncContext)`` and runs nothing.

        Returns:
            Ok(True) if workflow was paused, Ok(False) if not RUNNING (no-op).
            Err(HandleOperationError) on failure: match on the variant.
        """
        from horsies.core.workflows.lifecycle import pause_workflow_blocking

        if in_running_event_loop():
            return self._async_context_err('pause')
        return pause_workflow_blocking(self.broker, self.workflow_id)

    async def pause_async(self) -> HandleResult[bool]:
        """Async version of pause().

        Returns:
            Ok(True) if workflow was paused, Ok(False) if not RUNNING (no-op).
            Err(HandleOperationError) on failure: match on the variant.
        """
        from horsies.core.workflows.engine import pause_workflow

        return await pause_workflow(self.broker, self.workflow_id)

    # ─── resume ──────────────────────────────────────────────────────

    def resume(self) -> HandleResult[bool]:
        """
        Resume a paused workflow.

        Re-evaluates all PENDING tasks (marks READY if deps are terminal) and
        enqueues all READY tasks. Only works if workflow is currently PAUSED.

        The resume runs in the core (``CoreHandle.resume_workflow``) on the
        calling thread, with the GIL released; the outcomes and raises of
        ``resume_async``. The child starts it owes build their children by
        their ``build_with`` on the core's callback thread. A raised signal
        handler (Ctrl+C) leaves the call; the resume runs on to its end. On a
        thread that runs an event loop it returns ``Err(HandleAsyncContext)``
        and runs nothing.

        Returns:
            Ok(True) if workflow was resumed, Ok(False) if not PAUSED (no-op).
            Err(HandleOperationError) on failure: match on the variant.
        """
        from horsies.core.workflows.lifecycle import resume_workflow_blocking

        if in_running_event_loop():
            return self._async_context_err('resume')
        return resume_workflow_blocking(self.broker, self.workflow_id)

    async def resume_async(self) -> HandleResult[bool]:
        """Async version of resume().

        Returns:
            Ok(True) if workflow was resumed, Ok(False) if not PAUSED (no-op).
            Err(HandleOperationError) on failure: match on the variant.
        """
        from horsies.core.workflows.engine import resume_workflow

        return await resume_workflow(self.broker, self.workflow_id)
