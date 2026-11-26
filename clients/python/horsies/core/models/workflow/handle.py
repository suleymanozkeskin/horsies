"""Workflow handle for tracking and retrieving results."""

from __future__ import annotations

import uuid
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
from horsies.core.models.outcome import (
    AsyncContext,
    BrokerError,
    Failed,
    InvalidInput,
    NoTypeAvailable,
    ReadError,
    ReadResult,
    ResultDeserializationError,
    ResultNotReady,
    Succeeded,
    TaskOutcome,
    UnknownCustomCode,
    WaitInsideTask,
    WaitTimeout,
    WorkflowCtxMissingId,
    WorkflowNotFound,
)
from horsies.core.models.tasks import (
    TaskError,
    OperationalErrorCode,
    OutcomeCode,
)
from horsies.core.task_read import ok_of_outcome, typed_outcome
from horsies.core.types.core_failure import (
    CoreCallRefusal,
    call_refusal_or_raise,
    core_error_retryable,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.json_value import JsonInput
from horsies.core.types.result import Ok, Err, Result, is_err

from .context import SubWorkflowSummary
from .enums import (
    CodeT,
    OkT,
    OutT,
    WorkflowStatus,
    WorkflowTaskStatus,
    stored_node_status,
)
from .handle_types import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleInvalidWorkflowId,
    HandleResult,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)
from .nodes import NodeKey

logger = get_logger('workflow.handle')


if TYPE_CHECKING:
    from uuid import UUID

    from horsies._core import (
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
    result: TaskOutcome[Any, TaskError[Any]] | None
    started_at: datetime | None
    completed_at: datetime | None
    sub_workflow_id: str | None = None
    sub_workflow_summary: SubWorkflowSummary[Any] | None = None


def _broker_read_error(
    message: str, retryable: bool, exception: BaseException | NoCause,
) -> Err[ReadError]:
    """``BrokerError`` of a read whose core call or database read failed."""
    return Err(BrokerError(message=message, retryable=retryable, exception=exception))


def _unreadable(message: str) -> Err[ReadError]:
    """``ResultDeserializationError`` of a stored value that does not read."""
    return Err(ResultDeserializationError(message=message))


def _wait_inside_task_result(workflow_id: str, operation: str) -> Err[ReadError]:
    """``WaitInsideTask`` for a workflow result wait.

    Nothing ran, and nothing waited. The result is not cached anywhere.
    """
    return Err(WaitInsideTask(message=wait_inside_task_message(
        f'WorkflowHandle.{operation}() for workflow {workflow_id}',
    )))


def _async_context_task_result(operation: str) -> Err[ReadError]:
    """``AsyncContext`` for a sync read on a thread that runs an event loop.

    Nothing ran. The result is not cached anywhere: the ``*_async`` form
    reads the value.
    """
    return Err(AsyncContext(message=async_context_message(
        f'WorkflowHandle.{operation}()', f'handle.{operation}_async(...)',
    )))


type _TerminalErrorStatus = Literal[
    WorkflowStatus.FAILED,
    WorkflowStatus.CANCELLED,
    WorkflowStatus.EXPIRED,
]
"""A terminal workflow status that ``get()`` reports as a failed outcome."""


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


def _completed_without_stored_result(workflow_id: str) -> Err[ReadError]:
    """``ResultDeserializationError`` for a ``COMPLETED`` workflow row whose
    result column is NULL.

    The library writes a result in the same statement that sets
    ``COMPLETED``: the output node's stored result, an error envelope for an
    output node with no result, the outputless envelope, or the JSON text
    ``null`` when that envelope does not serialize. So a NULL result is
    stored data that does not parse, not a success.
    """
    return _unreadable(
        f'Workflow {workflow_id} is COMPLETED and its stored result is NULL',
    )


def _decode_stored_workflow_error(
    workflow_id: str, status: _TerminalErrorStatus, stored_error: str,
) -> ReadResult[Any, TaskError]:
    """Decode a stored workflow error that is not NULL.

    Outcomes:
        - An error object that decodes: ``Ok(Failed(task_error))``.
        - The plain expiry text that 0.5.0 and 0.5.1 stored for an
          ``EXPIRED`` workflow: ``Ok(Failed(WORKFLOW_EXPIRED))``.
        - Text that is not JSON (an empty text included: no library writer
          stores one), JSON that is not an object, or an object that does
          not decode: ``Err(ResultDeserializationError)``, with the cause in
          the message.

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
            return Ok(Failed(TaskError(
                code=OutcomeCode.WORKFLOW_EXPIRED,
                message=stored_error,
            )))
        logger.warning(
            'Workflow %s error payload corrupt: %s',
            workflow_id,
            loads_r.err_value,
        )
        return _unreadable(f'Workflow error payload corrupt: {loads_r.err_value}')
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
                return _unreadable(f'Workflow error payload validation failed: {exc}')
            return Ok(Failed(validated_err))
        case not_an_object:
            logger.warning(
                'Workflow %s error payload is not a JSON object: %s',
                workflow_id,
                type(not_an_object).__name__,
            )
            return _unreadable(
                'Workflow error payload is not a JSON '
                f'object: got {type(not_an_object).__name__}',
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
) -> ReadResult[Any, TaskError]:
    """The answer of ``get()`` for a FAILED, CANCELLED or EXPIRED workflow and
    its stored error.

    Outcomes:
        - A NULL stored error (no stored error): ``Ok(Failed(...))`` with the
          ``OutcomeCode`` of ``status``. A cancel does not write the error,
          and the failure finalize keeps the earlier error, which can be NULL.
        - Any other stored error, an empty text included: the outcome of
          ``_decode_stored_workflow_error``.
    """
    match stored_error:
        case None:
            return Ok(Failed(TaskError(
                code=_terminal_outcome_code(status),
                message=f'Workflow {status.value.lower()}',
            )))
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


@dataclass(frozen=True, slots=True)
class _NodeUnreadable:
    """A stored node result that does not read: the text and the data of its
    ``RESULT_DESERIALIZATION_ERROR``."""

    message: str
    data: JsonInput

    def as_outcome(self) -> Failed[TaskError]:
        """The entry of the node in a map of node outcomes."""
        return Failed(TaskError(
            code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
            message=self.message,
            data=self.data,
        ))


def _unknown_code_outcome(node_id: str, unknown: UnknownCustomCode) -> Failed[TaskError]:
    """The entry of a node whose stored custom code its task's ``Enum`` does
    not name, in a map of node outcomes."""
    return Failed(TaskError(
        code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
        message=f'node {node_id!r}: {unknown.message}',
        data={'node_id': node_id, 'custom_code': unknown.custom_code},
    ))


def _folded(
    node_id: str,
    decoded: Result[TaskOutcome[Any, TaskError[Any]], _NodeUnreadable | UnknownCustomCode],
) -> TaskOutcome[Any, TaskError[Any]]:
    """The entry of a node in a map of node outcomes (``results``,
    ``tasks``): a node that does not read, or whose stored custom code its
    task's ``Enum`` does not name, is a failed outcome with
    ``RESULT_DESERIALIZATION_ERROR``, so the other entries stay readable."""
    match decoded:
        case Ok(ok_value=outcome):
            return outcome
        case Err(err_value=_NodeUnreadable() as unreadable):
            return unreadable.as_outcome()
        case Err(err_value=UnknownCustomCode() as unknown):
            return _unknown_code_outcome(node_id, unknown)


def _decode_per_task_envelope(
    loaded: Json,
    *,
    app: Any | None,
    task_name: str | None,
    node_id: str,
    sub_definition_key: str | None = None,
) -> Result[TaskOutcome[Any, TaskError[Any]], _NodeUnreadable | UnknownCustomCode]:
    """Decode a single workflow_task row's envelope using the source's OkT
    and the source task's custom code type.

    Used by ``result_for``, ``results``, and ``tasks``. A node that does not
    read is ``Err(_NodeUnreadable)``, and a stored custom code that the
    source task's ``Enum`` does not name is ``Err(UnknownCustomCode)``:
    ``result_for`` returns them as read failures, and the maps of
    ``results`` and ``tasks`` fold them into the node's entry
    (``_folded``).

    ``sub_definition_key`` (from the row's ``sub_definition_key`` column)
    is passed for SubWorkflowNode rows so OkT resolves via the unique
    workflow ``definition_key`` rather than the ambiguous ``task_name``.
    """
    if not isinstance(task_name, str):
        return Err(_NodeUnreadable(
            message=f'Workflow task node {node_id!r} is missing task_name',
            data={'node_id': node_id},
        ))
    from .typing_utils import resolve_source_code_type, resolve_source_ok_type

    source_ok_type = resolve_source_ok_type(
        app, task_name, sub_definition_key=sub_definition_key,
    )
    if source_ok_type is None:
        return Err(_NodeUnreadable(
            message=(
                f'Source {task_name!r} not registered (no task or '
                f'workflow definition with this name) (node {node_id!r})'
            ),
            data={'node_id': node_id, 'task_name': task_name},
        ))
    try:
        outcome = decode_task_result(loaded, source_ok_type)
    except (StrictJsonError, ValueValidationError) as exc:
        return Err(_NodeUnreadable(
            message=f'decode_task_result failed for node {node_id!r}: {exc}',
            data={'node_id': node_id, 'task_name': task_name},
        ))
    return typed_outcome(
        outcome,
        resolve_source_code_type(app, task_name, sub_definition_key=sub_definition_key),
    )


def _stored_node_decode(
    stored: str,
    *,
    app: Any | None,
    task_name: str | None,
    node_id: str,
    sub_definition_key: str | None,
) -> Result[TaskOutcome[Any, TaskError[Any]], _NodeUnreadable | UnknownCustomCode]:
    """Decode the stored result text of one node. Pure apart from the type
    lookups of the decode.

    A text that is not JSON, an empty text included (no writer stores one),
    is ``_NodeUnreadable`` naming the node and the cause. JSON goes to
    ``_decode_per_task_envelope``.
    """
    loads_r = loads_json(stored)
    if is_err(loads_r):
        return Err(_NodeUnreadable(
            message=f'Result JSON corrupt for node {node_id}: {loads_r.err_value}',
            data=None,
        ))
    return _decode_per_task_envelope(
        loads_r.ok_value,
        app=app,
        task_name=task_name,
        node_id=node_id,
        sub_definition_key=sub_definition_key,
    )


def _node_read(
    decoded: Result[TaskOutcome[Any, TaskError[Any]], _NodeUnreadable | UnknownCustomCode],
) -> ReadResult[Any, TaskError[Any]]:
    """The read of ``result_for`` for a decoded node: its outcome, or the
    read failure. A node that does not read is
    ``ResultDeserializationError``; a stored custom code that the task's
    ``Enum`` does not name is ``UnknownCustomCode``. Pure."""
    match decoded:
        case Ok(ok_value=outcome):
            return ok_of_outcome(outcome)
        case Err(err_value=_NodeUnreadable() as unreadable):
            return _unreadable(unreadable.message)
        case Err(err_value=UnknownCustomCode() as unknown):
            return Err(unknown)


def _stored_node_result(
    stored: str | None,
    *,
    app: Any | None,
    task_name: str,
    node_id: str,
    sub_definition_key: str | None,
) -> TaskOutcome[Any, TaskError[Any]] | None:
    """The ``result`` of one node of ``tasks()``, from its stored result
    text. Pure apart from the type lookups of the decode.

    Outcomes:
        - NULL: ``None``. A node with no result stores NULL.
        - Any text: the folded outcome of ``_stored_node_decode``.
    """
    match stored:
        case None:
            return None
        case text:
            return _folded(node_id, _stored_node_decode(
                text,
                app=app,
                task_name=task_name,
                node_id=node_id,
                sub_definition_key=sub_definition_key,
            ))


def _outputless_node_outcome(
    workflow_id: str,
    node_id: str,
    per_node_envelope: Json,
    source_task_name: object,
    *,
    app: Any | None,
    sub_definition_key: object,
) -> TaskOutcome[Any, TaskError[Any]]:
    """The entry of one node in the outputless map of ``get()``.

    A node with no payload or no task name, or whose stored value does not
    decode, is a failed outcome with ``RESULT_DESERIALIZATION_ERROR``; a
    source that this process does not know is a failed outcome with
    ``NO_TYPE_AVAILABLE``. A failed entry has the custom codes of its task
    (``resolve_source_code_type``); a stored code that the task's ``Enum``
    does not name is a failed outcome with ``RESULT_DESERIALIZATION_ERROR``.
    The other entries stay readable.
    """
    from horsies.core.models.tasks import ContractCode
    from .typing_utils import resolve_source_code_type, resolve_source_ok_type

    if per_node_envelope is None or not isinstance(source_task_name, str):
        return Failed(TaskError(
            code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
            message=(
                f'Outputless workflow {workflow_id} node '
                f'{node_id!r} missing payload or task name'
            ),
            data={'node_id': node_id},
        ))
    source_ok_type = resolve_source_ok_type(
        app,
        source_task_name,
        sub_definition_key=(
            sub_definition_key if isinstance(sub_definition_key, str) else None
        ),
    )
    if source_ok_type is None:
        return Failed(TaskError(
            code=ContractCode.NO_TYPE_AVAILABLE,
            message=(
                f'Outputless workflow {workflow_id} node '
                f'{node_id!r}: source '
                f'{source_task_name!r} is not a known task or '
                f'workflow definition'
            ),
            data={'node_id': node_id, 'source_task_name': source_task_name},
        ))
    try:
        outcome = decode_task_result(per_node_envelope, source_ok_type)
    except (StrictJsonError, ValueValidationError) as exc:
        return Failed(TaskError(
            code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
            message=f'decode_task_result failed for node {node_id!r}: {exc}',
            data={'node_id': node_id},
        ))
    code_type = resolve_source_code_type(
        app,
        source_task_name,
        sub_definition_key=(
            sub_definition_key if isinstance(sub_definition_key, str) else None
        ),
    )
    match typed_outcome(outcome, code_type):
        case Ok(ok_value=typed):
            return typed
        case Err(err_value=unknown):
            return _unknown_code_outcome(node_id, unknown)


def _decode_outputless_ok(
    ok_slot: Json, *, app: Any | None, workflow_id: str,
) -> ReadResult[Any, TaskError]:
    """The read of the ok slot of an outputless workflow envelope: the map
    of each terminal node's outcome, by node id."""
    if not isinstance(ok_slot, dict):
        return _unreadable('Outputless workflow envelope ok slot malformed')
    results_raw = ok_slot.get('results_by_id', {})
    task_names = ok_slot.get('task_name_by_id', {})
    definition_keys = ok_slot.get('definition_key_by_id', {})
    if (
        not isinstance(results_raw, dict)
        or not isinstance(task_names, dict)
        or not isinstance(definition_keys, dict)
    ):
        return _unreadable(
            'Outputless workflow envelope missing or malformed '
            'results_by_id / task_name_by_id / definition_key_by_id',
        )
    decoded_results: dict[str, TaskOutcome[Any, TaskError[Any]]] = {
        node_id: _outputless_node_outcome(
            workflow_id,
            node_id,
            per_node_envelope,
            task_names.get(node_id),
            app=app,
            sub_definition_key=definition_keys.get(node_id),
        )
        for node_id, per_node_envelope in results_raw.items()
    }
    return Ok(Succeeded(decoded_results))


def _decode_workflow_envelope(
    raw: Json,
    out_type: TypeAnnotation | None,
    *,
    app: Any | None,
    workflow_id: str,
) -> ReadResult[Any, TaskError]:
    """Decode the workflow's stored result envelope into a ``ReadResult``.

    Routes:
    - Not an envelope, or an outputless marker that the handle's type does
      not match → ``Err(ResultDeserializationError)``.
    - Err slot populated → ``Ok(Failed(task_error))`` (no out_type needed).
    - out_type is ``_OUTPUTLESS_TERMINALS`` → per-node decode using
      ``task_name_by_id`` embedded in the envelope and the local task
      registry on ``app``.
    - out_type is set → ``decode_task_result(raw, out_type)``.
    - out_type is None and ok slot populated → ``Err(NoTypeAvailable)``.
    """
    if not isinstance(raw, dict):
        return _unreadable(
            f'Workflow {workflow_id} result envelope is not a '
            f'dict; got {type(raw).__name__}',
        )
    envelope = raw
    if envelope.get('__h_task_result__') is not True:
        return _unreadable(
            f'Workflow {workflow_id} result missing __h_task_result__ envelope marker',
        )

    # The outputless flag of the wire and the type of the handle must agree
    # before any per-slot decoding. A mismatch (typed out_type with
    # outputless payload, or outputless handle reading a typed payload) is a
    # contract violation: the workflow definition changed between produce
    # and consume, or the caller wired the wrong handle. Checking this
    # before the err slot ensures a smuggled outputless flag on a typed
    # envelope cannot slip through via the err route either.
    wire_outputless = envelope.get('__h_outputless_terminals__') is True
    handle_outputless = out_type is _OUTPUTLESS_TERMINALS
    if wire_outputless != handle_outputless:
        return _unreadable(
            f'Workflow {workflow_id} outputless/typed envelope '
            f'mismatch: wire_outputless={wire_outputless}, '
            f'handle_outputless={handle_outputless}. Workflow '
            f'definition likely changed between produce and '
            f'consume.',
        )

    err_slot = envelope.get('err')
    if err_slot is not None:
        # Polymorphic decode preserves SubWorkflowError fields
        # (``sub_workflow_id`` / ``sub_workflow_summary``) emitted by
        # the engine for failed parent nodes; plain TaskError decode
        # would drop them.
        try:
            return Ok(Failed(decode_task_error(err_slot)))
        except (StrictJsonError, ValueValidationError) as exc:
            return _unreadable(f'TaskError decode failed: {exc}')

    if handle_outputless:
        return _decode_outputless_ok(
            envelope.get('ok'), app=app, workflow_id=workflow_id,
        )
    if out_type is None:
        return Err(NoTypeAvailable(message=(
            f'Workflow {workflow_id} ok-result decode requires '
            f'a declared OutT; handle has none. Use '
            f'raw_result() instead.'
        )))
    try:
        return ok_of_outcome(decode_task_result(raw, out_type))
    except (StrictJsonError, ValueValidationError) as exc:
        return _unreadable(f'decode_task_result failed: {exc}')


@dataclass
class WorkflowHandle(Generic[OutT]):
    """
    Handle for tracking and retrieving workflow results.

    Provides methods to:
    - Check workflow status
    - Wait for and retrieve results
    - Inspect individual task states
    - Cancel the workflow

    ``workflow_id`` is a ``UUID``: a start returns a handle with the id of
    its workflow, and ``for_id`` builds the handle of a workflow id text, or
    refuses text that is not a UUID. The id in every message and error class
    of the handle is the canonical text of the UUID (``str(workflow_id)``).

    Error handling follows two strategies:

    **Wrap strategy** (status, cancel, pause, resume, results, tasks):
    Returns ``HandleResult[T]``. Infrastructure errors are
    ``Err(HandleOperationError)``.

    **Read strategy** (get, result_for):
    Returns ``ReadResult[T, TaskError]``. A failed read is
    ``Err(ReadError)``, apart from the outcome: infrastructure errors are
    ``Err(BrokerError)``.

    Every sync method called on a thread that runs an event loop returns
    ``ASYNC_CONTEXT`` (``HandleAsyncContext`` for the wrap strategy,
    ``AsyncContext`` for the read strategy) and runs nothing. Use the
    ``*_async`` form there.

    Every method that calls the core, in both strategies, raises
    ``ForkedAfterCoreStart`` and ``CoreStopped``: they are states of the
    process, never a result (``call_refusal_or_raise``).
    """

    workflow_id: uuid.UUID
    broker: PostgresBroker
    # Strict-serde phase 6: declared OutT for typed result decode.
    # Started handles populate this from the workflow spec (see
    # ``_resolve_source_node_ok_type(spec.output)``). For outputless
    # workflows, the sentinel ``_OUTPUTLESS_TERMINALS`` routes through
    # a dedicated per-node decode path. By-id reconstruction without a
    # registered spec leaves this ``None`` and ``get()`` folds
    # NO_TYPE_AVAILABLE into the err slot when the ok slot is present.
    out_type: TypeAnnotation | None = None

    @staticmethod
    def for_id(
        text: str, broker: PostgresBroker,
    ) -> Result[WorkflowHandle[Any], HandleInvalidWorkflowId]:
        """The handle of the workflow whose id is ``text``, read through
        ``broker``, or why ``text`` is not a workflow id. Pure: the handle
        reads nothing until one of its methods runs, and a workflow that does
        not exist is ``WORKFLOW_NOT_FOUND`` of that method.

        ``text`` is any spelling of a UUID that ``uuid.UUID`` accepts (upper
        case, braces, ``urn:uuid:``, no hyphens); the handle holds the UUID,
        whose text is the canonical form. Text that ``uuid.UUID`` refuses is
        ``Err(HandleInvalidWorkflowId)`` with its ``ValueError``; the same
        text gives the same error. The handle has no output type
        (``out_type`` is ``None``).
        """
        try:
            workflow_key = uuid.UUID(text)
        except ValueError as exc:
            return Err(HandleInvalidWorkflowId(
                message=f'workflow id {text!r} is not a UUID: {exc}',
                workflow_id_text=text,
                exception=exc,
            ))
        handle: WorkflowHandle[Any] = WorkflowHandle(workflow_id=workflow_key, broker=broker)
        return Ok(handle)

    # ─── sync refusal ────────────────────────────────────────────────

    def _async_context_err(self, operation: str) -> Err[HandleAsyncContext]:
        """The refusal of a wrap-strategy sync method called on a thread that
        runs an event loop. Nothing ran."""
        return Err(HandleAsyncContext(
            message=async_context_message(
                f'WorkflowHandle.{operation}()',
                f'handle.{operation}_async(...)',
            ),
            workflow_id=str(self.workflow_id),
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
        )

        if in_running_event_loop():
            return self._async_context_err('status')
        try:
            record = self.broker._core_handle.workflow_status(  # pyright: ignore[reportPrivateUsage]
                self.workflow_id,
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
        ``HandleDbOperationFailed`` for a failed read or a core boundary
        error; ``HandleStoredDataInvalid`` for a stored status that is not a
        ``WorkflowStatus``.
        """
        from horsies._core import CoreError
        from horsies.core.workflows.handle_records import (
            WorkflowOperation,
            core_call_error,
        )

        try:
            record = await self.broker._core_handle.workflow_status_async(  # pyright: ignore[reportPrivateUsage]
                self.workflow_id,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.STATUS,
            ))
        return self._status_answer(record)

    def _status_answer(
        self,
        record: (
            WorkflowStatusRead
            | WorkflowAbsent
            | HandleDbOperationFailed
            | HandleStoredDataInvalid
        ),
    ) -> Result[
        WorkflowStatus,
        HandleWorkflowNotFound | HandleDbOperationFailed | HandleStoredDataInvalid,
    ]:
        """The answer of ``status`` and ``status_async`` for the core's
        record. A failure is the core's class, returned as it is. Pure."""
        from horsies._core import WorkflowAbsent, WorkflowStatusRead

        match record:
            case WorkflowStatusRead(status=status):
                return Ok(WorkflowStatus(status))
            case WorkflowAbsent():
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=str(self.workflow_id),
                ))
            case HandleDbOperationFailed() | HandleStoredDataInvalid():
                return Err(record)
            case _ as unreachable:
                assert_never(unreachable)

    # ─── get ─────────────────────────────────────────────────────────

    def get(self, timeout_ms: int | None = None) -> ReadResult[OutT, TaskError]:
        """
        Block until workflow completes or timeout.

        The wait runs in the core (``CoreHandle.wait_workflow_result``) on the
        calling thread, with the GIL released and signals checked every
        100 ms: a raised signal handler (Ctrl+C) ends the wait with its
        exception, and the wait is dropped. A wait with a limit is bounded by
        the limit plus the core's bound of a blocking call; a wait with no
        limit has no call bound. The outcomes are those of ``get_async``.

        Returns:
            ``Ok(Succeeded(value))``: with an output task, that task's value;
            without one, the map of each terminal task's outcome by node id.
            ``Ok(Failed(task_error))``: the workflow failed, was cancelled,
            expired or is paused; a COMPLETED workflow whose output task is
            SKIPPED gives ``OutcomeCode.WORKFLOW_OUTPUT_SKIPPED`` (a SKIPPED
            terminal task has an entry with that same code in the map).
            ``Err(read_error)``: the checks before any wait come in this
            order: ``InvalidInput`` for a timeout_ms that is not None or an
            int from 0 to MAX_TASK_TIMEOUT_MS; ``WaitInsideTask`` in a process
            that runs task bodies; ``AsyncContext`` on a thread that runs an
            event loop.
        """
        from horsies._core import CoreError

        match self._wait_key(timeout_ms):
            case Err(err_value=refused):
                return Err(refused)
            case Ok(ok_value=workflow_key):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return _wait_inside_task_result(str(self.workflow_id), 'get')
        if in_running_event_loop():
            return _async_context_task_result('get')
        try:
            record = self.broker._core_handle.wait_workflow_result(  # pyright: ignore[reportPrivateUsage]
                workflow_key, timeout_ms=timeout_ms,
            )
        except CoreError as exc:
            return self._get_call_failed(call_refusal_or_raise(exc))
        return cast('ReadResult[OutT, TaskError]', self._get_answer(record, timeout_ms))

    async def get_async(
        self, timeout_ms: int | None = None,
    ) -> ReadResult[OutT, TaskError]:
        """Async version of get().

        The first check is the wait limit: a timeout_ms that is not None or
        an int from 0 to MAX_TASK_TIMEOUT_MS returns ``Err(InvalidInput)``.
        Then, in a process that runs task bodies, it returns
        ``Err(WaitInsideTask)`` with no wait. The wait runs in the core
        (``CoreHandle.wait_workflow_result_async``): a finished workflow is
        one read; a running one is read again on its ``workflow_done`` wake,
        and at least every 5 s. The handle decodes the stored result with its
        ``out_type``.
        """
        from horsies._core import CoreError

        match self._wait_key(timeout_ms):
            case Err(err_value=refused):
                return Err(refused)
            case Ok(ok_value=workflow_key):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return _wait_inside_task_result(str(self.workflow_id), 'get_async')
        try:
            record = await self.broker._core_handle.wait_workflow_result_async(  # pyright: ignore[reportPrivateUsage]
                workflow_key, timeout_ms=timeout_ms,
            )
        except CoreError as exc:
            return self._get_call_failed(call_refusal_or_raise(exc))
        return cast('ReadResult[OutT, TaskError]', self._get_answer(record, timeout_ms))

    def _wait_key(
        self, timeout_ms: int | None,
    ) -> Result[UUID, InvalidInput]:
        """The workflow id of a wait, or the refusal of the wait before any
        read: ``InvalidInput`` for a timeout_ms that is not None or an int
        from 0 to MAX_TASK_TIMEOUT_MS. Pure.
        """
        # timeout_ms=0 means "0ms budget": return the result if already
        # available, else WAIT_TIMEOUT immediately. Only None disables the
        # timeout. Matches PostgresBroker.get_result_async.
        match wait_timeout_seconds(timeout_ms):
            case Err(err_value=refused):
                return Err(InvalidInput(message=(
                    f'WorkflowHandle.get() for workflow '
                    f'{self.workflow_id}: {refused.message}'
                )))
            case Ok():
                return Ok(self.workflow_id)

    def _get_call_failed(self, exc: CoreCallRefusal) -> Err[ReadError]:
        """The answer of a wait whose core call raised a core boundary error:
        ``BrokerError`` with its type and text. Pure."""
        return _broker_read_error(
            f'Get failed for workflow {self.workflow_id}: {type(exc).__name__}: {exc}',
            core_error_retryable(exc),
            exc,
        )

    def _get_answer(
        self,
        record: (
            WorkflowCompletedRead
            | WorkflowEndedRead
            | WorkflowPausedRead
            | WaitTimedOut
            | WorkflowAbsent
            | HandleDbOperationFailed
            | HandleStoredDataInvalid
        ),
        timeout_ms: int | None,
    ) -> ReadResult[Any, TaskError]:
        """The answer of ``get`` and ``get_async`` for the core's record. A
        failure of the core is ``BrokerError`` with the core's message. Pure
        apart from the type lookups of the decode."""
        from horsies._core import (
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
                    str(self.workflow_id), _terminal_error_status(ended), stored_error,
                )
            case WorkflowPausedRead():
                return Ok(Failed(TaskError(
                    code=OutcomeCode.WORKFLOW_PAUSED,
                    message='Workflow is paused awaiting intervention',
                )))
            case WaitTimedOut():
                return Err(WaitTimeout(
                    message=f'Workflow did not complete within {timeout_ms}ms',
                ))
            case WorkflowAbsent():
                return Err(WorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                ))
            case (
                HandleDbOperationFailed(message=message, exception=cause)
                | HandleStoredDataInvalid(message=message, exception=cause)
            ):
                return _broker_read_error(message, record.retryable, cause)
            case _ as unreachable:
                assert_never(unreachable)

    def _completed_result(self, stored_result: str | None) -> ReadResult[Any, TaskError]:
        """The answer of ``get()`` for a COMPLETED workflow and its stored result. Pure
        apart from the type lookups of the decode.

        Outcomes:
            - A NULL stored result: ``ResultDeserializationError``. The
              library always stores a result with ``COMPLETED``.
            - A stored result that is not JSON, an empty text included:
              ``ResultDeserializationError``.
            - A stored result that is JSON: the outcome of
              ``_decode_workflow_envelope``.
        """
        match stored_result:
            case None:
                return _completed_without_stored_result(str(self.workflow_id))
            case stored:
                loads_r = loads_json(stored)
                if is_err(loads_r):
                    return _unreadable(f'Workflow result JSON corrupt: {loads_r.err_value}')
                return _decode_workflow_envelope(
                    loads_r.ok_value,
                    self.out_type,
                    app=self.broker.app,
                    workflow_id=str(self.workflow_id),
                )

    # ─── result_for ──────────────────────────────────────────────────

    def result_for(
        self, node: TaskNode[OkT, CodeT] | NodeKey[OkT, CodeT],
    ) -> ReadResult[OkT, TaskError[CodeT]]:
        """
        Get the result for a specific TaskNode or NodeKey.

        Non-blocking: queries the database once and returns immediately. The
        read runs in the core (``CoreHandle.workflow_node_result``) on the
        calling thread, with the GIL released; the outcomes are those of
        ``result_for_async``. On a thread that runs an event loop it returns
        ``Err(AsyncContext)`` and reads nothing.

        Args:
            node: The TaskNode or NodeKey whose result to retrieve.

        Returns:
            ReadResult[T, TaskError] where T matches the node's type.
            - If task completed: ``Ok(Succeeded(value))`` or
              ``Ok(Failed(task_error))``
            - If task not completed: ``Err(ResultNotReady)``
            - If node has no node_id: ``Err(WorkflowCtxMissingId)``
            - If the stored result does not read:
              ``Err(ResultDeserializationError)``

        Example:
            match handle.result_for(node):
                case Err(ResultNotReady()):
                    pass  # Task hasn't completed yet - wait or check later
                case read:
                    print(read)
        """
        from horsies._core import CoreError

        if in_running_event_loop():
            return _async_context_task_result('result_for')
        match self._node_result_key(node):
            case Err(err_value=refused):
                return Err(refused)
            case Ok(ok_value=(workflow_key, node_id)):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        try:
            record = self.broker._core_handle.workflow_node_result(  # pyright: ignore[reportPrivateUsage]
                workflow_key, node_id,
            )
        except CoreError as exc:
            return self._node_result_call_failed(node_id, call_refusal_or_raise(exc))
        return cast('ReadResult[OkT, TaskError[CodeT]]', self._node_result_answer(node_id, record))

    async def result_for_async(
        self, node: TaskNode[OkT, CodeT] | NodeKey[OkT, CodeT],
    ) -> ReadResult[OkT, TaskError[CodeT]]:
        """Async version of result_for(). See result_for() for full documentation.

        One read in the core (``CoreHandle.workflow_node_result_async``); the
        handle decodes the stored result.
        """
        from horsies._core import CoreError

        match self._node_result_key(node):
            case Err(err_value=refused):
                return Err(refused)
            case Ok(ok_value=(workflow_key, node_id)):
                pass
            case _ as unreachable:
                assert_never(unreachable)
        try:
            record = await self.broker._core_handle.workflow_node_result_async(  # pyright: ignore[reportPrivateUsage]
                workflow_key, node_id,
            )
        except CoreError as exc:
            return self._node_result_call_failed(node_id, call_refusal_or_raise(exc))
        return cast('ReadResult[OkT, TaskError[CodeT]]', self._node_result_answer(node_id, record))

    def _node_result_key(
        self, node: TaskNode[Any, Any] | NodeKey[Any, Any],
    ) -> Result[tuple[UUID, str], WorkflowCtxMissingId]:
        """The workflow id and the node id of a result read, or the refusal
        of the read before any read: ``WorkflowCtxMissingId`` for a node
        with no node_id. Pure.
        """
        node_id: str | None
        if isinstance(node, NodeKey):
            node_id = node.node_id
        else:
            node_id = node.node_id

        if node_id is None:
            return Err(WorkflowCtxMissingId(message=(
                'TaskNode node_id is not set. Ensure WorkflowSpec assigns '
                'node_id or provide an explicit node_id.'
            )))
        return Ok((self.workflow_id, node_id))

    def _node_result_call_failed(
        self, node_id: str, exc: CoreCallRefusal,
    ) -> Err[ReadError]:
        """The answer of a result read whose core call raised a core boundary
        error: ``BrokerError`` with its type and text. Pure."""
        return _broker_read_error(
            f'DB query failed for result_for node {node_id} in workflow '
            f'{self.workflow_id}: {type(exc).__name__}: {exc}',
            core_error_retryable(exc),
            exc,
        )

    def _node_result_answer(
        self,
        node_id: str,
        record: (
            StoredNodeResultRecord
            | NodeResultNotStored
            | HandleDbOperationFailed
            | HandleStoredDataInvalid
        ),
    ) -> ReadResult[Any, TaskError[Any]]:
        """The answer of ``result_for`` and ``result_for_async`` for the core's
        record. A failure of the core is ``BrokerError`` with the core's
        message. Pure apart from the type lookups of the decode."""
        from horsies._core import (
            NodeResultNotStored,
            StoredNodeResultRecord,
        )

        match record:
            case NodeResultNotStored():
                return Err(ResultNotReady(message=(
                    f"Task '{node_id}' has not completed yet "
                    f"in workflow '{self.workflow_id}'"
                )))
            case (
                HandleDbOperationFailed(message=message, exception=cause)
                | HandleStoredDataInvalid(message=message, exception=cause)
            ):
                return _broker_read_error(message, record.retryable, cause)
            case StoredNodeResultRecord():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        decoded = _stored_node_decode(
            record.result_json,
            app=self.broker.app,
            task_name=record.task_name,
            node_id=node_id,
            sub_definition_key=(
                record.sub_definition_key if record.is_subworkflow else None
            ),
        )
        return _node_read(decoded)

    # ─── results ─────────────────────────────────────────────────────

    def results(self) -> HandleResult[dict[str, TaskOutcome[Any, TaskError[Any]]]]:
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
        )

        if in_running_event_loop():
            return self._async_context_err('results')
        try:
            record = self.broker._core_handle.workflow_node_results(  # pyright: ignore[reportPrivateUsage]
                self.workflow_id,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.RESULTS,
            ))
        return self._results_answer(record)

    async def results_async(self) -> HandleResult[dict[str, TaskOutcome[Any, TaskError[Any]]]]:
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
        )

        try:
            record = await self.broker._core_handle.workflow_node_results_async(  # pyright: ignore[reportPrivateUsage]
                self.workflow_id,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.RESULTS,
            ))
        return self._results_answer(record)

    def _results_answer(
        self,
        record: (
            WorkflowNodeResultsRead
            | WorkflowAbsent
            | HandleDbOperationFailed
            | HandleStoredDataInvalid
        ),
    ) -> HandleResult[dict[str, TaskOutcome[Any, TaskError[Any]]]]:
        """The answer of ``results`` and ``results_async`` for the core's record.
        A failure is the core's class, returned as it is. Pure apart from the
        type lookups of the decode."""
        from horsies._core import WorkflowAbsent, WorkflowNodeResultsRead

        match record:
            case WorkflowAbsent():
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=str(self.workflow_id),
                ))
            case HandleDbOperationFailed() | HandleStoredDataInvalid():
                return Err(record)
            case WorkflowNodeResultsRead(nodes=nodes):
                pass
            case _ as unreachable:
                assert_never(unreachable)

        out: dict[str, TaskOutcome[Any, TaskError[Any]]] = {}
        for node in nodes:
            # The column is nullable; every spec assigns a node id, and the
            # read before the switch used the stored value as it is.
            node_id = cast(str, node.node_id)
            out[node_id] = _folded(node_id, _stored_node_decode(
                node.result_json,
                app=self.broker.app,
                task_name=node.task_name,
                node_id=node_id,
                sub_definition_key=(
                    node.sub_definition_key if node.is_subworkflow else None
                ),
            ))
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
        )

        if in_running_event_loop():
            return self._async_context_err('tasks')
        try:
            record = self.broker._core_handle.workflow_nodes(  # pyright: ignore[reportPrivateUsage]
                self.workflow_id,
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
        )

        try:
            record = await self.broker._core_handle.workflow_nodes_async(  # pyright: ignore[reportPrivateUsage]
                self.workflow_id,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.TASKS,
            ))
        return self._tasks_answer(record)

    def _tasks_answer(
        self,
        record: (
            WorkflowNodesRead
            | WorkflowAbsent
            | HandleDbOperationFailed
            | HandleStoredDataInvalid
        ),
    ) -> HandleResult[list[WorkflowTaskInfo]]:
        """The answer of ``tasks`` and ``tasks_async`` for the core's record.
        A failure is the core's class, returned as it is. Pure apart from the
        type lookups of the decode."""
        from horsies._core import WorkflowAbsent, WorkflowNodesRead

        match record:
            case WorkflowAbsent():
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=str(self.workflow_id),
                ))
            case HandleDbOperationFailed() | HandleStoredDataInvalid():
                return Err(record)
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
                            workflow_id=str(self.workflow_id),
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
                            workflow_id=str(self.workflow_id),
                            exception=summary_exc,
                        ))

            out.append(WorkflowTaskInfo(
                node_id=row.node_id,
                index=row.task_index,
                name=row.task_name,
                status=stored_node_status(
                    workflow_id=str(self.workflow_id),
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
        )

        if in_running_event_loop():
            return self._async_context_err('cancel')
        try:
            record = self.broker._core_handle.cancel_workflow(  # pyright: ignore[reportPrivateUsage]
                self.workflow_id,
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
            outcome is unknown.
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
        )

        try:
            record = await self.broker._core_handle.cancel_workflow_async(  # pyright: ignore[reportPrivateUsage]
                self.workflow_id,
            )
        except CoreError as exc:
            return Err(core_call_error(
                call_refusal_or_raise(exc), self.workflow_id, WorkflowOperation.CANCEL,
            ))
        return self._cancel_answer(record)

    def _cancel_answer(
        self,
        record: (
            WorkflowCancelled
            | WorkflowNotCancellable
            | WorkflowAbsent
            | HandleDbOperationFailed
            | HandleStoredDataInvalid
        ),
    ) -> HandleResult[None]:
        """The answer of ``cancel`` and ``cancel_async`` for the core's record.
        A failure is the core's class, returned as it is. Pure."""
        from horsies._core import WorkflowAbsent, WorkflowCancelled, WorkflowNotCancellable

        # The CANCELLED status write fires horsies_workflow_notify_trigger
        # per workflow row, so no workflow_done notify is sent here.
        match record:
            case WorkflowCancelled() | WorkflowNotCancellable():
                return Ok(None)
            case WorkflowAbsent():
                return Err(HandleWorkflowNotFound(
                    message=f'Workflow {self.workflow_id} not found',
                    workflow_id=str(self.workflow_id),
                ))
            case HandleDbOperationFailed() | HandleStoredDataInvalid():
                return Err(record)
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
