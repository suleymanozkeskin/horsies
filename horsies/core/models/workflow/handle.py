"""Workflow handle for tracking and retrieving results."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generic,
    Literal,
    TypeVar,
    assert_never,
    cast,
)

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from horsies.core.logging import get_logger
from horsies.core.utils.loop_runner import (
    LoopRunnerError,
    async_context_message,
    in_running_event_loop,
)
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.resources import (
    BrokerResources,
    EventLoopLimit,
    ListenerDisabled,
    ListenerEnabled,
)
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.lifecycle.commands import CancelNodesOfCancelledWorkflow
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
    TerminalizationOutcome,
)
from horsies.core.lifecycle.persistence import apply_batch_async
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
    BuiltInTaskCode,
    OperationalErrorCode,
    ContractCode,
    RetrievalCode,
    OutcomeCode,
)
from horsies.core.types.result import Ok, Err, Result, is_err
from horsies.core.types.status import TASK_TERMINAL_STATES

from .context import SubWorkflowSummary
from .enums import (
    OkT,
    OutT,
    WF_TASK_TERMINAL_VALUES,
    WorkflowStatus,
    WorkflowTaskStatus,
)
from .handle_types import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleEventLoopLimit,
    HandleLoopRunnerFailed,
    HandleResult,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)
from .nodes import NodeKey

logger = get_logger('workflow.handle')

_T = TypeVar('_T')
_TASK_TERMINAL_VALUES: list[str] = [status.value for status in TASK_TERMINAL_STATES]


def _require_transition_only_batch(
    outcomes: list[TerminalizationOutcome],
) -> None:
    """Workflow-scoped batches may report only rows they transitioned."""
    for outcome in outcomes:
        match outcome:
            case Applied():
                continue
            case (
                AlreadyApplied()
                | LostClaim()
                | SourceStateConflict()
                | TaskAbsent()
            ):
                raise RuntimeError(
                    'cancelled-workflow terminalization returned '
                    f'{type(outcome).__name__}; workflow-scoped batches '
                    'report transitioned rows only'
                )
            case _ as unreachable:
                assert_never(unreachable)

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

CANCEL_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'CANCELLED', updated_at = NOW()
    WHERE id = :wf_id AND status IN ('PENDING', 'RUNNING', 'PAUSED')
""")

# Lock the workflow row first, before any horsies_tasks/horsies_workflow_tasks
# rows, so the cancel transaction acquires {horsies_workflows,
# horsies_workflow_tasks} in the same order as COMPLETE_WORKFLOW_TASK_SQL
# (workflows before workflow_tasks). The two paths previously locked in
# opposite orders and deadlocked under contention — a task completing while
# its workflow is being cancelled — with Postgres aborting one side
# (SQLSTATE 40P01). Lock-order invariant (N6): any transaction locking both
# tables must take horsies_workflows first.
LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL = text("""
    SELECT id FROM horsies_workflows WHERE id = :wf_id FOR UPDATE
""")

LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL = text("""
    SELECT t.id
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
    WHERE wt.workflow_id = :wf_id
      AND NOT (wt.status = ANY(:wf_task_terminal_states))
      AND NOT (t.status = ANY(:task_terminal_states))
    FOR UPDATE OF t
""")

LOCK_WORKFLOW_TASKS_FOR_CANCEL_SQL = text("""
    SELECT id
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND NOT (status = ANY(:wf_task_terminal_states))
    FOR UPDATE
""")

SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND status IN ('PENDING', 'READY')
""")

# The cancelled backing tasks were MOVED to history by the batch
# cancellation, so a live-table join can no longer find them; the batch
# outcomes name exactly which tasks are cancelled, and the skip targets
# those ids.
SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = 'SKIPPED',
        completed_at = NOW()
    WHERE wt.workflow_id = :wf_id
      AND wt.status = 'ENQUEUED'
      AND wt.task_id = ANY(CAST(:cancelled_task_ids AS uuid[]))
""")

SELECT_CANCELLABLE_DESCENDANT_WORKFLOWS_SQL = text("""
    WITH RECURSIVE descendants AS (
        SELECT id, status
        FROM horsies_workflows
        WHERE parent_workflow_id = :wf_id
        UNION ALL
        SELECT child.id, child.status
        FROM horsies_workflows child
        JOIN descendants parent ON child.parent_workflow_id = parent.id
    )
    SELECT id FROM descendants
    WHERE status IN ('PENDING', 'RUNNING', 'PAUSED')
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


def _completed_without_stored_result(
    workflow_id: str, stored_form: Literal['NULL', 'empty'],
) -> TaskResult[Any, TaskError]:
    """``RESULT_DESERIALIZATION_ERROR`` for a ``COMPLETED`` workflow row whose
    result column is NULL or empty.

    The library writes a result in the same statement that sets
    ``COMPLETED``: the output node's stored result, an error envelope for an
    output node with no result, the outputless envelope, or the JSON text
    ``null`` when that envelope does not serialize. So a NULL or empty result
    is stored data that does not parse, not a success.
    """
    return TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
            message=(
                f'Workflow {workflow_id} is COMPLETED and its stored result '
                f'is {stored_form}'
            ),
            data={'workflow_id': workflow_id},
        ),
    )


def _empty_stored_error(workflow_id: str) -> TaskResult[Any, TaskError]:
    """``RESULT_DESERIALIZATION_ERROR`` for an empty stored workflow error.

    No library writer stores an empty error, so it is stored data that does
    not parse. A NULL error is not: a cancel does not write the error, and
    the failure finalize keeps the earlier error, which can be NULL.
    """
    logger.warning('Workflow %s error payload is empty', workflow_id)
    return TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
            message='Workflow error payload is empty',
            data={'workflow_id': workflow_id},
        ),
    )


def _decode_stored_workflow_error(
    workflow_id: str, status: _TerminalErrorStatus, stored_error: str,
) -> TaskResult[Any, TaskError]:
    """Decode a stored workflow error that is not NULL and not empty.

    Outcomes:
        - An error object that decodes: that ``TaskError``.
        - The plain expiry text that 0.5.0 and 0.5.1 stored for an
          ``EXPIRED`` workflow: ``OutcomeCode.WORKFLOW_EXPIRED``.
        - Text that is not JSON, JSON that is not an object, or an object
          that does not decode: ``RESULT_DESERIALIZATION_ERROR``, with the
          cause in the message.

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


class _OutputlessTerminals:
    """Sentinel ``out_type`` for workflows without an explicit output node.

    Started handles set ``out_type = _OUTPUTLESS_TERMINALS`` when the
    workflow spec's ``output`` is ``None``. ``WorkflowHandle.get()``
    dispatches to a dedicated decode path that walks the per-node
    ``results_by_id`` map embedded in the outer envelope.
    """

    __slots__ = ()


_OUTPUTLESS_TERMINALS: Any = _OutputlessTerminals()


_NODE_STATUS_BY_STORED_VALUE: dict[str, WorkflowTaskStatus] = {
    status.value: status for status in WorkflowTaskStatus
}


def stored_node_status(
    *, workflow_id: str, node_id: str, stored: str,
) -> WorkflowTaskStatus:
    """Return the node status that the database row stores.

    The CHECK ``horsies_workflow_tasks_status_check`` allows only the
    ``WorkflowTaskStatus`` values, so another value means the CHECK is
    missing or broken. Raises ``AssertionError`` that names the workflow,
    the node and the stored value.
    """
    status = _NODE_STATUS_BY_STORED_VALUE.get(stored)
    if status is None:
        raise AssertionError(
            f'workflow {workflow_id} node {node_id} stores status {stored!r}, '
            'which is not a WorkflowTaskStatus member; the CHECK '
            'horsies_workflow_tasks_status_check must forbid it'
        )
    return status


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

    def _event_loop_limit(
        self, loop_limit: EventLoopLimit, operation: str,
    ) -> Err[HandleEventLoopLimit]:
        """Build the wrap-strategy error for the event loop limit."""
        return Err(HandleEventLoopLimit(
            message=f'{operation} for workflow {self.workflow_id}: {loop_limit.describe()}',
            workflow_id=self.workflow_id,
            loop_limit=loop_limit,
        ))

    # ─── wrap-strategy sync helpers ──────────────────────────────────

    def _sync_call(
        self,
        coro_fn: Callable[..., Coroutine[Any, Any, HandleResult[_T]]],
        operation: str,
        *args: Any,
    ) -> HandleResult[_T]:
        """Sync bridge for wrap-strategy async methods.

        On a thread that runs an event loop it returns
        ``Err(HandleAsyncContext)`` and runs nothing. A runner failure returns
        ``HandleLoopRunnerFailed``; nothing ran. Any other exception
        propagates, as from the async form.
        """
        if in_running_event_loop():
            return Err(HandleAsyncContext(
                message=async_context_message(
                    f'WorkflowHandle.{operation}()',
                    f'handle.{operation}_async(...)',
                ),
                workflow_id=self.workflow_id,
            ))
        try:
            return self.broker.run_sync(coro_fn, *args)
        except LoopRunnerError as exc:
            return Err(HandleLoopRunnerFailed(
                message=f'Loop runner failed for {operation}: {exc}',
                workflow_id=self.workflow_id,
                exception=exc,
            ))

    # ─── fold-strategy sync helpers ──────────────────────────────────

    def _sync_task_result_call(
        self,
        coro_fn: Callable[..., Coroutine[Any, Any, TaskResult[_T, TaskError]]],
        operation: str,
        *args: Any,
    ) -> TaskResult[_T, TaskError]:
        """Sync bridge for fold-strategy async methods.

        On a thread that runs an event loop it returns an ``ASYNC_CONTEXT``
        result and runs nothing. A runner failure returns a ``BROKER_ERROR``
        result; nothing ran. Any other exception propagates, as from the
        async form.
        """
        if in_running_event_loop():
            return cast(
                'TaskResult[_T, TaskError]',
                _async_context_task_result(operation),
            )
        try:
            return self.broker.run_sync(coro_fn, *args)
        except LoopRunnerError as exc:
            return cast(
                'TaskResult[_T, TaskError]',
                _broker_task_error(f'Loop runner failed: {exc}'),
            )

    # ─── status ──────────────────────────────────────────────────────

    def status(self) -> HandleResult[WorkflowStatus]:
        """Get current workflow status."""
        return self._sync_call(self.status_async, 'status')

    async def status_async(self) -> HandleResult[WorkflowStatus]:
        """Async version of status()."""
        match await self.broker.select_resources():
            case Err(loop_limit):
                return self._event_loop_limit(loop_limit, 'status')
            case Ok(resources):
                pass
        return await self._status(resources)

    async def _status(
        self, resources: BrokerResources,
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

    def get(self, timeout_ms: int | None = None) -> TaskResult[OutT, TaskError]:
        """
        Block until workflow completes or timeout.

        Returns:
            If output task specified: that task's TaskResult. A COMPLETED
            workflow whose output task is SKIPPED returns
            ``OutcomeCode.WORKFLOW_OUTPUT_SKIPPED``.
            Otherwise: TaskResult containing dict of terminal task results
        """
        return self._sync_task_result_call(self.get_async, 'get', timeout_ms)

    async def get_async(
        self, timeout_ms: int | None = None,
    ) -> TaskResult[OutT, TaskError]:
        """Async version of get()."""

        start = time.monotonic()
        # timeout_ms=0 means "0ms budget": return the result if already
        # available, else WAIT_TIMEOUT immediately. Only None disables the
        # timeout. Matches PostgresBroker.get_result_async.
        timeout_sec = timeout_ms / 1000 if timeout_ms is not None else None

        match await self.broker.select_resources():
            case Err(loop_limit):
                return cast(
                    'TaskResult[OutT, TaskError]',
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
                status_r = await self._status(resources)

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
                        'TaskResult[OutT, TaskError]',
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
                        return await self._get_result(resources)
                    case (
                        WorkflowStatus.FAILED
                        | WorkflowStatus.CANCELLED
                        | WorkflowStatus.EXPIRED
                    ):
                        return await self._get_error(resources, status)
                    case WorkflowStatus.PAUSED:
                        return cast(
                            'TaskResult[OutT, TaskError]',
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
                        'TaskResult[OutT, TaskError]',
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
                                self._drain_queue_for_workflow(queue), timeout=wait_time,
                            )
                        except asyncio.TimeoutError:
                            pass
                    case None:
                        await asyncio.sleep(min(wait_time, 1.0))
        finally:
            match subscription:
                case (listener, queue):
                    await self._unsubscribe_workflow_done_safely(listener, queue)
                case None:
                    pass

    async def _unsubscribe_workflow_done_safely(
        self, listener: PostgresListener, q: asyncio.Queue[Any],
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

    async def _drain_queue_for_workflow(self, q: Any) -> None:
        """Drain notifications until one matches this workflow."""
        while True:
            note = await q.get()
            if note.payload == self.workflow_id:
                return

    async def _get_result(self, resources: BrokerResources) -> TaskResult[OutT, TaskError]:
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
                        'TaskResult[OutT, TaskError]',
                        _removed_after_status_read(
                            self.workflow_id, WorkflowStatus.COMPLETED,
                        ),
                    )
                match row.result:
                    case None:
                        return cast(
                            'TaskResult[OutT, TaskError]',
                            _completed_without_stored_result(
                                self.workflow_id, 'NULL',
                            ),
                        )
                    case '':
                        return cast(
                            'TaskResult[OutT, TaskError]',
                            _completed_without_stored_result(
                                self.workflow_id, 'empty',
                            ),
                        )
                    case stored_result:
                        loads_r = loads_json(stored_result)
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
        except SQLAlchemyError as exc:
            return cast(
                'TaskResult[OutT, TaskError]',
                _broker_task_error(
                    f'DB query failed fetching result for workflow {self.workflow_id}: {exc}',
                ),
            )

    async def _get_error(
        self, resources: BrokerResources, status: _TerminalErrorStatus,
    ) -> TaskResult[OutT, TaskError]:
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
                        'TaskResult[OutT, TaskError]',
                        _removed_after_status_read(self.workflow_id, status),
                    )
                match row.error:
                    case None:
                        return cast(
                            'TaskResult[OutT, TaskError]',
                            TaskResult(
                                err=TaskError(
                                    error_code=_terminal_outcome_code(status),
                                    message=f'Workflow {status.value.lower()}',
                                ),
                            ),
                        )
                    case '':
                        return cast(
                            'TaskResult[OutT, TaskError]',
                            _empty_stored_error(self.workflow_id),
                        )
                    case stored_error:
                        return cast(
                            'TaskResult[OutT, TaskError]',
                            _decode_stored_workflow_error(
                                self.workflow_id, status, stored_error,
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
        return self._sync_task_result_call(
            self.result_for_async, 'result_for', node,
        )

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

        match await self.broker.select_resources():
            case Err(loop_limit):
                return cast(
                    'TaskResult[OkT, TaskError]',
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
                    sub_definition_key=(
                        row.sub_definition_key if row.is_subworkflow else None
                    ),
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

    def _workflow_node_rows(
        self, rows: Sequence[Any],
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

    def results(self) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]:
        """
        Get all task results keyed by unique identifier.

        Keys are `node_id` values. If a TaskNode did not specify a node_id,
        WorkflowSpec auto-assigns one as "{workflow_name}:{task_index}".

        ``Ok({})``: the workflow exists and no node has a stored result yet.
        ``Err(HandleWorkflowNotFound)``: no workflow has this id.
        """
        return self._sync_call(self.results_async, 'results')

    async def results_async(self) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]:
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
                match self._workflow_node_rows(result.fetchall()):
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

    def tasks(self) -> HandleResult[list[WorkflowTaskInfo]]:
        """Get status of all tasks in workflow.

        ``Ok([])``: the workflow exists and has no node.
        ``Err(HandleWorkflowNotFound)``: no workflow has this id.
        """
        return self._sync_call(self.tasks_async, 'tasks')

    async def tasks_async(self) -> HandleResult[list[WorkflowTaskInfo]]:
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
                match self._workflow_node_rows(result.fetchall()):
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

    # ─── cancel ──────────────────────────────────────────────────────

    def cancel(self) -> HandleResult[None]:
        """Request workflow cancellation."""
        return self._sync_call(self.cancel_async, 'cancel')

    async def cancel_async(self) -> HandleResult[None]:
        """Async version of cancel()."""
        match await self.broker.select_resources():
            case Err(loop_limit):
                return self._event_loop_limit(loop_limit, 'cancel')
            case Ok(resources):
                pass

        try:
            async with resources.session_factory() as session:
                lock_params = {
                    'wf_id': self.workflow_id,
                    'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
                    'task_terminal_states': _TASK_TERMINAL_VALUES,
                }
                # Workflow row first — see the lock-order invariant (N6) on
                # LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL and COMPLETE_WORKFLOW_TASK_SQL.
                await session.execute(
                    LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL,
                    {'wf_id': self.workflow_id},
                )
                # Close the worker-pickup race before flipping workflow status:
                # workers claim horsies_tasks rows with SKIP LOCKED, while
                # enqueue/ready transitions mutate horsies_workflow_tasks rows.
                await session.execute(
                    LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL,
                    lock_params,
                )
                await session.execute(
                    LOCK_WORKFLOW_TASKS_FOR_CANCEL_SQL,
                    lock_params,
                )
                # If an enqueue finished while we waited for workflow_task locks,
                # catch the newly linked task row before the status update below.
                await session.execute(
                    LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL,
                    lock_params,
                )

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
                    return Err(HandleWorkflowNotFound(
                        message=f'Workflow {self.workflow_id} not found',
                        workflow_id=self.workflow_id,
                    ))

                # An Err returns before the commit: the transaction rolls back.
                match _parse_stored_workflow_status(
                    self.workflow_id, exists_row.status,
                ):
                    case Err() as stored_data_invalid:
                        return stored_data_invalid
                    case Ok(status_val):
                        pass
                if status_val != WorkflowStatus.CANCELLED:
                    # Non-cancellable state (COMPLETED, FAILED) → no-op
                    return Ok(None)

                # CANCELLED (either by our UPDATE or already) → idempotent cleanup
                # Cancel ENQUEUED tasks that have not started execution yet.
                # This guarantees they are no longer claimable by workers.
                # A backing task may briefly be RUNNING while workflow_task is
                # still ENQUEUED; user code starts only after the workflow_task
                # RUNNING handoff, so that state is still cancellable.
                parent_outcomes = await apply_batch_async(
                    await session.connection(),
                    CancelNodesOfCancelledWorkflow(
                        workflow_ids=(self.workflow_id,),
                    ),
                )
                _require_transition_only_batch(parent_outcomes)

                # Skip pending/ready tasks
                await session.execute(
                    SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL,
                    {'wf_id': self.workflow_id},
                )

                # Skip ENQUEUED workflow tasks whose backing task was cancelled above.
                await session.execute(
                    SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL,
                    {
                        'wf_id': self.workflow_id,
                        'cancelled_task_ids': [
                            outcome.task_id for outcome in parent_outcomes
                        ],
                    },
                )

                # Cascade cancellation to active child workflows. Each
                # descendant gets the same lock-before-flip treatment as the
                # parent above, so child cancellation is race-safe against a
                # worker picking up a child backing task. The CANCELLED status
                # flip fires ``horsies_workflow_notify_trigger`` per row, so no
                # manual ``workflow_done`` notify is needed. The parent node may
                # stay RUNNING until an already-started child task exits, but the
                # child workflow boundary is terminal immediately.
                descendant_rows = await session.execute(
                    SELECT_CANCELLABLE_DESCENDANT_WORKFLOWS_SQL,
                    {'wf_id': self.workflow_id},
                )
                child_workflow_ids = [row.id for row in descendant_rows.fetchall()]
                for child_workflow_id in child_workflow_ids:
                    child_lock_params = {
                        'wf_id': child_workflow_id,
                        'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
                        'task_terminal_states': _TASK_TERMINAL_VALUES,
                    }
                    # Child workflow row first (same lock-order invariant as the
                    # parent above), then backing tasks + workflow tasks before
                    # flipping status, then re-lock backing tasks to catch a row
                    # an enqueue linked while we waited for the workflow-task
                    # locks.
                    await session.execute(
                        LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL,
                        {'wf_id': child_workflow_id},
                    )
                    await session.execute(
                        LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL,
                        child_lock_params,
                    )
                    await session.execute(
                        LOCK_WORKFLOW_TASKS_FOR_CANCEL_SQL,
                        child_lock_params,
                    )
                    await session.execute(
                        LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL,
                        child_lock_params,
                    )
                    await session.execute(
                        CANCEL_WORKFLOW_SQL,
                        {'wf_id': child_workflow_id},
                    )
                    child_outcomes = await apply_batch_async(
                        await session.connection(),
                        CancelNodesOfCancelledWorkflow(
                            workflow_ids=(child_workflow_id,),
                        ),
                    )
                    _require_transition_only_batch(child_outcomes)
                    await session.execute(
                        SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL,
                        {'wf_id': child_workflow_id},
                    )
                    await session.execute(
                        SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL,
                        {
                            'wf_id': child_workflow_id,
                            'cancelled_task_ids': [
                                outcome.task_id for outcome in child_outcomes
                            ],
                        },
                    )

                await session.commit()
            return Ok(None)
        except SQLAlchemyError as exc:
            return Err(HandleDbOperationFailed(
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
            Err(HandleOperationError) on failure: match on the variant.
        """
        return self._sync_call(self.pause_async, 'pause')

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

        Returns:
            Ok(True) if workflow was resumed, Ok(False) if not PAUSED (no-op).
            Err(HandleOperationError) on failure: match on the variant.
        """
        return self._sync_call(self.resume_async, 'resume')

    async def resume_async(self) -> HandleResult[bool]:
        """Async version of resume().

        Returns:
            Ok(True) if workflow was resumed, Ok(False) if not PAUSED (no-op).
            Err(HandleOperationError) on failure: match on the variant.
        """
        from horsies.core.workflows.engine import resume_workflow

        return await resume_workflow(self.broker, self.workflow_id)
