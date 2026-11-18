"""Workflow lifecycle: start, pause, resume."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from collections import deque
from typing import TYPE_CHECKING, Any, TypeVar, assert_never, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from typing import Final


from horsies.core.codec.json_io import dumps_json, loads_json, SerializationError, SerdeResult
from horsies.core.codec.task_options import stored_execution, stored_max_retries
from horsies.core.codec.typed import encode_task_error
from horsies.core.models.tasks import Execution, OutcomeCode, TaskError
from horsies.core.types.result import Ok, Err, Result, is_err as _is_err
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    WorkflowHandle,
    SubWorkflowNode,
    validate_workflow_generic_output_match,
)
from horsies.core.models.workflow.handle import _OUTPUTLESS_TERMINALS
from horsies.core.models.workflow.typing_utils import _resolve_source_node_ok_type
from horsies.core.errors import WorkflowValidationError, ErrorCode
from horsies.core.utils.db import autocommit_connection
from horsies.core.models.workflow.handle_types import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleResult,
    HandleResumeRecoveryFailed,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)
from horsies.core.workflows.start_types import (
    WorkflowStartAsyncContext,
    WorkflowStartEnqueueFailed,
    WorkflowStartIdConflict,
    WorkflowStartResult,
    WorkflowStartValidationFailed,
)
from horsies.core.history.periods import RetentionClassKey
from horsies.core.workflows.sql import (
    GET_RUNNING_CHILD_WORKFLOWS_SQL,
    PAUSE_CHILD_WORKFLOW_SQL,
    SubWorkflowParams,
    TaskNodeFacts,
    task_node_facts,
)

logger = get_logger('workflow.engine')

# Internal retry constants for resend_on_transient_err.

PAUSED_EXPIRY_BATCH_SIZE: Final[int] = 50
"""The most PAUSED workflows that one expiry call reads and expires."""


def _ser_or_raise(result: SerdeResult[str], context: str) -> str:
    """Extract a serde Result or raise with context.

    Used inside the start transaction of ``start_workflow_async`` and by
    ``expire_paused_workflows``. Every value it gets there was built and
    validated in the process, so a ``SerializationError`` is a defect: the
    start transaction does not catch it, and the transaction rolls back.
    """
    if _is_err(result):
        raise SerializationError(f'Failed to serialize {context}: {result.err_value}')
    return result.ok_value


if TYPE_CHECKING:
    from horsies._core import (
        WorkflowAbsent,
        WorkflowExists,
        WorkflowIdTaken,
        WorkflowNotPaused,
        WorkflowNotRunning,
        WorkflowPaused,
        WorkflowResumed,
        WorkflowStartFailed,
        WorkflowStarted,
        WorkflowStartRequest,
    )
    from horsies.core.models.workflow import WorkflowSpec
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.types.core_failure import CoreCallRefusal

OutT = TypeVar('OutT')


def as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    str_items: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            return []
        str_items.append(item)
    return str_items


_LIFECYCLE_PRIVATE_EXPORTS = (as_str_list,)


def guard_no_positional_args(node_name: str, args: tuple[Any, ...]) -> None:
    """Raise if a workflow node still carries positional args.

    Under the kwargs-only contract, all runtime parameters must be
    passed via ``kwargs``.  Positional ``args`` should be empty for
    every new write.  This guard catches leftover callers that were
    not migrated.
    """
    if args:
        raise ValueError(
            f"Workflow node '{node_name}' has positional args={args!r}. "
            'Positional args are not supported for workflow nodes; '
            'use kwargs instead.',
        )




def root_retry_and_deadline(
    task_options_json: str | None,
    node_name: str,
) -> tuple[int, str | None]:
    """Extract (max_retries, good_until_str) for a task node at workflow start.

    Mirrors enqueue_workflow_task's task_options parsing. One deliberate
    difference: the promotion path folds corrupt options into a FAILED task
    (it re-reads rows written by another transaction); here the JSON is the
    task decorator's own serialization of a validated ``TaskOptions``, so
    corruption is a defect. The start transaction and the engine's child
    workflow start do not catch the ``SerializationError``: it propagates,
    and the caller's transaction rolls back.

    ``good_until_str`` is the ``good_until`` text of the options; the
    database parses it into the timestamp column.

    Raises:
        SerializationError: task_options_json does not parse to an object,
            or its retry_policy does not validate as RetryPolicy.
    """
    if task_options_json is None:
        return 0, None

    parsed_r = loads_json(task_options_json)
    if _is_err(parsed_r) or not isinstance(parsed_r.ok_value, dict):
        raise SerializationError(
            f'Corrupt task_options for root node {node_name}',
        )
    options = parsed_r.ok_value
    retry_limit_r = stored_max_retries(options)
    if _is_err(retry_limit_r):
        raise SerializationError(
            f'Invalid retry_policy for root node {node_name}: '
            f'{retry_limit_r.err_value.detail}',
        )
    good_until_raw = options.get('good_until')
    good_until_str = None if good_until_raw is None else str(good_until_raw)
    return retry_limit_r.ok_value, good_until_str


def root_execution(
    task_options_json: str | None,
    node_name: str,
) -> Execution:
    """The `Execution` that a fast-path root stores, from its task options.

    The JSON is the task decorator's own serialization of a validated
    ``TaskOptions``, as in ``root_retry_and_deadline``, so options that do
    not parse or do not validate are a defect: the ``SerializationError``
    propagates and the caller's transaction rolls back. No options store
    ``ISOLATED``.

    Raises:
        SerializationError: task_options_json does not parse, or its
            ``execution`` or ``cancel_grace_ms`` does not validate.
    """
    if task_options_json is None:
        return Execution.ISOLATED
    parsed_r = loads_json(task_options_json)
    if _is_err(parsed_r):
        raise SerializationError(
            f'Corrupt task_options for root node {node_name}',
        )
    execution_r = stored_execution(parsed_r.ok_value)
    if _is_err(execution_r):
        raise SerializationError(
            f'Invalid execution for root node {node_name}: '
            f'{execution_r.err_value.detail}',
        )
    return execution_r.ok_value


def start_task_node_facts(
    task_options_json: str | None,
    node_name: str,
    queue_retention_class: RetentionClassKey | None,
) -> TaskNodeFacts:
    """The typed facts that a workflow start writes on a task node row.

    Every task node gets them, root or not; a fast-path root's task row
    stores the same values. The options are the task decorator's own
    serialization of a validated ``TaskOptions``, as in
    ``root_retry_and_deadline`` and ``root_execution``. Does no I/O.

    Raises:
        SerializationError: the options do not parse to an object, or their
            ``retry_policy``, ``execution`` or ``cancel_grace_ms`` does not
            validate.
    """
    max_retries, good_until = root_retry_and_deadline(task_options_json, node_name)
    return task_node_facts(
        max_retries=max_retries,
        execution=root_execution(task_options_json, node_name),
        good_until=good_until,
        queue_retention_class=queue_retention_class,
    )


def subworkflow_params_of(node: SubWorkflowNode[Any]) -> SubWorkflowParams:
    """The params choice of a sub-workflow node, from its definition.

    ``GIVEN`` when the node was given params: its ``kwargs`` for the child's
    ``build_with`` are not empty. ``NOT_GIVEN`` when it was given none. The
    stored ``input`` text and ``args_from`` play no part: ``args_from``
    values are the engine's injections, not params the node was given.
    Does no I/O.
    """
    match len(node.kwargs):
        case 0:
            return SubWorkflowParams.NOT_GIVEN
        case _:
            return SubWorkflowParams.GIVEN


async def start_workflow_async(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
    sent_at: datetime | None = None,
    *,
    resend_on_transient_err: bool = False,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """Start a workflow asynchronously.

    The spec goes to the core as data (``workflow_spec_data``), and the core
    writes the tree (``CoreHandle.start_workflow_async``,
    ``horsies_workflow_start``): the workflow row, every node row, a
    pre-built child for every sub-workflow root, and a failed-node record for
    each sub-workflow root whose child cannot be built. The core builds each
    child through the broker's child spec source, which runs its
    ``build_with`` once per start, also when the id exists already, so it
    must have no side effects. The sub-workflow roots of each workflow are
    taken in index order, each child tree before the next root. A pre-built
    child of a root that comes after a failure that pauses its parent
    workflow is not written: its node stays READY, and the resume builds the
    child again. The call:

    - no failed-node record: one statement in autocommit;
    - failed-node records: ``BEGIN``, the start, one
      ``horsies_workflow_child_start`` per child start that their
      progression owes, ``COMMIT``. After the commit, the parent nodes of
      the child workflows that those child starts ended advance, one
      transaction per level; a failed advance is logged and left to
      workflow recovery.

    A cancel of the caller does not stop the start.

    Args:
        spec: The workflow specification.
        broker: PostgreSQL broker for database operations.
        workflow_id: Optional custom workflow ID.
        sent_at: Optional workflow call-site timestamp from Python.
        resend_on_transient_err: When ``True``, the core sends the start
            again up to 3 times on a transient failure (schema install and
            statement), after 200 ms, doubling up to 2 s.

    Returns:
        ``Ok(WorkflowHandle)`` on success.
        ``Err(WorkflowStartError)`` on failure; match on the variant. A
        ``WorkflowValidationError`` from prevalidation, and spec data that
        does not encode (a text with a lone surrogate), are
        ``WorkflowStartValidationFailed``. A failure of the schema install,
        the statement, an owed child start, a child build that did not run,
        or a core boundary error is ``WorkflowStartEnqueueFailed``; nothing
        committed, or the commit outcome is unknown (a resend with the same
        id then finds the workflow).

    Raises:
        ForkedAfterCoreStart, CoreStopped: the core of this process refuses
            every call (``call_refusal_or_raise``). Nothing was written.
        SerializationError: a value of the node rows does not serialize (a
            defect of values that the process built and validated); or the
            build of a sub-workflow child raised it (task options of a child
            root that do not parse), and the start rolled back.
        PanicException: a row outside a function's contract, or spec data
            that the core refuses (a defect).
        KeyboardInterrupt, SystemExit: raised by a child's ``build_with``;
            the start rolled back.
        Exception: another exception that the build of a sub-workflow child
            raised; the start rolled back.

    Notes:
        If workflow_id is provided and already exists with the same name,
        returns the existing handle (best-effort idempotent by workflow_id,
        not payload-verified); with another name, ``WorkflowStartIdConflict``.
    """
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise

    wf_id = workflow_id or str(uuid.uuid4())
    workflow_sent_at = sent_at or datetime.now(timezone.utc)
    match _start_request(
        spec, broker, workflow_id, wf_id, workflow_sent_at, resend_on_transient_err,
    ):
        case Err(err_value=refused):
            return Err(refused)
        case Ok(ok_value=request):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    try:
        outcome = await broker._core_handle.start_workflow_async(request)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(_start_call_failed(spec, wf_id, call_refusal_or_raise(exc)))
    return _start_answer(spec, broker, wf_id, outcome)


def _start_request(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None,
    wf_id: str,
    sent_at: datetime,
    resend_on_transient_err: bool,
) -> Result[WorkflowStartRequest, WorkflowStartValidationFailed]:
    """The start request of ``spec`` under ``wf_id``, or the validation that
    refuses it. No I/O. ``workflow_id`` is the id that the caller gave, if
    any: only a given id is checked to be a UUID.

    The prevalidation and the spec as data of ``start_workflow`` and
    ``start_workflow_async``. A ``WorkflowValidationError`` of the checks,
    and spec data that does not encode, are ``WorkflowStartValidationFailed``;
    any other exception is a defect and propagates.
    """
    from horsies._core import WorkflowStartRequest
    from horsies.core.workflows.spec_data import workflow_spec_json
    from horsies.core.workflows.start_tree import top_node_rows

    # ── Zone 1: Prevalidation (never retried) ────────────────────────
    try:
        if workflow_id is not None:
            try:
                uuid.UUID(workflow_id)
            except ValueError as exc:
                raise WorkflowValidationError(
                    message='workflow_id is not a UUID',
                    code=ErrorCode.WORKFLOW_INVALID_ID,
                    notes=[f'received {workflow_id!r}'],
                    help_text=(
                        'workflow identity is a UUID; supply one (for '
                        'example uuid.uuid4()) or omit workflow_id to have '
                        'one minted'
                    ),
                ) from exc

        spec._require_definition_key()

        # Validate output type matches declared generic (HRS-025)
        if spec.workflow_def_cls is not None:
            validate_workflow_generic_output_match(spec.workflow_def_cls, spec)

        # Validate that WorkflowSpec was created via app.workflow()
        # TaskNodes must have resolved queue and priority (SubWorkflowNodes don't have these)
        for node in spec.tasks:
            if isinstance(node, SubWorkflowNode):
                continue  # SubWorkflowNodes don't have queue/priority
            task = node
            if task.queue is None:
                raise WorkflowValidationError(
                    message='TaskNode has unresolved queue',
                    code=ErrorCode.WORKFLOW_UNRESOLVED_QUEUE,
                    notes=[f"TaskNode '{task.name}' has queue=None"],
                    help_text='use app.workflow() to create WorkflowSpec with proper validation',
                )
            if task.priority is None:
                raise WorkflowValidationError(
                    message='TaskNode has unresolved priority',
                    code=ErrorCode.WORKFLOW_UNRESOLVED_PRIORITY,
                    notes=[f"TaskNode '{task.name}' has priority=None"],
                    help_text='use app.workflow() to create WorkflowSpec with proper validation',
                )
    except WorkflowValidationError as exc:
        # The checks above raise only WorkflowValidationError on purpose.
        # Any other exception is a defect and propagates.
        return Err(WorkflowStartValidationFailed(
            message=str(exc),
            workflow_name=spec.name,
            workflow_id=wf_id,
            exception=exc,
        ))

    # ── Zone 2: the spec as data (no I/O) ────────────────────────────
    match workflow_spec_json(spec, top_node_rows(broker, spec)):
        case Ok(ok_value=spec_json):
            pass
        case Err(err_value=refused):
            return Err(WorkflowStartValidationFailed(
                message=f'the spec data of workflow {spec.name!r} does not encode: {refused}',
                workflow_name=spec.name,
                workflow_id=wf_id,
                exception=refused,
            ))
        case _ as unreachable:
            assert_never(unreachable)
    return Ok(WorkflowStartRequest(
        spec_json=spec_json,
        workflow_id=uuid.UUID(wf_id),
        sent_at=sent_at,
        resend_on_transient_err=resend_on_transient_err,
    ))


def _start_call_failed(
    spec: 'WorkflowSpec[OutT]',
    wf_id: str,
    exc: 'CoreCallRefusal',
) -> WorkflowStartEnqueueFailed:
    """The start's error for a core boundary error of the call: nothing ran,
    or the outcome is unknown (``CoreCallTimedOut``). Retryable by
    ``core_error_retryable``. Pure."""
    from horsies.core.types.core_failure import core_error_retryable

    return WorkflowStartEnqueueFailed(
        message=f'start of workflow {spec.name!r}: {type(exc).__name__}: {exc}',
        retryable=core_error_retryable(exc),
        workflow_name=spec.name,
        workflow_id=wf_id,
        exception=exc,
    )


def _start_answer(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    wf_id: str,
    outcome: WorkflowStarted | WorkflowExists | WorkflowIdTaken | WorkflowStartFailed,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """The answer of ``start_workflow`` and ``start_workflow_async`` for the
    core's start record. Pure apart from the WARNING log of an existing
    workflow and the type lookups of the handle's output type."""
    from horsies._core import (
        WorkflowExists,
        WorkflowIdTaken,
        WorkflowStartFailed,
        WorkflowStarted,
    )

    match outcome:
        case WorkflowStarted():
            pass
        case WorkflowExists():
            logger.warning(f'Workflow {wf_id} already exists, returning existing handle')
        case WorkflowIdTaken(existing_name=existing_name):
            return Err(WorkflowStartIdConflict(
                message=(
                    f"workflow_id '{wf_id}' already exists for workflow "
                    f"name '{existing_name}', cannot start workflow "
                    f"name '{spec.name}' with the same id"
                ),
                workflow_name=spec.name,
                workflow_id=wf_id,
                existing_workflow_name=existing_name,
            ))
        case WorkflowStartFailed(message=message, retryable=retryable, exception=exception):
            return Err(WorkflowStartEnqueueFailed(
                message=message,
                retryable=retryable,
                workflow_name=spec.name,
                workflow_id=wf_id,
                exception=exception,
            ))
        case _ as unreachable:
            assert_never(unreachable)

    return Ok(cast(
        'WorkflowHandle[OutT]',
        WorkflowHandle(
            workflow_id=wf_id,
            broker=broker,
            out_type=(
                _resolve_source_node_ok_type(spec.output)
                if spec.output is not None
                else _OUTPUTLESS_TERMINALS
            ),
        ),
    ))


def start_workflow(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
    sent_at: datetime | None = None,
    *,
    resend_on_transient_err: bool = False,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """Start a workflow synchronously.

    The blocking form of ``start_workflow_async()``: the same checks and the
    same start in the core (``CoreHandle.start_workflow``), on the calling
    thread with the GIL released, and the same outcomes. The children of
    the sub-workflow roots are built by their ``build_with`` on the core's
    callback thread while this thread waits. A raised signal handler (Ctrl+C)
    leaves the call: the start runs on to its end, and its outcome is
    unknown to the caller (a resend with the same id then finds the
    workflow). A start that does not end within the core's bound of a
    blocking call is ``WorkflowStartEnqueueFailed`` with the
    ``CoreCallTimedOut``; its outcome is unknown.

    On a thread that runs an event loop it returns ``ASYNC_CONTEXT`` and
    writes nothing; ``retry_start_async(err)`` completes the start with the
    error's id.
    ``WorkflowSpec.start()`` and ``retry_start()`` check first, so their
    message names their own async form.

    Args:
        spec: The workflow specification.
        broker: PostgreSQL broker for database operations.
        workflow_id: Optional custom workflow ID.
        sent_at: Optional workflow call-site timestamp from Python.
        resend_on_transient_err: As for ``start_workflow_async()``.

    Returns:
        ``Ok(WorkflowHandle)`` on success, ``Err(WorkflowStartError)`` on failure.

    Raises:
        As ``start_workflow_async()``.
    """
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise
    from horsies.core.utils.loop_runner import (
        async_context_message,
        in_running_event_loop,
    )

    wf_id = workflow_id or str(uuid.uuid4())
    if in_running_event_loop():
        return Err(WorkflowStartAsyncContext(
            message=async_context_message(
                f'start_workflow() of workflow {spec.name!r}',
                'start_workflow_async(...)',
            ),
            workflow_name=spec.name,
            workflow_id=wf_id,
        ))
    workflow_sent_at = sent_at or datetime.now(timezone.utc)
    match _start_request(
        spec, broker, workflow_id, wf_id, workflow_sent_at, resend_on_transient_err,
    ):
        case Err(err_value=refused):
            return Err(refused)
        case Ok(ok_value=request):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    try:
        outcome = broker._core_handle.start_workflow(request)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(_start_call_failed(spec, wf_id, call_refusal_or_raise(exc)))
    return _start_answer(spec, broker, wf_id, outcome)


async def pause_workflow(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Pause a running workflow.

    Transitions workflow from RUNNING to PAUSED state. Already-running tasks
    will continue to completion, but:
    - No new PENDING tasks will become READY
    - No READY tasks will be enqueued
    - Workflow completion check is skipped while PAUSED

    Also pauses every RUNNING descendant workflow, and abandons the claimed
    tasks of the paused tree: their nodes go back to READY, so the resume
    enqueues new tasks. One statement in autocommit
    (``horsies_workflow_pause``) that the core runs on the broker's core
    handle (``CoreHandle.pause_workflow_async``); a cancel of the caller
    does not stop it.

    Use resume_workflow() to continue execution.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to pause.

    Returns:
        ``Ok(True)`` if workflow was paused.
        ``Ok(False)`` if workflow exists but is not RUNNING (no-op).
        ``Err(HandleWorkflowNotFound)`` if workflow does not exist.
        ``Err(HandleDbOperationFailed)`` on a database error or a core
        boundary error; the statement wrote nothing, or its commit outcome
        is unknown. An id that is not a UUID is not retryable.
        ``Err(HandleStoredDataInvalid)`` if a stored value does not parse.

    Raises:
        ForkedAfterCoreStart, CoreStopped: the core of this process refuses
            every call (``call_refusal_or_raise``). Nothing ran.
        PanicException: the function returned a row outside its contract
            (a defect of the core).
    """
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise
    from horsies.core.workflows.handle_records import (
        WorkflowOperation,
        core_call_error,
        workflow_uuid,
    )

    match workflow_uuid(workflow_id, WorkflowOperation.PAUSE):
        case Err(not_an_id):
            return Err(not_an_id)
        case Ok(workflow_key):
            pass
    try:
        record = await broker._core_handle.pause_workflow_async(workflow_key)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(core_call_error(
            call_refusal_or_raise(exc), workflow_id, WorkflowOperation.PAUSE,
        ))
    return _pause_answer(workflow_id, record)


def pause_workflow_blocking(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """The blocking form of ``pause_workflow()``: ``CoreHandle.pause_workflow``
    on the calling thread, with the GIL released, and the same outcomes. A
    raised signal handler (Ctrl+C) leaves the call; the pause runs on to its
    end. A pause that does not end within the core's bound of a blocking
    call is ``HandleDbOperationFailed`` with the ``CoreCallTimedOut``; its
    outcome is unknown.

    The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
    event loop (``pause_workflow_sync``, ``WorkflowHandle.pause``).
    """
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise
    from horsies.core.workflows.handle_records import (
        WorkflowOperation,
        core_call_error,
        workflow_uuid,
    )

    match workflow_uuid(workflow_id, WorkflowOperation.PAUSE):
        case Err(not_an_id):
            return Err(not_an_id)
        case Ok(workflow_key):
            pass
    try:
        record = broker._core_handle.pause_workflow(workflow_key)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(core_call_error(
            call_refusal_or_raise(exc), workflow_id, WorkflowOperation.PAUSE,
        ))
    return _pause_answer(workflow_id, record)


def _pause_answer(
    workflow_id: str,
    record: (
        WorkflowPaused
        | WorkflowNotRunning
        | WorkflowAbsent
        | HandleDbOperationFailed
        | HandleStoredDataInvalid
    ),
) -> HandleResult[bool]:
    """The answer of a pause for the core's record. A failure is the core's
    class, returned as it is. Pure."""
    from horsies._core import (
        WorkflowAbsent,
        WorkflowNotRunning,
        WorkflowPaused,
    )

    # The status trigger (horsies_notify_workflow_changes) sends
    # workflow_done for each paused workflow at commit, so get() returns
    # WORKFLOW_PAUSED without waiting for a poll.
    match record:
        case WorkflowPaused():
            return Ok(True)
        case WorkflowNotRunning():
            return Ok(False)
        case WorkflowAbsent():
            return Err(HandleWorkflowNotFound(
                message=f'Workflow {workflow_id} not found',
                workflow_id=workflow_id,
            ))
        case HandleDbOperationFailed() | HandleStoredDataInvalid():
            return Err(record)
        case _ as unreachable:
            assert_never(unreachable)


async def cascade_pause_to_children(
    session: AsyncSession,
    workflow_id: str,
) -> list[str]:
    """
    Iteratively pause all running child workflows using BFS.
    Avoids deep recursion for deeply nested workflows.
    """
    queue: deque[str] = deque([workflow_id])
    paused_child_ids: list[str] = []

    while queue:
        current_id = queue.popleft()

        # Find running child workflows
        children = await session.execute(
            GET_RUNNING_CHILD_WORKFLOWS_SQL,
            {'wf_id': current_id},
        )

        for child_row in children.fetchall():
            child_id = child_row.id
            paused_child_ids.append(child_id)

            # Pause child; the status trigger sends its workflow_done.
            await session.execute(
                PAUSE_CHILD_WORKFLOW_SQL,
                {'wf_id': child_id},
            )

            # Add to queue to pause its children
            queue.append(child_id)

    return paused_child_ids


def pause_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Pause a running workflow synchronously.

    The blocking form of ``pause_workflow()`` (``pause_workflow_blocking``):
    the same outcomes, on the calling thread. On a thread that runs an event
    loop it returns ``HandleAsyncContext`` and changes nothing.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to pause.

    Returns:
        ``Ok(True)`` if workflow was paused.
        ``Ok(False)`` if workflow exists but is not RUNNING (no-op).
        ``Err(...)`` on failure — see ``HandleOperationError``.
    """
    from horsies.core.utils.loop_runner import (
        async_context_message,
        in_running_event_loop,
    )

    if in_running_event_loop():
        return Err(HandleAsyncContext(
            message=async_context_message(
                f'pause_workflow_sync for workflow {workflow_id}',
                'pause_workflow(broker, workflow_id)',
            ),
            workflow_id=workflow_id,
        ))
    return pause_workflow_blocking(broker, workflow_id)


async def resume_workflow(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Resume a paused workflow and its paused descendants.

    The core runs it on the broker's core handle
    (``CoreHandle.resume_workflow_async``): one transaction, ``BEGIN``,
    ``horsies_workflow_resume``, one ``horsies_workflow_child_start`` per
    child start that it owes (each child built through the broker's child
    spec source, which runs its ``build_with``), ``COMMIT``. The function
    moves the workflow and its PAUSED descendants to RUNNING, promotes their
    PENDING nodes, enqueues their READY nodes, and runs the completion check
    of each resumed workflow, deepest first. After the commit, the parent
    nodes of the child workflows that the step ended advance (one
    transaction per level), then the recovery pass of the tree runs. A
    cancel of the caller does not stop it.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to resume.

    Returns:
        ``Ok(True)`` if workflow was resumed.
        ``Ok(False)`` if workflow exists but is not PAUSED (no-op).
        ``Err(HandleWorkflowNotFound)`` if workflow does not exist.
        ``Err(HandleDbOperationFailed)`` on a database error before the
        resume commits, a child build that the core could not run, or a
        core boundary error; the transaction rolled back, or its commit
        outcome is unknown. An id that is not a UUID is not retryable.
        ``Err(HandleStoredDataInvalid)`` if a stored value does not parse.
        ``Err(HandleResumeRecoveryFailed)`` when the resume committed and the
        parent advance or the recovery pass after it failed; the workflow
        is RUNNING.

    Raises:
        ForkedAfterCoreStart, CoreStopped: the core of this process refuses
            every call (``call_refusal_or_raise``). Nothing ran.
        PanicException: a row outside a function's contract, or a child tree
            that the core refuses (a defect); nothing committed.
        KeyboardInterrupt, SystemExit: raised by a child's ``build_with``;
            the resume rolled back.
        Exception: an exception that the build of a sub-workflow child raised
            in a resume that failed with it, such as ``SerializationError``
            for task options of a child root that do not parse.
    """
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise
    from horsies.core.workflows.handle_records import (
        WorkflowOperation,
        core_call_error,
        workflow_uuid,
    )

    match workflow_uuid(workflow_id, WorkflowOperation.RESUME):
        case Err(not_an_id):
            return Err(not_an_id)
        case Ok(workflow_key):
            pass
    try:
        record = await broker._core_handle.resume_workflow_async(workflow_key)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(core_call_error(
            call_refusal_or_raise(exc), workflow_id, WorkflowOperation.RESUME,
        ))
    return _resume_answer(workflow_id, record)


def resume_workflow_blocking(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """The blocking form of ``resume_workflow()``: ``CoreHandle.resume_workflow``
    on the calling thread, with the GIL released, and the same outcomes and
    raises. The child starts that the resume owes build their children by
    their ``build_with`` on the core's callback thread while this thread
    waits. A raised signal handler (Ctrl+C) leaves the call; the resume runs
    on to its end. A resume that does not end within the core's bound of a
    blocking call is ``HandleDbOperationFailed`` with the
    ``CoreCallTimedOut``; its outcome is unknown.

    The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
    event loop (``resume_workflow_sync``, ``WorkflowHandle.resume``).
    """
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise
    from horsies.core.workflows.handle_records import (
        WorkflowOperation,
        core_call_error,
        workflow_uuid,
    )

    match workflow_uuid(workflow_id, WorkflowOperation.RESUME):
        case Err(not_an_id):
            return Err(not_an_id)
        case Ok(workflow_key):
            pass
    try:
        record = broker._core_handle.resume_workflow(workflow_key)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(core_call_error(
            call_refusal_or_raise(exc), workflow_id, WorkflowOperation.RESUME,
        ))
    return _resume_answer(workflow_id, record)


def _resume_answer(
    workflow_id: str,
    record: (
        WorkflowResumed
        | WorkflowNotPaused
        | WorkflowAbsent
        | HandleDbOperationFailed
        | HandleStoredDataInvalid
        | HandleResumeRecoveryFailed
    ),
) -> HandleResult[bool]:
    """The answer of a resume for the core's record. A failure is the core's
    class, returned as it is. Pure."""
    from horsies._core import (
        WorkflowAbsent,
        WorkflowNotPaused,
        WorkflowResumed,
    )

    match record:
        case WorkflowResumed():
            return Ok(True)
        case WorkflowNotPaused():
            return Ok(False)
        case WorkflowAbsent():
            return Err(HandleWorkflowNotFound(
                message=f'Workflow {workflow_id} not found',
                workflow_id=workflow_id,
            ))
        case (
            HandleDbOperationFailed()
            | HandleStoredDataInvalid()
            | HandleResumeRecoveryFailed()
        ):
            return Err(record)
        case _ as unreachable:
            assert_never(unreachable)


def resume_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Resume a paused workflow synchronously.

    The blocking form of ``resume_workflow()`` (``resume_workflow_blocking``):
    the same outcomes and raises, on the calling thread. On a thread that
    runs an event loop it returns ``HandleAsyncContext`` and changes
    nothing.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to resume.

    Returns:
        ``Ok(True)`` if workflow was resumed.
        ``Ok(False)`` if workflow exists but is not PAUSED (no-op).
        ``Err(...)`` on failure — see ``HandleOperationError``.
    """
    from horsies.core.utils.loop_runner import (
        async_context_message,
        in_running_event_loop,
    )

    if in_running_event_loop():
        return Err(HandleAsyncContext(
            message=async_context_message(
                f'resume_workflow_sync for workflow {workflow_id}',
                'resume_workflow(broker, workflow_id)',
            ),
            workflow_id=workflow_id,
        ))
    return resume_workflow_blocking(broker, workflow_id)


async def expire_paused_workflows(
    session_factory: 'async_sessionmaker[AsyncSession]',
    *,
    older_than: timedelta,
    batch_size: int = PAUSED_EXPIRY_BATCH_SIZE,
) -> int:
    """Expire PAUSED workflows older than the declared age policy.

    One read of the candidates, oldest first, at most ``batch_size``
    (``idx_horsies_workflows_paused_age``); then one statement in autocommit
    per candidate (``horsies_workflow_expire_paused``). The function locks
    the workflow row first (the cancel lock order), then the live tasks
    behind its nodes and its live nodes; it writes EXPIRED only while the
    workflow is PAUSED and older than the age, so a resume that committed
    first wins. The tasks behind ENQUEUED nodes move to history under the
    workflow-cancel kind, and the nodes that did not start are SKIPPED. No
    parent-to-child cascade: pause already cascaded, so every paused child
    carries its own age and expires on it.

    The age goes to the database as exact microseconds
    (``older_than // timedelta(microseconds=1)``, multiplied by
    ``interval '1 microsecond'``): a time interval with no day field, so
    ``NOW()`` minus the age is the same span in any session time zone,
    also across a daylight-saving change. The core sends the same value.

    Returns the number of workflows expired. Each candidate commits alone,
    so the pass is bounded; a failure on one candidate raises, and the
    candidates after it wait for the next tick.

    Raises:
        OutcomeDecodeError: the function returned a row outside its
            contract (a defect).
        Exception: what the pool and the driver raise for a candidate: a
            pool timeout, a connection error, a statement error. The
            candidates before it are expired; a connection error after the
            statement was sent leaves its outcome unknown.
    """
    from horsies.core.workflows.functions import (
        EXPIRE_PAUSED_WORKFLOW_FUNCTION_SQL,
        PausedExpiryOutcome,
        decode_paused_expiry_outcome,
    )

    error_text = f'paused_workflow_auto_cancel_after elapsed: {older_than}'
    error_json = _ser_or_raise(
        dumps_json(
            encode_task_error(
                TaskError(
                    error_code=OutcomeCode.WORKFLOW_EXPIRED,
                    message=error_text,
                    data={
                        'policy': 'paused_workflow_auto_cancel_after',
                        'older_than_seconds': older_than.total_seconds(),
                    },
                )
            )
        ),
        'expired workflow error',
    )
    age_us = older_than // timedelta(microseconds=1)
    async with session_factory() as candidate_session:
        candidate_rows = (
            await candidate_session.execute(
                text("""
                    SELECT id FROM horsies_workflows
                    WHERE status = 'PAUSED'
                      AND updated_at < NOW()
                          - CAST(:age_us AS bigint) * interval '1 microsecond'
                    ORDER BY updated_at, id
                    LIMIT :batch_size
                """),
                {'age_us': age_us, 'batch_size': batch_size},
            )
        ).all()
    candidate_ids = [row.id for row in candidate_rows]

    expired_count = 0
    for workflow_id in candidate_ids:
        async with session_factory() as session:
            connection = await autocommit_connection(session)
            rows = (
                await connection.execute(
                    EXPIRE_PAUSED_WORKFLOW_FUNCTION_SQL,
                    {
                        'wf_id': workflow_id,
                        'age_us': age_us,
                        'error': error_json,
                    },
                )
            ).fetchall()
        match decode_paused_expiry_outcome(rows):
            case PausedExpiryOutcome.EXPIRED:
                expired_count += 1
            case PausedExpiryOutcome.NOT_EXPIRED | PausedExpiryOutcome.NOT_FOUND:
                pass
            case _ as unreachable:
                assert_never(unreachable)
    return expired_count
