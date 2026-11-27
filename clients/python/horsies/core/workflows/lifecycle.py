"""Workflow lifecycle: start, pause, resume."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, TypeVar, assert_never, cast




from horsies.core.types.result import Ok, Err, Result
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    WorkflowHandle,
    SubWorkflowNode,
    validate_workflow_generic_output_match,
)
from horsies.core.models.workflow.handle import _OUTPUTLESS_TERMINALS
from horsies.core.models.workflow.typing_utils import _resolve_source_node_ok_type
from horsies.core.errors import WorkflowValidationError, HorsiesErrorCode
from horsies.core.models.workflow.handle_types import (
    WorkflowHandleAsyncContext,
    WorkflowHandleDatabaseOperationFailed,
    HandleResult,
    WorkflowHandleResumeRecoveryFailed,
    WorkflowHandleStoredDataInvalid,
    WorkflowHandleNotFound,
)
from horsies.core.workflows.start_types import (
    WorkflowStartAsyncContext,
    WorkflowStartCommandConflict,
    WorkflowStartEnqueueFailed,
    WorkflowStartIdConflict,
    WorkflowStartResult,
    WorkflowStartValidationFailed,
)
from horsies.core.workflows.sql import SubWorkflowParams
from horsies.core.telemetry.send_telemetry import (
    NO_SEND_TELEMETRY,
    NO_TRACE_CARRIER,
    NoSendTelemetry,
    OtelSendTelemetry,
    SendTelemetry,
    TraceArgument,
    WorkflowStartSubject,
    traced,
    traced_async,
    workflow_start_end,
)

logger = get_logger('workflow.engine')

# Internal retry constants for resend_on_transient_err.


if TYPE_CHECKING:
    from horsies._core import (
        WorkflowAbsent,
        WorkflowExists,
        WorkflowNotPaused,
        WorkflowNotRunning,
        WorkflowPaused,
        WorkflowResumed,
        WorkflowStarted,
        WorkflowStartRequest,
    )
    from horsies.core.models.workflow import WorkflowSpec
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.types.core_failure import CoreCallRefusal

OutT = TypeVar('OutT')


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
        SerializationError: a value of the node rows does not serialize, or
            the task options of a node with a ``good_until`` do not parse to
            a JSON object (a defect of values that the process built and
            validated); or the build of a sub-workflow child raised it, and
            the start rolled back.
        PanicException: a row outside a function's contract, or spec data
            that the core refuses, such as task options whose retry policy
            or execution class does not validate (a defect). Nothing was
            written.
        KeyboardInterrupt, SystemExit: raised by a child's ``build_with``;
            the start rolled back.
        Exception: another exception that the build of a sub-workflow child
            raised; the start rolled back.

    Notes:
        If workflow_id is provided and already exists, the core compares the
        start command (the workflow settings and each node: task, input,
        queue, priority, dependencies, ``args_from``, context, join and task
        options; not the id, the call time or the trace). The same command
        returns the existing handle; another command of the same name, or the
        id of a sub-workflow, is ``WorkflowStartCommandConflict``; another
        name is ``WorkflowStartIdConflict``. Both write nothing and are not
        retryable. A run that an earlier horsies version started has no stored
        start command: its handle comes back with no compare.
    """
    wf_id = workflow_id or str(uuid.uuid4())
    workflow_sent_at = sent_at or datetime.now(timezone.utc)

    async def start(trace: TraceArgument) -> WorkflowStartResult[WorkflowHandle[OutT]]:
        return await _start_async_with_trace(
            spec, broker, workflow_id, wf_id, workflow_sent_at,
            resend_on_transient_err, trace,
        )

    match _start_telemetry(broker):
        case NoSendTelemetry():
            return await start(NO_TRACE_CARRIER)
        case OtelSendTelemetry(recorder=recorder):
            return await traced_async(
                lambda: recorder.start_workflow_start(
                    WorkflowStartSubject(workflow_name=spec.name, workflow_id=wf_id),
                ),
                start,
                workflow_start_end,
            )
        case _ as unreachable:
            assert_never(unreachable)


def _start_telemetry(broker: 'PostgresBroker') -> SendTelemetry:
    """The send telemetry of the app of ``broker``; ``NO_SEND_TELEMETRY``
    for a broker that no app built. Pure."""
    match broker.app:
        case None:
            return NO_SEND_TELEMETRY
        case app:
            return app.send_telemetry()


async def _start_async_with_trace(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None,
    wf_id: str,
    sent_at: datetime,
    resend_on_transient_err: bool,
    trace: TraceArgument,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """The start of ``start_workflow_async`` under ``wf_id``, with the trace
    ``trace``."""
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise

    match _start_request(
        spec, broker, workflow_id, wf_id, sent_at, resend_on_transient_err, trace,
    ):
        case Err(err_value=refused):
            return Err(refused)
        case Ok(ok_value=(request, workflow_key)):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    try:
        outcome = await broker._core_handle.start_workflow_async(request)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(_start_call_failed(spec, wf_id, call_refusal_or_raise(exc)))
    return _start_answer(spec, broker, wf_id, workflow_key, outcome)


def _start_request(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None,
    wf_id: str,
    sent_at: datetime,
    resend_on_transient_err: bool,
    trace: TraceArgument,
) -> Result[tuple[WorkflowStartRequest, uuid.UUID], WorkflowStartValidationFailed]:
    """The start request of ``spec`` under ``wf_id`` and the UUID that it
    sends, or the validation that refuses it. No I/O. ``workflow_id`` is the
    id that the caller gave, if any: only a given id is checked to be a UUID.
    The request carries ``trace``, which every row of the tree stores.

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
                    code=HorsiesErrorCode.WORKFLOW_INVALID_ID,
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
                    code=HorsiesErrorCode.WORKFLOW_UNRESOLVED_QUEUE,
                    notes=[f"TaskNode '{task.name}' has queue=None"],
                    help_text='use app.workflow() to create WorkflowSpec with proper validation',
                )
            if task.priority is None:
                raise WorkflowValidationError(
                    message='TaskNode has unresolved priority',
                    code=HorsiesErrorCode.WORKFLOW_UNRESOLVED_PRIORITY,
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
            cause=exc,
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
                cause=refused,
            ))
        case _ as unreachable:
            assert_never(unreachable)
    workflow_key = uuid.UUID(wf_id)
    return Ok((
        WorkflowStartRequest(
            spec_json=spec_json,
            workflow_id=workflow_key,
            sent_at=sent_at,
            resend_on_transient_err=resend_on_transient_err,
            trace=trace,
        ),
        workflow_key,
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
        cause=exc,
    )


def _start_answer(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    wf_id: str,
    workflow_key: uuid.UUID,
    outcome: (
        WorkflowStarted
        | WorkflowExists
        | WorkflowStartIdConflict
        | WorkflowStartCommandConflict
        | WorkflowStartEnqueueFailed
    ),
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """The answer of ``start_workflow`` and ``start_workflow_async`` for the
    core's answer. Pure apart from the WARNING log of an existing workflow
    and the type lookups of the handle's output type. The handle holds
    ``workflow_key``, the UUID that the start sent.

    A refused or failed start is the generated class that the core built
    (``WorkflowStartIdConflict``, ``WorkflowStartCommandConflict``,
    ``WorkflowStartEnqueueFailed``), returned
    as it is: its ``workflow_id`` is the canonical text of ``workflow_key``,
    and ``wf_id`` as given when the caller gave that text."""
    from horsies._core import (
        WorkflowExists,
        WorkflowStarted,
    )

    match outcome:
        case WorkflowStarted():
            pass
        case WorkflowExists():
            logger.warning(f'Workflow {wf_id} already exists, returning existing handle')
        case WorkflowStartIdConflict() | WorkflowStartCommandConflict() | WorkflowStartEnqueueFailed():
            return Err(outcome)
        case _ as unreachable:
            assert_never(unreachable)

    return Ok(cast(
        'WorkflowHandle[OutT]',
        WorkflowHandle(
            workflow_id=workflow_key,
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
    wf_id = workflow_id or str(uuid.uuid4())

    def start(trace: TraceArgument) -> WorkflowStartResult[WorkflowHandle[OutT]]:
        return _start_with_trace(
            spec, broker, workflow_id, wf_id, sent_at, resend_on_transient_err, trace,
        )

    match _start_telemetry(broker):
        case NoSendTelemetry():
            return start(NO_TRACE_CARRIER)
        case OtelSendTelemetry(recorder=recorder):
            return traced(
                lambda: recorder.start_workflow_start(
                    WorkflowStartSubject(workflow_name=spec.name, workflow_id=wf_id),
                ),
                start,
                workflow_start_end,
            )
        case _ as unreachable:
            assert_never(unreachable)


def _start_with_trace(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None,
    wf_id: str,
    sent_at: datetime | None,
    resend_on_transient_err: bool,
    trace: TraceArgument,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """The start of ``start_workflow`` under ``wf_id``, with the trace
    ``trace``: ``ASYNC_CONTEXT`` on a thread that runs an event loop, else
    the blocking core call."""
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise
    from horsies.core.utils.loop_runner import (
        async_context_message,
        in_running_event_loop,
    )

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
        spec, broker, workflow_id, wf_id, workflow_sent_at, resend_on_transient_err, trace,
    ):
        case Err(err_value=refused):
            return Err(refused)
        case Ok(ok_value=(request, workflow_key)):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    try:
        outcome = broker._core_handle.start_workflow(request)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(_start_call_failed(spec, wf_id, call_refusal_or_raise(exc)))
    return _start_answer(spec, broker, wf_id, workflow_key, outcome)


async def pause_workflow(
    broker: 'PostgresBroker',
    workflow_id: uuid.UUID,
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
        workflow_id: The workflow ID to pause: a ``UUID``
            (``WorkflowHandle.for_id`` parses a text).

    Returns:
        ``Ok(True)`` if workflow was paused.
        ``Ok(False)`` if workflow exists but is not RUNNING (no-op).
        ``Err(WorkflowHandleNotFound)`` if workflow does not exist.
        ``Err(WorkflowHandleDatabaseOperationFailed)`` on a database error or a core
        boundary error; the statement wrote nothing, or its commit outcome
        is unknown.
        ``Err(WorkflowHandleStoredDataInvalid)`` if a stored value does not parse.

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
    )

    try:
        record = await broker._core_handle.pause_workflow_async(workflow_id)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(core_call_error(
            call_refusal_or_raise(exc), workflow_id, WorkflowOperation.PAUSE,
        ))
    return _pause_answer(workflow_id, record)


def pause_workflow_blocking(
    broker: 'PostgresBroker',
    workflow_id: uuid.UUID,
) -> HandleResult[bool]:
    """The blocking form of ``pause_workflow()``: ``CoreHandle.pause_workflow``
    on the calling thread, with the GIL released, and the same outcomes. A
    raised signal handler (Ctrl+C) leaves the call; the pause runs on to its
    end. A pause that does not end within the core's bound of a blocking
    call is ``WorkflowHandleDatabaseOperationFailed`` with the ``CoreCallTimedOut``; its
    outcome is unknown.

    The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
    event loop (``pause_workflow_sync``, ``WorkflowHandle.pause``).
    """
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise
    from horsies.core.workflows.handle_records import (
        WorkflowOperation,
        core_call_error,
    )

    try:
        record = broker._core_handle.pause_workflow(workflow_id)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(core_call_error(
            call_refusal_or_raise(exc), workflow_id, WorkflowOperation.PAUSE,
        ))
    return _pause_answer(workflow_id, record)


def _pause_answer(
    workflow_id: uuid.UUID,
    record: (
        WorkflowPaused
        | WorkflowNotRunning
        | WorkflowAbsent
        | WorkflowHandleDatabaseOperationFailed
        | WorkflowHandleStoredDataInvalid
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
            return Err(WorkflowHandleNotFound(
                message=f'Workflow {workflow_id} not found',
                workflow_id=str(workflow_id),
            ))
        case WorkflowHandleDatabaseOperationFailed() | WorkflowHandleStoredDataInvalid():
            return Err(record)
        case _ as unreachable:
            assert_never(unreachable)


def pause_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: uuid.UUID,
) -> HandleResult[bool]:
    """Pause a running workflow synchronously.

    The blocking form of ``pause_workflow()`` (``pause_workflow_blocking``):
    the same outcomes, on the calling thread. On a thread that runs an event
    loop it returns ``WorkflowHandleAsyncContext`` and changes nothing.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to pause: a ``UUID``
            (``WorkflowHandle.for_id`` parses a text).

    Returns:
        ``Ok(True)`` if workflow was paused.
        ``Ok(False)`` if workflow exists but is not RUNNING (no-op).
        ``Err(...)`` on failure — see ``WorkflowHandleError``.
    """
    from horsies.core.utils.loop_runner import (
        async_context_message,
        in_running_event_loop,
    )

    if in_running_event_loop():
        return Err(WorkflowHandleAsyncContext(
            message=async_context_message(
                f'pause_workflow_sync for workflow {workflow_id}',
                'pause_workflow(broker, workflow_id)',
            ),
            workflow_id=str(workflow_id),
        ))
    return pause_workflow_blocking(broker, workflow_id)


async def resume_workflow(
    broker: 'PostgresBroker',
    workflow_id: uuid.UUID,
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
        workflow_id: The workflow ID to resume: a ``UUID``
            (``WorkflowHandle.for_id`` parses a text).

    Returns:
        ``Ok(True)`` if workflow was resumed.
        ``Ok(False)`` if workflow exists but is not PAUSED (no-op).
        ``Err(WorkflowHandleNotFound)`` if workflow does not exist.
        ``Err(WorkflowHandleDatabaseOperationFailed)`` on a database error before the
        resume commits, a child build that the core could not run, or a
        core boundary error; the transaction rolled back, or its commit
        outcome is unknown.
        ``Err(WorkflowHandleStoredDataInvalid)`` if a stored value does not parse.
        ``Err(WorkflowHandleResumeRecoveryFailed)`` when the resume committed and the
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
    )

    try:
        record = await broker._core_handle.resume_workflow_async(workflow_id)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(core_call_error(
            call_refusal_or_raise(exc), workflow_id, WorkflowOperation.RESUME,
        ))
    return _resume_answer(workflow_id, record)


def resume_workflow_blocking(
    broker: 'PostgresBroker',
    workflow_id: uuid.UUID,
) -> HandleResult[bool]:
    """The blocking form of ``resume_workflow()``: ``CoreHandle.resume_workflow``
    on the calling thread, with the GIL released, and the same outcomes and
    raises. The child starts that the resume owes build their children by
    their ``build_with`` on the core's callback thread while this thread
    waits. A raised signal handler (Ctrl+C) leaves the call; the resume runs
    on to its end. A resume that does not end within the core's bound of a
    blocking call is ``WorkflowHandleDatabaseOperationFailed`` with the
    ``CoreCallTimedOut``; its outcome is unknown.

    The caller returns ``ASYNC_CONTEXT`` first on a thread that runs an
    event loop (``resume_workflow_sync``, ``WorkflowHandle.resume``).
    """
    from horsies._core import CoreError
    from horsies.core.types.core_failure import call_refusal_or_raise
    from horsies.core.workflows.handle_records import (
        WorkflowOperation,
        core_call_error,
    )

    try:
        record = broker._core_handle.resume_workflow(workflow_id)  # pyright: ignore[reportPrivateUsage]
    except CoreError as exc:
        return Err(core_call_error(
            call_refusal_or_raise(exc), workflow_id, WorkflowOperation.RESUME,
        ))
    return _resume_answer(workflow_id, record)


def _resume_answer(
    workflow_id: uuid.UUID,
    record: (
        WorkflowResumed
        | WorkflowNotPaused
        | WorkflowAbsent
        | WorkflowHandleDatabaseOperationFailed
        | WorkflowHandleStoredDataInvalid
        | WorkflowHandleResumeRecoveryFailed
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
            return Err(WorkflowHandleNotFound(
                message=f'Workflow {workflow_id} not found',
                workflow_id=str(workflow_id),
            ))
        case (
            WorkflowHandleDatabaseOperationFailed()
            | WorkflowHandleStoredDataInvalid()
            | WorkflowHandleResumeRecoveryFailed()
        ):
            return Err(record)
        case _ as unreachable:
            assert_never(unreachable)


def resume_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: uuid.UUID,
) -> HandleResult[bool]:
    """Resume a paused workflow synchronously.

    The blocking form of ``resume_workflow()`` (``resume_workflow_blocking``):
    the same outcomes and raises, on the calling thread. On a thread that
    runs an event loop it returns ``WorkflowHandleAsyncContext`` and changes
    nothing.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to resume: a ``UUID``
            (``WorkflowHandle.for_id`` parses a text).

    Returns:
        ``Ok(True)`` if workflow was resumed.
        ``Ok(False)`` if workflow exists but is not PAUSED (no-op).
        ``Err(...)`` on failure — see ``WorkflowHandleError``.
    """
    from horsies.core.utils.loop_runner import (
        async_context_message,
        in_running_event_loop,
    )

    if in_running_event_loop():
        return Err(WorkflowHandleAsyncContext(
            message=async_context_message(
                f'resume_workflow_sync for workflow {workflow_id}',
                'resume_workflow(broker, workflow_id)',
            ),
            workflow_id=str(workflow_id),
        ))
    return resume_workflow_blocking(broker, workflow_id)


