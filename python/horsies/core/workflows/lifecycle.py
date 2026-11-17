"""Workflow lifecycle: start, pause, resume."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from collections import deque
from typing import TYPE_CHECKING, Any, TypeVar, assert_never, cast

from psycopg import Error as PsycopgError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from typing import Final


from horsies.core.codec.json_io import dumps_json, loads_json, SerializationError, SerdeResult
from horsies.core.codec.task_options import stored_execution, stored_max_retries
from horsies.core.codec.typed import encode_task_error
from horsies.core.models.tasks import Execution, OutcomeCode, TaskError
from horsies.core.types.result import Ok, Err, is_err as _is_err
from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.brokers.result_types import (
    BrokerEventLoopLimit,
    BrokerOperationFailed,
)
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    WorkflowHandle,
    SubWorkflowNode,
    validate_workflow_generic_output_match,
)
from horsies.core.models.workflow.handle import _OUTPUTLESS_TERMINALS
from horsies.core.models.workflow.typing_utils import _resolve_source_node_ok_type
from horsies.core.errors import WorkflowValidationError, ErrorCode
from horsies.core.utils.db import autocommit_connection, is_retryable_connection_error
from horsies.core.models.workflow.handle_types import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleEventLoopLimit,
    HandleLoopRunnerFailed,
    HandleResult,
    HandleResumeRecoveryFailed,
    HandleWorkflowNotFound,
)
from horsies.core.workflows.start_types import (
    WorkflowStartAsyncContext,
    WorkflowStartEnqueueFailed,
    WorkflowStartEventLoopLimit,
    WorkflowStartIdConflict,
    WorkflowStartLoopRunnerFailed,
    WorkflowStartResult,
    WorkflowStartValidationFailed,
)
from horsies.core.history.periods import RetentionClassKey
from horsies.core.workflows.functions import (
    PAUSE_WORKFLOW_FUNCTION_SQL,
    PauseOutcome,
    decode_pause_outcome,
)
from horsies.core.workflows.start_tree import start_parameters, start_tree
from horsies.core.workflows.steps import (
    RESUME_WORKFLOW_FUNCTION_SQL,
    START_WORKFLOW_FUNCTION_SQL,
    ParentAdvanceFailure,
    ResumeDisposition,
    StartDisposition,
    advance_parents,
    decode_step_rows,
    require_no_child_starts,
    run_in_transaction,
)
from horsies.core.workflows.sql import (
    GET_RUNNING_CHILD_WORKFLOWS_SQL,
    PAUSE_CHILD_WORKFLOW_SQL,
    SubWorkflowParams,
    TaskNodeFacts,
    task_node_facts,
)

logger = get_logger('workflow.engine')

# Internal retry constants for resend_on_transient_err.
# 1 initial attempt + _START_RETRY_COUNT retries = 4 total attempts.
_START_RETRY_COUNT: Final[int] = 3
_START_RETRY_INITIAL_MS: Final[int] = 200
_START_RETRY_MAX_MS: Final[int] = 2000


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
    from horsies.core.models.workflow import WorkflowSpec
    from horsies.core.brokers.postgres import PostgresBroker

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
    if not task_options_json:
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
    if not task_options_json:
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


def _start_loop_limit(
    workflow_name: str, workflow_id: str, loop_limit: EventLoopLimit,
) -> WorkflowStartEventLoopLimit:
    """Build the start error for the event loop limit. Nothing was written."""
    return WorkflowStartEventLoopLimit(
        message=f'start of workflow {workflow_name!r}: {loop_limit.describe()}',
        workflow_name=workflow_name,
        workflow_id=workflow_id,
        loop_limit=loop_limit,
    )


_START_FUNCTION: Final = 'horsies_workflow_start'


async def _advance_after_start(
    broker: 'PostgresBroker',
    session_factory: async_sessionmaker[AsyncSession],
    workflow_id: str,
    parent_advances: tuple[str, ...],
) -> None:
    """Advance the parent nodes of the child workflows that the owed child
    starts of a start ended, one transaction per level.

    The start committed, so a failed advance does not fail it: the error is
    logged, and workflow recovery advances the node (a terminal child under
    a live parent node). Does nothing when none is owed.
    """
    if not parent_advances:
        return
    failure = await advance_parents(broker, session_factory, parent_advances)
    match failure:
        case None:
            return
        case ParentAdvanceFailure(failed_child_workflow_id=child_id, cause=cause):
            logger.error(
                f'Parent advance after the start of workflow {workflow_id} failed at '
                f'child workflow {child_id}: {type(cause).__name__}: {cause}; '
                'workflow recovery advances it',
            )


async def start_workflow_async(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
    sent_at: datetime | None = None,
    *,
    resend_on_transient_err: bool = False,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """Start a workflow asynchronously.

    The tree is built in memory first (``start_tree``): the workflow row,
    every node row, a pre-built child for every sub-workflow root (its
    ``build_with`` runs here, also when the id exists already, so it must
    have no side effects), and a failed-node record for each sub-workflow
    root whose child cannot be built. Then one call of
    ``horsies_workflow_start`` writes it and enqueues the root nodes. The
    sub-workflow roots of each workflow are taken in index order, each child
    tree before the next root. A pre-built child of a root that comes after a
    failure that pauses its parent workflow is not written: its node stays
    READY, and the resume builds the child again. The call:

    - no failed-node record: one statement in autocommit;
    - failed-node records: ``BEGIN``, the start, one
      ``horsies_workflow_child_start`` per child start that their
      progression owes, ``COMMIT``. After the commit, the parent nodes of
      the child workflows that those child starts ended advance, one
      transaction per level; a failed advance is logged and left to
      workflow recovery.

    Args:
        spec: The workflow specification.
        broker: PostgreSQL broker for database operations.
        workflow_id: Optional custom workflow ID.
        sent_at: Optional workflow call-site timestamp from Python.
        resend_on_transient_err: When ``True``, automatically retry up to
            ``_START_RETRY_COUNT`` times on transient infrastructure errors
            (schema init + DB transaction) with exponential backoff.

    Returns:
        ``Ok(WorkflowHandle)`` on success.
        ``Err(WorkflowStartError)`` on failure; match on the variant. A
        ``WorkflowValidationError`` from prevalidation is
        ``WorkflowStartValidationFailed``. A database or driver error of the
        start statement is ``WorkflowStartEnqueueFailed``; nothing committed,
        or the commit outcome is unknown (a resend with the same id then
        finds the workflow). Any other exception from prevalidation, from
        the tree build or from the statement is a defect and propagates,
        also a ``SerializationError`` (the tree encodes only values that the
        process built and validated before the start) and an
        ``OutcomeDecodeError`` (a row outside the function's contract).

    Notes:
        If workflow_id is provided and already exists with the same name,
        returns the existing handle (best-effort idempotent by workflow_id,
        not payload-verified); with another name, ``WorkflowStartIdConflict``.
    """
    wf_id = workflow_id or str(uuid.uuid4())
    workflow_sent_at = sent_at or datetime.now(timezone.utc)

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
    except asyncio.CancelledError:
        raise
    except WorkflowValidationError as exc:
        # The checks above raise only WorkflowValidationError on purpose.
        # Any other exception is a defect and propagates.
        return Err(WorkflowStartValidationFailed(
            message=str(exc),
            workflow_name=spec.name,
            workflow_id=wf_id,
            exception=exc,
        ))

    # ── Zone 1b: the tree (no I/O) ───────────────────────────────────
    # The child of every sub-workflow root is built here, before the
    # statement: its build_with runs even when the id exists already.
    tree = start_tree(broker, spec, wf_id)
    parameters = start_parameters(tree, workflow_sent_at)

    # ── Zone 2: Schema init + DB transaction (retried when transient) ─
    from horsies.core.types.result import is_err

    match await broker.select_resources():
        case Err(loop_limit):
            return Err(_start_loop_limit(spec.name, wf_id, loop_limit))
        case Ok(resources):
            pass

    max_attempts = 1 + (_START_RETRY_COUNT if resend_on_transient_err else 0)
    last_err: WorkflowStartEnqueueFailed | None = None

    for attempt in range(max_attempts):
        if attempt > 0 and last_err is not None:
            delay_ms = min(
                _START_RETRY_INITIAL_MS * (2 ** (attempt - 1)),
                _START_RETRY_MAX_MS,
            )
            await asyncio.sleep(delay_ms / 1000.0)

        # ── Schema init ──────────────────────────────────────────────
        init_result = await broker.ensure_schema_initialized()
        if is_err(init_result):
            match init_result.err_value:
                case BrokerOperationFailed(
                    message=message, retryable=retryable, exception=cause,
                ):
                    last_err = WorkflowStartEnqueueFailed(
                        message=f'Schema initialization failed: {message}',
                        retryable=retryable,
                        workflow_name=spec.name,
                        workflow_id=wf_id,
                        exception=cause,
                    )
                case BrokerEventLoopLimit(loop_limit=loop_limit):
                    return Err(_start_loop_limit(spec.name, wf_id, loop_limit))
                case _ as unreachable:
                    assert_never(unreachable)
            if last_err.retryable and attempt < max_attempts - 1:
                continue
            return Err(last_err)

        # ── The start statement ──────────────────────────────────────
        try:
            async with resources.session_factory() as session:
                match tree.has_failed_records:
                    case False:
                        connection = await autocommit_connection(session)
                        rows = (
                            await connection.execute(START_WORKFLOW_FUNCTION_SQL, parameters)
                        ).fetchall()
                        outcome = require_no_child_starts(
                            decode_step_rows(
                                rows, _START_FUNCTION, StartDisposition, tree.workflow_names(),
                            ),
                            _START_FUNCTION,
                        )
                        parent_advances: tuple[str, ...] = ()
                    case True:
                        committed = await run_in_transaction(
                            session,
                            broker,
                            START_WORKFLOW_FUNCTION_SQL,
                            parameters,
                            _START_FUNCTION,
                            StartDisposition,
                            tree.workflow_names(),
                        )
                        outcome = committed.outcome
                        parent_advances = committed.parent_advances
        except asyncio.CancelledError:
            raise
        except (SQLAlchemyError, PsycopgError) as exc:
            # A database or driver error. Any other exception is a defect
            # and propagates.
            last_err = WorkflowStartEnqueueFailed(
                message=f'{type(exc).__name__}: {exc}',
                retryable=is_retryable_connection_error(exc),
                workflow_name=spec.name,
                workflow_id=wf_id,
                exception=exc,
            )
            if last_err.retryable and attempt < max_attempts - 1:
                continue
            return Err(last_err)

        await _advance_after_start(broker, resources.session_factory, wf_id, parent_advances)
        match outcome.disposition:
            case StartDisposition.STARTED:
                pass
            case StartDisposition.EXISTS:
                match outcome.detail:
                    case str() as existing_name if existing_name != spec.name:
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
                    case _:
                        logger.warning(
                            f'Workflow {wf_id} already exists, returning existing handle'
                        )
            case _ as unreachable:
                assert_never(unreachable)

        # Success — exit retry loop.
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

    # Unreachable: max_attempts >= 1, and the last attempt returns.
    assert last_err is not None, (
        'start attempt loop ended without a failed attempt'
    )
    return Err(last_err)


def start_workflow(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
    sent_at: datetime | None = None,
    *,
    resend_on_transient_err: bool = False,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """Start a workflow synchronously.

    Sync wrapper around ``start_workflow_async()``.

    Inner async ``Err`` passes through unchanged.
    A failure of the broker's sync runner is returned as
    ``LOOP_RUNNER_FAILED``; nothing ran. Any other exception propagates,
    as from ``start_workflow_async()``.
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
        resend_on_transient_err: Forwarded to ``start_workflow_async()``.

    Returns:
        ``Ok(WorkflowHandle)`` on success, ``Err(WorkflowStartError)`` on failure.
    """
    from horsies.core.utils.loop_runner import (
        LoopRunnerError,
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
    try:
        return broker.run_sync(
            start_workflow_async,
            spec,
            broker,
            wf_id,
            workflow_sent_at,
            resend_on_transient_err=resend_on_transient_err,
        )
    except LoopRunnerError as exc:
        return Err(WorkflowStartLoopRunnerFailed(
            message=f'Loop runner failed for start_workflow(): {exc}',
            workflow_name=spec.name,
            workflow_id=wf_id,
            exception=exc,
        ))





def _event_loop_limit(
    workflow_id: str, operation: str, loop_limit: EventLoopLimit,
) -> Err[HandleEventLoopLimit]:
    """Build the lifecycle error for the event loop limit. Nothing ran."""
    return Err(HandleEventLoopLimit(
        message=f'{operation} of workflow {workflow_id}: {loop_limit.describe()}',
        workflow_id=workflow_id,
        loop_limit=loop_limit,
    ))


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
    (``horsies_workflow_pause``).

    Use resume_workflow() to continue execution.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to pause.

    Returns:
        ``Ok(True)`` if workflow was paused.
        ``Ok(False)`` if workflow exists but is not RUNNING (no-op).
        ``Err(HandleWorkflowNotFound)`` if workflow does not exist.
        ``Err(HandleDbOperationFailed)`` on a database error; the
        statement wrote nothing, or its commit outcome is unknown.
        ``Err(HandleEventLoopLimit)`` if the broker already serves its limit
        of open event loops and this is a new one; nothing ran.
        ``OutcomeDecodeError`` if the function returns a row outside its
        contract (a defect). Any other exception propagates.
    """
    match await broker.select_resources():
        case Err(loop_limit):
            return _event_loop_limit(workflow_id, 'pause', loop_limit)
        case Ok(resources):
            pass
    try:
        async with resources.session_factory() as session:
            connection = await autocommit_connection(session)
            result = await connection.execute(
                PAUSE_WORKFLOW_FUNCTION_SQL,
                {'wf_id': workflow_id},
            )
            rows = result.fetchall()
    except SQLAlchemyError as exc:
        return Err(HandleDbOperationFailed(
            message=f'Pause failed for workflow {workflow_id}: {type(exc).__name__}: {exc}',
            retryable=is_retryable_connection_error(exc),
            workflow_id=workflow_id,
            exception=exc,
        ))
    # The status trigger (horsies_notify_workflow_changes) sends
    # workflow_done for each paused workflow at commit, so get() returns
    # WORKFLOW_PAUSED without waiting for a poll.
    match decode_pause_outcome(rows):
        case PauseOutcome.PAUSED:
            return Ok(True)
        case PauseOutcome.NOT_RUNNING:
            return Ok(False)
        case PauseOutcome.NOT_FOUND:
            return Err(HandleWorkflowNotFound(
                message=f'Workflow {workflow_id} not found',
                workflow_id=workflow_id,
            ))
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

    Sync wrapper around ``pause_workflow()``.

    Inner async ``Err`` passes through unchanged.
    A failure of the broker's sync runner is returned as
    ``HandleLoopRunnerFailed``; nothing ran. Any other exception propagates,
    as from the async form.
    On a thread that runs an event loop it returns ``HandleAsyncContext``
    and changes nothing.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to pause.

    Returns:
        ``Ok(True)`` if workflow was paused.
        ``Ok(False)`` if workflow exists but is not RUNNING (no-op).
        ``Err(...)`` on failure — see ``HandleOperationError``.
    """
    from horsies.core.utils.loop_runner import (
        LoopRunnerError,
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
    try:
        return broker.run_sync(pause_workflow, broker, workflow_id)
    except LoopRunnerError as exc:
        return Err(HandleLoopRunnerFailed(
            message=f'Loop runner failed for pause_workflow_sync(): {exc}',
            workflow_id=workflow_id,
            exception=exc,
        ))






async def resume_workflow(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Resume a paused workflow and its paused descendants.

    One transaction: ``BEGIN``, ``horsies_workflow_resume``, one
    ``horsies_workflow_child_start`` per child start that it owes, ``COMMIT``.
    The function moves the workflow and its PAUSED descendants to RUNNING,
    promotes their PENDING nodes, enqueues their READY nodes (task rows
    minted, child starts owed for sub-workflow nodes), and runs the
    completion check of each resumed workflow, deepest first. After the
    commit, the parent nodes of the child workflows that the step ended
    advance (one transaction per level), then the recovery pass of the tree
    runs, as before.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to resume.

    Returns:
        ``Ok(True)`` if workflow was resumed.
        ``Ok(False)`` if workflow exists but is not PAUSED (no-op).
        ``Err(HandleWorkflowNotFound)`` if workflow does not exist.
        ``Err(HandleDbOperationFailed)`` on a database error before the
        resume commits; the transaction rolled back, or its commit outcome
        is unknown.
        ``Err(HandleResumeRecoveryFailed)`` when the resume committed and the
        parent advance or the recovery pass after it failed; the workflow
        is RUNNING.
        ``Err(HandleEventLoopLimit)`` if the broker already serves its limit
        of open event loops and this is a new one; nothing ran.
        ``OutcomeDecodeError`` for a row outside the function's contract,
        ``SerializationError`` for a child tree value that does not
        serialize, and ``ChildStartLimitExceeded``: defects; nothing
        committed. Any other exception propagates.
    """
    match await broker.select_resources():
        case Err(loop_limit):
            return _event_loop_limit(workflow_id, 'resume', loop_limit)
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            committed = await run_in_transaction(
                session,
                broker,
                RESUME_WORKFLOW_FUNCTION_SQL,
                {'wf_id': workflow_id},
                'horsies_workflow_resume',
                ResumeDisposition,
            )
    except SQLAlchemyError as exc:
        return Err(HandleDbOperationFailed(
            message=f'Resume failed for workflow {workflow_id}: {type(exc).__name__}: {exc}',
            retryable=is_retryable_connection_error(exc),
            workflow_id=workflow_id,
            exception=exc,
        ))
    match committed.outcome.disposition:
        case ResumeDisposition.NOT_FOUND:
            return Err(HandleWorkflowNotFound(
                message=f'Workflow {workflow_id} not found',
                workflow_id=workflow_id,
            ))
        case ResumeDisposition.NOT_PAUSED:
            return Ok(False)
        case ResumeDisposition.RESUMED:
            pass
        case _ as unreachable:
            assert_never(unreachable)

    match await advance_parents(broker, resources.session_factory, committed.parent_advances):
        case None:
            pass
        case ParentAdvanceFailure(failed_child_workflow_id=child_id, cause=cause):
            return Err(HandleResumeRecoveryFailed(
                message=(
                    f'Workflow {workflow_id} resumed; the parent advance of child '
                    f'workflow {child_id} failed: {type(cause).__name__}: {cause}'
                ),
                workflow_id=workflow_id,
                exception=cause,
            ))

    try:
        # A task can finish while this transaction has resumed a child workflow
        # but before that status change is visible to the task-completion
        # callback. The callback observes PAUSED and correctly avoids
        # propagation; once resume commits, run the recovery completion pass to
        # close that race through the same canonical engine paths. Scope it to
        # the resumed workflow's tree (self + descendants) so resume does not
        # trigger a full-DB recovery sweep over unrelated workflows.
        async with resources.session_factory() as recovery_session:
            from horsies.core.workflows.recovery import (
                GET_WORKFLOW_TREE_IDS_SQL,
                recover_stuck_workflow_tree,
            )

            tree_result = await recovery_session.execute(
                GET_WORKFLOW_TREE_IDS_SQL,
                {'wf_id': workflow_id},
            )
            tree_ids = [r.id for r in tree_result.fetchall()]
            await recover_stuck_workflow_tree(
                recovery_session,
                broker,
                tree_ids,
            )
            await recovery_session.commit()
    except SQLAlchemyError as exc:
        return Err(HandleResumeRecoveryFailed(
            message=(
                f'Workflow {workflow_id} resumed; the recovery pass for its '
                f'tree failed: {type(exc).__name__}: {exc}'
            ),
            workflow_id=workflow_id,
            exception=exc,
        ))
    return Ok(True)


def resume_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Resume a paused workflow synchronously.

    Sync wrapper around ``resume_workflow()``.

    Inner async ``Err`` passes through unchanged.
    A failure of the broker's sync runner is returned as
    ``HandleLoopRunnerFailed``; nothing ran. Any other exception propagates,
    as from the async form.
    On a thread that runs an event loop it returns ``HandleAsyncContext``
    and changes nothing.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to resume.

    Returns:
        ``Ok(True)`` if workflow was resumed.
        ``Ok(False)`` if workflow exists but is not PAUSED (no-op).
        ``Err(...)`` on failure — see ``HandleOperationError``.
    """
    from horsies.core.utils.loop_runner import (
        LoopRunnerError,
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
    try:
        return broker.run_sync(resume_workflow, broker, workflow_id)
    except LoopRunnerError as exc:
        return Err(HandleLoopRunnerFailed(
            message=f'Loop runner failed for resume_workflow_sync(): {exc}',
            workflow_id=workflow_id,
            exception=exc,
        ))


async def expire_paused_workflows(
    session_factory: 'async_sessionmaker[AsyncSession]',
    *,
    older_than: timedelta,
    batch_size: int = 50,
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
