"""The caller of ``horsies_workflow_start``, before it called the core.

``start_workflow_async`` (``python/horsies/core/workflows/lifecycle.py``)
built the tree in Python (``start_tree``, which runs the ``build_with`` of
each pre-built child), then sent ``horsies_workflow_start`` on the broker's
SQLAlchemy resources: one statement in autocommit, or a transaction with the
child starts that it owed, with Python's retry of a transient failure.
``start_workflow_async`` below is that body, with the same signature, so the
dual-run rebinding can put it in the place of the production function.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final, TypeVar, assert_never, cast

from psycopg import Error as PsycopgError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.brokers.result_types import BrokerEventLoopLimit, BrokerOperationFailed
from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    SubWorkflowNode,
    WorkflowHandle,
    validate_workflow_generic_output_match,
)
from horsies.core.models.workflow.handle import _OUTPUTLESS_TERMINALS  # pyright: ignore[reportPrivateUsage]
from horsies.core.models.workflow.typing_utils import _resolve_source_node_ok_type  # pyright: ignore[reportPrivateUsage]
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import autocommit_connection, is_retryable_connection_error
from horsies.core.workflows.start_tree import start_parameters, start_tree
from horsies.core.workflows.start_types import (
    WorkflowStartEnqueueFailed,
    WorkflowStartEventLoopLimit,
    WorkflowStartIdConflict,
    WorkflowStartResult,
    WorkflowStartValidationFailed,
)
from horsies.core.workflows.steps import (
    START_WORKFLOW_FUNCTION_SQL,
    ParentAdvanceFailure,
    StartDisposition,
    advance_parents,
    decode_step_rows,
    require_no_child_starts,
    run_in_transaction,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.workflow import WorkflowSpec

OutT = TypeVar('OutT')

logger = get_logger('workflow.engine')

# 1 initial attempt + START_RETRY_COUNT retries = 4 total attempts.
START_RETRY_COUNT: Final[int] = 3
START_RETRY_INITIAL_MS: Final[int] = 200
START_RETRY_MAX_MS: Final[int] = 2000


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
            ``START_RETRY_COUNT`` times on transient infrastructure errors
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

        spec._require_definition_key()  # pyright: ignore[reportPrivateUsage]

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

    max_attempts = 1 + (START_RETRY_COUNT if resend_on_transient_err else 0)
    last_err: WorkflowStartEnqueueFailed | None = None

    for attempt in range(max_attempts):
        if attempt > 0 and last_err is not None:
            delay_ms = min(
                START_RETRY_INITIAL_MS * (2 ** (attempt - 1)),
                START_RETRY_MAX_MS,
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
                workflow_id=uuid.UUID(wf_id),
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
