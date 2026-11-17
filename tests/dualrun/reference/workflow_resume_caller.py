"""The caller of ``horsies_workflow_resume``, before it called the core.

``resume_workflow`` (``python/horsies/core/workflows/lifecycle.py``) ran the
function and the child starts that it owed in one transaction on the
broker's SQLAlchemy resources (the children built with ``build_with`` in
Python), then the parent advances and the recovery pass of the tree.
``resume_workflow`` below is that body, with the same signature, so the
dual-run rebinding can put it in the place of the production function.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, assert_never

from sqlalchemy.exc import SQLAlchemyError

from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleEventLoopLimit,
    HandleResult,
    HandleResumeRecoveryFailed,
    HandleWorkflowNotFound,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.workflows.steps import (
    RESUME_WORKFLOW_FUNCTION_SQL,
    ParentAdvanceFailure,
    ResumeDisposition,
    advance_parents,
    run_in_transaction,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker


def _event_loop_limit(
    workflow_id: str, operation: str, loop_limit: EventLoopLimit,
) -> Err[HandleEventLoopLimit]:
    """Build the lifecycle error for the event loop limit. Nothing ran."""
    return Err(HandleEventLoopLimit(
        message=f'{operation} of workflow {workflow_id}: {loop_limit.describe()}',
        workflow_id=workflow_id,
        loop_limit=loop_limit,
    ))


async def resume_workflow(
    broker: PostgresBroker,
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
        # propagation; once resume commits, run the recovery pass to close
        # that race through the same step functions. Scope it to the resumed
        # workflow's tree (self + descendants) so resume does not trigger a
        # full-DB recovery sweep over unrelated workflows.
        from horsies.core.workflows.recovery import recover_stuck_workflow_tree

        _ = await recover_stuck_workflow_tree(
            resources.session_factory,
            broker,
            workflow_id,
        )
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
