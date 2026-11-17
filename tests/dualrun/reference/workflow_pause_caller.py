"""The caller of ``horsies_workflow_pause``, before it called the core.

``pause_workflow`` (``python/horsies/core/workflows/lifecycle.py``) sent the
function on an autocommit connection of the broker's SQLAlchemy resources,
one statement. ``pause_workflow`` below is that body, with the same signature,
so the dual-run rebinding can put it in the place of the production function.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, assert_never

from sqlalchemy.exc import SQLAlchemyError

from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleEventLoopLimit,
    HandleResult,
    HandleWorkflowNotFound,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import autocommit_connection, is_retryable_connection_error
from horsies.core.workflows.functions import (
    PAUSE_WORKFLOW_FUNCTION_SQL,
    PauseOutcome,
    decode_pause_outcome,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker


async def pause_workflow(
    broker: PostgresBroker,
    workflow_id: str,
) -> HandleResult[bool]:
    """Pause a running workflow: ``horsies_workflow_pause`` on the broker's
    SQLAlchemy resources.

    ``Ok(True)``: paused. ``Ok(False)``: not RUNNING. ``HandleWorkflowNotFound``,
    ``HandleDbOperationFailed``, ``HandleEventLoopLimit``. ``OutcomeDecodeError``
    (raised) for a row outside the function's contract.
    """
    match await broker.select_resources():
        case Err(loop_limit):
            return Err(HandleEventLoopLimit(
                message=f'pause of workflow {workflow_id}: {loop_limit.describe()}',
                workflow_id=workflow_id,
                loop_limit=loop_limit,
            ))
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
