# pyright: reportPrivateUsage=false
"""The caller of ``horsies_workflow_cancel``, before it called the core.

``WorkflowHandle.cancel_async`` (``python/horsies/core/models/workflow/handle.py``)
sent the function on an autocommit connection of the broker's SQLAlchemy
resources, one statement. ``cancel_async`` below is that body. It takes the
handle as its first parameter, so the dual-run rebinding can put it in the
place of the method.
"""

from __future__ import annotations

from typing import Any, assert_never

from sqlalchemy.exc import SQLAlchemyError

from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleResult,
    HandleWorkflowNotFound,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import autocommit_connection, is_retryable_connection_error
from horsies.core.workflows.functions import (
    CANCEL_WORKFLOW_FUNCTION_SQL,
    CancelOutcome,
    decode_cancel_outcome,
)


async def cancel_async(self: WorkflowHandle[Any]) -> HandleResult[None]:
    """Cancel the workflow tree: ``horsies_workflow_cancel`` on the broker's
    SQLAlchemy resources.

    ``Ok(None)``: cancelled, or already terminal. ``HandleWorkflowNotFound``,
    ``HandleDbOperationFailed``, ``HandleEventLoopLimit``.
    ``OutcomeDecodeError`` (raised) for a row outside the function's contract.
    """
    match await self.broker.select_resources():
        case Err(loop_limit):
            return self._event_loop_limit(loop_limit, 'cancel')
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            connection = await autocommit_connection(session)
            result = await connection.execute(
                CANCEL_WORKFLOW_FUNCTION_SQL,
                {'wf_id': self.workflow_id},
            )
            rows = result.fetchall()
    except SQLAlchemyError as exc:
        return Err(HandleDbOperationFailed(
            message=f'Cancel failed for workflow {self.workflow_id}: {exc}',
            retryable=is_retryable_connection_error(exc),
            workflow_id=self.workflow_id,
            exception=exc,
        ))
    match decode_cancel_outcome(rows):
        case CancelOutcome.CANCELLED | CancelOutcome.NOT_CANCELLABLE:
            return Ok(None)
        case CancelOutcome.NOT_FOUND:
            return Err(HandleWorkflowNotFound(
                message=f'Workflow {self.workflow_id} not found',
                workflow_id=self.workflow_id,
            ))
        case _ as unreachable:
            assert_never(unreachable)
