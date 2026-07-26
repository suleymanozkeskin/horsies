# pyright: reportUnusedFunction=false
# Route handlers are registered by their decorator, not called by name.
"""Action endpoints.

Every action's authority is a compare-and-set in the database, so there is no
optimistic-concurrency field to send: a lost race surfaces as 409 carrying the
status actually observed, and the client refetches.

Three primitives return an outcome the caller cannot interpret alone, and each
is resolved by re-reading the run's status server-side rather than pushing the
ambiguity to the browser:

* pause/resume answer ``Ok(False)`` when the run existed but was not in the
  expected state — not an error, but not the requested effect either.
* workflow cancel answers ``Ok(None)`` even when it was a no-op on an already
  finished run.
* resume can fail *after* committing, because its recovery pass runs in a
  second transaction. A run found RUNNING after such a failure was resumed;
  the response says so and carries a warning.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import get_logger
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    HandleErrorCode,
    HandleOperationError,
)
from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import pause_workflow, resume_workflow
from horsies.monitoring import (
    TaskActionError,
    TaskActionErrorCode,
    cancel_task,
    retry_task,
)

logger = get_logger('web')

STATE_CONFLICT = 'STATE_CONFLICT'
RESUME_RECOVERY_WARNING = 'post_resume_recovery_failed'


class CancelTaskBody(BaseModel):
    """Cancelling a RUNNING task requires saying so explicitly."""

    include_running: bool = False


class ActionResponse(BaseModel):
    """The success envelope shared by every action."""

    outcome: str
    was_status: str | None = None
    next_attempt_number: int | None = None
    warning: str | None = None


@dataclass(frozen=True, slots=True)
class _Outcome:
    """A decided response, before it is logged and rendered."""

    status_code: int
    content: dict[str, Any]


def _succeeded(
    outcome: str,
    *,
    was_status: str | None = None,
    next_attempt_number: int | None = None,
    warning: str | None = None,
) -> _Outcome:
    return _Outcome(
        status_code=200,
        content=ActionResponse(
            outcome=outcome,
            was_status=was_status,
            next_attempt_number=next_attempt_number,
            warning=warning,
        ).model_dump(),
    )


def _conflict(code: str, current_status: str | None) -> _Outcome:
    return _Outcome(
        status_code=409,
        content={'code': code, 'current_status': current_status},
    )


def _task_action_failed(error: TaskActionError) -> _Outcome:
    """Map a task action failure onto its response."""
    match error.code:
        case TaskActionErrorCode.TASK_NOT_FOUND:
            return _Outcome(status_code=404, content={'detail': error.message})
        case TaskActionErrorCode.TASK_IS_WORKFLOW_TASK:
            return _Outcome(status_code=400, content={'code': error.code.value})
        case (
            TaskActionErrorCode.TASK_NOT_CANCELLABLE
            | TaskActionErrorCode.TASK_NOT_RETRYABLE
            | TaskActionErrorCode.TASK_EXPIRY_PASSED
        ):
            current = error.current_status
            return _conflict(
                error.code.value, current.value if current is not None else None
            )
        case TaskActionErrorCode.DB_OPERATION_FAILED:
            return _Outcome(status_code=503, content={'detail': error.message})


def _handle_failed(error: HandleOperationError) -> _Outcome:
    """Map a workflow primitive's failure onto its response."""
    if error.code is HandleErrorCode.WORKFLOW_NOT_FOUND:
        return _Outcome(status_code=404, content={'detail': error.message})
    return _Outcome(status_code=503, content={'detail': error.message})


def _respond(route: str, entity_id: str, outcome: _Outcome) -> JSONResponse:
    """Log the decision and render it.

    One line per action: what was attempted, on what, and how it ended.
    """
    logger.info(
        f'monitoring action route={route} id={entity_id} '
        f'http={outcome.status_code} '
        f'result={outcome.content.get("outcome") or outcome.content.get("code") or "error"}'
    )
    return JSONResponse(status_code=outcome.status_code, content=outcome.content)


def build_router(broker: PostgresBroker) -> APIRouter:
    """Build the action router bound to one broker."""
    router = APIRouter(tags=['actions'])

    async def _current_status(workflow_id: str) -> WorkflowStatus | _Outcome:
        """Re-read a run's status, or the response explaining why we cannot."""
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=workflow_id, broker=broker
        )
        result = await handle.status_async()
        if is_err(result):
            return _handle_failed(result.err_value)
        return result.ok_value

    @router.post('/tasks/{task_id}/cancel')
    async def cancel_task_route(
        task_id: str, body: CancelTaskBody = CancelTaskBody()
    ) -> JSONResponse:
        """Cancel a task. RUNNING requires ``include_running``."""
        result = await cancel_task(
            broker, task_id, include_running=body.include_running
        )
        if is_err(result):
            return _respond(
                'tasks.cancel', task_id, _task_action_failed(result.err_value)
            )
        return _respond(
            'tasks.cancel',
            task_id,
            _succeeded('cancelled', was_status=result.ok_value.was_status.value),
        )

    @router.post('/tasks/{task_id}/retry')
    async def retry_task_route(task_id: str) -> JSONResponse:
        """Reset a settled task and re-enqueue it on its original queue."""
        result = await retry_task(broker, task_id)
        if is_err(result):
            return _respond(
                'tasks.retry', task_id, _task_action_failed(result.err_value)
            )
        retried = result.ok_value
        return _respond(
            'tasks.retry',
            task_id,
            _succeeded(
                'retried',
                was_status=retried.was_status.value,
                next_attempt_number=retried.next_attempt_number,
            ),
        )

    @router.post('/workflows/{workflow_id}/pause')
    async def pause_workflow_route(workflow_id: str) -> JSONResponse:
        """Stop scheduling new nodes. Executing nodes finish."""
        result = await pause_workflow(broker, workflow_id)
        if is_err(result):
            return _respond(
                'workflows.pause', workflow_id, _handle_failed(result.err_value)
            )
        if result.ok_value:
            return _respond('workflows.pause', workflow_id, _succeeded('paused'))

        current = await _current_status(workflow_id)
        if isinstance(current, _Outcome):
            return _respond('workflows.pause', workflow_id, current)
        return _respond(
            'workflows.pause',
            workflow_id,
            _conflict(STATE_CONFLICT, current.value),
        )

    @router.post('/workflows/{workflow_id}/resume')
    async def resume_workflow_route(workflow_id: str) -> JSONResponse:
        """Re-enqueue paused nodes.

        A failure here can still mean the resume committed: the recovery pass
        runs in its own transaction after the state change. The run's status
        settles that question.
        """
        result = await resume_workflow(broker, workflow_id)
        if is_err(result):
            error = result.err_value
            if error.code is HandleErrorCode.WORKFLOW_NOT_FOUND:
                return _respond('workflows.resume', workflow_id, _handle_failed(error))
            current = await _current_status(workflow_id)
            if isinstance(current, _Outcome):
                return _respond('workflows.resume', workflow_id, current)
            if current is WorkflowStatus.RUNNING:
                return _respond(
                    'workflows.resume',
                    workflow_id,
                    _succeeded('resumed', warning=RESUME_RECOVERY_WARNING),
                )
            return _respond(
                'workflows.resume',
                workflow_id,
                _Outcome(status_code=503, content={'detail': error.message}),
            )
        if result.ok_value:
            return _respond('workflows.resume', workflow_id, _succeeded('resumed'))

        current = await _current_status(workflow_id)
        if isinstance(current, _Outcome):
            return _respond('workflows.resume', workflow_id, current)
        return _respond(
            'workflows.resume',
            workflow_id,
            _conflict(STATE_CONFLICT, current.value),
        )

    @router.post('/workflows/{workflow_id}/cancel')
    async def cancel_workflow_route(workflow_id: str) -> JSONResponse:
        """Cancel a run and its descendants.

        ``cancel_async`` reports success even when it was a no-op on a run
        that had already finished, so the run's status decides the response.
        """
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=workflow_id, broker=broker
        )
        result = await handle.cancel_async()
        if is_err(result):
            return _respond(
                'workflows.cancel', workflow_id, _handle_failed(result.err_value)
            )

        current = await _current_status(workflow_id)
        if isinstance(current, _Outcome):
            return _respond('workflows.cancel', workflow_id, current)
        if current is WorkflowStatus.CANCELLED:
            return _respond('workflows.cancel', workflow_id, _succeeded('cancelled'))
        return _respond(
            'workflows.cancel',
            workflow_id,
            _conflict(STATE_CONFLICT, current.value),
        )

    return router
