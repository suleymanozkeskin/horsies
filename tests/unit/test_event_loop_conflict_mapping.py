"""EVENT_LOOP_CONFLICT keeps its own code when it crosses a catalog boundary.

Each mapping must not fold the conflict into a generic failure: the retry
condition differs (it clears when the other event loop closes, not when the
database recovers).
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendErrorCode
from horsies.core.models.tasks import RetrievalCode, TaskError, TaskResult
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    HandleErrorCode,
    HandleOperationError,
)
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.result import Err, is_err
from horsies.monitoring.task_actions import TaskActionError, TaskActionErrorCode
from horsies.web.routes.actions import _task_action_failed

CONFLICT_MESSAGE = 'resources in use by the open event loop on thread MainThread'


def _conflict_broker_error() -> BrokerOperationError:
    return BrokerOperationError(
        code=BrokerErrorCode.EVENT_LOOP_CONFLICT,
        message=CONFLICT_MESSAGE,
        retryable=False,
    )


@pytest.mark.unit
def test_send_maps_broker_conflict_to_send_conflict() -> None:
    def echo(*, x: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=x)

    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.custom_queues = None
    app.config.exception_mapper = {}
    app.config.default_unhandled_error_code = 'UNHANDLED_EXCEPTION'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = 'default'
    broker = MagicMock()
    broker.enqueue.return_value = Err(_conflict_broker_error())
    app.get_broker.return_value = broker
    wrapper = create_task_wrapper(echo, app, 'test.echo')

    result = wrapper.send(x=1)

    assert is_err(result)
    assert result.err_value.code is TaskSendErrorCode.EVENT_LOOP_CONFLICT
    assert result.err_value.retryable is False
    assert result.err_value.message == CONFLICT_MESSAGE


@pytest.mark.unit
def test_web_task_action_maps_conflict_to_503() -> None:
    error = TaskActionError(
        code=TaskActionErrorCode.EVENT_LOOP_CONFLICT,
        message=CONFLICT_MESSAGE,
        retryable=False,
        task_id='t-1',
        current_status=None,
    )

    outcome = _task_action_failed(error)

    assert outcome.status_code == 503
    assert outcome.content == {'detail': CONFLICT_MESSAGE}


@pytest.mark.unit
def test_workflow_get_maps_handle_conflict_to_retrieval_code() -> None:
    broker = MagicMock()
    broker.listener.listen = AsyncMock(return_value=Err(MagicMock()))
    handle: WorkflowHandle[Any] = WorkflowHandle(workflow_id='wf-1', broker=broker)
    handle.status_async = AsyncMock(  # type: ignore[method-assign]
        return_value=Err(HandleOperationError(
            code=HandleErrorCode.EVENT_LOOP_CONFLICT,
            message=CONFLICT_MESSAGE,
            retryable=False,
            workflow_id='wf-1',
        )),
    )

    result = asyncio.run(handle.get_async(timeout_ms=100))

    assert result.is_err()
    err = result.unwrap_err()
    assert err.error_code is RetrievalCode.EVENT_LOOP_CONFLICT
    assert err.message == CONFLICT_MESSAGE
