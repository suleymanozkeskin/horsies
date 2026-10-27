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
from horsies.core.models.workflow import NodeKey
from horsies.core.models.workflow.handle_types import HandleErrorCode
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.result import Err, is_err
from horsies.monitoring.task_actions import TaskActionError, TaskActionErrorCode
from horsies.core.workflows.lifecycle import (
    pause_workflow,
    resume_workflow,
    start_workflow_async,
)
from horsies.core.workflows.start_types import WorkflowStartErrorCode
from horsies.web.routes.actions import _task_action_failed
from tests.broker_resources_support import attach_conflict

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


WORKFLOW_ID = '9f1b7a2c-0d4e-4f61-8a23-5c6d7e8f9a0b'


def _conflicted_handle() -> tuple[WorkflowHandle[Any], MagicMock]:
    broker = MagicMock()
    attach_conflict(broker)
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker), broker


@pytest.mark.unit
@pytest.mark.parametrize('method', ['status_async', 'results_async', 'tasks_async', 'cancel_async'])
def test_handle_wrap_methods_return_conflict(method: str) -> None:
    handle, broker = _conflicted_handle()

    result = asyncio.run(getattr(handle, method)())

    assert is_err(result)
    assert result.err_value.code is HandleErrorCode.EVENT_LOOP_CONFLICT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_id == WORKFLOW_ID


@pytest.mark.unit
def test_handle_get_folds_conflict_into_retrieval_code() -> None:
    handle, broker = _conflicted_handle()

    result = asyncio.run(handle.get_async(timeout_ms=100))

    assert result.is_err()
    assert result.unwrap_err().error_code is RetrievalCode.EVENT_LOOP_CONFLICT


@pytest.mark.unit
def test_handle_result_for_folds_conflict_into_retrieval_code() -> None:
    handle, _ = _conflicted_handle()

    result = asyncio.run(handle.result_for_async(NodeKey('node-1')))

    assert result.is_err()
    assert result.unwrap_err().error_code is RetrievalCode.EVENT_LOOP_CONFLICT


@pytest.mark.unit
def test_start_workflow_returns_conflict_before_any_attempt() -> None:
    spec = MagicMock()
    spec.name = 'conflict-wf'
    spec.tasks = []
    spec.workflow_def_cls = None
    broker = MagicMock()
    broker.ensure_schema_initialized = AsyncMock()
    attach_conflict(broker)

    result = asyncio.run(start_workflow_async(spec, broker, WORKFLOW_ID))

    assert is_err(result)
    assert result.err_value.code is WorkflowStartErrorCode.EVENT_LOOP_CONFLICT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_id == WORKFLOW_ID
    broker.ensure_schema_initialized.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.parametrize('operation', [pause_workflow, resume_workflow])
def test_pause_and_resume_return_conflict(operation: Any) -> None:
    broker = MagicMock()
    attach_conflict(broker)

    result = asyncio.run(operation(broker, WORKFLOW_ID))

    assert is_err(result)
    assert result.err_value.code is HandleErrorCode.EVENT_LOOP_CONFLICT
    assert result.err_value.retryable is False
