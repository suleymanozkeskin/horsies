"""EVENT_LOOP_LIMIT keeps its own code when it crosses a catalog boundary.

Each mapping must not fold the limit into a generic failure: the retry
condition differs (it clears when one of the event loops that hold a set
closes, not when the database recovers).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import MAX_CALLER_EVENT_LOOPS, PostgresBroker
from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerEventLoopLimit, BrokerOperationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.monitoring.errors import MonitoringQueryErrorCode
from horsies.monitoring.queries import list_workflow_names
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendEventLoopLimit
from horsies.core.models.tasks import RetrievalCode, TaskError, TaskResult
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow import NodeKey
from horsies.core.models.workflow.handle_types import HandleErrorCode
from horsies.core.task_decorator import TaskHandle, create_task_wrapper
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.monitoring.task_actions import (
    TaskActionErrorCode,
    TaskActionEventLoopLimit,
    cancel_task,
)
from horsies.core.workflows.lifecycle import (
    pause_workflow,
    resume_workflow,
    start_workflow_async,
)
from horsies.core.workflows.start_types import (
    WorkflowStartErrorCode,
    WorkflowStartEventLoopLimit,
)
from horsies.web.routes.actions import _task_action_failed
from tests.broker_resources_support import attach_loop_limit

LIMIT_MESSAGE = 'this broker already holds async resources for 5 open event loops'


def _loop_limit_broker_error() -> BrokerOperationError:
    return BrokerEventLoopLimit(
        message=LIMIT_MESSAGE,
        loop_limit=EventLoopLimit(
            limit=MAX_CALLER_EVENT_LOOPS,
            held_thread_names=('MainThread',),
            current_thread_name='worker-1',
        ),
    )


@pytest.mark.unit
def test_send_maps_broker_loop_limit_to_send_loop_limit() -> None:
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
    broker.enqueue.return_value = Err(_loop_limit_broker_error())
    app.get_broker.return_value = broker
    wrapper = create_task_wrapper(echo, app, 'test.echo')

    result = wrapper.send(x=1)

    assert is_err(result)
    assert isinstance(result.err_value, TaskSendEventLoopLimit)
    assert result.err_value.message == LIMIT_MESSAGE


@pytest.mark.unit
def test_web_task_action_maps_loop_limit_to_503() -> None:
    error = TaskActionEventLoopLimit(
        message=LIMIT_MESSAGE,
        task_id='t-1',
        loop_limit=EventLoopLimit(
            limit=MAX_CALLER_EVENT_LOOPS,
            held_thread_names=('MainThread',),
            current_thread_name='worker-1',
        ),
    )

    outcome = _task_action_failed(error)

    assert outcome.status_code == 503
    assert outcome.content == {'detail': LIMIT_MESSAGE}


WORKFLOW_ID = '9f1b7a2c-0d4e-4f61-8a23-5c6d7e8f9a0b'


def _limited_handle() -> tuple[WorkflowHandle[Any], MagicMock]:
    broker = MagicMock()
    attach_loop_limit(broker)
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker), broker


@pytest.mark.unit
@pytest.mark.parametrize('method', ['status_async', 'results_async', 'tasks_async', 'cancel_async'])
def test_handle_wrap_methods_return_loop_limit(method: str) -> None:
    handle, broker = _limited_handle()

    result = asyncio.run(getattr(handle, method)())

    assert is_err(result)
    assert result.err_value.code is HandleErrorCode.EVENT_LOOP_LIMIT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_id == WORKFLOW_ID


@pytest.mark.unit
def test_handle_get_folds_loop_limit_into_retrieval_code() -> None:
    handle, broker = _limited_handle()

    result = asyncio.run(handle.get_async(timeout_ms=100))

    assert result.is_err()
    assert result.unwrap_err().error_code is RetrievalCode.EVENT_LOOP_LIMIT


@pytest.mark.unit
def test_handle_result_for_folds_loop_limit_into_retrieval_code() -> None:
    handle, _ = _limited_handle()

    result = asyncio.run(handle.result_for_async(NodeKey('node-1')))

    assert result.is_err()
    assert result.unwrap_err().error_code is RetrievalCode.EVENT_LOOP_LIMIT


@pytest.mark.unit
def test_start_workflow_returns_loop_limit_before_any_attempt() -> None:
    spec = MagicMock()
    spec.name = 'loop-limit-wf'
    spec.tasks = []
    spec.workflow_def_cls = None
    broker = MagicMock()
    broker.ensure_schema_initialized = AsyncMock()
    loop_limit = attach_loop_limit(broker)

    result = asyncio.run(start_workflow_async(spec, broker, WORKFLOW_ID))

    assert is_err(result)
    assert result.err_value.code is WorkflowStartErrorCode.EVENT_LOOP_LIMIT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_id == WORKFLOW_ID
    assert isinstance(result.err_value, WorkflowStartEventLoopLimit)
    assert result.err_value.loop_limit is loop_limit
    broker.ensure_schema_initialized.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.parametrize('operation', [pause_workflow, resume_workflow])
def test_pause_and_resume_return_loop_limit(operation: Any) -> None:
    broker = MagicMock()
    attach_loop_limit(broker)

    result = asyncio.run(operation(broker, WORKFLOW_ID))

    assert is_err(result)
    assert result.err_value.code is HandleErrorCode.EVENT_LOOP_LIMIT
    assert result.err_value.retryable is False


@dataclass(frozen=True)
class _AtLimit:
    """A real broker whose caller loops are all taken, and one more loop."""

    app: Horsies
    broker: PostgresBroker
    extra_loop: asyncio.AbstractEventLoop

    def run(self, coroutine: Any) -> Any:
        return self.extra_loop.run_until_complete(coroutine)


@pytest.fixture
def at_limit() -> Generator[_AtLimit, None, None]:
    """No database: building a set does no I/O, and the limit stops each call first."""
    app = Horsies(AppConfig(broker=PostgresConfig(
        database_url=SecretStr('postgresql+psycopg://u:p@localhost:5432/db'),
    )))
    broker = app.get_broker()
    loops = [asyncio.new_event_loop() for _ in range(MAX_CALLER_EVENT_LOOPS)]
    for loop in loops:
        assert is_ok(loop.run_until_complete(broker.select_resources()))
    extra_loop = asyncio.new_event_loop()
    yield _AtLimit(app=app, broker=broker, extra_loop=extra_loop)
    for loop in (*loops, extra_loop):
        loop.close()


LIMIT_TEXT = f'already holds async resources for {MAX_CALLER_EVENT_LOOPS} open event loops'


@pytest.mark.unit
class TestRealBrokerAtTheLimit:
    """Each entry point returns its own catalog's EVENT_LOOP_LIMIT."""

    def test_broker_call(self, at_limit: _AtLimit) -> None:
        result = at_limit.run(at_limit.broker.ping_database_async())

        assert is_err(result)
        assert result.err_value.code is BrokerErrorCode.EVENT_LOOP_LIMIT
        assert LIMIT_TEXT in result.err_value.message

    def test_task_send(self, at_limit: _AtLimit) -> None:
        @at_limit.app.task('loop_limit.echo')
        def echo(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        result = at_limit.run(echo.send_async(x=1))

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendEventLoopLimit)

    def test_workflow_handle_wrap_and_fold(self, at_limit: _AtLimit) -> None:
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id='wf-at-limit', broker=at_limit.broker,
        )

        status = at_limit.run(handle.status_async())
        result = at_limit.run(handle.get_async(timeout_ms=0))

        assert is_err(status)
        assert status.err_value.code is HandleErrorCode.EVENT_LOOP_LIMIT
        assert result.is_err()
        assert result.unwrap_err().error_code is RetrievalCode.EVENT_LOOP_LIMIT

    def test_monitoring_query(self, at_limit: _AtLimit) -> None:
        result = at_limit.run(list_workflow_names(at_limit.broker))

        assert is_err(result)
        assert result.err_value.code is MonitoringQueryErrorCode.EVENT_LOOP_LIMIT

    def test_task_action(self, at_limit: _AtLimit) -> None:
        result = at_limit.run(cancel_task(at_limit.broker, str(uuid.uuid4())))

        assert is_err(result)
        assert result.err_value.code is TaskActionErrorCode.EVENT_LOOP_LIMIT

    def test_task_handle_get_async(self, at_limit: _AtLimit) -> None:
        """TaskHandle folds the limit into RetrievalCode, as WorkflowHandle does."""
        handle: TaskHandle[int] = TaskHandle(
            str(uuid.uuid4()), app=at_limit.app, broker_mode=True, ok_type=int,
        )

        at_limit_result = at_limit.run(handle.get_async(timeout_ms=0))

        assert at_limit_result.is_err()
        error = at_limit_result.unwrap_err()
        assert error.error_code is RetrievalCode.EVENT_LOOP_LIMIT
        assert LIMIT_TEXT in (error.message or '')
        # Not cached: the next get_async() reads the broker again.
        with patch.object(
            at_limit.broker,
            'get_raw_result_record_async',
            AsyncMock(return_value=Ok(None)),
        ) as read:
            later = at_limit.run(handle.get_async(timeout_ms=0))
        read.assert_awaited_once()
        assert later.unwrap_err().error_code is RetrievalCode.TASK_NOT_FOUND
