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
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import MAX_CALLER_EVENT_LOOPS, PostgresBroker
from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerEventLoopLimit, BrokerOperationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.monitoring.errors import MonitoringQueryErrorCode
from tests.dualrun.reference.monitoring_queries import list_workflow_names
from tests.dualrun.reference.broker_monitoring import (
    ping_workers_async as reference_ping_workers,
)
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendEventLoopLimit
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow.handle_types import HandleErrorCode, HandleEventLoopLimit
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.result import Err, is_err, is_ok
from horsies.monitoring.task_actions import TaskActionEventLoopLimit
from horsies.core.workflows.lifecycle import (
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
        task_id=uuid.uuid4(),
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
def test_resume_returns_loop_limit() -> None:
    broker = MagicMock()
    loop_limit = attach_loop_limit(broker)

    result = asyncio.run(resume_workflow(broker, WORKFLOW_ID))

    assert is_err(result)
    assert result.err_value.code is HandleErrorCode.EVENT_LOOP_LIMIT
    assert result.err_value.retryable is False
    assert isinstance(result.err_value, HandleEventLoopLimit)
    assert result.err_value.loop_limit is loop_limit
    assert result.err_value.workflow_id == WORKFLOW_ID


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
        database_url=SecretStr('postgresql://u:p@localhost:5432/db'),
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
        # Python's worker ping before the monitoring switch: no broker method
        # selects a SQLAlchemy resource set any longer, and the reads and the
        # ping on the core have no loop limit.
        result = at_limit.run(
            reference_ping_workers(at_limit.broker, timeout_seconds=0.01),
        )

        assert is_err(result)
        assert result.err_value.code is BrokerErrorCode.EVENT_LOOP_LIMIT
        assert LIMIT_TEXT in result.err_value.message

    def test_monitoring_query(self, at_limit: _AtLimit) -> None:
        # Python's read before the monitoring switch: the reads on the core
        # select no resource set and have no loop limit.
        result = at_limit.run(list_workflow_names(at_limit.broker))

        assert is_err(result)
        assert result.err_value.code is MonitoringQueryErrorCode.EVENT_LOOP_LIMIT
