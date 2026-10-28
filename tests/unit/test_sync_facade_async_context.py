# pyright: reportPrivateUsage=false
"""Sync facades return ASYNC_CONTEXT on a thread that runs an event loop.

A sync facade blocks its calling thread until the broker's sync runner
completes the call. On a thread that runs an event loop, that wait stalls
the loop, so each facade returns the ASYNC_CONTEXT member of its own catalog
first: the runner thread does not start and no resource set is built.

The broker is real and its URL is unreachable. The guard returns before any
I/O, so no database is needed. With a guard removed, the call goes to the
runner and fails on the connection, so the code and runner-state assertions
fail.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import ListenerDisabled
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.errors import ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendErrorCode
from horsies.core.models.tasks import RetrievalCode, TaskError, TaskResult
from horsies.core.models.workflow import NodeKey
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    HandleErrorCode,
    HandleOperationError,
)
from horsies.core.task_decorator import TaskHandle, create_task_wrapper
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.utils.loop_runner import LoopRunner, in_running_event_loop
from horsies.core.workflows.lifecycle import (
    pause_workflow_sync,
    resume_workflow_sync,
)
from horsies.core.workflows.start_types import (
    WorkflowStartError,
    WorkflowStartErrorCode,
)
from horsies.web.routes.actions import _handle_failed

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL = 'postgresql+psycopg://u:p@127.0.0.1:1/db'
WORKFLOW_ID = '0b6f2f0e-6c1d-4f7a-9a52-3d2b1c0e9f81'
TASK_ID = '0b6f2f0e-6c1d-7f7a-9a52-3d2b1c0e9f82'


@pytest.fixture
def app() -> Generator[Horsies, None, None]:
    """An app whose broker cannot connect. Stops the runner if a call started it."""
    app = Horsies(AppConfig(broker=PostgresConfig(
        database_url=SecretStr(UNREACHABLE_URL),
    )))
    yield app
    app.get_broker()._loop_runner.stop()


def _inside_loop[T](call: Callable[[], T]) -> T:
    """Run a sync call on a thread whose event loop is running."""

    async def inside() -> T:
        return call()

    return asyncio.run(inside())


def _assert_nothing_ran(broker: PostgresBroker) -> None:
    """The sync runner never started and no resource set was built."""
    assert broker._loop_runner._started is False
    assert broker._loop_runner._thread is None
    assert broker._loop_sets == ()


# =============================================================================
# BrokerErrorCode.ASYNC_CONTEXT
# =============================================================================


BROKER_CALLS: dict[str, tuple[Callable[[PostgresBroker], Any], str]] = {
    'get_raw_result_record': (
        lambda broker: broker.get_raw_result_record(TASK_ID, 0),
        'get_raw_result_record_async',
    ),
    'enqueue': (
        lambda broker: broker.enqueue(
            'echo', task_id=TASK_ID, enqueue_sha='sha',
        ),
        'enqueue_async',
    ),
    'get_task_info': (
        lambda broker: broker.get_task_info(TASK_ID),
        'get_task_info_async',
    ),
}


@pytest.mark.parametrize('name', sorted(BROKER_CALLS))
def test_broker_facade_returns_async_context(app: Horsies, name: str) -> None:
    broker = app.get_broker()
    call, async_form = BROKER_CALLS[name]

    result = _inside_loop(lambda: call(broker))

    assert is_err(result)
    assert result.err_value.code is BrokerErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert async_form in result.err_value.message
    _assert_nothing_ran(broker)


def test_broker_close_returns_async_context_and_keeps_the_runner(
    app: Horsies,
) -> None:
    """The guard runs before the try/finally, so the runner is not stopped."""
    broker = app.get_broker()
    broker._loop_runner.start()

    result = _inside_loop(broker.close)

    assert is_err(result)
    assert result.err_value.code is BrokerErrorCode.ASYNC_CONTEXT
    assert 'close_async' in result.err_value.message
    assert broker._loop_runner._started is True
    assert broker._loop_runner._closed is False
    assert broker._loop_sets == ()


APP_CALLS: dict[str, Callable[[Horsies], Any]] = {
    'get_result': lambda app: app.get_result(TASK_ID, 0),
    'get_task_info': lambda app: app.get_task_info(TASK_ID),
    'ping_database': lambda app: app.ping_database(),
    'ping_workers': lambda app: app.ping_workers(),
    'list_worker_states': lambda app: app.list_worker_states(),
    'get_worker_state': lambda app: app.get_worker_state('worker-1'),
    'get_worker_state_history': lambda app: app.get_worker_state_history('worker-1'),
    'get_raw_result': lambda app: app.get_raw_result(TASK_ID, 0),
}


@pytest.mark.parametrize('name', sorted(APP_CALLS))
def test_app_facade_returns_async_context(app: Horsies, name: str) -> None:
    result = _inside_loop(lambda: APP_CALLS[name](app))

    assert is_err(result)
    assert result.err_value.code is BrokerErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert '_async' in result.err_value.message
    _assert_nothing_ran(app.get_broker())


# =============================================================================
# TaskHandle
# =============================================================================


def test_task_handle_info_returns_async_context(app: Horsies) -> None:
    handle: TaskHandle[int] = TaskHandle(
        TASK_ID, app=app, broker_mode=True, ok_type=int,
    )

    result = _inside_loop(handle.info)

    assert is_err(result)
    assert result.err_value.code is BrokerErrorCode.ASYNC_CONTEXT
    assert 'get_task_info_async' in result.err_value.message
    _assert_nothing_ran(app.get_broker())


def test_task_handle_get_returns_async_context_and_does_not_cache_it(
    app: Horsies,
) -> None:
    handle: TaskHandle[int] = TaskHandle(
        TASK_ID, app=app, broker_mode=True, ok_type=int,
    )
    broker = app.get_broker()

    inside = _inside_loop(handle.get)

    assert inside.is_err()
    assert inside.unwrap_err().error_code is RetrievalCode.ASYNC_CONTEXT
    assert 'get_raw_result_record_async' in (inside.unwrap_err().message or '')
    _assert_nothing_ran(broker)
    # A later sync get() outside the loop reads the broker again.
    with patch.object(
        broker, 'get_raw_result_record', return_value=Ok(None),
    ) as read:
        outside = handle.get()
    read.assert_called_once()
    assert outside.unwrap_err().error_code is RetrievalCode.TASK_NOT_FOUND


def test_task_handle_get_returns_cached_result_inside_loop(app: Horsies) -> None:
    """The cached path does not block, so it does not refuse."""
    handle: TaskHandle[int] = TaskHandle(
        TASK_ID, app=app, broker_mode=True, ok_type=int,
    )
    handle.set_immediate_result(TaskResult(ok=7))

    result = _inside_loop(handle.get)

    assert result.is_ok()
    assert result.unwrap() == 7
    _assert_nothing_ran(app.get_broker())


def _send_app(broker: MagicMock) -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.custom_queues = None
    app.config.exception_mapper = {}
    app.config.default_unhandled_error_code = 'UNHANDLED_EXCEPTION'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = 'default'
    app.get_broker.return_value = broker
    return app


def test_send_maps_broker_async_context_to_send_async_context() -> None:
    """A broker ASYNC_CONTEXT keeps its code across the send mapping."""

    def echo(*, x: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=x)

    broker = MagicMock()
    broker.enqueue.return_value = Err(BrokerOperationError(
        code=BrokerErrorCode.ASYNC_CONTEXT,
        message='refused',
        retryable=False,
    ))
    wrapper = create_task_wrapper(echo, _send_app(broker), 'test.echo')

    result = wrapper.send(x=1)

    assert is_err(result)
    assert result.err_value.code is TaskSendErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert result.err_value.payload is not None


# =============================================================================
# HandleErrorCode.ASYNC_CONTEXT and RetrievalCode.ASYNC_CONTEXT
# =============================================================================


@pytest.mark.parametrize(
    'name', ['status', 'results', 'tasks', 'cancel', 'pause', 'resume'],
)
def test_workflow_handle_wrap_method_returns_async_context(
    app: Horsies, name: str,
) -> None:
    broker = app.get_broker()
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=WORKFLOW_ID, broker=broker,
    )

    result = _inside_loop(getattr(handle, name))

    assert is_err(result)
    assert result.err_value.code is HandleErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_id == WORKFLOW_ID
    assert f'{name}_async' in result.err_value.message
    _assert_nothing_ran(broker)


FOLD_CALLS: dict[str, Callable[[WorkflowHandle[Any]], TaskResult[Any, TaskError]]] = {
    'get': lambda handle: handle.get(timeout_ms=0),
    'result_for': lambda handle: handle.result_for(NodeKey('node-1')),
}


@pytest.mark.parametrize('name', sorted(FOLD_CALLS))
def test_workflow_handle_fold_method_returns_async_context(
    app: Horsies, name: str,
) -> None:
    broker = app.get_broker()
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=WORKFLOW_ID, broker=broker,
    )

    result = _inside_loop(lambda: FOLD_CALLS[name](handle))

    assert result.is_err()
    assert result.unwrap_err().error_code is RetrievalCode.ASYNC_CONTEXT
    assert f'{name}_async' in (result.unwrap_err().message or '')
    _assert_nothing_ran(broker)


@pytest.mark.parametrize(
    'operation', [pause_workflow_sync, resume_workflow_sync],
)
def test_lifecycle_sync_wrapper_returns_async_context(
    app: Horsies, operation: Callable[[PostgresBroker, str], Any],
) -> None:
    broker = app.get_broker()

    result = _inside_loop(lambda: operation(broker, WORKFLOW_ID))

    assert is_err(result)
    assert result.err_value.code is HandleErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_id == WORKFLOW_ID
    _assert_nothing_ran(broker)


def test_workflow_get_async_asserts_on_async_context_status() -> None:
    """The async wait reads status through an async call only."""
    broker = MagicMock()
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=WORKFLOW_ID, broker=broker,
    )

    async def resources() -> Any:
        return Ok(MagicMock(listener=ListenerDisabled()))

    async def status(_resources: Any) -> Any:
        return Err(HandleOperationError(
            code=HandleErrorCode.ASYNC_CONTEXT,
            message='refused',
            retryable=False,
            workflow_id=WORKFLOW_ID,
        ))

    broker.select_resources = resources
    with (
        patch.object(handle, '_status', status),
        pytest.raises(AssertionError, match='ASYNC_CONTEXT'),
    ):
        asyncio.run(handle.get_async(timeout_ms=0))


def test_web_handle_failure_mapping_asserts_on_async_context() -> None:
    error = HandleOperationError(
        code=HandleErrorCode.ASYNC_CONTEXT,
        message='refused',
        retryable=False,
        workflow_id=WORKFLOW_ID,
    )

    with pytest.raises(AssertionError, match=WORKFLOW_ID):
        _handle_failed(error)


# =============================================================================
# WorkflowStartErrorCode.ASYNC_CONTEXT
# =============================================================================


def _spec(app: Horsies) -> Any:
    @app.task('async_context.echo')
    def echo(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return app.workflow(
        'async_context_wf',
        [echo.node()(value=1)],
        definition_key='tests.async_context_wf.v1',
    )


def test_spec_start_returns_async_context(app: Horsies) -> None:
    spec = _spec(app)

    result = _inside_loop(lambda: spec.start(WORKFLOW_ID))

    assert is_err(result)
    assert result.err_value.code is WorkflowStartErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_name == 'async_context_wf'
    assert result.err_value.workflow_id == WORKFLOW_ID
    assert 'start_async' in result.err_value.message
    _assert_nothing_ran(app.get_broker())


def test_spec_retry_start_returns_async_context(app: Horsies) -> None:
    spec = _spec(app)
    failed = WorkflowStartError(
        code=WorkflowStartErrorCode.ENQUEUE_FAILED,
        message='connection lost',
        retryable=True,
        workflow_name='async_context_wf',
        workflow_id=WORKFLOW_ID,
    )

    result = _inside_loop(lambda: spec.retry_start(failed))

    assert is_err(result)
    assert result.err_value.code is WorkflowStartErrorCode.ASYNC_CONTEXT
    assert result.err_value.workflow_id == WORKFLOW_ID
    _assert_nothing_ran(app.get_broker())


def test_spec_retry_start_refuses_an_async_context_error(app: Horsies) -> None:
    """ASYNC_CONTEXT is not a retry input; the async start is the remedy."""
    spec = _spec(app)
    refused = WorkflowStartError(
        code=WorkflowStartErrorCode.ASYNC_CONTEXT,
        message='refused',
        retryable=False,
        workflow_name='async_context_wf',
        workflow_id=WORKFLOW_ID,
    )

    result = spec.retry_start(refused)

    assert is_err(result)
    assert result.err_value.code is WorkflowStartErrorCode.VALIDATION_FAILED
    _assert_nothing_ran(app.get_broker())


# =============================================================================
# check(live=True)
# =============================================================================


def test_live_check_inside_loop_reports_its_own_code(app: Horsies) -> None:
    errors = _inside_loop(lambda: app.check(live=True))

    assert [error.code for error in errors] == [ErrorCode.CHECK_LIVE_ASYNC_CONTEXT]
    assert 'asyncio.to_thread' in (errors[0].help_text or '')


# =============================================================================
# close_async stops the sync runner off the caller's loop
# =============================================================================


def test_close_async_stops_the_runner_on_another_thread(app: Horsies) -> None:
    broker = app.get_broker()
    broker._loop_runner.start()
    stop_threads: list[tuple[str, bool]] = []
    original_stop = LoopRunner.stop

    def recording_stop(runner: LoopRunner) -> None:
        stop_threads.append(
            (threading.current_thread().name, in_running_event_loop()),
        )
        original_stop(runner)

    async def close_on_loop() -> tuple[Any, str]:
        result = await broker.close_async()
        return result, threading.current_thread().name

    with patch.object(LoopRunner, 'stop', recording_stop):
        result, loop_thread = asyncio.run(close_on_loop())

    assert is_ok(result)
    assert len(stop_threads) == 1
    stop_thread, stop_inside_loop = stop_threads[0]
    assert stop_thread != loop_thread
    assert stop_inside_loop is False
    assert broker._loop_runner._started is False
    assert broker._loop_runner._thread is None


# =============================================================================
# run_sync: a call from a thread that runs an event loop is a defect
# =============================================================================


async def _answer() -> int:
    return 42


def test_run_sync_inside_running_loop_fails_an_assertion(app: Horsies) -> None:
    broker = app.get_broker()

    with pytest.raises(AssertionError, match='run_sync\\(_answer\\)'):
        _inside_loop(lambda: broker.run_sync(_answer))

    _assert_nothing_ran(broker)


def test_run_sync_with_a_stopped_loop_on_the_thread_runs(app: Horsies) -> None:
    """A loop that is set on the thread but does not run does not trip it."""
    broker = app.get_broker()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(asyncio.sleep(0))

        assert broker.run_sync(_answer) == 42
    finally:
        asyncio.set_event_loop(None)
        loop.close()
