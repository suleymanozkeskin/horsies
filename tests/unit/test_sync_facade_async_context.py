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
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerAsyncContext, BrokerErrorCode, BrokerLoopRunnerFailed
from horsies.core.errors import ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendAsyncContext
from horsies.core.models.tasks import (
    OperationalErrorCode,
    RetrievalCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import NodeKey
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    HandleAsyncContext,
    HandleErrorCode,
    HandleLoopRunnerFailed,
)
from horsies.core.task_decorator import TaskHandle, create_task_wrapper
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.utils.loop_runner import (
    LoopRunner,
    LoopRunnerError,
    in_running_event_loop,
)
from horsies.core.workflows.lifecycle import (
    pause_workflow_sync,
    resume_workflow_sync,
    start_workflow,
)
from horsies.core.workflows.start_types import (
    WorkflowStartAsyncContext,
    WorkflowStartEnqueueFailed,
    WorkflowStartErrorCode,
    WorkflowStartLoopRunnerFailed,
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
    assert 'await handle.info_async(' in result.err_value.message
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
    assert 'await handle.get_async(' in (inside.unwrap_err().message or '')
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
    broker.enqueue.return_value = Err(BrokerAsyncContext(
        message='refused',
    ))
    wrapper = create_task_wrapper(echo, _send_app(broker), 'test.echo')

    result = wrapper.send(x=1)

    assert is_err(result)
    assert isinstance(result.err_value, TaskSendAsyncContext)
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


def test_web_handle_failure_mapping_asserts_on_async_context() -> None:
    error = HandleAsyncContext(message='refused', workflow_id=WORKFLOW_ID)

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
    assert 'await spec.start_async(' in result.err_value.message
    _assert_nothing_ran(app.get_broker())


def test_spec_retry_start_returns_async_context(app: Horsies) -> None:
    spec = _spec(app)

    result = _inside_loop(lambda: spec.retry_start(_failed_start()))

    assert is_err(result)
    assert result.err_value.code is WorkflowStartErrorCode.ASYNC_CONTEXT
    assert result.err_value.workflow_id == WORKFLOW_ID
    _assert_nothing_ran(app.get_broker())


def test_spec_retry_start_accepts_an_async_context_error(app: Horsies) -> None:
    """Outside a loop, retry_start of an ASYNC_CONTEXT error starts with its id."""
    spec = _spec(app)
    refused = WorkflowStartAsyncContext(
        message='refused',
        workflow_name='async_context_wf',
        workflow_id=WORKFLOW_ID,
    )
    started = MagicMock()

    # The spec is frozen; patch the method on its class.
    with patch.object(type(spec), 'start', return_value=Ok(started)) as start:
        result = spec.retry_start(refused)

    assert is_ok(result)
    assert result.ok_value is started
    start.assert_called_once_with(workflow_id=WORKFLOW_ID)


def test_start_workflow_returns_async_context(app: Horsies) -> None:
    """The standalone sync start names its own async form."""
    spec = _spec(app)

    result = _inside_loop(lambda: start_workflow(spec, app.get_broker(), WORKFLOW_ID))

    assert is_err(result)
    assert result.err_value.code is WorkflowStartErrorCode.ASYNC_CONTEXT
    assert 'await start_workflow_async(' in result.err_value.message
    _assert_nothing_ran(app.get_broker())


def test_task_handle_result_mapping_asserts_on_async_context(app: Horsies) -> None:
    """get() refuses the loop before the broker call, so the mapping never sees it."""
    handle: TaskHandle[int] = TaskHandle(
        TASK_ID, app=app, broker_mode=True, ok_type=int,
    )
    broker_result = Err(BrokerAsyncContext(
        message='refused',
    ))

    with pytest.raises(AssertionError, match='TaskHandle result read'):
        handle._record_to_task_result(broker_result, None)


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


# =============================================================================
# Every run_sync call site: a LoopRunnerError is the site's catalog error;
# every other exception propagates unchanged
# =============================================================================


def _workflow_handle(broker: PostgresBroker) -> WorkflowHandle[Any]:
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)


RUN_SYNC_SITES: dict[str, Callable[[Horsies, PostgresBroker], Any]] = {
    'broker.get_raw_result_record': lambda app, broker: broker.get_raw_result_record(TASK_ID, 0),
    'broker.enqueue': lambda app, broker: broker.enqueue(
        'echo', task_id=TASK_ID, enqueue_sha='sha',
    ),
    'broker.get_task_info': lambda app, broker: broker.get_task_info(TASK_ID),
    'broker.close': lambda app, broker: broker.close(),
    'app.get_result': lambda app, broker: app.get_result(TASK_ID, 0),
    'app.get_task_info': lambda app, broker: app.get_task_info(TASK_ID),
    'app.ping_database': lambda app, broker: app.ping_database(),
    'app.ping_workers': lambda app, broker: app.ping_workers(),
    'app.list_worker_states': lambda app, broker: app.list_worker_states(),
    'app.get_worker_state': lambda app, broker: app.get_worker_state('worker-1'),
    'app.get_worker_state_history': lambda app, broker: app.get_worker_state_history('worker-1'),
    'WorkflowHandle.status': lambda app, broker: _workflow_handle(broker).status(),
    'WorkflowHandle.get': lambda app, broker: _workflow_handle(broker).get(timeout_ms=0),
    'start_workflow': lambda app, broker: _spec(app).start(WORKFLOW_ID),
    'pause_workflow_sync': lambda app, broker: pause_workflow_sync(broker, WORKFLOW_ID),
    'resume_workflow_sync': lambda app, broker: resume_workflow_sync(broker, WORKFLOW_ID),
}


@pytest.mark.parametrize('site', list(RUN_SYNC_SITES))
def test_run_sync_call_site_reraises_an_assertion(app: Horsies, site: str) -> None:
    """A defect in the bridge propagates; it is not folded into an Err."""
    broker = app.get_broker()

    with (
        patch.object(
            broker, 'run_sync', side_effect=AssertionError('stub defect'),
        ) as run_sync,
        pytest.raises(AssertionError, match='stub defect'),
    ):
        RUN_SYNC_SITES[site](app, broker)

    run_sync.assert_called_once()


RUNNER_FAILURE_CODES: dict[str, object] = {
    **{
        site: BrokerErrorCode.LOOP_RUNNER_FAILED
        for site in RUN_SYNC_SITES
        if site.startswith(('broker.', 'app.'))
    },
    'WorkflowHandle.status': HandleErrorCode.LOOP_RUNNER_FAILED,
    'WorkflowHandle.get': OperationalErrorCode.BROKER_ERROR,
    'start_workflow': WorkflowStartErrorCode.LOOP_RUNNER_FAILED,
    'pause_workflow_sync': HandleErrorCode.LOOP_RUNNER_FAILED,
    'resume_workflow_sync': HandleErrorCode.LOOP_RUNNER_FAILED,
}


def _failure_code(result: object) -> object:
    """The code of a failed facade result: an ``Err`` or a failed ``TaskResult``."""
    match result:
        case TaskResult(err=TaskError(error_code=code)):
            return code
        case Err(err_value=(
            BrokerLoopRunnerFailed(code=code)
            | HandleLoopRunnerFailed(code=code)
            | WorkflowStartLoopRunnerFailed(code=code)
        )):
            return code
        case _:
            raise AssertionError(f'expected a failure, got {result!r}')


def _runner_error_fields(result: object) -> tuple[bool, BaseException | None] | None:
    """``(retryable, exception)`` of an ``Err`` result; ``None`` for a ``TaskResult``."""
    match result:
        case Err(err_value=(
            BrokerLoopRunnerFailed(retryable=retryable, exception=exception)
            | HandleLoopRunnerFailed(retryable=retryable, exception=exception)
            | WorkflowStartLoopRunnerFailed(retryable=retryable, exception=exception)
        )):
            return (retryable, exception)
        case _:
            return None


def test_runner_failure_codes_cover_every_site() -> None:
    assert set(RUNNER_FAILURE_CODES) == set(RUN_SYNC_SITES)


@pytest.mark.parametrize('site', list(RUN_SYNC_SITES))
def test_run_sync_call_site_propagates_a_defect(app: Horsies, site: str) -> None:
    """An exception that is not a LoopRunnerError propagates unchanged."""
    broker = app.get_broker()
    defect = TypeError('stub defect')

    with (
        patch.object(broker, 'run_sync', side_effect=defect) as run_sync,
        pytest.raises(TypeError) as exc_info,
    ):
        RUN_SYNC_SITES[site](app, broker)

    assert exc_info.value is defect
    run_sync.assert_called_once()


@pytest.mark.parametrize('site', list(RUN_SYNC_SITES))
def test_run_sync_call_site_returns_the_loop_runner_code(
    app: Horsies, site: str,
) -> None:
    """A LoopRunnerError is the site's loop-runner error; nothing else is raised."""
    broker = app.get_broker()
    runner_error = LoopRunnerError('runner stopped')

    with patch.object(broker, 'run_sync', side_effect=runner_error) as run_sync:
        result = RUN_SYNC_SITES[site](app, broker)

    assert _failure_code(result) is RUNNER_FAILURE_CODES[site]
    match _runner_error_fields(result):
        case None:
            assert site == 'WorkflowHandle.get'
        case (retryable, exception):
            assert retryable is False
            assert exception is runner_error
    run_sync.assert_called_once()


# =============================================================================
# The ASYNC_CONTEXT message names the async form of the called function
# =============================================================================


def _failed_start() -> WorkflowStartEnqueueFailed:
    return WorkflowStartEnqueueFailed(
        message='connection lost',
        retryable=True,
        workflow_name='async_context_wf',
        workflow_id=WORKFLOW_ID,
        exception=ConnectionError('connection lost'),
    )


def _task_handle(app: Horsies) -> TaskHandle[int]:
    return TaskHandle(TASK_ID, app=app, broker_mode=True, ok_type=int)


def _workflow_result_for(app: Horsies) -> TaskResult[Any, TaskError]:
    node: NodeKey[Any] = NodeKey('node-1')
    return _workflow_handle(app.get_broker()).result_for(node)


ASYNC_TWINS: dict[str, tuple[Callable[[Horsies], Any], str]] = {
    'PostgresBroker.get_raw_result_record': (
        lambda app: app.get_broker().get_raw_result_record(TASK_ID, 0),
        'broker.get_raw_result_record_async(',
    ),
    'PostgresBroker.enqueue': (
        lambda app: app.get_broker().enqueue('echo', task_id=TASK_ID, enqueue_sha='sha'),
        'broker.enqueue_async(',
    ),
    'PostgresBroker.get_task_info': (
        lambda app: app.get_broker().get_task_info(TASK_ID),
        'broker.get_task_info_async(',
    ),
    'PostgresBroker.close': (lambda app: app.get_broker().close(), 'broker.close_async('),
    **{
        f'Horsies.{name}': (call, f'app.{name}_async(')
        for name, call in APP_CALLS.items()
    },
    'TaskHandle.get': (lambda app: _task_handle(app).get(), 'handle.get_async('),
    'TaskHandle.info': (lambda app: _task_handle(app).info(), 'handle.info_async('),
    **{
        f'WorkflowHandle.{name}': (
            lambda app, name=name: getattr(_workflow_handle(app.get_broker()), name)(),
            f'handle.{name}_async(',
        )
        for name in ('status', 'results', 'tasks', 'cancel', 'pause', 'resume')
    },
    'WorkflowHandle.get': (
        lambda app: _workflow_handle(app.get_broker()).get(timeout_ms=0),
        'handle.get_async(',
    ),
    'WorkflowHandle.result_for': (_workflow_result_for, 'handle.result_for_async('),
    'pause_workflow_sync': (
        lambda app: pause_workflow_sync(app.get_broker(), WORKFLOW_ID),
        'pause_workflow(broker, workflow_id)',
    ),
    'resume_workflow_sync': (
        lambda app: resume_workflow_sync(app.get_broker(), WORKFLOW_ID),
        'resume_workflow(broker, workflow_id)',
    ),
    'WorkflowSpec.start': (lambda app: _spec(app).start(WORKFLOW_ID), 'spec.start_async('),
    'WorkflowSpec.retry_start': (
        lambda app: _spec(app).retry_start(_failed_start()),
        'spec.retry_start_async(',
    ),
    'start_workflow': (
        lambda app: start_workflow(_spec(app), app.get_broker(), WORKFLOW_ID),
        'start_workflow_async(',
    ),
}


def _refusal_message(result: object) -> str:
    """The ASYNC_CONTEXT message of an Err or of a TaskResult error."""
    match result:
        case TaskResult():
            task_error = cast('TaskResult[Any, TaskError]', result).unwrap_err()
            assert task_error.error_code is RetrievalCode.ASYNC_CONTEXT
            return task_error.message or ''
        case Err(
            BrokerAsyncContext() | HandleAsyncContext() | WorkflowStartAsyncContext() as error
        ):
            assert error.code.value == 'ASYNC_CONTEXT'
            return error.message
        case _:
            raise AssertionError(f'expected an ASYNC_CONTEXT result, got {result!r}')


@pytest.mark.parametrize('entry_point', list(ASYNC_TWINS))
def test_async_context_message_names_the_async_form_of_the_call(
    app: Horsies, entry_point: str,
) -> None:
    call, twin = ASYNC_TWINS[entry_point]

    message = _refusal_message(_inside_loop(lambda: call(app)))

    assert f'Use `await {twin}' in message
