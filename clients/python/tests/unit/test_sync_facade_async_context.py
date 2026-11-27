# pyright: reportPrivateUsage=false
"""Sync facades return ASYNC_CONTEXT on a thread that runs an event loop.

A sync facade blocks its calling thread until the core's blocking form
ends. On a thread that runs an event loop, that wait stalls the loop, so
each facade returns the ASYNC_CONTEXT member of its own catalog first: no
core call runs.

The broker is real and its URL is unreachable; a mock that records the calls
wraps its core handle. The guard returns before any I/O, so no database is
needed. With a guard removed, the call goes to the core and fails on the
connection, so the code and nothing-ran assertions fail.
"""

from __future__ import annotations

import uuid

import asyncio
from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import MagicMock, patch
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import SecretStr

from horsies.core.telemetry.send_telemetry import NO_SEND_TELEMETRY
from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies._core import CoreCallTimedOut
from horsies.core.brokers.result_types import (
    DatabaseAsyncContext,
    DatabaseErrorCode,
    DatabaseOperationFailed,
)
from horsies.core.errors import HorsiesErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.outcome import (
    ReadAsyncContext,
    ReadDatabaseError,
    ReadResult,
    ReadTaskNotFound,
    Succeeded,
)
from horsies.core.models.task_send_types import TaskSendAsyncContext
from horsies.core.models.tasks import (
    OperationalCode,
    TaskError,
)
from horsies.core.models.workflow import NodeKey
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    WorkflowHandleAsyncContext,
    WorkflowHandleDatabaseOperationFailed,
    WorkflowHandleErrorCode,
)
from horsies.core.task_decorator import TaskHandle, create_task_wrapper
from horsies.core.models.tasks import TextCodes
from horsies.core.task_read import read_of_record
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, Result, is_err, is_ok
from horsies.core.workflows.lifecycle import (
    pause_workflow_sync,
    resume_workflow_sync,
    start_workflow,
)
from horsies.core.workflows.start_types import (
    WorkflowStartAsyncContext,
    WorkflowStartEnqueueFailed,
    WorkflowStartErrorCode,
)

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL = 'postgresql://u:p@127.0.0.1:1/db'
WORKFLOW_ID = '0b6f2f0e-6c1d-4f7a-9a52-3d2b1c0e9f81'
TASK_ID = '0b6f2f0e-6c1d-7f7a-9a52-3d2b1c0e9f82'


@pytest.fixture
def app() -> Generator[Horsies, None, None]:
    """An app whose broker cannot connect, and whose core handle records each
    call it forwards."""
    app = Horsies(AppConfig(database=PostgresConfig(
        database_url=SecretStr(UNREACHABLE_URL),
    )))
    broker = app.get_broker()
    broker._core_handle = MagicMock(wraps=broker._core_handle)
    yield app


def _inside_loop[T](call: Callable[[], T]) -> T:
    """Run a sync call on a thread whose event loop is running."""

    async def inside() -> T:
        return call()

    return asyncio.run(inside())


def _assert_nothing_ran(broker: PostgresBroker) -> None:
    """No core call ran."""
    core_handle = broker._core_handle
    assert isinstance(core_handle, MagicMock), 'the app fixture wraps the core handle'
    assert core_handle.mock_calls == []


# =============================================================================
# DatabaseErrorCode.ASYNC_CONTEXT
# =============================================================================


BROKER_CALLS: dict[str, tuple[Callable[[PostgresBroker], Any], str]] = {
    'get_raw_result_record': (
        lambda broker: broker.get_raw_result_record(TASK_ID, 0),
        'get_raw_result_record_async',
    ),
    'enqueue': (
        lambda broker: broker.enqueue(
            'echo', task_id=TASK_ID, send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
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
    assert result.err_value.code is DatabaseErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert async_form in result.err_value.message
    _assert_nothing_ran(broker)


def test_broker_close_returns_async_context(app: Horsies) -> None:
    """The guard runs before the core close, so nothing closes."""
    broker = app.get_broker()

    result = _inside_loop(broker.close)

    assert is_err(result)
    assert result.err_value.code is DatabaseErrorCode.ASYNC_CONTEXT
    assert 'close_async' in result.err_value.message
    _assert_nothing_ran(broker)


APP_CALLS: dict[str, Callable[[Horsies], Any]] = {
    'get_task_info': lambda app: app.get_task_info(TASK_ID),
    'ping_database': lambda app: app.ping_database(),
    'ping_workers': lambda app: app.ping_workers(),
    'list_worker_states': lambda app: app.list_worker_states(),
    'get_worker_state': lambda app: app.get_worker_state('worker-1'),
    'get_worker_state_history': lambda app: app.get_worker_state_history('worker-1'),
    'get_execution_class_coverage': lambda app: app.get_execution_class_coverage(),
    'get_execution_class_report': lambda app: app.get_execution_class_report(),
    'get_stale_tasks': lambda app: app.get_stale_tasks(2),
    'get_expired_tasks': lambda app: app.get_expired_tasks(),
    'get_raw_result': lambda app: app.get_raw_result(TASK_ID, 0),
}


@pytest.mark.parametrize('name', sorted(APP_CALLS))
def test_app_facade_returns_async_context(app: Horsies, name: str) -> None:
    result = _inside_loop(lambda: APP_CALLS[name](app))

    assert is_err(result)
    assert result.err_value.code is DatabaseErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert '_async' in result.err_value.message
    _assert_nothing_ran(app.get_broker())


def test_app_get_result_returns_async_context(app: Horsies) -> None:
    """The typed result read returns the read failure ``ReadAsyncContext``."""
    result = _inside_loop(lambda: app.get_result(UUID(TASK_ID), 0))

    match result:
        case Err(ReadAsyncContext() as read_error):
            assert 'app.get_result_async(' in read_error.message
        case _:
            pytest.fail(f'expected Err(ReadAsyncContext), got {result!r}')
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
    assert result.err_value.code is DatabaseErrorCode.ASYNC_CONTEXT
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

    match inside:
        case Err(ReadAsyncContext() as read_error):
            assert 'await handle.get_async(' in read_error.message
        case _:
            pytest.fail(f'expected Err(ReadAsyncContext), got {inside!r}')
    _assert_nothing_ran(broker)
    # A later sync get() outside the loop reads the broker again.
    with patch.object(
        broker, 'get_raw_result_record', return_value=Ok(None),
    ) as read:
        outside = handle.get()
    read.assert_called_once()
    assert isinstance(outside, Err)
    assert isinstance(outside.err_value, ReadTaskNotFound)


def test_task_handle_get_returns_cached_result_inside_loop(app: Horsies) -> None:
    """The cached path does not block, so it does not refuse."""
    handle: TaskHandle[int] = TaskHandle(
        TASK_ID, app=app, broker_mode=True, ok_type=int,
    )
    handle.set_immediate_result(Ok(7))

    result = _inside_loop(handle.get)

    assert result == Ok(Succeeded(7))
    _assert_nothing_ran(app.get_broker())


def _send_app(broker: MagicMock) -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.custom_queues = None
    app.config.exception_mapper = {}
    app.config.default_unhandled_error_code = 'UNHANDLED_EXCEPTION'
    app.config.resend_on_transient_error = False
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.send_telemetry.return_value = NO_SEND_TELEMETRY
    app.validate_queue.return_value = 'default'
    app.get_broker.return_value = broker
    return app


def test_send_maps_broker_async_context_to_send_async_context() -> None:
    """A broker ASYNC_CONTEXT keeps its code across the send mapping."""

    def echo(*, x: int) -> Result[int, TaskError]:
        return Ok(x)

    broker = MagicMock()
    broker.enqueue.return_value = Err(DatabaseAsyncContext(
        message='refused',
    ))
    wrapper = create_task_wrapper(echo, _send_app(broker), 'test.echo')

    result = wrapper.send(x=1)

    assert is_err(result)
    assert isinstance(result.err_value, TaskSendAsyncContext)
    assert result.err_value.payload is not None


# =============================================================================
# WorkflowHandleErrorCode.ASYNC_CONTEXT and the read failure ReadAsyncContext
# =============================================================================


@pytest.mark.parametrize(
    'name', ['status', 'results', 'tasks', 'cancel', 'pause', 'resume'],
)
def test_workflow_handle_wrap_method_returns_async_context(
    app: Horsies, name: str,
) -> None:
    broker = app.get_broker()
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=uuid.UUID(WORKFLOW_ID), broker=broker,
    )

    result = _inside_loop(getattr(handle, name))

    assert is_err(result)
    assert result.err_value.code is WorkflowHandleErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_id == WORKFLOW_ID
    assert f'{name}_async' in result.err_value.message
    _assert_nothing_ran(broker)


READ_CALLS: dict[str, Callable[[WorkflowHandle[Any]], ReadResult[Any, TaskError]]] = {
    'get': lambda handle: handle.get(wait_ms=0),
    'result_for': lambda handle: handle.result_for(NodeKey('node-1')),
}


@pytest.mark.parametrize('name', sorted(READ_CALLS))
def test_workflow_handle_read_method_returns_async_context(
    app: Horsies, name: str,
) -> None:
    broker = app.get_broker()
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=uuid.UUID(WORKFLOW_ID), broker=broker,
    )

    result = _inside_loop(lambda: READ_CALLS[name](handle))

    match result:
        case Err(ReadAsyncContext() as read_error):
            assert f'{name}_async' in read_error.message
        case _:
            pytest.fail(f'expected Err(ReadAsyncContext), got {result!r}')
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
    assert result.err_value.code is WorkflowHandleErrorCode.ASYNC_CONTEXT
    assert result.err_value.retryable is False
    assert result.err_value.workflow_id == WORKFLOW_ID
    _assert_nothing_ran(broker)


# =============================================================================
# WorkflowStartErrorCode.ASYNC_CONTEXT
# =============================================================================


def _spec(app: Horsies) -> Any:
    @app.task('async_context.echo')
    def echo(*, value: int) -> Result[int, TaskError]:
        return Ok(value)

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


def test_result_read_mapping_asserts_on_async_context() -> None:
    """``TaskHandle.get()`` and ``app.get_result()`` refuse the loop before the
    broker call, so the mapping of the broker's read never sees it."""
    broker_result = Err(DatabaseAsyncContext(
        message='refused',
    ))

    with pytest.raises(AssertionError, match='got ASYNC_CONTEXT'):
        read_of_record(UUID(TASK_ID), broker_result, int, None, TextCodes())


# =============================================================================
# check(live=True)
# =============================================================================


def test_live_check_inside_loop_reports_its_own_code(app: Horsies) -> None:
    errors = _inside_loop(lambda: app.check(live=True).refusals)

    assert [error.code for error in errors] == [HorsiesErrorCode.CHECK_LIVE_ASYNC_CONTEXT]
    assert 'asyncio.to_thread' in (errors[0].help_text or '')


# =============================================================================
# Every blocking call site: a CoreError of the blocking form is the site's
# catalog error; every other exception propagates unchanged
# =============================================================================


def _workflow_handle(broker: PostgresBroker) -> WorkflowHandle[Any]:
    return WorkflowHandle(workflow_id=uuid.UUID(WORKFLOW_ID), broker=broker)


NODE_KEY: NodeKey[Any] = NodeKey('node-1')


@dataclass(frozen=True)
class BlockingSite:
    """A sync facade that calls a blocking form of the core handle."""

    call: Callable[[Horsies, PostgresBroker], Any]
    blocking_form: str
    """The method of ``CoreHandle`` that the facade calls."""
    core_error_code: object
    """The facade's failure code for a ``CoreError`` of the blocking form."""


BLOCKING_SITES: dict[str, BlockingSite] = {
    'broker.close': BlockingSite(
        lambda app, broker: broker.close(), 'close', DatabaseErrorCode.CLOSE_FAILED,
    ),
    'app.get_result': BlockingSite(
        lambda app, broker: app.get_result(UUID(TASK_ID), 0),
        'raw_result_record',
        OperationalCode.DATABASE_ERROR,
    ),
    'app.get_task_info': BlockingSite(
        lambda app, broker: app.get_task_info(UUID(TASK_ID)),
        'task_info',
        DatabaseErrorCode.TASK_INFO_QUERY_FAILED,
    ),
    'app.ping_database': BlockingSite(
        lambda app, broker: app.ping_database(),
        'ping_database',
        DatabaseErrorCode.DATABASE_PING_FAILED,
    ),
    'app.ping_workers': BlockingSite(
        lambda app, broker: app.ping_workers(),
        'ping_workers',
        DatabaseErrorCode.WORKER_PING_FAILED,
    ),
    'app.list_worker_states': BlockingSite(
        lambda app, broker: app.list_worker_states(),
        'worker_states',
        DatabaseErrorCode.MONITORING_QUERY_FAILED,
    ),
    'app.get_worker_state': BlockingSite(
        lambda app, broker: app.get_worker_state('worker-1'),
        'worker_state',
        DatabaseErrorCode.MONITORING_QUERY_FAILED,
    ),
    'app.get_worker_state_history': BlockingSite(
        lambda app, broker: app.get_worker_state_history('worker-1'),
        'worker_state_history',
        DatabaseErrorCode.MONITORING_QUERY_FAILED,
    ),
    'app.get_execution_class_coverage': BlockingSite(
        lambda app, broker: app.get_execution_class_coverage(),
        'execution_class_coverage',
        DatabaseErrorCode.MONITORING_QUERY_FAILED,
    ),
    'app.get_execution_class_report': BlockingSite(
        lambda app, broker: app.get_execution_class_report(),
        'execution_class_report',
        DatabaseErrorCode.MONITORING_QUERY_FAILED,
    ),
    'app.get_stale_tasks': BlockingSite(
        lambda app, broker: app.get_stale_tasks(2),
        'stale_tasks',
        DatabaseErrorCode.MONITORING_QUERY_FAILED,
    ),
    'app.get_expired_tasks': BlockingSite(
        lambda app, broker: app.get_expired_tasks(),
        'expired_tasks',
        DatabaseErrorCode.MONITORING_QUERY_FAILED,
    ),
    'WorkflowHandle.status': BlockingSite(
        lambda app, broker: _workflow_handle(broker).status(),
        'workflow_status',
        WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED,
    ),
    'WorkflowHandle.results': BlockingSite(
        lambda app, broker: _workflow_handle(broker).results(),
        'workflow_node_results',
        WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED,
    ),
    'WorkflowHandle.tasks': BlockingSite(
        lambda app, broker: _workflow_handle(broker).tasks(),
        'workflow_nodes',
        WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED,
    ),
    'WorkflowHandle.cancel': BlockingSite(
        lambda app, broker: _workflow_handle(broker).cancel(),
        'cancel_workflow',
        WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED,
    ),
    'WorkflowHandle.pause': BlockingSite(
        lambda app, broker: _workflow_handle(broker).pause(),
        'pause_workflow',
        WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED,
    ),
    'WorkflowHandle.resume': BlockingSite(
        lambda app, broker: _workflow_handle(broker).resume(),
        'resume_workflow',
        WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED,
    ),
    'WorkflowHandle.get': BlockingSite(
        lambda app, broker: _workflow_handle(broker).get(wait_ms=0),
        'wait_workflow_result',
        OperationalCode.DATABASE_ERROR,
    ),
    'WorkflowHandle.result_for': BlockingSite(
        lambda app, broker: _workflow_handle(broker).result_for(NODE_KEY),
        'workflow_node_result',
        OperationalCode.DATABASE_ERROR,
    ),
    'start_workflow': BlockingSite(
        lambda app, broker: _spec(app).start(WORKFLOW_ID),
        'start_workflow',
        WorkflowStartErrorCode.ENQUEUE_FAILED,
    ),
    'pause_workflow_sync': BlockingSite(
        lambda app, broker: pause_workflow_sync(broker, uuid.UUID(WORKFLOW_ID)),
        'pause_workflow',
        WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED,
    ),
    'resume_workflow_sync': BlockingSite(
        lambda app, broker: resume_workflow_sync(broker, uuid.UUID(WORKFLOW_ID)),
        'resume_workflow',
        WorkflowHandleErrorCode.DATABASE_OPERATION_FAILED,
    ),
}


def _core_handle_raising(site: BlockingSite, raised: BaseException) -> MagicMock:
    """A core handle whose blocking form of ``site`` raises ``raised``."""
    handle = MagicMock()
    setattr(handle, site.blocking_form, MagicMock(side_effect=raised))
    return handle


def _core_error_failure(result: object) -> tuple[object, BaseException | NoCause]:
    """The code of a failed facade result and its exception."""
    match result:
        case Err(err_value=(
            DatabaseOperationFailed(code=code, cause=exception)
            | WorkflowHandleDatabaseOperationFailed(code=code, cause=exception)
            | WorkflowStartEnqueueFailed(code=code, cause=exception)
            | ReadDatabaseError(code=code, cause=exception)
        )):
            return code, exception
        case _:
            raise AssertionError(f'expected a failure, got {result!r}')


@pytest.mark.parametrize('site', list(BLOCKING_SITES))
def test_blocking_call_site_propagates_a_defect(app: Horsies, site: str) -> None:
    """An exception that is not a CoreError propagates unchanged; the
    awaitable form is not called."""
    broker = app.get_broker()
    blocking_site = BLOCKING_SITES[site]
    defect = TypeError('stub defect')
    handle = _core_handle_raising(blocking_site, defect)

    with (
        patch.object(broker, '_core_handle', handle),
        pytest.raises(TypeError) as exc_info,
    ):
        blocking_site.call(app, broker)

    assert exc_info.value is defect
    getattr(handle, blocking_site.blocking_form).assert_called_once()
    getattr(handle, f'{blocking_site.blocking_form}_async').assert_not_called()


@pytest.mark.parametrize('site', list(BLOCKING_SITES))
def test_blocking_call_site_returns_its_code_for_a_core_error(
    app: Horsies, site: str,
) -> None:
    """A CoreError of the blocking form (here: the call reached its bound) is
    the site's own failure, with the error as its exception."""
    broker = app.get_broker()
    blocking_site = BLOCKING_SITES[site]
    timed_out = CoreCallTimedOut(f'CoreHandle.{blocking_site.blocking_form}', 1800, 'no_change')
    handle = _core_handle_raising(blocking_site, timed_out)

    with patch.object(broker, '_core_handle', handle):
        result = blocking_site.call(app, broker)

    code, exception = _core_error_failure(result)
    assert code is blocking_site.core_error_code
    assert exception is timed_out
    getattr(handle, blocking_site.blocking_form).assert_called_once()
    getattr(handle, f'{blocking_site.blocking_form}_async').assert_not_called()


# =============================================================================
# The ASYNC_CONTEXT message names the async form of the called function
# =============================================================================


def _failed_start() -> WorkflowStartEnqueueFailed:
    return WorkflowStartEnqueueFailed(
        message='connection lost',
        retryable=True,
        workflow_name='async_context_wf',
        workflow_id=WORKFLOW_ID,
        cause=ConnectionError('connection lost'),
    )


def _task_handle(app: Horsies) -> TaskHandle[int]:
    return TaskHandle(TASK_ID, app=app, broker_mode=True, ok_type=int)


def _workflow_result_for(app: Horsies) -> ReadResult[Any, TaskError]:
    node: NodeKey[Any] = NodeKey('node-1')
    return _workflow_handle(app.get_broker()).result_for(node)


ASYNC_TWINS: dict[str, tuple[Callable[[Horsies], Any], str]] = {
    'PostgresBroker.get_raw_result_record': (
        lambda app: app.get_broker().get_raw_result_record(TASK_ID, 0),
        'broker.get_raw_result_record_async(',
    ),
    'PostgresBroker.enqueue': (
        lambda app: app.get_broker().enqueue(
            'echo', task_id=TASK_ID, send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
        ),
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
    'Horsies.get_result': (
        lambda app: app.get_result(UUID(TASK_ID), 0),
        'app.get_result_async(',
    ),
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
        lambda app: _workflow_handle(app.get_broker()).get(wait_ms=0),
        'handle.get_async(',
    ),
    'WorkflowHandle.result_for': (_workflow_result_for, 'handle.result_for_async('),
    'pause_workflow_sync': (
        lambda app: pause_workflow_sync(app.get_broker(), uuid.UUID(WORKFLOW_ID)),
        'pause_workflow(broker, workflow_id)',
    ),
    'resume_workflow_sync': (
        lambda app: resume_workflow_sync(app.get_broker(), uuid.UUID(WORKFLOW_ID)),
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
    """The ASYNC_CONTEXT message of an Err."""
    match result:
        case Err(
            DatabaseAsyncContext()
            | WorkflowHandleAsyncContext()
            | WorkflowStartAsyncContext()
            | ReadAsyncContext() as error
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
