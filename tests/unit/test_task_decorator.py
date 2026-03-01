"""Tests for task_decorator: effective_priority, TaskHandle, create_task_wrapper."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.errors import ConfigurationError, ErrorCode, TaskDefinitionError
from horsies.core.exception_mapper import ExceptionMapper
from horsies.core.models.tasks import LibraryErrorCode, TaskError, TaskResult
from horsies.core.models.workflow import WorkflowContextMissingIdError
from horsies.core.task_decorator import (
    FromNodeMarker,
    NodeFactory,
    TaskHandle,
    create_task_wrapper,
    effective_priority,
    from_node,
)
from horsies.core.types.result import Err, Ok


# =============================================================================
# Helpers
# =============================================================================


def _make_app(
    *,
    queue_mode_name: str = 'DEFAULT',
    custom_queues: list[Any] | None = None,
    suppress_sends: bool = False,
    exception_mapper: ExceptionMapper | None = None,
    default_unhandled_error_code: str = 'UNHANDLED_EXCEPTION',
) -> MagicMock:
    """Build a minimal mock Horsies app for unit tests."""
    app = MagicMock()
    app.config.queue_mode.name = queue_mode_name
    app.config.custom_queues = custom_queues
    app.config.exception_mapper = exception_mapper or {}
    app.config.default_unhandled_error_code = default_unhandled_error_code
    app.are_sends_suppressed.return_value = suppress_sends
    app.validate_queue_name.return_value = 'default'
    return app


def _make_queue_config(name: str, priority: int) -> MagicMock:
    """Build a mock CustomQueueConfig."""
    cfg = MagicMock()
    cfg.name = name
    cfg.priority = priority
    return cfg


# =============================================================================
# effective_priority
# =============================================================================


@pytest.mark.unit
class TestEffectivePriority:
    """Tests for effective_priority function."""

    def test_default_mode_returns_100(self) -> None:
        """DEFAULT queue mode always returns priority 100."""
        app = _make_app(queue_mode_name='DEFAULT')

        result = effective_priority(app, 'anything')

        assert result == 100

    def test_custom_mode_matching_queue_returns_configured_priority(self) -> None:
        """CUSTOM mode returns the matching queue's priority."""
        queues = [_make_queue_config('fast', 1), _make_queue_config('slow', 50)]
        app = _make_app(queue_mode_name='CUSTOM', custom_queues=queues)

        result = effective_priority(app, 'fast')

        assert result == 1

    def test_custom_mode_nonexistent_queue_raises_configuration_error(self) -> None:
        """CUSTOM mode with unknown queue raises ConfigurationError(TASK_INVALID_QUEUE)."""
        queues = [_make_queue_config('fast', 1)]
        app = _make_app(queue_mode_name='CUSTOM', custom_queues=queues)

        with pytest.raises(ConfigurationError) as exc_info:
            effective_priority(app, 'missing')

        assert exc_info.value.code == ErrorCode.TASK_INVALID_QUEUE

    def test_custom_mode_none_custom_queues_raises(self) -> None:
        """CUSTOM mode with custom_queues=None raises ConfigurationError."""
        app = _make_app(queue_mode_name='CUSTOM', custom_queues=None)

        with pytest.raises(ConfigurationError) as exc_info:
            effective_priority(app, 'anything')

        assert exc_info.value.code == ErrorCode.TASK_INVALID_QUEUE


# =============================================================================
# TaskHandle._error_result
# =============================================================================


@pytest.mark.unit
class TestTaskHandleErrorResult:
    """Tests for TaskHandle._error_result."""

    def test_creates_error_result_and_caches_it(self) -> None:
        """_error_result creates a TaskError result and marks fetched."""
        handle: TaskHandle[int] = TaskHandle('task-1')

        result = handle._error_result(
            error_code=LibraryErrorCode.BROKER_ERROR,
            message='boom',
            data={'key': 'val'},
        )

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.BROKER_ERROR
        assert result.err.message == 'boom'
        assert handle._result_fetched is True
        assert handle._cached_result is result


# =============================================================================
# TaskHandle.get (sync)
# =============================================================================


@pytest.mark.unit
class TestTaskHandleGet:
    """Tests for TaskHandle.get (blocking)."""

    def test_cached_result_returned_directly(self) -> None:
        """When result is already fetched and cached, returns it."""
        handle: TaskHandle[int] = TaskHandle('t-1')
        cached = TaskResult[int, TaskError](ok=42)
        handle._cached_result = cached
        handle._result_fetched = True

        result = handle.get()

        assert result.is_ok()
        assert result.ok == 42

    def test_cached_none_after_fetch_returns_result_not_available(self) -> None:
        """Fetched flag True but cache is None returns RESULT_NOT_AVAILABLE."""
        handle: TaskHandle[int] = TaskHandle('t-2')
        handle._result_fetched = True
        handle._cached_result = None

        result = handle.get()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.RESULT_NOT_AVAILABLE

    def test_no_broker_mode_no_cache_returns_result_not_available(self) -> None:
        """Without broker mode and no cached result, returns RESULT_NOT_AVAILABLE."""
        handle: TaskHandle[int] = TaskHandle('t-3', broker_mode=False)

        result = handle.get()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.RESULT_NOT_AVAILABLE

    def test_broker_mode_success(self) -> None:
        """Broker mode: successful get_result is cached and returned."""
        app = _make_app()
        broker = MagicMock()
        broker_result = TaskResult[int, TaskError](ok=99)
        broker.get_result.return_value = broker_result
        app.get_broker.return_value = broker

        handle: TaskHandle[int] = TaskHandle('t-4', app=app, broker_mode=True)

        result = handle.get(timeout_ms=5000)

        assert result.is_ok()
        assert result.ok == 99
        assert handle._result_fetched is True
        broker.get_result.assert_called_once_with('t-4', 5000)

    def test_broker_mode_exception_returns_broker_error(self) -> None:
        """Broker mode: exception from broker returns BROKER_ERROR."""
        app = _make_app()
        broker = MagicMock()
        broker.get_result.side_effect = ConnectionError('db down')
        app.get_broker.return_value = broker

        handle: TaskHandle[int] = TaskHandle('t-5', app=app, broker_mode=True)

        result = handle.get()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.BROKER_ERROR
        assert result.err.exception is not None

    def test_wait_timeout_not_cached(self) -> None:
        """WAIT_TIMEOUT is transient; subsequent get() must re-query the broker."""
        app = _make_app()
        broker = MagicMock()
        timeout_result: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(
                error_code=LibraryErrorCode.WAIT_TIMEOUT,
                message='timed out',
                data={},
            ),
        )
        success_result: TaskResult[int, TaskError] = TaskResult(ok=42)
        broker.get_result.side_effect = [timeout_result, success_result]
        app.get_broker.return_value = broker

        handle: TaskHandle[int] = TaskHandle('t-6', app=app, broker_mode=True)

        first = handle.get(timeout_ms=1000)
        assert first.is_err()
        assert first.err is not None
        assert first.err.error_code == LibraryErrorCode.WAIT_TIMEOUT
        assert handle._result_fetched is False

        second = handle.get(timeout_ms=5000)
        assert second.is_ok()
        assert second.ok == 42
        assert handle._result_fetched is True

        assert broker.get_result.call_count == 2


# =============================================================================
# TaskHandle.get_async
# =============================================================================


@pytest.mark.unit
class TestTaskHandleGetAsync:
    """Tests for TaskHandle.get_async."""

    @pytest.mark.asyncio
    async def test_cached_result_returned_directly(self) -> None:
        """Cached result is returned without broker call."""
        handle: TaskHandle[str] = TaskHandle('t-1')
        cached = TaskResult[str, TaskError](ok='hello')
        handle._cached_result = cached
        handle._result_fetched = True

        result = await handle.get_async()

        assert result.is_ok()
        assert result.ok == 'hello'

    @pytest.mark.asyncio
    async def test_cached_none_after_fetch_returns_result_not_available(self) -> None:
        """Fetched but None cache returns RESULT_NOT_AVAILABLE."""
        handle: TaskHandle[str] = TaskHandle('t-2')
        handle._result_fetched = True
        handle._cached_result = None

        result = await handle.get_async()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.RESULT_NOT_AVAILABLE

    @pytest.mark.asyncio
    async def test_no_broker_mode_returns_result_not_available(self) -> None:
        """Without broker mode, returns RESULT_NOT_AVAILABLE."""
        handle: TaskHandle[str] = TaskHandle('t-3', broker_mode=False)

        result = await handle.get_async()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.RESULT_NOT_AVAILABLE

    @pytest.mark.asyncio
    async def test_broker_mode_success(self) -> None:
        """Broker mode: successful async result is cached."""
        app = _make_app()
        broker = MagicMock()
        broker_result = TaskResult[str, TaskError](ok='async-ok')
        broker.get_result_async = AsyncMock(return_value=broker_result)
        app.get_broker.return_value = broker

        handle: TaskHandle[str] = TaskHandle('t-4', app=app, broker_mode=True)

        result = await handle.get_async()

        assert result.is_ok()
        assert result.ok == 'async-ok'
        assert handle._result_fetched is True

    @pytest.mark.asyncio
    async def test_broker_mode_exception_returns_broker_error(self) -> None:
        """Broker mode: exception from broker returns BROKER_ERROR."""
        app = _make_app()
        broker = MagicMock()
        broker.get_result_async = AsyncMock(side_effect=ConnectionError('oops'))
        app.get_broker.return_value = broker

        handle: TaskHandle[str] = TaskHandle('t-5', app=app, broker_mode=True)

        result = await handle.get_async()

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.BROKER_ERROR

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self) -> None:
        """CancelledError is re-raised, not caught as BROKER_ERROR."""
        app = _make_app()
        broker = MagicMock()
        broker.get_result_async = AsyncMock(side_effect=asyncio.CancelledError)
        app.get_broker.return_value = broker

        handle: TaskHandle[str] = TaskHandle('t-6', app=app, broker_mode=True)

        with pytest.raises(asyncio.CancelledError):
            await handle.get_async()

    @pytest.mark.asyncio
    async def test_wait_timeout_not_cached(self) -> None:
        """WAIT_TIMEOUT is transient; subsequent get_async() must re-query the broker."""
        app = _make_app()
        broker = MagicMock()
        timeout_result: TaskResult[str, TaskError] = TaskResult(
            err=TaskError(
                error_code=LibraryErrorCode.WAIT_TIMEOUT,
                message='timed out',
                data={},
            ),
        )
        success_result: TaskResult[str, TaskError] = TaskResult(ok='done')
        broker.get_result_async = AsyncMock(
            side_effect=[timeout_result, success_result],
        )
        app.get_broker.return_value = broker

        handle: TaskHandle[str] = TaskHandle('t-7', app=app, broker_mode=True)

        first = await handle.get_async(timeout_ms=1000)
        assert first.is_err()
        assert first.err is not None
        assert first.err.error_code == LibraryErrorCode.WAIT_TIMEOUT
        assert handle._result_fetched is False

        second = await handle.get_async(timeout_ms=5000)
        assert second.is_ok()
        assert second.ok == 'done'
        assert handle._result_fetched is True

        assert broker.get_result_async.call_count == 2


# =============================================================================
# TaskHandle.info / info_async
# =============================================================================


@pytest.mark.unit
class TestTaskHandleInfo:
    """Tests for TaskHandle.info and info_async."""

    def test_info_without_broker_mode_returns_no_broker_error(self) -> None:
        """info() without broker mode returns Err(NO_BROKER)."""
        handle: TaskHandle[int] = TaskHandle('t-1', broker_mode=False)

        result = handle.info()

        assert result.is_err()
        err = result.err_value
        assert err.code == BrokerErrorCode.NO_BROKER
        assert 'requires a broker-backed' in err.message
        assert err.retryable is False

    @pytest.mark.asyncio
    async def test_info_async_without_broker_mode_returns_no_broker_error(self) -> None:
        """info_async() without broker mode returns Err(NO_BROKER)."""
        handle: TaskHandle[int] = TaskHandle('t-2', broker_mode=False)

        result = await handle.info_async()

        assert result.is_err()
        err = result.err_value
        assert err.code == BrokerErrorCode.NO_BROKER
        assert 'requires a broker-backed' in err.message
        assert err.retryable is False

    def test_info_with_broker_delegates(self) -> None:
        """info() with broker mode delegates to broker.get_task_info."""
        app = _make_app()
        broker = MagicMock()
        sentinel = object()
        broker.get_task_info.return_value = sentinel
        app.get_broker.return_value = broker

        handle: TaskHandle[int] = TaskHandle('t-3', app=app, broker_mode=True)

        result = handle.info(include_result=True)

        assert result is sentinel
        broker.get_task_info.assert_called_once_with(
            't-3',
            include_result=True,
            include_failed_reason=False,
        )

    @pytest.mark.asyncio
    async def test_info_async_with_broker_delegates(self) -> None:
        """info_async() with broker mode delegates to broker.get_task_info_async."""
        app = _make_app()
        broker = MagicMock()
        sentinel = object()
        broker.get_task_info_async = AsyncMock(return_value=sentinel)
        app.get_broker.return_value = broker

        handle: TaskHandle[int] = TaskHandle('t-4', app=app, broker_mode=True)

        result = await handle.info_async(include_failed_reason=True)

        assert result is sentinel
        broker.get_task_info_async.assert_called_once_with(
            't-4',
            include_result=False,
            include_failed_reason=True,
        )


# =============================================================================
# TaskHandle.set_immediate_result
# =============================================================================


@pytest.mark.unit
class TestTaskHandleSetImmediateResult:
    """Tests for TaskHandle.set_immediate_result."""

    def test_sets_cache_and_flag(self) -> None:
        """set_immediate_result sets cached result and fetched flag."""
        handle: TaskHandle[int] = TaskHandle('t-1')
        ok_result = TaskResult[int, TaskError](ok=42)

        handle.set_immediate_result(ok_result)

        assert handle._cached_result is ok_result
        assert handle._result_fetched is True


# =============================================================================
# create_task_wrapper — validation errors
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperValidation:
    """Tests for create_task_wrapper type-annotation validation."""

    def test_no_return_type_raises(self) -> None:
        """Function with no return annotation raises TASK_NO_RETURN_TYPE."""
        def bad_fn(x: int):  # type: ignore[no-untyped-def]
            pass

        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:
            create_task_wrapper(bad_fn, app, 'test.bad_fn')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.TASK_NO_RETURN_TYPE

    def test_wrong_return_type_raises(self) -> None:
        """Function returning non-TaskResult raises TASK_INVALID_RETURN_TYPE."""
        def bad_fn(x: int) -> int:
            return x

        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:
            create_task_wrapper(bad_fn, app, 'test.bad_fn')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.TASK_INVALID_RETURN_TYPE

    def test_wrong_type_args_count_raises(self) -> None:
        """TaskResult with wrong number of type args raises TASK_INVALID_RETURN_TYPE."""
        # Use raw TaskResult without type params (get_args returns ())
        def bad_fn(x: int) -> TaskResult:  # type: ignore[type-arg]
            return TaskResult(ok=x)  # pyright: ignore[reportUnknownVariableType]

        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:
            create_task_wrapper(bad_fn, app, 'test.bad_fn')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.TASK_INVALID_RETURN_TYPE

    def test_valid_function_creates_callable_wrapper(self) -> None:
        """Valid function produces a wrapper with correct task_name."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        assert wrapper.task_name == 'test.good_fn'
        assert callable(wrapper)


# =============================================================================
# create_task_wrapper — wrapped_function execution paths
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperExecution:
    """Tests for wrapped_function execution (calling the wrapper)."""

    def test_successful_execution(self) -> None:
        """Calling wrapper with valid function returns TaskResult(ok=...)."""
        def add_one(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x + 1)

        app = _make_app()
        wrapper = create_task_wrapper(add_one, app, 'test.add_one')

        result = wrapper(5)

        assert result.is_ok()
        assert result.ok == 6

    def test_returns_none_produces_task_exception(self) -> None:
        """Function returning None produces TASK_EXCEPTION error."""
        def bad_fn(x: int) -> TaskResult[int, TaskError]:
            return None  # type: ignore[return-value]

        app = _make_app()
        wrapper = create_task_wrapper(bad_fn, app, 'test.bad_fn')

        result = wrapper(1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.TASK_EXCEPTION
        assert 'returned None' in (result.err.message or '')

    def test_return_type_mismatch_produces_error(self) -> None:
        """Returning wrong ok type produces RETURN_TYPE_MISMATCH."""
        def bad_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok='not-an-int')  # type: ignore[arg-type]

        app = _make_app()
        wrapper = create_task_wrapper(bad_fn, app, 'test.bad_fn')

        result = wrapper(1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.RETURN_TYPE_MISMATCH

    def test_keyboard_interrupt_propagates(self) -> None:
        """KeyboardInterrupt re-raises for graceful worker shutdown."""
        def interrupting_fn(x: int) -> TaskResult[int, TaskError]:
            raise KeyboardInterrupt

        app = _make_app()
        wrapper = create_task_wrapper(interrupting_fn, app, 'test.interrupt')

        with pytest.raises(KeyboardInterrupt):
            wrapper(1)

    def test_workflow_context_missing_id_error(self) -> None:
        """WorkflowContextMissingIdError produces WORKFLOW_CTX_MISSING_ID."""
        def ctx_fn(x: int) -> TaskResult[int, TaskError]:
            raise WorkflowContextMissingIdError('missing node id')

        app = _make_app()
        wrapper = create_task_wrapper(ctx_fn, app, 'test.ctx_fn')

        result = wrapper(1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.WORKFLOW_CTX_MISSING_ID

    def test_generic_exception_uses_exception_mapper(self) -> None:
        """Unhandled exception resolved via exception_mapper chain."""

        class CustomError(Exception):
            pass

        def failing_fn(x: int) -> TaskResult[int, TaskError]:
            raise CustomError('kaboom')

        mapper: ExceptionMapper = {CustomError: 'CUSTOM_MAPPED'}
        app = _make_app()
        wrapper = create_task_wrapper(
            failing_fn,
            app,
            'test.failing_fn',
            exception_mapper=mapper,
        )

        result = wrapper(1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'CUSTOM_MAPPED'

    def test_generic_exception_falls_back_to_global_default(self) -> None:
        """Without mapper match, uses global default_unhandled_error_code."""
        def failing_fn(x: int) -> TaskResult[int, TaskError]:
            raise ValueError('oops')

        app = _make_app(default_unhandled_error_code='UNHANDLED_EXCEPTION')
        wrapper = create_task_wrapper(failing_fn, app, 'test.failing_fn')

        result = wrapper(1)

        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'UNHANDLED_EXCEPTION'
        assert 'ValueError' in (result.err.message or '')


# =============================================================================
# create_task_wrapper — send() paths
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperSend:
    """Tests for wrapper.send() method."""

    def test_send_suppressed_returns_send_suppressed(self) -> None:
        """When sends are suppressed, returns SEND_SUPPRESSED handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(suppress_sends=True)
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = wrapper.send(1)

        result = handle.get()
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.SEND_SUPPRESSED

    def test_send_queue_validation_failure_returns_error_handle(self) -> None:
        """Queue validation error returns an error handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        app.validate_queue_name.side_effect = ConfigurationError(
            message='bad queue',
            code=ErrorCode.TASK_INVALID_QUEUE,
        )
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = wrapper.send(1)

        result = handle.get()
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.UNHANDLED_EXCEPTION

    def test_send_success_returns_broker_mode_handle(self) -> None:
        """Successful send returns a TaskHandle with broker_mode=True."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('task-abc')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = wrapper.send(1)

        assert handle.task_id == 'task-abc'
        assert handle._broker_mode is True

    def test_send_broker_exception_returns_error_handle(self) -> None:
        """Broker Err result during enqueue returns error handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='db gone',
            retryable=True,
            exception=ConnectionError('db gone'),
        ))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = wrapper.send(1)

        result = handle.get()
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.UNHANDLED_EXCEPTION


# =============================================================================
# create_task_wrapper — send_async() paths
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperSendAsync:
    """Tests for wrapper.send_async() method."""

    @pytest.mark.asyncio
    async def test_send_async_suppressed(self) -> None:
        """When sends suppressed, send_async returns SEND_SUPPRESSED handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(suppress_sends=True)
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = await wrapper.send_async(1)

        result = handle.get()
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.SEND_SUPPRESSED

    @pytest.mark.asyncio
    async def test_send_async_success(self) -> None:
        """Successful async send returns broker-mode handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue_async = AsyncMock(return_value=Ok('task-xyz'))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = await wrapper.send_async(1)

        assert handle.task_id == 'task-xyz'
        assert handle._broker_mode is True

    @pytest.mark.asyncio
    async def test_send_async_broker_exception(self) -> None:
        """Broker Err result during async enqueue returns error handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue_async = AsyncMock(return_value=Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='fail',
            retryable=False,
            exception=RuntimeError('fail'),
        )))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = await wrapper.send_async(1)

        result = handle.get()
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.UNHANDLED_EXCEPTION

    @pytest.mark.asyncio
    async def test_send_async_enqueue_raises_returns_error_handle(self) -> None:
        """Async enqueue infrastructure exception returns error handle (not raise)."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue_async = AsyncMock(side_effect=RuntimeError('loop dead'))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = await wrapper.send_async(1)

        result = handle.get()
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.UNHANDLED_EXCEPTION
        assert isinstance(result.err.exception, RuntimeError)
        assert 'Failed to enqueue task good_fn: loop dead' in result.err.message

    @pytest.mark.asyncio
    async def test_send_async_queue_validation_failure(self) -> None:
        """Queue validation error in send_async returns error handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        app.validate_queue_name.side_effect = ConfigurationError(
            message='bad queue',
            code=ErrorCode.TASK_INVALID_QUEUE,
        )
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = await wrapper.send_async(1)

        result = handle.get()
        assert result.is_err()


# =============================================================================
# create_task_wrapper — schedule() paths
# =============================================================================


@pytest.mark.unit
class TestCreateTaskWrapperSchedule:
    """Tests for wrapper.schedule() method."""

    def test_schedule_suppressed_returns_send_suppressed(self) -> None:
        """When sends suppressed, schedule() returns SEND_SUPPRESSED handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app(suppress_sends=True)
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = wrapper.schedule(60, 1)

        result = handle.get()
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.SEND_SUPPRESSED

    def test_schedule_success(self) -> None:
        """Successful schedule returns broker-mode handle with delay."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Ok('sched-1')
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = wrapper.schedule(60, 1)

        assert handle.task_id == 'sched-1'
        assert handle._broker_mode is True
        # Verify enqueue was called with sent_at (call-site time) and enqueue_delay_seconds
        call_kwargs = broker.enqueue.call_args
        assert call_kwargs.kwargs.get('sent_at') is not None
        assert call_kwargs.kwargs.get('enqueue_delay_seconds') == 60

    def test_schedule_broker_exception_returns_error_handle(self) -> None:
        """Broker Err result during schedule returns error handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        broker = MagicMock()
        broker.enqueue.return_value = Err(BrokerOperationError(
            code=BrokerErrorCode.ENQUEUE_FAILED,
            message='gone',
            retryable=True,
            exception=ConnectionError('gone'),
        ))
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = wrapper.schedule(60, 1)

        result = handle.get()
        assert result.is_err()

    def test_schedule_queue_validation_failure(self) -> None:
        """Queue validation error in schedule returns error handle."""
        def good_fn(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        app = _make_app()
        app.validate_queue_name.side_effect = ConfigurationError(
            message='bad',
            code=ErrorCode.TASK_INVALID_QUEUE,
        )
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        handle = wrapper.schedule(60, 1)

        result = handle.get()
        assert result.is_err()


# =============================================================================
# FromNodeMarker / from_node()
# =============================================================================


@pytest.mark.unit
class TestFromNodeMarker:
    """Tests for FromNodeMarker and from_node() helper."""

    def test_from_node_returns_marker_with_correct_node(self) -> None:
        """from_node() wraps the upstream node in a FromNodeMarker."""
        from horsies.core.models.workflow import TaskNode

        upstream_node = TaskNode(fn=_make_task_fn(), kwargs={'value': 1})

        result = from_node(upstream_node)

        assert isinstance(result, FromNodeMarker)
        assert result.node is upstream_node

    def test_marker_repr_is_readable(self) -> None:
        """FromNodeMarker repr includes the wrapped node."""
        mock_node = MagicMock(name='MockNode')

        marker = FromNodeMarker(mock_node)

        assert 'FromNodeMarker' in repr(marker)
        assert 'MockNode' in repr(marker)

    def test_from_node_rejects_non_node_value(self) -> None:
        """from_node() rejects non-node inputs with a structured error."""
        from horsies.core.errors import WorkflowValidationError

        with pytest.raises(WorkflowValidationError) as exc_info:
            from_node('not_a_node')  # type: ignore[arg-type]

        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_ARGS_FROM


# =============================================================================
# NodeFactory — positional arg rejection
# =============================================================================


def _make_task_fn(app: MagicMock | None = None) -> Any:
    """Create a minimal wrapper via create_task_wrapper for NodeFactory tests."""
    def sample(value: int, label: str = 'default') -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    if app is None:
        app = _make_app()
    return create_task_wrapper(sample, app, 'test.sample')


@pytest.mark.unit
class TestNodeFactoryPositionalRejection:
    """Tests for NodeFactory rejecting positional .node()() calls (D.1)."""

    def test_positional_args_raise_workflow_validation_error(self) -> None:
        """Positional args in .node()() raise WorkflowValidationError(E026)."""
        from horsies.core.errors import WorkflowValidationError

        task_fn = _make_task_fn()
        factory = task_fn.node()

        with pytest.raises(WorkflowValidationError) as exc_info:
            factory(42)  # positional — forbidden

        assert exc_info.value.code == ErrorCode.WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED
        assert 'positional' in exc_info.value.message

    def test_kwargs_only_succeeds(self) -> None:
        """Kwargs-only .node()() call succeeds and creates TaskNode."""
        from horsies.core.models.workflow import TaskNode

        task_fn = _make_task_fn()
        factory = task_fn.node()

        node = factory(value=10, label='test')

        assert isinstance(node, TaskNode)
        assert node.kwargs == {'value': 10, 'label': 'test'}
        assert node.args == ()

    def test_empty_call_succeeds(self) -> None:
        """Empty .node()() call (no args) succeeds — all args from injection/defaults."""
        from horsies.core.models.workflow import TaskNode

        task_fn = _make_task_fn()
        factory = task_fn.node()

        node = factory()

        assert isinstance(node, TaskNode)
        assert node.kwargs == {}
        assert node.args == ()


# =============================================================================
# NodeFactory — from_node() marker conversion
# =============================================================================


@pytest.mark.unit
class TestNodeFactoryFromNodeConversion:
    """Tests for NodeFactory converting from_node() markers (C.1)."""

    def test_marker_kwarg_becomes_args_from_entry(self) -> None:
        """from_node() kwarg is extracted into args_from dict."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        # Producer
        def produce(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        # Consumer
        def consume(data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node()

        node = factory(data=from_node(producer_node))

        assert node.args_from == {'data': producer_node}
        assert node.kwargs == {}  # marker removed from static kwargs

    def test_marker_auto_wires_waits_for(self) -> None:
        """from_node() automatically adds upstream to waits_for."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node()

        node = factory(data=from_node(producer_node))

        assert producer_node in node.waits_for

    def test_marker_does_not_duplicate_waits_for(self) -> None:
        """If upstream is already in waits_for, from_node() doesn't add it again."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        # Explicit waits_for already includes producer
        factory = consumer_fn.node(waits_for=[producer_node])

        node = factory(data=from_node(producer_node))

        assert node.waits_for.count(producer_node) == 1
        assert node.args_from == {'data': producer_node}

    def test_multiple_markers_from_different_upstreams(self) -> None:
        """Multiple from_node() markers from different upstreams all wire correctly."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce_a(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        fn_a = create_task_wrapper(produce_a, app, 'test.produce_a')
        node_a = TaskNode(fn=fn_a, kwargs={'value': 1})

        def produce_b(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        fn_b = create_task_wrapper(produce_b, app, 'test.produce_b')
        node_b = TaskNode(fn=fn_b, kwargs={'value': 2})

        def consume(
            first: TaskResult[int, TaskError],
            second: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node()

        node = factory(first=from_node(node_a), second=from_node(node_b))

        assert node.args_from == {'first': node_a, 'second': node_b}
        assert node_a in node.waits_for
        assert node_b in node.waits_for
        assert node.kwargs == {}

    def test_mixed_static_and_marker_kwargs(self) -> None:
        """Static kwargs and from_node() markers coexist correctly."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(
            data: TaskResult[int, TaskError],
            label: str = 'default',
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node()

        node = factory(data=from_node(producer_node), label='custom')

        assert node.args_from == {'data': producer_node}
        assert node.kwargs == {'label': 'custom'}
        assert producer_node in node.waits_for


# =============================================================================
# NodeFactory — from_node() conflict detection
# =============================================================================


@pytest.mark.unit
class TestNodeFactoryFromNodeConflicts:
    """Tests for conflict detection between from_node() and explicit args_from."""

    def test_marker_conflicts_with_explicit_args_from_raises(self) -> None:
        """Same key in from_node() and explicit args_from raises E021."""
        from horsies.core.errors import WorkflowValidationError
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        # Explicit args_from for 'data'
        factory = consumer_fn.node(args_from={'data': producer_node})

        # Also from_node() for 'data' — conflict
        with pytest.raises(WorkflowValidationError) as exc_info:
            factory(data=from_node(producer_node))

        assert exc_info.value.code == ErrorCode.WORKFLOW_KWARGS_ARGS_FROM_OVERLAP
        assert 'data' in str(exc_info.value)

    def test_disjoint_marker_and_explicit_args_from_merges(self) -> None:
        """Disjoint from_node() and explicit args_from keys merge correctly."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce_a(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        fn_a = create_task_wrapper(produce_a, app, 'test.produce_a')
        node_a = TaskNode(fn=fn_a, kwargs={'value': 1})

        def produce_b(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        fn_b = create_task_wrapper(produce_b, app, 'test.produce_b')
        node_b = TaskNode(fn=fn_b, kwargs={'value': 2})

        def consume(
            first: TaskResult[int, TaskError],
            second: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        # 'first' via explicit args_from, 'second' via from_node()
        factory = consumer_fn.node(
            args_from={'first': node_a},
            waits_for=[node_a],
        )

        node = factory(second=from_node(node_b))  # type: ignore[call-arg]

        assert node.args_from == {'first': node_a, 'second': node_b}  # type: ignore[union-attr]
        assert node_a in node.waits_for  # type: ignore[union-attr]
        assert node_b in node.waits_for  # type: ignore[union-attr]


# =============================================================================
# NodeFactory — manual args_from preserved
# =============================================================================


@pytest.mark.unit
class TestNodeFactoryManualArgsFrom:
    """Tests that manual args_from continues to work without from_node()."""

    def test_manual_args_from_still_works(self) -> None:
        """Explicit args_from without markers is forwarded unchanged."""
        from horsies.core.models.workflow import TaskNode

        app = _make_app()

        def produce(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        producer_fn = create_task_wrapper(produce, app, 'test.produce')
        producer_node = TaskNode(fn=producer_fn, kwargs={'value': 42})

        def consume(data: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)
        consumer_fn = create_task_wrapper(consume, app, 'test.consume')
        factory = consumer_fn.node(
            args_from={'data': producer_node},
            waits_for=[producer_node],
        )

        node = factory()  # type: ignore[call-arg]  # no kwargs needed — all from injection

        assert node.args_from == {'data': producer_node}  # type: ignore[union-attr]
        assert producer_node in node.waits_for  # type: ignore[union-attr]
        assert node.kwargs == {}  # type: ignore[union-attr]
