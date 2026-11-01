"""The send error variants: one per code, redacted, and each cause shape.

``TaskSendError`` is a union with one frozen variant per
``TaskSendErrorCode`` member. A variant's repr hides the payload, the
message and the cause. ``TaskSendValidationFailed`` and
``TaskSendEnqueueFailed`` hold the exception, or ``NoCause()`` when a check
found the failure. ``TaskSendLoopRunnerFailed`` always holds the
``LoopRunnerError``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.result_types import (
    BrokerEnqueueFailed,
    BrokerLoopRunnerFailed,
    BrokerOperationError,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.errors import ErrorCode, HorsiesError
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import (
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendError,
    TaskSendErrorCode,
    TaskSendEventLoopLimit,
    TaskSendIdempotencyKeyConflict,
    TaskSendLoopRunnerFailed,
    TaskSendPayload,
    TaskSendPayloadMismatch,
    TaskSendPayloadTooLarge,
    TaskSendRetryable,
    TaskSendSuppressed,
    TaskSendValidationFailed,
)
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.utils.loop_runner import LoopRunnerError

PAYLOAD_MARKER = 'kwargs-marker-41c2'
MESSAGE_MARKER = 'message-marker-41c2'
CAUSE_MARKER = 'cause-marker-41c2'


def _payload() -> TaskSendPayload:
    return TaskSendPayload(
        task_name='test.good_fn',
        queue_name='default',
        priority=100,
        args_json=None,
        kwargs_json=f'{{"secret": "{PAYLOAD_MARKER}"}}',
        sent_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        good_until=None,
        enqueue_delay_seconds=None,
        task_options=None,
        enqueue_sha='abc123',
    )


def _every_variant() -> list[TaskSendError]:
    payload = _payload()
    message = f'failed: {MESSAGE_MARKER}'
    cause = RuntimeError(CAUSE_MARKER)
    return [
        TaskSendSuppressed(message=message),
        TaskSendAsyncContext(task_id='t-1', payload=payload, message=message),
        TaskSendValidationFailed(message=message, exception=cause),
        TaskSendEnqueueFailed(
            task_id='t-1',
            payload=payload,
            message=message,
            retryable=True,
            exception=cause,
        ),
        TaskSendPayloadMismatch(task_id='t-1', payload=payload, message=message),
        TaskSendPayloadTooLarge(message=message, size_bytes=2048, limit_bytes=1024),
        TaskSendIdempotencyKeyConflict(
            task_id='t-1',
            payload=payload,
            message=message,
            owner_task_id='owner-1',
        ),
        TaskSendEventLoopLimit(message=message),
        TaskSendLoopRunnerFailed(
            task_id='t-1',
            payload=payload,
            message=message,
            exception=cause,
        ),
    ]


def _refused_variants() -> list[TaskSendError]:
    accepted = set(get_args(TaskSendRetryable.__value__))
    return [error for error in _every_variant() if type(error) not in accepted]


def _make_app() -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = 'default'
    return app


def _wrapper(app: MagicMock) -> Any:
    def good_fn(*, x: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=x)

    return create_task_wrapper(good_fn, app, 'test.good_fn')


def _broker_returning(app: MagicMock, error: BrokerOperationError) -> MagicMock:
    broker = MagicMock()
    broker.enqueue.return_value = Err(error)
    app.get_broker.return_value = broker
    return broker


@pytest.mark.unit
class TestVariantCatalog:
    def test_each_code_has_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in get_args(TaskSendError.__value__)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(TaskSendErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant()}

        assert built == set(get_args(TaskSendError.__value__))

    def test_retryable_alias_lists_the_accepted_variants(self) -> None:
        assert set(get_args(TaskSendRetryable.__value__)) == {
            TaskSendEnqueueFailed,
            TaskSendAsyncContext,
            TaskSendIdempotencyKeyConflict,
            TaskSendLoopRunnerFailed,
        }

    @pytest.mark.parametrize(
        'error', _every_variant(), ids=lambda error: type(error).__name__,
    )
    def test_repr_hides_payload_message_and_cause(self, error: TaskSendError) -> None:
        for text in (repr(error), str(error)):
            assert text.startswith(f'{type(error).__name__}(')
            assert PAYLOAD_MARKER not in text
            assert MESSAGE_MARKER not in text
            assert CAUSE_MARKER not in text

    def test_no_cause_instances_are_equal(self) -> None:
        assert NoCause() == NoCause()


@pytest.mark.unit
class TestValidationFailedCause:
    def test_positional_args_give_no_cause(self) -> None:
        app = _make_app()

        result = _wrapper(app).send(1)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendValidationFailed)
        assert result.err_value.exception == NoCause()
        assert 'positional' in result.err_value.message
        app.get_broker.assert_not_called()

    def test_kwargs_that_fail_their_type_give_a_cause_without_input(self) -> None:
        app = _make_app()
        marker = 'send-marker-7f3a'

        result = _wrapper(app).send(x=marker)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, TaskSendValidationFailed)
        assert isinstance(err.exception, ValueValidationError)
        assert err.exception.__context__ is None
        assert err.exception.__cause__ is None
        assert '[int_parsing]' in err.message
        assert marker not in err.message
        assert marker not in str(err.exception)
        app.get_broker.assert_not_called()


@pytest.mark.unit
class TestEnqueueFailedCause:
    def test_broker_data_check_without_exception_gives_no_cause(self) -> None:
        app = _make_app()
        _broker_returning(app, BrokerEnqueueFailed(
            message='task row is gone before the key was bound',
            retryable=False,
            exception=NoCause(),
        ))

        result = _wrapper(app).send(x=1)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendEnqueueFailed)
        assert result.err_value.exception == NoCause()
        assert result.err_value.retryable is False

    def test_broker_exception_is_kept(self) -> None:
        app = _make_app()
        cause = ConnectionError('db gone')
        _broker_returning(app, BrokerEnqueueFailed(
            message='db gone',
            retryable=True,
            exception=cause,
        ))

        result = _wrapper(app).send(x=1)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendEnqueueFailed)
        assert result.err_value.exception is cause
        assert result.err_value.retryable is True

    @pytest.mark.parametrize('form', ['send', 'send_async', 'schedule', 'schedule_async'])
    def test_broker_init_failure_propagates(self, form: str) -> None:
        """``get_broker()`` raises ``BROKER_INIT_FAILED`` as a fail-fast error.

        Every send and schedule form lets it propagate; no form turns it
        into ``TaskSendEnqueueFailed``.
        """
        app = _make_app()
        init_failure = HorsiesError(
            message='Failed to initialize broker: bad url',
            code=ErrorCode.BROKER_INIT_FAILED,
        )
        app.get_broker.side_effect = init_failure
        wrapper = _wrapper(app)

        with pytest.raises(HorsiesError) as exc_info:
            match form:
                case 'send':
                    wrapper.send(x=1)
                case 'send_async':
                    asyncio.run(wrapper.send_async(x=1))
                case 'schedule':
                    wrapper.schedule(5, x=1)
                case 'schedule_async':
                    asyncio.run(wrapper.schedule_async(5, x=1))
                case _:
                    raise AssertionError(f'unknown form: {form}')

        assert exc_info.value is init_failure
        app.get_broker.assert_called_once_with()


@pytest.mark.unit
class TestRetryRefusesOtherVariants:
    """A caller without a type checker can pass a refused variant."""

    def test_the_fixture_has_five_refused_variants(self) -> None:
        assert len(_refused_variants()) == 5

    @pytest.mark.parametrize(
        'error', _refused_variants(), ids=lambda error: type(error).__name__,
    )
    def test_retry_send_fails_an_assertion_that_names_the_variant(
        self, error: TaskSendError,
    ) -> None:
        app = _make_app()

        with pytest.raises(AssertionError) as raised:
            _wrapper(app).retry_send(error)

        text = str(raised.value)
        assert f'got: {type(error).__name__}(' in text
        assert PAYLOAD_MARKER not in text
        assert MESSAGE_MARKER not in text
        app.get_broker.assert_not_called()


def _runner_failure() -> BrokerLoopRunnerFailed:
    return BrokerLoopRunnerFailed(
        message='Loop runner failed for PostgresBroker.enqueue()',
        exception=LoopRunnerError('runner stopped'),
    )


def _loop_runner_err(result: Any) -> TaskSendLoopRunnerFailed:
    assert is_err(result)
    err = result.err_value
    assert isinstance(err, TaskSendLoopRunnerFailed)
    return err


@pytest.mark.unit
class TestLoopRunnerFailed:
    """A sync send on a stopped runner: nothing ran, and the async retry completes it."""

    @pytest.mark.parametrize('form', ['send', 'schedule'])
    def test_a_sync_send_keeps_the_task_id_payload_and_cause(self, form: str) -> None:
        app = _make_app()
        failure = _runner_failure()
        broker = _broker_returning(app, failure)
        wrapper = _wrapper(app)

        match form:
            case 'send':
                result = wrapper.send(x=1)
            case 'schedule':
                result = wrapper.schedule(5, x=1)
            case _:
                raise AssertionError(f'unknown form: {form}')

        err = _loop_runner_err(result)
        assert err.exception is failure.exception
        assert err.message == failure.message
        assert err.task_id == broker.enqueue.call_args.kwargs['task_id']
        assert err.payload.kwargs_json == broker.enqueue.call_args.kwargs['kwargs_json']

    def test_a_sync_retry_on_a_stopped_runner_returns_the_same_error(self) -> None:
        app = _make_app()
        broker = _broker_returning(app, _runner_failure())
        wrapper = _wrapper(app)
        err = _loop_runner_err(wrapper.send(x=1))

        again = _loop_runner_err(wrapper.retry_send(err))

        assert again.task_id == err.task_id
        assert broker.enqueue.call_count == 2

    @pytest.mark.parametrize('form', ['send', 'schedule'])
    def test_the_async_retry_completes_the_same_dispatch(self, form: str) -> None:
        app = _make_app()
        broker = _broker_returning(app, _runner_failure())
        wrapper = _wrapper(app)
        match form:
            case 'send':
                err = _loop_runner_err(wrapper.send(x=1))
            case 'schedule':
                err = _loop_runner_err(wrapper.schedule(5, x=1))
            case _:
                raise AssertionError(f'unknown form: {form}')
        broker.enqueue_async = AsyncMock(return_value=Ok(err.task_id))

        match form:
            case 'send':
                result = asyncio.run(wrapper.retry_send_async(err))
            case 'schedule':
                result = asyncio.run(wrapper.retry_schedule_async(err))
            case _:
                raise AssertionError(f'unknown form: {form}')

        assert not is_err(result)
        assert result.ok_value.task_id == err.task_id
        sent = broker.enqueue_async.call_args.kwargs
        assert sent['task_id'] == err.task_id
        assert sent['enqueue_sha'] == err.payload.enqueue_sha
        assert sent['kwargs_json'] == err.payload.kwargs_json
