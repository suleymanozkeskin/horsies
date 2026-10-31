"""The send error variants: one per code, redacted, and each cause shape.

``TaskSendError`` is a union with one frozen variant per
``TaskSendErrorCode`` member. A variant's repr hides the payload, the
message and the cause. The two variants with a cause field hold the
exception, or ``NoCause()`` when a check found the failure.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, get_args
from unittest.mock import MagicMock

import pytest

from horsies.core.brokers.result_types import BrokerEnqueueFailed, BrokerOperationError
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import (
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendError,
    TaskSendErrorCode,
    TaskSendEventLoopLimit,
    TaskSendIdempotencyKeyConflict,
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
from horsies.core.types.result import Err, is_err

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

    def test_broker_lookup_failure_keeps_its_exception(self) -> None:
        app = _make_app()
        cause = RuntimeError('no broker configured')
        app.get_broker.side_effect = cause

        result = _wrapper(app).send(x=1)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendEnqueueFailed)
        assert result.err_value.exception is cause
        assert result.err_value.retryable is False
        assert result.err_value.payload.task_name == 'test.good_fn'


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
