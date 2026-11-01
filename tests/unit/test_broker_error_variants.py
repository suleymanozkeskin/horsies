"""The broker error variants: one per state, redacted, each cause shape, each mapping.

``BrokerOperationError`` is a union with one frozen variant per state. The
nine operation codes share ``BrokerOperationFailed``; every other code has a
variant of its own. A variant's repr hides the message and the cause.
``broker_error_cause`` gives the exception, or ``NoCause()``.
"""

from __future__ import annotations

from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.brokers.result_types import (
    BrokerAsyncContext,
    BrokerEnqueueFailed,
    BrokerErrorCode,
    BrokerEventLoopLimit,
    BrokerIdempotencyKeyConflict,
    BrokerInvalidInput,
    BrokerInvalidJsonPayload,
    BrokerInvalidTaskOptions,
    BrokerListenerDisabled,
    BrokerLoopRunnerFailed,
    BrokerNoTypeAvailable,
    BrokerNotAttached,
    BrokerOperationError,
    BrokerOperationFailed,
    BrokerOperationFailedCode,
    BrokerPayloadMismatch,
    broker_error_cause,
)
from horsies.core.cli import _raise_schema_error  # pyright: ignore[reportPrivateUsage]
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import (
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendError,
    TaskSendEventLoopLimit,
    TaskSendIdempotencyKeyConflict,
    TaskSendLoopRunnerFailed,
    TaskSendPayloadMismatch,
    TaskSendValidationFailed,
)
from horsies.core.models.tasks import (
    BuiltInTaskCode,
    ContractCode,
    OperationalErrorCode,
    RetrievalCode,
    TaskError,
    TaskResult,
)
from horsies.core.task_decorator import TaskHandle, create_task_wrapper
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, is_err
from horsies.core.utils.loop_runner import LoopRunnerError
from horsies.core.worker.reaper import _operation_sqlstate  # pyright: ignore[reportPrivateUsage]

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-7d3e'
CAUSE_MARKER = 'cause-marker-7d3e'


def _loop_limit() -> EventLoopLimit:
    return EventLoopLimit(
        limit=5,
        held_thread_names=('MainThread',),
        current_thread_name='worker-1',
    )


def _every_variant(cause: BaseException) -> list[BrokerOperationError]:
    """One value of each variant; each cause field holds ``cause``."""
    message = f'failed: {MESSAGE_MARKER}'
    return [
        BrokerOperationFailed(
            code=BrokerErrorCode.DB_PING_FAILED,
            message=message,
            retryable=True,
            exception=cause,
        ),
        BrokerEnqueueFailed(message=message, retryable=True, exception=cause),
        BrokerPayloadMismatch(message=message),
        BrokerIdempotencyKeyConflict(message=message, owner_task_id='owner-1'),
        BrokerNotAttached(message=message),
        BrokerNoTypeAvailable(message=message),
        BrokerInvalidJsonPayload(message=message, exception=cause),
        BrokerInvalidTaskOptions(message=message, exception=cause),
        BrokerInvalidInput(message=message, exception=cause),
        BrokerEventLoopLimit(message=message, loop_limit=_loop_limit()),
        BrokerAsyncContext(message=message),
        BrokerLoopRunnerFailed(message=message, exception=cause),
        BrokerListenerDisabled(message=message),
    ]


CAUSED_VARIANTS = (
    BrokerOperationFailed,
    BrokerEnqueueFailed,
    BrokerInvalidJsonPayload,
    BrokerInvalidTaskOptions,
    BrokerInvalidInput,
    BrokerLoopRunnerFailed,
)


def _variant_id(error: BrokerOperationError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_state(self) -> None:
        codes: list[BrokerErrorCode] = []
        for variant in get_args(BrokerOperationError.__value__):
            if variant is BrokerOperationFailed:
                codes.extend(get_args(BrokerOperationFailedCode.__value__))
            else:
                codes.append(variant.code)

        assert len(codes) == len(set(codes))
        assert set(codes) == set(BrokerErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(get_args(BrokerOperationError.__value__))

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError(CAUSE_MARKER)), ids=_variant_id
    )
    def test_repr_hides_message_and_cause(self, error: BrokerOperationError) -> None:
        text = repr(error)

        assert MESSAGE_MARKER not in text
        assert CAUSE_MARKER not in text
        assert type(error).__name__ in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id
    )
    def test_every_variant_has_a_code_and_a_retryable_flag(
        self,
        error: BrokerOperationError,
    ) -> None:
        assert isinstance(error.code, BrokerErrorCode)
        assert isinstance(error.retryable, bool)


class TestBrokerErrorCause:
    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id
    )
    def test_a_cause_field_gives_its_exception_and_no_field_gives_no_cause(
        self,
        error: BrokerOperationError,
    ) -> None:
        cause = broker_error_cause(error)

        match error:
            case (
                BrokerOperationFailed()
                | BrokerEnqueueFailed()
                | BrokerInvalidJsonPayload()
                | (
                    BrokerInvalidTaskOptions()
                    | BrokerInvalidInput()
                    | BrokerLoopRunnerFailed()
                )
            ):
                assert cause is error.exception
                assert isinstance(cause, RuntimeError)
            case _:
                assert type(error) not in CAUSED_VARIANTS
                assert cause == NoCause()

    def test_a_data_check_cause_gives_no_cause(self) -> None:
        error = BrokerInvalidJsonPayload(
            message='not a JSON object', exception=NoCause()
        )

        assert broker_error_cause(error) == NoCause()


def _make_app() -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = 'default'
    return app


def _send_with_broker_error(error: BrokerOperationError) -> TaskSendError:
    def good_fn(*, x: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=x)

    app = _make_app()
    broker = MagicMock()
    broker.enqueue.return_value = Err(error)
    app.get_broker.return_value = broker
    result = create_task_wrapper(good_fn, app, 'test.good_fn').send(x=1)
    assert is_err(result)
    return result.err_value


SEND_VARIANT_OF: dict[type[Any], type[Any]] = {
    BrokerPayloadMismatch: TaskSendPayloadMismatch,
    BrokerEventLoopLimit: TaskSendEventLoopLimit,
    BrokerIdempotencyKeyConflict: TaskSendIdempotencyKeyConflict,
    BrokerAsyncContext: TaskSendAsyncContext,
    BrokerInvalidTaskOptions: TaskSendValidationFailed,
    BrokerInvalidInput: TaskSendValidationFailed,
    BrokerLoopRunnerFailed: TaskSendLoopRunnerFailed,
}


class TestSendMapping:
    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('cause')), ids=_variant_id
    )
    def test_each_broker_variant_maps_to_its_send_variant(
        self,
        error: BrokerOperationError,
    ) -> None:
        send_error = _send_with_broker_error(error)

        assert type(send_error) is SEND_VARIANT_OF.get(
            type(error), TaskSendEnqueueFailed
        )
        assert send_error.message == error.message

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('cause')), ids=_variant_id
    )
    def test_an_enqueue_failure_keeps_retryable_and_the_cause(
        self,
        error: BrokerOperationError,
    ) -> None:
        if type(error) in SEND_VARIANT_OF:
            pytest.skip('maps to a send variant of its own')

        send_error = _send_with_broker_error(error)

        assert isinstance(send_error, TaskSendEnqueueFailed)
        assert send_error.retryable is error.retryable
        assert send_error.exception == broker_error_cause(error)

    @pytest.mark.parametrize('cause', [RuntimeError('cause'), NoCause()], ids=repr)
    @pytest.mark.parametrize(
        'variant',
        [BrokerInvalidTaskOptions, BrokerInvalidInput],
        ids=lambda v: v.__name__,
    )
    def test_a_refused_input_keeps_the_cause(
        self,
        variant: type[BrokerInvalidTaskOptions] | type[BrokerInvalidInput],
        cause: BaseException | NoCause,
    ) -> None:
        send_error = _send_with_broker_error(
            variant(message='refused', exception=cause)
        )

        assert isinstance(send_error, TaskSendValidationFailed)
        assert send_error.exception is cause

    def test_the_key_conflict_keeps_the_owner(self) -> None:
        send_error = _send_with_broker_error(
            BrokerIdempotencyKeyConflict(message='reserved', owner_task_id='owner-9'),
        )

        assert isinstance(send_error, TaskSendIdempotencyKeyConflict)
        assert send_error.owner_task_id == 'owner-9'


def _handle(broker: MagicMock) -> TaskHandle[int]:
    app = MagicMock()
    app.get_broker = MagicMock(return_value=broker)
    return TaskHandle(task_id='task-1', app=app, broker_mode=True, ok_type=int)


RESULT_CODE_OF: dict[type[Any], tuple[BuiltInTaskCode, bool]] = {
    BrokerNoTypeAvailable: (ContractCode.NO_TYPE_AVAILABLE, True),
    BrokerInvalidJsonPayload: (OperationalErrorCode.RESULT_DESERIALIZATION_ERROR, True),
    BrokerEventLoopLimit: (RetrievalCode.EVENT_LOOP_LIMIT, False),
}


@pytest.mark.asyncio
class TestResultMapping:
    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('cause')), ids=_variant_id
    )
    async def test_each_broker_variant_maps_to_its_task_error_and_cache_rule(
        self,
        error: BrokerOperationError,
    ) -> None:
        if isinstance(error, BrokerAsyncContext):
            pytest.skip(
                'ASYNC_CONTEXT fails an assertion: get() refuses a running loop first'
            )
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(return_value=Err(error))
        handle = _handle(broker)
        expected_code, cached = RESULT_CODE_OF.get(
            type(error),
            (OperationalErrorCode.BROKER_ERROR, False),
        )

        first = await handle.get_async()
        _ = await handle.get_async()

        task_error = first.unwrap_err()
        assert task_error.error_code == expected_code
        assert task_error.data == {'task_id': 'task-1', 'broker_code': error.code.value}
        match broker_error_cause(error):
            case NoCause():
                assert task_error.exception is None
            case cause:
                assert task_error.exception is cause
        assert broker.get_raw_result_record_async.await_count == (1 if cached else 2)

    async def test_async_context_fails_an_assertion(self) -> None:
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            return_value=Err(BrokerAsyncContext(message='sync call in a running loop')),
        )

        with pytest.raises(AssertionError, match='got ASYNC_CONTEXT'):
            await _handle(broker).get_async()


class TestProcessBoundaries:
    def test_cli_schema_error_with_a_cause_is_chained(self) -> None:
        cause = ConnectionError('db down')
        error = BrokerOperationFailed(
            code=BrokerErrorCode.SCHEMA_INIT_FAILED,
            message='schema init failed',
            retryable=False,
            exception=cause,
        )

        with pytest.raises(RuntimeError, match='schema init failed') as raised:
            _raise_schema_error(error)

        assert raised.value.__cause__ is cause

    def test_cli_schema_error_without_a_cause_is_not_chained(self) -> None:
        error = BrokerEventLoopLimit(message='loop limit', loop_limit=_loop_limit())

        with pytest.raises(RuntimeError, match='loop limit') as raised:
            _raise_schema_error(error)

        assert raised.value.__cause__ is None

    def test_reaper_sqlstate_reads_the_driver_cause(self) -> None:
        driver_error = MagicMock()
        driver_error.sqlstate = '55P03'
        wrapper = RuntimeError('lock not available')
        wrapper.orig = driver_error  # type: ignore[attr-defined]
        error = BrokerOperationFailed(
            code=BrokerErrorCode.CLEANUP_FAILED,
            message='audit refused',
            retryable=False,
            exception=wrapper,
        )

        assert _operation_sqlstate(error) == '55P03'

    def test_reaper_sqlstate_of_an_error_without_a_cause_is_none(self) -> None:
        error = BrokerListenerDisabled(message='no listener')

        assert _operation_sqlstate(error) is None

    def test_a_loop_runner_failure_keeps_the_runner_error(self) -> None:
        runner_error = LoopRunnerError('Loop runner is not running')
        error = BrokerLoopRunnerFailed(message='runner stopped', exception=runner_error)

        assert broker_error_cause(error) is runner_error
        assert error.retryable is False
