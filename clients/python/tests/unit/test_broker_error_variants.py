"""The broker error variants: one per state, redacted, each cause shape, each mapping.

``DatabaseOperationError`` is a union with one frozen variant per state. The
nine operation codes share ``DatabaseOperationFailed``; every other code has a
variant of its own. A variant's repr hides the message and the cause.
``broker_error_cause`` gives the exception, or ``NoCause()``. A send sees
only the variants of ``DatabaseEnqueueError | DatabaseSyncCallError``.
"""

from __future__ import annotations

from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from horsies.core.telemetry.send_telemetry import NO_SEND_TELEMETRY
from horsies.core.brokers.result_types import (
    DatabaseAsyncContext,
    DatabaseEnqueueError,
    DatabaseEnqueueFailed,
    DatabaseErrorCode,
    DatabaseIdempotencyKeyConflict,
    DatabaseInvalidInput,
    DatabaseInvalidJsonPayload,
    DatabaseInvalidTaskOptions,
    DatabaseListenerDisabled,
    DatabaseWaitInsideTask,
    DatabaseNoTypeAvailable,
    DatabaseNotAttached,
    DatabaseOperationError,
    DatabaseOperationFailed,
    DatabaseOperationFailedCode,
    DatabasePayloadMismatch,
    DatabaseSyncCallError,
    broker_error_cause,
)
from horsies.core.cli import _raise_schema_error  # pyright: ignore[reportPrivateUsage]
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import (
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendError,
    TaskSendIdempotencyKeyConflict,
    TaskSendPayloadMismatch,
    TaskSendValidationFailed,
)
from horsies.core.models.outcome import (
    ReadDatabaseError,
    NoTypeAvailable,
    ResultDeserializationError,
    WaitInsideTask,
)
from horsies.core.models.tasks import TaskError
from horsies.core.task_decorator import TaskHandle, create_task_wrapper
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, Result, is_err
from tests.union_members import union_members

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-7d3e'
CAUSE_MARKER = 'cause-marker-7d3e'
OWNER_TASK_ID = UUID('0198c0de-0000-7000-8000-00000000000a')
HANDLE_TASK_ID = UUID('0198c0de-0000-7000-8000-00000000000b')


def _every_variant(cause: BaseException) -> list[DatabaseOperationError]:
    """One value of each variant; each cause field holds ``cause``."""
    message = f'failed: {MESSAGE_MARKER}'
    return [
        DatabaseOperationFailed(
            code=DatabaseErrorCode.DATABASE_PING_FAILED,
            message=message,
            retryable=True,
            exception=cause,
        ),
        DatabaseEnqueueFailed(message=message, retryable=True, exception=cause),
        DatabasePayloadMismatch(message=message),
        DatabaseIdempotencyKeyConflict(message=message, owner_task_id=OWNER_TASK_ID),
        DatabaseNotAttached(message=message),
        DatabaseNoTypeAvailable(message=message),
        DatabaseInvalidJsonPayload(message=message, exception=cause),
        DatabaseInvalidTaskOptions(message=message, exception=cause),
        DatabaseInvalidInput(message=message, exception=cause),
        DatabaseAsyncContext(message=message),
        DatabaseListenerDisabled(message=message),
        DatabaseWaitInsideTask(message=message),
    ]


ENQUEUE_VARIANTS: frozenset[type[Any]] = frozenset(
    union_members(DatabaseEnqueueError)
    + union_members(DatabaseSyncCallError)
)


def _every_enqueue_variant(cause: BaseException) -> list[DatabaseOperationError]:
    """One value of each variant that ``PostgresBroker.enqueue`` returns."""
    return [
        error for error in _every_variant(cause) if type(error) in ENQUEUE_VARIANTS
    ]


def _every_other_variant(cause: BaseException) -> list[DatabaseOperationError]:
    """One value of each variant that ``PostgresBroker.enqueue`` does not return."""
    return [
        error
        for error in _every_variant(cause)
        if type(error) not in ENQUEUE_VARIANTS
    ]


CAUSED_VARIANTS = (
    DatabaseOperationFailed,
    DatabaseEnqueueFailed,
    DatabaseInvalidJsonPayload,
    DatabaseInvalidTaskOptions,
    DatabaseInvalidInput,
)


def _variant_id(error: DatabaseOperationError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_state(self) -> None:
        codes: list[DatabaseErrorCode] = []
        for variant in union_members(DatabaseOperationError):
            if variant is DatabaseOperationFailed:
                codes.extend(get_args(DatabaseOperationFailedCode.__value__))
            else:
                codes.append(variant.code)

        assert len(codes) == len(set(codes))
        assert set(codes) == set(DatabaseErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(union_members(DatabaseOperationError))

    def test_the_enqueue_errors_are_disjoint_broker_errors(self) -> None:
        enqueue = set(union_members(DatabaseEnqueueError))
        sync_call = set(union_members(DatabaseSyncCallError))

        assert enqueue.isdisjoint(sync_call)
        assert enqueue | sync_call <= set(union_members(DatabaseOperationError))

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError(CAUSE_MARKER)), ids=_variant_id
    )
    def test_repr_hides_message_and_cause(self, error: DatabaseOperationError) -> None:
        text = repr(error)

        assert MESSAGE_MARKER not in text
        assert CAUSE_MARKER not in text
        assert type(error).__name__ in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id
    )
    def test_every_variant_has_a_code_and_a_retryable_flag(
        self,
        error: DatabaseOperationError,
    ) -> None:
        assert isinstance(error.code, DatabaseErrorCode)
        assert isinstance(error.retryable, bool)


class TestBrokerErrorCause:
    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id
    )
    def test_a_cause_field_gives_its_exception_and_no_field_gives_no_cause(
        self,
        error: DatabaseOperationError,
    ) -> None:
        cause = broker_error_cause(error)

        match error:
            case (
                DatabaseOperationFailed()
                | DatabaseEnqueueFailed()
                | DatabaseInvalidJsonPayload()
                | (DatabaseInvalidTaskOptions() | DatabaseInvalidInput())
            ):
                assert cause is error.exception
                assert isinstance(cause, RuntimeError)
            case _:
                assert type(error) not in CAUSED_VARIANTS
                assert cause == NoCause()

    def test_a_data_check_cause_gives_no_cause(self) -> None:
        error = DatabaseInvalidJsonPayload(
            message='not a JSON object', exception=NoCause()
        )

        assert broker_error_cause(error) == NoCause()


def _make_app() -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.send_telemetry.return_value = NO_SEND_TELEMETRY
    app.validate_queue_name.return_value = 'default'
    return app


def _send_with_broker_error(error: DatabaseOperationError) -> TaskSendError:
    def good_fn(*, x: int) -> Result[int, TaskError]:
        return Ok(x)

    app = _make_app()
    broker = MagicMock()
    broker.enqueue.return_value = Err(error)
    app.get_broker.return_value = broker
    result = create_task_wrapper(good_fn, app, 'test.good_fn').send(x=1)
    assert is_err(result)
    return result.err_value


SEND_VARIANT_OF: dict[type[Any], type[Any]] = {
    DatabaseEnqueueFailed: TaskSendEnqueueFailed,
    DatabasePayloadMismatch: TaskSendPayloadMismatch,
    DatabaseIdempotencyKeyConflict: TaskSendIdempotencyKeyConflict,
    DatabaseAsyncContext: TaskSendAsyncContext,
    DatabaseInvalidTaskOptions: TaskSendValidationFailed,
    DatabaseInvalidInput: TaskSendValidationFailed,
}


class TestSendMapping:
    def test_every_enqueue_variant_has_a_send_variant(self) -> None:
        assert set(SEND_VARIANT_OF) == ENQUEUE_VARIANTS

    @pytest.mark.parametrize(
        'error', _every_enqueue_variant(RuntimeError('cause')), ids=_variant_id
    )
    def test_each_enqueue_variant_maps_to_its_send_variant(
        self,
        error: DatabaseOperationError,
    ) -> None:
        send_error = _send_with_broker_error(error)

        assert type(send_error) is SEND_VARIANT_OF[type(error)]
        assert send_error.message == error.message

    @pytest.mark.parametrize('cause', [RuntimeError('cause'), NoCause()], ids=repr)
    @pytest.mark.parametrize('retryable', [True, False], ids=repr)
    def test_an_enqueue_failure_keeps_retryable_and_the_cause(
        self,
        retryable: bool,
        cause: BaseException | NoCause,
    ) -> None:
        send_error = _send_with_broker_error(
            DatabaseEnqueueFailed(
                message='write failed', retryable=retryable, exception=cause,
            ),
        )

        assert isinstance(send_error, TaskSendEnqueueFailed)
        assert send_error.retryable is retryable
        assert send_error.exception is cause

    @pytest.mark.parametrize(
        'error', _every_other_variant(RuntimeError('cause')), ids=_variant_id
    )
    def test_a_variant_that_enqueue_does_not_return_is_a_defect(
        self,
        error: DatabaseOperationError,
    ) -> None:
        with pytest.raises(AssertionError, match=type(error).__name__):
            _ = _send_with_broker_error(error)

    @pytest.mark.parametrize('cause', [RuntimeError('cause'), NoCause()], ids=repr)
    @pytest.mark.parametrize(
        'variant',
        [DatabaseInvalidTaskOptions, DatabaseInvalidInput],
        ids=lambda v: v.__name__,
    )
    def test_a_refused_input_keeps_the_cause(
        self,
        variant: type[DatabaseInvalidTaskOptions] | type[DatabaseInvalidInput],
        cause: BaseException | NoCause,
    ) -> None:
        send_error = _send_with_broker_error(
            variant(message='refused', exception=cause)
        )

        assert isinstance(send_error, TaskSendValidationFailed)
        assert send_error.exception is cause

    def test_the_key_conflict_keeps_the_owner(self) -> None:
        send_error = _send_with_broker_error(
            DatabaseIdempotencyKeyConflict(message='reserved', owner_task_id=OWNER_TASK_ID),
        )

        assert isinstance(send_error, TaskSendIdempotencyKeyConflict)
        assert send_error.owner_task_id == OWNER_TASK_ID


def _handle(broker: MagicMock) -> TaskHandle[int]:
    app = MagicMock()
    app.get_broker = MagicMock(return_value=broker)
    return TaskHandle(task_id=HANDLE_TASK_ID, app=app, broker_mode=True, ok_type=int)


READ_ERROR_OF: dict[type[Any], tuple[type[Any], bool]] = {
    DatabaseNoTypeAvailable: (NoTypeAvailable, True),
    DatabaseInvalidJsonPayload: (ResultDeserializationError, True),
    DatabaseWaitInsideTask: (WaitInsideTask, False),
}


@pytest.mark.asyncio
class TestResultMapping:
    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('cause')), ids=_variant_id
    )
    async def test_each_broker_variant_maps_to_its_read_error_and_cache_rule(
        self,
        error: DatabaseOperationError,
    ) -> None:
        if isinstance(error, DatabaseAsyncContext):
            pytest.skip(
                'ASYNC_CONTEXT fails an assertion: get() refuses a running loop first'
            )
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(return_value=Err(error))
        handle = _handle(broker)
        expected_type, cached = READ_ERROR_OF.get(type(error), (ReadDatabaseError, False))

        first = await handle.get_async()
        _ = await handle.get_async()

        match first:
            case Err(err_value=ReadDatabaseError() as read_error):
                assert expected_type is ReadDatabaseError
                assert read_error.message == (
                    f'the result read of task {HANDLE_TASK_ID} failed '
                    f'({error.code.value}): {error.message}'
                )
                assert read_error.retryable is error.retryable
                match broker_error_cause(error):
                    case NoCause():
                        assert read_error.exception == NoCause()
                    case cause:
                        assert read_error.exception is cause
            case Err(err_value=read_error):
                assert type(read_error) is expected_type
                assert read_error.message == error.message
            case Ok():
                pytest.fail(f'expected a read error, got {first!r}')
        assert broker.get_raw_result_record_async.await_count == (1 if cached else 2)

    async def test_async_context_fails_an_assertion(self) -> None:
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            return_value=Err(DatabaseAsyncContext(message='sync call in a running loop')),
        )

        with pytest.raises(AssertionError, match='got ASYNC_CONTEXT'):
            await _handle(broker).get_async()


class TestProcessBoundaries:
    def test_cli_schema_error_with_a_cause_is_chained(self) -> None:
        cause = ConnectionError('db down')
        error = DatabaseOperationFailed(
            code=DatabaseErrorCode.SCHEMA_INIT_FAILED,
            message='schema init failed',
            retryable=False,
            exception=cause,
        )

        with pytest.raises(RuntimeError, match='schema init failed') as raised:
            _raise_schema_error(error)

        assert raised.value.__cause__ is cause

    def test_cli_schema_error_without_a_cause_is_not_chained(self) -> None:
        error = DatabaseNotAttached(message='no broker')

        with pytest.raises(RuntimeError, match='no broker') as raised:
            _raise_schema_error(error)

        assert raised.value.__cause__ is None
