"""Tests for PostgresBroker (clients/python/horsies/core/brokers/postgres.py).

Strategy: a double of the broker's core handle stands in for the core, so no
test reaches PostgreSQL. Tests verify logic, branching, error handling, and
delegation patterns.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from horsies.core.telemetry.send_telemetry import NO_SEND_TELEMETRY
from horsies.core.brokers.enqueue_task import SendKind
from horsies._core import (
    CoreConnectionError,
    CoreContractViolation,
    CorePreconditionFailed,
    CoreServerError,
    CoreStartFailed,
    DatabasePingRecord,
    Enqueued,
    EnqueueRequest,
    HandleClosed,
    NoTraceCarrier,
    StartAfterDelay as CoreStartAfterDelay,
    StartAt as CoreStartAt,
    StartNow as CoreStartNow,
    TraceCarrier,
    SchemaInstalled,
    SchemaInstallNotRun,
    RawResultAbsent,
    RawResultRecordRecord,
    TaskAttemptRecord,
    TaskInfoNotFound,
    TaskInfoRecord,
    StaleClaimsBoundReached,
    StaleClaimsDrained,
    StaleClaimsRequeueFailed,
    StaleRunningRecovered,
    StaleRunningScanFailed,
    StaleRunningScanFull,
    TooManyPendingCalls,
    OrphanAuditFailed,
    OrphanAuditLockBusy,
    OrphanPageAudited,
    PendingExpiryBoundReached,
    PendingExpiryDrained,
    PendingExpiryFailed,
)
from horsies.core.brokers.postgres import SIGNED_64_BIT_MAX, SIGNED_64_BIT_MIN
from horsies.core.brokers.result_types import (
    DatabaseAsyncContext,
    DatabaseEnqueueFailed,
    DatabaseErrorCode,
    DatabaseIdempotencyKeyConflict,
    DatabaseInvalidInput,
    DatabaseInvalidJsonPayload,
    DatabaseInvalidTaskOptions,
    DatabaseListenerDisabled,
    DatabaseOperationFailed,
    DatabasePayloadMismatch,
    RawResultRecord,
)
from horsies.core.codec.json_io import EmptyJsonTextError, SerializationError
from horsies.core.models.health import (
    DatabasePing,
)
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import (
    TaskSendErrorCode,
    TaskSendValidationFailed,
)
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.error_detail import NoCause
from horsies.core.models.tasks import (
    Execution,
    TaskError,
    TaskInfo,
)
from horsies.core.types.result import Err, Ok, Result, is_err, is_ok
from horsies.core.types.status import TaskAttemptOutcome, TaskStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_CONNECTION_LOST = CoreConnectionError('task_info', 'connection lost', True)
"""The exception of a failure record whose connection was lost."""

_DIGEST_MISMATCH = CoreContractViolation('task_info', 'stored result digest mismatch')
"""The exception of a failure record whose stored data does not read."""

_LOCK_NOT_AVAILABLE = CoreServerError(
    'audit_orphaned_workflow_tasks', '55P03', 'could not obtain lock', True,
)
"""The exception of an orphan page whose cursor another sweep holds."""

_COUNT_DIFFERS = CoreContractViolation(
    'audit_orphaned_workflow_tasks', 'the cursor candidate count 2 differs',
)
"""The exception of an orphan page outside the function's contract."""

_FAMILY_MISMATCH = CorePreconditionFailed(
    'install_schema', 'the database was installed by the RUST client', False,
)
"""The exception of a schema install into a database of the other family."""


def _make_broker(database_url: str = 'postgresql://u:p@localhost/db') -> Any:
    """A PostgresBroker that counts as initialized; a test replaces its core
    handle with a double. Building it does no I/O."""
    from horsies.core.models.broker import PostgresConfig
    from horsies.core.brokers.postgres import PostgresBroker

    broker = PostgresBroker(PostgresConfig(database_url=database_url))
    # Mark as initialized to skip the schema install.
    broker._initialized = True
    return broker


def _recording_thread(threads: list[threading.Thread], answer: object) -> MagicMock:
    """A blocking core call that records the thread it ran on and answers
    ``answer``."""

    def call(*_args: object, **_kwargs: object) -> object:
        threads.append(threading.current_thread())
        return answer

    return MagicMock(side_effect=call)


# ---------------------------------------------------------------------------
# TestEnqueueAsync
# ---------------------------------------------------------------------------


_NO_SERVER_URL = 'postgresql://u:p@127.0.0.1:9/db'
"""A URL where no server listens: a send that the facade or the core's
``prepare_send`` refuses never reaches it, and any other send fails as
ENQUEUE_FAILED."""


def _refusing_broker() -> Any:
    """A broker on the real core handle, for sends that are refused before any
    I/O."""
    return _make_broker(_NO_SERVER_URL)


@pytest.mark.unit
class TestEnqueueAsync:
    """``PostgresBroker.enqueue_async`` refuses a send before any I/O: task
    options, the input text and the start parameters."""

    @staticmethod
    def _broker_answering_enqueued() -> Any:
        broker = _make_broker()
        handle = MagicMock()
        handle.enqueue_async = AsyncMock(
            return_value=MagicMock(spec=Enqueued, task_id=SEND_TASK_ID),
        )
        broker._core_handle = handle
        return broker

    @pytest.mark.asyncio
    async def test_malformed_task_options_raises(self) -> None:
        """Malformed task_options JSON is refused as INVALID_TASK_OPTIONS with
        the SerializationError as its cause. Nothing was written."""
        result = await _refusing_broker().enqueue_async(
            'my_task',
            task_id=SEND_TASK_ID,
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            task_options='NOT_VALID_JSON{{',
        )

        assert is_err(result)
        match result.err_value:
            case DatabaseInvalidTaskOptions(message=message, cause=cause):
                assert 'JSON' in message
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_TASK_OPTIONS, got {other!r}')

    @pytest.mark.asyncio
    async def test_kwargs_that_repeat_a_key_are_refused_as_invalid_input(self) -> None:
        """The stored input would differ from the text the caller sent."""
        result = await _refusing_broker().enqueue_async(
            'my_task',
            task_id=SEND_TASK_ID,
            sent_at=datetime.now(timezone.utc),
            send_kind=SendKind.FIRST,
            input_json='{"x":1,"x":2}',
        )

        assert is_err(result)
        match result.err_value:
            case DatabaseInvalidInput(message=message, cause=cause):
                assert message == (
                    'kwargs JSON corrupt for my_task: '
                    'JSON parse failed: JSON object repeats a key'
                )
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')

    @pytest.mark.asyncio
    async def test_empty_kwargs_text_is_refused_as_invalid_input(self) -> None:
        """An empty text is not JSON; no input is None."""
        result = await _refusing_broker().enqueue_async(
            'my_task',
            task_id=SEND_TASK_ID,
            sent_at=datetime.now(timezone.utc),
            send_kind=SendKind.FIRST,
            input_json='',
        )

        assert is_err(result)
        match result.err_value:
            case DatabaseInvalidInput(message=message, cause=cause):
                assert message == (
                    'kwargs JSON corrupt for my_task: '
                    'JSON parse failed: the text is empty'
                )
                assert isinstance(cause, EmptyJsonTextError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')

    @pytest.mark.asyncio
    async def test_both_scheduling_parameters_are_refused_as_invalid_input(self) -> None:
        """enqueued_at and enqueue_delay_seconds together → INVALID_INPUT; nothing is written."""
        result = await _refusing_broker().enqueue_async(
            'my_task',
            task_id=SEND_TASK_ID,
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            enqueued_at=datetime.now(timezone.utc),
            enqueue_delay_seconds=30,
        )

        assert is_err(result)
        match result.err_value:
            case DatabaseInvalidInput(message=message, cause=cause):
                assert 'enqueued_at and enqueue_delay_seconds' in message
                assert cause == NoCause()
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')

    @pytest.mark.asyncio
    async def test_task_options_with_non_dict_retry_policy_is_refused(self) -> None:
        """A retry_policy that is not an object is refused before any write."""
        result = await _refusing_broker().enqueue_async(
            'my_task',
            task_id=SEND_TASK_ID,
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            task_options='{"retry_policy": "not_a_dict"}',
        )

        assert is_err(result)
        assert result.err_value.code == DatabaseErrorCode.INVALID_TASK_OPTIONS
        assert 'retry_policy is not an object: str' in result.err_value.message

    @pytest.mark.asyncio
    async def test_future_sent_at_without_scheduling_params_rejected(self) -> None:
        """Future sent_at without enqueued_at or enqueue_delay_seconds must error.

        sent_at is an immutable call-site timestamp, not a scheduling mechanism.
        Passing a future value without explicit scheduling params is ambiguous
        (legacy ETA pattern) and would silently run immediately.
        """
        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        result = await _refusing_broker().enqueue_async(
            'my_task',
            task_id=SEND_TASK_ID,
            send_kind=SendKind.FIRST,
            sent_at=future,
        )

        assert is_err(result)
        match result.err_value:
            case DatabaseInvalidInput(message=message, cause=cause):
                assert 'sent_at' in message
                assert cause == NoCause()
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')

    @pytest.mark.asyncio
    async def test_future_sent_at_with_enqueued_at_accepted(self) -> None:
        """Future sent_at is allowed when enqueued_at is explicitly provided."""
        broker = self._broker_answering_enqueued()
        future = datetime.now(timezone.utc) + timedelta(minutes=10)

        with patch('horsies.core.brokers.postgres.EnqueueRequest') as request_class:
            result = await broker.enqueue_async(
                'my_task',
                task_id=SEND_TASK_ID,
                send_kind=SendKind.FIRST,
                sent_at=future,
                enqueued_at=future,
            )

        assert result == Ok(SEND_TASK_ID)
        broker._core_handle.enqueue_async.assert_awaited_once()
        start = request_class.call_args.kwargs['start']
        assert isinstance(start, CoreStartAt)
        assert start.at == future

    @pytest.mark.asyncio
    async def test_future_sent_at_with_enqueue_delay_accepted(self) -> None:
        """Future sent_at is allowed when enqueue_delay_seconds is provided."""
        broker = self._broker_answering_enqueued()
        future = datetime.now(timezone.utc) + timedelta(minutes=10)

        with patch('horsies.core.brokers.postgres.EnqueueRequest') as request_class:
            result = await broker.enqueue_async(
                'my_task',
                task_id=SEND_TASK_ID,
                send_kind=SendKind.FIRST,
                sent_at=future,
                enqueue_delay_seconds=600,
            )

        assert result == Ok(SEND_TASK_ID)
        broker._core_handle.enqueue_async.assert_awaited_once()
        start = request_class.call_args.kwargs['start']
        assert isinstance(start, CoreStartAfterDelay)
        assert start.seconds == 600


# ---------------------------------------------------------------------------
# TestEnqueueOutcomes — the answers of horsies_enqueue_task
# ---------------------------------------------------------------------------


SEND_TASK_ID = UUID('0198c0de-0000-7000-8000-000000000017')


def _core_request(**values: Any) -> EnqueueRequest:
    """A request of the core with the values of a plain send of ``my_task``
    and ``values`` over them. The request is built directly, so the core
    sees a value that the facade's own checks refuse first."""
    request: dict[str, Any] = {
        'task_id': SEND_TASK_ID,
        'task_name': 'my_task',
        'queue_name': 'default',
        'priority': 100,
        'input_json': '{}',
        'sent_at': datetime.now(timezone.utc),
        'start': CoreStartNow(),
        'good_until': None,
        'task_options_json': None,
        'idempotency_key': None,
        'retention_class_key': 'standard_30d',
        'rerun_input': 'APP_DEFAULT',
        'send_kind': 'FIRST',
        'trace': NoTraceCarrier(),
        **values,
    }
    return EnqueueRequest(**request)


@pytest.mark.unit
class TestEnqueueThroughCore:
    """enqueue_async and enqueue build the core's request with the client
    rules, and return the outcome of one core send: the sent id, or the
    broker class that the core built, as it is."""

    @staticmethod
    def _broker_answering(outcome: object) -> Any:
        broker = _make_broker()
        handle = MagicMock()
        handle.enqueue_async = AsyncMock(return_value=outcome)
        handle.enqueue = MagicMock(return_value=outcome)
        broker._core_handle = handle
        return broker

    @staticmethod
    async def _send(broker: Any, **keywords: Any) -> Any:
        arguments: dict[str, Any] = {
            'task_id': SEND_TASK_ID,
            'send_kind': SendKind.FIRST,
            'input_json': '{"x":1}',
            'sent_at': datetime.now(timezone.utc),
            **keywords,
        }
        return await broker.enqueue_async('my_task', 'default', **arguments)

    @pytest.mark.asyncio
    async def test_an_enqueued_send_returns_its_id_and_builds_the_request(self) -> None:
        broker = self._broker_answering(MagicMock(spec=Enqueued, task_id=SEND_TASK_ID))
        with patch('horsies.core.brokers.postgres.EnqueueRequest') as request_class:
            result = await self._send(
                broker,
                send_kind=SendKind.REPEAT,
                enqueue_delay_seconds=30,
                idempotency_key='order-17',
                retain_rerun_input=False,
                priority=50,
            )

        assert result == Ok(SEND_TASK_ID)
        built = request_class.call_args.kwargs
        assert built['task_id'] == SEND_TASK_ID
        assert built['input_json'] == '{"x":1}'
        assert built['priority'] == 50
        assert built['send_kind'] == 'REPEAT'
        assert built['idempotency_key'] == 'order-17'
        assert built['retention_class_key'] == 'standard_30d'
        assert built['rerun_input'] == 'DROP'
        assert isinstance(built['start'], CoreStartAfterDelay)
        broker._core_handle.enqueue_async.assert_awaited_once_with(request_class.return_value)

    @pytest.mark.asyncio
    async def test_the_request_carries_the_trace_of_the_send(self) -> None:
        broker = self._broker_answering(MagicMock(spec=Enqueued, task_id=SEND_TASK_ID))
        carrier = TraceCarrier({'traceparent': '00-ab-cd-01'}, bytes(range(1, 17)))
        with patch('horsies.core.brokers.postgres.EnqueueRequest') as request_class:
            _ = await self._send(broker, trace=carrier)
            _ = await self._send(broker)

        traced, untraced = request_class.call_args_list
        assert traced.kwargs['trace'] is carrier
        assert isinstance(untraced.kwargs['trace'], NoTraceCarrier)

    def test_a_blocking_send_carries_the_trace_of_the_send(self) -> None:
        broker = self._broker_answering(MagicMock(spec=Enqueued, task_id=SEND_TASK_ID))
        carrier = TraceCarrier({'traceparent': '00-ab-cd-01'}, bytes(range(1, 17)))
        with patch('horsies.core.brokers.postgres.EnqueueRequest') as request_class:
            result = broker.enqueue(
                'my_task',
                'default',
                task_id=SEND_TASK_ID,
                send_kind=SendKind.FIRST,
                sent_at=datetime.now(timezone.utc),
                trace=carrier,
            )

        assert result == Ok(SEND_TASK_ID)
        assert request_class.call_args.kwargs['trace'] is carrier

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('keywords', 'expected'),
        [
            pytest.param({'input_json': None}, {'input_json': '{}'}, id='no-input'),
            pytest.param({'input_json': 'null'}, {'input_json': '{}'}, id='null-input'),
            pytest.param(
                {'retention_class_key': None}, {'retention_class_key': 'forever'}, id='forever',
            ),
            pytest.param({}, {'rerun_input': 'APP_DEFAULT'}, id='rerun-default'),
            pytest.param(
                {'retain_rerun_input': True}, {'rerun_input': 'KEEP'}, id='rerun-keep',
            ),
        ],
    )
    async def test_the_request_holds_the_resolved_values(
        self, keywords: dict[str, Any], expected: dict[str, Any],
    ) -> None:
        broker = self._broker_answering(MagicMock(spec=Enqueued, task_id=SEND_TASK_ID))
        with patch('horsies.core.brokers.postgres.EnqueueRequest') as request_class:
            _ = await self._send(broker, **keywords)

        built = request_class.call_args.kwargs
        assert {key: built[key] for key in expected} == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('keywords', 'message'),
        [
            pytest.param({'input_json': '[1]'}, 'must be an object or null', id='not-an-object'),
            pytest.param({'input_json': '{bad'}, 'kwargs JSON corrupt', id='unreadable'),
            pytest.param(
                {
                    'enqueued_at': datetime.now(timezone.utc),
                    'enqueue_delay_seconds': 5,
                },
                'Cannot specify both',
                id='both-starts',
            ),
            pytest.param(
                {'sent_at': datetime(2026, 1, 1)}, 'sent_at must be timezone-aware', id='naive-sent-at',
            ),
            pytest.param(
                {'good_until': datetime(2026, 1, 1)},
                'good_until must be timezone-aware',
                id='naive-good-until',
            ),
            pytest.param(
                {'priority': 2**31}, 'priority must be between 1 and 100', id='priority-range',
            ),
            pytest.param(
                {'enqueue_delay_seconds': 2**63}, 'not a 64-bit integer', id='delay-range',
            ),
        ],
    )
    async def test_a_client_rule_refuses_before_the_core(
        self, keywords: dict[str, Any], message: str,
    ) -> None:
        broker = self._broker_answering(None)

        result = await self._send(broker, **keywords)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidInput)
        assert message in result.err_value.message
        broker._core_handle.enqueue_async.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('values', 'code', 'error_class', 'cause_class'),
        [
            pytest.param(
                {'input_json': '{bad'},
                DatabaseErrorCode.INVALID_INPUT,
                DatabaseInvalidInput,
                SerializationError,
                id='INVALID_INPUT-unreadable-json',
            ),
            pytest.param(
                {'sent_at': datetime.now(timezone.utc) + timedelta(minutes=10)},
                DatabaseErrorCode.INVALID_INPUT,
                DatabaseInvalidInput,
                NoCause,
                id='INVALID_INPUT-by-rule',
            ),
            pytest.param(
                {'priority': 0},
                DatabaseErrorCode.INVALID_INPUT,
                DatabaseInvalidInput,
                ValueError,
                id='INVALID_INPUT-by-value-rule',
            ),
            pytest.param(
                {'task_options_json': '{bad'},
                DatabaseErrorCode.INVALID_TASK_OPTIONS,
                DatabaseInvalidTaskOptions,
                SerializationError,
                id='INVALID_TASK_OPTIONS-unreadable-json',
            ),
            pytest.param(
                {'task_options_json': '{"retry_policy": "not_a_dict"}'},
                DatabaseErrorCode.INVALID_TASK_OPTIONS,
                DatabaseInvalidTaskOptions,
                NoCause,
                id='INVALID_TASK_OPTIONS-by-rule',
            ),
        ],
    )
    async def test_a_core_refusal_keeps_its_code_and_cause(
        self,
        values: dict[str, Any],
        code: DatabaseErrorCode,
        error_class: type,
        cause_class: type,
    ) -> None:
        """The class that the real core builds for a refused part and cause,
        returned by the facade as it is: the class and the code of the part,
        not retryable, and the cause of Python's broker of 0.7.0 with its
        text. The request is built directly, so the core also sees an input
        that the facade's own JSON check refuses first.

        No core refusal of the task options carries a rule's ``ValueError``:
        the version-1 command and the scoped key are rules of the input. The
        core test ``an_enqueue_refusal_has_the_code_of_its_part_and_python_s_cause``
        pins that combination of the constructor."""
        core_handle = _refusing_broker()._core_handle
        outcome = await core_handle.enqueue_async(_core_request(**values))
        broker = self._broker_answering(outcome)

        result = await self._send(broker)

        assert is_err(result)
        refused = result.err_value
        assert refused is outcome
        assert isinstance(refused, error_class)
        assert type(refused) is error_class
        assert refused.code is code
        assert refused.retryable is False
        assert isinstance(refused.exception, cause_class)
        assert type(refused.exception) is cause_class
        match refused.exception:
            case SerializationError() as unreadable:
                reader_error = str(unreadable)
                assert reader_error != ''
                assert refused.message.endswith(f': {reader_error}')
            case NoCause() as no_cause:
                assert no_cause == NoCause()
            case ValueError() as rule:
                assert str(rule) == 'priority must be between 1 and 100'
                assert refused.message == (
                    'enqueue input refused for my_task: priority must be between 1 and 100'
                )
            case other:
                raise AssertionError(f'unexpected cause {other!r}')

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'outcome',
        [
            pytest.param(
                DatabaseInvalidInput(
                    message='enqueue input refused for my_task: priority must be between 1 and 100',
                    cause=ValueError('priority must be between 1 and 100'),
                ),
                id='invalid-input',
            ),
            pytest.param(
                DatabaseInvalidTaskOptions(
                    message='enqueue contract violation: task_options JSON is refused: x',
                    cause=SerializationError('x'),
                ),
                id='invalid-task-options',
            ),
            pytest.param(
                DatabaseEnqueueFailed(
                    message='Failed to enqueue task my_task: connection lost',
                    retryable=True,
                    cause=CoreConnectionError('enqueue', 'connection lost', True),
                ),
                id='enqueue-failed-retryable',
            ),
            pytest.param(
                DatabaseEnqueueFailed(
                    message='task_id conflict detected but row disappeared',
                    retryable=False,
                    cause=NoCause(),
                ),
                id='enqueue-failed-not-retryable',
            ),
            pytest.param(DatabasePayloadMismatch(message='mismatch'), id='payload-mismatch'),
            pytest.param(
                DatabaseIdempotencyKeyConflict(
                    message='conflict',
                    owner_task_id=UUID('0198c0de-0000-7000-8000-0000000000ff'),
                ),
                id='key-conflict',
            ),
        ],
    )
    async def test_a_class_that_the_core_built_is_returned_as_it_is(
        self, outcome: object,
    ) -> None:
        """The core decides the code, the message and the cause of a failed
        send (``DatabaseOperationError::enqueue_error``); the facade adds
        nothing."""
        broker = self._broker_answering(outcome)

        result = await self._send(broker)

        assert is_err(result)
        assert result.err_value is outcome

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'outcome',
        [
            pytest.param(DatabaseAsyncContext(message='loop'), id='async-context'),
            pytest.param(DatabaseListenerDisabled(message='no listener'), id='listener-disabled'),
        ],
    )
    async def test_a_class_that_the_core_send_never_answers_is_a_defect(
        self, outcome: object,
    ) -> None:
        broker = self._broker_answering(outcome)

        with pytest.raises(AssertionError, match=type(outcome).__name__):
            await self._send(broker)

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_send_is_enqueue_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.enqueue_async', 65_536)
        handle = MagicMock()
        handle.enqueue_async = AsyncMock(side_effect=error)
        broker._core_handle = handle

        result = await self._send(broker)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseEnqueueFailed)
        assert result.err_value.retryable is True
        assert result.err_value.cause is error

    def test_the_blocking_form_sends_on_the_calling_thread(self) -> None:
        broker = self._broker_answering(MagicMock(spec=Enqueued, task_id=SEND_TASK_ID))
        threads: list[threading.Thread] = []
        broker._core_handle.enqueue = _recording_thread(
            threads, MagicMock(spec=Enqueued, task_id=SEND_TASK_ID),
        )

        result = broker.enqueue(
            'my_task',
            'default',
            task_id=SEND_TASK_ID,
            send_kind=SendKind.FIRST,
            input_json='{"x":1}',
            sent_at=datetime.now(timezone.utc),
        )

        assert result == Ok(SEND_TASK_ID)
        broker._core_handle.enqueue.assert_called_once()
        broker._core_handle.enqueue_async.assert_not_called()
        assert threads == [threading.current_thread()]


# ---------------------------------------------------------------------------
# TestGetRawResultRecordAsync — strict-serde phase 6 broker primitive
# ---------------------------------------------------------------------------


RESULT_TASK_ID = UUID('0198c0de-0000-7000-8000-000000000123')
RESULT_MISSING_ID = UUID('0198c0de-0000-7000-8000-000000000001')


@pytest.mark.unit
class TestGetRawResultRecordAsync:
    """get_raw_result_record_async maps the record of one core read to its result."""

    @staticmethod
    def _broker_answering(outcome: object) -> Any:
        broker = _make_broker()
        handle = MagicMock()
        handle.raw_result_record_async = AsyncMock(return_value=outcome)
        broker._core_handle = handle
        return broker

    @staticmethod
    def _record(status: str, result_json: str | None) -> MagicMock:
        return MagicMock(
            spec=RawResultRecordRecord,
            task_id=RESULT_TASK_ID,
            task_name='my_task',
            status=status,
            result_json=result_json,
        )

    @pytest.mark.asyncio
    async def test_an_absent_task_is_ok_none(self) -> None:
        broker = self._broker_answering(
            MagicMock(spec=RawResultAbsent, task_id=RESULT_MISSING_ID),
        )

        result = await broker.get_raw_result_record_async(RESULT_MISSING_ID)

        assert result == Ok(None)
        broker._core_handle.raw_result_record_async.assert_awaited_once_with(
            RESULT_MISSING_ID, timeout_ms=None,
        )

    @pytest.mark.asyncio
    async def test_a_finished_task_gives_its_envelope(self) -> None:
        envelope = '{"__h_task_result__":true,"ok":7,"err":null}'
        broker = self._broker_answering(self._record('COMPLETED', envelope))

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID, timeout_ms=5_000)

        assert result == Ok(RawResultRecord(
            task_id=RESULT_TASK_ID,
            task_name='my_task',
            status=TaskStatus.COMPLETED,
            raw_result={'__h_task_result__': True, 'ok': 7, 'err': None},
        ))
        broker._core_handle.raw_result_record_async.assert_awaited_once_with(
            RESULT_TASK_ID, timeout_ms=5_000,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', ['CANCELLED', 'RUNNING'])
    async def test_a_record_without_a_result_has_no_payload(self, status: str) -> None:
        broker = self._broker_answering(self._record(status, None))

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID, timeout_ms=10)

        assert is_ok(result)
        record = result.ok_value
        assert record is not None
        assert record.status is TaskStatus(status)
        assert record.raw_result is None

    @pytest.mark.asyncio
    async def test_a_stored_result_that_is_not_an_object_is_invalid_json_payload(
        self,
    ) -> None:
        broker = self._broker_answering(self._record('COMPLETED', '[1]'))

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidJsonPayload)
        assert isinstance(result.err_value.cause, NoCause)

    @pytest.mark.asyncio
    async def test_an_empty_stored_result_is_invalid_json_payload(self) -> None:
        broker = self._broker_answering(self._record('COMPLETED', ''))

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidJsonPayload)
        assert isinstance(result.err_value.cause, EmptyJsonTextError)
        assert result.err_value.message == (
            f'Result JSON parse failed for task {RESULT_TASK_ID}: '
            'JSON parse failed: the text is empty'
        )

    @pytest.mark.asyncio
    async def test_corrupt_stored_data_found_by_the_core_is_invalid_json_payload(
        self,
    ) -> None:
        broker = self._broker_answering(
            DatabaseInvalidJsonPayload(message='Result archive digest mismatch', cause=_DIGEST_MISMATCH),
        )

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID)

        assert is_err(result)
        failed = result.err_value
        assert isinstance(failed, DatabaseInvalidJsonPayload)
        assert 'digest mismatch' in failed.message
        assert failed.cause is _DIGEST_MISMATCH

    @pytest.mark.asyncio
    async def test_a_failed_read_is_task_info_query_failed_and_logged(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = self._broker_answering(
            DatabaseOperationFailed(code=DatabaseErrorCode.TASK_INFO_QUERY_FAILED, message='connection lost', retryable=True, cause=_CONNECTION_LOST),
        )

        with caplog.at_level('ERROR', logger='horsies.broker'):
            result = await broker.get_raw_result_record_async(RESULT_TASK_ID)

        assert is_err(result)
        failed = result.err_value
        assert isinstance(failed, DatabaseOperationFailed)
        assert failed.code is DatabaseErrorCode.TASK_INFO_QUERY_FAILED
        assert failed.retryable is True
        assert failed.cause is _CONNECTION_LOST
        assert 'connection lost' in caplog.text

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_read_is_task_info_query_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.raw_result_record_async', 65_536)
        handle = MagicMock()
        handle.raw_result_record_async = AsyncMock(side_effect=error)
        broker._core_handle = handle

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID)

        assert is_err(result)
        assert result.err_value.code is DatabaseErrorCode.TASK_INFO_QUERY_FAILED
        assert result.err_value.retryable is True
        assert result.err_value.exception is error

    @pytest.mark.asyncio
    async def test_a_refused_limit_is_invalid_input_before_the_core(self) -> None:
        broker = self._broker_answering(None)

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID, timeout_ms=-1)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidInput)
        broker._core_handle.raw_result_record_async.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_another_failure_code_is_a_defect(self) -> None:
        broker = self._broker_answering(
            DatabaseInvalidInput(message='x', cause=NoCause()),
        )

        with pytest.raises(AssertionError, match='INVALID_INPUT'):
            await broker.get_raw_result_record_async(RESULT_TASK_ID)

    @pytest.mark.asyncio
    async def test_a_cancel_of_the_caller_propagates(self) -> None:
        broker = _make_broker()
        handle = MagicMock()
        handle.raw_result_record_async = AsyncMock(side_effect=asyncio.CancelledError)
        broker._core_handle = handle

        with pytest.raises(asyncio.CancelledError):
            await broker.get_raw_result_record_async(RESULT_TASK_ID)

    def test_the_blocking_form_runs_the_blocking_core_read(self) -> None:
        broker = _make_broker()
        threads: list[threading.Thread] = []
        handle = MagicMock()
        handle.raw_result_record = _recording_thread(
            threads, MagicMock(spec=RawResultAbsent, task_id=RESULT_TASK_ID),
        )
        broker._core_handle = handle

        result = broker.get_raw_result_record(RESULT_TASK_ID, timeout_ms=5_000)

        assert result == Ok(None)
        handle.raw_result_record.assert_called_once_with(RESULT_TASK_ID, timeout_ms=5_000)
        handle.raw_result_record_async.assert_not_called()
        assert threads == [threading.current_thread()]


# ---------------------------------------------------------------------------
# TestHealthApi
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPingDatabase:
    """ping_database_async maps the record of one core ping to its result."""

    @staticmethod
    def _handle(outcome: object) -> MagicMock:
        """A core handle whose ping answers ``outcome``."""
        handle = MagicMock()
        handle.ping_database_async = AsyncMock(return_value=outcome)
        return handle

    @pytest.mark.asyncio
    async def test_returns_the_latency_of_the_core_ping(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(MagicMock(spec=DatabasePingRecord, latency_ms=1.5))

        result = await broker.ping_database_async()

        assert result == Ok(DatabasePing(latency_ms=1.5))
        broker._core_handle.ping_database_async.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_a_failed_ping_is_db_ping_failed_with_the_core_classification(self) -> None:
        """The core's class is returned as it is: the core builds Python's
        message (``ping_database failed: {error}``)."""
        core_failure = DatabaseOperationFailed(
            code=DatabaseErrorCode.DATABASE_PING_FAILED,
            message='ping_database failed: connection failed: refused',
            retryable=True,
            cause=_CONNECTION_LOST,
        )
        broker = _make_broker()
        broker._core_handle = self._handle(core_failure)

        result = await broker.ping_database_async()

        assert is_err(result)
        failed = result.err_value
        assert failed is core_failure
        assert failed.code is DatabaseErrorCode.DATABASE_PING_FAILED
        assert failed.retryable is True
        assert failed.message == 'ping_database failed: connection failed: refused'
        assert failed.exception is _CONNECTION_LOST

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_ping_is_db_ping_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.ping_database_async', 65_536)
        handle = MagicMock()
        handle.ping_database_async = AsyncMock(side_effect=error)
        broker._core_handle = handle

        result = await broker.ping_database_async()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.DATABASE_PING_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.asyncio
    async def test_another_failure_code_is_a_defect(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            DatabaseOperationFailed(code=DatabaseErrorCode.CLEANUP_FAILED, message='x', retryable=False, cause=CoreConnectionError('test_operation', 'x', True)),
        )

        with pytest.raises(AssertionError, match='CLEANUP_FAILED'):
            await broker.ping_database_async()


@pytest.mark.unit
class TestPingWorkers:
    """ping_workers_async with no listener: an error result, not a raise."""

    @pytest.mark.asyncio
    async def test_disabled_listener_returns_err_not_raise(self) -> None:
        """A broker with no listener must Err, not raise (Result contract)."""
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        config = PostgresConfig(
            database_url='postgresql://u:p@localhost/db'
        )
        broker = PostgresBroker(config, assume_initialized=True)

        result = await broker.ping_workers_async(timeout_seconds=1.0)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseListenerDisabled)
        assert result.err_value.code is DatabaseErrorCode.LISTENER_DISABLED


# ---------------------------------------------------------------------------
# TestMarkStaleTasksAsFailed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMarkStaleTasksAsFailed:
    """mark_stale_tasks_as_failed maps the record of one core pass to its result."""

    @staticmethod
    def _handle(answer: object) -> MagicMock:
        """A core handle whose stale RUNNING pass answers ``answer``: a
        record, or an exception that the call raises."""
        handle = MagicMock()
        match answer:
            case BaseException():
                handle.mark_stale_running_async = AsyncMock(side_effect=answer)
            case _:
                handle.mark_stale_running_async = AsyncMock(return_value=answer)
        return handle

    @pytest.mark.asyncio
    async def test_a_pass_returns_the_recovered_count(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=StaleRunningRecovered, recovered=3),
        )

        result = await broker.mark_stale_tasks_as_failed(
            stale_threshold_ms=300_000,
            finalizing_stale_threshold_ms=60_000,
        )

        assert result == Ok(3)
        broker._core_handle.mark_stale_running_async.assert_awaited_once_with(
            300_000, 60_000,
        )

    @pytest.mark.asyncio
    async def test_a_full_scan_batch_warns_that_the_rest_waits(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=StaleRunningScanFull, recovered=7, scan_batch=1000),
        )

        with caplog.at_level('WARNING', logger='horsies.broker'):
            result = await broker.mark_stale_tasks_as_failed()

        assert result == Ok(7)
        assert 'read a full scan batch of 1000 candidates' in caplog.text

    @pytest.mark.asyncio
    async def test_a_failed_scan_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=StaleRunningScanFailed,
                message='stale scan: connection lost',
                retryable=True,
                exception=_CONNECTION_LOST,
            ),
        )

        result = await broker.mark_stale_tasks_as_failed()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.message == 'mark_stale_tasks_as_failed failed: stale scan: connection lost'
        assert failed.exception is _CONNECTION_LOST

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_pass_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.mark_stale_running_async', 65_536)
        broker._core_handle = self._handle(error)

        result = await broker.mark_stale_tasks_as_failed()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.parametrize(
        ('stale_ms', 'finalizing_ms'),
        [(SIGNED_64_BIT_MAX + 1, 60_000), (60_000, SIGNED_64_BIT_MIN - 1)],
        ids=['stale', 'finalizing'],
    )
    @pytest.mark.asyncio
    async def test_a_threshold_past_64_bits_is_refused_before_the_core(
        self, stale_ms: int, finalizing_ms: int,
    ) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(MagicMock(spec=StaleRunningRecovered, recovered=0))

        result = await broker.mark_stale_tasks_as_failed(
            stale_threshold_ms=stale_ms,
            finalizing_stale_threshold_ms=finalizing_ms,
        )

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is False
        assert isinstance(failed.exception, OverflowError)
        broker._core_handle.mark_stale_running_async.assert_not_awaited()


@pytest.mark.unit
class TestExpirePendingTasks:
    """expire_pending_tasks maps the record of one core pass to its result."""

    @staticmethod
    def _handle(answer: object) -> MagicMock:
        """A core handle whose pending expiry pass answers ``answer``: a
        record, or an exception that the call raises."""
        handle = MagicMock()
        match answer:
            case BaseException():
                handle.expire_pending_tasks_async = AsyncMock(side_effect=answer)
            case _:
                handle.expire_pending_tasks_async = AsyncMock(return_value=answer)
        return handle

    @pytest.mark.parametrize('expired', [0, 3])
    @pytest.mark.asyncio
    async def test_a_drained_pass_returns_the_expired_count(self, expired: int) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(MagicMock(spec=PendingExpiryDrained, expired=expired))

        result = await broker.expire_pending_tasks()

        assert result == Ok(expired)
        broker._core_handle.expire_pending_tasks_async.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_a_pass_at_its_bound_warns_that_the_rest_waits(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=PendingExpiryBoundReached, expired=100_000),
        )

        with caplog.at_level('WARNING', logger='horsies.broker'):
            result = await broker.expire_pending_tasks()

        assert result == Ok(100_000)
        assert 'reached the per-pass batch cap after expiring 100000 task(s)' in caplog.text

    @pytest.mark.asyncio
    async def test_a_failed_pass_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=PendingExpiryFailed,
                expired_before=500,
                message='pending expiry: connection lost',
                retryable=True,
                exception=_CONNECTION_LOST,
            ),
        )

        result = await broker.expire_pending_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.message == 'expire_pending_tasks failed: pending expiry: connection lost'
        assert failed.exception is _CONNECTION_LOST

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_pass_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.expire_pending_tasks_async', 65_536)
        broker._core_handle = self._handle(error)

        result = await broker.expire_pending_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is error


# ---------------------------------------------------------------------------
# TestTerminateOrphanedWorkflowTasks
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTerminateOrphanedWorkflowTasks:
    """The orphan audit and the orphan termination map the record of one
    core page to their results."""

    @staticmethod
    def _handle(answer: object) -> MagicMock:
        """A core handle whose orphan page answers ``answer``: a record, or
        an exception that the call raises."""
        handle = MagicMock()
        match answer:
            case BaseException():
                handle.audit_orphaned_workflow_tasks_async = AsyncMock(side_effect=answer)
            case _:
                handle.audit_orphaned_workflow_tasks_async = AsyncMock(return_value=answer)
        return handle

    @staticmethod
    def _page(rows_selected: int, candidates_returned: int) -> MagicMock:
        return MagicMock(
            spec=OrphanPageAudited,
            rows_selected=rows_selected,
            candidates_returned=candidates_returned,
            cancelled=candidates_returned,
        )

    @pytest.mark.parametrize('cancelled', [0, 3])
    @pytest.mark.asyncio
    async def test_terminate_returns_the_cancelled_count_of_one_page(
        self, cancelled: int,
    ) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(self._page(cancelled, cancelled))

        result = await broker.terminate_orphaned_workflow_tasks()

        assert result == Ok(cancelled)
        broker._core_handle.audit_orphaned_workflow_tasks_async.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_audit_reports_cursor_metrics(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(self._page(500, 2))

        result = await broker.audit_orphaned_workflow_tasks()

        assert is_ok(result)
        report = result.ok_value
        assert report.rows_selected == 500
        assert report.candidates_returned == 2
        assert report.cancelled == 2
        assert report.duration_ms >= 0
        assert report.health()['state'] == 'ready'

    @pytest.mark.asyncio
    async def test_a_held_cursor_is_a_refusal_with_its_sqlstate(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=OrphanAuditLockBusy,
                message='orphan page: lock not available',
                retryable=True,
                exception=_LOCK_NOT_AVAILABLE,
            ),
        )

        result = await broker.audit_orphaned_workflow_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is _LOCK_NOT_AVAILABLE
        assert isinstance(failed.exception, CoreServerError)
        assert failed.exception.sqlstate == '55P03'

    @pytest.mark.asyncio
    async def test_a_failed_page_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=OrphanAuditFailed,
                message='horsies_cancel_orphaned_tasks: the cursor candidate count 2 differs',
                retryable=False,
                exception=_COUNT_DIFFERS,
            ),
        )

        result = await broker.audit_orphaned_workflow_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is False
        assert failed.message.startswith('audit_orphaned_workflow_tasks failed: ')
        assert failed.exception is _COUNT_DIFFERS
        assert not isinstance(failed.exception, CoreServerError)

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_page_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.audit_orphaned_workflow_tasks_async', 65_536)
        broker._core_handle = self._handle(error)

        result = await broker.audit_orphaned_workflow_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.asyncio
    async def test_terminate_a_held_cursor_is_a_refusal_with_its_sqlstate(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=OrphanAuditLockBusy,
                message='orphan page: lock not available',
                retryable=True,
                exception=_LOCK_NOT_AVAILABLE,
            ),
        )

        result = await broker.terminate_orphaned_workflow_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.message == (
            'terminate_orphaned_workflow_tasks failed: orphan page: lock not available'
        )
        assert failed.exception is _LOCK_NOT_AVAILABLE
        assert isinstance(failed.exception, CoreServerError)
        assert failed.exception.sqlstate == '55P03'

    @pytest.mark.asyncio
    async def test_terminate_a_failed_page_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=OrphanAuditFailed,
                message='horsies_cancel_orphaned_tasks: the cursor candidate count 2 differs',
                retryable=False,
                exception=_COUNT_DIFFERS,
            ),
        )

        result = await broker.terminate_orphaned_workflow_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is False
        assert failed.message == (
            'terminate_orphaned_workflow_tasks failed: '
            'horsies_cancel_orphaned_tasks: the cursor candidate count 2 differs'
        )
        assert failed.exception is _COUNT_DIFFERS

    @pytest.mark.asyncio
    async def test_terminate_a_core_that_cannot_run_the_page_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.audit_orphaned_workflow_tasks_async', 65_536)
        broker._core_handle = self._handle(error)

        result = await broker.terminate_orphaned_workflow_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is error


# ---------------------------------------------------------------------------
# TestRequeueStaleClaimed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRequeueStaleClaimed:
    """requeue_stale_claimed maps the record of one core pass to its result."""

    @staticmethod
    def _handle(outcome: object) -> MagicMock:
        """A core handle whose requeue pass answers ``outcome``."""
        handle = MagicMock()
        handle.requeue_stale_claimed_async = AsyncMock(return_value=outcome)
        return handle

    @pytest.mark.asyncio
    async def test_a_drained_pass_returns_its_count(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=StaleClaimsDrained, requeued=5),
        )

        result = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)

        assert result == Ok(5)
        broker._core_handle.requeue_stale_claimed_async.assert_awaited_once_with(120_000)

    @pytest.mark.asyncio
    async def test_a_pass_at_the_batch_cap_returns_its_count_with_a_warning(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=StaleClaimsBoundReached, requeued=100_000),
        )

        with caplog.at_level('WARNING', logger='horsies.broker'):
            result = await broker.requeue_stale_claimed()

        assert result == Ok(100_000)
        assert 'reached the per-pass batch cap' in caplog.text

    @pytest.mark.asyncio
    async def test_a_failed_pass_is_cleanup_failed_with_the_rows_before_it(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=StaleClaimsRequeueFailed,
                requeued_before=7,
                message='requeue batch: connection lost',
                retryable=True,
                exception=_CONNECTION_LOST,
            ),
        )

        result = await broker.requeue_stale_claimed()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.message == (
            'requeue_stale_claimed failed after requeueing 7 task(s): '
            'requeue batch: connection lost'
        )
        assert failed.exception is _CONNECTION_LOST

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_pass_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.requeue_stale_claimed_async', 65_536)
        handle = MagicMock()
        handle.requeue_stale_claimed_async = AsyncMock(side_effect=error)
        broker._core_handle = handle

        result = await broker.requeue_stale_claimed()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.asyncio
    async def test_a_threshold_past_64_bits_is_refused_before_the_core(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(MagicMock(spec=StaleClaimsDrained, requeued=0))

        result = await broker.requeue_stale_claimed(
            stale_threshold_ms=SIGNED_64_BIT_MAX + 1,
        )

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.CLEANUP_FAILED
        assert failed.retryable is False
        assert isinstance(failed.exception, OverflowError)
        broker._core_handle.requeue_stale_claimed_async.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestSyncFacades
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSyncFacades:
    """The sync forms: the blocking core reads."""

    def test_get_task_info_runs_the_blocking_core_read(self) -> None:
        """get_task_info() calls CoreHandle.task_info on this thread."""
        broker = _make_broker()
        threads: list[threading.Thread] = []
        handle = MagicMock()
        handle.task_info = _recording_thread(threads, MagicMock(spec=TaskInfoNotFound))
        broker._core_handle = handle

        result = broker.get_task_info(
            TASK_INFO_ID,
            include_result=True,
            include_failed_reason=True,
        )

        assert result == Ok(None)
        handle.task_info.assert_called_once_with(
            TASK_INFO_ID,
            include_result=True,
            include_failed_reason=True,
            include_attempts=False,
        )
        handle.task_info_async.assert_not_called()
        assert threads == [threading.current_thread()]


# ---------------------------------------------------------------------------
# TestGetTaskInfoAsync
# ---------------------------------------------------------------------------


TASK_INFO_ID = UUID('0198c0de-0000-7000-8000-0000000000ab')


def _task_info_record(**changes: Any) -> MagicMock:
    """A core task info record of a RUNNING live task; ``changes`` overrides fields."""
    now = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)
    fields: dict[str, Any] = {
        'task_id': TASK_INFO_ID,
        'task_name': 'compute',
        'status': 'RUNNING',
        'queue_name': 'default',
        'priority': 100,
        'retry_count': 1,
        'max_retries': 3,
        'next_retry_at': None,
        'sent_at': now,
        'enqueued_at': now,
        'claimed_at': now,
        'started_at': now,
        'completed_at': None,
        'failed_at': None,
        'worker_hostname': 'host-1',
        'worker_pid': 9999,
        'worker_process_name': 'worker-0',
        'error_code': None,
        'failed_reason': None,
        'result_json': None,
        'attempts': None,
        'execution': 'ISOLATED',
    }
    fields.update(changes)
    return MagicMock(spec=TaskInfoRecord, **fields)


def _core_attempt_record(attempt: int, outcome: str, will_retry: bool) -> MagicMock:
    at = datetime(2026, 11, 17, 12, attempt, tzinfo=timezone.utc)
    return MagicMock(
        spec=TaskAttemptRecord,
        task_id=TASK_INFO_ID,
        attempt=attempt,
        outcome=outcome,
        will_retry=will_retry,
        started_at=at,
        finished_at=at,
        error_code=None,
        error_message=None,
        failed_reason=None,
        worker_id='w-1',
        worker_hostname='host-1',
        worker_pid=100,
        worker_process_name=None,
    )


@pytest.mark.unit
class TestGetTaskInfoAsync:
    """get_task_info_async maps the record of one core read to its result."""

    @staticmethod
    def _broker_answering(outcome: object) -> Any:
        broker = _make_broker()
        handle = MagicMock()
        handle.task_info_async = AsyncMock(return_value=outcome)
        broker._core_handle = handle
        return broker

    @pytest.mark.asyncio
    async def test_task_not_found_returns_none(self) -> None:
        broker = self._broker_answering(MagicMock(spec=TaskInfoNotFound, task_id=TASK_INFO_ID))

        result = await broker.get_task_info_async(TASK_INFO_ID, include_attempts=True)

        assert result == Ok(None)
        broker._core_handle.task_info_async.assert_awaited_once_with(
            TASK_INFO_ID,
            include_result=False,
            include_failed_reason=False,
            include_attempts=True,
        )

    @pytest.mark.asyncio
    async def test_a_live_record_gives_its_fields(self) -> None:
        broker = self._broker_answering(_task_info_record())

        result = await broker.get_task_info_async(TASK_INFO_ID)

        assert is_ok(result)
        info = result.ok_value
        assert isinstance(info, TaskInfo)
        assert info.task_id == TASK_INFO_ID
        assert info.task_name == 'compute'
        assert info.status is TaskStatus.RUNNING
        assert (info.priority, info.retry_count, info.max_retries) == (100, 1, 3)
        assert (info.worker_hostname, info.worker_pid) == ('host-1', 9999)
        assert info.worker_process_name == 'worker-0'
        assert info.raw_result is None
        assert info.decoded_result is None
        assert info.result_decoded is False
        assert info.failed_reason is None
        assert info.attempts is None
        assert info.execution is Execution.ISOLATED

    @pytest.mark.asyncio
    async def test_a_history_record_has_no_execution_class(self) -> None:
        broker = self._broker_answering(
            _task_info_record(status='FAILED', execution=None, failed_reason='boom'),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID, include_failed_reason=True)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.status is TaskStatus.FAILED
        assert info.execution is None
        assert info.failed_reason == 'boom'

    @pytest.mark.asyncio
    async def test_the_stored_envelope_is_the_raw_result(self) -> None:
        envelope = '{"__h_task_result__":true,"ok":"hello","err":null}'
        broker = self._broker_answering(
            _task_info_record(status='COMPLETED', result_json=envelope),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID, include_result=True)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.raw_result == {'__h_task_result__': True, 'ok': 'hello', 'err': None}
        assert info.decoded_result is None
        assert info.result_decoded is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('result_json', 'exception_class'),
        [
            pytest.param('{"x": NaN}', SerializationError, id='non-finite'),
            pytest.param('[1, 2]', NoCause, id='not-an-object'),
        ],
    )
    async def test_a_stored_result_that_does_not_read_is_invalid_json_payload(
        self, result_json: str, exception_class: type,
    ) -> None:
        broker = self._broker_answering(
            _task_info_record(status='COMPLETED', result_json=result_json),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID, include_result=True)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidJsonPayload)
        assert isinstance(result.err_value.cause, exception_class)

    @pytest.mark.asyncio
    async def test_an_empty_stored_result_is_invalid_json_payload(self) -> None:
        broker = self._broker_answering(
            _task_info_record(status='COMPLETED', result_json=''),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID, include_result=True)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidJsonPayload)
        assert isinstance(result.err_value.cause, EmptyJsonTextError)
        assert result.err_value.message == (
            f'Result JSON parse failed for task {TASK_INFO_ID}: '
            'JSON parse failed: the text is empty'
        )

    @pytest.mark.asyncio
    async def test_attempts_keep_the_core_order(self) -> None:
        broker = self._broker_answering(
            _task_info_record(
                attempts=[
                    _core_attempt_record(2, 'FAILED', False),
                    _core_attempt_record(1, 'WORKER_FAILURE', True),
                ],
            ),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID, include_attempts=True)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.attempts is not None
        assert [attempt.attempt for attempt in info.attempts] == [2, 1]
        assert [attempt.outcome for attempt in info.attempts] == [
            TaskAttemptOutcome.FAILED,
            TaskAttemptOutcome.WORKER_FAILURE,
        ]
        assert info.attempts[0].worker_id == 'w-1'

    @pytest.mark.asyncio
    async def test_a_failed_read_is_task_info_query_failed(self) -> None:
        """The core's class is returned as it is: the core builds Python's
        message (``get_task_info failed for {task_id}: {error}``)."""
        core_failure = DatabaseOperationFailed(
            code=DatabaseErrorCode.TASK_INFO_QUERY_FAILED,
            message=f'get_task_info failed for {TASK_INFO_ID}: pool timed out',
            retryable=True,
            cause=_CONNECTION_LOST,
        )
        broker = self._broker_answering(core_failure)

        result = await broker.get_task_info_async(TASK_INFO_ID)

        assert is_err(result)
        failed = result.err_value
        assert failed is core_failure
        assert isinstance(failed, DatabaseOperationFailed)
        assert failed.code is DatabaseErrorCode.TASK_INFO_QUERY_FAILED
        assert failed.retryable is True
        assert failed.message == f'get_task_info failed for {TASK_INFO_ID}: pool timed out'
        assert failed.cause is _CONNECTION_LOST

    @pytest.mark.asyncio
    async def test_corrupt_stored_data_found_by_the_core_is_invalid_json_payload(
        self,
    ) -> None:
        broker = self._broker_answering(
            DatabaseInvalidJsonPayload(message='stored result digest mismatch', cause=_DIGEST_MISMATCH),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID, include_result=True)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidJsonPayload)
        assert result.err_value.cause is _DIGEST_MISMATCH

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_read_is_task_info_query_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.task_info_async', 65_536)
        handle = MagicMock()
        handle.task_info_async = AsyncMock(side_effect=error)
        broker._core_handle = handle

        result = await broker.get_task_info_async(TASK_INFO_ID)

        assert is_err(result)
        assert result.err_value.code is DatabaseErrorCode.TASK_INFO_QUERY_FAILED
        assert result.err_value.retryable is True
        assert result.err_value.exception is error

    @pytest.mark.asyncio
    async def test_another_failure_code_is_a_defect(self) -> None:
        broker = self._broker_answering(
            DatabaseOperationFailed(code=DatabaseErrorCode.CLEANUP_FAILED, message='x', retryable=False, cause=CoreConnectionError('test_operation', 'x', True)),
        )

        with pytest.raises(AssertionError, match='CLEANUP_FAILED'):
            await broker.get_task_info_async(TASK_INFO_ID)


# ---------------------------------------------------------------------------
# TestClose
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestClose:
    """close (sync): the core handle's blocking close on the calling thread.
    The outcomes equal close_async's
    (``tests/unit/test_sync_facade_blocking_forms.py``)."""

    @staticmethod
    def _broker_closing(close: MagicMock) -> Any:
        broker = _make_broker()
        handle = MagicMock()
        handle.close = close
        broker._core_handle = handle
        return broker

    def test_close_calls_the_blocking_core_close(self) -> None:
        """close() calls CoreHandle.close on this thread, never close_async."""
        threads: list[threading.Thread] = []
        broker = self._broker_closing(
            _recording_thread(threads, MagicMock(spec=HandleClosed)),
        )

        result = broker.close()

        assert result == Ok(None)
        broker._core_handle.close.assert_called_once_with()
        broker._core_handle.close_async.assert_not_called()
        assert threads == [threading.current_thread()]

    def test_a_core_that_cannot_run_the_close_is_close_failed(self) -> None:
        """A core boundary error of the blocking close is CLOSE_FAILED with the error."""
        refused = CoreStartFailed('the core runtime could not start')
        broker = self._broker_closing(MagicMock(side_effect=refused))

        result = broker.close()

        assert is_err(result)
        assert result.err_value.code == DatabaseErrorCode.CLOSE_FAILED
        assert result.err_value.exception is refused

    def test_an_exception_that_is_not_a_core_error_propagates(self) -> None:
        """An exception that is not a CoreError propagates unchanged."""
        defect = RuntimeError('boom')
        broker = self._broker_closing(MagicMock(side_effect=defect))

        with pytest.raises(RuntimeError) as exc_info:
            broker.close()

        assert exc_info.value is defect


# ---------------------------------------------------------------------------
# New error path tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnsureSchemaInitialized:
    """ensure_schema_initialized maps the record of the core install to its result."""

    @staticmethod
    def _uninitialized(outcome: object) -> Any:
        """A broker that has not installed, whose core install answers ``outcome``."""
        broker = _make_broker()
        broker._initialized = False
        handle = MagicMock()
        handle.install_schema_async = AsyncMock(return_value=outcome)
        broker._core_handle = handle
        return broker

    @pytest.mark.asyncio
    async def test_an_initialized_broker_does_not_call_the_core(self) -> None:
        broker = _make_broker()
        handle = MagicMock()
        handle.install_schema_async = AsyncMock()
        broker._core_handle = handle

        result = await broker.ensure_schema_initialized()

        assert result == Ok(None)
        handle.install_schema_async.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('outcome_class', [SchemaInstalled, SchemaInstallNotRun])
    async def test_an_install_is_ok_once_per_broker(self, outcome_class: type) -> None:
        broker = self._uninitialized(MagicMock(spec=outcome_class))

        first = await broker.ensure_schema_initialized()
        second = await broker.ensure_schema_initialized()

        assert (first, second) == (Ok(None), Ok(None))
        broker._core_handle.install_schema_async.assert_awaited_once_with()
        assert broker._initialized is True

    @pytest.mark.asyncio
    async def test_a_failed_install_is_schema_init_failed_and_runs_again(self) -> None:
        broker = self._uninitialized(
            DatabaseOperationFailed(code=DatabaseErrorCode.SCHEMA_INIT_FAILED, message='the database was installed by the RUST client', retryable=False, cause=_FAMILY_MISMATCH),
        )

        first = await broker.ensure_schema_initialized()
        second = await broker.ensure_schema_initialized()

        assert is_err(first)
        failed = first.err_value
        assert failed.code is DatabaseErrorCode.SCHEMA_INIT_FAILED
        assert failed.retryable is False
        assert 'installed by the RUST client' in failed.message
        assert failed.exception is _FAMILY_MISMATCH
        assert is_err(second)
        assert broker._core_handle.install_schema_async.await_count == 2
        assert broker._initialized is False

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_install_is_schema_init_failed(self) -> None:
        broker = self._uninitialized(None)
        error = TooManyPendingCalls('CoreHandle.install_schema_async', 65_536)
        broker._core_handle.install_schema_async = AsyncMock(side_effect=error)

        result = await broker.ensure_schema_initialized()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is DatabaseErrorCode.SCHEMA_INIT_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.asyncio
    async def test_another_failure_code_is_a_defect(self) -> None:
        broker = self._uninitialized(
            DatabaseOperationFailed(code=DatabaseErrorCode.DATABASE_PING_FAILED, message='x', retryable=False, cause=CoreConnectionError('test_operation', 'x', True)),
        )

        with pytest.raises(AssertionError, match='DATABASE_PING_FAILED'):
            await broker.ensure_schema_initialized()


def _assert_refused_with_cause(
    result: Any, *, message: str, cause_type: type[BaseException], cause_text: str,
) -> None:
    """The outcome of Python's broker for a value that a rule refused:
    INVALID_INPUT, not retryable, with ``message`` and a cause of exactly
    ``cause_type`` whose text is ``cause_text``."""
    match result:
        case Err(DatabaseInvalidInput(message=refused_message, cause=cause) as refused):
            assert refused.code is DatabaseErrorCode.INVALID_INPUT
            assert refused.retryable is False
            assert refused_message == message
            assert type(cause) is cause_type, repr(cause)
            assert str(cause) == cause_text
        case other:
            raise AssertionError(f'expected INVALID_INPUT, got {other!r}')


REFUSED_KWARGS = ['5', '"a"', '[1]', 'true', '0', 'false', '""', '[]', '']


@pytest.mark.unit
class TestEnqueueInputRefusal:
    """A refused input is refused before the database, with its own code.

    The send runs on the real core handle with no server
    (``_refusing_broker``): a send that reached the database would fail as
    ENQUEUE_FAILED, so a refusal also shows that nothing was written."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize('input_json', REFUSED_KWARGS)
    async def test_kwargs_shape_is_refused(self, input_json: str) -> None:
        result = await _refusing_broker().enqueue_async(
            'my_task', task_id=SEND_TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), input_json=input_json,
        )

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidInput)
        assert 'kwargs JSON' in result.err_value.message

    @pytest.mark.asyncio
    async def test_shape_refusal_text_holds_no_input_value(self) -> None:
        result = await _refusing_broker().enqueue_async(
            'my_task', task_id=SEND_TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), input_json='"secret-value"',
        )

        assert is_err(result)
        assert 'secret-value' not in result.err_value.message

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('overrides', 'message', 'rule'),
        [
            pytest.param(
                {'task_name': ''},
                'enqueue input refused for : task_name must be non-empty',
                'task_name must be non-empty',
                id='empty-task-name',
            ),
            pytest.param(
                {'queue_name': ''},
                'enqueue input refused for my_task: queue_name must be non-empty',
                'queue_name must be non-empty',
                id='empty-queue-name',
            ),
            pytest.param(
                {'priority': 0},
                'enqueue input refused for my_task: priority must be between 1 and 100',
                'priority must be between 1 and 100',
                id='priority-below',
            ),
            pytest.param(
                {'priority': 101},
                'enqueue input refused for my_task: priority must be between 1 and 100',
                'priority must be between 1 and 100',
                id='priority-above',
            ),
            pytest.param(
                {'priority': 2**31},
                'enqueue input refused for my_task: priority must be between 1 and 100',
                'priority must be between 1 and 100',
                id='priority-beyond-32-bits',
            ),
            pytest.param(
                {'good_until': datetime(2030, 1, 1)},
                'enqueue input refused for my_task: good_until must be timezone-aware',
                'good_until must be timezone-aware',
                id='naive-good-until',
            ),
            pytest.param(
                {'enqueue_delay_seconds': -1},
                'enqueue input refused for my_task: enqueue_delay_seconds must be non-negative',
                'enqueue_delay_seconds must be non-negative',
                id='negative-delay',
            ),
            pytest.param(
                {'enqueue_delay_seconds': SIGNED_64_BIT_MIN - 1},
                'enqueue input refused for my_task: enqueue_delay_seconds must be non-negative',
                'enqueue_delay_seconds must be non-negative',
                id='delay-below-64-bits',
            ),
            pytest.param(
                {'retention_class_key': ''},
                'enqueue input refused for my_task: retention_class_key must be non-empty',
                'retention_class_key must be non-empty',
                id='empty-retention-key',
            ),
        ],
    )
    async def test_command_rule_is_refused_with_its_cause(
        self, overrides: dict[str, Any], message: str, rule: str,
    ) -> None:
        """Python's ``EnqueueCommandV1`` refuses each value with a
        ``ValueError`` of its rule (the command fingerprint of the Python
        send of 0.7.0)."""
        values: dict[str, Any] = {
            'task_name': 'my_task',
            'queue_name': 'default',
            'sent_at': datetime.now(timezone.utc),
        }
        values.update(overrides)

        result = await _refusing_broker().enqueue_async(
            task_id=SEND_TASK_ID, send_kind=SendKind.FIRST, **values,
        )

        _assert_refused_with_cause(
            result, message=message, cause_type=ValueError, cause_text=rule,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('key', 'cause_type', 'cause_text'),
        [
            pytest.param('', ValueError, 'idempotency key must be non-empty', id='empty'),
            pytest.param(
                'k' * 256,
                ValueError,
                'idempotency key must be at most 255 UTF-8 bytes',
                id='over-the-byte-bound',
            ),
            pytest.param(
                '\ud800',
                UnicodeEncodeError,
                "'utf-8' codec can't encode character '\\ud800' in position 0: surrogates not allowed",
                id='not-utf8-encodable',
            ),
        ],
    )
    async def test_idempotency_key_is_refused(
        self, key: str, cause_type: type[BaseException], cause_text: str,
    ) -> None:
        """Python's ``ScopedIdempotencyKey`` refuses each key with a
        ``ValueError``; a key that UTF-8 cannot encode, with the
        ``UnicodeEncodeError`` of the encode (the key of the Python send of
        0.7.0)."""
        result = await _refusing_broker().enqueue_async(
            'my_task',
            task_id=SEND_TASK_ID,
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            idempotency_key=key,
        )

        _assert_refused_with_cause(
            result,
            message=f'idempotency key refused for my_task: {cause_text}',
            cause_type=cause_type,
            cause_text=cause_text,
        )

    def test_sync_send_refuses_a_key_utf8_cannot_encode(self) -> None:
        """The sync form refuses the key as the async form does."""
        result = _refusing_broker().enqueue(
            'my_task',
            task_id=SEND_TASK_ID,
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            idempotency_key='\ud800',
        )

        cause_text = (
            "'utf-8' codec can't encode character '\\ud800' in position 0: surrogates not allowed"
        )
        _assert_refused_with_cause(
            result,
            message=f'idempotency key refused for my_task: {cause_text}',
            cause_type=UnicodeEncodeError,
            cause_text=cause_text,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize('field', ['sent_at', 'enqueued_at'])
    async def test_naive_instant_is_refused(self, field: str) -> None:
        instants: dict[str, datetime] = {
            'sent_at': datetime.now(timezone.utc),
            field: datetime(2020, 1, 1),
        }

        result = await _refusing_broker().enqueue_async(
            'my_task',
            task_id=SEND_TASK_ID,
            send_kind=SendKind.FIRST,
            **instants,
        )

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidInput)
        assert f'{field} must be timezone-aware' in result.err_value.message

    @pytest.mark.asyncio
    async def test_send_with_an_empty_key_is_validation_failed(self) -> None:
        """A normal send reaches this, through the real broker and mapping."""
        broker = _refusing_broker()
        app = MagicMock()
        app.config.queue_mode.name = 'DEFAULT'
        app.config.resend_on_transient_err = False
        app.config.payload = PayloadPolicy()
        app.are_sends_suppressed.return_value = False
        app.send_telemetry.return_value = NO_SEND_TELEMETRY
        app.validate_queue_name.return_value = 'default'
        app.get_broker.return_value = broker

        def good_fn(*, x: int) -> Result[int, TaskError]:
            return Ok(x)

        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = await wrapper.with_options(idempotency_key='').send_async(x=1)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendValidationFailed)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        # TaskSendRetryable excludes this variant, so a retry is a type error,
        # and at run time it is a defect that crashes loud.
        with pytest.raises(AssertionError, match='unreachable'):
            await wrapper.retry_send_async(result.err_value)  # pyright: ignore[reportArgumentType]

    @pytest.mark.asyncio
    async def test_refused_input_needs_no_schema_init(self) -> None:
        """With no server, a bad input is still INVALID_INPUT."""
        broker = _refusing_broker()
        broker._initialized = False

        result = await broker.enqueue_async(
            'my_task', task_id=SEND_TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), priority=0,
        )

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidInput)

    @pytest.mark.asyncio
    async def test_refused_task_options_need_no_schema_init(self) -> None:
        broker = _refusing_broker()
        broker._initialized = False

        result = await broker.enqueue_async(
            'my_task', task_id=SEND_TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), task_options='{bad',
        )

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidTaskOptions)
