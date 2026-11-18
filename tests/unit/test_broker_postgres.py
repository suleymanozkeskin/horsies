"""Tests for PostgresBroker (python/horsies/core/brokers/postgres.py).

Strategy: mock DB layer entirely (create_async_engine, async_sessionmaker,
PostgresListener) to avoid real PostgreSQL. Tests verify logic, branching,
error handling, and delegation patterns.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from horsies.core.brokers.enqueue_task import SendKind
from horsies._core import (
    BrokerFailure,
    CoreConnectionError,
    CoreContractViolation,
    CorePreconditionFailed,
    CoreServerError,
    CoreStartFailed,
    DatabasePingRecord,
    Enqueued,
    HandleClosed,
    SendEnqueueFailed,
    SendKeyConflict,
    SendPayloadMismatch,
    SendPayloadTooLarge,
    SendRefusalByRule,
    SendRefusalByValueRule,
    SendRefusalUnreadableJson,
    SendSuppressed,
    SendValidationFailed,
    StartAfterDelay as CoreStartAfterDelay,
    StartAt as CoreStartAt,
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
from horsies.core.brokers.resources import ListenerDisabled, ListenerEnabled
from horsies.core.brokers.result_types import (
    BrokerEnqueueFailed,
    BrokerErrorCode,
    BrokerIdempotencyKeyConflict,
    BrokerInvalidInput,
    BrokerInvalidJsonPayload,
    BrokerInvalidTaskOptions,
    BrokerListenerDisabled,
    BrokerOperationFailed,
    BrokerPayloadMismatch,
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
    TaskResult,
)
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.types.status import TaskAttemptOutcome, TaskStatus
from tests.broker_resources_support import (
    pin_resources,
    resources_of,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pinned(broker: Any) -> Any:
    """The mocked resource set that ``_make_broker`` pinned for every loop."""
    return broker._build_resources()


def _listener_of(broker: Any) -> Any:
    """The listener inside the pinned resource set."""
    match _pinned(broker).listener:
        case ListenerEnabled(listener=listener):
            return listener
        case ListenerDisabled():
            raise AssertionError('broker has no listener')


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
    """Create a PostgresBroker with fully mocked internals."""
    from horsies.core.models.broker import PostgresConfig

    with (
        patch('horsies.core.brokers.postgres.create_async_engine') as mock_engine,
        patch('horsies.core.brokers.postgres.async_sessionmaker') as mock_sm,
        patch('horsies.core.brokers.postgres.PostgresListener') as mock_listener_cls,
    ):
        mock_engine.return_value = MagicMock()
        mock_engine.return_value.dispose = AsyncMock()
        mock_engine.return_value.begin = MagicMock()

        mock_session = AsyncMock()
        mock_sm.return_value = MagicMock(return_value=mock_session)
        # Make the session work as an async context manager
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        mock_listener = AsyncMock()
        mock_listener_cls.return_value = mock_listener

        config = PostgresConfig(database_url=database_url)

        from horsies.core.brokers.postgres import PostgresBroker

        broker = PostgresBroker(config)
        # Build the mocked set under the patches; every loop gets it.
        pin_resources(broker, broker._build_resources())
        # Mark as initialized to skip real DB setup
        broker._initialized = True

    return broker


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
            case BrokerInvalidTaskOptions(message=message, exception=cause):
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
            case BrokerInvalidInput(message=message, exception=cause):
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
            case BrokerInvalidInput(message=message, exception=cause):
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
            case BrokerInvalidInput(message=message, exception=cause):
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
        assert result.err_value.code == BrokerErrorCode.INVALID_TASK_OPTIONS
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
            case BrokerInvalidInput(message=message, exception=cause):
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


def _stand_in_named_as(record_class: type) -> MagicMock:
    """A stand-in for a core record whose type has the record's class name.

    A native record has no Python constructor, and the facade names a record
    by ``type(outcome).__name__``: a plain ``MagicMock(spec=...)`` is of type
    ``MagicMock``. The stand-in is an instance of ``record_class`` for a
    class pattern, as with ``spec``.
    """
    named = cast(type[MagicMock], type(record_class.__name__, (MagicMock,), {}))
    return named(spec=record_class)


@pytest.mark.unit
class TestEnqueueThroughCore:
    """enqueue_async and enqueue build the core's request with the client
    rules, and map the record of one core send to their result."""

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
        assert isinstance(result.err_value, BrokerInvalidInput)
        assert message in result.err_value.message
        broker._core_handle.enqueue_async.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('code', 'error_class'),
        [
            ('INVALID_INPUT', BrokerInvalidInput),
            ('INVALID_TASK_OPTIONS', BrokerInvalidTaskOptions),
        ],
    )
    @pytest.mark.parametrize(
        ('cause', 'cause_class'),
        [
            pytest.param(
                MagicMock(spec=SendRefusalUnreadableJson, reader_error='expected value at line 1'),
                SerializationError,
                id='unreadable-json',
            ),
            pytest.param(MagicMock(spec=SendRefusalByRule), NoCause, id='by-rule'),
            pytest.param(
                MagicMock(spec=SendRefusalByValueRule, rule='priority must be between 1 and 100'),
                ValueError,
                id='by-value-rule',
            ),
        ],
    )
    async def test_a_core_refusal_keeps_its_code_and_cause(
        self, code: str, error_class: type, cause: MagicMock, cause_class: type,
    ) -> None:
        broker = self._broker_answering(
            MagicMock(spec=SendValidationFailed, code=code, message='refused', cause=cause),
        )

        result = await self._send(broker)

        assert is_err(result)
        assert isinstance(result.err_value, error_class)
        assert isinstance(result.err_value.exception, cause_class)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('retryable', [True, False])
    async def test_a_failed_enqueue_keeps_the_core_classification(self, retryable: bool) -> None:
        cause = CoreConnectionError('enqueue', 'connection lost', retryable)
        broker = self._broker_answering(
            MagicMock(
                spec=SendEnqueueFailed,
                task_id=SEND_TASK_ID,
                message='Failed to enqueue task my_task: connection lost',
                retryable=retryable,
                exception=cause,
            ),
        )

        result = await self._send(broker)

        assert is_err(result)
        failed = result.err_value
        assert isinstance(failed, BrokerEnqueueFailed)
        assert failed.retryable is retryable
        assert failed.exception is cause

    @pytest.mark.asyncio
    async def test_a_payload_mismatch_and_a_key_conflict_keep_their_facts(self) -> None:
        owner = UUID('0198c0de-0000-7000-8000-0000000000ff')
        mismatch = await self._send(
            self._broker_answering(
                MagicMock(spec=SendPayloadMismatch, task_id=SEND_TASK_ID, message='mismatch'),
            ),
        )
        conflict = await self._send(
            self._broker_answering(
                MagicMock(
                    spec=SendKeyConflict,
                    task_id=SEND_TASK_ID,
                    owner_task_id=owner,
                    message='conflict',
                ),
            ),
        )

        assert mismatch == Err(BrokerPayloadMismatch(message='mismatch'))
        assert conflict == Err(BrokerIdempotencyKeyConflict(message='conflict', owner_task_id=owner))

    @pytest.mark.asyncio
    @pytest.mark.parametrize('outcome_class', [SendSuppressed, SendPayloadTooLarge])
    async def test_a_record_the_core_send_never_answers_is_a_defect(
        self, outcome_class: type,
    ) -> None:
        broker = self._broker_answering(_stand_in_named_as(outcome_class))

        with pytest.raises(AssertionError, match=outcome_class.__name__):
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
        assert isinstance(result.err_value, BrokerEnqueueFailed)
        assert result.err_value.retryable is True
        assert result.err_value.exception is error

    def test_the_blocking_form_sends_on_the_calling_thread(self) -> None:
        broker = self._broker_answering(MagicMock(spec=Enqueued, task_id=SEND_TASK_ID))
        broker._loop_runner = MagicMock()

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
        broker._loop_runner.call.assert_not_called()


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
        assert isinstance(result.err_value, BrokerInvalidJsonPayload)
        assert isinstance(result.err_value.exception, NoCause)

    @pytest.mark.asyncio
    async def test_an_empty_stored_result_is_invalid_json_payload(self) -> None:
        broker = self._broker_answering(self._record('COMPLETED', ''))

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID)

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidJsonPayload)
        assert isinstance(result.err_value.exception, EmptyJsonTextError)
        assert result.err_value.message == (
            f'Result JSON parse failed for task {RESULT_TASK_ID}: '
            'JSON parse failed: the text is empty'
        )

    @pytest.mark.asyncio
    async def test_corrupt_stored_data_found_by_the_core_is_invalid_json_payload(
        self,
    ) -> None:
        broker = self._broker_answering(
            MagicMock(
                spec=BrokerFailure,
                code='INVALID_JSON_PAYLOAD',
                message='Result archive digest mismatch',
                retryable=False,
                exception=_DIGEST_MISMATCH,
            ),
        )

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID)

        assert is_err(result)
        failed = result.err_value
        assert isinstance(failed, BrokerInvalidJsonPayload)
        assert 'digest mismatch' in failed.message
        assert failed.exception is _DIGEST_MISMATCH

    @pytest.mark.asyncio
    async def test_a_failed_read_is_task_info_query_failed_and_logged(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = self._broker_answering(
            MagicMock(
                spec=BrokerFailure,
                code='TASK_INFO_QUERY_FAILED',
                message='connection lost',
                retryable=True,
                exception=_CONNECTION_LOST,
            ),
        )

        with caplog.at_level('ERROR', logger='horsies.broker'):
            result = await broker.get_raw_result_record_async(RESULT_TASK_ID)

        assert is_err(result)
        failed = result.err_value
        assert isinstance(failed, BrokerOperationFailed)
        assert failed.code is BrokerErrorCode.TASK_INFO_QUERY_FAILED
        assert failed.retryable is True
        assert failed.exception is _CONNECTION_LOST
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
        assert result.err_value.code is BrokerErrorCode.TASK_INFO_QUERY_FAILED
        assert result.err_value.retryable is True
        assert result.err_value.exception is error

    @pytest.mark.asyncio
    async def test_a_refused_limit_is_invalid_input_before_the_core(self) -> None:
        broker = self._broker_answering(None)

        result = await broker.get_raw_result_record_async(RESULT_TASK_ID, timeout_ms=-1)

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidInput)
        broker._core_handle.raw_result_record_async.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_another_failure_code_is_a_defect(self) -> None:
        broker = self._broker_answering(
            MagicMock(spec=BrokerFailure, code='INVALID_INPUT', message='x', retryable=False),
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
        broker._loop_runner = MagicMock()
        handle = MagicMock()
        handle.raw_result_record = MagicMock(
            return_value=MagicMock(spec=RawResultAbsent, task_id=RESULT_TASK_ID),
        )
        broker._core_handle = handle

        result = broker.get_raw_result_record(RESULT_TASK_ID, timeout_ms=5_000)

        assert result == Ok(None)
        handle.raw_result_record.assert_called_once_with(RESULT_TASK_ID, timeout_ms=5_000)
        broker._loop_runner.call.assert_not_called()


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
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=BrokerFailure,
                code='DB_PING_FAILED',
                message='database ping failed: connection refused',
                retryable=True,
                exception=_CONNECTION_LOST,
            ),
        )

        result = await broker.ping_database_async()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is BrokerErrorCode.DB_PING_FAILED
        assert failed.retryable is True
        assert failed.message == 'ping_database failed: database ping failed: connection refused'
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
        assert failed.code is BrokerErrorCode.DB_PING_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.asyncio
    async def test_another_failure_code_is_a_defect(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=BrokerFailure, code='CLEANUP_FAILED', message='x', retryable=False),
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
        assert isinstance((await resources_of(broker)).listener, ListenerDisabled)

        result = await broker.ping_workers_async(timeout_seconds=1.0)

        assert is_err(result)
        assert isinstance(result.err_value, BrokerListenerDisabled)
        assert result.err_value.code is BrokerErrorCode.LISTENER_DISABLED


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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is error


# ---------------------------------------------------------------------------
# TestTerminateOrphanedWorkflowTasks
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTerminateOrphanedWorkflowTasks:
    """The orphan audit maps the record of one core page to its result."""

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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is False
        assert failed.message.startswith('audit_orphaned_workflow_tasks failed: ')
        assert failed.exception is _COUNT_DIFFERS
        assert not isinstance(failed.exception, CoreServerError)

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_page_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.audit_orphaned_workflow_tasks_async', 65_536)
        broker._core_handle = self._handle(error)

        result = await broker.terminate_orphaned_workflow_tasks()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
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
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is False
        assert isinstance(failed.exception, OverflowError)
        broker._core_handle.requeue_stale_claimed_async.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestSyncFacades
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSyncFacades:
    """The sync forms: the blocking core reads."""

    # (test_get_result_delegates_to_loop_runner and
    # test_get_result_loop_runner_exception_returns_broker_error_result
    # removed: strict-serde phase 6 dropped ``broker.get_result``. Sync
    # bridge coverage moved up to ``Horsies.get_result`` — see
    # ``tests/unit/test_app_get_result.py``.)

    def test_get_task_info_runs_the_blocking_core_read(self) -> None:
        """get_task_info() calls CoreHandle.task_info on this thread, not the runner."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        handle = MagicMock()
        handle.task_info = MagicMock(return_value=MagicMock(spec=TaskInfoNotFound))
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
        broker._loop_runner.call.assert_not_called()


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
        assert isinstance(result.err_value, BrokerInvalidJsonPayload)
        assert isinstance(result.err_value.exception, exception_class)

    @pytest.mark.asyncio
    async def test_an_empty_stored_result_is_invalid_json_payload(self) -> None:
        broker = self._broker_answering(
            _task_info_record(status='COMPLETED', result_json=''),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID, include_result=True)

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidJsonPayload)
        assert isinstance(result.err_value.exception, EmptyJsonTextError)
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
        broker = self._broker_answering(
            MagicMock(
                spec=BrokerFailure,
                code='TASK_INFO_QUERY_FAILED',
                message='pool timed out',
                retryable=True,
                exception=_CONNECTION_LOST,
            ),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID)

        assert is_err(result)
        failed = result.err_value
        assert isinstance(failed, BrokerOperationFailed)
        assert failed.code is BrokerErrorCode.TASK_INFO_QUERY_FAILED
        assert failed.retryable is True
        assert failed.message == f'get_task_info failed for {TASK_INFO_ID}: pool timed out'
        assert failed.exception is _CONNECTION_LOST

    @pytest.mark.asyncio
    async def test_corrupt_stored_data_found_by_the_core_is_invalid_json_payload(
        self,
    ) -> None:
        broker = self._broker_answering(
            MagicMock(
                spec=BrokerFailure,
                code='INVALID_JSON_PAYLOAD',
                message='stored result digest mismatch',
                retryable=False,
                exception=_DIGEST_MISMATCH,
            ),
        )

        result = await broker.get_task_info_async(TASK_INFO_ID, include_result=True)

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidJsonPayload)
        assert result.err_value.exception is _DIGEST_MISMATCH

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_read_is_task_info_query_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.task_info_async', 65_536)
        handle = MagicMock()
        handle.task_info_async = AsyncMock(side_effect=error)
        broker._core_handle = handle

        result = await broker.get_task_info_async(TASK_INFO_ID)

        assert is_err(result)
        assert result.err_value.code is BrokerErrorCode.TASK_INFO_QUERY_FAILED
        assert result.err_value.retryable is True
        assert result.err_value.exception is error

    @pytest.mark.asyncio
    async def test_another_failure_code_is_a_defect(self) -> None:
        broker = self._broker_answering(
            MagicMock(spec=BrokerFailure, code='CLEANUP_FAILED', message='x', retryable=False),
        )

        with pytest.raises(AssertionError, match='CLEANUP_FAILED'):
            await broker.get_task_info_async(TASK_INFO_ID)


# ---------------------------------------------------------------------------
# TestCloseAsync / TestClose
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCloseAsync:
    """Tests for close_async: listener close then engine dispose."""

    @pytest.mark.asyncio
    async def test_close_async_closes_listener_then_disposes_engine(self) -> None:
        """close_async should close listener first, then dispose engine, returning Ok(None)."""
        broker = _make_broker()
        call_order: list[str] = []

        async def mock_listener_close() -> None:
            call_order.append('listener_close')

        async def mock_engine_dispose() -> None:
            call_order.append('engine_dispose')

        _listener_of(broker).close = mock_listener_close
        _pinned(broker).engine.dispose = mock_engine_dispose
        await resources_of(broker)  # build this loop's set

        result = await broker.close_async()

        assert is_ok(result)
        assert result.ok_value is None
        assert call_order == ['listener_close', 'engine_dispose']

    @pytest.mark.asyncio
    async def test_close_async_without_a_built_set_closes_nothing(self) -> None:
        """A broker that never made a call holds no set, so close touches nothing."""
        broker = _make_broker()

        result = await broker.close_async()

        assert is_ok(result)
        _listener_of(broker).close.assert_not_awaited()
        _pinned(broker).engine.dispose.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_async_stops_loop_runner_when_started_externally(self) -> None:
        """close_async should stop LoopRunner if sync APIs had started it."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner._started = True
        broker._loop_runner._thread = object()
        broker._loop_runner.stop = MagicMock()

        result = await broker.close_async()

        assert is_ok(result)
        broker._loop_runner.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_async_does_not_stop_loop_runner_from_its_own_thread(
        self,
    ) -> None:
        """close_async must not self-stop when running on loop-runner thread."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner._started = True
        broker._loop_runner._thread = threading.current_thread()
        broker._loop_runner.stop = MagicMock()

        result = await broker.close_async()

        assert is_ok(result)
        broker._loop_runner.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_async_stops_real_loop_runner_thread(self) -> None:
        """Regression: close_async should stop a real LoopRunner started by sync APIs."""
        broker = _make_broker()
        runner = broker._loop_runner
        assert runner._started is False

        runner.start()
        assert runner._started is True
        assert runner._thread is not None and runner._thread.is_alive()

        try:
            result = await broker.close_async()
            assert is_ok(result)
            assert runner._started is False
            assert runner._loop is None
            assert runner._thread is None
        finally:
            # Safety net for test isolation in case assertions fail mid-test.
            if runner._started:
                runner.stop()


@pytest.mark.unit
class TestClose:
    """close (sync): the core handle's blocking close on the calling thread,
    then the stop of the sync runner. The outcomes equal close_async's
    (``tests/unit/test_sync_facade_blocking_forms.py``)."""

    @staticmethod
    def _broker_closing(close: MagicMock) -> tuple[Any, MagicMock]:
        broker = _make_broker()
        handle = MagicMock()
        handle.close = close
        broker._core_handle = handle
        runner = MagicMock()
        broker._loop_runner = runner
        return broker, runner

    def test_close_calls_the_blocking_core_close_and_stops_the_runner(self) -> None:
        """close() calls CoreHandle.close on this thread, never the runner, then stops it."""
        broker, runner = self._broker_closing(MagicMock(return_value=MagicMock(spec=HandleClosed)))

        result = broker.close()

        assert result == Ok(None)
        broker._core_handle.close.assert_called_once_with()
        broker._core_handle.close_async.assert_not_called()
        runner.call.assert_not_called()
        runner.stop.assert_called_once()

    def test_a_core_that_cannot_run_the_close_is_close_failed_and_stops_the_runner(
        self,
    ) -> None:
        """A core boundary error of the blocking close is CLOSE_FAILED with the error."""
        refused = CoreStartFailed('the core runtime could not start')
        broker, runner = self._broker_closing(MagicMock(side_effect=refused))

        result = broker.close()

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.CLOSE_FAILED
        assert result.err_value.exception is refused
        runner.stop.assert_called_once()

    def test_close_stops_runner_when_an_exception_propagates(self) -> None:
        """An exception that is not a CoreError propagates; stop still runs."""
        defect = RuntimeError('boom')
        broker, runner = self._broker_closing(MagicMock(side_effect=defect))

        with pytest.raises(RuntimeError) as exc_info:
            broker.close()

        assert exc_info.value is defect
        runner.stop.assert_called_once()


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
            MagicMock(
                spec=BrokerFailure,
                code='SCHEMA_INIT_FAILED',
                message='the database was installed by the RUST client',
                retryable=False,
                exception=_FAMILY_MISMATCH,
            ),
        )

        first = await broker.ensure_schema_initialized()
        second = await broker.ensure_schema_initialized()

        assert is_err(first)
        failed = first.err_value
        assert failed.code is BrokerErrorCode.SCHEMA_INIT_FAILED
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
        assert failed.code is BrokerErrorCode.SCHEMA_INIT_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.asyncio
    async def test_another_failure_code_is_a_defect(self) -> None:
        broker = self._uninitialized(
            MagicMock(spec=BrokerFailure, code='DB_PING_FAILED', message='x', retryable=False),
        )

        with pytest.raises(AssertionError, match='DB_PING_FAILED'):
            await broker.ensure_schema_initialized()


@pytest.mark.unit
class TestCloseAsyncErrorPaths:
    """Regression tests for error handling in close_async."""

    @pytest.mark.asyncio
    async def test_close_async_attempts_both_on_first_failure(self) -> None:
        """When listener.close raises, engine.dispose is still called and result is Err."""
        broker = _make_broker()
        dispose_called = False

        async def failing_listener_close() -> None:
            raise RuntimeError('listener boom')

        async def tracking_engine_dispose() -> None:
            nonlocal dispose_called
            dispose_called = True

        _listener_of(broker).close = failing_listener_close
        _pinned(broker).engine.dispose = tracking_engine_dispose
        await resources_of(broker)  # build this loop's set

        result = await broker.close_async()

        assert (
            dispose_called
        ), 'engine.dispose must be called even after listener.close fails'
        assert is_err(result)
        err = result.err_value
        assert err.code == BrokerErrorCode.CLOSE_FAILED
        assert err.exception is not None


def _assert_refused_with_cause(
    result: Any, *, message: str, cause_type: type[BaseException], cause_text: str,
) -> None:
    """The outcome of Python's broker for a value that a rule refused:
    INVALID_INPUT, not retryable, with ``message`` and a cause of exactly
    ``cause_type`` whose text is ``cause_text``."""
    match result:
        case Err(BrokerInvalidInput(message=refused_message, exception=cause) as refused):
            assert refused.code is BrokerErrorCode.INVALID_INPUT
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
        assert isinstance(result.err_value, BrokerInvalidInput)
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
        assert isinstance(result.err_value, BrokerInvalidInput)
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
        app.validate_queue_name.return_value = 'default'
        app.get_broker.return_value = broker

        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

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
        assert isinstance(result.err_value, BrokerInvalidInput)

    @pytest.mark.asyncio
    async def test_refused_task_options_need_no_schema_init(self) -> None:
        broker = _refusing_broker()
        broker._initialized = False

        result = await broker.enqueue_async(
            'my_task', task_id=SEND_TASK_ID, send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), task_options='{bad',
        )

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidTaskOptions)
