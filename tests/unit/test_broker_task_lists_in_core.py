"""``get_stale_tasks`` and ``get_expired_tasks`` through the core handle.

The core returns ``TaskListComplete``, ``TaskListTruncated`` or its
generated broker class (``BrokerOperationFailed``, ``BrokerInvalidInput``);
the broker returns the rows, logs a cut list on ``horsies.broker``, and
returns a failure as it is. The stale threshold is
checked by the handle before any core call, with Python's rule and text: a
refused threshold needs no database. The rows and their order are the core's;
the dual-run pair (``tests/dualrun/test_stale_expired_lists.py``) compares
them with Python's.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Final, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies._core import (
    CoreConnectionError,
    TaskListComplete,
    TaskListTruncated,
    TooManyPendingCalls,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerInvalidInput,
    BrokerOperationFailed,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok
from tests.unit.test_broker_postgres import _make_broker  # pyright: ignore[reportPrivateUsage]

pytestmark = pytest.mark.unit

ROW: Final[dict[str, object]] = {
    'id': uuid.UUID('0198c0de-0000-7000-8000-000000000001'),
    'task_name': 'slow_task',
    'queue_name': 'default',
    'priority': 100,
    'sent_at': datetime(2026, 1, 1, tzinfo=UTC),
    'enqueued_at': datetime(2026, 1, 1, tzinfo=UTC),
    'good_until': datetime(2026, 1, 1, tzinfo=UTC),
    'expired_for': timedelta(minutes=5),
}


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a core record: an instance of ``record_class`` for a
    class pattern, with ``fields``."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _broker_answering(method: str, answer: object) -> PostgresBroker:
    """A broker whose core handle answers ``method`` with ``answer``: a
    record, or an exception that the call raises."""
    broker = cast(PostgresBroker, _make_broker())
    handle = MagicMock()
    match answer:
        case BaseException():
            setattr(handle, method, AsyncMock(side_effect=answer))
        case _:
            setattr(handle, method, AsyncMock(return_value=answer))
    broker._core_handle = handle  # pyright: ignore[reportPrivateUsage]
    return broker


class TestThreshold:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('minutes', 'cause'),
        [
            (-1, 'stale_threshold_minutes is below 0'),
            (525_601, 'stale_threshold_minutes is above 525600 (one year)'),
            (10 ** 5000, 'stale_threshold_minutes is above 525600 (one year)'),
            (-(10 ** 5000), 'stale_threshold_minutes is below 0'),
            (True, 'stale_threshold_minutes is a bool, not an int'),
            (2.5, 'stale_threshold_minutes is a float, not an int'),
            ('5', 'stale_threshold_minutes is a str, not an int'),
        ],
        ids=['negative', 'above-one-year', 'huge-int', 'huge-negative', 'bool', 'float', 'str'],
    )
    async def test_a_threshold_outside_the_rule_is_invalid_input_before_any_core_call(
        self, minutes: object, cause: str,
    ) -> None:
        """The handle refuses with Python's text; no database is reachable here."""
        broker = cast(PostgresBroker, _make_broker())

        result = await broker.get_stale_tasks(stale_threshold_minutes=cast(int, minutes))

        match result:
            case Err(BrokerInvalidInput(message=message, exception=NoCause())):
                assert message == f'{cause}; give whole minutes from 0 to 525600'
            case _:
                raise AssertionError(f'expected INVALID_INPUT, got {result!r}')

    @pytest.mark.asyncio
    async def test_the_value_reaches_the_handle_as_given(self) -> None:
        broker = _broker_answering('stale_tasks_async', _record(TaskListComplete, rows=[]))

        result = await broker.get_stale_tasks(stale_threshold_minutes=7)

        assert result == Ok([])
        handle: Any = broker._core_handle  # pyright: ignore[reportPrivateUsage]
        handle.stale_tasks_async.assert_awaited_once_with(7)


class TestAnswers:
    @pytest.mark.asyncio
    async def test_a_complete_list_is_the_rows_and_logs_nothing(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _broker_answering('expired_tasks_async', _record(TaskListComplete, rows=[ROW]))

        with caplog.at_level('WARNING'):
            result = await broker.get_expired_tasks()

        assert result == Ok([ROW])
        assert not [r for r in caplog.records if r.levelname == 'WARNING']

    @pytest.mark.asyncio
    async def test_a_truncated_list_logs_the_bound_on_the_broker_logger(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _broker_answering(
            'stale_tasks_async', _record(TaskListTruncated, rows=[ROW], bound=10_000),
        )

        with caplog.at_level('WARNING'):
            result = await broker.get_stale_tasks()

        assert result == Ok([ROW])
        warnings = [
            (r.name, r.getMessage()) for r in caplog.records if r.levelname == 'WARNING'
        ]
        assert warnings == [
            ('horsies.broker', 'get_stale_tasks returned the first 10000 rows; more rows match'),
        ]

    @pytest.mark.parametrize('retryable', [True, False])
    @pytest.mark.asyncio
    async def test_a_read_failure_keeps_its_message_and_retry_rule(self, retryable: bool) -> None:
        message = 'get_expired_tasks failed: connection refused'
        cause = CoreConnectionError('get_expired_tasks', 'connection refused', retryable)
        broker = _broker_answering(
            'expired_tasks_async',
            BrokerOperationFailed(code=BrokerErrorCode.MONITORING_QUERY_FAILED, message=message, retryable=retryable, exception=cause),
        )

        result = await broker.get_expired_tasks()

        match result:
            case Err(BrokerOperationFailed() as error):
                assert error.code is BrokerErrorCode.MONITORING_QUERY_FAILED
                assert error.message == message
                assert error.retryable is retryable
                assert error.exception is cause
            case _:
                raise AssertionError(f'expected MONITORING_QUERY_FAILED, got {result!r}')

    @pytest.mark.asyncio
    async def test_a_core_boundary_error_is_a_monitoring_failure(self) -> None:
        raised = TooManyPendingCalls('CoreHandle.stale_tasks_async', 65_536)
        broker = _broker_answering('stale_tasks_async', raised)

        result = await broker.get_stale_tasks()

        match result:
            case Err(BrokerOperationFailed() as error):
                assert error.code is BrokerErrorCode.MONITORING_QUERY_FAILED
                assert error.exception is raised
                assert error.retryable is True
                assert error.message.startswith('get_stale_tasks failed: ')
            case _:
                raise AssertionError(f'expected MONITORING_QUERY_FAILED, got {result!r}')

    @pytest.mark.asyncio
    async def test_another_code_from_the_core_is_a_defect(self) -> None:
        broker = _broker_answering(
            'expired_tasks_async',
            BrokerOperationFailed(code=BrokerErrorCode.DB_PING_FAILED, message='x', retryable=False, exception=CoreConnectionError('test_operation', 'x', True)),
        )

        with pytest.raises(AssertionError, match='DB_PING_FAILED'):
            await broker.get_expired_tasks()
