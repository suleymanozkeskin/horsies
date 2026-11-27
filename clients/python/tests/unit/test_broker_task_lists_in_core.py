"""``get_stale_tasks`` and ``get_expired_tasks`` through the core handle.

The core returns the complete or truncated class of each list, or its
generated broker class (``DatabaseOperationFailed``, ``DatabaseInvalidInput``);
the broker returns the typed rows in a ``BoundedTaskList`` and a failure as
it is. The stale threshold is checked by the handle before any core call,
with Python's rule and text: a refused threshold needs no database. The rows
and their order are the core's.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Final, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies._core import (
    CoreConnectionError,
    ExpiredTaskListComplete,
    ExpiredTaskRecord,
    StaleTaskListComplete,
    StaleTaskListTruncated,
    StaleTaskRecord,
    TooManyPendingCalls,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    DatabaseErrorCode,
    DatabaseInvalidInput,
    DatabaseOperationFailed,
)
from horsies.core.models.health_reads import (
    BoundedTaskListComplete,
    BoundedTaskListTruncated,
    ExpiredTaskRow,
    StaleTaskRow,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok
from tests.unit.test_broker_postgres import _make_broker  # pyright: ignore[reportPrivateUsage]

pytestmark = pytest.mark.unit

TASK_ID: Final = uuid.UUID('0198c0de-0000-7000-8000-000000000001')
AT: Final = datetime(2026, 1, 1, tzinfo=UTC)

EXPIRED_ROW: Final = ExpiredTaskRow(
    task_id=TASK_ID,
    task_name='slow_task',
    queue_name='default',
    priority=100,
    sent_at=AT,
    enqueued_at=AT,
    good_until=AT,
    expired_for=timedelta(minutes=5),
)

STALE_ROW: Final = StaleTaskRow(
    task_id=TASK_ID,
    task_name='slow_task',
    worker_hostname='host-1',
    worker_pid=1234,
    worker_process_name='child-1',
    started_at=AT,
    last_heartbeat_at=None,
)


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a core record: an instance of ``record_class`` for a
    class pattern, with ``fields``."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _expired_record() -> MagicMock:
    """The core record of ``EXPIRED_ROW``."""
    return _record(
        ExpiredTaskRecord,
        task_id=TASK_ID,
        task_name='slow_task',
        queue_name='default',
        priority=100,
        sent_at=AT,
        enqueued_at=AT,
        good_until=AT,
        expired_for=timedelta(minutes=5),
    )


def _stale_record() -> MagicMock:
    """The core record of ``STALE_ROW``."""
    return _record(
        StaleTaskRecord,
        task_id=TASK_ID,
        task_name='slow_task',
        worker_hostname='host-1',
        worker_pid=1234,
        worker_process_name='child-1',
        started_at=AT,
        last_heartbeat_at=None,
    )


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
            (-1, 'threshold_minutes is below 0'),
            (525_601, 'threshold_minutes is above 525600 (one year)'),
            (10 ** 5000, 'threshold_minutes is above 525600 (one year)'),
            (-(10 ** 5000), 'threshold_minutes is below 0'),
            (True, 'threshold_minutes is a bool, not an int'),
            (2.5, 'threshold_minutes is a float, not an int'),
            ('5', 'threshold_minutes is a str, not an int'),
        ],
        ids=['negative', 'above-one-year', 'huge-int', 'huge-negative', 'bool', 'float', 'str'],
    )
    async def test_a_threshold_outside_the_rule_is_invalid_input_before_any_core_call(
        self, minutes: object, cause: str,
    ) -> None:
        """The handle refuses with Python's text; no database is reachable here."""
        broker = cast(PostgresBroker, _make_broker())

        result = await broker.get_stale_tasks(threshold_minutes=cast(int, minutes))

        match result:
            case Err(DatabaseInvalidInput(message=message, cause=NoCause())):
                assert message == f'{cause}; give whole minutes from 0 to 525600'
            case _:
                raise AssertionError(f'expected INVALID_INPUT, got {result!r}')

    @pytest.mark.asyncio
    async def test_the_value_reaches_the_handle_as_given(self) -> None:
        broker = _broker_answering('stale_tasks_async', _record(StaleTaskListComplete, rows=[]))

        result = await broker.get_stale_tasks(threshold_minutes=7)

        assert result == Ok(BoundedTaskListComplete(rows=()))
        handle: Any = broker._core_handle  # pyright: ignore[reportPrivateUsage]
        handle.stale_tasks_async.assert_awaited_once_with(7)


class TestAnswers:
    @pytest.mark.asyncio
    async def test_a_complete_list_is_its_typed_rows(self) -> None:
        broker = _broker_answering(
            'expired_tasks_async', _record(ExpiredTaskListComplete, rows=[_expired_record()]),
        )

        result = await broker.get_expired_tasks()

        assert result == Ok(BoundedTaskListComplete(rows=(EXPIRED_ROW,)))

    @pytest.mark.asyncio
    async def test_a_truncated_list_is_its_typed_rows_and_its_bound(self) -> None:
        broker = _broker_answering(
            'stale_tasks_async',
            _record(StaleTaskListTruncated, rows=[_stale_record()], bound=10_000),
        )

        result = await broker.get_stale_tasks()

        assert result == Ok(BoundedTaskListTruncated(rows=(STALE_ROW,), bound=10_000))

    @pytest.mark.parametrize('retryable', [True, False])
    @pytest.mark.asyncio
    async def test_a_read_failure_keeps_its_message_and_retry_rule(self, retryable: bool) -> None:
        message = 'get_expired_tasks failed: connection refused'
        cause = CoreConnectionError('get_expired_tasks', 'connection refused', retryable)
        broker = _broker_answering(
            'expired_tasks_async',
            DatabaseOperationFailed(code=DatabaseErrorCode.MONITORING_QUERY_FAILED, message=message, retryable=retryable, cause=cause),
        )

        result = await broker.get_expired_tasks()

        match result:
            case Err(DatabaseOperationFailed() as error):
                assert error.code is DatabaseErrorCode.MONITORING_QUERY_FAILED
                assert error.message == message
                assert error.retryable is retryable
                assert error.cause is cause
            case _:
                raise AssertionError(f'expected MONITORING_QUERY_FAILED, got {result!r}')

    @pytest.mark.asyncio
    async def test_a_core_boundary_error_is_a_monitoring_failure(self) -> None:
        raised = TooManyPendingCalls('CoreHandle.stale_tasks_async', 65_536)
        broker = _broker_answering('stale_tasks_async', raised)

        result = await broker.get_stale_tasks()

        match result:
            case Err(DatabaseOperationFailed() as error):
                assert error.code is DatabaseErrorCode.MONITORING_QUERY_FAILED
                assert error.cause is raised
                assert error.retryable is True
                assert error.message.startswith('get_stale_tasks failed: ')
            case _:
                raise AssertionError(f'expected MONITORING_QUERY_FAILED, got {result!r}')

    @pytest.mark.asyncio
    async def test_another_code_from_the_core_is_a_defect(self) -> None:
        broker = _broker_answering(
            'expired_tasks_async',
            DatabaseOperationFailed(code=DatabaseErrorCode.DATABASE_PING_FAILED, message='x', retryable=False, cause=CoreConnectionError('test_operation', 'x', True)),
        )

        with pytest.raises(AssertionError, match='DATABASE_PING_FAILED'):
            await broker.get_expired_tasks()
