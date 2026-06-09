"""Regression tests: TaskHandle must not cache transient retrieval errors.

A handle previously cached BROKER_ERROR / TASK_NOT_FOUND permanently —
one DB hiccup during get() poisoned the handle, and every later call
replayed the stale error even after the task completed. WAIT_TIMEOUT
already (correctly) bypassed the cache; these tests pin the same
behavior for the other transient codes and confirm terminal errors are
still cached.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerOperationError,
    RawResultRecord,
)
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
    TaskError,
)
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus

pytestmark = [pytest.mark.unit]


def _make_handle(broker: MagicMock) -> TaskHandle[int]:
    app = MagicMock()
    app.get_broker = MagicMock(return_value=broker)
    return TaskHandle(
        task_id='task-1',
        app=app,
        broker_mode=True,
        ok_type=int,
    )


def _ok_record(value: int) -> Ok[RawResultRecord]:
    envelope: dict[str, Any] = {
        '__h_task_result__': True,
        'ok': value,
        'err': None,
    }
    return Ok(
        RawResultRecord(
            task_id='task-1',
            task_name='t',
            status=TaskStatus.COMPLETED,
            raw_result=envelope,
        )
    )


def _assert_err_code(result: Any, code: object) -> None:
    assert result.is_err()
    err: TaskError = result.unwrap_err()
    assert err.error_code == code


@pytest.mark.asyncio
class TestTransientErrorsNotCached:
    async def test_broker_exception_then_success(self) -> None:
        """A raised broker exception must not poison the handle."""
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            side_effect=[RuntimeError('db down'), _ok_record(42)],
        )
        handle = _make_handle(broker)

        first = await handle.get_async()
        _assert_err_code(first, OperationalErrorCode.BROKER_ERROR)

        second = await handle.get_async()
        assert second.is_ok(), (
            'Handle must re-query after a transient broker exception, '
            f'got {second}'
        )
        assert second.unwrap() == 42
        assert broker.get_raw_result_record_async.await_count == 2

    async def test_broker_error_result_then_success(self) -> None:
        """An Err(BrokerOperationError) must not poison the handle."""
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            side_effect=[
                Err(BrokerOperationError(
                    code=BrokerErrorCode.TASK_INFO_QUERY_FAILED,
                    message='transient',
                    retryable=True,
                )),
                _ok_record(7),
            ],
        )
        handle = _make_handle(broker)

        first = await handle.get_async()
        _assert_err_code(first, OperationalErrorCode.BROKER_ERROR)

        second = await handle.get_async()
        assert second.is_ok()
        assert second.unwrap() == 7

    async def test_task_not_found_then_success(self) -> None:
        """TASK_NOT_FOUND (handle racing the enqueue) must not be cached."""
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            side_effect=[Ok(None), _ok_record(3)],
        )
        handle = _make_handle(broker)

        first = await handle.get_async()
        _assert_err_code(first, RetrievalCode.TASK_NOT_FOUND)

        second = await handle.get_async()
        assert second.is_ok()
        assert second.unwrap() == 3


@pytest.mark.asyncio
class TestTerminalErrorsStillCached:
    async def test_cancelled_is_cached(self) -> None:
        """TASK_CANCELLED is terminal: no re-query on later calls."""
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            return_value=Ok(
                RawResultRecord(
                    task_id='task-1',
                    task_name='t',
                    status=TaskStatus.CANCELLED,
                    raw_result=None,
                )
            ),
        )
        handle = _make_handle(broker)

        first = await handle.get_async()
        _assert_err_code(first, OutcomeCode.TASK_CANCELLED)

        second = await handle.get_async()
        _assert_err_code(second, OutcomeCode.TASK_CANCELLED)
        assert broker.get_raw_result_record_async.await_count == 1, (
            'Terminal errors must stay cached (single broker query)'
        )

    async def test_ok_result_is_cached(self) -> None:
        """Successful results remain cached (single broker query)."""
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            return_value=_ok_record(11),
        )
        handle = _make_handle(broker)

        first = await handle.get_async()
        assert first.is_ok() and first.unwrap() == 11
        second = await handle.get_async()
        assert second.is_ok() and second.unwrap() == 11
        assert broker.get_raw_result_record_async.await_count == 1
