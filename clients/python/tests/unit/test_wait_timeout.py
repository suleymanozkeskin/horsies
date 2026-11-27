"""A result get refuses a wait timeout that no wait can use, before any read."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import DatabaseInvalidInput
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.outcome import ReadInvalidInput
from horsies.core.models.tasks import MAX_TASK_TIMEOUT_MS
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import Err, Ok
from horsies.core.utils.wait_timeout import (
    WAIT_TIMEOUT_ABOVE_LIMIT,
    WAIT_TIMEOUT_NEGATIVE,
    WAIT_TIMEOUT_NOT_AN_INT,
    WaitTimeoutRefused,
    wait_timeout_seconds,
)

pytestmark = pytest.mark.unit

_UNREACHABLE_URL = SecretStr('postgresql://user:pass@127.0.0.1:1/none')

_REFUSED: list[Any] = [
    pytest.param(MAX_TASK_TIMEOUT_MS + 1, WAIT_TIMEOUT_ABOVE_LIMIT, id='limit+1'),
    pytest.param(10**5000, WAIT_TIMEOUT_ABOVE_LIMIT, id='5001-digits'),
    pytest.param(-1, WAIT_TIMEOUT_NEGATIVE, id='negative'),
    pytest.param(True, WAIT_TIMEOUT_NOT_AN_INT, id='bool'),
    pytest.param(1.5, WAIT_TIMEOUT_NOT_AN_INT, id='float'),
    pytest.param('5000', WAIT_TIMEOUT_NOT_AN_INT, id='str'),
]


class TestWaitTimeoutSeconds:
    @pytest.mark.parametrize(
        ('wait_ms', 'seconds'),
        [
            (None, None),
            (0, 0.0),
            (1_500, 1.5),
            (MAX_TASK_TIMEOUT_MS, MAX_TASK_TIMEOUT_MS / 1000.0),
        ],
    )
    def test_accepted(self, wait_ms: int | None, seconds: float | None) -> None:
        assert wait_timeout_seconds(wait_ms) == Ok(seconds)

    @pytest.mark.parametrize(('wait_ms', 'message'), _REFUSED)
    def test_refused(self, wait_ms: object, message: str) -> None:
        assert wait_timeout_seconds(wait_ms) == Err(WaitTimeoutRefused(message=message))


def _task_handle() -> tuple[TaskHandle[int], MagicMock]:
    app = MagicMock()
    handle: TaskHandle[int] = TaskHandle(
        str(uuid.uuid4()), app=app, broker_mode=True, ok_type=int,
    )
    return handle, app.get_broker.return_value


class TestTaskHandle:
    @pytest.mark.parametrize(('wait_ms', 'message'), _REFUSED)
    def test_get_refuses_before_any_read(self, wait_ms: Any, message: str) -> None:
        handle, broker = _task_handle()

        result = handle.get(wait_ms=wait_ms)

        assert isinstance(result, Err) and isinstance(result.err_value, ReadInvalidInput), result
        assert message in result.err_value.message
        broker.get_raw_result_record.assert_not_called()

    @pytest.mark.parametrize(('wait_ms', 'message'), _REFUSED)
    def test_get_async_refuses_before_any_read(
        self, wait_ms: Any, message: str,
    ) -> None:
        handle, broker = _task_handle()

        result = asyncio.run(handle.get_async(wait_ms=wait_ms))

        assert isinstance(result, Err) and isinstance(result.err_value, ReadInvalidInput), result
        assert message in result.err_value.message
        broker.get_raw_result_record_async.assert_not_called()

    def test_a_refusal_is_not_cached(self) -> None:
        handle, _ = _task_handle()

        _ = handle.get(wait_ms=-1)

        assert handle._cached_result is None  # pyright: ignore[reportPrivateUsage]


class TestWorkflowHandle:
    @pytest.mark.parametrize(('wait_ms', 'message'), _REFUSED)
    def test_get_async_refuses_before_any_read(
        self, wait_ms: Any, message: str,
    ) -> None:
        broker = MagicMock()
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=uuid.uuid4(), broker=broker,
        )

        result = asyncio.run(handle.get_async(wait_ms=wait_ms))

        assert isinstance(result, Err) and isinstance(result.err_value, ReadInvalidInput), result
        assert message in result.err_value.message
        assert broker._core_handle.mock_calls == []


class TestBrokerAndApp:
    @pytest.mark.parametrize(('wait_ms', 'message'), _REFUSED)
    def test_broker_refuses_before_any_read(
        self, wait_ms: Any, message: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broker = PostgresBroker(PostgresConfig(database_url=_UNREACHABLE_URL))
        core_handle = MagicMock()
        monkeypatch.setattr(broker, '_core_handle', core_handle)

        result = asyncio.run(broker.get_raw_result_record_async('task-1', wait_ms))

        refused = result.unwrap_err()
        assert isinstance(refused, DatabaseInvalidInput)
        assert message in refused.message
        assert core_handle.mock_calls == []

    def test_app_get_raw_result_returns_the_broker_refusal(self) -> None:
        app = Horsies(AppConfig(database=PostgresConfig(database_url=_UNREACHABLE_URL)))

        result = asyncio.run(app.get_raw_result_async(uuid.uuid4(), wait_ms=-1))

        assert isinstance(result.unwrap_err(), DatabaseInvalidInput)

    def test_app_get_result_refuses_before_any_read(self) -> None:
        app = Horsies(AppConfig(database=PostgresConfig(database_url=_UNREACHABLE_URL)))

        result = asyncio.run(app.get_result_async(uuid.uuid4(), wait_ms=-1))

        assert isinstance(result, Err) and isinstance(result.err_value, ReadInvalidInput), result
