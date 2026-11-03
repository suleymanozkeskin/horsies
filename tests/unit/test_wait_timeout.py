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
from horsies.core.brokers.result_types import BrokerInvalidInput
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import MAX_TASK_TIMEOUT_MS, RetrievalCode
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

_UNREACHABLE_URL = SecretStr('postgresql+psycopg://user:pass@127.0.0.1:1/none')

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
        ('timeout_ms', 'seconds'),
        [
            (None, None),
            (0, 0.0),
            (1_500, 1.5),
            (MAX_TASK_TIMEOUT_MS, MAX_TASK_TIMEOUT_MS / 1000.0),
        ],
    )
    def test_accepted(self, timeout_ms: int | None, seconds: float | None) -> None:
        assert wait_timeout_seconds(timeout_ms) == Ok(seconds)

    @pytest.mark.parametrize(('timeout_ms', 'message'), _REFUSED)
    def test_refused(self, timeout_ms: object, message: str) -> None:
        assert wait_timeout_seconds(timeout_ms) == Err(WaitTimeoutRefused(message=message))


def _task_handle() -> tuple[TaskHandle[int], MagicMock]:
    app = MagicMock()
    handle: TaskHandle[int] = TaskHandle(
        str(uuid.uuid4()), app=app, broker_mode=True, ok_type=int,
    )
    return handle, app.get_broker.return_value


class TestTaskHandle:
    @pytest.mark.parametrize(('timeout_ms', 'message'), _REFUSED)
    def test_get_refuses_before_any_read(self, timeout_ms: Any, message: str) -> None:
        handle, broker = _task_handle()

        result = handle.get(timeout_ms=timeout_ms)

        assert result.unwrap_err().error_code is RetrievalCode.INVALID_INPUT
        assert message in (result.unwrap_err().message or '')
        broker.get_raw_result_record.assert_not_called()

    @pytest.mark.parametrize(('timeout_ms', 'message'), _REFUSED)
    def test_get_async_refuses_before_any_read(
        self, timeout_ms: Any, message: str,
    ) -> None:
        handle, broker = _task_handle()

        result = asyncio.run(handle.get_async(timeout_ms=timeout_ms))

        assert result.unwrap_err().error_code is RetrievalCode.INVALID_INPUT
        assert message in (result.unwrap_err().message or '')
        broker.get_raw_result_record_async.assert_not_called()

    def test_a_refusal_is_not_cached(self) -> None:
        handle, _ = _task_handle()

        _ = handle.get(timeout_ms=-1)

        assert handle._result_fetched is False  # pyright: ignore[reportPrivateUsage]


class TestWorkflowHandle:
    @pytest.mark.parametrize(('timeout_ms', 'message'), _REFUSED)
    def test_get_async_refuses_before_any_read(
        self, timeout_ms: Any, message: str,
    ) -> None:
        broker = MagicMock()
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=str(uuid.uuid4()), broker=broker,
        )

        result = asyncio.run(handle.get_async(timeout_ms=timeout_ms))

        assert result.unwrap_err().error_code is RetrievalCode.INVALID_INPUT
        assert message in (result.unwrap_err().message or '')
        broker.select_resources.assert_not_called()


class TestBrokerAndApp:
    @pytest.mark.parametrize(('timeout_ms', 'message'), _REFUSED)
    def test_broker_refuses_before_any_read(
        self, timeout_ms: Any, message: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broker = PostgresBroker(PostgresConfig(database_url=_UNREACHABLE_URL))
        select_resources = MagicMock()
        monkeypatch.setattr(broker, 'select_resources', select_resources)

        result = asyncio.run(broker.get_raw_result_record_async('task-1', timeout_ms))

        refused = result.unwrap_err()
        assert isinstance(refused, BrokerInvalidInput)
        assert message in refused.message
        select_resources.assert_not_called()

    @pytest.mark.parametrize('method', ['get_result_async', 'get_raw_result_async'])
    def test_app_returns_the_broker_refusal(self, method: str) -> None:
        app = Horsies(AppConfig(broker=PostgresConfig(database_url=_UNREACHABLE_URL)))

        result = asyncio.run(getattr(app, method)('task-1', timeout_ms=-1))

        assert isinstance(result.unwrap_err(), BrokerInvalidInput)
