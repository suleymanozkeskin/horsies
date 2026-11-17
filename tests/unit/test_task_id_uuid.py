"""Task ids are ``uuid.UUID`` on the send path and in the scheduler."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from horsies.core.brokers.result_types import BrokerEnqueueFailed
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendEnqueueFailed
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.task_decorator import TaskHandle, create_task_wrapper
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.utils.fingerprint import schedule_slot_task_id

pytestmark = [pytest.mark.unit]


def _app_with_broker(broker: MagicMock) -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = 'default'
    app.get_broker.return_value = broker
    return app


def _wrapper(app: MagicMock) -> Any:
    def add_one(*, x: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=x + 1)

    return create_task_wrapper(add_one, app, 'test.add_one')


def _sent_task_id(broker: MagicMock) -> Any:
    return broker.enqueue.call_args.kwargs['task_id']


def _enqueued(*_args: object, task_id: UUID, **_kwargs: object) -> Ok[UUID]:
    return Ok(task_id)


def test_send_gives_the_broker_and_the_handle_one_minted_uuid() -> None:
    broker = MagicMock()
    broker.enqueue.side_effect = _enqueued

    result = _wrapper(_app_with_broker(broker)).send(x=1)

    assert is_ok(result)
    handle = result.ok_value
    assert isinstance(handle, TaskHandle)
    sent = _sent_task_id(broker)
    assert isinstance(sent, UUID)
    assert sent.version == 7
    assert handle.task_id == sent


def test_a_failed_send_carries_the_minted_uuid() -> None:
    broker = MagicMock()
    broker.enqueue.return_value = Err(BrokerEnqueueFailed(
        message='connection lost', retryable=True, exception=NoCause(),
    ))

    result = _wrapper(_app_with_broker(broker)).send(x=1)

    assert is_err(result)
    error = result.err_value
    assert isinstance(error, TaskSendEnqueueFailed)
    assert error.task_id == _sent_task_id(broker)
    assert isinstance(error.task_id, UUID)


def test_a_schedule_slot_task_id_is_a_version_7_uuid() -> None:
    slot = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)

    slot_task_id = schedule_slot_task_id('daily_report', slot)

    assert isinstance(slot_task_id, UUID)
    assert slot_task_id.version == 7
