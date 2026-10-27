"""Monitoring reads and task actions return EVENT_LOOP_CONFLICT when the
broker's resources are in use by another event loop. Nothing runs.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from horsies.core.types.result import is_err
from horsies.monitoring.errors import MonitoringQueryErrorCode
from horsies.monitoring.queries import list_schedules
from horsies.monitoring.task_actions import TaskActionErrorCode, cancel_task
from tests.broker_resources_support import attach_conflict

pytestmark = [pytest.mark.unit]


@pytest.mark.asyncio
async def test_query_maps_conflict_to_monitoring_conflict() -> None:
    broker = MagicMock()
    conflict = attach_conflict(broker)

    result = await list_schedules(broker)

    assert is_err(result)
    error = result.err_value
    assert error.code is MonitoringQueryErrorCode.EVENT_LOOP_CONFLICT
    assert error.retryable is False
    assert error.exception is None
    assert error.message == f'schedule state query: {conflict.describe()}'
    broker.select_resources.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_maps_conflict_to_task_action_conflict() -> None:
    broker = MagicMock()
    conflict = attach_conflict(broker)
    task_id = str(uuid.uuid4())

    result = await cancel_task(broker, task_id)

    assert is_err(result)
    error = result.err_value
    assert error.code is TaskActionErrorCode.EVENT_LOOP_CONFLICT
    assert error.retryable is False
    assert error.task_id == task_id
    assert error.current_status is None
    assert error.message == f'Task cancel of {task_id}: {conflict.describe()}'
    broker.select_resources.assert_awaited_once()
