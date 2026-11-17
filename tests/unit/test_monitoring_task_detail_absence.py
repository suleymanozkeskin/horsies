"""``get_task_detail`` names the absence of a task, and reads one snapshot.

No live row and no history row: ``Ok(TaskDetailNotFound)`` with the
``TaskAbsenceReason`` that ``horsies_monitoring_task_detail`` classifies.
Both detail reads send one statement in autocommit, so every part comes
from one snapshot; the function raises ``data_corrupted`` when the history
reader finds live a task that the live read missed, and that is a contract
violation. The web route answers 404 for each reason.

Since the monitoring switch the production reads run in the core; the read
cases test Python's body before the switch, the dual-run reference
(``tests/dualrun/reference/monitoring_queries.py``), which specifies the
answers that the dual-run pairs hold the core to. The production facade is
tested on a mocked core handle in ``tests/unit/test_monitoring_reads_in_core.py``.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException

from horsies.core.history.errors import HistoryContractError
from horsies.core.utils.db import AUTOCOMMIT_EXECUTION_OPTIONS
from horsies.monitoring.errors import MonitoringQueryHistoryContractViolation
from horsies.monitoring.models import TaskAbsenceReason, TaskDetailNotFound
from horsies.core.types.result import Err, Ok, is_err
from tests.dualrun.reference.web_routes import tasks as tasks_routes
from tests.dualrun.reference.monitoring_queries import get_task_detail, get_workflow_node
from tests.monitoring_function_support import (
    absent_row,
    data_corrupted,
    detail_row,
    function_broker,
    node_row,
)

pytestmark = [pytest.mark.unit]

TASK_ID = UUID('0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2b')
WORKFLOW_ID = '7a8b9c0d-1e2f-4a3b-8c4d-5e6f7a8b9c0d'
LIVE_MISS_MESSAGE = (
    f'horsies_monitoring_task_detail: the published reader reports task {TASK_ID} '
    'at location LIVE after a live miss in the same snapshot'
)


class TestTaskDetailNotFound:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'reason',
        list(TaskAbsenceReason),
        ids=lambda reason: reason.value,
    )
    async def test_the_reason_is_the_function_classification(
        self, reason: TaskAbsenceReason,
    ) -> None:
        broker, _ = function_broker([absent_row(str(TASK_ID), reason.value)])

        result = await get_task_detail(broker, TASK_ID)

        assert result == Ok(TaskDetailNotFound(task_id=TASK_ID, reason=reason))


class TestLiveInTheSnapshotOfTheLiveMiss:
    @pytest.mark.asyncio
    async def test_task_detail_is_a_contract_violation(self) -> None:
        broker, _ = function_broker(data_corrupted(LIVE_MISS_MESSAGE))

        result = await get_task_detail(broker, TASK_ID)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)
        assert str(TASK_ID) in error.message
        assert 'after a live miss in the same snapshot' in error.message

    @pytest.mark.asyncio
    async def test_workflow_node_is_a_contract_violation(self) -> None:
        broker, _ = function_broker(data_corrupted(LIVE_MISS_MESSAGE))

        result = await get_workflow_node(broker, WORKFLOW_ID, 0)

        assert is_err(result)
        assert isinstance(result.err_value, MonitoringQueryHistoryContractViolation)


class TestOneStatement:
    @pytest.mark.asyncio
    async def test_task_detail_sends_one_statement_in_autocommit(self) -> None:
        broker, connection = function_broker([detail_row(str(TASK_ID))])

        _ = await get_task_detail(broker, TASK_ID)

        session = broker.select_resources.return_value.ok_value.session_factory.return_value
        opened = await session.__aenter__()
        opened.connection.assert_awaited_once_with(
            execution_options=dict(AUTOCOMMIT_EXECUTION_OPTIONS),
        )
        assert connection.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_workflow_node_sends_one_statement_in_autocommit(self) -> None:
        broker, connection = function_broker([node_row(0, detail_row(str(TASK_ID)))])

        _ = await get_workflow_node(broker, WORKFLOW_ID, 0)

        session = broker.select_resources.return_value.ok_value.session_factory.return_value
        opened = await session.__aenter__()
        opened.connection.assert_awaited_once_with(
            execution_options=dict(AUTOCOMMIT_EXECUTION_OPTIONS),
        )
        assert connection.execute.await_count == 1


def _read_task_route() -> Any:
    router = tasks_routes.build_router(MagicMock(), caches=MagicMock())
    for route in router.routes:
        if getattr(route, 'path', None) == '/tasks/{task_id}':
            return getattr(route, 'endpoint')
    raise AssertionError('the task detail route is not registered')


class TestWebRoute:
    @pytest.mark.parametrize(
        ('reason', 'text'),
        [
            (
                TaskAbsenceReason.PREDATES_RETAINED_HISTORY,
                'Task not found. Its id is older than the retained task history.',
            ),
            (TaskAbsenceReason.WITHIN_RETAINED_HISTORY, 'Task not found.'),
            (TaskAbsenceReason.NOT_CLASSIFIED, 'Task not found.'),
        ],
        ids=['predates', 'within', 'unclassified'],
    )
    def test_each_reason_is_not_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
        reason: TaskAbsenceReason,
        text: str,
    ) -> None:
        monkeypatch.setattr(
            tasks_routes,
            'get_task_detail',
            AsyncMock(return_value=Ok(TaskDetailNotFound(task_id=TASK_ID, reason=reason))),
        )

        with pytest.raises(HTTPException) as raised:
            asyncio.run(_read_task_route()(task_id=TASK_ID))

        assert raised.value.status_code == 404
        assert raised.value.detail == text

    def test_a_query_failure_is_not_a_not_found(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cause = HistoryContractError('bad row')
        monkeypatch.setattr(
            tasks_routes,
            'get_task_detail',
            AsyncMock(return_value=Err(MonitoringQueryHistoryContractViolation(
                message='task detail history read failed: bad row', exception=cause,
            ))),
        )

        with pytest.raises(HTTPException) as raised:
            asyncio.run(_read_task_route()(task_id=TASK_ID))

        assert raised.value.status_code != 404
