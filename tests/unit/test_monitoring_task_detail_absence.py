"""``get_task_detail`` names the absence of a task, and reads one snapshot.

No live row and no history row: ``Ok(TaskDetailNotFound)`` with the
``TaskAbsenceReason`` that the staged read classifies. The detail reads
begin in REPEATABLE READ before the first statement, so the staged read
cannot find live a task that the live read missed: that report is a
contract violation. The web route answers 404 for each reason.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from fastapi import HTTPException

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.detail import LiveTaskLocation, TaskDetailAbsent
from horsies.monitoring import queries as queries_module
from horsies.monitoring.errors import MonitoringQueryHistoryContractViolation
from horsies.monitoring.models import TaskAbsenceReason, TaskDetailNotFound
from horsies.monitoring.queries import get_task_detail, get_workflow_node
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.web.routes import tasks as tasks_routes
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

TASK_ID = '0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2b'
WORKFLOW_ID = '7a8b9c0d-1e2f-4a3b-8c4d-5e6f7a8b9c0d'
REPEATABLE_READ_BEGIN = call.connection(
    execution_options={'isolation_level': 'REPEATABLE READ'},
)


def _broker_with_session(session: AsyncMock) -> MagicMock:
    session_ctx = AsyncMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    attach_resources(
        broker, fake_resources(session_factory=MagicMock(return_value=session_ctx)),
    )
    return broker


def _scalar_result(value: Any) -> MagicMock:
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def _session(*results: MagicMock) -> AsyncMock:
    """A session whose statements return ``results`` in order.

    ``session.connection`` and ``session.execute`` share one recorder, so a
    test can read the order of the calls.
    """
    recorder = MagicMock()
    recorder.connection = AsyncMock(return_value=MagicMock())
    recorder.execute = AsyncMock(side_effect=list(results))
    session = AsyncMock()
    session.connection = recorder.connection
    session.execute = recorder.execute
    session.recorder = recorder
    return session


def _staged_read(
    monkeypatch: pytest.MonkeyPatch,
    *,
    published: bool,
    detail: LiveTaskLocation | TaskDetailAbsent | None = None,
) -> None:
    monkeypatch.setattr(
        queries_module, 'staged_detail_published', AsyncMock(return_value=published),
    )
    monkeypatch.setattr(
        queries_module, 'read_task_detail', AsyncMock(return_value=detail),
    )


class TestTaskDetailNotFound:
    @pytest.mark.asyncio
    async def test_a_non_uuid_id_is_not_classified_and_reads_nothing(self) -> None:
        broker = MagicMock()

        result = await get_task_detail(broker, 'not-a-task-id')

        assert result == Ok(TaskDetailNotFound(
            task_id='not-a-task-id', reason=TaskAbsenceReason.NOT_CLASSIFIED,
        ))
        broker.select_resources.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_database_without_the_staged_read_is_not_classified(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _staged_read(monkeypatch, published=False)
        broker = _broker_with_session(_session(_scalar_result(None)))

        result = await get_task_detail(broker, TASK_ID)

        assert result == Ok(TaskDetailNotFound(
            task_id=TASK_ID, reason=TaskAbsenceReason.NOT_CLASSIFIED,
        ))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('predates', 'reason'),
        [
            (True, TaskAbsenceReason.PREDATES_RETAINED_HISTORY),
            (False, TaskAbsenceReason.WITHIN_RETAINED_HISTORY),
            (None, TaskAbsenceReason.NOT_CLASSIFIED),
        ],
        ids=['predates', 'within', 'unclassified'],
    )
    async def test_the_reason_is_the_staged_classification(
        self,
        monkeypatch: pytest.MonkeyPatch,
        predates: bool | None,
        reason: TaskAbsenceReason,
    ) -> None:
        _staged_read(
            monkeypatch,
            published=True,
            detail=TaskDetailAbsent(task_id=TASK_ID, predates_retained_floor=predates),
        )
        broker = _broker_with_session(_session(_scalar_result(None)))

        result = await get_task_detail(broker, TASK_ID)

        assert result == Ok(TaskDetailNotFound(task_id=TASK_ID, reason=reason))


class TestLiveInTheSnapshotOfTheLiveMiss:
    @pytest.mark.asyncio
    async def test_task_detail_is_a_contract_violation(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _staged_read(
            monkeypatch, published=True, detail=LiveTaskLocation(task_id=TASK_ID),
        )
        broker = _broker_with_session(_session(_scalar_result(None)))

        result = await get_task_detail(broker, TASK_ID)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)
        assert TASK_ID in error.message
        assert 'live in the snapshot' in error.message

    @pytest.mark.asyncio
    async def test_workflow_node_is_a_contract_violation(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _staged_read(
            monkeypatch, published=True, detail=LiveTaskLocation(task_id=TASK_ID),
        )
        node = MagicMock()
        node.task_id = TASK_ID
        broker = _broker_with_session(
            _session(_scalar_result(node), _scalar_result(None)),
        )

        result = await get_workflow_node(broker, WORKFLOW_ID, 0)

        assert is_err(result)
        assert isinstance(result.err_value, MonitoringQueryHistoryContractViolation)


class TestOneSnapshot:
    @pytest.mark.asyncio
    async def test_task_detail_begins_in_repeatable_read(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _staged_read(monkeypatch, published=False)
        session = _session(_scalar_result(None))
        broker = _broker_with_session(session)

        _ = await get_task_detail(broker, TASK_ID)

        assert session.recorder.mock_calls[0] == REPEATABLE_READ_BEGIN

    @pytest.mark.asyncio
    async def test_workflow_node_begins_in_repeatable_read(self) -> None:
        session = _session(_scalar_result(None))
        broker = _broker_with_session(session)

        result = await get_workflow_node(broker, WORKFLOW_ID, 0)

        assert is_ok(result)
        assert session.recorder.mock_calls[0] == REPEATABLE_READ_BEGIN


def _read_task_route() -> Any:
    router = tasks_routes.build_router(MagicMock(), stats_cache=MagicMock())
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
