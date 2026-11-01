"""The monitoring query error variants, the sites that build them, and the web mapping.

``MonitoringQueryError`` is a union with one frozen variant per
``MonitoringQueryErrorCode`` member. A variant's repr hides the message and
the cause. History data that does not decode is
``MonitoringQueryHistoryContractViolation`` in ``list_tasks`` and in both
detail reads. The query tests run on a mocked session.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import OperationalError, ProgrammingError

from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.pages import HistoryWindow
from horsies.core.types.result import is_err
from horsies.monitoring import queries as queries_module
from horsies.monitoring.errors import (
    MonitoringQueryDbOperationFailed,
    MonitoringQueryError,
    MonitoringQueryErrorCode,
    MonitoringQueryEventLoopLimit,
    MonitoringQueryHistoryContractViolation,
)
from horsies.monitoring.pages import TaskPage
from horsies.monitoring.queries import (
    get_task_detail,
    get_workflow_node,
    list_tasks,
)
from horsies.web.routes._common import query_failed
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-91af'
CAUSE_MARKER = 'cause-marker-91af'
TASK_ID = '0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718'
WORKFLOW_ID = '1c2d3e4f-5061-4b72-8c83-d4e5f6071829'
LOOP_LIMIT = EventLoopLimit(
    limit=5,
    held_thread_names=('MainThread',),
    current_thread_name='worker-1',
)


def _every_variant(cause: BaseException) -> list[MonitoringQueryError]:
    """One value of each variant; each cause field holds ``cause``."""
    message = f'failed: {MESSAGE_MARKER}'
    return [
        MonitoringQueryDbOperationFailed(message=message, retryable=True, exception=cause),
        MonitoringQueryEventLoopLimit(message=message, loop_limit=LOOP_LIMIT),
        MonitoringQueryHistoryContractViolation(message=message, exception=cause),
    ]


def _variant_id(error: MonitoringQueryError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in get_args(MonitoringQueryError.__value__)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(MonitoringQueryErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(get_args(MonitoringQueryError.__value__))

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError(CAUSE_MARKER)), ids=_variant_id,
    )
    def test_repr_hides_message_and_cause(self, error: MonitoringQueryError) -> None:
        text = repr(error)

        assert MESSAGE_MARKER not in text
        assert CAUSE_MARKER not in text
        assert type(error).__name__ in text

    def test_repr_shows_the_loop_limit(self) -> None:
        error = MonitoringQueryEventLoopLimit(message='x', loop_limit=LOOP_LIMIT)

        assert repr(LOOP_LIMIT) in repr(error)

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_only_a_database_failure_can_be_retryable(
        self, error: MonitoringQueryError,
    ) -> None:
        match error:
            case MonitoringQueryDbOperationFailed():
                assert error.retryable is True
            case _:
                assert error.retryable is False


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


def _session(*results: MagicMock | BaseException) -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=list(results))
    session.connection = AsyncMock(return_value=MagicMock())
    return session


def _refuse_history_read(monkeypatch: pytest.MonkeyPatch, cause: BaseException) -> None:
    monkeypatch.setattr(
        queries_module, 'staged_detail_published', AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        queries_module, 'read_task_detail', AsyncMock(side_effect=cause),
    )


class TestDatabaseFailure:
    @pytest.mark.parametrize(
        'cause,retryable',
        [
            (OperationalError('SELECT 1', {}, Exception('connection lost')), True),
            (ProgrammingError('SELECT 1', {}, Exception('syntax error')), False),
        ],
        ids=['operational', 'programming'],
    )
    @pytest.mark.asyncio
    async def test_the_cause_is_kept_and_classified(
        self, cause: BaseException, retryable: bool,
    ) -> None:
        broker = _broker_with_session(_session(cause))

        result = await get_task_detail(broker, TASK_ID)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryDbOperationFailed)
        assert error.exception is cause
        assert error.retryable is retryable
        assert error.message.startswith('task detail query failed: ')


class TestHistoryContractViolation:
    @pytest.mark.asyncio
    async def test_task_detail_returns_the_variant_with_the_cause(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cause = HistoryContractError(CAUSE_MARKER)
        _refuse_history_read(monkeypatch, cause)
        broker = _broker_with_session(_session(_scalar_result(None)))

        result = await get_task_detail(broker, TASK_ID)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert error.exception is cause
        assert error.message == f'task detail history read failed: {CAUSE_MARKER}'

    @pytest.mark.asyncio
    async def test_workflow_node_returns_the_variant_with_the_cause(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cause = HistoryContractError(CAUSE_MARKER)
        _refuse_history_read(monkeypatch, cause)
        node = MagicMock()
        node.task_id = TASK_ID
        broker = _broker_with_session(
            _session(_scalar_result(node), _scalar_result(None)),
        )

        result = await get_workflow_node(broker, WORKFLOW_ID, 0)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert error.exception is cause
        assert error.message == f'workflow node history read failed: {CAUSE_MARKER}'

    @pytest.mark.asyncio
    async def test_the_task_list_estimate_returns_the_variant_with_the_cause(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cause = HistoryContractError(CAUSE_MARKER)
        monkeypatch.setattr(
            queries_module, '_estimated_task_total', AsyncMock(return_value=0),
        )
        monkeypatch.setattr(
            queries_module, '_estimated_history_total', AsyncMock(side_effect=cause),
        )
        broker = _broker_with_session(_session())
        upper = datetime(2026, 11, 1, tzinfo=timezone.utc)

        result = await list_tasks(
            broker,
            window=HistoryWindow(lower=upper - timedelta(days=1), upper=upper),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            page=TaskPage(offset=0, limit=50),
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert error.exception is cause
        assert error.message == f'task list estimate decode failed: {CAUSE_MARKER}'


class TestWebMapping:
    @pytest.mark.parametrize(
        'error,status_code',
        list(zip(_every_variant(RuntimeError('x')), [503, 503, 500], strict=True)),
        ids=[_variant_id(error) for error in _every_variant(RuntimeError('x'))],
    )
    def test_each_variant_maps_to_its_status(
        self, error: MonitoringQueryError, status_code: int,
    ) -> None:
        response = query_failed('Task detail', error)

        assert isinstance(response, HTTPException)
        assert response.status_code == status_code
        assert response.detail == f'Task detail query failed: {error.message}'
