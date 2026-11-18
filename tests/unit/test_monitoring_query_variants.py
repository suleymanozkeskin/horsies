"""The monitoring query error variants, the sites that build them, and the web mapping.

``MonitoringQueryError`` is a union with one frozen variant per
``MonitoringQueryErrorCode`` member. A variant's repr hides the message and
the cause. History data that does not decode is
``MonitoringQueryHistoryContractViolation`` in ``list_tasks`` (the EXPLAIN
text of the history estimate) and in both detail reads. The query tests run
on a double of the one function statement.

Since the monitoring switch every read runs in the core; the cases here
test Python's bodies before the switch, the
dual-run reference (``tests/dualrun/reference/monitoring_queries.py``). The
production facades are tested on a mocked core handle in
``tests/unit/test_monitoring_reads_in_core.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from uuid import UUID

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import OperationalError, ProgrammingError

from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.history.errors import HistoryContractError
from horsies.monitoring.history_window import MonitoringWindow
from horsies.core.types.result import is_err, is_ok
from horsies.monitoring.errors import (
    MonitoringQueryDbOperationFailed,
    MonitoringQueryError,
    MonitoringQueryErrorCode,
    MonitoringQueryEventLoopLimit,
    MonitoringQueryHistoryContractViolation,
)
from horsies.monitoring.pages import TaskPage
from tests.dualrun.reference import monitoring_queries as python_reads
from tests.dualrun.reference.monitoring_queries import (
    get_task_detail,
    get_workflow_node,
    list_tasks,
)
from tests.dualrun.reference.web_routes._common import query_failed
from tests.monitoring_function_support import (
    function_broker,
    history_row,
    list_totals_row,
    node_row,
)
from tests.union_members import union_members

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-91af'
CAUSE_MARKER = 'cause-marker-91af'
TASK_ID = UUID('0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718')
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
        codes = [variant.code for variant in union_members(MonitoringQueryError)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(MonitoringQueryErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(union_members(MonitoringQueryError))

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
        broker, _ = function_broker(cause)

        result = await get_task_detail(broker, TASK_ID)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryDbOperationFailed)
        assert error.exception is cause
        assert error.retryable is retryable
        assert error.message.startswith('task detail query failed: ')


class TestHistoryContractViolation:
    @pytest.mark.asyncio
    async def test_task_detail_returns_the_variant_with_the_cause(self) -> None:
        broker, _ = function_broker([history_row(str(TASK_ID), status=CAUSE_MARKER)])

        result = await get_task_detail(broker, TASK_ID)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)
        assert error.message == (
            f'task detail history read failed: history row {TASK_ID} has status '
            f"'{CAUSE_MARKER}', which is not a task status"
        )

    @pytest.mark.asyncio
    async def test_workflow_node_returns_the_variant_with_the_cause(self) -> None:
        broker, _ = function_broker(
            [node_row(0, history_row(str(TASK_ID), status=CAUSE_MARKER))],
        )

        result = await get_workflow_node(broker, WORKFLOW_ID, 0)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)
        assert error.message == (
            f'workflow node history read failed: history row {TASK_ID} has status '
            f"'{CAUSE_MARKER}', which is not a task status"
        )

    @pytest.mark.asyncio
    async def test_an_empty_stored_node_result_returns_the_variant_naming_the_node(
        self,
    ) -> None:
        """No writer stores an empty result text: it is corrupt data, not "no
        result", as the core's ``node_error_from_result`` reads it. NULL is
        no result."""
        broker, _ = function_broker([node_row(3, None, node_result='')])

        result = await get_workflow_node(broker, WORKFLOW_ID, 3)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)
        assert error.message == (
            'workflow node history read failed: workflow node 3: the stored result '
            'is not JSON: JSON parse failed: the text is empty'
        )

        null_broker, _ = function_broker([node_row(3, None, node_result=None)])
        null_result = await get_workflow_node(null_broker, WORKFLOW_ID, 3)

        assert is_ok(null_result)
        assert null_result.ok_value is not None
        assert null_result.ok_value.node_error is None

    @pytest.mark.asyncio
    async def test_the_task_list_estimate_returns_the_variant_with_the_cause(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cause = HistoryContractError(CAUSE_MARKER)

        def refuse(payload: object) -> int:
            raise cause

        monkeypatch.setattr(python_reads, 'plan_rows_from_explain', refuse)
        broker, _ = function_broker(
            [
                list_totals_row(
                    'ESTIMATE_HISTORY_PLAN', live_total=0, history_total=None, plan='[]',
                ),
            ],
        )
        upper = datetime(2026, 11, 1, tzinfo=timezone.utc)

        result = await list_tasks(
            broker,
            window=MonitoringWindow(lower=upper - timedelta(days=1), upper=upper),
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
