"""``list_tasks`` over the rows of ``horsies_monitoring_task_list``.

The read sends one statement; a double answers it with the function's rows
(a TOTALS row, then the TASK rows in page order). One test per outcome: the
three total kinds, a history estimate that does not decode, the mapping of
a live and of a history row, the arguments of the statement, a database
error and the event-loop limit.

Since the monitoring switch the production read runs in the core; this file
tests Python's body before the switch, the dual-run reference
(``tests/dualrun/reference/monitoring_queries.py``), which specifies the
answers that the dual-run pairs hold the core to. The production facade is
tested on a mocked core handle in ``tests/unit/test_monitoring_reads_in_core.py``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Final
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError

from horsies.core.history.errors import HistoryContractError
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring import (
    ErrorCategory,
    MonitoringQueryDbOperationFailed,
    MonitoringQueryEventLoopLimit,
    MonitoringQueryHistoryContractViolation,
    MonitoringWindow,
    TaskListPage,
    TaskPage,
)
from horsies.monitoring.errors import MonitoringResult
from horsies.monitoring.queries import SortDirection
from tests.broker_resources_support import attach_loop_limit
from tests.dualrun.reference.monitoring_queries import list_tasks
from tests.monitoring_function_support import (
    DETAIL_TIME,
    FunctionRow,
    function_broker,
    list_task_row,
    list_totals_row,
)

pytestmark = [pytest.mark.unit]

_WINDOW: Final = MonitoringWindow(lower=DETAIL_TIME - timedelta(days=1), upper=DETAIL_TIME)
_PLAN_OF_7_ROWS: Final = '[{"Plan": {"Node Type": "Seq Scan", "Plan Rows": 7}}]'


async def _list(
    broker: MagicMock,
    *,
    statuses: list[TaskStatus] | None = None,
    sort_dir: SortDirection = 'desc',
    page: TaskPage | None = None,
) -> MonitoringResult[TaskListPage]:
    return await list_tasks(
        broker,
        window=_WINDOW,
        statuses=statuses or [],
        task_names=[],
        queues=['fast'],
        workers=[],
        error_codes=[],
        error_categories=[ErrorCategory.DOMAIN],
        retried_only=True,
        sort_by='task_name',
        sort_dir=sort_dir,
        page=page or TaskPage(offset=0, limit=50),
    )


async def _page(rows: list[FunctionRow]) -> TaskListPage:
    broker, _ = function_broker(rows)
    match await _list(broker):
        case Ok(page):
            return page
        case Err(error):
            raise AssertionError(f'the list failed: {error}')


class TestTotals:
    @pytest.mark.asyncio
    async def test_exact_totals_add_the_live_and_the_history_count(self) -> None:
        page = await _page([list_totals_row('EXACT', live_total=3, history_total=4)])

        assert page == TaskListPage(rows=[], total=7)

    @pytest.mark.asyncio
    async def test_an_empty_history_adds_nothing_to_the_live_estimate(self) -> None:
        page = await _page(
            [list_totals_row('ESTIMATE_HISTORY_EMPTY', live_total=5, history_total=0)],
        )

        assert page.total == 5

    @pytest.mark.asyncio
    async def test_a_history_plan_adds_its_top_row_estimate(self) -> None:
        page = await _page(
            [
                list_totals_row(
                    'ESTIMATE_HISTORY_PLAN', live_total=5, history_total=None, plan=_PLAN_OF_7_ROWS,
                ),
            ],
        )

        assert page.total == 12

    @pytest.mark.asyncio
    async def test_a_history_plan_that_does_not_decode_is_a_contract_violation(self) -> None:
        broker, _ = function_broker(
            [
                list_totals_row(
                    'ESTIMATE_HISTORY_PLAN', live_total=5, history_total=None, plan='[{"Plan": {}}]',
                ),
            ],
        )

        result = await _list(broker)

        match result:
            case Err(MonitoringQueryHistoryContractViolation() as error):
                assert isinstance(error.exception, HistoryContractError)
                assert error.retryable is False
                assert error.message == (
                    'task list estimate decode failed: '
                    'EXPLAIN payload did not carry a top plan row estimate'
                )
            case _:
                raise AssertionError(f'expected a contract violation, got {result!r}')


class TestRows:
    @pytest.mark.asyncio
    async def test_rows_keep_the_order_of_the_function(self) -> None:
        ids = [uuid.uuid4() for _ in range(3)]

        page = await _page(
            [
                list_totals_row('EXACT', live_total=1, history_total=2),
                list_task_row('HISTORY', str(ids[0]), row_position=1),
                list_task_row('LIVE', str(ids[1]), row_position=2),
                list_task_row('HISTORY', str(ids[2]), row_position=3),
            ],
        )

        assert [row.id for row in page.rows] == ids

    @pytest.mark.asyncio
    async def test_a_live_row_has_no_end_time_and_its_open_span_counts_up(self) -> None:
        task_id = uuid.uuid4()
        started = datetime.now(UTC) - timedelta(seconds=30)

        page = await _page(
            [
                list_totals_row('EXACT', live_total=1, history_total=0),
                list_task_row(
                    'LIVE',
                    str(task_id),
                    status='RUNNING',
                    worker_id='worker-1',
                    worker_hostname='host-1',
                    enqueued_at=started - timedelta(seconds=10),
                    started_at=started,
                ),
            ],
        )

        [row] = page.rows
        assert (row.id, row.status, row.worker_id, row.worker_hostname) == (
            task_id, 'RUNNING', 'worker-1', 'host-1',
        )
        assert (row.error_code, row.error_category, row.completed_at, row.failed_at) == (
            None, None, None, None,
        )
        assert row.queue_s == 10
        assert row.exec_s is not None and 30 <= row.exec_s < 600

    @pytest.mark.asyncio
    async def test_a_completed_history_row_ends_in_completed_at(self) -> None:
        page = await _page(
            [
                list_totals_row('EXACT', live_total=0, history_total=1),
                list_task_row(
                    'HISTORY',
                    str(uuid.uuid4()),
                    enqueued_at=DETAIL_TIME - timedelta(seconds=30),
                    started_at=DETAIL_TIME - timedelta(seconds=20),
                ),
            ],
        )

        [row] = page.rows
        assert (row.status, row.completed_at, row.failed_at) == ('COMPLETED', DETAIL_TIME, None)
        assert (row.queue_s, row.exec_s) == (10, 20)

    @pytest.mark.asyncio
    async def test_a_failed_history_row_ends_in_failed_at_with_its_error_family(self) -> None:
        page = await _page(
            [
                list_totals_row('EXACT', live_total=0, history_total=1),
                list_task_row(
                    'HISTORY', str(uuid.uuid4()), status='FAILED', error_code='TASK_EXCEPTION',
                ),
            ],
        )

        [row] = page.rows
        assert (row.status, row.completed_at, row.failed_at) == ('FAILED', None, DETAIL_TIME)
        assert (row.error_code, row.error_category) == (
            'TASK_EXCEPTION', ErrorCategory.OPERATIONAL.value,
        )
        # Never started: the queue span closes at the terminal time.
        assert (row.queue_s, row.exec_s) == (0, None)

    @pytest.mark.asyncio
    async def test_an_empty_history_error_code_is_absent(self) -> None:
        page = await _page(
            [
                list_totals_row('EXACT', live_total=0, history_total=1),
                list_task_row('HISTORY', str(uuid.uuid4()), status='FAILED', error_code=''),
            ],
        )

        [row] = page.rows
        assert (row.error_code, row.error_category) == (None, None)


class TestStatement:
    @pytest.mark.parametrize(('sort_dir', 'direction'), [('asc', 'ASC'), ('desc', 'DESC')])
    @pytest.mark.asyncio
    async def test_one_statement_carries_the_filters_the_sort_and_the_page(
        self, sort_dir: SortDirection, direction: str,
    ) -> None:
        broker, connection = function_broker(
            [list_totals_row('EXACT', live_total=0, history_total=0)],
        )

        result = await _list(
            broker,
            statuses=[TaskStatus.FAILED],
            sort_dir=sort_dir,
            page=TaskPage(offset=40, limit=20),
        )

        assert result == Ok(TaskListPage(rows=[], total=0))
        connection.execute.assert_awaited_once()
        statement, parameters = connection.execute.await_args.args
        assert 'horsies_monitoring_task_list(' in str(statement)
        assert {
            name: parameters[name]
            for name in (
                'window_lower', 'window_upper', 'statuses', 'queue_names', 'error_categories',
                'retried_only', 'sort_key', 'sort_direction', 'page_offset', 'page_limit',
            )
        } == {
            'window_lower': _WINDOW.lower,
            'window_upper': _WINDOW.upper,
            'statuses': ['FAILED'],
            'queue_names': ['fast'],
            'error_categories': ['DOMAIN'],
            'retried_only': True,
            'sort_key': 'task_name',
            'sort_direction': direction,
            'page_offset': 40,
            'page_limit': 20,
        }


class TestFailures:
    @pytest.mark.asyncio
    async def test_a_database_error_is_a_database_failure_with_its_cause(self) -> None:
        cause = OperationalError('SELECT 1', {}, Exception('connection lost'))
        broker, _ = function_broker(cause)

        result = await _list(broker)

        match result:
            case Err(MonitoringQueryDbOperationFailed() as error):
                assert error.exception is cause
                assert error.retryable is True
                assert error.message.startswith('task list query failed: ')
            case _:
                raise AssertionError(f'expected a database failure, got {result!r}')

    @pytest.mark.asyncio
    async def test_the_event_loop_limit_runs_nothing(self) -> None:
        broker = MagicMock()
        loop_limit = attach_loop_limit(broker)

        result = await _list(broker)

        match result:
            case Err(MonitoringQueryEventLoopLimit() as error):
                assert error.loop_limit is loop_limit
                assert error.message == f'task list query: {loop_limit.describe()}'
            case _:
                raise AssertionError(f'expected the loop limit, got {result!r}')
