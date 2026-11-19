"""The monitoring reads that run in the core: the record of the core decides the answer.

Every read of ``horsies.monitoring`` calls the broker's core handle. The tests run on a mocked handle: the record of each call is the
seam. ``MonitoringJson`` is validated into the response model; a failure is
the core's generated class, which every read returns as it is; a core
boundary error of the call is a database failure. The decoding of the
function rows is the core's, and its tests cover it. The values that the
core builds into a failure class (message, retry rule, exception class and
text) are pinned through the built module in
``tests/integration/test_monitoring_queries.py`` and by the
core's own tests.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Final
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from horsies._core import (
    CoreConnectionError,
    CoreStartFailed,
    MonitoringJson,
    TaskBreakdownQuery,
    TaskDetailAbsent,
    TaskFacetsQuery,
    TaskListQuery,
    TaskStatsQuery,
    TooManyPendingCalls,
    WorkflowRunsQuery,
)
from horsies.core.history.errors import HistoryContractError
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring import (
    Breakdown,
    BreakdownLimit,
    BreakdownUnclaimedGroup,
    BreakdownValueGroup,
    ErrorCategory,
    Facets,
    MonitoringQueryDbOperationFailed,
    MonitoringQueryHistoryContractViolation,
    MonitoringResult,
    MonitoringWindow,
    TaskAbsenceReason,
    TaskDetail,
    TaskDetailNotFound,
    TaskListPage,
    TaskPage,
    ScheduleStateInfo,
    StatusCount,
    WorkflowRunDetail,
    WorkflowRunsLimit,
    WorkflowRunSummary,
    get_task_detail,
    get_workflow_node,
    get_workflow_run,
    list_schedules,
    list_tasks,
    list_workflow_names,
    list_workflow_runs,
    task_breakdown,
    task_facets,
    task_stats,
)

pytestmark = [pytest.mark.unit]

TASK_ID: Final = uuid.UUID('0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2b')
UPPER: Final = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
WINDOW: Final = MonitoringWindow(lower=UPPER - timedelta(days=1), upper=UPPER)
MESSAGE: Final = 'task list query failed: message-marker-4c1e'

PAGE_JSON: Final = (
    '{"rows": [{"id": "0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2b", "task_name": "t", '
    '"queue_name": "default", "status": "FAILED", "priority": 100, "retry_count": 1, '
    '"max_retries": 3, "is_workflow_task": false, "error_code": "ORDER_REJECTED", '
    '"error_category": "DOMAIN", "worker_hostname": "host-1", "worker_id": "worker-1", '
    '"enqueued_at": "2026-11-17T11:00:00Z", "started_at": "2026-11-17T11:00:05.250000Z", '
    '"completed_at": null, "failed_at": "2026-11-17T11:00:09Z", "queue_s": 5, "exec_s": 3}], '
    '"total": 7}'
)
FACETS_JSON: Final = (
    '{"workers": [{"value": "worker-1", "count": 2}], "task_names": [], '
    '"queues": [{"value": "default", "count": 2}], '
    '"error_codes": [{"value": "ORDER_REJECTED", "count": 1, "category": "DOMAIN"}], '
    '"error_category_totals": {"DOMAIN": 1}}'
)
DETAIL_JSON: Final = (
    '{"leaf": {"task_id": "0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2b", "status": "RUNNING", '
    '"error_code": null, "failed_reason": null, "retry_count": 0, "max_retries": 3, '
    '"enqueued_at": "2026-11-17T11:00:00Z", "started_at": "2026-11-17T11:00:01Z", '
    '"completed_at": null, "failed_at": null, "queue_s": 1, "exec_s": 42, '
    '"worker_hostname": "host-1", "good_until": null, "execution": "SHARED_THREAD", '
    '"trace_id": "0af7651916cd43dd8448eb211c80319c"}, '
    '"task_name": "t", "queue_name": "default", "priority": 100, "is_workflow_task": true, '
    '"error_category": null, "attempts": [], '
    '"workflow_id": "0190a1b2-c3d4-7e5f-8a6b-000000000001", "workflow_task_index": 2}'
)


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a core record: an instance of ``record_class`` for a
    class pattern, with ``fields``."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _broker(method: str, answer: object) -> tuple[MagicMock, AsyncMock]:
    """A broker whose core handle answers ``method`` with ``answer``: a record,
    or an exception that the call raises."""
    match answer:
        case BaseException():
            call = AsyncMock(side_effect=answer)
        case _:
            call = AsyncMock(return_value=answer)
    handle = MagicMock()
    setattr(handle, method, call)
    broker = MagicMock()
    broker._core_handle = handle
    return broker, call


def _only_argument(call: AsyncMock) -> object:
    """The one positional argument of the one await of ``call``."""
    call.assert_awaited_once()
    arguments: tuple[object, ...] = tuple(call.await_args_list[0].args)
    assert len(arguments) == 1, arguments
    return arguments[0]


async def _list(answer: object) -> tuple[MonitoringResult[TaskListPage], AsyncMock]:
    broker, call = _broker('monitoring_list_tasks_async', answer)
    result = await list_tasks(
        broker,
        window=WINDOW,
        statuses=[TaskStatus.FAILED],
        task_names=['t'],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[ErrorCategory.DOMAIN],
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        page=TaskPage(offset=0, limit=50),
    )
    return result, call


class TestAnswers:
    @pytest.mark.asyncio
    async def test_the_page_is_the_json_of_the_core_model(self) -> None:
        result, call = await _list(_record(MonitoringJson, json=PAGE_JSON))

        assert result == Ok(TaskListPage.model_validate_json(PAGE_JSON))
        match result:
            case Ok(TaskListPage(rows=[row], total=7)):
                assert row.id == TASK_ID
                assert row.started_at == datetime(2026, 11, 17, 11, 0, 5, 250000, tzinfo=UTC)
            case _:
                raise AssertionError(f'expected one row and the total: {result!r}')
        assert _only_argument(call).__class__ is TaskListQuery

    @pytest.mark.asyncio
    async def test_the_facets_are_the_json_of_the_core_model(self) -> None:
        broker, call = _broker('monitoring_task_facets_async', _record(MonitoringJson, json=FACETS_JSON))

        result = await task_facets(
            broker,
            window=WINDOW,
            statuses=[],
            error_categories=[ErrorCategory.DOMAIN],
            retried_only=True,
        )

        assert result == Ok(Facets.model_validate_json(FACETS_JSON))
        assert _only_argument(call).__class__ is TaskFacetsQuery

    @pytest.mark.asyncio
    async def test_the_detail_is_the_json_of_the_core_model_with_the_class(self) -> None:
        broker, call = _broker('monitoring_task_detail_async', _record(MonitoringJson, json=DETAIL_JSON))

        result = await get_task_detail(broker, TASK_ID)

        match result:
            case Ok(TaskDetail(leaf=leaf, workflow_id=workflow_id)):
                assert leaf.execution is Execution.SHARED_THREAD
                assert leaf.trace_id == '0af7651916cd43dd8448eb211c80319c'
                assert workflow_id == '0190a1b2-c3d4-7e5f-8a6b-000000000001'
            case _:
                raise AssertionError(f'expected the task: {result!r}')
        call.assert_awaited_once_with(TASK_ID)

    @pytest.mark.parametrize('reason', list(TaskAbsenceReason))
    @pytest.mark.asyncio
    async def test_an_absent_task_keeps_its_reason(self, reason: TaskAbsenceReason) -> None:
        broker, _ = _broker(
            'monitoring_task_detail_async',
            _record(TaskDetailAbsent, task_id=TASK_ID, reason=reason.value),
        )

        result = await get_task_detail(broker, TASK_ID)

        assert result == Ok(TaskDetailNotFound(task_id=TASK_ID, reason=reason))


def _core_failures() -> list[MonitoringQueryDbOperationFailed | MonitoringQueryHistoryContractViolation]:
    """One value of each class that the core builds for a failed read."""
    return [
        MonitoringQueryDbOperationFailed(
            message=MESSAGE,
            retryable=True,
            exception=CoreConnectionError('task list query', 'connection refused', True),
        ),
        MonitoringQueryHistoryContractViolation(
            message='task detail query failed: history row has status BOGUS',
            exception=HistoryContractError('history row has status BOGUS'),
        ),
    ]


#: Each read of ``horsies.monitoring``: its core method, and its call on a broker.
READS: Final[tuple[tuple[str, Callable[[MagicMock], Awaitable[MonitoringResult[Any]]]], ...]] = (
    (
        'monitoring_task_stats_async',
        lambda broker: task_stats(
            broker, window=WINDOW, task_names=[], queues=[], workers=[], error_codes=[],
            error_categories=[], retried_only=False,
        ),
    ),
    (
        'monitoring_task_facets_async',
        lambda broker: task_facets(
            broker, window=WINDOW, statuses=[], error_categories=[], retried_only=False,
        ),
    ),
    (
        'monitoring_task_breakdown_async',
        lambda broker: task_breakdown(
            broker, window=WINDOW, group_by='worker', statuses=[], task_names=[], queues=[],
            workers=[], error_codes=[], error_categories=[], retried_only=False,
            limit=BreakdownLimit(value=50),
        ),
    ),
    (
        'monitoring_list_tasks_async',
        lambda broker: list_tasks(
            broker, window=WINDOW, statuses=[], task_names=[], queues=[], workers=[],
            error_codes=[], error_categories=[], retried_only=False, sort_by='enqueued_at',
            sort_dir='desc', page=TaskPage(offset=0, limit=50),
        ),
    ),
    ('monitoring_task_detail_async', lambda broker: get_task_detail(broker, TASK_ID)),
    ('monitoring_workflow_names_async', lambda broker: list_workflow_names(broker)),
    (
        'monitoring_workflow_runs_async',
        lambda broker: list_workflow_runs(
            broker, name=None, status=None, limit=WorkflowRunsLimit(value=30),
        ),
    ),
    ('monitoring_workflow_run_async', lambda broker: get_workflow_run(broker, RUN_ID)),
    ('monitoring_workflow_node_async', lambda broker: get_workflow_node(broker, RUN_ID, 7)),
    ('monitoring_schedules_async', lambda broker: list_schedules(broker)),
)


class TestFailures:
    @pytest.mark.parametrize('read', READS, ids=[method for method, _ in READS])
    @pytest.mark.parametrize('failure', _core_failures(), ids=lambda failure: type(failure).__name__)
    @pytest.mark.asyncio
    async def test_every_read_returns_the_core_s_failure_as_it_is(
        self,
        read: tuple[str, Callable[[MagicMock], Awaitable[MonitoringResult[Any]]]],
        failure: MonitoringQueryDbOperationFailed | MonitoringQueryHistoryContractViolation,
    ) -> None:
        method, call = read
        broker, _ = _broker(method, failure)

        result = await call(broker)

        match result:
            case Err(error):
                assert error is failure
            case _:
                raise AssertionError(f'expected the core failure: {result!r}')

    @pytest.mark.asyncio
    async def test_a_core_text_that_does_not_match_the_model_raises_its_error(self) -> None:
        text = '{"rows": []}'
        with pytest.raises(ValidationError) as expected:
            _ = TaskListPage.model_validate_json(text)

        with pytest.raises(ValidationError) as raised:
            _ = await _list(_record(MonitoringJson, json=text))

        assert raised.value.title == expected.value.title == 'TaskListPage'
        assert raised.value.errors(include_url=False) == expected.value.errors(include_url=False)

    @pytest.mark.parametrize(
        ('raised', 'retryable'),
        [
            (TooManyPendingCalls('CoreHandle.monitoring_list_tasks_async', 65_536), True),
            (CoreStartFailed('the core runtime could not start'), False),
        ],
        ids=['too-many-calls', 'start-failed'],
    )
    @pytest.mark.asyncio
    async def test_a_core_boundary_error_is_a_database_failure(
        self, raised: BaseException, retryable: bool,
    ) -> None:
        result, _ = await _list(raised)

        match result:
            case Err(MonitoringQueryDbOperationFailed() as error):
                assert error.exception is raised
                assert error.retryable is retryable
                assert error.message.startswith('task list query failed: ')
            case _:
                raise AssertionError(f'expected a database failure: {result!r}')


RUN_ID: Final = '0190a1b2-c3d4-7e5f-8a6b-000000000001'
STATS_JSON: Final = '[{"status": "PENDING", "count": 2}, {"status": "FAILED", "count": 1}]'
BREAKDOWN_JSON: Final = (
    '{"group_by": "worker", "groups": [{"total": 2, "pending": 1, "claimed": 0, "running": 1, '
    '"completed": 0, "failed": 0, "cancelled": 0, "expired": 0, "retried": 0, '
    '"group": {"kind": "value", "value": "worker-1"}}, {"total": 1, "pending": 1, "claimed": 0, '
    '"running": 0, "completed": 0, "failed": 0, "cancelled": 0, "expired": 0, "retried": 0, '
    '"group": {"kind": "unclaimed"}}], "total": {"total": 3, "pending": 2, "claimed": 0, '
    '"running": 1, "completed": 0, "failed": 0, "cancelled": 0, "expired": 0, "retried": 0}, '
    '"group_count": 2}'
)
RUNS_JSON: Final = (
    '[{"id": "0190a1b2-c3d4-7e5f-8a6b-000000000001", "name": "alpha.flow", '
    '"definition_key": null, "status": "RUNNING", "created_at": "2026-11-17T11:00:00Z", '
    '"completed_at": null, "wall_s": 60}]'
)
RUN_JSON: Final = (
    '{"run": {"id": "0190a1b2-c3d4-7e5f-8a6b-000000000001", "name": "alpha.flow", '
    '"definition_key": null, "status": "RUNNING", "created_at": "2026-11-17T11:00:00Z", '
    '"completed_at": null, "wall_s": 60}, "nodes": [], "edges": [], "failed_count": 0, '
    '"failed_indices": [], "trace_id": null}'
)
SCHEDULES_JSON: Final = (
    '[{"schedule_name": "nightly", "last_run_at": null, "next_run_at": "2026-11-18T00:00:00Z", '
    '"last_task_id": null, "run_count": 0, "updated_at": "2026-11-17T11:00:00Z"}]'
)


class TestAggregateAndWorkflowAnswers:
    @pytest.mark.asyncio
    async def test_the_stats_are_the_json_list_of_the_core(self) -> None:
        broker, call = _broker('monitoring_task_stats_async', _record(MonitoringJson, json=STATS_JSON))

        result = await task_stats(
            broker,
            window=WINDOW,
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
        )

        assert result == Ok([StatusCount(status='PENDING', count=2), StatusCount(status='FAILED', count=1)])
        assert _only_argument(call).__class__ is TaskStatsQuery

    @pytest.mark.asyncio
    async def test_the_breakdown_keeps_both_group_kinds(self) -> None:
        broker, call = _broker(
            'monitoring_task_breakdown_async', _record(MonitoringJson, json=BREAKDOWN_JSON),
        )

        result = await task_breakdown(
            broker,
            window=WINDOW,
            group_by='worker',
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            limit=BreakdownLimit(value=50),
        )

        match result:
            case Ok(Breakdown(groups=[first, second], group_count=2)):
                assert first.group == BreakdownValueGroup(value='worker-1')
                assert second.group == BreakdownUnclaimedGroup()
            case _:
                raise AssertionError(f'expected two groups: {result!r}')
        assert _only_argument(call).__class__ is TaskBreakdownQuery

    @pytest.mark.asyncio
    async def test_the_names_runs_and_schedules_are_the_json_lists_of_the_core(self) -> None:
        names_broker, _ = _broker(
            'monitoring_workflow_names_async', _record(MonitoringJson, json='["alpha.flow"]'),
        )
        runs_broker, runs_call = _broker(
            'monitoring_workflow_runs_async', _record(MonitoringJson, json=RUNS_JSON),
        )
        schedules_broker, _ = _broker(
            'monitoring_schedules_async', _record(MonitoringJson, json=SCHEDULES_JSON),
        )

        names = await list_workflow_names(names_broker)
        runs = await list_workflow_runs(
            runs_broker, name='alpha.flow', status=None, limit=WorkflowRunsLimit(value=30),
        )
        schedules = await list_schedules(schedules_broker)

        assert names == Ok(['alpha.flow'])
        match runs:
            case Ok([WorkflowRunSummary(id=run_id, wall_s=60)]):
                assert run_id == RUN_ID
            case _:
                raise AssertionError(f'expected one run: {runs!r}')
        assert _only_argument(runs_call).__class__ is WorkflowRunsQuery
        match schedules:
            case Ok([ScheduleStateInfo(schedule_name='nightly', last_task_id=None)]):
                pass
            case _:
                raise AssertionError(f'expected one schedule: {schedules!r}')

    @pytest.mark.asyncio
    async def test_a_run_is_its_json_and_null_is_no_run(self) -> None:
        found_broker, found_call = _broker(
            'monitoring_workflow_run_async', _record(MonitoringJson, json=RUN_JSON),
        )
        absent_broker, _ = _broker(
            'monitoring_workflow_run_async', _record(MonitoringJson, json='null'),
        )

        found = await get_workflow_run(found_broker, RUN_ID)
        absent = await get_workflow_run(absent_broker, RUN_ID)

        assert found == Ok(WorkflowRunDetail.model_validate_json(RUN_JSON))
        found_call.assert_awaited_once_with(uuid.UUID(RUN_ID))
        assert absent == Ok(None)

    @pytest.mark.asyncio
    async def test_an_id_text_that_is_not_a_uuid_names_no_run_or_node_and_calls_nothing(
        self,
    ) -> None:
        run_broker, run_call = _broker(
            'monitoring_workflow_run_async', _record(MonitoringJson, json='null'),
        )
        node_broker, node_call = _broker(
            'monitoring_workflow_node_async', _record(MonitoringJson, json='null'),
        )

        assert await get_workflow_run(run_broker, 'not-a-uuid') == Ok(None)
        assert await get_workflow_node(node_broker, 'not-a-uuid', 0) == Ok(None)
        run_call.assert_not_awaited()
        node_call.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_node_index_reaches_the_core_and_null_is_no_node(self) -> None:
        broker, call = _broker('monitoring_workflow_node_async', _record(MonitoringJson, json='null'))

        result = await get_workflow_node(broker, RUN_ID, 7)

        assert result == Ok(None)
        call.assert_awaited_once_with(uuid.UUID(RUN_ID), task_index=7)

    @pytest.mark.parametrize('task_index', [-(2**31) - 1, 2**31, 10**30])
    @pytest.mark.asyncio
    async def test_a_node_index_outside_the_integer_column_names_no_node(
        self, task_index: int,
    ) -> None:
        """An index that the integer column cannot hold names no node: ``Ok(None)``,
        with no call. A client value is not a database failure."""
        broker, call = _broker('monitoring_workflow_node_async', _record(MonitoringJson, json='null'))

        result = await get_workflow_node(broker, RUN_ID, task_index)

        assert result == Ok(None)
        call.assert_not_awaited()
