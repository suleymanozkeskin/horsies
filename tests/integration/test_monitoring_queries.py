"""Integration tests for the horsies.monitoring query API.

Every query runs against real rows through the ``broker`` fixture (which
applies migrations). Coverage:

1. task_stats — zero-fill, fixed order, per-dimension scoping
2. task_facets — scoping, NULL/empty exclusion, caps, uncapped category rollup
3. task_breakdown — rollup row, 'unknown' key, limit vs group_count
4. list_tasks — filters, sorting, NULLS LAST, pagination envelope
5. error-category filter — family expansion, DOMAIN as the registry complement
6. get_task_detail — attempt ordering, empty-text normalization, absence
7. duration semantics (spec 5.3) — which spans count up and which stay None
8. workflow names / runs / run detail / node detail
9. list_schedules — NULLS LAST ordering
10. database failure — Err(DB_OPERATION_FAILED) rather than a raised exception
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio

from horsies.core.history.reads.pages import HistoryWindow


def _test_window() -> HistoryWindow:
    """A window wide enough that every seeded row falls inside it."""
    now = datetime.now(timezone.utc)
    return HistoryWindow(lower=now - timedelta(days=29), upper=now + timedelta(hours=1))
from pydantic import SecretStr
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.task_pg import (
    ScheduleStateModel,
    TaskAttemptModel,
    TaskModel,
)
from horsies.core.models.tasks import BUILTIN_CODE_REGISTRY
from horsies.core.models.workflow_pg import WorkflowModel, WorkflowTaskModel
from horsies.core.types.result import is_err, is_ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring import (
    ErrorCategory,
    MonitoringQueryErrorCode,
    get_task_detail,
    get_workflow_node,
    get_workflow_run,
    list_schedules,
    list_tasks,
    list_workflow_names,
    list_workflow_runs,
    TaskSummary,
    task_breakdown,
    task_facets,
    task_stats,
)
from tests.integration.conftest import compute_test_enqueue_sha
from tests.integration.history_seeding import route_rows

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

UTC = timezone.utc

# Upper bound for "counts up to now" assertions: generous enough to survive a
# slow runner, tight enough to catch a span measured from the wrong origin.
_LIVE_SPAN_CEILING_S = 600

_STATUS_ORDER = [
    'PENDING',
    'CLAIMED',
    'RUNNING',
    'COMPLETED',
    'FAILED',
    'CANCELLED',
    'EXPIRED',
]


# --------------------------------------------------------------------------- #
# Fixtures and row factories
# --------------------------------------------------------------------------- #
@pytest_asyncio.fixture
async def clean_monitoring_tables(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - ensures migrations are applied
) -> AsyncGenerator[None, None]:
    """Empty every table the monitoring queries read, so counts are exact."""
    await session.execute(
        text(
            'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks, '
            'horsies_schedule_state, horsies_task_history CASCADE'
        )
    )
    await session.commit()
    yield


async def insert_rows(session: AsyncSession, *rows: Any) -> None:
    """Persist fixture rows on their lifecycle side and commit.

    Terminal-status tasks land in ``horsies_task_history`` — the live
    table's status domain admits only live rows, exactly as production
    writes them post-terminalization — and any attempt rows passed in
    the same call for such a task fold into its attempt snapshot.
    Workflow linkage comes from a same-call ``WorkflowTaskModel`` naming
    the task. Everything else persists through the ORM unchanged.
    """
    await route_rows(session, rows)


def ago(seconds: int) -> datetime:
    """A timestamp ``seconds`` in the past, aware UTC."""
    return datetime.now(UTC) - timedelta(seconds=seconds)


def _terminal_instant(
    status: TaskStatus,
    completed_at: datetime | None,
    failed_at: datetime | None,
) -> datetime | None:
    """The instant a terminal row is dated by, mirroring what production writes.

    Terminal exactly when dated is a database constraint, so a fixture that
    sets a terminal status without one is not a lighter fixture — it is a row
    that cannot exist. The instant is taken from whichever end timestamp the
    row carries, which keeps aged fixtures aged: a row completed sixty days ago
    is terminal sixty days ago, not now.
    """
    if not status.is_terminal:
        return None
    return completed_at or failed_at or datetime.now(timezone.utc)


def make_task(
    *,
    task_id: str | None = None,
    task_name: str = 'alpha_task',
    queue_name: str = 'default',
    status: TaskStatus = TaskStatus.PENDING,
    priority: int = 100,
    retry_count: int = 0,
    max_retries: int = 0,
    is_workflow_task: bool = False,
    error_code: str | None = None,
    failed_reason: str | None = None,
    worker_id: str | None = None,
    worker_hostname: str | None = None,
    enqueued_at: datetime | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    failed_at: datetime | None = None,
    good_until: datetime | None = None,
) -> TaskModel:
    """Build a task row. Every timestamp defaults to unset, not to now."""
    sent_at, enqueue_sha = compute_test_enqueue_sha(
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
    )
    return TaskModel(
        id=task_id or str(uuid.uuid4()),
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
        args='[]',
        kwargs='{}',
        status=status,
        sent_at=sent_at,
        enqueued_at=enqueued_at or ago(60),
        started_at=started_at,
        completed_at=completed_at,
        failed_at=failed_at,
        terminal_at=_terminal_instant(status, completed_at, failed_at),
        good_until=good_until,
        error_code=error_code,
        failed_reason=failed_reason,
        claimed=worker_id is not None,
        claimed_by_worker_id=worker_id,
        worker_hostname=worker_hostname,
        retry_count=retry_count,
        max_retries=max_retries,
        is_workflow_task=is_workflow_task,
        enqueue_sha=enqueue_sha,
    )


def make_attempt(
    *,
    task_id: str,
    attempt: int,
    outcome: str = 'FAILED',
    will_retry: bool = False,
    error_code: str | None = None,
    error_message: str | None = None,
    failed_reason: str | None = None,
    worker_hostname: str | None = 'host-1',
) -> TaskAttemptModel:
    """Build an attempt row for a task."""
    return TaskAttemptModel(
        task_id=task_id,
        attempt=attempt,
        outcome=outcome,
        will_retry=will_retry,
        started_at=ago(50),
        finished_at=ago(40),
        error_code=error_code,
        error_message=error_message,
        failed_reason=failed_reason,
        worker_hostname=worker_hostname,
    )


def make_workflow(
    *,
    workflow_id: str | None = None,
    name: str = 'alpha_flow',
    status: str = 'RUNNING',
    definition_key: str | None = 'tests.alpha.v1',
    parent_workflow_id: str | None = None,
    created_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> WorkflowModel:
    """Build a workflow run row."""
    return WorkflowModel(
        id=workflow_id or str(uuid.uuid4()),
        name=name,
        status=status,
        on_error='fail',
        definition_key=definition_key,
        parent_workflow_id=parent_workflow_id,
        depth=0 if parent_workflow_id is None else 1,
        created_at=created_at or ago(120),
        completed_at=completed_at,
    )


def make_node(
    *,
    workflow_id: str,
    task_index: int,
    task_name: str = 'alpha_task',
    status: str = 'PENDING',
    node_id: str | None = None,
    dependencies: list[int] | None = None,
    task_id: str | None = None,
    is_subworkflow: bool = False,
    sub_workflow_id: str | None = None,
    allow_failed_deps: bool = False,
    error: str | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> WorkflowTaskModel:
    """Build a workflow node row."""
    return WorkflowTaskModel(
        id=str(uuid.uuid4()),
        workflow_id=workflow_id,
        task_index=task_index,
        node_id=node_id,
        task_name=task_name,
        queue_name='default',
        priority=100,
        dependencies=dependencies if dependencies is not None else [],
        allow_failed_deps=allow_failed_deps,
        join_type='all',
        status=status,
        task_id=task_id,
        is_subworkflow=is_subworkflow,
        sub_workflow_id=sub_workflow_id,
        error=error,
        started_at=started_at,
        completed_at=completed_at,
    )


def make_schedule(
    *,
    schedule_name: str,
    next_run_at: datetime | None,
    last_run_at: datetime | None = None,
    last_task_id: str | None = None,
    run_count: int = 0,
) -> ScheduleStateModel:
    """Build a schedule state row."""
    return ScheduleStateModel(
        schedule_name=schedule_name,
        last_run_at=last_run_at,
        next_run_at=next_run_at,
        last_task_id=last_task_id,
        run_count=run_count,
    )


# --------------------------------------------------------------------------- #
# task_stats
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestTaskStats:
    """Status cards: seven rows, fixed order, scoped by every filter but status."""

    async def test_zero_fills_all_statuses_in_fixed_order(
        self, broker: PostgresBroker
    ) -> None:
        result = await task_stats(
            broker,
            window=_test_window(),
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
        )

        assert is_ok(result)
        assert [row.status for row in result.ok_value] == _STATUS_ORDER
        assert [row.count for row in result.ok_value] == [0] * 7

    async def test_counts_each_status(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(status=TaskStatus.PENDING),
            make_task(status=TaskStatus.PENDING),
            make_task(status=TaskStatus.FAILED, error_code='BOOM'),
        )

        result = await task_stats(
            broker,
            window=_test_window(),
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
        )

        assert is_ok(result)
        counts = {row.status: row.count for row in result.ok_value}
        assert counts['PENDING'] == 2
        assert counts['FAILED'] == 1
        assert counts['COMPLETED'] == 0

    @pytest.mark.parametrize(
        'task_names,queues,workers,error_codes,retried_only,expected_total',
        [
            (['alpha_task'], [], [], [], False, 2),
            ([], ['fast'], [], [], False, 1),
            ([], [], ['worker-1'], [], False, 1),
            ([], [], [], ['BOOM'], False, 1),
            ([], [], [], [], True, 1),
            (['alpha_task'], ['default'], [], [], False, 1),
        ],
    )
    async def test_filters_scope_the_counts(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        task_names: list[str],
        queues: list[str],
        workers: list[str],
        error_codes: list[str],
        retried_only: bool,
        expected_total: int,
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='alpha_task', queue_name='default'),
            make_task(task_name='alpha_task', queue_name='fast', worker_id='worker-1'),
            make_task(
                task_name='beta_task',
                queue_name='slow',
                error_code='BOOM',
                retry_count=2,
            ),
        )

        result = await task_stats(
            broker,
            window=_test_window(),
            task_names=task_names,
            queues=queues,
            workers=workers,
            error_codes=error_codes,
            error_categories=[],
            retried_only=retried_only,
        )

        assert is_ok(result)
        assert sum(row.count for row in result.ok_value) == expected_total


# --------------------------------------------------------------------------- #
# task_facets
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestTaskFacets:
    """Filter options: coarse scoping only, NULLs excluded, caps applied."""

    async def test_reports_distinct_values_with_counts(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='alpha_task', queue_name='default', worker_id='w1'),
            make_task(task_name='alpha_task', queue_name='fast', worker_id='w1'),
            make_task(task_name='beta_task', queue_name='fast'),
        )

        result = await task_facets(
            broker,
        window=_test_window(), statuses=[], error_categories=[], retried_only=False
        )

        assert is_ok(result)
        facets = result.ok_value
        assert {f.value: f.count for f in facets.task_names} == {
            'alpha_task': 2,
            'beta_task': 1,
        }
        assert {f.value: f.count for f in facets.queues} == {'default': 1, 'fast': 2}

    async def test_unclaimed_tasks_are_absent_from_the_worker_facet(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(worker_id='worker-1'),
            make_task(worker_id=None),
        )

        result = await task_facets(
            broker,
        window=_test_window(), statuses=[], error_categories=[], retried_only=False
        )

        assert is_ok(result)
        assert [f.value for f in result.ok_value.workers] == ['worker-1']
        assert result.ok_value.workers[0].count == 1

    async def test_null_and_empty_error_codes_are_excluded(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(status=TaskStatus.FAILED, error_code='BOOM'),
            make_task(status=TaskStatus.FAILED, error_code=''),
            make_task(status=TaskStatus.COMPLETED, error_code=None),
        )

        result = await task_facets(
            broker,
        window=_test_window(), statuses=[], error_categories=[], retried_only=False
        )

        assert is_ok(result)
        assert [f.value for f in result.ok_value.error_codes] == ['BOOM']

    async def test_error_codes_on_completed_tasks_are_counted(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The facet counts tasks carrying a code, not failures."""
        await insert_rows(
            session,
            make_task(status=TaskStatus.COMPLETED, error_code='SOFT_FAIL'),
        )

        result = await task_facets(
            broker,
        window=_test_window(), statuses=[], error_categories=[], retried_only=False
        )

        assert is_ok(result)
        assert [f.value for f in result.ok_value.error_codes] == ['SOFT_FAIL']

    async def test_error_codes_carry_their_taxonomy_category(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(status=TaskStatus.FAILED, error_code='TASK_EXCEPTION'),
            make_task(status=TaskStatus.CANCELLED, error_code='TASK_CANCELLED'),
            make_task(status=TaskStatus.FAILED, error_code='MY_DOMAIN_ERROR'),
        )

        result = await task_facets(
            broker,
        window=_test_window(), statuses=[], error_categories=[], retried_only=False
        )

        assert is_ok(result)
        categories = {f.value: f.category for f in result.ok_value.error_codes}
        assert categories['TASK_EXCEPTION'] == ErrorCategory.OPERATIONAL.value
        assert categories['TASK_CANCELLED'] == ErrorCategory.OUTCOME.value
        assert categories['MY_DOMAIN_ERROR'] == ErrorCategory.DOMAIN.value
        assert result.ok_value.error_category_totals == {
            ErrorCategory.OPERATIONAL.value: 1,
            ErrorCategory.OUTCOME.value: 1,
            ErrorCategory.DOMAIN.value: 1,
        }

    async def test_scoped_by_status_and_retried_only(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='alpha_task', status=TaskStatus.FAILED, retry_count=1),
            make_task(task_name='beta_task', status=TaskStatus.FAILED, retry_count=0),
            make_task(task_name='gamma_task', status=TaskStatus.COMPLETED),
        )

        by_status = await task_facets(
            broker,
            window=_test_window(),
            statuses=[TaskStatus.FAILED],
            error_categories=[],
            retried_only=False,
        )
        by_retried = await task_facets(
            broker,
        window=_test_window(), statuses=[], error_categories=[], retried_only=True
        )

        assert is_ok(by_status)
        assert is_ok(by_retried)
        assert {f.value for f in by_status.ok_value.task_names} == {
            'alpha_task',
            'beta_task',
        }
        assert {f.value for f in by_retried.ok_value.task_names} == {'alpha_task'}

    async def test_value_facets_cap_at_fifty(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            *[make_task(task_name=f'task_{index:03d}') for index in range(55)],
        )

        result = await task_facets(
            broker,
        window=_test_window(), statuses=[], error_categories=[], retried_only=False
        )

        assert is_ok(result)
        assert len(result.ok_value.task_names) == 50

    async def test_value_facets_sum_both_sides_before_the_global_cap(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        live_rows = [
            make_task(task_name=f'live_{index:03d}')
            for index in range(50)
            for _ in range(2)
        ]
        await insert_rows(
            session,
            *live_rows,
            make_task(task_name='shared_target'),
            make_task(
                task_name='shared_target', status=TaskStatus.COMPLETED
            ),
            make_task(
                task_name='shared_target', status=TaskStatus.COMPLETED
            ),
        )

        result = await task_facets(
            broker,
            window=_test_window(),
            statuses=[],
            error_categories=[],
            retried_only=False,
        )

        assert is_ok(result)
        counts = {
            facet.value: facet.count for facet in result.ok_value.task_names
        }
        assert len(counts) == 50
        assert counts['shared_target'] == 3

    async def test_selected_error_family_is_ranked_before_its_code_cap(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            *[
                make_task(
                    status=TaskStatus.FAILED,
                    error_code=f'HISTORY_DOMAIN_{index:03d}',
                )
                for index in range(201)
            ],
            make_task(
                status=TaskStatus.CANCELLED,
                error_code='TASK_CANCELLED',
            ),
        )

        result = await task_facets(
            broker,
            window=_test_window(),
            statuses=[],
            error_categories=[ErrorCategory.OUTCOME],
            retried_only=False,
        )

        assert is_ok(result)
        assert [facet.value for facet in result.ok_value.error_codes] == [
            'TASK_CANCELLED'
        ]

    async def test_category_totals_cover_codes_the_list_drops(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The dropdown is capped at 30; the rollup still counts all 35 codes."""
        await insert_rows(
            session,
            *[
                make_task(status=TaskStatus.FAILED, error_code=f'DOMAIN_{index:03d}')
                for index in range(35)
            ],
        )

        result = await task_facets(
            broker,
        window=_test_window(), statuses=[], error_categories=[], retried_only=False
        )

        assert is_ok(result)
        assert len(result.ok_value.error_codes) == 30
        assert result.ok_value.error_category_totals == {ErrorCategory.DOMAIN.value: 35}

    async def test_history_category_totals_exceed_the_history_facet_cap(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            *[
                make_task(
                    status=TaskStatus.FAILED,
                    error_code=f'HISTORY_DOMAIN_{index:03d}',
                )
                for index in range(201)
            ],
        )

        result = await task_facets(
            broker,
            window=_test_window(),
            statuses=[],
            error_categories=[],
            retried_only=False,
        )

        assert is_ok(result)
        assert len(result.ok_value.error_codes) == 30
        assert result.ok_value.error_category_totals == {
            ErrorCategory.DOMAIN.value: 201
        }


# --------------------------------------------------------------------------- #
# task_breakdown
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestTaskBreakdown:
    """Group pivot: rollup row, 'unknown' NULL key, limit independent of TOTAL."""

    async def test_groups_by_worker_with_rollup_total(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(worker_id='w1', status=TaskStatus.COMPLETED),
            make_task(worker_id='w1', status=TaskStatus.FAILED, retry_count=1),
            make_task(worker_id='w2', status=TaskStatus.RUNNING),
        )

        result = await task_breakdown(
            broker,
            window=_test_window(),
            group_by='worker',
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            limit=50,
        )

        assert is_ok(result)
        breakdown = result.ok_value
        rows = {row.group: row for row in breakdown.groups}
        assert rows['w1'].total == 2
        assert rows['w1'].completed == 1
        assert rows['w1'].failed == 1
        assert rows['w1'].retried == 1
        assert rows['w2'].running == 1
        assert breakdown.total.group == 'TOTAL'
        assert breakdown.total.total == 3
        assert breakdown.group_count == 2

    async def test_empty_scope_yields_no_groups_and_a_zero_total(
        self, broker: PostgresBroker
    ) -> None:
        result = await task_breakdown(
            broker,
            window=_test_window(),
            group_by='worker',
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            limit=50,
        )

        assert is_ok(result)
        assert result.ok_value.groups == []
        assert result.ok_value.group_count == 0
        assert result.ok_value.total.group == 'TOTAL'
        assert result.ok_value.total.total == 0

    async def test_null_group_key_is_labelled_unknown(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(session, make_task(worker_id=None))

        result = await task_breakdown(
            broker,
            window=_test_window(),
            group_by='worker',
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            limit=50,
        )

        assert is_ok(result)
        assert [row.group for row in result.ok_value.groups] == ['unknown']

    async def test_limit_caps_groups_but_not_total_or_group_count(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            *[make_task(queue_name=f'queue_{index}') for index in range(5)],
        )

        result = await task_breakdown(
            broker,
            window=_test_window(),
            group_by='queue',
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            limit=2,
        )

        assert is_ok(result)
        assert len(result.ok_value.groups) == 2
        assert result.ok_value.group_count == 5
        assert result.ok_value.total.total == 5

    async def test_groups_by_task_name(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='alpha_task'),
            make_task(task_name='alpha_task'),
            make_task(task_name='beta_task'),
        )

        result = await task_breakdown(
            broker,
            window=_test_window(),
            group_by='task_name',
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            limit=50,
        )

        assert is_ok(result)
        assert result.ok_value.group_by == 'task_name'
        assert [row.group for row in result.ok_value.groups] == [
            'alpha_task',
            'beta_task',
        ]

    async def test_filters_apply_to_groups_and_total(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='alpha_task', worker_id='w1'),
            make_task(task_name='beta_task', worker_id='w2'),
        )

        result = await task_breakdown(
            broker,
            window=_test_window(),
            group_by='worker',
            statuses=[],
            task_names=['alpha_task'],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            limit=50,
        )

        assert is_ok(result)
        assert [row.group for row in result.ok_value.groups] == ['w1']
        assert result.ok_value.total.total == 1


# --------------------------------------------------------------------------- #
# list_tasks
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestListTasks:
    """Paginated slice: filters AND across dimensions, allowlisted sorting."""

    async def test_total_counts_matches_not_page_size(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(session, *[make_task() for _ in range(5)])
        # The unfiltered total is a planner estimate; sample explicitly so
        # the estimate is current rather than whatever autovacuum last saw.
        await session.execute(text('ANALYZE horsies_tasks'))
        await session.commit()

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=2,
        )

        assert is_ok(result)
        assert len(result.ok_value.rows) == 2
        assert result.ok_value.total == 5

    async def test_offset_walks_the_sorted_result(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='first', enqueued_at=ago(30)),
            make_task(task_name='second', enqueued_at=ago(20)),
            make_task(task_name='third', enqueued_at=ago(10)),
        )

        page = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='asc',
            offset=1,
            limit=1,
        )

        assert is_ok(page)
        assert [row.task_name for row in page.ok_value.rows] == ['second']

    @pytest.mark.parametrize(
        'sort_dir,expected',
        [
            ('desc', ['third', 'second', 'first']),
            ('asc', ['first', 'second', 'third']),
        ],
    )
    async def test_sort_direction(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        sort_dir: Any,
        expected: list[str],
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='first', enqueued_at=ago(30)),
            make_task(task_name='second', enqueued_at=ago(20)),
            make_task(task_name='third', enqueued_at=ago(10)),
        )

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir=sort_dir,
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert [row.task_name for row in result.ok_value.rows] == expected

    @pytest.mark.parametrize('sort_dir', ['asc', 'desc'])
    async def test_null_sort_keys_come_last_in_both_directions(
        self, broker: PostgresBroker, session: AsyncSession, sort_dir: Any
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='never_ran', status=TaskStatus.PENDING),
            make_task(
                task_name='finished',
                status=TaskStatus.COMPLETED,
                started_at=ago(40),
                completed_at=ago(30),
            ),
        )

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='completed_at',
            sort_dir=sort_dir,
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert [row.task_name for row in result.ok_value.rows] == [
            'finished',
            'never_ran',
        ]

    async def test_live_rows_sort_as_null_on_derived_spans(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """A running row displays a growing exec_s but has no SQL-side span."""
        await insert_rows(
            session,
            make_task(
                task_name='running_now', status=TaskStatus.RUNNING, started_at=ago(20)
            ),
            make_task(
                task_name='finished',
                status=TaskStatus.COMPLETED,
                started_at=ago(40),
                completed_at=ago(30),
            ),
        )

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='exec_s',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert [row.task_name for row in result.ok_value.rows] == [
            'finished',
            'running_now',
        ]

    async def test_filters_combine_across_dimensions(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(task_name='alpha_task', queue_name='fast'),
            make_task(task_name='alpha_task', queue_name='slow'),
            make_task(task_name='beta_task', queue_name='fast'),
        )

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=['alpha_task'],
            queues=['fast'],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert result.ok_value.total == 1
        assert result.ok_value.rows[0].queue_name == 'fast'

    async def test_multiple_values_within_a_dimension_are_or_combined(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(status=TaskStatus.FAILED),
            make_task(status=TaskStatus.CANCELLED),
            make_task(status=TaskStatus.COMPLETED),
        )

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[TaskStatus.FAILED, TaskStatus.CANCELLED],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert result.ok_value.total == 2
        assert {row.status for row in result.ok_value.rows} == {'FAILED', 'CANCELLED'}

    async def test_rows_carry_derived_error_category_and_worker_id(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.FAILED,
                error_code='TASK_EXCEPTION',
                worker_id='worker-7',
                worker_hostname='host-7',
            ),
        )

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        row = result.ok_value.rows[0]
        assert row.error_category == ErrorCategory.OPERATIONAL.value
        assert row.worker_id == 'worker-7'
        assert row.worker_hostname == 'host-7'

    async def test_empty_error_code_is_reported_as_absent(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(session, make_task(error_code=''))

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert result.ok_value.rows[0].error_code is None
        assert result.ok_value.rows[0].error_category is None

    async def test_unfiltered_total_uses_planner_estimate_once_sampled(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """After ANALYZE, the unfiltered total is the sampled estimate.

        Rows inserted after the sample are not reflected in ``total`` — the
        stale value proves the estimate branch ran instead of an exact count.
        """
        await insert_rows(session, *[make_task() for _ in range(5)])
        await session.execute(text('ANALYZE horsies_tasks'))
        await session.commit()
        await insert_rows(session, *[make_task() for _ in range(3)])

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert result.ok_value.total == 5
        assert len(result.ok_value.rows) == 8

    async def test_filtered_total_stays_exact_despite_stale_estimate(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(session, *[make_task() for _ in range(5)])
        await session.execute(text('ANALYZE horsies_tasks'))
        await session.commit()
        await insert_rows(session, *[make_task() for _ in range(3)])

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[TaskStatus.PENDING],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert result.ok_value.total == 8

    async def test_unsampled_table_falls_back_to_exact_total(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """An unsampled table (``reltuples`` = -1) falls back to exact count.

        The sentinel is forced through the catalog because reaching it
        naturally is version-dependent: PG 18 resets ``reltuples`` on
        TRUNCATE, PG 16 keeps the stale pre-truncate value.
        """
        await session.execute(
            text(
                'UPDATE pg_class SET reltuples = -1 '
                "WHERE oid = 'horsies_tasks'::regclass"
            )
        )
        await session.commit()
        await insert_rows(session, *[make_task() for _ in range(5)])

        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_ok(result)
        assert result.ok_value.total == 5

    async def test_list_page_does_not_fetch_payload_columns(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The list SELECT must not reference args/kwargs/result/task_options.

        The wire statement is the contract: payload columns fetched here ship
        every task's payload on every page and detoast large results.
        """
        await insert_rows(session, make_task())
        captured: list[str] = []

        def capture_statement(
            conn: Any,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            captured.append(statement)

        sync_engine = broker.async_engine.sync_engine
        event.listen(sync_engine, 'before_cursor_execute', capture_statement)
        try:
            result = await list_tasks(
                broker,
            window=_test_window(),
                statuses=[],
                task_names=[],
                queues=[],
                workers=[],
                error_codes=[],
                error_categories=[],
                retried_only=False,
                sort_by='enqueued_at',
                sort_dir='desc',
                offset=0,
                limit=50,
            )
        finally:
            event.remove(sync_engine, 'before_cursor_execute', capture_statement)

        assert is_ok(result)
        list_statements = [
            s for s in captured if 'FROM horsies_tasks' in s and 'ORDER BY' in s
        ]
        assert len(list_statements) == 1
        statement = list_statements[0]
        for excluded in ('args', 'kwargs', 'result', 'task_options'):
            assert f'horsies_tasks.{excluded}' not in statement
        for included in ('task_name', 'enqueued_at', 'error_code'):
            assert f'horsies_tasks.{included}' in statement


# --------------------------------------------------------------------------- #
# error-category filter
# --------------------------------------------------------------------------- #
async def rows_by_category(
    broker: PostgresBroker,
    categories: list[ErrorCategory],
    *,
    queues: list[str] | None = None,
    error_codes: list[str] | None = None,
) -> list[TaskSummary]:
    """List the rows a category selection matches, other dimensions left open."""
    result = await list_tasks(
        broker,
            window=_test_window(),
        statuses=[],
        task_names=[],
        queues=queues or [],
        workers=[],
        error_codes=error_codes or [],
        error_categories=categories,
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        offset=0,
        limit=200,
    )
    assert is_ok(result)
    return result.ok_value.rows


# One representative code per family, plus a user-defined one for DOMAIN.
_FAMILY_SAMPLES: dict[ErrorCategory, str] = {
    ErrorCategory.OPERATIONAL: 'TASK_EXCEPTION',
    ErrorCategory.CONTRACT: 'RETURN_TYPE_MISMATCH',
    ErrorCategory.RETRIEVAL: 'WAIT_TIMEOUT',
    ErrorCategory.OUTCOME: 'TASK_CANCELLED',
    ErrorCategory.DOMAIN: 'PAYMENT_DECLINED',
}

_BUILT_IN_FAMILIES = [
    ErrorCategory.OPERATIONAL,
    ErrorCategory.CONTRACT,
    ErrorCategory.RETRIEVAL,
    ErrorCategory.OUTCOME,
]


@pytest.mark.usefixtures('clean_monitoring_tables')
class TestErrorCategoryFilter:
    """Families expand to codes in SQL; DOMAIN is the complement of the registry."""

    async def seed_one_per_family(self, session: AsyncSession) -> None:
        """One failed task per family, each carrying that family's sample code."""
        await insert_rows(
            session,
            *[
                make_task(status=TaskStatus.FAILED, error_code=code)
                for code in _FAMILY_SAMPLES.values()
            ],
        )

    @pytest.mark.parametrize('category', list(_FAMILY_SAMPLES))
    async def test_a_family_selects_exactly_its_own_codes(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        category: ErrorCategory,
    ) -> None:
        await self.seed_one_per_family(session)

        rows = await rows_by_category(broker, [category])

        assert [row.error_code for row in rows] == [_FAMILY_SAMPLES[category]]

    async def test_a_user_defined_code_is_domain_and_no_family_claims_it(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(status=TaskStatus.FAILED, error_code='PAYMENT_DECLINED'),
        )

        domain = await rows_by_category(broker, [ErrorCategory.DOMAIN])

        assert [row.error_code for row in domain] == ['PAYMENT_DECLINED']
        assert domain[0].error_category == ErrorCategory.DOMAIN.value
        for family in _BUILT_IN_FAMILIES:
            assert await rows_by_category(broker, [family]) == []

    async def test_absent_and_empty_codes_match_no_category(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """DOMAIN means "carries a code the library does not define", not "no code"."""
        await insert_rows(
            session,
            make_task(status=TaskStatus.COMPLETED, error_code=None),
            make_task(status=TaskStatus.FAILED, error_code=''),
        )

        for category in ErrorCategory:
            assert await rows_by_category(broker, [category]) == []

    async def test_categories_are_or_combined(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await self.seed_one_per_family(session)

        rows = await rows_by_category(
            broker, [ErrorCategory.CONTRACT, ErrorCategory.DOMAIN]
        )

        assert {row.error_code for row in rows} == {
            'RETURN_TYPE_MISMATCH',
            'PAYMENT_DECLINED',
        }

    async def test_the_category_dimension_ands_against_the_others(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(
                queue_name='fast',
                status=TaskStatus.FAILED,
                error_code='TASK_EXCEPTION',
            ),
            make_task(
                queue_name='slow',
                status=TaskStatus.FAILED,
                error_code='BROKER_ERROR',
            ),
        )

        scoped = await rows_by_category(
            broker, [ErrorCategory.OPERATIONAL], queues=['fast']
        )
        # Same column, different dimensions: the code must also be in the family.
        disjoint = await rows_by_category(
            broker, [ErrorCategory.OPERATIONAL], error_codes=['PAYMENT_DECLINED']
        )

        assert [row.error_code for row in scoped] == ['TASK_EXCEPTION']
        assert disjoint == []

    async def test_an_empty_selection_filters_nothing(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await self.seed_one_per_family(session)
        await insert_rows(session, make_task(error_code=None))

        rows = await rows_by_category(broker, [])

        assert len(rows) == len(_FAMILY_SAMPLES) + 1

    async def test_every_built_in_code_is_reachable_from_the_category_it_reports(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The label and the filter must stay one mapping.

        A code core registers but the taxonomy map has not been taught would be
        labelled DOMAIN while the DOMAIN filter — the complement of that same
        registry — excludes it, leaving the row unreachable from the chip that
        describes it.
        """
        await insert_rows(
            session,
            *[
                make_task(status=TaskStatus.FAILED, error_code=code)
                for code in BUILTIN_CODE_REGISTRY
            ],
        )

        reported = {
            row.error_code: row.error_category
            for row in await rows_by_category(broker, [])
        }
        assert len(reported) == len(BUILTIN_CODE_REGISTRY)
        for category in ErrorCategory:
            matched = {
                row.error_code for row in await rows_by_category(broker, [category])
            }
            expected = {
                code for code, value in reported.items() if value == category.value
            }
            assert matched == expected

    async def test_stats_and_breakdown_apply_the_same_expansion(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await self.seed_one_per_family(session)

        stats = await task_stats(
            broker,
            window=_test_window(),
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[ErrorCategory.OUTCOME],
            retried_only=False,
        )
        breakdown = await task_breakdown(
            broker,
            window=_test_window(),
            group_by='task_name',
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[ErrorCategory.OUTCOME],
            retried_only=False,
            limit=50,
        )

        assert is_ok(stats)
        assert is_ok(breakdown)
        assert sum(row.count for row in stats.ok_value) == 1
        assert breakdown.ok_value.total.total == 1

    async def test_facets_narrow_the_code_list_and_keep_every_category_total(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The strip offers the categories, so its own totals ignore the selection.

        Scoping them by the selection would hide every family the user has not
        picked, and a control cannot offer a second selection it does not show.
        """
        await self.seed_one_per_family(session)

        result = await task_facets(
            broker,
            window=_test_window(),
            statuses=[],
            error_categories=[ErrorCategory.OPERATIONAL],
            retried_only=False,
        )

        assert is_ok(result)
        assert [f.value for f in result.ok_value.error_codes] == ['TASK_EXCEPTION']
        assert result.ok_value.error_category_totals == {
            category.value: 1 for category in _FAMILY_SAMPLES
        }


# --------------------------------------------------------------------------- #
# get_task_detail
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestGetTaskDetail:
    """Single task plus attempt history; absence is data, not an error."""

    async def test_missing_task_is_ok_none(self, broker: PostgresBroker) -> None:
        result = await get_task_detail(broker, str(uuid.uuid4()))

        assert is_ok(result)
        assert result.ok_value is None

    async def test_returns_attempts_in_ascending_order(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.FAILED, error_code='TASK_EXCEPTION')
        await insert_rows(
            session,
            task,
            make_attempt(task_id=task.id, attempt=2, error_code='TASK_EXCEPTION'),
            make_attempt(task_id=task.id, attempt=1, will_retry=True),
        )

        result = await get_task_detail(broker, task.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        assert [a.attempt for a in detail.attempts] == [1, 2]
        assert detail.attempts[0].will_retry is True
        assert detail.error_category == ErrorCategory.OPERATIONAL.value

    async def test_standalone_task_has_no_workflow_linkage(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task()
        await insert_rows(session, task)

        result = await get_task_detail(broker, task.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        assert detail.workflow_id is None
        assert detail.workflow_task_index is None

    async def test_workflow_bound_task_links_to_its_run_and_node(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The detail panel needs these to link out instead of offering actions."""
        run = make_workflow()
        task = make_task(is_workflow_task=True)
        await insert_rows(session, run)
        await insert_rows(session, task)
        await insert_rows(
            session,
            make_node(workflow_id=run.id, task_index=3, task_id=task.id),
        )

        result = await get_task_detail(broker, task.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        assert detail.is_workflow_task is True
        assert detail.workflow_id == run.id
        assert detail.workflow_task_index == 3

    async def test_a_node_row_pointing_elsewhere_does_not_leak_in(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The hop matches on task_id, not on membership in some workflow."""
        run = make_workflow()
        linked = make_task(is_workflow_task=True)
        unrelated = make_task()
        await insert_rows(session, run)
        await insert_rows(session, linked, unrelated)
        await insert_rows(
            session,
            make_node(workflow_id=run.id, task_index=0, task_id=linked.id),
        )

        result = await get_task_detail(broker, unrelated.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        assert detail.workflow_id is None
        assert detail.workflow_task_index is None

    async def test_reports_good_until_and_workflow_membership(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        expiry = datetime.now(UTC) + timedelta(hours=2)
        task = make_task(is_workflow_task=True, good_until=expiry)
        await insert_rows(session, task)

        result = await get_task_detail(broker, task.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        assert detail.is_workflow_task is True
        assert detail.leaf.good_until is not None
        assert abs((detail.leaf.good_until - expiry).total_seconds()) < 1

    async def test_empty_attempt_text_is_normalized_to_none(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.FAILED, failed_reason='')
        await insert_rows(
            session,
            task,
            make_attempt(
                task_id=task.id,
                attempt=1,
                error_code='',
                error_message='   ',
                failed_reason='',
            ),
        )

        result = await get_task_detail(broker, task.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        assert detail.leaf.failed_reason is None
        assert detail.attempts[0].error_code is None
        assert detail.attempts[0].error_message is None
        assert detail.attempts[0].failed_reason is None


# --------------------------------------------------------------------------- #
# Duration semantics (spec 5.3)
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestTaskDurations:
    """Which spans count up to now, and which report nothing at all."""

    async def _only_row(self, broker: PostgresBroker) -> tuple[int | None, int | None]:
        result = await list_tasks(
            broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=1,
        )
        assert is_ok(result)
        row = result.ok_value.rows[0]
        return row.queue_s, row.exec_s

    async def test_pending_task_queue_time_counts_up(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session, make_task(status=TaskStatus.PENDING, enqueued_at=ago(30))
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s is not None
        assert 30 <= queue_s <= _LIVE_SPAN_CEILING_S
        assert exec_s is None

    async def test_claimed_task_queue_time_counts_up(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.CLAIMED, enqueued_at=ago(25), worker_id='worker-1'
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s is not None
        assert 25 <= queue_s <= _LIVE_SPAN_CEILING_S
        assert exec_s is None

    async def test_running_task_freezes_queue_time_and_counts_execution(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.RUNNING,
                enqueued_at=ago(60),
                started_at=ago(40),
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 20
        assert exec_s is not None
        assert 40 <= exec_s <= _LIVE_SPAN_CEILING_S

    async def test_completed_task_reports_both_closed_spans(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.COMPLETED,
                enqueued_at=ago(90),
                started_at=ago(60),
                completed_at=ago(45),
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 30
        assert exec_s == 15

    async def test_failed_task_measures_execution_to_failed_at(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.FAILED,
                enqueued_at=ago(90),
                started_at=ago(70),
                failed_at=ago(55),
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 20
        assert exec_s == 15

    async def test_cancelled_before_start_reports_the_closed_queue_span(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The terminal instant closes the queue span; nothing counts up.

        A history row always carries its terminal instant, so a task
        cancelled before starting reports the time it actually waited —
        a closed span, never a live count-up — and no execution span.
        """
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.CANCELLED,
                enqueued_at=ago(300),
                error_code='TASK_CANCELLED',
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 300
        assert exec_s is None

    async def test_expired_before_start_reports_the_closed_queue_span(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(status=TaskStatus.EXPIRED, enqueued_at=ago(300)),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 300
        assert exec_s is None

    async def test_detail_leaf_uses_the_same_spans_as_the_list_row(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(
            status=TaskStatus.CANCELLED,
            enqueued_at=ago(300),
            error_code='TASK_CANCELLED',
        )
        await insert_rows(session, task)

        queue_s, exec_s = await self._only_row(broker)
        detail = await get_task_detail(broker, task.id)

        assert is_ok(detail)
        assert detail.ok_value is not None
        assert detail.ok_value.leaf.queue_s == queue_s
        assert detail.ok_value.leaf.exec_s == exec_s


# --------------------------------------------------------------------------- #
# Workflow names and runs
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestWorkflowNames:
    """Only root runs feed the name picker."""

    async def test_returns_sorted_distinct_root_names(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        root = make_workflow(name='zeta_flow')
        await insert_rows(
            session,
            root,
            make_workflow(name='alpha_flow'),
            make_workflow(name='alpha_flow'),
        )
        await insert_rows(
            session,
            make_workflow(name='child_flow', parent_workflow_id=root.id),
        )

        result = await list_workflow_names(broker)

        assert is_ok(result)
        assert result.ok_value == ['alpha_flow', 'zeta_flow']

    async def test_empty_database_yields_empty_list(
        self, broker: PostgresBroker
    ) -> None:
        result = await list_workflow_names(broker)

        assert is_ok(result)
        assert result.ok_value == []


@pytest.mark.usefixtures('clean_monitoring_tables')
class TestListWorkflowRuns:
    """Recent root runs, newest first, with exact-match filters."""

    async def test_newest_first_and_children_excluded(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        root = make_workflow(name='alpha_flow', created_at=ago(300))
        await insert_rows(
            session,
            root,
            make_workflow(name='beta_flow', created_at=ago(100)),
        )
        await insert_rows(
            session,
            make_workflow(
                name='child_flow', parent_workflow_id=root.id, created_at=ago(10)
            ),
        )

        result = await list_workflow_runs(broker, name=None, status=None, limit=30)

        assert is_ok(result)
        assert [run.name for run in result.ok_value] == ['beta_flow', 'alpha_flow']

    async def test_name_and_status_filters_combine(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_workflow(name='alpha_flow', status='FAILED'),
            make_workflow(name='alpha_flow', status='COMPLETED'),
            make_workflow(name='beta_flow', status='FAILED'),
        )

        result = await list_workflow_runs(
            broker, name='alpha_flow', status='FAILED', limit=30
        )

        assert is_ok(result)
        assert len(result.ok_value) == 1
        assert result.ok_value[0].status == 'FAILED'

    async def test_unknown_status_yields_empty_list(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(session, make_workflow(status='RUNNING'))

        result = await list_workflow_runs(
            broker, name=None, status='NOT_A_STATUS', limit=30
        )

        assert is_ok(result)
        assert result.ok_value == []

    async def test_limit_truncates_the_result(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            *[make_workflow(created_at=ago(index + 1)) for index in range(4)],
        )

        result = await list_workflow_runs(broker, name=None, status=None, limit=2)

        assert is_ok(result)
        assert len(result.ok_value) == 2

    async def test_wall_time_counts_up_until_completion(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_workflow(name='open_flow', status='PAUSED', created_at=ago(45)),
            make_workflow(
                name='closed_flow',
                status='COMPLETED',
                created_at=ago(200),
                completed_at=ago(140),
            ),
        )

        result = await list_workflow_runs(broker, name=None, status=None, limit=30)

        assert is_ok(result)
        walls = {run.name: run.wall_s for run in result.ok_value}
        assert walls['closed_flow'] == 60
        open_wall = walls['open_flow']
        assert open_wall is not None
        assert 45 <= open_wall <= _LIVE_SPAN_CEILING_S


# --------------------------------------------------------------------------- #
# Workflow run detail
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestGetWorkflowRun:
    """The DAG rebuilt from persisted node rows."""

    async def test_missing_run_is_ok_none(self, broker: PostgresBroker) -> None:
        result = await get_workflow_run(broker, str(uuid.uuid4()))

        assert is_ok(result)
        assert result.ok_value is None

    async def test_nodes_ordered_and_edges_derived_from_dependencies(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await insert_rows(session, run)
        await insert_rows(
            session,
            make_node(workflow_id=run.id, task_index=2, dependencies=[0, 1]),
            make_node(workflow_id=run.id, task_index=0),
            make_node(workflow_id=run.id, task_index=1, dependencies=[0]),
        )

        result = await get_workflow_run(broker, run.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        assert [node.task_index for node in detail.nodes] == [0, 1, 2]
        assert {(e.from_index, e.to_index) for e in detail.edges} == {
            (0, 1),
            (0, 2),
            (1, 2),
        }

    async def test_dependency_on_a_missing_index_is_dropped(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await insert_rows(session, run)
        await insert_rows(
            session,
            make_node(workflow_id=run.id, task_index=0, dependencies=[99]),
        )

        result = await get_workflow_run(broker, run.id)

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.edges == []

    async def test_failed_nodes_are_indexed_in_ascending_order(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow(status='FAILED')
        await insert_rows(session, run)
        await insert_rows(
            session,
            make_node(workflow_id=run.id, task_index=0, status='COMPLETED'),
            make_node(workflow_id=run.id, task_index=1, status='FAILED'),
            make_node(workflow_id=run.id, task_index=2, status='FAILED'),
        )

        result = await get_workflow_run(broker, run.id)

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.failed_indices == [1, 2]
        assert result.ok_value.failed_count == 2

    async def test_subworkflow_node_rolls_up_its_child_run(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        child = make_workflow(name='child_flow', parent_workflow_id=run.id)
        empty_child = make_workflow(name='empty_child', parent_workflow_id=run.id)
        await insert_rows(session, run)
        await insert_rows(session, child, empty_child)
        await insert_rows(
            session,
            make_node(
                workflow_id=run.id,
                task_index=0,
                is_subworkflow=True,
                sub_workflow_id=child.id,
            ),
            make_node(
                workflow_id=run.id,
                task_index=1,
                is_subworkflow=True,
                sub_workflow_id=empty_child.id,
            ),
            make_node(workflow_id=run.id, task_index=2),
            make_node(workflow_id=child.id, task_index=0, status='COMPLETED'),
            make_node(workflow_id=child.id, task_index=1, status='FAILED'),
        )

        result = await get_workflow_run(broker, run.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        nodes = {node.task_index: node for node in detail.nodes}
        assert (nodes[0].child_total, nodes[0].child_failed) == (2, 1)
        assert (nodes[1].child_total, nodes[1].child_failed) == (None, None)
        assert (nodes[2].child_total, nodes[2].child_failed) == (None, None)

    async def test_child_run_is_reachable_by_the_same_query(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        child = make_workflow(name='child_flow', parent_workflow_id=run.id)
        await insert_rows(session, run)
        await insert_rows(session, child)
        await insert_rows(session, make_node(workflow_id=child.id, task_index=0))

        result = await get_workflow_run(broker, child.id)

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.run.id == child.id
        assert len(result.ok_value.nodes) == 1

    async def test_node_execution_time_per_status(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await insert_rows(session, run)
        await insert_rows(
            session,
            make_node(
                workflow_id=run.id,
                task_index=0,
                status='RUNNING',
                started_at=ago(35),
            ),
            make_node(
                workflow_id=run.id,
                task_index=1,
                status='COMPLETED',
                started_at=ago(90),
                completed_at=ago(60),
            ),
            make_node(
                workflow_id=run.id,
                task_index=2,
                status='SKIPPED',
                started_at=ago(90),
                completed_at=None,
            ),
            make_node(
                workflow_id=run.id,
                task_index=3,
                status='ENQUEUED',
                started_at=ago(90),
            ),
        )

        result = await get_workflow_run(broker, run.id)

        assert is_ok(result)
        detail = result.ok_value
        assert detail is not None
        nodes = {node.task_index: node for node in detail.nodes}
        running_exec = nodes[0].exec_s
        assert running_exec is not None
        assert 35 <= running_exec <= _LIVE_SPAN_CEILING_S
        assert nodes[1].exec_s == 30
        assert nodes[2].exec_s is None
        assert nodes[3].exec_s is None


# --------------------------------------------------------------------------- #
# Workflow node detail
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestGetWorkflowNode:
    """Node detail: backing task and attempts when there are any."""

    async def test_missing_node_is_ok_none(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await insert_rows(session, run)

        result = await get_workflow_node(broker, run.id, 0)

        assert is_ok(result)
        assert result.ok_value is None

    async def test_returns_backing_task_and_attempts(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        task = make_task(status=TaskStatus.FAILED, is_workflow_task=True)
        await insert_rows(
            session,
            run,
            task,
            make_node(
                workflow_id=run.id,
                task_index=0,
                node_id='step-one',
                status='FAILED',
                task_id=task.id,
                error='node blew up',
            ),
            make_attempt(task_id=task.id, attempt=2),
            make_attempt(task_id=task.id, attempt=1, will_retry=True),
        )

        result = await get_workflow_node(broker, run.id, 0)

        assert is_ok(result)
        node = result.ok_value
        assert node is not None
        assert node.node_id == 'step-one'
        assert node.node_error == 'node blew up'
        assert node.leaf is not None
        assert node.leaf.task_id == task.id
        assert [a.attempt for a in node.attempts] == [1, 2]

    async def test_subworkflow_node_has_no_leaf_or_attempts(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        child = make_workflow(name='child_flow', parent_workflow_id=run.id)
        await insert_rows(session, run)
        await insert_rows(session, child)
        await insert_rows(
            session,
            make_node(
                workflow_id=run.id,
                task_index=0,
                is_subworkflow=True,
                sub_workflow_id=child.id,
            ),
        )

        result = await get_workflow_node(broker, run.id, 0)

        assert is_ok(result)
        node = result.ok_value
        assert node is not None
        assert node.is_subworkflow is True
        assert node.leaf is None
        assert node.attempts == []

    async def test_vanished_backing_task_still_returns_the_node(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """Retention can remove the task row while the node row survives."""
        run = make_workflow()
        await insert_rows(session, run)
        await insert_rows(
            session,
            make_node(
                workflow_id=run.id,
                task_index=0,
                status='COMPLETED',
                task_id=str(uuid.uuid4()),
            ),
        )

        result = await get_workflow_node(broker, run.id, 0)

        assert is_ok(result)
        node = result.ok_value
        assert node is not None
        assert node.leaf is None
        assert node.attempts == []

    async def test_empty_node_error_is_normalized_to_none(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await insert_rows(session, run)
        await insert_rows(
            session, make_node(workflow_id=run.id, task_index=0, error='')
        )

        result = await get_workflow_node(broker, run.id, 0)

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.node_error is None


# --------------------------------------------------------------------------- #
# Schedules
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestListSchedules:
    """Soonest next-run first, with unscheduled entries last."""

    async def test_empty_state_yields_empty_list(self, broker: PostgresBroker) -> None:
        result = await list_schedules(broker)

        assert is_ok(result)
        assert result.ok_value == []

    async def test_ordered_by_next_run_with_nulls_last(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        soon = datetime.now(UTC) + timedelta(minutes=5)
        later = datetime.now(UTC) + timedelta(hours=3)
        await insert_rows(
            session,
            make_schedule(schedule_name='unscheduled', next_run_at=None),
            make_schedule(schedule_name='later', next_run_at=later),
            make_schedule(schedule_name='soon', next_run_at=soon, run_count=4),
        )

        result = await list_schedules(broker)

        assert is_ok(result)
        assert [row.schedule_name for row in result.ok_value] == [
            'soon',
            'later',
            'unscheduled',
        ]
        assert result.ok_value[0].run_count == 4


# --------------------------------------------------------------------------- #
# Database failure
# --------------------------------------------------------------------------- #
class TestDatabaseFailure:
    """An unreachable database is an Err, never a raised exception."""

    @pytest_asyncio.fixture
    async def unreachable_broker(self) -> AsyncGenerator[PostgresBroker, None]:
        broken = PostgresBroker(
            PostgresConfig(
                database_url=SecretStr(
                    'postgresql+psycopg://postgres:none@127.0.0.1:1/none'
                )
            ),
            assume_initialized=True,
        )
        yield broken
        await broken.close_async()

    async def test_list_tasks_reports_a_retryable_db_failure(
        self, unreachable_broker: PostgresBroker
    ) -> None:
        result = await list_tasks(
            unreachable_broker,
            window=_test_window(),
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            sort_by='enqueued_at',
            sort_dir='desc',
            offset=0,
            limit=50,
        )

        assert is_err(result)
        error = result.err_value
        assert error.code is MonitoringQueryErrorCode.DB_OPERATION_FAILED
        assert error.retryable is True
        assert 'task list query failed' in error.message
        assert error.exception is not None

    async def test_detail_failure_is_not_confused_with_absence(
        self, unreachable_broker: PostgresBroker
    ) -> None:
        result = await get_task_detail(unreachable_broker, str(uuid.uuid4()))

        assert is_err(result)
        assert result.err_value.code is MonitoringQueryErrorCode.DB_OPERATION_FAILED

    async def test_aggregate_failure_is_reported(
        self, unreachable_broker: PostgresBroker
    ) -> None:
        result = await task_stats(
            unreachable_broker,
            window=_test_window(),
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
        )

        assert is_err(result)
        assert result.err_value.retryable is True


# --------------------------------------------------------------------------- #
# list_tasks — the unfiltered-total contract on the history side
# --------------------------------------------------------------------------- #
class _StatementCapture:
    """Every SQL string the engine executes inside the ``with`` block."""

    def __init__(self, broker: PostgresBroker) -> None:
        self._engine = broker.async_engine.sync_engine
        self.statements: list[str] = []

    def _collect(
        self,
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        self.statements.append(statement)

    def __enter__(self) -> '_StatementCapture':
        event.listen(self._engine, 'before_cursor_execute', self._collect)
        return self

    def __exit__(self, *exc_info: Any) -> None:
        event.remove(self._engine, 'before_cursor_execute', self._collect)


async def _unfiltered_list(broker: PostgresBroker) -> Any:
    return await list_tasks(
        broker,
        window=_test_window(),
        statuses=[],
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        offset=0,
        limit=50,
    )


@pytest.mark.usefixtures('clean_monitoring_tables')
class TestUnfilteredHistoryTotalContract:
    """The documented total contract holds on the history side.

    ``TaskListPage`` documents the unfiltered total as a planner
    estimate; the live side branches on scope and the history side must
    branch with it — estimate together, exact together. The proof is
    the statements themselves: which ran, and which provably did not.
    """

    async def test_unfiltered_total_estimates_and_never_counts_history(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(),
            make_task(),
            make_task(status=TaskStatus.COMPLETED),
            make_task(status=TaskStatus.COMPLETED),
            make_task(status=TaskStatus.FAILED),
        )
        await session.execute(
            text('ANALYZE horsies_tasks, horsies_task_history')
        )
        await session.commit()

        with _StatementCapture(broker) as capture:
            result = await _unfiltered_list(broker)

        assert is_ok(result)
        assert isinstance(result.ok_value.total, int)
        assert any(
            statement.startswith('EXPLAIN (FORMAT JSON)')
            for statement in capture.statements
        ), capture.statements
        assert not any(
            'count(*) FROM horsies_task_history' in statement
            for statement in capture.statements
        ), 'the unfiltered view must never run the exact history count'

    async def test_filtered_total_counts_history_exactly(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(),
            make_task(status=TaskStatus.COMPLETED),
            make_task(status=TaskStatus.COMPLETED),
            make_task(status=TaskStatus.COMPLETED),
        )

        with _StatementCapture(broker) as capture:
            result = await list_tasks(
                broker,
                window=_test_window(),
                statuses=[TaskStatus.COMPLETED],
                task_names=[],
                queues=[],
                workers=[],
                error_codes=[],
                error_categories=[],
                retried_only=False,
                sort_by='enqueued_at',
                sort_dir='desc',
                offset=0,
                limit=50,
            )

        assert is_ok(result)
        assert result.ok_value.total == 3
        assert any(
            'count(*) FROM horsies_task_history' in statement
            for statement in capture.statements
        ), capture.statements
        assert not any(
            statement.startswith('EXPLAIN')
            for statement in capture.statements
        ), 'a filtered total is exact; no estimate statement may run'

    async def test_truncated_history_reports_zero_not_stale_estimate(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """An emptied history contributes zero, not leftover statistics.

        A truncated leaf's ``reltuples`` is version-dependent (PG 18
        resets it, PG 16 keeps the stale value), and the planner clamps
        every scanned relation to at least one row — so without the
        emptiness guard the total would carry phantom history rows,
        differently per major. Sampled statistics are left in place
        deliberately; the truncation must beat them.
        """
        await insert_rows(
            session,
            make_task(status=TaskStatus.COMPLETED),
            make_task(status=TaskStatus.COMPLETED),
            make_task(status=TaskStatus.FAILED),
        )
        await session.execute(
            text('ANALYZE horsies_tasks, horsies_task_history')
        )
        await session.commit()
        await session.execute(text('TRUNCATE horsies_task_history'))
        await session.commit()
        await insert_rows(session, make_task(), make_task())
        await session.execute(text('ANALYZE horsies_tasks'))
        await session.commit()

        result = await _unfiltered_list(broker)

        assert is_ok(result)
        assert result.ok_value.total == 2

    async def test_estimate_decode_failure_is_a_typed_error(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An unrecognized EXPLAIN payload surfaces as ``Err``, not a raise.

        The failure is injected at the decode seam because a real
        payload-shape drift needs a server this suite does not run;
        what the boundary owes is the same either way: a typed,
        non-retryable monitoring error and no silent exact fallback.
        """
        from horsies.core.history.errors import HistoryContractError
        from horsies.monitoring import queries as queries_module

        await insert_rows(
            session, make_task(status=TaskStatus.COMPLETED)
        )

        def _refuse(payload: object) -> int:
            raise HistoryContractError('injected decode failure')

        monkeypatch.setattr(
            queries_module, 'plan_rows_from_explain', _refuse
        )

        result = await _unfiltered_list(broker)

        assert is_err(result)
        assert (
            result.err_value.code
            is MonitoringQueryErrorCode.DB_OPERATION_FAILED
        )
        assert result.err_value.retryable is False
        assert 'estimate decode failed' in result.err_value.message
