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
10. database failure — Err(DATABASE_OPERATION_FAILED) rather than a raised exception
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from collections.abc import Awaitable, Callable, Generator
from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio

from horsies.monitoring import MonitoringWindow
from tests.integration.autocommit_support import proxied_url, steady_state_turns, upstream_of
from tests.integration.round_trip_proxy import RoundTripProxy, Turn


_FORWARD_WAIT_S = 0.5
"""How long a test that expects no record waits for a late one: the core
hands its records to Python ``logging`` on its forwarder thread."""


async def _until_logged(caplog: pytest.LogCaptureFixture, text_part: str) -> None:
    """Wait until a record whose message holds ``text_part`` reaches ``caplog``.

    The core hands its records to Python ``logging`` on its forwarder thread,
    so a record can arrive after the call that logged it returned. Python's
    read logs before it returns, so the wait ends at once there.
    """
    for _ in range(100):
        if any(text_part in record.getMessage() for record in caplog.records):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f'no log record holds {text_part!r}')


def _test_window() -> MonitoringWindow:
    """A window wide enough that every seeded row falls inside it."""
    now = datetime.now(timezone.utc)
    return MonitoringWindow(lower=now - timedelta(days=29), upper=now + timedelta(hours=1))
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import BUILTIN_CODE_REGISTRY
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.models.tasks import TaskError
from horsies._core import CoreConnectionError
from horsies.core.types.result import Err, Ok, Result, is_err, is_ok
from horsies.core.types.status import TaskStatus
from horsies.core.history.errors import HistoryContractError
from horsies._catalog.monitoring_bounds import LIST_ROW_BOUND
from horsies.monitoring import (
    Breakdown,
    BreakdownLimit,
    BreakdownUnclaimedGroup,
    BreakdownValueGroup,
    MonitoringResult,
    TaskPage,
    WorkflowRunsLimit,
    ErrorCategory,
    MonitoringQueryDatabaseOperationFailed,
    MonitoringQueryHistoryContractViolation,
    TaskAbsenceReason,
    TaskDetail,
    TaskDetailNotFound,
    get_task_detail,
    get_workflow_node,
    get_workflow_run,
    list_schedules,
    list_tasks,
    list_workflow_names,
    list_workflow_runs,
    TaskSummary,
    categorize_error_code,
    task_breakdown,
    task_facets,
    task_stats,
)
from tests.integration.history_seeding import stored_history_status
from tests.integration.monitoring_seeding import (
    FIXTURE_WORKER_PID,
    FIXTURE_WORKER_PROCESS_NAME,
    AttemptRow,
    LastRun,
    NodeRow,
    ScheduleRow,
    SeedRow,
    SubworkflowNode,
    TaskNode,
    TaskRow,
    WorkflowParent,
    WorkflowRow,
    task_row,
)
from tests.integration.monitoring_seeding import insert_rows as seed_rows

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


async def insert_rows(session: AsyncSession, *rows: SeedRow) -> None:
    """Persist fixture rows on their lifecycle side and commit.

    Terminal-status tasks land in ``horsies_task_history`` — the live
    table holds only live rows, exactly as production writes them after
    the move — and any attempt rows passed in the same call for such a
    task fold into its attempt snapshot. Workflow linkage comes from a
    same-call node naming the task (``tests/integration/monitoring_seeding.py``).
    """
    await seed_rows(session, *rows)


def ago(seconds: int) -> datetime:
    """A timestamp ``seconds`` in the past, aware UTC."""
    return datetime.now(UTC) - timedelta(seconds=seconds)


def before(instant: datetime, seconds: int) -> datetime:
    """The timestamp ``seconds`` before ``instant``.

    The spans of the monitoring reads are exact to the microsecond. Times
    built from one ``instant`` differ by whole seconds exactly; two calls of
    ``ago`` read the clock twice, so their difference is a few microseconds
    longer.
    """
    return instant - timedelta(seconds=seconds)


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
) -> TaskRow:
    """Build a task row. Every timestamp defaults to unset, not to now.

    A terminal status is dated by whichever end timestamp the row carries,
    else now, which keeps aged fixtures aged: a row completed sixty days ago
    is terminal sixty days ago.

    A CLAIMED or RUNNING row holds its claim generation (CHECK
    ``horsies_tasks_claim_generation_by_status``): the claim took the row at
    its ``enqueued_at``.
    """
    enqueued = enqueued_at or ago(60)
    claimed_at: datetime | None
    match status:
        case TaskStatus.CLAIMED | TaskStatus.RUNNING:
            claimed_at = enqueued
        case (
            TaskStatus.PENDING
            | TaskStatus.COMPLETED
            | TaskStatus.FAILED
            | TaskStatus.CANCELLED
            | TaskStatus.EXPIRED
        ):
            claimed_at = None
    return task_row(
        task_id=task_id or str(uuid.uuid4()),
        task_name=task_name,
        queue_name=queue_name,
        status=status,
        priority=priority,
        retry_count=retry_count,
        max_retries=max_retries,
        is_workflow_task=is_workflow_task,
        enqueued_at=enqueued,
        claimed_at=claimed_at,
        started_at=started_at,
        completed_at=completed_at,
        failed_at=failed_at,
        good_until=good_until,
        error_code=error_code,
        failed_reason=failed_reason,
        worker_id=worker_id,
        worker_hostname=worker_hostname,
        worker_pid=None,
        worker_process_name=None,
        claim_expires_at=None,
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
    worker_hostname: str = 'host-1',
) -> AttemptRow:
    """Build an attempt row for a task."""
    return AttemptRow(
        task_id=task_id,
        attempt=attempt,
        outcome=outcome,
        will_retry=will_retry,
        started_at=ago(50),
        finished_at=ago(40),
        error_code=error_code,
        error_message=error_message,
        failed_reason=failed_reason,
        worker_id='worker-1',
        worker_hostname=worker_hostname,
        worker_pid=FIXTURE_WORKER_PID,
        worker_process_name=FIXTURE_WORKER_PROCESS_NAME,
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
) -> WorkflowRow:
    """Build a workflow run row. A child run hangs off node 0 of its parent."""
    created = created_at or ago(120)
    return WorkflowRow(
        id=workflow_id or str(uuid.uuid4()),
        name=name,
        status=status,
        on_error='fail',
        definition_key=definition_key,
        parent=(
            None
            if parent_workflow_id is None
            else WorkflowParent(
                workflow_id=parent_workflow_id,
                task_index=0,
                root_workflow_id=parent_workflow_id,
            )
        ),
        sent_at=created,
        created_at=created,
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
    allow_failed_dependencies: bool = False,
    result: str | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> NodeRow:
    """Build a workflow node row."""
    assert not (is_subworkflow and task_id is not None), (
        'a sub-workflow node backs no task'
    )
    return NodeRow(
        id=str(uuid.uuid4()),
        workflow_id=workflow_id,
        task_index=task_index,
        node_id=node_id,
        task_name=task_name,
        status=status,
        dependencies=tuple(dependencies) if dependencies is not None else (),
        allow_failed_dependencies=allow_failed_dependencies,
        kind=(
            SubworkflowNode(sub_workflow_id=sub_workflow_id)
            if is_subworkflow
            else TaskNode(task_id=task_id)
        ),
        result=result,
        created_at=ago(120),
        started_at=started_at,
        completed_at=completed_at,
    )


def make_schedule(
    *,
    schedule_name: str,
    next_run_at: datetime,
    last_run: LastRun | None = None,
) -> ScheduleRow:
    """Build a schedule state row. A schedule that never fired has no last run."""
    return ScheduleRow(
        schedule_name=schedule_name,
        next_run_at=next_run_at,
        last_run=last_run,
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
                status=TaskStatus.FAILED,
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

    async def test_a_history_row_with_an_unknown_status_is_a_contract_violation(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.FAILED)
        await insert_rows(session, task, make_task(status=TaskStatus.PENDING))

        async with stored_history_status(session, task.id, 'NOT_KNOWN'):
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

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)
        assert 'NOT_KNOWN' in error.message
        # Python's texts: the message names the read and holds the error's
        # text, and the exception is a HistoryContractError with that text.
        assert type(error.exception) is HistoryContractError
        cause_text = str(error.exception)
        assert 'NOT_KNOWN' in cause_text
        assert error.exception.args == (cause_text,)
        assert error.message.endswith(f' failed: {cause_text}')


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

    async def test_a_poll_runs_one_statement_for_both_sides(
        self,
        broker: PostgresBroker,  # noqa: ARG002 - the schema and history leaves
        session: AsyncSession,
        proxy: RoundTripProxy,
        db_url: str,
    ) -> None:
        await insert_rows(session, make_task(task_name='alpha_task'))

        turns = await _steady_read_turns(
            proxy,
            db_url,
            lambda proxied: task_facets(
                proxied,
                window=_test_window(),
                statuses=[],
                error_categories=[],
                retried_only=False,
            ),
        )

        assert len(turns) == 1, turns

    async def test_ties_follow_the_database_collation(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        names = ['b_task', 'B_task', 'a_task', 'A_task', '_task', 'Z_task']
        await insert_rows(
            session,
            *[
                make_task(
                    task_name=name,
                    status=TaskStatus.FAILED,
                    error_code=f'DOMAIN_{name}',
                )
                for name in names
            ],
        )
        expected = (
            await session.execute(
                text(
                    'SELECT name FROM unnest(CAST(:names AS text[])) AS name '
                    'ORDER BY name'
                ),
                {'names': names},
            )
        ).scalars().all()

        result = await task_facets(
            broker,
            window=_test_window(),
            statuses=[],
            error_categories=[],
            retried_only=False,
        )

        assert is_ok(result)
        assert [f.value for f in result.ok_value.task_names] == list(expected)
        assert [f.value for f in result.ok_value.error_codes] == [
            f'DOMAIN_{name}' for name in expected
        ]

    async def test_the_result_stays_bounded_with_many_distinct_codes(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """An error code is a free string: the rows must not follow the codes."""
        distinct_codes = 1_000
        await insert_rows(
            session,
            *[
                make_task(
                    task_name='many_codes_task',
                    status=TaskStatus.FAILED,
                    error_code=f'DOMAIN_{index}',
                )
                for index in range(distinct_codes)
            ],
        )
        window = _test_window()
        builtin = sorted(BUILTIN_CODE_REGISTRY)
        rows = (
            await session.execute(
                text(
                    'SELECT facet FROM horsies_monitoring_task_facets('
                    'CAST(:lower AS timestamptz), CAST(:upper AS timestamptz), '
                    "CAST('{}' AS text[]), FALSE, "
                    'CAST(:codes AS text[]), CAST(:categories AS text[]))'
                ),
                {
                    'lower': window.lower,
                    'upper': window.upper,
                    'codes': builtin,
                    'categories': [_builtin_category(code) for code in builtin],
                },
            )
        ).all()
        result = await task_facets(
            broker,
            window=_test_window(),
            statuses=[],
            error_categories=[],
            retried_only=False,
        )

        assert len([row for row in rows if row.facet == 'error_code']) == 30
        assert is_ok(result)
        assert len(result.ok_value.error_codes) == 30
        assert result.ok_value.error_category_totals == {
            ErrorCategory.DOMAIN.value: distinct_codes,
        }

    async def test_a_live_and_a_history_row_sum_before_the_rank(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The sum decides the rank: neither side alone would put it first."""
        await insert_rows(
            session,
            make_task(task_name='split_task', status=TaskStatus.RUNNING),
            make_task(task_name='split_task', status=TaskStatus.COMPLETED),
            make_task(task_name='other_task', status=TaskStatus.COMPLETED),
        )

        result = await task_facets(
            broker,
            window=_test_window(),
            statuses=[],
            error_categories=[],
            retried_only=False,
        )

        assert is_ok(result)
        assert [(f.value, f.count) for f in result.ok_value.task_names] == [
            ('split_task', 2),
            ('other_task', 1),
        ]

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
    """Group pivot: counts of every group, the unclaimed group, limit independent of the total."""

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
            limit=BreakdownLimit(value=50),
        )

        assert is_ok(result)
        breakdown = result.ok_value
        rows = {row.group: row for row in breakdown.groups}
        assert rows[BreakdownValueGroup(value='w1')].total == 2
        assert rows[BreakdownValueGroup(value='w1')].completed == 1
        assert rows[BreakdownValueGroup(value='w1')].failed == 1
        assert rows[BreakdownValueGroup(value='w1')].retried == 1
        assert rows[BreakdownValueGroup(value='w2')].running == 1
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
            limit=BreakdownLimit(value=50),
        )

        assert is_ok(result)
        assert result.ok_value.groups == []
        assert result.ok_value.group_count == 0
        assert result.ok_value.total.total == 0

    async def test_tasks_no_worker_claimed_are_the_unclaimed_group(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(worker_id=None),
            make_task(worker_id=None, status=TaskStatus.CANCELLED),
        )

        result = await _worker_breakdown(broker)

        assert is_ok(result)
        assert [(row.group, row.total) for row in result.ok_value.groups] == [
            (BreakdownUnclaimedGroup(), 2),
        ]

    async def test_a_worker_named_unknown_is_a_group_apart_from_the_unclaimed_tasks(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(worker_id='unknown', status=TaskStatus.RUNNING),
            make_task(worker_id='unknown', status=TaskStatus.COMPLETED),
            make_task(worker_id=None),
        )

        result = await _worker_breakdown(broker)

        assert is_ok(result)
        breakdown = result.ok_value
        assert [(row.group, row.total) for row in breakdown.groups] == [
            (BreakdownValueGroup(value='unknown'), 2),
            (BreakdownUnclaimedGroup(), 1),
        ]
        assert breakdown.group_count == 2
        assert breakdown.total.total == sum(row.total for row in breakdown.groups)

    async def test_with_one_total_a_worker_named_unknown_comes_before_the_unclaimed_group(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session,
            make_task(worker_id=None, status=TaskStatus.COMPLETED),
            make_task(worker_id='unknown', status=TaskStatus.RUNNING),
            make_task(worker_id='a-worker', status=TaskStatus.RUNNING),
            make_task(worker_id='zz-worker', status=TaskStatus.RUNNING),
        )

        result = await _worker_breakdown(broker)

        assert is_ok(result)
        assert [row.group for row in result.ok_value.groups] == [
            BreakdownValueGroup(value='a-worker'),
            BreakdownValueGroup(value='unknown'),
            BreakdownUnclaimedGroup(),
            BreakdownValueGroup(value='zz-worker'),
        ]

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
            limit=BreakdownLimit(value=2),
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
            limit=BreakdownLimit(value=50),
        )

        assert is_ok(result)
        assert result.ok_value.group_by == 'task_name'
        assert [row.group for row in result.ok_value.groups] == [
            BreakdownValueGroup(value='alpha_task'),
            BreakdownValueGroup(value='beta_task'),
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
            limit=BreakdownLimit(value=50),
        )

        assert is_ok(result)
        assert [row.group for row in result.ok_value.groups] == [
            BreakdownValueGroup(value='w1'),
        ]
        assert result.ok_value.total.total == 1


def _stored_error(message: str) -> str:
    """The stored result text of a failed node whose error has ``message``."""
    failed: Result[None, TaskError] = Err(
        TaskError(error_code='ORDER_REJECTED', message=message),
    )
    match dumps_json(encode_task_result(failed, type(None))):
        case Ok(text_value):
            return text_value
        case Err(cause):
            raise AssertionError(f'the failed result does not encode: {cause}')


def _stored_value(value: int) -> str:
    """The stored result text of a node that completed with ``value``."""
    completed: Result[int, TaskError] = Ok(value)
    match dumps_json(encode_task_result(completed, int)):
        case Ok(text_value):
            return text_value
        case Err(cause):
            raise AssertionError(f'the result does not encode: {cause}')


def _uuid7_born_at(birth: datetime) -> uuid.UUID:
    """A UUIDv7 whose embedded birth is ``birth``, with random tail bits."""
    milliseconds = int(birth.timestamp() * 1000)
    value = (milliseconds << 80) | int.from_bytes(os.urandom(10), 'big')
    value = (value & ~(0xF << 76)) | (0x7 << 76)
    value = (value & ~(0x3 << 62)) | (0x2 << 62)
    return uuid.UUID(int=value)


@asynccontextmanager
async def _detached_period_leaf(
    session: AsyncSession, *, upper_anchor: datetime
) -> AsyncGenerator[None, None]:
    """A catalog row of a detached and dropped leaf of the default class, for the block.

    Only the absence rule reads it: it names no relation. The row is
    deleted at the end, so later tests see no detached leaf.
    """
    leaf_name = f'itest_detached_{uuid.uuid4().hex}'
    _ = await session.execute(
        text(
            'INSERT INTO horsies_task_history_leaf_catalog ('
            'leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, '
            'index_schema_version, created_at, detached_at, dropped_at'
            ") VALUES (:leaf, 'standard_30d', 'AT', :lower, :upper, 1, now(), now(), now())"
        ),
        {
            'leaf': leaf_name,
            'lower': upper_anchor - timedelta(days=1),
            'upper': upper_anchor,
        },
    )
    await session.commit()
    try:
        yield
    finally:
        _ = await session.execute(
            text('DELETE FROM horsies_task_history_leaf_catalog WHERE leaf_name = :leaf'),
            {'leaf': leaf_name},
        )
        await session.commit()


def _builtin_category(code: str) -> str:
    """The category of a built-in error code, as the facet function takes it."""
    match categorize_error_code(code):
        case ErrorCategory() as category:
            return category.value
        case None:
            raise AssertionError(f'built-in code {code!r} has no category')


async def _worker_breakdown(broker: PostgresBroker) -> MonitoringResult[Breakdown]:
    """The worker breakdown of every task in the test window, with no filter."""
    return await task_breakdown(
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
        limit=BreakdownLimit(value=50),
    )


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
            page=TaskPage(offset=0, limit=2),
        )

        assert is_ok(result)
        assert len(result.ok_value.rows) == 2
        assert result.ok_value.total == 5

    async def test_live_and_history_rows_carry_the_task_id_as_uuid(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        live = make_task(status=TaskStatus.PENDING)
        ended = make_task(status=TaskStatus.FAILED, error_code='TASK_EXCEPTION')
        await insert_rows(session, live, ended)

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
            page=TaskPage(offset=0, limit=10),
        )

        assert is_ok(result)
        assert {row.id for row in result.ok_value.rows} == {
            uuid.UUID(live.id), uuid.UUID(ended.id),
        }

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
            page=TaskPage(offset=1, limit=1),
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
            page=TaskPage(offset=0, limit=50),
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
            page=TaskPage(offset=0, limit=50),
        )

        assert is_ok(result)
        assert [row.task_name for row in result.ok_value.rows] == [
            'finished',
            'never_ran',
        ]

    @pytest.mark.parametrize(
        'sort_dir,expected_priorities',
        [('asc', [50, 50, 50, 100, 100, 100]), ('desc', [100, 100, 100, 50, 50, 50])],
    )
    async def test_rows_of_one_side_and_one_sort_value_come_by_task_id(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        sort_dir: Any,
        expected_priorities: list[int],
    ) -> None:
        """Each side's rows of one priority come by id, ascending in both directions.

        The rows are written in descending id order, so a read in write
        order would answer the ids descending.
        """
        live_ids = sorted((uuid.uuid4() for _ in range(3)), reverse=True)
        history_ids = sorted((uuid.uuid4() for _ in range(3)), reverse=True)
        await insert_rows(
            session,
            *[make_task(task_id=str(task_id), priority=100) for task_id in live_ids],
            *[
                make_task(task_id=str(task_id), priority=50, status=TaskStatus.COMPLETED)
                for task_id in history_ids
            ],
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
            sort_by='priority',
            sort_dir=sort_dir,
            page=TaskPage(offset=0, limit=50),
        )

        assert is_ok(result)
        rows = result.ok_value.rows
        assert [row.priority for row in rows] == expected_priorities
        by_priority = {
            priority: [row.id for row in rows if row.priority == priority]
            for priority in (50, 100)
        }
        assert by_priority == {50: sorted(history_ids), 100: sorted(live_ids)}

    @pytest.mark.parametrize('sort_dir', ['asc', 'desc'])
    async def test_a_live_row_comes_before_a_history_row_of_one_sort_value(
        self, broker: PostgresBroker, session: AsyncSession, sort_dir: Any
    ) -> None:
        """On one sort value the side decides before the id: the live row has the higher id."""
        lower_id, higher_id = sorted(uuid.uuid4() for _ in range(2))
        await insert_rows(
            session,
            make_task(task_id=str(lower_id), task_name='same_name', status=TaskStatus.FAILED),
            make_task(task_id=str(higher_id), task_name='same_name'),
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
            sort_by='task_name',
            sort_dir=sort_dir,
            page=TaskPage(offset=0, limit=50),
        )

        assert is_ok(result)
        assert [(row.id, row.status) for row in result.ok_value.rows] == [
            (higher_id, 'PENDING'),
            (lower_id, 'FAILED'),
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
            page=TaskPage(offset=0, limit=50),
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
            page=TaskPage(offset=0, limit=50),
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
            page=TaskPage(offset=0, limit=50),
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
            page=TaskPage(offset=0, limit=50),
        )

        assert is_ok(result)
        row = result.ok_value.rows[0]
        assert row.error_category == ErrorCategory.OPERATIONAL.value
        assert row.worker_id == 'worker-7'
        assert row.worker_hostname == 'host-7'

    async def test_empty_error_code_is_reported_as_absent(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(
            session, make_task(status=TaskStatus.FAILED, error_code='')
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
            page=TaskPage(offset=0, limit=50),
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
            page=TaskPage(offset=0, limit=50),
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
            page=TaskPage(offset=0, limit=50),
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
            page=TaskPage(offset=0, limit=50),
        )

        assert is_ok(result)
        assert result.ok_value.total == 5

    async def test_one_statement_reads_the_page_and_no_payload_column(
        self,
        broker: PostgresBroker,  # noqa: ARG002 - the schema and history leaves
        session: AsyncSession,
        proxy: RoundTripProxy,
        db_url: str,
    ) -> None:
        """One statement reads the page and the total; the function returns no payload column.

        A payload column on a list page would ship every task's payload on
        every poll and detoast large values.
        """
        await insert_rows(session, make_task(), make_task(status=TaskStatus.COMPLETED))

        turns = await _steady_read_turns(proxy, db_url, _unfiltered_list)

        assert len(turns) == 1, turns
        returned = (
            await session.execute(
                text(
                    'SELECT pg_get_function_result(p.oid) FROM pg_proc AS p '
                    "WHERE p.proname = 'horsies_monitoring_task_list'"
                )
            )
        ).scalar_one()
        columns = {
            column.strip().split(' ', 1)[0]
            for column in str(returned).removeprefix('TABLE(').removesuffix(')').split(',')
        }
        assert 'task_name' in columns
        assert not columns & {'input', 'task_options', 'result_payload', 'attempt_snapshot'}


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
        page=TaskPage(offset=0, limit=200),
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
                error_code='DATABASE_ERROR',
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
            limit=BreakdownLimit(value=50),
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

    async def test_missing_task_is_not_found(self, broker: PostgresBroker) -> None:
        task_id = uuid.uuid4()

        result = await get_task_detail(broker, task_id)

        assert is_ok(result)
        # A UUIDv4 has no birth, so the history read cannot classify it.
        assert result.ok_value == TaskDetailNotFound(
            task_id=task_id, reason=TaskAbsenceReason.NOT_CLASSIFIED,
        )

    async def test_an_absent_v7_task_with_no_detached_leaf_is_within_retained_history(
        self, broker: PostgresBroker
    ) -> None:
        task_id = _uuid7_born_at(datetime.now(UTC) - timedelta(days=2))

        result = await get_task_detail(broker, task_id)

        assert result == Ok(
            TaskDetailNotFound(
                task_id=task_id, reason=TaskAbsenceReason.WITHIN_RETAINED_HISTORY,
            )
        )

    async def test_an_absent_v7_task_born_before_a_detached_leaf_predates_retained_history(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        detached_upper = datetime.now(UTC) - timedelta(days=1)
        before = _uuid7_born_at(detached_upper - timedelta(hours=1))
        after = _uuid7_born_at(detached_upper + timedelta(hours=1))

        async with _detached_period_leaf(session, upper_anchor=detached_upper):
            before_result = await get_task_detail(broker, before)
            after_result = await get_task_detail(broker, after)

        assert before_result == Ok(
            TaskDetailNotFound(
                task_id=before, reason=TaskAbsenceReason.PREDATES_RETAINED_HISTORY,
            )
        )
        assert after_result == Ok(
            TaskDetailNotFound(
                task_id=after, reason=TaskAbsenceReason.WITHIN_RETAINED_HISTORY,
            )
        )

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

        result = await get_task_detail(broker, uuid.UUID(task.id))

        assert is_ok(result)
        detail = result.ok_value
        assert isinstance(detail, TaskDetail)
        assert [a.attempt for a in detail.attempts] == [1, 2]
        assert detail.attempts[0].will_retry is True
        assert detail.error_category == ErrorCategory.OPERATIONAL.value

    async def test_history_row_with_an_unknown_status_is_a_contract_violation(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.FAILED)
        await insert_rows(session, task)

        async with stored_history_status(session, task.id, 'NOT_KNOWN'):
            result = await get_task_detail(broker, uuid.UUID(task.id))

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)
        assert "'NOT_KNOWN'" in error.message

    async def test_standalone_task_has_no_workflow_linkage(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task()
        await insert_rows(session, task)

        result = await get_task_detail(broker, uuid.UUID(task.id))

        assert is_ok(result)
        detail = result.ok_value
        assert isinstance(detail, TaskDetail)
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

        result = await get_task_detail(broker, uuid.UUID(task.id))

        assert is_ok(result)
        detail = result.ok_value
        assert isinstance(detail, TaskDetail)
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

        result = await get_task_detail(broker, uuid.UUID(unrelated.id))

        assert is_ok(result)
        detail = result.ok_value
        assert isinstance(detail, TaskDetail)
        assert detail.workflow_id is None
        assert detail.workflow_task_index is None

    async def test_reports_good_until_and_workflow_membership(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        expiry = datetime.now(UTC) + timedelta(hours=2)
        task = make_task(is_workflow_task=True, good_until=expiry)
        await insert_rows(session, task)

        result = await get_task_detail(broker, uuid.UUID(task.id))

        assert is_ok(result)
        detail = result.ok_value
        assert isinstance(detail, TaskDetail)
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

        result = await get_task_detail(broker, uuid.UUID(task.id))

        assert is_ok(result)
        detail = result.ok_value
        assert isinstance(detail, TaskDetail)
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

    async def _only_row(self, broker: PostgresBroker) -> tuple[float | None, float | None]:
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
            page=TaskPage(offset=0, limit=1),
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
        now = datetime.now(UTC)
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.RUNNING,
                enqueued_at=before(now, 60),
                started_at=before(now, 40),
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 20.0
        assert exec_s is not None
        assert 40 <= exec_s <= _LIVE_SPAN_CEILING_S

    async def test_completed_task_reports_both_closed_spans(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        now = datetime.now(UTC)
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.COMPLETED,
                enqueued_at=before(now, 90),
                started_at=before(now, 60),
                completed_at=before(now, 45),
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 30.0
        assert exec_s == 15.0

    async def test_failed_task_measures_execution_to_failed_at(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        now = datetime.now(UTC)
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.FAILED,
                enqueued_at=before(now, 90),
                started_at=before(now, 70),
                failed_at=before(now, 55),
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 20.0
        assert exec_s == 15.0

    async def test_cancelled_before_start_reports_the_closed_queue_span(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """The terminal instant closes the queue span; nothing counts up.

        A history row always carries its terminal instant, so a task
        cancelled before starting reports the time it actually waited —
        a closed span, never a live count-up — and no execution span. The
        terminal instant of a task that did not complete is its ``failed_at``.
        """
        now = datetime.now(UTC)
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.CANCELLED,
                enqueued_at=before(now, 300),
                failed_at=now,
                error_code='TASK_CANCELLED',
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 300.0
        assert exec_s is None

    async def test_expired_before_start_reports_the_closed_queue_span(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        now = datetime.now(UTC)
        await insert_rows(
            session,
            make_task(
                status=TaskStatus.EXPIRED,
                enqueued_at=before(now, 300),
                failed_at=now,
            ),
        )

        queue_s, exec_s = await self._only_row(broker)

        assert queue_s == 300.0
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
        detail = await get_task_detail(broker, uuid.UUID(task.id))

        assert is_ok(detail)
        assert isinstance(detail.ok_value, TaskDetail)
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

        result = await list_workflow_runs(broker, name=None, status=None, limit=WorkflowRunsLimit(value=30))

        assert is_ok(result)
        assert [run.name for run in result.ok_value] == ['beta_flow', 'alpha_flow']

    async def test_runs_created_at_one_time_come_by_id_highest_first(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        created = ago(60)
        runs = [make_workflow(name=f'flow_{index}', created_at=created) for index in range(4)]
        await insert_rows(session, *runs)

        result = await list_workflow_runs(
            broker, name=None, status=None, limit=WorkflowRunsLimit(value=30)
        )

        assert is_ok(result)
        assert [run.id for run in result.ok_value] == sorted(
            (run.id for run in runs), key=uuid.UUID, reverse=True
        )

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
            broker, name='alpha_flow', status='FAILED', limit=WorkflowRunsLimit(value=30)
        )

        assert is_ok(result)
        assert len(result.ok_value) == 1
        assert result.ok_value[0].status == 'FAILED'

    async def test_unknown_status_yields_empty_list(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        await insert_rows(session, make_workflow(status='RUNNING'))

        result = await list_workflow_runs(
            broker, name=None, status='NOT_A_STATUS', limit=WorkflowRunsLimit(value=30)
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

        result = await list_workflow_runs(broker, name=None, status=None, limit=WorkflowRunsLimit(value=2))

        assert is_ok(result)
        assert len(result.ok_value) == 2

    async def test_wall_time_counts_up_until_completion(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        now = datetime.now(UTC)
        await insert_rows(
            session,
            make_workflow(name='open_flow', status='PAUSED', created_at=before(now, 45)),
            make_workflow(
                name='closed_flow',
                status='COMPLETED',
                created_at=before(now, 200),
                completed_at=before(now, 140),
            ),
        )

        result = await list_workflow_runs(broker, name=None, status=None, limit=WorkflowRunsLimit(value=30))

        assert is_ok(result)
        walls = {run.name: run.wall_s for run in result.ok_value}
        assert walls['closed_flow'] == 60.0
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
        now = datetime.now(UTC)
        await insert_rows(
            session,
            make_node(
                workflow_id=run.id,
                task_index=0,
                status='RUNNING',
                started_at=before(now, 35),
            ),
            make_node(
                workflow_id=run.id,
                task_index=1,
                status='COMPLETED',
                started_at=before(now, 90),
                completed_at=before(now, 60),
            ),
            make_node(
                workflow_id=run.id,
                task_index=2,
                status='SKIPPED',
                started_at=before(now, 90),
                completed_at=None,
            ),
            make_node(
                workflow_id=run.id,
                task_index=3,
                status='ENQUEUED',
                started_at=before(now, 90),
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
        assert nodes[1].exec_s == 30.0
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
                result=_stored_error('node blew up'),
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
        assert node.leaf.task_id == uuid.UUID(task.id)
        assert [a.attempt for a in node.attempts] == [1, 2]

    async def test_backing_history_row_with_an_unknown_status_is_a_contract_violation(
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
            ),
        )

        async with stored_history_status(session, task.id, 'NOT_KNOWN'):
            result = await get_workflow_node(broker, run.id, 0)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)
        assert "'NOT_KNOWN'" in error.message

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
            session,
            make_node(
                workflow_id=run.id, task_index=0, status='FAILED', result=_stored_error(''),
            ),
        )

        result = await get_workflow_node(broker, run.id, 0)

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.node_error is None

    async def test_a_node_whose_result_holds_a_value_has_no_error(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await insert_rows(session, run)
        await insert_rows(
            session,
            make_node(
                workflow_id=run.id, task_index=0, status='COMPLETED', result=_stored_value(7),
            ),
        )

        result = await get_workflow_node(broker, run.id, 0)

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.node_error is None

    async def test_a_node_result_that_does_not_decode_is_a_contract_violation(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await insert_rows(session, run)
        await insert_rows(
            session,
            make_node(
                workflow_id=run.id, task_index=0, status='FAILED', result='{"err": "boom"}',
            ),
        )

        result = await get_workflow_node(broker, run.id, 0)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryHistoryContractViolation)
        assert isinstance(error.exception, HistoryContractError)


# --------------------------------------------------------------------------- #
# Schedules
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_monitoring_tables')
class TestListSchedules:
    """Soonest next-run first. Every schedule state has a next run."""

    async def test_empty_state_yields_empty_list(self, broker: PostgresBroker) -> None:
        result = await list_schedules(broker)

        assert is_ok(result)
        assert result.ok_value == []

    async def test_ordered_by_next_run(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        soon = datetime.now(UTC) + timedelta(minutes=5)
        later = datetime.now(UTC) + timedelta(hours=3)
        last_task_id = uuid.uuid4()
        await insert_rows(
            session,
            make_schedule(schedule_name='later', next_run_at=later),
            make_schedule(
                schedule_name='soon',
                next_run_at=soon,
                last_run=LastRun(at=ago(60), task_id=str(last_task_id), run_count=4),
            ),
        )

        result = await list_schedules(broker)

        assert is_ok(result)
        assert [row.schedule_name for row in result.ok_value] == [
            'soon',
            'later',
        ]
        assert result.ok_value[0].run_count == 4
        assert result.ok_value[0].last_task_id == last_task_id
        assert result.ok_value[1].last_task_id is None

    async def test_schedules_due_at_one_time_come_by_name(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        due = datetime.now(UTC) + timedelta(minutes=5)
        await insert_rows(
            session,
            *[
                make_schedule(schedule_name=name, next_run_at=due)
                for name in ('gamma', 'alpha', 'delta', 'beta')
            ],
        )

        result = await list_schedules(broker)

        assert is_ok(result)
        assert [row.schedule_name for row in result.ok_value] == [
            'alpha',
            'beta',
            'delta',
            'gamma',
        ]


@pytest.mark.usefixtures('clean_monitoring_tables')
class TestListBounds:
    """The names and schedules lists stop at the bound and log the cut."""

    async def _seed_schedules(self, session: AsyncSession, count: int) -> None:
        _ = await session.execute(
            text(
                'INSERT INTO horsies_schedule_state ('
                'schedule_name, next_run_at, last_run_at, last_task_id, run_count, '
                'config_hash, updated_at'
                ") SELECT 'schedule_' || lpad(n::text, 6, '0'), now() + interval '1 hour', "
                "NULL, NULL, 0, sha256(n::text::bytea), now() "
                'FROM generate_series(1, :count) AS n'
            ),
            {'count': count},
        )
        await session.commit()

    async def test_schedules_past_the_bound_are_cut_and_the_cut_is_logged(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await self._seed_schedules(session, LIST_ROW_BOUND + 1)

        with caplog.at_level(logging.WARNING, logger='horsies.monitoring'):
            result = await list_schedules(broker)
            await _until_logged(caplog, 'the list is cut at')

        assert is_ok(result)
        names = [row.schedule_name for row in result.ok_value]
        assert len(names) == LIST_ROW_BOUND
        assert names[-1] == f'schedule_{LIST_ROW_BOUND:06d}'
        assert [record.getMessage() for record in caplog.records] == [
            f'schedule state: the list is cut at {LIST_ROW_BOUND} schedules; '
            'more exist and are not returned',
        ]

    async def test_schedules_at_the_bound_are_complete_and_nothing_is_logged(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await self._seed_schedules(session, LIST_ROW_BOUND)

        with caplog.at_level(logging.WARNING, logger='horsies.monitoring'):
            result = await list_schedules(broker)
            await asyncio.sleep(_FORWARD_WAIT_S)

        assert is_ok(result)
        assert len(result.ok_value) == LIST_ROW_BOUND
        assert caplog.records == []

    async def test_workflow_names_past_the_bound_are_cut_and_the_cut_is_logged(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _ = await session.execute(
            text(
                'INSERT INTO horsies_workflows ('
                'id, name, definition_key, status, on_error, output_task_index, '
                'success_policy, result, error, parent_workflow_id, parent_task_index, '
                'root_workflow_id, depth, sent_at, created_at, started_at, completed_at, '
                'updated_at'
                ') SELECT run.id, '
                "'flow_' || lpad(run.n::text, 6, '0'), NULL, 'RUNNING', 'fail', NULL, "
                'NULL, NULL, NULL, NULL, NULL, run.id, 0, now(), now(), NULL, NULL, now() '
                'FROM (SELECT horsies_uuidv7() AS id, n FROM generate_series(1, :count) AS n) AS run'
            ),
            {'count': LIST_ROW_BOUND + 1},
        )
        await session.commit()

        with caplog.at_level(logging.WARNING, logger='horsies.monitoring'):
            result = await list_workflow_names(broker)
            await _until_logged(caplog, 'the list is cut at')

        assert is_ok(result)
        assert len(result.ok_value) == LIST_ROW_BOUND
        assert result.ok_value[-1] == f'flow_{LIST_ROW_BOUND:06d}'
        assert [record.getMessage() for record in caplog.records] == [
            f'workflow names: the list is cut at {LIST_ROW_BOUND} root workflow names; '
            'more exist and are not returned',
        ]

    async def test_the_fields_of_the_cut_warning_are_attributes_of_the_record(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The core logs the cut with the ``tracing`` fields ``read`` and
        ``bound``. The bridged record's message is the text alone, and each
        field is an attribute of the record with its type, as ``extra`` sets
        it."""
        await self._seed_schedules(session, LIST_ROW_BOUND + 1)

        with caplog.at_level(logging.WARNING, logger='horsies.monitoring'):
            result = await list_schedules(broker)
            await _until_logged(caplog, 'the list is cut at')

        assert is_ok(result)
        match caplog.records:
            case [record]:
                pass
            case records:
                raise AssertionError(f'one record expected, got {records!r}')
        assert record.name == 'horsies.monitoring'
        assert record.getMessage() == (
            f'schedule state: the list is cut at {LIST_ROW_BOUND} schedules; '
            'more exist and are not returned'
        )
        read: object = getattr(record, 'read')
        bound: object = getattr(record, 'bound')
        assert (read, bound) == ('schedule state', LIST_ROW_BOUND)
        assert type(bound) is int


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
                    'postgresql://postgres:none@127.0.0.1:1/none'
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
            page=TaskPage(offset=0, limit=50),
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, MonitoringQueryDatabaseOperationFailed)
        assert error.retryable is True
        assert 'task list query failed' in error.message
        # The driver error of Python's read, or the core's class of the cause
        # of the read on the core: no answer from the server.
        assert isinstance(error.exception, SQLAlchemyError | CoreConnectionError)
        match error.exception:
            case CoreConnectionError():
                # Python's texts: the message names the read and holds the
                # cause's text; the exception is that text with the read as
                # its subject.
                assert error.message.startswith('task list query failed: ')
                cause_text = error.message.removeprefix('task list query failed: ')
                assert cause_text != ''
                assert error.exception.subject == 'task list query'
                assert error.exception.message == cause_text
                assert str(error.exception) == f'task list query: {cause_text}'
            case _:
                pass

    async def test_detail_failure_is_not_confused_with_absence(
        self, unreachable_broker: PostgresBroker
    ) -> None:
        result = await get_task_detail(unreachable_broker, uuid.uuid4())

        assert is_err(result)
        assert isinstance(result.err_value, MonitoringQueryDatabaseOperationFailed)

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
@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    """A round-trip counting proxy in front of the test database."""
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


async def _steady_read_turns(
    proxy: RoundTripProxy,
    db_url: str,
    read: Callable[[PostgresBroker], Awaitable[object]],
) -> tuple[Turn, ...]:
    """The client turns of ``read`` in the steady state of prepared statements,
    on a broker that connects through ``proxy``. Python's read and the read on
    the core both connect through it, so the count holds for either."""
    proxied = PostgresBroker(
        PostgresConfig(database_url=SecretStr(proxied_url(db_url, proxy))),
    )

    async def arrange() -> None:
        return None

    async def act(_case: None) -> None:
        result = await read(proxied)
        assert isinstance(result, Ok), result

    try:
        _, turns = await steady_state_turns(proxy, arrange, act)
    finally:
        _ = await proxied.close_async()
    return turns


async def _unfiltered_list(
    broker: PostgresBroker, window: MonitoringWindow | None = None,
) -> Any:
    return await list_tasks(
        broker,
        window=window if window is not None else _test_window(),
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


def _plan_rows(payload: object) -> int:
    """The row estimate of the top plan node of EXPLAIN (FORMAT JSON) output,
    as text or as parsed JSON."""
    loaded: object = json.loads(payload) if isinstance(payload, str) else payload
    match loaded:
        case [{'Plan': {'Plan Rows': bool()}}, *_]:
            raise AssertionError(f'a boolean row estimate: {payload!r}')
        case [{'Plan': {'Plan Rows': int() | float() as rows}}, *_]:
            return int(rows)
        case _:
            raise AssertionError(f'not EXPLAIN (FORMAT JSON) output: {payload!r}')


@pytest.mark.usefixtures('clean_monitoring_tables')
class TestUnfilteredHistoryTotalContract:
    """The documented total contract holds on the history side.

    ``TaskListPage`` documents the unfiltered total as a planner
    estimate: the live side and the history side estimate together, and
    count exactly together when any filter is active. The function answers
    which one it ran in its TOTALS row; the history estimate is EXPLAIN
    text that the read decodes, so the decode runs only for the estimate.
    A history estimate that does not decode is a contract violation: the
    reference's decode seam and the core's own test
    (``an_unreadable_estimate_is_a_contract_violation_that_is_not_retryable``)
    cover it, since the read of the core has no seam to inject it here.
    """

    async def test_unfiltered_total_estimates_and_never_counts_history(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
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
        window = _test_window()

        result = await _unfiltered_list(broker, window)
        totals = (
            await session.execute(
                text(
                    'SELECT total_kind, live_total, history_estimate_plan '
                    'FROM horsies_monitoring_task_list('
                    'CAST(:window_lower AS timestamptz), CAST(:window_upper AS timestamptz), '
                    "CAST('{}' AS text[]), CAST('{}' AS text[]), CAST('{}' AS text[]), "
                    "CAST('{}' AS text[]), CAST('{}' AS text[]), CAST('{}' AS text[]), FALSE, "
                    "CAST('{}' AS text[]), CAST('{}' AS text[]), 'enqueued_at', 'DESC', 0, 50) "
                    "WHERE row_kind = 'TOTALS'"
                ),
                {'window_lower': window.lower, 'window_upper': window.upper},
            )
        ).one()

        assert is_ok(result)
        assert totals.total_kind == 'ESTIMATE_HISTORY_PLAN'
        assert totals.live_total == 2
        assert str(totals.history_estimate_plan).startswith('[')
        assert result.ok_value.total == 2 + _plan_rows(totals.history_estimate_plan)

    async def test_filtered_total_counts_history_exactly(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        await insert_rows(
            session,
            make_task(),
            make_task(status=TaskStatus.COMPLETED),
            make_task(status=TaskStatus.COMPLETED),
            make_task(status=TaskStatus.COMPLETED),
        )

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
            page=TaskPage(offset=0, limit=50),
        )

        assert is_ok(result)
        assert result.ok_value.total == 3

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
