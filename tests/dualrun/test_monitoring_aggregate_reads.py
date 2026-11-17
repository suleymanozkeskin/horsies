"""Dual-run of the aggregate switch: the client statements against the monitoring functions.

Reference: ``tests/dualrun/monitoring_reference.py`` (``task_stats``,
``task_facets`` and ``task_breakdown`` as two statements and a client
merge). Candidate: one call of ``horsies_monitoring_task_stats``,
``horsies_monitoring_task_facets`` and ``horsies_monitoring_task_breakdown``,
Python's bodies before the monitoring switch
(``tests/dualrun/reference/monitoring_queries.py``). The seed has live
and history rows of every status, claimed and unclaimed rows, a worker named
``unknown``, error codes of every built-in category, user codes and an empty
code. Each scope must give equal answers.

The caller switch pair of the monitoring switch: on the same seed, Python's
three reads against the production reads, which the core runs. The reports
must be equal.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Final

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import (
    ContractCode,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
)
from horsies.core.types.status import TaskStatus
from horsies.core.types.result import Ok
from horsies.monitoring import (
    BreakdownLimit,
    ErrorCategory,
    MonitoringWindow,
    StatusCount,
    task_breakdown,
    task_facets,
    task_stats,
)
from horsies.monitoring.errors import MonitoringResult
from horsies.monitoring.queries import TaskGroupBy
from tests.dualrun import monitoring_reference as reference
from tests.dualrun.monitoring_reads import (
    MakeDatabase,
    ReadPair,
    differing,
    differing_reports,
    read_dual_run,
)
from tests.dualrun.reference import monitoring_queries as python_reads
from tests.integration.monitoring_seeding import SeedRow, TaskRow, task_row

pytestmark = [pytest.mark.integration]

_NOW: Final = datetime.now(UTC)
# The seeded history rows end inside the last two minutes, so the history
# leaves that the seed covers hold them; the last one ends after the window.
_WINDOW: Final = MonitoringWindow(
    lower=_NOW - timedelta(seconds=120), upper=_NOW - timedelta(seconds=5)
)

_OPERATIONAL: Final = next(iter(OperationalErrorCode)).value
_CONTRACT: Final = next(iter(ContractCode)).value
_RETRIEVAL: Final = next(iter(RetrievalCode)).value
_OUTCOME: Final = next(iter(OutcomeCode)).value


def _task(
    *,
    status: TaskStatus,
    task_name: str = 'aggregate.alpha',
    queue_name: str = 'default',
    worker_id: str | None = None,
    retry_count: int = 0,
    error_code: str | None = None,
    ended_ago: timedelta | None = None,
) -> TaskRow:
    terminal_at = _NOW - ended_ago if ended_ago is not None else None
    return task_row(
        task_id=str(uuid.uuid4()),
        task_name=task_name,
        queue_name=queue_name,
        status=status,
        priority=100,
        retry_count=retry_count,
        max_retries=3,
        is_workflow_task=False,
        enqueued_at=_NOW - timedelta(seconds=600),
        claimed_at=None,
        started_at=None,
        completed_at=terminal_at if status is TaskStatus.COMPLETED else None,
        failed_at=terminal_at if status is not TaskStatus.COMPLETED else None,
        good_until=None,
        error_code=error_code,
        failed_reason=None,
        worker_id=worker_id,
        worker_hostname=None if worker_id is None else 'host-1',
        worker_pid=None,
        worker_process_name=None,
        claim_expires_at=None,
    )


def _seed() -> tuple[SeedRow, ...]:
    live = (
        _task(status=TaskStatus.PENDING),
        _task(status=TaskStatus.PENDING, queue_name='fast', task_name='aggregate.beta'),
        _task(status=TaskStatus.CLAIMED, worker_id='worker-a'),
        _task(status=TaskStatus.RUNNING, worker_id='worker-a', retry_count=1),
        _task(status=TaskStatus.RUNNING, worker_id='unknown', queue_name='fast'),
    )
    history = (
        _task(status=TaskStatus.COMPLETED, worker_id='worker-a', ended_ago=timedelta(seconds=10)),
        _task(status=TaskStatus.COMPLETED, worker_id='unknown', ended_ago=timedelta(seconds=20)),
        _task(
            status=TaskStatus.FAILED,
            worker_id='worker-b',
            error_code=_OPERATIONAL,
            retry_count=2,
            ended_ago=timedelta(seconds=30),
        ),
        _task(
            status=TaskStatus.FAILED,
            worker_id='worker-b',
            error_code=_CONTRACT,
            task_name='aggregate.beta',
            ended_ago=timedelta(seconds=40),
        ),
        _task(status=TaskStatus.FAILED, error_code=_RETRIEVAL, ended_ago=timedelta(seconds=50)),
        _task(status=TaskStatus.CANCELLED, error_code=_OUTCOME, ended_ago=timedelta(seconds=60)),
        _task(
            status=TaskStatus.FAILED,
            worker_id='worker-a',
            error_code='PAYMENT_DECLINED',
            queue_name='fast',
            retry_count=1,
            ended_ago=timedelta(seconds=70),
        ),
        _task(status=TaskStatus.FAILED, worker_id='worker-a', error_code='', ended_ago=timedelta(seconds=80)),
        _task(status=TaskStatus.EXPIRED, ended_ago=timedelta(seconds=90)),
        # After the window: counted by neither side.
        _task(status=TaskStatus.COMPLETED, worker_id='worker-a', ended_ago=timedelta(seconds=2)),
    )
    return live + history


type _Filters = tuple[list[str], list[str], list[str], list[str], list[ErrorCategory], bool]
"""task names, queues, workers, error codes, error categories, retried only."""

_FILTERS: Final[tuple[tuple[str, _Filters], ...]] = (
    ('none', ([], [], [], [], [], False)),
    ('task name', (['aggregate.beta'], [], [], [], [], False)),
    ('queue', ([], ['fast'], [], [], [], False)),
    ('workers', ([], [], ['worker-a', 'unknown'], [], [], False)),
    ('error code', ([], [], [], [_OPERATIONAL, 'PAYMENT_DECLINED'], [], False)),
    ('operational', ([], [], [], [], [ErrorCategory.OPERATIONAL], False)),
    ('domain', ([], [], [], [], [ErrorCategory.DOMAIN], False)),
    (
        'two categories',
        ([], [], [], [], [ErrorCategory.CONTRACT, ErrorCategory.OUTCOME], False),
    ),
    ('retried', ([], [], [], [], [], True)),
    ('queue and worker', ([], ['fast'], ['worker-a'], [], [], False)),
)

_STATUS_SETS: Final[tuple[tuple[str, list[TaskStatus]], ...]] = (
    ('all statuses', []),
    ('failed', [TaskStatus.FAILED]),
    ('pending and completed', [TaskStatus.PENDING, TaskStatus.COMPLETED]),
)

type _StatsRead = Callable[..., Awaitable[MonitoringResult[object]]]


def _stats_pair(
    name: str,
    filters: _Filters,
    *,
    reference_stats: _StatsRead = reference.task_stats,
    candidate_stats: _StatsRead = python_reads.task_stats,
) -> ReadPair[object]:
    task_names, queues, workers, error_codes, categories, retried_only = filters

    def read(stats: _StatsRead) -> Callable[[PostgresBroker], Awaitable[object]]:
        async def run(broker: PostgresBroker) -> object:
            return await stats(
                broker,
                window=_WINDOW,
                task_names=task_names,
                queues=queues,
                workers=workers,
                error_codes=error_codes,
                error_categories=categories,
                retried_only=retried_only,
            )

        return run

    return ReadPair(
        name=f'stats, {name}',
        reference=read(reference_stats),
        candidate=read(candidate_stats),
    )


def _breakdown_pair(
    group_by: TaskGroupBy,
    status_name: str,
    statuses: list[TaskStatus],
    filter_name: str,
    filters: _Filters,
    limit: int,
    *,
    reference_breakdown: _StatsRead = reference.task_breakdown,
    candidate_breakdown: _StatsRead = python_reads.task_breakdown,
) -> ReadPair[object]:
    task_names, queues, workers, error_codes, categories, retried_only = filters

    def read(breakdown: _StatsRead) -> Callable[[PostgresBroker], Awaitable[object]]:
        async def run(broker: PostgresBroker) -> object:
            return await breakdown(
                broker,
                window=_WINDOW,
                group_by=group_by,
                statuses=statuses,
                task_names=task_names,
                queues=queues,
                workers=workers,
                error_codes=error_codes,
                error_categories=categories,
                retried_only=retried_only,
                limit=BreakdownLimit(value=limit),
            )

        return run

    return ReadPair(
        name=f'breakdown by {group_by}, {status_name}, {filter_name}, limit {limit}',
        reference=read(reference_breakdown),
        candidate=read(candidate_breakdown),
    )


def _facets_pair(
    status_name: str,
    statuses: list[TaskStatus],
    categories: list[ErrorCategory],
    retried_only: bool,
    *,
    reference_facets: _StatsRead,
    candidate_facets: _StatsRead,
) -> ReadPair[object]:
    def read(facets: _StatsRead) -> Callable[[PostgresBroker], Awaitable[object]]:
        async def run(broker: PostgresBroker) -> object:
            return await facets(
                broker,
                window=_WINDOW,
                statuses=statuses,
                error_categories=categories,
                retried_only=retried_only,
            )

        return run

    return ReadPair(
        name=f'facets, {status_name}, categories {categories}, retried {retried_only}',
        reference=read(reference_facets),
        candidate=read(candidate_facets),
    )


_FACET_SCOPES: Final = tuple(
    (status_name, statuses, categories, retried_only)
    for status_name, statuses in _STATUS_SETS
    for categories in ([], [ErrorCategory.DOMAIN], [ErrorCategory.OPERATIONAL, ErrorCategory.CONTRACT])
    for retried_only in (False, True)
)


def _pairs() -> list[ReadPair[object]]:
    pairs = [_stats_pair(name, filters) for name, filters in _FILTERS]
    pairs += [
        _breakdown_pair(group_by, status_name, statuses, filter_name, filters, limit)
        for group_by in ('worker', 'task_name', 'queue')
        for status_name, statuses in _STATUS_SETS
        for filter_name, filters in _FILTERS
        for limit in (1, 2, 500)
    ]
    pairs += [
        _facets_pair(
            status_name,
            statuses,
            categories,
            retried_only,
            reference_facets=reference.task_facets,
            candidate_facets=python_reads.task_facets,
        )
        for status_name, statuses, categories, retried_only in _FACET_SCOPES
    ]
    return pairs


def test_the_functions_answer_what_the_client_statements_answered(
    empty_database: MakeDatabase,
) -> None:
    pairs = _pairs()

    answers = read_dual_run(empty_database, _seed(), pairs)

    assert len(answers) == len(pairs) == 10 + 270 + 18
    assert [each.name for each in answers if not isinstance(each.candidate, Ok)] == []
    assert differing(answers) == []
    # 5 live rows and 9 history rows inside the window.
    assert answers[0].candidate == Ok(
        [
            StatusCount(status=status, count=count)
            for status, count in (
                ('PENDING', 2),
                ('CLAIMED', 1),
                ('RUNNING', 2),
                ('COMPLETED', 2),
                ('FAILED', 5),
                ('CANCELLED', 1),
                ('EXPIRED', 1),
            )
        ]
    )


def test_the_comparison_sees_a_candidate_that_drops_a_filter(
    empty_database: MakeDatabase,
) -> None:
    """The candidate reads with the retried filter dropped; the answers must differ."""
    _, filters = _FILTERS[8]
    task_names, queues, workers, error_codes, categories, _ = filters

    async def dropped(broker: PostgresBroker) -> object:
        return await task_stats(
            broker,
            window=_WINDOW,
            task_names=task_names,
            queues=queues,
            workers=workers,
            error_codes=error_codes,
            error_categories=categories,
            retried_only=False,
        )

    pair = _stats_pair('retried', filters)
    answers = read_dual_run(
        empty_database,
        _seed(),
        [ReadPair(name=pair.name, reference=pair.reference, candidate=dropped)],
    )

    assert differing(answers) == [pair.name]


def test_the_core_answers_what_python_answered(empty_database: MakeDatabase) -> None:
    """The caller switch: Python's three reads against the production reads on the core."""
    pairs = [
        _stats_pair(
            name, filters, reference_stats=python_reads.task_stats, candidate_stats=task_stats,
        )
        for name, filters in _FILTERS
    ]
    pairs += [
        _breakdown_pair(
            group_by,
            status_name,
            statuses,
            filter_name,
            filters,
            limit,
            reference_breakdown=python_reads.task_breakdown,
            candidate_breakdown=task_breakdown,
        )
        for group_by in ('worker', 'task_name', 'queue')
        for status_name, statuses in _STATUS_SETS
        for filter_name, filters in _FILTERS
        for limit in (1, 2, 500)
    ]
    pairs += [
        _facets_pair(
            status_name,
            statuses,
            categories,
            retried_only,
            reference_facets=python_reads.task_facets,
            candidate_facets=task_facets,
        )
        for status_name, statuses, categories, retried_only in _FACET_SCOPES
    ]

    answers = read_dual_run(empty_database, _seed(), pairs)

    assert len(answers) == 10 + 270 + 18
    assert [each.name for each in answers if not isinstance(each.candidate, Ok)] == []
    assert differing_reports(answers) == {}


def test_the_core_comparison_sees_a_candidate_that_drops_the_category_selection(
    empty_database: MakeDatabase,
) -> None:
    """The core candidate reads without the category selection; the reports must differ."""
    status_name, statuses, categories, retried_only = _FACET_SCOPES[2]
    pair = _facets_pair(
        status_name,
        statuses,
        categories,
        retried_only,
        reference_facets=python_reads.task_facets,
        candidate_facets=task_facets,
    )

    async def dropped(broker: PostgresBroker) -> object:
        return await task_facets(
            broker, window=_WINDOW, statuses=statuses, error_categories=[], retried_only=retried_only,
        )

    answers = read_dual_run(
        empty_database,
        _seed(),
        [ReadPair(name=pair.name, reference=pair.reference, candidate=dropped)],
    )

    assert list(differing_reports(answers)) == [pair.name]
