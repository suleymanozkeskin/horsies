"""Dual-run of the task list switch: the client statements against ``horsies_monitoring_task_list``.

Reference: ``tests/dualrun/monitoring_list_reference.py`` (``list_tasks``
as the totals, the live page and the history page in one session, merged
in client code, in the decided order). Candidate: one call of
``horsies_monitoring_task_list``, Python's body before the monitoring switch
(``tests/dualrun/reference/monitoring_queries.py``). The seed has live and
history rows that tie on every sort key: one ``enqueued_at`` on both sides,
one task name in two cases, one priority, NULL start and end times; the
history rows hold the lower task ids, so an order by id alone puts them
first. Each sort key, direction, filter set and page must give equal
answers.

A live row's open span counts up to the read time, so both sides read
their spans at one fixed instant (``elapsed_s`` is replaced for the run).

The caller switch pair of the monitoring switch: on the same seed, Python's
``list_tasks`` against the production read, which the core runs. The core
reads its own clock, so an open span of the core may be up to
``COUNTING_SPAN_TOLERANCE_S`` longer; every other value of the reports must
be equal.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Final, get_args

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import OperationalErrorCode, OutcomeCode
from horsies.core.types.result import Ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring import (
    ErrorCategory,
    MonitoringWindow,
    TaskListPage,
    TaskPage,
    list_tasks,
)
from horsies.monitoring.errors import MonitoringResult
from horsies.monitoring.queries import SortDirection, TaskSortField, as_utc
from tests.dualrun import monitoring_list_reference as reference
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
_READ_NOW: Final = _NOW + timedelta(minutes=1)
"""The instant both sides measure a live row's open span to."""

# The seeded history rows end inside the last two minutes, so the history
# leaves that the seed covers hold them; H8 ends after the window.
_WINDOW: Final = MonitoringWindow(
    lower=_NOW - timedelta(seconds=120), upper=_NOW - timedelta(seconds=5)
)
_T0: Final = _NOW - timedelta(seconds=600)

_OPERATIONAL: Final = next(iter(OperationalErrorCode)).value
_OUTCOME: Final = next(iter(OutcomeCode)).value

_LIVE_LABELS: Final = ('L1', 'L2', 'L3', 'L4', 'L5', 'L6')
_HISTORY_LABELS: Final = ('H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7', 'H8')


def _ids() -> dict[str, str]:
    """Task ids by label: every history id is lower than every live id."""
    ordered = sorted(str(uuid.uuid4()) for _ in range(len(_HISTORY_LABELS) + len(_LIVE_LABELS)))
    return dict(zip(_HISTORY_LABELS + _LIVE_LABELS, ordered, strict=True))


_ID: Final = _ids()
_LABEL: Final = {task_id: label for label, task_id in _ID.items()}


def _task(
    label: str,
    *,
    status: TaskStatus,
    task_name: str = 'list.alpha',
    queue_name: str = 'default',
    priority: int = 100,
    retry_count: int = 0,
    enqueued_at: datetime = _T0,
    started_at: datetime | None = None,
    ended_ago: timedelta | None = None,
    worker_id: str | None = None,
    error_code: str | None = None,
) -> TaskRow:
    terminal_at = _NOW - ended_ago if ended_ago is not None else None
    return task_row(
        task_id=_ID[label],
        task_name=task_name,
        queue_name=queue_name,
        status=status,
        priority=priority,
        retry_count=retry_count,
        max_retries=3,
        is_workflow_task=False,
        enqueued_at=enqueued_at,
        claimed_at=enqueued_at if worker_id is not None else None,
        started_at=started_at,
        completed_at=terminal_at if status is TaskStatus.COMPLETED else None,
        failed_at=terminal_at if status.is_terminal and status is not TaskStatus.COMPLETED else None,
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
    started = _T0 + timedelta(seconds=5)
    live = (
        _task('L1', status=TaskStatus.PENDING),
        _task('L2', status=TaskStatus.PENDING, queue_name='fast'),
        _task(
            'L3',
            status=TaskStatus.CLAIMED,
            task_name='list.beta',
            priority=50,
            enqueued_at=_T0 - timedelta(seconds=10),
            worker_id='worker-a',
        ),
        _task('L4', status=TaskStatus.RUNNING, retry_count=1, started_at=started, worker_id='worker-a'),
        _task(
            'L5',
            status=TaskStatus.RUNNING,
            task_name='List.alpha',
            enqueued_at=_T0 - timedelta(seconds=20),
            started_at=started,
            worker_id='worker-b',
        ),
        _task('L6', status=TaskStatus.PENDING, task_name='list.gamma', retry_count=2, enqueued_at=_T0 - timedelta(seconds=30)),
    )
    history = (
        _task('H1', status=TaskStatus.COMPLETED, started_at=started, ended_ago=timedelta(seconds=20), worker_id='worker-a'),
        # The same sort value as H1 on every key.
        _task('H2', status=TaskStatus.COMPLETED, started_at=started, ended_ago=timedelta(seconds=20), worker_id='worker-a'),
        _task(
            'H3',
            status=TaskStatus.FAILED,
            task_name='list.beta',
            priority=50,
            retry_count=2,
            enqueued_at=_T0 - timedelta(seconds=10),
            started_at=_T0,
            ended_ago=timedelta(seconds=30),
            worker_id='worker-b',
            error_code=_OPERATIONAL,
        ),
        _task(
            'H4',
            status=TaskStatus.FAILED,
            task_name='List.alpha',
            enqueued_at=_T0 - timedelta(seconds=20),
            ended_ago=timedelta(seconds=40),
            error_code='PAYMENT_DECLINED',
        ),
        _task(
            'H5',
            status=TaskStatus.CANCELLED,
            task_name='list.gamma',
            queue_name='fast',
            ended_ago=timedelta(seconds=50),
            error_code=_OUTCOME,
        ),
        _task('H6', status=TaskStatus.EXPIRED, enqueued_at=_T0 - timedelta(seconds=30), ended_ago=timedelta(seconds=60)),
        _task(
            'H7',
            status=TaskStatus.FAILED,
            task_name='list.beta',
            started_at=started,
            ended_ago=timedelta(seconds=20),
            worker_id='worker-a',
            error_code='',
        ),
        # After the window: on neither side.
        _task('H8', status=TaskStatus.COMPLETED, ended_ago=timedelta(seconds=2)),
    )
    return live + history


type _Filters = tuple[list[TaskStatus], list[str], list[str], list[str], list[str], list[ErrorCategory], bool]
"""statuses, task names, queues, workers, error codes, error categories, retried only."""

_FILTERS: Final[tuple[tuple[str, _Filters], ...]] = (
    ('none', ([], [], [], [], [], [], False)),
    ('statuses', ([TaskStatus.PENDING, TaskStatus.COMPLETED], [], [], [], [], [], False)),
    ('task names', ([], ['list.alpha', 'List.alpha'], [], [], [], [], False)),
    ('queue', ([], [], ['fast'], [], [], [], False)),
    ('worker', ([], [], [], ['worker-a'], [], [], False)),
    ('error codes', ([], [], [], [], [_OPERATIONAL, 'PAYMENT_DECLINED'], [], False)),
    ('domain', ([], [], [], [], [], [ErrorCategory.DOMAIN], False)),
    ('two categories', ([], [], [], [], [], [ErrorCategory.OPERATIONAL, ErrorCategory.OUTCOME], False)),
    ('retried', ([], [], [], [], [], [], True)),
)

_PAGES: Final = (TaskPage(offset=0, limit=200), TaskPage(offset=2, limit=3), TaskPage(offset=5, limit=4))

_SORT_KEYS: Final[tuple[TaskSortField, ...]] = get_args(TaskSortField.__value__)
_DIRECTIONS: Final[tuple[SortDirection, ...]] = get_args(SortDirection.__value__)

type _ListRead = Callable[..., Awaitable[MonitoringResult[TaskListPage]]]


def _list_pair(
    sort_by: TaskSortField,
    sort_dir: SortDirection,
    filter_name: str,
    filters: _Filters,
    page: TaskPage,
    *,
    reference_list: _ListRead = reference.list_tasks,
    candidate_list: _ListRead = python_reads.list_tasks,
) -> ReadPair[object]:
    statuses, task_names, queues, workers, error_codes, categories, retried_only = filters

    def read(list_read: _ListRead) -> Callable[[PostgresBroker], Awaitable[object]]:
        async def run(broker: PostgresBroker) -> object:
            return await list_read(
                broker,
                window=_WINDOW,
                statuses=statuses,
                task_names=task_names,
                queues=queues,
                workers=workers,
                error_codes=error_codes,
                error_categories=categories,
                retried_only=retried_only,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
            )

        return run

    return ReadPair(
        name=f'{sort_by} {sort_dir}, {filter_name}, offset {page.offset} limit {page.limit}',
        reference=read(reference_list),
        candidate=read(candidate_list),
    )


def _elapsed_at_read_now(start: datetime | None, end: datetime | None) -> int | None:
    """``elapsed_s`` with ``_READ_NOW`` as the end of an open span."""
    if start is None:
        return None
    finish = as_utc(end) if end is not None else _READ_NOW
    return int((finish - as_utc(start)).total_seconds())


@pytest.fixture
def fixed_read_time(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Both sides measure open spans to ``_READ_NOW``."""
    monkeypatch.setattr(python_reads, 'elapsed_s', _elapsed_at_read_now)
    yield


def _labels(answer: object) -> list[str]:
    match answer:
        case Ok(TaskListPage(rows=rows)):
            return [_LABEL[str(row.id)] for row in rows]
        case _:
            raise AssertionError(f'expected a page, got {answer!r}')


@pytest.mark.usefixtures('fixed_read_time')
def test_the_function_answers_what_the_client_statements_answered(
    empty_database: MakeDatabase,
) -> None:
    pairs = [
        _list_pair(sort_by, sort_dir, filter_name, filters, page)
        for sort_by in _SORT_KEYS
        for sort_dir in _DIRECTIONS
        for filter_name, filters in _FILTERS
        for page in _PAGES
    ]

    answers = read_dual_run(empty_database, _seed(), pairs)

    assert len(answers) == len(pairs) == 11 * 2 * 9 * 3
    assert [each.name for each in answers if not isinstance(each.candidate, Ok)] == []
    assert differing(answers) == []
    by_name = {each.name: each.candidate for each in answers}
    # One enqueued_at on both sides: LIVE before HISTORY, then the id; the
    # history ids are the lower ones.
    assert _labels(by_name['enqueued_at desc, none, offset 0 limit 200']) == [
        'L1', 'L2', 'L4', 'H1', 'H2', 'H5', 'H7', 'L3', 'H3', 'L5', 'H4', 'L6', 'H6',
    ]
    # NULL completed_at last in both directions; H1 and H2 tie on the value.
    assert _labels(by_name['completed_at asc, none, offset 0 limit 200']) == _labels(
        by_name['completed_at desc, none, offset 0 limit 200']
    ) == ['H1', 'H2', 'L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'H3', 'H4', 'H5', 'H6', 'H7']
    # The test server's collation (C.UTF-8) orders 'List.alpha' before 'list.alpha'.
    assert _labels(by_name['task_name asc, none, offset 0 limit 200']) == [
        'L5', 'H4', 'L1', 'L2', 'L4', 'H1', 'H2', 'H6', 'L3', 'H3', 'H7', 'L6', 'H5',
    ]


@pytest.mark.usefixtures('fixed_read_time')
def test_the_comparison_sees_a_candidate_that_reads_the_other_direction(
    empty_database: MakeDatabase,
) -> None:
    """The candidate reads the page ascending where the reference reads it descending; the answers must differ."""
    _, filters = _FILTERS[0]
    pair = _list_pair('enqueued_at', 'desc', 'none', filters, _PAGES[0])
    flipped = _list_pair('enqueued_at', 'asc', 'none', filters, _PAGES[0])

    answers = read_dual_run(
        empty_database,
        _seed(),
        [ReadPair(name=pair.name, reference=pair.reference, candidate=flipped.candidate)],
    )

    assert differing(answers) == [pair.name]


def test_the_core_answers_what_python_answered(empty_database: MakeDatabase) -> None:
    """The caller switch: Python's list read against the production read on the core."""
    pairs = [
        _list_pair(
            sort_by,
            sort_dir,
            filter_name,
            filters,
            page,
            reference_list=python_reads.list_tasks,
            candidate_list=list_tasks,
        )
        for sort_by in _SORT_KEYS
        for sort_dir in _DIRECTIONS
        for filter_name, filters in _FILTERS
        for page in _PAGES
    ]

    answers = read_dual_run(empty_database, _seed(), pairs)

    assert len(answers) == 11 * 2 * 9 * 3
    assert [each.name for each in answers if not isinstance(each.candidate, Ok)] == []
    assert differing_reports(answers) == {}


def test_the_core_comparison_sees_a_candidate_that_reads_the_other_direction(
    empty_database: MakeDatabase,
) -> None:
    """The core candidate reads the page ascending where Python reads it descending."""
    _, filters = _FILTERS[0]
    pair = _list_pair(
        'enqueued_at',
        'desc',
        'none',
        filters,
        _PAGES[0],
        reference_list=python_reads.list_tasks,
        candidate_list=list_tasks,
    )
    flipped = _list_pair(
        'enqueued_at',
        'asc',
        'none',
        filters,
        _PAGES[0],
        reference_list=python_reads.list_tasks,
        candidate_list=list_tasks,
    )

    answers = read_dual_run(
        empty_database,
        _seed(),
        [ReadPair(name=pair.name, reference=pair.reference, candidate=flipped.candidate)],
    )

    assert list(differing_reports(answers)) == [pair.name]
