# pyright: reportPrivateUsage=false
"""Dual-runs of the scheduler: the fire redesign, and the switch of the loop to the core.

Fire pairs. Reference: ``tests/dualrun/reference/schedule_fire.py`` (the
advisory lock, the state read, one ``enqueue_async`` per slot, the state
update). Candidate: ``Scheduler._check_schedule`` of the Python loop
(``tests/dualrun/reference/scheduler_loop_caller.py``; one call of
``horsies_fire_schedule``). Each case runs on its own pair of databases: a
started scheduler with one schedule, the state row set for the case, then one
fire at the database time of the act. The snapshots must be equal, except
where the one statement deliberately differs:

- a catch-up of several slots writes every slot row in one statement, so
  the slots share one ``enqueued_at`` (``now()`` of the fire); the client
  path wrote each slot in its own statement.

Loop pairs. Reference: the Python loop (``scheduler_loop_caller.Scheduler``).
Candidate: the production ``Scheduler``, a facade on
``horsies._core.CoreScheduler``. Each case seeds one schedule's state, then
runs ``start()`` and one tick (``_check_and_run_schedules()``). The snapshots
must be equal (``config_hash`` is compared exactly), and each run's report of
the computed times (the next run and each slot's ``sent_at``, as offsets
from the database time of the act) must be equal and follow Python's
calculator.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Final, Protocol

import psycopg
import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import async_sessionmaker

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.schedule import IntervalSchedule, ScheduleConfig, TaskSchedule
from horsies.core.models.tasks import RetryPolicy, TaskError, TaskResult
from horsies.core.scheduler.calculator import calculate_next_run
from horsies.core.scheduler.service import Scheduler as CoreSchedulerFacade
from horsies.core.scheduler.state import DueSchedule
from horsies.core.types.result import Err, Ok, Result, is_ok
from tests.dualrun.capture import Connection
from tests.dualrun.compare import ColumnDifference, render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import schedule_fire as reference
from tests.dualrun.reference.scheduler_loop_caller import Scheduler
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_INTERVAL: Final = timedelta(seconds=60)
_TASK_NAME: Final = 'dualrun.scheduled_report'
_SCHEDULE_NAME: Final = 'dualrun.report_every_minute'
_CATCH_UP_SLOTS: Final = 5


class _Case(Enum):
    ONE_SLOT = 'one slot'
    CATCH_UP = 'a catch-up of several slots'
    SAME_COMMAND = 'a slot that already has its task, same command'
    ANOTHER_COMMAND = 'a slot whose id holds another command'
    MISSING_STATE = 'a missing state row'
    MOVED_STATE = 'a state row that another scheduler moved'


@dataclass(frozen=True, slots=True)
class _Setup:
    """The state row of the case, relative to the database time of the act."""

    stored_next_run_ago: timedelta | None
    """How long before the act the stored next run is; None deletes the row."""
    expected_next_run_ago: timedelta
    """The next run the due read saw, that the fire computes its slots from."""
    catch_up: bool


_SETUPS: Final = {
    _Case.ONE_SLOT: _Setup(timedelta(seconds=30), timedelta(seconds=30), False),
    _Case.CATCH_UP: _Setup(
        _INTERVAL * (_CATCH_UP_SLOTS - 1) + timedelta(seconds=30),
        _INTERVAL * (_CATCH_UP_SLOTS - 1) + timedelta(seconds=30),
        True,
    ),
    _Case.SAME_COMMAND: _Setup(timedelta(seconds=30), timedelta(seconds=30), False),
    _Case.ANOTHER_COMMAND: _Setup(timedelta(seconds=30), timedelta(seconds=30), False),
    _Case.MISSING_STATE: _Setup(None, timedelta(seconds=30), False),
    _Case.MOVED_STATE: _Setup(-timedelta(hours=1), timedelta(seconds=30), False),
}

type _Fire = Callable[[Scheduler, TaskSchedule, DueSchedule, datetime], Awaitable[None]]


def _app(database: DualRunDatabase, catch_up: bool) -> Horsies:
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr(database.url)),
            schedule=ScheduleConfig(
                schedules=[
                    TaskSchedule(
                        name=_SCHEDULE_NAME,
                        task_name=_TASK_NAME,
                        pattern=IntervalSchedule(seconds=int(_INTERVAL.total_seconds())),
                        kwargs={'value': 1},
                        catch_up_missed=catch_up,
                        max_catch_up_runs=10,
                    ),
                ],
            ),
        ),
    )

    @app.task(_TASK_NAME, timeout_ms=30_000, retry_policy=RetryPolicy.fixed([1, 2], auto_retry_for=['TASK_EXCEPTION']))
    def scheduled_report(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    _ = scheduled_report
    return app


def _set_state(database: DualRunDatabase, setup: _Setup) -> datetime:
    """Set the case's state row and return the database time of the act.

    An anchor row (no schedule of the config) holds ``next_run_at`` = the
    act's database time plus 30 s: the next run after a slot 30 s before the
    act. The time ranks of ``next_run_at`` then compare each side's next run
    with it.
    """
    with psycopg.connect(database.url, autocommit=True) as connection:
        row = connection.execute('SELECT statement_timestamp()').fetchone()
        assert row is not None and isinstance(row[0], datetime)
        database_now: datetime = row[0]
        _ = connection.execute(
            'INSERT INTO horsies_schedule_state (schedule_name, last_run_at, next_run_at, '
            'last_task_id, run_count, config_hash, updated_at) '
            "VALUES ('dualrun.anchor', NULL, %(next_run_at)s, NULL, 0, %(hash)s, now())",
            {'next_run_at': database_now + timedelta(seconds=30), 'hash': bytes(32)},
        )
        match setup.stored_next_run_ago:
            case None:
                _ = connection.execute(
                    'DELETE FROM horsies_schedule_state WHERE schedule_name = %(name)s',
                    {'name': _SCHEDULE_NAME},
                )
            case ago:
                _ = connection.execute(
                    'UPDATE horsies_schedule_state SET next_run_at = %(next_run_at)s '
                    'WHERE schedule_name = %(name)s',
                    {'name': _SCHEDULE_NAME, 'next_run_at': database_now - ago},
                )
    return database_now


async def _act_async(database: DualRunDatabase, case: _Case, fire: _Fire) -> None:
    setup = _SETUPS[case]
    scheduler = Scheduler(_app(database, setup.catch_up))
    await scheduler.start()
    try:
        schedule = scheduler.schedule_config.schedules[0]
        database_now = _set_state(database, setup)
        slot = database_now - setup.expected_next_run_ago
        prepared = scheduler._prepared_schedule(schedule)
        match case:
            case _Case.SAME_COMMAND:
                earlier = await reference.enqueue_slot(
                    scheduler, schedule, slot_time=slot, kwargs_json=prepared.kwargs_json,
                )
                assert is_ok(earlier), earlier
            case _Case.ANOTHER_COMMAND:
                earlier = await reference.enqueue_slot(
                    scheduler, schedule, slot_time=slot, kwargs_json='{"value":2}',
                )
                assert is_ok(earlier), earlier
            case _Case.ONE_SLOT | _Case.CATCH_UP | _Case.MISSING_STATE | _Case.MOVED_STATE:
                pass
        await fire(scheduler, schedule, DueSchedule(_SCHEDULE_NAME, slot), database_now)
    finally:
        await scheduler.stop()


async def _reference_fire(
    scheduler: Scheduler, schedule: TaskSchedule, due: DueSchedule, database_now: datetime,
) -> None:
    broker = scheduler.broker
    assert broker is not None
    lock_engine = broker.create_single_connection_engine()
    try:
        await reference.check_schedule(
            scheduler,
            reference.LockDatabase(async_sessionmaker(lock_engine, expire_on_commit=False)),
            schedule,
            database_now,
        )
    finally:
        await lock_engine.dispose()
    _ = due


async def _candidate_fire(
    scheduler: Scheduler, schedule: TaskSchedule, due: DueSchedule, database_now: datetime,
) -> None:
    await scheduler._check_schedule(schedule, due, database_now)


def _side(name: str, case: _Case, fire: _Fire) -> OperationSide[None, None]:
    def act(database: DualRunDatabase, _seed: None) -> None:
        asyncio.run(_act_async(database, case, fire))

    return OperationSide(name=name, act=act)


def _no_seed(_connection: Connection) -> None:
    return None


def _ok[RR, CR](
    result: Result[OperationDualRun[RR, CR], SelfCheckStepFailed],
) -> OperationDualRun[RR, CR]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def _run(make_database: MakeDatabase, case: _Case) -> OperationDualRun[None, None]:
    return _ok(
        dual_run(
            make_database,
            _no_seed,
            _side('lock transaction and per-slot enqueues', case, _reference_fire),
            _side('horsies_fire_schedule', case, _candidate_fire),
        )
    )


def _task_rows(run: OperationDualRun[None, None]) -> int:
    return len(run.candidate.run.snapshot.table('horsies_tasks').rows)


@pytest.mark.parametrize(
    'case',
    [
        _Case.ONE_SLOT,
        _Case.SAME_COMMAND,
        _Case.ANOTHER_COMMAND,
        _Case.MISSING_STATE,
        _Case.MOVED_STATE,
    ],
    ids=lambda case: case.name.lower(),
)
def test_the_fire_writes_what_the_client_path_wrote(
    empty_database: MakeDatabase, case: _Case,
) -> None:
    run = _run(empty_database, case)

    assert run.diff.entries == (), render_diff(run.diff)
    expected_tasks = {
        _Case.ONE_SLOT: 1,
        _Case.SAME_COMMAND: 1,
        _Case.ANOTHER_COMMAND: 1,
        _Case.MISSING_STATE: 0,
        _Case.MOVED_STATE: 0,
    }
    assert _task_rows(run) == expected_tasks[case]


def test_a_catch_up_writes_the_client_path_slots_with_one_enqueued_at(
    empty_database: MakeDatabase,
) -> None:
    run = _run(empty_database, _Case.CATCH_UP)

    assert _task_rows(run) == _CATCH_UP_SLOTS
    assert run.diff.entries, 'the client path enqueued each slot at its own time'
    for entry in run.diff.entries:
        match entry:
            case ColumnDifference(table='horsies_tasks', column='enqueued_at'):
                pass
            case other:
                raise AssertionError(f'a difference other than enqueued_at: {other}')


def test_the_comparison_sees_a_fire_that_records_no_run(
    empty_database: MakeDatabase,
) -> None:
    """The candidate moves the row past the slot and writes nothing else; the snapshots must differ."""

    async def skip_only(
        scheduler: Scheduler, schedule: TaskSchedule, due: DueSchedule, database_now: datetime,
    ) -> None:
        plan = scheduler._fire_plan(schedule, due.next_run_at, database_now)
        await scheduler._move_past_refused(schedule, due, plan, scheduler._command(schedule))

    run = _ok(
        dual_run(
            empty_database,
            _no_seed,
            _side('lock transaction and per-slot enqueues', _Case.ONE_SLOT, _reference_fire),
            _side('a fire with no slots', _Case.ONE_SLOT, skip_only),
        )
    )

    assert run.diff.entries != ()


# ---------------------------------------------------------------------------
# Loop pairs: start and one tick, the Python loop against the core
# ---------------------------------------------------------------------------


class _TickCase(Enum):
    ONE_SLOT = 'one due slot'
    CATCH_UP = 'a catch-up of several slots'
    SAME_COMMAND = 'a due slot that already has its task, same command'
    ANOTHER_COMMAND = 'a due slot whose id holds another command'
    MISSING_STATE = 'a state row deleted after the start: the heal of the tick'
    NOT_DUE = 'a state row whose next run is an hour ahead'
    NEW_ROW = 'no state row before the start'
    CHANGED_DIGEST = 'a due state row of another config digest before the start'
    CURRENT_DIGEST = 'a due state row of this config digest before the start'


_TICK_SETUPS: Final = {
    _TickCase.ONE_SLOT: _SETUPS[_Case.ONE_SLOT],
    _TickCase.CATCH_UP: _SETUPS[_Case.CATCH_UP],
    _TickCase.SAME_COMMAND: _SETUPS[_Case.SAME_COMMAND],
    _TickCase.ANOTHER_COMMAND: _SETUPS[_Case.ANOTHER_COMMAND],
    _TickCase.MISSING_STATE: _SETUPS[_Case.MISSING_STATE],
    _TickCase.NOT_DUE: _SETUPS[_Case.MOVED_STATE],
}
"""The cases seeded after the start, with the fire pairs' state rows."""

_DUE_SLOT_AGO: Final = timedelta(seconds=30)
"""How long before the act a seeded due slot is."""


class _SchedulerLoop(Protocol):
    """What a loop pair drives on the Python loop and on the facade."""

    async def start(self) -> None: ...

    async def _check_and_run_schedules(self) -> None: ...

    async def stop(self) -> None: ...


type _MakeLoop = Callable[[Horsies], _SchedulerLoop]


@dataclass(frozen=True, slots=True)
class _Window:
    """The database time before and after one call."""

    before: datetime
    after: datetime


@dataclass(frozen=True, slots=True)
class _TickReport:
    """The times of one loop run, read back after its tick."""

    act_at: datetime
    """The database time of the seed."""
    start: _Window
    tick: _Window
    next_run_at: datetime
    slot_sent_at: tuple[datetime, ...]
    """The ``sent_at`` of each task of the schedule's task, in order."""


def _database_now(database: DualRunDatabase) -> datetime:
    with psycopg.connect(database.url, autocommit=True) as connection:
        row = connection.execute('SELECT statement_timestamp()').fetchone()
    assert row is not None and isinstance(row[0], datetime)
    return row[0]


async def _timed(database: DualRunDatabase, call: Awaitable[None]) -> _Window:
    before = _database_now(database)
    await call
    return _Window(before=before, after=_database_now(database))


def _python_digest(app: Horsies) -> bytes:
    """Python's config digest of the app's one schedule."""
    python_loop = Scheduler(app)
    return python_loop._compute_config_hash(python_loop.schedule_config.schedules[0])


def _seed_before_start(database: DualRunDatabase, case: _TickCase, app: Horsies) -> datetime:
    """The anchor row, and the schedule's row that the start reads; returns the
    database time of the seed."""
    act_at = _database_now(database)
    insert = (
        'INSERT INTO horsies_schedule_state (schedule_name, last_run_at, next_run_at, '
        'last_task_id, run_count, config_hash, updated_at) '
        'VALUES (%(name)s, NULL, %(next_run_at)s, NULL, 0, %(hash)s, now())'
    )
    with psycopg.connect(database.url, autocommit=True) as connection:
        _ = connection.execute(
            insert,
            {'name': 'dualrun.anchor', 'next_run_at': act_at + _DUE_SLOT_AGO, 'hash': bytes(32)},
        )
        match case:
            case _TickCase.NEW_ROW:
                pass
            case _TickCase.CHANGED_DIGEST:
                _ = connection.execute(
                    insert,
                    {
                        'name': _SCHEDULE_NAME,
                        'next_run_at': act_at - _DUE_SLOT_AGO,
                        'hash': bytes(32),
                    },
                )
            case _TickCase.CURRENT_DIGEST:
                _ = connection.execute(
                    insert,
                    {
                        'name': _SCHEDULE_NAME,
                        'next_run_at': act_at - _DUE_SLOT_AGO,
                        'hash': _python_digest(app),
                    },
                )
            case (
                _TickCase.ONE_SLOT
                | _TickCase.CATCH_UP
                | _TickCase.SAME_COMMAND
                | _TickCase.ANOTHER_COMMAND
                | _TickCase.MISSING_STATE
                | _TickCase.NOT_DUE
            ):
                raise AssertionError(f'{case} is seeded after the start')
    return act_at


async def _seed_after_start(database: DualRunDatabase, case: _TickCase, app: Horsies) -> datetime:
    """The fire pairs' state row of the case, and the earlier slot task of the
    two command cases (sent as the Python loop sent it); returns the database
    time of the seed."""
    setup = _TICK_SETUPS[case]
    act_at = _set_state(database, setup)
    slot = act_at - setup.expected_next_run_ago
    seeder = Scheduler(app)
    seeder.broker = app.get_broker()
    seeder._validate_schedules()
    schedule = seeder.schedule_config.schedules[0]
    match case:
        case _TickCase.SAME_COMMAND:
            earlier = await reference.enqueue_slot(
                seeder,
                schedule,
                slot_time=slot,
                kwargs_json=seeder._prepared_schedule(schedule).kwargs_json,
            )
            assert is_ok(earlier), earlier
        case _TickCase.ANOTHER_COMMAND:
            earlier = await reference.enqueue_slot(
                seeder, schedule, slot_time=slot, kwargs_json='{"value":2}',
            )
            assert is_ok(earlier), earlier
        case (
            _TickCase.ONE_SLOT
            | _TickCase.CATCH_UP
            | _TickCase.MISSING_STATE
            | _TickCase.NOT_DUE
            | _TickCase.NEW_ROW
            | _TickCase.CHANGED_DIGEST
            | _TickCase.CURRENT_DIGEST
        ):
            pass
    return act_at


def _read_report(database: DualRunDatabase, act_at: datetime, start: _Window, tick: _Window) -> _TickReport:
    with psycopg.connect(database.url, autocommit=True) as connection:
        state = connection.execute(
            'SELECT next_run_at FROM horsies_schedule_state WHERE schedule_name = %(name)s',
            {'name': _SCHEDULE_NAME},
        ).fetchone()
        sent = connection.execute(
            'SELECT sent_at FROM horsies_tasks WHERE task_name = %(task)s ORDER BY sent_at',
            {'task': _TASK_NAME},
        ).fetchall()
    assert state is not None and isinstance(state[0], datetime), 'the tick leaves a state row'
    sent_at = tuple(row[0] for row in sent)
    assert all(isinstance(value, datetime) for value in sent_at)
    return _TickReport(
        act_at=act_at,
        start=start,
        tick=tick,
        next_run_at=state[0],
        slot_sent_at=sent_at,
    )


async def _loop_act_async(
    database: DualRunDatabase, case: _TickCase, make_loop: _MakeLoop,
) -> _TickReport:
    app = _app(database, case is _TickCase.CATCH_UP)
    match case:
        case _TickCase.NEW_ROW | _TickCase.CHANGED_DIGEST | _TickCase.CURRENT_DIGEST:
            act_at = _seed_before_start(database, case, app)
            loop = make_loop(app)
            start = await _timed(database, loop.start())
        case (
            _TickCase.ONE_SLOT
            | _TickCase.CATCH_UP
            | _TickCase.SAME_COMMAND
            | _TickCase.ANOTHER_COMMAND
            | _TickCase.MISSING_STATE
            | _TickCase.NOT_DUE
        ):
            loop = make_loop(app)
            start = await _timed(database, loop.start())
            act_at = await _seed_after_start(database, case, app)
    try:
        tick = await _timed(database, loop._check_and_run_schedules())
    finally:
        await loop.stop()
    return _read_report(database, act_at, start, tick)


def _loop_side(name: str, case: _TickCase, make_loop: _MakeLoop) -> OperationSide[None, _TickReport]:
    def act(database: DualRunDatabase, _seed: None) -> _TickReport:
        return asyncio.run(_loop_act_async(database, case, make_loop))

    return OperationSide(name=name, act=act)


def _python_loop(app: Horsies) -> _SchedulerLoop:
    return Scheduler(app)


def _core_loop(app: Horsies) -> _SchedulerLoop:
    return CoreSchedulerFacade(app)


def _next_run(after: datetime) -> datetime:
    """Python's next run of the pairs' pattern after ``after``."""
    return calculate_next_run(IntervalSchedule(seconds=int(_INTERVAL.total_seconds())), after)


def _assert_python_times(case: _TickCase, report: _TickReport) -> None:
    """The run's next run and slots are those of Python's calculator applied
    to the seeded state."""
    due_slot = report.act_at - _DUE_SLOT_AGO
    match case:
        case (
            _TickCase.ONE_SLOT
            | _TickCase.SAME_COMMAND
            | _TickCase.ANOTHER_COMMAND
            | _TickCase.CURRENT_DIGEST
        ):
            assert report.slot_sent_at == (due_slot,)
            assert report.next_run_at == _next_run(due_slot)
        case _TickCase.CATCH_UP:
            first = report.act_at - _TICK_SETUPS[case].expected_next_run_ago
            slots = tuple(first + _INTERVAL * index for index in range(_CATCH_UP_SLOTS))
            assert report.slot_sent_at == slots
            assert report.next_run_at == _next_run(slots[-1])
        case _TickCase.NOT_DUE:
            assert report.slot_sent_at == ()
            assert report.next_run_at == report.act_at + timedelta(hours=1)
        case _TickCase.MISSING_STATE:
            assert report.slot_sent_at == ()
            assert _next_run(report.tick.before) <= report.next_run_at <= _next_run(report.tick.after)
        case _TickCase.NEW_ROW | _TickCase.CHANGED_DIGEST:
            assert report.slot_sent_at == ()
            assert _next_run(report.start.before) <= report.next_run_at <= _next_run(report.start.after)


def _offsets(report: _TickReport) -> tuple[timedelta, tuple[timedelta, ...]]:
    """The next run and the slots as offsets from the seed."""
    return (
        report.next_run_at - report.act_at,
        tuple(sent_at - report.act_at for sent_at in report.slot_sent_at),
    )


_FIXED_TIME_CASES: Final = frozenset({
    _TickCase.ONE_SLOT,
    _TickCase.CATCH_UP,
    _TickCase.SAME_COMMAND,
    _TickCase.ANOTHER_COMMAND,
    _TickCase.NOT_DUE,
    _TickCase.CURRENT_DIGEST,
})
"""The cases whose times follow from the seed alone; the others follow the
database time of the start or the tick."""


@pytest.mark.parametrize('case', list(_TickCase), ids=lambda case: case.name.lower())
def test_the_core_loop_writes_what_the_python_loop_wrote(
    empty_database: MakeDatabase, case: _TickCase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _no_seed,
            _loop_side('Python scheduler loop', case, _python_loop),
            _loop_side('scheduler facade on CoreScheduler', case, _core_loop),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    for report in (run.reference.report, run.candidate.report):
        _assert_python_times(case, report)
    if case in _FIXED_TIME_CASES:
        assert _offsets(run.reference.report) == _offsets(run.candidate.report)
