# pyright: reportPrivateUsage=false
"""Dual-run of the schedule fire switch: the lock transaction and per-slot enqueues against ``horsies_fire_schedule``.

Reference: ``tests/dualrun/reference/schedule_fire.py`` (the advisory lock,
the state read, one ``enqueue_async`` per slot, the state update). Candidate:
``Scheduler._check_schedule`` (one call of ``horsies_fire_schedule``). Each
case runs on its own pair of databases: a started scheduler with one
schedule, the state row set for the case, then one fire at the database
time of the act. The snapshots must be equal, except where the one
statement deliberately differs:

- a catch-up of several slots writes every slot row in one statement, so
  the slots share one ``enqueued_at`` (``now()`` of the fire); the client
  path wrote each slot in its own statement.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Final

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
from horsies.core.scheduler.service import Scheduler
from horsies.core.scheduler.state import DueSchedule
from horsies.core.types.result import Err, Ok, Result, is_ok
from tests.dualrun.capture import Connection
from tests.dualrun.compare import ColumnDifference, render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import schedule_fire as reference
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


def _ok(
    result: Result[OperationDualRun[None, None], SelfCheckStepFailed],
) -> OperationDualRun[None, None]:
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
