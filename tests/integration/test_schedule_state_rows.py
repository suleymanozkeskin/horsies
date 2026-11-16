# pyright: reportPrivateUsage=false
"""The scheduler's writes to ``horsies_schedule_state`` on the baseline schema.

The table stores ``config_hash`` as 32 bytes, ``last_task_id`` as uuid and
``next_run_at`` NOT NULL. CHECKs tie ``last_run_at``, ``last_task_id`` and
``run_count`` together and bound the hash length. Each test writes through the
scheduler's own code and reads the row back with SQL, so the column types and
the CHECKs see every kind of value the scheduler writes.

The due read, the start and the existence heal take their time from the
database clock. The clock tests move the scheduler's host clock one day ahead
(``_HostClockAhead``): every time they check must still follow the database.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from pydantic import SecretStr
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.schedule import IntervalSchedule, ScheduleConfig, TaskSchedule
from horsies.core.scheduler.service import Scheduler
from horsies.core.scheduler.state import ScheduleStateManager
from horsies.core.types.result import Ok
from horsies.core.utils.fingerprint import schedule_slot_task_id
from tests.broker_resources_support import current_resources
from tests.integration.conftest import DB_URL, make_simple_task

pytestmark = [pytest.mark.integration]

SCHEDULE_INTERVAL_SECONDS = 60
CONFIG_HASH_BYTES = 32
CONFIG_HASH = bytes(range(CONFIG_HASH_BYTES))
# No schedule config hashes to this value.
STALE_CONFIG_HASH = bytes(CONFIG_HASH_BYTES)

STATE_ROW_SQL = text("""
    SELECT next_run_at, last_run_at, last_task_id::text AS last_task_id,
           run_count, config_hash, octet_length(config_hash) AS config_hash_length
    FROM horsies_schedule_state
    WHERE schedule_name = :schedule_name
""")


def _names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:12]
    return f'schedule_state_task_{suffix}', f'schedule_state_{suffix}'


def _app_with_schedule(task_name: str, schedule_name: str) -> Horsies:
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr(DB_URL)),
            schedule=ScheduleConfig(
                schedules=[
                    TaskSchedule(
                        name=schedule_name,
                        task_name=task_name,
                        pattern=IntervalSchedule(seconds=SCHEDULE_INTERVAL_SECONDS),
                        kwargs={'value': 1},
                    ),
                ],
            ),
        ),
    )
    make_simple_task(app, task_name)
    return app


async def _state_row(session: AsyncSession, schedule_name: str) -> Row[tuple[object, ...]]:
    result = await session.execute(STATE_ROW_SQL, {'schedule_name': schedule_name})
    return result.one()


async def _database_now(session: AsyncSession) -> datetime:
    result = await session.execute(text('SELECT statement_timestamp()'))
    value = result.scalar_one()
    assert isinstance(value, datetime)
    return value


async def _set_next_run(session: AsyncSession, schedule_name: str, next_run_at: datetime) -> None:
    result = await session.execute(
        text("""
            UPDATE horsies_schedule_state
            SET next_run_at = :next_run_at
            WHERE schedule_name = :schedule_name
        """),
        {'schedule_name': schedule_name, 'next_run_at': next_run_at},
    )
    assert getattr(result, 'rowcount', 0) == 1
    await session.commit()


HOST_CLOCK_AHEAD = timedelta(days=1)


class _HostClockAhead(datetime):
    """``datetime`` whose ``now`` is one day ahead of the real clock."""

    @classmethod
    def now(cls, tz: object = None) -> datetime:  # type: ignore[override]
        return datetime.now(tz) + HOST_CLOCK_AHEAD  # type: ignore[arg-type]


@contextmanager
def _scheduler_host_clock_ahead() -> Generator[None]:
    """The scheduler service's host clock is one day ahead of the database."""
    with patch('horsies.core.scheduler.service.datetime', _HostClockAhead):
        yield


async def _set_config_hash(
    session: AsyncSession,
    schedule_name: str,
    config_hash: bytes,
) -> None:
    result = await session.execute(
        text("""
            UPDATE horsies_schedule_state
            SET config_hash = :config_hash
            WHERE schedule_name = :schedule_name
        """),
        {'schedule_name': schedule_name, 'config_hash': config_hash},
    )
    assert getattr(result, 'rowcount', 0) == 1
    await session.commit()


@pytest.mark.asyncio(loop_scope='function')
async def test_scheduler_start_writes_the_config_digest_and_no_last_run(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    scheduler = Scheduler(_app_with_schedule(task_name, schedule_name))
    started_at = datetime.now(timezone.utc)
    await scheduler.start()
    try:
        expected_hash = scheduler._compute_config_hash(
            scheduler.schedule_config.schedules[0],
        )
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    assert row.config_hash == expected_hash
    assert row.config_hash_length == CONFIG_HASH_BYTES
    assert row.next_run_at > started_at
    assert (row.last_run_at, row.last_task_id, row.run_count) == (None, None, 0)


@pytest.mark.asyncio(loop_scope='function')
async def test_start_computes_the_first_run_from_the_database_time(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    scheduler = Scheduler(_app_with_schedule(task_name, schedule_name))
    before = await _database_now(session)
    with _scheduler_host_clock_ahead():
        await scheduler.start()
    await scheduler.stop()
    after = await _database_now(session)

    row = await _state_row(session, schedule_name)
    assert before < row.next_run_at <= after + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
    assert (row.last_run_at, row.last_task_id, row.run_count) == (None, None, 0)


@pytest.mark.asyncio(loop_scope='function')
async def test_a_changed_config_digest_at_start_rewrites_the_digest_and_keeps_the_run_record(
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    task_name, schedule_name = _names()
    scheduler = Scheduler(_app_with_schedule(task_name, schedule_name))
    await scheduler.start()
    try:
        schedule = scheduler.schedule_config.schedules[0]
        manager = ScheduleStateManager(current_resources(broker).session_factory)
        slot = await _database_now(session)
        task_id = schedule_slot_task_id(schedule_name, slot)
        await manager.update_after_run(
            schedule_name=schedule_name,
            task_id=task_id,
            executed_at=slot,
            next_run_at=slot + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS),
        )
        await _set_config_hash(session, schedule_name, STALE_CONFIG_HASH)
        before = await _database_now(session)
        with _scheduler_host_clock_ahead():
            await scheduler._initialize_schedules()
        after = await _database_now(session)
        expected_hash = scheduler._compute_config_hash(schedule)
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    assert row.config_hash == expected_hash
    assert before < row.next_run_at <= after + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
    assert (row.last_task_id, row.last_run_at, row.run_count) == (task_id, slot, 1)


@pytest.mark.asyncio(loop_scope='function')
async def test_a_recorded_run_stores_the_slot_task_id_and_counts_one_run(
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    _, schedule_name = _names()
    manager = ScheduleStateManager(current_resources(broker).session_factory)
    slot = datetime.now(timezone.utc).replace(microsecond=0)
    await manager.initialize_state(schedule_name, slot, CONFIG_HASH)

    task_id = schedule_slot_task_id(schedule_name, slot)
    executed_at = slot + timedelta(seconds=1)
    next_run = slot + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
    await manager.update_after_run(
        schedule_name=schedule_name,
        task_id=task_id,
        executed_at=executed_at,
        next_run_at=next_run,
    )

    row = await _state_row(session, schedule_name)
    assert (
        row.last_task_id,
        row.last_run_at,
        row.next_run_at,
        row.run_count,
        row.config_hash,
    ) == (task_id, executed_at, next_run, 1, CONFIG_HASH)


@pytest.mark.asyncio(loop_scope='function')
async def test_the_due_read_returns_the_rows_at_or_before_the_database_time(
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    suffix = uuid.uuid4().hex[:12]
    manager = ScheduleStateManager(current_resources(broker).session_factory)
    seeded_at = await _database_now(session)
    next_runs = {
        f'due_now_{suffix}': seeded_at,
        f'due_earlier_{suffix}': seeded_at - timedelta(seconds=1),
        f'not_due_{suffix}': seeded_at + timedelta(hours=1),
    }
    for schedule_name, next_run_at in next_runs.items():
        await manager.initialize_state(schedule_name, next_run_at, CONFIG_HASH)
    await manager.initialize_state(f'other_{suffix}', seeded_at, CONFIG_HASH)

    before = await _database_now(session)
    read = await manager.read_due_schedules(list(next_runs))
    after = await _database_now(session)

    assert before <= read.database_now <= after
    assert [due.schedule_name for due in read.due] == [
        f'due_earlier_{suffix}',
        f'due_now_{suffix}',
    ]
    nothing_due = await manager.read_due_schedules([f'not_due_{suffix}'])
    assert nothing_due.due == ()
    assert after <= nothing_due.database_now


@pytest.mark.asyncio(loop_scope='function')
async def test_a_tick_with_the_host_clock_ahead_fires_no_slot_that_the_database_has_not_reached(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    scheduler = Scheduler(_app_with_schedule(task_name, schedule_name))
    await scheduler.start()
    try:
        not_yet = await _database_now(session) + timedelta(minutes=30)
        await _set_next_run(session, schedule_name, not_yet)
        with _scheduler_host_clock_ahead():
            await scheduler._check_and_run_schedules()
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    assert (row.next_run_at, row.run_count, row.last_task_id) == (not_yet, 0, None)


@pytest.mark.asyncio(loop_scope='function')
async def test_a_due_slot_fires_and_records_its_run_at_the_database_time(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    scheduler = Scheduler(_app_with_schedule(task_name, schedule_name))
    fired: list[datetime] = []

    async def _enqueue(schedule: TaskSchedule, *, slot_time: datetime) -> Ok[str]:
        # The state move is under test here, not the enqueue.
        fired.append(slot_time)
        return Ok(schedule_slot_task_id(schedule.name, slot_time))

    await scheduler.start()
    try:
        slot = await _database_now(session) - timedelta(seconds=1)
        await _set_next_run(session, schedule_name, slot)
        before = await _database_now(session)
        with (
            _scheduler_host_clock_ahead(),
            patch.object(scheduler, '_enqueue_scheduled_task', side_effect=_enqueue),
        ):
            await scheduler._check_and_run_schedules()
        after = await _database_now(session)
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    # The latest slot due at the database time is the stored one, not a slot
    # a day later, and the run is recorded at the due read's database time.
    assert fired == [slot]
    assert row.last_task_id == schedule_slot_task_id(schedule_name, slot)
    assert row.next_run_at == slot + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
    assert row.run_count == 1
    assert before <= row.last_run_at <= after


@pytest.mark.asyncio(loop_scope='function')
async def test_the_heal_creates_a_missing_row_from_the_database_time(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    scheduler = Scheduler(_app_with_schedule(task_name, schedule_name))
    await scheduler.start()
    try:
        await session.execute(
            text('DELETE FROM horsies_schedule_state WHERE schedule_name = :name'),
            {'name': schedule_name},
        )
        await session.commit()
        scheduler._ticks_until_existence_check = 0
        before = await _database_now(session)
        with _scheduler_host_clock_ahead():
            await scheduler._check_and_run_schedules()
        after = await _database_now(session)
        expected_hash = scheduler._compute_config_hash(scheduler.schedule_config.schedules[0])
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    assert before < row.next_run_at <= after + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
    assert row.config_hash == expected_hash
    assert (row.last_run_at, row.last_task_id, row.run_count) == (None, None, 0)
