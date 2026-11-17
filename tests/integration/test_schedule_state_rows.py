# pyright: reportPrivateUsage=false
"""The scheduler's writes to ``horsies_schedule_state`` on the baseline schema.

The table stores ``config_hash`` as 32 bytes, ``last_task_id`` as uuid and
``next_run_at`` NOT NULL. CHECKs tie ``last_run_at``, ``last_task_id`` and
``run_count`` together and bound the hash length. Each test writes through the
scheduler's own code and reads the row back with SQL, so the column types and
the CHECKs see every kind of value the scheduler writes.

The due read, the start, the existence heal and the fire take their time
from the database clock. The clock tests move the host clock of the Python
reference loop one day ahead (``_HostClockAhead``); the core scheduler reads
no host clock. Every time they check must follow the database.

The digest that the start writes is Python's (``_python_config_hash``, the
reference loop's ``_compute_config_hash``): the core writes the same bytes.
"""

from __future__ import annotations

import asyncio
import logging
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
from horsies.core.utils.fingerprint import schedule_slot_task_id
from tests.broker_resources_support import current_resources
from tests.dualrun.reference.scheduler_loop_caller import Scheduler as ReferenceScheduler
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


def _python_config_hash(app: Horsies) -> bytes:
    """Python's digest of the app's one schedule: the pattern and timezone text
    that ``Scheduler._compute_config_hash`` of the reference loop hashes."""
    reference = ReferenceScheduler(app)
    return reference._compute_config_hash(reference.schedule_config.schedules[0])


async def _until_logged(caplog: pytest.LogCaptureFixture, text_part: str) -> None:
    """Wait until a record whose message holds ``text_part`` reaches ``caplog``.

    The core hands its records to Python ``logging`` on its forwarder thread,
    so a record can arrive after the call that logged it returned.
    """
    for _ in range(100):
        if any(text_part in record.getMessage() for record in caplog.records):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f'no log record holds {text_part!r}')


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


async def _insert_state(session: AsyncSession, schedule_name: str, next_run_at: datetime) -> None:
    """A state row with no run recorded."""
    _ = await session.execute(
        text("""
            INSERT INTO horsies_schedule_state (
                schedule_name, last_run_at, next_run_at, last_task_id, run_count,
                config_hash, updated_at
            ) VALUES (:schedule_name, NULL, :next_run_at, NULL, 0, :config_hash, now())
        """),
        {'schedule_name': schedule_name, 'next_run_at': next_run_at, 'config_hash': CONFIG_HASH},
    )
    await session.commit()


async def _record_run(
    session: AsyncSession,
    schedule_name: str,
    *,
    task_id: str,
    run_at: datetime,
    next_run_at: datetime,
) -> None:
    """Record one run on the row, as a fire records it."""
    result = await session.execute(
        text("""
            UPDATE horsies_schedule_state
            SET last_run_at = :run_at, last_task_id = CAST(:task_id AS uuid),
                run_count = run_count + 1, next_run_at = :next_run_at
            WHERE schedule_name = :schedule_name
        """),
        {
            'schedule_name': schedule_name,
            'run_at': run_at,
            'task_id': task_id,
            'next_run_at': next_run_at,
        },
    )
    assert getattr(result, 'rowcount', 0) == 1
    await session.commit()


async def _sent_at(session: AsyncSession, task_id: str) -> datetime:
    result = await session.execute(
        text('SELECT sent_at FROM horsies_tasks WHERE id = CAST(:task_id AS uuid)'),
        {'task_id': task_id},
    )
    value = result.scalar_one()
    assert isinstance(value, datetime)
    return value


HOST_CLOCK_AHEAD = timedelta(days=1)


class _HostClockAhead(datetime):
    """``datetime`` whose ``now`` is one day ahead of the real clock."""

    @classmethod
    def now(cls, tz: object = None) -> datetime:  # type: ignore[override]
        return datetime.now(tz) + HOST_CLOCK_AHEAD  # type: ignore[arg-type]


@contextmanager
def _scheduler_host_clock_ahead() -> Generator[None]:
    """The host clock of the Python reference loop is one day ahead of the
    database. The core scheduler reads no host clock."""
    with patch('tests.dualrun.reference.scheduler_loop_caller.datetime', _HostClockAhead):
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
    app = _app_with_schedule(task_name, schedule_name)
    scheduler = Scheduler(app)
    started_at = datetime.now(timezone.utc)
    await scheduler.start()
    await scheduler.stop()
    expected_hash = _python_config_hash(app)

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
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    app = _app_with_schedule(task_name, schedule_name)
    scheduler = Scheduler(app)
    await scheduler.start()
    await scheduler.stop()
    slot = await _database_now(session)
    task_id = str(schedule_slot_task_id(schedule_name, slot))
    await _record_run(
        session,
        schedule_name,
        task_id=task_id,
        run_at=slot,
        next_run_at=slot + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS),
    )
    await _set_config_hash(session, schedule_name, STALE_CONFIG_HASH)
    # A start of the same config in a new process: its state start finds the
    # stale digest.
    restarted = Scheduler(_app_with_schedule(task_name, schedule_name))
    before = await _database_now(session)
    with _scheduler_host_clock_ahead():
        await restarted.start()
    after = await _database_now(session)
    await restarted.stop()
    expected_hash = _python_config_hash(app)

    row = await _state_row(session, schedule_name)
    assert row.config_hash == expected_hash
    assert before < row.next_run_at <= after + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
    assert (row.last_task_id, row.last_run_at, row.run_count) == (task_id, slot, 1)


@pytest.mark.asyncio(loop_scope='function')
async def test_a_fired_slot_stores_its_task_id_and_counts_one_run(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    app = _app_with_schedule(task_name, schedule_name)
    scheduler = Scheduler(app)
    expected_hash = _python_config_hash(app)
    await scheduler.start()
    try:
        slot = (await _database_now(session)).replace(microsecond=0) - timedelta(seconds=1)
        await _set_next_run(session, schedule_name, slot)
        await scheduler._check_and_run_schedules()
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    task_id = str(schedule_slot_task_id(schedule_name, slot))
    assert (row.last_task_id, row.next_run_at, row.run_count, row.config_hash) == (
        task_id,
        slot + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS),
        1,
        expected_hash,
    )
    assert await _sent_at(session, task_id) == slot


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
        await _insert_state(session, schedule_name, next_run_at)
    await _insert_state(session, f'other_{suffix}', seeded_at)

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
    await scheduler.start()
    try:
        slot = await _database_now(session) - timedelta(seconds=1)
        await _set_next_run(session, schedule_name, slot)
        before = await _database_now(session)
        with _scheduler_host_clock_ahead():
            await scheduler._check_and_run_schedules()
        after = await _database_now(session)
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    # The latest slot due at the database time is the stored one, not a slot
    # a day later, and the run is recorded at the database time of the fire.
    task_id = str(schedule_slot_task_id(schedule_name, slot))
    assert row.last_task_id == task_id
    assert await _sent_at(session, task_id) == slot
    assert row.next_run_at == slot + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
    assert row.run_count == 1
    assert before <= row.last_run_at <= after


@pytest.mark.asyncio(loop_scope='function')
async def test_the_heal_creates_a_missing_row_from_the_database_time(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    app = _app_with_schedule(task_name, schedule_name)
    scheduler = Scheduler(app)
    expected_hash = _python_config_hash(app)
    await scheduler.start()
    try:
        await session.execute(
            text('DELETE FROM horsies_schedule_state WHERE schedule_name = :name'),
            {'name': schedule_name},
        )
        await session.commit()
        # The first tick after a start runs the existence heal.
        before = await _database_now(session)
        with _scheduler_host_clock_ahead():
            await scheduler._check_and_run_schedules()
        after = await _database_now(session)
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    assert before < row.next_run_at <= after + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
    assert row.config_hash == expected_hash
    assert (row.last_run_at, row.last_task_id, row.run_count) == (None, None, 0)


def _catch_up_app(task_name: str, schedule_name: str) -> Horsies:
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
                        catch_up_missed=True,
                    ),
                ],
            ),
        ),
    )
    make_simple_task(app, task_name)
    return app


async def _slot_task_count(session: AsyncSession, task_name: str) -> int:
    result = await session.execute(
        text('SELECT count(*) FROM horsies_tasks WHERE task_name = :task_name'),
        {'task_name': task_name},
    )
    return int(result.scalar_one())


async def _terminate_the_waiting_fire(session: AsyncSession) -> None:
    """Terminate the backend that waits for the state row lock that ``session`` holds."""
    for _ in range(100):
        result = await session.execute(
            text("""
                SELECT pg_terminate_backend(a.pid)
                FROM pg_stat_activity AS a
                WHERE a.datname = current_database()
                  AND a.wait_event_type = 'Lock'
                  AND a.pid <> pg_backend_pid()
                  AND pg_backend_pid() = ANY(pg_blocking_pids(a.pid))
            """),
        )
        if result.scalars().all():
            return
        await asyncio.sleep(0.05)
    raise AssertionError('no fire statement waited for the state row lock')


@pytest.mark.asyncio(loop_scope='function')
async def test_a_catch_up_whose_fire_fails_commits_nothing_and_the_next_tick_fires_each_slot_once(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The fire of three slots loses its connection while it waits for the
    state row: no slot and no state move commit. The next tick fires the
    three slots once and records one run."""
    task_name, schedule_name = _names()
    scheduler = Scheduler(_catch_up_app(task_name, schedule_name))
    await scheduler.start()
    try:
        first = (await _database_now(session)).replace(microsecond=0) - timedelta(
            seconds=2 * SCHEDULE_INTERVAL_SECONDS + 1,
        )
        await _set_next_run(session, schedule_name, first)
        # Hold the state row, so the fire waits for it; then end its connection.
        _ = await session.execute(
            text('SELECT 1 FROM horsies_schedule_state WHERE schedule_name = :name FOR UPDATE'),
            {'name': schedule_name},
        )
        with caplog.at_level(logging.WARNING, logger='horsies.scheduler'):
            tick = asyncio.create_task(scheduler._check_and_run_schedules())
            await _terminate_the_waiting_fire(session)
            await session.rollback()
            await tick
            await _until_logged(caplog, 'fire failed (transient)')

        row = await _state_row(session, schedule_name)
        assert (row.next_run_at, row.run_count) == (first, 0)
        assert await _slot_task_count(session, task_name) == 0

        await scheduler._check_and_run_schedules()
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    slots = [first + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS * index) for index in range(3)]
    assert await _slot_task_count(session, task_name) == 3
    assert (row.last_task_id, row.run_count) == (str(schedule_slot_task_id(schedule_name, slots[-1])), 1)
    assert row.next_run_at == slots[-1] + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)


@pytest.mark.asyncio(loop_scope='function')
async def test_two_schedulers_that_fire_one_slot_write_one_task_and_one_run(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    """Both tick at once; the fire locks the state row, so the second finds it
    moved (or does not read the slot as due) and writes nothing."""
    task_name, schedule_name = _names()
    first = Scheduler(_app_with_schedule(task_name, schedule_name))
    second = Scheduler(_app_with_schedule(task_name, schedule_name))
    await first.start()
    await second.start()
    try:
        slot = (await _database_now(session)).replace(microsecond=0) - timedelta(seconds=1)
        await _set_next_run(session, schedule_name, slot)
        _ = await asyncio.gather(
            first._check_and_run_schedules(),
            second._check_and_run_schedules(),
        )
    finally:
        await first.stop()
        await second.stop()

    row = await _state_row(session, schedule_name)
    assert await _slot_task_count(session, task_name) == 1
    assert (row.last_task_id, row.run_count) == (str(schedule_slot_task_id(schedule_name, slot)), 1)
    assert row.next_run_at == slot + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)


@pytest.mark.asyncio(loop_scope='function')
async def test_a_fire_repeated_after_its_commit_writes_nothing_once_the_task_left_the_live_table(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A slot fire is the first send of the slot's task id and reads no
    history. The scheduler repeats a fire whose commit it did not see (a lost
    reply), after the slot's task finished and left ``horsies_tasks``. The
    state row moved in the same commit as the slot, so the repeat finds the
    row moved and the slot does not run a second time; the next tick's due
    read does not show the slot either.

    A tick reads the due rows again, so it cannot repeat a fire with the next
    run it computed the slots from. The test drives the fire of the Python
    reference loop (``ReferenceScheduler._check_schedule``): one call of
    ``horsies_fire_schedule``, the statement that the core's fire sends."""
    task_name, schedule_name = _names()
    scheduler = ReferenceScheduler(_app_with_schedule(task_name, schedule_name))
    await scheduler.start()
    try:
        assert scheduler.state_manager is not None
        schedule = scheduler.schedule_config.schedules[0]
        slot = (await _database_now(session)).replace(microsecond=0) - timedelta(seconds=1)
        await _set_next_run(session, schedule_name, slot)
        read = await scheduler.state_manager.read_due_schedules([schedule_name])
        [due] = read.due
        await scheduler._check_schedule(schedule, due, read.database_now)
        task_id = str(schedule_slot_task_id(schedule_name, slot))
        assert await _slot_task_count(session, task_name) == 1
        _ = await session.execute(
            text('DELETE FROM horsies_tasks WHERE id = CAST(:task_id AS uuid)'),
            {'task_id': task_id},
        )
        await session.commit()

        with caplog.at_level(logging.DEBUG, logger='horsies.scheduler'):
            await scheduler._check_schedule(schedule, due, read.database_now)
        assert any(
            'was moved by another scheduler' in record.getMessage()
            for record in caplog.records
        )
        await scheduler._check_and_run_schedules()
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    assert await _slot_task_count(session, task_name) == 0
    assert (row.last_task_id, row.run_count) == (task_id, 1)
    assert row.next_run_at == slot + timedelta(seconds=SCHEDULE_INTERVAL_SECONDS)
