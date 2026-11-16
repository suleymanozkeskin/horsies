# pyright: reportPrivateUsage=false
"""The scheduler's writes to ``horsies_schedule_state`` on the baseline schema.

The table stores ``config_hash`` as 32 bytes, ``last_task_id`` as uuid and
``next_run_at`` NOT NULL. CHECKs tie ``last_run_at``, ``last_task_id`` and
``run_count`` together and bound the hash length. Each test writes through the
scheduler's own code and reads the row back with SQL, so the column types and
the CHECKs see every kind of value the scheduler writes.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

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
from horsies.core.scheduler.calculator import calculate_next_run
from horsies.core.scheduler.service import Scheduler
from horsies.core.scheduler.state import ScheduleStateManager
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
async def test_a_changed_config_digest_at_start_rewrites_the_digest_and_the_next_run(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
) -> None:
    task_name, schedule_name = _names()
    scheduler = Scheduler(_app_with_schedule(task_name, schedule_name))
    await scheduler.start()
    try:
        schedule = scheduler.schedule_config.schedules[0]
        await _set_config_hash(session, schedule_name, STALE_CONFIG_HASH)
        now = datetime.now(timezone.utc)
        await scheduler._initialize_single_schedule(schedule, now)
        expected_hash = scheduler._compute_config_hash(schedule)
    finally:
        await scheduler.stop()

    row = await _state_row(session, schedule_name)
    assert row.config_hash == expected_hash
    assert row.next_run_at == calculate_next_run(schedule.pattern, now, schedule.timezone)
    assert row.run_count == 0


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
async def test_the_due_read_returns_the_rows_at_or_before_now_in_next_run_order(
    broker: PostgresBroker,
) -> None:
    suffix = uuid.uuid4().hex[:12]
    manager = ScheduleStateManager(current_resources(broker).session_factory)
    now = datetime.now(timezone.utc)
    next_runs = {
        f'due_now_{suffix}': now,
        f'due_earlier_{suffix}': now - timedelta(seconds=1),
        f'not_due_{suffix}': now + timedelta(hours=1),
    }
    for schedule_name, next_run_at in next_runs.items():
        await manager.initialize_state(schedule_name, next_run_at, CONFIG_HASH)

    due = await manager.get_due_states(list(next_runs), now)

    assert [state.schedule_name for state in due] == [
        f'due_earlier_{suffix}',
        f'due_now_{suffix}',
    ]
