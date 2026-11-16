# pyright: reportPrivateUsage=false
"""A scheduler whose runtime pool holds one connection still fires.

The fire is one statement on one connection (``horsies_fire_schedule``): no
lock transaction holds a connection while the slot is written, so a runtime
pool of one connection never waits for ``pool_timeout``.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.schedule import IntervalSchedule, ScheduleConfig, TaskSchedule
from horsies.core.scheduler.service import Scheduler
from tests.integration.conftest import DB_URL, make_simple_task

pytestmark = [pytest.mark.integration]

RUNTIME_POOL_TIMEOUT_SECONDS = 2
SCHEDULE_CHECK_TIMEOUT_SECONDS = 10.0
SCHEDULE_INTERVAL_SECONDS = 60


def _single_connection_app(task_name: str, schedule_name: str) -> Horsies:
    broker_config = PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=1,
        max_overflow=0,
        pool_timeout=RUNTIME_POOL_TIMEOUT_SECONDS,
    )
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=broker_config,
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


async def _make_schedule_due(session: AsyncSession, schedule_name: str) -> None:
    """Move the schedule's next run one second into the past."""
    result = await session.execute(
        text("""
            UPDATE horsies_schedule_state
            SET next_run_at = NOW() - interval '1 second'
            WHERE schedule_name = :schedule_name
        """),
        {'schedule_name': schedule_name},
    )
    assert getattr(result, 'rowcount', 0) == 1
    await session.commit()


async def _enqueued_count(session: AsyncSession, task_name: str) -> int:
    result = await session.execute(
        text('SELECT count(*) FROM horsies_tasks WHERE task_name = :task_name'),
        {'task_name': task_name},
    )
    return int(result.scalar_one())


@pytest.mark.asyncio(loop_scope='function')
async def test_due_schedule_enqueues_with_a_one_connection_runtime_pool(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    suffix = uuid.uuid4().hex[:12]
    task_name = f'single_pool_task_{suffix}'
    schedule_name = f'single_pool_schedule_{suffix}'
    app = _single_connection_app(task_name, schedule_name)
    scheduler = Scheduler(app)
    await scheduler.start()
    try:
        await _make_schedule_due(session, schedule_name)
        await asyncio.wait_for(
            scheduler._check_and_run_schedules(),
            SCHEDULE_CHECK_TIMEOUT_SECONDS,
        )
    finally:
        await scheduler.stop()

    assert await _enqueued_count(session, task_name) == 1
