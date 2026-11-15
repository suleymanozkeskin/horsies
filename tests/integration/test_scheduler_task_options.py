# pyright: reportPrivateUsage=false
"""A scheduled run stores the options of its task on the row.

The worker reads the timeout, the retry limit and the execution class from
the row. Each test enters at ``Scheduler._check_schedule`` (the tick's caller
of the enqueue) and reads the row and the schedule state from the database.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.retention import resolve_queue_retention_class
from horsies.core.models.schedule import IntervalSchedule, ScheduleConfig, TaskSchedule
from horsies.core.models.tasks import RetryPolicy, TaskError, TaskResult
from horsies.core.scheduler.service import Scheduler
from horsies.core.task_decorator import effective_priority
from horsies.core.types.result import Ok
from horsies.core.utils.fingerprint import enqueue_fingerprint, schedule_slot_task_id
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

SCHEDULE_INTERVAL_SECONDS = 60
TASK_TIMEOUT_MS = 30_000
RETRY_INTERVALS_SECONDS = [1, 2]


def _app_with_scheduled_task(task_name: str, schedule_name: str) -> Horsies:
    """An app with one task that declares a timeout and a retry policy."""
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

    @app.task(
        task_name,
        timeout_ms=TASK_TIMEOUT_MS,
        retry_policy=RetryPolicy.fixed(
            RETRY_INTERVALS_SECONDS,
            auto_retry_for=['TASK_EXCEPTION'],
        ),
    )
    def scheduled_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    _ = scheduled_task
    return app


def _names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:12]
    return f'scheduled_options_task_{suffix}', f'scheduled_options_{suffix}'


async def _set_next_run_at(
    session: AsyncSession,
    schedule_name: str,
    next_run_at: datetime,
) -> None:
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


@pytest.mark.asyncio(loop_scope='function')
async def test_a_scheduled_run_stores_the_timeout_and_retry_policy_of_its_task(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_name, schedule_name = _names()
    app = _app_with_scheduled_task(task_name, schedule_name)
    task_options_json = app.tasks[task_name].task_options_json
    scheduler = Scheduler(app)
    await scheduler.start()
    try:
        check_time = datetime.now(timezone.utc).replace(microsecond=0)
        await _set_next_run_at(
            session,
            schedule_name,
            check_time - timedelta(seconds=1),
        )
        await scheduler._check_schedule(
            scheduler.schedule_config.schedules[0],
            check_time,
        )
    finally:
        await scheduler.stop()

    rows = (
        await session.execute(
            text("""
                SELECT task_options, max_retries, execution
                FROM horsies_tasks
                WHERE task_name = :task_name
            """),
            {'task_name': task_name},
        )
    ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert task_options_json is not None
    assert row.task_options == task_options_json
    match loads_json(row.task_options):
        case Ok(dict() as stored):
            assert stored['timeout_ms'] == TASK_TIMEOUT_MS
        case other:
            pytest.fail(f'stored task_options are not a JSON object: {other!r}')
    assert row.max_retries == len(RETRY_INTERVALS_SECONDS)
    assert row.execution == 'ISOLATED'


@pytest.mark.asyncio(loop_scope='function')
async def test_a_slot_that_an_earlier_version_enqueued_counts_as_fired(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - initializes the schema
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The upgrade slot: the row of an earlier version has no options, so its
    fingerprint differs. The broker returns PAYLOAD_MISMATCH; the scheduler
    keeps that row, records the slot as run, and moves to the next slot."""
    task_name, schedule_name = _names()
    app = _app_with_scheduled_task(task_name, schedule_name)
    scheduler = Scheduler(app)
    await scheduler.start()
    try:
        assert scheduler.broker is not None
        schedule = scheduler.schedule_config.schedules[0]
        check_time = datetime.now(timezone.utc).replace(microsecond=0)
        first_due = check_time - timedelta(seconds=1)
        slot, next_run, _ = scheduler._advance_to_latest_due_slot(
            schedule,
            first_due,
            check_time,
        )
        slot_task_id = schedule_slot_task_id(schedule_name, slot)
        priority = effective_priority(app, 'default')
        # The same kwargs text as this version, so only the options differ.
        prepared = scheduler._prepared_schedules[schedule_name]
        earlier_row = await scheduler.broker.enqueue_async(
            task_name=task_name,
            queue_name='default',
            task_id=slot_task_id,
            enqueue_sha=enqueue_fingerprint(
                task_name=task_name,
                queue_name='default',
                priority=priority,
                args_json=None,
                kwargs_json=prepared.kwargs_json,
                sent_at=slot,
                good_until=None,
                enqueue_delay_seconds=None,
                task_options=None,
            ),
            args_json=None,
            kwargs_json=prepared.kwargs_json,
            priority=priority,
            sent_at=slot,
            retention_class_key=resolve_queue_retention_class(
                app.config.retention,
                'default',
            ),
        )
        assert earlier_row == Ok(slot_task_id)

        await _set_next_run_at(session, schedule_name, first_due)
        await scheduler._check_schedule(schedule, check_time)
    finally:
        await scheduler.stop()

    rows = (
        await session.execute(
            text("""
                SELECT id::text AS id, task_options
                FROM horsies_tasks
                WHERE task_name = :task_name
            """),
            {'task_name': task_name},
        )
    ).fetchall()
    assert [(row.id, row.task_options) for row in rows] == [(slot_task_id, None)]

    state = (
        await session.execute(
            text("""
                SELECT last_task_id, next_run_at, run_count
                FROM horsies_schedule_state
                WHERE schedule_name = :schedule_name
            """),
            {'schedule_name': schedule_name},
        )
    ).one()
    assert state.last_task_id == slot_task_id
    assert state.next_run_at == next_run
    assert state.run_count == 1
