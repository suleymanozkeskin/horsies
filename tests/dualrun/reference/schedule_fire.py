# pyright: reportPrivateUsage=false
"""The schedule fire, before ``horsies_fire_schedule``.

``Scheduler._check_schedule`` (now ``tests/dualrun/reference/scheduler_loop_caller.py``) ran,
per due schedule, on a private one-connection engine: BEGIN, the advisory
lock of the schedule name, the read of the state row. A missing row was
created in that transaction; a row that was no longer due ended it. Then
each slot was enqueued on the runtime pool by ``enqueue_async`` (a
``SendKind.FIRST`` send in its own statement, which answers the stored
command on a task id conflict),
and the state row was updated in the lock transaction: the run recorded, or
only ``next_run_at`` moved past a slot that the broker refused for good.
A transient enqueue error rolled the lock transaction back.

The slot computation (``_calculate_missed_runs``,
``_advance_to_latest_due_slot``), the config digest and the prepared text
are the scheduler's own; the queue, priority and retention class are
resolved as the old enqueue did.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.result_types import BrokerEnqueueError, BrokerPayloadMismatch
from horsies.core.models.retention import resolve_queue_retention_class
from horsies.core.models.schedule import TaskSchedule
from horsies.core.models.task_pg import ScheduleStateModel
from horsies.core.scheduler.calculator import calculate_next_run, should_run_now
from tests.dualrun.reference.scheduler_loop_caller import Scheduler
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.utils.fingerprint import enqueue_fingerprint, schedule_slot_task_id

SCHEDULE_ADVISORY_LOCK_SQL = text('SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))')

UPDATE_SCHEDULE_AFTER_RUN_SQL = text("""
    UPDATE horsies_schedule_state
    SET last_run_at = :executed_at,
        next_run_at = :next_run_at,
        last_task_id = :task_id,
        run_count = run_count + 1,
        updated_at = :now
    WHERE schedule_name = :schedule_name
""")

UPDATE_SCHEDULE_NEXT_RUN_SQL = text("""
    UPDATE horsies_schedule_state
    SET next_run_at = :next_run_at,
        updated_at = :now
    WHERE schedule_name = :schedule_name
""")


@dataclass(frozen=True, slots=True)
class LockDatabase:
    """The private one-connection session factory of the lock transactions."""

    session_factory: async_sessionmaker[AsyncSession]


def _advisory_key(schedule_name: str) -> int:
    digest = hashlib.sha256(f'horsies-schedule:{schedule_name}'.encode('utf-8')).digest()
    return int.from_bytes(digest[:8], byteorder='big', signed=True)


async def _update_after_run(
    session: AsyncSession,
    *,
    schedule_name: str,
    task_id: UUID,
    executed_at: datetime,
    next_run_at: datetime,
) -> None:
    _ = await session.execute(
        UPDATE_SCHEDULE_AFTER_RUN_SQL,
        {
            'schedule_name': schedule_name,
            'executed_at': executed_at,
            'next_run_at': next_run_at,
            'task_id': str(task_id),
            'now': datetime.now(timezone.utc),
        },
    )


async def _update_next_run(
    session: AsyncSession, *, schedule_name: str, next_run_at: datetime,
) -> None:
    _ = await session.execute(
        UPDATE_SCHEDULE_NEXT_RUN_SQL,
        {
            'schedule_name': schedule_name,
            'next_run_at': next_run_at,
            'now': datetime.now(timezone.utc),
        },
    )


async def enqueue_slot(
    scheduler: Scheduler,
    schedule: TaskSchedule,
    *,
    slot_time: datetime,
    kwargs_json: str | None,
) -> Result[UUID, BrokerEnqueueError]:
    """The old enqueue of one slot, with the given kwargs text.

    The slot id and ``sent_at`` come from the slot time; the fingerprint
    covers the kwargs text and the task's options text. A task id that holds
    another command counts as fired (``Ok(task_id)``).
    """
    from horsies.core.task_decorator import effective_priority

    broker = scheduler.broker
    assert broker is not None, 'the scheduler is not started'
    queue_name = scheduler._resolve_schedule_queue(schedule)
    priority = effective_priority(scheduler.app, queue_name)
    task_id = schedule_slot_task_id(schedule.name, slot_time)
    prepared = scheduler._prepared_schedule(schedule)
    enqueue_sha = enqueue_fingerprint(
        task_name=schedule.task_name,
        queue_name=queue_name,
        priority=priority,
        args_json=None,
        kwargs_json=kwargs_json,
        sent_at=slot_time,
        good_until=None,
        enqueue_delay_seconds=None,
        task_options=prepared.task_options_json,
    )
    result = await broker.enqueue_async(
        task_name=schedule.task_name,
        queue_name=queue_name,
        task_id=task_id,
        enqueue_sha=enqueue_sha,
        send_kind=SendKind.FIRST,
        args_json=None,
        kwargs_json=kwargs_json,
        priority=priority,
        sent_at=slot_time,
        task_options=prepared.task_options_json,
        retention_class_key=resolve_queue_retention_class(
            getattr(scheduler.app.config, 'retention', None),
            queue_name,
        ),
    )
    match result:
        case Err(BrokerPayloadMismatch()):
            return Ok(task_id)
        case _:
            return result


async def check_schedule(
    scheduler: Scheduler,
    lock_database: LockDatabase,
    schedule: TaskSchedule,
    check_time: datetime,
) -> None:
    """The old fire of one schedule at the due read's database time ``check_time``."""
    prepared = scheduler._prepared_schedule(schedule)
    async with lock_database.session_factory() as session:
        _ = await session.execute(SCHEDULE_ADVISORY_LOCK_SQL, {'key': _advisory_key(schedule.name)})
        state = await session.get(ScheduleStateModel, schedule.name)
        if state is None:
            session.add(
                ScheduleStateModel(
                    schedule_name=schedule.name,
                    last_run_at=None,
                    next_run_at=calculate_next_run(schedule.pattern, check_time, schedule.timezone),
                    last_task_id=None,
                    run_count=0,
                    config_hash=scheduler._compute_config_hash(schedule),
                    updated_at=datetime.now(timezone.utc),
                ),
            )
            await session.commit()
            return
        if not should_run_now(state.next_run_at, check_time):
            await session.commit()
            return

        due_runs: list[datetime] = []
        if schedule.catch_up_missed:
            due_runs = scheduler._calculate_missed_runs(
                schedule=schedule,
                first_due_run=state.next_run_at,
                current_time=check_time,
                max_runs=schedule.max_catch_up_runs,
            )
        if due_runs:
            last_task_id: UUID | None = None
            caught_up: list[datetime] = []
            permanent_skip_slot: datetime | None = None
            for due_time in due_runs:
                result = await enqueue_slot(
                    scheduler, schedule, slot_time=due_time, kwargs_json=prepared.kwargs_json,
                )
                if is_err(result):
                    if not result.err_value.retryable:
                        permanent_skip_slot = due_time
                    break
                last_task_id = result.ok_value
                caught_up.append(due_time)
            progress_slot = permanent_skip_slot or (caught_up[-1] if caught_up else None)
            if progress_slot is None:
                await session.rollback()
                return
            next_run = calculate_next_run(schedule.pattern, progress_slot, schedule.timezone)
            match last_task_id:
                case UUID() as recorded_task_id:
                    await _update_after_run(
                        session,
                        schedule_name=schedule.name,
                        task_id=recorded_task_id,
                        executed_at=check_time,
                        next_run_at=next_run,
                    )
                case None:
                    await _update_next_run(
                        session, schedule_name=schedule.name, next_run_at=next_run,
                    )
            await session.commit()
            return

        slot_time, next_run, _ = scheduler._advance_to_latest_due_slot(
            schedule, state.next_run_at, check_time,
        )
        if next_run <= check_time:
            await _update_next_run(session, schedule_name=schedule.name, next_run_at=next_run)
            await session.commit()
            return
        result = await enqueue_slot(
            scheduler, schedule, slot_time=slot_time, kwargs_json=prepared.kwargs_json,
        )
        if is_err(result):
            if result.err_value.retryable:
                await session.rollback()
                return
            await _update_next_run(session, schedule_name=schedule.name, next_run_at=next_run)
            await session.commit()
            return
        await _update_after_run(
            session,
            schedule_name=schedule.name,
            task_id=result.ok_value,
            executed_at=check_time,
            next_run_at=next_run,
        )
        await session.commit()
