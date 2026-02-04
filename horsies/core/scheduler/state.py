# horsies/core/scheduler/state.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.models.task_pg import ScheduleStateModel
from horsies.core.logging import get_logger

logger = get_logger('scheduler.state')


class ScheduleStateManager:
    """
    Manages schedule state persistence in PostgreSQL.

    Provides atomic operations for tracking schedule execution state:
    - Get current state for a schedule
    - Update state after execution
    - Initialize new schedules

    All operations are async and use SQLAlchemy async sessions.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def get_state(self, schedule_name: str) -> Optional[ScheduleStateModel]:
        """
        Retrieve current state for a schedule.

        Args:
            schedule_name: Unique schedule identifier

        Returns:
            ScheduleStateModel if exists, None otherwise
        """
        async with self.session_factory() as session:
            result = await session.get(ScheduleStateModel, schedule_name)
            return result

    async def get_due_states(
        self,
        schedule_names: list[str],
        now: datetime,
    ) -> list[ScheduleStateModel]:
        """
        Retrieve states whose next_run_at is due for the provided schedule names.

        Args:
            schedule_names: Filter to these schedule identifiers
            now: Current time in UTC

        Returns:
            List of due ScheduleStateModel records
        """
        if not schedule_names:
            return []

        async with self.session_factory() as session:
            stmt = (
                select(ScheduleStateModel)
                .where(ScheduleStateModel.schedule_name.in_(schedule_names))
                .where(ScheduleStateModel.next_run_at <= now)
                .order_by(ScheduleStateModel.next_run_at.asc())
            )
            result = await session.execute(stmt)
            return list(result.scalars())

    async def initialize_state(
        self,
        schedule_name: str,
        next_run_at: datetime,
        config_hash: Optional[str] = None,
    ) -> ScheduleStateModel:
        """
        Initialize state for a new schedule.

        Args:
            schedule_name: Unique schedule identifier
            next_run_at: Calculated next run time

        Returns:
            Created ScheduleStateModel
        """
        async with self.session_factory() as session:
            # Check if already exists (race condition guard)
            existing = await session.get(ScheduleStateModel, schedule_name)
            if existing:
                logger.debug(f"Schedule '{schedule_name}' already initialized")
                return existing

            # Create new state
            state = ScheduleStateModel(
                schedule_name=schedule_name,
                last_run_at=None,
                next_run_at=next_run_at,
                last_task_id=None,
                run_count=0,
                config_hash=config_hash,
                updated_at=datetime.now(timezone.utc),
            )
            session.add(state)
            await session.commit()
            await session.refresh(state)
            logger.info(
                f"Initialized schedule state for '{schedule_name}', next_run_at={next_run_at}"
            )
            return state

    async def update_after_run(
        self,
        schedule_name: str,
        task_id: str,
        executed_at: datetime,
        next_run_at: datetime,
    ) -> None:
        """
        Update schedule state after successful task enqueue.

        Args:
            schedule_name: Unique schedule identifier
            task_id: ID of the enqueued task
            executed_at: When the schedule was executed (UTC)
            next_run_at: Calculated next run time (UTC)
        """
        async with self.session_factory() as session:
            # Use raw SQL for atomic update with increment
            result = await session.execute(
                text("""
                    UPDATE horsies_schedule_state
                    SET last_run_at = :executed_at,
                        next_run_at = :next_run_at,
                        last_task_id = :task_id,
                        run_count = run_count + 1,
                        updated_at = :now
                    WHERE schedule_name = :schedule_name
                """),
                {
                    'schedule_name': schedule_name,
                    'executed_at': executed_at,
                    'next_run_at': next_run_at,
                    'task_id': task_id,
                    'now': datetime.now(timezone.utc),
                },
            )
            await session.commit()

            rows_updated = getattr(result, 'rowcount', 0)
            if rows_updated == 0:
                logger.warning(
                    f"Failed to update schedule state for '{schedule_name}' - not found"
                )
            else:
                logger.debug(
                    f"Updated schedule '{schedule_name}': "
                    f'last_run={executed_at}, next_run={next_run_at}, task_id={task_id}'
                )

    async def update_next_run(
        self,
        schedule_name: str,
        next_run_at: datetime,
        config_hash: Optional[str] = None,
    ) -> None:
        """
        Update next_run_at and optionally config_hash (used for rescheduling without execution).

        Args:
            schedule_name: Unique schedule identifier
            next_run_at: New next run time (UTC)
            config_hash: Optional new config hash
        """
        async with self.session_factory() as session:
            # Build UPDATE query dynamically based on whether config_hash is provided
            if config_hash is not None:
                query = """
                    UPDATE horsies_schedule_state
                    SET next_run_at = :next_run_at,
                        config_hash = :config_hash,
                        updated_at = :now
                    WHERE schedule_name = :schedule_name
                """
                params = {
                    'schedule_name': schedule_name,
                    'next_run_at': next_run_at,
                    'config_hash': config_hash,
                    'now': datetime.now(timezone.utc),
                }
            else:
                query = """
                    UPDATE horsies_schedule_state
                    SET next_run_at = :next_run_at,
                        updated_at = :now
                    WHERE schedule_name = :schedule_name
                """
                params = {
                    'schedule_name': schedule_name,
                    'next_run_at': next_run_at,
                    'now': datetime.now(timezone.utc),
                }

            result = await session.execute(text(query), params)
            await session.commit()

            rows_updated = getattr(result, 'rowcount', 0)
            if rows_updated == 0:
                logger.warning(
                    f"Failed to update next_run for '{schedule_name}' - not found"
                )
            else:
                logger.debug(f"Updated next_run for '{schedule_name}': {next_run_at}")

    async def delete_state(self, schedule_name: str) -> bool:
        """
        Delete schedule state (used when schedule is removed from config).

        Args:
            schedule_name: Unique schedule identifier

        Returns:
            True if deleted, False if not found
        """
        async with self.session_factory() as session:
            result = await session.execute(
                text(
                    'DELETE FROM horsies_schedule_state WHERE schedule_name = :schedule_name'
                ),
                {'schedule_name': schedule_name},
            )
            await session.commit()

            rows_deleted = getattr(result, 'rowcount', 0)
            if rows_deleted > 0:
                logger.info(f"Deleted schedule state for '{schedule_name}'")
                return True
            else:
                logger.debug(f"No state found to delete for '{schedule_name}'")
                return False

    async def get_all_states(self) -> list[ScheduleStateModel]:
        """
        Retrieve all schedule states (useful for monitoring/debugging).

        Returns:
            List of all ScheduleStateModel records
        """
        async with self.session_factory() as session:
            result = await session.execute(
                text('SELECT * FROM horsies_schedule_state ORDER BY schedule_name')
            )
            rows = result.fetchall()
            columns = result.keys()

            # Manually construct models from rows
            states: list[ScheduleStateModel] = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                state = ScheduleStateModel(**row_dict)
                states.append(state)

            return states
