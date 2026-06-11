# horsies/core/scheduler/state.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.models.task_pg import ScheduleStateModel
from horsies.core.logging import get_logger

logger = get_logger('scheduler.state')

UPDATE_SCHEDULE_AFTER_RUN_SQL = text("""
    UPDATE horsies_schedule_state
    SET last_run_at = :executed_at,
        next_run_at = :next_run_at,
        last_task_id = :task_id,
        run_count = run_count + 1,
        updated_at = :now
    WHERE schedule_name = :schedule_name
""")

UPDATE_SCHEDULE_NEXT_RUN_WITH_HASH_SQL = text("""
    UPDATE horsies_schedule_state
    SET next_run_at = :next_run_at,
        config_hash = :config_hash,
        updated_at = :now
    WHERE schedule_name = :schedule_name
""")

UPDATE_SCHEDULE_NEXT_RUN_SQL = text("""
    UPDATE horsies_schedule_state
    SET next_run_at = :next_run_at,
        updated_at = :now
    WHERE schedule_name = :schedule_name
""")


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

    async def get_state(
        self,
        schedule_name: str,
        session: Optional[AsyncSession] = None,
    ) -> Optional[ScheduleStateModel]:
        """
        Retrieve current state for a schedule.

        Args:
            schedule_name: Unique schedule identifier
            session: Reuse this session (caller owns the transaction);
                None opens a short-lived owned session

        Returns:
            ScheduleStateModel if exists, None otherwise

        Raises:
            Exception: DB errors propagate to the per-schedule isolation
                seams, which log and continue with the remaining schedules.
                From `_check_and_run_schedules` the row is still due, so
                the next tick retries; from `_initialize_schedules`
                (startup-only) that schedule's init/config-check is skipped
                until the next scheduler restart. On the session-reusing
                path the raise unwinds `_check_schedule`'s transaction,
                releasing the schedule advisory lock.
        """
        if session is not None:
            return await session.get(ScheduleStateModel, schedule_name)

        async with self.session_factory() as owned_session:
            return await owned_session.get(ScheduleStateModel, schedule_name)

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

        Raises:
            Exception: DB errors propagate to `run_forever`'s per-tick
                except, which logs and retries on the next tick. Nothing
                was written; no state can be lost.
        """
        if not schedule_names:
            return []

        async with self.session_factory() as session:
            stmt = (
                select(ScheduleStateModel)
                .where(ScheduleStateModel.schedule_name.in_(schedule_names))
                .where(
                    or_(
                        ScheduleStateModel.next_run_at.is_(None),
                        ScheduleStateModel.next_run_at <= now,
                    )
                )
                .order_by(ScheduleStateModel.next_run_at.asc())
            )
            result = await session.execute(stmt)
            return list(result.scalars())

    async def initialize_state(
        self,
        schedule_name: str,
        next_run_at: datetime,
        config_hash: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> ScheduleStateModel:
        """
        Initialize state for a new schedule.

        Args:
            schedule_name: Unique schedule identifier
            next_run_at: Calculated next run time
            config_hash: Hash of the schedule config for change detection
            session: Reuse this session (caller commits); None opens an
                owned session that commits here

        Returns:
            Created ScheduleStateModel (or the existing one — check-then-
            insert guard; a concurrent scheduler losing the race raises
            IntegrityError on commit, landing at the same seam as any
            other DB error)

        Raises:
            Exception: DB errors propagate to the per-schedule isolation
                seams (see `get_state`). A failure here is NOT retried on
                the next tick: without a state row the schedule is never
                returned by `get_due_states`, so it stays dormant until
                the next scheduler restart re-runs initialization (which
                the existing-row guard makes idempotent).
        """
        if session is not None:
            return await self._initialize_state_on_session(
                session=session,
                schedule_name=schedule_name,
                next_run_at=next_run_at,
                config_hash=config_hash,
                commit=False,
            )

        async with self.session_factory() as owned_session:
            return await self._initialize_state_on_session(
                session=owned_session,
                schedule_name=schedule_name,
                next_run_at=next_run_at,
                config_hash=config_hash,
                commit=True,
            )

    async def _initialize_state_on_session(
        self,
        session: AsyncSession,
        schedule_name: str,
        next_run_at: datetime,
        config_hash: Optional[str],
        commit: bool,
    ) -> ScheduleStateModel:
        """Create the state row unless it exists; commit when owning the
        session. Raises DB errors to the caller's seam (see
        `initialize_state`)."""
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
        if commit:
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
        session: Optional[AsyncSession] = None,
    ) -> None:
        """
        Update schedule state after successful task enqueue.

        Args:
            schedule_name: Unique schedule identifier
            task_id: ID of the enqueued task
            executed_at: When the schedule was executed (UTC)
            next_run_at: Calculated next run time (UTC)
            session: Reuse this session (caller commits); None opens an
                owned session that commits here

        Raises:
            Exception: DB errors propagate to `_check_schedule`'s except,
                which rolls back — the slot stays due and re-fires next
                tick. The re-enqueue is safe: the task_id is deterministic
                per (schedule, slot), so the duplicate INSERT hits
                ON CONFLICT DO NOTHING.
        """
        if session is not None:
            await self._update_after_run_on_session(
                session=session,
                schedule_name=schedule_name,
                task_id=task_id,
                executed_at=executed_at,
                next_run_at=next_run_at,
                commit=False,
            )
            return

        async with self.session_factory() as owned_session:
            await self._update_after_run_on_session(
                session=owned_session,
                schedule_name=schedule_name,
                task_id=task_id,
                executed_at=executed_at,
                next_run_at=next_run_at,
                commit=True,
            )

    async def _update_after_run_on_session(
        self,
        session: AsyncSession,
        schedule_name: str,
        task_id: str,
        executed_at: datetime,
        next_run_at: datetime,
        commit: bool,
    ) -> None:
        """Run the after-run UPDATE; commit when this call owns the session.

        rowcount == 0 is log-only by design: no horsies code path deletes
        state rows, and both callers read the row in this same transaction
        first — 0 rows means an external delete landed in between. The
        scheduler cannot heal a missing row mid-flight (`get_due_states`
        never returns it again); startup reinitialization recreates it.
        Raises DB errors to the caller's seam (see `update_after_run`).
        """
        # Use raw SQL for atomic update with increment
        result = await session.execute(
            UPDATE_SCHEDULE_AFTER_RUN_SQL,
            {
                'schedule_name': schedule_name,
                'executed_at': executed_at,
                'next_run_at': next_run_at,
                'task_id': task_id,
                'now': datetime.now(timezone.utc),
            },
        )
        if commit:
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
        session: Optional[AsyncSession] = None,
    ) -> None:
        """
        Update next_run_at and optionally config_hash (used for rescheduling without execution).

        Args:
            schedule_name: Unique schedule identifier
            next_run_at: New next run time (UTC)
            config_hash: Optional new config hash
            session: Reuse this session (caller commits); None opens an
                owned session that commits here

        Raises:
            Exception: DB errors propagate per call site. The tick-path
                callers terminate at `_check_schedule`'s own except (log +
                rollback; the row is unchanged and still due, so the next
                tick retries). The startup config-change caller
                (`_initialize_single_schedule`) terminates at
                `_initialize_schedules`' per-schedule except: the
                reschedule is deferred to the next restart, and the row
                keeps its old next_run_at meanwhile.
        """
        if session is not None:
            await self._update_next_run_on_session(
                session=session,
                schedule_name=schedule_name,
                next_run_at=next_run_at,
                config_hash=config_hash,
                commit=False,
            )
            return

        async with self.session_factory() as owned_session:
            await self._update_next_run_on_session(
                session=owned_session,
                schedule_name=schedule_name,
                next_run_at=next_run_at,
                config_hash=config_hash,
                commit=True,
            )

    async def _update_next_run_on_session(
        self,
        session: AsyncSession,
        schedule_name: str,
        next_run_at: datetime,
        config_hash: Optional[str],
        commit: bool,
    ) -> None:
        """Run the next-run UPDATE; commit when this call owns the session.

        rowcount == 0 is log-only by design — same external-delete anomaly
        as `_update_after_run_on_session`, except the startup config-change
        caller reads the row in an earlier short transaction (no advisory
        lock), so its read-then-update window is wider. Either way the
        missing row is recreated only by startup reinitialization. Raises
        DB errors to the caller's seam (see `update_next_run`).
        """
        # Build UPDATE query dynamically based on whether config_hash is provided
        if config_hash is not None:
            query = UPDATE_SCHEDULE_NEXT_RUN_WITH_HASH_SQL
            params = {
                'schedule_name': schedule_name,
                'next_run_at': next_run_at,
                'config_hash': config_hash,
                'now': datetime.now(timezone.utc),
            }
        else:
            query = UPDATE_SCHEDULE_NEXT_RUN_SQL
            params = {
                'schedule_name': schedule_name,
                'next_run_at': next_run_at,
                'now': datetime.now(timezone.utc),
            }

        result = await session.execute(query, params)
        if commit:
            await session.commit()

        rows_updated = getattr(result, 'rowcount', 0)
        if rows_updated == 0:
            logger.warning(f"Failed to update next_run for '{schedule_name}' - not found")
        else:
            logger.debug(f"Updated next_run for '{schedule_name}': {next_run_at}")

    async def get_all_states(self) -> list[ScheduleStateModel]:
        """
        Retrieve all schedule states (orphan-row inspection, monitoring).

        Returns:
            List of all ScheduleStateModel records, ordered by schedule_name

        Raises:
            Exception: DB errors propagate to the caller's own try
                (`_initialize_schedules`), which warns and continues —
                orphan inspection is advisory and must not block startup.
        """
        async with self.session_factory() as session:
            stmt = select(ScheduleStateModel).order_by(
                ScheduleStateModel.schedule_name.asc(),
            )
            result = await session.execute(stmt)
            return list(result.scalars())
