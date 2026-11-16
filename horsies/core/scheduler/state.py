# horsies/core/scheduler/state.py
from __future__ import annotations
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.models.task_pg import ScheduleStateModel
from horsies.core.logging import get_logger

logger = get_logger('scheduler.state')

# The due read: the rows of these names whose next_run_at is at or before
# the database time, soonest first, and that database time. The one-row
# join returns the time also when no row is due (one row with NULL name).
DUE_SCHEDULES_SQL = text("""
    SELECT statement_timestamp() AS database_now, s.schedule_name, s.next_run_at
    FROM (VALUES (1)) AS one
    LEFT JOIN horsies_schedule_state AS s
        ON s.schedule_name = ANY(CAST(:schedule_names AS text[]))
       AND s.next_run_at <= statement_timestamp()
    ORDER BY s.next_run_at
""")

# The start read: every row's name and digest, and the database time. The
# one-row join returns the time also for an empty table.
START_STATES_SQL = text("""
    SELECT statement_timestamp() AS database_now, s.schedule_name, s.config_hash
    FROM (VALUES (1)) AS one
    LEFT JOIN horsies_schedule_state AS s ON TRUE
    ORDER BY s.schedule_name
""")

# The start write: a new schedule gets its row with no run recorded; a row
# whose digest differs gets the new next_run_at and digest and keeps
# run_count, last_run_at and last_task_id. The WHERE leaves alone a row that
# a concurrent scheduler already moved to this digest.
STORE_START_STATES_SQL = text("""
    INSERT INTO horsies_schedule_state (
        schedule_name, last_run_at, next_run_at, last_task_id, run_count,
        config_hash, updated_at
    )
    SELECT u.schedule_name, NULL, u.next_run_at, NULL, 0, u.config_hash,
           statement_timestamp()
    FROM unnest(
        CAST(:schedule_names AS text[]),
        CAST(:next_runs AS timestamptz[]),
        CAST(:config_hashes AS bytea[])
    ) AS u(schedule_name, next_run_at, config_hash)
    ON CONFLICT (schedule_name) DO UPDATE
    SET next_run_at = EXCLUDED.next_run_at,
        config_hash = EXCLUDED.config_hash,
        updated_at = EXCLUDED.updated_at
    WHERE horsies_schedule_state.config_hash <> EXCLUDED.config_hash
    RETURNING schedule_name
""")

# The existence heal: a row for each name that has none. A row that exists
# is not written.
HEAL_MISSING_STATES_SQL = text("""
    INSERT INTO horsies_schedule_state (
        schedule_name, last_run_at, next_run_at, last_task_id, run_count,
        config_hash, updated_at
    )
    SELECT u.schedule_name, NULL, u.next_run_at, NULL, 0, u.config_hash,
           statement_timestamp()
    FROM unnest(
        CAST(:schedule_names AS text[]),
        CAST(:next_runs AS timestamptz[]),
        CAST(:config_hashes AS bytea[])
    ) AS u(schedule_name, next_run_at, config_hash)
    ON CONFLICT (schedule_name) DO NOTHING
    RETURNING schedule_name
""")


@dataclass(frozen=True, slots=True)
class DueSchedule:
    """One due row of the due read."""

    schedule_name: str
    next_run_at: datetime


@dataclass(frozen=True, slots=True)
class DueScheduleRead:
    """The due rows, soonest first, and the database time of the read.

    The tick computes slots and the heal's first runs from
    ``database_now``, never from the host clock.
    """

    database_now: datetime
    due: tuple[DueSchedule, ...]


@dataclass(frozen=True, slots=True)
class StoredConfigHash:
    """The name and stored config digest of one state row."""

    schedule_name: str
    config_hash: bytes


@dataclass(frozen=True, slots=True)
class StartStateRead:
    """Every state row's name and digest, and the database time of the read."""

    database_now: datetime
    rows: tuple[StoredConfigHash, ...]


@dataclass(frozen=True, slots=True)
class ScheduleStateWrite:
    """One row for the start write or the existence heal."""

    schedule_name: str
    next_run_at: datetime
    config_hash: bytes


def _write_params(writes: Sequence[ScheduleStateWrite]) -> dict[str, Any]:
    return {
        'schedule_names': [write.schedule_name for write in writes],
        'next_runs': [write.next_run_at for write in writes],
        'config_hashes': [write.config_hash for write in writes],
    }

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
                the next tick retries; from `_initialize_schedules` the
                schedule's init/config-check is skipped — a missing row is
                then recreated by the tick loop's missing-row check
                (`_ensure_states_exist`), while a config-hash recheck for
                an existing row waits for the next restart. On the
                session-reusing path the raise unwinds `_check_schedule`'s
                transaction, releasing the schedule advisory lock.
        """
        if session is not None:
            return await session.get(ScheduleStateModel, schedule_name)

        async with self.session_factory() as owned_session:
            return await owned_session.get(ScheduleStateModel, schedule_name)

    async def read_due_schedules(
        self,
        schedule_names: Sequence[str],
    ) -> DueScheduleRead:
        """
        Read the due rows of these names and the database time, in one statement.

        A row is due when its next_run_at is at or before the statement's
        database time. Rows of other names are never returned.

        Args:
            schedule_names: The enabled schedules of this scheduler

        Returns:
            DueScheduleRead: the database time and the due rows, soonest first

        Raises:
            Exception: DB errors propagate to `run_forever`'s per-tick
                except, which logs and retries on the next tick. Nothing
                was written; no state can be lost.
        """
        async with self.session_factory() as session:
            result = await session.execute(
                DUE_SCHEDULES_SQL,
                {'schedule_names': list(schedule_names)},
            )
            rows = result.all()
        if not rows:
            raise AssertionError('the due read returned no row')
        database_now = rows[0].database_now
        due: list[DueSchedule] = []
        for row in rows:
            match (row.schedule_name, row.next_run_at):
                case (None, None):
                    pass
                case (str() as schedule_name, datetime() as next_run_at):
                    due.append(DueSchedule(schedule_name=schedule_name, next_run_at=next_run_at))
                case other:
                    raise AssertionError(f'the due read returned a partial row: {other!r}')
        return DueScheduleRead(database_now=database_now, due=tuple(due))

    async def read_start_states(self) -> StartStateRead:
        """
        Read every state row's name and digest and the database time, in one statement.

        Feeds the scheduler start: it computes the first run of each new or
        changed schedule from ``database_now``, and logs the rows of names
        that are not in its config.

        Raises:
            Exception: DB errors propagate to `_initialize_schedules`, which
                logs them; the existence heal then creates missing rows.
                Read-only.
        """
        async with self.session_factory() as session:
            result = await session.execute(START_STATES_SQL)
            rows = result.all()
        if not rows:
            raise AssertionError('the start read returned no row')
        stored: list[StoredConfigHash] = []
        for row in rows:
            match (row.schedule_name, row.config_hash):
                case (None, None):
                    pass
                case (str() as schedule_name, bytes() as config_hash):
                    stored.append(
                        StoredConfigHash(schedule_name=schedule_name, config_hash=config_hash),
                    )
                case other:
                    raise AssertionError(f'the start read returned a partial row: {other!r}')
        return StartStateRead(database_now=rows[0].database_now, rows=tuple(stored))

    async def store_start_states(
        self,
        writes: Sequence[ScheduleStateWrite],
    ) -> tuple[str, ...]:
        """
        Write the rows of new and changed schedules in one statement, and commit.

        A new name gets a row with no run recorded. A row whose digest
        differs gets the new next_run_at and digest; its run_count,
        last_run_at and last_task_id stay. A row that already holds the
        digest is not written. ``updated_at`` is the statement's database
        time.

        Returns:
            The names of the rows written

        Raises:
            Exception: DB errors propagate to `_initialize_schedules`, which
                logs them. The statement is rolled back: no row changed.
        """
        if not writes:
            return ()
        async with self.session_factory() as session:
            result = await session.execute(STORE_START_STATES_SQL, _write_params(writes))
            written = tuple(str(name) for name in result.scalars())
            await session.commit()
        return written

    async def heal_missing_states(
        self,
        writes: Sequence[ScheduleStateWrite],
    ) -> tuple[str, ...]:
        """
        Create the row of each name that has none, in one statement, and commit.

        A row that exists is not written. ``updated_at`` is the statement's
        database time.

        Returns:
            The names of the rows created

        Raises:
            Exception: DB errors propagate to `run_forever`'s per-tick
                except. The statement is rolled back: no row changed, and
                the next tick runs the heal again.
        """
        if not writes:
            return ()
        async with self.session_factory() as session:
            result = await session.execute(HEAL_MISSING_STATES_SQL, _write_params(writes))
            healed = tuple(str(name) for name in result.scalars())
            await session.commit()
        return healed

    async def initialize_state(
        self,
        schedule_name: str,
        next_run_at: datetime,
        config_hash: bytes,
        session: Optional[AsyncSession] = None,
    ) -> ScheduleStateModel:
        """
        Initialize state for a new schedule.

        Args:
            schedule_name: Unique schedule identifier
            next_run_at: Calculated next run time
            config_hash: SHA-256 digest (32 bytes) of the schedule config,
                for change detection. The column is NOT NULL.
            session: Reuse this session (caller commits); None opens an
                owned session that commits here

        Returns:
            Created ScheduleStateModel (or the existing one — check-then-
            insert guard; a concurrent scheduler losing the race raises
            IntegrityError on commit, landing at the same seam as any
            other DB error)

        Raises:
            Exception: DB errors propagate to `_check_schedule`'s caller
                (see `get_state`). A schedule left without a state row is
                invisible to `read_due_schedules`; the tick loop's
                existence heal (`_ensure_states_exist`) creates it.
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
        config_hash: bytes,
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
        task_id: UUID,
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
        task_id: UUID,
        executed_at: datetime,
        next_run_at: datetime,
        commit: bool,
    ) -> None:
        """Run the after-run UPDATE; commit when this call owns the session.

        rowcount == 0 is log-only by design: no horsies code path deletes
        state rows, and both callers read the row in this same transaction
        first — 0 rows means an external delete landed in between. The
        update is lost, and the tick loop's missing-row check recreates
        the row (fresh next_run_at) on a subsequent tick.
        Raises DB errors to the caller's seam (see `update_after_run`).
        """
        # Use raw SQL for atomic update with increment
        result = await session.execute(
            UPDATE_SCHEDULE_AFTER_RUN_SQL,
            {
                'schedule_name': schedule_name,
                'executed_at': executed_at,
                'next_run_at': next_run_at,
                'task_id': str(task_id),
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
        session: Optional[AsyncSession] = None,
    ) -> None:
        """
        Update next_run_at without recording a run (rescheduling without execution).

        Args:
            schedule_name: Unique schedule identifier
            next_run_at: New next run time (UTC)
            session: Reuse this session (caller commits); None opens an
                owned session that commits here

        Raises:
            Exception: DB errors propagate per call site. The tick-path
                callers terminate at `_check_schedule`'s own except (log +
                rollback; the row is unchanged and still due, so the next
                tick retries).
        """
        if session is not None:
            await self._update_next_run_on_session(
                session=session,
                schedule_name=schedule_name,
                next_run_at=next_run_at,
                commit=False,
            )
            return

        async with self.session_factory() as owned_session:
            await self._update_next_run_on_session(
                session=owned_session,
                schedule_name=schedule_name,
                next_run_at=next_run_at,
                commit=True,
            )

    async def _update_next_run_on_session(
        self,
        session: AsyncSession,
        schedule_name: str,
        next_run_at: datetime,
        commit: bool,
    ) -> None:
        """Run the next-run UPDATE; commit when this call owns the session.

        rowcount == 0 is log-only by design — same external-delete anomaly
        as `_update_after_run_on_session`. The missing row is recreated by
        the tick loop's missing-row check.
        Raises DB errors to the caller's seam (see `update_next_run`).
        """
        result = await session.execute(
            UPDATE_SCHEDULE_NEXT_RUN_SQL,
            {
                'schedule_name': schedule_name,
                'next_run_at': next_run_at,
                'now': datetime.now(timezone.utc),
            },
        )
        if commit:
            await session.commit()

        rows_updated = getattr(result, 'rowcount', 0)
        if rows_updated == 0:
            logger.warning(f"Failed to update next_run for '{schedule_name}' - not found")
        else:
            logger.debug(f"Updated next_run for '{schedule_name}': {next_run_at}")
