# python/horsies/core/scheduler/state.py
from __future__ import annotations
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Final
import psycopg
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.brokers.postgres import SendCommand
from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import autocommit_connection, is_retryable_connection_error

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

# The fire: lock the state row, compare next_run_at, insert the slot tasks,
# move the row, in one statement (horsies_fire_schedule, 0007).
FIRE_SCHEDULE_SQL = text("""
    SELECT row_kind, slot_at, task_id, slot_outcome
    FROM horsies_fire_schedule(
        CAST(:schedule_name AS text),
        CAST(:expected_next_run_at AS timestamptz),
        CAST(:next_run_at AS timestamptz),
        CAST(:slot_ats AS timestamptz[]),
        CAST(:task_name AS text),
        CAST(:queue_name AS text),
        CAST(:priority AS integer),
        CAST(:input AS text),
        CAST(:task_options AS text),
        CAST(:execution AS text),
        CAST(:max_retries AS integer),
        CAST(:retention_class_key AS text),
        CAST(:retain_rerun_input AS boolean),
        CAST(:fingerprint_version AS smallint),
        CAST(:fingerprint AS bytea)
    )
""")

# The SQLSTATEs of an argument that breaks a precondition of the fire
# function (invalid_parameter_value, null_value_not_allowed): a defect of the
# caller, not a refusal of the fire.
_CALLER_DEFECT_SQLSTATES: Final = frozenset({'22023', '22004'})


class SlotOutcome(Enum):
    """What the fire did with one slot (the outcomes of ``horsies_insert_task_rows``)."""

    INSERTED = 'INSERTED'
    """The slot's task row is new."""
    SAME_COMMAND = 'SAME_COMMAND'
    """An earlier fire of this slot committed its task: the same command."""
    PAYLOAD_MISMATCH = 'PAYLOAD_MISMATCH'
    """The slot's task id holds another command; the slot counts as fired."""
    CONFLICT_UNVERIFIABLE = 'CONFLICT_UNVERIFIABLE'
    """The slot's task existed and moved to history between the conflict and the
    read: not enqueued again and not recorded as a run."""


@dataclass(frozen=True, slots=True)
class FiredSlot:
    """One slot of an advanced fire."""

    slot_at: datetime
    task_id: str
    outcome: SlotOutcome


@dataclass(frozen=True, slots=True)
class ScheduleFire:
    """The arguments of one fire.

    ``slot_ats`` strictly increase, from ``expected_next_run_at`` to before
    ``next_run_at`` and not after the database time of the due read they were
    computed from. Empty slots only move ``next_run_at``.
    """

    schedule_name: str
    expected_next_run_at: datetime
    next_run_at: datetime
    slot_ats: tuple[datetime, ...]
    command: SendCommand


@dataclass(frozen=True, slots=True)
class ScheduleAdvanced:
    """The slots are written and the row moved, in one commit.

    ``last_run_at``, ``last_task_id`` and ``run_count`` record the fire when
    a slot is recorded (every outcome except ``CONFLICT_UNVERIFIABLE``).
    """

    slots: tuple[FiredSlot, ...]


@dataclass(frozen=True, slots=True)
class ScheduleStateMissing:
    """The schedule has no state row. Nothing was written."""


@dataclass(frozen=True, slots=True)
class ScheduleStateMoved:
    """The row's next_run_at is not the expected one: another scheduler fired,
    or a start moved the row. Nothing was written."""


type ScheduleFireAnswer = ScheduleAdvanced | ScheduleStateMissing | ScheduleStateMoved


@dataclass(frozen=True, slots=True)
class ScheduleFireOutcomeUnknown:
    """The connection failed during the fire: it may or may not have committed.

    Transient. The next due read shows whether the row moved; a repeat of a
    committed slot answers ``SAME_COMMAND`` or ``STATE_MOVED``, never a
    second task.
    """

    schedule_name: str
    cause: SQLAlchemyError


@dataclass(frozen=True, slots=True)
class ScheduleFireRefused:
    """The database refused the fire (not a connection failure). Nothing committed.

    A repeat with the same arguments gives the same refusal, for example a
    CHECK violation of the slot rows.
    """

    schedule_name: str
    cause: SQLAlchemyError


type ScheduleFireError = ScheduleFireOutcomeUnknown | ScheduleFireRefused


def _fire_params(fire: ScheduleFire) -> dict[str, Any]:
    command = fire.command
    return {
        'schedule_name': fire.schedule_name,
        'expected_next_run_at': fire.expected_next_run_at,
        'next_run_at': fire.next_run_at,
        'slot_ats': list(fire.slot_ats),
        'task_name': command.task_name,
        'queue_name': command.queue_name,
        'priority': command.priority,
        'input': command.stored_input,
        'task_options': command.task_options,
        'execution': command.execution.value,
        'max_retries': command.max_retries,
        'retention_class_key': command.retention_class_key,
        'retain_rerun_input': command.retain_rerun_input,
        'fingerprint_version': command.fingerprint_version,
        'fingerprint': command.fingerprint,
    }


def _fired_slot(row: RowMapping) -> FiredSlot:
    """One SLOT row; a NULL or a value of another type breaks the function's contract."""
    slot_at = row['slot_at']
    task_id = row['task_id']
    outcome = row['slot_outcome']
    assert row['row_kind'] == 'SLOT', row
    assert isinstance(slot_at, datetime), row
    assert isinstance(task_id, str), row
    assert isinstance(outcome, str), row
    return FiredSlot(slot_at=slot_at, task_id=task_id, outcome=SlotOutcome(outcome))


def _fire_answer(fire: ScheduleFire, rows: Sequence[RowMapping]) -> ScheduleFireAnswer:
    """The call outcome row, then one SLOT row per slot after ADVANCED."""
    match rows:
        case [first, *slot_rows]:
            pass
        case _:
            raise AssertionError(f'the fire of {fire.schedule_name} answered no row')
    match first['row_kind']:
        case 'ADVANCED':
            slots = tuple(_fired_slot(row) for row in slot_rows)
            assert tuple(slot.slot_at for slot in slots) == fire.slot_ats, (
                f'the fire of {fire.schedule_name} answered slots {slots}'
            )
            return ScheduleAdvanced(slots=slots)
        case 'STATE_MISSING':
            assert not slot_rows, slot_rows
            return ScheduleStateMissing()
        case 'STATE_MOVED':
            assert not slot_rows, slot_rows
            return ScheduleStateMoved()
        case other:
            raise AssertionError(f'the fire of {fire.schedule_name} answered row kind {other!r}')


def _is_caller_defect(exc: SQLAlchemyError) -> bool:
    """Whether the fire function refused an argument that breaks its precondition."""
    match exc:
        case DBAPIError(orig=psycopg.Error(sqlstate=sqlstate)) if (
            sqlstate in _CALLER_DEFECT_SQLSTATES
        ):
            return True
        case _:
            return False


class ScheduleStateManager:
    """
    The scheduler's statements on ``horsies_schedule_state``.

    - the due read, the start read and write, the existence heal;
    - the fire: the slot tasks and the state move in one statement.

    Each method sends one statement and uses SQLAlchemy async sessions.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

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

    async def fire_schedule(
        self,
        fire: ScheduleFire,
    ) -> Result[ScheduleFireAnswer, ScheduleFireError]:
        """
        Fire the slots of one schedule and move its state row, in one statement in autocommit.

        ``horsies_fire_schedule`` locks the row, compares ``next_run_at`` with
        ``fire.expected_next_run_at``, inserts the slot tasks (task id from the
        schedule name and the slot time, ``sent_at`` = the slot time, the
        command's values) and moves the row to ``fire.next_run_at``. The slot
        rows and the row move commit together or not at all.

        Returns:
            Ok(ScheduleAdvanced): the slots, each with its outcome, in slot order.
            Ok(ScheduleStateMissing | ScheduleStateMoved): nothing was written.
            Err(ScheduleFireOutcomeUnknown): a connection failure; transient.
            Err(ScheduleFireRefused): any other database error; nothing committed.

        Raises:
            DBAPIError: SQLSTATE 22023 or 22004, an argument that breaks the
                function's precondition. A defect of the caller; nothing
                committed.
        """
        try:
            async with self.session_factory() as session:
                connection = await autocommit_connection(session)
                result = await connection.execute(FIRE_SCHEDULE_SQL, _fire_params(fire))
                rows = result.mappings().all()
        except SQLAlchemyError as exc:
            if _is_caller_defect(exc):
                raise
            if is_retryable_connection_error(exc):
                return Err(ScheduleFireOutcomeUnknown(schedule_name=fire.schedule_name, cause=exc))
            return Err(ScheduleFireRefused(schedule_name=fire.schedule_name, cause=exc))
        return Ok(_fire_answer(fire, rows))
