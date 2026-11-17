"""Tests for ScheduleStateManager (async DB operations via mocked sessions)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Final
from unittest.mock import AsyncMock, MagicMock

import psycopg.errors
import pytest
from sqlalchemy.exc import DBAPIError, OperationalError

from horsies.core.brokers.postgres import SendCommand
from horsies.core.models.tasks import Execution
from horsies.core.scheduler.state import (
    DUE_SCHEDULES_SQL,
    FIRE_SCHEDULE_SQL,
    HEAL_MISSING_STATES_SQL,
    START_STATES_SQL,
    STORE_START_STATES_SQL,
    DueSchedule,
    DueScheduleRead,
    FiredSlot,
    ScheduleAdvanced,
    ScheduleFire,
    ScheduleFireOutcomeUnknown,
    ScheduleFireRefused,
    ScheduleStateManager,
    ScheduleStateMissing,
    ScheduleStateMoved,
    ScheduleStateWrite,
    SlotOutcome,
    StartStateRead,
    StoredConfigHash,
)
from horsies.core.types.result import Err, Ok


# =============================================================================
# Helpers
# =============================================================================


# A 32-byte value: the length that the config_hash column holds.
CONFIG_HASH = bytes(range(32))
OTHER_CONFIG_HASH = bytes(range(32, 64))
# The text form of a uuid: the type of the last_task_id column.
TASK_ID = '0b0e7e64-6d2c-5a8f-9b1e-3c4d5e6f7a8b'


def _make_manager() -> tuple[ScheduleStateManager, AsyncMock]:
    """Create a ScheduleStateManager with a mocked async session factory.

    Returns (manager, mock_session).
    The session mock is configured as an async context manager.
    """
    mock_session = AsyncMock()
    # session.add() is synchronous in SQLAlchemy — use MagicMock to avoid coroutine warning
    mock_session.add = MagicMock()

    # Make the session factory return an async context manager yielding mock_session
    mock_factory = MagicMock()
    mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

    manager = ScheduleStateManager(session_factory=mock_factory)
    return manager, mock_session


def _rows_result(rows: list[SimpleNamespace]) -> MagicMock:
    result = MagicMock()
    result.all.return_value = rows
    return result


def _names_result(names: list[str]) -> MagicMock:
    result = MagicMock()
    result.scalars.return_value = iter(names)
    return result


DATABASE_NOW = datetime(2026, 11, 16, 12, 0, 0, tzinfo=timezone.utc)


# =============================================================================
# read_due_schedules
# =============================================================================


@pytest.mark.unit
class TestReadDueSchedules:
    """Tests for ScheduleStateManager.read_due_schedules."""

    @pytest.mark.asyncio
    async def test_returns_the_database_time_and_the_due_rows(self) -> None:
        manager, session = _make_manager()
        earlier = DATABASE_NOW - timedelta(seconds=2)
        session.execute = AsyncMock(
            return_value=_rows_result(
                [
                    SimpleNamespace(database_now=DATABASE_NOW, schedule_name='a', next_run_at=earlier),
                    SimpleNamespace(database_now=DATABASE_NOW, schedule_name='b', next_run_at=DATABASE_NOW),
                ],
            ),
        )

        result = await manager.read_due_schedules(['a', 'b', 'c'])

        assert result == DueScheduleRead(
            database_now=DATABASE_NOW,
            due=(DueSchedule('a', earlier), DueSchedule('b', DATABASE_NOW)),
        )
        statement, params = session.execute.await_args.args
        assert statement is DUE_SCHEDULES_SQL
        assert params == {'schedule_names': ['a', 'b', 'c']}

    @pytest.mark.asyncio
    async def test_nothing_due_still_returns_the_database_time(self) -> None:
        """The one-row join answers one row with no name when nothing is due."""
        manager, session = _make_manager()
        session.execute = AsyncMock(
            return_value=_rows_result(
                [SimpleNamespace(database_now=DATABASE_NOW, schedule_name=None, next_run_at=None)],
            ),
        )

        result = await manager.read_due_schedules(['a'])

        assert result == DueScheduleRead(database_now=DATABASE_NOW, due=())

    @pytest.mark.asyncio
    async def test_a_partial_row_is_a_defect(self) -> None:
        manager, session = _make_manager()
        session.execute = AsyncMock(
            return_value=_rows_result(
                [SimpleNamespace(database_now=DATABASE_NOW, schedule_name='a', next_run_at=None)],
            ),
        )

        with pytest.raises(AssertionError, match='partial row'):
            await manager.read_due_schedules(['a'])

    def test_the_statement_reads_the_database_clock(self) -> None:
        """Due means next_run_at at or before statement_timestamp(); no
        client time is a parameter."""
        sql = str(DUE_SCHEDULES_SQL)
        assert 's.next_run_at <= statement_timestamp()' in sql
        assert 'statement_timestamp() AS database_now' in sql
        assert ':schedule_names' in sql
        assert DUE_SCHEDULES_SQL.compile().params.keys() == {'schedule_names'}


# =============================================================================
# read_start_states / store_start_states / heal_missing_states
# =============================================================================


@pytest.mark.unit
class TestStartStates:
    """Tests for the start read and the start write."""

    @pytest.mark.asyncio
    async def test_start_read_returns_the_database_time_and_every_digest(self) -> None:
        manager, session = _make_manager()
        session.execute = AsyncMock(
            return_value=_rows_result(
                [
                    SimpleNamespace(database_now=DATABASE_NOW, schedule_name='a', config_hash=CONFIG_HASH),
                    SimpleNamespace(database_now=DATABASE_NOW, schedule_name='b', config_hash=OTHER_CONFIG_HASH),
                ],
            ),
        )

        result = await manager.read_start_states()

        assert result == StartStateRead(
            database_now=DATABASE_NOW,
            rows=(StoredConfigHash('a', CONFIG_HASH), StoredConfigHash('b', OTHER_CONFIG_HASH)),
        )
        assert session.execute.await_args.args[0] is START_STATES_SQL

    @pytest.mark.asyncio
    async def test_start_read_of_an_empty_table_returns_the_database_time(self) -> None:
        manager, session = _make_manager()
        session.execute = AsyncMock(
            return_value=_rows_result(
                [SimpleNamespace(database_now=DATABASE_NOW, schedule_name=None, config_hash=None)],
            ),
        )

        result = await manager.read_start_states()

        assert result == StartStateRead(database_now=DATABASE_NOW, rows=())

    @pytest.mark.asyncio
    async def test_start_write_sends_one_statement_and_commits(self) -> None:
        manager, session = _make_manager()
        session.execute = AsyncMock(return_value=_names_result(['a']))
        writes = [
            ScheduleStateWrite('a', DATABASE_NOW, CONFIG_HASH),
            ScheduleStateWrite('b', DATABASE_NOW, OTHER_CONFIG_HASH),
        ]

        written = await manager.store_start_states(writes)

        assert written == ('a',)
        statement, params = session.execute.await_args.args
        assert statement is STORE_START_STATES_SQL
        assert params == {
            'schedule_names': ['a', 'b'],
            'next_runs': [DATABASE_NOW, DATABASE_NOW],
            'config_hashes': [CONFIG_HASH, OTHER_CONFIG_HASH],
        }
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_writes_send_no_statement(self) -> None:
        manager, session = _make_manager()

        assert await manager.store_start_states([]) == ()
        assert await manager.heal_missing_states([]) == ()
        session.execute.assert_not_awaited()

    def test_a_changed_row_keeps_its_run_record(self) -> None:
        sql = str(STORE_START_STATES_SQL)
        update = sql.split('DO UPDATE', 1)[1]
        for kept in ('run_count', 'last_run_at', 'last_task_id'):
            assert kept not in update
        assert 'WHERE horsies_schedule_state.config_hash <> EXCLUDED.config_hash' in update
        assert 'statement_timestamp()' in sql


@pytest.mark.unit
class TestHealMissingStates:
    """Tests for ScheduleStateManager.heal_missing_states."""

    @pytest.mark.asyncio
    async def test_returns_the_created_names_and_commits(self) -> None:
        manager, session = _make_manager()
        session.execute = AsyncMock(return_value=_names_result(['b']))

        healed = await manager.heal_missing_states(
            [ScheduleStateWrite('a', DATABASE_NOW, CONFIG_HASH), ScheduleStateWrite('b', DATABASE_NOW, CONFIG_HASH)],
        )

        assert healed == ('b',)
        assert session.execute.await_args.args[0] is HEAL_MISSING_STATES_SQL
        session.commit.assert_awaited_once()

    def test_an_existing_row_is_not_written(self) -> None:
        sql = str(HEAL_MISSING_STATES_SQL)
        assert 'ON CONFLICT (schedule_name) DO NOTHING' in sql
        assert 'DO UPDATE' not in sql
        assert 'statement_timestamp()' in sql


# =============================================================================
# fire_schedule
# =============================================================================

SLOT_1: Final = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
SLOT_2: Final = SLOT_1 + timedelta(minutes=1)
COMMAND: Final = SendCommand(
    task_name='tasks.report',
    queue_name='default',
    priority=100,
    stored_input='{"value":1}',
    task_options=None,
    execution=Execution.ISOLATED,
    max_retries=0,
    retention_class_key='standard_30d',
    retain_rerun_input=False,
    fingerprint_version=1,
    fingerprint=bytes(32),
)
FIRE: Final = ScheduleFire(
    schedule_name='sched-1',
    expected_next_run_at=SLOT_1,
    next_run_at=SLOT_2 + timedelta(minutes=1),
    slot_ats=(SLOT_1, SLOT_2),
    command=COMMAND,
)


def _fire_manager(answer: list[dict[str, object]] | BaseException) -> tuple[ScheduleStateManager, MagicMock]:
    """A manager whose fire statement answers ``answer`` on the autocommit connection."""
    manager, session = _make_manager()
    connection = MagicMock()
    match answer:
        case BaseException():
            connection.execute = AsyncMock(side_effect=answer)
        case rows:
            result = MagicMock()
            result.mappings.return_value.all.return_value = rows
            connection.execute = AsyncMock(return_value=result)
    session.connection = AsyncMock(return_value=connection)
    return manager, connection


def _row(kind: str, slot_at: datetime | None = None, task_id: str | None = None, outcome: str | None = None) -> dict[str, object]:
    return {'row_kind': kind, 'slot_at': slot_at, 'task_id': task_id, 'slot_outcome': outcome}


@pytest.mark.unit
class TestFireSchedule:
    """ScheduleStateManager.fire_schedule: one statement, one typed outcome."""

    @pytest.mark.asyncio
    async def test_one_autocommit_statement_carries_the_slots_and_the_command(self) -> None:
        manager, connection = _fire_manager([_row('STATE_MOVED')])

        await manager.fire_schedule(FIRE)

        connection.execute.assert_awaited_once()
        statement, parameters = connection.execute.await_args.args
        assert statement is FIRE_SCHEDULE_SQL
        assert parameters == {
            'schedule_name': 'sched-1',
            'expected_next_run_at': SLOT_1,
            'next_run_at': SLOT_2 + timedelta(minutes=1),
            'slot_ats': [SLOT_1, SLOT_2],
            'task_name': 'tasks.report',
            'queue_name': 'default',
            'priority': 100,
            'input': '{"value":1}',
            'task_options': None,
            'execution': 'ISOLATED',
            'max_retries': 0,
            'retention_class_key': 'standard_30d',
            'retain_rerun_input': False,
            'fingerprint_version': 1,
            'fingerprint': bytes(32),
        }

    @pytest.mark.asyncio
    async def test_an_advanced_fire_answers_each_slot_with_its_outcome(self) -> None:
        manager, _ = _fire_manager(
            [
                _row('ADVANCED'),
                _row('SLOT', SLOT_1, TASK_ID, 'INSERTED'),
                _row('SLOT', SLOT_2, TASK_ID, 'PAYLOAD_MISMATCH'),
            ],
        )

        assert await manager.fire_schedule(FIRE) == Ok(
            ScheduleAdvanced(
                slots=(
                    FiredSlot(SLOT_1, TASK_ID, SlotOutcome.INSERTED),
                    FiredSlot(SLOT_2, TASK_ID, SlotOutcome.PAYLOAD_MISMATCH),
                ),
            ),
        )

    @pytest.mark.asyncio
    async def test_a_missing_row_answers_state_missing(self) -> None:
        manager, _ = _fire_manager([_row('STATE_MISSING')])

        assert await manager.fire_schedule(FIRE) == Ok(ScheduleStateMissing())

    @pytest.mark.asyncio
    async def test_a_moved_row_answers_state_moved(self) -> None:
        manager, _ = _fire_manager([_row('STATE_MOVED')])

        assert await manager.fire_schedule(FIRE) == Ok(ScheduleStateMoved())

    @pytest.mark.asyncio
    async def test_a_connection_failure_is_an_unknown_outcome(self) -> None:
        cause = OperationalError('SELECT', {}, Exception('connection lost'))
        manager, _ = _fire_manager(cause)

        assert await manager.fire_schedule(FIRE) == Err(
            ScheduleFireOutcomeUnknown(schedule_name='sched-1', cause=cause),
        )

    @pytest.mark.asyncio
    async def test_another_database_error_is_a_refusal(self) -> None:
        cause = DBAPIError('SELECT', {}, psycopg.errors.CheckViolation('check'))
        manager, _ = _fire_manager(cause)

        assert await manager.fire_schedule(FIRE) == Err(
            ScheduleFireRefused(schedule_name='sched-1', cause=cause),
        )

    @pytest.mark.parametrize(
        'driver_error',
        [psycopg.errors.InvalidParameterValue('slots'), psycopg.errors.NullValueNotAllowed('null')],
        ids=['22023', '22004'],
    )
    @pytest.mark.asyncio
    async def test_a_broken_precondition_is_a_defect_that_raises(
        self, driver_error: psycopg.Error,
    ) -> None:
        cause = DBAPIError('SELECT', {}, driver_error)
        manager, _ = _fire_manager(cause)

        with pytest.raises(DBAPIError) as raised:
            await manager.fire_schedule(FIRE)

        assert raised.value is cause

    @pytest.mark.asyncio
    async def test_slots_that_differ_from_the_arguments_break_the_contract(self) -> None:
        manager, _ = _fire_manager([_row('ADVANCED'), _row('SLOT', SLOT_1, TASK_ID, 'INSERTED')])

        with pytest.raises(AssertionError, match='answered slots'):
            await manager.fire_schedule(FIRE)
