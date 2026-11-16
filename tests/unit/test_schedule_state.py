"""Tests for ScheduleStateManager (async DB operations via mocked sessions)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.models.task_pg import ScheduleStateModel
from horsies.core.scheduler.state import (
    DUE_SCHEDULES_SQL,
    HEAL_MISSING_STATES_SQL,
    START_STATES_SQL,
    STORE_START_STATES_SQL,
    DueSchedule,
    DueScheduleRead,
    ScheduleStateManager,
    ScheduleStateWrite,
    StartStateRead,
    StoredConfigHash,
    UPDATE_SCHEDULE_AFTER_RUN_SQL,
    UPDATE_SCHEDULE_NEXT_RUN_SQL,
)


# =============================================================================
# Helpers
# =============================================================================


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# A 32-byte value: the length that the config_hash column holds.
CONFIG_HASH = bytes(range(32))
OTHER_CONFIG_HASH = bytes(range(32, 64))
# The text form of a uuid: the type of the last_task_id column.
TASK_ID = '0b0e7e64-6d2c-5a8f-9b1e-3c4d5e6f7a8b'


def _make_state(name: str = 'sched-1') -> ScheduleStateModel:
    """Build a minimal ScheduleStateModel for assertions."""
    return ScheduleStateModel(
        schedule_name=name,
        last_run_at=None,
        next_run_at=_utc_now(),
        last_task_id=None,
        run_count=0,
        config_hash=CONFIG_HASH,
        updated_at=_utc_now(),
    )


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


# =============================================================================
# get_state
# =============================================================================


@pytest.mark.unit
class TestGetState:
    """Tests for ScheduleStateManager.get_state."""

    @pytest.mark.asyncio
    async def test_returns_model_when_found(self) -> None:
        """Returns ScheduleStateModel when record exists."""
        manager, session = _make_manager()
        expected = _make_state('my-schedule')
        session.get = AsyncMock(return_value=expected)

        result = await manager.get_state('my-schedule')

        assert result is expected
        session.get.assert_awaited_once_with(ScheduleStateModel, 'my-schedule')

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self) -> None:
        """Returns None when no record exists."""
        manager, session = _make_manager()
        session.get = AsyncMock(return_value=None)

        result = await manager.get_state('nonexistent')

        assert result is None

    @pytest.mark.asyncio
    async def test_uses_provided_session_without_factory(self) -> None:
        """When session is provided, no new session is opened."""
        manager, session = _make_manager()
        session.get = AsyncMock(return_value=None)

        result = await manager.get_state('sched-1', session=session)

        assert result is None
        session.get.assert_awaited_once_with(ScheduleStateModel, 'sched-1')
        manager.session_factory.assert_not_called()


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
# initialize_state
# =============================================================================


@pytest.mark.unit
class TestInitializeState:
    """Tests for ScheduleStateManager.initialize_state."""

    @pytest.mark.asyncio
    async def test_creates_new_state_when_not_exists(self) -> None:
        """Creates new state, commits, refreshes when schedule not found."""
        manager, session = _make_manager()
        session.get = AsyncMock(return_value=None)
        next_run = _utc_now()

        result = await manager.initialize_state('new-sched', next_run, config_hash=CONFIG_HASH)

        assert result.schedule_name == 'new-sched'
        assert result.config_hash == CONFIG_HASH
        session.add.assert_called_once()
        session.commit.assert_awaited_once()
        session.refresh.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_existing_on_race_condition(self) -> None:
        """Returns existing state when record already initialized (race guard)."""
        manager, session = _make_manager()
        existing = _make_state('existing')
        session.get = AsyncMock(return_value=existing)

        result = await manager.initialize_state('existing', _utc_now(), CONFIG_HASH)

        assert result is existing
        session.add.assert_not_called()
        session.commit.assert_not_awaited()


# =============================================================================
# update_after_run
# =============================================================================


@pytest.mark.unit
class TestUpdateAfterRun:
    """Tests for ScheduleStateManager.update_after_run."""

    @pytest.mark.asyncio
    async def test_successful_update(self) -> None:
        """Rows updated > 0: commits and logs debug."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        session.execute = AsyncMock(return_value=mock_result)
        now = _utc_now()
        next_run = _utc_now()

        await manager.update_after_run('sched-1', TASK_ID, now, next_run)

        session.execute.assert_awaited_once()
        session.commit.assert_awaited_once()
        # Verify the SQL statement used
        call_args = session.execute.call_args
        assert call_args[0][0] is UPDATE_SCHEDULE_AFTER_RUN_SQL

    @pytest.mark.asyncio
    async def test_no_rows_updated_logs_warning(self) -> None:
        """Rows updated == 0: logs warning (schedule not found)."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 0
        session.execute = AsyncMock(return_value=mock_result)

        with patch('horsies.core.scheduler.state.logger') as mock_logger:
            await manager.update_after_run('missing', TASK_ID, _utc_now(), _utc_now())

            mock_logger.warning.assert_called_once()
            assert 'missing' in mock_logger.warning.call_args[0][0]


# =============================================================================
# update_next_run
# =============================================================================


@pytest.mark.unit
class TestUpdateNextRun:
    """Tests for ScheduleStateManager.update_next_run."""

    @pytest.mark.asyncio
    async def test_uses_the_next_run_statement(self) -> None:
        """Uses UPDATE_SCHEDULE_NEXT_RUN_SQL and commits an owned session."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        session.execute = AsyncMock(return_value=mock_result)

        await manager.update_next_run('sched-1', _utc_now())

        call_args = session.execute.call_args
        assert call_args[0][0] is UPDATE_SCHEDULE_NEXT_RUN_SQL
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_rows_updated_logs_warning(self) -> None:
        """rowcount == 0 logs a warning."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 0
        session.execute = AsyncMock(return_value=mock_result)

        with patch('horsies.core.scheduler.state.logger') as mock_logger:
            await manager.update_next_run('missing', _utc_now())

            mock_logger.warning.assert_called_once()
