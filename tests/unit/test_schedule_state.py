"""Tests for ScheduleStateManager (async DB operations via mocked sessions)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.models.task_pg import ScheduleStateModel
from horsies.core.scheduler.state import (
    ScheduleStateManager,
    UPDATE_SCHEDULE_AFTER_RUN_SQL,
    UPDATE_SCHEDULE_NEXT_RUN_SQL,
    UPDATE_SCHEDULE_NEXT_RUN_WITH_HASH_SQL,
)


# =============================================================================
# Helpers
# =============================================================================


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _make_state(name: str = 'sched-1') -> ScheduleStateModel:
    """Build a minimal ScheduleStateModel for assertions."""
    return ScheduleStateModel(
        schedule_name=name,
        last_run_at=None,
        next_run_at=_utc_now(),
        last_task_id=None,
        run_count=0,
        config_hash=None,
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


# =============================================================================
# get_due_states
# =============================================================================


@pytest.mark.unit
class TestGetDueStates:
    """Tests for ScheduleStateManager.get_due_states."""

    @pytest.mark.asyncio
    async def test_empty_schedule_names_returns_empty_list(self) -> None:
        """Guard clause: empty schedule_names returns [] without DB call."""
        manager, session = _make_manager()

        result = await manager.get_due_states([], _utc_now())

        assert result == []
        # No DB interaction should occur
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_matching_due_states(self) -> None:
        """Returns list of due states from query result."""
        manager, session = _make_manager()
        states = [_make_state('a'), _make_state('b')]

        mock_result = MagicMock()
        mock_result.scalars.return_value = states
        session.execute = AsyncMock(return_value=mock_result)

        result = await manager.get_due_states(['a', 'b'], _utc_now())

        assert result == states
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_empty_when_nothing_due(self) -> None:
        """Returns empty list when no states are due."""
        manager, session = _make_manager()

        mock_result = MagicMock()
        mock_result.scalars.return_value = []
        session.execute = AsyncMock(return_value=mock_result)

        result = await manager.get_due_states(['a'], _utc_now())

        assert result == []


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

        result = await manager.initialize_state('new-sched', next_run, config_hash='abc')

        assert result.schedule_name == 'new-sched'
        assert result.config_hash == 'abc'
        session.add.assert_called_once()
        session.commit.assert_awaited_once()
        session.refresh.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_existing_on_race_condition(self) -> None:
        """Returns existing state when record already initialized (race guard)."""
        manager, session = _make_manager()
        existing = _make_state('existing')
        session.get = AsyncMock(return_value=existing)

        result = await manager.initialize_state('existing', _utc_now())

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

        await manager.update_after_run('sched-1', 'task-abc', now, next_run)

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
            await manager.update_after_run('missing', 'task-1', _utc_now(), _utc_now())

            mock_logger.warning.assert_called_once()
            assert 'missing' in mock_logger.warning.call_args[0][0]


# =============================================================================
# update_next_run
# =============================================================================


@pytest.mark.unit
class TestUpdateNextRun:
    """Tests for ScheduleStateManager.update_next_run."""

    @pytest.mark.asyncio
    async def test_without_config_hash(self) -> None:
        """Without config_hash uses UPDATE_SCHEDULE_NEXT_RUN_SQL."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        session.execute = AsyncMock(return_value=mock_result)

        await manager.update_next_run('sched-1', _utc_now())

        call_args = session.execute.call_args
        assert call_args[0][0] is UPDATE_SCHEDULE_NEXT_RUN_SQL
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_with_config_hash(self) -> None:
        """With config_hash uses UPDATE_SCHEDULE_NEXT_RUN_WITH_HASH_SQL."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        session.execute = AsyncMock(return_value=mock_result)

        await manager.update_next_run('sched-1', _utc_now(), config_hash='deadbeef')

        call_args = session.execute.call_args
        assert call_args[0][0] is UPDATE_SCHEDULE_NEXT_RUN_WITH_HASH_SQL
        params = call_args[0][1]
        assert params['config_hash'] == 'deadbeef'

    @pytest.mark.asyncio
    async def test_no_rows_updated_logs_warning(self) -> None:
        """rowcount == 0 logs a warning for both branches."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 0
        session.execute = AsyncMock(return_value=mock_result)

        with patch('horsies.core.scheduler.state.logger') as mock_logger:
            await manager.update_next_run('missing', _utc_now())

            mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_rows_updated_with_hash_logs_warning(self) -> None:
        """rowcount == 0 with config_hash also logs warning."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 0
        session.execute = AsyncMock(return_value=mock_result)

        with patch('horsies.core.scheduler.state.logger') as mock_logger:
            await manager.update_next_run('missing', _utc_now(), config_hash='abc')

            mock_logger.warning.assert_called_once()


# =============================================================================
# delete_state
# =============================================================================


@pytest.mark.unit
class TestDeleteState:
    """Tests for ScheduleStateManager.delete_state."""

    @pytest.mark.asyncio
    async def test_successful_delete_returns_true(self) -> None:
        """Rows deleted > 0 returns True."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        session.execute = AsyncMock(return_value=mock_result)

        result = await manager.delete_state('sched-1')

        assert result is True
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_not_found_returns_false(self) -> None:
        """Rows deleted == 0 returns False."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.rowcount = 0
        session.execute = AsyncMock(return_value=mock_result)

        result = await manager.delete_state('nonexistent')

        assert result is False


# =============================================================================
# get_all_states
# =============================================================================


@pytest.mark.unit
class TestGetAllStates:
    """Tests for ScheduleStateManager.get_all_states."""

    @pytest.mark.asyncio
    async def test_returns_constructed_models_from_rows(self) -> None:
        """Constructs ScheduleStateModel from raw SQL rows."""
        manager, session = _make_manager()
        now = _utc_now()
        columns = [
            'schedule_name',
            'last_run_at',
            'next_run_at',
            'last_task_id',
            'run_count',
            'config_hash',
            'updated_at',
        ]
        rows = [
            ('sched-a', None, now, None, 5, 'hash1', now),
            ('sched-b', now, now, 'task-1', 10, None, now),
        ]
        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_result.keys.return_value = columns
        session.execute = AsyncMock(return_value=mock_result)

        result = await manager.get_all_states()

        assert len(result) == 2
        assert result[0].schedule_name == 'sched-a'
        assert result[0].run_count == 5
        assert result[1].schedule_name == 'sched-b'
        assert result[1].last_task_id == 'task-1'

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_states(self) -> None:
        """Returns empty list when no schedule states exist."""
        manager, session = _make_manager()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        session.execute = AsyncMock(return_value=mock_result)

        result = await manager.get_all_states()

        assert result == []
