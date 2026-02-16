"""Tests for PostgresBroker (horsies/core/brokers/postgres.py).

Strategy: mock DB layer entirely (create_async_engine, async_sessionmaker,
PostgresListener) to avoid real PostgreSQL. Tests verify logic, branching,
error handling, and delegation patterns.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.models.tasks import (
    LibraryErrorCode,
    TaskError,
    TaskInfo,
    TaskResult,
)
from horsies.core.types.status import TaskStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_broker(database_url: str = 'postgresql+psycopg://u:p@localhost/db') -> Any:
    """Create a PostgresBroker with fully mocked internals."""
    from horsies.core.models.broker import PostgresConfig

    with (
        patch('horsies.core.brokers.postgres.create_async_engine') as mock_engine,
        patch('horsies.core.brokers.postgres.async_sessionmaker') as mock_sm,
        patch('horsies.core.brokers.postgres.PostgresListener') as mock_listener_cls,
    ):
        mock_engine.return_value = MagicMock()
        mock_engine.return_value.dispose = AsyncMock()
        mock_engine.return_value.begin = MagicMock()

        mock_session = AsyncMock()
        mock_sm.return_value = MagicMock(return_value=mock_session)
        # Make the session work as an async context manager
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        # session.add() is synchronous in SQLAlchemy — use MagicMock to
        # avoid "coroutine never awaited" warnings from AsyncMock.
        mock_session.add = MagicMock()

        mock_listener = AsyncMock()
        mock_listener_cls.return_value = mock_listener

        config = PostgresConfig(database_url=database_url)

        from horsies.core.brokers.postgres import PostgresBroker

        broker = PostgresBroker(config)
        # Mark as initialized to skip real DB setup
        broker._initialized = True

    return broker


def _make_task_row(**overrides: Any) -> MagicMock:
    """Build a mock TaskModel row with sensible defaults."""
    defaults = {
        'id': 'task-123',
        'task_name': 'my_task',
        'queue_name': 'default',
        'priority': 100,
        'status': TaskStatus.PENDING,
        'args': None,
        'kwargs': None,
        'result': None,
        'failed_reason': None,
        'sent_at': datetime.now(timezone.utc),
        'claimed_at': None,
        'started_at': None,
        'completed_at': None,
        'failed_at': None,
        'retry_count': 0,
        'max_retries': 0,
        'next_retry_at': None,
        'worker_hostname': None,
        'worker_pid': None,
        'worker_process_name': None,
    }
    defaults.update(overrides)
    row = MagicMock()
    for k, v in defaults.items():
        setattr(row, k, v)
    return row


# ---------------------------------------------------------------------------
# TestSchemaAdvisoryKey
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSchemaAdvisoryKey:
    """Tests for _schema_advisory_key determinism and uniqueness."""

    def test_returns_stable_int_from_database_url(self) -> None:
        """Same URL should always produce the same advisory key."""
        broker = _make_broker('postgresql+psycopg://u:p@host1/db')

        key1 = broker._schema_advisory_key()
        key2 = broker._schema_advisory_key()

        assert isinstance(key1, int)
        assert key1 == key2

    def test_different_urls_produce_different_keys(self) -> None:
        """Different database URLs should produce different advisory keys."""
        broker_a = _make_broker('postgresql+psycopg://u:p@host_a/db')
        broker_b = _make_broker('postgresql+psycopg://u:p@host_b/db')

        key_a = broker_a._schema_advisory_key()
        key_b = broker_b._schema_advisory_key()

        assert key_a != key_b

    def test_key_is_signed_64_bit(self) -> None:
        """Advisory key must fit in a signed 64-bit range for pg_advisory_xact_lock."""
        broker = _make_broker()

        key = broker._schema_advisory_key()

        assert -(2**63) <= key <= 2**63 - 1


# ---------------------------------------------------------------------------
# TestEnqueueAsync
# ---------------------------------------------------------------------------


def _make_enqueue_session() -> AsyncMock:
    """Build a mock session for enqueue_async tests.

    session.add is a sync MagicMock (not AsyncMock) because
    SQLAlchemy's Session.add() is synchronous.
    """
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.add = MagicMock()
    return session


@pytest.mark.unit
class TestEnqueueAsync:
    """Tests for enqueue_async: task creation, option parsing, retry extraction."""

    @pytest.mark.asyncio
    async def test_basic_enqueue_returns_uuid_string(self) -> None:
        """enqueue_async should return a UUID string and commit the session."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        task_id = await broker.enqueue_async(
            'my_task', (1, 2), {'key': 'val'}, 'default',
        )

        assert isinstance(task_id, str)
        assert len(task_id) == 36  # UUID format
        session.add.assert_called_once()
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_with_sent_at_uses_provided_timestamp(self) -> None:
        """When sent_at is provided, it should be forwarded to the TaskModel."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        custom_ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        await broker.enqueue_async(
            'my_task', (), {}, sent_at=custom_ts,
        )

        added_task = session.add.call_args[0][0]
        assert added_task.sent_at == custom_ts

    @pytest.mark.asyncio
    async def test_with_retry_policy_extracts_max_retries(self) -> None:
        """task_options containing retry_policy should set max_retries on the task."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        task_options = '{"retry_policy": {"max_retries": 5}}'
        await broker.enqueue_async(
            'my_task', (), {}, task_options=task_options,
        )

        added_task = session.add.call_args[0][0]
        assert added_task.max_retries == 5

    @pytest.mark.asyncio
    async def test_malformed_task_options_falls_back_to_zero_retries(self) -> None:
        """Malformed task_options JSON should not crash; max_retries defaults to 0."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        await broker.enqueue_async(
            'my_task', (), {}, task_options='NOT_VALID_JSON{{',
        )

        added_task = session.add.call_args[0][0]
        assert added_task.max_retries == 0

    @pytest.mark.asyncio
    async def test_with_good_until_passes_expiry(self) -> None:
        """good_until should be passed through to the TaskModel."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        expiry = datetime(2099, 12, 31, tzinfo=timezone.utc)
        await broker.enqueue_async(
            'my_task', (), {}, good_until=expiry,
        )

        added_task = session.add.call_args[0][0]
        assert added_task.good_until == expiry

    @pytest.mark.asyncio
    async def test_task_options_with_non_dict_retry_policy_keeps_zero(self) -> None:
        """retry_policy that is not a dict should not set max_retries."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        task_options = '{"retry_policy": "not_a_dict"}'
        await broker.enqueue_async(
            'my_task', (), {}, task_options=task_options,
        )

        added_task = session.add.call_args[0][0]
        assert added_task.max_retries == 0

    @pytest.mark.asyncio
    async def test_task_options_without_retry_policy_keeps_zero(self) -> None:
        """task_options dict without retry_policy key should keep max_retries=0."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        task_options = '{"some_other_key": "value"}'
        await broker.enqueue_async(
            'my_task', (), {}, task_options=task_options,
        )

        added_task = session.add.call_args[0][0]
        assert added_task.max_retries == 0


# ---------------------------------------------------------------------------
# TestGetResultAsync
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetResultAsync:
    """Tests for get_result_async: various task states, timeout, errors."""

    @pytest.mark.asyncio
    async def test_task_not_found_returns_task_not_found(self) -> None:
        """When task is not in DB, return TASK_NOT_FOUND error."""
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.get = AsyncMock(return_value=None)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_result_async('missing-id')

        assert result.is_err()
        err = result.unwrap_err()
        assert err.error_code == LibraryErrorCode.TASK_NOT_FOUND
        assert 'missing-id' in (err.message or '')

    @pytest.mark.asyncio
    async def test_completed_task_returns_deserialized_result(self) -> None:
        """COMPLETED task should return the deserialized result immediately."""
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)

        row = _make_task_row(
            status=TaskStatus.COMPLETED,
            result='{"__task_result__":true,"ok":42,"err":null}',
        )
        session.get = AsyncMock(return_value=row)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_result_async('task-123')

        assert result.is_ok()
        assert result.unwrap() == 42

    @pytest.mark.asyncio
    async def test_failed_task_returns_deserialized_error(self) -> None:
        """FAILED task should return the deserialized TaskError result."""
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)

        result_json = (
            '{"__task_result__":true,"ok":null,"err":'
            '{"__task_error__":true,"error_code":"UNHANDLED_EXCEPTION","message":"boom","data":null}}'
        )
        row = _make_task_row(status=TaskStatus.FAILED, result=result_json)
        session.get = AsyncMock(return_value=row)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_result_async('task-123')

        assert result.is_err()
        err = result.unwrap_err()
        assert err.error_code == LibraryErrorCode.UNHANDLED_EXCEPTION
        assert err.message == 'boom'

    @pytest.mark.asyncio
    async def test_cancelled_task_returns_task_cancelled(self) -> None:
        """CANCELLED task should return TASK_CANCELLED error."""
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)

        row = _make_task_row(status=TaskStatus.CANCELLED)
        session.get = AsyncMock(return_value=row)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_result_async('task-123')

        assert result.is_err()
        err = result.unwrap_err()
        assert err.error_code == LibraryErrorCode.TASK_CANCELLED

    @pytest.mark.asyncio
    async def test_generic_exception_returns_broker_error(self) -> None:
        """Generic exception during get_result_async should return BROKER_ERROR."""
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.get = AsyncMock(side_effect=RuntimeError('db crash'))
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_result_async('task-123')

        assert result.is_err()
        err = result.unwrap_err()
        assert err.error_code == LibraryErrorCode.BROKER_ERROR
        assert err.exception is not None

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self) -> None:
        """asyncio.CancelledError should be re-raised, not caught."""
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.get = AsyncMock(side_effect=asyncio.CancelledError)
        broker.session_factory = MagicMock(return_value=session)

        with pytest.raises(asyncio.CancelledError):
            await broker.get_result_async('task-123')

    @pytest.mark.asyncio
    async def test_cross_loop_listener_error_falls_back_to_polling(self) -> None:
        """RuntimeError from listener.listen should fall back to DB polling."""
        broker = _make_broker()
        broker.listener.listen = AsyncMock(
            side_effect=RuntimeError('different event loop')
        )
        broker.listener.unsubscribe = AsyncMock()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        row_running = _make_task_row(status=TaskStatus.RUNNING)
        row_completed = _make_task_row(
            status=TaskStatus.COMPLETED,
            result='{"__task_result__":true,"ok":"done","err":null}',
        )
        # First get() is the initial quick-path check, second is first polling pass.
        session.get = AsyncMock(side_effect=[row_running, row_completed])
        broker.session_factory = MagicMock(return_value=session)

        with patch('horsies.core.brokers.postgres.asyncio.sleep', new=AsyncMock()):
            result = await broker.get_result_async('task-123', timeout_ms=1000)

        assert result.is_ok()
        assert result.unwrap() == 'done'
        broker.listener.listen.assert_awaited_once_with('task_done')
        broker.listener.unsubscribe.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestMonitoringQueries
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMonitoringQueries:
    """Tests for get_stale_tasks, get_worker_stats, get_expired_tasks."""

    def _setup_session_with_rows(
        self,
        broker: Any,
        columns: list[str],
        rows: list[tuple[Any, ...]],
    ) -> None:
        """Configure broker's session_factory to return given rows."""
        mock_result = MagicMock()
        mock_result.keys.return_value = columns
        mock_result.fetchall.return_value = rows

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=mock_result)
        broker.session_factory = MagicMock(return_value=session)

    @pytest.mark.asyncio
    async def test_get_stale_tasks_returns_list_of_dicts(self) -> None:
        """get_stale_tasks should return dicts keyed by column names."""
        broker = _make_broker()
        columns = ['id', 'worker_hostname', 'worker_pid', 'last_heartbeat']
        rows = [
            ('task-1', 'host-1', 1234, None),
            ('task-2', 'host-2', 5678, None),
        ]
        self._setup_session_with_rows(broker, columns, rows)

        result = await broker.get_stale_tasks(stale_threshold_minutes=5)

        assert len(result) == 2
        assert result[0] == {
            'id': 'task-1',
            'worker_hostname': 'host-1',
            'worker_pid': 1234,
            'last_heartbeat': None,
        }
        assert result[1]['id'] == 'task-2'

    @pytest.mark.asyncio
    async def test_get_stale_tasks_empty(self) -> None:
        """get_stale_tasks with no rows returns empty list."""
        broker = _make_broker()
        self._setup_session_with_rows(broker, ['id'], [])

        result = await broker.get_stale_tasks()

        assert result == []

    @pytest.mark.asyncio
    async def test_get_worker_stats_returns_list_of_dicts(self) -> None:
        """get_worker_stats should return dicts keyed by column names."""
        broker = _make_broker()
        columns = ['worker_hostname', 'worker_pid', 'worker_process_name', 'active_tasks']
        rows = [('host-1', 1234, 'worker-0', 3)]
        self._setup_session_with_rows(broker, columns, rows)

        result = await broker.get_worker_stats()

        assert len(result) == 1
        assert result[0]['active_tasks'] == 3

    @pytest.mark.asyncio
    async def test_get_expired_tasks_returns_list_of_dicts(self) -> None:
        """get_expired_tasks should return dicts keyed by column names."""
        broker = _make_broker()
        columns = ['id', 'task_name', 'queue_name', 'priority', 'sent_at', 'good_until', 'expired_for']
        rows = [('task-1', 'slow_task', 'default', 100, None, None, '00:05:00')]
        self._setup_session_with_rows(broker, columns, rows)

        result = await broker.get_expired_tasks()

        assert len(result) == 1
        assert result[0]['task_name'] == 'slow_task'


# ---------------------------------------------------------------------------
# TestMarkStaleTasksAsFailed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMarkStaleTasksAsFailed:
    """Tests for mark_stale_tasks_as_failed stale detection and cleanup."""

    @pytest.mark.asyncio
    async def test_no_stale_tasks_returns_zero(self) -> None:
        """When no stale tasks found, should return 0 without commit."""
        broker = _make_broker()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=mock_result)
        broker.session_factory = MagicMock(return_value=session)

        count = await broker.mark_stale_tasks_as_failed()

        assert count == 0
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_tasks_marked_failed_and_committed(self) -> None:
        """Found stale tasks should be marked FAILED and committed."""
        broker = _make_broker()
        stale_started_at = datetime(2025, 1, 1, tzinfo=timezone.utc)
        # Each row: (id, worker_pid, worker_hostname, claimed_by_worker_id, started_at, last_heartbeat)
        stale_rows = [
            ('task-1', 1234, 'host-1', 'worker-1', stale_started_at, None),
            ('task-2', 5678, 'host-2', 'worker-2', stale_started_at, stale_started_at),
        ]
        select_result = MagicMock()
        select_result.fetchall.return_value = stale_rows

        update_result = MagicMock()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        # First execute call is SELECT, subsequent are UPDATEs
        session.execute = AsyncMock(side_effect=[select_result, update_result, update_result])
        broker.session_factory = MagicMock(return_value=session)

        count = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)

        assert count == 2
        session.commit.assert_awaited_once()
        # 1 SELECT + 2 UPDATEs
        assert session.execute.await_count == 3

    @pytest.mark.asyncio
    async def test_update_includes_worker_crashed_error_code(self) -> None:
        """Each update call should include WORKER_CRASHED in the result JSON."""
        broker = _make_broker()
        stale_rows = [
            ('task-1', 100, 'host', 'w-1', datetime(2025, 1, 1, tzinfo=timezone.utc), None),
        ]
        select_result = MagicMock()
        select_result.fetchall.return_value = stale_rows

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=[select_result, MagicMock()])
        broker.session_factory = MagicMock(return_value=session)

        await broker.mark_stale_tasks_as_failed()

        # Second execute call is the UPDATE
        update_call = session.execute.call_args_list[1]
        params = update_call[0][1]
        assert params['task_id'] == 'task-1'
        assert 'WORKER_CRASHED' in params['result']
        assert 'Worker process crashed' in params['failed_reason']


# ---------------------------------------------------------------------------
# TestRequeueStaleClaimed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRequeueStaleClaimed:
    """Tests for requeue_stale_claimed."""

    @pytest.mark.asyncio
    async def test_returns_rowcount(self) -> None:
        """Should return rowcount from the update result."""
        broker = _make_broker()
        mock_result = MagicMock()
        mock_result.rowcount = 5

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=mock_result)
        broker.session_factory = MagicMock(return_value=session)

        count = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)

        assert count == 5
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_rowcount(self) -> None:
        """Should fall back to 0 when rowcount attribute is missing."""
        broker = _make_broker()
        mock_result = object()  # no rowcount attribute

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=mock_result)
        broker.session_factory = MagicMock(return_value=session)

        count = await broker.requeue_stale_claimed()

        assert count == 0

    @pytest.mark.asyncio
    async def test_threshold_converted_to_seconds(self) -> None:
        """stale_threshold_ms should be converted to seconds for the SQL parameter."""
        broker = _make_broker()
        mock_result = MagicMock()
        mock_result.rowcount = 0

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=mock_result)
        broker.session_factory = MagicMock(return_value=session)

        await broker.requeue_stale_claimed(stale_threshold_ms=60_000)

        execute_args = session.execute.call_args[0]
        params = execute_args[1]
        assert params['stale_threshold'] == 60.0


# ---------------------------------------------------------------------------
# TestSyncFacades
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSyncFacades:
    """Tests for sync wrappers: enqueue, get_result, get_task_info."""

    def test_enqueue_delegates_to_loop_runner(self) -> None:
        """enqueue() should call _loop_runner.call with enqueue_async."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(return_value='task-id-123')

        result = broker.enqueue(
            'my_task', (1,), {'k': 'v'}, 'queue',
            priority=50, sent_at=None, good_until=None, task_options=None,
        )

        assert result == 'task-id-123'
        broker._loop_runner.call.assert_called_once()
        call_args = broker._loop_runner.call.call_args
        # Bound methods are recreated on each access; verify by name
        coro_fn = call_args[0][0]
        assert getattr(coro_fn, '__name__', None) == 'enqueue_async'

    def test_get_result_delegates_to_loop_runner(self) -> None:
        """get_result() should call _loop_runner.call with get_result_async."""
        broker = _make_broker()
        sentinel: TaskResult[str, TaskError] = TaskResult(ok='hello')
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(return_value=sentinel)

        result = broker.get_result('task-id', timeout_ms=5000)

        assert result is sentinel
        broker._loop_runner.call.assert_called_once()
        call_args = broker._loop_runner.call.call_args
        coro_fn = call_args[0][0]
        assert getattr(coro_fn, '__name__', None) == 'get_result_async'

    def test_get_task_info_delegates_to_loop_runner(self) -> None:
        """get_task_info() should call _loop_runner.call with get_task_info_async."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(return_value=None)

        result = broker.get_task_info(
            'task-id', include_result=True, include_failed_reason=True,
        )

        assert result is None
        broker._loop_runner.call.assert_called_once()
        call_args = broker._loop_runner.call.call_args
        coro_fn = call_args[0][0]
        assert getattr(coro_fn, '__name__', None) == 'get_task_info_async'


# ---------------------------------------------------------------------------
# TestGetTaskInfoAsync
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetTaskInfoAsync:
    """Tests for get_task_info_async: field extraction, optional includes."""

    def _setup_task_info_session(
        self,
        broker: Any,
        row: tuple[Any, ...] | None,
    ) -> None:
        """Configure session to return a single row from execute().fetchone()."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = row

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=mock_result)
        broker.session_factory = MagicMock(return_value=session)

    @pytest.mark.asyncio
    async def test_task_not_found_returns_none(self) -> None:
        """When task row is None, should return None."""
        broker = _make_broker()
        self._setup_task_info_session(broker, row=None)

        result = await broker.get_task_info_async('missing-id')

        assert result is None

    @pytest.mark.asyncio
    async def test_basic_task_returns_task_info(self) -> None:
        """Basic task (no optional includes) should return TaskInfo with base fields."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 16 base columns: id, task_name, status, queue_name, priority,
        # retry_count, max_retries, next_retry_at, sent_at, claimed_at,
        # started_at, completed_at, failed_at, worker_hostname, worker_pid, worker_process_name
        row = (
            'task-abc', 'compute', 'RUNNING', 'default', 100,
            1, 3, None, now, None,
            now, None, None, 'host-1', 9999, 'worker-0',
        )
        self._setup_task_info_session(broker, row=row)

        info = await broker.get_task_info_async('task-abc')

        assert info is not None
        assert isinstance(info, TaskInfo)
        assert info.task_id == 'task-abc'
        assert info.task_name == 'compute'
        assert info.status == TaskStatus.RUNNING
        assert info.priority == 100
        assert info.retry_count == 1
        assert info.max_retries == 3
        assert info.worker_hostname == 'host-1'
        assert info.worker_pid == 9999
        assert info.result is None
        assert info.failed_reason is None

    @pytest.mark.asyncio
    async def test_include_result_deserializes_result(self) -> None:
        """include_result=True should add deserialized result to TaskInfo."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        result_json = '{"__task_result__":true,"ok":"hello","err":null}'
        # 16 base + 1 result column
        row = (
            'task-abc', 'compute', 'COMPLETED', 'default', 100,
            0, 0, None, now, None,
            now, now, None, 'host-1', 9999, 'worker-0',
            result_json,
        )
        self._setup_task_info_session(broker, row=row)

        info = await broker.get_task_info_async('task-abc', include_result=True)

        assert info is not None
        assert info.result is not None
        assert info.result.is_ok()
        assert info.result.unwrap() == 'hello'

    @pytest.mark.asyncio
    async def test_include_failed_reason_returns_reason(self) -> None:
        """include_failed_reason=True should add failed_reason to TaskInfo."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 16 base + 1 failed_reason column
        row = (
            'task-abc', 'compute', 'FAILED', 'default', 100,
            0, 0, None, now, None,
            now, None, now, 'host-1', 9999, 'worker-0',
            'Worker crashed unexpectedly',
        )
        self._setup_task_info_session(broker, row=row)

        info = await broker.get_task_info_async('task-abc', include_failed_reason=True)

        assert info is not None
        assert info.failed_reason == 'Worker crashed unexpectedly'

    @pytest.mark.asyncio
    async def test_include_result_null_stays_none(self) -> None:
        """include_result=True with NULL result in DB should keep result=None."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 16 base + 1 result column (None)
        row = (
            'task-abc', 'compute', 'RUNNING', 'default', 100,
            0, 0, None, now, None,
            now, None, None, None, None, None,
            None,
        )
        self._setup_task_info_session(broker, row=row)

        info = await broker.get_task_info_async('task-abc', include_result=True)

        assert info is not None
        assert info.result is None

    @pytest.mark.asyncio
    async def test_include_both_result_and_failed_reason(self) -> None:
        """Both include flags should add both columns."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        result_json = '{"__task_result__":true,"ok":null,"err":{"__task_error__":true,"error_code":"TASK_EXCEPTION","message":"fail","data":null}}'
        # 16 base + 1 result + 1 failed_reason
        row = (
            'task-abc', 'compute', 'FAILED', 'default', 100,
            0, 0, None, now, None,
            now, None, now, 'host-1', 9999, 'worker-0',
            result_json, 'Something broke',
        )
        self._setup_task_info_session(broker, row=row)

        info = await broker.get_task_info_async(
            'task-abc', include_result=True, include_failed_reason=True,
        )

        assert info is not None
        assert info.result is not None
        assert info.result.is_err()
        assert info.failed_reason == 'Something broke'


# ---------------------------------------------------------------------------
# TestCloseAsync / TestClose
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCloseAsync:
    """Tests for close_async: listener close then engine dispose."""

    @pytest.mark.asyncio
    async def test_close_async_closes_listener_then_disposes_engine(self) -> None:
        """close_async should close listener first, then dispose engine."""
        broker = _make_broker()
        call_order: list[str] = []

        async def mock_listener_close() -> None:
            call_order.append('listener_close')

        async def mock_engine_dispose() -> None:
            call_order.append('engine_dispose')

        broker.listener.close = mock_listener_close
        broker.async_engine.dispose = mock_engine_dispose

        await broker.close_async()

        assert call_order == ['listener_close', 'engine_dispose']


@pytest.mark.unit
class TestClose:
    """Tests for close (sync): delegates to close_async then stops loop_runner."""

    def test_close_calls_close_async_and_stops_runner(self) -> None:
        """close() should call close_async via loop_runner then stop it."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(return_value=None)
        broker._loop_runner.stop = MagicMock()

        broker.close()

        broker._loop_runner.call.assert_called_once_with(broker.close_async)
        broker._loop_runner.stop.assert_called_once()

    def test_close_stops_runner_even_on_error(self) -> None:
        """close() should stop loop_runner even when close_async raises."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(side_effect=RuntimeError('boom'))
        broker._loop_runner.stop = MagicMock()

        with pytest.raises(RuntimeError, match='boom'):
            broker.close()

        # stop is called in finally block
        broker._loop_runner.stop.assert_called_once()
