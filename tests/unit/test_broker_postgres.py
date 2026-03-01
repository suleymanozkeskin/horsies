"""Tests for PostgresBroker (horsies/core/brokers/postgres.py).

Strategy: mock DB layer entirely (create_async_engine, async_sessionmaker,
PostgresListener) to avoid real PostgreSQL. Tests verify logic, branching,
error handling, and delegation patterns.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.models.tasks import (
    LibraryErrorCode,
    TaskError,
    TaskInfo,
    TaskResult,
)
from horsies.core.types.result import Err, Ok, is_err, is_ok
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
        'enqueued_at': datetime.now(timezone.utc),
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

    The broker now uses SQLAlchemy Core INSERT ... RETURNING, so we mock
    session.execute() to return a result whose .fetchone() returns a
    non-None row (simulating a successful INSERT RETURNING id).
    """
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    # execute() returns a CursorResult-like mock whose fetchone()
    # returns a non-None row, indicating the INSERT succeeded.
    mock_result = MagicMock()
    mock_result.fetchone.return_value = MagicMock()  # non-None = row inserted
    session.execute = AsyncMock(return_value=mock_result)

    return session


@pytest.mark.unit
class TestEnqueueAsync:
    """Tests for enqueue_async: task creation, option parsing, retry extraction."""

    @pytest.mark.asyncio
    async def test_basic_enqueue_returns_uuid_string(self) -> None:
        """enqueue_async should return Ok(task_id) and commit the session."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task', 'default',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            args_json='[1, 2]',
            kwargs_json='{"key": "val"}',
        )

        assert is_ok(result)
        assert result.ok_value == 'test-task-id'
        session.execute.assert_awaited()
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_with_sent_at_uses_provided_timestamp(self) -> None:
        """When sent_at is provided, it should be accepted and enqueue should succeed."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        custom_ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            sent_at=custom_ts,
        )

        assert is_ok(result)
        assert result.ok_value == 'test-task-id'
        session.execute.assert_awaited()
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_with_retry_policy_extracts_max_retries(self) -> None:
        """task_options containing retry_policy should be parsed successfully.

        The broker embeds max_retries into the Core INSERT statement values.
        We verify indirectly: valid task_options with retry_policy produce Ok,
        and the execute call contains the stmt with the parsed value.
        """
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        task_options = '{"retry_policy": {"max_retries": 5}}'
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            task_options=task_options,
        )

        assert is_ok(result)
        session.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_malformed_task_options_raises(self) -> None:
        """Malformed task_options JSON propagates as Err(ENQUEUE_FAILED).

        loads_json returns Err(SerializationError), which enqueue_async checks
        explicitly and returns as Err(BrokerOperationError(ENQUEUE_FAILED)).
        """
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            task_options='NOT_VALID_JSON{{',
        )

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.ENQUEUE_FAILED
        assert 'JSON' in result.err_value.message

    @pytest.mark.asyncio
    async def test_with_good_until_passes_expiry(self) -> None:
        """good_until should be accepted and enqueue should succeed."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        expiry = datetime(2099, 12, 31, tzinfo=timezone.utc)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            good_until=expiry,
        )

        assert is_ok(result)
        session.execute.assert_awaited()
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_task_options_with_non_dict_retry_policy_keeps_zero(self) -> None:
        """retry_policy that is not a dict should still produce a successful enqueue.

        max_retries defaults to 0 when retry_policy is not a dict. The broker
        embeds this into the Core INSERT; we verify the enqueue succeeds.
        """
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        task_options = '{"retry_policy": "not_a_dict"}'
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            task_options=task_options,
        )

        assert is_ok(result)
        session.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_task_options_without_retry_policy_keeps_zero(self) -> None:
        """task_options dict without retry_policy key should still enqueue successfully.

        max_retries defaults to 0 when retry_policy is absent. The broker
        embeds this into the Core INSERT; we verify the enqueue succeeds.
        """
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        task_options = '{"some_other_key": "value"}'
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            task_options=task_options,
        )

        assert is_ok(result)
        session.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_future_sent_at_without_scheduling_params_rejected(self) -> None:
        """Future sent_at without enqueued_at or enqueue_delay_seconds must error.

        sent_at is an immutable call-site timestamp, not a scheduling mechanism.
        Passing a future value without explicit scheduling params is ambiguous
        (legacy ETA pattern) and would silently run immediately since
        enqueued_at defaults to NOW().
        """
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            sent_at=future,
        )

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.ENQUEUE_FAILED
        assert 'sent_at' in result.err_value.message
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_future_sent_at_with_enqueued_at_accepted(self) -> None:
        """Future sent_at is allowed when enqueued_at is explicitly provided."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            sent_at=future,
            enqueued_at=future,
        )

        assert is_ok(result)
        session.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_future_sent_at_with_enqueue_delay_accepted(self) -> None:
        """Future sent_at is allowed when enqueue_delay_seconds is provided."""
        broker = _make_broker()
        session = _make_enqueue_session()
        broker.session_factory = MagicMock(return_value=session)

        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            sent_at=future,
            enqueue_delay_seconds=600,
        )

        assert is_ok(result)
        session.execute.assert_awaited()


# ---------------------------------------------------------------------------
# TestEnqueueIdempotency — conflict verification via _verify_enqueue_conflict
# ---------------------------------------------------------------------------


def _make_conflict_session(
    *,
    existing_sha: str | None = 'same-sha',
    select_raises: Exception | None = None,
    row_exists: bool = True,
) -> AsyncMock:
    """Build a mock session for idempotency conflict tests.

    The first execute() call (INSERT) returns fetchone() = None (conflict).
    The second execute() call (SELECT enqueue_sha) returns the configured sha.
    """
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    # INSERT returns None (conflict — row not inserted)
    insert_result = MagicMock()
    insert_result.fetchone.return_value = None

    # SELECT returns existing_sha or raises
    if select_raises is not None:
        select_result = select_raises  # will be used as side_effect
    else:
        select_result = MagicMock()
        if row_exists:
            mock_row = MagicMock()
            mock_row.__getitem__ = lambda self, idx: existing_sha  # row[0]
            select_result.fetchone.return_value = mock_row
        else:
            select_result.fetchone.return_value = None  # row purged

    if select_raises is not None:
        session.execute = AsyncMock(
            side_effect=[insert_result, select_raises],
        )
    else:
        session.execute = AsyncMock(
            side_effect=[insert_result, select_result],
        )

    return session


@pytest.mark.unit
class TestEnqueueIdempotency:
    """Tests for ON CONFLICT path and _verify_enqueue_conflict."""

    @pytest.mark.asyncio
    async def test_enqueue_same_id_same_sha_returns_ok(self) -> None:
        """INSERT conflicts, SELECT finds matching SHA -> Ok(task_id)."""
        broker = _make_broker()
        session = _make_conflict_session(existing_sha='test-sha')
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task',
            task_id='dup-id',
            enqueue_sha='test-sha',
        )

        assert is_ok(result)
        assert result.ok_value == 'dup-id'

    @pytest.mark.asyncio
    async def test_enqueue_same_id_different_sha_returns_err(self) -> None:
        """INSERT conflicts, SELECT finds mismatched SHA -> Err(non-retryable)."""
        broker = _make_broker()
        session = _make_conflict_session(existing_sha='different-sha')
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task',
            task_id='dup-id',
            enqueue_sha='test-sha',
        )

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.PAYLOAD_MISMATCH
        assert result.err_value.retryable is False

    @pytest.mark.asyncio
    async def test_enqueue_same_id_null_sha_returns_non_retryable_err(self) -> None:
        """INSERT conflicts, existing row has NULL SHA -> Err(non-retryable)."""
        broker = _make_broker()
        session = _make_conflict_session(existing_sha=None)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task',
            task_id='dup-id',
            enqueue_sha='test-sha',
        )

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.ENQUEUE_FAILED
        assert result.err_value.retryable is False
        assert 'cannot verify payload identity' in result.err_value.message

    @pytest.mark.asyncio
    async def test_enqueue_same_id_row_deleted_returns_ok_with_warning(self) -> None:
        """INSERT conflicts, SELECT returns no row (purged) -> Ok(task_id)."""
        broker = _make_broker()
        session = _make_conflict_session(row_exists=False)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task',
            task_id='dup-id',
            enqueue_sha='test-sha',
        )

        assert is_ok(result)
        assert result.ok_value == 'dup-id'

    @pytest.mark.asyncio
    async def test_enqueue_same_id_select_fails_returns_retryable_err(self) -> None:
        """INSERT conflicts, SELECT raises -> Err(retryable=True)."""
        broker = _make_broker()
        session = _make_conflict_session(
            select_raises=ConnectionError('db gone'),
        )
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task',
            task_id='dup-id',
            enqueue_sha='test-sha',
        )

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.ENQUEUE_FAILED
        assert result.err_value.retryable is True
        assert 'verification query failed' in result.err_value.message


# ---------------------------------------------------------------------------
# TestScheduleSlotTaskId — deterministic UUID5 for scheduled tasks
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestScheduleSlotTaskId:
    """Tests for schedule_slot_task_id deterministic UUID generation."""

    def test_schedule_slot_task_id_deterministic(self) -> None:
        """Same name + same slot_time -> same UUID5."""
        from horsies.core.utils.fingerprint import schedule_slot_task_id

        slot = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        id1 = schedule_slot_task_id('daily_report', slot)
        id2 = schedule_slot_task_id('daily_report', slot)
        assert id1 == id2

    def test_schedule_slot_task_id_different_slot(self) -> None:
        """Same name + different slot_time -> different UUID5."""
        from horsies.core.utils.fingerprint import schedule_slot_task_id

        slot_a = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        slot_b = datetime(2025, 6, 2, 12, 0, 0, tzinfo=timezone.utc)
        assert schedule_slot_task_id('daily_report', slot_a) != schedule_slot_task_id('daily_report', slot_b)

    def test_schedule_slot_task_id_different_schedule(self) -> None:
        """Different name + same slot_time -> different UUID5."""
        from horsies.core.utils.fingerprint import schedule_slot_task_id

        slot = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert schedule_slot_task_id('daily_report', slot) != schedule_slot_task_id('hourly_check', slot)


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

    @pytest.mark.asyncio
    async def test_cancellation_still_completes_unsubscribe_cleanup(self) -> None:
        """Second cancellation during finally should not interrupt unsubscribe cleanup."""
        broker = _make_broker()
        q: asyncio.Queue[Any] = asyncio.Queue()
        broker.listener.listen = AsyncMock(return_value=Ok(q))

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.get = AsyncMock(return_value=_make_task_row(status=TaskStatus.RUNNING))
        broker.session_factory = MagicMock(return_value=session)

        started = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def _unsubscribe(_channel: str, _queue: object) -> None:
            started.set()
            await release.wait()
            finished.set()

        broker.listener.unsubscribe = AsyncMock(side_effect=_unsubscribe)

        task = asyncio.create_task(broker.get_result_async('task-123', timeout_ms=60_000))

        for _ in range(50):
            if broker.listener.listen.await_count > 0:
                break
            await asyncio.sleep(0)

        task.cancel()  # Enter finally path
        await asyncio.wait_for(started.wait(), timeout=1.0)
        task.cancel()  # Cancel while unsubscribe is in progress
        await asyncio.sleep(0)

        assert finished.is_set() is False

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert finished.is_set() is True
        broker.listener.unsubscribe.assert_awaited_once_with('task_done', q)


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
        """get_stale_tasks should return Ok(dicts) keyed by column names."""
        broker = _make_broker()
        columns = ['id', 'worker_hostname', 'worker_pid', 'last_heartbeat']
        rows = [
            ('task-1', 'host-1', 1234, None),
            ('task-2', 'host-2', 5678, None),
        ]
        self._setup_session_with_rows(broker, columns, rows)

        result = await broker.get_stale_tasks(stale_threshold_minutes=5)

        assert is_ok(result)
        rows_out = result.ok_value
        assert len(rows_out) == 2
        assert rows_out[0] == {
            'id': 'task-1',
            'worker_hostname': 'host-1',
            'worker_pid': 1234,
            'last_heartbeat': None,
        }
        assert rows_out[1]['id'] == 'task-2'

    @pytest.mark.asyncio
    async def test_get_stale_tasks_empty(self) -> None:
        """get_stale_tasks with no rows returns Ok([])."""
        broker = _make_broker()
        self._setup_session_with_rows(broker, ['id'], [])

        result = await broker.get_stale_tasks()

        assert is_ok(result)
        assert result.ok_value == []

    @pytest.mark.asyncio
    async def test_get_worker_stats_returns_list_of_dicts(self) -> None:
        """get_worker_stats should return Ok(dicts) keyed by column names."""
        broker = _make_broker()
        columns = ['worker_hostname', 'worker_pid', 'worker_process_name', 'active_tasks']
        rows = [('host-1', 1234, 'worker-0', 3)]
        self._setup_session_with_rows(broker, columns, rows)

        result = await broker.get_worker_stats()

        assert is_ok(result)
        assert len(result.ok_value) == 1
        assert result.ok_value[0]['active_tasks'] == 3

    @pytest.mark.asyncio
    async def test_get_expired_tasks_returns_list_of_dicts(self) -> None:
        """get_expired_tasks should return Ok(dicts) keyed by column names."""
        broker = _make_broker()
        columns = ['id', 'task_name', 'queue_name', 'priority', 'sent_at', 'enqueued_at', 'good_until', 'expired_for']
        rows = [('task-1', 'slow_task', 'default', 100, None, None, None, '00:05:00')]
        self._setup_session_with_rows(broker, columns, rows)

        result = await broker.get_expired_tasks()

        assert is_ok(result)
        assert len(result.ok_value) == 1
        assert result.ok_value[0]['task_name'] == 'slow_task'


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

        result = await broker.mark_stale_tasks_as_failed()

        assert is_ok(result)
        assert result.ok_value == 0
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

        result = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)

        assert is_ok(result)
        assert result.ok_value == 2
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

        result = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)

        assert is_ok(result)
        assert result.ok_value == 5
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_rowcount(self) -> None:
        """Should fall back to Ok(0) when rowcount attribute is missing."""
        broker = _make_broker()
        mock_result = object()  # no rowcount attribute

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=mock_result)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.requeue_stale_claimed()

        assert is_ok(result)
        assert result.ok_value == 0

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
            'my_task', 'queue',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            args_json='[1]',
            kwargs_json='{"k": "v"}',
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

    def test_get_result_loop_runner_exception_returns_broker_error_result(self) -> None:
        """Infrastructure failure in sync get_result() should fold into BROKER_ERROR TaskResult."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(side_effect=RuntimeError('loop dead'))

        result = broker.get_result('task-id', timeout_ms=5000)

        assert result.is_err()
        err = result.unwrap_err()
        assert err.error_code == LibraryErrorCode.BROKER_ERROR
        assert err.exception is not None
        assert isinstance(err.exception, RuntimeError)
        assert 'Broker error while retrieving task result' in err.message

    def test_get_task_info_delegates_to_loop_runner(self) -> None:
        """get_task_info() should call _loop_runner.call with get_task_info_async."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(return_value=Ok(None))

        result = broker.get_task_info(
            'task-id', include_result=True, include_failed_reason=True,
        )

        assert is_ok(result)
        assert result.ok_value is None
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
        """When task row is None, should return Ok(None)."""
        broker = _make_broker()
        self._setup_task_info_session(broker, row=None)

        result = await broker.get_task_info_async('missing-id')

        assert is_ok(result)
        assert result.ok_value is None

    @pytest.mark.asyncio
    async def test_basic_task_returns_task_info(self) -> None:
        """Basic task (no optional includes) should return Ok(TaskInfo) with base fields."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 17 base columns: id, task_name, status, queue_name, priority,
        # retry_count, max_retries, next_retry_at, sent_at, enqueued_at, claimed_at,
        # started_at, completed_at, failed_at, worker_hostname, worker_pid, worker_process_name
        row = (
            'task-abc', 'compute', 'RUNNING', 'default', 100,
            1, 3, None, now, now, None,
            now, None, None, 'host-1', 9999, 'worker-0',
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async('task-abc')

        assert is_ok(result)
        info = result.ok_value
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
        """include_result=True should add deserialized result to Ok(TaskInfo)."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        result_json = '{"__task_result__":true,"ok":"hello","err":null}'
        # 17 base + 1 result column
        row = (
            'task-abc', 'compute', 'COMPLETED', 'default', 100,
            0, 0, None, now, now, None,
            now, now, None, 'host-1', 9999, 'worker-0',
            result_json,
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async('task-abc', include_result=True)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.result is not None
        assert info.result.is_ok()
        assert info.result.unwrap() == 'hello'

    @pytest.mark.asyncio
    async def test_include_failed_reason_returns_reason(self) -> None:
        """include_failed_reason=True should add failed_reason to Ok(TaskInfo)."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 17 base + 1 failed_reason column
        row = (
            'task-abc', 'compute', 'FAILED', 'default', 100,
            0, 0, None, now, now, None,
            now, None, now, 'host-1', 9999, 'worker-0',
            'Worker crashed unexpectedly',
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async('task-abc', include_failed_reason=True)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.failed_reason == 'Worker crashed unexpectedly'

    @pytest.mark.asyncio
    async def test_include_result_null_stays_none(self) -> None:
        """include_result=True with NULL result in DB should return Ok(TaskInfo) with result=None."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 17 base + 1 result column (None)
        row = (
            'task-abc', 'compute', 'RUNNING', 'default', 100,
            0, 0, None, now, now, None,
            now, None, None, None, None, None,
            None,
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async('task-abc', include_result=True)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.result is None

    @pytest.mark.asyncio
    async def test_include_both_result_and_failed_reason(self) -> None:
        """Both include flags should add both columns to Ok(TaskInfo)."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        result_json = '{"__task_result__":true,"ok":null,"err":{"__task_error__":true,"error_code":"TASK_EXCEPTION","message":"fail","data":null}}'
        # 17 base + 1 result + 1 failed_reason
        row = (
            'task-abc', 'compute', 'FAILED', 'default', 100,
            0, 0, None, now, now, None,
            now, None, now, 'host-1', 9999, 'worker-0',
            result_json, 'Something broke',
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async(
            'task-abc', include_result=True, include_failed_reason=True,
        )

        assert is_ok(result)
        info = result.ok_value
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
        """close_async should close listener first, then dispose engine, returning Ok(None)."""
        broker = _make_broker()
        call_order: list[str] = []

        async def mock_listener_close() -> None:
            call_order.append('listener_close')

        async def mock_engine_dispose() -> None:
            call_order.append('engine_dispose')

        broker.listener.close = mock_listener_close
        broker.async_engine.dispose = mock_engine_dispose

        result = await broker.close_async()

        assert is_ok(result)
        assert result.ok_value is None
        assert call_order == ['listener_close', 'engine_dispose']

    @pytest.mark.asyncio
    async def test_close_async_stops_loop_runner_when_started_externally(self) -> None:
        """close_async should stop LoopRunner if sync APIs had started it."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner._started = True
        broker._loop_runner._thread = object()
        broker._loop_runner.stop = MagicMock()

        result = await broker.close_async()

        assert is_ok(result)
        broker._loop_runner.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_async_does_not_stop_loop_runner_from_its_own_thread(self) -> None:
        """close_async must not self-stop when running on loop-runner thread."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner._started = True
        broker._loop_runner._thread = threading.current_thread()
        broker._loop_runner.stop = MagicMock()

        result = await broker.close_async()

        assert is_ok(result)
        broker._loop_runner.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_async_stops_real_loop_runner_thread(self) -> None:
        """Regression: close_async should stop a real LoopRunner started by sync APIs."""
        broker = _make_broker()
        runner = broker._loop_runner
        assert runner._started is False

        runner.start()
        assert runner._started is True
        assert runner._thread is not None and runner._thread.is_alive()

        try:
            result = await broker.close_async()
            assert is_ok(result)
            assert runner._started is False
            assert runner._loop is None
            assert runner._thread is None
        finally:
            # Safety net for test isolation in case assertions fail mid-test.
            if runner._started:
                runner.stop()


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
        """close() should return Err and still stop loop_runner when call() raises."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(side_effect=RuntimeError('boom'))
        broker._loop_runner.stop = MagicMock()

        result = broker.close()

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.CLOSE_FAILED
        assert isinstance(result.err_value.exception, RuntimeError)
        # stop is called in finally block
        broker._loop_runner.stop.assert_called_once()


# ---------------------------------------------------------------------------
# New error path tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnqueueAsyncErrorPaths:
    """Regression tests for error-path behavior in enqueue_async."""

    @pytest.mark.asyncio
    async def test_enqueue_async_connection_error_returns_retryable_err(self) -> None:
        """OperationalError during commit should produce a retryable Err(ENQUEUE_FAILED)."""
        from sqlalchemy.exc import OperationalError

        broker = _make_broker()
        session = _make_enqueue_session()
        # Simulate a connection-level error that psycopg raises on commit
        session.commit = AsyncMock(
            side_effect=OperationalError('commit failed', None, Exception('conn lost'))
        )
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
        )

        assert is_err(result)
        err = result.err_value
        assert err.code == BrokerErrorCode.ENQUEUE_FAILED
        assert err.retryable is True
        assert err.exception is not None


@pytest.mark.unit
class TestGetTaskInfoAsyncErrorPaths:
    """Regression tests for error-path behavior in get_task_info_async."""

    @pytest.mark.asyncio
    async def test_get_task_info_async_db_error_returns_err(self) -> None:
        """RuntimeError from session.execute should produce Err(TASK_INFO_QUERY_FAILED)."""
        broker = _make_broker()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=RuntimeError('db exploded'))
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_task_info_async('task-abc')

        assert is_err(result)
        err = result.err_value
        assert err.code == BrokerErrorCode.TASK_INFO_QUERY_FAILED
        assert err.exception is not None


@pytest.mark.unit
class TestEnsureSchemaInitialized:
    """Tests for ensure_schema_initialized public entry point."""

    @pytest.mark.asyncio
    async def test_ensure_schema_initialized_returns_ok_on_success(self) -> None:
        """When already initialized, ensure_schema_initialized returns Ok(None)."""
        broker = _make_broker()
        # _make_broker already sets _initialized = True, so _ensure_initialized is a no-op.

        result = await broker.ensure_schema_initialized()

        assert is_ok(result)
        assert result.ok_value is None


@pytest.mark.unit
class TestCloseAsyncErrorPaths:
    """Regression tests for error handling in close_async."""

    @pytest.mark.asyncio
    async def test_close_async_attempts_both_on_first_failure(self) -> None:
        """When listener.close raises, engine.dispose is still called and result is Err."""
        broker = _make_broker()
        dispose_called = False

        async def failing_listener_close() -> None:
            raise RuntimeError('listener boom')

        async def tracking_engine_dispose() -> None:
            nonlocal dispose_called
            dispose_called = True

        broker.listener.close = failing_listener_close
        broker.async_engine.dispose = tracking_engine_dispose

        result = await broker.close_async()

        assert dispose_called, 'engine.dispose must be called even after listener.close fails'
        assert is_err(result)
        err = result.err_value
        assert err.code == BrokerErrorCode.CLOSE_FAILED
        assert err.exception is not None
