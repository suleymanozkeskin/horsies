"""Tests for PostgresBroker (horsies/core/brokers/postgres.py).

Strategy: mock DB layer entirely (create_async_engine, async_sessionmaker,
PostgresListener) to avoid real PostgreSQL. Tests verify logic, branching,
error handling, and delegation patterns.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.brokers.result_types import BrokerErrorCode
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
    TaskError,
    TaskInfo,
    TaskResult,
)
from horsies.core.lifecycle.commands import FailStaleTask
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    Applied,
    ObservedStaleness,
    ObservedTaskState,
    SourceStateConflict,
)
from horsies.core.types.result import Ok, is_err, is_ok
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


def _stale_applied(task_id: str, terminal_at: datetime) -> Applied:
    return Applied(
        task_id=task_id,
        ordinality=None,
        terminal_at=terminal_at,
        kind=TerminalizationKind.FAIL_STALE,
        observed=ObservedTaskState(
            status=TaskStatus.RUNNING,
            worker_id='worker-1',
            claimed_at=None,
        ),
    )


def _make_result_session(row: Any) -> AsyncMock:
    """Mock session for get_raw_result_record_async.

    The broker polls a slim status/name SELECT first and, once a terminal
    status is observed, fetches only the columns RawResultRecord consumes
    (task_name, status, result) — never the full entity, whose args/kwargs
    payload columns can be multi-MB. ``session.get`` is a tripwire: the
    result path regressing to a full-entity load fails loudly here.
    """
    from horsies.core.brokers.postgres import GET_TASK_RESULT_RECORD_SQL

    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    if row is None:
        probe_row = None
        record_row = None
    else:
        probe_row = MagicMock()
        status = row.status
        probe_row.status = (
            status.value if isinstance(status, TaskStatus) else status
        )
        probe_row.task_name = row.task_name
        record_row = MagicMock()
        record_row.status = probe_row.status
        record_row.task_name = row.task_name
        record_row.result = row.result

    async def _execute(statement: Any, *args: Any, **kwargs: Any) -> Any:
        result = MagicMock()
        if statement is GET_TASK_RESULT_RECORD_SQL:
            result.fetchone = MagicMock(return_value=record_row)
        else:
            result.fetchone = MagicMock(return_value=probe_row)
        return result

    session.execute = AsyncMock(side_effect=_execute)
    session.get = AsyncMock(
        side_effect=AssertionError(
            'result path must not full-entity load TaskModel; '
            'use GET_TASK_RESULT_RECORD_SQL'
        ),
    )
    return session


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
    """Tests for schema advisory key behavior."""

    def test_returns_stable_int_from_database_url(self) -> None:
        """Same URL should always produce the same advisory key."""
        broker = _make_broker('postgresql+psycopg://u:p@host1/db')

        key1 = broker._schema_advisory_key()
        key2 = broker._schema_advisory_key()

        assert isinstance(key1, int)
        assert key1 == key2

    def test_schema_key_is_url_independent(self) -> None:
        """Different URLs should share the new schema advisory key."""
        broker_a = _make_broker('postgresql+psycopg://u:p@host_a/db')
        broker_b = _make_broker('postgresql+psycopg://u:p@host_b/db')

        key_a = broker_a._schema_advisory_key()
        key_b = broker_b._schema_advisory_key()

        assert key_a == key_b

    def test_legacy_schema_key_remains_url_derived(self) -> None:
        """The transitional legacy lock still protects old rolling-deploy peers."""
        broker_a = _make_broker('postgresql+psycopg://u:p@host_a/db')
        broker_b = _make_broker('postgresql+psycopg://u:p@host_b/db')

        assert (
            broker_a._legacy_schema_advisory_key()
            != broker_b._legacy_schema_advisory_key()
        )

    def test_legacy_schema_keys_include_runtime_and_session_urls(self) -> None:
        """Split URL deploys acquire both old URL-derived schema locks."""
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        database_url = 'postgresql+psycopg://u:p@pooler:6432/db'
        session_url = 'postgresql+psycopg://u:p@direct:5432/db'
        broker = PostgresBroker(
            PostgresConfig(
                database_url=database_url,
                session_database_url=session_url,
                pgbouncer_transaction_mode=True,
            )
        )

        assert broker._legacy_schema_advisory_keys() == tuple(
            sorted(
                {
                    broker._legacy_schema_advisory_key_for_url(database_url),
                    broker._legacy_schema_advisory_key_for_url(session_url),
                }
            )
        )

    def test_key_is_signed_64_bit(self) -> None:
        """Advisory key must fit in a signed 64-bit range for pg_advisory_xact_lock."""
        broker = _make_broker()

        key = broker._schema_advisory_key()

        assert -(2**63) <= key <= 2**63 - 1


@pytest.mark.unit
class TestPostgresBrokerPgBouncerWiring:
    """Tests for split URL and PgBouncer-specific broker wiring."""

    def test_runtime_engine_uses_database_url_and_listener_uses_session_url(
        self,
    ) -> None:
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        database_url = 'postgresql+psycopg://u:p@pooler:6432/db'
        session_url = 'postgresql+psycopg://u:p@direct:5432/db'

        with (
            patch('horsies.core.brokers.postgres.create_async_engine') as mock_engine,
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch(
                'horsies.core.brokers.postgres.PostgresListener'
            ) as mock_listener_cls,
        ):
            mock_engine.return_value = MagicMock()

            config = PostgresConfig(
                database_url=database_url,
                session_database_url=session_url,
                pgbouncer_transaction_mode=True,
            )
            PostgresBroker(config)

        mock_engine.assert_called_once()
        assert mock_engine.call_args.args[0] == database_url
        assert mock_engine.call_args.kwargs['connect_args'] == {
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 3,
            'prepare_threshold': None,
        }
        mock_listener_cls.assert_called_once_with('postgresql://u:p@direct:5432/db')

    @pytest.mark.asyncio
    async def test_schema_initialization_uses_session_url_when_split(self) -> None:
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        runtime_engine = MagicMock()
        schema_engine = MagicMock()
        schema_engine.dispose = AsyncMock()
        conn = AsyncMock()
        conn.run_sync = AsyncMock()
        version_result = MagicMock()
        version_result.scalar_one.return_value = 0
        conn.execute.return_value = version_result
        begin_ctx = MagicMock()
        begin_ctx.__aenter__ = AsyncMock(return_value=conn)
        begin_ctx.__aexit__ = AsyncMock(return_value=None)
        schema_engine.begin.return_value = begin_ctx

        database_url = 'postgresql+psycopg://u:p@pooler:6432/db'
        session_url = 'postgresql+psycopg://u:p@direct:5432/db'

        with (
            patch(
                'horsies.core.brokers.postgres.create_async_engine',
                side_effect=[runtime_engine, schema_engine],
            ) as mock_engine,
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch('horsies.core.brokers.postgres.PostgresListener'),
        ):
            config = PostgresConfig(
                database_url=database_url,
                session_database_url=session_url,
                pgbouncer_transaction_mode=True,
            )
            broker = PostgresBroker(config)
            await broker._ensure_initialized()

        assert mock_engine.call_args_list[0].args[0] == database_url
        assert mock_engine.call_args_list[1].args[0] == session_url
        schema_engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_schema_initialization_acquires_legacy_then_constant_lock(
        self,
    ) -> None:
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        engine = MagicMock()
        conn = AsyncMock()
        conn.run_sync = AsyncMock()
        version_result = MagicMock()
        version_result.scalar_one.return_value = 0
        conn.execute.return_value = version_result
        begin_ctx = MagicMock()
        begin_ctx.__aenter__ = AsyncMock(return_value=conn)
        begin_ctx.__aexit__ = AsyncMock(return_value=None)
        engine.begin.return_value = begin_ctx

        with (
            patch(
                'horsies.core.brokers.postgres.create_async_engine', return_value=engine
            ),
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch('horsies.core.brokers.postgres.PostgresListener'),
        ):
            config = PostgresConfig(
                database_url='postgresql+psycopg://u:p@localhost/db',
            )
            broker = PostgresBroker(config)
            await broker._ensure_initialized()

        from horsies.core.schemas.migrations import SCHEMA_ADVISORY_LOCK_SQL

        lock_calls = [
            call for call in conn.execute.await_args_list
            if call.args[0] is SCHEMA_ADVISORY_LOCK_SQL
        ]
        first_params = lock_calls[0].args[1]
        second_params = lock_calls[1].args[1]
        assert first_params == {'key': broker._legacy_schema_advisory_key()}
        assert second_params == {'key': broker._schema_advisory_key()}

    @pytest.mark.asyncio
    async def test_schema_initialization_acquires_split_legacy_locks_before_constant(
        self,
    ) -> None:
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        runtime_engine = MagicMock()
        schema_engine = MagicMock()
        schema_engine.dispose = AsyncMock()
        conn = AsyncMock()
        conn.run_sync = AsyncMock()
        version_result = MagicMock()
        version_result.scalar_one.return_value = 0
        conn.execute.return_value = version_result
        begin_ctx = MagicMock()
        begin_ctx.__aenter__ = AsyncMock(return_value=conn)
        begin_ctx.__aexit__ = AsyncMock(return_value=None)
        schema_engine.begin.return_value = begin_ctx

        with (
            patch(
                'horsies.core.brokers.postgres.create_async_engine',
                side_effect=[runtime_engine, schema_engine],
            ),
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch('horsies.core.brokers.postgres.PostgresListener'),
        ):
            config = PostgresConfig(
                database_url='postgresql+psycopg://u:p@pooler:6432/db',
                session_database_url='postgresql+psycopg://u:p@direct:5432/db',
                pgbouncer_transaction_mode=True,
            )
            broker = PostgresBroker(config)
            await broker._ensure_initialized()

        from horsies.core.schemas.migrations import SCHEMA_ADVISORY_LOCK_SQL

        lock_params = [
            call.args[1]
            for call in conn.execute.await_args_list
            if call.args[0] is SCHEMA_ADVISORY_LOCK_SQL
        ]
        expected_legacy_keys = broker._legacy_schema_advisory_keys()
        assert lock_params == [
            {'key': expected_legacy_keys[0]},
            {'key': expected_legacy_keys[1]},
            {'key': broker._schema_advisory_key()},
        ]

    def test_assume_initialized_skips_listener_creation(self) -> None:
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        with (
            patch('horsies.core.brokers.postgres.create_async_engine'),
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch(
                'horsies.core.brokers.postgres.PostgresListener'
            ) as mock_listener_cls,
        ):
            broker = PostgresBroker(
                PostgresConfig(database_url='postgresql+psycopg://u:p@localhost/db'),
                assume_initialized=True,
            )

        assert broker._initialized is True
        assert broker._listener is None
        with pytest.raises(RuntimeError, match='listener is disabled'):
            _ = broker.listener
        mock_listener_cls.assert_not_called()


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
            'my_task',
            'default',
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
            mock_row.enqueue_sha = existing_sha
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
    async def test_enqueue_same_id_null_sha_raises_assertion(self) -> None:
        """INSERT conflicts, existing row has NULL SHA -> AssertionError (data corruption).

        enqueue_sha is NOT NULL in the schema. A NULL value from the DB
        indicates data corruption, so the assertion is the expected outcome.
        """
        broker = _make_broker()
        session = _make_conflict_session(existing_sha=None)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.enqueue_async(
            'my_task',
            task_id='dup-id',
            enqueue_sha='test-sha',
        )

        # The assertion fires inside _verify_enqueue_conflict, which is
        # caught by the outer try/except in enqueue_async -> Err(ENQUEUE_FAILED).
        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.ENQUEUE_FAILED
        assert result.err_value.retryable is False

    @pytest.mark.asyncio
    async def test_enqueue_same_id_row_deleted_returns_non_retryable_err(self) -> None:
        """INSERT conflicts, SELECT returns no row -> cannot verify payload identity."""
        broker = _make_broker()
        session = _make_conflict_session(row_exists=False)
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
        assert schedule_slot_task_id('daily_report', slot_a) != schedule_slot_task_id(
            'daily_report', slot_b
        )

    def test_schedule_slot_task_id_different_schedule(self) -> None:
        """Different name + same slot_time -> different UUID5."""
        from horsies.core.utils.fingerprint import schedule_slot_task_id

        slot = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert schedule_slot_task_id('daily_report', slot) != schedule_slot_task_id(
            'hourly_check', slot
        )


# ---------------------------------------------------------------------------
# (Removed) TestGetResultAsync
#
# Strict-serde phase 6 dropped ``broker.get_result(_async)`` — the broker
# no longer typed-decodes results. Coverage of the typed-decode semantics
# moved to ``tests/unit/test_app_get_result.py`` (Horsies layer); the
# raw-fetch broker primitive is covered in ``TestGetRawResultRecordAsync``
# below.
# ---------------------------------------------------------------------------


# (TestGetResultAsync body removed — see header note above.)



# ---------------------------------------------------------------------------
# TestGetRawResultRecordAsync — strict-serde phase 6 broker primitive
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetRawResultRecordAsync:
    """Cover ``PostgresBroker.get_raw_result_record_async`` directly.

    Replaces the removed ``get_result_async`` semantics at the broker
    layer. Typed-decode coverage lives at the ``Horsies.get_result_async``
    layer; broker tests only verify the raw envelope-fetch contract.
    """

    @pytest.mark.asyncio
    async def test_missing_row_returns_ok_none(self) -> None:
        """Row absent → Ok(None); no INVALID_JSON_PAYLOAD."""
        broker = _make_broker()
        session = _make_result_session(None)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_raw_result_record_async('missing-id')

        assert is_ok(result)
        assert result.unwrap() is None

    @pytest.mark.asyncio
    async def test_terminal_row_with_valid_envelope_returns_record(self) -> None:
        """COMPLETED row with parseable envelope → Ok(RawResultRecord)."""
        from horsies.core.brokers.result_types import RawResultRecord

        broker = _make_broker()
        row = _make_task_row(
            status=TaskStatus.COMPLETED,
            result='{"__h_task_result__":true,"ok":42,"err":null}',
        )
        session = _make_result_session(row)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_raw_result_record_async('task-123')

        assert is_ok(result)
        record = result.unwrap()
        assert isinstance(record, RawResultRecord)
        assert record.task_id == 'task-123'
        assert record.task_name == 'my_task'
        assert record.status == TaskStatus.COMPLETED
        assert record.raw_result == {
            '__h_task_result__': True,
            'ok': 42,
            'err': None,
        }

    @pytest.mark.asyncio
    async def test_cancelled_row_returns_record_with_none_payload(self) -> None:
        """CANCELLED row → Ok(RawResultRecord(raw_result=None))."""
        from horsies.core.brokers.result_types import RawResultRecord

        broker = _make_broker()
        row = _make_task_row(status=TaskStatus.CANCELLED, result=None)
        session = _make_result_session(row)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_raw_result_record_async('task-123')

        assert is_ok(result)
        record = result.unwrap()
        assert isinstance(record, RawResultRecord)
        assert record.status == TaskStatus.CANCELLED
        assert record.raw_result is None

    @pytest.mark.asyncio
    async def test_non_terminal_timeout_returns_record_with_none_payload(
        self,
    ) -> None:
        """Timeout fires with row still non-terminal → Ok(record, raw=None).

        Caller maps this to WAIT_TIMEOUT; broker layer stays agnostic.
        """
        from horsies.core.brokers.result_types import RawResultRecord

        broker = _make_broker()
        broker.listener.listen = AsyncMock(
            side_effect=RuntimeError('listener down; force polling'),
        )
        running_row = _make_task_row(status=TaskStatus.RUNNING, result=None)
        session = _make_result_session(running_row)
        broker.session_factory = MagicMock(return_value=session)

        with patch(
            'horsies.core.brokers.postgres.asyncio.sleep', new=AsyncMock(),
        ):
            result = await broker.get_raw_result_record_async(
                'task-123', timeout_ms=10,
            )

        assert is_ok(result)
        record = result.unwrap()
        assert isinstance(record, RawResultRecord)
        assert record.status == TaskStatus.RUNNING
        assert record.raw_result is None

    @pytest.mark.asyncio
    async def test_malformed_json_returns_invalid_json_payload(self) -> None:
        """Result column with malformed JSON → Err(INVALID_JSON_PAYLOAD)."""
        broker = _make_broker()
        row = _make_task_row(
            status=TaskStatus.COMPLETED,
            result='{not valid json',
        )
        session = _make_result_session(row)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_raw_result_record_async('task-123')

        assert is_err(result)
        err = result.unwrap_err()
        assert err.code == BrokerErrorCode.INVALID_JSON_PAYLOAD
        assert err.retryable is False
        assert 'task-123' in err.message

    @pytest.mark.asyncio
    async def test_non_object_envelope_returns_invalid_json_payload(self) -> None:
        """Result column parses to a non-object JSON value → Err(INVALID_JSON_PAYLOAD).

        Envelope grammar requires a JSON object at the top level; a bare
        scalar (e.g. ``42``) is a contract violation distinct from a
        parse failure.
        """
        broker = _make_broker()
        row = _make_task_row(status=TaskStatus.COMPLETED, result='42')
        session = _make_result_session(row)
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.get_raw_result_record_async('task-123')

        assert is_err(result)
        err = result.unwrap_err()
        assert err.code == BrokerErrorCode.INVALID_JSON_PAYLOAD
        assert 'not a JSON object' in err.message

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self) -> None:
        """``asyncio.CancelledError`` must re-raise; no broker-error wrap."""
        broker = _make_broker()
        session = _make_result_session(None)
        session.execute = AsyncMock(side_effect=asyncio.CancelledError)
        broker.session_factory = MagicMock(return_value=session)

        with pytest.raises(asyncio.CancelledError):
            await broker.get_raw_result_record_async('task-123')

    @pytest.mark.asyncio
    async def test_cancellation_still_completes_unsubscribe_cleanup(self) -> None:
        """Cancellation while unsubscribe is in-flight must still cleanup.

        Regression test for the listener finally-path: a second cancel
        while ``listener.unsubscribe`` is awaiting should not interrupt
        the cleanup before it finishes.
        """
        broker = _make_broker()
        q: asyncio.Queue[Any] = asyncio.Queue()
        broker.listener.listen_payload = AsyncMock(return_value=Ok(q))

        session = _make_result_session(
            _make_task_row(status=TaskStatus.RUNNING),
        )
        broker.session_factory = MagicMock(return_value=session)

        started = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def _unsubscribe(
            _channel: str, _payload: str, _queue: object,
        ) -> None:
            started.set()
            await release.wait()
            finished.set()

        broker.listener.unsubscribe_payload = AsyncMock(
            side_effect=_unsubscribe,
        )

        task = asyncio.create_task(
            broker.get_raw_result_record_async(
                'task-123', timeout_ms=60_000,
            )
        )

        for _ in range(50):
            if broker.listener.listen_payload.await_count > 0:
                break
            await asyncio.sleep(0)

        task.cancel()
        await asyncio.wait_for(started.wait(), timeout=1.0)
        task.cancel()
        await asyncio.sleep(0)

        assert finished.is_set() is False

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert finished.is_set() is True
        broker.listener.unsubscribe_payload.assert_awaited_once_with(
            'task_done', 'task-123', q,
        )


# ---------------------------------------------------------------------------
# TestMonitoringQueries
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMonitoringQueries:
    """Tests for get_stale_tasks, get_expired_tasks."""

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
    async def test_get_expired_tasks_returns_list_of_dicts(self) -> None:
        """get_expired_tasks should return Ok(dicts) keyed by column names."""
        broker = _make_broker()
        columns = [
            'id',
            'task_name',
            'queue_name',
            'priority',
            'sent_at',
            'enqueued_at',
            'good_until',
            'expired_for',
        ]
        rows = [('task-1', 'slow_task', 'default', 100, None, None, None, '00:05:00')]
        self._setup_session_with_rows(broker, columns, rows)

        result = await broker.get_expired_tasks()

        assert is_ok(result)
        assert len(result.ok_value) == 1
        assert result.ok_value[0]['task_name'] == 'slow_task'


# ---------------------------------------------------------------------------
# TestHealthApi
# ---------------------------------------------------------------------------


def _worker_state_row(**overrides: Any) -> Any:
    """Build a fake worker-states row exposing ``_mapping`` like SQLAlchemy."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    mapping: dict[str, Any] = {
        'worker_id': 'worker-1',
        'snapshot_at': now,
        'hostname': 'host-1',
        'pid': 1234,
        'processes': 4,
        'max_claim_batch': 10,
        'max_claim_per_worker': 4,
        'cluster_wide_cap': None,
        'queues': ['default'],
        'queue_priorities': None,
        'queue_max_concurrency': None,
        'recovery_config': None,
        'tasks_running': 2,
        'tasks_claimed': 1,
        'memory_usage_mb': 12.5,
        'memory_percent': 0.5,
        'cpu_percent': 3.0,
        'children_memory_mb': 250.0,
        'worker_started_at': now,
    }
    mapping.update(overrides)
    return SimpleNamespace(_mapping=mapping)


def _setup_session_returning(
    broker: Any,
    *,
    fetchall: list[Any] | None = None,
    fetchone: Any = None,
) -> AsyncMock:
    """Configure broker.session_factory to return a result with given rows."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = fetchall if fetchall is not None else []
    mock_result.fetchone.return_value = fetchone

    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.execute = AsyncMock(return_value=mock_result)
    session.commit = AsyncMock()
    broker.session_factory = MagicMock(return_value=session)
    return session


@pytest.mark.unit
class TestPingDatabase:
    """ping_database_async: latency on success, typed error on failure."""

    @pytest.mark.asyncio
    async def test_returns_latency_on_success(self) -> None:
        broker = _make_broker()
        _setup_session_returning(broker)

        result = await broker.ping_database_async()

        assert is_ok(result)
        assert result.ok_value.latency_ms >= 0.0

    @pytest.mark.asyncio
    async def test_returns_db_ping_failed_on_error(self) -> None:
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=RuntimeError('connection refused'))
        broker.session_factory = MagicMock(return_value=session)

        result = await broker.ping_database_async()

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.DB_PING_FAILED


@pytest.mark.unit
class TestWorkerStateReads:
    """list/get worker-state reads map rows to typed snapshots."""

    @pytest.mark.asyncio
    async def test_list_maps_rows_to_snapshots(self) -> None:
        broker = _make_broker()
        _setup_session_returning(
            broker,
            fetchall=[
                _worker_state_row(worker_id='w1'),
                _worker_state_row(worker_id='w2'),
            ],
        )

        result = await broker.list_worker_states_async()

        assert is_ok(result)
        snaps = result.ok_value
        assert [s.worker_id for s in snaps] == ['w1', 'w2']
        assert snaps[0].tasks_running == 2
        assert snaps[0].queues == ['default']

    @pytest.mark.asyncio
    async def test_get_returns_none_when_unknown(self) -> None:
        broker = _make_broker()
        _setup_session_returning(broker, fetchone=None)

        result = await broker.get_worker_state_async('missing')

        assert is_ok(result)
        assert result.ok_value is None

    @pytest.mark.asyncio
    async def test_get_returns_snapshot(self) -> None:
        broker = _make_broker()
        _setup_session_returning(broker, fetchone=_worker_state_row(worker_id='w9'))

        result = await broker.get_worker_state_async('w9')

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.worker_id == 'w9'

    @pytest.mark.asyncio
    async def test_history_rejects_nonpositive_limit(self) -> None:
        broker = _make_broker()

        result = await broker.get_worker_state_history_async('w1', limit=0)

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.MONITORING_QUERY_FAILED
        assert result.err_value.retryable is False

    @pytest.mark.asyncio
    async def test_history_returns_snapshots(self) -> None:
        broker = _make_broker()
        _setup_session_returning(
            broker, fetchall=[_worker_state_row(), _worker_state_row()]
        )

        result = await broker.get_worker_state_history_async('worker-1', limit=10)

        assert is_ok(result)
        assert len(result.ok_value) == 2


@pytest.mark.unit
class TestPingWorkers:
    """ping_workers_async validation and pong decoding."""

    @pytest.mark.asyncio
    async def test_rejects_nonpositive_timeout(self) -> None:
        broker = _make_broker()

        result = await broker.ping_workers_async(timeout_seconds=0)

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.WORKER_PING_FAILED

    @pytest.mark.asyncio
    async def test_disabled_listener_returns_err_not_raise(self) -> None:
        """A broker with no listener must Err, not raise (Result contract)."""
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        config = PostgresConfig(
            database_url='postgresql+psycopg://u:p@localhost/db'
        )
        broker = PostgresBroker(config, assume_initialized=True)
        assert broker._listener is None

        result = await broker.ping_workers_async(timeout_seconds=1.0)

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.WORKER_PING_FAILED

    def test_decode_pong_accepts_matching_correlation(self) -> None:
        from horsies.core.codec.json_io import dumps_json
        from horsies.core.models.health import WorkerPongPayload

        broker = _make_broker()
        payload = dumps_json(
            WorkerPongPayload(
                correlation_id='corr-1', worker_id='w1', hostname='h', pid=7
            ).model_dump()
        ).ok_value

        pong = broker._decode_pong(payload, 'corr-1', 0.05)

        assert pong is not None
        assert pong.worker_id == 'w1'
        assert pong.round_trip_ms == pytest.approx(50.0, abs=1.0)

    def test_decode_pong_drops_mismatched_correlation(self) -> None:
        from horsies.core.codec.json_io import dumps_json
        from horsies.core.models.health import WorkerPongPayload

        broker = _make_broker()
        payload = dumps_json(
            WorkerPongPayload(
                correlation_id='other', worker_id='w1', hostname='h', pid=7
            ).model_dump()
        ).ok_value

        assert broker._decode_pong(payload, 'corr-1', 0.05) is None

    def test_decode_pong_drops_malformed_payload(self) -> None:
        broker = _make_broker()
        assert broker._decode_pong('not json', 'corr-1', 0.05) is None
        assert (
            broker._decode_pong('{"correlation_id": "corr-1"}', 'corr-1', 0.05) is None
        )

    @pytest.mark.asyncio
    async def test_rejects_nonpositive_min_responses(self) -> None:
        broker = _make_broker()

        result = await broker.ping_workers_async(min_responses=0)

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.WORKER_PING_FAILED

    @pytest.mark.asyncio
    async def test_min_responses_returns_early(self) -> None:
        """With min_responses=1, return on the first pong, not at the deadline."""
        import uuid as uuidlib
        from unittest.mock import patch
        from horsies.core.codec.json_io import dumps_json
        from horsies.core.models.health import WorkerPongPayload

        broker = _make_broker()
        fixed = uuidlib.UUID(int=1)
        corr = fixed.hex

        reply_queue: asyncio.Queue[Any] = asyncio.Queue()
        pong_json = dumps_json(
            WorkerPongPayload(
                correlation_id=corr, worker_id='w1', hostname='h', pid=5
            ).model_dump()
        ).ok_value
        reply_queue.put_nowait(SimpleNamespace(payload=pong_json, channel='reply'))

        broker.listener.listen = AsyncMock(return_value=Ok(reply_queue))
        broker.listener.unsubscribe = AsyncMock()
        _setup_session_returning(broker)  # NOTIFY send is a no-op

        loop = asyncio.get_running_loop()
        with patch('horsies.core.brokers.postgres.uuid.uuid4', return_value=fixed):
            start = loop.time()
            result = await broker.ping_workers_async(
                timeout_seconds=5.0, min_responses=1
            )
            elapsed = loop.time() - start

        assert is_ok(result)
        assert [p.worker_id for p in result.ok_value] == ['w1']
        assert elapsed < 2.0, 'should return on first pong, not wait the 5s window'


# ---------------------------------------------------------------------------
# TestMarkStaleTasksAsFailed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMarkStaleTasksAsFailed:
    """Tests for mark_stale_tasks_as_failed stale detection and cleanup."""

    @pytest.mark.asyncio
    async def test_fractional_second_thresholds_keep_millisecond_precision(
        self,
    ) -> None:
        """The public millisecond contract is not restricted to whole seconds."""
        broker = _make_broker()
        scan_result = MagicMock()
        scan_result.fetchall.return_value = []
        scan_session = AsyncMock()
        scan_session.__aenter__ = AsyncMock(return_value=scan_session)
        scan_session.__aexit__ = AsyncMock(return_value=None)
        scan_session.execute = AsyncMock(return_value=scan_result)
        broker.session_factory = MagicMock(return_value=scan_session)

        result = await broker.mark_stale_tasks_as_failed(
            stale_threshold_ms=1_500,
            finalizing_stale_threshold_ms=2_750,
        )

        assert is_ok(result)
        parameters = scan_session.execute.call_args.args[1]
        assert parameters['stale_threshold'] == 1.5
        assert parameters['finalizing_stale_threshold'] == 2.75

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
        """Found stale tasks (no retry policy) should be marked FAILED per-task."""
        broker = _make_broker()
        stale_started_at = datetime(2025, 1, 1, tzinfo=timezone.utc)
        db_now = datetime(2025, 1, 1, 0, 10, tzinfo=timezone.utc)
        # Phase 1 scan returns rows with .id for candidate collection
        scan_rows = [
            SimpleNamespace(id='task-1'),
            SimpleNamespace(id='task-2'),
        ]
        scan_result = MagicMock()
        scan_result.fetchall.return_value = scan_rows

        scan_session = AsyncMock()
        scan_session.__aenter__ = AsyncMock(return_value=scan_session)
        scan_session.__aexit__ = AsyncMock(return_value=None)
        scan_session.execute = AsyncMock(return_value=scan_result)

        # Phase 2: each task locks fresh state, writes the attempt, then invokes
        # the database-owned terminalization operation.
        def _make_task_session(task_id: str, retry_count: int = 0) -> AsyncMock:
            ctx_row = SimpleNamespace(
                id=task_id,
                worker_pid=1234,
                worker_hostname='host-1',
                claimed_by_worker_id='worker-1',
                started_at=stale_started_at,
                retry_count=retry_count,
                worker_process_name='proc-1',
                max_retries=0,
                task_options=None,
                good_until=None,
                db_now=db_now,
                queue_name='default',
            )
            ctx_result = MagicMock()
            ctx_result.fetchone.return_value = ctx_row
            ts = AsyncMock()
            ts.__aenter__ = AsyncMock(return_value=ts)
            ts.__aexit__ = AsyncMock(return_value=None)
            ts.execute = AsyncMock(side_effect=[ctx_result, MagicMock()])
            return ts

        task_sessions = [
            _make_task_session('task-1'),
            _make_task_session('task-2', retry_count=2),
        ]
        broker.session_factory = MagicMock(side_effect=[scan_session, *task_sessions])

        with patch(
            'horsies.core.brokers.postgres.apply_async',
            new=AsyncMock(side_effect=[
                _stale_applied('task-1', db_now),
                _stale_applied('task-2', db_now),
            ]),
        ):
            result = await broker.mark_stale_tasks_as_failed(
                stale_threshold_ms=300_000,
            )

        assert is_ok(result)
        assert result.ok_value == 2
        for ts in task_sessions:
            ts.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_includes_worker_crashed_error_code(self) -> None:
        """Each update call should include WORKER_CRASHED in the result JSON."""
        broker = _make_broker()
        db_now = datetime(2025, 1, 1, 0, 10, tzinfo=timezone.utc)
        scan_rows = [SimpleNamespace(id='task-1')]
        scan_result = MagicMock()
        scan_result.fetchall.return_value = scan_rows

        scan_session = AsyncMock()
        scan_session.__aenter__ = AsyncMock(return_value=scan_session)
        scan_session.__aexit__ = AsyncMock(return_value=None)
        scan_session.execute = AsyncMock(return_value=scan_result)

        ctx_row = SimpleNamespace(
            id='task-1',
            worker_pid=100,
            worker_hostname='host',
            claimed_by_worker_id='w-1',
            started_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            retry_count=0,
            worker_process_name='proc-1',
            max_retries=0,
            task_options=None,
            good_until=None,
            db_now=db_now,
            queue_name='default',
        )
        ctx_result = MagicMock()
        ctx_result.fetchone.return_value = ctx_row

        task_session = AsyncMock()
        task_session.__aenter__ = AsyncMock(return_value=task_session)
        task_session.__aexit__ = AsyncMock(return_value=None)
        task_session.execute = AsyncMock(side_effect=[ctx_result, MagicMock()])

        broker.session_factory = MagicMock(side_effect=[scan_session, task_session])

        apply_mock = AsyncMock(return_value=_stale_applied('task-1', db_now))
        with patch('horsies.core.brokers.postgres.apply_async', new=apply_mock):
            await broker.mark_stale_tasks_as_failed()

        # Per-task SQL: [0]=SELECT FOR UPDATE, [1]=UPSERT_ATTEMPT. The terminal
        # transition itself crosses the typed operation boundary.
        attempt_call = task_session.execute.call_args_list[1]
        attempt_params = attempt_call[0][1]
        assert attempt_params['task_id'] == 'task-1'
        assert attempt_params['outcome'] == 'FAILED'
        assert attempt_params['error_code'] == 'WORKER_CRASHED'
        assert attempt_params['attempt'] == 1

        assert apply_mock.await_args is not None
        command = apply_mock.await_args.args[1]
        assert isinstance(command, FailStaleTask)
        assert command.task_id == 'task-1'
        assert command.stale_after_ms == 300_000
        assert command.finalizing_stale_after_ms == 300_000
        assert 'WORKER_CRASHED' in command.result_json
        assert 'Worker process crashed' in command.failed_reason

    @pytest.mark.asyncio
    async def test_terminalization_refusal_rolls_back_the_attempt(self) -> None:
        """The attempt cannot commit without its corresponding transition."""
        broker = _make_broker()
        db_now = datetime(2025, 1, 1, 0, 10, tzinfo=timezone.utc)
        scan_result = MagicMock()
        scan_result.fetchall.return_value = [SimpleNamespace(id='task-raced')]
        scan_session = AsyncMock()
        scan_session.__aenter__ = AsyncMock(return_value=scan_session)
        scan_session.__aexit__ = AsyncMock(return_value=None)
        scan_session.execute = AsyncMock(return_value=scan_result)

        ctx_row = SimpleNamespace(
            id='task-raced',
            worker_pid=100,
            worker_hostname='host',
            claimed_by_worker_id='w-1',
            started_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            retry_count=0,
            worker_process_name='proc-1',
            max_retries=0,
            task_options=None,
            good_until=None,
            db_now=db_now,
            queue_name='default',
        )
        ctx_result = MagicMock()
        ctx_result.fetchone.return_value = ctx_row
        task_session = AsyncMock()
        task_session.__aenter__ = AsyncMock(return_value=task_session)
        task_session.__aexit__ = AsyncMock(return_value=None)
        task_session.execute = AsyncMock(side_effect=[ctx_result, MagicMock()])
        broker.session_factory = MagicMock(
            side_effect=[scan_session, task_session],
        )

        refusal = SourceStateConflict(
            task_id='task-raced',
            ordinality=None,
            observed=ObservedTaskState(
                status=TaskStatus.RUNNING,
                worker_id='w-1',
                claimed_at=None,
            ),
            evidence=ObservedStaleness(
                last_heartbeat_at=db_now,
                started_at=ctx_row.started_at,
                finalizing_at=None,
                stale_after_ms=300_000,
                finalizing_stale_after_ms=300_000,
                evaluated_at=db_now,
            ),
        )
        with patch(
            'horsies.core.brokers.postgres.apply_async',
            new=AsyncMock(return_value=refusal),
        ):
            result = await broker.mark_stale_tasks_as_failed()

        assert is_ok(result)
        assert result.ok_value == 0
        task_session.rollback.assert_awaited_once()
        task_session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_task_with_retry_policy_schedules_retry(self) -> None:
        """Stale task with WORKER_CRASHED in auto_retry_for should be retried."""
        import json

        broker = _make_broker()
        db_now = datetime(2025, 1, 1, 0, 10, tzinfo=timezone.utc)
        task_options_json = json.dumps(
            {
                'task_name': 'my_task',
                'retry_policy': {
                    'max_retries': 3,
                    'intervals': [60, 300, 900],
                    'backoff_strategy': 'fixed',
                    'jitter': False,
                    'auto_retry_for': ['WORKER_CRASHED'],
                },
            }
        )
        scan_rows = [SimpleNamespace(id='task-retry-1')]
        scan_result = MagicMock()
        scan_result.fetchall.return_value = scan_rows

        scan_session = AsyncMock()
        scan_session.__aenter__ = AsyncMock(return_value=scan_session)
        scan_session.__aexit__ = AsyncMock(return_value=None)
        scan_session.execute = AsyncMock(return_value=scan_result)

        ctx_row = SimpleNamespace(
            id='task-retry-1',
            worker_pid=100,
            worker_hostname='host',
            claimed_by_worker_id='w-1',
            started_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            retry_count=0,
            worker_process_name='proc-1',
            max_retries=3,
            task_options=task_options_json,
            good_until=None,
            db_now=db_now,
            queue_name='default',
        )
        ctx_result = MagicMock()
        ctx_result.fetchone.return_value = ctx_row

        # [0]=SELECT FOR UPDATE, [1]=UPSERT_ATTEMPT, [2]=SCHEDULE_STALE_TASK_RETRY
        retry_result = MagicMock()
        retry_result.fetchone.return_value = SimpleNamespace(id='task-retry-1')
        task_session = AsyncMock()
        task_session.__aenter__ = AsyncMock(return_value=task_session)
        task_session.__aexit__ = AsyncMock(return_value=None)
        task_session.execute = AsyncMock(
            side_effect=[ctx_result, MagicMock(), retry_result]
        )

        # notify session (best-effort pg_notify after retry commit)
        notify_session = AsyncMock()
        notify_session.__aenter__ = AsyncMock(return_value=notify_session)
        notify_session.__aexit__ = AsyncMock(return_value=None)

        broker.session_factory = MagicMock(
            side_effect=[scan_session, task_session, notify_session]
        )

        result = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)

        assert is_ok(result)
        assert result.ok_value == 1
        task_session.commit.assert_awaited_once()

        # [1] = UPSERT_ATTEMPT with will_retry=True
        attempt_call = task_session.execute.call_args_list[1]
        attempt_params = attempt_call[0][1]
        assert attempt_params['will_retry'] is True
        assert attempt_params['error_code'] == 'WORKER_CRASHED'

    @pytest.mark.asyncio
    async def test_stale_task_retries_exhausted_marks_failed(self) -> None:
        """Stale task with retry policy but retries exhausted should be marked FAILED."""
        import json

        broker = _make_broker()
        db_now = datetime(2025, 1, 1, 0, 10, tzinfo=timezone.utc)
        task_options_json = json.dumps(
            {
                'task_name': 'my_task',
                'retry_policy': {
                    'max_retries': 3,
                    'intervals': [60, 300, 900],
                    'backoff_strategy': 'fixed',
                    'jitter': False,
                    'auto_retry_for': ['WORKER_CRASHED'],
                },
            }
        )
        scan_rows = [SimpleNamespace(id='task-exhausted')]
        scan_result = MagicMock()
        scan_result.fetchall.return_value = scan_rows

        scan_session = AsyncMock()
        scan_session.__aenter__ = AsyncMock(return_value=scan_session)
        scan_session.__aexit__ = AsyncMock(return_value=None)
        scan_session.execute = AsyncMock(return_value=scan_result)

        ctx_row = SimpleNamespace(
            id='task-exhausted',
            worker_pid=100,
            worker_hostname='host',
            claimed_by_worker_id='w-1',
            started_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            retry_count=3,
            worker_process_name='proc-1',
            max_retries=3,
            task_options=task_options_json,
            good_until=None,
            db_now=db_now,
            queue_name='default',
        )
        ctx_result = MagicMock()
        ctx_result.fetchone.return_value = ctx_row

        task_session = AsyncMock()
        task_session.__aenter__ = AsyncMock(return_value=task_session)
        task_session.__aexit__ = AsyncMock(return_value=None)
        task_session.execute = AsyncMock(side_effect=[ctx_result, MagicMock()])

        broker.session_factory = MagicMock(side_effect=[scan_session, task_session])

        with patch(
            'horsies.core.brokers.postgres.apply_async',
            new=AsyncMock(
                return_value=_stale_applied('task-exhausted', db_now),
            ),
        ):
            result = await broker.mark_stale_tasks_as_failed(
                stale_threshold_ms=300_000,
            )

        assert is_ok(result)
        assert result.ok_value == 1

        # [1] = UPSERT_ATTEMPT with will_retry=False (exhausted)
        attempt_call = task_session.execute.call_args_list[1]
        attempt_params = attempt_call[0][1]
        assert attempt_params['will_retry'] is False

    @pytest.mark.asyncio
    async def test_concurrent_finalize_skips_no_longer_running(self) -> None:
        """If worker finalized between scan and per-task lock, reaper skips the task."""
        broker = _make_broker()
        scan_rows = [SimpleNamespace(id='task-raced')]
        scan_result = MagicMock()
        scan_result.fetchall.return_value = scan_rows

        scan_session = AsyncMock()
        scan_session.__aenter__ = AsyncMock(return_value=scan_session)
        scan_session.__aexit__ = AsyncMock(return_value=None)
        scan_session.execute = AsyncMock(return_value=scan_result)

        # SELECT FOR UPDATE returns None — task is no longer RUNNING
        ctx_result = MagicMock()
        ctx_result.fetchone.return_value = None
        task_session = AsyncMock()
        task_session.__aenter__ = AsyncMock(return_value=task_session)
        task_session.__aexit__ = AsyncMock(return_value=None)
        task_session.execute = AsyncMock(return_value=ctx_result)

        broker.session_factory = MagicMock(side_effect=[scan_session, task_session])

        result = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)

        assert is_ok(result)
        assert result.ok_value == 0
        task_session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fresh_heartbeat_between_scan_and_phase2_skips_task(self) -> None:
        """A candidate is skipped when phase 2 revalidation no longer sees it as stale."""
        broker = _make_broker()
        scan_rows = [SimpleNamespace(id='task-fresh')]
        scan_result = MagicMock()
        scan_result.fetchall.return_value = scan_rows

        scan_session = AsyncMock()
        scan_session.__aenter__ = AsyncMock(return_value=scan_session)
        scan_session.__aexit__ = AsyncMock(return_value=None)
        scan_session.execute = AsyncMock(return_value=scan_result)

        ctx_result = MagicMock()
        ctx_result.fetchone.return_value = None
        task_session = AsyncMock()
        task_session.__aenter__ = AsyncMock(return_value=task_session)
        task_session.__aexit__ = AsyncMock(return_value=None)
        task_session.execute = AsyncMock(return_value=ctx_result)

        broker.session_factory = MagicMock(side_effect=[scan_session, task_session])

        result = await broker.mark_stale_tasks_as_failed(stale_threshold_ms=300_000)

        assert is_ok(result)
        assert result.ok_value == 0
        select_params = task_session.execute.call_args_list[0][0][1]
        assert select_params['stale_threshold'] == 300.0
        task_session.commit.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestExpirePendingTasks
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExpirePendingTasks:
    """Tests for expire_pending_tasks."""

    @pytest.mark.asyncio
    async def test_no_expired_returns_zero(self) -> None:
        """When UPDATE matches no rows, return 0."""
        broker = _make_broker()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        broker.session_factory = MagicMock(return_value=session)

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(return_value=[]),
        ) as apply_batch:
            result = await broker.expire_pending_tasks()

        assert is_ok(result)
        assert result.ok_value == 0
        apply_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_expired_tasks_transitioned(self) -> None:
        """When UPDATE matches rows, return count and verify TASK_EXPIRED in params."""
        from horsies.core.lifecycle.commands import ExpirePendingTasks
        from horsies.core.lifecycle.outcomes import Applied

        broker = _make_broker()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        broker.session_factory = MagicMock(return_value=session)

        outcomes = [MagicMock(spec=Applied) for _ in range(3)]
        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(return_value=outcomes),
        ) as apply_batch:
            result = await broker.expire_pending_tasks()

        assert is_ok(result)
        assert result.ok_value == 3
        session.commit.assert_awaited_once()

        command = apply_batch.await_args.args[1]
        assert isinstance(command, ExpirePendingTasks)
        assert command.error_code == 'TASK_EXPIRED'
        assert 'TASK_EXPIRED' in command.result_json

    @pytest.mark.asyncio
    async def test_a_full_batch_continues_until_a_short_batch(self) -> None:
        """The operation's returned rows preserve the existing drain loop."""
        from horsies.core.lifecycle.outcomes import Applied

        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        broker.session_factory = MagicMock(return_value=session)
        full = [MagicMock(spec=Applied) for _ in range(500)]
        final = [MagicMock(spec=Applied) for _ in range(2)]

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(side_effect=[full, final]),
        ) as apply_batch:
            result = await broker.expire_pending_tasks()

        assert is_ok(result)
        assert result.ok_value == 502
        assert apply_batch.await_count == 2
        assert session.commit.await_count == 2

    @pytest.mark.asyncio
    async def test_a_non_applied_batch_outcome_fails_closed(self) -> None:
        """Discovery functions report transitions, never refusal-shaped rows."""
        from horsies.core.lifecycle.outcomes import TaskAbsent

        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        broker.session_factory = MagicMock(return_value=session)

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(return_value=[MagicMock(spec=TaskAbsent)]),
        ):
            result = await broker.expire_pending_tasks()

        assert is_err(result)
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_db_error_returns_err(self) -> None:
        """DB exception is wrapped in BrokerOperationError."""
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        broker.session_factory = MagicMock(return_value=session)

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(side_effect=Exception('db down')),
        ):
            result = await broker.expire_pending_tasks()
        assert not is_ok(result)


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
            'my_task',
            'queue',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            args_json='[1]',
            kwargs_json='{"k": "v"}',
            priority=50,
            sent_at=None,
            good_until=None,
            task_options=None,
        )

        assert result == 'task-id-123'
        broker._loop_runner.call.assert_called_once()
        call_args = broker._loop_runner.call.call_args
        # Bound methods are recreated on each access; verify by name
        coro_fn = call_args[0][0]
        assert getattr(coro_fn, '__name__', None) == 'enqueue_async'

    # (test_get_result_delegates_to_loop_runner and
    # test_get_result_loop_runner_exception_returns_broker_error_result
    # removed: strict-serde phase 6 dropped ``broker.get_result``. Sync
    # bridge coverage moved up to ``Horsies.get_result`` — see
    # ``tests/unit/test_app_get_result.py``.)

    def test_get_raw_result_record_delegates_to_loop_runner(self) -> None:
        """get_raw_result_record() should call _loop_runner.call with
        get_raw_result_record_async (the strict-serde phase 6 broker
        primitive that replaces the old get_result loop bridge)."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(return_value=Ok(None))

        result = broker.get_raw_result_record('task-id', timeout_ms=5000)

        assert is_ok(result)
        broker._loop_runner.call.assert_called_once()
        call_args = broker._loop_runner.call.call_args
        coro_fn = call_args[0][0]
        assert (
            getattr(coro_fn, '__name__', None)
            == 'get_raw_result_record_async'
        )

    def test_get_task_info_delegates_to_loop_runner(self) -> None:
        """get_task_info() should call _loop_runner.call with get_task_info_async."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(return_value=Ok(None))

        result = broker.get_task_info(
            'task-id',
            include_result=True,
            include_failed_reason=True,
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
        # 18 base columns: id, task_name, status, queue_name, priority,
        # retry_count, max_retries, next_retry_at, sent_at, enqueued_at, claimed_at,
        # started_at, completed_at, failed_at, worker_hostname, worker_pid,
        # worker_process_name, error_code
        row = (
            'task-abc',
            'compute',
            'RUNNING',
            'default',
            100,
            1,
            3,
            None,
            now,
            now,
            None,
            now,
            None,
            None,
            'host-1',
            9999,
            'worker-0',
            None,
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
        # Strict-serde phase 6: broker emits raw_result (envelope dict)
        # only; typed decoded_result is filled at the Horsies layer.
        assert info.raw_result is None
        assert info.decoded_result is None
        assert info.result_decoded is False
        assert info.failed_reason is None

    @pytest.mark.asyncio
    async def test_include_result_populates_raw_result(self) -> None:
        """include_result=True populates ``raw_result`` (the parsed
        envelope dict).

        Strict-serde phase 6: broker no longer typed-decodes. Typed
        ``decoded_result`` is filled at the ``Horsies.get_task_info``
        layer — see ``tests/unit/test_app_get_result.py`` for the
        Horsies-level coverage.
        """
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        result_json = '{"__h_task_result__":true,"ok":"hello","err":null}'
        # 18 base + 1 result column
        row = (
            'task-abc',
            'compute',
            'COMPLETED',
            'default',
            100,
            0,
            0,
            None,
            now,
            now,
            None,
            now,
            now,
            None,
            'host-1',
            9999,
            'worker-0',
            None,
            result_json,
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async('task-abc', include_result=True)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.raw_result == {
            '__h_task_result__': True,
            'ok': 'hello',
            'err': None,
        }
        # decoded_result stays None at the broker layer.
        assert info.decoded_result is None
        assert info.result_decoded is False

    @pytest.mark.asyncio
    async def test_include_failed_reason_returns_reason(self) -> None:
        """include_failed_reason=True should add failed_reason to Ok(TaskInfo)."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 18 base + 1 failed_reason column
        row = (
            'task-abc',
            'compute',
            'FAILED',
            'default',
            100,
            0,
            0,
            None,
            now,
            now,
            None,
            now,
            None,
            now,
            'host-1',
            9999,
            'worker-0',
            None,
            'Worker crashed unexpectedly',
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async(
            'task-abc', include_failed_reason=True
        )

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.failed_reason == 'Worker crashed unexpectedly'

    @pytest.mark.asyncio
    async def test_include_result_null_stays_none(self) -> None:
        """include_result=True with NULL result in DB should return Ok(TaskInfo) with result=None."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 18 base + 1 result column (None)
        row = (
            'task-abc',
            'compute',
            'RUNNING',
            'default',
            100,
            0,
            0,
            None,
            now,
            now,
            None,
            now,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async('task-abc', include_result=True)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.raw_result is None
        assert info.decoded_result is None
        assert info.result_decoded is False

    @pytest.mark.asyncio
    async def test_include_both_result_and_failed_reason(self) -> None:
        """Both include flags should add both columns to Ok(TaskInfo)."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        result_json = (
            '{"__h_task_result__":true,"ok":null,"err":'
            '{"error_code":{"__builtin_task_code__":"TASK_EXCEPTION"},'
            '"message":"fail","data":null,"exception":null}}'
        )
        # 18 base + 1 result + 1 failed_reason
        row = (
            'task-abc',
            'compute',
            'FAILED',
            'default',
            100,
            0,
            0,
            None,
            now,
            now,
            None,
            now,
            None,
            now,
            'host-1',
            9999,
            'worker-0',
            'TASK_EXCEPTION',
            result_json,
            'Something broke',
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async(
            'task-abc',
            include_result=True,
            include_failed_reason=True,
        )

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        # Broker emits the parsed envelope as raw_result; typed decode
        # is the Horsies layer's job.
        assert info.raw_result is not None
        assert info.raw_result.get('__h_task_result__') is True
        assert info.raw_result.get('ok') is None
        assert isinstance(info.raw_result.get('err'), dict)
        assert info.decoded_result is None
        assert info.result_decoded is False
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
    async def test_close_async_does_not_stop_loop_runner_from_its_own_thread(
        self,
    ) -> None:
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
class TestSchemaVersionInitialization:
    """Tests for schema-version fast path and slow-path retry behavior."""

    @pytest.mark.asyncio
    async def test_current_schema_version_skips_schema_migrations(self) -> None:
        from horsies.core.brokers.postgres import PostgresBroker
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.schemas.migrations import (
            READ_SCHEMA_VERSION_SQL,
            SCHEMA_ADVISORY_LOCK_SQL,
            SCHEMA_VERSION,
            SCHEMA_VERSION_TABLE_EXISTS_SQL,
        )

        engine = MagicMock()
        conn = AsyncMock()
        conn.run_sync = AsyncMock()
        exists_result = MagicMock()
        exists_result.scalar.return_value = True
        version_result = MagicMock()
        version_result.scalar_one.return_value = SCHEMA_VERSION
        conn.execute.side_effect = [exists_result, version_result]
        begin_ctx = MagicMock()
        begin_ctx.__aenter__ = AsyncMock(return_value=conn)
        begin_ctx.__aexit__ = AsyncMock(return_value=None)
        engine.begin.return_value = begin_ctx

        with (
            patch(
                'horsies.core.brokers.postgres.create_async_engine',
                return_value=engine,
            ),
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch('horsies.core.brokers.postgres.PostgresListener'),
        ):
            broker = PostgresBroker(
                PostgresConfig(database_url='postgresql+psycopg://u:p@localhost/db')
            )
            await broker._ensure_initialized()

        assert len(conn.execute.await_args_list) == 2
        assert conn.execute.await_args_list[0].args == (
            SCHEMA_VERSION_TABLE_EXISTS_SQL,
        )
        assert conn.execute.await_args_list[1].args == (READ_SCHEMA_VERSION_SQL,)
        assert not any(
            call.args[0] is SCHEMA_ADVISORY_LOCK_SQL
            for call in conn.execute.await_args_list
        )
        conn.run_sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_schema_version_table_skips_version_read(self) -> None:
        from horsies.core.schemas.migrations import READ_SCHEMA_VERSION_SQL

        engine = MagicMock()
        conn = AsyncMock()
        exists_result = MagicMock()
        exists_result.scalar.return_value = False
        conn.execute.return_value = exists_result
        begin_ctx = MagicMock()
        begin_ctx.__aenter__ = AsyncMock(return_value=conn)
        begin_ctx.__aexit__ = AsyncMock(return_value=None)
        engine.begin.return_value = begin_ctx

        broker = _make_broker()

        assert await broker._read_schema_version_if_exists(engine) == 0
        assert not any(
            call.args[0] is READ_SCHEMA_VERSION_SQL
            for call in conn.execute.await_args_list
        )

    @pytest.mark.asyncio
    async def test_slow_path_inserts_schema_version_after_migrations(self) -> None:
        from horsies.core.schemas.migrations import (
            ADD_TASK_FINALIZING_COLUMNS_SQL,
            ADD_TASK_IS_WORKFLOW_TASK_COLUMN_SQL,
            BACKFILL_TASK_IS_WORKFLOW_TASK_SQL,
            CREATE_SCHEMA_VERSION_TABLE_SQL,
            INSERT_SCHEMA_VERSION_SQL,
            SCHEMA_ADVISORY_LOCK_SQL,
            SCHEMA_VERSION,
        )

        engine = MagicMock()
        conn = AsyncMock()
        conn.run_sync = AsyncMock()
        version_result = MagicMock()
        version_result.scalar_one.return_value = 0
        conn.execute.return_value = version_result
        begin_ctx = MagicMock()
        begin_ctx.__aenter__ = AsyncMock(return_value=conn)
        begin_ctx.__aexit__ = AsyncMock(return_value=None)
        engine.begin.return_value = begin_ctx

        broker = _make_broker()
        await broker._run_schema_migrations(engine)

        calls = conn.execute.await_args_list
        lock_indices = [
            index
            for index, call in enumerate(calls)
            if call.args[0] is SCHEMA_ADVISORY_LOCK_SQL
        ]
        create_version_index = next(
            index
            for index, call in enumerate(calls)
            if call.args[0] is CREATE_SCHEMA_VERSION_TABLE_SQL
        )
        insert_version_index = next(
            index
            for index, call in enumerate(calls)
            if call.args[0] is INSERT_SCHEMA_VERSION_SQL
        )
        add_workflow_flag_index = next(
            index
            for index, call in enumerate(calls)
            if call.args[0] is ADD_TASK_IS_WORKFLOW_TASK_COLUMN_SQL
        )
        backfill_workflow_flag_index = next(
            index
            for index, call in enumerate(calls)
            if call.args[0] is BACKFILL_TASK_IS_WORKFLOW_TASK_SQL
        )
        add_finalizing_index = next(
            index
            for index, call in enumerate(calls)
            if call.args[0] is ADD_TASK_FINALIZING_COLUMNS_SQL
        )

        assert max(lock_indices) < create_version_index
        conn.run_sync.assert_awaited_once()
        assert add_workflow_flag_index < backfill_workflow_flag_index
        assert backfill_workflow_flag_index < add_finalizing_index
        assert add_finalizing_index < insert_version_index
        assert insert_version_index > create_version_index
        assert calls[insert_version_index].args[1] == {'version': SCHEMA_VERSION}

    @pytest.mark.asyncio
    async def test_schema_deadlock_retries_slow_path(self) -> None:
        broker = _make_broker()
        engine = MagicMock()

        class FakeDeadlock(Exception):
            orig = SimpleNamespace(sqlstate='40P01')

        broker._run_schema_migrations = AsyncMock(
            side_effect=[FakeDeadlock(), None]
        )

        with (
            patch(
                'horsies.core.brokers.postgres.random.uniform',
                return_value=0,
            ),
            patch(
                'horsies.core.brokers.postgres.asyncio.sleep',
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            await broker._run_schema_migrations_with_retry(engine)

        assert broker._run_schema_migrations.await_count == 2
        mock_sleep.assert_awaited_once_with(0.05)


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

        assert (
            dispose_called
        ), 'engine.dispose must be called even after listener.close fails'
        assert is_err(result)
        err = result.err_value
        assert err.code == BrokerErrorCode.CLOSE_FAILED
        assert err.exception is not None
