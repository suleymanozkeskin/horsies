"""Tests for PostgresBroker (python/horsies/core/brokers/postgres.py).

Strategy: mock DB layer entirely (create_async_engine, async_sessionmaker,
PostgresListener) to avoid real PostgreSQL. Tests verify logic, branching,
error handling, and delegation patterns.
"""

from __future__ import annotations

import asyncio
import dataclasses
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import psycopg
import pytest

from sqlalchemy.exc import OperationalError as SAOperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from horsies.core.brokers.enqueue_task import SendKind
from horsies._core import (
    StaleClaimsBoundReached,
    StaleClaimsDrained,
    StaleClaimsRequeueFailed,
    StaleRunningRecovered,
    StaleRunningScanFailed,
    StaleRunningScanFull,
    TooManyPendingCalls,
)
from horsies.core.types.core_failure import CoreOperationFailed
from horsies.core.brokers.postgres import SIGNED_64_BIT_MAX, SIGNED_64_BIT_MIN, SyncSendRoute
from horsies.core.brokers.resources import ListenerDisabled, ListenerEnabled
from horsies.core.brokers.result_types import (
    BrokerEnqueueFailed,
    BrokerErrorCode,
    BrokerIdempotencyKeyConflict,
    BrokerInvalidInput,
    BrokerInvalidJsonPayload,
    BrokerInvalidTaskOptions,
    BrokerListenerDisabled,
)
from horsies.core.codec.json_io import SerializationError
from horsies.core.models.boot_failure import ResourceShortage, ShortageErrno
from horsies.core.models.health import (
    AllSlotsServing,
    ClassBudget,
    ClassBudgets,
    DownSlot,
    SlotsDown,
)
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import (
    TaskSendErrorCode,
    TaskSendValidationFailed,
)
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.history.archive.attempts import AttemptRecord
from horsies.core.history.archive.versions import archive_digest
from horsies.core.utils.url import sqlalchemy_url
from horsies.core.history.reads.detail import (
    HistoryTaskDetail,
    TaskAbsenceReason,
    TaskDetailAbsent,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.models.tasks import (
    Execution,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
    TaskError,
    TaskInfo,
    TaskResult,
)
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.baseline.install import (
    BaselineCurrent,
    ClientFamily,
    EmptyDatabase,
    FamilyRecorded,
    ReadersMissing,
    ReadersPublished,
    SchemaNewer,
    SchemaStatementFailed,
)
from horsies.core.types.status import TaskStatus
from horsies.core.utils.loop_runner import LoopRunnerError
from tests.broker_resources_support import (
    attach_loop_limit,
    pin_resources,
    resources_of,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _schema_lock_connection(
    *,
    lock_attempts: tuple[bool, ...] = (True,),
    unlock_succeeds: bool = True,
) -> AsyncMock:
    """A schema lock connection whose lock tries return ``lock_attempts``.

    Each key takes lock tries from ``lock_attempts`` in order; after the
    last one, every try succeeds.
    """
    connection = AsyncMock()
    pending_attempts = list(lock_attempts)
    unlock_cursor = AsyncMock()
    unlock_cursor.fetchone = AsyncMock(return_value=(unlock_succeeds,))

    async def _execute(sql: str, params: tuple[object, ...]) -> AsyncMock:
        _ = params
        match sql:
            case 'SELECT pg_advisory_unlock(%s)':
                return unlock_cursor
            case _:
                acquired = pending_attempts.pop(0) if pending_attempts else True
                lock_cursor = AsyncMock()
                lock_cursor.fetchone = AsyncMock(return_value=(acquired,))
                return lock_cursor

    connection.execute = AsyncMock(side_effect=_execute)
    connection.close = AsyncMock()
    return connection


def _orphan_scan_stats(rows: int, candidates: int) -> MagicMock:
    result = MagicMock()
    result.one.return_value = SimpleNamespace(
        last_scan_rows=rows,
        last_candidate_rows=candidates,
    )
    return result


def _pinned(broker: Any) -> Any:
    """The mocked resource set that ``_make_broker`` pinned for every loop."""
    return broker._build_resources()


def _set_resources(broker: Any, **parts: Any) -> None:
    """Replace parts of the pinned resource set (white-box test helper)."""
    pin_resources(broker, dataclasses.replace(_pinned(broker), **parts))


def _listener_of(broker: Any) -> Any:
    """The listener inside the pinned resource set."""
    match _pinned(broker).listener:
        case ListenerEnabled(listener=listener):
            return listener
        case ListenerDisabled():
            raise AssertionError('broker has no listener')


def _make_broker(database_url: str = 'postgresql://u:p@localhost/db') -> Any:
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
        # Build the mocked set under the patches; every loop gets it.
        pin_resources(broker, broker._build_resources())
        # Mark as initialized to skip real DB setup
        broker._initialized = True

    return broker


def _make_result_session(row: Any) -> AsyncMock:
    """Mock session for get_raw_result_record_async.

    The broker polls a slim status/name SELECT of the live row; a live row
    holds no result. ``session.get`` is a tripwire: the result path
    regressing to a full-entity load fails loudly here.
    """
    from horsies.core.brokers.postgres import GET_TASK_STATUS_NAME_SQL

    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    if row is None:
        probe_row = None
    else:
        probe_row = MagicMock()
        status = row.status
        probe_row.status = (
            status.value if isinstance(status, TaskStatus) else status
        )
        probe_row.task_name = row.task_name

    async def _execute(statement: Any, *args: Any, **kwargs: Any) -> Any:
        assert statement is GET_TASK_STATUS_NAME_SQL, statement
        result = MagicMock()
        result.fetchone = MagicMock(return_value=probe_row)
        return result

    session.execute = AsyncMock(side_effect=_execute)
    session.get = AsyncMock(
        side_effect=AssertionError(
            'result path must not full-entity load TaskModel; '
            'use GET_TASK_STATUS_NAME_SQL'
        ),
    )
    # The session's connection models a database where the staged result
    # reader is not published (its probe answers False), so the live probe
    # runs, and where history does not hold the task: the staged detail
    # read returns no row and the absence reason is WITHIN_RETAINED_HISTORY.
    # The integration suite covers history rows on real schemas.
    async def _connection_execute(statement: Any, *args: Any, **kwargs: Any) -> Any:
        result = MagicMock()
        match str(statement):
            case sql if 'to_regprocedure' in sql:
                result.scalar_one = MagicMock(return_value=False)
            case sql if 'horsies_task_absence_reason' in sql:
                result.scalar_one = MagicMock(return_value='WITHIN_RETAINED_HISTORY')
            case _:
                result.one_or_none = MagicMock(return_value=None)
        return result

    connection = AsyncMock()
    connection.execute = AsyncMock(side_effect=_connection_execute)
    session.connection = AsyncMock(return_value=connection)
    return session


def _make_task_row(**overrides: Any) -> MagicMock:
    """Build a mock TaskModel row with sensible defaults."""
    defaults = {
        'id': 'task-123',
        'task_name': 'my_task',
        'queue_name': 'default',
        'priority': 100,
        'status': TaskStatus.PENDING,
        'input': '{}',
        'sent_at': datetime.now(timezone.utc),
        'enqueued_at': datetime.now(timezone.utc),
        'claimed_at': None,
        'started_at': None,
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
        broker = _make_broker('postgresql://u:p@host1/db')

        key1 = broker._schema_advisory_key()
        key2 = broker._schema_advisory_key()

        assert isinstance(key1, int)
        assert key1 == key2

    def test_schema_key_is_url_independent(self) -> None:
        """Different URLs should share the new schema advisory key."""
        broker_a = _make_broker('postgresql://u:p@host_a/db')
        broker_b = _make_broker('postgresql://u:p@host_b/db')

        key_a = broker_a._schema_advisory_key()
        key_b = broker_b._schema_advisory_key()

        assert key_a == key_b

    def test_key_is_signed_64_bit(self) -> None:
        """Advisory key must fit in a signed 64-bit range for pg_advisory_xact_lock."""
        broker = _make_broker()

        key = broker._schema_advisory_key()

        assert -(2**63) <= key <= 2**63 - 1


@pytest.mark.unit
class TestPostgresBrokerPgBouncerWiring:
    """Tests for split URL and PgBouncer-specific broker wiring."""

    @pytest.mark.asyncio
    async def test_worker_validation_rejects_failed_session_behavior(self) -> None:
        from horsies.core.errors import ConfigurationError

        broker = _make_broker()
        connection = AsyncMock()
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=connection)
        context.__aexit__ = AsyncMock(return_value=None)
        _pinned(broker).engine.connect.return_value = context
        _set_resources(broker, partition_maintenance=MagicMock())
        _pinned(broker).partition_maintenance.connect.return_value = context

        with (
            patch(
                'horsies.core.brokers.postgres.probe_session_capability',
                new=AsyncMock(side_effect=asyncio.TimeoutError),
            ),
            pytest.raises(
                ConfigurationError,
                match='broker session capability check failed',
            ),
        ):
            await broker.validate_worker_database_paths()

    @pytest.mark.asyncio
    async def test_worker_validation_connects_both_paths_and_probes_session_url(
        self,
    ) -> None:
        from horsies.core.brokers.postgres import PostgresBroker
        from horsies.core.models.broker import PostgresConfig

        runtime_engine = MagicMock()
        session_engine = MagicMock()
        runtime_engine.dispose = AsyncMock()
        session_engine.dispose = AsyncMock()
        runtime_connection = AsyncMock()
        session_connection = AsyncMock()
        runtime_context = MagicMock()
        runtime_context.__aenter__ = AsyncMock(return_value=runtime_connection)
        runtime_context.__aexit__ = AsyncMock(return_value=None)
        session_context = MagicMock()
        session_context.__aenter__ = AsyncMock(return_value=session_connection)
        session_context.__aexit__ = AsyncMock(return_value=None)
        runtime_engine.connect.return_value = runtime_context
        session_engine.connect.return_value = session_context
        listener = AsyncMock()

        with (
            patch(
                'horsies.core.brokers.postgres.create_async_engine',
                side_effect=[runtime_engine, session_engine],
            ),
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch(
                'horsies.core.brokers.postgres.PostgresListener',
                return_value=listener,
            ),
            patch(
                'horsies.core.brokers.postgres.probe_session_capability',
                new=AsyncMock(),
            ) as probe,
        ):
            broker = PostgresBroker(
                PostgresConfig(
                    database_url='postgresql://u:p@pooler:6432/db',
                    session_database_url=(
                        'postgresql://u:p@direct:5432/db'
                    ),
                    pgbouncer_transaction_mode=True,
                )
            )
            await broker.validate_worker_database_paths()
            closed = await broker.close_async()

        runtime_connection.execute.assert_awaited_once()
        session_connection.execute.assert_awaited_once()
        probe.assert_awaited_once_with('postgresql://u:p@direct:5432/db')
        assert is_ok(closed)
        listener.close.assert_awaited_once()
        runtime_engine.dispose.assert_awaited_once()
        session_engine.dispose.assert_awaited_once()

    def test_runtime_engine_uses_database_url_and_listener_uses_session_url(
        self,
    ) -> None:
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        database_url = 'postgresql://u:p@pooler:6432/db'
        session_url = 'postgresql://u:p@direct:5432/db'

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
            PostgresBroker(config)._build_resources()

        assert mock_engine.call_count == 2
        # The engines open the configured URLs with the psycopg 3 driver.
        assert mock_engine.call_args_list[0].args[0] == sqlalchemy_url(database_url)
        assert mock_engine.call_args_list[0].kwargs['connect_args'] == {
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 3,
            'tcp_user_timeout': 30_000,
            'prepare_threshold': None,
        }
        assert mock_engine.call_args_list[1].args[0] == sqlalchemy_url(session_url)
        assert mock_engine.call_args_list[1].kwargs['connect_args'] == {
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 3,
            'tcp_user_timeout': 30_000,
        }
        mock_listener_cls.assert_called_once_with(
            'postgresql://u:p@direct:5432/db', tcp_user_timeout_ms=30_000,
        )

    @pytest.mark.asyncio
    async def test_schema_initialization_uses_session_url_when_split(self) -> None:
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        runtime_engine = MagicMock()
        schema_engine = MagicMock()
        schema_engine.dispose = AsyncMock()
        database_url = 'postgresql://u:p@pooler:6432/db'
        session_url = 'postgresql://u:p@direct:5432/db'
        lock_connection = _schema_lock_connection()

        with (
            patch(
                'horsies.core.brokers.postgres.create_async_engine',
                side_effect=[runtime_engine, schema_engine],
            ) as mock_engine,
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch('horsies.core.brokers.postgres.PostgresListener'),
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=lock_connection),
            ) as connect_lock,
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(side_effect=[Ok(EmptyDatabase()), Ok(EmptyDatabase())]),
            ),
            patch(
                'horsies.core.brokers.postgres.install_baseline',
                new=AsyncMock(return_value=Ok(None)),
            ),
            patch(
                'horsies.core.brokers.postgres.publish_history_readers',
                new=AsyncMock(return_value=Ok(None)),
            ),
        ):
            config = PostgresConfig(
                database_url=database_url,
                session_database_url=session_url,
                pgbouncer_transaction_mode=True,
            )
            broker = PostgresBroker(config)
            await broker._ensure_initialized(await resources_of(broker))

        assert mock_engine.call_args_list[0].args[0] == sqlalchemy_url(database_url)
        assert mock_engine.call_args_list[1].args[0] == sqlalchemy_url(session_url)
        assert connect_lock.await_args is not None
        assert connect_lock.await_args.args[0] == 'postgresql://u:p@direct:5432/db'
        schema_engine.dispose.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_schema_initialization_acquires_only_the_constant_lock(
        self,
    ) -> None:
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        lock_connection = _schema_lock_connection()

        with (
            patch('horsies.core.brokers.postgres.create_async_engine'),
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch('horsies.core.brokers.postgres.PostgresListener'),
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=lock_connection),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(side_effect=[Ok(EmptyDatabase()), Ok(EmptyDatabase())]),
            ),
            patch(
                'horsies.core.brokers.postgres.install_baseline',
                new=AsyncMock(return_value=Ok(None)),
            ),
            patch(
                'horsies.core.brokers.postgres.publish_history_readers',
                new=AsyncMock(return_value=Ok(None)),
            ),
        ):
            config = PostgresConfig(
                database_url='postgresql://u:p@pooler:6432/db',
                session_database_url='postgresql://u:p@direct:5432/db',
                pgbouncer_transaction_mode=True,
            )
            broker = PostgresBroker(config)
            await broker._ensure_initialized(await resources_of(broker))

        lock_keys = [
            call.args[1][0]
            for call in lock_connection.execute.await_args_list
            if call.args[0] == 'SELECT pg_try_advisory_lock(%s)'
        ]
        assert lock_keys == [broker._schema_advisory_key()]

    @pytest.mark.asyncio
    async def test_schema_lock_unlock_failure_closes_connection(self) -> None:
        broker = _make_broker()
        lock_connection = _schema_lock_connection(unlock_succeeds=False)

        with patch(
            'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
            new=AsyncMock(return_value=lock_connection),
        ):
            with pytest.raises(
                RuntimeError,
                match='schema advisory lock ownership was lost',
            ):
                async with broker._schema_session_lock():
                    pass

        lock_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_schema_lock_preserves_body_error_when_unlock_fails(
        self,
    ) -> None:
        broker = _make_broker()
        lock_connection = _schema_lock_connection(unlock_succeeds=False)

        with patch(
            'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
            new=AsyncMock(return_value=lock_connection),
        ):
            with pytest.raises(ValueError, match='migration failed') as raised:
                async with broker._schema_session_lock():
                    raise ValueError('migration failed')

        assert raised.value.__notes__ == [
            'schema advisory lock ownership was lost'
        ]
        lock_connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_schema_lock_polls_until_the_key_is_free(self) -> None:
        """A held key is retried after a sleep, never with a waiting statement."""
        broker = _make_broker()
        lock_connection = _schema_lock_connection(lock_attempts=(False, False, True))

        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=lock_connection),
            ),
            patch(
                'horsies.core.brokers.postgres.asyncio.sleep',
                new=AsyncMock(),
            ) as sleep,
        ):
            async with broker._schema_session_lock():
                pass

        statements = [call.args[0] for call in lock_connection.execute.await_args_list]
        assert 'SELECT pg_advisory_lock(%s)' not in statements
        first_key = broker._schema_advisory_key()
        first_key_tries = [
            call
            for call in lock_connection.execute.await_args_list
            if call.args[0] == 'SELECT pg_try_advisory_lock(%s)'
            and call.args[1][0] == first_key
        ]
        assert len(first_key_tries) == 3
        assert sleep.await_count == 2

    @pytest.mark.asyncio
    async def test_schema_lock_cancellation_releases_and_closes(self) -> None:
        broker = _make_broker()
        lock_connection = _schema_lock_connection()

        with patch(
            'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
            new=AsyncMock(return_value=lock_connection),
        ):
            with pytest.raises(asyncio.CancelledError):
                async with broker._schema_session_lock():
                    raise asyncio.CancelledError

        unlock_calls = [
            call
            for call in lock_connection.execute.await_args_list
            if call.args[0] == 'SELECT pg_advisory_unlock(%s)'
        ]
        assert len(unlock_calls) == 1
        lock_connection.close.assert_awaited_once()

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
                PostgresConfig(database_url='postgresql://u:p@localhost/db'),
                assume_initialized=True,
            )
            resources = broker._build_resources()

        assert broker._initialized is True
        assert isinstance(resources.listener, ListenerDisabled)
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


_SENT_TASK_ID = UUID('0198c0de-0000-7000-8000-0000000000a1')
_HOLDER_TASK_ID = UUID('0198c0de-0000-7000-8000-0000000000a2')


def _make_enqueue_engine(
    *,
    outcome: str = 'INSERTED',
    task_id: UUID = _SENT_TASK_ID,
) -> tuple[MagicMock, AsyncMock]:
    """Mock async engine for the enqueue statement.

    Every enqueue executes one ``horsies_enqueue_task`` statement on an
    AUTOCOMMIT connection from the broker's runtime engine — no session, no
    commit. Returns ``(engine, connection)``; the statement answers one row
    ``(outcome, task_id)``.
    """
    conn = AsyncMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [(outcome, task_id)]
    conn.execute = AsyncMock(return_value=mock_result)
    conn.execution_options = AsyncMock(return_value=conn)

    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=None)

    engine = MagicMock()
    engine.connect = MagicMock(return_value=ctx)
    return engine, conn


@pytest.mark.unit
class TestEnqueueAsync:
    """Tests for enqueue_async: task creation, option parsing, retry extraction."""

    @pytest.mark.asyncio
    async def test_basic_enqueue_returns_uuid_string(self) -> None:
        """Unkeyed enqueue_async returns Ok(task_id) via one AUTOCOMMIT insert."""
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        result = await broker.enqueue_async(
            'my_task',
            'default',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            args_json='[]',
            kwargs_json='{"key": "val"}',
        )

        assert is_ok(result)
        assert result.ok_value == 'test-task-id'
        conn.execute.assert_awaited()
        conn.execution_options.assert_awaited_once_with(
            isolation_level='AUTOCOMMIT',
        )

    @pytest.mark.asyncio
    async def test_with_sent_at_uses_provided_timestamp(self) -> None:
        """When sent_at is provided, it should be accepted and enqueue should succeed."""
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        custom_ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=custom_ts,
        )

        assert is_ok(result)
        assert result.ok_value == 'test-task-id'
        conn.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_with_retry_policy_extracts_max_retries(self) -> None:
        """task_options containing retry_policy should be parsed successfully.

        The broker embeds max_retries into the Core INSERT statement values.
        We verify indirectly: valid task_options with retry_policy produce Ok,
        and the execute call carries the parsed value as a bind parameter.
        """
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        task_options = (
            '{"retry_policy": {"max_retries": 5, "intervals": [5], '
            '"backoff_strategy": "exponential", "auto_retry_for": ["TRANSIENT"]}}'
        )
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            task_options=task_options,
        )

        assert is_ok(result)
        conn.execute.assert_awaited()
        insert_params = conn.execute.await_args.args[1]
        assert insert_params['max_retries'] == 5

    @pytest.mark.asyncio
    async def test_malformed_task_options_raises(self) -> None:
        """Malformed task_options JSON is refused as INVALID_TASK_OPTIONS.

        loads_json returns Err(SerializationError); enqueue_async returns it
        as the cause of BrokerInvalidTaskOptions. Nothing was written.
        """
        broker = _make_broker()
        session = _make_enqueue_session()
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            task_options='NOT_VALID_JSON{{',
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidTaskOptions(message=message, exception=cause):
                assert 'JSON' in message
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_TASK_OPTIONS, got {other!r}')

    @pytest.mark.asyncio
    async def test_overflowing_number_in_task_options_is_refused(self) -> None:
        """A number outside finite binary64 never reaches the input envelope."""
        broker = _make_broker()
        session = _make_enqueue_session()
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            task_options='{"x":1e9999}',
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidTaskOptions(message=message, exception=cause):
                assert 'finite binary64' in message
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_TASK_OPTIONS, got {other!r}')
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kwargs_that_repeat_a_key_are_refused_as_invalid_input(self) -> None:
        """The stored input would differ from the text the caller sent."""
        broker = _make_broker()
        session = _make_enqueue_session()
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            sent_at=datetime.now(timezone.utc),
            send_kind=SendKind.FIRST,
            kwargs_json='{"x":1,"x":2}',
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(message=message, exception=cause):
                assert message == (
                    'kwargs JSON corrupt for my_task: '
                    'JSON parse failed: JSON object repeats a key'
                )
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lone_surrogate_in_kwargs_is_refused_as_invalid_input(self) -> None:
        """A lone surrogate never reaches the UTF-8 encode of the input envelope."""
        broker = _make_broker()
        session = _make_enqueue_session()
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            kwargs_json=r'{"x":"\ud800"}',
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(message=message, exception=cause):
                assert 'kwargs JSON corrupt' in message
                assert 'lone surrogate' in message
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kwargs_past_the_depth_bound_are_refused_as_invalid_input(self) -> None:
        """A reader could not walk the stored text, so it is refused here."""
        broker = _make_broker()
        session = _make_enqueue_session()
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            kwargs_json='{"x":' + '[' * 1_100 + ']' * 1_100 + '}',
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(message=message, exception=cause):
                assert 'kwargs JSON corrupt' in message
                assert 'nested deeper than' in message
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_overflowing_number_in_kwargs_is_refused_as_invalid_input(self) -> None:
        """A number outside finite binary64 never reaches the input envelope."""
        broker = _make_broker()
        session = _make_enqueue_session()
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            kwargs_json='{"x":1e9999}',
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(message=message, exception=cause):
                assert 'kwargs JSON corrupt' in message
                assert 'finite binary64' in message
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_both_scheduling_parameters_are_refused_as_invalid_input(self) -> None:
        """enqueued_at and enqueue_delay_seconds together → INVALID_INPUT; nothing is written."""
        broker = _make_broker()
        session = _make_enqueue_session()
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            enqueued_at=datetime.now(timezone.utc),
            enqueue_delay_seconds=30,
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(message=message, exception=cause):
                assert 'enqueued_at and enqueue_delay_seconds' in message
                assert cause == NoCause()
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_corrupt_args_json_is_refused_as_invalid_input_with_the_cause(self) -> None:
        """Malformed args JSON → INVALID_INPUT with the parse error as the cause; nothing is written."""
        broker = _make_broker()
        session = _make_enqueue_session()
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            args_json='NOT_VALID_JSON{{',
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(message=message, exception=cause):
                assert 'args JSON corrupt' in message
                assert isinstance(cause, SerializationError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_with_good_until_passes_expiry(self) -> None:
        """good_until should be accepted and enqueue should succeed."""
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        expiry = datetime(2099, 12, 31, tzinfo=timezone.utc)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            good_until=expiry,
        )

        assert is_ok(result)
        conn.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_task_options_with_non_dict_retry_policy_is_refused(self) -> None:
        """A retry_policy that is not an object is refused before any write."""
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        task_options = '{"retry_policy": "not_a_dict"}'
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            task_options=task_options,
        )

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.INVALID_TASK_OPTIONS
        assert 'retry_policy is not an object: str' in result.err_value.message
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_task_options_without_retry_policy_keeps_zero(self) -> None:
        """task_options dict without retry_policy key should still enqueue successfully.

        max_retries defaults to 0 when retry_policy is absent. The broker
        embeds this into the Core INSERT; we verify the enqueue succeeds.
        """
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        task_options = '{"some_other_key": "value"}'
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            task_options=task_options,
        )

        assert is_ok(result)
        conn.execute.assert_awaited()
        assert conn.execute.await_args.args[1]['max_retries'] == 0

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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=future,
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(message=message, exception=cause):
                assert 'sent_at' in message
                assert cause == NoCause()
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_future_sent_at_with_enqueued_at_accepted(self) -> None:
        """Future sent_at is allowed when enqueued_at is explicitly provided."""
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=future,
            enqueued_at=future,
        )

        assert is_ok(result)
        conn.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_future_sent_at_with_enqueue_delay_accepted(self) -> None:
        """Future sent_at is allowed when enqueue_delay_seconds is provided."""
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=future,
            enqueue_delay_seconds=600,
        )

        assert is_ok(result)
        conn.execute.assert_awaited()
        params = conn.execute.await_args.args[1]
        assert params['start_kind'] == 'AFTER_DELAY'
        assert params['start_at'] is None
        assert params['start_delay_s'] == 600


# ---------------------------------------------------------------------------
# TestEnqueueOutcomes — the answers of horsies_enqueue_task
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnqueueOutcomes:
    """Each outcome of the statement maps to the result of enqueue_async."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize('outcome', ['INSERTED', 'SAME_COMMAND'])
    async def test_the_sent_id_holds_the_send(self, outcome: str) -> None:
        broker = _make_broker()
        engine, _conn = _make_enqueue_engine(outcome=outcome, task_id=_SENT_TASK_ID)
        _set_resources(broker, engine=engine)

        result = await broker.enqueue_async(
            'my_task',
            task_id=_SENT_TASK_ID,
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
        )

        assert result == Ok(_SENT_TASK_ID)

    @pytest.mark.asyncio
    async def test_a_key_replay_returns_the_id_that_holds_the_key(self) -> None:
        broker = _make_broker()
        engine, _conn = _make_enqueue_engine(outcome='KEY_REPLAY', task_id=_HOLDER_TASK_ID)
        _set_resources(broker, engine=engine)

        result = await broker.enqueue_async(
            'my_task',
            task_id=_SENT_TASK_ID,
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            idempotency_key='key',
        )

        assert result == Ok(_HOLDER_TASK_ID)

    @pytest.mark.asyncio
    async def test_a_key_conflict_names_the_owner(self) -> None:
        broker = _make_broker()
        engine, _conn = _make_enqueue_engine(outcome='KEY_CONFLICT', task_id=_HOLDER_TASK_ID)
        _set_resources(broker, engine=engine)

        result = await broker.enqueue_async(
            'my_task',
            task_id=_SENT_TASK_ID,
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            idempotency_key='key',
        )

        match result:
            case Err(BrokerIdempotencyKeyConflict(owner_task_id=owner)):
                assert owner == _HOLDER_TASK_ID
            case other:
                raise AssertionError(f'expected IDEMPOTENCY_KEY_CONFLICT, got {other!r}')

    @pytest.mark.asyncio
    async def test_a_payload_mismatch_is_not_retryable(self) -> None:
        broker = _make_broker()
        engine, _conn = _make_enqueue_engine(outcome='PAYLOAD_MISMATCH', task_id=_SENT_TASK_ID)
        _set_resources(broker, engine=engine)

        result = await broker.enqueue_async(
            'my_task',
            task_id=_SENT_TASK_ID,
            enqueue_sha='test-sha',
            send_kind=SendKind.REPEAT,
            sent_at=datetime.now(timezone.utc),
        )

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.PAYLOAD_MISMATCH
        assert result.err_value.retryable is False

    @pytest.mark.asyncio
    async def test_an_unverifiable_conflict_is_a_non_retryable_enqueue_failure(self) -> None:
        broker = _make_broker()
        engine, _conn = _make_enqueue_engine(outcome='CONFLICT_UNVERIFIABLE', task_id=_SENT_TASK_ID)
        _set_resources(broker, engine=engine)

        result = await broker.enqueue_async(
            'my_task',
            task_id=_SENT_TASK_ID,
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
        )

        match result:
            case Err(BrokerEnqueueFailed(message=message, retryable=False, exception=NoCause())):
                assert 'cannot verify payload identity' in message
            case other:
                raise AssertionError(f'expected ENQUEUE_FAILED, got {other!r}')

    @pytest.mark.asyncio
    @pytest.mark.parametrize('send_kind', [SendKind.FIRST, SendKind.REPEAT])
    async def test_the_send_kind_reaches_the_statement(self, send_kind: SendKind) -> None:
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        _ = await broker.enqueue_async(
            'my_task',
            task_id='t',
            enqueue_sha='test-sha',
            send_kind=send_kind,
            sent_at=datetime.now(timezone.utc),
        )

        assert conn.execute.await_args.args[1]['send_kind'] == send_kind.value

    @pytest.mark.asyncio
    async def test_a_keyed_send_carries_its_digest_scope_and_window(self) -> None:
        from horsies.core.history.identity.keys import (
            IDEMPOTENCY_SCOPE_VERSION,
            ScopedIdempotencyKey,
        )

        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        _ = await broker.enqueue_async(
            'my_task',
            task_id='t',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            idempotency_key='key',
        )

        params = conn.execute.await_args.args[1]
        assert params['key_kind'] == 'KEYED'
        assert params['key_digest'] == ScopedIdempotencyKey(task_name='my_task', key='key').digest
        assert params['key_scope_version'] == IDEMPOTENCY_SCOPE_VERSION
        assert params['key_window_us'] == (
            broker.idempotency_reservation_window // timedelta(microseconds=1)
        )


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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.get_raw_result_record_async('missing-id')

        assert is_ok(result)
        assert result.unwrap() is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'status',
        [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.EXPIRED],
    )
    async def test_live_row_with_a_terminal_status_returns_record_with_none_payload(
        self, status: TaskStatus,
    ) -> None:
        """A live row holds no result: a terminal status read from it gives
        Ok(RawResultRecord(raw_result=None)), and only the status probe runs."""
        from horsies.core.brokers.result_types import RawResultRecord

        broker = _make_broker()
        session = _make_result_session(_make_task_row(status=status))
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await broker.get_raw_result_record_async('task-123')

        assert is_ok(result)
        record = result.unwrap()
        assert isinstance(record, RawResultRecord)
        assert record.task_id == 'task-123'
        assert record.task_name == 'my_task'
        assert record.status == status
        assert record.raw_result is None
        assert session.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_cancelled_row_returns_record_with_none_payload(self) -> None:
        """CANCELLED row → Ok(RawResultRecord(raw_result=None))."""
        from horsies.core.brokers.result_types import RawResultRecord

        broker = _make_broker()
        row = _make_task_row(status=TaskStatus.CANCELLED)
        session = _make_result_session(row)
        _set_resources(broker, session_factory=MagicMock(return_value=session))

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
        _listener_of(broker).listen = AsyncMock(
            side_effect=RuntimeError('listener down; force polling'),
        )
        running_row = _make_task_row(status=TaskStatus.RUNNING)
        session = _make_result_session(running_row)
        _set_resources(broker, session_factory=MagicMock(return_value=session))

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
    async def test_cancelled_error_propagates(self) -> None:
        """``asyncio.CancelledError`` must re-raise; no broker-error wrap."""
        broker = _make_broker()
        session = _make_result_session(None)
        session.execute = AsyncMock(side_effect=asyncio.CancelledError)
        _set_resources(broker, session_factory=MagicMock(return_value=session))

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
        _listener_of(broker).listen_payload = AsyncMock(return_value=Ok(q))

        session = _make_result_session(
            _make_task_row(status=TaskStatus.RUNNING),
        )
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        started = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def _unsubscribe(
            _channel: str, _payload: str, _queue: object,
        ) -> None:
            started.set()
            await release.wait()
            finished.set()

        _listener_of(broker).unsubscribe_payload = AsyncMock(
            side_effect=_unsubscribe,
        )

        task = asyncio.create_task(
            broker.get_raw_result_record_async(
                'task-123', timeout_ms=60_000,
            )
        )

        for _ in range(50):
            if _listener_of(broker).listen_payload.await_count > 0:
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
        _listener_of(broker).unsubscribe_payload.assert_awaited_once_with(
            'task_done', 'task-123', q,
        )


# ---------------------------------------------------------------------------
# TestMonitoringQueries
# ---------------------------------------------------------------------------


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
        'queue_priorities': {},
        'queue_max_concurrency': {},
        'recovery_config': {},
        'tasks_running': 2,
        'tasks_claimed': 1,
        'memory_usage_mb': 12.5,
        'memory_percent': 0.5,
        'cpu_percent': 3.0,
        'children_memory_mb': 250.0,
        'worker_started_at': now,
        'slots_down': [],
        'class_capacity': {
            'ISOLATED': {
                'capacity': 4,
                'serves': ['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'],
            },
        },
    }
    mapping.update(overrides)
    return SimpleNamespace(_mapping=mapping)


def _setup_session_returning(
    broker: Any,
    *,
    fetchall: list[Any] | None = None,
    fetchone: Any = None,
) -> AsyncMock:
    """Configure the pinned session_factory to return a result with given rows."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = fetchall if fetchall is not None else []
    mock_result.fetchone.return_value = fetchone

    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.execute = AsyncMock(return_value=mock_result)
    session.commit = AsyncMock()
    _set_resources(broker, session_factory=MagicMock(return_value=session))
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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

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
        assert isinstance(result.err_value, BrokerInvalidInput)
        assert result.err_value.exception == NoCause()

    @pytest.mark.asyncio
    async def test_history_returns_snapshots(self) -> None:
        broker = _make_broker()
        _setup_session_returning(
            broker, fetchall=[_worker_state_row(), _worker_state_row()]
        )

        result = await broker.get_worker_state_history_async('worker-1', limit=10)

        assert is_ok(result)
        assert len(result.ok_value) == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('stored', 'expected'),
        [
            ([], AllSlotsServing()),
            (
                [
                    {
                        'slot': 3,
                        'cause': {'kind': 'resource_shortage', 'errno': 'EAGAIN'},
                        'detail': 'OSError: [Errno 11] Resource temporarily unavailable',
                        'down_since': '2026-01-01T00:00:00+00:00',
                        'failed_boots': 5,
                    },
                ],
                SlotsDown(
                    (
                        DownSlot(
                            3,
                            ResourceShortage(ShortageErrno.EAGAIN),
                            'OSError: [Errno 11] Resource temporarily unavailable',
                            datetime(2026, 1, 1, tzinfo=timezone.utc),
                            5,
                        ),
                    ),
                ),
            ),
        ],
        ids=['all-serving', 'one-down'],
    )
    async def test_the_snapshot_carries_the_slot_health(
        self,
        stored: object,
        expected: object,
    ) -> None:
        broker = _make_broker()
        _setup_session_returning(broker, fetchone=_worker_state_row(slots_down=stored))

        result = await broker.get_worker_state_async('worker-1')

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.slot_health == expected

    @pytest.mark.asyncio
    async def test_an_unreadable_slot_health_fails_the_read(self) -> None:
        broker = _make_broker()
        _setup_session_returning(
            broker,
            fetchone=_worker_state_row(slots_down=[{'slot': 9}]),
        )

        result = await broker.get_worker_state_async('worker-1')

        assert is_err(result)
        assert 'slots_down is unreadable' in result.err_value.message
        assert 'worker-1' in result.err_value.message

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('stored', 'expected'),
        [
            ({}, ClassBudgets(())),
            (
                {'ISOLATED': {'capacity': 2, 'serves': ['ISOLATED']}},
                ClassBudgets(
                    (ClassBudget(Execution.ISOLATED, 2, (Execution.ISOLATED,)),),
                ),
            ),
        ],
        ids=['serves-no-class', 'one-budget'],
    )
    async def test_the_snapshot_carries_the_class_capacity(
        self,
        stored: object,
        expected: object,
    ) -> None:
        broker = _make_broker()
        _setup_session_returning(
            broker,
            fetchone=_worker_state_row(class_capacity=stored),
        )

        result = await broker.get_worker_state_async('worker-1')

        assert is_ok(result)
        assert result.ok_value is not None
        assert result.ok_value.class_capacity == expected

    @pytest.mark.asyncio
    async def test_an_unreadable_class_capacity_fails_the_read(self) -> None:
        broker = _make_broker()
        _setup_session_returning(
            broker,
            fetchone=_worker_state_row(class_capacity={'ISOLATED': {'capacity': -1}}),
        )

        result = await broker.get_worker_state_async('worker-1')

        assert is_err(result)
        assert 'class_capacity is unreadable' in result.err_value.message
        assert 'worker-1' in result.err_value.message


@pytest.mark.unit
class TestClassReadOutcomes:
    """The error side of ``get_class_coverage_async`` and
    ``get_class_report_async``. The Ok side and the refusals run against a
    real database in ``tests/integration/test_class_coverage.py``."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize('method', ['get_class_coverage_async', 'get_class_report_async'])
    async def test_a_driver_error_is_a_retryable_monitoring_failure(
        self,
        method: str,
    ) -> None:
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(
            side_effect=SAOperationalError('SELECT', None, Exception('conn lost')),
        )
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        result = await getattr(broker, method)()

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.MONITORING_QUERY_FAILED
        assert result.err_value.retryable is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize('method', ['get_class_coverage_async', 'get_class_report_async'])
    async def test_the_loop_limit_runs_nothing(self, method: str) -> None:
        broker = _make_broker()
        attach_loop_limit(broker)

        result = await getattr(broker, method)()

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.EVENT_LOOP_LIMIT

    @pytest.mark.asyncio
    async def test_a_code_defect_is_not_turned_into_a_monitoring_failure(self) -> None:
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=AssertionError('defect'))
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        with pytest.raises(AssertionError):
            await broker.get_class_coverage_async()


@pytest.mark.unit
class TestPingWorkers:
    """ping_workers_async validation and pong decoding."""

    @pytest.mark.asyncio
    async def test_rejects_nonpositive_timeout(self) -> None:
        broker = _make_broker()

        result = await broker.ping_workers_async(timeout_seconds=0)

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidInput)
        assert 'timeout_seconds' in result.err_value.message

    @pytest.mark.asyncio
    async def test_disabled_listener_returns_err_not_raise(self) -> None:
        """A broker with no listener must Err, not raise (Result contract)."""
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        config = PostgresConfig(
            database_url='postgresql://u:p@localhost/db'
        )
        broker = PostgresBroker(config, assume_initialized=True)
        assert isinstance((await resources_of(broker)).listener, ListenerDisabled)

        result = await broker.ping_workers_async(timeout_seconds=1.0)

        assert is_err(result)
        assert isinstance(result.err_value, BrokerListenerDisabled)
        assert result.err_value.code is BrokerErrorCode.LISTENER_DISABLED

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
        assert isinstance(result.err_value, BrokerInvalidInput)
        assert 'min_responses' in result.err_value.message

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

        _listener_of(broker).listen = AsyncMock(return_value=Ok(reply_queue))
        _listener_of(broker).unsubscribe = AsyncMock()
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

    @pytest.mark.asyncio
    async def test_request_encode_failure_is_a_defect_not_an_err(self) -> None:
        """The library builds the ping request; an encode failure raises before any subscribe."""
        broker = _make_broker()
        _listener_of(broker).listen = AsyncMock()

        with patch(
            'horsies.core.brokers.postgres.dumps_json',
            return_value=Err(SerializationError('cannot encode')),
        ):
            with pytest.raises(AssertionError, match='ping_workers request encode failed'):
                await broker.ping_workers_async(timeout_seconds=1.0)

        _listener_of(broker).listen.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_refused_request_field_gives_text_and_cause_without_value(self) -> None:
        """A request field that fails validation names the field, not the value."""
        from horsies.core.codec.validation_errors import ValueValidationError

        marker = 'ping-marker-41d0'
        broker = _make_broker()
        reply_queue: asyncio.Queue[Any] = asyncio.Queue()
        _listener_of(broker).listen = AsyncMock(return_value=Ok(reply_queue))
        _listener_of(broker).unsubscribe = AsyncMock()

        result = await broker.ping_workers_async(
            target_worker_id=cast(Any, {'secret': marker}),
            timeout_seconds=1.0,
        )

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, BrokerInvalidInput)
        assert 'target_worker_id: Input should be a valid string' in err.message
        assert marker not in err.message
        assert isinstance(err.exception, ValueValidationError)
        assert marker not in str(err.exception)
        _listener_of(broker).listen.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestMarkStaleTasksAsFailed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMarkStaleTasksAsFailed:
    """mark_stale_tasks_as_failed maps the record of one core pass to its result."""

    @staticmethod
    def _handle(answer: object) -> MagicMock:
        """A core handle whose stale RUNNING pass answers ``answer``: a
        record, or an exception that the call raises."""
        handle = MagicMock()
        match answer:
            case BaseException():
                handle.mark_stale_running_async = AsyncMock(side_effect=answer)
            case _:
                handle.mark_stale_running_async = AsyncMock(return_value=answer)
        return handle

    @pytest.mark.asyncio
    async def test_a_pass_returns_the_recovered_count(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=StaleRunningRecovered, recovered=3),
        )

        result = await broker.mark_stale_tasks_as_failed(
            stale_threshold_ms=300_000,
            finalizing_stale_threshold_ms=60_000,
        )

        assert result == Ok(3)
        broker._core_handle.mark_stale_running_async.assert_awaited_once_with(
            300_000, 60_000,
        )

    @pytest.mark.asyncio
    async def test_a_full_scan_batch_warns_that_the_rest_waits(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=StaleRunningScanFull, recovered=7, scan_batch=1000),
        )

        with caplog.at_level('WARNING', logger='horsies.broker'):
            result = await broker.mark_stale_tasks_as_failed()

        assert result == Ok(7)
        assert 'read a full scan batch of 1000 candidates' in caplog.text

    @pytest.mark.asyncio
    async def test_a_failed_scan_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=StaleRunningScanFailed,
                message='stale scan: connection lost',
                retryable=True,
            ),
        )

        result = await broker.mark_stale_tasks_as_failed()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.message == 'mark_stale_tasks_as_failed failed: stale scan: connection lost'
        assert isinstance(failed.exception, CoreOperationFailed)

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_pass_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.mark_stale_running_async', 65_536)
        broker._core_handle = self._handle(error)

        result = await broker.mark_stale_tasks_as_failed()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.parametrize(
        ('stale_ms', 'finalizing_ms'),
        [(SIGNED_64_BIT_MAX + 1, 60_000), (60_000, SIGNED_64_BIT_MIN - 1)],
        ids=['stale', 'finalizing'],
    )
    @pytest.mark.asyncio
    async def test_a_threshold_past_64_bits_is_refused_before_the_core(
        self, stale_ms: int, finalizing_ms: int,
    ) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(MagicMock(spec=StaleRunningRecovered, recovered=0))

        result = await broker.mark_stale_tasks_as_failed(
            stale_threshold_ms=stale_ms,
            finalizing_stale_threshold_ms=finalizing_ms,
        )

        assert is_err(result)
        failed = result.err_value
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is False
        assert isinstance(failed.exception, OverflowError)
        broker._core_handle.mark_stale_running_async.assert_not_awaited()


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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

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
        _set_resources(broker, session_factory=MagicMock(return_value=session))
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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(side_effect=Exception('db down')),
        ):
            result = await broker.expire_pending_tasks()
        assert not is_ok(result)


# ---------------------------------------------------------------------------
# TestTerminateOrphanedWorkflowTasks
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTerminateOrphanedWorkflowTasks:
    """Tests for the bounded orphan discovery operation."""

    @pytest.mark.asyncio
    async def test_no_orphans_returns_zero(self) -> None:
        from horsies.core.lifecycle.commands import CancelOrphanedTasks

        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute.return_value = _orphan_scan_stats(0, 0)
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(return_value=[]),
        ) as apply_batch:
            result = await broker.terminate_orphaned_workflow_tasks()

        assert is_ok(result)
        assert result.ok_value == 0
        assert apply_batch.await_args is not None
        command = apply_batch.await_args.args[1]
        assert isinstance(command, CancelOrphanedTasks)
        assert command.batch_size == 500
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_counts_only_transitioned_rows(self) -> None:
        from horsies.core.lifecycle.outcomes import Applied

        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute.return_value = _orphan_scan_stats(3, 3)
        _set_resources(broker, session_factory=MagicMock(return_value=session))
        outcomes = [MagicMock(spec=Applied) for _ in range(3)]

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(return_value=outcomes),
        ):
            result = await broker.terminate_orphaned_workflow_tasks()

        assert is_ok(result)
        assert result.ok_value == 3
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_audit_reports_cursor_metrics(self) -> None:
        from horsies.core.lifecycle.outcomes import Applied

        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute.return_value = _orphan_scan_stats(500, 2)
        _set_resources(broker, session_factory=MagicMock(return_value=session))
        outcomes = [MagicMock(spec=Applied) for _ in range(2)]

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(return_value=outcomes),
        ):
            result = await broker.audit_orphaned_workflow_tasks()

        assert is_ok(result)
        report = result.ok_value
        assert report.rows_selected == 500
        assert report.candidates_returned == 2
        assert report.cancelled == 2
        assert report.health()['state'] == 'ready'
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_audit_refuses_cursor_outcome_count_drift(self) -> None:
        from horsies.core.lifecycle.outcomes import Applied

        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute.return_value = _orphan_scan_stats(10, 2)
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(return_value=[MagicMock(spec=Applied)]),
        ):
            result = await broker.audit_orphaned_workflow_tasks()

        assert not is_ok(result)
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_full_batch_stops_after_one_cursor_page(self) -> None:
        from horsies.core.lifecycle.outcomes import Applied

        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute.return_value = _orphan_scan_stats(500, 500)
        _set_resources(broker, session_factory=MagicMock(return_value=session))
        full = [MagicMock(spec=Applied) for _ in range(500)]
        final = [MagicMock(spec=Applied) for _ in range(2)]

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(side_effect=[full, final]),
        ) as apply_batch:
            result = await broker.terminate_orphaned_workflow_tasks()

        assert is_ok(result)
        assert result.ok_value == 500
        assert apply_batch.await_count == 1
        assert session.commit.await_count == 1

    @pytest.mark.asyncio
    async def test_a_non_applied_outcome_fails_closed(self) -> None:
        from horsies.core.lifecycle.outcomes import TaskAbsent

        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(return_value=[MagicMock(spec=TaskAbsent)]),
        ):
            result = await broker.terminate_orphaned_workflow_tasks()

        assert not is_ok(result)
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_database_error_returns_err(self) -> None:
        broker = _make_broker()
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        _set_resources(broker, session_factory=MagicMock(return_value=session))

        with patch(
            'horsies.core.brokers.postgres.apply_batch_async',
            new=AsyncMock(side_effect=Exception('db down')),
        ):
            result = await broker.terminate_orphaned_workflow_tasks()

        assert not is_ok(result)
        session.commit.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestRequeueStaleClaimed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRequeueStaleClaimed:
    """requeue_stale_claimed maps the record of one core pass to its result."""

    @staticmethod
    def _handle(outcome: object) -> MagicMock:
        """A core handle whose requeue pass answers ``outcome``."""
        handle = MagicMock()
        handle.requeue_stale_claimed_async = AsyncMock(return_value=outcome)
        return handle

    @pytest.mark.asyncio
    async def test_a_drained_pass_returns_its_count(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=StaleClaimsDrained, requeued=5),
        )

        result = await broker.requeue_stale_claimed(stale_threshold_ms=120_000)

        assert result == Ok(5)
        broker._core_handle.requeue_stale_claimed_async.assert_awaited_once_with(120_000)

    @pytest.mark.asyncio
    async def test_a_pass_at_the_batch_cap_returns_its_count_with_a_warning(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(spec=StaleClaimsBoundReached, requeued=100_000),
        )

        with caplog.at_level('WARNING', logger='horsies.broker'):
            result = await broker.requeue_stale_claimed()

        assert result == Ok(100_000)
        assert 'reached the per-pass batch cap' in caplog.text

    @pytest.mark.asyncio
    async def test_a_failed_pass_is_cleanup_failed_with_the_rows_before_it(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(
            MagicMock(
                spec=StaleClaimsRequeueFailed,
                requeued_before=7,
                message='requeue batch: connection lost',
                retryable=True,
            ),
        )

        result = await broker.requeue_stale_claimed()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.message == (
            'requeue_stale_claimed failed after requeueing 7 task(s): '
            'requeue batch: connection lost'
        )
        assert isinstance(failed.exception, CoreOperationFailed)
        assert failed.exception.subject == 'PostgresBroker.requeue_stale_claimed'

    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_pass_is_cleanup_failed(self) -> None:
        broker = _make_broker()
        error = TooManyPendingCalls('CoreHandle.requeue_stale_claimed_async', 65_536)
        handle = MagicMock()
        handle.requeue_stale_claimed_async = AsyncMock(side_effect=error)
        broker._core_handle = handle

        result = await broker.requeue_stale_claimed()

        assert is_err(result)
        failed = result.err_value
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is True
        assert failed.exception is error

    @pytest.mark.asyncio
    async def test_a_threshold_past_64_bits_is_refused_before_the_core(self) -> None:
        broker = _make_broker()
        broker._core_handle = self._handle(MagicMock(spec=StaleClaimsDrained, requeued=0))

        result = await broker.requeue_stale_claimed(
            stale_threshold_ms=SIGNED_64_BIT_MAX + 1,
        )

        assert is_err(result)
        failed = result.err_value
        assert failed.code is BrokerErrorCode.CLEANUP_FAILED
        assert failed.retryable is False
        assert isinstance(failed.exception, OverflowError)
        broker._core_handle.requeue_stale_claimed_async.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestSyncFacades
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSyncFacades:
    """Tests for sync wrappers: enqueue, get_result, get_task_info."""

    def test_enqueue_on_the_caller_thread_route_skips_the_loop_runner(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """On ``SyncSendRoute.CALLER_THREAD``, enqueue() writes through
        ``_enqueue_on_caller_thread`` and does not call the loop runner."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()

        def _route() -> SyncSendRoute:
            return SyncSendRoute.CALLER_THREAD

        monkeypatch.setattr(broker, 'send_route', _route)
        on_caller = MagicMock(return_value='task-id-456')
        monkeypatch.setattr(broker, '_enqueue_on_caller_thread', on_caller)

        result = broker.enqueue(
            'my_task',
            'queue',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            kwargs_json='{"k": "v"}',
            priority=50,
            idempotency_key='order-17',
        )

        assert result == 'task-id-456'
        broker._loop_runner.call.assert_not_called()
        assert on_caller.call_args.args == ('my_task', 'queue')
        assert on_caller.call_args.kwargs['task_id'] == 'test-task-id'
        assert on_caller.call_args.kwargs['priority'] == 50
        assert on_caller.call_args.kwargs['idempotency_key'] == 'order-17'

    def test_enqueue_delegates_to_loop_runner(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """On ``SyncSendRoute.LOOP_RUNNER``, enqueue() calls
        _loop_runner.call with enqueue_async."""
        broker = _make_broker()
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(return_value='task-id-123')

        def _route() -> SyncSendRoute:
            return SyncSendRoute.LOOP_RUNNER

        monkeypatch.setattr(broker, 'send_route', _route)

        result = broker.enqueue(
            'my_task',
            'queue',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
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


TASK_INFO_ID = UUID('0198c0de-0000-7000-8000-0000000000ab')


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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

    @pytest.mark.asyncio
    async def test_task_not_found_returns_none(self) -> None:
        """No live row and no history row: Ok(None)."""
        broker = _make_broker()
        self._setup_task_info_session(broker, row=None)

        with patch(
            'horsies.core.history.reads.detail.read_task_detail',
            AsyncMock(
                return_value=TaskDetailAbsent(
                    task_id=TASK_INFO_ID,
                    reason=TaskAbsenceReason.WITHIN_RETAINED_HISTORY,
                )
            ),
        ):
            result = await broker.get_task_info_async(TASK_INFO_ID)

        assert is_ok(result)
        assert result.ok_value is None

    @pytest.mark.asyncio
    async def test_basic_task_returns_task_info(self) -> None:
        """Basic task (no optional includes) should return Ok(TaskInfo) with base fields."""
        broker = _make_broker()
        now = datetime.now(timezone.utc)
        # 18 base columns: task_name, status, queue_name, priority,
        # retry_count, max_retries, next_retry_at, sent_at, enqueued_at, claimed_at,
        # started_at, completed_at, failed_at, worker_hostname, worker_pid,
        # worker_process_name, error_code, execution
        row = (
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
            'ISOLATED',
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async(TASK_INFO_ID)

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert isinstance(info, TaskInfo)
        assert info.task_id == TASK_INFO_ID
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
        assert info.execution is Execution.ISOLATED

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
            'ISOLATED',
            result_json,
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async(TASK_INFO_ID, include_result=True)

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
            'ISOLATED',
            'Worker crashed unexpectedly',
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async(
            TASK_INFO_ID, include_failed_reason=True
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
            'ISOLATED',
            None,
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async(TASK_INFO_ID, include_result=True)

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
            'ISOLATED',
            result_json,
            'Something broke',
        )
        self._setup_task_info_session(broker, row=row)

        result = await broker.get_task_info_async(
            TASK_INFO_ID,
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

        _listener_of(broker).close = mock_listener_close
        _pinned(broker).engine.dispose = mock_engine_dispose
        await resources_of(broker)  # build this loop's set

        result = await broker.close_async()

        assert is_ok(result)
        assert result.ok_value is None
        assert call_order == ['listener_close', 'engine_dispose']

    @pytest.mark.asyncio
    async def test_close_async_without_a_built_set_closes_nothing(self) -> None:
        """A broker that never made a call holds no set, so close touches nothing."""
        broker = _make_broker()

        result = await broker.close_async()

        assert is_ok(result)
        _listener_of(broker).close.assert_not_awaited()
        _pinned(broker).engine.dispose.assert_not_awaited()

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

    def test_close_stops_runner_on_loop_runner_error(self) -> None:
        """close() returns Err(LOOP_RUNNER_FAILED) and still stops loop_runner."""
        broker = _make_broker()
        runner_error = LoopRunnerError('Loop runner is not running')
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(side_effect=runner_error)
        broker._loop_runner.stop = MagicMock()

        result = broker.close()

        assert is_err(result)
        assert result.err_value.code == BrokerErrorCode.LOOP_RUNNER_FAILED
        assert result.err_value.retryable is False
        assert result.err_value.exception is runner_error
        # stop is called in finally block
        broker._loop_runner.stop.assert_called_once()

    def test_close_stops_runner_when_an_exception_propagates(self) -> None:
        """An exception that is not a LoopRunnerError propagates; stop still runs."""
        broker = _make_broker()
        defect = RuntimeError('boom')
        broker._loop_runner = MagicMock()
        broker._loop_runner.call = MagicMock(side_effect=defect)
        broker._loop_runner.stop = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            broker.close()

        assert exc_info.value is defect
        broker._loop_runner.stop.assert_called_once()


# ---------------------------------------------------------------------------
# New error path tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnqueueAsyncErrorPaths:
    """Regression tests for error-path behavior in enqueue_async."""

    @pytest.mark.asyncio
    async def test_enqueue_async_connection_error_returns_retryable_err(self) -> None:
        """OperationalError during the insert should produce a retryable Err(ENQUEUE_FAILED)."""
        from sqlalchemy.exc import OperationalError

        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        # Simulate a connection-level error that psycopg raises on execute
        conn.execute = AsyncMock(
            side_effect=OperationalError('insert failed', None, Exception('conn lost'))
        )
        _set_resources(broker, engine=engine)

        result = await broker.enqueue_async(
            'my_task',
            task_id='test-task-id',
            enqueue_sha='test-sha',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
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
        _set_resources(broker, session_factory=MagicMock(return_value=session))

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
class TestBaselineInstall:
    """The install pass: no lock on a current database, lock and act otherwise."""

    @staticmethod
    def _current() -> Any:
        return Ok(
            BaselineCurrent(
                family=FamilyRecorded(family=ClientFamily.PYTHON),
                readers=ReadersPublished(),
            )
        )

    @staticmethod
    def _readers_missing() -> Any:
        return Ok(
            BaselineCurrent(
                family=FamilyRecorded(family=ClientFamily.PYTHON),
                readers=ReadersMissing(),
            )
        )

    @pytest.mark.asyncio
    async def test_current_database_takes_no_lock_and_writes_nothing(self) -> None:
        broker = _make_broker()
        connection = _schema_lock_connection()
        install = AsyncMock(return_value=Ok(None))
        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=connection),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(return_value=self._current()),
            ),
            patch('horsies.core.brokers.postgres.install_baseline', new=install),
        ):
            await broker._maybe_run_schema_init(MagicMock())

        install.assert_not_awaited()
        statements = [call.args[0] for call in connection.execute.await_args_list]
        assert 'SELECT pg_try_advisory_lock(%s)' not in statements
        connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_database_is_installed_and_published_under_the_lock(self) -> None:
        broker = _make_broker()
        connection = _schema_lock_connection()
        engine = MagicMock()
        install = AsyncMock(return_value=Ok(None))
        publish = AsyncMock(return_value=Ok(None))
        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=connection),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(side_effect=[Ok(EmptyDatabase()), Ok(EmptyDatabase())]),
            ) as read_state,
            patch('horsies.core.brokers.postgres.install_baseline', new=install),
            patch('horsies.core.brokers.postgres.publish_history_readers', new=publish),
        ):
            await broker._maybe_run_schema_init(engine)

        assert read_state.await_count == 2
        install.assert_awaited_once()
        publish.assert_awaited_once_with(engine)
        statements = [call.args[0] for call in connection.execute.await_args_list]
        assert statements.count('SELECT pg_try_advisory_lock(%s)') == 1
        assert statements.count('SELECT pg_advisory_unlock(%s)') == 1

    @pytest.mark.asyncio
    async def test_missing_readers_are_published_without_a_second_migration(
        self,
    ) -> None:
        broker = _make_broker()
        engine = MagicMock()
        install = AsyncMock(return_value=Ok(None))
        publish = AsyncMock(return_value=Ok(None))
        record_family = AsyncMock(return_value=Ok(None))
        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=_schema_lock_connection()),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(
                    side_effect=[
                        self._readers_missing(),
                        self._readers_missing(),
                        self._current(),
                    ]
                ),
            ) as read_state,
            patch('horsies.core.brokers.postgres.install_baseline', new=install),
            patch('horsies.core.brokers.postgres.publish_history_readers', new=publish),
            patch(
                'horsies.core.brokers.postgres.record_client_family', new=record_family
            ),
        ):
            await broker._maybe_run_schema_init(engine)

        assert read_state.await_count == 3
        install.assert_not_awaited()
        publish.assert_awaited_once_with(engine)
        record_family.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failed_publication_raises_its_cause_with_the_step(self) -> None:
        broker = _make_broker()
        failure = SchemaStatementFailed(
            step='publish the history readers',
            cause=psycopg.errors.InsufficientPrivilege('permission denied'),
        )
        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=_schema_lock_connection()),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(side_effect=[Ok(EmptyDatabase()), Ok(EmptyDatabase())]),
            ),
            patch(
                'horsies.core.brokers.postgres.install_baseline',
                new=AsyncMock(return_value=Ok(None)),
            ),
            patch(
                'horsies.core.brokers.postgres.publish_history_readers',
                new=AsyncMock(return_value=Err(failure)),
            ),
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege) as raised:
                await broker._maybe_run_schema_init(MagicMock())

        assert raised.value.__notes__ == [
            'schema install step: publish the history readers',
        ]

    @pytest.mark.asyncio
    async def test_install_finished_by_another_broker_while_waiting_is_not_repeated(
        self,
    ) -> None:
        broker = _make_broker()
        install = AsyncMock(return_value=Ok(None))
        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=_schema_lock_connection()),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(side_effect=[Ok(EmptyDatabase()), self._current()]),
            ),
            patch('horsies.core.brokers.postgres.install_baseline', new=install),
        ):
            await broker._maybe_run_schema_init(MagicMock())

        install.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_foreign_database_is_refused_with_its_description(self) -> None:
        broker = _make_broker()
        install = AsyncMock(return_value=Ok(None))
        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=_schema_lock_connection()),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(return_value=Ok(SchemaNewer(stored=1001, own=1000))),
            ),
            patch('horsies.core.brokers.postgres.install_baseline', new=install),
        ):
            with pytest.raises(RuntimeError, match='newer than this build supports'):
                await broker._maybe_run_schema_init(MagicMock())

        install.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_read_only_broker_runs_no_schema_statement(self) -> None:
        broker = _make_broker()
        broker.run_schema_migrations = False
        connect = AsyncMock()
        with patch(
            'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
            new=connect,
        ):
            await broker._maybe_run_schema_init(MagicMock())

        connect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_schema_deadlock_retries_the_install(self) -> None:
        broker = _make_broker()
        deadlock = SchemaStatementFailed(
            step='install the baseline schema',
            cause=psycopg.errors.DeadlockDetected('deadlock detected'),
        )
        install = AsyncMock(side_effect=[Err(deadlock), Ok(None)])
        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=_schema_lock_connection()),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(return_value=Ok(EmptyDatabase())),
            ),
            patch('horsies.core.brokers.postgres.install_baseline', new=install),
            patch(
                'horsies.core.brokers.postgres.publish_history_readers',
                new=AsyncMock(return_value=Ok(None)),
            ),
            patch(
                'horsies.core.brokers.postgres.random.uniform',
                return_value=0,
            ),
            patch(
                'horsies.core.brokers.postgres.asyncio.sleep',
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            await broker._maybe_run_schema_init(MagicMock())

        assert install.await_count == 2
        mock_sleep.assert_awaited_once_with(0.05)

    @pytest.mark.asyncio
    async def test_other_statement_failure_raises_its_cause_with_the_step(self) -> None:
        broker = _make_broker()
        failure = SchemaStatementFailed(
            step='read the schema state',
            cause=psycopg.errors.InsufficientPrivilege('permission denied'),
        )
        with (
            patch(
                'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
                new=AsyncMock(return_value=_schema_lock_connection()),
            ),
            patch(
                'horsies.core.brokers.postgres.read_database_state',
                new=AsyncMock(return_value=Err(failure)),
            ),
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege) as raised:
                await broker._maybe_run_schema_init(MagicMock())

        assert raised.value.__notes__ == ['schema install step: read the schema state']


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

        _listener_of(broker).close = failing_listener_close
        _pinned(broker).engine.dispose = tracking_engine_dispose
        await resources_of(broker)  # build this loop's set

        result = await broker.close_async()

        assert (
            dispose_called
        ), 'engine.dispose must be called even after listener.close fails'
        assert is_err(result)
        err = result.err_value
        assert err.code == BrokerErrorCode.CLOSE_FAILED
        assert err.exception is not None


@pytest.mark.unit
class TestHistoryTaskInfoResultDecode:
    """A stored result outside strict JSON is refused, as a live read refuses it."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'payload',
        [
            b'{"x":NaN}',
            b'{"x":1e9999}',
            b'{"x":' + b'1' * 5000 + b'}',
            b'',
        ],
    )
    async def test_stored_result_is_refused_as_invalid_json_payload(
        self, payload: bytes,
    ) -> None:
        detail = MagicMock(spec=HistoryTaskDetail)
        detail.status = 'FAILED'
        detail.terminal_at = datetime(2026, 8, 7, tzinfo=timezone.utc)
        detail.result_payload = payload
        detail.result_digest = archive_digest(payload)
        detail.result_envelope_version = 1

        with (
            patch(
                'horsies.core.history.reads.detail.read_task_detail',
                AsyncMock(return_value=detail),
            ),
        ):
            result = await _make_broker()._history_task_info(
                AsyncMock(),
                '0192b3a4-0000-7000-8000-000000000001',
                include_result=True,
                include_failed_reason=False,
                include_attempts=False,
            )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidJsonPayload(message=message):
                assert 'invalid_json' in message
            case other:
                raise AssertionError(f'expected INVALID_JSON_PAYLOAD, got {other!r}')


def _attempt_record(attempt: int) -> AttemptRecord:
    started_at = datetime(2026, 8, 7, attempt, tzinfo=timezone.utc)
    return AttemptRecord(
        attempt=attempt,
        outcome='FAILED',
        will_retry=attempt < 3,
        started_at=started_at,
        finished_at=started_at + timedelta(minutes=1),
        error_code='BOOM',
        error_message=None,
        failed_reason=None,
        worker_id='w-1',
        worker_hostname='host-1',
        worker_pid=100 + attempt,
        worker_process_name=None,
    )


@pytest.mark.unit
class TestHistoryTaskInfoAttemptOrder:
    """A history read lists the attempts newest first, as the live read does."""

    @pytest.mark.asyncio
    async def test_attempts_are_listed_newest_first(self) -> None:
        task_id = UUID('0192b3a4-0000-7000-8000-000000000002')
        detail = MagicMock(spec=HistoryTaskDetail)
        detail.task_id = task_id
        detail.status = TaskStatus.FAILED
        detail.terminal_at = datetime(2026, 8, 7, 4, tzinfo=timezone.utc)
        # The snapshot decodes oldest first.
        detail.attempts = tuple(_attempt_record(n) for n in (1, 2, 3))

        with (
            patch(
                'horsies.core.history.reads.detail.read_task_detail',
                AsyncMock(return_value=detail),
            ),
        ):
            result = await _make_broker()._history_task_info(
                AsyncMock(),
                task_id,
                include_result=False,
                include_failed_reason=False,
                include_attempts=True,
            )

        assert is_ok(result)
        info = result.ok_value
        assert info is not None
        assert info.attempts is not None
        assert [a.attempt for a in info.attempts] == [3, 2, 1]
        assert [a.worker_pid for a in info.attempts] == [103, 102, 101]


class _DatabaseMustNotRun:
    """Broker wiring for a refused input: any database use fails the test."""

    def __init__(self) -> None:
        self.broker = _make_broker()
        self.engine, self.conn = _make_enqueue_engine()
        self.session = _make_enqueue_session()
        _set_resources(
            self.broker,
            engine=self.engine,
            session_factory=MagicMock(return_value=self.session),
        )

    def assert_nothing_ran(self) -> None:
        self.conn.execute.assert_not_awaited()
        self.session.execute.assert_not_awaited()


REFUSED_ARGS = ['5', '"abc"', '{"a":1}', '0', 'false', '""', '{}', '[1]', '[null]', '']
REFUSED_KWARGS = ['5', '"a"', '[1]', 'true', '0', 'false', '""', '[]', '']


@pytest.mark.unit
class TestEnqueueInputRefusal:
    """A refused input is refused before the database, with its own code."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize('args_json', REFUSED_ARGS)
    async def test_args_shape_is_refused(self, args_json: str) -> None:
        wiring = _DatabaseMustNotRun()

        result = await wiring.broker.enqueue_async(
            'my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), args_json=args_json,
        )

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidInput)
        assert 'args JSON' in result.err_value.message
        wiring.assert_nothing_ran()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('kwargs_json', REFUSED_KWARGS)
    async def test_kwargs_shape_is_refused(self, kwargs_json: str) -> None:
        wiring = _DatabaseMustNotRun()

        result = await wiring.broker.enqueue_async(
            'my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), kwargs_json=kwargs_json,
        )

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidInput)
        assert 'kwargs JSON' in result.err_value.message
        wiring.assert_nothing_ran()

    @pytest.mark.asyncio
    async def test_shape_refusal_text_holds_no_input_value(self) -> None:
        wiring = _DatabaseMustNotRun()

        result = await wiring.broker.enqueue_async(
            'my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), kwargs_json='"secret-value"',
        )

        assert is_err(result)
        assert 'secret-value' not in result.err_value.message

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'overrides',
        [
            pytest.param({'task_name': ''}, id='empty-task-name'),
            pytest.param({'queue_name': ''}, id='empty-queue-name'),
            pytest.param({'priority': 0}, id='priority-below'),
            pytest.param({'priority': 101}, id='priority-above'),
            pytest.param({'good_until': datetime(2030, 1, 1)}, id='naive-good-until'),
            pytest.param({'enqueue_delay_seconds': -1}, id='negative-delay'),
            pytest.param({'retention_class_key': ''}, id='empty-retention-key'),
        ],
    )
    async def test_command_rule_is_refused_with_its_cause(
        self, overrides: dict[str, Any],
    ) -> None:
        wiring = _DatabaseMustNotRun()
        values: dict[str, Any] = {
            'task_name': 'my_task',
            'queue_name': 'default',
            'sent_at': datetime.now(timezone.utc),
        }
        values.update(overrides)

        result = await wiring.broker.enqueue_async(
            task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, **values,
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(exception=cause):
                assert isinstance(cause, ValueError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        wiring.assert_nothing_ran()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'key',
        [
            pytest.param('', id='empty'),
            pytest.param('k' * 256, id='over-the-byte-bound'),
            pytest.param('\ud800', id='not-utf8-encodable'),
        ],
    )
    async def test_idempotency_key_is_refused(self, key: str) -> None:
        wiring = _DatabaseMustNotRun()

        result = await wiring.broker.enqueue_async(
            'my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), idempotency_key=key,
        )

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(exception=cause):
                assert isinstance(cause, ValueError)
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        wiring.assert_nothing_ran()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('field', ['sent_at', 'enqueued_at'])
    async def test_naive_instant_is_refused(self, field: str) -> None:
        wiring = _DatabaseMustNotRun()
        instants: dict[str, datetime] = {
            'sent_at': datetime.now(timezone.utc),
            field: datetime(2020, 1, 1),
        }

        result = await wiring.broker.enqueue_async(
            'my_task',
            task_id='t',
            enqueue_sha='s',
            send_kind=SendKind.FIRST,
            **instants,
        )

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidInput)
        assert f'{field} must be timezone-aware' in result.err_value.message
        wiring.assert_nothing_ran()

    @pytest.mark.asyncio
    async def test_send_with_an_empty_key_is_validation_failed(self) -> None:
        """A normal send reaches this, through the real broker and mapping."""
        wiring = _DatabaseMustNotRun()
        app = MagicMock()
        app.config.queue_mode.name = 'DEFAULT'
        app.config.resend_on_transient_err = False
        app.config.payload = PayloadPolicy()
        app.are_sends_suppressed.return_value = False
        app.validate_queue_name.return_value = 'default'
        app.get_broker.return_value = wiring.broker

        def good_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

        result = await wrapper.with_options(idempotency_key='').send_async(x=1)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendValidationFailed)
        assert result.err_value.code == TaskSendErrorCode.VALIDATION_FAILED
        wiring.assert_nothing_ran()
        # TaskSendRetryable excludes this variant, so a retry is a type error,
        # and at run time it is a defect that crashes loud.
        with pytest.raises(AssertionError, match='unreachable'):
            await wrapper.retry_send_async(result.err_value)  # pyright: ignore[reportArgumentType]

    @pytest.mark.asyncio
    async def test_refused_input_needs_no_schema_init(self) -> None:
        """With the database down, a bad input is still INVALID_INPUT."""
        wiring = _DatabaseMustNotRun()
        wiring.broker._initialized = False
        wiring.broker._ensure_initialized = AsyncMock(
            side_effect=SAOperationalError('connect', None, Exception('down'))
        )

        result = await wiring.broker.enqueue_async(
            'my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), priority=0,
        )

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidInput)
        wiring.broker._ensure_initialized.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_refused_task_options_need_no_schema_init(self) -> None:
        wiring = _DatabaseMustNotRun()
        wiring.broker._ensure_initialized = AsyncMock()

        result = await wiring.broker.enqueue_async(
            'my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc), task_options='{bad',
        )

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidTaskOptions)
        wiring.broker._ensure_initialized.assert_not_awaited()


@pytest.mark.unit
class TestEnqueueAcceptedInput:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('args_json', 'kwargs_json', 'expected_input'),
        [
            (None, None, '{}'),
            ('null', 'null', '{}'),
            ('[]', '{}', '{}'),
            ('[ ]', '{"a":1}', '{"a":1}'),
            (None, '{ "z": 1, "a": 2 }', '{ "z": 1, "a": 2 }'),
        ],
    )
    async def test_the_row_stores_the_kwargs_text_as_its_input(
        self, args_json: str | None, kwargs_json: str | None, expected_input: str,
    ) -> None:
        """The input is the kwargs text exactly as sent; no kwargs store ``{}``.
        The fingerprint covers the stored input in its kwargs element and
        null in its args element."""
        from horsies.core.history.identity.fingerprint import EnqueueCommandV1

        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)

        result = await broker.enqueue_async(
            'my_task',
            task_id='t',
            enqueue_sha='s',
            send_kind=SendKind.FIRST,
            sent_at=datetime.now(timezone.utc),
            args_json=args_json,
            kwargs_json=kwargs_json,
            retain_rerun_input=True,
        )

        assert is_ok(result)
        params = conn.execute.await_args.args[1]
        assert params['input'] == expected_input
        assert params['fingerprint'] == EnqueueCommandV1(
            task_name='my_task',
            queue_name='default',
            priority=100,
            args_json=None,
            kwargs_json=expected_input,
            good_until=None,
            enqueue_delay_seconds=None,
            task_options_json=None,
            retention_class_key='standard_30d',
            retain_rerun_input=True,
            rerun_of_task_id=None,
            rerun_root_task_id=None,
        ).fingerprint


@pytest.mark.unit
class TestEnqueueCatch:
    """Schema init and the write section each state what they catch."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'message',
        [
            'database schema v99 is newer than this build supports (v51)',
            'schema migrations are current but the offline task-history cutover is incomplete',
            'schema advisory lock ownership was lost',
        ],
    )
    async def test_schema_init_operator_state_stays_enqueue_failed(
        self, message: str,
    ) -> None:
        broker = _make_broker()
        engine, _conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)
        state = RuntimeError(message)
        broker._ensure_initialized = AsyncMock(side_effect=state)

        result = await broker.enqueue_async('my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc))

        assert is_err(result)
        match result.err_value:
            case BrokerEnqueueFailed(retryable=retryable, exception=cause):
                assert retryable is False
                assert cause is state
            case other:
                raise AssertionError(f'expected ENQUEUE_FAILED, got {other!r}')

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('error', 'retryable'),
        [
            pytest.param(
                SAOperationalError('INSERT', None, Exception('conn lost')),
                True,
                id='driver-connection-error',
            ),
            pytest.param(SATimeoutError('pool'), True, id='pool-timeout'),
            pytest.param(OSError('socket closed'), False, id='os-error'),
            pytest.param(TimeoutError('timed out'), False, id='timeout-error'),
            pytest.param(
                psycopg.errors.InternalError('boom'), False, id='psycopg-error',
            ),
        ],
    )
    async def test_write_error_keeps_its_classification(
        self, error: Exception, retryable: bool,
    ) -> None:
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)
        conn.execute = AsyncMock(side_effect=error)

        result = await broker.enqueue_async('my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc))

        assert is_err(result)
        match result.err_value:
            case BrokerEnqueueFailed(retryable=flag, exception=cause):
                assert flag is retryable
                assert cause is error
            case other:
                raise AssertionError(f'expected ENQUEUE_FAILED, got {other!r}')

    @pytest.mark.asyncio
    async def test_defect_in_the_write_crashes_loud(self) -> None:
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)
        conn.execute = AsyncMock(side_effect=TypeError('defect'))

        with pytest.raises(TypeError, match='defect'):
            await broker.enqueue_async('my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST, sent_at=datetime.now(timezone.utc))

    @pytest.mark.asyncio
    async def test_defect_in_the_keyed_write_crashes_loud(self) -> None:
        broker = _make_broker()
        engine, conn = _make_enqueue_engine()
        _set_resources(broker, engine=engine)
        conn.execute = AsyncMock(side_effect=TypeError('defect'))

        with pytest.raises(TypeError, match='defect'):
            await broker.enqueue_async(
                'my_task', task_id='t', enqueue_sha='s', send_kind=SendKind.FIRST,
                sent_at=datetime.now(timezone.utc), idempotency_key='key',
            )

    @pytest.mark.asyncio
    async def test_an_outcome_outside_the_contract_crashes_loud(self) -> None:
        broker = _make_broker()
        engine, _conn = _make_enqueue_engine(outcome='SOMETHING_ELSE')
        _set_resources(broker, engine=engine)

        with pytest.raises(AssertionError, match='SOMETHING_ELSE'):
            await broker.enqueue_async(
                'my_task', task_id='dup', enqueue_sha='s', send_kind=SendKind.FIRST,
                sent_at=datetime.now(timezone.utc),
            )
