"""Tests for PostgresListener (horsies/core/brokers/listener.py).

Strategy: mock psycopg.AsyncConnection.connect to avoid real PostgreSQL.
Test internal state transitions, dispatcher distribution, fd monitoring,
subscribe/unsubscribe flows, and graceful shutdown.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest
from psycopg import Notify, OperationalError

from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.result_types import BrokerErrorCode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_listener(url: str = 'postgresql://u:p@localhost/db') -> PostgresListener:
    """Create a PostgresListener without real connections."""
    return PostgresListener(url)


def _make_mock_conn(closed: bool = False, fileno: int = 5) -> MagicMock:
    """Build a mock AsyncConnection with sensible defaults.

    Uses MagicMock (not AsyncMock) so that methods like notifies()
    return their configured value directly instead of wrapping in a coroutine.
    Async methods (close, execute) are explicitly set to AsyncMock.
    """
    conn = MagicMock()
    type(conn).closed = PropertyMock(return_value=closed)
    conn.fileno = MagicMock(return_value=fileno)
    conn.close = AsyncMock()
    conn.execute = AsyncMock()
    return conn


class _BusyCommandConnection:
    """Simulate psycopg command connection single-flight behavior."""

    def __init__(self, started: asyncio.Event, release: asyncio.Event) -> None:
        self.closed: bool = False
        self._started = started
        self._release = release
        self._in_progress = False

    async def execute(self, _query: Any, *_args: Any, **_kwargs: Any) -> None:
        if self._in_progress:
            raise OperationalError(
                'sending query failed: another command is already in progress',
            )
        self._in_progress = True
        self._started.set()
        await self._release.wait()
        self._in_progress = False


# ---------------------------------------------------------------------------
# TestPostgresListenerInit
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPostgresListenerInit:
    """Tests for initial state of PostgresListener."""

    def test_initial_state_no_connections(self) -> None:
        """Freshly created listener should have no connections."""
        listener = _make_listener()

        assert listener._dispatcher_conn is None
        assert listener._command_conn is None

    def test_initial_state_empty_channels_and_subs(self) -> None:
        """Freshly created listener should have no channels or subscribers."""
        listener = _make_listener()

        assert len(listener._listen_channels) == 0
        assert len(listener._subs) == 0

    def test_initial_state_no_tasks(self) -> None:
        """No dispatcher or health check tasks should be running initially."""
        listener = _make_listener()

        assert listener._dispatcher_task is None
        assert listener._health_check_task is None

    def test_fd_not_registered_initially(self) -> None:
        """File descriptor monitoring should not be active initially."""
        listener = _make_listener()

        assert listener._fd_registered is False

    def test_stores_database_url(self) -> None:
        """Should store the provided database URL."""
        url = 'postgresql://user:pass@myhost/mydb'
        listener = _make_listener(url)

        assert listener.database_url == url


# ---------------------------------------------------------------------------
# TestStartListening
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStartListening:
    """Tests for _start_listening: issues LISTEN for each tracked channel."""

    @pytest.mark.asyncio
    async def test_issues_listen_for_each_channel(self) -> None:
        """Should execute LISTEN for every channel in _listen_channels."""
        listener = _make_listener()
        listener._listen_channels = {'task_done', 'task_new'}
        conn = _make_mock_conn()

        await listener._start_listening(conn)

        assert conn.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_no_listen_when_no_channels(self) -> None:
        """Should not execute any LISTEN when _listen_channels is empty."""
        listener = _make_listener()
        listener._listen_channels = set()
        conn = _make_mock_conn()

        await listener._start_listening(conn)

        conn.execute.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestEnsureConnections
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnsureConnections:
    """Tests for _ensure_connections: creates connections and re-listens."""

    @pytest.mark.asyncio
    async def test_creates_dispatcher_conn_when_none(self) -> None:
        """Should create dispatcher connection when it is None."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn):
            result = await listener._ensure_connections()

        assert result.is_ok()
        assert listener._dispatcher_conn is mock_conn

    @pytest.mark.asyncio
    async def test_creates_command_conn_when_none(self) -> None:
        """Should create command connection when it is None."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn):
            result = await listener._ensure_connections()

        assert result.is_ok()
        assert listener._command_conn is mock_conn

    @pytest.mark.asyncio
    async def test_re_listens_on_existing_channels_after_reconnect(self) -> None:
        """After reconnection, should re-issue LISTEN for tracked channels."""
        listener = _make_listener()
        listener._listen_channels = {'task_done'}
        mock_conn = _make_mock_conn()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn):
            result = await listener._ensure_connections()

        assert result.is_ok()
        # Both dispatcher and command conn should have _start_listening called
        # Each conn gets LISTEN for 'task_done' = 2 calls total
        assert mock_conn.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_rolls_back_dispatcher_when_command_connect_fails(self) -> None:
        """If command connection fails, dispatcher setup from this call should be rolled back."""
        listener = _make_listener()
        dispatcher_conn = _make_mock_conn()

        with (
            patch(
                'psycopg.AsyncConnection.connect',
                new_callable=AsyncMock,
                side_effect=[dispatcher_conn, OperationalError('command down')],
            ),
            patch.object(listener, '_unregister_fd_monitoring') as mock_unregister,
        ):
            result = await listener._ensure_connections()

        assert result.is_err()
        assert result.err_value.code == BrokerErrorCode.LISTENER_START_FAILED
        dispatcher_conn.close.assert_awaited_once()
        mock_unregister.assert_called_once()
        assert listener._dispatcher_conn is None
        assert listener._command_conn is None


# ---------------------------------------------------------------------------
# TestEnsureDispatcherConnection / TestEnsureCommandConnection
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnsureDispatcherConnection:
    """Tests for _ensure_dispatcher_connection."""

    @pytest.mark.asyncio
    async def test_returns_existing_conn_when_alive(self) -> None:
        """Should return existing connection when it's alive."""
        listener = _make_listener()
        mock_conn = _make_mock_conn(closed=False)
        listener._dispatcher_conn = mock_conn

        result = await listener._ensure_dispatcher_connection()

        assert result is mock_conn

    @pytest.mark.asyncio
    async def test_reconnects_when_conn_is_none(self) -> None:
        """Should call _ensure_connections when dispatcher conn is None."""
        listener = _make_listener()
        listener._dispatcher_conn = None
        new_conn = _make_mock_conn()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=new_conn):
            result = await listener._ensure_dispatcher_connection()

        assert result is new_conn


# ---------------------------------------------------------------------------
# TestCloseConnections
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCloseConnections:
    """Tests for _close_connections: closes both and resets to None."""

    @pytest.mark.asyncio
    async def test_closes_open_connections_and_resets(self) -> None:
        """Should close both connections and set them to None."""
        listener = _make_listener()
        disp_conn = _make_mock_conn()
        cmd_conn = _make_mock_conn()
        listener._dispatcher_conn = disp_conn
        listener._command_conn = cmd_conn

        await listener._close_connections()

        disp_conn.close.assert_awaited_once()
        cmd_conn.close.assert_awaited_once()
        assert listener._dispatcher_conn is None
        assert listener._command_conn is None

    @pytest.mark.asyncio
    async def test_handles_already_closed_connections(self) -> None:
        """Should handle connections that are already closed without raising."""
        listener = _make_listener()
        disp_conn = _make_mock_conn(closed=True)
        listener._dispatcher_conn = disp_conn
        listener._command_conn = None

        await listener._close_connections()

        disp_conn.close.assert_not_awaited()
        assert listener._dispatcher_conn is None

    @pytest.mark.asyncio
    async def test_handles_close_exception_gracefully(self) -> None:
        """Should not propagate exceptions from conn.close()."""
        listener = _make_listener()
        disp_conn = _make_mock_conn()
        disp_conn.close = AsyncMock(side_effect=RuntimeError('already broken'))
        listener._dispatcher_conn = disp_conn
        listener._command_conn = None

        # Should not raise
        await listener._close_connections()

        assert listener._dispatcher_conn is None

    @pytest.mark.asyncio
    async def test_continues_when_first_close_raises_unexpected_error(self) -> None:
        """Even unexpected close errors on dispatcher should not skip command close."""
        listener = _make_listener()
        disp_conn = _make_mock_conn()
        cmd_conn = _make_mock_conn()
        disp_conn.close = AsyncMock(side_effect=ValueError('unexpected'))
        listener._dispatcher_conn = disp_conn
        listener._command_conn = cmd_conn

        await listener._close_connections()

        cmd_conn.close.assert_awaited_once()
        assert listener._dispatcher_conn is None
        assert listener._command_conn is None


# ---------------------------------------------------------------------------
# TestFdMonitoring
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFdMonitoring:
    """Tests for _register_fd_monitoring and _unregister_fd_monitoring."""

    def test_register_sets_flag_on_success(self) -> None:
        """Successful registration should set _fd_registered to True."""
        listener = _make_listener()
        mock_conn = _make_mock_conn(closed=False, fileno=7)
        listener._dispatcher_conn = mock_conn

        mock_loop = MagicMock()
        with patch('asyncio.get_running_loop', return_value=mock_loop):
            listener._register_fd_monitoring()

        assert listener._fd_registered is True
        mock_loop.add_reader.assert_called_once()

    def test_register_noop_on_os_error(self) -> None:
        """OSError from fileno() should leave _fd_registered False."""
        listener = _make_listener()
        mock_conn = _make_mock_conn(closed=False)
        mock_conn.fileno = MagicMock(side_effect=OSError('bad fd'))
        listener._dispatcher_conn = mock_conn

        mock_loop = MagicMock()
        with patch('asyncio.get_running_loop', return_value=mock_loop):
            listener._register_fd_monitoring()

        assert listener._fd_registered is False

    def test_register_noop_when_no_conn(self) -> None:
        """Should do nothing when dispatcher conn is None."""
        listener = _make_listener()
        listener._dispatcher_conn = None

        listener._register_fd_monitoring()

        assert listener._fd_registered is False

    def test_register_noop_when_conn_closed(self) -> None:
        """Should do nothing when dispatcher conn is closed."""
        listener = _make_listener()
        listener._dispatcher_conn = _make_mock_conn(closed=True)

        listener._register_fd_monitoring()

        assert listener._fd_registered is False

    def test_register_noop_when_already_registered(self) -> None:
        """Should not re-register if already registered."""
        listener = _make_listener()
        mock_conn = _make_mock_conn(closed=False)
        listener._dispatcher_conn = mock_conn
        listener._fd_registered = True

        mock_loop = MagicMock()
        with patch('asyncio.get_running_loop', return_value=mock_loop):
            listener._register_fd_monitoring()

        mock_loop.add_reader.assert_not_called()

    def test_unregister_clears_flag(self) -> None:
        """Successful unregistration should clear _fd_registered."""
        listener = _make_listener()
        listener._fd_registered = True
        listener._dispatcher_conn = _make_mock_conn(closed=False, fileno=7)

        mock_loop = MagicMock()
        with patch('asyncio.get_running_loop', return_value=mock_loop):
            listener._unregister_fd_monitoring()

        assert listener._fd_registered is False
        mock_loop.remove_reader.assert_called_once()

    def test_unregister_clears_flag_on_error(self) -> None:
        """Should still clear _fd_registered even when remove_reader raises."""
        listener = _make_listener()
        listener._fd_registered = True
        listener._dispatcher_conn = _make_mock_conn(closed=False)

        mock_loop = MagicMock()
        mock_loop.remove_reader = MagicMock(side_effect=OSError('bad fd'))
        with patch('asyncio.get_running_loop', return_value=mock_loop):
            listener._unregister_fd_monitoring()

        assert listener._fd_registered is False

    def test_unregister_noop_when_not_registered(self) -> None:
        """Should do nothing when _fd_registered is False."""
        listener = _make_listener()
        listener._fd_registered = False

        # Should not raise or try to remove reader
        listener._unregister_fd_monitoring()

        assert listener._fd_registered is False


# ---------------------------------------------------------------------------
# TestHealthMonitor
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHealthMonitor:
    """Tests for _health_monitor disconnect handling."""

    @pytest.mark.asyncio
    async def test_operational_error_closes_connections_before_reset(self) -> None:
        """Detected dead connection should trigger best-effort close/reset."""
        listener = _make_listener()
        disp_conn = _make_mock_conn()
        cmd_conn = _make_mock_conn()
        cmd_conn.execute = AsyncMock(side_effect=OperationalError('dead'))
        listener._dispatcher_conn = disp_conn
        listener._command_conn = cmd_conn
        listener._fd_registered = True
        listener._fd_activity.set()

        with patch.object(listener, '_unregister_fd_monitoring') as mock_unregister:
            task = asyncio.create_task(listener._health_monitor())
            try:
                for _ in range(50):
                    if listener._dispatcher_conn is None and listener._command_conn is None:
                        break
                    await asyncio.sleep(0)
                else:
                    pytest.fail('health monitor did not reset connections in time')
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

        mock_unregister.assert_called_once()
        disp_conn.close.assert_awaited_once()
        cmd_conn.close.assert_awaited_once()
        assert listener._dispatcher_conn is None
        assert listener._command_conn is None


# ---------------------------------------------------------------------------
# TestDispatcher
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDispatcher:
    """Tests for _dispatcher: notification distribution, error handling."""

    @pytest.mark.asyncio
    async def test_distributes_to_all_subscriber_queues(self) -> None:
        """Dispatcher should put notification in all subscriber queues for that channel."""
        listener = _make_listener()
        q1: asyncio.Queue[Notify] = asyncio.Queue()
        q2: asyncio.Queue[Notify] = asyncio.Queue()
        listener._subs['task_done'] = {q1, q2}

        notification = Notify(channel='task_done', payload='task-123', pid=0)

        # Build a mock connection that yields one notification then cancels
        mock_conn = _make_mock_conn()

        async def _fake_notifies() -> Any:
            yield notification
            # After yielding one notification, cancel to stop the loop
            raise asyncio.CancelledError

        mock_conn.notifies.return_value = _fake_notifies()
        listener._dispatcher_conn = mock_conn

        with pytest.raises(asyncio.CancelledError):
            await listener._dispatcher()

        assert q1.qsize() == 1
        assert q2.qsize() == 1
        assert (await q1.get()) is notification
        assert (await q2.get()) is notification

    @pytest.mark.asyncio
    async def test_queue_full_drops_silently(self) -> None:
        """When subscriber queue is full, dispatcher should drop without error."""
        listener = _make_listener()
        # Queue with maxsize=0 means unlimited, so use maxsize=1
        q_full: asyncio.Queue[Notify] = asyncio.Queue(maxsize=1)
        # Pre-fill the queue
        q_full.put_nowait(Notify(channel='x', payload='old', pid=0))
        q_ok: asyncio.Queue[Notify] = asyncio.Queue()
        listener._subs['task_done'] = {q_full, q_ok}

        notification = Notify(channel='task_done', payload='new-task', pid=0)

        mock_conn = _make_mock_conn()

        async def _fake_notifies() -> Any:
            yield notification
            raise asyncio.CancelledError

        mock_conn.notifies.return_value = _fake_notifies()
        listener._dispatcher_conn = mock_conn

        with pytest.raises(asyncio.CancelledError):
            await listener._dispatcher()

        # Full queue still has the old item
        assert q_full.qsize() == 1
        assert (await q_full.get()).payload == 'old'
        # ok queue got the new notification
        assert q_ok.qsize() == 1

    @pytest.mark.asyncio
    async def test_cancelled_error_unregisters_fd_and_re_raises(self) -> None:
        """CancelledError should unregister fd monitoring and propagate."""
        listener = _make_listener()
        listener._fd_registered = True

        mock_conn = _make_mock_conn()

        async def _cancel_immediately() -> Any:
            if False:
                yield  # Make this an async generator
            raise asyncio.CancelledError

        mock_conn.notifies.return_value = _cancel_immediately()
        listener._dispatcher_conn = mock_conn

        mock_loop = MagicMock()
        with (
            patch('asyncio.get_running_loop', return_value=mock_loop),
            pytest.raises(asyncio.CancelledError),
        ):
            await listener._dispatcher()

        assert listener._fd_registered is False


# ---------------------------------------------------------------------------
# TestListen
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestListen:
    """Tests for listen(): subscription management and LISTEN commands."""

    @pytest.mark.asyncio
    async def test_cross_loop_access_raises_runtime_error(self) -> None:
        """listen() should fail fast when called from a different event loop."""
        listener = _make_listener()
        listener._owner_loop = object()  # Force mismatch with current running loop.

        with pytest.raises(RuntimeError, match='different event loop'):
            await listener.listen('task_done')

    @pytest.mark.asyncio
    async def test_first_subscriber_issues_listen_and_starts_dispatcher(self) -> None:
        """First subscriber for a channel should issue LISTEN and start dispatcher."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()

        def _close_and_return_mock(coro: Any, **_kwargs: Any) -> MagicMock:
            """Close the coroutine to avoid 'never awaited' warning."""
            coro.close()
            return MagicMock()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn):
            # Patch create_task to capture the dispatcher creation
            with patch.object(asyncio, 'create_task', side_effect=_close_and_return_mock) as mock_create_task:
                result = await listener.listen('task_done')

        assert result.is_ok()
        q = result.ok_value
        assert isinstance(q, asyncio.Queue)
        assert 'task_done' in listener._listen_channels
        assert q in listener._subs['task_done']
        # Dispatcher task should be started
        mock_create_task.assert_called()

    @pytest.mark.asyncio
    async def test_second_subscriber_same_channel_adds_queue_only(self) -> None:
        """Second subscriber to same channel should not issue new LISTEN."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()
        listener._dispatcher_conn = mock_conn
        listener._command_conn = mock_conn
        # Pretend channel is already listened
        listener._listen_channels.add('task_done')
        listener._subs['task_done'] = {asyncio.Queue()}

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn):
            result = await listener.listen('task_done')

        assert result.is_ok()
        q = result.ok_value
        # Should have 2 queues now
        assert len(listener._subs['task_done']) == 2
        assert q in listener._subs['task_done']

    @pytest.mark.asyncio
    async def test_new_channel_restarts_dispatcher(self) -> None:
        """Adding a new channel should cancel and restart the dispatcher task."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()
        listener._dispatcher_conn = mock_conn
        listener._command_conn = mock_conn

        # Create a real asyncio task that we can cancel and await
        async def _noop() -> None:
            await asyncio.sleep(999)

        old_dispatcher = asyncio.create_task(_noop())

        listener._dispatcher_task = old_dispatcher

        def _close_and_return_mock(coro: Any, **_kwargs: Any) -> MagicMock:
            """Close the coroutine to avoid 'never awaited' warning."""
            coro.close()
            return MagicMock()

        with (
            patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn),
            patch.object(asyncio, 'create_task', side_effect=_close_and_return_mock) as mock_create_task,
        ):
            result = await listener.listen('new_channel')

        assert result.is_ok()
        assert old_dispatcher.cancelled()
        mock_create_task.assert_called()

    @pytest.mark.asyncio
    async def test_new_channel_restart_still_happens_when_listen_fails(self) -> None:
        """LISTEN failure should not leave a previously running dispatcher stopped."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()
        listener._dispatcher_conn = mock_conn
        listener._command_conn = mock_conn
        mock_conn.execute = AsyncMock(side_effect=OperationalError('LISTEN failed'))

        async def _noop() -> None:
            await asyncio.sleep(999)

        old_dispatcher = asyncio.create_task(_noop())
        listener._dispatcher_task = old_dispatcher

        def _close_and_return_mock(coro: Any, **_kwargs: Any) -> MagicMock:
            coro.close()
            return MagicMock()

        with patch.object(asyncio, 'create_task', side_effect=_close_and_return_mock) as mock_create_task:
            result = await listener.listen('new_channel')

        assert result.is_err()
        assert old_dispatcher.cancelled()
        mock_create_task.assert_called_once()
        assert 'new_channel' not in listener._listen_channels


# ---------------------------------------------------------------------------
# TestUnlisten
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestUnlisten:
    """Tests for unlisten(): queue removal and server-side UNLISTEN."""

    @pytest.mark.asyncio
    async def test_removes_queue_and_issues_unlisten_when_last(self) -> None:
        """When last subscriber leaves, should issue UNLISTEN and remove channel."""
        listener = _make_listener()
        q: asyncio.Queue[Notify] = asyncio.Queue()
        listener._subs['task_done'] = {q}
        listener._listen_channels.add('task_done')
        mock_conn = _make_mock_conn()
        listener._command_conn = mock_conn

        await listener.unlisten('task_done', q)

        assert 'task_done' not in listener._subs
        assert 'task_done' not in listener._listen_channels
        mock_conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_unlisten_when_other_subscribers_remain(self) -> None:
        """When other subscribers remain, should not issue UNLISTEN."""
        listener = _make_listener()
        q1: asyncio.Queue[Notify] = asyncio.Queue()
        q2: asyncio.Queue[Notify] = asyncio.Queue()
        listener._subs['task_done'] = {q1, q2}
        listener._listen_channels.add('task_done')
        mock_conn = _make_mock_conn()
        listener._command_conn = mock_conn

        await listener.unlisten('task_done', q1)

        assert q1 not in listener._subs['task_done']
        assert q2 in listener._subs['task_done']
        assert 'task_done' in listener._listen_channels
        mock_conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_removes_channel_from_listen_channels(self) -> None:
        """Channel should be removed from _listen_channels when all subs gone."""
        listener = _make_listener()
        listener._subs['ch'] = set()  # already empty
        listener._listen_channels.add('ch')
        mock_conn = _make_mock_conn()
        listener._command_conn = mock_conn

        await listener.unlisten('ch', None)

        assert 'ch' not in listener._subs
        assert 'ch' not in listener._listen_channels

    @pytest.mark.asyncio
    async def test_no_unlisten_when_command_conn_closed(self) -> None:
        """Should not issue UNLISTEN when command connection is closed."""
        listener = _make_listener()
        listener._subs['ch'] = set()
        listener._listen_channels.add('ch')
        mock_conn = _make_mock_conn(closed=True)
        listener._command_conn = mock_conn

        await listener.unlisten('ch', None)

        mock_conn.execute.assert_not_awaited()
        # Channel still removed from tracking since no subs remain
        assert 'ch' not in listener._listen_channels


# ---------------------------------------------------------------------------
# TestUnsubscribe
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestUnsubscribe:
    """Tests for unsubscribe(): local queue cleanup and channel tracking removal."""

    @pytest.mark.asyncio
    async def test_removes_queue_and_issues_unlisten_when_last(self) -> None:
        """Should remove queue and issue UNLISTEN when it was the last subscriber."""
        listener = _make_listener()
        q: asyncio.Queue[Notify] = asyncio.Queue()
        listener._subs['task_done'] = {q}
        listener._listen_channels.add('task_done')
        mock_conn = _make_mock_conn()
        listener._command_conn = mock_conn

        await listener.unsubscribe('task_done', q)

        assert 'task_done' not in listener._subs
        mock_conn.execute.assert_awaited_once()
        assert 'task_done' not in listener._listen_channels

    @pytest.mark.asyncio
    async def test_no_unlisten_when_other_subscribers_remain(self) -> None:
        """Should keep channel tracked when other local subscribers remain."""
        listener = _make_listener()
        q1: asyncio.Queue[Notify] = asyncio.Queue()
        q2: asyncio.Queue[Notify] = asyncio.Queue()
        listener._subs['task_done'] = {q1, q2}
        listener._listen_channels.add('task_done')
        mock_conn = _make_mock_conn()
        listener._command_conn = mock_conn

        await listener.unsubscribe('task_done', q1)

        assert q1 not in listener._subs['task_done']
        assert q2 in listener._subs['task_done']
        assert 'task_done' in listener._listen_channels
        mock_conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_when_queue_is_none(self) -> None:
        """Passing None as queue should be a no-op."""
        listener = _make_listener()
        listener._subs['ch'] = {asyncio.Queue()}

        await listener.unsubscribe('ch', None)

        assert len(listener._subs['ch']) == 1

    @pytest.mark.asyncio
    async def test_serializes_unsubscribe_with_health_check_probe(self) -> None:
        """unsubscribe should wait for in-flight health query, not raise race errors."""
        listener = _make_listener()
        started = asyncio.Event()
        release = asyncio.Event()
        listener._command_conn = _BusyCommandConnection(started, release)  # type: ignore[assignment]

        channel = 'task_done'
        queue: asyncio.Queue[Notify] = asyncio.Queue()
        listener._subs[channel] = {queue}
        listener._listen_channels.add(channel)

        health_task = asyncio.create_task(listener._health_monitor())
        listener._fd_activity.set()
        await asyncio.wait_for(started.wait(), timeout=1.0)

        unsubscribe_task = asyncio.create_task(listener.unsubscribe(channel, queue))
        await asyncio.sleep(0)
        assert not unsubscribe_task.done()

        release.set()
        await asyncio.wait_for(unsubscribe_task, timeout=1.0)
        assert channel not in listener._listen_channels

        health_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await health_task


# ---------------------------------------------------------------------------
# TestClose
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestClose:
    """Tests for close(): cancels tasks, closes connections."""

    @pytest.mark.asyncio
    async def test_cancels_health_check_and_dispatcher(self) -> None:
        """close() should cancel both health check and dispatcher tasks."""
        listener = _make_listener()

        async def _noop() -> None:
            await asyncio.sleep(999)

        health_task = asyncio.create_task(_noop())
        listener._health_check_task = health_task

        disp_task = asyncio.create_task(_noop())
        listener._dispatcher_task = disp_task

        listener._dispatcher_conn = _make_mock_conn()
        listener._command_conn = _make_mock_conn()

        await listener.close()

        assert health_task.cancelled()
        assert disp_task.cancelled()
        assert listener._health_check_task is None
        assert listener._dispatcher_task is None

    @pytest.mark.asyncio
    async def test_closes_both_connections(self) -> None:
        """close() should close both dispatcher and command connections."""
        listener = _make_listener()
        disp_conn = _make_mock_conn()
        cmd_conn = _make_mock_conn()
        listener._dispatcher_conn = disp_conn
        listener._command_conn = cmd_conn

        await listener.close()

        disp_conn.close.assert_awaited_once()
        cmd_conn.close.assert_awaited_once()
        assert listener._dispatcher_conn is None
        assert listener._command_conn is None
        assert len(listener._subs) == 0
        assert len(listener._listen_channels) == 0

    @pytest.mark.asyncio
    async def test_safe_to_call_when_already_closed(self) -> None:
        """close() should not raise when called on already-closed listener."""
        listener = _make_listener()
        listener._dispatcher_conn = None
        listener._command_conn = None
        listener._health_check_task = None
        listener._dispatcher_task = None

        # Should not raise
        await listener.close()

    @pytest.mark.asyncio
    async def test_already_closed_close_does_not_rebind_owner_loop(self) -> None:
        """close() on fully closed listener should not bind loop ownership again."""
        listener = _make_listener()

        with patch.object(listener, '_bind_or_validate_loop') as mock_bind:
            await listener.close()

        mock_bind.assert_not_called()
        assert listener._owner_loop is None

    @pytest.mark.asyncio
    async def test_unregisters_fd_monitoring(self) -> None:
        """close() should unregister fd monitoring."""
        listener = _make_listener()
        listener._fd_registered = True
        listener._dispatcher_conn = _make_mock_conn()
        listener._command_conn = _make_mock_conn()

        mock_loop = MagicMock()
        with patch('asyncio.get_running_loop', return_value=mock_loop):
            await listener.close()

        assert listener._fd_registered is False

    @pytest.mark.asyncio
    async def test_cross_loop_close_hands_off_to_owner_loop(self) -> None:
        """close() should hand off cleanup to the owner loop when called cross-loop."""
        listener = _make_listener()
        owner_loop = MagicMock()
        owner_loop.is_closed.return_value = False
        listener._owner_loop = owner_loop

        def _mock_run_coroutine_threadsafe(
            coro: Any, _loop: asyncio.AbstractEventLoop
        ) -> concurrent.futures.Future[None]:
            # Prevent "coroutine was never awaited" warnings in this unit test.
            coro.close()
            fut: concurrent.futures.Future[None] = concurrent.futures.Future()
            fut.set_result(None)
            return fut

        with patch(
            'asyncio.run_coroutine_threadsafe',
            side_effect=_mock_run_coroutine_threadsafe,
        ) as mock_threadsafe:
            await listener.close()

        mock_threadsafe.assert_called_once()

    @pytest.mark.asyncio
    async def test_cross_loop_close_noops_when_owner_loop_closed(self) -> None:
        """close() should return without raising when owner loop is already closed."""
        listener = _make_listener()
        owner_loop = MagicMock()
        owner_loop.is_closed.return_value = True
        listener._owner_loop = owner_loop

        with patch('asyncio.run_coroutine_threadsafe') as mock_threadsafe:
            await listener.close()

        mock_threadsafe.assert_not_called()

    @pytest.mark.asyncio
    async def test_cross_loop_close_handoff_failure_falls_back_to_local_cleanup(self) -> None:
        """If handoff scheduling fails, close() should close handed coroutine and cleanup locally."""
        listener = _make_listener()
        owner_loop = MagicMock()
        owner_loop.is_closed.return_value = False
        listener._owner_loop = owner_loop
        disp_conn = _make_mock_conn()
        cmd_conn = _make_mock_conn()
        listener._dispatcher_conn = disp_conn
        listener._command_conn = cmd_conn
        listener._listen_channels.add('task_done')
        listener._subs['task_done'].add(asyncio.Queue())

        captured_coro: dict[str, Any] = {}

        def _fail_handoff(
            coro: Any, _loop: asyncio.AbstractEventLoop
        ) -> concurrent.futures.Future[None]:
            captured_coro['coro'] = coro
            raise RuntimeError('owner loop not running')

        with patch('asyncio.run_coroutine_threadsafe', side_effect=_fail_handoff):
            await listener.close()

        handed_coro = captured_coro['coro']
        assert handed_coro.cr_frame is None
        disp_conn.close.assert_awaited_once()
        cmd_conn.close.assert_awaited_once()
        assert listener._dispatcher_conn is None
        assert listener._command_conn is None
        assert listener._owner_loop is None
        assert len(listener._listen_channels) == 0
        assert len(listener._subs) == 0


# ---------------------------------------------------------------------------
# TestListenerResultSurface
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestListenerResultSurface:
    """Tests for BrokerResult return types on listener public methods."""

    # -- start() --

    @pytest.mark.asyncio
    async def test_start_returns_ok_on_success(self) -> None:
        """start() should return Ok(None) when connections succeed."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn):
            result = await listener.start()

        assert result.is_ok()
        assert result.ok_value is None

        # Teardown: cancel the health_monitor task spawned by start().
        await listener.close()

    @pytest.mark.asyncio
    async def test_start_returns_err_on_connect_failure(self) -> None:
        """start() should return Err with LISTENER_START_FAILED on connection error."""
        listener = _make_listener()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, side_effect=OperationalError('conn refused')):
            result = await listener.start()

        assert result.is_err()
        err = result.err_value
        assert err.code == BrokerErrorCode.LISTENER_START_FAILED
        assert err.retryable is True
        assert 'conn refused' in err.message

    @pytest.mark.asyncio
    async def test_start_err_preserves_original_exception(self) -> None:
        """Err payload should carry the original exception for caller inspection."""
        listener = _make_listener()
        original = OperationalError('conn refused')

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, side_effect=original):
            result = await listener.start()

        assert result.is_err()
        assert result.err_value.exception is original

    @pytest.mark.asyncio
    async def test_start_propagates_non_operational_error(self) -> None:
        """Non-operational errors (e.g. ValueError) must propagate, not be wrapped as Err."""
        listener = _make_listener()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, side_effect=ValueError('bad config')):
            with pytest.raises(ValueError, match='bad config'):
                await listener.start()

    # -- listen() --

    @pytest.mark.asyncio
    async def test_listen_returns_ok_with_queue(self) -> None:
        """listen() should return Ok(Queue) on success."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()

        def _close_and_return_mock(coro: Any, **_kwargs: Any) -> MagicMock:
            coro.close()
            return MagicMock()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn):
            with patch.object(asyncio, 'create_task', side_effect=_close_and_return_mock):
                result = await listener.listen('task_done')

        assert result.is_ok()
        assert isinstance(result.ok_value, asyncio.Queue)

    @pytest.mark.asyncio
    async def test_listen_returns_err_on_connect_failure(self) -> None:
        """listen() should return Err with LISTENER_SUBSCRIBE_FAILED on connection error."""
        listener = _make_listener()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, side_effect=OperationalError('refused')):
            result = await listener.listen('task_done')

        assert result.is_err()
        err = result.err_value
        assert err.code == BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED
        assert err.retryable is True
        assert 'task_done' in err.message

    @pytest.mark.asyncio
    async def test_listen_cross_loop_still_raises_runtime_error(self) -> None:
        """RuntimeError from _bind_or_validate_loop is NOT wrapped in Result."""
        listener = _make_listener()
        listener._owner_loop = object()  # force mismatch

        with pytest.raises(RuntimeError, match='different event loop'):
            await listener.listen('task_done')

    @pytest.mark.asyncio
    async def test_listen_returns_err_when_listen_sql_fails(self) -> None:
        """listen() should return Err when the LISTEN SQL command fails."""
        listener = _make_listener()
        # Pre-set connections so _ensure_connections is a no-op
        mock_conn = _make_mock_conn()
        listener._dispatcher_conn = mock_conn
        listener._command_conn = mock_conn

        # Make the LISTEN execute call fail
        mock_conn.execute = AsyncMock(side_effect=OperationalError('LISTEN failed'))

        result = await listener.listen('task_done')

        assert result.is_err()
        assert result.err_value.code == BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED

    # -- listen_many() --

    @pytest.mark.asyncio
    async def test_listen_many_returns_ok_with_queues(self) -> None:
        """listen_many() should return Ok(list[Queue]) on success."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()

        def _close_and_return_mock(coro: Any, **_kwargs: Any) -> MagicMock:
            coro.close()
            return MagicMock()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, return_value=mock_conn):
            with patch.object(asyncio, 'create_task', side_effect=_close_and_return_mock):
                result = await listener.listen_many(['ch1', 'ch2'])

        assert result.is_ok()
        queues = result.ok_value
        assert len(queues) == 2
        assert all(isinstance(q, asyncio.Queue) for q in queues)

    @pytest.mark.asyncio
    async def test_listen_many_returns_err_on_connect_failure(self) -> None:
        """listen_many() should return Err with LISTENER_SUBSCRIBE_FAILED on connection error."""
        listener = _make_listener()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock, side_effect=OperationalError('refused')):
            result = await listener.listen_many(['ch1'])

        assert result.is_err()
        err = result.err_value
        assert err.code == BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED
        assert err.retryable is True

    # -- narrowed catch: programming bugs propagate, not silently wrapped --

    @pytest.mark.asyncio
    async def test_listen_propagates_programming_error_not_wrapped(self) -> None:
        """Non-operational exceptions in listen() must propagate, not silently become Err."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()
        listener._dispatcher_conn = mock_conn
        listener._command_conn = mock_conn

        # TypeError is a programming bug, not an infrastructure error
        mock_conn.execute = AsyncMock(side_effect=TypeError('mock bug'))

        with pytest.raises(TypeError, match='mock bug'):
            await listener.listen('task_done')

    @pytest.mark.asyncio
    async def test_listen_many_propagates_programming_error_not_wrapped(self) -> None:
        """Non-operational exceptions in listen_many() must propagate, not silently become Err."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()
        listener._dispatcher_conn = mock_conn
        listener._command_conn = mock_conn

        mock_conn.execute = AsyncMock(side_effect=AttributeError('mock bug'))

        with pytest.raises(AttributeError, match='mock bug'):
            await listener.listen_many(['ch1'])

    @pytest.mark.asyncio
    async def test_listen_many_restarts_dispatcher_when_listen_fails_mid_update(self) -> None:
        """A failing LISTEN in listen_many() should revive a previously running dispatcher."""
        listener = _make_listener()
        mock_conn = _make_mock_conn()
        listener._dispatcher_conn = mock_conn
        listener._command_conn = mock_conn
        mock_conn.execute = AsyncMock(side_effect=OperationalError('LISTEN failed'))

        async def _noop() -> None:
            await asyncio.sleep(999)

        old_dispatcher = asyncio.create_task(_noop())
        listener._dispatcher_task = old_dispatcher

        def _close_and_return_mock(coro: Any, **_kwargs: Any) -> MagicMock:
            coro.close()
            return MagicMock()

        with patch.object(asyncio, 'create_task', side_effect=_close_and_return_mock) as mock_create_task:
            result = await listener.listen_many(['ch1', 'ch2'])

        assert result.is_err()
        assert old_dispatcher.cancelled()
        mock_create_task.assert_called_once()
        assert 'ch1' not in listener._listen_channels

    @pytest.mark.asyncio
    async def test_ensure_connections_propagates_programming_error_not_wrapped(self) -> None:
        """Non-operational exceptions in _ensure_connections() must propagate,
        not silently become Err (which would degrade callers to polling)."""
        listener = _make_listener()

        with patch('psycopg.AsyncConnection.connect', new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = TypeError('mock programming bug')

            with pytest.raises(TypeError, match='mock programming bug'):
                await listener._ensure_connections()


# ---------------------------------------------------------------------------
# TestCallerUnwrapContract
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCallerUnwrapContract:
    """Verify that higher-level callers fail-fast and preserve error semantics
    when the listener returns Err.

    These test the contract between listener and its callers, not the full
    broker/worker logic.
    """

    @pytest.mark.asyncio
    async def test_broker_ensure_initialized_raises_on_listener_start_err(self) -> None:
        """_ensure_initialized should raise when listener.start() returns Err,
        and ensure_schema_initialized wraps it as SCHEMA_INIT_FAILED."""
        from horsies.core.brokers.result_types import BrokerOperationError
        from horsies.core.types.result import Err as Err_, is_err as is_err_
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.brokers.postgres import PostgresBroker

        original_exc = OperationalError('listener conn refused')

        with (
            patch('horsies.core.brokers.postgres.create_async_engine') as mock_engine,
            patch('horsies.core.brokers.postgres.async_sessionmaker'),
            patch('horsies.core.brokers.postgres.PostgresListener') as mock_listener_cls,
        ):
            mock_engine.return_value = MagicMock()
            mock_engine.return_value.dispose = AsyncMock()
            mock_engine.return_value.begin = MagicMock()

            # begin() as async context manager for schema init
            mock_conn_ctx = AsyncMock()
            mock_conn_ctx.run_sync = AsyncMock()
            mock_conn_ctx.execute = AsyncMock()
            mock_begin_ctx = MagicMock()
            mock_begin_ctx.__aenter__ = AsyncMock(return_value=mock_conn_ctx)
            mock_begin_ctx.__aexit__ = AsyncMock(return_value=None)
            mock_engine.return_value.begin.return_value = mock_begin_ctx

            mock_listener = AsyncMock()
            mock_listener.start.return_value = Err_(BrokerOperationError(
                code=BrokerErrorCode.LISTENER_START_FAILED,
                message='Failed to establish listener connections: listener conn refused',
                retryable=True,
                exception=original_exc,
            ))
            mock_listener_cls.return_value = mock_listener

            config = PostgresConfig(database_url='postgresql+psycopg://u:p@localhost/db')
            broker = PostgresBroker(config)

            result = await broker.ensure_schema_initialized()

        assert is_err_(result)
        assert result.err_value.code == BrokerErrorCode.SCHEMA_INIT_FAILED
        assert 'listener conn refused' in result.err_value.message

    @pytest.mark.asyncio
    async def test_worker_start_raises_on_listener_start_err(self) -> None:
        """Worker.start() should raise the original exception when listener.start()
        returns Err, preserving retryability for _start_with_resilience_config."""
        from horsies.core.brokers.result_types import BrokerOperationError
        from horsies.core.types.result import Err as Err_

        original_exc = OperationalError('listener conn refused')
        mock_listener = AsyncMock()
        mock_listener.start.return_value = Err_(BrokerOperationError(
            code=BrokerErrorCode.LISTENER_START_FAILED,
            message='Failed to establish listener connections',
            retryable=True,
            exception=original_exc,
        ))

        from horsies.core.worker.worker import Worker
        from horsies.core.worker.config import WorkerConfig

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://localhost/test',
            psycopg_dsn='postgresql://localhost/test',
            queues=['default'],
        )

        worker = Worker.__new__(Worker)
        worker.cfg = cfg
        worker.listener = mock_listener
        worker._stop = asyncio.Event()
        worker._executor = None
        worker._service_tasks = set()
        worker._finalizer_tasks = set()
        worker._create_executor = MagicMock(return_value=MagicMock())
        worker._preload_modules_main = MagicMock()

        with pytest.raises(OperationalError, match='listener conn refused'):
            await worker.start()
        worker._create_executor.assert_not_called()
        mock_listener.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_worker_start_raises_on_listener_listen_many_err(self) -> None:
        """Worker.start() should raise when listener.listen_many() returns Err."""
        from horsies.core.brokers.result_types import BrokerOperationError
        from horsies.core.types.result import Err as Err_, Ok as Ok_

        original_exc = OperationalError('subscribe failed')
        mock_listener = AsyncMock()
        mock_listener.start.return_value = Ok_(None)
        mock_listener.listen_many.return_value = Err_(BrokerOperationError(
            code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
            message='Failed to subscribe to channels',
            retryable=True,
            exception=original_exc,
        ))

        from horsies.core.worker.worker import Worker
        from horsies.core.worker.config import WorkerConfig

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://localhost/test',
            psycopg_dsn='postgresql://localhost/test',
            queues=['default'],
        )

        worker = Worker.__new__(Worker)
        worker.cfg = cfg
        worker.listener = mock_listener
        worker._stop = asyncio.Event()
        worker._executor = None
        worker._service_tasks = set()
        worker._finalizer_tasks = set()
        worker._create_executor = MagicMock(return_value=MagicMock())
        worker._preload_modules_main = MagicMock()

        with pytest.raises(OperationalError, match='subscribe failed'):
            await worker.start()
        worker._create_executor.assert_not_called()
        mock_listener.close.assert_awaited_once()
