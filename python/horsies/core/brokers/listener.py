# app/core/brokers/listener.py
"""
PostgreSQL LISTEN/NOTIFY-based notification system for task management:

Architecture:
  TaskApp -> PostgresBroker -> PostgresListener

Flow:
  1. Task submission: INSERT into tasks table -> PostgreSQL trigger -> NOTIFY task_new/task_queue_*
  2. Worker notification: Workers listen to queue-specific channels
  3. Task processing: Worker picks up task, processes it, updates status
  4. Completion notification: UPDATE tasks status -> PostgreSQL trigger -> NOTIFY task_done
  5. Result retrieval: Clients listen to task_done channel, filter by task_id

Key channels:
  - task_new: Global notification for any new task (external observers;
    workers listen to queue channels only to avoid a thundering herd)
  - task_queue_*: Queue-specific notifications (workers)
  - task_done: Task completion notifications (result waiters)
"""

# app/core/brokers/listener.py
from __future__ import annotations
import asyncio
import contextlib
from collections import defaultdict
from typing import Any, DefaultDict, Final, Optional, Set
from asyncio import Task, Queue  # precise type for Pylance/pyright

import psycopg
from psycopg import AsyncConnection, InterfaceError, OperationalError, Notify
from psycopg.conninfo import make_conninfo
from psycopg import sql

from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerOperationFailed,
)
from horsies.core.defaults import DEFAULT_TCP_USER_TIMEOUT_MS
from horsies.core.logging import get_logger
from horsies.core.types.result import Ok, Err, Result, is_err
from horsies.core.utils.db import is_retryable_connection_error

logger = get_logger('listener')

ACTIVITY_PROBE_MIN_INTERVAL_S: Final = 1.0
"""The shortest time between two health probes that notification activity
starts. The probe covers the activity that arrives within this time."""

_SUBSCRIBER_QUEUE_MAXSIZE: int = 4096

# TCP keepalives for the long-lived notification connections: the
# dispatcher blocks indefinitely in conn.notifies(), so a silently
# dropped connection (NAT/firewall idle timeout, no FIN) would otherwise
# hang it forever with only the poll fallback masking the stall.
# tcp_user_timeout bounds a write to a dead connection, which keepalives
# do not probe.
def _keepalive_conninfo(database_url: str, tcp_user_timeout_ms: int) -> str:
    return make_conninfo(
        database_url,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        tcp_user_timeout=tcp_user_timeout_ms,
    )


class PostgresListener:
    """
    PostgreSQL LISTEN/NOTIFY wrapper with async queue distribution.

    Features:
      - Single dispatcher consuming conn.notifies() to avoid competing readers
      - Multiple subscribers per channel via independent asyncio queues
      - Safe SQL with identifier quoting and parameter binding
      - Auto-reconnect with exponential backoff and re-LISTEN
      - Periodic connection health monitoring

    Architecture:
      - dispatcher_conn: Exclusively consumes notifications from conn.notifies()
      - command_conn: Runs lightweight health-check SQL separately
      - Per-channel queues: Each subscriber gets independent notification delivery

    Usage:
    ------
    listener = PostgresListener(database_url)
    start_r = await listener.start()       # Result[None, BrokerOperationFailed]
    assert start_r.is_ok()

    # Subscribe: returns Result[Queue[Notify], BrokerOperationFailed]
    listen_r = await listener.listen("task_done")
    queue = listen_r.ok_value

    # Wait for notifications (blocks until one arrives)
    notification = await queue.get()
    if notification.payload == "task_123":  # Filter by payload if needed
        # Process notification
        pass

    # Cleanup
    await listener.close()

    Notes:
    ------
    * Uses autocommit=True: NOTIFYs are sent immediately, not held in transactions
    * Dispatcher distributes each notification to ALL subscriber queues for that channel
    * put_nowait() prevents slow consumers from blocking others (drops on queue full)
    * Channel names are PostgreSQL identifiers, automatically quoted for safety
    """

    def __init__(
        self,
        database_url: str,
        *,
        tcp_user_timeout_ms: int = DEFAULT_TCP_USER_TIMEOUT_MS,
    ) -> None:
        self.database_url = database_url
        self._tcp_user_timeout_ms = tcp_user_timeout_ms

        # Two separate connections to avoid blocking issues:
        # 1. Dispatcher connection: exclusively for conn.notifies() consumption
        self._dispatcher_conn: Optional[AsyncConnection] = None
        # 2. Command connection: for lightweight probes (e.g. health checks)
        self._command_conn: Optional[AsyncConnection] = None

        # Set of channels we have LISTENed to on the server.
        self._listen_channels: Set[str] = set()

        # Per-channel subscribers: each subscriber is an asyncio.Queue[Notify].
        # Multiple subscribers can wait on the same channel independently.
        self._subs: DefaultDict[str, Set[Queue[Notify]]] = defaultdict(set)

        # Payload-keyed subscribers: (channel, payload) → queues. Result
        # waiters use these so a busy channel (task_done carries every
        # completion cluster-wide) is not fanned into every waiter's queue;
        # the dispatcher delivers only to the matching payload key.
        self._payload_subs: DefaultDict[
            tuple[str, str], Set[Queue[Notify]]
        ] = defaultdict(set)

        # Background dispatcher task consuming conn.notifies() and distributing to subscriber queues.
        self._dispatcher_task: Optional[Task[None]] = None

        # Health check task for periodic disconnection detection
        self._health_check_task: Optional[Task[None]] = None

        # Event set when the dispatcher receives notifications; health monitor
        # also probes on timeout when no notifications arrive.
        self._notification_activity = asyncio.Event()

        # A lock used to serialize LISTEN/UNLISTEN and subscription book-keeping.
        self._lock = asyncio.Lock()
        # A lock used to serialize dispatcher-connection creation/replacement.
        self._dispatcher_lock = asyncio.Lock()
        # A lock used to serialize command-connection SQL execution.
        self._command_lock = asyncio.Lock()

        # Event-loop ownership: listener state (lock/tasks/connections) is loop-affine.
        self._owner_loop: Optional[asyncio.AbstractEventLoop] = None

    def _bind_or_validate_loop(self) -> None:
        """Bind to the first caller loop; a later call from another loop is a defect.

        The broker builds one listener per event loop, so a call from a loop
        other than the owner fails an assertion instead of a catalog error.
        """
        current_loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = current_loop
            return
        assert self._owner_loop is current_loop, (
            'PostgresListener used from an event loop other than its owner '
            f'loop: owner {self._owner_loop!r}, caller {current_loop!r}. '
            'Each event loop needs its own listener.'
        )

    async def start(self) -> Result[None, BrokerOperationFailed]:
        """Establish notification connections (autocommit) and start health monitor.

        Safe to call multiple times.

        Returns Ok(None) on success, Err(BrokerOperationFailed) on connection failure.
        A call from a non-owner event loop fails an assertion (programming error).
        """
        self._bind_or_validate_loop()
        conn_r = await self._ensure_connections()
        if is_err(conn_r):
            return conn_r
        # Defer starting the dispatcher until at least one channel is LISTENed.
        self._start_health_monitor_if_needed()
        return Ok(None)

    def _start_health_monitor_if_needed(self) -> None:
        if self._health_check_task is None or self._health_check_task.done():
            self._health_check_task = asyncio.create_task(
                self._health_monitor(), name='pg-listener-health'
            )

    async def _start_listening(self, conn: AsyncConnection) -> None:
        """Start listening to all _listen_channels for the given connection.

        Raises connection/SQL errors to the sole caller
        (_ensure_connections), whose try folds them into Err and rolls
        back the partially created connections.
        """
        for channel in self._listen_channels:
            await conn.execute(sql.SQL('LISTEN {}').format(sql.Identifier(channel)))

    async def _ensure_connections(self) -> Result[None, BrokerOperationFailed]:
        """Ensure we have live AsyncConnections in autocommit mode.

        On reconnection, re-issue LISTEN for any previously tracked channels.

        Returns Ok(None) on success, Err(BrokerOperationFailed) on failure.
        """
        created_dispatcher = False
        created_command = False
        try:
            # Ensure dispatcher connection (for conn.notifies() consumption)
            if self._dispatcher_conn is None or self._dispatcher_conn.closed:
                async with self._dispatcher_lock:
                    if self._dispatcher_conn is None or self._dispatcher_conn.closed:
                        self._dispatcher_conn = await psycopg.AsyncConnection.connect(
                            _keepalive_conninfo(
                                self.database_url, self._tcp_user_timeout_ms,
                            ),
                            autocommit=True,
                        )
                        created_dispatcher = True
                        await self._start_listening(self._dispatcher_conn)

            # Ensure command connection (for lightweight health/probe SQL)
            if self._command_conn is None or self._command_conn.closed:
                async with self._command_lock:
                    if self._command_conn is None or self._command_conn.closed:
                        self._command_conn = await psycopg.AsyncConnection.connect(
                            _keepalive_conninfo(
                                self.database_url, self._tcp_user_timeout_ms,
                            ),
                            autocommit=True,
                        )
                        created_command = True

            return Ok(None)
        except (OperationalError, InterfaceError, OSError) as exc:
            # Roll back partial setup in this call to avoid leaked half-initialized state.
            if created_command:
                if self._command_conn is not None and not self._command_conn.closed:
                    with contextlib.suppress(
                        OperationalError, InterfaceError, RuntimeError, OSError
                    ):
                        await self._command_conn.close()
                self._command_conn = None
            if created_dispatcher:
                if (
                    self._dispatcher_conn is not None
                    and not self._dispatcher_conn.closed
                ):
                    with contextlib.suppress(
                        OperationalError, InterfaceError, RuntimeError, OSError
                    ):
                        await self._dispatcher_conn.close()
                self._dispatcher_conn = None
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.LISTENER_START_FAILED,
                    message=f'Failed to establish listener connections: {exc}',
                    retryable=is_retryable_connection_error(exc),
                    exception=exc,
                )
            )

    async def _execute_on_command_conn(
        self,
        statement: Any,
    ) -> None:
        """Execute SQL on command connection with strict single-query concurrency."""
        async with self._command_lock:
            if self._command_conn is None or self._command_conn.closed:
                return
            await self._command_conn.execute(statement)

    async def _ensure_dispatcher_connection(self) -> AsyncConnection:
        """Ensure dispatcher connection is available.

        Unwraps the Result from _ensure_connections, re-raising the original
        exception on Err so the dispatcher loop's existing exception handling
        (OperationalError catch + reconnect) continues to work.

        Raises:
            OperationalError | InterfaceError | OSError: the connection
                failure from _ensure_connections, re-raised at this seam.
                Callers: the dispatcher loop (backoff + reconnect) and
                listen (fold to Err).
            RuntimeError: the connection was
                torn down concurrently (close/health reset run without
                self._lock) between ensure and this check — same exposure
                the bare assert had, now explicit and -O-proof.
                listen folds it to Err.
        """
        if self._dispatcher_conn is None or self._dispatcher_conn.closed:
            conn_r = await self._ensure_connections()
            if is_err(conn_r):
                err = conn_r.err_value
                raise err.exception
        if self._dispatcher_conn is None:
            raise RuntimeError('dispatcher connection missing after ensure')
        return self._dispatcher_conn

    async def _close_connections(self) -> None:
        """Close raw connections and reset to None for reconnection."""
        for conn in (self._dispatcher_conn, self._command_conn):
            if conn is not None and not conn.closed:
                try:
                    await conn.close()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass  # Best-effort close; keep tearing down remaining connections.
        self._dispatcher_conn = None
        self._command_conn = None

    async def _pause_dispatcher(self) -> bool:
        """Cancel and await dispatcher task, returning whether one was running."""
        if self._dispatcher_task is None:
            return False
        self._dispatcher_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._dispatcher_task
        self._dispatcher_task = None
        return True

    def _start_dispatcher_if_needed(self) -> None:
        """Create dispatcher task only when absent."""
        if self._dispatcher_task is None:
            self._dispatcher_task = asyncio.create_task(
                self._dispatcher(), name='pg-listener-dispatcher'
            )

    async def _handle_health_disconnect(self) -> None:
        """Reset listener connections after health monitor detects a dead connection.

        Total in practice: _pause_dispatcher suppresses the awaited
        cancellation and _close_connections contains close failures; the
        health-monitor loop's broad except is the backstop either way.
        """
        async with self._lock:
            dispatcher_was_running = await self._pause_dispatcher()
            await self._close_connections()
            if dispatcher_was_running and self._listen_channels:
                self._start_dispatcher_if_needed()

    async def _health_monitor(self) -> None:
        """
        Health monitoring for listener connections.

        The dispatcher already owns psycopg's notification socket reader via
        conn.notifies(); this monitor must not register another event-loop
        reader on the same fd. Instead, it probes after notification activity
        and periodically during idle periods.
        """
        while True:
            try:
                # Wait up to 60 seconds for notification activity
                try:
                    await asyncio.wait_for(
                        self._notification_activity.wait(), timeout=60.0
                    )
                    self._notification_activity.clear()

                    # Activity detected - verify connection health
                    if self._command_conn and not self._command_conn.closed:
                        try:
                            await self._execute_on_command_conn('SELECT 1')
                        except (OperationalError, InterfaceError):
                            # Connection is dead, trigger reconnection
                            await self._handle_health_disconnect()
                            continue
                    # At most one activity probe per interval: steady
                    # notifications would otherwise start a probe per burst.
                    await asyncio.sleep(ACTIVITY_PROBE_MIN_INTERVAL_S)

                except asyncio.TimeoutError:
                    # No activity for 60 seconds - perform health check
                    if self._command_conn and not self._command_conn.closed:
                        try:
                            await self._execute_on_command_conn('SELECT 1')
                        except (OperationalError, InterfaceError):
                            # Connection is dead, trigger reconnection
                            await self._handle_health_disconnect()
                            continue

            except asyncio.CancelledError:
                raise
            except Exception:
                # Unexpected error in health monitoring - brief pause
                await asyncio.sleep(1.0)
                continue

    async def _dispatcher(self) -> None:
        """
        Core notification dispatcher:
          - Efficiently waits on PostgreSQL socket (no polling)
          - Distributes each notification to all subscriber queues for that channel
          - Handles disconnections with exponential backoff and automatic re-LISTEN
          - Uses put_nowait() to prevent slow consumers from blocking others

        The dispatcher is the single consumer of conn.notifies() to avoid race conditions
        when multiple coroutines try to read from the same notification stream.
        """
        backoff = 0.2  # start small; increase on failures up to a cap
        while True:
            try:
                conn = await self._ensure_dispatcher_connection()
                # This async iterator blocks efficiently on the socket.
                async for notification in conn.notifies():
                    # Activity means the connection is healthy; reset backoff.
                    backoff = 0.2

                    # Signal notification activity for the health monitor.
                    self._notification_activity.set()

                    # Deliver to payload-keyed subscribers (exact match only).
                    payload_key = (notification.channel, notification.payload)
                    for q in list(self._payload_subs.get(payload_key, ())):
                        try:
                            q.put_nowait(notification)
                        except asyncio.QueueFull:
                            logger.warning(
                                'Notification dropped on channel %r: payload '
                                'subscriber queue full',
                                notification.channel,
                            )

                    # Distribute to all subscriber queues for this channel
                    # Use list() snapshot to avoid mutation during iteration
                    for q in list(self._subs.get(notification.channel, ())):
                        try:
                            q.put_nowait(notification)
                        except asyncio.QueueFull:
                            logger.warning(
                                'Notification dropped on channel %r: subscriber queue full '
                                '(maxsize=%d). Consumer may fall back to polling.',
                                notification.channel,
                                _SUBSCRIBER_QUEUE_MAXSIZE,
                            )

            except OperationalError:
                # Likely a disconnect. Back off a bit, then force reconnect and re-LISTEN.
                await self._close_connections()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)  # cap backoff
                continue
            except asyncio.CancelledError:
                # Graceful shutdown: stop dispatching and exit the task.
                raise
            except Exception as exc:
                # Unexpected issue: brief pause to avoid a hot loop, then try
                # again. Logged — a persistent non-OperationalError failure
                # would otherwise be an invisible busy-reconnect loop.
                logger.error(
                    'Listener dispatcher failed unexpectedly; reconnecting: %s',
                    exc,
                )
                await self._close_connections()
                await asyncio.sleep(0.5)
                continue

    async def listen(
        self, channel_name: str,
    ) -> Result[Queue[Notify], BrokerOperationFailed]:
        """Subscribe to a PostgreSQL notification channel.

        Creates a new asyncio queue for this subscriber and issues LISTEN command
        to PostgreSQL (only once per channel, regardless of subscriber count).

        Parameters
        ----------
        channel_name : str
            PostgreSQL channel name (automatically quoted for SQL safety)

        Returns
        -------
        Result[Queue[Notify], BrokerOperationFailed]
            Ok(queue) on success, Err(BrokerOperationFailed) on connection or SQL failure.

        Raises
        ------
        AssertionError
            If called from a non-owner event loop (programming error, not wrapped).

        Notes
        -----
        - Multiple subscribers to same channel each get independent queues
        - Server-side LISTEN is issued only once per channel
        - Dispatcher automatically starts after first subscription
        """
        self._bind_or_validate_loop()
        conn_r = await self._ensure_connections()
        if is_err(conn_r):
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
                    message=f'Failed to subscribe to {channel_name!r}: {conn_r.err_value.message}',
                    retryable=conn_r.err_value.retryable,
                    exception=conn_r.err_value.exception,
                )
            )

        try:
            async with self._lock:
                # LISTEN on the server if this is the first subscriber for the channel.
                if channel_name not in self._listen_channels:
                    # Stop dispatcher temporarily to safely modify connection listen state.
                    dispatcher_was_running = await self._pause_dispatcher()

                    try:
                        # Issue LISTEN on the dispatcher connection (the one consuming notifies)
                        disp_conn = await self._ensure_dispatcher_connection()
                        await disp_conn.execute(
                            sql.SQL('LISTEN {}').format(sql.Identifier(channel_name))
                        )
                        self._listen_channels.add(channel_name)
                    finally:
                        # If we stopped a running dispatcher, always revive it (even on LISTEN error).
                        if dispatcher_was_running:
                            self._start_dispatcher_if_needed()

                    # First subscription path: no prior dispatcher existed.
                    self._start_dispatcher_if_needed()

                # Create a fresh bounded queue for this subscriber.
                q: Queue[Notify] = asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_MAXSIZE)
                self._subs[channel_name].add(q)
                self._start_health_monitor_if_needed()
                return Ok(q)
        except (OperationalError, InterfaceError, OSError, RuntimeError) as exc:
            # RuntimeError: _ensure_dispatcher_connection found the dispatcher
            # connection torn down by a concurrent reset.
            return Err(
                BrokerOperationFailed(
                    code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
                    message=f'Failed to subscribe to {channel_name!r}: {exc}',
                    retryable=is_retryable_connection_error(exc),
                    exception=exc,
                )
            )

    async def close(self) -> None:
        """
        Stop the dispatcher, health monitor and close the notification connection.
        Safe to call more than once.

        From a non-owner loop: when the owner loop is closed, the listener is
        released on the running loop (``release_after_owner_loop_closed``);
        otherwise the close is handed off to the owner loop, to avoid
        cross-loop task and connection use during teardown.

        Raises:
            Exception: _close_impl itself is best-effort total, but the
                cross-loop handoff can raise (owner loop closing mid-flight
                fails the wrapped future). Both production callers contain
                this: broker.close_async folds it into its error list,
                worker stop/cleanup paths log it.
        """
        current_loop = asyncio.get_running_loop()
        if (
            self._owner_loop is None
            and self._dispatcher_conn is None
            and self._command_conn is None
            and self._health_check_task is None
            and self._dispatcher_task is None
            and not self._subs
            and not self._listen_channels
        ):
            # Already fully closed and detached; preserve loop-agnostic state.
            return
        if self._owner_loop is not None and self._owner_loop is not current_loop:
            owner_loop = self._owner_loop
            if owner_loop.is_closed():
                await self.release_after_owner_loop_closed()
                return
            close_coro = self._close_impl()
            try:
                fut = asyncio.run_coroutine_threadsafe(close_coro, owner_loop)
            except RuntimeError:
                # The owner loop closed after the check above.
                close_coro.close()
                await self.release_after_owner_loop_closed()
                return
            await asyncio.wrap_future(fut)
            return

        self._bind_or_validate_loop()
        await self._close_impl()

    async def release_after_owner_loop_closed(self) -> None:
        """Release this listener after its owner event loop closed.

        The dispatcher and health tasks belong to the closed loop, which can
        never run them again, so they are dropped without ``cancel()``. The
        two connections close on the running loop: a psycopg close is local
        work (``PQfinish``) and needs no event loop. Local bookkeeping is
        cleared, so the listener can bind to a new loop.

        Precondition: the owner loop is closed, or the listener never bound
        to a loop. Breaking it is a programming error and fails an assertion.
        Total otherwise: connection close failures are contained.
        """
        owner_loop = self._owner_loop
        assert owner_loop is None or owner_loop.is_closed(), (
            f'release_after_owner_loop_closed called while the owner loop '
            f'{owner_loop!r} is open; close() it on that loop instead'
        )
        self._health_check_task = None
        self._dispatcher_task = None
        await self._close_connections()
        self._subs.clear()
        self._payload_subs.clear()
        self._listen_channels.clear()
        self._owner_loop = None

    async def _close_impl(self) -> None:
        """Stop the dispatcher, health monitor and close the notification connection.

        Safe to call more than once. Best-effort total: task cancellation
        awaits suppress CancelledError/RuntimeError and connection closes
        are contained in _close_connections; bookkeeping is always cleared.
        """
        if self._health_check_task:
            self._health_check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._health_check_task
            self._health_check_task = None

        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self._dispatcher_task
            self._dispatcher_task = None

        await self._close_connections()

        # Release local bookkeeping to avoid process-lifetime growth.
        self._subs.clear()
        self._payload_subs.clear()
        self._listen_channels.clear()
        self._owner_loop = None
