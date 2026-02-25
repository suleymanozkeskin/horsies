# app/core/brokers/listener.py
"""
PostgreSQL LISTEN/NOTIFY-based notification system for task management:

Architecture:
  TaskApp -> PostgresBroker -> PostgresListener

Flow:
  1. Task submission: INSERT into tasks table -> PostgreSQL trigger -> NOTIFY task_new/task_queue_*
  2. Worker notification: Workers listen to task_new + queue-specific channels
  3. Task processing: Worker picks up task, processes it, updates status
  4. Completion notification: UPDATE tasks status -> PostgreSQL trigger -> NOTIFY task_done
  5. Result retrieval: Clients listen to task_done channel, filter by task_id

Key channels:
  - task_new: Global notification for any new task (workers)
  - task_queue_*: Queue-specific notifications (workers)
  - task_done: Task completion notifications (result waiters)
"""

# app/core/brokers/listener.py
from __future__ import annotations
import asyncio
import contextlib
from collections import defaultdict
from typing import DefaultDict, Optional, Sequence, Set
from asyncio import Task, Queue  # precise type for Pylance/mypy

import psycopg
from psycopg import AsyncConnection, InterfaceError, OperationalError, Notify
from psycopg import sql

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError, BrokerResult
from horsies.core.logging import get_logger
from horsies.core.types.result import Ok, Err, is_err
from horsies.core.utils.db import is_retryable_connection_error

logger = get_logger('listener')

_SUBSCRIBER_QUEUE_MAXSIZE: int = 4096


class PostgresListener:
    """
    PostgreSQL LISTEN/NOTIFY wrapper with async queue distribution.

    Features:
      - Single dispatcher consuming conn.notifies() to avoid competing readers
      - Multiple subscribers per channel via independent asyncio queues
      - Safe SQL with identifier quoting and parameter binding
      - Auto-reconnect with exponential backoff and re-LISTEN
      - Proactive connection health monitoring

    Architecture:
      - dispatcher_conn: Exclusively consumes notifications from conn.notifies()
      - command_conn: Handles LISTEN/UNLISTEN commands separately
      - Per-channel queues: Each subscriber gets independent notification delivery

    Usage:
    ------
    listener = PostgresListener(database_url)
    start_r = await listener.start()       # BrokerResult[None]
    assert start_r.is_ok()

    # Subscribe: returns BrokerResult[Queue[Notify]]
    listen_r = await listener.listen("task_done")
    queue = listen_r.ok_value

    # Wait for notifications (blocks until one arrives)
    notification = await queue.get()
    if notification.payload == "task_123":  # Filter by payload if needed
        # Process notification
        pass

    # Cleanup: remove local subscription
    await listener.unsubscribe("task_done", queue)
    await listener.close()

    Notes:
    ------
    * Uses autocommit=True: NOTIFYs are sent immediately, not held in transactions
    * Dispatcher distributes each notification to ALL subscriber queues for that channel
    * put_nowait() prevents slow consumers from blocking others (drops on queue full)
    * Channel names are PostgreSQL identifiers, automatically quoted for safety
    """

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

        # Two separate connections to avoid blocking issues:
        # 1. Dispatcher connection: exclusively for conn.notifies() consumption
        self._dispatcher_conn: Optional[AsyncConnection] = None
        # 2. Command connection: for LISTEN/UNLISTEN SQL commands
        self._command_conn: Optional[AsyncConnection] = None

        # Set of channels we have LISTENed to on the server.
        self._listen_channels: Set[str] = set()

        # Per-channel subscribers: each subscriber is an asyncio.Queue[Notify].
        # Multiple subscribers can wait on the same channel independently.
        self._subs: DefaultDict[str, Set[Queue[Notify]]] = defaultdict(set)

        # Background dispatcher task consuming conn.notifies() and distributing to subscriber queues.
        self._dispatcher_task: Optional[Task[None]] = None

        # Health check task for proactive disconnection detection
        self._health_check_task: Optional[Task[None]] = None

        # Event for file descriptor activity detection
        self._fd_activity = asyncio.Event()
        self._fd_registered = False

        # A lock used to serialize LISTEN/UNLISTEN and subscription book-keeping.
        self._lock = asyncio.Lock()

        # Event-loop ownership: listener state (lock/tasks/connections) is loop-affine.
        self._owner_loop: Optional[asyncio.AbstractEventLoop] = None

    def _bind_or_validate_loop(self) -> None:
        """Bind to first caller loop and reject cross-loop access thereafter."""
        current_loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = current_loop
            return
        if self._owner_loop is not current_loop:
            raise RuntimeError(
                'PostgresListener is bound to a different event loop. '
                'Do not share a broker/listener instance across async loop and sync LoopRunner contexts.'
            )

    def _remove_local_subscription(
        self, channel_name: str, q: Optional[Queue[Notify]]
    ) -> bool:
        """Remove a local subscriber and compact empty bookkeeping.

        Returns True when no local subscribers remain for the channel.
        """
        subs = self._subs.get(channel_name)
        if subs is None:
            return True
        if q is not None:
            subs.discard(q)
        if not subs:
            self._subs.pop(channel_name, None)
            return True
        return False

    async def start(self) -> BrokerResult[None]:
        """Establish notification connections (autocommit) and start health monitor.

        Safe to call multiple times.

        Returns Ok(None) on success, Err(BrokerOperationError) on connection failure.
        Raises RuntimeError if called from a non-owner event loop (programming error).
        """
        self._bind_or_validate_loop()
        conn_r = await self._ensure_connections()
        if is_err(conn_r):
            return conn_r
        # Defer starting the dispatcher until at least one channel is LISTENed.
        if self._health_check_task is None:
            # Start proactive health monitoring
            self._health_check_task = asyncio.create_task(
                self._health_monitor(), name='pg-listener-health'
            )
        return Ok(None)

    async def _start_listening(self, conn: AsyncConnection) -> None:
        """
        Start listening to all _listen_channels for the given connection.
        """
        for channel in self._listen_channels:
            await conn.execute(sql.SQL('LISTEN {}').format(sql.Identifier(channel)))

    async def _ensure_connections(self) -> BrokerResult[None]:
        """Ensure we have live AsyncConnections in autocommit mode.

        On reconnection, re-issue LISTEN for any previously tracked channels.

        Returns Ok(None) on success, Err(BrokerOperationError) on failure.
        """
        created_dispatcher = False
        created_command = False
        try:
            # Ensure dispatcher connection (for conn.notifies() consumption)
            if self._dispatcher_conn is None or self._dispatcher_conn.closed:
                self._dispatcher_conn = await psycopg.AsyncConnection.connect(
                    self.database_url,
                    autocommit=True,
                )
                created_dispatcher = True
                await self._start_listening(self._dispatcher_conn)
                # Register file descriptor for activity monitoring
                self._register_fd_monitoring()

            # Ensure command connection (for LISTEN/UNLISTEN commands)
            if self._command_conn is None or self._command_conn.closed:
                self._command_conn = await psycopg.AsyncConnection.connect(
                    self.database_url,
                    autocommit=True,
                )
                created_command = True
                await self._start_listening(self._command_conn)

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
                self._unregister_fd_monitoring()
                if self._dispatcher_conn is not None and not self._dispatcher_conn.closed:
                    with contextlib.suppress(
                        OperationalError, InterfaceError, RuntimeError, OSError
                    ):
                        await self._dispatcher_conn.close()
                self._dispatcher_conn = None
            return Err(BrokerOperationError(
                code=BrokerErrorCode.LISTENER_START_FAILED,
                message=f'Failed to establish listener connections: {exc}',
                retryable=is_retryable_connection_error(exc),
                exception=exc,
            ))

    async def _ensure_dispatcher_connection(self) -> AsyncConnection:
        """Ensure dispatcher connection is available.

        Unwraps the Result from _ensure_connections, re-raising the original
        exception on Err so the dispatcher loop's existing exception handling
        (OperationalError catch + reconnect) continues to work.
        """
        if self._dispatcher_conn is None or self._dispatcher_conn.closed:
            conn_r = await self._ensure_connections()
            if is_err(conn_r):
                err = conn_r.err_value
                raise err.exception or RuntimeError(err.message)
        assert self._dispatcher_conn is not None
        return self._dispatcher_conn

    async def _ensure_command_connection(self) -> AsyncConnection:
        """Ensure command connection is available.

        Unwraps the Result from _ensure_connections, re-raising the original
        exception on Err so internal callers (health monitor) see the expected
        exception types.
        """
        if self._command_conn is None or self._command_conn.closed:
            conn_r = await self._ensure_connections()
            if is_err(conn_r):
                err = conn_r.err_value
                raise err.exception or RuntimeError(err.message)
        assert self._command_conn is not None
        return self._command_conn

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

    def _register_fd_monitoring(self) -> None:
        """
        Register file descriptor monitoring for proactive disconnection detection.
        """
        if (
            self._dispatcher_conn
            and not self._dispatcher_conn.closed
            and not self._fd_registered
        ):
            try:
                loop = asyncio.get_running_loop()
                loop.add_reader(self._dispatcher_conn.fileno(), self._fd_activity.set)
                self._fd_registered = True
            except (OSError, AttributeError):
                # fileno() might not be available or connection might be closed
                pass

    def _unregister_fd_monitoring(self) -> None:
        """
        Unregister file descriptor monitoring.
        """
        if self._fd_registered and self._dispatcher_conn:
            try:
                loop = asyncio.get_running_loop()
                loop.remove_reader(self._dispatcher_conn.fileno())
            except (OSError, AttributeError, ValueError, OperationalError):
                # Connection might be closed or already removed;
                # OperationalError raised by fileno() when connection is dead.
                pass
            finally:
                self._fd_registered = False

    async def _handle_health_disconnect(self) -> None:
        """Reset listener connections after health monitor detects a dead connection."""
        self._unregister_fd_monitoring()
        await self._close_connections()

    async def _health_monitor(self) -> None:
        """
        Proactive health monitoring using psycopg3 recommended pattern:
        - Monitor file descriptor activity with timeout
        - Perform health checks during idle periods
        - Detect disconnections faster than waiting for operations to fail
        """
        while True:
            try:
                # Wait up to 60 seconds for file descriptor activity
                try:
                    await asyncio.wait_for(self._fd_activity.wait(), timeout=60.0)
                    self._fd_activity.clear()

                    # Activity detected - verify connection health
                    if self._command_conn and not self._command_conn.closed:
                        try:
                            await self._command_conn.execute('SELECT 1')
                        except OperationalError:
                            # Connection is dead, trigger reconnection
                            await self._handle_health_disconnect()
                            continue

                except asyncio.TimeoutError:
                    # No activity for 60 seconds - perform health check
                    if self._command_conn and not self._command_conn.closed:
                        try:
                            await self._command_conn.execute('SELECT 1')
                        except OperationalError:
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

                    # Signal file descriptor activity
                    self._fd_activity.set()

                    # Distribute to all subscriber queues for this channel
                    # Use list() snapshot to avoid mutation during iteration
                    for q in list(self._subs.get(notification.channel, ())):
                        try:
                            q.put_nowait(notification)
                        except asyncio.QueueFull:
                            # Drop notification if queue is full (prevents blocking other subscribers)
                            # Consider increasing queue size if this happens frequently
                            pass

            except OperationalError:
                # Likely a disconnect. Back off a bit, then force reconnect and re-LISTEN.
                self._unregister_fd_monitoring()
                await self._close_connections()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)  # cap backoff
                continue
            except asyncio.CancelledError:
                # Graceful shutdown: stop dispatching and exit the task.
                self._unregister_fd_monitoring()
                raise
            except Exception:
                # Unexpected issue: brief pause to avoid a hot loop, then try again.
                self._unregister_fd_monitoring()
                await self._close_connections()
                await asyncio.sleep(0.5)
                continue

    async def listen(self, channel_name: str) -> BrokerResult[Queue[Notify]]:
        """Subscribe to a PostgreSQL notification channel.

        Creates a new asyncio queue for this subscriber and issues LISTEN command
        to PostgreSQL (only once per channel, regardless of subscriber count).

        Parameters
        ----------
        channel_name : str
            PostgreSQL channel name (automatically quoted for SQL safety)

        Returns
        -------
        BrokerResult[Queue[Notify]]
            Ok(queue) on success, Err(BrokerOperationError) on connection or SQL failure.

        Raises
        ------
        RuntimeError
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
            return Err(BrokerOperationError(
                code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
                message=f'Failed to subscribe to {channel_name!r}: {conn_r.err_value.message}',
                retryable=conn_r.err_value.retryable,
                exception=conn_r.err_value.exception,
            ))

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
                return Ok(q)
        except (OperationalError, InterfaceError, OSError) as exc:
            return Err(BrokerOperationError(
                code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
                message=f'Failed to subscribe to {channel_name!r}: {exc}',
                retryable=is_retryable_connection_error(exc),
                exception=exc,
            ))

    async def listen_many(
        self, channel_names: Sequence[str],
    ) -> BrokerResult[list[Queue[Notify]]]:
        """Subscribe to multiple channels in a single lock acquisition.

        More efficient than calling listen() in a loop because the
        dispatcher is stopped/restarted at most once.

        Returns Ok(queues) in the same order as *channel_names*,
        or Err(BrokerOperationError) on connection or SQL failure.

        Raises RuntimeError if called from a non-owner event loop.
        """
        self._bind_or_validate_loop()
        conn_r = await self._ensure_connections()
        if is_err(conn_r):
            return Err(BrokerOperationError(
                code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
                message=f'Failed to subscribe to channels: {conn_r.err_value.message}',
                retryable=conn_r.err_value.retryable,
                exception=conn_r.err_value.exception,
            ))

        try:
            async with self._lock:
                new_channels = [
                    ch for ch in channel_names if ch not in self._listen_channels
                ]

                if new_channels:
                    # Stop dispatcher once for all new channels.
                    dispatcher_was_running = await self._pause_dispatcher()

                    try:
                        disp_conn = await self._ensure_dispatcher_connection()
                        for ch in new_channels:
                            await disp_conn.execute(
                                sql.SQL('LISTEN {}').format(sql.Identifier(ch))
                            )
                            self._listen_channels.add(ch)
                    finally:
                        # If dispatcher was already running, revive it on both success/failure.
                        if dispatcher_was_running:
                            self._start_dispatcher_if_needed()

                    # First-subscription path: no prior dispatcher existed.
                    self._start_dispatcher_if_needed()

                # Create bounded queues for all requested channels
                queues: list[Queue[Notify]] = []
                for ch in channel_names:
                    q: Queue[Notify] = asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_MAXSIZE)
                    self._subs[ch].add(q)
                    queues.append(q)
                return Ok(queues)
        except (OperationalError, InterfaceError, OSError) as exc:
            return Err(BrokerOperationError(
                code=BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
                message=f'Failed to subscribe to channels: {exc}',
                retryable=is_retryable_connection_error(exc),
                exception=exc,
            ))

    async def unlisten(
        self, channel_name: str, q: Optional[Queue[Notify]] = None
    ) -> None:
        """
        Remove subscription and issue UNLISTEN if no subscribers remain.

        This removes the server-side LISTEN when the last local subscriber for the
        channel is removed.

        Parameters
        ----------
        channel_name : str
            Channel name used in listen()
        q : Optional[Queue[Notify]]
            Specific queue to remove. If None, only checks for server-side cleanup.

        Notes
        -----
        - Removes server-side LISTEN when local subscriber count reaches zero
        - Cleans up local bookkeeping for empty channels
        """
        self._bind_or_validate_loop()
        async with self._lock:
            no_local_subs = self._remove_local_subscription(channel_name, q)
            # If nobody is listening locally, drop the server-side LISTEN.
            if no_local_subs and channel_name in self._listen_channels:
                # Only UNLISTEN via the command connection to avoid racing dispatcher notifies iterator
                if self._command_conn and not self._command_conn.closed:
                    await self._command_conn.execute(
                        sql.SQL('UNLISTEN {}').format(sql.Identifier(channel_name))
                    )

                self._listen_channels.discard(channel_name)

    async def unsubscribe(
        self, channel_name: str, q: Optional[Queue[Notify]] = None
    ) -> None:
        """
        Remove a local queue subscription.

        If the removed queue was the last local subscriber for the channel, this
        also issues UNLISTEN and removes local channel tracking to prevent
        unbounded metadata growth over long runtimes.

        Parameters
        ----------
        channel_name : str
            Channel name used in listen()
        q : Optional[Queue[Notify]]
            Specific queue to remove from local subscriptions

        Notes
        -----
        - Cleans up empty local subscription entries
        - Drops server-side LISTEN when local subscriber count reaches zero
        - Prevents channel tracking from growing without bound
        """
        self._bind_or_validate_loop()
        async with self._lock:
            no_local_subs = self._remove_local_subscription(channel_name, q)
            if no_local_subs and channel_name in self._listen_channels:
                if self._command_conn and not self._command_conn.closed:
                    await self._command_conn.execute(
                        sql.SQL('UNLISTEN {}').format(sql.Identifier(channel_name))
                    )
                self._listen_channels.discard(channel_name)

    async def close(self) -> None:
        """
        Stop the dispatcher, health monitor and close the notification connection.
        Safe to call more than once.

        If called from a non-owner loop, close is handed off to the owner loop
        to avoid cross-loop task/connection misuse during teardown.
        """
        current_loop = asyncio.get_running_loop()
        if (
            self._owner_loop is None
            and self._dispatcher_conn is None
            and self._command_conn is None
            and self._health_check_task is None
            and self._dispatcher_task is None
            and not self._fd_registered
            and not self._subs
            and not self._listen_channels
        ):
            # Already fully closed and detached; preserve loop-agnostic state.
            return
        if self._owner_loop is not None and self._owner_loop is not current_loop:
            owner_loop = self._owner_loop
            if owner_loop.is_closed():
                logger.warning(
                    'Skipping listener close from non-owner loop: owner loop already closed'
                )
                return
            try:
                fut = asyncio.run_coroutine_threadsafe(
                    self._close_impl(), owner_loop
                )
            except RuntimeError:
                logger.warning(
                    'Skipping listener close from non-owner loop: owner loop not running'
                )
                return
            await asyncio.wrap_future(fut)
            return

        self._bind_or_validate_loop()
        await self._close_impl()

    async def _close_impl(self) -> None:
        """
        Stop the dispatcher, health monitor and close the notification connection.
        Safe to call more than once.
        """
        if self._health_check_task:
            self._health_check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_check_task
            self._health_check_task = None

        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dispatcher_task
            self._dispatcher_task = None

        self._unregister_fd_monitoring()
        await self._close_connections()

        # Release local bookkeeping to avoid process-lifetime growth.
        self._subs.clear()
        self._listen_channels.clear()
        self._owner_loop = None
