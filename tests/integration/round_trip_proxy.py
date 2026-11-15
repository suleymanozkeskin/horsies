"""A TCP proxy in front of Postgres that counts client round trips.

A round trip is a client send that follows server data, or the first send of
a connection. The proxy parses each client send into its protocol messages,
so a turn shows what the client sent: ``Q:<sql>`` for a simple query,
``P:<sql>`` for a parse, ``startup`` for the startup packet, and the type
letter of each other message (``B`` bind, ``D`` describe, ``E`` execute,
``S`` sync). A prepared statement in its steady state sends no SQL text, only
``B``, ``D``, ``E`` and ``S``.

The proxy runs on its own thread and event loop, so sync and async clients
on any thread or loop of the test can use it.
"""

from __future__ import annotations

import asyncio
import struct
import threading
from dataclasses import dataclass, field
from typing import Final

_READ_CHUNK_BYTES: Final = 65536
_START_TIMEOUT_S: Final = 5.0
_STOP_TIMEOUT_S: Final = 5.0
_SSL_OR_GSS_REQUEST_CODES: Final = frozenset({80877103, 80877104})
_SQL_PREVIEW_CHARS: Final = 90


@dataclass(frozen=True, slots=True)
class Turn:
    """One client round trip: the connection it ran on and its messages."""

    connection: int
    messages: tuple[str, ...]

    def has_simple_query(self, sql: str) -> bool:
        """Whether the turn sent ``sql`` as a simple query."""
        return f'Q:{sql}' in self.messages


@dataclass
class _OpenTurn:
    connection: int
    messages: list[str] = field(default_factory=list[str])


@dataclass
class _Connection:
    number: int
    startup_done: bool = False
    pending: bytes = b''
    server_spoke_last: bool = True
    turn: _OpenTurn | None = None
    drop_reply: bool = False


def _parse_client_messages(connection: _Connection) -> list[str]:
    """Split the buffered client bytes into complete protocol messages."""
    messages: list[str] = []
    buffer = connection.pending
    while True:
        if not connection.startup_done:
            if len(buffer) < 8:
                break
            (length,) = struct.unpack('!I', buffer[:4])
            (code,) = struct.unpack('!I', buffer[4:8])
            if len(buffer) < length:
                break
            if code in _SSL_OR_GSS_REQUEST_CODES:
                messages.append('ssl-request')
            else:
                messages.append('startup')
                connection.startup_done = True
            buffer = buffer[length:]
            continue
        if len(buffer) < 5:
            break
        kind = chr(buffer[0])
        (length,) = struct.unpack('!I', buffer[1:5])
        if len(buffer) < 1 + length:
            break
        payload = buffer[5 : 1 + length]
        buffer = buffer[1 + length :]
        messages.append(_describe_message(kind, payload))
    connection.pending = buffer
    return messages


def _describe_message(kind: str, payload: bytes) -> str:
    match kind:
        case 'Q':
            sql = payload.rstrip(b'\x00').decode('utf-8', 'replace')
            return 'Q:' + ' '.join(sql.split())[:_SQL_PREVIEW_CHARS]
        case 'P':
            name_end = payload.index(b'\x00')
            rest = payload[name_end + 1 :]
            sql = rest[: rest.index(b'\x00')].decode('utf-8', 'replace')
            return 'P:' + ' '.join(sql.split())[:_SQL_PREVIEW_CHARS]
        case _:
            return kind


class RoundTripProxy:
    """Forwards 127.0.0.1:<port> to the upstream Postgres and records turns.

    ``start_recording`` and ``stop_recording`` bracket the turns to count.
    ``drop_next_execute_reply`` makes the proxy drop the server reply of the
    next client turn that executes a statement, and close both sides of that
    connection: the server ran the statement, and the client never reads the
    reply.
    """

    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        self._upstream_host = upstream_host
        self._upstream_port = upstream_port
        self._lock = threading.Lock()
        self._turns: list[_OpenTurn] = []
        self._recording = False
        self._drop_armed = False
        self._dropped = threading.Event()
        self._connections = 0
        self._writers: set[asyncio.StreamWriter] = set()
        self._handlers: set[asyncio.Task[None]] = set()
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name='round-trip-proxy',
            daemon=True,
        )
        self._server: asyncio.Server | None = None
        self.port = 0

    def start(self) -> None:
        """Start listening on a free port of 127.0.0.1 (``port``)."""
        self._thread.start()
        if not self._ready.wait(_START_TIMEOUT_S):
            raise RuntimeError('round trip proxy did not start')

    def stop(self) -> None:
        """Close every proxied connection and stop the thread."""
        future = asyncio.run_coroutine_threadsafe(self._close(), self._loop)
        future.result(_STOP_TIMEOUT_S)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(_STOP_TIMEOUT_S)

    def start_recording(self) -> None:
        with self._lock:
            self._turns = []
            self._recording = True

    def stop_recording(self) -> tuple[Turn, ...]:
        with self._lock:
            self._recording = False
            return tuple(
                Turn(connection=turn.connection, messages=tuple(turn.messages))
                for turn in self._turns
            )

    def drop_next_execute_reply(self) -> None:
        with self._lock:
            self._drop_armed = True
            self._dropped.clear()

    def reply_was_dropped(self, timeout_s: float) -> bool:
        return self._dropped.wait(timeout_s)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._server = self._loop.run_until_complete(
            asyncio.start_server(self._handle, '127.0.0.1', 0),
        )
        self.port = int(self._server.sockets[0].getsockname()[1])
        self._ready.set()
        self._loop.run_forever()

    async def _close(self) -> None:
        if self._server is not None:
            self._server.close()
        for writer in list(self._writers):
            writer.close()
        if self._handlers:
            _, pending = await asyncio.wait(self._handlers, timeout=_STOP_TIMEOUT_S)
            for handler in pending:
                _ = handler.cancel()

    async def _handle(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        handler = asyncio.current_task()
        assert handler is not None, 'a client handler runs as a task'
        self._handlers.add(handler)
        handler.add_done_callback(self._handlers.discard)
        with self._lock:
            self._connections += 1
            connection = _Connection(number=self._connections)
        upstream_reader, upstream_writer = await asyncio.open_connection(
            self._upstream_host,
            self._upstream_port,
        )
        self._writers.update((client_writer, upstream_writer))
        _ = await asyncio.gather(
            self._client_to_server(connection, client_reader, upstream_writer),
            self._server_to_client(
                connection,
                upstream_reader,
                client_writer,
                upstream_writer,
            ),
            return_exceptions=True,
        )
        self._writers.difference_update((client_writer, upstream_writer))

    async def _client_to_server(
        self,
        connection: _Connection,
        client_reader: asyncio.StreamReader,
        upstream_writer: asyncio.StreamWriter,
    ) -> None:
        try:
            await self._pump_client(connection, client_reader, upstream_writer)
        except ConnectionError:
            pass
        upstream_writer.close()

    async def _pump_client(
        self,
        connection: _Connection,
        client_reader: asyncio.StreamReader,
        upstream_writer: asyncio.StreamWriter,
    ) -> None:
        while data := await client_reader.read(_READ_CHUNK_BYTES):
            with self._lock:
                connection.pending += data
                messages = _parse_client_messages(connection)
                if connection.server_spoke_last or connection.turn is None:
                    connection.turn = _OpenTurn(connection=connection.number)
                    if self._recording:
                        self._turns.append(connection.turn)
                connection.turn.messages.extend(messages)
                connection.server_spoke_last = False
                if self._drop_armed and 'E' in messages:
                    self._drop_armed = False
                    connection.drop_reply = True
            upstream_writer.write(data)
            await upstream_writer.drain()

    async def _server_to_client(
        self,
        connection: _Connection,
        upstream_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        upstream_writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while data := await upstream_reader.read(_READ_CHUNK_BYTES):
                with self._lock:
                    connection.server_spoke_last = True
                    drop = connection.drop_reply
                if drop:
                    upstream_writer.close()
                    self._dropped.set()
                    break
                client_writer.write(data)
                await client_writer.drain()
        except ConnectionError:
            pass
        client_writer.close()
